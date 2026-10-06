package workflow

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

// ---- 共享重试预算：耗尽后不再触发重试，受影响节点按重试耗尽对待 ----

func TestSharedRetryBudgetExhaustion(t *testing.T) {
	// 两个互不依赖、永远失败的节点；每节点自身允许 5 次尝试，但整次运行只给 3 次重试。
	var mu sync.Mutex
	calls := map[string]int{}
	reg := TaskRegistry{}
	for _, n := range []string{"x", "y"} {
		n := n
		reg[n] = func(_ context.Context, _ string, attempt int) error {
			mu.Lock()
			calls[n]++
			mu.Unlock()
			t.Logf("输入: 节点 %s 第 %d 次尝试，判定依据: 共享重试预算=3，首次尝试不扣预算", n, attempt)
			return fmt.Errorf("%s boom on attempt %d", n, attempt)
		}
	}
	nodes := []NodeDef{
		{Name: "x"},
		{Name: "y"},
	}
	cfg := Config{
		MaxConcurrency: 1, // 串行：x 先把预算用满，y 完全拿不到重试，结论确定
		DefaultRetry:   RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Millisecond},
		MaxRetries:     3,
	}
	store := NewFileStore(filepath.Join(t.TempDir(), "s.json"))
	e, err := NewEngine("wf-budget", nodes, reg, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	e.WithLogger(t.Logf)
	res, err := e.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	rep := res.State.RetryReport()
	t.Logf("结果: succeeded=%v 调用次数=%v 预算用量 used=%d/%d 每节点=%v 预算阻断=%v",
		res.Succeeded, calls, rep.Used, rep.Budget, rep.PerNode, rep.BudgetBlocked)

	if res.Succeeded {
		t.Fatal("存在永久失败节点，整体不得判定为成功")
	}
	// 预算 3 次全被 x 用掉：x 共 4 次尝试（1 首次 + 3 重试），y 只有 1 次首次尝试。
	if calls["x"] != 4 {
		t.Fatalf("x 应发起 1 次首次 + 3 次重试 = 4 次尝试，实际 %d", calls["x"])
	}
	if calls["y"] != 1 {
		t.Fatalf("y 不应获得任何重试，实际尝试 %d 次", calls["y"])
	}
	if rep.Used != 3 || rep.Budget != 3 {
		t.Fatalf("预算用量错误: used=%d budget=%d", rep.Used, rep.Budget)
	}
	if rep.PerNode["x"] != 3 || rep.PerNode["y"] != 0 {
		t.Fatalf("每节点重试用量错误: %v", rep.PerNode)
	}
	// 两个节点都是“因为共享预算耗尽”才没再重试：
	// x 自身 MaxAttempts=5 只跑到 4，y 只跑到 1；二者都不是自身上限耗尽。
	for _, n := range []string{"x", "y"} {
		st := res.State.Nodes[n]
		if st.Status != StatusFailed {
			t.Fatalf("%s 状态=%s，期望 failed", n, st.Status)
		}
		if !st.BudgetBlocked {
			t.Fatalf("%s 应标记 BudgetBlocked（自身 MaxAttempts 未用尽，因共享预算耗尽而终止）", n)
		}
	}
	gotBlocked := append([]string(nil), rep.BudgetBlocked...)
	sort.Strings(gotBlocked)
	if len(gotBlocked) != 2 || gotBlocked[0] != "x" || gotBlocked[1] != "y" {
		t.Fatalf("预算阻断节点列表错误: %v", gotBlocked)
	}
	if res.State.Nodes["x"].Attempts != 4 || res.State.Nodes["y"].Attempts != 1 {
		t.Fatalf("Attempts 记账错误: x=%d y=%d",
			res.State.Nodes["x"].Attempts, res.State.Nodes["y"].Attempts)
	}
}

// ---- 并发受限：空槽按优先级降序分配，同优先级按定义顺序（稳定） ----

func TestPrioritySchedulingUnderConcurrency(t *testing.T) {
	const limit = 2
	// 每个节点一个独立 gate：进入任务体后登记在途集合，测试可逐个放行以精确观测
	// “哪个节点拿到了刚空出的槽位”。goroutine 启动顺序不确定，但槽位归属由
	// 调度器在锁内决定，是确定的。
	gates := map[string]chan struct{}{
		"low":  make(chan struct{}),
		"high": make(chan struct{}),
		"mid":  make(chan struct{}),
		"tie":  make(chan struct{}),
	}
	entered := map[string]chan struct{}{}
	for n, g := range gates {
		_ = g
		c := make(chan struct{}, 4)
		entered[n] = c
	}
	var enteredMu sync.Mutex
	inflight := map[string]int{}

	// 定义顺序故意打乱：low 在前、high 在中、mid 在后；
	// tie 与 low 同优先级但定义顺序更靠后，验证同优先级稳定性。
	nodes := []NodeDef{
		{Name: "low", Priority: 1},
		{Name: "high", Priority: 10},
		{Name: "mid", Priority: 5},
		{Name: "tie", Priority: 1},
	}
	reg := TaskRegistry{}
	for _, n := range nodes {
		n := n
		reg[n.Name] = func(_ context.Context, _ string, _ int) error {
			enteredMu.Lock()
			inflight[n.Name]++
			enteredMu.Unlock()
			select {
			case entered[n.Name] <- struct{}{}:
			default:
			}
			<-gates[n.Name] // 占住槽位，等测试显式放行
			return nil
		}
	}
	e, err := NewEngine("wf-prio", nodes, reg,
		Config{MaxConcurrency: limit, DefaultRetry: RetryPolicy{MaxAttempts: 1}},
		NewFileStore(filepath.Join(t.TempDir(), "s.json")))
	if err != nil {
		t.Fatal(err)
	}
	e.WithLogger(t.Logf)
	done := make(chan struct{})
	go func() {
		res, _ := e.Run(context.Background())
		if !res.Succeeded {
			t.Errorf("应全部成功: %+v", res)
		}
		close(done)
	}()

	waitIn := func(want ...string) {
		t.Helper()
		deadline := time.After(2 * time.Second)
		for _, n := range want {
			select {
			case <-entered[n]:
			case <-deadline:
				t.Fatalf("超时等待节点 %q 拿到槽位，当前在途=%v", n, snapshotInflight(&enteredMu, inflight))
			}
		}
	}
	assertNotIn := func(forbidden string, d time.Duration) {
		t.Helper()
		select {
		case <-entered[forbidden]:
			t.Fatalf("节点 %q 不应在此刻拿到槽位", forbidden)
		case <-time.After(d):
		}
	}

	// 首批：只有 high、mid 能拿到槽位；low/tie 必须等待。
	waitIn("high", "mid")
	assertNotIn("low", 30*time.Millisecond)
	assertNotIn("tie", 0)
	t.Logf("输入: 槽位=%d 定义顺序=[low high mid tie] 优先级={high:10,mid:5,low:1,tie:1}；首批占槽=[high mid]", limit)

	// high 空出的槽位：low 与 tie 同优先级，按定义顺序必须先给 low。
	close(gates["high"])
	waitIn("low")
	assertNotIn("tie", 30*time.Millisecond)
	t.Logf("high 放后空槽给 low（同优先级按定义顺序），tie 仍在等待")

	// mid 空出的槽位给最后一个就绪节点 tie。
	close(gates["mid"])
	waitIn("tie")
	close(gates["low"])
	close(gates["tie"])
	<-done

	enteredMu.Lock()
	t.Logf("各节点进入任务体次数=%v 判定依据: 每个节点恰好 1 次", inflight)
	for _, n := range []string{"low", "high", "mid", "tie"} {
		if inflight[n] != 1 {
			t.Fatalf("节点 %s 进入次数=%d，期望 1", n, inflight[n])
		}
	}
	enteredMu.Unlock()
}

// runInmem 用临时文件 store 跑一次工作流（测试辅助）。
func runInmem(t *testing.T, name string, nodes []NodeDef, reg TaskRegistry, cfg Config) (*RunResult, error) {
	t.Helper()
	e, err := NewEngine(name, nodes, reg, cfg, NewFileStore(filepath.Join(t.TempDir(), "s.json")))
	if err != nil {
		return nil, err
	}
	e.WithLogger(t.Logf)
	return e.Run(context.Background())
}

func snapshotInflight(mu *sync.Mutex, m map[string]int) map[string]int {
	mu.Lock()
	defer mu.Unlock()
	cp := make(map[string]int, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

// ---- 中断重启：已花预算、已成功节点、调度选择都不丢，整体判定前后一致 ----

func TestRestartBudgetAndSchedulingPersist(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s.json")
	nodes := []NodeDef{
		{Name: "a"},
		{Name: "b", DependsOn: []string{"a"}},
		{Name: "c", DependsOn: []string{"b"}},
	}
	cfg := func() Config {
		return Config{
			MaxConcurrency: 1,
			DefaultRetry:   RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Hour},
			MaxRetries:     2,
		}
	}
	// 恢复进程使用短退避：共享预算存在状态文件里、与本次配置无关；
	// 同时避免第二进程卡在一小时退避窗口里。
	cfgShort := func() Config {
		c := cfg()
		c.DefaultRetry.InitialBackoff = time.Millisecond
		return c
	}

	// 第一次进程：a 成功；b 首次失败，退避窗口内取消（模拟进程重启）。
	// 用确定性信号代替 time.Sleep：b 首次失败返回后关闭通道，测试再取消，
	// 取消点必然落在长退避窗口内（此时尚未为第 2 次尝试扣预算）。
	var aCalls1, cCalls1 int
	bFailedCh := make(chan struct{})
	reg1 := TaskRegistry{
		"a": func(context.Context, string, int) error { aCalls1++; return nil },
		"b": func(_ context.Context, _ string, attempt int) error {
			if attempt == 1 {
				close(bFailedCh)
				return fmt.Errorf("b first failure")
			}
			return nil // 重启后第 2 次尝试才应成功
		},
		"c": func(context.Context, string, int) error { cCalls1++; return nil },
	}
	e1, _ := NewEngine("wf-budget-resume", nodes, reg1, cfg(), NewFileStore(dir))
	e1.WithLogger(t.Logf)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-bFailedCh: // b 已首次失败，随后进入长退避，此刻取消必然落在退避中
		case <-time.After(2 * time.Second):
		}
		cancel()
	}()
	res1, err := e1.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("第一次进程: succeeded=%v finished=%v b状态=%s b尝试=%d 预算used=%d/%d c执行=%d",
		res1.Succeeded, res1.State.Finished, res1.State.Nodes["b"].Status,
		res1.State.Nodes["b"].Attempts, res1.State.RetriesUsed, res1.State.RetryBudget, cCalls1)
	if res1.State.Finished {
		t.Fatal("中断时工作流不应被结算为终态")
	}
	if res1.State.RetryBudget != 2 {
		t.Fatalf("预算总量应落盘为 2，实际 %d", res1.State.RetryBudget)
	}
	// 取消发生在退避中：尚未为第 2 次尝试扣预算，因此 used=0、b.Attempts=1。
	if res1.State.RetriesUsed != 0 || res1.State.Nodes["b"].Attempts != 1 {
		t.Fatalf("退避中取消不应消耗预算: used=%d bAttempts=%d",
			res1.State.RetriesUsed, res1.State.Nodes["b"].Attempts)
	}

	// 第二次进程：同一状态文件。b 的第 2 次尝试要扣 1 次共享预算并成功；
	// c 首次尝试失败后申请重试，此时预算还剩 1 → 扣得后第 2 次仍失败且节点自身
	// MaxAttempts 未用尽，但预算已用完 → c 永久失败（BudgetBlocked）。
	var aCalls2 int
	bAttempts2 := []int{}
	var mu sync.Mutex
	reg2 := TaskRegistry{
		"a": func(context.Context, string, int) error { aCalls2++; return nil },
		"b": func(_ context.Context, _ string, attempt int) error {
			mu.Lock()
			bAttempts2 = append(bAttempts2, attempt)
			mu.Unlock()
			if attempt == 2 {
				return nil
			}
			return fmt.Errorf("b unexpected attempt %d", attempt)
		},
		"c": func(_ context.Context, _ string, attempt int) error {
			// 第 1、2 次都失败；第 2 次是用掉最后 1 次共享预算的重试。
			return fmt.Errorf("c fails on attempt %d", attempt)
		},
	}
	e2, _ := NewEngine("wf-budget-resume", nodes, reg2, cfgShort(), NewFileStore(dir))
	e2.WithLogger(t.Logf)
	res2, err := e2.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rep := res2.State.RetryReport()
	t.Logf("第二次进程: resumed=%v finished=%v succeeded=%v b尝试=%v a重跑=%d 预算used=%d/%d 每节点=%v 阻断=%v",
		res2.Resumed, res2.State.Finished, res2.Succeeded, bAttempts2, aCalls2,
		rep.Used, rep.Budget, rep.PerNode, rep.BudgetBlocked)

	if !res2.Resumed {
		t.Fatal("应识别为恢复执行")
	}
	if !res2.State.Finished {
		t.Fatal("第二次进程应走到终态结算")
	}
	if res2.Succeeded {
		t.Fatal("c 永久失败，整体必须判定为失败（与中断前“未结算”区分，恢复后判定确定）")
	}
	if aCalls1 != 1 || aCalls2 != 0 {
		t.Fatalf("已成功节点 a 不得重跑: first=%d second=%d", aCalls1, aCalls2)
	}
	if len(bAttempts2) != 1 || bAttempts2[0] != 2 {
		t.Fatalf("b 应以历史计数继续，只在第二进程发起第 2 次尝试，实际 %v", bAttempts2)
	}
	if res2.State.Nodes["b"].Status != StatusSucceeded {
		t.Fatalf("b 应在第 2 次尝试成功，实际 %s", res2.State.Nodes["b"].Status)
	}
	cst := res2.State.Nodes["c"]
	if cst.Status != StatusFailed || !cst.BudgetBlocked || cst.Attempts != 2 {
		t.Fatalf("c 应用尽共享预算后永久失败: status=%s blocked=%v attempts=%d",
			cst.Status, cst.BudgetBlocked, cst.Attempts)
	}
	if rep.Budget != 2 || rep.Used != 2 {
		t.Fatalf("预算必须跨进程累计且不重置: budget=%d used=%d", rep.Budget, rep.Used)
	}
	if rep.PerNode["b"] != 1 || rep.PerNode["c"] != 1 {
		t.Fatalf("每节点预算用量跨进程记账错误: %v", rep.PerNode)
	}

	// 第三次再跑：终态已定，不得再有任何尝试，判定保持失败。
	var aCalls3, cCalls3 int
	reg3 := TaskRegistry{
		"a": func(context.Context, string, int) error { aCalls3++; return nil },
		"b": func(context.Context, string, int) error { t.Fatal("b 已成功，不得重跑"); return nil },
		"c": func(context.Context, string, int) error { cCalls3++; return nil },
	}
	e3, _ := NewEngine("wf-budget-resume", nodes, reg3, cfgShort(), NewFileStore(dir))
	res3, err := e3.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("第三次进程: resumed=%v succeeded=%v 预算used=%d a执行=%d c执行=%d 判定依据: 终态稳定",
		res3.Resumed, res3.Succeeded, res3.State.RetriesUsed, aCalls3, cCalls3)
	if !res3.Resumed || res3.Succeeded || res3.State.RetriesUsed != 2 || aCalls3 != 0 || cCalls3 != 0 {
		t.Fatalf("终态恢复不一致: resumed=%v succeeded=%v used=%d a=%d c=%d",
			res3.Resumed, res3.Succeeded, res3.State.RetriesUsed, aCalls3, cCalls3)
	}
}

// ---- 旧用法兼容：不设共享预算、不设优先级时，结果与调度语义保持不变 ----

func TestLegacyConfigUnchanged(t *testing.T) {
	// Config 不含 MaxRetries（零值），NodeDef 不含 Priority（零值）。
	nodes := []NodeDef{
		{Name: "c", DependsOn: []string{"a", "b"}},
		{Name: "a"},
		{Name: "b", DependsOn: []string{"a"}},
	}
	var mu sync.Mutex
	var order []string
	reg := TaskRegistry{}
	for _, n := range []string{"a", "b", "c"} {
		n := n
		reg[n] = func(context.Context, string, int) error {
			mu.Lock()
			order = append(order, n)
			mu.Unlock()
			return nil
		}
	}
	cfg := Config{MaxConcurrency: 2, DefaultRetry: RetryPolicy{MaxAttempts: 1}}
	res, err := runInmem(t, "wf-legacy", nodes, reg, cfg)
	if err != nil || !res.Succeeded {
		t.Fatalf("旧用法应整体成功: err=%v res=%+v", err, res)
	}
	mu.Lock()
	t.Logf("旧用法执行顺序=%v 判定依据: 与历史一致 a→b→c", order)
	if len(order) != 3 || order[0] != "a" || order[1] != "b" || order[2] != "c" {
		t.Fatalf("旧用法调度顺序改变: %v", order)
	}
	mu.Unlock()

	if res.State.RetryBudget != 0 || res.State.RetriesUsed != 0 {
		t.Fatalf("未设 MaxRetries 时不得出现共享预算记账: budget=%d used=%d",
			res.State.RetryBudget, res.State.RetriesUsed)
	}
	rep := res.State.RetryReport()
	if rep.Budget != 0 || rep.Used != 0 || len(rep.BudgetBlocked) != 0 {
		t.Fatalf("旧用法不应产生预算报告: %+v", rep)
	}
	for _, n := range nodes {
		if res.State.Nodes[n.Name].BudgetBlocked {
			t.Fatalf("旧用法节点 %s 不应被标记预算阻断", n.Name)
		}
	}

	// 旧用法下重试只受节点自身 MaxAttempts 约束：预算报告不参与判定。
	failNodes := []NodeDef{{Name: "f"}}
	failReg := TaskRegistry{"f": func(_ context.Context, _ string, attempt int) error {
		return fmt.Errorf("legacy fail %d", attempt)
	}}
	res2, err := runInmem(t, "wf-legacy-fail", failNodes, failReg,
		Config{MaxConcurrency: 1, DefaultRetry: RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	st := res2.State.Nodes["f"]
	t.Logf("旧用法失败节点: status=%s attempts=%d blocked=%v 判定依据: MaxAttempts=2 耗尽即 failed",
		st.Status, st.Attempts, st.BudgetBlocked)
	if res2.Succeeded || st.Status != StatusFailed || st.Attempts != 2 || st.BudgetBlocked {
		t.Fatalf("旧用法重试耗尽语义改变: %+v", st)
	}
}
