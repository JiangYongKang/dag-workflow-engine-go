package workflow

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// alwaysFailRegistry 构造若干“永远失败”的节点并统计各节点的执行次数。
func alwaysFailRegistry(names ...string) (TaskRegistry, map[string]*int32) {
	reg := TaskRegistry{}
	counts := map[string]*int32{}
	for _, n := range names {
		n := n
		c := new(int32)
		counts[n] = c
		reg[n] = func(_ context.Context, node string, attempt int) error {
			atomic.AddInt32(c, 1)
			return fmt.Errorf("%s boom on attempt %d", node, attempt)
		}
	}
	return reg, counts
}

// ---- 全局共享重试预算：先到先得，耗尽后受影响节点按重试耗尽处理，统计可解释 ----

func TestSharedRetryBudgetExhausted(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s.json")
	nodes := []NodeDef{{Name: "x"}, {Name: "y"}}
	reg, counts := alwaysFailRegistry("x", "y")
	cfg := Config{
		MaxConcurrency: 1, // 串行：x 先扣预算，y 随后立即无额度可用，顺序确定
		DefaultRetry: RetryPolicy{
			MaxAttempts:    5, // 单节点自身允许 5 次，远大于全局预算
			InitialBackoff: time.Millisecond,
			Factor:         1,
		},
		MaxRetryBudget: 2,
	}

	e, err := NewEngine("wf-budget", nodes, reg, cfg, NewFileStore(dir))
	if err != nil {
		t.Fatal(err)
	}
	e.WithLogger(t.Logf)
	res, err := e.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("输入: MaxRetryBudget=2, MaxAttempts=5, x/y 永远失败, 并发=1")
	t.Logf("结果: succeeded=%v 总重试=%v x(attempts=%d retries=%d reason=%q) y(attempts=%d retries=%d reason=%q)",
		res.Succeeded, res.State.RetriesUsed,
		res.State.Nodes["x"].Attempts, res.State.Nodes["x"].Retries, res.State.Nodes["x"].FailReason,
		res.State.Nodes["y"].Attempts, res.State.Nodes["y"].Retries, res.State.Nodes["y"].FailReason)

	if res.Succeeded {
		t.Fatal("预算耗尽导致节点永久失败，整体不得判定为成功")
	}
	if got := res.State.RetriesUsed; got != 2 {
		t.Fatalf("全局共享重试总数应为 2，实际 %d", got)
	}
	if got := atomic.LoadInt32(counts["x"]); got != 3 {
		t.Fatalf("x 应执行 3 次（首次 + 用掉 2 次共享重试），实际 %d", got)
	}
	if rx := res.State.Nodes["x"]; rx.Attempts != 3 || rx.Retries != 2 || rx.FailReason != FailReasonBudgetExhausted {
		t.Fatalf("x 状态异常: attempts=%d retries=%d reason=%q", rx.Attempts, rx.Retries, rx.FailReason)
	}
	if got := atomic.LoadInt32(counts["y"]); got != 1 {
		t.Fatalf("y 预算已被 x 扣完，应只执行 1 次，实际 %d", got)
	}
	if ry := res.State.Nodes["y"]; ry.Attempts != 1 || ry.Retries != 0 || ry.FailReason != FailReasonBudgetExhausted {
		t.Fatalf("y 应因全局预算耗尽而不再重试: attempts=%d retries=%d reason=%q", ry.Attempts, ry.Retries, ry.FailReason)
	}

	// 重启恢复：已花掉的预算不回滚，两个节点都不得再执行，整体仍失败。
	e2, _ := NewEngine("wf-budget", nodes, reg, cfg, NewFileStore(dir))
	e2.WithLogger(t.Logf)
	res2, err := e2.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("恢复后: resumed=%v succeeded=%v 总重试=%d x执行=%d y执行=%d",
		res2.Resumed, res2.Succeeded, res2.State.RetriesUsed,
		atomic.LoadInt32(counts["x"]), atomic.LoadInt32(counts["y"]))
	if !res2.Resumed {
		t.Fatal("应识别为恢复执行")
	}
	if res2.Succeeded {
		t.Fatal("预算耗尽是永久失败，重启后整体仍应失败")
	}
	if res2.State.RetriesUsed != 2 {
		t.Fatalf("已消耗预算重启后不得回滚，实际 %d", res2.State.RetriesUsed)
	}
	if got := atomic.LoadInt32(counts["x"]) + atomic.LoadInt32(counts["y"]); got != 4 {
		t.Fatalf("耗尽节点重启后不得再执行，总执行次数=%d，期望 4", got)
	}
	for _, n := range []string{"x", "y"} {
		if got := res2.State.Nodes[n].FailReason; got != FailReasonBudgetExhausted {
			t.Fatalf("节点 %s 重启后失败原因丢失: %q", n, got)
		}
	}
}

// ---- 并发槽位紧张时按优先级挑选，同优先级次序稳定可复现 ----

func TestPrioritySchedulingUnderConcurrencyLimit(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s.json")
	// 故意乱序定义：low 在最前，避免“恰好按定义顺序通过”。
	nodes := []NodeDef{
		{Name: "low", Priority: 0},
		{Name: "critical", Priority: 10},
		{Name: "mid", Priority: 5},
		{Name: "other", Priority: 0}, // 与 low 同优先级，应按下标升序：low 在 other 前
	}

	active := make(chan string, 8)
	gate := make(chan struct{})
	reg := TaskRegistry{}
	for _, n := range nodes {
		n := n
		reg[n.Name] = func(_ context.Context, node string, attempt int) error {
			select {
			case active <- node:
			default:
			}
			<-gate
			return nil
		}
	}

	e, err := NewEngine("wf-prio", nodes, reg,
		Config{MaxConcurrency: 2, DefaultRetry: RetryPolicy{MaxAttempts: 1}},
		NewFileStore(dir))
	if err != nil {
		t.Fatal(err)
	}
	e.WithLogger(t.Logf)

	done := make(chan struct{})
	var res *RunResult
	go func() {
		res, _ = e.Run(context.Background())
		close(done)
	}()

	// 槽位只有 2 个：第一批必须恰好是优先级最高的 critical 与 mid（goroutine
	// 真正开始执行的先后由 Go 运行时决定，存在竞争，因此这里只比对“集合”；
	// 调度器的决定次序以下面持久化的 ScheduleOrder 为准）。
	first := drainActive(active, 2)
	t.Logf("输入: 并发槽位=2，节点优先级 critical=10 mid=5 low/other=0；第一批在途节点集合: %v", first)
	if len(first) != 2 {
		t.Fatalf("第一批在途节点数=%d，期望 2", len(first))
	}
	firstSet := map[string]bool{}
	for _, s := range first {
		firstSet[s] = true
	}
	if !firstSet["critical"] || !firstSet["mid"] {
		t.Fatalf("第一批节点错误: %v，期望恰好为 {critical, mid}", first)
	}
	select {
	case extra := <-active:
		t.Fatalf("槽位未释放前不应有第三个节点开始执行，实际开始: %s", extra)
	default:
	}
	close(gate)
	<-done
	if res == nil || !res.Succeeded {
		t.Fatalf("工作流应成功: res=%+v", res)
	}
	// 持久化的调度次序：critical mid 在前；low/other 同优先级按下标升序。
	want := []string{"critical", "mid", "low", "other"}
	t.Logf("持久化调度次序: %v 判定依据: 优先级降序，同优先级按定义下标升序", res.State.ScheduleOrder)
	if got := res.State.ScheduleOrder; len(got) != 4 ||
		got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("调度次序错误: got=%v want=%v", got, want)
	}
}

// drainActive 等待 n 个节点进入执行（次序即真实被放行的次序）。
func drainActive(ch <-chan string, n int) []string {
	out := make([]string, 0, n)
	deadline := time.After(2 * time.Second)
	for len(out) < n {
		select {
		case s := <-ch:
			out = append(out, s)
		case <-deadline:
			return out
		}
	}
	return out
}

// ---- 中断重启后：已花预算不回滚、调度记录不丢失、终局判断一致 ----

func TestRestartKeepsBudgetAndScheduleRecord(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s.json")
	nodes := []NodeDef{
		{Name: "a", Priority: 0},
		{Name: "b", Priority: 0, DependsOn: []string{"a"}},
		{Name: "c", Priority: 0, DependsOn: []string{"b"}},
	}
	cfg := Config{
		MaxConcurrency: 1,
		DefaultRetry:   RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Hour}, // 长退避：取消点稳定
		MaxRetryBudget: 1,
	}

	// 第一次进程：a 成功；b 第 1 次失败后申请到唯一的 1 次共享重试，长退避期间取消（模拟重启）。
	var aCalls1, bCalls1, cCalls1 int32
	reg1 := TaskRegistry{
		"a": func(context.Context, string, int) error { atomic.AddInt32(&aCalls1, 1); return nil },
		"b": func(context.Context, string, int) error {
			atomic.AddInt32(&bCalls1, 1)
			return fmt.Errorf("first failure")
		},
		"c": func(context.Context, string, int) error { atomic.AddInt32(&cCalls1, 1); return nil },
	}
	e1, _ := NewEngine("wf-restart-fb", nodes, reg1, cfg, NewFileStore(dir))
	e1.WithLogger(t.Logf)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond) // 落在 b 失败后的长退避窗口内
		cancel()
	}()
	res1, err := e1.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("第一次进程: succeeded=%v b状态=%s b尝试=%d 已花预算=%d 调度记录=%v",
		res1.Succeeded, res1.State.Nodes["b"].Status, res1.State.Nodes["b"].Attempts,
		res1.State.RetriesUsed, res1.State.ScheduleOrder)
	if atomic.LoadInt32(&bCalls1) != 1 {
		t.Fatalf("第一次进程中 b 应只执行 1 次，实际 %d", bCalls1)
	}
	if res1.State.RetriesUsed != 1 {
		t.Fatalf("被取消前应已扣掉 1 次共享预算，实际 %d", res1.State.RetriesUsed)
	}
	if res1.State.Nodes["b"].Retries != 1 {
		t.Fatalf("b 应已登记 1 次重试，实际 %d", res1.State.Nodes["b"].Retries)
	}
	wantOrder1 := []string{"a", "b"}
	if got := res1.State.ScheduleOrder; len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("第一次进程调度记录错误: %v，期望 %v", got, wantOrder1)
	}

	// 第二次进程：同一状态文件。已扣的预算不回滚：b 的第 2 次尝试仍会发生（额度在取消
	// 前已授予），再失败后全局预算已耗尽，b 永久失败；c 连带 skipped；整体失败。
	var aCalls2, bCalls2, cCalls2 int32
	reg2 := TaskRegistry{
		"a": func(context.Context, string, int) error { atomic.AddInt32(&aCalls2, 1); return nil },
		"b": func(context.Context, string, int) error {
			atomic.AddInt32(&bCalls2, 1)
			return fmt.Errorf("still failing")
		},
		"c": func(context.Context, string, int) error { atomic.AddInt32(&cCalls2, 1); return nil },
	}
	e2, _ := NewEngine("wf-restart-fb", nodes, reg2, cfg, NewFileStore(dir))
	e2.WithLogger(t.Logf)
	res2, err := e2.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("第二次进程: resumed=%v succeeded=%v b(attempts=%d retries=%d reason=%q) c=%s 已花预算=%d 调度记录=%v",
		res2.Resumed, res2.Succeeded,
		res2.State.Nodes["b"].Attempts, res2.State.Nodes["b"].Retries, res2.State.Nodes["b"].FailReason,
		res2.State.Nodes["c"].Status, res2.State.RetriesUsed, res2.State.ScheduleOrder)

	if !res2.Resumed {
		t.Fatal("应识别为恢复执行")
	}
	if res2.Succeeded {
		t.Fatal("b 预算耗尽永久失败时整体应失败，且与第一次进程的失败判断方向一致")
	}
	if atomic.LoadInt32(&aCalls2) != 0 {
		t.Fatal("已成功的 a 重启后不得重跑")
	}
	if atomic.LoadInt32(&bCalls2) != 1 {
		t.Fatalf("b 应接续计数只再执行 1 次（第 2 次尝试），实际 %d", bCalls2)
	}
	if b := res2.State.Nodes["b"]; b.Attempts != 2 || b.Retries != 1 || b.FailReason != FailReasonBudgetExhausted {
		t.Fatalf("b 最终状态错误: attempts=%d retries=%d reason=%q", b.Attempts, b.Retries, b.FailReason)
	}
	if atomic.LoadInt32(&cCalls2) != 0 || res2.State.Nodes["c"].Status != StatusSkipped {
		t.Fatalf("c 应被连带 skipped 且从未执行: calls=%d status=%s", cCalls2, res2.State.Nodes["c"].Status)
	}
	if res2.State.RetriesUsed != 1 {
		t.Fatalf("已花预算重启后必须保持为 1，实际 %d", res2.State.RetriesUsed)
	}
	// 调度记录跨重启追加、不重复：a/b 不出现第二次，c 从未被调度（skipped 不占位）。
	if got, want := res2.State.ScheduleOrder, []string{"a", "b"}; len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("重启后调度记录必须与重启前一致且不重复: got=%v want=%v", got, want)
	}

	// 第三次进程：终局必须稳定——不再执行任何节点，预算/失败原因/调度记录/整体判断全部一致。
	var calls3 int32
	reg3 := TaskRegistry{
		"a": func(context.Context, string, int) error { atomic.AddInt32(&calls3, 1); return nil },
		"b": func(context.Context, string, int) error { atomic.AddInt32(&calls3, 1); return nil },
		"c": func(context.Context, string, int) error { atomic.AddInt32(&calls3, 1); return nil },
	}
	e3, _ := NewEngine("wf-restart-fb", nodes, reg3, cfg, NewFileStore(dir))
	e3.WithLogger(t.Logf)
	res3, err := e3.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("第三次进程: resumed=%v succeeded=%v 执行任务数=%d 已花预算=%d 调度记录=%v",
		res3.Resumed, res3.Succeeded, calls3, res3.State.RetriesUsed, res3.State.ScheduleOrder)
	if atomic.LoadInt32(&calls3) != 0 {
		t.Fatalf("终局后重启不得执行任何节点，实际执行 %d 次", calls3)
	}
	if res3.Succeeded != res2.Succeeded || res3.State.RetriesUsed != res2.State.RetriesUsed {
		t.Fatalf("重启前后终局判断不一致: r2(succeeded=%v budget=%d) r3(succeeded=%v budget=%d)",
			res2.Succeeded, res2.State.RetriesUsed, res3.Succeeded, res3.State.RetriesUsed)
	}
	if len(res3.State.ScheduleOrder) != len(res2.State.ScheduleOrder) {
		t.Fatalf("终局后调度记录不应继续增长: %v vs %v", res3.State.ScheduleOrder, res2.State.ScheduleOrder)
	}
}

// ---- 兼容：既不设共享预算也不设优先级的旧用法，行为与结果保持不变 ----

func TestLegacyConfigUnchanged(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s.json")
	// 不设置 Priority（零值）、不设置 MaxRetryBudget（零值）：旧用法。
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
	// 旧配置形态：只有 MaxConcurrency 与 DefaultRetry，新增字段一律零值。
	cfg := Config{MaxConcurrency: 4, DefaultRetry: RetryPolicy{MaxAttempts: 3}}
	e, err := NewEngine("wf-legacy", nodes, reg, cfg, NewFileStore(dir))
	if err != nil {
		t.Fatal(err)
	}
	e.WithLogger(t.Logf)
	res, err := e.Run(context.Background())
	if err != nil || !res.Succeeded {
		t.Fatalf("旧用法应整体成功: err=%v res=%+v", err, res)
	}
	t.Logf("旧用法结果: succeeded=%v 实际执行顺序=%v 调度记录=%v 总重试=%d",
		res.Succeeded, order, res.State.ScheduleOrder, res.State.RetriesUsed)
	// 同优先级（全部 0）时仍严格按定义顺序挑选的就绪次序：a 先于 b 先于 c。
	if got := res.State.ScheduleOrder; len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("旧用法调度顺序改变: got=%v want=[a b c]", got)
	}
	if res.State.RetriesUsed != 0 {
		t.Fatalf("全部一次成功时不应有重试计数，实际 %d", res.State.RetriesUsed)
	}
	for _, n := range nodes {
		if st := res.State.Nodes[n.Name]; st.FailReason != "" || st.Retries != 0 || st.Attempts != 1 {
			t.Fatalf("节点 %s 旧用法状态异常: %+v", n.Name, st)
		}
	}
}

// ---- 兼容：旧版本状态文件（无 Retries/FailReason/ScheduleOrder/RetriesUsed）仍可恢复 ----

func TestLegacyStateFileCompat(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s.json")
	// 手工写一份旧版本状态：b 失败且 Attempts=1 < MaxAttempts=2，应回到 pending 再试。
	old := &WorkflowState{
		WorkflowName: "wf-oldstate",
		Nodes: map[string]*NodeState{
			"b": {Status: StatusFailed, Attempts: 1, LastError: "old failure"},
		},
	}
	if err := NewFileStore(dir).Save(old); err != nil {
		t.Fatal(err)
	}
	nodes := []NodeDef{{Name: "b"}}
	cfg := Config{MaxConcurrency: 1, DefaultRetry: RetryPolicy{MaxAttempts: 2}}
	var attempts []int
	var mu sync.Mutex
	reg := TaskRegistry{
		"b": func(_ context.Context, _ string, attempt int) error {
			mu.Lock()
			attempts = append(attempts, attempt)
			mu.Unlock()
			return nil
		},
	}
	e, _ := NewEngine("wf-oldstate", nodes, reg, cfg, NewFileStore(dir))
	e.WithLogger(t.Logf)
	res, err := e.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("旧状态文件恢复: resumed=%v succeeded=%v 尝试序号=%v 调度记录=%v",
		res.Resumed, res.Succeeded, attempts, res.State.ScheduleOrder)
	if !res.Resumed || !res.Succeeded {
		t.Fatalf("旧状态文件应可恢复并最终成功: resumed=%v succeeded=%v", res.Resumed, res.Succeeded)
	}
	if len(attempts) != 1 || attempts[0] != 2 {
		t.Fatalf("应接续第 2 次尝试，实际 %v", attempts)
	}
	if got := res.State.ScheduleOrder; len(got) != 1 || got[0] != "b" {
		t.Fatalf("旧状态应回填调度记录 [b]，实际 %v", got)
	}
}

// ---- 高并发争抢共享预算：账目守恒（总重试恰好等于预算，先到先得不超额）----

func TestSharedBudgetFirstComeFirstServed(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s.json")
	const N = 12
	var nodes []NodeDef
	for i := 0; i < N; i++ {
		nodes = append(nodes, NodeDef{Name: fmt.Sprintf("n%02d", i)})
	}
	reg, _ := alwaysFailRegistry(nodeNames(nodes)...)
	cfg := Config{
		MaxConcurrency: 6,
		DefaultRetry:   RetryPolicy{MaxAttempts: 100, Factor: 1, InitialBackoff: time.Millisecond},
		MaxRetryBudget: 5,
	}
	e, _ := NewEngine("wf-budget-race", nodes, reg, cfg, NewFileStore(dir))
	e.WithLogger(t.Logf)
	res, err := e.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// 账目守恒一：全局总重试恰好等于预算。
	if res.State.RetriesUsed != 5 {
		t.Fatalf("总重设=%d，期望恰好 5", res.State.RetriesUsed)
	}
	// 账目守恒二：各节点 Retries 之和 == 全局计数；每个节点 attempts==retries+1。
	sumRetries := 0
	budgetStopped := 0
	for _, n := range nodes {
		st := res.State.Nodes[n.Name]
		sumRetries += st.Retries
		if st.Attempts != st.Retries+1 {
			t.Fatalf("节点 %s 账目不平: attempts=%d retries=%d", n.Name, st.Attempts, st.Retries)
		}
		if st.FailReason == FailReasonBudgetExhausted {
			budgetStopped++
		}
	}
	t.Logf("总重试=%d 节点Retries之和=%d 因预算耗尽停止的节点数=%d（预算=5 远小于总需求，所有节点都应被预算拦下）",
		res.State.RetriesUsed, sumRetries, budgetStopped)
	if sumRetries != 5 {
		t.Fatalf("各节点重试之和=%d，期望 5", sumRetries)
	}
	if budgetStopped != N {
		t.Fatalf("预算远小于总需求时所有节点都应因预算耗尽停止，实际 %d/%d", budgetStopped, N)
	}
	if res.Succeeded {
		t.Fatal("所有节点永远失败，整体应失败")
	}
}

// ---- 重启复现：同一定义 + 同一份状态，恢复后的调度次序与全新跑的确定性次序一致 ----

func TestPriorityOrderReproducibleAcrossRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s.json")
	nodes := []NodeDef{
		{Name: "p0a", Priority: 0},
		{Name: "p10", Priority: 10},
		{Name: "p5a", Priority: 5},
		{Name: "p5b", Priority: 5}, // 与 p5a 同优先级，按下标升序 p5a 在 p5b 前
		{Name: "p0b", Priority: 0},
	}
	// 第一次进程：所有节点进入后阻塞，只放行前 2 个（高优先级），随后取消进程。
	active := make(chan string, 8)
	gate := make(chan struct{})
	started := make(map[string]bool)
	var mu sync.Mutex
	reg := TaskRegistry{}
	for _, n := range nodes {
		n := n
		reg[n.Name] = func(context.Context, string, int) error {
			mu.Lock()
			started[n.Name] = true
			mu.Unlock()
			select {
			case active <- n.Name:
			default:
			}
			<-gate
			return nil
		}
	}
	cfg := Config{MaxConcurrency: 2, DefaultRetry: RetryPolicy{MaxAttempts: 2}}
	e1, _ := NewEngine("wf-prio-restart", nodes, reg, cfg, NewFileStore(dir))
	e1.WithLogger(t.Logf)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		drainActive(active, 2) // 等 p10 与 p5a 占满槽位
		cancel()               // 它们在途被取消，其余节点从未被选中：模拟进程中断
		close(gate)            // 放行在途任务（任务体本身只监听 gate），让 Run 得以收尾
	}()
	res1, err := e1.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("中断时调度记录=%v", res1.State.ScheduleOrder)
	if len(res1.State.ScheduleOrder) != 2 || res1.State.ScheduleOrder[0] != "p10" || res1.State.ScheduleOrder[1] != "p5a" {
		t.Fatalf("中断前调度次序错误: %v，期望 [p10 p5a]", res1.State.ScheduleOrder)
	}

	// 第二次进程：在途被取消的两个节点回到 pending 再跑并成功，但不重复登记调度；
	// 其余节点按同一确定性次序追加：p5b(5)、p0a(0,idx0)、p0b(0,idx4)。
	reg2 := TaskRegistry{}
	for _, nd := range nodes {
		nd := nd
		reg2[nd.Name] = func(context.Context, string, int) error { return nil }
	}
	_ = started
	e2, _ := NewEngine("wf-prio-restart", nodes, reg2, cfg, NewFileStore(dir))
	e2.WithLogger(t.Logf)
	res2, err := e2.Run(context.Background())
	if err != nil || !res2.Succeeded {
		t.Fatalf("恢复后应整体成功: err=%v succeeded=%v", err, res2.Succeeded)
	}
	want := []string{"p10", "p5a", "p5b", "p0a", "p0b"}
	t.Logf("恢复后完整调度记录=%v 判定依据: 中断前的 %v 不重复，其余按同一次序追加",
		res2.State.ScheduleOrder, want)
	if got := res2.State.ScheduleOrder; len(got) != len(want) {
		t.Fatalf("调度记录长度错误: got=%v want=%v", got, want)
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("调度次序不可复现: got=%v want=%v", got, want)
			}
		}
	}
}

// nodeNames 提取节点名列表。
func nodeNames(nodes []NodeDef) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.Name
	}
	return out
}
