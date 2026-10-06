package workflow

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// 本组测试锁定运行结束后重试统计的口径：
//  1. 没配共享重试预算（旧用法）但确实发生了重试时，报告总数 Used 仍然正确，不再恒为 0；
//  2. Used 恒等于 PerNode 各节点重试次数之和；
//  3. “自身 MaxAttempts 耗尽”与“共享预算被别的节点先用完”两种不再重试的原因可区分、计数不混；
//  4. 中断后用保存的状态继续跑，总数与每节点计数接着累计，不重置也不重复；
//     运行被取消、尚未到终态时，报告只反映截至取消时已经发生的重试；
//     从头到尾没重试过的节点记为 0，不虚增。
//
// 所有用例都以 MaxConcurrency=1 串行执行，调度轨迹确定，可重复跑（-count=N）。

// flakyTask 前 failN 次返回失败、第 failN+1 次起成功；calls 累计实际发起尝试的次数，
// 并校验任务收到的 attempt 序号与实际调用次数一致（重复执行/计数错位会立即暴露）。
func flakyTask(node string, failN int, calls *int) TaskFunc {
	var mu sync.Mutex
	return func(_ context.Context, _ string, attempt int) error {
		mu.Lock()
		*calls++
		n := *calls
		mu.Unlock()
		if attempt != n {
			return fmt.Errorf("%s 尝试序号不连续: 传入attempt=%d 实际第%d次调用", node, attempt, n)
		}
		if n <= failN {
			return fmt.Errorf("%s fail #%d", node, n)
		}
		return nil
	}
}

// alwaysFailTask 永远失败；calls 累计尝试次数。
func alwaysFailTask(node string, calls *int) TaskFunc {
	var mu sync.Mutex
	return func(_ context.Context, _ string, attempt int) error {
		mu.Lock()
		*calls++
		mu.Unlock()
		return fmt.Errorf("%s fail on attempt %d", node, attempt)
	}
}

// ---- 用例 1：旧用法（无共享预算）下有重试时，Used 不为 0 且等于各节点之和 ----
//
// 业务覆盖：
//   - 不配置 Config.MaxRetries，节点仍按各自 MaxAttempts 失败再重试；
//   - 旧缺陷表现为 PerNode 非 0 而 Used 恒为 0，本用例稳定复现并锁定修复；
//   - 一次成功未重试的节点在 PerNode 中显式记 0，不允许虚增也不允许缺键。
func TestRetryReportLegacyTotalEqualsPerNodeSum(t *testing.T) {
	// 输入：三个互不依赖节点，串行执行；MaxRetries 不设（=0，旧用法）。
	//   ok      首次成功                 -> 重试 0 次
	//   flaky   前 2 次失败、第 3 次成功 -> 重试 2 次（3 次尝试）
	//   capped  永远失败、自身上限 4 次  -> 重试 3 次（4 次尝试），自身上限耗尽
	// 判定依据：Used 必须为 0+2+3=5；PerNode 与之一一对上；Budget=0 时账本 RetriesUsed=0。
	var callsOK, callsFlaky, callsCapped int
	reg := TaskRegistry{
		"ok":     flakyTask("ok", 0, &callsOK),
		"flaky":  flakyTask("flaky", 2, &callsFlaky),
		"capped": alwaysFailTask("capped", &callsCapped),
	}
	nodes := []NodeDef{{Name: "ok"}, {Name: "flaky"}, {Name: "capped"}}
	cfg := Config{
		MaxConcurrency: 1,
		DefaultRetry: RetryPolicy{
			MaxAttempts:    4,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     2 * time.Millisecond,
			Factor:         2,
		},
		// MaxRetries 刻意留空：旧用法，只有节点自身 MaxAttempts 生效。
	}

	t.Logf("输入: MaxRetries=未配置(0) MaxAttempts=%d 节点重试预期={ok:0,flaky:2,capped:3}",
		cfg.DefaultRetry.MaxAttempts)

	res, err := runInmem(t, "wf-report-legacy", nodes, reg, cfg)
	if err != nil {
		t.Fatal(err)
	}
	rep := res.State.RetryReport()
	t.Logf("结果: succeeded=%v 实际调用={ok:%d,flaky:%d,capped:%d} 报告 Budget=%d Used=%d PerNode=%v 自身上限耗尽=%v 预算阻断=%v RetriesUsed账本=%d",
		res.Succeeded, callsOK, callsFlaky, callsCapped,
		rep.Budget, rep.Used, rep.PerNode, rep.OwnLimitExhausted, rep.BudgetBlocked, res.State.RetriesUsed)

	// 实际尝试次数与预期一致（重试确实发生了）。
	if callsOK != 1 || callsFlaky != 3 || callsCapped != 4 {
		t.Fatalf("实际尝试次数错误: ok=%d flaky=%d capped=%d", callsOK, callsFlaky, callsCapped)
	}
	if rep.Budget != 0 {
		t.Fatalf("旧用法报告 Budget 应为 0，实际 %d", rep.Budget)
	}
	// 核心断言：旧用法下 Used 必须如实反映实际重试，而不是读账本得到 0。
	if rep.Used != 5 {
		t.Fatalf("旧用法下重试总数应等于实际重试 5 次，实际 Used=%d（PerNode=%v，二者自相矛盾即旧缺陷）",
			rep.Used, rep.PerNode)
	}
	wantPerNode := map[string]int{"ok": 0, "flaky": 2, "capped": 3}
	for name, want := range wantPerNode {
		got, present := rep.PerNode[name]
		if !present {
			t.Fatalf("PerNode 缺少节点 %q：未重试节点也必须显式记为 0", name)
		}
		if got != want {
			t.Fatalf("节点 %q 重试次数=%d，期望 %d", name, got, want)
		}
	}
	// 口径恒等式：Used == Σ PerNode（逐节点重算，不信任报告自己的求和）。
	sum := 0
	for _, v := range rep.PerNode {
		sum += v
	}
	if rep.Used != sum {
		t.Fatalf("总数与每节点之和对不上: Used=%d sum(PerNode)=%d", rep.Used, sum)
	}
	// capped 因自身上限耗尽终止；没有任何节点被共享预算阻断。
	if len(rep.BudgetBlocked) != 0 {
		t.Fatalf("未配置共享预算不应出现预算阻断节点，实际 %v", rep.BudgetBlocked)
	}
	if len(rep.OwnLimitExhausted) != 1 || rep.OwnLimitExhausted[0] != "capped" {
		t.Fatalf("capped 应归类为自身上限耗尽，实际 %v", rep.OwnLimitExhausted)
	}
	if st := res.State.Nodes["capped"]; !st.AttemptLimitExhausted || st.BudgetBlocked {
		t.Fatalf("capped 终止原因标记错误: AttemptLimitExhausted=%v BudgetBlocked=%v",
			st.AttemptLimitExhausted, st.BudgetBlocked)
	}
	if res.State.RetriesUsed != 0 {
		t.Fatalf("未启用共享预算时账本 RetriesUsed 必须保持 0，实际 %d", res.State.RetriesUsed)
	}
}

// ---- 用例 2：启用共享预算时，两种终止原因可区分、计数不混，总数仍等于每节点之和 ----
//
// 业务覆盖：
//   - MaxRetries=3、每节点 MaxAttempts=3（即每节点自身最多 2 次重试）；
//   - 串行调度下（定义顺序）三个永远失败节点的命运互不相同：
//     own   用尽自身 2 次重试（尝试 3 次）后因【自身上限耗尽】失败——它把 2 次预算用掉；
//     blockedA 只拿到 1 次重试（尝试 2 次），再申请时共享预算恰好被 own 用完 -> 【预算阻断】；
//     blockedB 连一次重试都拿不到（尝试 1 次），首次失败后申请即被拒 -> 【预算阻断】。
//   - 断言 OwnLimitExhausted=[own]、BudgetBlocked=[blockedA blockedB] 两个列表不相交；
//   - Used = own 重试 2 + blockedA 重试 1 + blockedB 重试 0 = 3，等于账本 RetriesUsed=3，
//     且等于 ΣPerNode。
func TestRetryReportDistinguishesStopReasons(t *testing.T) {
	var callsOwn, callsA, callsB int
	reg := TaskRegistry{
		"own":      alwaysFailTask("own", &callsOwn),
		"blockedA": alwaysFailTask("blockedA", &callsA),
		"blockedB": alwaysFailTask("blockedB", &callsB),
	}
	// 定义顺序即串行执行顺序，确保预算先被 own 消耗，结论确定。
	nodes := []NodeDef{{Name: "own"}, {Name: "blockedA"}, {Name: "blockedB"}}
	cfg := Config{
		MaxConcurrency: 1,
		DefaultRetry:   RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond},
		MaxRetries:     3,
	}
	t.Logf("输入: MaxRetries=3(共享) MaxAttempts=3(每节点至多2次重试) 串行顺序=%v；预期 own 自身耗尽、blockedA/B 预算阻断",
		[]string{"own", "blockedA", "blockedB"})

	res, err := runInmem(t, "wf-report-reasons", nodes, reg, cfg)
	if err != nil {
		t.Fatal(err)
	}
	rep := res.State.RetryReport()
	t.Logf("结果: succeeded=%v 实际尝试={own:%d,blockedA:%d,blockedB:%d} Used=%d/%d PerNode=%v 自身上限耗尽=%v 预算阻断=%v",
		res.Succeeded, callsOwn, callsA, callsB,
		rep.Used, rep.Budget, rep.PerNode, rep.OwnLimitExhausted, rep.BudgetBlocked)

	if res.Succeeded {
		t.Fatal("存在永久失败节点，整体不得成功")
	}
	if callsOwn != 3 || callsA != 2 || callsB != 1 {
		t.Fatalf("尝试次数错误: own=%d(期望3) blockedA=%d(期望2) blockedB=%d(期望1)",
			callsOwn, callsA, callsB)
	}
	// 每节点重试次数。
	wantPerNode := map[string]int{"own": 2, "blockedA": 1, "blockedB": 0}
	for name, want := range wantPerNode {
		if got := rep.PerNode[name]; got != want {
			t.Fatalf("PerNode[%s]=%d，期望 %d", name, got, want)
		}
	}
	// 总数：实际重试 3 次，恰好等于预算账本，但口径来自 ΣPerNode 而非直接读账本。
	if rep.Used != 3 {
		t.Fatalf("Used=%d，期望 3", rep.Used)
	}
	if rep.Used != res.State.RetriesUsed {
		t.Fatalf("正常跑完时实际重试用量应与预算账本一致: Used=%d 账本=%d",
			rep.Used, res.State.RetriesUsed)
	}
	sum := 0
	for _, v := range rep.PerNode {
		sum += v
	}
	if rep.Used != sum {
		t.Fatalf("Used(%d) != ΣPerNode(%d)", rep.Used, sum)
	}
	// 两种原因分别归类、互不混淆。
	if len(rep.OwnLimitExhausted) != 1 || rep.OwnLimitExhausted[0] != "own" {
		t.Fatalf("自身上限耗尽列表应为 [own]，实际 %v", rep.OwnLimitExhausted)
	}
	if len(rep.BudgetBlocked) != 2 ||
		rep.BudgetBlocked[0] != "blockedA" || rep.BudgetBlocked[1] != "blockedB" {
		t.Fatalf("预算阻断列表应为 [blockedA blockedB]，实际 %v", rep.BudgetBlocked)
	}
	own := res.State.Nodes["own"]
	if !own.AttemptLimitExhausted || own.BudgetBlocked {
		t.Fatalf("own 应只标 AttemptLimitExhausted: limit=%v blocked=%v",
			own.AttemptLimitExhausted, own.BudgetBlocked)
	}
	for _, name := range []string{"blockedA", "blockedB"} {
		st := res.State.Nodes[name]
		if !st.BudgetBlocked || st.AttemptLimitExhausted {
			t.Fatalf("%s 应只标 BudgetBlocked: blocked=%v limit=%v",
				name, st.BudgetBlocked, st.AttemptLimitExhausted)
		}
	}
}

// ---- 用例 3：中断重启后总数与每节点计数接着累计；取消时报告只含已发生的重试 ----
//
// 场景（旧用法，无共享预算；串行）：
//
//	节点 flaky 前 3 次失败、第 4 次成功，MaxAttempts=4。
//	退避序列：第 1 次失败后 2ms、第 2 次失败后 200ms、第 3 次失败后 200ms。
//	- 第一进程启动 ~30ms 后取消：此时 flaky 已完成第 1、2 次尝试，正卡在第 2 次失败后的
//	  长退避里；报告必须如实显示 flaky 已重试 1 次、Used=1（运行未到终态也要对）。
//	- 第二进程用同一状态文件、退避全部缩短：flaky 接续历史计数，只再发起第 3、4 次尝试，
//	  第 4 次成功；全程实际尝试恰好 4 次（没有因重启重复第 1、2 次），
//	  Used=3、PerNode[flaky]=3，成功后自身上限标记不得残留。
func TestRetryReportResumeKeepsCount(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s.json")
	nodes := []NodeDef{{Name: "flaky"}}

	cfgLong := Config{
		MaxConcurrency: 1,
		DefaultRetry: RetryPolicy{
			MaxAttempts:    4,
			InitialBackoff: 2 * time.Millisecond, // 第 1 次失败后短退避，尽快把第 2 次尝试跑起来
			MaxBackoff:     200 * time.Millisecond,
			Factor:         100, // 第 2 次失败后退避跳到 200ms，取消点落在其中
		},
	}
	cfgShort := Config{
		MaxConcurrency: 1,
		DefaultRetry:   RetryPolicy{MaxAttempts: 4, InitialBackoff: time.Millisecond},
	}

	var calls1, calls2 int
	// 按【传入的 attempt 序号】判成败：该序号在恢复时接续历史计数，
	// 因此前 3 次失败、第 4 次成功的规则天然跨进程生效；本地 calls 仅用于校验
	// 每个进程实际发起了哪几次尝试。
	mkReg := func(calls *int) TaskRegistry {
		var mu sync.Mutex
		return TaskRegistry{"flaky": func(_ context.Context, _ string, attempt int) error {
			mu.Lock()
			*calls++
			mu.Unlock()
			if attempt <= 3 {
				return fmt.Errorf("flaky fail on attempt %d", attempt)
			}
			return nil
		}}
	}
	reg1, reg2 := mkReg(&calls1), mkReg(&calls2)

	// ---- 第一进程：跑到第 2 次尝试完成、长退避中取消 ----
	e1, err := NewEngine("wf-report-resume", nodes, reg1, cfgLong, NewFileStore(dir))
	if err != nil {
		t.Fatal(err)
	}
	e1.WithLogger(t.Logf)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	res1, err := e1.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rep1 := res1.State.RetryReport()
	t.Logf("第一进程(取消时): finished=%v flaky尝试=%d 报告Used=%d PerNode=%v；判定依据: 取消发生在第2次失败后的退避中",
		res1.State.Finished, calls1, rep1.Used, rep1.PerNode)
	if res1.State.Finished {
		t.Fatal("取消时工作流不应已结算终态")
	}
	if calls1 != 2 {
		t.Fatalf("取消点应恰好完成 2 次尝试，实际 %d（调整退避/取消时序）", calls1)
	}
	// 取消后、未到终态的报告：已发生 1 次重试，不能是 0（清零）也不能是 3（预支）。
	if rep1.Used != 1 || rep1.PerNode["flaky"] != 1 {
		t.Fatalf("取消时报告应只含已发生的 1 次重试: Used=%d PerNode=%v", rep1.Used, rep1.PerNode)
	}
	if rep1.Used != sumPerNode(rep1) {
		t.Fatalf("取消时 Used(%d) 也必须等于每节点之和(%d)", rep1.Used, sumPerNode(rep1))
	}
	if st := res1.State.Nodes["flaky"]; st.Attempts != 2 || st.AttemptLimitExhausted || st.BudgetBlocked {
		t.Fatalf("取消时 flaky 状态错误: attempts=%d limit=%v blocked=%v",
			st.Attempts, st.AttemptLimitExhausted, st.BudgetBlocked)
	}

	// ---- 第二进程：同一状态文件恢复，只补跑第 3、4 次尝试 ----
	e2, err := NewEngine("wf-report-resume", nodes, reg2, cfgShort, NewFileStore(dir))
	if err != nil {
		t.Fatal(err)
	}
	e2.WithLogger(t.Logf)
	res2, err := e2.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rep2 := res2.State.RetryReport()
	t.Logf("第二进程(恢复): resumed=%v succeeded=%v 本进程尝试=%d 累计尝试=%d Used=%d PerNode=%v 自身上限耗尽=%v 预算阻断=%v",
		res2.Resumed, res2.Succeeded, calls2, calls1+calls2,
		rep2.Used, rep2.PerNode, rep2.OwnLimitExhausted, rep2.BudgetBlocked)

	if !res2.Resumed {
		t.Fatal("应识别为基于保存状态恢复")
	}
	if !res2.Succeeded {
		t.Fatal("第 4 次尝试应成功，整体成功")
	}
	// 恢复后不得重复第 1、2 次尝试，也不得漏掉第 3、4 次。
	if calls2 != 2 {
		t.Fatalf("第二进程应只补跑第 3、4 次尝试共 2 次，实际 %d", calls2)
	}
	if calls1+calls2 != 4 {
		t.Fatalf("跨进程累计尝试应为 4 次（不重置不重复），实际 %d", calls1+calls2)
	}
	if rep2.Used != 3 || rep2.PerNode["flaky"] != 3 {
		t.Fatalf("恢复后重试统计应接着累计到 3 次: Used=%d PerNode=%v", rep2.Used, rep2.PerNode)
	}
	if rep2.Used != sumPerNode(rep2) {
		t.Fatalf("恢复后 Used(%d) != ΣPerNode(%d)", rep2.Used, sumPerNode(rep2))
	}
	st := res2.State.Nodes["flaky"]
	if st.Attempts != 4 {
		t.Fatalf("flaky 持久化 Attempts 应为 4，实际 %d", st.Attempts)
	}
	if st.AttemptLimitExhausted || st.BudgetBlocked {
		t.Fatalf("成功节点不得残留任何终止原因标记: limit=%v blocked=%v",
			st.AttemptLimitExhausted, st.BudgetBlocked)
	}
	if res2.State.RetriesUsed != 0 || rep2.Budget != 0 {
		t.Fatalf("旧用法下账本应始终为 0: RetriesUsed=%d Budget=%d", res2.State.RetriesUsed, rep2.Budget)
	}
}

// sumPerNode 重算每节点重试次数之和，用于独立校验报告的 Used 口径（不信任报告自身求和）。
func sumPerNode(r RetryReport) int {
	sum := 0
	for _, v := range r.PerNode {
		sum += v
	}
	return sum
}

// ---- 用例 4：启用共享预算时中断重启，实际重试与预算账本跨进程接着累计、不重置不重复 ----
//
// 串行三节点，MaxRetries=2（全局只够 2 次重试）、MaxAttempts=5（自身上限用不完）：
//
//	a 永不失败（首次成功）          -> 重试 0，边界节点，必须显式记 0；
//	b 依赖 a，前 2 次失败第 3 次成功 -> 需要 2 次重试，恰好用光共享预算，跨两个进程发生；
//	c 依赖 b，首次失败后申请重试即被拒 -> 0 次重试，BudgetBlocked。
//
// 中断点：b 第 1 次失败后进入退避，在退避中取消。
// 注意时序语义：重试预算在重试【发起前】预留，因此取消发生在退避中时，
// b 的第 2 次尝试尚未预留预算——第一进程 Used=0、账本 RetriesUsed=0。
func TestRetryReportResumeWithSharedBudget(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s.json")
	nodes := []NodeDef{
		{Name: "a"},
		{Name: "b", DependsOn: []string{"a"}},
		{Name: "c", DependsOn: []string{"b"}},
	}
	mkCfg := func(backoff time.Duration) Config {
		return Config{
			MaxConcurrency: 1,
			DefaultRetry:   RetryPolicy{MaxAttempts: 5, InitialBackoff: backoff, MaxBackoff: backoff},
			MaxRetries:     2,
		}
	}

	var bCalls1 int
	reg1 := TaskRegistry{
		"a": func(context.Context, string, int) error { return nil },
		"b": func(_ context.Context, _ string, attempt int) error {
			bCalls1++
			return fmt.Errorf("b fail #%d", attempt) // 第一进程只应见到第 1 次尝试
		},
		"c": func(context.Context, string, int) error { t.Fatal("c 在第一进程不应执行"); return nil },
	}
	e1, err := NewEngine("wf-report-resume-budget", nodes, reg1, mkCfg(time.Hour), NewFileStore(dir))
	if err != nil {
		t.Fatal(err)
	}
	e1.WithLogger(t.Logf)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond) // b 第 1 次失败后立刻进入一小时退避，20ms 足够
		cancel()
	}()
	res1, err := e1.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rep1 := res1.State.RetryReport()
	t.Logf("第一进程(取消时): a成功 b尝试=%d Used=%d 账本=%d/%d PerNode=%v；判定依据: 退避中取消，第2次尝试尚未预留预算",
		bCalls1, rep1.Used, res1.State.RetriesUsed, res1.State.RetryBudget, rep1.PerNode)
	if res1.State.Finished {
		t.Fatal("取消时不应结算终态")
	}
	if bCalls1 != 1 {
		t.Fatalf("第一进程 b 应只发起首次尝试，实际 %d", bCalls1)
	}
	if rep1.Used != 0 || res1.State.RetriesUsed != 0 || res1.State.RetryBudget != 2 {
		t.Fatalf("取消时不应有已预留/已发生的重试: Used=%d 账本=%d budget=%d",
			rep1.Used, res1.State.RetriesUsed, res1.State.RetryBudget)
	}

	// 第二进程：短退避恢复。b 第 2、3 次尝试依次预留 2 次共享预算，第 3 次成功；
	// c 首次失败后申请重试时预算已尽 -> BudgetBlocked；a 不重跑。
	var aCalls2, bCalls2, cCalls2 int
	bSeen := []int{}
	var mu sync.Mutex
	reg2 := TaskRegistry{
		"a": func(context.Context, string, int) error { aCalls2++; return nil },
		"b": func(_ context.Context, _ string, attempt int) error {
			mu.Lock()
			bSeen = append(bSeen, attempt)
			bCalls2++
			mu.Unlock()
			if attempt == 3 {
				return nil
			}
			return fmt.Errorf("b fail #%d", attempt)
		},
		"c": func(_ context.Context, _ string, attempt int) error {
			cCalls2++
			return fmt.Errorf("c fail attempt %d", attempt)
		},
	}
	e2, err := NewEngine("wf-report-resume-budget", nodes, reg2, mkCfg(time.Millisecond), NewFileStore(dir))
	if err != nil {
		t.Fatal(err)
	}
	e2.WithLogger(t.Logf)
	res2, err := e2.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rep2 := res2.State.RetryReport()
	t.Logf("第二进程(恢复): resumed=%v succeeded=%v b本进程尝试序号=%v a重跑=%d c尝试=%d Used=%d 账本=%d/%d PerNode=%v 自身上限耗尽=%v 预算阻断=%v",
		res2.Resumed, res2.Succeeded, bSeen, aCalls2, cCalls2,
		rep2.Used, res2.State.RetriesUsed, rep2.Budget, rep2.PerNode,
		rep2.OwnLimitExhausted, rep2.BudgetBlocked)

	if !res2.Resumed || res2.Succeeded {
		t.Fatalf("应识别恢复且因 c 失败整体失败: resumed=%v succeeded=%v", res2.Resumed, res2.Succeeded)
	}
	if aCalls2 != 0 {
		t.Fatalf("已成功的 a 不得重跑，实际 %d 次", aCalls2)
	}
	if len(bSeen) != 2 || bSeen[0] != 2 || bSeen[1] != 3 {
		t.Fatalf("b 应接续历史计数只补跑第 2、3 次，实际 %v", bSeen)
	}
	if cCalls2 != 1 {
		t.Fatalf("c 被预算阻断，只应发起首次尝试，实际 %d", cCalls2)
	}
	// b 跨进程共 3 次尝试（2 次重试）；a 与 c 各 1 次尝试（0 次重试）。
	want := map[string]int{"a": 0, "b": 2, "c": 0}
	for name, w := range want {
		if got := rep2.PerNode[name]; got != w {
			t.Fatalf("PerNode[%s]=%d，期望 %d（%v）", name, got, w, rep2.PerNode)
		}
	}
	// 实际重试总数 = 2，等于账本（正常跑完无崩溃窗口），等于 ΣPerNode。
	if rep2.Used != 2 || res2.State.RetriesUsed != 2 || rep2.Budget != 2 {
		t.Fatalf("预算/实际重试用量跨进程累计错误: Used=%d 账本=%d budget=%d",
			rep2.Used, res2.State.RetriesUsed, rep2.Budget)
	}
	if rep2.Used != sumPerNode(rep2) {
		t.Fatalf("Used(%d) != ΣPerNode(%d)", rep2.Used, sumPerNode(rep2))
	}
	// 终止原因：c 是预算被 b 先用完；没有节点是自身上限耗尽（MaxAttempts=5 都没用满）。
	if len(rep2.BudgetBlocked) != 1 || rep2.BudgetBlocked[0] != "c" {
		t.Fatalf("预算阻断列表应为 [c]，实际 %v", rep2.BudgetBlocked)
	}
	if len(rep2.OwnLimitExhausted) != 0 {
		t.Fatalf("不应有自身上限耗尽节点，实际 %v", rep2.OwnLimitExhausted)
	}
	if st := res2.State.Nodes["b"]; st.Status != StatusSucceeded ||
		st.BudgetBlocked || st.AttemptLimitExhausted || st.Attempts != 3 {
		t.Fatalf("b 最终状态错误: %+v", st)
	}
	if st := res2.State.Nodes["c"]; st.Status != StatusFailed || !st.BudgetBlocked || st.Attempts != 1 {
		t.Fatalf("c 应首次失败即被预算阻断: %+v", st)
	}
}
