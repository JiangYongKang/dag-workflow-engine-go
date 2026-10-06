package workflow

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

// 本文件锁定“重试统计口径”：
//  1. 不配置共享重试预算（旧用法）时，报告总数 Used 仍如实统计，并等于各节点重试次数之和；
//  2. “节点自身 MaxAttempts 耗尽”与“共享预算被别的节点先用完”两种不再重试的原因可区分、计数不混；
//  3. 中断（取消 / running 残留）后用保存的状态继续跑，总数与每节点计数接着累计，不重置也不重复；
//  4. 未到终态被取消时报告反映截至目前已发起的重试；一次都没重试过的节点显式记 0。
//
// 一次“重试”的口径：某节点首次尝试之后、实际发起的每一次尝试，即 max(Attempts-1, 0)；
// 仅退避等待、第 k 次尝试尚未发起时不计。

// sumPerNode 汇总报告中每节点重试次数，用于和报告总数逐一对账。
func sumPerNode(t *testing.T, nodes []NodeDef, rep RetryReport) int {
	t.Helper()
	total := 0
	for _, n := range nodes {
		v, ok := rep.PerNode[n.Name]
		if !ok {
			t.Fatalf("节点 %q 未在 PerNode 报告中显式列出（0 重试也必须记 0）", n.Name)
		}
		total += v
	}
	return total
}

// ---- 场景 1：旧用法（无共享预算）有重试时，总数正确且等于各节点之和 ----

func TestRetryTotalWithoutSharedBudget(t *testing.T) {
	// 输入定义：
	//   a 前 2 次失败、第 3 次成功         -> Attempts=3，重试 2 次
	//   b 永远失败（自身 MaxAttempts=3）   -> Attempts=3，重试 2 次，自身上限耗尽
	//   c 依赖 a，首次即成功               -> Attempts=1，重试 0 次（必须显式记 0）
	//   d 依赖 b，被失败传播跳过           -> Attempts=0，重试 0 次（未执行也记 0）
	// Config 不设 MaxRetries（零值，旧用法），期望总数 = 2+2+0+0 = 4。
	nodes := []NodeDef{
		{Name: "a"},
		{Name: "b", DependsOn: []string{"a"}},
		{Name: "c", DependsOn: []string{"a"}},
		{Name: "d", DependsOn: []string{"b"}},
	}
	var bCalls int32
	reg := TaskRegistry{
		"a": func(_ context.Context, _ string, attempt int) error {
			if attempt < 3 {
				return fmt.Errorf("a fail on attempt %d", attempt)
			}
			return nil
		},
		"b": func(_ context.Context, _ string, attempt int) error {
			atomic.AddInt32(&bCalls, 1)
			return fmt.Errorf("b fail on attempt %d", attempt)
		},
		"c": func(context.Context, string, int) error { return nil },
		"d": func(context.Context, string, int) error { t.Fatal("d 应被跳过，不得执行"); return nil },
	}
	cfg := Config{
		MaxConcurrency: 1, // 串行，调度轨迹确定
		DefaultRetry: RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     2 * time.Millisecond,
			Factor:         2,
		},
	}
	t.Logf("输入: MaxRetries 未配置(=0) MaxAttempts=3 节点重试预期 a=2 b=2 c=0 d=0")
	res, err := runInmem(t, "wf-report-nobudget", nodes, reg, cfg)
	if err != nil {
		t.Fatal(err)
	}
	rep := res.State.RetryReport()
	t.Logf("结果: succeeded=%v 各节点Attempts=a:%d b:%d c:%d d:%d 报告 Used=%d PerNode=%v BudgetBlocked=%v",
		res.Succeeded,
		res.State.Nodes["a"].Attempts, res.State.Nodes["b"].Attempts,
		res.State.Nodes["c"].Attempts, res.State.Nodes["d"].Attempts,
		rep.Used, rep.PerNode, rep.BudgetBlocked)

	if rep.Budget != 0 {
		t.Fatalf("旧用法 Budget 应为 0（不限制），实际 %d", rep.Budget)
	}
	// 这是本次 bug 的直接复现点：旧用法下 Used 过去恒为 0。
	if rep.Used != 4 {
		t.Fatalf("旧用法下实际发生了重试，总数 Used 必须如实为 4，实际 %d", rep.Used)
	}
	if got := res.State.RetriesUsed; got != 4 {
		t.Fatalf("持久化的重试总计数也应为 4（与预算是否启用无关），实际 %d", got)
	}
	want := map[string]int{"a": 2, "b": 2, "c": 0, "d": 0}
	for name, wantV := range want {
		if got := rep.PerNode[name]; got != wantV {
			t.Fatalf("节点 %q 重试次数=%d，期望 %d（PerNode=%v）", name, got, wantV, rep.PerNode)
		}
	}
	if sum := sumPerNode(t, nodes, rep); sum != rep.Used {
		t.Fatalf("总数必须等于各节点重试次数之和: Used=%d sum(PerNode)=%d", rep.Used, sum)
	}
	if res.State.Nodes["b"].Status != StatusFailed || res.State.Nodes["b"].BudgetBlocked {
		st := res.State.Nodes["b"]
		t.Fatalf("b 应为自身 MaxAttempts 耗尽（failed 且 BudgetBlocked=false）: status=%s blocked=%v",
			st.Status, st.BudgetBlocked)
	}
	if res.State.Nodes["d"].Status != StatusSkipped {
		t.Fatalf("d 应被标记 skipped，实际 %s", res.State.Nodes["d"].Status)
	}
	if atomic.LoadInt32(&bCalls) != 3 {
		t.Fatalf("b 应恰好尝试 3 次，实际 %d", bCalls)
	}
	t.Logf("判定: Used=4 == ΣPerNode=4；b=自身上限耗尽(非预算阻断)；c/d 零重试显式记 0")
}

// ---- 场景 2：两种“不再重试”的原因可区分，且两类计数互不混 ----

func TestStopReasonsDistinguished(t *testing.T) {
	t.Run("self exhausted without budget", func(t *testing.T) {
		// 不启用共享预算：r 自身 MaxAttempts=2，2 次全败 -> failed + BudgetBlocked=false。
		nodes := []NodeDef{{Name: "r"}}
		reg := TaskRegistry{"r": func(_ context.Context, _ string, attempt int) error {
			t.Logf("输入: r 第 %d 次尝试，MaxAttempts=2，无共享预算", attempt)
			return fmt.Errorf("r boom %d", attempt)
		}}
		cfg := Config{MaxConcurrency: 1, DefaultRetry: RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Millisecond}}
		res, err := runInmem(t, "wf-report-self", nodes, reg, cfg)
		if err != nil {
			t.Fatal(err)
		}
		rep := res.State.RetryReport()
		st := res.State.Nodes["r"]
		t.Logf("结果: r status=%s attempts=%d blocked=%v Used=%d PerNode=%v",
			st.Status, st.Attempts, st.BudgetBlocked, rep.Used, rep.PerNode)
		if res.Succeeded || st.Status != StatusFailed {
			t.Fatalf("r 应永久失败: succeeded=%v status=%s", res.Succeeded, st.Status)
		}
		if st.BudgetBlocked {
			t.Fatal("自身 MaxAttempts 耗尽不得被标记为 BudgetBlocked（两种原因不可混）")
		}
		if st.Attempts != 2 || rep.Used != 1 || rep.PerNode["r"] != 1 {
			t.Fatalf("自身上限耗尽前发起的 1 次重试仍应计入: attempts=%d used=%d per=%v",
				st.Attempts, rep.Used, rep.PerNode)
		}
		if len(rep.BudgetBlocked) != 0 {
			t.Fatalf("无共享预算时 BudgetBlocked 列表必须为空: %v", rep.BudgetBlocked)
		}
		t.Logf("判定: r 归类为“自身上限耗尽”，其 1 次重试照常在 Used/PerNode 中计数")
	})

	t.Run("budget exhausted blocks later nodes", func(t *testing.T) {
		// 启用共享预算：p/q 自身均允许 4 次尝试，但整次运行只给 2 次共享重试，串行执行：
		//   p: 首次 + 重试2 + 重试3（用掉全部预算）-> 第 4 次尝试被预算拒绝，BudgetBlocked；
		//   q: 仅首次尝试失败 -> 申请重试时预算已被 p 用完，BudgetBlocked（0 次重试）。
		nodes := []NodeDef{{Name: "p"}, {Name: "q"}}
		calls := map[string]int{}
		reg := TaskRegistry{}
		for _, n := range nodes {
			n := n
			reg[n.Name] = func(_ context.Context, _ string, attempt int) error {
				calls[n.Name]++
				t.Logf("输入: 节点 %s 第 %d 次尝试（共享预算=2，自身上限=4）", n.Name, attempt)
				return fmt.Errorf("%s boom %d", n.Name, attempt)
			}
		}
		cfg := Config{
			MaxConcurrency: 1,
			DefaultRetry:   RetryPolicy{MaxAttempts: 4, InitialBackoff: time.Millisecond},
			MaxRetries:     2,
		}
		res, err := runInmem(t, "wf-report-budget", nodes, reg, cfg)
		if err != nil {
			t.Fatal(err)
		}
		rep := res.State.RetryReport()
		t.Logf("结果: calls=%v Used=%d/%d PerNode=%v BudgetBlocked=%v",
			calls, rep.Used, rep.Budget, rep.PerNode, rep.BudgetBlocked)

		if calls["p"] != 3 || calls["q"] != 1 {
			t.Fatalf("p 应发起 3 次尝试（1+2 次重试），q 仅 1 次首次尝试: calls=%v", calls)
		}
		for _, n := range nodes {
			st := res.State.Nodes[n.Name]
			if st.Status != StatusFailed || !st.BudgetBlocked {
				t.Fatalf("%s 必须是预算耗尽型失败（failed + BudgetBlocked=true）: status=%s blocked=%v",
					n.Name, st.Status, st.BudgetBlocked)
			}
		}
		// 被拒绝的那次“重试”从未发起，不得计入总数：只有 p 的 2 次实际重试计数。
		if rep.Used != 2 || rep.PerNode["p"] != 2 || rep.PerNode["q"] != 0 {
			t.Fatalf("计数与预算阻断混淆: used=%d per=%v（被预算拒绝的尝试不得计数）", rep.Used, rep.PerNode)
		}
		if sum := sumPerNode(t, nodes, rep); sum != rep.Used {
			t.Fatalf("总数必须等于各节点之和: Used=%d sum=%d", rep.Used, sum)
		}
		got := append([]string(nil), rep.BudgetBlocked...)
		sort.Strings(got)
		if len(got) != 2 || got[0] != "p" || got[1] != "q" {
			t.Fatalf("预算阻断节点列表错误: %v", got)
		}
		t.Logf("判定: p/q 均为“共享预算耗尽”；与上一子用例的自身上限耗尽严格区分；被拒尝试不计数")
	})
}

// ---- 场景 3：取消/中断后续跑，总数与每节点计数不重置、不重复；取消时报告反映在途计数 ----

func TestRetryCountsPersistAcrossRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s.json")
	nodes := []NodeDef{{Name: "b"}}
	// 旧用法（不设 MaxRetries）：节点自身 MaxAttempts=3。
	cfg1 := Config{MaxConcurrency: 1, DefaultRetry: RetryPolicy{MaxAttempts: 3, InitialBackoff: 5 * time.Millisecond}}
	cfg2 := Config{MaxConcurrency: 1, DefaultRetry: RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond}}

	// 第一次进程：attempt1 立即失败；attempt2 已发起（重试已记账）并阻塞，此时取消进程。
	startedAttempt2 := make(chan struct{})
	var calls1 int32
	reg1 := TaskRegistry{
		"b": func(ctx context.Context, _ string, attempt int) error {
			atomic.AddInt32(&calls1, 1)
			t.Logf("进程1 输入: b attempt=%d（MaxAttempts=3）", attempt)
			if attempt == 1 {
				return fmt.Errorf("b fail on attempt 1")
			}
			close(startedAttempt2) // attempt2 已实际发起：这次重试必须已计数
			<-ctx.Done()
			return ctx.Err()
		},
	}
	e1, _ := NewEngine("wf-report-resume", nodes, reg1, cfg1, NewFileStore(dir))
	e1.WithLogger(t.Logf)
	ctx, cancel := context.WithCancel(context.Background())
	run1Done := make(chan struct{})
	var res1 *RunResult
	go func() {
		r, _ := e1.Run(ctx)
		res1 = r
		close(run1Done)
	}()
	select {
	case <-startedAttempt2:
	case <-time.After(2 * time.Second):
		t.Fatal("未等到 attempt2 发起")
	}
	cancel()
	<-run1Done

	rep1 := res1.State.RetryReport()
	t.Logf("进程1 取消时: finished=%v b状态=%s attempts=%d Used=%d PerNode=%v",
		res1.State.Finished, res1.State.Nodes["b"].Status,
		res1.State.Nodes["b"].Attempts, rep1.Used, rep1.PerNode)
	if res1.State.Finished {
		t.Fatal("取消后工作流不应已结算为终态")
	}
	if res1.State.Nodes["b"].Attempts != 2 || rep1.Used != 1 || rep1.PerNode["b"] != 1 {
		t.Fatalf("取消未到终态时，报告必须如实反映已发起的 1 次重试: attempts=%d used=%d per=%v",
			res1.State.Nodes["b"].Attempts, rep1.Used, rep1.PerNode)
	}
	if got := atomic.LoadInt32(&calls1); got != 2 {
		t.Fatalf("进程1 应实际发起 2 次尝试（attempt1 失败 + attempt2 在途被取消），实际 %d", got)
	}

	// 第二次进程：attempt2 曾发起但随进程中断（running 残留），恢复后必须直接续跑 attempt3，
	// 不得重放 attempt2（不重复计）；attempt3 成功 -> Attempts=3，累计重试 2 次。
	var seen2 []int
	reg2 := TaskRegistry{
		"b": func(_ context.Context, _ string, attempt int) error {
			seen2 = append(seen2, attempt)
			t.Logf("进程2 输入: b attempt=%d（期望只看到 3，不重放 2）", attempt)
			if attempt == 3 {
				return nil
			}
			return fmt.Errorf("进程2 不应重放 attempt %d", attempt)
		},
	}
	e2, _ := NewEngine("wf-report-resume", nodes, reg2, cfg2, NewFileStore(dir))
	e2.WithLogger(t.Logf)
	res2, err := e2.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rep2 := res2.State.RetryReport()
	t.Logf("进程2 结果: resumed=%v succeeded=%v seen=%v attempts=%d Used=%d PerNode=%v",
		res2.Resumed, res2.Succeeded, seen2, res2.State.Nodes["b"].Attempts, rep2.Used, rep2.PerNode)
	if !res2.Resumed || !res2.Succeeded {
		t.Fatalf("续跑应识别恢复并最终成功: resumed=%v succeeded=%v", res2.Resumed, res2.Succeeded)
	}
	if len(seen2) != 1 || seen2[0] != 3 {
		t.Fatalf("attempt2 已计过数，恢复后只能续跑 attempt3，实际看到 %v", seen2)
	}
	if res2.State.Nodes["b"].Attempts != 3 || rep2.Used != 2 || rep2.PerNode["b"] != 2 {
		t.Fatalf("计数应接着累计为 2 次重试（不重置不重复）: attempts=%d used=%d per=%v",
			res2.State.Nodes["b"].Attempts, rep2.Used, rep2.PerNode)
	}
	if sum := sumPerNode(t, nodes, rep2); sum != rep2.Used {
		t.Fatalf("恢复后总数仍须等于各节点之和: Used=%d sum=%d", rep2.Used, sum)
	}

	// 第三次进程：已是成功终态，不得再有任何尝试，计数保持 2（不重置也不膨胀）。
	var calls3 int32
	reg3 := TaskRegistry{
		"b": func(context.Context, string, int) error {
			atomic.AddInt32(&calls3, 1)
			t.Fatal("b 已成功，第三次进程不得重跑")
			return nil
		},
	}
	e3, _ := NewEngine("wf-report-resume", nodes, reg3, cfg2, NewFileStore(dir))
	res3, err := e3.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rep3 := res3.State.RetryReport()
	t.Logf("进程3 结果: resumed=%v succeeded=%v calls=%d Used=%d PerNode=%v",
		res3.Resumed, res3.Succeeded, calls3, rep3.Used, rep3.PerNode)
	if !res3.Resumed || !res3.Succeeded || atomic.LoadInt32(&calls3) != 0 {
		t.Fatalf("终态节点不得重跑: resumed=%v succeeded=%v calls=%d",
			res3.Resumed, res3.Succeeded, calls3)
	}
	if rep3.Used != 2 || rep3.PerNode["b"] != 2 || res3.State.RetriesUsed != 2 {
		t.Fatalf("终态后计数必须稳定为 2（不重置、不重复）: used=%d per=%v retriesUsed=%d",
			rep3.Used, rep3.PerNode, res3.State.RetriesUsed)
	}
	t.Logf("判定: 取消时 Used=1 如实；续跑只补 attempt3；终态 Used=2 跨三次进程稳定")
}
