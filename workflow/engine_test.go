package workflow

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- 校验：错误归类 ----

func TestValidateErrors(t *testing.T) {
	cases := []struct {
		name  string
		nodes []NodeDef
		want  string // "empty" | "dup" | "missing" | "cycle"
	}{
		{"empty name", []NodeDef{{Name: ""}}, "empty"},
		{"duplicate node", []NodeDef{{Name: "a"}, {Name: "a"}}, "dup"},
		{"missing dependency", []NodeDef{{Name: "a"}, {Name: "b", DependsOn: []string{"x"}}}, "missing"},
		{"cycle", []NodeDef{
			{Name: "a", DependsOn: []string{"c"}},
			{Name: "b", DependsOn: []string{"a"}},
			{Name: "c", DependsOn: []string{"b"}},
		}, "cycle"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("输入: nodes=%v 判定依据: 期望错误类别=%q", tc.nodes, tc.want)
			_, err := Validate(tc.nodes)
			if err == nil {
				t.Fatalf("期望校验失败，实际通过")
			}
			var got string
			switch {
			case errors.As(err, new(*EmptyNameError)):
				got = "empty"
			case errors.As(err, new(*DuplicateNodeError)):
				got = "dup"
			case errors.As(err, new(*MissingDependencyError)):
				got = "missing"
			case errors.As(err, new(*CycleError)):
				got = "cycle"
			default:
				t.Fatalf("未归类错误: %v", err)
			}
			t.Logf("实际错误: %v 归类=%q", err, got)
			if got != tc.want {
				t.Fatalf("错误类别不符: got=%q want=%q", got, tc.want)
			}
		})
	}
}

func TestValidateTopoOrder(t *testing.T) {
	nodes := []NodeDef{
		{Name: "c", DependsOn: []string{"a", "b"}},
		{Name: "a"},
		{Name: "b", DependsOn: []string{"a"}},
	}
	order, err := Validate(nodes)
	if err != nil {
		t.Fatalf("校验失败: %v", err)
	}
	t.Logf("输入: %v 拓扑序: %v", nodes, order)
	pos := map[string]int{}
	for i, n := range order {
		pos[n] = i
	}
	if !(pos["a"] < pos["b"] && pos["b"] < pos["c"] && pos["a"] < pos["c"]) {
		t.Fatalf("拓扑序错误: %v", order)
	}
}

// ---- 执行顺序 ----

func TestExecuteInDependencyOrder(t *testing.T) {
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
	nodes := []NodeDef{
		{Name: "c", DependsOn: []string{"a", "b"}},
		{Name: "a"},
		{Name: "b", DependsOn: []string{"a"}},
	}
	e, err := NewEngine("wf-order", nodes, reg, Config{MaxConcurrency: 4}, NewFileStore(filepath.Join(t.TempDir(), "s.json")))
	if err != nil {
		t.Fatal(err)
	}
	e.WithLogger(t.Logf)
	res, err := e.Run(context.Background())
	if err != nil || !res.Succeeded {
		t.Fatalf("执行失败 err=%v res=%+v", err, res)
	}
	t.Logf("实际执行顺序: %v 判定依据: a 必须先于 b，b 必须先于 c", order)
	if len(order) != 3 || order[0] != "a" || order[1] != "b" || order[2] != "c" {
		t.Fatalf("执行顺序不确定或错误: %v", order)
	}
}

// ---- 并发上限 ----

func TestConcurrencyLimit(t *testing.T) {
	const limit = 2
	var active, maxActive int32
	gate := make(chan struct{})
	reg := TaskRegistry{}
	// 5 个互不依赖的节点，全部进入后一起放行，借此观测峰值并发。
	for i := 0; i < 5; i++ {
		reg[fmt.Sprintf("n%d", i)] = func(context.Context, string, int) error {
			cur := atomic.AddInt32(&active, 1)
			for {
				old := atomic.LoadInt32(&maxActive)
				if cur <= old || atomic.CompareAndSwapInt32(&maxActive, old, cur) {
					break
				}
			}
			<-gate
			atomic.AddInt32(&active, -1)
			return nil
		}
	}
	var nodes []NodeDef
	for i := 0; i < 5; i++ {
		nodes = append(nodes, NodeDef{Name: fmt.Sprintf("n%d", i)})
	}
	e, err := NewEngine("wf-conc", nodes, reg, Config{MaxConcurrency: limit}, NewFileStore(filepath.Join(t.TempDir(), "s.json")))
	if err != nil {
		t.Fatal(err)
	}
	e.WithLogger(t.Logf)
	done := make(chan struct{})
	go func() {
		res, _ := e.Run(context.Background())
		t.Logf("判定依据: 峰值并发=%d 上限=%d 整体成功=%v", atomic.LoadInt32(&maxActive), limit, res.Succeeded)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	t.Logf("放行前在途任务数: %d (输入: MaxConcurrency=%d)", atomic.LoadInt32(&active), limit)
	close(gate)
	<-done
	if max := atomic.LoadInt32(&maxActive); max > limit {
		t.Fatalf("并发超限: max=%d limit=%d", max, limit)
	}
	if max := atomic.LoadInt32(&maxActive); max < limit {
		t.Fatalf("未达到并发上限，疑似串行: max=%d", max)
	}
}

// ---- 重试、退避、耗尽后不重复调度 + 下游跳过 + 整体成败区分 ----

func TestRetryExhaustionAndSkip(t *testing.T) {
	var mu sync.Mutex
	bCalls := 0
	cCalls := 0
	reg := TaskRegistry{
		"a": func(context.Context, string, int) error { return nil },
		"b": func(_ context.Context, _ string, attempt int) error {
			mu.Lock()
			bCalls++
			mu.Unlock()
			t.Logf("输入: 节点 b 第 %d 次尝试，判定依据: 永远失败，MaxAttempts=3", attempt)
			return fmt.Errorf("boom on attempt %d", attempt)
		},
		"c": func(context.Context, string, int) error {
			mu.Lock()
			cCalls++
			mu.Unlock()
			return nil
		},
	}
	nodes := []NodeDef{
		{Name: "a"},
		{Name: "b", DependsOn: []string{"a"}},
		{Name: "c", DependsOn: []string{"b"}},
	}
	cfg := Config{
		MaxConcurrency: 2,
		DefaultRetry: RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     5 * time.Millisecond,
			Factor:         2,
		},
	}
	store := NewFileStore(filepath.Join(t.TempDir(), "s.json"))
	e, err := NewEngine("wf-retry", nodes, reg, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	e.WithLogger(t.Logf)
	start := time.Now()
	res, err := e.Run(context.Background())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("结果: succeeded=%v b尝试次数=%d c执行次数=%d 耗时=%v 判定依据: 至少经历 1ms+2ms 两次退避",
		res.Succeeded, bCalls, cCalls, elapsed)
	if res.Succeeded {
		t.Fatal("b 永久失败时工作流不得判定为整体成功")
	}
	if bCalls != 3 {
		t.Fatalf("b 应恰好执行 3 次（无重复调度），实际 %d", bCalls)
	}
	if cCalls != 0 {
		t.Fatalf("c 应被上游失败连带跳过，实际执行 %d 次", cCalls)
	}
	if got := res.State.Nodes["b"].Status; got != StatusFailed {
		t.Fatalf("b 状态=%s，期望 failed", got)
	}
	if got := res.State.Nodes["c"].Status; got != StatusSkipped {
		t.Fatalf("c 状态=%s，期望 skipped", got)
	}
	if elapsed < 3*time.Millisecond {
		t.Fatalf("退避未生效: 耗时 %v", elapsed)
	}

	// 再跑一次（重启恢复）：b 预算已耗尽，不得被重复调度；整体仍失败。
	e2, _ := NewEngine("wf-retry", nodes, reg, cfg, store)
	e2.WithLogger(t.Logf)
	res2, err := e2.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("恢复后: succeeded=%v resumed=%v b总尝试=%d 判定依据: 耗尽节点重启后不得再执行",
		res2.Succeeded, res2.Resumed, bCalls)
	if !res2.Resumed || res2.Succeeded || bCalls != 3 {
		t.Fatalf("恢复语义错误: resumed=%v succeeded=%v bCalls=%d", res2.Resumed, res2.Succeeded, bCalls)
	}
}

// ---- 超时传播 ----

func TestNodeTimeout(t *testing.T) {
	var attempts int32
	reg := TaskRegistry{
		"slow": func(ctx context.Context, _ string, attempt int) error {
			t.Logf("输入: slow 第 %d 次尝试，节点超时=20ms，判定依据: 任务必须感知 ctx", attempt)
			<-ctx.Done()
			atomic.AddInt32(&attempts, 1)
			return ctx.Err()
		},
	}
	nodes := []NodeDef{{Name: "slow", Timeout: 20 * time.Millisecond}}
	cfg := Config{MaxConcurrency: 1, DefaultRetry: RetryPolicy{MaxAttempts: 1}}
	e, _ := NewEngine("wf-timeout", nodes, reg, cfg, NewFileStore(filepath.Join(t.TempDir(), "s.json")))
	e.WithLogger(t.Logf)
	start := time.Now()
	res, err := e.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("耗时=%v 状态=%s 错误=%q", time.Since(start), res.State.Nodes["slow"].Status, res.State.Nodes["slow"].LastError)
	if res.Succeeded || atomic.LoadInt32(&attempts) != 1 {
		t.Fatalf("超时节点应失败且只执行一次: attempts=%d", attempts)
	}
	if res.State.Nodes["slow"].LastError == "" {
		t.Fatal("失败原因应被记录")
	}
}

// ---- 取消传播到在途任务 ----

func TestCancelPropagation(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	reg := TaskRegistry{
		"task": func(ctx context.Context, _ string, _ int) error {
			close(started)
			<-ctx.Done()
			t.Logf("在途任务感知到取消: %v", ctx.Err())
			close(cancelled)
			return ctx.Err()
		},
	}
	nodes := []NodeDef{{Name: "task"}}
	e, _ := NewEngine("wf-cancel", nodes, reg, Config{MaxConcurrency: 1, DefaultRetry: RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Hour}},
		NewFileStore(filepath.Join(t.TempDir(), "s.json")))
	e.WithLogger(t.Logf)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		res, _ := e.Run(ctx)
		t.Logf("取消后返回: succeeded=%v 状态=%s", res.Succeeded, res.State.Nodes["task"].Status)
		close(done)
	}()
	<-started
	cancel()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("取消未传播到在途任务")
	}
	<-done
}

// ---- 重启恢复：成功节点不重跑，未耗尽失败节点继续重试，最终整体成功 ----

func TestRestartResumeRetriesThenSuccess(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s.json")
	nodes := []NodeDef{
		{Name: "a"},
		{Name: "b", DependsOn: []string{"a"}},
		{Name: "c", DependsOn: []string{"b"}},
	}
	cfg1 := Config{MaxConcurrency: 2, DefaultRetry: RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Hour}}
	cfg2 := Config{MaxConcurrency: 2, DefaultRetry: RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond}}

	// 第一次进程：a 成功；b 第 1 次尝试失败，长退避期间进程被取消（模拟重启）。
	// 用确定性信号代替 time.Sleep：b 首次失败返回后、进入长退避前关闭通道，
	// 测试收到信号再取消，取消点必然落在退避窗口内，不再依赖机器调度时序。
	var aCalls int32
	callsB := int32(1)
	bFailedCh := make(chan struct{})
	reg1 := TaskRegistry{
		"a": func(context.Context, string, int) error { atomic.AddInt32(&aCalls, 1); return nil },
		"b": func(ctx context.Context, _ string, attempt int) error {
			if attempt == 1 {
				close(bFailedCh)
				return errors.New("first failure")
			}
			<-ctx.Done() // 不应走到：第 2 次尝试前进程即被取消
			return ctx.Err()
		},
		"c": func(context.Context, string, int) error { t.Fatal("c 在第一次进程中不应执行"); return nil },
	}
	e1, _ := NewEngine("wf-resume", nodes, reg1, cfg1, NewFileStore(dir))
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
	t.Logf("第一次进程: succeeded=%v b状态=%s b尝试=%d 判定依据: 被取消且预算未耗尽",
		res1.Succeeded, res1.State.Nodes["b"].Status, res1.State.Nodes["b"].Attempts)
	_ = callsB

	// 第二次进程：同一状态文件，b 第 2 次尝试成功；a 不得重复执行。
	var aCalls2, cCalls2 int32
	reg2 := TaskRegistry{
		"a": func(context.Context, string, int) error { atomic.AddInt32(&aCalls2, 1); return nil },
		"b": func(_ context.Context, _ string, attempt int) error {
			t.Logf("输入: b 恢复后尝试序号=%d 判定依据: 接续 Attempts 计数而非从 1 重来", attempt)
			if attempt == 2 {
				return nil
			}
			return fmt.Errorf("unexpected attempt %d", attempt)
		},
		"c": func(context.Context, string, int) error { atomic.AddInt32(&cCalls2, 1); return nil },
	}
	e2, _ := NewEngine("wf-resume", nodes, reg2, cfg2, NewFileStore(dir))
	e2.WithLogger(t.Logf)
	res2, err := e2.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("第二次进程: resumed=%v succeeded=%v a重跑次数=%d c执行=%d b最终状态=%s",
		res2.Resumed, res2.Succeeded, aCalls2, cCalls2, res2.State.Nodes["b"].Status)
	if !res2.Resumed {
		t.Fatal("应识别为恢复执行")
	}
	if !res2.Succeeded {
		t.Fatal("b 重试成功后应整体成功")
	}
	if atomic.LoadInt32(&aCalls) != 1 || aCalls2 != 0 {
		t.Fatalf("已成功节点 a 不得重复执行: first=%d second=%d", aCalls, aCalls2)
	}
	if cCalls2 != 1 {
		t.Fatalf("c 应恰好执行一次，实际 %d", cCalls2)
	}
	if res2.State.Nodes["b"].Attempts != 2 {
		t.Fatalf("b 总尝试次数应为 2，实际 %d", res2.State.Nodes["b"].Attempts)
	}
}

// ---- 恢复：中断时处于 running 的节点按残留处理并重试 ----

func TestRestartInterruptedRunning(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s.json")
	nodes := []NodeDef{{Name: "b"}}
	cfg := Config{MaxConcurrency: 1, DefaultRetry: RetryPolicy{MaxAttempts: 2}}

	// 手工构造一个“进程被杀死”时的状态：b 正在 running，已尝试 1 次。
	interrupted := &WorkflowState{
		WorkflowName: "wf-interrupt",
		Nodes: map[string]*NodeState{
			"b": {Status: StatusRunning, Attempts: 1, LastError: ""},
		},
	}
	if err := NewFileStore(dir).Save(interrupted); err != nil {
		t.Fatal(err)
	}

	var seen []int
	var mu sync.Mutex
	reg := TaskRegistry{
		"b": func(_ context.Context, _ string, attempt int) error {
			mu.Lock()
			seen = append(seen, attempt)
			mu.Unlock()
			return nil
		},
	}
	e, _ := NewEngine("wf-interrupt", nodes, reg, cfg, NewFileStore(dir))
	e.WithLogger(t.Logf)
	res, err := e.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("恢复输入: b=running,attempts=1；实际重试序号=%v；succeeded=%v 判定依据: running 为中断残留", seen, res.Succeeded)
	if !res.Succeeded || len(seen) != 1 || seen[0] != 2 {
		t.Fatalf("running 残留节点应以第 2 次尝试继续执行，实际 %v", seen)
	}
}

// ---- 重复调度检测：所有节点在一次运行中至多执行一次（失败节点至多 MaxAttempts 次）----

func TestNoDuplicateScheduling(t *testing.T) {
	reg := TaskRegistry{}
	var nodes []NodeDef
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("n%d", i)
		var deps []string
		if i > 0 {
			deps = []string{fmt.Sprintf("n%d", i-1)}
		}
		nodes = append(nodes, NodeDef{Name: name, DependsOn: deps})
		reg[name] = func(context.Context, string, int) error { return nil }
	}
	counts := sync.Map{}
	for name, fn := range reg {
		fn := fn
		reg[name] = func(ctx context.Context, n string, a int) error {
			v, _ := counts.LoadOrStore(n, new(int32))
			atomic.AddInt32(v.(*int32), 1)
			return fn(ctx, n, a)
		}
	}
	e, _ := NewEngine("wf-dup", nodes, reg, Config{MaxConcurrency: 3, DefaultRetry: RetryPolicy{MaxAttempts: 1}},
		NewFileStore(filepath.Join(t.TempDir(), "s.json")))
	e.WithLogger(t.Logf)
	res, err := e.Run(context.Background())
	if err != nil || !res.Succeeded {
		t.Fatalf("执行失败 err=%v", err)
	}
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("n%d", i)
		v, _ := counts.Load(name)
		if got := atomic.LoadInt32(v.(*int32)); got != 1 {
			t.Fatalf("节点 %s 被执行 %d 次，期望恰好 1 次（存在重复调度）", name, got)
		}
	}
	t.Logf("判定依据: 8 个节点各执行 1 次，无重复调度")
}
