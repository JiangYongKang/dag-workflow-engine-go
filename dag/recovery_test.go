package dag

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// crashStore 在指定节点尝试开始并持久化 running 后返回错误，
// 模拟进程在任务在途时崩溃（running 状态已落盘）。
type crashStore struct {
	Store
	crashKey string
	crashed  atomic.Bool
}

func (c *crashStore) Save(st *WorkflowState) error {
	if !c.crashed.Load() {
		if ns := st.Nodes[c.crashKey]; ns != nil && ns.Status == StatusRunning && ns.Attempts > 0 {
			c.crashed.Store(true)
			// 先让 running 状态落盘（写到被包装的底层存储），再模拟崩溃。
			_ = c.Store.Save(st)
			return errors.New("simulated process crash during node execution")
		}
	}
	return c.Store.Save(st)
}

func TestRestartRecoverySucceedsWithRetriesLeft(t *testing.T) {
	globalLogger = testLogger(t)
	defer func() { globalLogger = nil }()

	dir := filepath.Join(t.TempDir(), "store")
	fs, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	cs := &crashStore{Store: fs, crashKey: "b"}

	var aCalls, bCalls int32

	mkDef := func() *WorkflowDef {
		return &WorkflowDef{ID: "wf-resume-ok", Nodes: []NodeDef{
			{ID: "a", Run: func(ctx context.Context, in NodeInput) error {
				n := atomic.AddInt32(&aCalls, 1)
				tlogf("[input node=a attempt=%d] run #%d in this process", in.Attempt, n)
				return nil
			}},
			{
				ID:   "b",
				Deps: []string{"a"},
				Run: func(ctx context.Context, in NodeInput) error {
					n := atomic.AddInt32(&bCalls, 1)
					tlogf("[input node=b attempt=%d] run #%d in this process", in.Attempt, n)
					return nil
				},
				Retry: RetryPolicy{MaxAttempts: 3}, // 崩溃的那次计入，仍有 2 次预算
			},
		}}
	}

	// 第一次运行：b 开始执行时"崩溃"。
	eng1 := NewEngine(Config{MaxConcurrency: 1, Logger: testLogger(t)}, cs)
	_, err1 := eng1.Run(context.Background(), mkDef())
	tlogf("[run1] returned err=%v (expected simulated crash)", err1)
	if err1 == nil {
		t.Fatal("expect crash error on run1")
	}

	// 检查落盘文件：a 已成功，b 停留在 running。
	raw, _ := os.ReadFile(filepath.Join(dir, "wf-resume-ok.json"))
	tlogf("[run1] persisted snapshot:\n%s", string(raw))

	// 模拟新进程：全新引擎 + 同一文件存储 + 计数器清零后重新累计便于解读。
	atomic.StoreInt32(&aCalls, 0)
	atomic.StoreInt32(&bCalls, 0)
	eng2 := NewEngine(Config{MaxConcurrency: 1, Logger: testLogger(t)}, fs)
	st, err := eng2.Run(context.Background(), mkDef())
	if err != nil {
		t.Fatalf("run2: %v", err)
	}

	if st.Status != WFSucceeded {
		t.Fatalf("expect overall succeeded after recovery, got %s: %s", st.Status, st.Error)
	}
	if st.Nodes["a"].Status != StatusSucceeded || st.Nodes["b"].Status != StatusSucceeded {
		t.Fatalf("node status a=%s b=%s", st.Nodes["a"].Status, st.Nodes["b"].Status)
	}
	// 恢复后 a 绝不能重跑；b 在新进程恰好执行 1 次（崩溃前总尝试 1 + 恢复后 1 = 2 <= 3）。
	if got := atomic.LoadInt32(&aCalls); got != 0 {
		t.Fatalf("succeeded node a re-executed after restart: %d times", got)
	}
	if got := atomic.LoadInt32(&bCalls); got != 1 {
		t.Fatalf("node b expected exactly 1 post-restart run, got %d", got)
	}
	if st.Nodes["a"].Attempts != 1 {
		t.Fatalf("a attempts=%d", st.Nodes["a"].Attempts)
	}
	if st.Nodes["b"].Attempts != 2 {
		t.Fatalf("b total attempts=%d expect 2 (1 interrupted + 1 after resume)", st.Nodes["b"].Attempts)
	}
	tlogf("[recovery-ok] verdict PASS: a not re-run, b retried once after resume, overall SUCCEEDED")
}

func TestRestartRecoveryFailsWhenBudgetExhausted(t *testing.T) {
	globalLogger = testLogger(t)
	defer func() { globalLogger = nil }()

	dir := filepath.Join(t.TempDir(), "store2")
	fs, _ := NewFileStore(dir)
	cs := &crashStore{Store: fs, crashKey: "only"}

	var calls int32
	mkDef := func() *WorkflowDef {
		return &WorkflowDef{ID: "wf-resume-fail", Nodes: []NodeDef{
			{
				ID: "only",
				Run: func(ctx context.Context, in NodeInput) error {
					atomic.AddInt32(&calls, 1)
					tlogf("[input node=only attempt=%d]", in.Attempt)
					return nil
				},
				Retry: RetryPolicy{MaxAttempts: 1}, // 崩溃那次即第 1 次，预算耗尽
			},
		}}
	}

	eng1 := NewEngine(Config{MaxConcurrency: 1, Logger: testLogger(t)}, cs)
	if _, err := eng1.Run(context.Background(), mkDef()); err == nil {
		t.Fatal("expect crash on run1")
	}
	atomic.StoreInt32(&calls, 0)

	eng2 := NewEngine(Config{MaxConcurrency: 1, Logger: testLogger(t)}, fs)
	st, err := eng2.Run(context.Background(), mkDef())
	if err != nil {
		t.Fatalf("run2: %v", err)
	}
	if st.Status != WFFailed {
		t.Fatalf("expect overall FAILED distinguishable from success, got %s", st.Status)
	}
	if st.Nodes["only"].Status != StatusFailed {
		t.Fatalf("node expect failed after budget exhausted, got %s", st.Nodes["only"].Status)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("exhausted node must not run again, got %d", got)
	}
	if st.Nodes["only"].Attempts != 1 {
		t.Fatalf("attempts=%d", st.Nodes["only"].Attempts)
	}
	tlogf("[recovery-fail] verdict PASS: interrupted node budget exhausted -> overall FAILED, no re-execution")
}

func TestFileStoreRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	fs, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	st := &WorkflowState{ID: "x", Status: WFRunning, Nodes: map[string]*NodeState{
		"n": {ID: "n", Status: StatusSucceeded, Attempts: 1,
			Records: []AttemptRecord{{Attempt: 1, Err: "e"}}},
	}}
	if err := fs.Save(st); err != nil {
		t.Fatal(err)
	}
	got, err := fs.Load("x")
	if err != nil {
		t.Fatal(err)
	}
	if got.Nodes["n"].Status != StatusSucceeded || got.Nodes["n"].Records[0].Err != "e" {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if _, err := fs.Load("missing"); !errors.Is(err, ErrWorkflowNotFound) {
		t.Fatalf("expect ErrWorkflowNotFound, got %v", err)
	}
	tlogf("[filestore] verdict PASS: save/load round trip ok, missing id classified not-found")
}

// midRunCrashStore 在目标节点尝试已落盘 running 后、尝试结束前制造致命错误，
// 模拟节点正在执行时进程被杀（状态文件停在 running + 未闭合 record）。
type midRunCrashStore struct {
	Store
	crashKey string
	done     atomic.Bool
}

func (m *midRunCrashStore) Save(st *WorkflowState) error {
	if !m.done.Load() {
		if ns := st.Nodes[m.crashKey]; ns != nil && ns.Status == StatusRunning &&
			ns.Attempts == 1 && len(ns.Records) == 1 && ns.Records[0].EndedAt.IsZero() {
			m.done.Store(true)
			_ = m.Store.Save(st) // running 状态先落盘
			return errors.New("simulated kill -9 during execution")
		}
	}
	return m.Store.Save(st)
}

func TestRecoveryAfterMidRunCrashClosesInterruptedAttempt(t *testing.T) {
	globalLogger = testLogger(t)
	defer func() { globalLogger = nil }()

	dir := filepath.Join(t.TempDir(), "store3")
	fs, _ := NewFileStore(dir)
	cs := &midRunCrashStore{Store: fs, crashKey: "job"}

	var calls int32
	mkDef := func() *WorkflowDef {
		return &WorkflowDef{ID: "wf-killed", Nodes: []NodeDef{
			{
				ID: "job",
				Run: func(ctx context.Context, in NodeInput) error {
					atomic.AddInt32(&calls, 1)
					tlogf("[input node=job attempt=%d] executing...", in.Attempt)
					return nil
					// 注：第一次运行在"尝试开始落盘"时即致命中止，节点函数实际未被调用；
					// 进程重启后由恢复路径重新调度该节点。
				},
				Retry: RetryPolicy{MaxAttempts: 2},
			},
		}}
	}

	eng1 := NewEngine(Config{MaxConcurrency: 1, Logger: testLogger(t)}, cs)
	_, err := eng1.Run(context.Background(), mkDef())
	if err == nil {
		t.Fatal("expect fatal crash error")
	}
	tlogf("[run1] aborted as expected: %v", err)

	// 落盘快照应停留在 running 且 record 未闭合。
	st0, _ := fs.Load("wf-killed")
	if st0.Nodes["job"].Status != StatusRunning || st0.Nodes["job"].Records[0].EndedAt.IsZero() == false {
		t.Fatalf("unexpected on-disk state before resume: status=%s records=%+v",
			st0.Nodes["job"].Status, st0.Nodes["job"].Records)
	}
	tlogf("[run1] on-disk: job running with open record as expected")

	atomic.StoreInt32(&calls, 0)
	eng2 := NewEngine(Config{MaxConcurrency: 1, Logger: testLogger(t)}, fs)
	st, err := eng2.Run(context.Background(), mkDef())
	if err != nil {
		t.Fatalf("run2: %v", err)
	}
	if st.Status != WFSucceeded {
		t.Fatalf("expect succeeded after retrying interrupted attempt, got %s", st.Status)
	}
	ns := st.Nodes["job"]
	if ns.Attempts != 2 {
		t.Fatalf("expect total attempts=2 (1 interrupted + 1 retry), got %d", ns.Attempts)
	}
	if len(ns.Records) != 2 || ns.Records[0].Err != "interrupted by process restart" {
		t.Fatalf("interrupted record not closed correctly: %+v", ns.Records)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("expect exactly 1 run after restart, got %d", calls)
	}
	tlogf("[recovery-midrun] verdict PASS: open attempt marked interrupted, one retry, overall SUCCEEDED")
}
