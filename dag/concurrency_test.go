package dag

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestConcurrencyLimitEnforced(t *testing.T) {
	globalLogger = testLogger(t)
	defer func() { globalLogger = nil }()

	const N = 4
	const limit = 2

	var mu sync.Mutex
	inFlight, maxObserved := 0, 0
	gate := make(chan struct{})

	nodes := make([]NodeDef, 0, N)
	for i := 0; i < N; i++ {
		id := string(rune('a' + i))
		id = "n" + id
		nodes = append(nodes, NodeDef{
			ID: id,
			Run: func(ctx context.Context, in NodeInput) error {
				tlogf("[input wf=%s node=%s attempt=%d] acquiring slot", in.WorkflowID, in.NodeID, in.Attempt)
				mu.Lock()
				inFlight++
				if inFlight > maxObserved {
					maxObserved = inFlight
				}
				mu.Unlock()
				defer func() {
					mu.Lock()
					inFlight--
					mu.Unlock()
				}()
				<-gate // 全部通过 gate 后才放行，制造并发重叠
				tlogf("[node=%s] running, observed in-flight peak check", in.NodeID)
				return nil
			},
		})
	}

	eng := NewEngine(Config{MaxConcurrency: limit, Logger: testLogger(t)}, NewMemoryStore())
	done := make(chan struct{})
	go func() {
		_, err := eng.Run(context.Background(), &WorkflowDef{ID: "conc", Nodes: nodes})
		if err != nil {
			t.Errorf("run: %v", err)
		}
		close(done)
	}()

	time.Sleep(150 * time.Millisecond) // 等前 limit 个节点到达 gate
	mu.Lock()
	peakBefore := maxObserved
	started := inFlight
	mu.Unlock()
	tlogf("[concurrency] before gate release: inFlight=%d peak=%d (limit=%d)", started, peakBefore, limit)
	if started != limit {
		t.Fatalf("expect exactly %d nodes in flight before release, got %d", limit, started)
	}
	close(gate)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("workflow did not finish in time")
	}
	if maxObserved > limit {
		t.Fatalf("in-flight peak %d exceeded limit %d", maxObserved, limit)
	}
	tlogf("[concurrency] verdict PASS: maxObserved=%d <= limit=%d", maxObserved, limit)

	st, _ := eng.LoadState("conc")
	if st.Status != WFSucceeded {
		t.Fatalf("expect workflow succeeded, got %s: %s", st.Status, st.Error)
	}
}

func TestDuplicateExecutionDetectedViaExecCount(t *testing.T) {
	globalLogger = testLogger(t)
	defer func() { globalLogger = nil }()

	var calls int
	var mu sync.Mutex
	def := &WorkflowDef{ID: "once", Nodes: []NodeDef{
		{ID: "a", Run: func(ctx context.Context, in NodeInput) error {
			mu.Lock()
			calls++
			mu.Unlock()
			tlogf("[input node=%s attempt=%d] call count now=%d", in.NodeID, in.Attempt, calls)
			return nil
		}},
		{ID: "b", Deps: []string{"a"}, Run: func(ctx context.Context, in NodeInput) error {
			tlogf("[input node=%s attempt=%d] runs only after a", in.NodeID, in.Attempt)
			return nil
		}},
	}}
	eng := NewEngine(Config{MaxConcurrency: 1, Logger: testLogger(t)}, NewMemoryStore())
	st, err := eng.Run(context.Background(), def)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != WFSucceeded {
		t.Fatalf("status=%s", st.Status)
	}
	if got := eng.ExecCount("once", "a"); got != 1 {
		t.Fatalf("node a executed %d times, expect exactly 1", got)
	}
	// 再次 Run 终态工作流：不得重复执行。
	st2, err := eng.Run(context.Background(), def)
	if err != nil {
		t.Fatal(err)
	}
	if got := eng.ExecCount("once", "a"); got != 1 {
		t.Fatalf("node a re-executed on terminal rerun: count=%d", got)
	}
	if st2.Status != WFSucceeded {
		t.Fatalf("rerun status=%s", st2.Status)
	}
	tlogf("[idempotency] verdict PASS: node a exec count stayed 1 across rerun")
}
