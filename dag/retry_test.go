package dag

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryBackoffThenSuccess(t *testing.T) {
	globalLogger = testLogger(t)
	defer func() { globalLogger = nil }()

	var attempts int32
	def := &WorkflowDef{ID: "retry-ok", Nodes: []NodeDef{
		{
			ID: "flaky",
			Run: func(ctx context.Context, in NodeInput) error {
				n := atomic.AddInt32(&attempts, 1)
				tlogf("[input node=%s attempt=%d]", in.NodeID, n)
				if n < 3 {
					return fmt.Errorf("simulated failure #%d", n)
				}
				return nil
			},
			Retry: RetryPolicy{MaxAttempts: 3, InitialDelay: 20 * time.Millisecond, BackoffFactor: 2},
		},
	}}
	eng := NewEngine(Config{MaxConcurrency: 1, Logger: testLogger(t)}, NewMemoryStore())
	start := time.Now()
	st, err := eng.Run(context.Background(), def)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if st.Nodes["flaky"].Status != StatusSucceeded {
		t.Fatalf("expect succeeded, got %s: %s", st.Nodes["flaky"].Status, st.Nodes["flaky"].Error)
	}
	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Fatalf("expect 3 attempts, got %d", got)
	}
	if eng.ExecCount("retry-ok", "flaky") != 3 {
		t.Fatalf("exec count=%d", eng.ExecCount("retry-ok", "flaky"))
	}
	// 退避：20ms + 40ms = 60ms（允许调度抖动）
	if elapsed < 55*time.Millisecond {
		t.Fatalf("backoff not honored: elapsed=%s", elapsed)
	}
	tlogf("[retry] verdict PASS: attempts=%d elapsed=%s (expected backoff ~60ms)", attempts, elapsed)
}

func TestRetryExhaustedMarksFailedAndSkipsDownstream(t *testing.T) {
	globalLogger = testLogger(t)
	defer func() { globalLogger = nil }()

	var aCalls, bCalls int32
	def := &WorkflowDef{ID: "retry-fail", Nodes: []NodeDef{
		{
			ID: "a",
			Run: func(ctx context.Context, in NodeInput) error {
				atomic.AddInt32(&aCalls, 1)
				tlogf("[input node=a attempt=%d] -> always fail", in.Attempt)
				return errors.New("permanent boom")
			},
			Retry: RetryPolicy{MaxAttempts: 2, InitialDelay: 10 * time.Millisecond, BackoffFactor: 2},
		},
		{ID: "b", Deps: []string{"a"}, Run: func(ctx context.Context, in NodeInput) error {
			atomic.AddInt32(&bCalls, 1)
			return nil
		}},
	}}
	eng := NewEngine(Config{MaxConcurrency: 2, Logger: testLogger(t)}, NewMemoryStore())
	st, err := eng.Run(context.Background(), def)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != WFFailed {
		t.Fatalf("workflow expect failed, got %s", st.Status)
	}
	if st.Nodes["a"].Status != StatusFailed {
		t.Fatalf("node a expect failed, got %s", st.Nodes["a"].Status)
	}
	if st.Nodes["b"].Status != StatusSkipped {
		t.Fatalf("node b expect skipped, got %s", st.Nodes["b"].Status)
	}
	if aCalls != 2 {
		t.Fatalf("node a attempts=%d expect 2", aCalls)
	}
	if bCalls != 0 {
		t.Fatalf("skipped node b was executed %d times", bCalls)
	}
	if eng.ExecCount("retry-fail", "b") != 0 {
		t.Fatalf("exec count for skipped b = %d", eng.ExecCount("retry-fail", "b"))
	}
	tlogf("[retry-exhausted] verdict PASS: a failed after 2 attempts, b skipped without execution; wf failed")
}

func TestBackoffDelayMath(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 4, InitialDelay: 100 * time.Millisecond, BackoffFactor: 2, MaxDelay: 300 * time.Millisecond}
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond, 0}
	got := []time.Duration{p.BackoffDelay(1), p.BackoffDelay(2), p.BackoffDelay(3), p.BackoffDelay(4)}
	tlogf("[backoff] attempts delays=%v want=%v", got, want)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delay[%d]=%s want %s", i, got[i], want[i])
		}
	}
}

func TestNodeTimeoutPropagatesAndRetries(t *testing.T) {
	globalLogger = testLogger(t)
	defer func() { globalLogger = nil }()

	var calls int32
	def := &WorkflowDef{ID: "timeout", Nodes: []NodeDef{
		{
			ID:      "slow",
			Timeout: 30 * time.Millisecond,
			Run: func(ctx context.Context, in NodeInput) error {
				atomic.AddInt32(&calls, 1)
				tlogf("[input node=slow attempt=%d] will block on ctx...", in.Attempt)
				<-ctx.Done()
				tlogf("[node=slow attempt=%d] ctx done: %v", in.Attempt, ctx.Err())
				return ctx.Err()
			},
			Retry: RetryPolicy{MaxAttempts: 2, InitialDelay: 5 * time.Millisecond},
		},
	}}
	eng := NewEngine(Config{MaxConcurrency: 1, Logger: testLogger(t)}, NewMemoryStore())
	st, err := eng.Run(context.Background(), def)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != WFFailed || st.Nodes["slow"].Status != StatusFailed {
		t.Fatalf("expect failed, wf=%s node=%s", st.Status, st.Nodes["slow"].Status)
	}
	if calls != 2 {
		t.Fatalf("expect 2 timed-out attempts, got %d", calls)
	}
	if calls2 := eng.ExecCount("timeout", "slow"); calls2 != 2 {
		t.Fatalf("exec count=%d", calls2)
	}
	tlogf("[timeout] verdict PASS: node failed after %d timed-out attempts; deadline propagated", calls)
}

func TestCancellationPropagatesToInFlight(t *testing.T) {
	globalLogger = testLogger(t)
	defer func() { globalLogger = nil }()

	started := make(chan struct{})
	var sawCancel int32
	var mu sync.Mutex
	def := &WorkflowDef{ID: "cancel", Nodes: []NodeDef{
		{ID: "a", Run: func(ctx context.Context, in NodeInput) error {
			tlogf("[input node=a attempt=%d] started, waiting for ctx", in.Attempt)
			close(started)
			<-ctx.Done()
			mu.Lock()
			sawCancel++
			mu.Unlock()
			tlogf("[node=a] observed cancellation: %v", ctx.Err())
			return ctx.Err()
		}},
		{ID: "b", Run: func(ctx context.Context, in NodeInput) error { return nil }},
	}}
	eng := NewEngine(Config{MaxConcurrency: 1, Logger: testLogger(t)}, NewMemoryStore())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var st *WorkflowState
	go func() {
		st, _ = eng.Run(ctx, def)
		close(done)
	}()
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return after cancel")
	}
	if st.Status != WFFailed {
		t.Fatalf("expect wf failed after cancel, got %s", st.Status)
	}
	if st.Nodes["a"].Status != StatusFailed {
		t.Fatalf("in-flight node expect failed, got %s", st.Nodes["a"].Status)
	}
	if st.Nodes["b"].Status != StatusSkipped {
		t.Fatalf("unstarted node expect skipped, got %s", st.Nodes["b"].Status)
	}
	if sawCancel != 1 {
		t.Fatalf("in-flight node did not observe exactly one cancel: %d", sawCancel)
	}
	tlogf("[cancel] verdict PASS: in-flight a failed via ctx, unstarted b skipped; wf failed")
}
