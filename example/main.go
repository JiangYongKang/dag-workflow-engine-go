// 命令行示例：用文件存储跑一个含失败重试节点的 DAG，
// 删掉状态目录可模拟全新运行；保留目录再次启动可观察断点恢复（终态幂等）。
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/highcumontoa/dag-workflow-engine-go/dag"
)

func main() {
	dir := ".workflow-state"
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	store, err := dag.NewFileStore(dir)
	if err != nil {
		log.Fatal(err)
	}

	var flakyCalls int32
	def := &dag.WorkflowDef{
		ID: "example-etl",
		Nodes: []dag.NodeDef{
			{ID: "extract", Run: succeed("extract")},
			{ID: "transform-a", Deps: []string{"extract"}, Run: succeed("transform-a")},
			{ID: "transform-b", Deps: []string{"extract"}, Run: succeed("transform-b")},
			{
				ID:    "flaky-load",
				Deps:  []string{"transform-a", "transform-b"},
				Run:   flakyNode(&flakyCalls),
				Retry: dag.RetryPolicy{MaxAttempts: 3, InitialDelay: 100 * time.Millisecond, BackoffFactor: 2, MaxDelay: time.Second},
			},
		},
	}

	eng := dag.NewEngine(dag.Config{
		MaxConcurrency: 2,
		DefaultTimeout: 5 * time.Second,
		Logger:         log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds),
	}, store)

	st, err := eng.Run(context.Background(), def)
	if err != nil {
		log.Fatalf("engine aborted: %v", err)
	}
	fmt.Printf("\n==== workflow %s finished: %s (%s) ====\n", st.ID, st.Status, st.Error)
	for _, id := range []string{"extract", "transform-a", "transform-b", "flaky-load"} {
		ns := st.Nodes[id]
		fmt.Printf("node %-12s status=%-10s attempts=%d\n", id, ns.Status, ns.Attempts)
	}
}

func succeed(id string) dag.NodeFunc {
	return func(ctx context.Context, in dag.NodeInput) error {
		fmt.Printf("  -> run %s (attempt %d)\n", id, in.Attempt)
		return nil
	}
}

func flakyNode(calls *int32) dag.NodeFunc {
	return func(ctx context.Context, in dag.NodeInput) error {
		n := atomic.AddInt32(calls, 1)
		fmt.Printf("  -> run flaky-load (attempt %d)\n", n)
		if n < 2 {
			return errors.New("temporary load failure")
		}
		return nil
	}
}
