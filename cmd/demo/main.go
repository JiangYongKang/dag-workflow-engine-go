// demo 演示本地工作流引擎：flaky 节点前两次失败第三次成功，状态持久化到文件。
// 连续运行多次可观察断点恢复语义。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/highcumontoa/dag-workflow-engine-go/workflow"
)

func main() {
	statePath := flag.String("state", ".workflow-demo-state.json", "状态文件路径")
	flag.Parse()

	nodes := []workflow.NodeDef{
		{Name: "extract"},
		{Name: "transform", DependsOn: []string{"extract"}, Timeout: 2 * time.Second},
		{Name: "load", DependsOn: []string{"transform"}},
	}

	var flakyAttempts int32
	registry := workflow.TaskRegistry{
		"extract": func(_ context.Context, node string, attempt int) error {
			fmt.Printf("  -> run %s (attempt %d)\n", node, attempt)
			return nil
		},
		"transform": func(ctx context.Context, node string, attempt int) error {
			n := atomic.AddInt32(&flakyAttempts, 1)
			fmt.Printf("  -> run %s (attempt %d)\n", node, attempt)
			if n < 3 {
				return fmt.Errorf("%s: simulated failure #%d", node, n)
			}
			return nil
		},
		"load": func(_ context.Context, node string, attempt int) error {
			fmt.Printf("  -> run %s (attempt %d)\n", node, attempt)
			return nil
		},
	}

	cfg := workflow.Config{
		MaxConcurrency: 2,
		DefaultRetry: workflow.RetryPolicy{
			MaxAttempts:    5,
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     100 * time.Millisecond,
			Factor:         2,
		},
	}

	engine, err := workflow.NewEngine("etl-demo", nodes, registry, cfg, workflow.NewFileStore(*statePath))
	if err != nil {
		log.Fatalf("invalid workflow: %v", err)
	}
	engine.WithLogger(func(format string, args ...any) {
		log.Printf(format, args...)
	})

	result, err := engine.Run(context.Background())
	if err != nil {
		log.Fatalf("run error: %v", err)
	}
	fmt.Printf("succeeded=%v resumed=%v\n", result.Succeeded, result.Resumed)
	if !result.Succeeded {
		os.Exit(1)
	}
}
