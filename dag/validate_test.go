package dag

import (
	"context"
	"errors"
	"log"
	"os"
	"testing"
)

// testLogger 在单测中打印调度输入与判定依据。
func testLogger(t *testing.T) Logger {
	return log.New(os.Stdout, "  ["+t.Name()+"] ", log.LstdFlags|log.Lmicroseconds)
}

func okNode(id string, deps ...string) NodeDef {
	return NodeDef{
		ID:   id,
		Deps: deps,
		Run: func(ctx context.Context, in NodeInput) error {
			tlogf("[input wf=%s node=%s attempt=%d] -> success", in.WorkflowID, in.NodeID, in.Attempt)
			return nil
		},
	}
}

var globalLogger Logger

func tlogf(format string, args ...any) {
	if globalLogger != nil {
		globalLogger.Printf(format, args...)
	}
}

func TestValidateRejectsDuplicateNode(t *testing.T) {
	globalLogger = testLogger(t)
	defer func() { globalLogger = nil }()

	def := &WorkflowDef{ID: "dup", Nodes: []NodeDef{
		okNode("a"),
		okNode("a"),
	}}
	err := Validate(def)
	tlogf("[validate duplicate] input nodes=a,a err=%v", err)
	if !errors.Is(err, ErrDuplicateNode) {
		t.Fatalf("expect ErrDuplicateNode, got %v", err)
	}
	var de *DefinitionError
	if !errors.As(err, &de) || de.NodeID != "a" {
		t.Fatalf("expect DefinitionError node=a, got %#v", err)
	}
}

func TestValidateRejectsMissingDependency(t *testing.T) {
	globalLogger = testLogger(t)
	defer func() { globalLogger = nil }()

	def := &WorkflowDef{ID: "miss", Nodes: []NodeDef{
		okNode("a", "ghost"),
	}}
	err := Validate(def)
	tlogf("[validate missing-dep] input a->ghost err=%v", err)
	if !errors.Is(err, ErrDependencyNotFound) {
		t.Fatalf("expect ErrDependencyNotFound, got %v", err)
	}
}

func TestValidateRejectsCycle(t *testing.T) {
	globalLogger = testLogger(t)
	defer func() { globalLogger = nil }()

	def := &WorkflowDef{ID: "cyc", Nodes: []NodeDef{
		okNode("a", "c"),
		okNode("b", "a"),
		okNode("c", "b"),
	}}
	err := Validate(def)
	tlogf("[validate cycle] input a->c->b->a err=%v", err)
	if !errors.Is(err, ErrCycleDetected) {
		t.Fatalf("expect ErrCycleDetected, got %v", err)
	}
}

func TestValidateRejectsSelfCycle(t *testing.T) {
	globalLogger = testLogger(t)
	defer func() { globalLogger = nil }()

	def := &WorkflowDef{ID: "self", Nodes: []NodeDef{
		okNode("a", "a"),
	}}
	if err := Validate(def); !errors.Is(err, ErrCycleDetected) {
		t.Fatalf("expect ErrCycleDetected for self dependency, got %v", err)
	}
}

func TestErrorKindsAreDistinguishable(t *testing.T) {
	cases := map[error]error{
		ErrDuplicateNode:      ErrCycleDetected,
		ErrDependencyNotFound: ErrDuplicateNode,
		ErrCycleDetected:      ErrDependencyNotFound,
	}
	for err, other := range cases {
		if errors.Is(err, other) {
			t.Fatalf("%v must not match %v", err, other)
		}
	}
}

func TestTopoLevelsOrdering(t *testing.T) {
	globalLogger = testLogger(t)
	defer func() { globalLogger = nil }()

	def := &WorkflowDef{ID: "topo", Nodes: []NodeDef{
		okNode("a"),
		okNode("b", "a"),
		okNode("c", "a"),
		okNode("d", "b", "c"),
	}}
	levels, err := TopoLevels(def)
	if err != nil {
		t.Fatal(err)
	}
	tlogf("[topo] levels=%v", levels)
	if len(levels) != 3 || levels[0][0] != "a" || levels[2][0] != "d" {
		t.Fatalf("unexpected levels: %v", levels)
	}
}
