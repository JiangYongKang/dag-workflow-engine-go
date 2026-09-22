package workflow

import (
	"errors"
	"fmt"
)

// 以下错误类型用于可区分的失败原因归类。

// DuplicateNodeError 表示工作流定义中出现重复节点。
type DuplicateNodeError struct {
	Node string
}

func (e *DuplicateNodeError) Error() string {
	return fmt.Sprintf("duplicate node: %q", e.Node)
}

// MissingDependencyError 表示依赖指向不存在的节点。
type MissingDependencyError struct {
	Node string
	Dep  string
}

func (e *MissingDependencyError) Error() string {
	return fmt.Sprintf("node %q depends on missing node %q", e.Node, e.Dep)
}

// CycleError 表示依赖图中存在环。
type CycleError struct {
	Cycle []string
}

func (e *CycleError) Error() string {
	return fmt.Sprintf("cyclic dependency detected: %v", e.Cycle)
}

// EmptyNameError 表示节点名为空。
type EmptyNameError struct {
	Index int
}

func (e *EmptyNameError) Error() string {
	return fmt.Sprintf("node at index %d has empty name", e.Index)
}

// MissingTaskError 表示节点缺少已注册的任务体。
type MissingTaskError struct {
	Node string
}

func (e *MissingTaskError) Error() string {
	return fmt.Sprintf("no task registered for node %q", e.Node)
}

// AsCycleError 提取环错误。
func AsCycleError(err error) (*CycleError, bool) {
	var e *CycleError
	ok := errors.As(err, &e)
	return e, ok
}

// AsDuplicateNodeError 提取重复节点错误。
func AsDuplicateNodeError(err error) (*DuplicateNodeError, bool) {
	var e *DuplicateNodeError
	ok := errors.As(err, &e)
	return e, ok
}

// AsMissingDependencyError 提取缺失依赖错误。
func AsMissingDependencyError(err error) (*MissingDependencyError, bool) {
	var e *MissingDependencyError
	ok := errors.As(err, &e)
	return e, ok
}
