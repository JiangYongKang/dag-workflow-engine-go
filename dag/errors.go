package dag

import "errors"

// 定义阶段与执行阶段的错误分类，保证失败原因可区分。
var (
	// ErrDuplicateNode 工作流定义中存在重复节点。
	ErrDuplicateNode = errors.New("dag: duplicate node in definition")
	// ErrDependencyNotFound 节点依赖指向不存在的节点。
	ErrDependencyNotFound = errors.New("dag: dependency points to missing node")
	// ErrCycleDetected 依赖图中存在环。
	ErrCycleDetected = errors.New("dag: cycle detected in dependency graph")

	// ErrInvalidConfig 引擎配置非法。
	ErrInvalidConfig = errors.New("dag: invalid engine config")
	// ErrAlreadyRunning 工作流已在运行中，禁止重复启动。
	ErrAlreadyRunning = errors.New("dag: workflow already running")
	// ErrWorkflowNotFound 存储中找不到对应工作流状态。
	ErrWorkflowNotFound = errors.New("dag: workflow state not found")
)

// DefinitionError 携带具体节点信息的定义阶段错误。
type DefinitionError struct {
	Kind   error  // ErrDuplicateNode / ErrDependencyNotFound / ErrCycleDetected
	NodeID string // 触发错误的节点（环错误时为环上首个节点）
	Detail string // 人类可读说明
}

func (e *DefinitionError) Error() string {
	return e.Kind.Error() + ": node=" + e.NodeID + " " + e.Detail
}

func (e *DefinitionError) Unwrap() error { return e.Kind }
