package workflow

import (
	"context"
	"time"
)

// Status 表示节点或工作流的运行状态。
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusSkipped   Status = "skipped" // 上游永久失败，本节点不会执行
)

// NodeDef 是工作流定义中的一个节点。
type NodeDef struct {
	Name      string
	DependsOn []string
	Timeout   time.Duration // <=0 表示不限时
}

// RetryPolicy 描述单节点的重试策略。
type RetryPolicy struct {
	MaxAttempts    int           // 总尝试次数上限（含首次），<=0 表示 1 次
	InitialBackoff time.Duration // 首次退避
	MaxBackoff     time.Duration // 退避上限
	Factor         float64       // 退避倍率，<=1 表示不增长
}

// Config 是引擎级配置。
type Config struct {
	MaxConcurrency int // 整体并发度上限，<=0 表示 1
	DefaultRetry   RetryPolicy
}

// TaskFunc 是节点任务体。
type TaskFunc func(ctx context.Context, node string, attempt int) error

// TaskRegistry 保存节点名到任务体的映射。
type TaskRegistry map[string]TaskFunc

// NodeState 是单个节点的持久化状态。
type NodeState struct {
	Status     Status
	Attempts   int    // 已发起的尝试次数
	LastError  string // 最近一次错误
	StartedAt  time.Time
	FinishedAt time.Time
}

// WorkflowState 是整个工作流的持久化状态。
type WorkflowState struct {
	WorkflowName string
	Nodes        map[string]*NodeState
	Finished     bool
	Succeeded    bool
	UpdatedAt    time.Time
}

// RunResult 是一次 Run 调用的最终结果。
type RunResult struct {
	Succeeded bool
	State     *WorkflowState
	Resumed   bool // 本次是否基于已有状态恢复
}

// Store 抽象运行状态的持久化介质。
type Store interface {
	Load() (*WorkflowState, error)
	Save(state *WorkflowState) error
	Clear() error
}
