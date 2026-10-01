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

// 永久失败原因归类，写入 NodeState.FailReason，空串表示尚未永久失败。
const (
	// FailReasonAttemptsExhausted：节点自身重试策略的 MaxAttempts 已用尽。
	FailReasonAttemptsExhausted = "attempts exhausted"
	// FailReasonBudgetExhausted：全局共享重试预算已被其他节点先扣完，本节点因此不再重试。
	FailReasonBudgetExhausted = "retry budget exhausted"
)

// NodeDef 是工作流定义中的一个节点。
type NodeDef struct {
	Name      string
	DependsOn []string
	Timeout   time.Duration // <=0 表示不限时
	// Priority 为节点调度优先级，数值越大优先级越高；默认为 0。
	// 仅在并发槽位不足以同时运行所有就绪节点时影响挑选顺序。
	Priority int
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
	// MaxRetryBudget 是一次运行跨所有节点共享的重试总次数（只计首次之后的额外尝试）。
	// <=0 表示不设统一预算，各节点仅受自身 RetryPolicy 约束（旧用法）。
	// >0 时重试机会全局先到先得：耗尽后仍失败的节点直接按重试耗尽永久失败处理。
	MaxRetryBudget int
}

// TaskFunc 是节点任务体。
type TaskFunc func(ctx context.Context, node string, attempt int) error

// TaskRegistry 保存节点名到任务体的映射。
type TaskRegistry map[string]TaskFunc

// NodeState 是单个节点的持久化状态。
type NodeState struct {
	Status     Status
	Attempts   int    // 已发起的尝试次数
	Retries    int    // 已消耗的重试次数（Attempts-1，含历史进程）
	LastError  string // 最近一次错误
	FailReason string // 永久失败原因归类（如节点策略耗尽 / 全局重试预算耗尽）
	StartedAt  time.Time
	FinishedAt time.Time
}

// WorkflowState 是整个工作流的持久化状态。
type WorkflowState struct {
	WorkflowName string
	Nodes        map[string]*NodeState
	Finished     bool
	Succeeded    bool
	// RetriesUsed 是本次运行跨所有节点已消耗的共享重试次数；
	// MaxRetryBudget<=0 时仍按实际重尝试数累计，便于事后统计。
	RetriesUsed int
	// ScheduleOrder 记录节点被选中进入 running 的先后顺序（跨重启追加，不重复），
	// 用于让调度决策可复核、可复现。
	ScheduleOrder []string
	UpdatedAt     time.Time
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
