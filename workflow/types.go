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
	// Priority 为调度优先级，数值越大越优先；并发槽位不足时，已就绪节点按
	// Priority 降序挑选。同优先级按定义顺序挑选，因此同一定义配同一状态的
	// 调度顺序确定可复现。零值 0 为默认优先级，旧定义不填时行为与以前一致。
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
	// MaxRetries 是一次运行内跨所有节点共享的“重试”总次数：每个节点的首次
	// 尝试不扣预算，此后每发起一次重试扣 1，先到先得；预算扣完后仍失败的
	// 节点直接按重试耗尽永久失败处理（NodeState.BudgetBlocked=true）。
	// <=0 表示不启用共享预算，仅受各节点 MaxAttempts 约束（旧用法行为不变）。
	MaxRetries int
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
	// BudgetBlocked 标记该节点是否因为运行级共享重试预算耗尽而未再重试，
	// 用于事后区分“本节点 MaxAttempts 耗尽”与“全局预算用完”。
	BudgetBlocked bool
}

// WorkflowState 是整个工作流的持久化状态。
type WorkflowState struct {
	WorkflowName string
	Nodes        map[string]*NodeState
	Finished     bool
	Succeeded    bool
	UpdatedAt    time.Time
	// RetryBudget 为本次运行的共享重试总次数（首次启动时取自 Config.MaxRetries，
	// 恢复时沿用持久化值）；0 表示不启用共享预算。
	RetryBudget int
	// RetriesUsed 为已消耗的共享重试次数，与节点 Attempts 一同持久化，
	// 中断重启后不丢失、不重置。
	RetriesUsed int
}

// RetryReport 汇总一次运行的共享重试预算使用情况。
type RetryReport struct {
	Budget        int            // 共享重试总次数；0 表示不限制
	Used          int            // 已消耗的重试次数（跨所有节点）
	PerNode       map[string]int // 每个节点消耗的重试次数（首次尝试不计）
	BudgetBlocked []string       // 因共享预算耗尽而未再重试的节点（按名称排序）
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
