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
	// AttemptLimitExhausted 标记该节点是否因为自身 MaxAttempts 用尽而未再重试。
	// 与 BudgetBlocked 互斥：二者分别对应“自身上限耗尽”与“共享预算被用完”
	// 两种不再重试的原因，节点重新发起尝试时两个标记都会清除，避免旧标记残留。
	AttemptLimitExhausted bool
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
	// RetriesUsed 为已预留（扣减）的共享重试次数账本，仅在 RetryBudget>0 时增长，
	// 与节点 Attempts 一同持久化，中断重启后不丢失、不重置。它用于调度时判断预算
	// 是否用尽；重试在“发起前”预留并落盘，所以进程恰在预留之后、尝试记账之前
	// 崩溃时，该值可能比实际发生的重试多 1。RetryReport.Used 只按实际 Attempts
	// 统计实际重试，不直接采用该账本，保证报告口径始终等于各节点重试次数之和。
	RetriesUsed int
}

// RetryReport 汇总一次运行的重试情况。
//
// 口径（无论是否配置共享重试预算都一致）：
//   - 节点的首次尝试不算重试；同一节点第 2 次及以后的每一次尝试各算 1 次重试；
//   - PerNode[name] = max(Attempts-1, 0)，从头到尾没重试过的节点记为 0；
//   - Used 为本次运行实际发生的重试总数，恒等于 sum(PerNode)；
//     报告按持久化的各节点 Attempts 现算，因此取消后未到终态、或中断重启后，
//     都只统计已经实际发起过的重试，既不回零也不虚增。
//   - BudgetBlocked 与 OwnLimitExhausted 分别列出因两种不同原因不再重试的节点，
//     两者互斥、计数互不混淆（一个节点不可能同时出现在两个列表里）。
type RetryReport struct {
	// Budget 为共享重试总次数；0 表示不限制（旧用法）。
	Budget int
	// Used 为实际发生的重试总数（跨所有节点、跨重启累计），恒等于 PerNode 之和。
	// 注意它不是“共享预算账本”：共享预算的账本是 WorkflowState.RetriesUsed，
	// 预算在重试发起前预留，极端情况下可能比实际重试多 1（预留后进程即崩溃）；
	// Used 只如实反映实际发生的重试。
	Used int
	// PerNode 为每个节点实际重试的次数（Attempts-1，首次尝试不计）；
	// 未发生重试的节点也显式记为 0，避免读取方对“缺键”和“0 次”产生歧义。
	PerNode map[string]int
	// BudgetBlocked 为因共享预算被别的节点先用完而未再重试的节点（按名称排序）。
	BudgetBlocked []string
	// OwnLimitExhausted 为因节点自身 MaxAttempts 用尽而未再重试的节点（按名称排序）。
	OwnLimitExhausted []string
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
