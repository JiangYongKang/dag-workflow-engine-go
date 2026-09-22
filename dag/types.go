package dag

import (
	"context"
	"time"
)

// NodeStatus 节点运行状态。
type NodeStatus string

const (
	StatusPending   NodeStatus = "pending"
	StatusRunning   NodeStatus = "running"
	StatusSucceeded NodeStatus = "succeeded"
	StatusFailed    NodeStatus = "failed"  // 重试耗尽后的永久失败
	StatusSkipped   NodeStatus = "skipped" // 上游失败导致不再执行
)

// WorkflowStatus 工作流整体状态。
type WorkflowStatus string

const (
	WFPending   WorkflowStatus = "pending"
	WFRunning   WorkflowStatus = "running"
	WFSucceeded WorkflowStatus = "succeeded"
	WFFailed    WorkflowStatus = "failed"
)

// NodeFunc 节点执行函数。实现方应尊重 ctx 的取消与超时。
type NodeFunc func(ctx context.Context, input NodeInput) error

// NodeInput 传给节点的输入，供日志与判定使用。
type NodeInput struct {
	WorkflowID string
	NodeID     string
	Attempt    int // 从 1 开始的尝试序号
}

// NodeDef 节点定义。
type NodeDef struct {
	ID      string
	Deps    []string
	Run     NodeFunc
	Retry   RetryPolicy
	Timeout time.Duration // 0 表示不设单次执行超时
}

// WorkflowDef 工作流定义。
type WorkflowDef struct {
	ID    string
	Nodes []NodeDef
}

// AttemptRecord 单次尝试的结果记录。
type AttemptRecord struct {
	Attempt   int
	StartedAt time.Time
	EndedAt   time.Time
	Err       string
}

// NodeState 节点持久化状态。
type NodeState struct {
	ID        string
	Status    NodeStatus
	Attempts  int // 已发起的尝试总次数
	Records   []AttemptRecord
	UpdatedAt time.Time
	Error     string
}

// WorkflowState 工作流持久化状态。
type WorkflowState struct {
	ID        string
	Status    WorkflowStatus
	Nodes     map[string]*NodeState
	UpdatedAt time.Time
	Error     string
}

// Clone 返回状态的深拷贝，避免外部并发修改或持久化数据被调用方篡改。
func (s *WorkflowState) Clone() *WorkflowState {
	if s == nil {
		return nil
	}
	cp := &WorkflowState{
		ID:        s.ID,
		Status:    s.Status,
		UpdatedAt: s.UpdatedAt,
		Error:     s.Error,
		Nodes:     make(map[string]*NodeState, len(s.Nodes)),
	}
	for id, ns := range s.Nodes {
		ncp := &NodeState{
			ID:        ns.ID,
			Status:    ns.Status,
			Attempts:  ns.Attempts,
			UpdatedAt: ns.UpdatedAt,
			Error:     ns.Error,
			Records:   append([]AttemptRecord(nil), ns.Records...),
		}
		cp.Nodes[id] = ncp
	}
	return cp
}
