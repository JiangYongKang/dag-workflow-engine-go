package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// FileStore 以 JSON 文件作为持久化介质。
// Save 先写同目录临时文件再 rename，保证状态文件要么是旧版本要么是新版本，不会出现半截内容。
type FileStore struct {
	Path string
}

func NewFileStore(path string) *FileStore {
	return &FileStore{Path: path}
}

// Load 读取已保存状态；文件不存在时返回 (nil, nil) 表示尚无历史状态。
func (s *FileStore) Load() (*WorkflowState, error) {
	data, err := os.ReadFile(s.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("load state from %q: %w", s.Path, err)
	}
	var st WorkflowState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("parse state file %q: %w", s.Path, err)
	}
	return &st, nil
}

// Save 原子地写入状态。
func (s *FileStore) Save(state *WorkflowState) error {
	state.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	dir := filepath.Dir(s.Path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create state dir %q: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".workflow-state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp state file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // rename 成功后 Remove 一个不存在的文件无副作用
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp state file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("fsync temp state file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp state file: %w", err)
	}
	if err := os.Rename(tmpName, s.Path); err != nil {
		return fmt.Errorf("replace state file: %w", err)
	}
	return nil
}

// Clear 删除状态文件（文件不存在不视为错误）。
func (s *FileStore) Clear() error {
	if err := os.Remove(s.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("clear state file %q: %w", s.Path, err)
	}
	return nil
}

// newState 依据定义构建初始工作流状态。
func newState(name string, nodes []NodeDef) *WorkflowState {
	st := &WorkflowState{
		WorkflowName: name,
		Nodes:        make(map[string]*NodeState, len(nodes)),
	}
	for _, n := range nodes {
		st.Nodes[n.Name] = &NodeState{Status: StatusPending, Attempts: 0}
	}
	return st
}

// nodeRetries 返回某节点已实际发生的重试次数：首次尝试不计，
// 此后每发起一次尝试（Attempts 的第 2 次及以后）计 1 次重试。
// 退避等待期间被取消、第 2 次尝试尚未发起时不计。
func nodeRetries(n *NodeState) int {
	if n.Attempts > 1 {
		return n.Attempts - 1
	}
	return 0
}

// totalRetries 按各节点 Attempts 汇总整次运行已发生的重试次数。
// 它是重试计数的唯一事实来源：报告总数、预算扣账闸门、重启规整都以它为准，
// 因此“总数 == 各节点重试次数之和”在任何时刻（含取消后未到终态）都成立，
// 与是否配置共享重试预算无关。
func (s *WorkflowState) totalRetries() int {
	total := 0
	for _, n := range s.Nodes {
		total += nodeRetries(n)
	}
	return total
}

// RetryReport 汇总一次运行的重试情况：
//   - PerNode 逐个节点列出已发生的重试次数 max(Attempts-1, 0)，从未重试的节点显式记 0；
//   - Used 为各节点重试次数之和，无论是否配置共享预算都如实统计；
//   - BudgetBlocked 为因共享预算耗尽而未再重试的节点，按名称排序，
//     与“节点自身 MaxAttempts 耗尽”（failed 且 BudgetBlocked=false）严格区分。
func (s *WorkflowState) RetryReport() RetryReport {
	r := RetryReport{
		Budget:  s.RetryBudget,
		PerNode: make(map[string]int, len(s.Nodes)),
	}
	for name, n := range s.Nodes {
		retries := nodeRetries(n)
		// 每个节点都显式落项：从头到尾没重试过的节点记 0，不允许缺席导致对不上。
		r.PerNode[name] = retries
		// 总数直接由各节点计数求和，不另取可能漂移的计数器。
		r.Used += retries
		if n.BudgetBlocked {
			r.BudgetBlocked = append(r.BudgetBlocked, name)
		}
	}
	sort.Strings(r.BudgetBlocked)
	return r
}
