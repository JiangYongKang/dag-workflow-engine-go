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

// RetryReport 汇总共享重试预算的使用情况：
// 每个节点消耗的重试次数为 max(Attempts-1, 0)（首次尝试不扣预算）；
// BudgetBlocked 节点按名称排序返回，保证报告结果确定。
func (s *WorkflowState) RetryReport() RetryReport {
	r := RetryReport{
		Budget:  s.RetryBudget,
		Used:    s.RetriesUsed,
		PerNode: make(map[string]int, len(s.Nodes)),
	}
	for name, n := range s.Nodes {
		if n.Attempts > 1 {
			r.PerNode[name] = n.Attempts - 1
		}
		if n.BudgetBlocked {
			r.BudgetBlocked = append(r.BudgetBlocked, name)
		}
	}
	sort.Strings(r.BudgetBlocked)
	return r
}
