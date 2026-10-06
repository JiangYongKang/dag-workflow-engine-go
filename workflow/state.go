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

// RetryReport 汇总本次运行的重试情况，口径与 RetryReport 类型注释一致：
//   - 每节点重试次数 = max(Attempts-1, 0)（首次尝试不算重试），未重试节点显式记 0；
//   - Used 直接对各节点重试次数求和——这是“实际发生了多少次重试”的唯一口径，
//     无论是否配置共享预算（RetryBudget==0 时预算账本 RetriesUsed 恒为 0，
//     若直接读账本就会出现“每节点非 0、总数却是 0”的自相矛盾）；
//   - 数据全部来自持久化的各节点 Attempts，因此取消后出报告、或中断重启后再出报告，
//     已发生的重试不回零、未发生的重试不虚增；
//   - BudgetBlocked / OwnLimitExhausted 按名称排序返回，保证报告结果确定。
func (s *WorkflowState) RetryReport() RetryReport {
	r := RetryReport{
		Budget:  s.RetryBudget,
		PerNode: make(map[string]int, len(s.Nodes)),
	}
	// 固定遍历顺序仅用于确定性日志；map 本身的写入顺序不影响结果。
	names := make([]string, 0, len(s.Nodes))
	for name := range s.Nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		n := s.Nodes[name]
		retries := n.Attempts - 1
		if retries < 0 {
			retries = 0
		}
		r.PerNode[name] = retries
		r.Used += retries
		if n.BudgetBlocked {
			r.BudgetBlocked = append(r.BudgetBlocked, name)
		}
		if n.AttemptLimitExhausted {
			r.OwnLimitExhausted = append(r.OwnLimitExhausted, name)
		}
	}
	sort.Strings(r.BudgetBlocked)
	sort.Strings(r.OwnLimitExhausted)
	return r
}
