package dag

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Store 是工作流状态的持久化抽象。
// Save 必须是原子的：崩溃恢复时要么读到旧完整状态，要么读到新完整状态。
type Store interface {
	// Save 原子保存工作流状态（存储内部应持有深拷贝语义）。
	Save(state *WorkflowState) error
	// Load 读取工作流状态；不存在返回 ErrWorkflowNotFound。
	Load(id string) (*WorkflowState, error)
}

// MemoryStore 进程内存储，默认引擎与测试使用。
type MemoryStore struct {
	mu     sync.RWMutex
	states map[string]*WorkflowState
}

// NewMemoryStore 创建内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{states: make(map[string]*WorkflowState)}
}

func (s *MemoryStore) Save(state *WorkflowState) error {
	if state == nil {
		return fmt.Errorf("%w: nil state", ErrInvalidConfig)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[state.ID] = state.Clone()
	return nil
}

func (s *MemoryStore) Load(id string) (*WorkflowState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.states[id]
	if !ok {
		return nil, fmt.Errorf("%w: id=%s", ErrWorkflowNotFound, id)
	}
	return st.Clone(), nil
}

// FileStore 基于 JSON 文件的持久化实现，采用"临时文件 + rename"原子提交，
// 供进程重启后的断点恢复使用。
type FileStore struct {
	dir string
	mu  sync.Mutex
}

// NewFileStore 准备目录 dir（不存在则创建）用于存放状态文件。
func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: empty store dir", ErrInvalidConfig)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) path(id string) string {
	return filepath.Join(s.dir, id+".json")
}

func (s *FileStore) Save(state *WorkflowState) error {
	if state == nil {
		return fmt.Errorf("%w: nil state", ErrInvalidConfig)
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	final := s.path(state.ID)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write temp state: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("commit state: %w", err)
	}
	return nil
}

func (s *FileStore) Load(id string) (*WorkflowState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path(id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: id=%s", ErrWorkflowNotFound, id)
		}
		return nil, fmt.Errorf("read state: %w", err)
	}
	var st WorkflowState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("corrupt state file: %w", err)
	}
	if st.Nodes == nil {
		st.Nodes = make(map[string]*NodeState)
	}
	return &st, nil
}
