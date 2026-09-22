package workflow

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Engine 按定义与配置调度工作流，状态通过 Store 持久化以支持断点恢复。
type Engine struct {
	name     string
	nodes    []NodeDef
	registry TaskRegistry
	config   Config
	store    Store

	mu    sync.Mutex // 保护 state 与所有调度决策
	state *WorkflowState
	deps  map[string][]string
	logf  func(format string, args ...any)
}

// NewEngine 校验定义并创建引擎。定义非法（空名/重复/缺失依赖/环）时返回可区分的错误。
func NewEngine(name string, nodes []NodeDef, registry TaskRegistry, config Config, store Store) (*Engine, error) {
	if _, err := Validate(nodes); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if registry == nil {
		registry = TaskRegistry{}
	}
	if config.MaxConcurrency <= 0 {
		config.MaxConcurrency = 1
	}
	deps := make(map[string][]string, len(nodes))
	for _, n := range nodes {
		deps[n.Name] = append([]string(nil), n.DependsOn...)
	}
	return &Engine{
		name:     name,
		nodes:    nodes,
		registry: registry,
		config:   config,
		store:    store,
		deps:     deps,
		logf:     func(string, ...any) {},
	}, nil
}

// WithLogger 注入日志函数（测试中用于打印输入与判定依据）。
func (e *Engine) WithLogger(f func(format string, args ...any)) *Engine {
	if f != nil {
		e.logf = f
	}
	return e
}

// Run 执行（或恢复执行）工作流。
//
// 调度模型：节点在其全部依赖进入终态后才被考虑；所有依赖成功才可执行，
// 任一依赖永久失败/被跳过时本节点标记为 skipped。运行态节点数受 MaxConcurrency 限制。
//
// 恢复语义：启动时加载 Store 中的状态；已成功节点不重复执行；失败节点沿用
// Attempts 计数按重试策略决定是否继续尝试；running 节点视为上次进程中断的残留，
// 按失败节点处理。
func (e *Engine) Run(ctx context.Context) (*RunResult, error) {
	saved, err := e.store.Load()
	if err != nil {
		return nil, fmt.Errorf("load workflow state: %w", err)
	}

	e.mu.Lock()
	if saved != nil && saved.WorkflowName == e.name {
		e.state = e.reconcileState(saved)
	} else {
		e.state = newState(e.name, e.nodes)
	}
	resumed := saved != nil && saved.WorkflowName == e.name
	if err := e.store.Save(e.state); err != nil {
		e.mu.Unlock()
		return nil, fmt.Errorf("save initial state: %w", err)
	}
	e.mu.Unlock()

	e.logf("[engine] workflow=%q start resumed=%v concurrency=%d", e.name, resumed, e.config.MaxConcurrency)

	sem := make(chan struct{}, e.config.MaxConcurrency)
	var wg sync.WaitGroup
	var inflight int32
	wake := make(chan struct{}, 1)
	signal := func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			e.logf("[engine] context done, waiting in-flight tasks: %v", err)
			wg.Wait()
			return e.result(resumed), nil
		}

		e.mu.Lock()
		e.propagateLocked()
		if e.state.Finished {
			e.mu.Unlock()
			break
		}
		ready := e.pickReadyLocked()
		e.mu.Unlock()

		for _, name := range ready {
			wg.Add(1)
			atomic.AddInt32(&inflight, 1)
			go e.executeNode(ctx, name, &wg, sem, func() {
				atomic.AddInt32(&inflight, -1)
				signal()
			})
		}
		if len(ready) > 0 {
			continue // 立即重新评估，尽量填满并发额度
		}

		if atomic.LoadInt32(&inflight) == 0 {
			// 无在途且无可调度：全部终态。
			e.mu.Lock()
			e.propagateLocked()
			e.mu.Unlock()
			break
		}

		// 等待任一 worker 完成后再重新评估（事件驱动，不必等整批）。
		select {
		case <-wake:
		case <-ctx.Done():
			wg.Wait()
			return e.result(resumed), nil
		}
	}
	wg.Wait()
	return e.result(resumed), nil
}

// reconcileState 校验并规整加载到的历史状态。
func (e *Engine) reconcileState(saved *WorkflowState) *WorkflowState {
	st := newState(e.name, e.nodes)
	for _, n := range e.nodes {
		old, ok := saved.Nodes[n.Name]
		if !ok {
			continue // 定义中新增的节点保持 pending
		}
		cp := *old
		switch cp.Status {
		case StatusSucceeded:
			// 已成功：保持终态，绝不重复执行。
		case StatusFailed:
			// 重试预算耗尽才是永久失败；预算未用尽（如上次被取消/超时中断）则回到 pending 继续重试。
			if cp.Attempts < normalizeRetry(e.config.DefaultRetry).MaxAttempts {
				cp.Status = StatusPending
			}
		case StatusSkipped:
			// 被上游失败连带跳过的节点保持终态。
		case StatusRunning:
			// 上次进程中断时在途的节点：未落任何终态，按失败处理等待重试。
			cp.Status = StatusPending
			cp.LastError = "interrupted while running"
		default:
			cp.Status = StatusPending
		}
		st.Nodes[n.Name] = &cp
	}
	return st
}

// propagateLocked 在锁内把依赖终态向前传播：可重试的 pending 节点保持不动，
// 依赖永久失败的节点标记 skipped，全部完成后结算工作流整体成败。
func (e *Engine) propagateLocked() {
	changed := true
	for changed {
		changed = false
		for _, n := range e.nodes {
			st := e.state.Nodes[n.Name]
			if st.Status != StatusPending {
				continue
			}
			allDone := true
			for _, d := range n.DependsOn {
				ds := e.state.Nodes[d]
				switch ds.Status {
				case StatusSucceeded:
				case StatusFailed, StatusSkipped:
					st.Status = StatusSkipped
					changed = true
					e.logf("[skip] node=%q because dependency=%q status=%s", n.Name, d, ds.Status)
				default:
					allDone = false
				}
			}
			if allDone && st.Status == StatusPending {
				// 依赖全部成功（或无依赖）：保持 pending 等待调度。
			}
		}
		if changed {
			_ = e.store.Save(e.state)
		}
	}

	allTerminal := true
	anyFailure := false
	for _, n := range e.nodes {
		s := e.state.Nodes[n.Name].Status
		if s != StatusSucceeded && s != StatusFailed && s != StatusSkipped {
			allTerminal = false
		}
		if s == StatusFailed || s == StatusSkipped {
			anyFailure = true
		}
	}
	if allTerminal && !e.state.Finished {
		e.state.Finished = true
		e.state.Succeeded = !anyFailure
		e.logf("[engine] workflow finished succeeded=%v", e.state.Succeeded)
		_ = e.store.Save(e.state)
	}
}

// pickReadyLocked 在锁内选出当前可立即调度的节点（按定义顺序，结果确定）。
// 每个节点在整个生命周期内只会被选中一次。
func (e *Engine) pickReadyLocked() []string {
	var ready []string
	for _, n := range e.nodes {
		st := e.state.Nodes[n.Name]
		if st.Status != StatusPending {
			continue
		}
		ok := true
		for _, d := range n.DependsOn {
			if e.state.Nodes[d].Status != StatusSucceeded {
				ok = false
				break
			}
		}
		if ok {
			// 标记 running 占位，保证该节点不会被重复调度（并发状态冲突防护）。
			st.Status = StatusRunning
			ready = append(ready, n.Name)
		}
	}
	if len(ready) > 0 {
		_ = e.store.Save(e.state)
		for _, name := range ready {
			e.logf("[schedule] node=%q attempts=%d", name, e.state.Nodes[name].Attempts)
		}
	}
	return ready
}

// executeNode 在独立 goroutine 中执行节点，通过回调串行化落盘。
func (e *Engine) executeNode(ctx context.Context, name string, wg *sync.WaitGroup, sem chan struct{}, onExit func()) {
	defer wg.Done()
	defer onExit()
	// 获取并发额度；等待期间也要响应取消，避免 ctx 已取消时空占调度槽位。
	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	case <-ctx.Done():
		e.mu.Lock()
		if st := e.state.Nodes[name]; st.Status == StatusRunning {
			st.Status = StatusPending
			st.LastError = ctx.Err().Error()
			_ = e.store.Save(e.state)
		}
		e.mu.Unlock()
		return
	}

	var def NodeDef
	for _, n := range e.nodes {
		if n.Name == name {
			def = n
			break
		}
	}
	task, ok := e.registry[name]
	if !ok {
		task = func(context.Context, string, int) error {
			return &MissingTaskError{Node: name}
		}
	}

	e.mu.Lock()
	st := e.state.Nodes[name]
	snapshot := *st
	e.mu.Unlock()

	e.logf("[run] node=%q attempt=%d timeout=%s input-deps=%v",
		name, snapshot.Attempts+1, def.Timeout, def.DependsOn)

	runNode(ctx, def, task, e.config.DefaultRetry, &snapshot, func(updated *NodeState) {
		e.mu.Lock()
		// 字段拷贝进既有条目：绝不能替换 map 条目，否则会覆盖调度器设置的
		// running 占位，造成节点状态回退与重复调度。
		*e.state.Nodes[name] = *updated
		err := e.store.Save(e.state)
		e.mu.Unlock()
		if err != nil {
			e.logf("[warn] persist node=%q: %v", name, err)
		}
	})
	e.logf("[done] node=%q final=%s attempts=%d error=%q",
		name, snapshot.Status, snapshot.Attempts, snapshot.LastError)
}

func (e *Engine) result(resumed bool) *RunResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	return &RunResult{Succeeded: e.state.Succeeded, Resumed: resumed, State: e.state}
}
