package workflow

import (
	"context"
	"fmt"
	"sort"
	"sync"
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
// 有空槽时，就绪节点按 NodeDef.Priority 降序挑选，同优先级按定义顺序挑选，
// 保证同一定义配同一状态的调度轨迹确定可复现。
//
// 共享重试预算：Config.MaxRetries>0 时，所有节点的首次尝试之外的重试共用该预算，
// 先到先扣；预算耗尽后等待重试的节点立即永久失败并标记 BudgetBlocked。
// 预算总量与已消耗量持久化在 WorkflowState 中，恢复时沿用，不重置。
//
// 恢复语义：启动时加载 Store 中的状态；已成功节点不重复执行；失败节点沿用
// Attempts 计数按重试策略与共享预算决定是否继续尝试；running 节点视为上次进程
// 中断的残留，按失败节点处理。
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
		e.state.RetryBudget = e.config.MaxRetries
	}
	resumed := saved != nil && saved.WorkflowName == e.name
	if err := e.store.Save(e.state); err != nil {
		e.mu.Unlock()
		return nil, fmt.Errorf("save initial state: %w", err)
	}
	e.mu.Unlock()

	e.logf("[engine] workflow=%q start resumed=%v concurrency=%d retryBudget=%d",
		e.name, resumed, e.config.MaxConcurrency, e.state.RetryBudget)

	var wg sync.WaitGroup
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
		// 只按“当前空闲的槽位数”挑选节点：高优先级就绪节点一旦出现，
		// 下一个空出的槽位一定先给它，而不是被早已排队的低优先级任务占走。
		free := e.config.MaxConcurrency - e.inflightLocked()
		var ready []string
		if free > 0 {
			ready = e.pickReadyLocked(free)
		}
		e.mu.Unlock()

		for _, name := range ready {
			wg.Add(1)
			go e.executeNode(ctx, name, &wg, func() {
				signal()
			})
		}
		if len(ready) > 0 {
			continue // 立即重新评估，尽量填满并发额度
		}

		e.mu.Lock()
		anyInflight := e.inflightLocked() > 0
		e.mu.Unlock()
		if !anyInflight {
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

// inflightLocked 返回当前处于 running 占位的节点数（调用方持锁）。
func (e *Engine) inflightLocked() int {
	n := 0
	for _, st := range e.state.Nodes {
		if st.Status == StatusRunning {
			n++
		}
	}
	return n
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
			// 永久失败的两种情况都必须在重启后保持终态：
			//  1. 节点自身 MaxAttempts 耗尽（cp.Attempts >= MaxAttempts）；
			//  2. 运行级共享重试预算耗尽（cp.BudgetBlocked）。
			// 仅上次被取消/超时中断且预算未用尽的节点才回到 pending 继续重试。
			if !cp.BudgetBlocked && cp.Attempts < normalizeRetry(e.config.DefaultRetry).MaxAttempts {
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
	// 共享预算属于一次运行：恢复时沿用已持久化的预算总量，绝不重置。
	st.RetryBudget = saved.RetryBudget
	// 重试总数以各节点 Attempts 为唯一事实来源核对（等价于 Σmax(Attempts-1,0)）：
	// 已经发生的重试随 Attempts 保留、不清零；旧版状态文件（RetriesUsed 漏记）或
	// 极端中断残留（已扣未跑的幻影计数）也在此收敛，保证重启后“总数==各节点之和”，
	// 不会凭空多算。
	st.RetriesUsed = st.totalRetries()
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

// pickReadyLocked 在锁内选出至多 limit 个当前可立即调度的节点，并就地置 running
// 占位，保证节点不会被重复调度。
//
// 挑选键（全部确定）：
//  1. Priority 降序——并发槽位紧张时关键路径优先；
//  2. 定义下标升序——同优先级保持稳定，旧定义（Priority 全为 0）时与历史行为一致。
//
// 返回顺序即选中顺序，因此同一份定义配同一份已有状态，调度轨迹可复现。
func (e *Engine) pickReadyLocked(limit int) []string {
	type cand struct {
		name  string
		index int
	}
	var cands []cand
	for i, n := range e.nodes {
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
			cands = append(cands, cand{n.Name, i})
		}
	}
	sort.SliceStable(cands, func(a, b int) bool {
		pa, pb := e.nodes[cands[a].index].Priority, e.nodes[cands[b].index].Priority
		if pa != pb {
			return pa > pb // 优先级数值越大越先
		}
		return cands[a].index < cands[b].index // 同优先级按定义顺序
	})
	if len(cands) > limit {
		cands = cands[:limit]
	}
	ready := make([]string, 0, len(cands))
	for _, c := range cands {
		// 标记 running 占位，保证该节点不会被重复调度（并发状态冲突防护）。
		e.state.Nodes[c.name].Status = StatusRunning
		ready = append(ready, c.name)
	}
	if len(ready) > 0 {
		_ = e.store.Save(e.state)
		for _, name := range ready {
			e.logf("[schedule] node=%q priority=%d attempts=%d",
				name, e.nodePriority(name), e.state.Nodes[name].Attempts)
		}
	}
	return ready
}

// nodePriority 返回节点的声明优先级（不存在时为 0）。
func (e *Engine) nodePriority(name string) int {
	for _, n := range e.nodes {
		if n.Name == name {
			return n.Priority
		}
	}
	return 0
}

// executeNode 在独立 goroutine 中执行节点，通过回调串行化落盘。
// 并发上限由调度器保证：只有存在空闲槽位时才会启动本 goroutine，因此这里不再排队等额度。
func (e *Engine) executeNode(ctx context.Context, name string, wg *sync.WaitGroup, onExit func()) {
	defer wg.Done()
	defer onExit()

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

	e.logf("[run] node=%q attempt=%d priority=%d timeout=%s input-deps=%v",
		name, snapshot.Attempts+1, def.Priority, def.Timeout, def.DependsOn)

	runNode(ctx, def, task, e.config.DefaultRetry, &snapshot,
		func(updated *NodeState) {
			e.mu.Lock()
			// 字段拷贝进既有条目：绝不能替换 map 条目，否则会覆盖调度器设置的
			// running 占位，造成节点状态回退与重复调度。
			*e.state.Nodes[name] = *updated
			err := e.store.Save(e.state)
			e.mu.Unlock()
			if err != nil {
				e.logf("[warn] persist node=%q: %v", name, err)
			}
		},
		// beginRetry：节点首次尝试失败后、准备发起第 nextAttempt 次尝试前，
		// 在引擎锁内一次性完成“预算闸门判定 + Attempts+1 + RetriesUsed+1 + running
		// 占位”并同一次落盘。只有这一个地方会产生新的重试计数，因此任何持久化时刻
		// 都有不变式：RetriesUsed == Σmax(Attempts-1,0)，不存在“预算扣了但尝试没
		// 记账”的幻影计数，也不会重复计。
		//
		// 注意：map 条目与执行循环手中的 st 快照在这里同步递增；随后的尝试结果
		// （成功/失败）仍由 onUpdate 整体回写，二者操作同一条目的不同阶段。
		func(node string, nextAttempt int) bool {
			e.mu.Lock()
			defer e.mu.Unlock()
			if e.state.RetryBudget > 0 && e.state.RetriesUsed >= e.state.RetryBudget {
				e.logf("[retry] node=%q attempt=%d denied: shared budget exhausted used=%d/%d",
					node, nextAttempt, e.state.RetriesUsed, e.state.RetryBudget)
				return false
			}
			cur := e.state.Nodes[node]
			cur.Attempts++
			cur.Status = StatusRunning
			e.state.RetriesUsed++
			e.logf("[retry] node=%q attempt=%d granted used=%d budget=%d",
				node, nextAttempt, e.state.RetriesUsed, e.state.RetryBudget)
			if err := e.store.Save(e.state); err != nil {
				e.logf("[warn] persist retry node=%q: %v", node, err)
			}
			return true
		},
	)
	e.logf("[done] node=%q final=%s attempts=%d error=%q budgetBlocked=%v",
		name, snapshot.Status, snapshot.Attempts, snapshot.LastError, snapshot.BudgetBlocked)
}

func (e *Engine) result(resumed bool) *RunResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	return &RunResult{Succeeded: e.state.Succeeded, Resumed: resumed, State: e.state}
}
