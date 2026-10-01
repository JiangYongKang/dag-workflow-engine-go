package workflow

import (
	"context"
	"fmt"
	"sort"
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

	mu              sync.Mutex // 保护 state 与所有调度决策
	state           *WorkflowState
	deps            map[string][]string
	indices         map[string]int  // 节点名 -> 定义中的下标，作为同优先级时的稳定次序
	scheduledLocked map[string]bool // 本次 Run 内已进入 ScheduleOrder 的节点
	logf            func(format string, args ...any)
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
	indices := make(map[string]int, len(nodes))
	for i, n := range nodes {
		deps[n.Name] = append([]string(nil), n.DependsOn...)
		indices[n.Name] = i
	}
	return &Engine{
		name:     name,
		nodes:    nodes,
		registry: registry,
		config:   config,
		store:    store,
		deps:     deps,
		indices:  indices,
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
// 任一依赖永久失败/被跳过时本节点标记为 skipped。运行态节点数受 MaxConcurrency
// 限制；槽位不足时按 Priority 降序、定义下标升序的确定性次序挑选就绪节点。
// 节点重试除受自身 RetryPolicy 约束外，还要申请 Config.MaxRetryBudget 共享额度。
//
// 恢复语义：启动时加载 Store 中的状态；已成功节点不重复执行；已花费的共享预算与
// 调度记录原样保留；失败节点沿用 Attempts/Retries 计数决定是否继续尝试；
// running 节点视为上次进程中断的残留，回到 pending 继续。
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
	e.scheduledLocked = make(map[string]bool, len(e.state.ScheduleOrder))
	for _, n := range e.state.ScheduleOrder {
		e.scheduledLocked[n] = true
	}
	resumed := saved != nil && saved.WorkflowName == e.name
	if err := e.store.Save(e.state); err != nil {
		e.mu.Unlock()
		return nil, fmt.Errorf("save initial state: %w", err)
	}
	e.mu.Unlock()

	e.logf("[engine] workflow=%q start resumed=%v concurrency=%d retryBudget=%d",
		e.name, resumed, e.config.MaxConcurrency, e.config.MaxRetryBudget)

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
		// 只按当前空闲槽位数量挑选节点：谁被选中完全由
		// “优先级降序、定义下标升序”的确定性次序决定，不会先到先抢。
		freeSlots := e.config.MaxConcurrency - int(atomic.LoadInt32(&inflight))
		ready := e.pickReadyLocked(freeSlots)
		e.mu.Unlock()

		for _, name := range ready {
			wg.Add(1)
			atomic.AddInt32(&inflight, 1)
			go e.executeNode(ctx, name, &wg, func() {
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
// 已花费的共享预算（RetriesUsed）与调度记录（ScheduleOrder）原样保留，
// 保证中断重启后“预算不回滚、已成功节点不重跑、调度决策可复核”。
func (e *Engine) reconcileState(saved *WorkflowState) *WorkflowState {
	st := newState(e.name, e.nodes)
	st.RetriesUsed = saved.RetriesUsed
	st.ScheduleOrder = append([]string(nil), saved.ScheduleOrder...)
	scheduled := make(map[string]bool, len(st.ScheduleOrder))
	for _, n := range st.ScheduleOrder {
		scheduled[n] = true
	}
	maxAttempts := normalizeRetry(e.config.DefaultRetry).MaxAttempts
	for _, n := range e.nodes {
		old, ok := saved.Nodes[n.Name]
		if !ok {
			continue // 定义中新增的节点保持 pending
		}
		cp := *old
		if cp.Retries < 0 || cp.Retries > cp.Attempts {
			cp.Retries = 0 // 防御非法历史数据
		}
		if cp.Attempts > 0 && cp.Retries == 0 && cp.Attempts > 1 {
			// 兼容旧版本状态文件：旧文件没有 Retries 字段，按 Attempts 推导。
			cp.Retries = cp.Attempts - 1
		}
		switch cp.Status {
		case StatusSucceeded:
			// 已成功：保持终态，绝不重复执行。
		case StatusFailed:
			// 预算耗尽（节点自身或全局共享）才是永久失败；预算未用尽（如上次被
			// 取消/超时中断）则回到 pending 继续重试。FailReason 非空或 Attempts
			// 已达节点上限都视为已确认的永久失败。
			permanent := cp.FailReason != "" || cp.Attempts >= maxAttempts
			if !permanent {
				cp.Status = StatusPending
				cp.FailReason = ""
			}
		case StatusSkipped:
			// 被上游失败连带跳过的节点保持终态。
		case StatusRunning:
			// 上次进程中断时在途的节点：未落任何终态，按失败处理等待重试。
			cp.Status = StatusPending
			cp.FailReason = ""
			cp.LastError = "interrupted while running"
		default:
			cp.Status = StatusPending
			cp.FailReason = ""
		}
		if !scheduled[n.Name] && old.Status != StatusPending && cp.Attempts > 0 {
			// 兼容旧版本状态文件（没有 ScheduleOrder）：只有真正发起过尝试的节点
			// 才是历史上被调度过的节点；failed/skipped 且 Attempts=0 的节点（连带
			// 跳过、或被取消后重置回 pending 再跳过）从未被选中，不得补入记录。
			st.ScheduleOrder = append(st.ScheduleOrder, n.Name)
			scheduled[n.Name] = true
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

// pickReadyLocked 在锁内选出当前可立即调度的节点，数量不超过 freeSlots。
//
// 挑选次序是确定的：先按 Priority 降序（数值越大越优先），优先级相同则按节点在
// 定义中的下标升序（即旧用法的定义顺序）。因此同一份定义配同一份已保存状态，
// 无论重启多少次，得到的调度次序与结果都一致。每个节点在整个生命周期内只会被
// 选中一次；被选中的节点按此次序追加到 ScheduleOrder 并落盘。
func (e *Engine) pickReadyLocked(freeSlots int) []string {
	if freeSlots <= 0 {
		return nil
	}
	type candidate struct {
		name string
		prio int
		idx  int
	}
	var cands []candidate
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
			cands = append(cands, candidate{name: n.Name, prio: n.Priority, idx: i})
		}
	}
	// 稳定排序：优先级降序，同优先级按下标升序（slice 已按下标收集，用稳定排序保底）。
	sort.SliceStable(cands, func(a, b int) bool {
		if cands[a].prio != cands[b].prio {
			return cands[a].prio > cands[b].prio
		}
		return cands[a].idx < cands[b].idx
	})
	if len(cands) > freeSlots {
		cands = cands[:freeSlots]
	}
	if len(cands) == 0 {
		return nil
	}
	ready := make([]string, 0, len(cands))
	var newlyRecorded []string
	for _, c := range cands {
		// 标记 running 占位，保证该节点不会被重复调度（并发状态冲突防护）。
		st := e.state.Nodes[c.name]
		st.Status = StatusRunning
		ready = append(ready, c.name)
		// 同一节点的重试（中断恢复后再次被选中）属于同一次生命周期的延续，
		// 调度记录只登记首次选中，保证跨重启不重复、可复现。
		if !e.scheduledLocked[c.name] {
			e.state.ScheduleOrder = append(e.state.ScheduleOrder, c.name)
			e.scheduledLocked[c.name] = true
			newlyRecorded = append(newlyRecorded, c.name)
		}
	}
	_ = e.store.Save(e.state)
	for _, name := range ready {
		e.logf("[schedule] node=%q priority=%d attempts=%d retriesUsed=%d firstPick=%v",
			name, e.nodePriority(name), e.state.Nodes[name].Attempts, e.state.RetriesUsed,
			containsName(newlyRecorded, name))
	}
	return ready
}

// containsName 判断 name 是否在 names 中（调度日志辅助，节点数很少，线性即可）。
func containsName(names []string, name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

// nodePriority 返回节点定义的优先级。
func (e *Engine) nodePriority(name string) int {
	return e.nodes[e.indices[name]].Priority
}

// executeNode 在独立 goroutine 中执行节点，通过回调串行化落盘。
// 调度器在派发时已预留并发槽位（inflight 计数），因此这里直接执行，不再争抢信号量。
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

	e.logf("[run] node=%q attempt=%d timeout=%s input-deps=%v priority=%d",
		name, snapshot.Attempts+1, def.Timeout, def.DependsOn, def.Priority)

	runNode(ctx, def, task, e.config.DefaultRetry, &snapshot,
		func(updated *NodeState) bool { return e.claimRetryLocked(name, updated) },
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
		})
	e.logf("[done] node=%q final=%s attempts=%d retries=%d reason=%q error=%q",
		name, snapshot.Status, snapshot.Attempts, snapshot.Retries, snapshot.FailReason, snapshot.LastError)
}

// claimRetryLocked 在引擎锁内为指定节点申请一次全局共享重试额度（先到先得）。
// MaxRetryBudget<=0 表示不设统一预算：始终批准，但仍累计 RetriesUsed 以便事后统计。
// MaxRetryBudget>0 时，RetriesUsed 达到上限即拒绝；被拒绝的节点由执行循环标记为
// FailReasonBudgetExhausted 的永久失败。授予时同时把这笔重试记到节点名下（Retries）
// 并与工作流计数在同一次持锁落盘中完成，保证中断后预算账目不丢、不重。
func (e *Engine) claimRetryLocked(name string, updated *NodeState) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.config.MaxRetryBudget > 0 && e.state.RetriesUsed >= e.config.MaxRetryBudget {
		e.logf("[budget] node=%q shared retry budget exhausted: used=%d limit=%d",
			name, e.state.RetriesUsed, e.config.MaxRetryBudget)
		return false
	}
	e.state.RetriesUsed++
	updated.Retries = updated.Attempts
	*e.state.Nodes[name] = *updated
	if err := e.store.Save(e.state); err != nil {
		e.logf("[warn] persist retry grant node=%q: %v", name, err)
	}
	e.logf("[budget] node=%q retry granted: nodeRetries=%d used=%d limit=%d",
		name, updated.Retries, e.state.RetriesUsed, e.config.MaxRetryBudget)
	return true
}

func (e *Engine) result(resumed bool) *RunResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	return &RunResult{Succeeded: e.state.Succeeded, Resumed: resumed, State: e.state}
}
