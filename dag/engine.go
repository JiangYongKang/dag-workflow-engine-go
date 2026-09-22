package dag

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Logger 引擎日志钩子：打印调度输入与判定依据。
type Logger interface {
	Printf(format string, args ...any)
}

// Config 引擎配置。
type Config struct {
	MaxConcurrency int           // 全局同时执行的节点尝试数上限，<=0 按 1 处理
	DefaultTimeout time.Duration // 节点未单独配置 Timeout 时使用，0 表示不限制
	Logger         Logger        // 可选日志
}

// fatalError 表示状态持久化失败等不可继续的故障：引擎必须立即中止，
// 不能把它降级为节点失败后继续调度（否则会破坏状态一致性）。
type fatalError struct{ err error }

func (e *fatalError) Error() string { return "dag: fatal: " + e.err.Error() }
func (e *fatalError) Unwrap() error { return e.err }

// execKey identifies one node within a workflow for attempt counting.
type execKey struct{ wf, node string }

// Engine 本地工作流编排引擎。
// 所有状态读写都在 e.mu 保护下进行；节点函数本身在锁外执行。
type Engine struct {
	cfg   Config
	store Store

	mu        sync.Mutex
	running   map[string]struct{}            // 进程内运行去重
	active    map[string]*WorkflowState      // Run 期间持有的活动状态（锁内唯一实例）
	depCache  map[string]map[string][]string // wfID -> nodeID -> 去重排序后的依赖
	execCount map[execKey]int                // 本进程各节点实际发起尝试的次数

	runMu sync.Mutex
}

// NewEngine 创建引擎。
func NewEngine(cfg Config, store Store) *Engine {
	if store == nil {
		store = NewMemoryStore()
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 1
	}
	return &Engine{
		cfg:       cfg,
		store:     store,
		running:   make(map[string]struct{}),
		active:    make(map[string]*WorkflowState),
		depCache:  make(map[string]map[string][]string),
		execCount: make(map[execKey]int),
	}
}

func (e *Engine) logf(format string, args ...any) {
	if e.cfg.Logger != nil {
		e.cfg.Logger.Printf(format, args...)
	}
}

// Run 启动工作流；若存储中已有状态，则按恢复语义继续执行。
// 终态工作流重复调用是幂等的：直接返回已保存终态，不会重复执行任何节点。
func (e *Engine) Run(ctx context.Context, def *WorkflowDef) (*WorkflowState, error) {
	if err := Validate(def); err != nil {
		return nil, err
	}

	defs := make(map[string]*NodeDef, len(def.Nodes))
	deps := make(map[string][]string, len(def.Nodes))
	for i := range def.Nodes {
		nd := &def.Nodes[i]
		defs[nd.ID] = nd
		deps[nd.ID] = uniqueSorted(nd.Deps)
	}

	e.mu.Lock()
	if _, alive := e.active[def.ID]; alive {
		e.mu.Unlock()
		return nil, fmt.Errorf("%w: id=%s", ErrAlreadyRunning, def.ID)
	}
	state, err := e.loadOrInit(def, defs)
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}
	e.active[def.ID] = state
	e.depCache[def.ID] = deps

	// 终态幂等：直接返回，不重复执行任何节点。
	if state.Status == WFSucceeded || state.Status == WFFailed {
		e.logf("[wf=%s] already terminal status=%s -> return persisted state, no node will run",
			def.ID, state.Status)
		res := state.Clone()
		delete(e.active, def.ID)
		delete(e.depCache, def.ID)
		e.mu.Unlock()
		return res, nil
	}

	state.Status = WFRunning
	if err := e.persist(state); err != nil {
		delete(e.active, def.ID)
		delete(e.depCache, def.ID)
		e.mu.Unlock()
		return nil, err
	}
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		delete(e.active, def.ID)
		delete(e.depCache, def.ID)
		e.mu.Unlock()
	}()

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	sem := make(chan struct{}, e.cfg.MaxConcurrency)
	events := make(chan string, len(def.Nodes))
	fatalCh := make(chan error, 1)
	var wg sync.WaitGroup

	runNode := func(nd *NodeDef) {
		defer wg.Done()
		if ferr := e.executeNode(runCtx, def.ID, nd, sem); ferr != nil {
			select {
			case fatalCh <- ferr:
			default:
			}
			return
		}
		select {
		case events <- nd.ID:
		default:
		}
	}

	for {
		var wake <-chan time.Time
		e.mu.Lock()
		// 外部取消：尚未开始的节点标记 skipped，在途尝试由 ctx 传播失败。
		if ctx.Err() != nil {
			for _, id := range sortedNodes(state) {
				ns := state.Nodes[id]
				if ns.Status == StatusPending {
					ns.Status = StatusSkipped
					if ns.Error == "" {
						ns.Error = ctx.Err().Error()
					}
					ns.UpdatedAt = time.Now()
					e.logf("[wf=%s node=%s] ctx canceled before start -> skipped", def.ID, id)
				}
			}
			if err := e.persist(state); err != nil {
				e.mu.Unlock()
				return nil, err
			}
		}
		e.propagateSkipped(def.ID, state)

		inFlight := 0
		for _, ns := range state.Nodes {
			if ns.Status == StatusRunning {
				inFlight++
			}
		}
		slots := e.cfg.MaxConcurrency - inFlight

		for _, id := range e.readyNodes(state) {
			if slots <= 0 {
				break
			}
			// 占位申领为 running：保证同一节点绝不会被并发重复调度。
			state.Nodes[id].Status = StatusRunning
			slots--
			wg.Add(1)
			e.logf("[wf=%s node=%s] claimed for scheduling (in-flight=%d, limit=%d)",
				def.ID, id, inFlight+e.cfg.MaxConcurrency-slots, e.cfg.MaxConcurrency)
			go runNode(defs[id])
		}

		if hasWaiting(state) {
			wake = time.After(5 * time.Millisecond)
		}

		allDone := true
		for _, ns := range state.Nodes {
			if ns.Status == StatusPending || ns.Status == StatusRunning {
				allDone = false
			}
		}
		e.mu.Unlock()

		if allDone {
			e.mu.Lock()
			e.finalize(def.ID, state)
			res := state.Clone()
			e.mu.Unlock()
			wg.Wait()
			return res, nil
		}

		select {
		case <-events:
		case <-wake:
		case <-ctx.Done():
		case ferr := <-fatalCh:
			// 致命中止：取消所有在途任务，等它们退出后再清理活动状态，避免状态表被回收后写入。
			cancelRun()
			wg.Wait()
			return nil, ferr
		}
	}
}

// executeNode 负责单节点生命周期：预算判定 -> 信号量 -> 超时执行 -> 重试/退避。
// 返回非 nil 仅发生于状态持久化失败等致命场景，此时引擎必须中止。
func (e *Engine) executeNode(ctx context.Context, wfID string, nd *NodeDef, sem chan struct{}) error {
	policy := nd.Retry
	if policy.MaxAttempts == 0 && policy.InitialDelay == 0 {
		policy = DefaultRetryPolicy()
	}
	maxAttempts := policy.EffectiveMaxAttempts()
	input := NodeInput{WorkflowID: wfID, NodeID: nd.ID}

	for {
		e.mu.Lock()
		ns := e.active[wfID].Nodes[nd.ID]
		if ns.Status != StatusRunning {
			e.mu.Unlock()
			return nil // 已被取消传播等路径终结
		}
		if ctx.Err() != nil {
			ns.Status = StatusSkipped
			if ns.Error == "" {
				ns.Error = ctx.Err().Error()
			}
			ns.UpdatedAt = time.Now()
			if perr := e.persist(e.active[wfID]); perr != nil {
				e.mu.Unlock()
				return &fatalError{perr}
			}
			e.mu.Unlock()
			e.logf("[wf=%s node=%s] canceled while queued -> skipped", wfID, nd.ID)
			return nil
		}
		if ns.Attempts >= maxAttempts {
			ns.Status = StatusFailed
			if ns.Error == "" {
				ns.Error = "retry budget exhausted"
			}
			ns.UpdatedAt = time.Now()
			if perr := e.persist(e.active[wfID]); perr != nil {
				e.mu.Unlock()
				return &fatalError{perr}
			}
			e.mu.Unlock()
			e.logf("[wf=%s node=%s] attempts=%d >= max=%d -> failed, will not be scheduled again",
				wfID, nd.ID, ns.Attempts, maxAttempts)
			return nil
		}
		e.mu.Unlock()

		// 获取全局并发槽位；等待期间取消可中断。
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			e.mu.Lock()
			if cur := e.active[wfID].Nodes[nd.ID]; cur.Status == StatusRunning {
				cur.Status = StatusSkipped
				if cur.Error == "" {
					cur.Error = ctx.Err().Error()
				}
				cur.UpdatedAt = time.Now()
				if perr := e.persist(e.active[wfID]); perr != nil {
					e.mu.Unlock()
					return &fatalError{perr}
				}
			}
			e.mu.Unlock()
			return nil
		}

		attemptErr := e.runOnce(ctx, wfID, nd, input, sem)
		if attemptErr == nil {
			return nil
		}
		var fatal *fatalError
		if errors.As(attemptErr, &fatal) {
			return attemptErr
		}

		e.mu.Lock()
		cur := e.active[wfID].Nodes[nd.ID]
		if ctx.Err() != nil {
			// 外部取消/超时来自父 ctx：中断即终态，不消耗后续重试。
			cur.Status = StatusFailed
			cur.Error = attemptErr.Error()
			cur.UpdatedAt = time.Now()
			if perr := e.persist(e.active[wfID]); perr != nil {
				e.mu.Unlock()
				return &fatalError{perr}
			}
			e.mu.Unlock()
			e.logf("[wf=%s node=%s] attempt=%d interrupted by context cancel -> failed: %v",
				wfID, nd.ID, cur.Attempts, attemptErr)
			return nil
		}
		if cur.Attempts >= maxAttempts {
			cur.Status = StatusFailed
			cur.Error = attemptErr.Error()
			cur.UpdatedAt = time.Now()
			if perr := e.persist(e.active[wfID]); perr != nil {
				e.mu.Unlock()
				return &fatalError{perr}
			}
			e.mu.Unlock()
			e.logf("[wf=%s node=%s] attempt=%d/%d failed, retries exhausted -> failed: %v",
				wfID, nd.ID, cur.Attempts, maxAttempts, attemptErr)
			return nil
		}
		delay := policy.BackoffDelay(cur.Attempts)
		cur.Status = StatusPending // 等待退避；此期间不占并发槽位
		cur.Error = attemptErr.Error()
		cur.UpdatedAt = time.Now()
		if perr := e.persist(e.active[wfID]); perr != nil {
			e.mu.Unlock()
			return &fatalError{perr}
		}
		e.mu.Unlock()

		e.logf("[wf=%s node=%s] attempt=%d/%d failed: %v | decision=retry after %s",
			wfID, nd.ID, cur.Attempts, maxAttempts, attemptErr, delay)
		if delay > 0 {
			t := time.NewTimer(delay)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
			}
		}
	}
}

// runOnce 执行单次尝试：管理超时、记录、尝试计数与持久化，返回尝试错误。
func (e *Engine) runOnce(ctx context.Context, wfID string, nd *NodeDef, input NodeInput, sem chan struct{}) error {
	defer func() { <-sem }()

	timeout := nd.Timeout
	if timeout == 0 {
		timeout = e.cfg.DefaultTimeout
	}

	e.mu.Lock()
	st := e.active[wfID]
	ns := st.Nodes[nd.ID]
	attempt := ns.Attempts + 1
	ns.Attempts = attempt
	ns.Status = StatusRunning
	now := time.Now()
	ns.Records = append(ns.Records, AttemptRecord{Attempt: attempt, StartedAt: now})
	ns.UpdatedAt = now
	input.Attempt = attempt
	e.execCount[execKey{wfID, nd.ID}]++
	if perr := e.persist(st); perr != nil {
		e.mu.Unlock()
		return &fatalError{perr}
	}
	e.mu.Unlock()

	e.logf("[wf=%s node=%s] >>> execute input={wf:%s,node:%s,attempt:%d} timeout=%s",
		wfID, nd.ID, input.WorkflowID, input.NodeID, input.Attempt, timeout)

	runCtx, cancel := context.WithCancel(ctx)
	if timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	start := time.Now()
	err := nd.Run(runCtx, input)
	end := time.Now()

	e.mu.Lock()
	defer e.mu.Unlock()
	st = e.active[wfID]
	ns = st.Nodes[nd.ID]
	rec := &ns.Records[len(ns.Records)-1]
	rec.EndedAt = end
	if err != nil {
		rec.Err = err.Error()
		ns.Error = err.Error()
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			detail := "node timeout after " + end.Sub(start).String()
			rec.Err = detail + ": " + rec.Err
			ns.Error = rec.Err
		}
	}
	ns.UpdatedAt = end
	if err == nil {
		ns.Status = StatusSucceeded
		ns.Error = ""
		e.logf("[wf=%s node=%s] <<< attempt=%d succeeded in %s", wfID, nd.ID, attempt, end.Sub(start))
	} else {
		e.logf("[wf=%s node=%s] <<< attempt=%d failed in %s: %v", wfID, nd.ID, attempt, end.Sub(start), err)
	}
	if perr := e.persist(st); perr != nil {
		e.mu.Unlock()
		return &fatalError{perr}
	}
	return err
}

// loadOrInit 在持锁状态下调用：加载并转换持久化状态，或初始化新状态。
func (e *Engine) loadOrInit(def *WorkflowDef, defs map[string]*NodeDef) (*WorkflowState, error) {
	loaded, err := e.store.Load(def.ID)
	if err != nil && !errors.Is(err, ErrWorkflowNotFound) {
		return nil, err
	}
	if loaded == nil {
		st := &WorkflowState{ID: def.ID, Status: WFPending, Nodes: make(map[string]*NodeState)}
		for _, nd := range def.Nodes {
			st.Nodes[nd.ID] = &NodeState{ID: nd.ID, Status: StatusPending}
		}
		st.UpdatedAt = time.Now()
		if err := e.persist(st); err != nil {
			return nil, err
		}
		e.logf("[wf=%s] initialized fresh state with %d nodes", def.ID, len(def.Nodes))
		return st, nil
	}

	for id := range loaded.Nodes {
		if _, ok := defs[id]; !ok {
			return nil, fmt.Errorf("%w: persisted node %q no longer exists in definition", ErrInvalidConfig, id)
		}
	}
	for _, nd := range def.Nodes {
		if _, ok := loaded.Nodes[nd.ID]; !ok {
			loaded.Nodes[nd.ID] = &NodeState{ID: nd.ID, Status: StatusPending}
			e.logf("[wf=%s node=%s] newly added node on resume -> pending", def.ID, nd.ID)
		}
	}

	// 恢复转换：崩溃时处于 running 的尝试视为中断，按剩余预算决定 failed/pending。
	for _, id := range sortedNodes(loaded) {
		ns := loaded.Nodes[id]
		if ns.Status != StatusRunning {
			continue
		}
		if len(ns.Records) > 0 {
			rec := &ns.Records[len(ns.Records)-1]
			if rec.EndedAt.IsZero() {
				rec.EndedAt = time.Now()
				rec.Err = "interrupted by process restart"
			}
		}
		policy := defs[id].Retry
		if policy.MaxAttempts == 0 && policy.InitialDelay == 0 {
			policy = DefaultRetryPolicy()
		}
		if ns.Attempts >= policy.EffectiveMaxAttempts() {
			ns.Status = StatusFailed
			ns.Error = "interrupted by process restart; retry budget exhausted"
			e.logf("[wf=%s node=%s] recover interrupted attempt=%d, budget exhausted -> failed",
				def.ID, id, ns.Attempts)
		} else {
			ns.Status = StatusPending
			ns.Error = ""
			e.logf("[wf=%s node=%s] recover interrupted attempt=%d/%d -> pending for retry",
				def.ID, id, ns.Attempts, policy.EffectiveMaxAttempts())
		}
		ns.UpdatedAt = time.Now()
	}
	loaded.UpdatedAt = time.Now()
	if err := e.store.Save(loaded.Clone()); err != nil {
		return nil, err
	}
	e.logf("[wf=%s] resumed from persisted state status=%s", def.ID, loaded.Status)
	return loaded, nil
}

// propagateSkipped 持锁调用：依赖 failed/skipped 的 pending 节点级联 skipped。
func (e *Engine) propagateSkipped(wfID string, st *WorkflowState) {
	for changed := true; changed; {
		changed = false
		for _, id := range sortedNodes(st) {
			ns := st.Nodes[id]
			if ns.Status != StatusPending {
				continue
			}
			for _, depID := range e.depCache[wfID][id] {
				if dep := st.Nodes[depID]; dep.Status == StatusFailed || dep.Status == StatusSkipped {
					ns.Status = StatusSkipped
					ns.Error = "upstream " + string(dep.Status) + ": " + depID
					ns.UpdatedAt = time.Now()
					changed = true
					e.logf("[wf=%s node=%s] upstream %s is %s -> skipped", wfID, id, depID, dep.Status)
					break
				}
			}
		}
	}
}

// readyNodes 持锁调用：依赖全部 succeeded 的 pending 节点，按 ID 排序。
func (e *Engine) readyNodes(st *WorkflowState) []string {
	out := make([]string, 0)
	for _, id := range sortedNodes(st) {
		if st.Nodes[id].Status != StatusPending {
			continue
		}
		ready := true
		for _, depID := range e.depCache[st.ID][id] {
			if st.Nodes[depID].Status != StatusSucceeded {
				ready = false
				break
			}
		}
		if ready {
			out = append(out, id)
		}
	}
	return out
}

// hasWaiting 是否存在因失败退避而暂回 pending 的节点（需要定时唤醒）。
func hasWaiting(st *WorkflowState) bool {
	for _, ns := range st.Nodes {
		if ns.Status == StatusPending && ns.Error != "" && len(ns.Records) > 0 {
			return true
		}
	}
	return false
}

// finalize 持锁调用：任一节点 failed 即整体 failed；全 succeeded 为整体 succeeded。
func (e *Engine) finalize(wfID string, st *WorkflowState) {
	var failedID string
	for _, id := range sortedNodes(st) {
		if st.Nodes[id].Status == StatusFailed {
			failedID = id
			break
		}
	}
	st.UpdatedAt = time.Now()
	if failedID != "" {
		st.Status = WFFailed
		st.Error = "node failed: " + failedID
	} else {
		st.Status = WFSucceeded
		st.Error = ""
	}
	_ = e.store.Save(st.Clone())
	e.logf("[wf=%s] final status=%s basis: failed-node=%q", wfID, st.Status, failedID)
}

func (e *Engine) persist(st *WorkflowState) error {
	return e.store.Save(st.Clone())
}

// ExecCount 返回节点在本进程内实际发起执行的尝试次数（重复执行检测用）。
func (e *Engine) ExecCount(workflowID, nodeID string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.execCount[execKey{workflowID, nodeID}]
}

// LoadState 读取已持久化的工作流状态深拷贝。
func (e *Engine) LoadState(workflowID string) (*WorkflowState, error) {
	return e.store.Load(workflowID)
}

func sortedNodes(st *WorkflowState) []string {
	ids := make([]string, 0, len(st.Nodes))
	for id := range st.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func uniqueSorted(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, x := range in {
		if _, ok := seen[x]; ok {
			continue
		}
		seen[x] = struct{}{}
		out = append(out, x)
	}
	sort.Strings(out)
	return out
}
