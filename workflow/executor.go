package workflow

import (
	"context"
	"time"
)

// normalizeRetry 规整重试策略为确定的内部取值。
func normalizeRetry(p RetryPolicy) RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 1
	}
	if p.Factor < 1 {
		p.Factor = 1
	}
	if p.MaxBackoff > 0 && p.InitialBackoff > p.MaxBackoff {
		p.InitialBackoff = p.MaxBackoff
	}
	return p
}

// backoff 计算第 failureCount 次失败后的退避时长（failureCount 从 1 开始）：
// delay = InitialBackoff * Factor^(failureCount-1)，超过 MaxBackoff 则截断。
func backoff(p RetryPolicy, failureCount int) time.Duration {
	p = normalizeRetry(p)
	if p.InitialBackoff <= 0 || failureCount <= 0 {
		return 0
	}
	d := float64(p.InitialBackoff)
	for i := 1; i < failureCount; i++ {
		d *= p.Factor
		if p.MaxBackoff > 0 && d >= float64(p.MaxBackoff) {
			return p.MaxBackoff
		}
	}
	return time.Duration(d)
}

// runNode 在当前 goroutine 内按重试策略执行单个节点，直到成功、重试耗尽或 ctx 取消。
// st.Attempts 为此前（含历史进程）已发起的尝试次数；恢复时沿用该计数，保证节点不会超额重试。
//
// claimRetry 在节点失败后、准备再试一次之前调用：向引擎申请一次全局共享重试额度，
// 返回 false 表示全局预算已被其他节点先扣完，节点直接按重试耗尽永久失败处理。
// 返回 true 时额度已记到该节点名下（st.Retries）并与工作流计数同一次持锁落盘：
// 即使进程在随后的退避等待中被杀掉，这笔账也已持久化，恢复后不再重复申请。
//
// 每次状态变更都通过 onUpdate 回调落盘。返回该节点的最终 Status。
func runNode(
	ctx context.Context,
	def NodeDef,
	task TaskFunc,
	policy RetryPolicy,
	st *NodeState,
	claimRetry func(st *NodeState) bool,
	onUpdate func(*NodeState),
) Status {
	policy = normalizeRetry(policy)

	for {
		// 取消先于调度：ctx 已取消时不再发起新尝试。
		if err := ctx.Err(); err != nil {
			st.LastError = err.Error()
			onUpdate(st)
			return StatusFailed
		}
		if st.Attempts >= policy.MaxAttempts {
			// 节点自身重试次数耗尽（恢复场景下进入时即可能成立）。
			if st.Status != StatusSucceeded {
				st.Status = StatusFailed
				if st.LastError == "" {
					st.LastError = "retry budget exhausted"
				}
				st.FailReason = FailReasonAttemptsExhausted
				onUpdate(st)
			}
			return st.Status
		}

		st.Attempts++
		st.Retries = st.Attempts - 1
		if st.StartedAt.IsZero() {
			st.StartedAt = time.Now().UTC()
		}
		st.Status = StatusRunning
		st.LastError = ""
		onUpdate(st)

		attemptCtx := ctx
		var cancel context.CancelFunc
		if def.Timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, def.Timeout)
		}

		err := task(attemptCtx, def.Name, st.Attempts)
		if cancel != nil {
			cancel()
		}

		if err == nil {
			st.Status = StatusSucceeded
			st.FinishedAt = time.Now().UTC()
			st.LastError = ""
			st.FailReason = ""
			onUpdate(st)
			return StatusSucceeded
		}

		st.LastError = err.Error()
		if st.Attempts >= policy.MaxAttempts {
			// 节点自身的 MaxAttempts 已用尽。
			st.Status = StatusFailed
			st.FailReason = FailReasonAttemptsExhausted
			st.FinishedAt = time.Now().UTC()
			onUpdate(st)
			return StatusFailed
		}

		// 失败本身就是取消/超时导致时不再申请重试额度：这次尝试已不可能延续，
		// 不应消耗全局共享预算（与旧版本“取消不计重试”的行为保持一致）。
		if ctx.Err() != nil {
			st.Status = StatusFailed
			st.LastError = ctx.Err().Error()
			st.FinishedAt = time.Now().UTC()
			onUpdate(st)
			return StatusFailed
		}

		// 还在节点自身策略范围内：先扣全局共享预算，先到先得，再退避。
		if !claimRetry(st) {
			st.Status = StatusFailed
			st.FailReason = FailReasonBudgetExhausted
			st.FinishedAt = time.Now().UTC()
			onUpdate(st)
			return StatusFailed
		}

		// 退避期间也响应取消与超时。
		wait := backoff(policy, st.Attempts)
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				st.Status = StatusFailed
				st.LastError = ctx.Err().Error()
				st.FinishedAt = time.Now().UTC()
				onUpdate(st)
				return StatusFailed
			case <-timer.C:
			}
		}
	}
}
