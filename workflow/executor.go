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
// 每次状态变更都通过 onUpdate 回调落盘。返回该节点的最终 Status。
func runNode(
	ctx context.Context,
	def NodeDef,
	task TaskFunc,
	policy RetryPolicy,
	st *NodeState,
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
			// 重试预算耗尽（恢复场景下进入时即可能成立）。
			if st.Status != StatusSucceeded {
				st.Status = StatusFailed
				if st.LastError == "" {
					st.LastError = "retry budget exhausted"
				}
				onUpdate(st)
			}
			return st.Status
		}

		st.Attempts++
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
			onUpdate(st)
			return StatusSucceeded
		}

		st.LastError = err.Error()
		if st.Attempts >= policy.MaxAttempts {
			st.Status = StatusFailed
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
