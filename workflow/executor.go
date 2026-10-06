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
//
// beginRetry 在节点首次尝试失败、准备发起第 (st.Attempts+1) 次尝试前由引擎在锁内调用，
// 原子地完成预算闸门判定、Attempts 与 RetriesUsed 同步递增及 running 落盘。返回：
//   - true：已记账 1 次重试，应继续发起本次尝试（st.Attempts 已同步 +1）；
//   - false：运行级共享重试预算已耗尽，节点必须立即永久失败（BudgetBlocked=true）。
//
// 未启用共享预算（RetryBudget<=0）时 beginRetry 恒返回 true，重试只受节点自身
// MaxAttempts 约束；达到自身上限的节点在调用 beginRetry 之前就被判失败，不产生重试计数。
func runNode(
	ctx context.Context,
	def NodeDef,
	task TaskFunc,
	policy RetryPolicy,
	st *NodeState,
	onUpdate func(*NodeState),
	beginRetry func(node string, nextAttempt int) bool,
) Status {
	policy = normalizeRetry(policy)

	for {
		// 取消先于调度：ctx 已取消时不再发起新尝试，也不产生新的重试计数。
		if err := ctx.Err(); err != nil {
			st.LastError = err.Error()
			onUpdate(st)
			return StatusFailed
		}

		if st.Attempts > 0 {
			// 先检查节点自身上限：已达 MaxAttempts 时直接永久失败。
			// 此分支不申请、不消耗共享预算，BudgetBlocked 保持 false，
			// 与“共享预算耗尽”的永久失败严格区分。
			if st.Attempts >= policy.MaxAttempts {
				st.Status = StatusFailed
				if st.LastError == "" {
					st.LastError = "node retry attempts exhausted"
				}
				onUpdate(st)
				return StatusFailed
			}
			// 再走运行级闸门（含共享预算判定）并原子记账本次重试。
			if !beginRetry(def.Name, st.Attempts+1) {
				// 仅当共享预算已耗尽时才会走到这里：永久失败，
				// 保留最近一次真实失败原因，标记 BudgetBlocked 以区分终止原因。
				st.Status = StatusFailed
				st.BudgetBlocked = true
				if st.LastError == "" {
					st.LastError = "run retry budget exhausted"
				}
				st.FinishedAt = time.Now().UTC()
				onUpdate(st)
				return StatusFailed
			}
			// 重试已被引擎原子记账（map 中 Attempts/RetriesUsed/running 已落盘），
			// 同步本地快照，保持单一事实一致。
			st.Attempts++
		} else {
			// 首次尝试：不扣任何重试预算/计数。
			st.Attempts++
			if st.StartedAt.IsZero() {
				st.StartedAt = time.Now().UTC()
			}
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
		// 本次失败后已达节点自身上限：永久失败（BudgetBlocked=false）。
		// 注意该判定发生在本次尝试执行之后，下一轮循环顶部不会重复计入重试。
		if st.Attempts >= policy.MaxAttempts {
			st.Status = StatusFailed
			st.FinishedAt = time.Now().UTC()
			onUpdate(st)
			return StatusFailed
		}

		// 退避期间也响应取消与超时；取消时第 (Attempts+1) 次尝试尚未发起，
		// 不计重试，重启后接续计数继续。
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
