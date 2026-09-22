package dag

import "time"

// RetryPolicy 单节点重试策略。
// 退避规则：第 n 次失败后的等待为 InitialDelay * BackoffFactor^(n-1)，
// 以 MaxDelay 封顶；MaxAttempts 为总尝试次数（含首次执行）。
type RetryPolicy struct {
	MaxAttempts   int           // 最大尝试次数（含首次），<=0 表示 1
	InitialDelay  time.Duration // 首次重试前的基础退避
	MaxDelay      time.Duration // 退避上限，0 表示不封顶
	BackoffFactor float64       // 指数退避乘数，<1 时按 1 处理（不放大）
}

// DefaultRetryPolicy 不重试、不退避。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 1}
}

// EffectiveMaxAttempts 返回规范化后的最大尝试次数（至少为 1）。
func (p RetryPolicy) EffectiveMaxAttempts() int {
	if p.MaxAttempts <= 0 {
		return 1
	}
	return p.MaxAttempts
}

// BackoffDelay 计算第 failedAttempt 次失败（从 1 开始）后的等待时长。
// 若该次失败后已无剩余重试，返回 0。
func (p RetryPolicy) BackoffDelay(failedAttempt int) time.Duration {
	if failedAttempt <= 0 {
		return 0
	}
	// failedAttempt 次失败后，至少要还能再发起一次尝试。
	if failedAttempt >= p.EffectiveMaxAttempts() {
		return 0
	}
	if p.InitialDelay <= 0 {
		return 0
	}
	factor := p.BackoffFactor
	if factor < 1 {
		factor = 1
	}
	d := float64(p.InitialDelay)
	for i := 1; i < failedAttempt; i++ {
		d *= factor
		if p.MaxDelay > 0 && d >= float64(p.MaxDelay) {
			return p.MaxDelay
		}
	}
	delay := time.Duration(d)
	if p.MaxDelay > 0 && delay > p.MaxDelay {
		delay = p.MaxDelay
	}
	return delay
}
