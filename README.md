# 本地 DAG 工作流编排引擎

一个零外部依赖的本地工作流引擎：以可组合组件完成 DAG 任务的**调度、执行、重试与断点恢复**，
同一份工作流定义在不同重试与重启场景下得到**确定且可解释**的结果。

## 组件结构

| 文件 | 职责 |
| --- | --- |
| `workflow/types.go` | 核心类型：`NodeDef` / `Config` / `RetryPolicy` / `NodeState` / `WorkflowState` / `Store` |
| `workflow/errors.go` | 可区分的定义错误：空名、重复节点、缺失依赖、环形依赖、任务未注册 |
| `workflow/validate.go` | 定义校验与确定性拓扑排序（DFS 三色标记 + 名称排序） |
| `workflow/state.go` | `FileStore`：JSON 状态持久化，临时文件 + rename 原子写入 |
| `workflow/executor.go` | 单节点执行循环：节点重试策略、共享重试预算申请、指数退避、节点超时、取消传播 |
| `workflow/engine.go` | 调度器：优先级就绪挑选、并发槽位控制、共享预算扣减、失败传播跳过、恢复规整 |
| `cmd/demo/main.go` | 可运行示例 |

## 快速开始

```go
nodes := []workflow.NodeDef{
    {Name: "a"},
    {Name: "b", DependsOn: []string{"a"}, Timeout: 2 * time.Second},
    {Name: "c", DependsOn: []string{"b"}},
}
reg := workflow.TaskRegistry{
    "a": func(ctx context.Context, node string, attempt int) error { return nil },
    // ...
}
engine, err := workflow.NewEngine("my-wf", nodes, reg,
    workflow.Config{
        MaxConcurrency: 4,
        DefaultRetry: workflow.RetryPolicy{
            MaxAttempts: 3, InitialBackoff: 100 * time.Millisecond,
            MaxBackoff: time.Second, Factor: 2,
        },
        MaxRetryBudget: 20, // 跨所有节点共享的重试总次数（可选，<=0 表示不设）
    },
    workflow.NewFileStore("state/wf.json"))
if err != nil { /* 定义非法，见下“错误归类” */ }

// 节点可声明优先级（可选，默认 0；数值越大越优先）：
// NodeDef{Name: "critical", Priority: 10, DependsOn: ...}

res, err := engine.Run(ctx)
res.Succeeded // 整体成功 / 整体失败可区分
res.Resumed   // 本次是否基于历史状态恢复
res.State.RetriesUsed        // 本次运行总共消耗的共享重试次数
res.State.ScheduleOrder      // 节点首次被选中的确定性先后顺序
// res.State.Nodes[name].Retries     该节点各自用掉的重试次数
// res.State.Nodes[name].FailReason  永久失败原因归类
```

运行示例：

```bash
go build -o /tmp/dag-demo ./cmd/demo
/tmp/dag-demo -state /tmp/demo-state.json   # 首次执行，含退避重试
/tmp/dag-demo -state /tmp/demo-state.json   # 再次运行：识别恢复，成功节点不重跑
```

## 调度模型

1. **校验先行**：`NewEngine` 调用 `Validate`，定义非法直接拒绝，不执行任何任务。
2. **就绪条件**：节点的**所有依赖均为 `succeeded`** 时才进入就绪集合；依赖按定义与名称排序处理，结果确定。
3. **优先级挑选（新增）**：当就绪节点数多于当前空闲槽位数时，调度器**按槽位数**挑选，
   次序为 `Priority` 降序（数值越大越优先，默认 0），优先级相同则按**节点在定义中的下标升序**。
   因此槽位再紧张，关键路径（高优先级）也不会被一堆低优先级长任务长期占住；同一份定义配
   同一份已保存状态，无论重启多少次，调度顺序与结果都一致（同优先级次序稳定可复现）。
4. **失败传播**：任一依赖进入永久终态（`failed` / `skipped`），该节点标记为 `skipped`，永不执行。
5. **并发约束**：并发槽位由调度器在**选中节点的瞬间**按空闲槽位数量预留（运行中节点计数），
   worker 完成即唤醒重新调度（事件驱动流水线，而非批次栅栏）；槽位未释放前，其余就绪节点
   只能等待，等待谁完全由上面的优先级次序决定，而非 goroutine 启动先后。
6. **不重复调度**：节点被选中的瞬间在状态中置 `running` 占位；每个节点在整个生命周期内只会被
   **选中登记一次**（中断恢复后的重试是同一生命周期的延续，不会在调度记录中重复出现）。
   重试只发生在该节点的执行循环内部，不会产生并发状态冲突（可 `-race` 验证）。
7. **整体结算**：全部节点进入终态后结算——全部 `succeeded` 才算整体成功；
   存在 `failed` 或 `skipped` 即整体失败，二者在 `RunResult.Succeeded` 中严格区分。
8. **调度记录（新增）**：节点首次被选中的顺序持久化到 `WorkflowState.ScheduleOrder`，跨重启
   追加且不重复，事后可直接复核每一次调度选择。

### 定义错误归类（不静默忽略）

| 错误类型 | 触发条件 | 判定方式 |
| --- | --- | --- |
| `EmptyNameError` | 节点名为空 | `errors.As` |
| `DuplicateNodeError` | 同名节点出现多次 | `errors.As` |
| `MissingDependencyError` | 依赖指向不存在的节点（含节点名与依赖名） | `errors.As` |
| `CycleError` | 依赖图存在环（错误中给出环路径） | `errors.As` |
| `MissingTaskError` | 节点没有已注册任务体 | 运行时返回并落盘到节点状态 |

## 重试与退避规则

- `MaxAttempts` 为**总尝试次数上限（含首次）**；`<=0` 视为 1（不重试）。
- 第 `k` 次失败后、准备再试一次之前：先受节点自身 `MaxAttempts` 约束，再申请一次
  **全局共享重试预算**，扣到额度后才进入退避。
- 第 `k` 次失败后的退避：`InitialBackoff × Factor^(k-1)`，超过 `MaxBackoff` 则截断；
  `InitialBackoff<=0` 表示立即重试；退避等待期间同样响应取消。
- 尝试计数 `Attempts` 与节点已用重试数 `Retries`（= `Attempts-1`）都是持久化状态的一部分：
  **恢复后沿用历史计数**，重启不会"刷新"节点策略或全局预算。

### 全局共享重试预算（新增）

- `Config.MaxRetryBudget > 0` 时，预算是**一次运行跨所有节点共享**的重试总次数，只计首次成功
  启动之后的额外尝试：`总重试 = Σ NodeState.Retries = WorkflowState.RetriesUsed`。
- 分配规则是**先到先得**：哪个节点先失败并进入"准备重试"，就在引擎锁内扣一次；并发争抢由
  互斥锁串行化，账目不会超额、不会丢。授予的同时把这笔重试记到该节点名下并**同一次持锁落盘**，
  即使进程随后在退避中被杀掉，恢复后也不会重复申请或回滚这笔账。
- 预算扣完之后，其余节点的重试机会不再触发：节点记 `failed`，失败原因归类为
  `FailReasonBudgetExhausted`（`NodeState.FailReason == "retry budget exhausted"`），
  下游连带 `skipped`；节点自身 `MaxAttempts` 先到时则记 `FailReasonAttemptsExhausted`。
- `MaxRetryBudget <= 0`（含零值不配）表示**不设统一预算**，行为与旧版本完全一致：各节点仅受
  自身策略约束。为便于事后统计，此模式下仍会累计实际重尝试数到 `RetriesUsed` 与 `Retries`。
- 事后说清楚账：`RunResult.State.RetriesUsed` 给出总重试次数；每个节点的
  `NodeState.Retries` 给出各自用量；`FailReason` 给出"哪些节点是因为预算用完才没再重试"。

### 节点优先级（新增）

- 字段 `NodeDef.Priority`，默认 0，数值越大优先级越高；**仅在槽位不足、需要取舍时生效**，
  依赖关系与失败传播规则不受影响。
- 同优先级节点严格按**定义下标升序**挑选（等价于旧版本的定义顺序），配合状态持久化保证
  "同一份定义 + 同一份已有状态 ⇒ 相同调度顺序与结果"。

### 节点级 Timeout 与取消

- 节点级 `Timeout` 为**每次尝试**的超时，通过 `context.WithTimeout` 传播给任务体；
  引擎 `Run` 的 `ctx` 取消会传播到所有在途任务（包括执行中、退避中两种状态；节点在被选中
  时即已占用槽位，不存在"排队等额度"的窗口）。
- 任务体必须尊重 `ctx.Done()`，超时/取消以任务返回的 `ctx.Err()` 作为可解释的失败原因落盘。

## 恢复语义

进程重启后再次 `Run` 时，引擎从 `Store` 加载状态并按以下规则规整：

| 历史状态 | 恢复处理 |
| --- | --- |
| `succeeded` | 保持终态，**绝不重复执行** |
| `failed` 且已确认永久失败（`FailReason` 非空，或 `Attempts >= MaxAttempts`） | 保持 `failed`，不再调度；预算/策略耗尽的归类原样保留 |
| `failed` 且未确认永久失败 | 上次被取消/超时中断且预算未用尽 → 回到 `pending`，接续计数继续重试 |
| `skipped` | 上游失败连带跳过，保持终态 |
| `running` | 上次进程中断的在途残留（无终态记录）→ 回到 `pending` 并记录 `interrupted while running`，按重试策略继续 |
| 状态文件缺失 | 全新执行；定义中新增而状态里没有的节点同样保持 `pending` |

新增规则在中断/重启后的一致性：

- **预算不回滚、不重复扣**：`RetriesUsed` 与每节点 `Retries` 原样恢复；额度在授予时即与节点
  账目同一次持锁落盘，退避中被杀也不丢。恢复后继续失败但预算已尽的节点，直接以
  `FailReasonBudgetExhausted` 永久失败。
- **调度记录不丢不重**：`ScheduleOrder` 原样恢复；重试节点再次被选中不重复登记，已成功节点
  不重跑，后续节点按同一确定性次序追加。
- **终局判断一致**：成功 / 失败 / 取消的判定在重启前后保持一致（成功不重跑、永久失败不复活、
  连带跳过不翻案）；再次 `Run` 一个已终局的工作流不会执行任何任务，结果与上次完全相同。
- **旧定义兼容**：不写 `Priority`（零值）、不写 `MaxRetryBudget`（零值）时，调度次序与重试
  行为与旧版本一致——同优先级即按定义下标升序，等价于旧的定义顺序。
- **旧状态文件兼容**：旧 JSON 没有 `Retries` / `FailReason` / `RetriesUsed` / `ScheduleOrder`
  字段时按零值加载；`Retries` 由 `Attempts` 推导，`ScheduleOrder` 为真正发起过尝试
  （`Attempts>0`）的节点按定义顺序回填，其余恢复规则不变。

一致性保障：

- 状态写入采用**临时文件 + fsync + rename**，状态文件永远是完整的旧版或新版，不会出现半截 JSON；
- 所有状态变更经单把互斥锁串行化后立即落盘：调度占位与调度记录、每次尝试开始/结束、
  共享预算授予、终态结算均有记录；
- worker 回写时只向既有状态条目拷贝字段，不替换条目，避免覆盖调度占位造成状态回退。

## 验证方法

```bash
go test ./... -count=1 -v     # 查看每个用例的输入与判定依据日志
go test -race ./...           # 重复调度 / 并发状态冲突检测
go vet ./...
```

测试覆盖（`workflow/engine_test.go` 为原有场景，`workflow/feature_test.go` 为新增场景）：

- `TestValidateErrors`：空名 / 重复 / 缺失依赖 / 环四类错误归类与可区分原因；
- `TestValidateTopoOrder`、`TestExecuteInDependencyOrder`：拓扑序与执行顺序确定性；
- `TestConcurrencyLimit`：峰值并发严格等于上限（不超不限）；
- `TestRetryExhaustionAndSkip`：恰好 3 次尝试、退避耗时、下游 `skipped`、整体失败，
  以及重启后耗尽节点不再被调度；
- `TestNodeTimeout` / `TestCancelPropagation`：超时与取消传播到在途任务；
- `TestRestartResumeRetriesThenSuccess`：取消后重启，成功节点不重跑、失败节点接续计数重试成功；
- `TestRestartInterruptedRunning`：`running` 残留按中断处理；
- `TestNoDuplicateScheduling`：链式 DAG 中每个节点恰好执行一次。

新增（调度优先级 / 共享预算 / 重启一致性 / 兼容性）：

- `TestSharedRetryBudgetExhausted`：预算=2、两节点都失败（串行）——先扣的节点用掉全部额度，
  后者第 1 次失败后不再重试；校验总重试数、每节点用量、`FailReasonBudgetExhausted` 归类，
  以及重启后预算不回滚、节点不再执行、整体仍失败；
- `TestSharedBudgetFirstComeFirstServed`：12 节点 × 并发 6 同时失败争抢 5 次预算——
  全局计数与各节点计数之和守恒（都等于 5）、不超额、所有节点都被预算拦下；
- `TestPrioritySchedulingUnderConcurrencyLimit`：槽位=2、4 个不同优先级就绪节点——
  第一批恰好是最高优先级的两个；`ScheduleOrder` 为确定的 `[critical mid low other]`
  （同优先级按定义下标升序）；
- `TestPriorityOrderReproducibleAcrossRestart`：槽位紧张时中断再恢复——
  中断前记录 `[p10 p5a]`，恢复后完整记录 `[p10 p5a p5b p0a p0b]`，已登记节点不重复、
  其余按同一次序追加；
- `TestRestartKeepsBudgetAndScheduleRecord`：长退避中取消（已扣 1 次预算）→ 重启 →
  预算保持 1、b 再试一次后被预算耗尽永久失败、c 被连带跳过、调度记录不重不丢；
  第三次启动验证终局稳定，不执行任何节点且成败结论一致；
- `TestLegacyConfigUnchanged`：不设预算、不设优先级的旧配置形态，调度顺序仍为定义序
  `[a b c]`、无预算字段写入、整体成功；
- `TestLegacyStateFileCompat`：手工构造的旧版本状态文件（无新增字段）可正常恢复，
  失败节点接续第 2 次尝试成功并回填调度记录。

单测日志通过 `t.Logf` 打印输入参数与判定依据，`Engine.WithLogger(t.Logf)` 会额外输出
`[schedule]` / `[run]` / `[done]` / `[skip]` 的完整调度轨迹。

## 状态文件示例

```json
{
  "WorkflowName": "my-wf",
  "Nodes": {
    "a": {"Status": "succeeded", "Attempts": 1, "Retries": 0, "LastError": "", "FailReason": "", ...},
    "b": {"Status": "failed", "Attempts": 3, "Retries": 2, "FailReason": "retry budget exhausted", "LastError": "..."},
    "downstream": {"Status": "skipped", "Attempts": 0, "Retries": 0, ...}
  },
  "RetriesUsed": 2,
  "ScheduleOrder": ["a", "b"],
  "Finished": true,
  "Succeeded": false,
  "UpdatedAt": "2026-09-23T00:00:00Z"
}
```
