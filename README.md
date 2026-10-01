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
| `workflow/executor.go` | 单节点执行循环：节点重试上限、共享预算申请回调、指数退避、节点超时、取消传播 |
| `workflow/engine.go` | 调度器：优先级就绪挑选、槽位限并发、共享预算扣减、失败传播跳过、恢复规整 |
| `cmd/demo/main.go` | 可运行示例 |

## 快速开始

```go
nodes := []workflow.NodeDef{
    {Name: "a"},
    {Name: "b", DependsOn: []string{"a"}, Timeout: 2 * time.Second, Priority: 10},
    {Name: "c", DependsOn: []string{"b"}}, // Priority 不填默认 0，旧定义无需改动
}
reg := workflow.TaskRegistry{
    "a": func(ctx context.Context, node string, attempt int) error { return nil },
    // ...
}
engine, err := workflow.NewEngine("my-wf", nodes, reg,
    workflow.Config{
        MaxConcurrency: 4,
        MaxRetries:     10, // 跨所有节点共享的重试总次数；不填（<=0）即旧用法
        DefaultRetry: workflow.RetryPolicy{
            MaxAttempts: 3, InitialBackoff: 100 * time.Millisecond,
            MaxBackoff: time.Second, Factor: 2,
        },
    },
    workflow.NewFileStore("state/wf.json"))
if err != nil { /* 定义非法，见下“错误归类” */ }

res, err := engine.Run(ctx)
res.Succeeded // 整体成功 / 整体失败可区分
res.Resumed   // 本次是否基于历史状态恢复
report := res.State.RetryReport() // 预算总用量 / 每节点用量 / 预算阻断节点
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
3. **失败传播**：任一依赖进入永久终态（`failed` / `skipped`），该节点标记为 `skipped`，永不执行。
4. **并发约束**：调度器只为当前空闲的槽位（`MaxConcurrency` 减去在途节点数）派发 goroutine，
   worker 完成即唤醒重新调度（事件驱动流水线，而非批次栅栏），因此并发峰值严格等于上限。
5. **优先级挑选**：同一时刻有多个就绪节点而槽位不足时，按 `NodeDef.Priority` **降序**挑选；
   优先级相同则按**定义顺序**挑选。因为槽位是"空一个补一个"地重新挑选，高优先级节点一旦因
   依赖完成而就绪，下一个空出的槽位一定先给它，不会被早已排队的低优先级长任务长期占住。
   挑选键完全确定，同一份定义配同一份已有状态，调度轨迹可复现（见
   `TestPrioritySchedulingUnderConcurrency`）。`Priority` 不填时全部为 0，退化为按定义顺序，
   与旧行为一致。
6. **不重复调度**：节点被选中的瞬间在状态中置 `running` 占位；每个节点在整个生命周期内只会被选中一次。
   重试只发生在该节点的执行循环内部，不会产生并发状态冲突（可 `-race` 验证）。
7. **整体结算**：全部节点进入终态后结算——全部 `succeeded` 才算整体成功；
   存在 `failed` 或 `skipped` 即整体失败，二者在 `RunResult.Succeeded` 中严格区分。

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
- 第 `k` 次失败后的退避：`InitialBackoff × Factor^(k-1)`，超过 `MaxBackoff` 则截断；
  `InitialBackoff<=0` 表示立即重试；退避等待期间同样响应取消。
- 尝试计数 `Attempts` 是持久化状态的一部分：**恢复后沿用历史计数**，重启不会"刷新"重试预算，
  因此任何节点的总尝试次数都不会超过 `MaxAttempts`。
- 节点级 `Timeout` 为**每次尝试**的超时，通过 `context.WithTimeout` 传播给任务体；
  引擎 `Run` 的 `ctx` 取消会传播到所有在途任务（执行中、退避中）；未派发的节点
  不占槽位，取消后直接随运行终止并在下次运行时继续调度。
- 任务体必须尊重 `ctx.Done()`，超时/取消以任务返回的 `ctx.Err()` 作为可解释的失败原因落盘。

## 运行级共享重试预算（`Config.MaxRetries`）

除每个节点各自的 `MaxAttempts` 外，可以给**一次运行**设一个跨所有节点共享的重试总次数：

- 每个节点的**首次尝试不扣预算**；首次失败之后，每发起一次重试从共享预算扣 1，**谁先申请谁先扣**。
- 预算扣完后，再申请重试的节点不再发起尝试，立即永久失败：节点状态为 `failed` 且
  `NodeState.BudgetBlocked=true`，`LastError` 保留最近一次真实失败原因。
  该节点的下游照常按失败传播规则标记 `skipped`。
- 节点自身的 `MaxAttempts` 仍然生效：达到自身上限的节点直接失败，**不占用**共享预算。
- `MaxRetries<=0`（零值）表示不启用共享预算，重试只受各节点 `MaxAttempts` 约束，即旧用法。

事后通过 `state.RetryReport()` 可解释预算去向：

| 字段 | 含义 |
| --- | --- |
| `Budget` | 共享重试总次数（0 表示不限制） |
| `Used` | 本次运行已消耗的重试总次数（跨所有节点、跨重启累计） |
| `PerNode` | 每个节点各自消耗的重试次数（`Attempts-1`，首次尝试不计） |
| `BudgetBlocked` | 因共享预算耗尽而未再重试的节点名（按名称排序） |

`RetryBudget` 与 `RetriesUsed` 都是 `WorkflowState` 的持久化字段：进程重启后沿用，不会重置；
预算扣减与节点 `Attempts` 在同一把锁、同一次落盘内完成，不会出现"预算扣了但尝试没记账"。
注意预算总量取自**首次启动时的配置并落盘**，之后即使配置改动，恢复的仍是同一份运行，
预算不随之改变（与"一次运行一个预算"的语义一致）。

## 恢复语义

进程重启后再次 `Run` 时，引擎从 `Store` 加载状态并按以下规则规整：

| 历史状态 | 恢复处理 |
| --- | --- |
| `succeeded` | 保持终态，**绝不重复执行** |
| `failed` 且 `Attempts >= MaxAttempts` | 节点自身重试上限耗尽的永久失败，保持 `failed`，不再调度 |
| `failed` 且 `BudgetBlocked=true` | 运行级共享预算耗尽的永久失败，保持 `failed`，不再调度 |
| `failed` 且预算未用尽 | 上次被取消/超时中断 → 回到 `pending`，接续计数继续重试（仍受共享预算剩余量约束） |
| `skipped` | 上游失败连带跳过，保持终态 |
| `running` | 上次进程中断的在途残留（无终态记录）→ 回到 `pending` 并记录 `interrupted while running`，按重试策略继续 |
| 状态文件缺失 | 全新执行；定义中新增而状态里没有的节点同样保持 `pending` |

重启前后的判定一致性：

- 已成功节点不重跑、已消耗的 `Attempts`/`RetriesUsed`/`RetryBudget` 不重置、
  `BudgetBlocked` 永久失败保持终态；
- 中断发生在退避/执行窗口时，终态尚未结算（`Finished=false`），恢复后继续跑到终态再判定；
  一旦已结算（`Finished=true`），后续任意次 `Run` 的 `Succeeded` 结果保持不变。

一致性保障：

- 状态写入采用**临时文件 + fsync + rename**，状态文件永远是完整的旧版或新版，不会出现半截 JSON；
- 所有状态变更经单把互斥锁串行化后立即落盘：调度占位、每次尝试开始/结束、终态结算均有记录；
- worker 回写时只向既有状态条目拷贝字段，不替换条目，避免覆盖调度占位造成状态回退。

## 对已有定义与已有状态的兼容

- **旧定义不改即兼容**：`NodeDef.Priority` 与 `Config.MaxRetries` 的零值分别表示
  "默认优先级（全部相等）"与"不启用共享预算"。旧代码不填这两个字段时，调度顺序
  （同优先级按定义顺序）、重试次数与成败结果与升级前完全一致，由
  `TestLegacyConfigUnchanged` 锁定。
- **旧状态文件可直接加载**：旧版 JSON 没有 `RetryBudget`/`RetriesUsed`/`BudgetBlocked`
  字段时，反序列化为零值，等价于"未启用共享预算"；旧状态中的 `failed` 节点仍按
  原 `Attempts >= MaxAttempts` 规则判定是否永久失败。新字段都是追加字段，不改动原有字段含义。
- 新规则只对**新发起**的运行生效：用旧状态恢复时，预算为 0（不限制），行为与旧引擎一致；
  只有首次以 `MaxRetries>0` 的配置启动的运行才会落盘并使用共享预算。

## 验证方法

```bash
go test ./... -count=1 -v     # 查看每个用例的输入与判定依据日志
go test -race ./...           # 重复调度 / 并发状态冲突检测
go test ./workflow/ -run 'TestPrioritySchedulingUnderConcurrency|TestSharedRetryBudgetExhaustion|TestRestartBudgetAndSchedulingPersist' -count=20  # 重复跑验证调度确定性
go vet ./...
```

测试覆盖：

- `TestValidateErrors`：空名 / 重复 / 缺失依赖 / 环四类错误归类与可区分原因；
- `TestValidateTopoOrder`、`TestExecuteInDependencyOrder`：拓扑序与执行顺序确定性；
- `TestConcurrencyLimit`：峰值并发严格等于上限（不超不限）；
- `TestRetryExhaustionAndSkip`：恰好 3 次尝试、退避耗时、下游 `skipped`、整体失败，
  以及重启后耗尽节点不再被调度；
- `TestNodeTimeout` / `TestCancelPropagation`：超时与取消传播到在途任务；
- `TestRestartResumeRetriesThenSuccess`：取消后重启，成功节点不重跑、失败节点接续计数重试成功；
- `TestRestartInterruptedRunning`：`running` 残留按中断处理；
- `TestNoDuplicateScheduling`：链式 DAG 中每个节点恰好执行一次；
- `TestSharedRetryBudgetExhaustion`：共享预算先到先扣、扣完后受影响节点直接
  `failed` + `BudgetBlocked`，并通过 `RetryReport` 核对总量、每节点用量与阻断节点；
- `TestPrioritySchedulingUnderConcurrency`：并发槽位紧张时首批槽位归最高优先级节点、
  空槽逐个按优先级降序与同优先级定义顺序补位，每个节点恰好执行一次；
- `TestRestartBudgetAndSchedulingPersist`：中断后预算已用量/总量、成功节点、预算阻断
  终态跨三次进程保持一致，整体成败判定重启前后不矛盾；
- `TestLegacyConfigUnchanged`：不设预算与优先级时调度顺序、重试耗尽语义与结果均不变。

单测日志通过 `t.Logf` 打印输入参数与判定依据，`Engine.WithLogger(t.Logf)` 会额外输出
`[schedule]` / `[run]` / `[done]` / `[skip]` 的完整调度轨迹。

## 状态文件示例

```json
{
  "WorkflowName": "my-wf",
  "Nodes": {
    "a": {"Status": "succeeded", "Attempts": 1, ...},
    "b": {"Status": "failed", "Attempts": 3, "LastError": "boom on attempt 3", "BudgetBlocked": true}
  },
  "Finished": true,
  "Succeeded": false,
  "RetryBudget": 5,
  "RetriesUsed": 5,
  "UpdatedAt": "2026-09-23T00:00:00Z"
}
```
