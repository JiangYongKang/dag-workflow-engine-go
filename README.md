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
| `workflow/executor.go` | 单节点执行循环：重试预算、指数退避、节点超时、取消传播 |
| `workflow/engine.go` | 调度器：就绪判定、并发信号量、失败传播跳过、恢复规整 |
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
    },
    workflow.NewFileStore("state/wf.json"))
if err != nil { /* 定义非法，见下“错误归类” */ }

res, err := engine.Run(ctx)
res.Succeeded // 整体成功 / 整体失败可区分
res.Resumed   // 本次是否基于历史状态恢复
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
4. **并发约束**：就绪 worker 通过容量为 `MaxConcurrency` 的信号量获取执行额度；
   调度器不预占额度，worker 完成即唤醒重新调度（事件驱动流水线，而非批次栅栏）。
5. **不重复调度**：节点被选中的瞬间在状态中置 `running` 占位；每个节点在整个生命周期内只会被选中一次。
   重试只发生在该节点的执行循环内部，不会产生并发状态冲突（可 `-race` 验证）。
6. **整体结算**：全部节点进入终态后结算——全部 `succeeded` 才算整体成功；
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
  引擎 `Run` 的 `ctx` 取消会传播到所有在途任务（包括排队等额度、执行中、退避中三种状态）。
- 任务体必须尊重 `ctx.Done()`，超时/取消以任务返回的 `ctx.Err()` 作为可解释的失败原因落盘。

## 恢复语义

进程重启后再次 `Run` 时，引擎从 `Store` 加载状态并按以下规则规整：

| 历史状态 | 恢复处理 |
| --- | --- |
| `succeeded` | 保持终态，**绝不重复执行** |
| `failed` 且 `Attempts >= MaxAttempts` | 重试预算耗尽的永久失败，保持 `failed`，不再调度 |
| `failed` 且 `Attempts < MaxAttempts` | 上次被取消/超时中断且预算未用尽 → 回到 `pending`，接续计数继续重试 |
| `skipped` | 上游失败连带跳过，保持终态 |
| `running` | 上次进程中断的在途残留（无终态记录）→ 回到 `pending` 并记录 `interrupted while running`，按重试策略继续 |
| 状态文件缺失 | 全新执行；定义中新增而状态里没有的节点同样保持 `pending` |

一致性保障：

- 状态写入采用**临时文件 + fsync + rename**，状态文件永远是完整的旧版或新版，不会出现半截 JSON；
- 所有状态变更经单把互斥锁串行化后立即落盘：调度占位、每次尝试开始/结束、终态结算均有记录；
- worker 回写时只向既有状态条目拷贝字段，不替换条目，避免覆盖调度占位造成状态回退。

## 验证方法

```bash
go test ./... -count=1 -v     # 查看每个用例的输入与判定依据日志
go test -race ./...           # 重复调度 / 并发状态冲突检测
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
- `TestNoDuplicateScheduling`：链式 DAG 中每个节点恰好执行一次。

单测日志通过 `t.Logf` 打印输入参数与判定依据，`Engine.WithLogger(t.Logf)` 会额外输出
`[schedule]` / `[run]` / `[done]` / `[skip]` 的完整调度轨迹。

## 状态文件示例

```json
{
  "WorkflowName": "my-wf",
  "Nodes": {
    "a": {"Status": "succeeded", "Attempts": 1, ...},
    "b": {"Status": "failed", "Attempts": 3, "LastError": "boom on attempt 3"}
  },
  "Finished": true,
  "Succeeded": false,
  "UpdatedAt": "2026-09-23T00:00:00Z"
}
```
