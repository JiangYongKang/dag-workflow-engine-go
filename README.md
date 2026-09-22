# 本地 DAG 工作流编排引擎（Go）

一个可在单进程内运行、可组合组件的工作流编排引擎：按依赖关系调度 DAG 节点，
支持并发度约束、按节点的指数退避重试、超时与取消传播、JSON 文件持久化与进程重启后的断点恢复。
同一份工作流定义在不同重试 / 重启场景下产出确定且可解释的结果。

- 模块路径：`github.com/highcumontoa/dag-workflow-engine-go/dag`
- Go 版本：1.25

## 组件一览

| 文件 | 职责 |
| --- | --- |
| `dag/types.go` | 工作流 / 节点定义、状态结构（`WorkflowState` / `NodeState` / `AttemptRecord`）与深拷贝 |
| `dag/errors.go` | 可区分的哨兵错误（重复节点、依赖缺失、环、配置非法等）与 `DefinitionError` |
| `dag/validate.go` | 定义校验（确定性检查顺序）与分层拓扑排序 `TopoLevels` |
| `dag/retry.go` | 每节点 `RetryPolicy`：最大尝试次数、指数退避与退避上限 |
| `dag/store.go` | 状态存储抽象 `Store`、`MemoryStore`、原子落盘的 `FileStore` |
| `dag/engine.go` | 编排引擎：调度循环、并发信号量、重试 / 退避、超时、取消、恢复转换、终态判定 |
| `example/main.go` | 可运行示例（含一个会先失败后成功的 flaky 节点） |

## 快速开始

```go
store, _ := dag.NewFileStore("./.workflow-state")
eng := dag.NewEngine(dag.Config{MaxConcurrency: 2, DefaultTimeout: 5 * time.Second, Logger: logger}, store)

def := &dag.WorkflowDef{
    ID: "etl",
    Nodes: []dag.NodeDef{
        {ID: "extract", Run: extractFunc},
        {ID: "transform", Deps: []string{"extract"}, Run: transformFunc,
         Retry: dag.RetryPolicy{MaxAttempts: 3, InitialDelay: 100 * time.Millisecond, BackoffFactor: 2}},
        {ID: "load", Deps: []string{"transform"}, Run: loadFunc, Timeout: 2 * time.Second},
    },
}
st, err := eng.Run(ctx, def)
fmt.Println(st.Status) // succeeded / failed
```

运行示例：

```bash
go run ./example ./.workflow-state   # 首次运行（含一次失败重试）
go run ./example ./.workflow-state   # 再次运行：终态幂等，任何节点都不会重跑
rm -rf ./.workflow-state             # 清空状态重新开始
```

## 调度模型

1. **定义校验（失败原因可区分，绝不静默忽略）**，按固定顺序返回首个错误：
   1. 空 ID / 重复 ID → `ErrDuplicateNode`（包装在 `*DefinitionError` 中，含节点名）
   2. 依赖指向不存在的节点 → `ErrDependencyNotFound`
   3. 环形依赖（含自依赖）→ `ErrCycleDetected`（Kahn 入度剥离，错误信息列出环上节点）
2. **就绪判定**：节点所有依赖均为 `succeeded` 时进入就绪集合；同一层（`TopoLevels`）节点间互不依赖。
3. **确定性**：节点按 ID 排序扫描，因此在相同输入下调度顺序、终态与状态文件内容确定。
4. **并发约束**：`Config.MaxConcurrency` 通过信号量限制"同时在执行单次尝试"的节点数；
   等待退避的节点不占用槽位。节点在派发瞬间于状态机中占位为 `running`，
   因此同一节点不可能被重复派发或产生并发状态冲突。
5. **失败传播**：节点 `failed` 后，所有直接 / 间接依赖它的未开始节点标记为 `skipped`
   （与 `failed` 区分：`skipped` 表示从未执行），`skipped` 会级联。
6. **整体结果**：只要存在 `failed` 节点，工作流为 `failed`（`st.Error` 记录首个失败节点）；
   全部节点 `succeeded` 才为 `succeeded`。两者是明确可区分的终态。

## 重试与退避规则

每节点独立配置 `RetryPolicy`：

- `MaxAttempts`：总尝试次数（含首次），`<=0` 视为 1（不重试）。
- 第 n 次失败后的等待：`InitialDelay * BackoffFactor^(n-1)`，以 `MaxDelay` 封顶；
  `BackoffFactor < 1` 时按 1 处理；`InitialDelay=0` 表示立即重试。
- 最后一次允许的尝试失败后 `BackoffDelay` 返回 0，节点永久 `failed`，不会再次被调度。
- 退避期间节点状态回到 `pending` 但不占并发槽位；退避等待可被 `ctx` 取消中断。

**超时**：`NodeDef.Timeout`（缺省用 `Config.DefaultTimeout`）通过 `context.WithTimeout`
包裹节点函数；超时通过 `ctx.Done()` 传播到在途任务，节点须主动尊重 context。
超时返回的错误按普通失败处理：有剩余预算则重试，耗尽则 `failed`。

**取消**：父 `ctx` 取消时——

- 尚未开始的节点立即标记 `skipped`；
- 已在途的节点经 context 收到取消，本次尝试记为失败且不再重试，节点终态为 `failed`；
- 整体工作流终态为 `failed`，与正常成功明确区分。

## 持久化与断点恢复语义

- `FileStore` 以 `<id>.json` 保存状态，采用"临时文件 + `rename`"原子提交，
  崩溃后只会读到上一份完整状态。每次状态转换（尝试开始 / 结束 / 终态变化）都落盘。
- 进程重启后对同一工作流再次 `Run`：
  - **已 `succeeded` 的节点绝不重跑**；
  - 崩溃时停留在 `running` 的节点，其最后一次未闭合尝试记为
    `interrupted by process restart`，然后按重试预算决定：
    - 仍有剩余尝试 → 回到 `pending` 重新调度（中断的那次尝试**计入** `Attempts`）；
    - 预算已耗尽 → 标记 `failed`，其下游 `skipped`，整体 `failed`；
  - 退避计时不跨进程：恢复后不再睡眠，剩余尝试立即重新调度（次数仍受 `MaxAttempts` 约束）；
  - 已处于终态（`succeeded` / `failed`）的工作流再次 `Run` 直接返回已存状态，幂等不执行。
- 定义与状态的一致性：已持久化节点若从定义中删除会报错拒绝；新增节点以 `pending` 加入。
- 状态写入失败被视为致命错误（包装 `*fatalError` 中止 `Run`），
  不会被误判为节点业务失败后继续调度，避免内存状态与持久化状态分叉。

## 验证方法

```bash
go test ./...                 # 全部单测（日志打印输入与判定依据）
go test -race -count=1 ./...  # 竞态检测
go test -v ./...              # 查看详细调度/重试/恢复判定日志
go vet ./...
```

测试覆盖：

| 场景 | 测试 |
| --- | --- |
| 重复节点 / 依赖缺失 / 环 / 自依赖，错误可区分 | `TestValidateRejects*`、`TestErrorKindsAreDistinguishable` |
| 拓扑分层 | `TestTopoLevelsOrdering` |
| 全局并发上限不被突破 | `TestConcurrencyLimitEnforced` |
| 成功节点不重复执行 + 终态重跑幂等 | `TestDuplicateExecutionDetectedViaExecCount` |
| 指数退避后成功 / 重试耗尽失败并跳过下游 | `TestRetryBackoffThenSuccess`、`TestRetryExhaustedMarksFailedAndSkipsDownstream` |
| 退避时长计算 | `TestBackoffDelayMath` |
| 节点超时传播与重试 | `TestNodeTimeoutPropagatesAndRetries` |
| 取消传播到在途任务、未开始节点跳过 | `TestCancellationPropagatesToInFlight` |
| 重启后成功节点不重跑、中断节点按预算重试、整体成功 | `TestRestartRecoverySucceedsWithRetriesLeft` |
| 重启后预算耗尽 → 整体失败可区分、节点不重跑 | `TestRestartRecoveryFailsWhenBudgetExhausted` |
| 文件存储原子读写与 not-found 分类 | `TestFileStoreRoundTrip` |
