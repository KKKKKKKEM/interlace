# 第六章：Runtime 内部架构

前五章描述用户可观察的行为。本章转向实现内部：Runtime 如何通过插件装配角色，一次直接执行和一次事件执行分别
经过哪些组件，以及各层为什么保持独立。

## 分层总览

| 层次 | 源码边界 | 对象 | 责任 |
| --- | --- | --- | --- |
| 用户模型 | `interlace`（Graph、Node、Output、Context、Event）与 `interlace.engine` | Node、Output、Edge、Graph、Event、Context、InputPolicy、ExecutionPlan、Execution、Slot、SlotPool | 描述业务计算与可观察执行状态 |
| 用户门面 | `interlace.runtime`，由 `interlace` 导出 | Runtime | 注册 Graph，代理执行、事件、观察和控制入口，公开钩子总线 `hooks` |
| 装配宿主 | `interlace.plugins`、`interlace.runtime.plugin` | PluginHost、ExtensionAPI、LocalInfrastructurePlugin、LocalRuntimePlugin | 解析插件依赖，提供与装饰具名值，管理钩子总线与插件清理 |
| 运行角色 | `interlace.runtime` | EventRouter、GraphWorker | 分别处理 Event -> Work 与 Work -> Graph execution |
| 能力端口 | `interlace.spi` | RouterRole、WorkerRole（即 ExecutionHost）、EventBus、任务传输、GraphExecutor、SlotProvider、执行资源协议 | 隔离编排、传输、执行和资源实现 |
| 默认适配器 | `interlace.adapters.memory`、`interlace.engine.executor` | EventBus、TaskBackend、Engine | 提供单进程内存运行时 |
| 可选官方服务 | `interlace.service` | GraphService、NodeCatalog、Studio | 提供 RPC、图编辑发布与观测历史，按 extra 安装 |

Runtime 是面向应用的稳定门面，PluginHost 是系统装配根。普通应用不需要看到后四层。

## 源码目录与包职责

源码目录按照架构层次组织，而不是把所有运行时职责都放入 `engine`：

```text
interlace/
├── __init__.py              # 宪法规定的顶层公共 API（六个名字）
├── engine/                  # Graph 执行微内核及受控内核扩展点
│   ├── core.py              # Node、Output、InputPolicy 与冻结端口快照
│   ├── graph.py             # Edge、Graph、ExecutionPlan 与冻结校验
│   ├── execution.py         # Execution 句柄、限制与状态
│   ├── execution_resources.py # 输出存储与等待通知协议及默认实现
│   ├── executor.py          # 单张冻结 Graph 的默认执行器 Engine
│   ├── events.py            # Event 与 Context
│   ├── slots.py             # Slot 与 SlotPool
│   ├── extension.py         # 两种钩子点、HookBus、具名值与 Registration
│   ├── points.py            # 节点调用、采集与生命周期事实的核心钩子点
│   ├── hooks.py             # NodeCall、NodeInspection 与 StopGraph
│   ├── policies.py          # selector 契约、内建 selector 与冻结绑定
│   ├── observation.py       # 只读 Runtime 生命周期事实
│   ├── errors.py            # 内核和运行控制错误
│   └── runner.py            # 同步/异步 Node 调用桥接
├── runtime/                 # Runtime 门面和运行角色
│   ├── facade.py            # Runtime
│   ├── router.py            # EventRouter
│   ├── worker.py            # GraphWorker：注册、消费与执行宿主编排
│   ├── history.py           # Execution 记录保留与活跃计数
│   ├── direct.py            # 直接执行线程池
│   ├── points.py            # Router、Worker 与 Runtime 生命周期的核心钩子点
│   ├── plugin.py            # LocalInfrastructurePlugin、LocalRuntimePlugin
│   └── _utils.py            # 私有生命周期状态机与关闭辅助函数
├── plugins/                 # PluginHost、ExtensionAPI、descriptor、capability 与 points 重导出
├── spi/                     # EventBus、任务传输、GraphExecutor 等窄协议
├── adapters/                # 随包提供的具体部署适配器
│   └── memory.py            # memory.EventBus、memory.TaskBackend
├── nodes/                   # 可复用的非内核 Node，例如 KeyedJoin
└── service/                 # 按需安装的官方 RPC 与 Studio
    ├── catalog.py           # 显式节点工厂与图文档编译
    ├── models.py            # JSON 传输和编辑文档模型
    ├── service.py           # 协议共用的图服务
    ├── store.py             # 草稿、版本、观测与输出历史
    ├── http.py              # HTTP、JSON-RPC、SSE 与 ASGI 装配
    └── static/              # 随 Python 包交付的已构建工作台
```

`interlace.engine` 不再充当高级 API 聚合入口。普通应用只从 `interlace` 导入；插件、SPI 和基础设施作者根据职责从
`interlace.runtime`、`interlace.plugins`、`interlace.spi`、`interlace.adapters`、`interlace.nodes` 或具体 `interlace.engine.*` 模块导入。
仓库不保留旧模块路径的兼容 re-export。

## 依赖方向

包依赖必须保持单向，Runtime 是组合这些层次的门面，而不是被内核反向调用：

```mermaid
flowchart LR
    Public[interlace 顶层 API] --> Engine[interlace.engine]
    Public --> Runtime[interlace.runtime]
    Runtime --> Engine
    Runtime --> SPI[interlace.spi]
    Runtime --> Plugins[interlace.plugins]
    Runtime --> Adapters[interlace.adapters]
    SPI --> Engine
    Adapters --> SPI
    Adapters --> Engine
    Plugins --> Engine
    Plugins --> SPI
    Nodes[interlace.nodes] --> Engine
    Service[interlace.service] --> Public
```

这里的关键约束是：

- `engine` 不依赖 `runtime`、`plugins`、`spi`、`adapters` 或可复用 `nodes`；
- `spi` 只引用协议签名所需的 Graph、Event、Execution 和 Slot 模型；
- `adapters` 实现 SPI，可以引用搬运 Event/Work 所需的最小内核类型；
- `plugins` 管理装配元数据、钩子总线与生命周期，只依赖公开 capability，不读取 Runtime 私有状态；
- `runtime` 可以依赖前述各层并完成组合，但不得把部署算法重新实现到门面中；
- `nodes` 只放可复用的非内核 Node，只依赖内核公共语义，不能成为 Runtime 的隐式前置条件。
- `service` 依赖公开 Runtime、Graph、Execution 及观察模型；核心不导入服务或 Web 依赖。`web/` 保存工作台源代码。

服务通过 `Runtime.graphs()` 读取已注册图的只读快照；默认 GraphWorker 实现 `interlace.spi.GraphCatalog`。
替代 Worker 若需启用图检查与服务，同样实现该窄协议。服务不回退读取默认 Worker 的私有注册表。

## 默认装配

```mermaid
flowchart TB
    Runtime --> Host[PluginHost]
    Host --> Bus[(HookBus)]
    Host --> Infra[LocalInfrastructurePlugin]
    Host --> Local[LocalRuntimePlugin]
    Host --> Features[应用插件]

    Infra --> EventBus
    Infra --> Tasks[TaskBackend]
    Infra --> Engine
    Local --> Router[EventRouter]
    Local --> Worker[GraphWorker]
    Router --> EventBus
    Router --> Tasks
    Worker --> Tasks
    Worker --> Engine

    Features -->|on / provide| Bus
    Router -->|EVENT_EMIT / WORK_ROUTE / RUNTIME_EVENT| Bus
    Worker -->|WORK_DELIVER / EXECUTION_RUN / 查找 selector| Bus
    Engine -->|NODE_CALL / NODE_INSPECT| Bus
```

`Runtime()` 汇总插件声明：未被提供的角色由 `LocalRuntimePlugin(roles=...)` 补齐；补齐角色需要、或任一插件依赖或装饰
却无人提供的基础设施由 `LocalInfrastructurePlugin(provides=...)` 补齐。内建实现和应用插件使用相同的依赖解析、
capability 提供、装饰与清理流程，Runtime 只有这一种装配路径。

PluginHost 拥有 Runtime 唯一的 HookBus。角色与 Engine 在构造时取得同一总线，并在固定边界触发核心钩子点；插件在
setup 或运行期登记处理函数。扩展行为因此不属于角色协议：替代角色只要以 `api.hooks` 构造，就自动支持全部插件。

## 直接执行路径

`run()`、`start()`、`iter()`、`aiter()` 和 `arun()` 最终共享同一条执行路径：

```mermaid
flowchart LR
    App[Application] --> Runtime
    Runtime --> Worker[GraphWorker]
    Worker --> Executor[GraphExecutor]
    Executor --> Graph[Frozen Graph]
    Graph --> Result[Terminal Output / Execution]
```

GraphWorker 管理注册表、Execution 句柄和直接执行线程；GraphExecutor 只负责执行一张已经冻结的 Graph。这个边界
允许替换执行器，而不把 Graph 注册、队列消费或事件路由混入执行算法。

直接执行线程池大小由 `GraphWorker(direct_workers=...)` 指定，None 使用标准库 ThreadPoolExecutor 的默认值。Worker 最多保留
`history_limit`（默认 1000）条 Execution 记录，超出时按登记顺序淘汰已结束的记录，尚未收尾的执行始终保留；两个参数均可经
`LocalRuntimePlugin(history_limit=..., direct_workers=...)` 传入。`idle` 与 `wait_idle()` 只统计尚未收尾的执行，不随历史记录数增长。

执行宿主在调用 `succeed()`/`fail()` 唤醒等待方之前发布 `execution.finished`，状态由 `terminal_status(error)` 计算；因此
`run()` 返回或 `Execution.wait()` 醒来时，观察者已经收到终态事件，而观察者回调内读到的 `execution.status` 可能仍为 RUNNING，
应以事件的 `status` 与 `error_type` 为准。写入成功前宿主先执行一次 `checkpoint()`，使取消与超时反映在终态事件中。

Runtime 按 `RouterRole`、`WorkerRole` 接受结构化实现，具体的 EventRouter 与 GraphWorker 是默认角色实现。
ExecutionFactory 为直接执行和队列 Work 创建同一类句柄，可注入 OutputStore 与 ExecutionNotifier。宿主调用公开的
`execution.start(graph)`、`succeed()` 和 `fail(error)`；执行器调用 `step()`、`checkpoint()` 与输出交付接口，
不负责切换 execution 的最终状态。

GraphExecutor 同步执行后返回 None，也可以返回 Awaitable[None]。默认 GraphWorker 会等待异步执行及其取消清理
真正结束，仍占用该次 execution 的消费并发；需要其他调度方式时可替换 WorkerRole。两种执行器都通过统一输出接口
交付结果，Worker 不再根据最终返回值补发输出或强制读取完整 tuple。

Node 使用同一个基类声明行为。Engine 调用 `execute()` 后检查实际返回值：Awaitable 由运行器等待，完成后再按
Output、普通 iterable 或 None 处理。普通 `def` 和 `async def` 共享这条路径；异步生成器不在 Node 返回契约内。

## 事件执行路径

```mermaid
sequenceDiagram
    participant Source as Source Graph
    participant Router as EventRouter
    participant Bus as EventBus
    participant Tasks as TaskBackend
    participant Worker as GraphWorker
    participant Target as Target Graph

    Source->>Router: Context.emit(Event)
    Router->>Bus: publish(Event)
    Bus->>Router: matching route handler
    Router->>Tasks: submit(Work)
    Tasks->>Worker: Delivery(work, attempt, local lease)
    Worker->>Target: execute(payload)
    Worker-->>Tasks: DeliveryResult
```

EventRouter 不需要目标 Graph 定义，只负责把匹配 Event 转成 Work。GraphWorker 不需要源 Event，只按 Work 中的
注册名寻找 Graph。两者可以位于不同进程，只要 EventBus 和任务传输实现对应部署语义；进程边界不会延续 Slot，
接收端 TaskConsumer 会为该 Work 开始新的本地 Slot 链。

`Work.id` 同时作为本地 Execution ID。同一 id 的重投递在前一次 Execution 已收尾时替换其保留记录，`get_execution(id)` 返回最新一次
尝试；前一次仍在执行时 Worker 以 REJECT 拒绝本次交付，不并发执行同一 Work。

## Graph 冻结边界

Runtime 注册 Graph 时，GraphWorker 以钩子总线上当前提供的具名值快照绑定输入 selector 并冻结 Graph：

```mermaid
flowchart LR
    Build[构建 Graph] --> Reach[校验入口与可达性]
    Reach --> Ports[校验端口与 Edge 类型]
    Ports --> Policy[绑定 InputSelector 快照]
    Policy --> Timeout[固定 Node timeout]
    Timeout --> Frozen[可执行的 frozen Graph]
```

冻结后，节点 binding、Edge、端口和输入策略不会变化。ExecutionPlan 只能选择原 Graph 中已有的节点与 Edge，不能
跨过未选择节点自动补边。

## Engine 调度循环

默认 Engine 为每个 Node input port 维护 FIFO token 队列，并用 FIFO ready queue 调度 Node：

1. 把入口输入放入 entrypoint 队列；
2. InputSelector 只根据端口和可用 token 数选择本次消费组合；
3. 在 `execution.step(node_id)` 内执行 NODE_CALL 中间件链与 Node；
4. 校验 Output 端口与类型；
5. 沿 Edge 投递下游，或发布为 terminal Output；
6. ready queue 清空后检查残留输入并运行 quiescence callback。

每轮只消费一组输入，避免持续循环饿死其他就绪分支。普通回边与其他 Edge 使用完全相同的投递规则。

## 快照与动态扩展

不同扩展点在不同边界读取登记快照：

| 扩展 | 读取时机 | 目的 |
| --- | --- | --- |
| 输入 selector（具名值） | Graph freeze | 同一张 Graph 的触发语义稳定 |
| NODE_CALL / NODE_INSPECT | execution start | 在途 execution 不受登记或注销影响 |
| 其他钩子点 | 每次触发 | 支持运行期动态扩展 |

Notice 处理函数与 `observe()` 注册的 Event 观察者的异常都会被记录并隔离；Wrap 中间件的失败按所在
操作的错误契约传播，例如 `EVENT_EMIT` 与 `WORK_ROUTE` 的失败以 `EventDispatchError` 传回 `emit()`。中间件可以转换输入、
输出或流程，但最终结果仍受 Graph 端口和执行控制契约约束。

## Slot 租约与分支

Event 和 Work 都不携带 lease。同一进程内，TaskConsumer 为根 Work 从 SlotPool 获取 lease；本地 Router 与
TaskBackend 通过仅限进程内的发布上下文和 Delivery 为每个下游分支保留引用，交付完成后释放，最后一个分支结束时
Slot 回到池中。发布上下文只由 route 的 Work 提交读取；`observe()` 观察者调用期间该上下文被清空，观察者内部的
emit 开始新的 Slot 链。同一个 Slot 使用 execution lock 保证 Graph 不会并发修改链路状态。远程传输只序列化 Event/Work；
接收端反序列化后创建新的本地根 Delivery 和 Slot 链。

## 生命周期与所有权

Runtime 关闭时先发布 `runtime.closing`，调用 Worker 的 `begin_close()` 解除未读订阅的背压并排空工作，关闭角色后发布
`runtime.closed`，最后关闭 PluginHost。宿主先逆序注销各插件经 `api.on`/`api.provide` 的登记，再按登记的逆序执行
`api.defer` 清理函数；`setup()` 中途失败也按同一清理流程回滚，单项清理失败不跳过后续步骤并写入日志，回滚
最终重新抛出原始失败。内建插件关闭它们创建的角色与底层组件；注入组件默认由调用方管理，`close_injected=True`
才转移所有权。selector 撤销只影响后续 Graph 冻结，已经绑定的 Graph 保留原 selector 快照。

组件所有权与注册所有权分别管理。`EventBus.subscribe()`、`TaskConsumer.bind()` 返回仅撤销本次注册的幂等函数；
Router 保存并注销自己的 Event 订阅，Worker 保存并注销自己的消费绑定。共享传输可以继续供其他角色使用。
消费注销需要等待已分配交付完成最终 lease 释放，再回收该绑定的线程资源和不再使用的 Slot 可用通知。

Runtime 与 GraphWorker 使用同一 OPEN -> CLOSING -> CLOSED 状态机。首个 `close()` 原子进入 CLOSING：Runtime 拒绝新的 `emit()`、
直接执行与配置变更，GraphWorker 同样拒绝新的直接执行、注册与消费绑定，并以 RuntimeClosedError 报告；已绑定通道中的 Work 及其
跨图延续继续排空。排空后 Worker 关闭直接执行池、注销消费绑定、关闭自有组件并进入 CLOSED。并发 `close()` 等待同一次关闭完成后返回。
`Runtime.on()` 先绑定消费者再注册 route；route 失败时撤销本次新建的消费绑定，此前已存在的消费者不受影响。

默认内存通道只在已有消费者时接受 Work：从未绑定或最后消费者已经注销的通道，`submit()`、`submit_local()` 立即抛错，
route 因而以 `EventDispatchError` 让 `emit()` 快速失败，不会留下使 `wait_idle()`、`close()` 挂起的排队 Work。
最后一个消费者注销前，会排空该通道已经接受的排队和在途 Work，包括产生的同通道续作，然后原子地移除消费者；重新
成功绑定消费者后恢复接收。这个边界避免 Router 与 Worker 关闭的间隙留下无人处理的新 Work。EventRouter 与
memory.TaskBackend 关闭时先进入 CLOSING：Router 拒绝新的注册与发布，TaskBackend 拒绝新绑定、继续接受已有消费者
通道的续作并在排空后撤下消费者；并发 `close()` 等待同一次关闭完成。

## 架构边界

以下约束防止内部职责泄漏为用户概念：

- Runtime 不实现消息持久化、队列算法或 Node 执行；
- Router 不注册或执行 Graph；
- Worker 不订阅源 Event；
- GraphExecutor 不管理队列与 Graph 注册表；
- 插件通过 capability 工作，不读取 Runtime 私有字段；
- Work、Delivery 和窄角色协议位于 `interlace.spi`，不进入顶层 `interlace` API；默认内存实现位于 `interlace.adapters`。

[上一章：Execution、并发与失败](05-execution.md) · [下一章：插件、SPI 与适配器开发](07-plugins.md)
