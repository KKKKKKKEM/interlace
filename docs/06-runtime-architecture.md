# 第六章：Runtime 内部架构

前五章描述用户可观察的行为。本章转向实现内部：Runtime 如何通过插件装配角色，一次直接执行和一次事件执行分别
经过哪些组件，以及各层为什么保持独立。

## 分层总览

| 层次 | 源码边界 | 对象 | 责任 |
| --- | --- | --- | --- |
| 用户模型 | `interlace.engine`，由 `interlace` 导出 | Ports、Node、Output、Edge、Graph、Event、Context、Execution、Slot | 描述业务计算与可观察执行状态 |
| 用户门面 | `interlace.runtime`，由 `interlace` 导出 | Runtime | 注册 Graph，代理执行、事件、观察和控制入口 |
| 装配宿主 | `interlace.plugins`、`interlace.runtime.plugin` | PluginHost、LocalRuntimePlugin | 解析插件依赖，注册 capability，安装标准贡献并管理生命周期 |
| 运行角色 | `interlace.runtime` | EventRouter、GraphWorker | 分别处理 Event -> Work 与 Work -> Graph execution |
| 能力端口 | `interlace.spi` | RouterRole、WorkerRole、EventBus、任务传输、GraphExecutor、SlotProvider、执行资源协议 | 隔离编排、传输、执行和资源实现 |
| 默认适配器 | `interlace.adapters.memory`、`interlace.engine.executor` | EventBus、TaskBackend、Engine | 提供单进程内存运行时 |
| 可选官方服务 | `interlace.service` | GraphService、NodeCatalog、Studio | 提供 RPC、图编辑发布与观测历史，按 extra 安装 |

Runtime 是面向应用的稳定门面，PluginHost 是系统装配根。普通应用不需要看到后四层。

## 源码目录与包职责

源码目录按照架构层次组织，而不是把所有运行时职责都放入 `engine`：

```text
interlace/
├── __init__.py              # 宪法规定的顶层公共 API
├── engine/                  # Graph 执行微内核及受控内核扩展点
│   ├── core.py              # Ports、Node、Output、InputPolicy
│   ├── graph.py             # Edge、Graph、ExecutionPlan 与冻结校验
│   ├── execution.py         # Execution 句柄、限制与状态
│   ├── execution_resources.py # 输出存储与等待通知协议及默认实现
│   ├── executor.py          # 单张冻结 Graph 的默认执行器 Engine
│   ├── events.py            # Event 与 Context
│   ├── slots.py             # Slot 与 SlotPool
│   ├── hooks.py             # Node Hook 契约和快照注册
│   ├── policies.py          # selector 契约、引用与冻结绑定
│   ├── observation.py       # 只读 Runtime 观察模型
│   ├── errors.py            # 内核和运行控制错误
│   └── runner.py            # 同步/异步 Node 调用桥接
├── runtime/                 # Runtime 门面和运行角色
│   ├── facade.py            # Runtime
│   ├── router.py            # EventRouter
│   ├── worker.py            # GraphWorker
│   ├── plugin.py            # LocalRuntimePlugin
│   └── _utils.py            # 私有生命周期辅助函数
├── plugins/                 # PluginHost、descriptor、capability 与 contribution
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
- `spi` 只引用协议签名所需的 Graph、Event、Execution、Hook 和 Slot 模型；
- `adapters` 实现 SPI，可以引用搬运 Event/Work 所需的最小内核类型；
- `plugins` 管理装配元数据与生命周期，通过 SPI 的公开角色安装标准贡献，不读取 Runtime 私有状态；
- `runtime` 可以依赖前述各层并完成组合，但不得把部署算法重新实现到门面中；
- `nodes` 只放可复用的非内核 Node，只依赖内核公共语义，不能成为 Runtime 的隐式前置条件。
- `service` 依赖公开 Runtime、Graph、Execution 及观察模型；核心不导入服务或 Web 依赖。`web/` 保存工作台源代码。

服务通过 `Runtime.graphs()` 读取已注册图的只读快照；默认 GraphWorker 实现 `interlace.spi.GraphCatalog`。
替代 Worker 若需启用图检查与服务，同样实现该窄协议。服务不回退读取默认 Worker 的私有注册表。

## 默认装配

```mermaid
flowchart TB
    Runtime --> Host[PluginHost]
    Host --> Local[LocalRuntimePlugin]
    Host --> Contributions[Contribution plugins]

    Local --> Router[EventRouter]
    Local --> Worker[GraphWorker]
    Router --> Bus[EventBus]
    Router --> Publisher[TaskPublisher]
    Worker --> Consumer[TaskConsumer]
    Worker --> Executor[GraphExecutor]

    Contributions --> Selectors[InputSelector contributions]
    Contributions --> Hooks[NodeHook contributions]
    Contributions --> Observers[RuntimeObserver contributions]
    Selectors --> Host
    Hooks --> Host
    Observers --> Host
    Host -->|install through WorkerRole| Worker
    Host -->|install through RouterRole| Router
```

`Runtime()` 根据已声明 capability 让 LocalRuntimePlugin 逐项补齐缺失的 EventBus、TaskBackend、GraphExecutor 和 ExecutionFactory，
再组装 Router 与 Worker。内建实现和应用插件使用相同的依赖解析、capability 注册、启动和停止流程。显式
`Runtime(router=..., worker=...)` 用于 Router/Worker 独立部署，此模式不创建 PluginHost。

全部插件完成 `setup()` 后，PluginHost 先通过 `RouterRole`、`WorkerRole` 安装 selector、Hook 和 Observer，
再按依赖顺序调用 `start()`。因此插件在启动阶段注册 Graph 时，标准贡献已经可用。提供完整替代角色的插件同样经过
这条安装路径；缺少贡献所需的角色会在启动前失败。LocalRuntimePlugin 只负责提供默认能力与角色，不单独安装贡献。

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

## Graph 冻结边界

Runtime 注册 Graph 时，GraphWorker 使用当前 PolicyRegistry 绑定输入选择器并冻结 Graph：

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
3. 在 `execution.step(node_id)` 内执行 Hook 和 Node；
4. 校验 Output 端口与类型；
5. 沿 Edge 投递下游，或发布为 terminal Output；
6. ready queue 清空后检查残留输入并运行 quiescence callback。

每轮只消费一组输入，避免持续循环饿死其他就绪分支。普通回边与其他 Edge 使用完全相同的投递规则。

## 快照与动态扩展

不同扩展点在不同边界固定快照：

| 扩展 | 固定时机 | 目的 |
| --- | --- | --- |
| InputSelector | Graph freeze | 同一张 Graph 的触发语义稳定 |
| Node Hook | execution start | 在途 execution 不受 attach/detach 影响 |
| Runtime Observer | 发布每个 RuntimeEvent 时读取 | 支持动态观察且不干预业务结果 |

Observer 异常会被隔离。Hook 可以转换输入、输出或流程，但最终结果仍受 Graph 端口和执行控制契约约束。

## Slot 租约与分支

Event 和 Work 都不携带 lease。同一进程内，TaskConsumer 为根 Work 从 SlotPool 获取 lease；本地 Router 与
TaskBackend 通过仅限进程内的发布上下文和 Delivery 为每个下游分支保留引用，交付完成后释放，最后一个分支结束时
Slot 回到池中。同一个 Slot 使用 execution lock 保证 Graph 不会并发修改链路状态。远程传输只序列化 Event/Work；
接收端反序列化后创建新的本地根 Delivery 和 Slot 链。

## 生命周期与所有权

插件模式下，Runtime 排空工作后关闭 PluginHost。宿主先逆序注销已安装的 selector、Hook 和 Observer，再逆序停止已进入
`setup()` 的插件；贡献安装或 `start()` 中途失败也按同一清理流程回滚。延迟 Hook 在目标 Graph 注册前后均可注销，
注入执行器不会因宿主退出而留下本次贡献。LocalRuntimePlugin 关闭它提供的角色和自己创建的底层组件。
注入组件默认由调用方管理，`close_injected=True` 才转移所有权。
selector 注销只影响后续 Graph 冻结，已经绑定的 Graph 保留原选择器快照；共享 PolicyRegistry 可供后续装配继续使用。

显式 Router/Worker 模式下，Runtime 直接关闭两个角色，各角色再按自己的所有权配置处理底层组件。

组件所有权与注册所有权分别管理。`EventBus.subscribe()`、`TaskConsumer.bind()` 返回仅撤销本次注册的幂等函数；
Router 保存并注销自己的 Event 订阅，Worker 保存并注销自己的消费绑定。共享传输可以继续供其他角色使用。
消费注销需要等待已分配交付完成最终 lease 释放，再回收该绑定的线程资源和不再使用的 Slot 可用通知。

默认内存通道的最后一个消费者注销前，还会排空该通道已经接受的排队和在途 Work，包括产生的同通道续作，然后
原子地移除消费者并停止该通道接收新 Work。退订后的 `submit()`、`submit_local()` 明确抛错，重新成功绑定消费者后
恢复接收；从未绑定消费者的通道仍允许先排队。这个边界避免 Router 与 Worker 关闭的间隙留下无人处理的新 Work。

## 架构边界

以下约束防止内部职责泄漏为用户概念：

- Runtime 不实现消息持久化、队列算法或 Node 执行；
- Router 不注册或执行 Graph；
- Worker 不订阅源 Event；
- GraphExecutor 不管理队列与 Graph 注册表；
- 插件通过 capability 工作，不读取 Runtime 私有字段；
- Work、Delivery 和窄角色协议位于 `interlace.spi`，不进入顶层 `interlace` API；默认内存实现位于 `interlace.adapters`。

[上一章：Execution、并发与失败](05-execution.md) · [下一章：插件、SPI 与适配器开发](07-plugins.md)
