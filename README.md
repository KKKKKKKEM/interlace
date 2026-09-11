# Interlace

Interlace 是一个可扩展的 Python 编排微内核：Graph 内以 `Output` 和 `Edge` 传递局部数据，Graph 间以
`Event` 和 `Runtime` 连接领域工作流。

核心实现通过公开协议替换，功能通过插件和领域节点扩展，Graph 与 Execution 的公共契约保持稳定。
运行时没有第三方依赖；爬虫模型、下载器与下载节点由独立的 [Bricks](https://github.com/KKKKKKKEM/bricks) 提供。

## 安装

当前通过 Git 安装，尚未发布到 PyPI：

```bash
uv pip install "interlace @ git+https://github.com/KKKKKKKEM/interlace.git"
```

生产项目应将 Git 依赖固定到经过验证的提交。开发本仓库可使用 `uv sync`。

```text
Node -- Output / Edge --> Node
Graph -- Event / Runtime --> Graph
```

顶层 API 还提供进程内的 `Slot` 与 `SlotPool`：队列 Work 可以在同一进程中跨 Consumer 传递并复用代理、Cookie、
连接等执行状态，而不依赖具体线程。Slot 不跨进程或消息边界传输。

执行默认不限步数和时长；`run()`、`start()` 和事件 route 可按需设置 `max_steps` 与 Graph `timeout`，单次
Node firing 的时限由该 Node 的 `timeout` 属性声明。`start()` 返回可查询和协作式取消的 `Execution`。
`Execution` 可调用 `result()`、直接 `await`，也可同步或异步迭代 terminal Output；`Runtime.iter()` 和
`Runtime.aiter()` 提供对应便利入口。

所有节点统一继承 `Node`。`execute()` 可以使用普通 `def` 或 `async def`；普通函数返回的 Awaitable 也由
Engine 等待，再按相同的 Output 契约处理结果。

## 最小示例

```python
from interlace import Graph, Node, Output, Ports, Runtime


class Upper(Node):
    input_ports = Ports(text=str)
    output_ports = Ports(result=str)
    timeout = 5

    def execute(self, inputs, context):
        del context
        return Output(inputs["text"].upper(), "result")


graph = Graph.compose(nodes={"upper": Upper()}, entrypoint="upper")

with Runtime() as runtime:
    runtime.register("upper.graph", graph)
    print(runtime.run("upper.graph", "interlace"))
```

完整内容按一本手册组织，从[文档目录](docs/README.md)开始：

1. [设计哲学与心智模型](docs/01-design-philosophy.md)
2. [第一张 Graph](docs/02-first-graph.md)
3. [Graph 数据流](docs/03-graph-dataflow.md)
4. [Event 与跨图工作流](docs/04-events-and-workflows.md)
5. [Execution、并发与失败](docs/05-execution.md)
6. [Runtime 内部架构](docs/06-runtime-architecture.md)
7. [插件、SPI 与适配器开发](docs/07-plugins.md)
8. [编排模式](docs/08-patterns.md)
9. [RPC 与可视化工作台](docs/09-service-and-studio.md)

## 可选 RPC 与可视化

官方服务 extra 提供 HTTP/JSON 调用、JSON-RPC 2.0、SSE 输出流，以及内置的图编辑与运行工作台。
基础安装仍保持零运行时依赖，不自动启动服务。

```bash
uv pip install "interlace[service] @ git+https://github.com/KKKKKKKEM/interlace.git"
uv run --extra service python examples/service.py
```

打开 `http://127.0.0.1:8000`，按“项目列表 → 定义列表 → 定义详情”进入管理界面。已有数据迁入默认项目；
详情分为概览、图编排与运行记录。已有代码图直接支持远程调用和运行查看；登记节点工厂与配置模型后，工作台还可
拖拽连线、编辑配置、校验、保存草稿与发布不可变版本。界面已随包构建，使用者无需安装 Node.js。

已有 Runtime 可直接接入：

```python
from interlace.service import GraphService

# runtime 为已经注册 Graph 的运行时，由调用方管理生命周期。
service = GraphService(runtime, database=".interlace/studio.sqlite3")
service.serve()
```

安装、节点目录、HTTP/JSON-RPC 调用及服务所有权见[第九章](docs/09-service-and-studio.md)。

## 当前范围

默认实现通过内建 `LocalRuntimePlugin` 提供内存事件分发、线程池队列并发和 Graph 执行；它与第三方扩展一样由
统一 `PluginHost` 装配。EventBus、任务传输、GraphExecutor、输入策略、Node Hook 和 Runtime Observer 均可受控
扩展，但 Graph 的类型、冻结和 Execution 语义保持固定。默认实现不提供持久化、broker ack、进程恢复、定时器、
死信队列或 exactly-once 语义。

源码按 `engine`、`runtime`、`plugins`、`spi`、`adapters` 和可复用 `nodes` 分层；各包职责、目录树和允许的
依赖方向见[Runtime 内部架构](docs/06-runtime-architecture.md#源码目录与包职责)。

```bash
uv run python examples/linear.py
uv run python examples/fan_in.py
uv run python examples/cycle.py
uv run python examples/event_routing.py
uv run python examples/async_node.py
uv run python examples/output_stream.py
uv run python examples/domain_context.py
uv run --with pytest pytest -q
```
