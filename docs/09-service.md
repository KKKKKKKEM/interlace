# 第九章：RPC 与图服务

`interlace.service` 是官方维护、按需启用的服务层。它把已注册的 Graph 接入 HTTP/JSON、JSON-RPC 2.0 和
SSE，并提供图编辑发布、项目管理与运行历史接口。服务只提供数据接口，不托管前端页面。基础包依旧没有第三方
运行时依赖，也不会隐式监听端口。

## 安装与启动

当前通过 Git 安装：

```bash
uv pip install "interlace[service] @ git+https://github.com/KKKKKKKEM/interlace.git"
```

在仓库中运行完整示例：

```bash
uv run --extra service python examples/service.py --port 8000
```

服务启动后可在 `http://127.0.0.1:8000/docs` 查看 OpenAPI 文档。示例注册一张文本处理图和两个可编辑节点类型，
数据库保存在 `.interlace/service.sqlite3`。改用 `--database :memory:` 可仅在本次进程中保存编辑与运行记录。

## 项目与定义管理

管理接口按“项目 → 定义 → 草稿、发布版本与运行实例”组织。项目可以创建、修改名称和描述，空的非默认项目可以
删除。项目内的定义摘要给出来源、发布状态、最新版本、草稿修订号、节点数量、运行次数和最近运行；定义详情给出
草稿、全部发布版本与运行记录。项目级运行记录只包含该项目内定义的执行。

一条逻辑定义只对应一条摘要，其草稿和多个发布版本在详情中给出。定义标识继续在服务中全局唯一，Runtime
注册名仍是原代码名称或 `name@vN`，不因所属项目改变。项目是管理分组，不是独立 Runtime、租户权限或资源隔离。

已有数据库在首次升级时创建项目表，并将原草稿和发布版本迁入“默认项目”，保留原文档、修订号与运行历史。
已有和之后首次发现的代码图也进入默认项目；可以通过移动定义接口调整归属，之后重启不会重置。
移动操作同时调整整条定义的草稿、版本和运行记录的管理归属，不重写原 Graph 或执行事实。

管理接口与图调用接口共用服务鉴权：

| 操作 | 接口 |
| --- | --- |
| 项目列表与创建 | `GET /api/projects`、`POST /api/projects` |
| 更新或删除空项目 | `POST /api/projects/{id}`、`DELETE /api/projects/{id}` |
| 项目定义列表与新建 | `GET /api/projects/{id}/definitions`、`POST /api/projects/{id}/definitions` |
| 定义详情 | `GET /api/definitions/{name}?project_id={id}` |
| 项目运行记录 | `GET /api/projects/{id}/executions` |
| 移动定义 | `POST /api/definition-project`，请求包含 name 和 project_id |

`/api/drafts` 和 `/api/publish` 的请求包含 `project_id`，省略时使用默认项目；不匹配现有归属时返回冲突，不隐式移动。
新建定义、首次保存草稿和首次发布使用同一全局唯一性校验：标识已被代码图、其他草稿或版本、历史运行引用的定义
占用时返回 HTTP 409，包括服务启动后才注册的代码图。列表与详情等读取接口不写数据库；尚未持久化归属的代码图按
默认项目展示，在写入路径、首次运行或移动定义时才建立归属。
RPC 的图注册名与调用参数保持独立，不需要添加项目字段。

## 接入已有 Runtime

```python
from interlace.service import GraphService

# runtime 已由应用创建并注册 Graph；离开 with 时关闭服务，不关闭 Runtime。
with GraphService(runtime, database=".interlace/service.sqlite3") as service:
    service.serve(host="127.0.0.1", port=8000)
```

服务通过 `Runtime.graphs()` 自动取得已有及之后注册的代码图，无需重复注册到服务。默认 GraphWorker 提供
公开的 `interlace.spi.GraphCatalog`；自定义 Worker 实现同一窄协议即可接入图查看。

所有权保持明确：Runtime 由应用管理，GraphService 只拥有自己的服务插件、归档线程与数据库。服务以
`runtime.plugin_host.install()` 安装一个运行期插件（ID 形如 `interlace.service/graph-service-1`），由它登记
`RUNTIME_EVENT` 观察、可选的 `NODE_INSPECT` 诊断采集，并以 `defer` 登记诊断器的关闭；同一 Runtime 可以接入多个服务。
`service.close()` 幂等：先卸载服务插件，再等待已排队的输出归档完成并关闭数据库，不关闭 Runtime、不撤销已接受的
执行。Runtime 先关闭时，插件宿主一并卸载服务插件，随后的 `service.close()` 只关闭归档与数据库；已关闭的 Runtime
不能再接入新服务。服务由创建它的一方关闭：`create_app()` 返回的 ASGI 应用和 `serve()` 都不在退出时关闭服务，
因此独立启动与挂载到已有应用的行为一致；挂载时在父应用生命周期结束处调用 `service.close()`。

```python
from fastapi import FastAPI

app = FastAPI()
app.mount("/interlace", service.create_app(token="application-provided-token"))
```

`token` 是可选 Bearer 令牌，保护 HTTP、JSON-RPC、SSE 和 OpenAPI 数据接口。客户端通过 Authorization 请求头传递，
不写入 URL。令牌按字节比较，非 ASCII 的 Authorization 头同样返回 401。

每个 HTTP 与 WebSocket 请求的 Host 头必须在 `allowed_hosts` 中，否则返回 HTTP 400，用于阻止 DNS rebinding：
攻击页面即使让 Host 与 Origin 一致（例如都指向 `evil.example`），也不在白名单中。`create_app()` 默认只接受
`127.0.0.1`、`localhost` 与 `::1`；`serve()` 默认再加入实际监听地址。通过域名、局域网地址或反向代理访问时，
显式传入 `allowed_hosts=["graphs.example.com"]`；包含 `"*"` 表示不检查 Host，只应在前置代理已校验 Host 时使用。
默认仅监听 `127.0.0.1`；`serve()` 监听非回环地址且没有设置令牌时记录警告。全部响应带 `X-Frame-Options: DENY`
与 `Content-Security-Policy: frame-ancestors 'none'`，禁止第三方页面嵌入服务响应诱导点击。TLS、用户体系和多用户权限由应用或
反向代理承担。

单个 HTTP 请求体默认最多 1 MiB，可通过 `create_app(max_request_bytes=...)` 或
`serve(max_request_bytes=...)` 调整；该上限同样约束 `inputs` 与 `options` 的大小。
`GraphService(max_active_executions=...)` 默认把当前 Runtime 的同时活跃执行限制为 128，达到上限返回 HTTP 429。
反向代理仍应设置自己的连接数、请求大小和速率限制。

服务启动时先读取并编译全部已保存版本、标记中断记录、安装服务插件，最后才向 Runtime 注册恢复的版本。
注册之前的任何失败都会卸载服务插件（插件 setup 失败时由安装回滚完成）并关闭数据库，同一 Runtime 可以直接
重新创建服务。Runtime 没有注销接口，若注册阶段本身失败（例如 Runtime 并发关闭），已注册的版本会保留在该
Runtime 中。

## 节点目录与编辑边界

普通 Python Graph 可以直接查看和运行，构造器不会被反射成编辑表单。希望通过编辑文档创建节点时，应用显式登记
节点工厂和 Pydantic 配置模型：

```python
from interlace.service import GraphService, NodeCatalog

# TextConfig、TextNode、SplitNode 的完整实现见 examples/service.py。
catalog = NodeCatalog()
catalog.register("text.transform", TextNode, config=TextConfig, title="文本转换")
catalog.register_node("text.split", SplitNode, title="按行拆分")
service = GraphService(runtime, nodes=catalog)
```

配置模型生成 JSON Schema 并负责服务端校验；工厂接收验证后的模型，返回普通 Node。数据库、HTTP client 等
资源仍由应用注入工厂或 Node，不能放入 JSON 配置。工厂应只构造可重入的节点定义，不在构造期间启动网络任务；
节点预览、图校验、发布和恢复定义都可能调用工厂，工厂捕获的外部资源由应用管理。

`GET /api/nodes` 返回已登记节点类型及其配置 JSON Schema；`POST /api/nodes/preview` 通过真实工厂读取单节点端口，
因此允许端口随配置变化。`POST /api/validate` 以核心冻结逻辑校验完整图文档，`POST /api/definitions/parse` 校验
导入文档格式并补齐默认值；服务端冻结校验始终为最终依据。
编译图文档时使用 Runtime 钩子总线上当前提供的具名值查找 selector，自定义 selector 应在发布或恢复图定义之前提供。

## 草稿与发布版本

编辑文档描述 `name`、`entrypoint`、`nodes` 和 `edges`，画布位置不进入核心执行语义。例如：

```json
{
  "name": "greeting",
  "entrypoint": "upper",
  "nodes": [{"id": "upper", "type": "text.transform", "config": {"prefix": "HELLO: "}}],
  "edges": []
}
```

草稿允许缺少入口或连线，保存时通过 `revision` 做乐观并发检查，冲突返回 HTTP 409。发布必须经过现有
Graph 构建、端口类型、输入策略和可达性校验。普通环仍是普通 Edge，不新增执行语义。

每次发布生成新的 Runtime 注册名，例如 `greeting@v1`、`greeting@v2`。旧版本不被替换，调用者明确选择版本；
重新编辑打开当前草稿，不能直接修改已经发布的版本。重启服务时，通过当前登记的工厂恢复保存的发布定义；
缺失工厂或无法编译时启动失败，不隐式跳过或导入 Python 模块。

服务数据库是单个服务实例的本地状态文件，不用于多个独立服务进程共同维护同一 Runtime 注册表。

## HTTP/JSON 调用

HTTP 与 JSON-RPC 的 `inputs` 都是按入口端口命名的对象，即使只有一个端口也不省略端口名。
协议适配层据此解码，然后按现有 Runtime 的单端口或多端口入口契约提交。

```bash
curl http://127.0.0.1:8000/api/call \
  -H 'Content-Type: application/json' \
  -d '{"graph":"text.pipeline","inputs":{"text":"hello\ninterlace"}}'
```

响应包含 execution ID、状态和有序 `outputs`，每项保留 `port` 与 `value`。
`max_steps`、`timeout`、`max_queued` 使用现有执行契约；零步数不限步数，零队列上限不限积压，缺省 timeout 不限时，
时间单位为秒。

远程 `options` 默认关闭：非空 `options` 返回 HTTP 422，JSON-RPC 返回 `-32602`。应用通过
`GraphService(runtime, allowed_options=["text"])` 声明允许的顶层领域命名空间，未声明的键仍被拒绝；值的大小受请求体上限
约束，内容由领域节点自行校验。`examples/service.py` 只允许 `text` 命名空间，例如
`"options": {"text": {"suffix": "!"}}`。

输入通过 Pydantic TypeAdapter 的严格 JSON 解码验证，支持内置 JSON 类型以及 Pydantic 能描述的模型和数据类。
输出使用明确的 JSON 序列化规则，不把不支持的对象转换为 repr 字符串。任意 Python Client、Slot 等对象不属于
远程传输值；领域模型应提供可序列化端口类型。编码失败是传输错误，不把已成功的 Graph 改为业务失败。

HTTP 只按服务声明的错误类型映射状态码，其他异常记录日志后返回不含异常消息的 500：

| 状态码 | 含义 |
| --- | --- |
| 400 | Host 不在 `allowed_hosts` 中 |
| 401 / 403 | 缺少有效令牌 / 浏览器跨站访问 |
| 404 | 项目、定义、图或执行不存在 |
| 409 | 修订号过期、定义标识冲突、Graph 执行失败（`/api/call`），或服务、Runtime 已关闭 |
| 413 | 请求体超过上限 |
| 422 | 请求字段、输入类型、`options` 键、节点配置或图冻结校验不合法 |
| 429 | 活跃执行达到 `max_active_executions` |
| 500 | 输出无法编码为 JSON（`error_type` 为 `OutputEncodingError`，响应仍包含成功的执行状态），或未预期的服务错误 |

## 长任务与 SSE

```bash
curl http://127.0.0.1:8000/api/executions \
  -H 'Content-Type: application/json' \
  -d '{"graph":"text.pipeline","inputs":{"text":"hello"}}'
```

返回 HTTP 202 和 execution ID。随后可使用：

| 操作 | 接口 |
| --- | --- |
| 查询执行 | `GET /api/executions/{id}` |
| 查询执行列表 | `GET /api/executions` |
| 协作式取消 | `POST /api/executions/{id}/cancel` |
| 订阅节点观测 | `GET /api/executions/{id}/events` |
| 订阅终端输出 | `GET /api/executions/{id}/outputs` |

观测 SSE 提供 `observation` 与 `finished` 消息，支持 `Last-Event-ID` 或 `after` 游标恢复。服务先读取执行状态再读取
观测批次，只有在已观察到终态之后再读到空批次时才发送 `finished`，因此终态前写入的观测不会被跳过。
输出 SSE 提供 `output`、`finished` 或 `failure` 消息，从首项重放。当前执行由服务端任务及时读取原生异步输出并转交客户端，
观察端不对业务执行施加背压；客户端未发送积压超过 10,000 项时以 `failure` 断开，执行结束后可再次请求历史重放。
历史输出从数据库逐批读取，均不要求先构建全量结果。同一 Work.id 被重投递时，历史记录与输出只保留最新一次尝试，
观测事件按追加顺序保留全部尝试。

客户端断开只释放输出订阅，不自动取消执行；需要取消时调用取消接口。Graph timeout 约束业务执行，不等于
HTTP 连接超时。已经交付的输出不会因后续失败撤回。同步 Node 仍需通过 checkpoint 协作式响应取消。

## JSON-RPC 2.0

```bash
curl http://127.0.0.1:8000/rpc \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"graph.call","params":{"graph":"text.pipeline","inputs":{"text":"hello"}}}'
```

`graph.call` 接收与 HTTP 相同的命名参数并等待执行结果。标准分发由 jsonrpcserver 完成，支持请求 ID、批量请求、
通知以及标准的解析、方法与参数错误；通知不返回 JSON-RPC 内容，对应 HTTP 204。服务端错误码与 HTTP 状态对应：

| 错误码 | 含义 | 对应 HTTP |
| --- | --- | --- |
| `-32602` | 参数非法、`options` 键未允许或图不存在 | 422 / 404 |
| `-32000` | Graph 执行失败，`data` 包含执行标识、状态和 `detail` | 409 |
| `-32001` | 输出无法编码为 JSON，执行状态保持成功 | 500 |
| `-32002` | 活跃执行达到上限 | 429 |
| `-32003` | 服务或 Runtime 已关闭 | 409 |
| `-32603` | 未预期的服务错误，不包含异常消息 | 500 |

长任务管理与流式输出使用上述 HTTP/SSE 接口。

本版实现 HTTP/JSON、JSON-RPC 2.0 和 SSE；没有把 gRPC、Thrift 或其他协议写成已实现能力。

## 运行历史与诊断

`GET /api/executions/{id}` 返回执行状态、步数、错误以及执行时的图结构快照。观测 SSE 逐条交付节点 firing 的
生命周期事件与诊断记录，每条记录按 execution、node 与 step 关联；同一节点在循环中多次执行时分别保留每次 firing。
启用诊断时，每次 firing 记录：

- 参数：实际传给 Node.execute 的端口输入和当前 Context.options；发布图的执行快照另含已保存的节点构造配置。
- 输出：Node 原始输出逐项记录，出口 Hook 改写后的终端结果通过输出接口读取。
- 日志：Python logging 的时间、级别、logger、消息和可选堆栈。
- 异常：调用或迭代失败的类型、消息和堆栈；取消与超时仍沿用现有执行语义。

诊断默认启用，可用 `GraphService(runtime, diagnostics=False)` 关闭参数、原始输出和日志采集，保留生命周期与终端结果。
参数通过 `NODE_INSPECT` 钩子点同步采集：输入在全部节点中间件改写之后，原始输出在中间件改写之前。
同一 firing 的诊断记录最多积累 64 条后批量提交，并在节点结束前冲刷剩余记录，减少文件 SQLite 的逐条事务开销。
采集立即生成独立快照，不修改原对象，失败不改变业务结果；生成器等一次性迭代器以 `$unavailable` 标记代替，不被读取。
终端输出含一次性迭代器时编码明确失败，不消耗应用仍持有的对象。生命周期事实经 `RUNTIME_EVENT` 钩子点记录，
`output.routed` 只记录 step、port 和目标端口集合，不携带业务对象。

日志通过当前观测作用域的 execution、node 与 step 关联，不能按线程 ID 猜测归属。采集遵守应用已有日志级别与
传播配置，不修改根 logger 级别、不替换日志工厂、不重定向全局 stdout。应用可为自身 logger 设置 INFO，再在
Node.execute 中调用 logger.info。同步、async 节点和生成器迭代期间均支持；自建线程、队列日志监听器或进程边界
需要应用显式传递观测上下文。print 不属于此接口，节点结束后的后台任务日志不会写入已结束的 firing。

参数、配置与输出快照按字段名片段隐藏凭据：字段名不区分大小写地包含 secret、token、password、passwd、key、
authorization、cookie 或 credential 时整体替换为 `[REDACTED]`。快照中的字符串、日志消息、异常消息、堆栈、生命周期
事件属性和执行错误还会隐藏 URL userinfo（`scheme://user:pass@` 中的凭据）、`Bearer`/`Basic` 凭据，以及
`password=...`、`api_key: ...` 这类凭据名后的值；自由文本不单独匹配 key，避免把 `KeyError` 等名称当成凭据。
脱敏只依据形态识别，应用仍不应在日志中写入凭据。单条快照、日志消息或堆栈保留最多 65,536 个字符，过大快照和
无法编码的值有明确标记。旧运行不会补造未采集的参数或日志。替代执行器需支持 Hook 安装、inspect 与观测作用域
才能提供同等诊断粒度；仅提供生命周期的实现可关闭 diagnostics 后接入。

服务启动后的直接调用和队列执行都可记录；服务接入前已经结束的执行只有 Runtime 仍保留的状态，没有补造历史
节点事件。每次执行保存当时的图结构，防止代码图在下一次部署改变后误读旧运行。

Worker 在写入 Execution 终态之前发布 `execution.finished`，服务因此直接按该事件的 status 与 error_type 记录终态，
不读取此刻可能仍为 running 的句柄状态。终端输出由服务的归档线程在执行真正终止后通过公开迭代接口逐项保存，
随后写入带错误消息的最终快照；`service.close()` 会等待已排队的归档，进程在归档前退出时不能保证保留尚未归档的输出。

SQLite 保存草稿、发布版本、运行元数据、启用诊断后采集的参数、节点输出和日志，以及可编码的终端输出。没有执行
标识的 Runtime 事件没有读取入口，不写入数据库。`GraphService(retention=1000)` 默认最多保留 1,000 条执行记录：新执行
写入后，超出数量的最早已结束执行连同其观测、诊断和终端输出一起删除；`retention=None` 关闭清理。当前运行列表在
全局、项目或定义范围内分别读取最近五百条，先筛选范围再限制数量；观测和输出接口分批读取。数据库结构版本记录在
SQLite `user_version` 中，旧版本在启动时逐级迁移，高于当前代码支持版本的数据库会被拒绝打开。

服务重启后把历史在途记录标记为中断，不恢复任务，不提供 broker ack、重试、exactly-once 或跨进程 Slot 连续性。
跨图事件的业务关联键仍属于领域 payload；服务不推断远程调用链。

## 开发与验证

```bash
uv sync --extra service
uv run --extra service pytest -q
uv run --extra service mypy interlace
uv run --extra service ruff check interlace tests examples
uv run --extra service ruff format --check interlace tests examples
```

[上一章：编排模式](08-patterns.md)
