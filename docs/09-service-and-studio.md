# 第九章：RPC 与可视化工作台

`interlace.service` 是官方维护、按需启用的服务层。它把已注册的 Graph 接入 HTTP/JSON、JSON-RPC 2.0 和
SSE，并提供同一服务下的可视化编辑与运行工作台。基础包依旧没有第三方运行时依赖，也不会隐式监听端口。

## 安装与启动

当前通过 Git 安装：

```bash
uv pip install "interlace[service] @ git+https://github.com/KKKKKKKEM/interlace.git"
```

在仓库中运行完整示例：

```bash
uv run --extra service python examples/service.py --port 8000
```

打开 `http://127.0.0.1:8000`。示例注册一张文本处理图和两个可编辑节点类型，数据库保存在
`.interlace/studio.sqlite3`。改用 `--database :memory:` 可仅在本次进程中保存编辑与运行记录。

工作台静态资源随 Python wheel 和 sdist 一起交付，安装使用者不需要 Node.js 或联网加载前端 CDN。

## 项目与定义管理

工作台入口是项目列表。进入项目后查看定义列表，点击单条定义进入详情，默认展示概览：

1. 项目列表支持创建、搜索和进入项目；项目设置可修改名称和描述，空的非默认项目可以删除。
2. 定义列表展示来源、发布状态、最新版本、节点数量和最近运行，可搜索、筛选、新建或导入定义。
3. 定义详情分为“概览”“图编排”“运行记录”。概览展示定义信息和版本；图编排复用原有画布，运行记录可进入实例
   查看节点参数、输出、日志与异常。项目级运行记录只展示该项目内定义的执行。

一条逻辑定义只在列表中占一行，其草稿和多个发布版本在详情中选择。定义标识继续在服务中全局唯一，Runtime
注册名仍是原代码名称或 `name@vN`，不因所属项目改变。项目是管理分组，不是独立 Runtime、租户权限或资源隔离。

已有数据库在首次升级时创建项目表，并将原草稿和发布版本迁入“默认项目”，保留原文档、修订号与运行历史。
已有和之后首次发现的代码图也进入默认项目；可以在定义详情中通过“移动到项目”调整归属，之后重启不会重置。
移动操作同时调整整条定义的草稿、版本和运行记录的管理归属，不重写原 Graph 或执行事实。

页面使用稳定的 hash 路由，例如 `#/projects/{project_id}/definitions/{name}?tab=overview`，支持刷新、直接打开和浏览器
前进后退，也支持挂载在已有 Web 应用的子路径。未保存的图编辑会在离开页面时提示确认。仅进入图编排或运行详情
时加载画布代码。

桌面端进入图编排或运行详情时，项目侧栏默认收起为图标导航，可通过顶部按钮展开。图操作集中在标题区，画布提供
缩放比例与适应视图工具。运行详情的节点诊断在右侧独立滚动；底部运行面板可收起，也可拖动分隔线调整高度，聚焦
分隔线后可用上下方向键调整。移动端通过导航抽屉和节点详情浮层保留相同操作。

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
with GraphService(runtime, database=".interlace/studio.sqlite3") as service:
    service.serve(host="127.0.0.1", port=8000)
```

服务通过 `Runtime.graphs()` 自动取得已有及之后注册的代码图，无需重复注册到服务。默认 GraphWorker 提供
公开的 `interlace.spi.GraphCatalog`；自定义 Worker 实现同一窄协议即可接入图查看。

所有权保持明确：Runtime 由应用管理，GraphService 只拥有自己的观察注册、归档线程与数据库。
`service.close()` 幂等，不关闭 Runtime、不撤销已接受的执行；关闭时等待已排队的输出归档完成。服务由创建它的
一方关闭：`create_app()` 返回的 ASGI 应用和 `serve()` 都不在退出时关闭服务，因此独立启动与挂载到已有应用的
行为一致；挂载时在父应用生命周期结束处调用 `service.close()`。

```python
from fastapi import FastAPI

app = FastAPI()
app.mount("/interlace", service.create_app(token="application-provided-token"))
```

`token` 是可选 Bearer 令牌，保护 HTTP、JSON-RPC、SSE 和 OpenAPI 数据接口。工作台可在令牌对话框中输入，
保存在当前浏览器标签页的 sessionStorage，随后放入 Authorization 请求头，不写入 URL。令牌按字节比较，
非 ASCII 的 Authorization 头同样返回 401。

每个 HTTP 与 WebSocket 请求的 Host 头必须在 `allowed_hosts` 中，否则返回 HTTP 400，用于阻止 DNS rebinding：
攻击页面即使让 Host 与 Origin 一致（例如都指向 `evil.example`），也不在白名单中。`create_app()` 默认只接受
`127.0.0.1`、`localhost` 与 `::1`；`serve()` 默认再加入实际监听地址。通过域名、局域网地址或反向代理访问时，
显式传入 `allowed_hosts=["studio.example.com"]`；包含 `"*"` 表示不检查 Host，只应在前置代理已校验 Host 时使用。
默认仅监听 `127.0.0.1`；`serve()` 监听非回环地址且没有设置令牌时记录警告。TLS、用户体系和多用户权限由应用或
反向代理承担。

单个 HTTP 请求体默认最多 1 MiB，可通过 `create_app(max_request_bytes=...)` 或
`serve(max_request_bytes=...)` 调整；该上限同样约束 `inputs` 与 `options` 的大小。
`GraphService(max_active_executions=...)` 默认把当前 Runtime 的同时活跃执行限制为 128，达到上限返回 HTTP 429。
反向代理仍应设置自己的连接数、请求大小和速率限制。

服务启动时先读取并编译全部已保存版本、标记中断记录、安装诊断与观察，最后才向 Runtime 注册恢复的版本。
注册之前的任何失败都会卸载已安装的观察与诊断并关闭数据库，同一 Runtime 可以直接重新创建服务。Runtime 没有
注销接口，若注册阶段本身失败（例如 Runtime 并发关闭），已注册的版本会保留在该 Runtime 中。

## 节点目录与编辑边界

普通 Python Graph 可以直接查看和运行，构造器不会被反射成编辑表单。希望在画布上创建节点时，应用显式登记
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

工作台支持节点添加、删除、标识修改、拖拽、端口连线、入口选择、配置表单、撤销重做、定义导入导出和校验。
复杂配置字段使用 JSON 编辑。端口预览通过真实工厂读取，因此允许端口随配置变化；服务端冻结校验始终为最终依据。
自定义 selector 可通过 `NodeCatalog(policies=...)` 显式注入对应的 PolicyRegistry。

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
输出 SSE 提供 `output`、`finished` 或 `failure` 消息，从首项重放。当前执行沿用原生异步输出迭代与背压，
历史输出从数据库逐批读取，均不要求先构建全量结果。

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

## 运行可视化与历史

工作台展示图结构、节点状态、每次 firing 的序号与耗时、失败类型、已交付输出的端口，以及 terminal Outputs。
同一节点在循环中多次执行时，时间线分别保留每次 firing，画布展示最新节点状态与累计触发次数。

选中运行实例后，点击画布节点或时间线中的某一步，可打开该节点的运行详情，并切换不同触发记录：

- 参数：实际传给 Node.execute 的端口输入和当前 Context.options；发布图还显示已保存的节点构造配置。
- 输出：Node 原始输出逐项记录，后续出口 Hook 改写后的终端结果在“终端输出”中查看。
- 日志：Python logging 的时间、级别、logger、消息和可选堆栈，支持级别筛选与文字搜索。
- 异常：调用或迭代失败的类型、消息和堆栈；取消与超时仍沿用现有执行语义。

诊断默认启用，可用 `GraphService(runtime, diagnostics=False)` 关闭参数、原始输出和日志采集，保留生命周期与终端结果。
参数通过同一 Hook 贡献通道的同步 `NodeHook.inspect()` 采集：输入在全部 enter 转换之后，原始输出在 exit 转换之前。
同一 firing 的诊断记录最多积累 64 条后批量提交，并在节点结束前冲刷剩余记录，减少文件 SQLite 的逐条事务开销。
采集立即生成独立快照，不修改原对象，失败不改变业务结果。生命周期仍使用公开 Runtime Observer，
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
节点事件。每次执行保存当时的图结构，防止代码图在下一次部署改变后误画旧运行。

Worker 在写入 Execution 终态之前发布 `execution.finished`，服务因此直接按该事件的 status 与 error_type 记录终态，
不读取此刻可能仍为 running 的句柄状态。终端输出由服务的归档线程在执行真正终止后通过公开迭代接口逐项保存，
随后写入带错误消息的最终快照；`service.close()` 会等待已排队的归档，进程在归档前退出时不能保证保留尚未归档的输出。

SQLite 保存草稿、发布版本、运行元数据、启用诊断后采集的参数、节点输出和日志，以及可编码的终端输出。没有执行
标识的 Runtime 事件没有读取入口，不写入数据库。`GraphService(retention=1000)` 默认最多保留 1,000 条执行记录：新执行
写入后，超出数量的最早已结束执行连同其观测、诊断和终端输出一起删除；`retention=None` 关闭清理。当前运行列表在
全局、项目或定义范围内分别读取最近五百条，先筛选范围再限制数量；观测和输出接口分批读取。数据库结构版本记录在
SQLite `user_version` 中，旧版本在启动时逐级迁移，高于当前代码支持版本的数据库会被拒绝打开。

服务重启后把历史在途记录标记为中断，不恢复任务，不提供 broker ack、重试、exactly-once 或跨进程 Slot 连续性。
跨图事件的业务关联键仍属于领域 payload；当前工作台不推断远程调用链。

## 开发与验证

```bash
uv sync --extra service
uv run --extra service pytest -q
uv run --extra service mypy interlace
uv run --extra service ruff check interlace tests examples
uv run --extra service ruff format --check interlace tests examples
```

工作台源码位于 `web/`，使用 React Flow 的图交互与 Lucide 图标。修改界面后在该目录运行：

```bash
npm ci
npm run build
npx playwright install chromium
npm test
```

构建输出位于 `interlace/service/static/`。浏览器测试启动自己的示例服务，验证建图、连线、配置、发布、运行查看
以及桌面和手机布局。普通使用者只需安装 Python extra。

### 主题与 Tokens

右上角外观菜单提供浅色、深色和跟随系统三种模式。偏好保存在浏览器的 `interlace.theme` localStorage 项中，
跟随系统模式会实时响应系统变化。页面在应用加载前应用偏好，切换主题不重新创建当前图或运行实例。

主题统一定义在 [`web/src/tokens.css`](../web/src/tokens.css)，按三层组织：

- 基础层定义调色板、字体、字号、圆角、动效时长与模糊参数。
- 语义层按 `data-theme="light"` 和 `data-theme="dark"` 指定背景、文字、边框、交互、状态及磨砂颜色。
- 组件层定义按钮、输入框、节点、连线、弹窗和表格的用途映射；React Flow 通过其公开 CSS 变量使用相同 tokens。

调整配色优先修改语义层；单独调整组件时修改组件层。布局尺寸与响应式规则保留在组件样式中，业务样式不写颜色
字面量。减少透明效果的系统偏好也由 tokens 切换为当前主题的实色表面。

[上一章：编排模式](08-patterns.md)
