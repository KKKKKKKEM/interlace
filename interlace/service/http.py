"""HTTP、JSON-RPC 与 SSE 协议适配，内置 Studio 静态资源。"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
from collections.abc import AsyncIterator, Iterable
from contextlib import suppress
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from jsonrpcserver import Error, Result, Success, async_dispatch
from pydantic import ValidationError
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from interlace import Execution
from interlace.engine.errors import GraphError, RuntimeClosedError

from .diagnostics import redact_text
from .errors import (
    ConflictError,
    InvalidRequestError,
    NotFoundError,
    OutputEncodingError,
    ServiceCapacityError,
    ServiceClosedError,
)
from .models import (
    DefinitionProjectRequest,
    DraftRequest,
    GraphDefinition,
    NodeDefinition,
    ProjectRequest,
    RunRequest,
)
from .service import LOOPBACK_HOSTS, GraphService, encode

_LOGGER = logging.getLogger(__name__)

# 单个输出 SSE 允许积压的未发送条数；客户端落后超过该值即断开，观察端不得背压业务执行。
FOLLOW_BACKLOG = 10_000

# JSON-RPC 服务端错误码，与 HTTP 接口的状态码一一对应。
RPC_EXECUTION_FAILED = -32000  # Graph 执行失败，HTTP /api/call 返回 409
RPC_OUTPUT_ENCODING_FAILED = -32001  # 输出无法编码为 JSON，HTTP 500
RPC_CAPACITY_EXCEEDED = -32002  # 活跃执行达到上限，HTTP 429
RPC_UNAVAILABLE = -32003  # 服务或 Runtime 已关闭，HTTP 409

# 已知错误类型到 HTTP 状态码，按顺序匹配；未列出的异常返回 500 且不透出消息。
_ERROR_STATUS: tuple[tuple[type[Exception], int], ...] = (
    (NotFoundError, 404),
    (InvalidRequestError, 422),
    (ConflictError, 409),
    (ServiceCapacityError, 429),
    (ServiceClosedError, 409),
    (OutputEncodingError, 500),
    (ValidationError, 422),
    (GraphError, 422),
    (RuntimeClosedError, 409),
)

# 需要令牌与同源检查的路径前缀，相对于挂载点。
_PROTECTED = ("/api", "/rpc", "/openapi.json", "/docs", "/redoc")


def sse(kind: str, value: Any, event_id: int | None = None) -> str:
    """编码一条符合 SSE 行协议的 JSON 消息。

    Args:
        kind: SSE event 字段。
        value: 可严格编码为 JSON 的内容。
        event_id: 可用于 Last-Event-ID 恢复的观测游标；None 不写 id 行。

    Returns:
        以空行结尾的完整消息文本。
    """

    prefix = "" if event_id is None else f"id: {event_id}\n"
    return (
        prefix
        + f"event: {kind}\ndata: {json.dumps(value, ensure_ascii=False, allow_nan=False)}\n\n"
    )


class RequestBodyLimitMiddleware:
    """在应用解析请求前限制 HTTP body 的累计字节数。

    Attributes:
        app: 下游 ASGI 应用。
        max_bytes: 单个请求体允许的最大字节数。
    """

    def __init__(self, app: ASGIApp, max_bytes: int) -> None:
        """保存下游应用和请求体上限。

        Args:
            app: 待保护的 ASGI 应用。
            max_bytes: 单个请求体允许的最大字节数。
        """

        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """先按 Content-Length 拒绝过大请求，再在读取分块时累计检查。

        Args:
            scope: 当前 ASGI 连接作用域。
            receive: 读取请求消息的异步回调。
            send: 发送响应消息的异步回调。
        """

        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        content_length = Headers(scope=scope).get("content-length")
        if content_length is not None:
            try:
                too_large = int(content_length) > self.max_bytes
            except ValueError:
                too_large = False
            if too_large:
                response = JSONResponse(
                    {"detail": "请求体超过服务允许的大小"}, status_code=413
                )
                await response(scope, receive, send)
                return
        received = 0

        async def limited_receive() -> Message:
            """读取下一条请求消息，累计 body 超过上限时抛出 HTTP 413。"""

            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    raise HTTPException(
                        status_code=413, detail="请求体超过服务允许的大小"
                    )
            return message

        await self.app(scope, limited_receive, send)


class AccessMiddleware:
    """纯 ASGI 访问控制：Host 白名单、Bearer 令牌、浏览器同源写入与安全响应头。

    Host 检查作用于全部 HTTP 与 WebSocket 请求，用于抵御 DNS rebinding；令牌与 Origin
    检查只作用于 API、RPC 和 OpenAPI 路径，Studio 静态资源无需令牌即可加载。

    Attributes:
        app: 下游 ASGI 应用。
        allowed_hosts: 允许的小写主机名，包含 `*` 时不限制。
        expected: 期望的完整 Authorization 头字节，None 表示不要求令牌。
    """

    def __init__(
        self, app: ASGIApp, *, token: str | None, allowed_hosts: frozenset[str]
    ) -> None:
        """保存下游应用、令牌与 Host 白名单。

        Args:
            app: 待保护的 ASGI 应用。
            token: Bearer 令牌，None 不检查 Authorization。
            allowed_hosts: 已规范化的主机名集合。
        """

        self.app = app
        self.allowed_hosts = allowed_hosts
        self.expected = None if token is None else f"Bearer {token}".encode()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """依次检查 Host、令牌和 Origin，通过后为响应补充安全头。

        Args:
            scope: 当前 ASGI 连接作用域。
            receive: 读取请求消息的异步回调。
            send: 发送响应消息的异步回调。
        """

        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        headers = Headers(scope=scope)
        host = headers.get("host", "")
        if "*" not in self.allowed_hosts and _hostname(host) not in self.allowed_hosts:
            await _reject(scope, receive, send, 400, "Host 不在服务允许的主机列表中")
            return
        path = scope["path"].removeprefix(scope.get("root_path", ""))
        protected = path.startswith(_PROTECTED)
        if protected and self.expected is not None:
            # ASGI 头是原始字节，按字节比较，非 ASCII 凭据得到 401 而不是异常。
            supplied = headers.get("authorization", "").encode("latin-1")
            if not secrets.compare_digest(supplied, self.expected):
                await _reject(scope, receive, send, 401, "需要有效的访问令牌")
                return
        origin = headers.get("origin")
        if protected and origin and urlsplit(origin).netloc != host:
            await _reject(scope, receive, send, 403, "不允许跨站请求")
            return

        async def secured_send(message: Message) -> None:
            """加入 nosniff 与禁止嵌入的响应头，受保护路径额外禁止缓存。"""

            if message["type"] == "http.response.start":
                response_headers = MutableHeaders(scope=message)
                response_headers["X-Content-Type-Options"] = "nosniff"
                # 禁止第三方页面以 iframe 嵌入工作台诱导点击发布、运行或删除。
                response_headers["X-Frame-Options"] = "DENY"
                response_headers["Content-Security-Policy"] = "frame-ancestors 'none'"
                if protected:
                    response_headers["Cache-Control"] = "no-store"
            await send(message)

        await self.app(scope, receive, secured_send)


def _hostname(host: str) -> str | None:
    """从 Host 头取出小写主机名，去掉端口与 IPv6 方括号；无法解析时为 None。"""

    try:
        return urlsplit(f"//{host}").hostname
    except ValueError:
        return None


async def _reject(
    scope: Scope, receive: Receive, send: Send, status: int, detail: str
) -> None:
    """以 JSON 错误响应拒绝 HTTP 请求，WebSocket 握手以策略违规关闭。"""

    if scope["type"] == "websocket":
        await send({"type": "websocket.close", "code": 1008})
        return
    await JSONResponse({"detail": detail}, status_code=status)(scope, receive, send)


def _host_allowlist(allowed_hosts: Iterable[str] | None) -> frozenset[str]:
    """规范化 Host 白名单；None 只允许本机回环名称。

    Raises:
        TypeError: allowed_hosts 是单个字符串。
    """

    if allowed_hosts is None:
        return LOOPBACK_HOSTS
    if isinstance(allowed_hosts, str):
        raise TypeError("allowed_hosts must be an iterable of host names")
    return frozenset(
        host.strip().lower().removeprefix("[").removesuffix("]")
        for host in allowed_hosts
    )


def create_app(
    service: GraphService,
    *,
    token: str | None = None,
    max_request_bytes: int = 1_048_576,
    allowed_hosts: Iterable[str] | None = None,
) -> FastAPI:
    """创建可独立启动或挂载的官方服务应用；应用不拥有服务的生命周期。

    Args:
        service: 协议共同使用的图服务；ASGI 关闭时不关闭它，由创建服务的调用方负责 `close()`。
        token: 可选 API 访问令牌，通过 Authorization: Bearer 传递，不放入 URL。
        max_request_bytes: 单个 HTTP 请求体允许的最大字节数，默认 1 MiB。
        allowed_hosts: 允许的 Host 头主机名，None 只允许 127.0.0.1、localhost 与 ::1；
            包含 `*` 时不限制。

    Returns:
        带 OpenAPI、JSON-RPC 和 Studio 的 FastAPI 应用。

    Raises:
        ValueError: max_request_bytes 不是正整数。
        TypeError: allowed_hosts 是单个字符串。
    """

    if type(max_request_bytes) is not int or max_request_bytes < 1:
        raise ValueError("max_request_bytes must be an integer greater than zero")
    hosts = _host_allowlist(allowed_hosts)

    app = FastAPI(title="Interlace Graph Service", version="1.0")
    app.add_middleware(RequestBodyLimitMiddleware, max_bytes=max_request_bytes)
    app.add_middleware(AccessMiddleware, token=token, allowed_hosts=hosts)
    _add_error_handlers(app)
    _add_project_routes(app, service)
    _add_editor_routes(app, service)
    _add_execution_routes(app, service)
    _add_stream_routes(app, service)
    _add_rpc_route(app, service)
    static = Path(__file__).with_name("static")
    if static.is_dir():
        app.mount("/", StaticFiles(directory=static, html=True), name="studio")
    return app


def _add_error_handlers(app: FastAPI) -> None:
    """把已知服务错误映射为稳定状态码，其他异常记录日志后返回通用 500。"""

    async def known_failure(request: Request, exc: Exception) -> Response:
        """返回已知错误的状态码、消息与类型名。"""

        del request
        status = next(code for kind, code in _ERROR_STATUS if isinstance(exc, kind))
        return JSONResponse(
            {"detail": str(exc), "error_type": type(exc).__name__}, status_code=status
        )

    async def internal_failure(request: Request, exc: Exception) -> Response:
        """记录未预期异常，响应中不包含异常消息。"""

        _LOGGER.error(
            "unhandled graph service error: %s %s",
            request.method,
            request.url.path,
            exc_info=exc,
        )
        return JSONResponse({"detail": "服务内部错误"}, status_code=500)

    for kind, _ in _ERROR_STATUS:
        app.add_exception_handler(kind, known_failure)
    app.add_exception_handler(Exception, internal_failure)


def _add_project_routes(app: FastAPI, service: GraphService) -> None:
    """注册项目、定义管理与项目运行记录接口。"""

    @app.get("/api/projects")
    def projects() -> Any:
        """读取项目列表与定义统计。"""

        return service.projects()

    @app.post("/api/projects", status_code=201)
    def create_project(body: ProjectRequest) -> Any:
        """创建新的管理项目。"""

        return service.create_project(body.name, body.description)

    @app.post("/api/projects/{project_id}")
    def update_project(project_id: str, body: ProjectRequest) -> Any:
        """更新项目名称与描述。"""

        return service.update_project(project_id, body.name, body.description)

    @app.delete("/api/projects/{project_id}")
    def delete_project(project_id: str) -> Any:
        """删除不含定义的非默认项目。"""

        service.delete_project(project_id)
        return {"deleted": True}

    @app.get("/api/projects/{project_id}/definitions")
    def project_definitions(project_id: str) -> Any:
        """读取项目元数据与项目内的定义摘要。"""

        return {
            "project": service.project(project_id),
            "definitions": service.definitions(project_id),
        }

    @app.post("/api/projects/{project_id}/definitions", status_code=201)
    def create_definition(project_id: str, body: GraphDefinition) -> Any:
        """在项目中创建新的定义草稿，已存在的全局标识返回 409。"""

        return service.create_definition(project_id, body)

    @app.get("/api/projects/{project_id}/executions")
    def project_executions(project_id: str) -> Any:
        """读取项目内全部定义的最近运行，每项带 definition_name。"""

        return service.executions(project_id=project_id)

    @app.get("/api/definitions/{name:path}")
    def definition_detail(name: str, project_id: str | None = None) -> Any:
        """读取单条定义详情；指定 project_id 且不匹配时返回 404。"""

        return service.definition(name, project_id)

    @app.post("/api/definition-project")
    def move_definition(body: DefinitionProjectRequest) -> Any:
        """整体移动定义的项目归属。"""

        return service.move_definition(body.name, body.project_id)


def _add_editor_routes(app: FastAPI, service: GraphService) -> None:
    """注册图目录、节点目录、草稿、校验与发布接口。"""

    @app.get("/api/graphs")
    def graphs() -> Any:
        """读取 Runtime 中全部注册图的结构。"""

        return service.graphs()

    @app.get("/api/nodes")
    def nodes() -> Any:
        """读取可编辑节点类型及配置表单 Schema。"""

        return service.node_types()

    @app.post("/api/nodes/preview")
    def preview(body: NodeDefinition) -> Any:
        """构造单节点并返回真实端口，包括依配置变化的端口。"""

        return service.preview_node(body)

    @app.get("/api/drafts")
    def drafts() -> Any:
        """读取全部编辑草稿与修订号。"""

        return service.drafts()

    @app.post("/api/drafts")
    def save_draft(body: DraftRequest) -> Any:
        """保存可不完整的草稿，返回新修订号。"""

        return {"revision": service.save_draft(body)}

    @app.post("/api/validate")
    def validate(body: GraphDefinition) -> Any:
        """通过核心冻结逻辑校验图文档并返回真实结构。"""

        return service.validate(body)

    @app.post("/api/definitions/parse")
    def parse_definition(body: GraphDefinition) -> Any:
        """校验导入文件的文档格式并补齐默认值，允许尚未完成的草稿。"""

        return body.model_dump()

    @app.post("/api/publish")
    def publish(body: DraftRequest) -> Any:
        """发布新的不可变版本，返回注册名、版本号与新修订号。"""

        return service.publish(body)


def _add_execution_routes(app: FastAPI, service: GraphService) -> None:
    """注册执行提交、同步调用、查询与取消接口。"""

    @app.get("/api/executions")
    def executions() -> Any:
        """查询当前与历史执行。"""

        return service.executions()

    @app.post("/api/executions", status_code=202)
    def start(body: RunRequest) -> Any:
        """提交后台执行并返回初始状态。"""

        return service.snapshot(service.start(body))

    @app.post("/api/call")
    async def call(body: RunRequest) -> Any:
        """等待执行结束并返回按追加顺序排列的全部 terminal Outputs。

        执行失败返回 409 与状态快照；输出无法编码为 JSON 返回 500，执行终态不变。
        """

        execution = await run_in_threadpool(service.start, body)
        try:
            outputs = await execution
        except Exception as exc:
            return JSONResponse(
                {**service.snapshot(execution), "detail": redact_text(str(exc))},
                status_code=409,
            )
        try:
            encoded = encode(outputs)
        except OutputEncodingError as exc:
            return JSONResponse(
                {
                    **service.snapshot(execution),
                    "detail": str(exc),
                    "error_type": type(exc).__name__,
                },
                status_code=500,
            )
        return {**service.snapshot(execution), "outputs": encoded}

    @app.get("/api/executions/{execution_id}")
    def execution(execution_id: str) -> Any:
        """读取一次执行的当前或历史状态。"""

        return service.execution(execution_id)

    @app.post("/api/executions/{execution_id}/cancel")
    def cancel(execution_id: str) -> Any:
        """向当前进程中的执行提交协作式取消。"""

        return service.cancel(execution_id)


async def _pump_outputs(
    handle: Execution, pending: asyncio.Queue[tuple[str, Any]]
) -> None:
    """尽快读取执行输出放入连接的发送队列，积压超过 FOLLOW_BACKLOG 时停止订阅。

    Args:
        handle: 正在运行的执行。
        pending: 本连接独占的发送队列，元素为事件类别与值。
    """

    iterator = handle.__aiter__()
    try:
        async for output in iterator:
            if pending.qsize() >= FOLLOW_BACKLOG:
                pending.put_nowait(("lagged", None))
                return
            pending.put_nowait(("output", output))
        pending.put_nowait(("finished", None))
    except Exception as exc:
        pending.put_nowait(("failure", exc))
    finally:
        await iterator.aclose()


async def _follow_outputs(
    service: GraphService, handle: Execution
) -> AsyncIterator[str]:
    """由服务端任务及时读取输出再转交客户端，失败保留之前已经交付的项目。

    输出订阅只由读取任务持有并持续读空，客户端读取快慢不会阻塞执行；积压超过
    FOLLOW_BACKLOG 时停止订阅并以 failure 事件断开，执行结束后可经历史重放完整输出。

    Args:
        service: 提供快照的服务。
        handle: 正在运行的执行。

    Yields:
        SSE 格式的 output、finished 或 failure 消息。
    """

    pending: asyncio.Queue[tuple[str, Any]] = asyncio.Queue()
    reader = asyncio.create_task(_pump_outputs(handle, pending))
    try:
        while True:
            kind, value = await pending.get()
            if kind == "output":
                yield sse("output", encode(value))
                continue
            if kind == "finished":
                yield sse("finished", service.snapshot(handle))
                return
            detail = (
                "output subscriber fell behind; replay after the execution finishes"
                if kind == "lagged"
                else redact_text(str(value))
            )
            yield sse("failure", {**service.snapshot(handle), "detail": detail})
            return
    except Exception as exc:
        yield sse(
            "failure", {**service.snapshot(handle), "detail": redact_text(str(exc))}
        )
    finally:
        reader.cancel()
        with suppress(asyncio.CancelledError):
            await reader


def _add_stream_routes(app: FastAPI, service: GraphService) -> None:
    """注册执行观测与终端输出的 SSE 订阅接口。"""

    @app.get("/api/executions/{execution_id}/events")
    async def events(
        execution_id: str, request: Request, after: int = 0
    ) -> StreamingResponse:
        """重放并持续订阅执行观测，支持 after 参数或 Last-Event-ID 重连。"""

        await run_in_threadpool(service.execution, execution_id)
        try:
            cursor = max(after, int(request.headers.get("last-event-id", "0")))
        except ValueError as exc:
            raise InvalidRequestError("Last-Event-ID 必须是整数") from exc

        async def stream() -> AsyncIterator[str]:
            """逐批交付观测；先读终态再读批次，终态后读到空批次才发送 finished。"""

            nonlocal cursor
            while not await request.is_disconnected():
                state = await run_in_threadpool(service.execution, execution_id)
                batch = await run_in_threadpool(service.events, execution_id, cursor)
                for item in batch:
                    cursor = item["id"]
                    yield sse("observation", item, cursor)
                if batch:
                    continue
                if state["done"]:
                    # 终态之前发布的观测都已在上面的空批次之前写入并交付。
                    yield sse(
                        "finished",
                        await run_in_threadpool(service.execution, execution_id),
                    )
                    return
                yield ": heartbeat\n\n"
                await asyncio.sleep(0.2)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"X-Accel-Buffering": "no"},
        )

    @app.get("/api/executions/{execution_id}/outputs")
    async def outputs(execution_id: str) -> StreamingResponse:
        """从首项重放并持续交付输出流；客户端断开只释放订阅，不取消执行。"""

        handle = service.live_execution(execution_id)
        state = await run_in_threadpool(service.execution, execution_id)

        async def replay() -> AsyncIterator[str]:
            """从历史数据库分批重放已归档输出，并按记录发送终态。"""

            offset = 0
            while batch := await run_in_threadpool(
                service.outputs, execution_id, offset
            ):
                for item in batch:
                    yield sse("output", item)
                offset += len(batch)
            error = state.get("output_error") or state.get("error")
            yield sse("failure" if error else "finished", {**state, "detail": error})

        return StreamingResponse(
            replay() if handle is None else _follow_outputs(service, handle),
            media_type="text/event-stream",
            headers={"X-Accel-Buffering": "no"},
        )


def _add_rpc_route(app: FastAPI, service: GraphService) -> None:
    """注册 JSON-RPC 2.0 入口，提供 graph.call 方法。"""

    async def rpc_call(**params: Any) -> Result:
        """执行 graph.call：参数与 RunRequest 相同，等待全部终端输出。

        参数错误与未知图返回 -32602；容量、关闭、执行失败与输出编码失败使用模块常量中的
        专用错误码，其他异常记录日志后返回不含消息的 -32603。
        """

        try:
            body = RunRequest.model_validate(params)
            handle = await run_in_threadpool(service.start, body)
        except (ValidationError, InvalidRequestError, NotFoundError) as exc:
            return Error(-32602, "Invalid params", str(exc))
        except ServiceCapacityError as exc:
            return Error(RPC_CAPACITY_EXCEEDED, "Service capacity exceeded", str(exc))
        except (ServiceClosedError, RuntimeClosedError) as exc:
            return Error(RPC_UNAVAILABLE, "Service unavailable", str(exc))
        except Exception:
            _LOGGER.exception("graph.call failed before execution started")
            return Error(-32603, "Internal error")
        try:
            values = await handle
        except Exception as exc:
            return Error(
                RPC_EXECUTION_FAILED,
                "Graph execution failed",
                {**service.snapshot(handle), "detail": redact_text(str(exc))},
            )
        try:
            encoded = encode(values)
        except OutputEncodingError as exc:
            return Error(
                RPC_OUTPUT_ENCODING_FAILED,
                "Output encoding failed",
                {**service.snapshot(handle), "detail": str(exc)},
            )
        return Success({**service.snapshot(handle), "outputs": encoded})

    @app.post("/rpc")
    async def rpc(request: Request) -> Response:
        """通过标准 JSON-RPC 分发器处理调用、批量与通知；通知返回 HTTP 204。"""

        response = await async_dispatch(
            await request.body(), methods={"graph.call": rpc_call}
        )
        return Response(
            response,
            media_type="application/json",
            status_code=200 if response else 204,
        )
