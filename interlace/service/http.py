"""HTTP、JSON-RPC 与 SSE 协议适配，内置 Studio 静态资源。"""

from __future__ import annotations

import asyncio
import json
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from jsonrpcserver import Error, Result, Success, async_dispatch
from pydantic import ValidationError
from starlette.concurrency import run_in_threadpool

from interlace.engine.errors import GraphError, InterlaceRuntimeError

from .models import (
    DraftRequest,
    GraphDefinition,
    NodeDefinition,
    RunRequest,
    ProjectRequest,
    DefinitionProjectRequest,
)
from .service import GraphService, ServiceCapacityError, encode
from .store import ConflictError


def sse(kind: str, value: Any, event_id: int | None = None) -> str:
    """编码一条符合 SSE 行协议的 JSON 消息。

    Args:
        kind: 事件类型。
        value: 可编码的 JSON 内容。
        event_id: 可恢复的观测游标，输出流不设置此字段。

    Returns:
        包含完整消息分隔符的文本。
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

    def __init__(self, app: Any, max_bytes: int) -> None:
        """保存下游应用和请求体上限。

        Args:
            app: 待保护的 ASGI 应用。
            max_bytes: 单个请求体允许的最大字节数。
        """

        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        """累计 HTTP body，并在超过上限时中止解析。

        Args:
            scope: 当前 ASGI 连接作用域。
            receive: 读取请求消息的异步回调。
            send: 发送响应消息的异步回调。
        """

        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        headers = dict(scope.get("headers", ()))
        content_length = headers.get(b"content-length")
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

        async def limited_receive() -> Any:
            """读取一段请求体并检查累计大小。

            Returns:
                未超过容量限制的下一条 ASGI 消息。

            Raises:
                HTTPException: 累计请求体超过配置上限。
            """

            nonlocal received
            message = await receive()
            if message.get("type") == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    raise HTTPException(
                        status_code=413, detail="请求体超过服务允许的大小"
                    )
            return message

        await self.app(scope, limited_receive, send)


def create_app(
    service: GraphService,
    *,
    token: str | None = None,
    max_request_bytes: int = 1_048_576,
) -> FastAPI:
    """创建可独立启动或挂载的官方服务应用。

    Args:
        service: 协议共同使用的图服务。
        token: 可选 API 访问令牌，不放入 URL。
        max_request_bytes: 单个 HTTP 请求体允许的最大字节数。

    Returns:
        带 OpenAPI、RPC 和 Studio 的 FastAPI 应用。
    """

    if type(max_request_bytes) is not int or max_request_bytes < 1:
        raise ValueError("max_request_bytes must be an integer greater than zero")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """在 ASGI 停止时清理服务观察与存储。

        Args:
            app: 当前服务应用。

        Yields:
            应用正常提供服务的生命周期区间。
        """

        del app
        try:
            yield
        finally:
            service.close()

    app = FastAPI(title="Interlace Graph Service", version="1.0", lifespan=lifespan)
    app.add_middleware(RequestBodyLimitMiddleware, max_bytes=max_request_bytes)

    @app.middleware("http")
    async def access(request: Request, call_next: Any) -> Response:
        """检查 API 令牌及浏览器同源写入。

        Args:
            request: 当前请求。
            call_next: 下游应用调用入口。

        Returns:
            业务响应或明确的访问拒绝。
        """

        relative_path = request.url.path.removeprefix(
            request.scope.get("root_path", "")
        )
        protected = relative_path.startswith(
            ("/api", "/rpc", "/openapi.json", "/docs", "/redoc")
        )
        if protected and token is not None:
            supplied = request.headers.get("authorization", "")
            if not secrets.compare_digest(supplied, f"Bearer {token}"):
                return JSONResponse({"detail": "需要有效的访问令牌"}, status_code=401)
        origin = request.headers.get("origin")
        if (
            protected
            and origin
            and urlsplit(origin).netloc != request.headers.get("host")
        ):
            return JSONResponse({"detail": "不允许跨站请求"}, status_code=403)
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        if protected:
            response.headers["Cache-Control"] = "no-store"
        return response

    @app.exception_handler(Exception)
    async def failure(request: Request, exc: Exception) -> JSONResponse:
        """把服务异常映射为稳定 HTTP 错误。

        Args:
            request: 发生异常的请求。
            exc: 服务抛出的异常。

        Returns:
            带错误详情的 JSON 响应。
        """

        del request
        status = 500
        if isinstance(exc, KeyError):
            status = 404
        elif isinstance(exc, ConflictError):
            status = 409
        elif isinstance(exc, ServiceCapacityError):
            status = 429
        elif isinstance(exc, (ValueError, TypeError, ValidationError, GraphError)):
            status = 422
        elif isinstance(exc, InterlaceRuntimeError):
            status = 409
        return JSONResponse(
            {"detail": str(exc), "error_type": type(exc).__name__}, status_code=status
        )

    # 注册具体错误处理器，使普通参数错误不会穿过 ASGI 的服务器异常边界。
    for error_type in (
        KeyError,
        ValueError,
        TypeError,
        GraphError,
        InterlaceRuntimeError,
        ServiceCapacityError,
    ):
        app.add_exception_handler(error_type, failure)

    @app.get("/api/graphs")
    def graphs() -> Any:
        """读取已注册图。

        Returns:
            代码图与发布图结构集合。
        """

        return service.graphs()

    @app.get("/api/projects")
    def projects() -> Any:
        """读取项目列表与定义统计。

        Returns:
            持久项目清单。
        """

        return service.projects()

    @app.post("/api/projects", status_code=201)
    def create_project(body: ProjectRequest) -> Any:
        """创建新的管理项目。

        Args:
            body: 项目名称和描述。

        Returns:
            新建项目的稳定标识和元数据。
        """

        return service.store.save_project(body.name, body.description)

    @app.post("/api/projects/{project_id}")
    def update_project(project_id: str, body: ProjectRequest) -> Any:
        """更新项目显示信息。

        Args:
            project_id: 目标项目标识。
            body: 新名称与描述。

        Returns:
            更新后的项目信息。
        """

        return service.store.save_project(body.name, body.description, project_id)

    @app.delete("/api/projects/{project_id}")
    def delete_project(project_id: str) -> Any:
        """删除不含定义的项目。

        Args:
            project_id: 目标项目标识。

        Returns:
            删除结果。
        """

        service.definitions()
        service.store.delete_project(project_id)
        return {"deleted": True}

    @app.get("/api/projects/{project_id}/definitions")
    def project_definitions(project_id: str) -> Any:
        """读取指定项目内的定义列表。

        Args:
            project_id: 所属项目。

        Returns:
            项目元数据与定义摘要列表。
        """

        return {
            "project": service.store.project(project_id),
            "definitions": service.definitions(project_id),
        }

    @app.get("/api/definitions/{name:path}")
    def definition_detail(name: str, project_id: str | None = None) -> Any:
        """读取单条定义详情及其运行和版本。

        Args:
            name: 定义全局标识。
            project_id: 可选项目范围，不匹配时返回不存在。

        Returns:
            管理详情。
        """

        detail = service.definition(name)
        if project_id is not None and detail["project_id"] != project_id:
            raise KeyError("该项目下没有此定义")
        return detail

    @app.post("/api/projects/{project_id}/definitions", status_code=201)
    def create_definition(project_id: str, body: GraphDefinition) -> Any:
        """在项目中创建新的定义草稿，不覆盖已有全局标识。

        Args:
            project_id: 所属项目标识。
            body: 新定义的初始文档。

        Returns:
            创建后的定义详情。
        """

        with service.store.lock:
            service.definitions()
            if body.name in service.store.memberships():
                raise ConflictError("定义标识已存在，请使用其他标识")
            service.store.save_draft(body.name, body.model_dump(), 0, project_id)
        return service.definition(body.name)

    @app.get("/api/projects/{project_id}/executions")
    def project_executions(project_id: str) -> Any:
        """读取项目内全部定义的最近运行。

        Args:
            project_id: 所属项目标识。

        Returns:
            带逻辑定义标识的运行记录。
        """

        allowed = {item["name"] for item in service.definitions(project_id)}
        names = {
            f"{item['name']}@v{item['version']}": item["name"]
            for item in service.store.versions()
        }
        return [
            {**run, "definition_name": names.get(run["graph"], run["graph"])}
            for run in service.executions(project_id=project_id)
            if names.get(run["graph"], run["graph"]) in allowed
        ]

    @app.post("/api/definition-project")
    def move_definition(body: DefinitionProjectRequest) -> Any:
        """整体移动定义的项目归属。

        Args:
            body: 定义标识和目标项目。

        Returns:
            更新后的定义详情。
        """

        service.definition(body.name)
        service.store.assign_definition(body.name, body.project_id)
        return service.definition(body.name)

    @app.get("/api/nodes")
    def nodes() -> Any:
        """读取可编辑节点目录。

        Returns:
            节点类型及配置表单 Schema。
        """

        return service.nodes.describe()

    @app.post("/api/nodes/preview")
    def preview(body: NodeDefinition) -> Any:
        """构造单节点并读取真实端口，包括依配置变化的端口。

        Args:
            body: 节点类型与配置。

        Returns:
            节点输入和输出端口。
        """

        node = service.nodes.create(body.type, body.config)
        return {
            "inputs": {key: value.__name__ for key, value in node.input_ports.items()},
            "outputs": {
                key: value.__name__ for key, value in node.output_ports.items()
            },
        }

    @app.get("/api/drafts")
    def drafts() -> Any:
        """读取保存的编辑草稿。

        Returns:
            草稿文档与修订号。
        """

        return service.store.drafts()

    @app.post("/api/drafts")
    def save(body: DraftRequest) -> Any:
        """允许保存尚未满足冻结约束的编辑草稿。

        Args:
            body: 文档和预期修订号。

        Returns:
            新草稿修订号。
        """

        service.definitions()
        return {
            "revision": service.store.save_draft(
                body.definition.name,
                body.definition.model_dump(),
                body.revision,
                body.project_id,
            )
        }

    @app.post("/api/validate")
    def validate(body: GraphDefinition) -> Any:
        """通过核心冻结逻辑校验编辑中的图。

        Args:
            body: 完整图文档。

        Returns:
            校验后的真实图结构。
        """

        return service.validate(body)

    @app.post("/api/definitions/parse")
    def parse_definition(body: GraphDefinition) -> Any:
        """校验导入文件的文档格式，允许尚未完成的草稿。

        Args:
            body: 待导入的图文档。

        Returns:
            补齐画布默认值后的结构化文档。
        """

        return body.model_dump()

    @app.post("/api/publish")
    def publish(body: DraftRequest) -> Any:
        """发布一个新的不可变执行版本。

        Args:
            body: 当前图文档与修订号。

        Returns:
            发布版本及运行名称。
        """

        return service.publish(body)

    @app.get("/api/executions")
    def executions() -> Any:
        """查询当前和历史执行。

        Returns:
            执行状态快照集合。
        """

        return service.executions()

    @app.post("/api/executions", status_code=202)
    def start(body: RunRequest) -> Any:
        """提交后台执行并返回标识。

        Args:
            body: 图注册名与执行输入。

        Returns:
            已提交 Execution 的状态。
        """

        return service.snapshot(service.start(body))

    @app.post("/api/call")
    async def call(body: RunRequest) -> Any:
        """等待执行结束并返回完整 terminal Outputs。

        Args:
            body: 图注册名与执行参数。

        Returns:
            执行状态以及按追加顺序排列的输出。
        """

        execution = await run_in_threadpool(service.start, body)
        try:
            outputs = await execution
        except Exception as exc:
            return JSONResponse(
                {**service.snapshot(execution), "detail": str(exc)}, status_code=409
            )
        return {**service.snapshot(execution), "outputs": encode(outputs)}

    @app.get("/api/executions/{execution_id}")
    def execution(execution_id: str) -> Any:
        """取得一次执行的当前状态。

        Args:
            execution_id: 执行标识。

        Returns:
            当前或历史状态快照。
        """

        return service.execution(execution_id)

    @app.post("/api/executions/{execution_id}/cancel")
    def cancel(execution_id: str) -> Any:
        """向当前进程的执行提交协作式取消。

        Args:
            execution_id: 执行标识。

        Returns:
            取消请求是否被接受。
        """

        handle = service.runtime.get_execution(execution_id)
        return {"accepted": handle.cancel(), **service.snapshot(handle)}

    @app.get("/api/executions/{execution_id}/events")
    async def events(
        execution_id: str, request: Request, after: int = 0
    ) -> StreamingResponse:
        """重放并持续订阅执行观测，支持 Last-Event-ID 重连。

        Args:
            execution_id: 执行标识。
            request: 当前 HTTP 请求。
            after: 已读观测编号，默认从头。

        Returns:
            带观测游标与终态通知的 SSE 流。
        """

        service.execution(execution_id)
        cursor = max(after, int(request.headers.get("last-event-id", "0")))

        async def stream() -> AsyncIterator[str]:
            """按数据库游标逐批交付观测并等待终态。

            Yields:
                观测消息、心跳或结束消息。
            """

            nonlocal cursor
            while not await request.is_disconnected():
                batch = await run_in_threadpool(
                    service.store.events, execution_id, cursor
                )
                for item in batch:
                    cursor = item["id"]
                    yield sse("observation", item, cursor)
                state = service.execution(execution_id)
                if state["done"] and not batch:
                    yield sse("finished", state)
                    return
                if not batch:
                    yield ": heartbeat\n\n"
                    await asyncio.sleep(0.2)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"X-Accel-Buffering": "no"},
        )

    @app.get("/api/executions/{execution_id}/outputs")
    async def outputs(execution_id: str) -> StreamingResponse:
        """从首项重放并持续交付原生输出流，断开只释放订阅。

        Args:
            execution_id: 当前 Runtime 保留的执行标识。

        Returns:
            逐项输出和终态的 SSE 流。
        """

        live = {item.id: item for item in service.runtime.executions()}
        state = service.execution(execution_id)

        async def stream() -> AsyncIterator[str]:
            """逐项等待输出，失败保留之前已经交付的项目。

            Yields:
                输出消息及成功或失败终态。
            """

            if execution_id not in live:
                offset = 0
                while True:
                    batch = await run_in_threadpool(
                        service.store.outputs, execution_id, offset
                    )
                    if not batch:
                        break
                    for item in batch:
                        yield sse("output", item)
                    offset += len(batch)
                error = state.get("output_error") or state.get("error")
                yield sse(
                    "failure" if error else "finished", {**state, "detail": error}
                )
                return
            handle = live[execution_id]
            iterator = handle.__aiter__()
            try:
                async for output in iterator:
                    yield sse("output", encode(output))
                yield sse("finished", service.snapshot(handle))
            except Exception as exc:
                yield sse("failure", {**service.snapshot(handle), "detail": str(exc)})
            finally:
                await iterator.aclose()

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"X-Accel-Buffering": "no"},
        )

    async def rpc_call(**params: Any) -> Result:
        """调用图并返回 JSON-RPC 结果。

        Args:
            **params: RunRequest 的命名参数。

        Returns:
            JSON-RPC 成功结果或明确错误。
        """

        try:
            body = RunRequest.model_validate(params)
            handle = await run_in_threadpool(service.start, body)
        except (ValueError, TypeError, KeyError) as exc:
            return Error(-32602, "Invalid params", str(exc))
        try:
            values = await handle
            return Success({**service.snapshot(handle), "outputs": encode(values)})
        except Exception as exc:
            return Error(
                -32000,
                "Graph execution failed",
                {**service.snapshot(handle), "detail": str(exc)},
            )

    @app.post("/rpc")
    async def rpc(request: Request) -> Response:
        """通过标准 JSON-RPC 分发器处理调用、批量与通知。

        Args:
            request: 原始 JSON-RPC 2.0 请求。

        Returns:
            协议响应；通知返回无内容 HTTP 响应。
        """

        response = await async_dispatch(
            await request.body(), methods={"graph.call": rpc_call}
        )
        return Response(
            response,
            media_type="application/json",
            status_code=200 if response else 204,
        )

    static = Path(__file__).with_name("static")
    if static.is_dir():
        app.mount("/", StaticFiles(directory=static, html=True), name="studio")
    return app
