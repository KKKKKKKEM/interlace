"""服务访问边界、错误映射、观测终态、历史保留与启动回滚契约。"""

from __future__ import annotations

import logging
import sqlite3
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jsonrpcserver")

from fastapi.testclient import TestClient

from interlace import ExecutionStatus, Graph, Node, Output, Runtime
from interlace.engine.errors import ExecutionError
from interlace.engine.observation import RuntimeEvent, RuntimeEventKind
from interlace.service import GraphService, NodeCatalog, RunRequest
from interlace.service.http import (
    RPC_CAPACITY_EXCEEDED,
    RPC_OUTPUT_ENCODING_FAILED,
    RPC_UNAVAILABLE,
)
from interlace.service.models import DraftRequest, GraphDefinition
from interlace.service.store import ServiceStore

# 默认 Host 白名单只接受本机名称，测试客户端使用 localhost 而非 testserver。
LOCAL = "http://localhost"


class Echo(Node):
    """原样返回 default 端口输入。"""

    def execute(self, inputs: Any, context: Any) -> Output:
        """返回输入值。

        Args:
            inputs: default 端口输入。
            context: 当前执行上下文。

        Returns:
            原始输入值。
        """

        del context
        return Output(inputs["default"])


class Opaque(Node):
    """成功返回无法编码为 JSON 的对象。"""

    def execute(self, inputs: Any, context: Any) -> Output:
        """返回没有序列化规则的对象。

        Args:
            inputs: 忽略的入口值。
            context: 当前执行上下文。

        Returns:
            携带普通 object 的输出。
        """

        del inputs, context
        return Output(object())


class Broken(Node):
    """立即以业务异常失败。"""

    def execute(self, inputs: Any, context: Any) -> Output:
        """抛出测试异常。

        Args:
            inputs: 忽略的入口值。
            context: 当前执行上下文。

        Raises:
            ValueError: 测试业务失败。
        """

        del inputs, context
        raise ValueError("broken")


class Waiting(Node):
    """通过检查点等待取消的节点。"""

    def execute(self, inputs: Any, context: Any) -> None:
        """持续检查取消直到执行被终止。

        Args:
            inputs: 忽略的入口值。
            context: 提供协作式检查点的上下文。
        """

        del inputs
        while True:
            context.checkpoint()
            time.sleep(0.002)


def document(name: str) -> dict[str, Any]:
    """构造单节点可发布文档。

    Args:
        name: 定义全局标识。

    Returns:
        使用 echo 节点类型的图文档。
    """

    return {
        "name": name,
        "entrypoint": "echo",
        "nodes": [{"id": "echo", "type": "echo", "config": {}}],
        "edges": [],
    }


def catalog() -> NodeCatalog:
    """创建只登记 echo 节点的目录。

    Returns:
        测试节点目录。
    """

    return NodeCatalog().register_node("echo", Echo)


@pytest.fixture
def runtime() -> Iterator[Runtime]:
    """提供注册了常用测试图的 Runtime。

    Yields:
        测试结束时关闭的 Runtime。
    """

    with Runtime() as value:
        value.register("echo", Graph(entrypoint="echo").add(echo=Echo()))
        value.register("opaque", Graph(entrypoint="opaque").add(opaque=Opaque()))
        value.register("broken", Graph(entrypoint="broken").add(broken=Broken()))
        value.register("wait", Graph(entrypoint="wait").add(wait=Waiting()))
        yield value


def rpc(client: TestClient, graph: str, **params: Any) -> dict[str, Any]:
    """发送一次 graph.call 并返回 JSON-RPC 响应。

    Args:
        client: 服务测试客户端。
        graph: 图注册名。
        **params: 额外的 RunRequest 字段。

    Returns:
        JSON-RPC 响应对象。
    """

    return client.post(
        "/rpc",
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "graph.call",
            "params": {"graph": graph, "inputs": {"default": None}, **params},
        },
    ).json()


def test_host_allowlist_rejects_dns_rebinding_and_accepts_configured_hosts(
    runtime: Runtime,
) -> None:
    """Host 与 Origin 一致的重绑定请求仍被拒绝，显式配置的主机和回环地址可访问。

    Args:
        runtime: 测试 Runtime。
    """

    with (
        GraphService(runtime) as service,
        TestClient(service.create_app(), base_url=LOCAL) as client,
    ):
        rebinding = client.get(
            "/api/graphs",
            headers={"Host": "evil.example", "Origin": "http://evil.example"},
        )
        assert rebinding.status_code == 400
        assert client.get("/", headers={"Host": "evil.example"}).status_code == 400
        assert (
            client.get("/api/graphs", headers={"Host": "[::1]:8000"}).status_code == 200
        )
        assert (
            client.get("/api/graphs", headers={"Host": "127.0.0.1:8000"}).status_code
            == 200
        )
    with (
        GraphService(runtime) as service,
        TestClient(
            service.create_app(allowed_hosts=["Studio.Example"]),
            base_url="http://studio.example",
        ) as client,
    ):
        assert client.get("/api/graphs").status_code == 200
        assert (
            client.get("/api/graphs", headers={"Host": "localhost"}).status_code == 400
        )
    with GraphService(runtime) as service:
        with TestClient(service.create_app(allowed_hosts=["*"])) as client:
            assert client.get("/api/graphs").status_code == 200
        with pytest.raises(TypeError):
            service.create_app(allowed_hosts="localhost")


def test_token_compares_bytes_and_rejects_non_ascii_authorization(
    runtime: Runtime,
) -> None:
    """非 ASCII Authorization 返回 401，而不是在比较时引发服务器错误。

    Args:
        runtime: 测试 Runtime。
    """

    with (
        GraphService(runtime) as service,
        TestClient(service.create_app(token="secret"), base_url=LOCAL) as client,
    ):
        response = client.get(
            "/api/graphs", headers={"Authorization": "Bearer sécret".encode()}
        )
        assert response.status_code == 401
        assert (
            client.get(
                "/api/graphs", headers={"Authorization": "Bearer secret"}
            ).status_code
            == 200
        )
        assert client.get("/").status_code == 200
        authorized = client.get(
            "/api/graphs", headers={"Authorization": "Bearer secret"}
        )
        assert authorized.headers["Cache-Control"] == "no-store"
        assert authorized.headers["X-Content-Type-Options"] == "nosniff"


def test_serve_warns_without_token_on_public_address(
    runtime: Runtime, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """监听非回环地址且无令牌时警告，并把监听地址加入默认 Host 白名单。

    Args:
        runtime: 测试 Runtime。
        monkeypatch: 替换 uvicorn.run 的夹具。
        caplog: 捕获警告日志。
    """

    import uvicorn

    started: list[tuple[Any, str]] = []

    def run(app: Any, *, host: str, port: int) -> None:
        """记录将要启动的应用与地址。

        Args:
            app: 服务应用。
            host: 监听地址。
            port: 监听端口。
        """

        del port
        started.append((app, host))

    monkeypatch.setattr(uvicorn, "run", run)
    with GraphService(runtime) as service:
        with caplog.at_level(logging.WARNING, logger="interlace.service"):
            service.serve(host="10.0.0.5")
            service.serve(host="10.0.0.5", token="secret")
            service.serve()
        warnings = [item for item in caplog.records if "non-loopback" in item.message]
        assert len(warnings) == 1
        with TestClient(started[0][0], base_url="http://10.0.0.5:8000") as client:
            assert client.get("/api/graphs").status_code == 200
            assert (
                client.get("/api/graphs", headers={"Host": "other"}).status_code == 400
            )


def test_remote_options_require_explicit_allowlist(runtime: Runtime) -> None:
    """默认拒绝非空 options，只放行应用声明的顶层键。

    Args:
        runtime: 测试 Runtime。
    """

    body = {"graph": "echo", "inputs": {"default": 1}}
    with (
        GraphService(runtime) as service,
        TestClient(service.create_app(), base_url=LOCAL) as client,
    ):
        assert client.post("/api/call", json=body).status_code == 200
        rejected = client.post("/api/call", json={**body, "options": {"app": {}}})
        assert rejected.status_code == 422
        assert rejected.json()["error_type"] == "InvalidRequestError"
        assert rpc(client, "echo", options={"app": {}})["error"]["code"] == -32602
    with (
        GraphService(runtime, allowed_options=["app"]) as service,
        TestClient(service.create_app(), base_url=LOCAL) as client,
    ):
        allowed = client.post("/api/call", json={**body, "options": {"app": {"x": 1}}})
        assert allowed.status_code == 200
        assert (
            client.post(
                "/api/call", json={**body, "options": {"app": {}, "other": 1}}
            ).status_code
            == 422
        )
    with pytest.raises(TypeError):
        GraphService(runtime, allowed_options="app")


def test_rpc_and_http_error_codes_follow_service_errors(runtime: Runtime) -> None:
    """编码失败属于传输错误，容量与关闭使用专用 RPC 错误码。

    Args:
        runtime: 测试 Runtime。
    """

    service = GraphService(runtime, max_active_executions=1)
    with TestClient(service.create_app(), base_url=LOCAL) as client:
        encoded = client.post(
            "/api/call", json={"graph": "opaque", "inputs": {"default": None}}
        )
        assert encoded.status_code == 500
        assert encoded.json()["status"] == "succeeded"
        assert encoded.json()["error_type"] == "OutputEncodingError"
        error = rpc(client, "opaque")["error"]
        assert error["code"] == RPC_OUTPUT_ENCODING_FAILED
        assert error["data"]["status"] == "succeeded"
        failed = rpc(client, "broken")["error"]
        assert failed["code"] == -32000 and failed["data"]["status"] == "failed"
        assert rpc(client, "missing")["error"]["code"] == -32602
        assert client.post("/api/call", json={"graph": "missing"}).status_code == 404

        waiting = client.post(
            "/api/executions", json={"graph": "wait", "inputs": {"default": None}}
        ).json()
        assert rpc(client, "echo")["error"]["code"] == RPC_CAPACITY_EXCEEDED
        client.post(f"/api/executions/{waiting['id']}/cancel")
        with pytest.raises(Exception, match="cancel"):
            runtime.get_execution(waiting["id"]).result(timeout=2)
        runtime.wait_idle()

        service.close()
        assert rpc(client, "echo")["error"]["code"] == RPC_UNAVAILABLE
        closed = client.post(
            "/api/executions", json={"graph": "echo", "inputs": {"default": 1}}
        )
        assert closed.status_code == 409


def test_closed_runtime_maps_to_unavailable() -> None:
    """Runtime 关闭后提交执行返回 HTTP 409 和专用 RPC 错误码。"""

    runtime = Runtime()
    runtime.register("echo", Graph(entrypoint="echo").add(echo=Echo()))
    service = GraphService(runtime)
    client = TestClient(service.create_app(), base_url=LOCAL)
    runtime.close()
    assert rpc(client, "echo")["error"]["code"] == RPC_UNAVAILABLE
    response = client.post(
        "/api/executions", json={"graph": "echo", "inputs": {"default": 1}}
    )
    assert response.status_code == 409
    assert response.json()["error_type"] == "RuntimeClosedError"
    service.close()


def test_unknown_errors_return_generic_500_and_are_logged(
    runtime: Runtime, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """未声明的异常不再按 KeyError/ValueError 猜测状态码，也不泄露异常消息。

    Args:
        runtime: 测试 Runtime。
        monkeypatch: 替换服务方法的夹具。
        caplog: 捕获服务错误日志。
    """

    service = GraphService(runtime)

    def fail() -> None:
        """模拟服务内部缺陷。

        Raises:
            KeyError: 包含内部细节的意外异常。
        """

        raise KeyError("internal-secret-detail")

    monkeypatch.setattr(service, "graphs", fail)
    app = service.create_app()
    with TestClient(app, base_url=LOCAL, raise_server_exceptions=False) as client:
        with caplog.at_level(logging.ERROR, logger="interlace.service"):
            response = client.get("/api/graphs")
        assert response.status_code == 500
        assert "internal-secret-detail" not in response.text
        logged = [item for item in caplog.records if item.exc_info]
        assert any("internal-secret-detail" in str(item.exc_info[1]) for item in logged)
        assert client.get("/api/executions/missing").status_code == 404
        assert client.post("/api/executions/missing/cancel").status_code == 404
    service.close()


def test_events_stream_waits_for_drain_after_terminal_state(
    runtime: Runtime, monkeypatch: pytest.MonkeyPatch
) -> None:
    """在读取空批次与读取终态之间写入的观测仍先于 finished 交付。

    Args:
        runtime: 测试 Runtime。
        monkeypatch: 在第一次批量读取后注入完成竞态的夹具。
    """

    service = GraphService(runtime)
    service.store.record_execution(
        {"id": "race", "graph": "echo", "done": False, "status": "running"}
    )
    original = service.events
    calls = 0

    def events(execution_id: str, after: int = 0) -> list[dict[str, Any]]:
        """首次读取后模拟执行在下一次状态读取前完成。

        Args:
            execution_id: 执行标识。
            after: 已消费游标。

        Returns:
            真实存储返回的批次。
        """

        nonlocal calls
        batch = original(execution_id, after)
        calls += 1
        if calls == 1:
            service.store.append_event("race", {"kind": "execution.finished"})
            service.store.record_execution(
                {"id": "race", "graph": "echo", "done": True, "status": "succeeded"}
            )
        return batch

    monkeypatch.setattr(service, "events", events)
    with TestClient(service.create_app(), base_url=LOCAL) as client:
        text = client.get("/api/executions/race/events").text
    assert "execution.finished" in text
    assert text.index("event: observation") < text.index("event: finished")
    service.close()


def test_finished_event_status_is_recorded_before_handle_turns_terminal(
    runtime: Runtime,
) -> None:
    """观察者收到终态事件时句柄仍可能运行，服务按事件记录终态并稍后归档输出。

    Args:
        runtime: 测试 Runtime。
    """

    service = GraphService(runtime)
    seen: list[tuple[ExecutionStatus, dict[str, Any]]] = []

    def after_service(event: RuntimeEvent) -> None:
        """在服务观察者之后读取句柄状态与已保存记录。

        Args:
            event: Runtime 生命周期事件。
        """

        if event.kind is RuntimeEventKind.EXECUTION_FINISHED:
            assert event.execution_id is not None
            seen.append(
                (
                    runtime.get_execution(event.execution_id).status,
                    service.store.execution(event.execution_id),
                )
            )

    handle = runtime.observe_runtime(after_service)
    succeeded = runtime.start("echo", "value")
    succeeded.result()
    failed = runtime.start("broken", None)
    with pytest.raises(ExecutionError, match="broken"):
        failed.result()
    runtime.wait_idle()
    handle.detach()
    service.close()
    assert [status for status, _ in seen] == [ExecutionStatus.RUNNING] * 2
    assert [(item["status"], item["done"]) for _, item in seen] == [
        ("succeeded", True),
        ("failed", True),
    ]
    assert seen[1][1]["error_type"] == "ExecutionError"


def test_archive_completes_before_close_and_records_final_snapshot(
    tmp_path: Path, runtime: Runtime
) -> None:
    """关闭服务会等待已排队的归档，输出与最终错误消息都写入数据库。

    Args:
        tmp_path: 测试数据库目录。
        runtime: 测试 Runtime。
    """

    database = tmp_path / "archive.sqlite3"
    service = GraphService(runtime, database=database)
    ok = runtime.start("echo", "kept")
    ok.result()
    failed = runtime.start("broken", None)
    with pytest.raises(ExecutionError, match="broken"):
        failed.result()
    runtime.wait_idle()
    service.close()
    store = ServiceStore(database)
    assert store.outputs(ok.id) == [{"port": "default", "value": "kept"}]
    assert store.execution(failed.id)["error"] == "node 'broken' failed: broken"
    store.close()


def test_retention_counts_executions_in_one_database(tmp_path: Path) -> None:
    """同一数据库只保留最近的指定数量执行，较早执行的观测与输出一起删除。

    Args:
        tmp_path: 测试数据库目录。
    """

    database = tmp_path / "retention.sqlite3"
    with Runtime() as runtime:
        runtime.register("echo", Graph(entrypoint="echo").add(echo=Echo()))
        service = GraphService(runtime, database=database, retention=2)
        handles = []
        for value in range(3):
            handle = service.start(RunRequest(graph="echo", inputs={"default": value}))
            handle.result()
            runtime.wait_idle()
            handles.append(handle)
        ids = {item["id"] for item in service.store.executions()}
        assert ids == {handles[1].id, handles[2].id}
        assert service.store.events(handles[0].id) == []
        assert service.store.outputs(handles[0].id) == []
        assert service.store.events(handles[2].id)
        service.close()
    with pytest.raises(ValueError):
        ServiceStore(retention=0)


def test_observer_does_not_decode_saved_versions_per_event(
    runtime: Runtime, monkeypatch: pytest.MonkeyPatch
) -> None:
    """运行发布版本时观测热路径使用内存索引，不重复解码全部版本。

    Args:
        runtime: 测试 Runtime。
        monkeypatch: 禁止读取版本表的夹具。
    """

    service = GraphService(runtime, nodes=catalog())
    service.publish(
        DraftRequest(definition=GraphDefinition.model_validate(document("doc")))
    )

    def forbidden() -> list[dict[str, Any]]:
        """在热路径中读取版本表即失败。

        Raises:
            AssertionError: 观察或快照解码了版本表。
        """

        raise AssertionError("versions decoded on the observer path")

    monkeypatch.setattr(service.store, "versions", forbidden)
    handle = service.start(RunRequest(graph="doc@v1", inputs={"default": 3}))
    assert handle.result()[0].value == 3
    runtime.wait_idle()
    record = service.execution(handle.id)
    assert record["definition_name"] == "doc"
    assert record["node_configs"] == {"echo": {}}
    assert service.definition("doc")["runs"][0]["id"] == handle.id
    service.close()


def test_constructor_failure_before_registration_allows_rebuild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """观察注册失败时尚未注册保存的版本，同一 Runtime 可以重新创建服务。

    Args:
        tmp_path: 测试数据库目录。
        monkeypatch: 让一次观察注册失败的夹具。
    """

    database = tmp_path / "rebuild.sqlite3"
    with Runtime() as runtime:
        service = GraphService(runtime, nodes=catalog(), database=database)
        service.publish(
            DraftRequest(definition=GraphDefinition.model_validate(document("saved")))
        )
        service.close()
    root = logging.getLogger()
    handlers = tuple(root.handlers)
    with Runtime() as runtime:
        original = runtime.observe_runtime

        def reject(observer: Any) -> Any:
            """拒绝观察注册。

            Args:
                observer: 服务观察回调。

            Raises:
                RuntimeError: 模拟安装失败。
            """

            del observer
            raise RuntimeError("observer rejected")

        monkeypatch.setattr(runtime, "observe_runtime", reject)
        with pytest.raises(RuntimeError, match="observer rejected"):
            GraphService(runtime, nodes=catalog(), database=database)
        assert "saved@v1" not in runtime.graphs()
        assert tuple(root.handlers) == handlers
        monkeypatch.setattr(runtime, "observe_runtime", original)
        service = GraphService(runtime, nodes=catalog(), database=database)
        assert runtime.run("saved@v1", 5)[0].value == 5
        runtime.wait_idle()
        service.close()


def test_newer_database_schema_is_rejected(tmp_path: Path) -> None:
    """数据库结构版本高于当前代码时拒绝打开，避免误写未知格式。

    Args:
        tmp_path: 测试数据库目录。
    """

    database = tmp_path / "future.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA user_version=99")
    with pytest.raises(RuntimeError, match="newer"):
        ServiceStore(database)
