"""官方服务的图版本、RPC、流式输出和运行观测契约。"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jsonrpcserver")

from fastapi.testclient import TestClient
from pydantic import BaseModel, ConfigDict

from interlace import Graph, Node, Output, Ports, Runtime
from interlace.service import GraphService, NodeCatalog, RunRequest
from interlace.service.models import DraftRequest, GraphDefinition


class Config(BaseModel):
    """测试节点配置。

    Attributes:
        model_config: 拒绝未知字段。
        prefix: 输出前缀。
    """

    model_config = ConfigDict(extra="forbid")
    prefix: str = ""


class Upper(Node):
    """可由编辑器构建的转换节点。

    Attributes:
        input_ports: 文本输入。
        output_ports: 文本输出。
        prefix: 不随执行变化的前缀。
    """

    input_ports = Ports(text=str)
    output_ports = Ports(text=str)

    def __init__(self, config: Config) -> None:
        """保存构造参数。

        Args:
            config: 经验证的文本配置。
        """

        self.prefix = config.prefix

    def execute(self, inputs: Any, context: Any) -> Output:
        """生成大写文本。

        Args:
            inputs: text 端口值。
            context: 当前执行上下文。

        Returns:
            添加前缀后的大写文本。
        """

        del context
        return Output(self.prefix + inputs["text"].upper(), "text")


class Failing(Node):
    """产生一项输出后失败的节点。"""

    def execute(self, inputs: Any, context: Any) -> Iterator[Output]:
        """验证已经交付的输出不会因后续异常撤回。

        Args:
            inputs: 测试入口值。
            context: 当前上下文。

        Yields:
            异常前的一项输出。

        Raises:
            ValueError: 第一项之后的业务失败。
        """

        del inputs, context
        yield Output(7)
        raise ValueError("later failure")


class Waiting(Node):
    """通过检查点响应取消的测试节点。"""

    def execute(self, inputs: Any, context: Any) -> None:
        """持续运行直到取消或超时。

        Args:
            inputs: 测试入口值。
            context: 提供协作式检查点的上下文。
        """

        del inputs
        while True:
            context.checkpoint()
            time.sleep(0.002)


def catalog() -> NodeCatalog:
    """创建独立的测试节点目录。

    Returns:
        登记了文本转换节点的目录。
    """

    return NodeCatalog().register("test.upper", Upper, config=Config)


def definition(prefix: str = "") -> dict[str, Any]:
    """构造可发布的两节点图文档。

    Args:
        prefix: 第一节点输出前缀。

    Returns:
        可序列化图定义。
    """

    return {
        "name": "visual",
        "entrypoint": "a",
        "nodes": [
            {"id": "a", "type": "test.upper", "config": {"prefix": prefix}},
            {"id": "b", "type": "test.upper"},
        ],
        "edges": [
            {"source": "a", "target": "b", "source_port": "text", "target_port": "text"}
        ],
    }


@pytest.fixture
def api() -> Iterator[tuple[TestClient, GraphService]]:
    """提供接入已有 Runtime 的 HTTP 测试客户端。

    Yields:
        客户端与服务，离开时按所有权关闭。
    """

    with Runtime() as runtime:
        runtime.register("code", Graph(entrypoint="upper").add(upper=Upper(Config())))
        service = GraphService(runtime, nodes=catalog())
        with TestClient(service.create_app()) as client:
            yield client, service


def test_existing_graph_http_rpc_and_public_catalog(api: Any) -> None:
    """已有代码图无需二次登记即可通过 HTTP 调用。

    Args:
        api: 测试客户端与服务。
    """

    client, service = api
    assert client.get("/api/graphs").json()[0]["name"] == "code"
    with pytest.raises(TypeError):
        service.runtime.graphs()["other"] = Graph()
    result = client.post(
        "/api/call", json={"graph": "code", "inputs": {"text": "hello"}}
    )
    assert result.status_code == 200
    assert result.json()["outputs"] == [{"port": "text", "value": "HELLO"}]
    assert (
        client.post(
            "/api/call", json={"graph": "code", "inputs": {"text": 123}}
        ).status_code
        == 422
    )
    assert client.post("/api/call", json={"graph": "missing"}).status_code == 404
    assert (
        client.post(
            "/api/executions", json={"graph": "code", "max_steps": True}
        ).status_code
        == 422
    )


def test_jsonrpc_batch_notifications_and_errors(api: Any) -> None:
    """JSON-RPC 分发遵循批量、通知及标准错误契约。

    Args:
        api: 测试客户端与服务。
    """

    client, _ = api
    request = {
        "jsonrpc": "2.0",
        "method": "graph.call",
        "params": {"graph": "code", "inputs": {"text": "abc"}},
    }
    batch = [
        {**request, "id": 7},
        request,
        {"jsonrpc": "2.0", "method": "missing", "id": 8},
    ]
    result = client.post("/rpc", json=batch).json()
    assert len(result) == 2
    assert result[0]["result"]["outputs"][0]["value"] == "ABC"
    assert result[1]["error"]["code"] == -32601
    assert client.post("/rpc", json=request).status_code == 204
    assert client.post("/rpc", content="{").json()["error"]["code"] == -32700
    assert client.post("/rpc", content=b"\xff").json()["error"]["code"] == -32700
    assert client.post("/rpc", json=[]).json()["error"]["code"] == -32600
    invalid = {**request, "id": 9, "params": {"graph": "code", "inputs": {}}}
    assert client.post("/rpc", json=invalid).json()["error"]["code"] == -32602


def test_draft_conflict_freeze_validation_and_immutable_versions(api: Any) -> None:
    """草稿允许不完整，发布必须冻结且历史版本输出保持不变。

    Args:
        api: 测试客户端与服务。
    """

    client, _ = api
    body = definition()
    invalid = {**body, "edges": []}
    assert (
        client.post("/api/drafts", json={"definition": invalid}).json()["revision"] == 1
    )
    assert client.post("/api/validate", json=invalid).status_code == 422
    assert (
        client.post(
            "/api/publish", json={"definition": invalid, "revision": 1}
        ).status_code
        == 422
    )
    assert client.post("/api/drafts", json={"definition": body}).status_code == 409
    first = client.post("/api/publish", json={"definition": body, "revision": 1}).json()
    assert first == {"graph": "visual@v1", "version": 1, "revision": 2}
    second = client.post(
        "/api/publish", json={"definition": definition("NEW "), "revision": 2}
    ).json()
    assert second["graph"] == "visual@v2"
    for graph, expected in [("visual@v1", "HI"), ("visual@v2", "NEW HI")]:
        result = client.post(
            "/api/call", json={"graph": graph, "inputs": {"text": "hi"}}
        ).json()
        assert result["outputs"][0]["value"] == expected


def test_publish_registration_failure_rolls_back_version_and_draft(
    api: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Runtime 拒绝新版本时不得提交对应版本、草稿或归属。

    Args:
        api: 测试客户端与服务。
        monkeypatch: 替换 Runtime 注册入口的测试夹具。
    """

    _, service = api

    def reject(name: str, graph: Graph) -> Runtime:
        """拒绝测试发布的 Runtime 注册。

        Args:
            name: 待注册的版本名称。
            graph: 已编译的冻结 Graph。

        Raises:
            RuntimeError: 模拟 Runtime 注册失败。
        """

        del name, graph
        raise RuntimeError("registration failed")

    monkeypatch.setattr(service.runtime, "register", reject)
    request = DraftRequest(definition=GraphDefinition.model_validate(definition()))
    with pytest.raises(RuntimeError, match="registration failed"):
        service.publish(request)

    assert service.store.versions() == []
    assert service.store.drafts() == []
    assert "visual" not in service.store.memberships()


def test_observation_replay_and_output_failure_preservation(api: Any) -> None:
    """输出失败保留已交付项目，节点时间线支持从游标重放。

    Args:
        api: 测试客户端与服务。
    """

    client, service = api
    service.runtime.register("failure", Graph(entrypoint="fail").add(fail=Failing()))
    run = client.post(
        "/api/executions", json={"graph": "failure", "inputs": {"default": None}}
    ).json()
    stream = client.get(f"/api/executions/{run['id']}/outputs").text
    assert stream.index('"value": 7') < stream.index("event: failure")
    events = client.get(f"/api/executions/{run['id']}/events").text
    assert (
        "node.started" in events
        and "output.routed" in events
        and "node.finished" in events
    )
    records = service.store.events(run["id"])
    assert all("value" not in record["attributes"] for record in records)
    replay = client.get(
        f"/api/executions/{run['id']}/events",
        headers={"Last-Event-ID": str(records[-1]["id"])},
    ).text
    assert "event: observation" not in replay
    assert "event: finished" in replay


def test_cancel_timeout_and_independent_subscription(api: Any) -> None:
    """HTTP 控制沿用 Execution 的协作式取消与超时。

    Args:
        api: 测试客户端与服务。
    """

    client, service = api
    service.runtime.register("wait", Graph(entrypoint="wait").add(wait=Waiting()))
    run = client.post(
        "/api/executions", json={"graph": "wait", "inputs": {"default": None}}
    ).json()
    assert client.post(f"/api/executions/{run['id']}/cancel").json()["accepted"]
    handle = service.runtime.get_execution(run["id"])
    with pytest.raises(Exception, match="cancel"):
        handle.result(timeout=2)
    result = client.post(
        "/api/call",
        json={"graph": "wait", "inputs": {"default": None}, "timeout": 0.02},
    )
    assert result.status_code == 409
    assert result.json()["done"]


def test_service_auth_and_cross_site_writes() -> None:
    """令牌保护所有 API，浏览器跨站调用被明确拒绝。"""

    with Runtime() as runtime:
        service = GraphService(runtime)
        with TestClient(service.create_app(token="secret")) as client:
            assert client.get("/api/graphs").status_code == 401
            assert (
                client.get(
                    "/api/graphs", headers={"Authorization": "Bearer secret"}
                ).status_code
                == 200
            )
            assert (
                client.post(
                    "/api/drafts",
                    json={},
                    headers={
                        "Authorization": "Bearer secret",
                        "Origin": "https://other.example",
                    },
                ).status_code
                == 403
            )
            assert client.get("/openapi.json").status_code == 401


def test_service_limits_request_body_and_active_executions() -> None:
    """服务边界拒绝过大请求和超过容量的并发执行。"""

    with Runtime() as runtime:
        runtime.register("wait", Graph(entrypoint="wait").add(wait=Waiting()))
        service = GraphService(runtime, max_active_executions=1)
        with TestClient(service.create_app(max_request_bytes=256)) as client:
            oversized = client.post(
                "/api/call",
                json={
                    "graph": "wait",
                    "inputs": {"default": "x" * 512},
                },
            )
            assert oversized.status_code == 413
            chunked = client.post(
                "/rpc",
                content=(part for part in (b"{" + b"x" * 200, b"x" * 200 + b"}")),
                headers={"Content-Type": "application/json"},
            )
            assert chunked.status_code == 413

            first = client.post(
                "/api/executions",
                json={"graph": "wait", "inputs": {"default": None}},
            ).json()
            second = client.post(
                "/api/executions",
                json={"graph": "wait", "inputs": {"default": None}},
            )
            assert second.status_code == 429
            assert "max_active_executions=1" in second.json()["detail"]
            client.post(f"/api/executions/{first['id']}/cancel")
            with pytest.raises(Exception, match="cancel"):
                runtime.get_execution(first["id"]).result(timeout=2)


def test_published_definitions_and_history_survive_restart(tmp_path: Path) -> None:
    """重启恢复发布定义和观测历史，不宣称恢复在途任务。

    Args:
        tmp_path: 测试数据库目录。
    """

    database = tmp_path / "service.sqlite3"
    with Runtime() as runtime:
        service = GraphService(runtime, nodes=catalog(), database=database)
        service.publish(
            DraftRequest(definition=GraphDefinition.model_validate(definition()))
        )
        handle = service.start(RunRequest(graph="visual@v1", inputs={"text": "old"}))
        assert handle.result()[0].value == "OLD"
        runtime.wait_idle()
        service.close()
    with Runtime() as runtime:
        service = GraphService(runtime, nodes=catalog(), database=database)
        assert "visual@v1" in runtime.graphs()
        assert service.execution(handle.id)["status"] == "succeeded"
        assert service.store.events(handle.id)
        assert service.store.outputs(handle.id) == [{"port": "text", "value": "OLD"}]
        assert service.execution(handle.id)["graph_snapshot"]["entrypoint"] == "a"
        with TestClient(service.create_app()) as client:
            replay = client.get(f"/api/executions/{handle.id}/outputs").text
            assert '"value": "OLD"' in replay
            assert "event: finished" in replay
        service.close()
    with Runtime() as runtime:
        service = GraphService(runtime, nodes=catalog(), database=database)
        assert (
            service.start(RunRequest(graph="visual@v1", inputs={"text": "new"}))
            .result()[0]
            .value
            == "NEW"
        )
        runtime.wait_idle()
        service.close()


def test_service_close_detaches_without_closing_runtime() -> None:
    """服务退出只释放自己拥有的观察注册与数据库。"""

    with Runtime() as runtime:
        runtime.register("code", Graph(entrypoint="upper").add(upper=Upper(Config())))
        service = GraphService(runtime)
        service.close()
        service.close()
        assert runtime.run("code", "still usable")[0].value == "STILL USABLE"


def test_unknown_factory_and_configuration_fail_without_registration(api: Any) -> None:
    """不接受动态导入节点，配置错误和端口错误均不产生发布版本。

    Args:
        api: 测试客户端与服务。
    """

    client, service = api
    body = definition()
    body["nodes"][0]["type"] = "os.system"
    assert client.post("/api/publish", json={"definition": body}).status_code == 422
    body = definition()
    body["nodes"][0]["config"] = {"unknown": 1}
    assert client.post("/api/validate", json=body).status_code == 422
    body = definition()
    body["edges"][0]["target_port"] = "missing"
    assert client.post("/api/publish", json={"definition": body}).status_code == 422
    assert list(service.runtime.graphs()) == ["code"]


def test_schema_and_preview_follow_node_configuration(api: Any) -> None:
    """目录给出配置 Schema，预览给出真实端口而非猜测。

    Args:
        api: 测试客户端与服务。
    """

    client, _ = api
    schema = client.get("/api/nodes").json()[0]["schema"]
    assert schema["properties"]["prefix"]["type"] == "string"
    response = client.post(
        "/api/nodes/preview",
        json={"id": "a", "type": "test.upper", "config": {"prefix": "x"}},
    )
    assert response.json() == {"inputs": {"text": "str"}, "outputs": {"text": "str"}}


def test_service_assets_and_openapi_are_packaged(api: Any) -> None:
    """服务提供可打开的工作台及调用协议文档。

    Args:
        api: 测试客户端与服务。
    """

    client, _ = api
    index = client.get("/")
    assert index.status_code == 200
    assert "Interlace Studio" in index.text
    assert "/api/call" in client.get("/openapi.json").json()["paths"]
    assert client.get("/api/executions/missing").status_code == 404
    assert json.loads(client.get("/api/executions").text) == []


def test_mounted_service_keeps_authentication() -> None:
    """挂载路径不能绕过子应用的令牌检查。"""

    from fastapi import FastAPI

    with Runtime() as runtime:
        service = GraphService(runtime)
        parent = FastAPI()
        parent.mount("/studio", service.create_app(token="secret"))
        with TestClient(parent) as client:
            assert client.get("/studio/api/graphs").status_code == 401
            assert (
                client.get(
                    "/studio/api/graphs", headers={"Authorization": "Bearer secret"}
                ).status_code
                == 200
            )
        service.close()


def test_restart_marks_pending_history_interrupted(tmp_path: Path) -> None:
    """重启不会把旧进程中的在途记录误报为仍在运行。

    Args:
        tmp_path: 独立历史数据库目录。
    """

    database = tmp_path / "history.sqlite3"
    with Runtime() as runtime:
        service = GraphService(runtime, database=database)
        service.store.record_execution(
            {
                "id": "old",
                "done": False,
                "status": "running",
                "started_at": "2026-01-01T00:00:00Z",
            }
        )
        service.close()

    with Runtime() as runtime:
        service = GraphService(runtime, database=database)
        assert service.execution("old")["status"] == "interrupted"
        assert service.execution("old")["done"] is True
        service.close()


def test_live_sse_delivers_before_completion_and_disconnect_keeps_work() -> None:
    """通过真实 HTTP 连接验证逐项交付和断开后的执行所有权。"""

    import socket
    from threading import Event, Thread

    import httpx
    import uvicorn

    release = Event()

    class Stream(Node):
        """首项输出后等待测试释放的流节点。"""

        def execute(self, inputs: Any, context: Any) -> Iterator[Output]:
            """产生两项输出，中间保留可观察的运行窗口。

            Args:
                inputs: 测试输入。
                context: 当前执行检查点。

            Yields:
                等待前后各一项输出。
            """

            del inputs
            yield Output("first")
            while not release.wait(0.005):
                context.checkpoint()
            yield Output("second")

    with Runtime() as runtime:
        runtime.register("stream", Graph(entrypoint="source").add(source=Stream()))
        service = GraphService(runtime)
        server = uvicorn.Server(uvicorn.Config(service.create_app(), log_level="error"))
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            address = f"http://127.0.0.1:{listener.getsockname()[1]}"
            thread = Thread(target=server.run, kwargs={"sockets": [listener]})
            thread.start()
            try:
                deadline = time.monotonic() + 5
                while (
                    not server.started
                    and thread.is_alive()
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.01)
                assert server.started
                with httpx.Client(base_url=address, timeout=3) as client:
                    run = client.post(
                        "/api/executions",
                        json={"graph": "stream", "inputs": {"default": None}},
                    ).json()
                    with client.stream(
                        "GET", f"/api/executions/{run['id']}/outputs"
                    ) as response:
                        for line in response.iter_lines():
                            if line.startswith("data:"):
                                assert json.loads(line[5:])["value"] == "first"
                                break
                    handle = runtime.get_execution(run["id"])
                    assert not handle.done
                    assert not handle.cancel_requested
                    release.set()
                    assert [item.value for item in handle.result(timeout=3)] == [
                        "first",
                        "second",
                    ]
                runtime.wait_idle()
            finally:
                release.set()
                server.should_exit = True
                thread.join(timeout=5)
                assert not thread.is_alive()
                service.close()


def test_step_limit_does_not_invent_a_node_firing(api: Any) -> None:
    """步数上限拒绝的节点不能留下未结束的可视化 firing。

    Args:
        api: 服务客户端与运行时。
    """

    client, service = api
    published = client.post("/api/publish", json={"definition": definition()}).json()
    result = client.post(
        "/api/call",
        json={"graph": published["graph"], "inputs": {"text": "x"}, "max_steps": 1},
    ).json()
    assert result["status"] == "step_limited"
    service.runtime.wait_idle()
    events = service.store.events(result["id"])
    started = [item for item in events if item["kind"] == "node.started"]
    finished = [item for item in events if item["kind"] == "node.finished"]
    assert [item["node"] for item in started] == ["a"]
    assert [item["node"] for item in finished] == ["a"]
    assert started[0]["attributes"]["step"] == finished[0]["attributes"]["step"] == 1
