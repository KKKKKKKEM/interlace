"""GraphService 把调用方 SlotProvider 传给服务提交的每次执行。"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jsonrpcserver")

from fastapi.testclient import TestClient

from interlace import Graph, Node, Output, Runtime
from interlace.engine import Slot, SlotPool
from interlace.service import GraphService

# 默认 Host 白名单只接受本机名称，测试客户端使用 localhost 而非 testserver。
LOCAL = "http://localhost"


class SlotTag(Node):
    """把输入文本与当前 Slot 的 id 拼接输出。

    Attributes:
        inputs: 文本输入。
        outputs: 文本输出。
    """

    inputs = {"text": str}
    outputs = {"text": str}

    def execute(self, inputs: Any, context: Any) -> Output:
        """输出 `<text>@<slot id>`，没有 Slot 时使用 none。"""

        slot = context.slot
        return Output(f"{inputs['text']}@{'none' if slot is None else slot.id}", "text")


def test_service_runs_every_protocol_in_the_provided_slot() -> None:
    """HTTP 调用、后台执行与 JSON-RPC graph.call 都在服务配置的 Slot 内运行。"""

    pool = SlotPool(slots=[Slot(id="crawler")])
    with Runtime() as runtime:
        runtime.register("tag", Graph(entrypoint="node").add(node=SlotTag()))
        with (
            GraphService(runtime, slots=pool) as service,
            TestClient(service.create_app(), base_url=LOCAL) as client,
        ):
            assert service.slots is pool
            call = client.post(
                "/api/call", json={"graph": "tag", "inputs": {"text": "a"}}
            )
            assert call.json()["outputs"] == [{"port": "text", "value": "a@crawler"}]
            started = client.post(
                "/api/executions", json={"graph": "tag", "inputs": {"text": "b"}}
            )
            assert started.status_code == 202
            execution = service.live_execution(started.json()["id"])
            assert execution is not None
            assert execution.result(5) == (Output("b@crawler", "text"),)
            rpc = client.post(
                "/rpc",
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "graph.call",
                    "params": {"graph": "tag", "inputs": {"text": "c"}},
                },
            ).json()
            assert rpc["result"]["outputs"] == [{"port": "text", "value": "c@crawler"}]
        runtime.wait_idle(5)
        assert pool.available == 1
        with GraphService(runtime) as plain:
            handle = plain.runtime.start("tag", "d")
            assert handle.result(5) == (Output("d@none", "text"),)
    pool.close()


def test_service_rejects_non_provider_slots() -> None:
    """slots 必须实现 SlotProvider。"""

    with Runtime() as runtime, pytest.raises(TypeError, match="SlotProvider"):
        GraphService(runtime, slots=2)  # type: ignore[arg-type]
