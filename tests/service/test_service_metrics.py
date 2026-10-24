"""服务 /metrics 接口契约：显式启用、Prometheus 文本类型与 API 相同的访问控制。"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jsonrpcserver")

from fastapi.testclient import TestClient

from interlace import Graph, Node, Output, Runtime
from interlace.metrics import Metrics
from interlace.service import GraphService

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


def test_metrics_endpoint_serves_prometheus_text_behind_token() -> None:
    """启用后 /metrics 返回 Prometheus 文本，与 API 共用令牌、Host 与同源检查。"""

    metrics = Metrics()
    with Runtime(plugins=[metrics.plugin]) as runtime:
        runtime.register("echo", Graph(entrypoint="echo").add(echo=Echo()))
        with (
            GraphService(runtime) as service,
            TestClient(
                service.create_app(token="secret", metrics=metrics), base_url=LOCAL
            ) as client,
        ):
            auth = {"Authorization": "Bearer secret"}
            called = client.post(
                "/api/call",
                json={"graph": "echo", "inputs": {"default": 1}},
                headers=auth,
            )
            assert called.status_code == 200
            assert client.get("/metrics").status_code == 401
            assert (
                client.get(
                    "/metrics", headers={**auth, "Host": "evil.example"}
                ).status_code
                == 400
            )
            assert (
                client.get(
                    "/metrics", headers={**auth, "Origin": "http://evil.example"}
                ).status_code
                == 403
            )
            response = client.get("/metrics", headers=auth)

    assert response.status_code == 200
    assert (
        response.headers["Content-Type"] == "text/plain; version=0.0.4; charset=utf-8"
    )
    assert response.headers["Cache-Control"] == "no-store"
    assert response.text == metrics.prometheus()
    assert 'interlace_executions_total{graph="echo",status="succeeded"} 1.0' in (
        response.text
    )


def test_metrics_endpoint_is_absent_without_metrics_and_open_without_token() -> None:
    """未传入聚合器时 /metrics 不存在；未设置令牌时与 API 一样只做 Host 检查。"""

    metrics = Metrics()
    with Runtime() as runtime, GraphService(runtime) as service:
        with TestClient(service.create_app(), base_url=LOCAL) as client:
            assert client.get("/metrics").status_code == 404
        with TestClient(service.create_app(metrics=metrics), base_url=LOCAL) as client:
            response = client.get("/metrics")
            assert response.status_code == 200
            assert response.text == ""
