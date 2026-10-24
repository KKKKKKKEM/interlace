"""OpenTelemetry 桥接契约：按指标名懒创建仪表，标签作为属性记录。"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("opentelemetry.sdk.metrics")

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from interlace import Graph, Node, Output, Runtime
from interlace.metrics import Metric, Metrics, count, observe
from interlace.metrics.otel import otel_exporter


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


def collected(reader: InMemoryMetricReader) -> dict[str, list[Any]]:
    """读取内存读取器中按仪表名分组的数据点。

    Args:
        reader: 测试读取器。

    Returns:
        仪表名到数据点列表的映射。
    """

    data = reader.get_metrics_data()
    assert data is not None
    return {
        metric.name: list(metric.data.data_points)
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }


def test_otel_exporter_records_counters_and_histograms_with_attributes() -> None:
    """计数器与直方图分别创建一次仪表，仪表名保留点分形式，标签成为属性，生命周期指标同样导出。"""

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    metrics = Metrics(exporters=[otel_exporter(provider.get_meter("test"))])
    with Runtime(plugins=[metrics.plugin]) as runtime:
        count(runtime.hooks, "crawler.pages", site="a")
        count(runtime.hooks, "crawler.pages", 2, site="a")
        count(runtime.hooks, "crawler.pages", site="b")
        observe(runtime.hooks, "crawler.fetch.seconds", 0.5, site="a")
        observe(runtime.hooks, "crawler.fetch.seconds", 1.5, site="a")
        runtime.register("echo", Graph(entrypoint="echo").add(echo=Echo()))
        runtime.run("echo", 1)

    points = collected(reader)
    pages = {
        dict(point.attributes)["site"]: point.value for point in points["crawler.pages"]
    }
    assert pages == {"a": 3, "b": 1}
    (fetch,) = points["crawler.fetch.seconds"]
    assert dict(fetch.attributes) == {"site": "a"}
    assert (fetch.count, fetch.sum, fetch.max) == (2, 2.0, 1.5)
    (executions,) = points["interlace.executions"]
    assert dict(executions.attributes) == {"graph": "echo", "status": "succeeded"}
    assert executions.value == 1
    assert "interlace.execution.seconds" in points
    provider.shutdown()


def test_otel_exporter_rejects_kind_change_for_existing_instrument() -> None:
    """同名指标以另一种类到达时拒绝，交给 Notice 宿主隔离。"""

    provider = MeterProvider(metric_readers=[InMemoryMetricReader()])
    export = otel_exporter(provider.get_meter("test"))
    export(Metric("jobs"))
    with pytest.raises(ValueError, match="already exported as counter"):
        export(Metric("jobs", 1.0, "histogram"))
    provider.shutdown()
