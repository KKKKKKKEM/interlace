"""指标样本校验、聚合、序列上限、快照、Prometheus 渲染、导出隔离与包边界契约。"""

from __future__ import annotations

import logging
import subprocess
import sys
from collections.abc import Iterator

import pytest

from interlace import Runtime
from interlace.metrics import METRIC, Metric, Metrics, count, observe


@pytest.fixture
def metrics() -> Metrics:
    """提供默认配置的聚合器。

    Returns:
        尚未安装的聚合器。
    """

    return Metrics()


@pytest.fixture
def runtime(metrics: Metrics) -> Iterator[Runtime]:
    """提供以启动插件安装了聚合器的 Runtime。

    Args:
        metrics: 测试聚合器。

    Yields:
        测试结束时关闭的 Runtime。
    """

    with Runtime(plugins=[metrics.plugin]) as value:
        yield value


def test_metric_validates_name_value_kind_and_labels() -> None:
    """非法名称、非有限值、负计数增量、未知种类与非法标签在构造时失败，标签被冻结复制。"""

    labels = {"route": "a"}
    metric = Metric("crawler.pages", 3, labels=labels)
    labels["route"] = "b"
    assert metric.value == 3.0
    assert isinstance(metric.value, float)
    assert dict(metric.labels) == {"route": "a"}
    with pytest.raises(TypeError):
        metric.labels["route"] = "c"  # type: ignore[index]
    assert Metric("delay.seconds", -0.5, "histogram").value == -0.5

    for name in ("", "Crawler", "1x", "a..b", "a.", "a-b", ".a"):
        with pytest.raises(ValueError, match="metric name"):
            Metric(name)
    with pytest.raises(TypeError):
        Metric(1)  # type: ignore[arg-type]
    for value in (float("nan"), float("inf")):
        with pytest.raises(ValueError, match="finite"):
            Metric("a", value)
    with pytest.raises(TypeError):
        Metric("a", True)
    with pytest.raises(TypeError):
        Metric("a", "1")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="negative"):
        Metric("a", -1)
    with pytest.raises(ValueError, match="kind"):
        Metric("a", kind="gauge")  # type: ignore[arg-type]
    for key in ("Route", "1a", "a-b", "__name"):
        with pytest.raises(ValueError, match="label key"):
            Metric("a", labels={key: "x"})
    with pytest.raises(TypeError):
        Metric("a", labels={"route": 1})  # type: ignore[dict-item]


def test_counters_and_histograms_aggregate_per_label_set(
    runtime: Runtime, metrics: Metrics
) -> None:
    """同一标签集合累加到同一序列，标签顺序不影响身份，不同标签分开计数。

    Args:
        runtime: 安装了聚合器的 Runtime。
        metrics: 测试聚合器。
    """

    count(runtime.hooks, "pages", method="get", site="a")
    count(runtime.hooks, "pages", 2, site="a", method="get")
    count(runtime.hooks, "pages", 0.5, site="b", method="get")
    observe(runtime.hooks, "latency.seconds", 0.25, site="a")
    observe(runtime.hooks, "latency.seconds", 0.75, site="a")
    observe(runtime.hooks, "latency.seconds", 0.5, site="a")

    assert metrics.snapshot() == {
        "pages": {
            "kind": "counter",
            "series": [
                {"labels": {"method": "get", "site": "a"}, "value": 3.0},
                {"labels": {"method": "get", "site": "b"}, "value": 0.5},
            ],
        },
        "latency.seconds": {
            "kind": "histogram",
            "series": [
                {"labels": {"site": "a"}, "count": 3, "sum": 1.5, "max": 0.75},
            ],
        },
    }
    assert metrics.dropped == 0


def test_kind_conflicts_and_exposed_name_collisions_are_isolated(
    runtime: Runtime, metrics: Metrics, caplog: pytest.LogCaptureFixture
) -> None:
    """同名不同种类与 Prometheus 暴露名重名的样本被拒绝并记录，不影响调用方与已有数据。

    Args:
        runtime: 安装了聚合器的 Runtime。
        metrics: 测试聚合器。
        caplog: 捕获 Notice 隔离日志。
    """

    count(runtime.hooks, "jobs.done")
    with caplog.at_level(logging.ERROR, logger="interlace.engine.extension"):
        observe(runtime.hooks, "jobs.done", 1.0)
        count(runtime.hooks, "jobs_done")
        runtime.hooks.notify(METRIC, "not a metric", name="x")  # type: ignore[arg-type]

    assert len(caplog.records) == 3
    assert metrics.snapshot() == {
        "jobs.done": {"kind": "counter", "series": [{"labels": {}, "value": 1.0}]}
    }


def test_series_cap_drops_new_label_sets_but_keeps_existing_series() -> None:
    """超过每个指标名的序列上限后，新标签集合的样本被丢弃计数，已有序列继续累加。"""

    metrics = Metrics(max_series=2)
    with Runtime(plugins=[metrics.plugin]) as runtime:
        for site in ("a", "b", "c", "d"):
            count(runtime.hooks, "pages", site=site)
        count(runtime.hooks, "pages", site="a")
        count(runtime.hooks, "other", site="c")

    snapshot = metrics.snapshot()
    assert snapshot["pages"]["series"] == [
        {"labels": {"site": "a"}, "value": 2.0},
        {"labels": {"site": "b"}, "value": 1.0},
    ]
    assert snapshot["other"]["series"] == [{"labels": {"site": "c"}, "value": 1.0}]
    assert metrics.dropped == 2
    with pytest.raises(ValueError):
        Metrics(max_series=0)
    with pytest.raises(TypeError):
        Metrics(max_series=True)
    with pytest.raises(TypeError):
        Metrics(exporters=[object()])  # type: ignore[list-item]


def test_snapshot_returns_independent_copies(
    runtime: Runtime, metrics: Metrics
) -> None:
    """修改快照不影响聚合器，后续样本也不改变已取得的快照。

    Args:
        runtime: 安装了聚合器的 Runtime。
        metrics: 测试聚合器。
    """

    count(runtime.hooks, "pages", site="a")
    first = metrics.snapshot()
    first["pages"]["series"][0]["labels"]["site"] = "changed"
    first["pages"]["series"][0]["value"] = 100.0
    first["pages"]["series"].clear()
    count(runtime.hooks, "pages", site="a")

    assert metrics.snapshot()["pages"]["series"] == [
        {"labels": {"site": "a"}, "value": 2.0}
    ]
    assert first["pages"]["series"] == []


def test_prometheus_renders_counters_summaries_and_escaped_labels(
    runtime: Runtime, metrics: Metrics
) -> None:
    """点替换为下划线，计数器带 _total，直方图渲染为 summary，标签值转义。

    Args:
        runtime: 安装了聚合器的 Runtime。
        metrics: 测试聚合器。
    """

    assert metrics.prometheus() == ""
    count(runtime.hooks, "crawler.pages", 2, path='a\\b"c\nd')
    count(runtime.hooks, "crawler.requests_total")
    observe(runtime.hooks, "crawler.fetch.seconds", 0.5, site="x")
    observe(runtime.hooks, "crawler.fetch.seconds", 1.5, site="x")

    assert metrics.prometheus() == (
        "# TYPE crawler_pages_total counter\n"
        'crawler_pages_total{path="a\\\\b\\"c\\nd"} 2.0\n'
        "# TYPE crawler_requests_total counter\n"
        "crawler_requests_total 1.0\n"
        "# TYPE crawler_fetch_seconds summary\n"
        'crawler_fetch_seconds_count{site="x"} 2\n'
        'crawler_fetch_seconds_sum{site="x"} 2.0\n'
    )


def test_exporters_receive_samples_and_failures_are_isolated(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """导出器在聚合之后同步收到每个样本；失败的导出器被记录隔离，不影响其他导出器与聚合。

    Args:
        caplog: 捕获 Notice 隔离日志。
    """

    received: list[Metric] = []

    def broken(metric: Metric) -> None:
        """总是失败的导出器。

        Args:
            metric: 收到的样本。

        Raises:
            RuntimeError: 测试导出失败。
        """

        del metric
        raise RuntimeError("exporter down")

    metrics = Metrics(exporters=[broken, received.append])
    with (
        Runtime(plugins=[metrics.plugin]) as runtime,
        caplog.at_level(logging.ERROR, logger="interlace.engine.extension"),
    ):
        count(runtime.hooks, "pages", site="a")

    assert [(item.name, dict(item.labels)) for item in received] == [
        ("pages", {"site": "a"})
    ]
    assert metrics.snapshot()["pages"]["series"][0]["value"] == 1.0
    assert "exporter down" in caplog.text


def test_runtime_installed_plugin_stops_collecting_after_uninstall() -> None:
    """运行期安装后开始采集，卸载后不再采集，已聚合数据保留且可重新安装。"""

    metrics = Metrics()
    with Runtime() as runtime:
        count(runtime.hooks, "pages")
        handle = runtime.plugin_host.install(metrics.plugin)
        count(runtime.hooks, "pages")
        handle()
        count(runtime.hooks, "pages")
        assert metrics.snapshot()["pages"]["series"][0]["value"] == 1.0
        runtime.plugin_host.install(metrics.plugin)
        count(runtime.hooks, "pages")

    assert metrics.snapshot()["pages"]["series"][0]["value"] == 2.0


def test_importing_interlace_does_not_load_metrics_or_opentelemetry() -> None:
    """导入 interlace 不加载指标包；导入指标包不加载 OpenTelemetry。"""

    script = (
        "import sys\n"
        "import interlace, interlace.plugins, interlace.engine\n"
        "assert 'interlace.metrics' not in sys.modules\n"
        "import interlace.metrics\n"
        "assert not any(name.split('.')[0] == 'opentelemetry' for name in sys.modules)\n"
    )
    subprocess.run([sys.executable, "-c", script], check=True)
