"""把 METRIC 样本桥接到 OpenTelemetry Meter；需要安装 ``interlace-py[otel]``。"""

from __future__ import annotations

from collections.abc import Callable
from threading import Lock

try:
    from opentelemetry.metrics import Counter, Histogram, Meter
except ImportError as error:
    raise ImportError(
        "interlace.metrics.otel requires OpenTelemetry; "
        "install it with: pip install 'interlace-py[otel]'"
    ) from error

from .metric import Metric


def otel_exporter(meter: Meter) -> Callable[[Metric], None]:
    """创建把样本记录到 OpenTelemetry 仪表的同步导出函数。

    每个指标名在首次出现时按种类创建一个 Counter 或 Histogram，仪表名保留点分形式，标签作为属性。
    返回值可传给 ``Metrics(exporters=[...])``，也可直接以 ``api.on(METRIC, exporter)`` 登记。

    Args:
        meter: 创建仪表的 OpenTelemetry Meter。

    Returns:
        线程安全的导出函数；同名指标以另一种类到达时抛出 ValueError，由 Notice 宿主隔离。
    """

    lock = Lock()
    instruments: dict[str, tuple[str, Counter | Histogram]] = {}

    def export(metric: Metric) -> None:
        """按指标名取得或创建仪表并记录一次样本。

        Raises:
            ValueError: 同名指标已以另一种类创建仪表。
        """

        with lock:
            entry = instruments.get(metric.name)
            if entry is None:
                instrument: Counter | Histogram
                if metric.kind == "counter":
                    instrument = meter.create_counter(metric.name)
                else:
                    instrument = meter.create_histogram(metric.name)
                entry = instruments[metric.name] = (metric.kind, instrument)
        kind, instrument = entry
        if kind != metric.kind:
            raise ValueError(f"metric {metric.name!r} is already exported as {kind}")
        attributes = dict(metric.labels)
        if isinstance(instrument, Counter):
            instrument.add(metric.value, attributes)
        else:
            instrument.record(metric.value, attributes)

    return export


__all__ = ["otel_exporter"]
