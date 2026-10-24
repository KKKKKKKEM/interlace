"""零依赖的进程内指标：METRIC 钩子点、记录函数与聚合插件。

领域代码以 ``count(context.hooks, ...)`` 与 ``observe(context.hooks, ...)`` 经 METRIC 通知样本；
`Metrics` 以运行期插件聚合样本与 Runtime 生命周期指标，提供 JSON 快照与 Prometheus 文本。
OpenTelemetry 桥接位于 `interlace.metrics.otel`，需要 ``interlace-py[otel]``。导入 `interlace` 不加载本包。
"""

from .aggregator import Metrics
from .metric import METRIC, Metric, count, observe

__all__ = ["METRIC", "Metric", "Metrics", "count", "observe"]
