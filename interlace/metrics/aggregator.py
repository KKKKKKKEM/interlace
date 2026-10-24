"""进程内指标聚合插件：汇总 METRIC 样本与生命周期指标，输出快照和 Prometheus 文本。"""

from __future__ import annotations

import math
import time
from collections import OrderedDict
from collections.abc import Callable, Hashable, Sequence
from functools import partial
from threading import Lock
from typing import Any

from ..engine.extension import HookBus
from ..engine.observation import RuntimeEvent, RuntimeEventKind
from ..engine.points import RUNTIME_EVENT
from ..plugins import ExtensionAPI, Plugin, plugin
from .metric import METRIC, Metric

# 聚合插件的命名空间标识；同一 Runtime 只能安装一个 Metrics 插件。
PLUGIN_ID = "interlace.metrics/collector"
# 等待结束事件配对的开始时间条目上限；超过时淘汰最早的条目，不记录其耗时。
_PENDING_LIMIT = 10_000

# 标签值在 Prometheus 文本中需要转义的字符。
_ESCAPES = str.maketrans({"\\": "\\\\", '"': '\\"', "\n": "\\n"})


class _Series:
    """一组标签下的可变聚合值，只在 `Metrics._lock` 内读写。

    Attributes:
        labels: 序列标签的私有副本。
        value: 计数器累计值。
        count: 直方图观测次数。
        sum: 直方图观测值总和。
        max: 直方图观测最大值；尚无观测时为 0.0。
    """

    __slots__ = ("count", "labels", "max", "sum", "value")

    def __init__(self, labels: dict[str, str]) -> None:
        """创建空序列。

        Args:
            labels: 序列标签，由调用方交出所有权。
        """

        self.labels = labels
        self.value = 0.0
        self.count = 0
        self.sum = 0.0
        self.max = 0.0

    def add(self, kind: str, value: float) -> None:
        """按指标种类累加一次样本。"""

        if kind == "counter":
            self.value += value
            return
        self.max = value if self.count == 0 else max(self.max, value)
        self.count += 1
        self.sum += value

    def view(self, kind: str) -> dict[str, Any]:
        """返回可 JSON 编码的独立副本。"""

        if kind == "counter":
            return {"labels": dict(self.labels), "value": self.value}
        return {
            "labels": dict(self.labels),
            "count": self.count,
            "sum": self.sum,
            "max": self.max,
        }


class _Family:
    """同名指标的种类与按标签区分的序列，只在 `Metrics._lock` 内读写。

    Attributes:
        kind: 指标种类，由首个样本确定。
        series: 排序后的标签元组到序列的映射，按首次出现顺序排列。
    """

    __slots__ = ("kind", "series")

    def __init__(self, kind: str) -> None:
        """创建没有序列的指标族。"""

        self.kind = kind
        self.series: dict[tuple[tuple[str, str], ...], _Series] = {}


class Metrics:
    """线程安全的进程内指标聚合器，以运行期插件采集 METRIC 样本与 Runtime 生命周期指标。

    ``plugin`` 可传给 ``Runtime(plugins=[...])`` 或 ``runtime.plugin_host.install(...)``；卸载后停止采集，
    已聚合的数据保留。同一实例可安装到多个 Runtime，数据在本实例内合并。导出器作为 METRIC 处理函数
    登记在聚合之后，同步接收每个样本，失败按 Notice 契约记录并隔离；每个指标名的序列上限只约束本地内存
    聚合，不过滤导出。

    生命周期指标由 RUNTIME_EVENT 派生并同样经 METRIC 通知：

    - ``interlace.executions{graph,status}``：结束的 execution 数量。
    - ``interlace.execution.seconds{graph,status}``：execution 开始到结束的秒数。
    - ``interlace.node.firings{graph,node,status}``：结束的 Node firing 数量。
    - ``interlace.node.seconds{graph,node}``：Node firing 开始到结束的秒数。
    - ``interlace.works.submitted{queue}``：路由提交的 Work 数量。
    - ``interlace.works{queue,status}``：结束交付的 Work 数量。
    - ``interlace.events{event_type}``：开始分发的领域事件数量。

    Attributes:
        plugin: 不提供 capability 的函数插件，标识为 ``interlace.metrics/collector``。
        _max_series: 每个指标名最多保存的标签序列数。
        _exporters: 接收每个 METRIC 样本的同步导出函数。
        _lock: 保护聚合数据、丢弃计数与计时表的唯一锁。
        _families: 指标名到指标族的映射，按首次出现顺序排列。
        _exposed: Prometheus 暴露名到指标名的映射，用于拒绝点与下划线归一化后的重名。
        _dropped: 因序列上限而丢弃的样本数。
        _executions: execution_id 到开始时刻（`time.monotonic()` 秒）的映射。
        _firings: (execution_id, node, step) 到开始时刻（`time.monotonic()` 秒）的映射。
    """

    def __init__(
        self,
        *,
        max_series: int = 1000,
        exporters: Sequence[Callable[[Metric], object]] = (),
    ) -> None:
        """创建空聚合器与对应插件。

        Args:
            max_series: 每个指标名最多保存的标签序列数，超出的新序列样本被丢弃并计入 `dropped`。
            exporters: 同步导出函数，例如 `interlace.metrics.otel.otel_exporter` 的返回值。

        Raises:
            TypeError: max_series 不是 int，或导出函数不可调用。
            ValueError: max_series 小于 1。
        """

        if isinstance(max_series, bool) or not isinstance(max_series, int):
            raise TypeError("max_series must be an int")
        if max_series < 1:
            raise ValueError("max_series must be positive")
        exporters = tuple(exporters)
        if not all(callable(exporter) for exporter in exporters):
            raise TypeError("metric exporters must be callable")
        self._max_series = max_series
        self._exporters = exporters
        self._lock = Lock()
        self._families: dict[str, _Family] = {}
        self._exposed: dict[str, str] = {}
        self._dropped = 0
        self._executions: OrderedDict[str, float] = OrderedDict()
        self._firings: OrderedDict[tuple[str, str, int], float] = OrderedDict()
        self.plugin: Plugin = plugin(PLUGIN_ID)(self._setup)

    @property
    def dropped(self) -> int:
        """返回因每个指标名的序列上限而丢弃的样本数。"""

        with self._lock:
            return self._dropped

    def snapshot(self) -> dict[str, dict[str, Any]]:
        """返回当前聚合值的独立、可 JSON 编码副本。

        Returns:
            指标名到 ``{"kind": ..., "series": [...]}`` 的映射；计数器序列为
            ``{"labels": {...}, "value": v}``，直方图序列为
            ``{"labels": {...}, "count": n, "sum": s, "max": m}``。
        """

        with self._lock:
            return {
                name: {
                    "kind": family.kind,
                    "series": [
                        series.view(family.kind) for series in family.series.values()
                    ],
                }
                for name, family in self._families.items()
            }

    def prometheus(self) -> str:
        """按 Prometheus 文本格式 0.0.4 渲染当前快照。

        指标名中的点替换为下划线；计数器以 ``_total`` 结尾，直方图渲染为只含 ``_count`` 与 ``_sum``
        的 summary。

        Returns:
            以换行结尾的文本；没有任何指标时为空字符串。
        """

        lines: list[str] = []
        for name, family in self.snapshot().items():
            exposed = _exposed_name(name, family["kind"])
            if family["kind"] == "counter":
                lines.append(f"# TYPE {exposed} counter")
                for series in family["series"]:
                    lines.append(
                        f"{exposed}{_labels(series['labels'])} "
                        f"{_number(series['value'])}"
                    )
                continue
            lines.append(f"# TYPE {exposed} summary")
            for series in family["series"]:
                labels = _labels(series["labels"])
                lines.append(f"{exposed}_count{labels} {series['count']}")
                lines.append(f"{exposed}_sum{labels} {_number(series['sum'])}")
        return "".join(f"{line}\n" for line in lines)

    def _setup(self, api: ExtensionAPI) -> None:
        """登记聚合、导出与生命周期派生处理函数。"""

        api.on(METRIC, self._record)
        for exporter in self._exporters:
            api.on(METRIC, exporter)
        derived: dict[RuntimeEventKind, Callable[[HookBus, RuntimeEvent], None]] = {
            RuntimeEventKind.EXECUTION_STARTED: self._execution_started,
            RuntimeEventKind.EXECUTION_FINISHED: self._execution_finished,
            RuntimeEventKind.NODE_STARTED: self._node_started,
            RuntimeEventKind.NODE_FINISHED: self._node_finished,
            RuntimeEventKind.WORK_SUBMITTED: self._work_submitted,
            RuntimeEventKind.WORK_FINISHED: self._work_finished,
            RuntimeEventKind.EVENT_PUBLISHED: self._event_published,
        }
        for kind, handler in derived.items():
            api.on(RUNTIME_EVENT, partial(handler, api.hooks), kind=kind)

    def _record(self, metric: Metric) -> None:
        """把一个样本累加到对应序列；新序列超出上限时丢弃并计数。

        Raises:
            TypeError: 通知值不是 Metric。
            ValueError: 指标名已以另一种类记录，或其 Prometheus 暴露名与其他指标重名。
        """

        if not isinstance(metric, Metric):
            raise TypeError("METRIC values must be Metric")
        key = tuple(sorted(metric.labels.items()))
        with self._lock:
            family = self._families.get(metric.name)
            if family is None:
                exposed = _exposed_name(metric.name, metric.kind)
                owner = self._exposed.setdefault(exposed, metric.name)
                if owner != metric.name:
                    raise ValueError(
                        f"metric {metric.name!r} collides with {owner!r} as {exposed}"
                    )
                family = self._families[metric.name] = _Family(metric.kind)
            elif family.kind != metric.kind:
                raise ValueError(
                    f"metric {metric.name!r} is already recorded as {family.kind}"
                )
            series = family.series.get(key)
            if series is None:
                if len(family.series) >= self._max_series:
                    self._dropped += 1
                    return
                series = family.series[key] = _Series(dict(key))
            series.add(family.kind, metric.value)

    def _execution_started(self, hooks: HookBus, event: RuntimeEvent) -> None:
        """记录 execution 开始时刻。"""

        del hooks
        if event.execution_id is not None:
            self._track(self._executions, event.execution_id)

    def _execution_finished(self, hooks: HookBus, event: RuntimeEvent) -> None:
        """计数结束的 execution，并观测与开始事件配对的耗时。"""

        labels = {"graph": event.graph or "", "status": event.status or ""}
        _emit(hooks, Metric("interlace.executions", labels=labels))
        elapsed = self._elapsed(self._executions, event.execution_id)
        if elapsed is not None:
            _emit(
                hooks,
                Metric("interlace.execution.seconds", elapsed, "histogram", labels),
            )

    def _node_started(self, hooks: HookBus, event: RuntimeEvent) -> None:
        """记录 Node firing 开始时刻；同一 execution 的 firing 依次执行，按步序号配对。"""

        del hooks
        key = _firing(event)
        if key is not None:
            self._track(self._firings, key)

    def _node_finished(self, hooks: HookBus, event: RuntimeEvent) -> None:
        """计数结束的 Node firing，并观测与开始事件配对的耗时。"""

        graph, node = event.graph or "", event.node or ""
        _emit(
            hooks,
            Metric(
                "interlace.node.firings",
                labels={"graph": graph, "node": node, "status": event.status or ""},
            ),
        )
        elapsed = self._elapsed(self._firings, _firing(event))
        if elapsed is not None:
            _emit(
                hooks,
                Metric(
                    "interlace.node.seconds",
                    elapsed,
                    "histogram",
                    {"graph": graph, "node": node},
                ),
            )

    def _work_submitted(self, hooks: HookBus, event: RuntimeEvent) -> None:
        """计数路由提交的 Work。"""

        queue = str(event.attributes.get("queue", ""))
        _emit(hooks, Metric("interlace.works.submitted", labels={"queue": queue}))

    def _work_finished(self, hooks: HookBus, event: RuntimeEvent) -> None:
        """按交付结果计数结束的 Work。"""

        queue = str(event.attributes.get("queue", ""))
        labels = {"queue": queue, "status": event.status or ""}
        _emit(hooks, Metric("interlace.works", labels=labels))

    def _event_published(self, hooks: HookBus, event: RuntimeEvent) -> None:
        """按事件类型计数开始分发的领域事件。"""

        labels = {"event_type": event.event_type or ""}
        _emit(hooks, Metric("interlace.events", labels=labels))

    def _track(self, pending: OrderedDict[Any, float], key: Hashable) -> None:
        """记录开始时刻；条目超过上限时淘汰最早的一项。"""

        now = time.monotonic()
        with self._lock:
            pending[key] = now
            pending.move_to_end(key)
            if len(pending) > _PENDING_LIMIT:
                pending.popitem(last=False)

    def _elapsed(
        self, pending: OrderedDict[Any, float], key: Hashable | None
    ) -> float | None:
        """取出并返回自开始以来的秒数；没有配对的开始记录时为 None。"""

        if key is None:
            return None
        now = time.monotonic()
        with self._lock:
            started = pending.pop(key, None)
        return None if started is None else max(now - started, 0.0)


def _emit(hooks: HookBus, metric: Metric) -> None:
    """经 METRIC 通知生命周期指标，使聚合器与导出器以同一路径接收。"""

    hooks.notify(METRIC, metric, name=metric.name)


def _firing(event: RuntimeEvent) -> tuple[str, str, int] | None:
    """返回 Node firing 的配对键；缺少 execution 或步序号时为 None。"""

    step = event.attributes.get("step")
    if event.execution_id is None or event.node is None or not isinstance(step, int):
        return None
    return (event.execution_id, event.node, step)


def _exposed_name(name: str, kind: str) -> str:
    """返回指标在 Prometheus 文本中的名称：点改为下划线，计数器以 ``_total`` 结尾。"""

    base = name.replace(".", "_")
    if kind == "counter" and not base.endswith("_total"):
        return f"{base}_total"
    return base


def _labels(labels: dict[str, str]) -> str:
    """渲染标签集合并转义值中的反斜杠、双引号与换行；没有标签时为空字符串。"""

    if not labels:
        return ""
    pairs = ",".join(
        f'{key}="{value.translate(_ESCAPES)}"' for key, value in labels.items()
    )
    return f"{{{pairs}}}"


def _number(value: float) -> str:
    """按 Prometheus 文本格式渲染浮点数，溢出的无穷值写作 ``+Inf``/``-Inf``。"""

    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    return repr(value)
