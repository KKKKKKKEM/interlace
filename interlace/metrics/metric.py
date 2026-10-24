"""指标样本、METRIC 钩子点与领域代码使用的记录函数。"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal

from ..engine.extension import HookBus, HookCaller, NoticePoint

# 指标名：小写字母开头，以点分隔命名空间片段。
_NAME = re.compile(r"[a-z][a-z0-9_]*(?:\.[a-z0-9_]+)*")
# 标签键：小写标识符；`__` 前缀为 Prometheus 保留。
_LABEL = re.compile(r"[a-z_][a-z0-9_]*")
# 支持的指标种类。
_KINDS = ("counter", "histogram")


@dataclass(frozen=True, slots=True)
class Metric:
    """一次计数或观测样本；构造时校验名称、取值与标签。

    Attributes:
        name: 点分命名空间的指标名，例如 ``"crawler.pages"``；匹配
            ``[a-z][a-z0-9_]*(\\.[a-z0-9_]+)*``。
        value: 有限浮点值；计数器增量必须不小于 0，默认 1.0。
        kind: ``"counter"`` 累加增量，``"histogram"`` 记录观测值的数量、总和与最大值。
        labels: 标签键到字符串值的只读快照；键匹配 ``[a-z_][a-z0-9_]*`` 且不以 ``__`` 开头。
    """

    name: str
    value: float = 1.0
    kind: Literal["counter", "histogram"] = "counter"
    labels: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """校验字段，把 value 规范为 float、labels 冻结为只读快照。

        Raises:
            TypeError: name、value、kind 或标签键值类型不符。
            ValueError: 名称或标签键不合法、kind 未知、value 非有限值或计数器增量为负。
        """

        if not isinstance(self.name, str):
            raise TypeError("metric name must be a string")
        if not _NAME.fullmatch(self.name):
            raise ValueError(f"invalid metric name {self.name!r}")
        if isinstance(self.value, bool) or not isinstance(self.value, int | float):
            raise TypeError("metric value must be a real number")
        value = float(self.value)
        if not math.isfinite(value):
            raise ValueError("metric value must be finite")
        if self.kind not in _KINDS:
            raise ValueError(f"metric kind must be one of {_KINDS!r}")
        if self.kind == "counter" and value < 0:
            raise ValueError("counter increments must not be negative")
        if not isinstance(self.labels, Mapping):
            raise TypeError("metric labels must be a mapping")
        labels = dict(self.labels)
        for key, label in labels.items():
            if not isinstance(key, str) or not isinstance(label, str):
                raise TypeError("metric label keys and values must be strings")
            if not _LABEL.fullmatch(key) or key.startswith("__"):
                raise ValueError(f"invalid metric label key {key!r}")
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "labels", MappingProxyType(labels))


METRIC: NoticePoint[Metric] = NoticePoint("interlace.metrics/record", scope=("name",))
"""指标样本的只读通知；scope 的 name 为 `Metric.name`，处理函数可按名称过滤。"""


def count(
    hooks: HookCaller | HookBus,
    name: str,
    value: float = 1.0,
    /,
    **labels: str,
) -> None:
    """经 METRIC 通知一次计数器增量；没有处理函数时只构造并校验样本。

    Args:
        hooks: 触发钩子点的视图，Node 中为 ``context.hooks``。
        name: 指标名。
        value: 非负增量，默认 1.0。
        **labels: 标签键值，值必须是字符串。

    Raises:
        TypeError: 参数类型不符。
        ValueError: 名称、标签键或增量不合法。
    """

    hooks.notify(METRIC, Metric(name, value, "counter", labels), name=name)


def observe(
    hooks: HookCaller | HookBus,
    name: str,
    value: float,
    /,
    **labels: str,
) -> None:
    """经 METRIC 通知一次直方图观测值，例如以秒为单位的耗时。

    Args:
        hooks: 触发钩子点的视图，Node 中为 ``context.hooks``。
        name: 指标名。
        value: 有限观测值。
        **labels: 标签键值，值必须是字符串。

    Raises:
        TypeError: 参数类型不符。
        ValueError: 名称、标签键或观测值不合法。
    """

    hooks.notify(METRIC, Metric(name, value, "histogram", labels), name=name)
