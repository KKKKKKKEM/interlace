"""受控、可命名且可冻结的输入选择策略实现。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol

from .core import ALL_POLICY, ANY_POLICY, LONGEST_POLICY, InputPolicy
from .errors import PolicySelectionError


class InputSelector(Protocol):
    """具名输入策略的选择实现协议。

    实现只能读取端口名称、各端口可用 token 数量和策略配置，不得读取领域值。
    """

    def select(
        self,
        ports: Sequence[str],
        available: Mapping[str, int],
        config: Mapping[str, Any],
    ) -> tuple[str, ...] | None:
        """选择本次 firing 各消费一个 token 的端口。

        Args:
            ports: 保持声明顺序的输入端口名称。
            available: 各端口当前可消费的 token 数量，只读。
            config: 当前具名策略的只读参数映射。

        Returns:
            非空、无重复且只含当前非空端口的元组；暂不触发时返回 None。
        """


@dataclass(frozen=True, slots=True)
class BoundPolicy:
    """Graph 冻结时绑定的输入策略与 selector 实现快照。

    Attributes:
        policy: 策略名称及参数的不可变引用。
        selector: 冻结时取得的选择实现，后续注销不影响该快照。
    """

    policy: InputPolicy
    selector: InputSelector

    def select(
        self,
        ports: Sequence[str],
        queues: Mapping[str, Sequence[object]],
    ) -> tuple[str, ...] | None:
        """把各端口队列长度交给 selector，并校验其选择结果。

        Args:
            ports: 保持声明顺序的输入端口名称。
            queues: 各端口已有的输入 token 队列；缺失端口视为空。

        Returns:
            本次各消费一个 token 的端口元组；暂不触发时返回 None。

        Raises:
            PolicySelectionError: selector 返回非元组、空元组、重复端口、未声明端口或空端口。
                该异常不填写 graph 与 node，由执行器补齐。
        """

        available = MappingProxyType(
            {port: len(queues.get(port, ())) for port in ports}
        )
        name = self.policy.name
        selected = self.selector.select(ports, available, self.policy.config)
        if selected is None:
            return None
        if not isinstance(selected, tuple) or not selected:
            raise PolicySelectionError(
                f"policy {name!r} must return a non-empty tuple or None"
            )
        if len(set(selected)) != len(selected):
            raise PolicySelectionError(
                f"policy {name!r} selected a port more than once"
            )
        unknown = set(selected) - set(ports)
        empty = tuple(port for port in selected if available.get(port, 0) < 1)
        if unknown or empty:
            raise PolicySelectionError(
                f"policy {name!r} made an invalid selection; "
                f"unknown={sorted(unknown)!r}, empty={empty!r}"
            )
        return selected


class _AllSelector:
    """内建 ALL 策略：全部声明端口都有 token 时一次各消费一个。"""

    def select(
        self,
        ports: Sequence[str],
        available: Mapping[str, int],
        config: Mapping[str, Any],
    ) -> tuple[str, ...] | None:
        """全部端口非空时按声明顺序选择全部端口，否则返回 None。"""

        del config
        selected = tuple(ports)
        return (
            selected if selected and all(available[port] for port in selected) else None
        )


class _AnySelector:
    """内建 ANY 策略：每次只消费声明顺序中第一个非空端口。"""

    def select(
        self,
        ports: Sequence[str],
        available: Mapping[str, int],
        config: Mapping[str, Any],
    ) -> tuple[str, ...] | None:
        """返回第一个非空端口组成的单元素元组；全部为空时返回 None。"""

        del config
        return next(((port,) for port in ports if available[port]), None)


class _LongestSelector:
    """内建 longest 策略：每次只消费可用 token 最多的端口，相同时按声明顺序。

    交错到达的多端口输入因此被交替消费，不会像 ANY 那样先排空声明顺序靠前的端口。
    """

    def select(
        self,
        ports: Sequence[str],
        available: Mapping[str, int],
        config: Mapping[str, Any],
    ) -> tuple[str, ...] | None:
        """返回可用 token 最多的非空端口组成的单元素元组；全部为空时返回 None。"""

        del config
        port = max(ports, key=lambda name: available[name], default=None)
        return None if port is None or not available[port] else (port,)


BUILTIN_SELECTORS: Mapping[str, InputSelector] = MappingProxyType(
    {
        ALL_POLICY: _AllSelector(),
        ANY_POLICY: _AnySelector(),
        LONGEST_POLICY: _LongestSelector(),
    }
)  # 始终可用的内建 selector，名称不能被同名提供覆盖。


def bind_policy(
    policy: InputPolicy, selectors: Mapping[str, InputSelector]
) -> BoundPolicy:
    """将输入策略绑定到内建或具名 selector 的实现快照。

    Args:
        policy: 节点声明的输入策略。
        selectors: 冻结时可用的具名 selector 快照，内建名称优先。

    Returns:
        保存策略与 selector 实现的 BoundPolicy。

    Raises:
        TypeError: policy 不是 InputPolicy，或 selector 没有可调用的 select。
        ValueError: 策略名称未提供。
    """

    if not isinstance(policy, InputPolicy):
        raise TypeError("input_policy must be InputPolicy")
    selector = BUILTIN_SELECTORS.get(policy.name) or selectors.get(policy.name)
    if selector is None:
        raise ValueError(f"input policy {policy.name!r} is not provided")
    if not callable(getattr(selector, "select", None)):
        raise TypeError(f"input policy {policy.name!r} selector must define select()")
    return BoundPolicy(policy, selector)
