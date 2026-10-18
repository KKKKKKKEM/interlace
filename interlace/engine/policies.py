"""受控、可命名且可冻结的输入选择策略贡献点。"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from threading import RLock
from types import MappingProxyType
from typing import Any, ClassVar, Protocol

from .._validation import require_non_empty_string
from .core import InputPolicy
from .errors import PolicySelectionError

ALL_POLICY = "interlace.core/all"  # InputPolicy.ALL 对应的内建 selector 名称。
ANY_POLICY = "interlace.core/any"  # InputPolicy.ANY 对应的内建 selector 名称。
ON_START_POLICY = (
    "interlace.core/on-start"  # InputPolicy.ON_START 对应的内建 selector 名称。
)
LONGEST_POLICY = (
    "interlace.core/longest"  # 可用 token 最多的端口优先的内建 selector 名称。
)

_BUILTIN_NAMES: Mapping[InputPolicy, str] = MappingProxyType(
    {
        InputPolicy.ALL: ALL_POLICY,
        InputPolicy.ANY: ANY_POLICY,
        InputPolicy.ON_START: ON_START_POLICY,
    }
)  # 固定输入策略到内建 selector 名称的只读映射。


def _freeze_config(value: Any) -> Any:
    """递归深复制策略配置，并把映射、列表/元组、集合分别转为只读映射、元组、frozenset。"""

    if isinstance(value, Mapping):
        return MappingProxyType(
            {deepcopy(key): _freeze_config(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_config(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze_config(item) for item in value)
    return deepcopy(value)


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
class PolicyRef:
    """带命名空间名称和只读参数的输入策略引用。

    按值比较 name 与 config。config 冻结为只读映射，无法稳定哈希，因此 PolicyRef
    显式不可哈希。

    Attributes:
        name: 已注册 selector 的命名空间名称。
        config: 构造时深复制并递归冻结的策略参数映射。
    """

    name: str
    config: Mapping[str, Any] = field(default_factory=dict)

    __hash__: ClassVar[None] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        """校验名称并冻结配置副本。

        Raises:
            TypeError: name 不是字符串或 config 不是映射。
            ValueError: name 为空。
        """

        require_non_empty_string(self.name, "policy name")
        if not isinstance(self.config, Mapping):
            raise TypeError("policy config must be a mapping")
        object.__setattr__(self, "config", _freeze_config(self.config))


@dataclass(frozen=True, slots=True)
class BoundPolicy:
    """Graph 冻结时绑定的策略引用与 selector 实现快照。

    Attributes:
        ref: 策略名称及参数的不可变引用。
        selector: 冻结时从注册表取得的选择实现，后续注销不影响该快照。
    """

    ref: PolicyRef
    selector: InputSelector

    @property
    def on_start(self) -> bool:
        """判断绑定的是否为内建启动触发策略。

        Returns:
            策略名称为 ON_START_POLICY 时返回 True。
        """

        return self.ref.name == ON_START_POLICY

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
        selected = self.selector.select(ports, available, self.ref.config)
        if selected is None:
            return None
        if not isinstance(selected, tuple) or not selected:
            raise PolicySelectionError(
                f"policy {self.ref.name!r} must return a non-empty tuple or None"
            )
        if len(set(selected)) != len(selected):
            raise PolicySelectionError(
                f"policy {self.ref.name!r} selected a port more than once"
            )
        unknown = set(selected) - set(ports)
        empty = tuple(port for port in selected if available.get(port, 0) < 1)
        if unknown or empty:
            raise PolicySelectionError(
                f"policy {self.ref.name!r} made an invalid selection; "
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


class _OnStartSelector:
    """内建 ON_START 策略：从不选择 token，由执行器在启动时单次触发节点。"""

    def select(
        self,
        ports: Sequence[str],
        available: Mapping[str, int],
        config: Mapping[str, Any],
    ) -> None:
        """始终返回 None。"""

        del ports, available, config


class PolicyRegistry:
    """注册 namespaced selector；Graph freeze 会保存实现快照。

    Attributes:
        _selectors: 名称到 selector 实现的映射，预置 all、any、on-start 与 longest 内建策略。
        _lock: 保护 _selectors 的可重入锁。
    """

    def __init__(self) -> None:
        """注册内建输入策略并初始化注册锁。"""

        self._selectors: dict[str, InputSelector] = {
            ALL_POLICY: _AllSelector(),
            ANY_POLICY: _AnySelector(),
            ON_START_POLICY: _OnStartSelector(),
            LONGEST_POLICY: _LongestSelector(),
        }
        self._lock = RLock()

    def register(self, name: str, selector: InputSelector) -> Callable[[], None]:
        """注册具名输入选择器，拒绝重复名称和缺失命名空间。

        Args:
            name: 含 ``/`` 命名空间分隔的 selector 名称。
            selector: 仅依据端口和 token 数量选择输入的实现。

        Returns:
            仅撤销本次注册的幂等函数，不改变 Graph 已绑定的选择器快照。

        Raises:
            TypeError: name 不是字符串，或 selector 没有可调用的 select。
            ValueError: name 为空、不含命名空间或已注册。
        """

        name = require_non_empty_string(name, "policy name")
        if "/" not in name:
            raise ValueError("contributed policy names must be namespaced")
        if not callable(getattr(selector, "select", None)):
            raise TypeError("policy selector must define select()")
        with self._lock:
            if name in self._selectors:
                raise ValueError(f"input policy {name!r} is already registered")
            self._selectors[name] = selector
        detached = False

        def detach() -> None:
            """撤销当前具名注册，重复调用不影响后续同名注册。"""

            nonlocal detached
            with self._lock:
                if not detached:
                    del self._selectors[name]
                    detached = True

        return detach

    def bind(self, policy: InputPolicy | PolicyRef) -> BoundPolicy:
        """将输入策略绑定到当前注册的 selector 实现快照。

        Args:
            policy: 内建输入策略或具名策略引用。

        Returns:
            保存策略引用与 selector 实现的 BoundPolicy。

        Raises:
            TypeError: policy 既不是 InputPolicy 也不是 PolicyRef。
            ValueError: 策略名称未注册。
        """

        if isinstance(policy, InputPolicy):
            ref = PolicyRef(_BUILTIN_NAMES[policy])
        elif isinstance(policy, PolicyRef):
            ref = policy
        else:
            raise TypeError("input_policy must be InputPolicy or PolicyRef")
        with self._lock:
            try:
                selector = self._selectors[ref.name]
            except KeyError as exc:
                raise ValueError(
                    f"input policy {ref.name!r} is not registered"
                ) from exc
        return BoundPolicy(ref, selector)
