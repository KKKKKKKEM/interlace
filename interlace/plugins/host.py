"""统一、受控的 Runtime 插件宿主。"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from .._validation import require_non_empty_string
from ..engine.hooks import HookPhase, NodeHook
from ..engine.observation import RuntimeObserver
from ..engine.policies import InputSelector
from ..spi.roles import HookHost, ObservationHost, PolicyHost

_VERSION = re.compile(r"^[0-9]+(?:\.[0-9]+){0,2}(?:[-+][A-Za-z0-9.-]+)?$")
_logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PluginDescriptor:
    """插件身份、依赖以及声明提供的能力。

    Attributes:
        id: 含 ``/`` 的命名空间插件标识。
        version: 插件自身的简单语义化版本。
        requires: 必须先 setup/start 的插件标识。
        provides: 插件必须在 setup 中提供的单例 capability。
        api_version: 插件兼容的 SPI 主版本字符串，必须与宿主一致。
        requires_capabilities: 必须由其他插件提供的单例 capability，提供者先于本插件装配。
        contributes: 插件必须在 setup 中至少贡献一项的聚合 capability。
    """

    id: str
    version: str
    requires: tuple[str, ...] = ()
    provides: tuple[str, ...] = ()
    api_version: str = "2"
    requires_capabilities: tuple[str, ...] = ()
    contributes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """校验标识、版本格式和各 capability 声明之间的一致性。

        Raises:
            TypeError: 任一声明集合不是 tuple，或元素不是字符串。
            ValueError: 标识缺少命名空间、版本格式不合法、声明重复、依赖自身，或同一 capability
                同时声明为单例与聚合、同时提供与依赖。
        """

        plugin_id = require_non_empty_string(self.id, "plugin id")
        if "/" not in plugin_id:
            raise ValueError("plugin id must be namespaced")
        version = require_non_empty_string(self.version, "plugin version")
        if not _VERSION.match(version):
            raise ValueError("plugin version must use a simple semantic version")
        api_version = require_non_empty_string(self.api_version, "plugin API version")
        if not api_version.isdigit() or int(api_version) < 1:
            raise ValueError("plugin API version must be a positive integer")
        for label, values in (
            ("plugin requirement", self.requires),
            ("required capability", self.requires_capabilities),
            ("provided capability", self.provides),
            ("contributed capability", self.contributes),
        ):
            if not isinstance(values, tuple):
                raise TypeError(f"{label}s must be a tuple")
            normalized = tuple(
                require_non_empty_string(value, label) for value in values
            )
            if len(set(normalized)) != len(normalized):
                raise ValueError(f"duplicate {label}")
        if plugin_id in self.requires:
            raise ValueError("plugin must not require itself")
        if set(self.provides) & set(self.contributes):
            raise ValueError("a capability cannot be both singleton and aggregate")
        overlap = set(self.requires_capabilities) & set(self.provides)
        if overlap:
            raise ValueError(
                f"plugin must not require capabilities it provides: {sorted(overlap)!r}"
            )


class Plugin(Protocol):
    """向宿主提供 capability 或贡献、并参与 setup/start/stop 生命周期的插件协议。

    Attributes:
        descriptor: 插件身份、依赖和能力声明。
    """

    descriptor: PluginDescriptor

    def setup(self, context: PluginContext) -> None:
        """登记声明的 capability 与贡献；此阶段不能使用尚未启动的外部资源。

        Args:
            context: 绑定当前插件身份的宿主上下文。
        """

    def start(self, context: PluginContext) -> None:
        """在全部插件完成 setup 且标准贡献安装后，按依赖顺序启动。

        Args:
            context: 绑定当前插件身份的宿主上下文。
        """

    def stop(self, context: PluginContext) -> None:
        """释放资源；宿主按 setup 顺序的逆序调用，setup 中途失败的插件也会被调用。

        Args:
            context: 绑定当前插件身份的宿主上下文。
        """


@dataclass(frozen=True, slots=True)
class Contribution:
    """一个插件登记的具名聚合能力贡献。

    Attributes:
        plugin: 登记该贡献的插件标识。
        capability: 贡献所属的聚合 capability。
        name: 在该 capability 内唯一的命名空间名称。
        value: 贡献对象，例如 InputSelector、NodeHook 或 RuntimeObserver；不参与比较和 repr。
    """

    plugin: str
    capability: str
    name: str
    value: Any = field(compare=False, repr=False)


@dataclass(frozen=True, slots=True)
class NodeHookContribution:
    """一个带作用域的动态 Node Hook 贡献。

    Attributes:
        hook: NodeHook 对象或单阶段回调；不参与比较和 repr。
        phase: 函数 Hook 的执行阶段；NodeHook 对象必须为 None。
        graph: 限定生效的 Graph 注册名称，None 表示全部 Graph。
        node: 限定生效的 Graph 内节点 ID，需要同时指定 graph。
    """

    hook: NodeHook | Callable[..., object] = field(compare=False, repr=False)
    phase: HookPhase | str | None = None
    graph: str | None = None
    node: str | None = None

    def __post_init__(self) -> None:
        """校验 Hook 类型、阶段与作用域组合。

        Raises:
            TypeError: hook 既不是 NodeHook 也不可调用，或 NodeHook 对象指定了 phase。
            ValueError: phase 不是有效阶段、node 缺少 graph，或作用域名称为空字符串。
        """

        if not isinstance(self.hook, NodeHook) and not callable(self.hook):
            raise TypeError("hook must be a NodeHook or callable")
        if isinstance(self.hook, NodeHook) and self.phase is not None:
            raise TypeError("phase is only valid for a function hook")
        if self.phase is not None:
            HookPhase(self.phase)
        if self.node is not None and self.graph is None:
            raise ValueError("node-scoped hook contribution requires graph")
        if self.graph is not None:
            require_non_empty_string(self.graph, "hook graph")
        if self.node is not None:
            require_non_empty_string(self.node, "hook node")


class RegistrationHandle:
    """撤销一项插件注册的幂等句柄。

    setup 阶段可以撤销并重新登记；宿主冻结后、开始关闭或回滚前撤销会失败，避免已取得 capability 的组件
    看到注册消失；关闭和启动回滚期间（包括插件 stop 内）撤销总是成功。

    Attributes:
        __slots__: 限制实例只保存下列字段。
        _detach: 执行实际撤销的回调。
        _detached: 是否已经成功撤销；成功后再次调用不产生作用。
    """

    __slots__ = ("_detach", "_detached")

    def __init__(self, detach: Callable[[], None]) -> None:
        """保存执行撤销的回调。

        Args:
            detach: 从宿主移除本次注册的回调。
        """

        self._detach = detach
        self._detached = False

    def detach(self) -> None:
        """撤销本次注册；成功后重复调用不产生作用。

        Raises:
            RuntimeError: 宿主已冻结且尚未开始关闭或回滚。
        """

        if not self._detached:
            self._detach()
            self._detached = True

    def __enter__(self) -> RegistrationHandle:
        """进入作用域并返回当前句柄。

        Returns:
            当前句柄。
        """

        return self

    def __exit__(self, *args: object) -> None:
        """离开作用域时撤销本次注册。"""

        del args
        self.detach()


class PluginContext:
    """绑定单个插件身份的最小宿主接口。

    Attributes:
        __slots__: 限制实例只保存下列字段。
        _host: 所属的插件宿主。
        _plugin: 绑定的插件标识，所有注册以该身份登记。
    """

    __slots__ = ("_host", "_plugin")

    def __init__(self, host: PluginHost, plugin: str) -> None:
        """把插件标识绑定到宿主的受控注册入口。

        Args:
            host: 所属的插件宿主。
            plugin: 插件标识。
        """

        self._host = host
        self._plugin = plugin

    @property
    def plugin(self) -> str:
        """返回绑定的插件标识。

        Returns:
            含命名空间的插件标识。
        """

        return self._plugin

    def provide(self, capability: str, value: Any) -> RegistrationHandle:
        """提供一项已在 descriptor.provides 声明的单例 capability。

        Args:
            capability: 单例 capability 名称。
            value: capability 的实现对象。

        Returns:
            撤销本次提供的句柄。

        Raises:
            RuntimeError: 宿主已结束 setup 阶段。
            ValueError: capability 未声明，或已被其他插件提供。
        """

        return self._host._provide(self._plugin, capability, value)

    def contribute(
        self,
        capability: str,
        name: str,
        value: Any,
    ) -> RegistrationHandle:
        """向已在 descriptor.contributes 声明的聚合 capability 登记一项具名贡献。

        Args:
            capability: 聚合 capability 名称。
            name: 在该 capability 内唯一的命名空间名称。
            value: 贡献对象。

        Returns:
            撤销本次贡献的句柄。

        Raises:
            RuntimeError: 宿主已结束 setup 阶段。
            ValueError: capability 未声明、name 缺少命名空间或已经存在。
        """

        return self._host._contribute(self._plugin, capability, name, value)

    def require(self, capability: str) -> Any:
        """取得已经提供的单例 capability。

        Args:
            capability: 单例 capability 名称。

        Returns:
            capability 的实现对象。

        Raises:
            LookupError: capability 尚未被提供。
        """

        return self._host.require(capability)

    def contributions(self, capability: str) -> tuple[Contribution, ...]:
        """取得聚合 capability 当前的全部贡献。

        Args:
            capability: 聚合 capability 名称。

        Returns:
            按登记顺序排列的贡献快照。
        """

        return self._host.contributions(capability)


class PluginHost:
    """解析插件依赖，并管理统一能力注册和生命周期。

    启动失败或关闭时先逆序注销已安装的标准贡献，再逆序停止已进入 setup 的插件；单项失败不跳过后续清理。
    关闭时抛出首个失败，其余失败写入 ``interlace.plugins.host`` 的 ERROR 日志；启动回滚时重新抛出原始
    启动失败，清理失败全部写入日志。

    Attributes:
        API_VERSION: 宿主支持的 SPI 主版本字符串。
        _plugins: 按依赖顺序排列的 (声明, 插件) 对。
        _capabilities: 单例 capability 名称到 (提供者标识, 实现) 的映射。
        _contributions: 聚合 capability 名称到 {贡献名称: 贡献} 的映射。
        _contexts: 插件标识到其宿主上下文的映射。
        _started: 已进入 setup、需要逆序 stop 的插件，按 setup 顺序保存。
        _contribution_detachers: 标准贡献安装后的注销函数，按安装顺序保存、逆序执行。
        _frozen: setup 阶段是否已结束；结束后拒绝新注册。
        _closed: 是否已开始关闭或启动回滚；开始后允许撤销注册，拒绝再次启动。
    """

    API_VERSION = "2"

    def __init__(self, plugins: Iterable[Plugin]) -> None:
        """校验插件声明并按依赖关系确定装配顺序。

        Args:
            plugins: 待装配的插件。

        Raises:
            TypeError: plugins 不可迭代，或插件缺少 PluginDescriptor 与生命周期方法。
            ValueError: 插件重复、SPI 版本不兼容、单例 capability 冲突、依赖缺失或循环依赖。
        """

        candidates = _collect(plugins)
        self._plugins = _order(candidates, _index_providers(candidates))
        self._capabilities: dict[str, tuple[str, Any]] = {}
        self._contributions: dict[str, dict[str, Contribution]] = defaultdict(dict)
        self._contexts = {
            descriptor.id: PluginContext(self, descriptor.id)
            for descriptor, _ in self._plugins
        }
        self._started: list[tuple[PluginDescriptor, Plugin]] = []
        self._contribution_detachers: list[Callable[[], None]] = []
        self._frozen = False
        self._closed = False

    @property
    def descriptors(self) -> tuple[PluginDescriptor, ...]:
        """返回按装配顺序排列的插件声明快照。

        Returns:
            按依赖顺序排列的插件声明。
        """

        return tuple(descriptor for descriptor, _ in self._plugins)

    def start(self) -> PluginHost:
        """依次 setup 全部插件、安装标准贡献并 start；已启动时直接返回。

        任一步骤失败时执行回滚，回滚中的清理失败写入日志，随后重新抛出原始失败，宿主进入关闭状态。

        Returns:
            当前宿主。

        Raises:
            RuntimeError: 宿主已经关闭，或插件未兑现声明的 capability。
            BaseException: 插件 setup/start 或贡献安装抛出的原始异常。
        """

        if self._closed:
            raise RuntimeError("plugin host is closed")
        if self._frozen:
            return self
        try:
            for descriptor, plugin in self._plugins:
                # setup 即使中途失败也可能已经创建资源，因此先纳入回滚栈。
                self._started.append((descriptor, plugin))
                plugin.setup(self._contexts[descriptor.id])
                self._ensure_fulfilled(descriptor)
            self._frozen = True
            self._install_runtime_contributions()
            for descriptor, plugin in self._started:
                plugin.start(self._contexts[descriptor.id])
        except BaseException:
            self._frozen = True
            self._closed = True
            failure = self._stop_started()
            if failure is not None:
                _logger.error("plugin host rollback failed", exc_info=failure)
            raise
        return self

    def require(self, capability: str) -> Any:
        """按名称取得已经提供的单例 capability。

        Args:
            capability: 单例 capability 名称。

        Returns:
            capability 的实现对象。

        Raises:
            LookupError: capability 尚未被提供。
        """

        capability = require_non_empty_string(capability, "capability")
        try:
            return self._capabilities[capability][1]
        except KeyError as exc:
            raise LookupError(
                f"required capability {capability!r} is unavailable"
            ) from exc

    def contributions(self, capability: str) -> tuple[Contribution, ...]:
        """返回聚合 capability 当前的全部贡献。

        Args:
            capability: 聚合 capability 名称。

        Returns:
            按登记顺序排列的贡献快照，没有贡献时为空元组。
        """

        capability = require_non_empty_string(capability, "capability")
        return tuple(self._contributions.get(capability, {}).values())

    def close(self) -> None:
        """逆序注销标准贡献并停止插件；重复调用不产生作用。

        Raises:
            BaseException: 清理中出现的首个失败；其余失败已写入日志。
        """

        if self._closed:
            return
        self._closed = True
        failure = self._stop_started()
        if failure is not None:
            raise failure

    def _provide(self, plugin: str, capability: str, value: Any) -> RegistrationHandle:
        """以插件身份登记单例 capability，并返回撤销句柄。

        Raises:
            RuntimeError: 宿主已结束 setup 阶段。
            ValueError: capability 未声明，或已被其他插件提供。
        """

        self._ensure_registering()
        capability = require_non_empty_string(capability, "capability")
        self._ensure_declared(plugin, capability, aggregate=False)
        existing = self._capabilities.get(capability)
        if existing is not None:
            raise ValueError(
                f"capability {capability!r} is already provided by {existing[0]!r}"
            )
        self._capabilities[capability] = (plugin, value)
        return RegistrationHandle(lambda: self._remove_capability(plugin, capability))

    def _contribute(
        self, plugin: str, capability: str, name: str, value: Any
    ) -> RegistrationHandle:
        """以插件身份登记聚合 capability 的具名贡献，并返回撤销句柄。

        Raises:
            RuntimeError: 宿主已结束 setup 阶段。
            ValueError: capability 未声明、name 缺少命名空间或已经存在。
        """

        self._ensure_registering()
        capability = require_non_empty_string(capability, "capability")
        self._ensure_declared(plugin, capability, aggregate=True)
        name = require_non_empty_string(name, "contribution name")
        if "/" not in name:
            raise ValueError("contribution name must be namespaced")
        entries = self._contributions[capability]
        if name in entries:
            raise ValueError(f"contribution {capability!r}/{name!r} already exists")
        entries[name] = Contribution(plugin, capability, name, value)
        return RegistrationHandle(
            lambda: self._remove_contribution(plugin, capability, name)
        )

    def _ensure_declared(
        self, plugin: str, capability: str, *, aggregate: bool
    ) -> None:
        """拒绝登记插件未在 provides（aggregate=False）或 contributes 中声明的 capability。

        Raises:
            ValueError: capability 未声明。
        """

        descriptor = next(item for item, _ in self._plugins if item.id == plugin)
        declared = descriptor.contributes if aggregate else descriptor.provides
        if capability not in declared:
            raise ValueError(
                f"plugin {plugin!r} did not declare capability {capability!r}"
            )

    def _ensure_fulfilled(self, descriptor: PluginDescriptor) -> None:
        """确认插件 setup 后兑现了声明的全部单例与聚合 capability。

        Raises:
            RuntimeError: 存在未提供的单例或没有任何贡献的聚合 capability。
        """

        actual = {
            capability
            for capability, (owner, _) in self._capabilities.items()
            if owner == descriptor.id
        }
        contributed = {
            capability
            for capability, entries in self._contributions.items()
            if any(item.plugin == descriptor.id for item in entries.values())
        }
        missing = (set(descriptor.provides) - actual) | (
            set(descriptor.contributes) - contributed
        )
        if missing:
            raise RuntimeError(
                f"plugin {descriptor.id!r} did not provide declared capabilities: "
                f"{sorted(missing)!r}"
            )

    def _remove_capability(self, plugin: str, capability: str) -> None:
        """撤销插件自己登记的单例 capability；已被替换为其他提供者时不产生作用。"""

        self._ensure_detachable()
        if self._capabilities.get(capability, (None,))[0] == plugin:
            del self._capabilities[capability]

    def _remove_contribution(self, plugin: str, capability: str, name: str) -> None:
        """撤销插件自己登记的具名贡献；已被替换为其他插件的贡献时不产生作用。"""

        self._ensure_detachable()
        entry = self._contributions.get(capability, {}).get(name)
        if entry is not None and entry.plugin == plugin:
            del self._contributions[capability][name]

    def _ensure_registering(self) -> None:
        """只在 setup 阶段允许新注册。

        Raises:
            RuntimeError: 宿主已冻结或已关闭。
        """

        if self._frozen or self._closed:
            raise RuntimeError("plugin registrations are frozen")

    def _ensure_detachable(self) -> None:
        """允许 setup 阶段与关闭/回滚期间撤销注册，拒绝运行期间撤销。

        Raises:
            RuntimeError: 宿主已冻结且尚未开始关闭或回滚。
        """

        if self._frozen and not self._closed:
            raise RuntimeError("plugin registrations are frozen while the host runs")

    def _stop_started(self) -> BaseException | None:
        """逆序注销标准贡献，再逆序停止已进入 setup 的插件；单项失败不跳过后续清理。

        Returns:
            首个清理失败，全部成功时为 None；其余失败已写入 ERROR 日志。
        """

        failure: BaseException | None = None

        def record(exc: BaseException) -> None:
            """保留首个清理失败，其余失败写入日志。"""

            nonlocal failure
            if failure is None:
                failure = exc
            else:
                _logger.error("plugin host cleanup failed", exc_info=exc)

        for detach in reversed(self._contribution_detachers):
            try:
                detach()
            except BaseException as exc:
                record(exc)
        self._contribution_detachers.clear()
        for descriptor, plugin in reversed(self._started):
            try:
                plugin.stop(self._contexts[descriptor.id])
            except BaseException as exc:
                record(exc)
        self._started.clear()
        return failure

    def _install_runtime_contributions(self) -> None:
        """通过最窄的公开角色协议安装标准贡献，默认角色与替代角色走同一路径。

        selector 需要 Worker 实现 PolicyHost，Hook 需要 HookHost，Observer 需要 Router 与 Worker 都实现
        ObservationHost；没有标准贡献时不要求任何角色。

        Raises:
            LookupError: 标准贡献需要的角色 capability 缺失。
            TypeError: 角色不满足所需协议，或贡献值不符合角色契约。
        """

        selectors = self.contributions(CAP_INPUT_SELECTOR)
        hooks = self.contributions(CAP_NODE_HOOK)
        observers = self.contributions(CAP_RUNTIME_OBSERVER)
        if selectors:
            policies = self.require(CAP_GRAPH_WORKER)
            if not isinstance(policies, PolicyHost):
                raise TypeError("worker must implement PolicyHost")
            for contribution in selectors:
                self._contribution_detachers.append(
                    policies.register_policy(contribution.name, contribution.value)
                )
        if hooks:
            hook_host = self.require(CAP_GRAPH_WORKER)
            if not isinstance(hook_host, HookHost):
                raise TypeError("worker must implement HookHost")
            for contribution in hooks:
                value = contribution.value
                configured = (
                    value
                    if isinstance(value, NodeHookContribution)
                    else NodeHookContribution(value)
                )
                self._contribution_detachers.append(
                    hook_host.contribute_hook(
                        configured.hook,
                        phase=configured.phase,
                        graph=configured.graph,
                        node=configured.node,
                    )
                )
        if observers:
            roles: list[ObservationHost] = []
            for capability, label in (
                (CAP_EVENT_ROUTER, "router"),
                (CAP_GRAPH_WORKER, "worker"),
            ):
                role = self.require(capability)
                if not isinstance(role, ObservationHost):
                    raise TypeError(f"{label} must implement ObservationHost")
                roles.append(role)
            for contribution in observers:
                for role in roles:
                    handle = role.observe_runtime(contribution.value)
                    self._contribution_detachers.append(handle.detach)


def _collect(plugins: Iterable[Plugin]) -> dict[str, tuple[PluginDescriptor, Plugin]]:
    """按插件标识收集插件，校验声明类型、生命周期方法、唯一性与 SPI 版本。

    Raises:
        TypeError: plugins 不可迭代，或插件缺少 PluginDescriptor 与生命周期方法。
        ValueError: 插件标识重复或 SPI 版本与宿主不一致。
    """

    try:
        values = tuple(plugins)
    except TypeError as exc:
        raise TypeError("plugins must be iterable") from exc
    candidates: dict[str, tuple[PluginDescriptor, Plugin]] = {}
    for plugin in values:
        descriptor = getattr(plugin, "descriptor", None)
        if not isinstance(descriptor, PluginDescriptor):
            raise TypeError("plugin must define a PluginDescriptor")
        for method in ("setup", "start", "stop"):
            if not callable(getattr(plugin, method, None)):
                raise TypeError(f"plugin must define {method}()")
        if descriptor.id in candidates:
            raise ValueError(f"duplicate plugin {descriptor.id!r}")
        if descriptor.api_version != PluginHost.API_VERSION:
            raise ValueError(
                f"plugin {descriptor.id!r} requires API {descriptor.api_version}, "
                f"host provides {PluginHost.API_VERSION}"
            )
        candidates[descriptor.id] = (descriptor, plugin)
    return candidates


def _index_providers(
    candidates: dict[str, tuple[PluginDescriptor, Plugin]],
) -> dict[str, str]:
    """建立单例 capability 到提供者标识的索引，并校验冲突与缺失的必需 capability。

    Returns:
        单例 capability 名称到提供者插件标识的映射。

    Raises:
        ValueError: capability 同时作为单例与聚合、被多个插件提供，或必需 capability 无人提供。
    """

    aggregates = {
        capability
        for descriptor, _ in candidates.values()
        for capability in descriptor.contributes
    }
    providers: dict[str, str] = {}
    for descriptor, _ in candidates.values():
        for capability in descriptor.provides:
            if capability in aggregates:
                raise ValueError(
                    f"capability {capability!r} cannot be both singleton and aggregate"
                )
            existing = providers.get(capability)
            if existing is not None:
                raise ValueError(
                    f"capability {capability!r} is already provided by "
                    f"{existing!r}; {descriptor.id!r} also declares it"
                )
            providers[capability] = descriptor.id
    for descriptor, _ in candidates.values():
        for capability in descriptor.requires_capabilities:
            if capability not in providers:
                raise ValueError(
                    f"missing required capability {capability!r} "
                    f"for plugin {descriptor.id!r}"
                )
    return providers


def _order(
    candidates: dict[str, tuple[PluginDescriptor, Plugin]],
    providers: dict[str, str],
) -> tuple[tuple[PluginDescriptor, Plugin], ...]:
    """按插件依赖与 capability 提供者拓扑排序，同层保持传入顺序。

    Raises:
        ValueError: 依赖的插件缺失或存在循环依赖。
    """

    ordered: list[tuple[PluginDescriptor, Plugin]] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(plugin_id: str) -> None:
        """深度优先访问插件依赖，先追加依赖再追加自身。"""

        if plugin_id in visiting:
            raise ValueError(f"cyclic plugin dependency involving {plugin_id!r}")
        if plugin_id in visited:
            return
        try:
            descriptor, plugin = candidates[plugin_id]
        except KeyError as exc:
            raise ValueError(f"missing required plugin {plugin_id!r}") from exc
        visiting.add(plugin_id)
        for required in descriptor.requires:
            visit(required)
        for capability in descriptor.requires_capabilities:
            visit(providers[capability])
        visiting.remove(plugin_id)
        visited.add(plugin_id)
        ordered.append((descriptor, plugin))

    for plugin_id in candidates:
        visit(plugin_id)
    return tuple(ordered)


CAP_EVENT_BUS = "interlace.runtime/event-bus"
CAP_TASK_BACKEND = "interlace.runtime/task-backend"
CAP_GRAPH_EXECUTOR = "interlace.runtime/graph-executor"
CAP_EXECUTION_FACTORY = "interlace.runtime/execution-factory"
CAP_EVENT_ROUTER = "interlace.runtime/event-router"
CAP_GRAPH_WORKER = "interlace.runtime/graph-worker"
CAP_INPUT_SELECTOR = "interlace.contribution/input-selector"
CAP_NODE_HOOK = "interlace.contribution/node-hook"
CAP_RUNTIME_OBSERVER = "interlace.contribution/runtime-observer"


class ContributionPlugin:
    """把常见 Policy、Hook 和 Observer 组合成一个声明式插件。

    Attributes:
        _selectors: 贡献名称到 InputSelector 的映射副本。
        _hooks: 贡献名称到 NodeHookContribution、NodeHook 或单阶段回调的映射副本。
        _observers: 贡献名称到 RuntimeObserver 的映射副本。
        descriptor: 按非空贡献类别生成的插件声明。
    """

    def __init__(
        self,
        plugin_id: str,
        *,
        version: str = "1.0.0",
        requires: tuple[str, ...] = (),
        selectors: Mapping[str, InputSelector] | None = None,
        hooks: Mapping[str, NodeHookContribution | NodeHook | Callable[..., object]]
        | None = None,
        observers: Mapping[str, RuntimeObserver] | None = None,
    ) -> None:
        """复制待贡献的选择器、Hook 和观察者，并据此生成插件声明。

        Args:
            plugin_id: 带命名空间的插件标识。
            version: 插件自身的简单语义化版本。
            requires: 必须先装配的插件标识。
            selectors: 命名空间名称到 InputSelector 的映射。
            hooks: 命名空间名称到 Hook 贡献的映射。
            observers: 命名空间名称到 RuntimeObserver 的映射。

        Raises:
            TypeError: requires 不是 tuple。
            ValueError: plugin_id、version 或 requires 不符合 PluginDescriptor 校验。
        """

        self._selectors = {} if selectors is None else dict(selectors)
        self._hooks = {} if hooks is None else dict(hooks)
        self._observers = {} if observers is None else dict(observers)
        contributes = tuple(
            capability
            for capability, entries in (
                (CAP_INPUT_SELECTOR, self._selectors),
                (CAP_NODE_HOOK, self._hooks),
                (CAP_RUNTIME_OBSERVER, self._observers),
            )
            if entries
        )
        self.descriptor = PluginDescriptor(
            plugin_id,
            version,
            requires=requires,
            contributes=contributes,
        )

    def setup(self, context: PluginContext) -> None:
        """向宿主登记全部选择器、Hook 和观察者贡献。

        Args:
            context: 绑定当前插件身份的宿主上下文。
        """

        for name, selector in self._selectors.items():
            context.contribute(CAP_INPUT_SELECTOR, name, selector)
        for name, hook in self._hooks.items():
            context.contribute(CAP_NODE_HOOK, name, hook)
        for name, observer in self._observers.items():
            context.contribute(CAP_RUNTIME_OBSERVER, name, observer)

    def start(self, context: PluginContext) -> None:
        """纯贡献插件没有需要启动的资源。

        Args:
            context: 绑定当前插件身份的宿主上下文。
        """

        del context

    def stop(self, context: PluginContext) -> None:
        """纯贡献插件没有需要释放的资源；贡献由宿主注销。

        Args:
            context: 绑定当前插件身份的宿主上下文。
        """

        del context
