"""统一、受控的 Runtime 插件宿主与插件扩展接口。"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from threading import RLock
from typing import Any, Protocol

from .._validation import require_namespaced, require_non_empty_string
from ..engine.extension import AnyPoint, HookBus, Registration
from .settings import build_settings, check_settings_type

_VERSION = re.compile(r"^[0-9]+(?:\.[0-9]+){0,2}(?:[-+][A-Za-z0-9.-]+)?$")
_logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PluginDescriptor:
    """插件身份、依赖以及参与装配排序的单例 capability 声明。

    Attributes:
        id: 含 ``/`` 的命名空间插件标识。
        version: 插件自身的简单语义化版本，默认 ``"1.0.0"``。
        provides: 插件必须在 setup 中提供的单例 capability。
        wraps: 插件必须在 setup 中装饰的单例 capability；宿主保证提供者先于全部装饰者、全部装饰者先于其余依赖方。
        requires_capabilities: 必须由其他插件提供的单例 capability，提供者与装饰者先于本插件装配。
        api_version: 插件兼容的扩展 SPI 主版本字符串，必须与宿主一致。
        settings: 插件设置的冻结 dataclass 类型；宿主据此校验配置并交给插件，None 表示没有设置。
    """

    id: str
    version: str = "1.0.0"
    provides: tuple[str, ...] = ()
    wraps: tuple[str, ...] = ()
    requires_capabilities: tuple[str, ...] = ()
    api_version: str = "5"
    settings: type | None = None

    def __post_init__(self) -> None:
        """校验标识、版本格式和各 capability 声明之间的一致性。

        Raises:
            TypeError: 任一声明集合不是 tuple，元素不是字符串，或 settings 不是合法的设置类型。
            ValueError: 标识或 capability 缺少命名空间、版本格式不合法、声明重复，
                或同一 capability 在 provides、wraps、requires_capabilities 中出现多次。
        """

        require_namespaced(self.id, "plugin id")
        version = require_non_empty_string(self.version, "plugin version")
        # fullmatch 拒绝 `$` 允许的末尾换行。
        if not _VERSION.fullmatch(version):
            raise ValueError("plugin version must use a simple semantic version")
        api_version = require_non_empty_string(self.api_version, "plugin API version")
        if not api_version.isdigit() or int(api_version) < 1:
            raise ValueError("plugin API version must be a positive integer")
        for label, values in (
            ("required capability", self.requires_capabilities),
            ("provided capability", self.provides),
            ("wrapped capability", self.wraps),
        ):
            if not isinstance(values, tuple):
                raise TypeError(f"{label}s must be a tuple")
            normalized = tuple(require_namespaced(value, label) for value in values)
            if len(set(normalized)) != len(normalized):
                raise ValueError(f"duplicate {label}")
        declared = (*self.provides, *self.wraps, *self.requires_capabilities)
        if len(set(declared)) != len(declared):
            raise ValueError(
                "a capability can appear only once across provides, wraps "
                "and requires_capabilities"
            )
        if self.settings is not None:
            check_settings_type(self.settings)


class Plugin(Protocol):
    """只有声明与 ``setup(api)`` 的插件协议；函数插件与类插件能力相同。

    需要在关闭时释放的资源通过 ``api.defer`` 登记；需要在全部插件装配完成后执行的逻辑订阅
    RUNTIME_EVENT 的 ``runtime.started``。

    Attributes:
        descriptor: 插件身份、依赖和能力声明。
    """

    descriptor: PluginDescriptor

    def setup(self, api: ExtensionAPI) -> None:
        """登记钩子、具名值、清理函数与声明的 capability。

        Args:
            api: 绑定当前插件身份的扩展接口。
        """


class ExtensionAPI:
    """绑定单个插件身份的扩展接口：on、provide、wrap、require、defer 与只读的 settings。

    以插件身份进行的全部登记在关闭或启动回滚时由宿主统一注销，随后逆序执行 defer 登记的清理函数。
    运行期安装的插件卸载时同样注销其登记并执行其清理，此后扩展接口拒绝新的登记。

    Attributes:
        __slots__: 限制实例只保存下列字段。
        _host: 所属的插件宿主。
        _plugin: 绑定的插件标识，所有登记以该身份进行。
        _live: 插件是否仍处于安装状态；运行期安装的插件卸载或宿主关闭后为 False。
    """

    __slots__ = ("_host", "_live", "_plugin")

    def __init__(self, host: PluginHost, plugin: str) -> None:
        """把插件标识绑定到宿主。

        Args:
            host: 所属的插件宿主。
            plugin: 插件标识。
        """

        self._host = host
        self._plugin = plugin
        self._live = True

    @property
    def plugin(self) -> str:
        """返回绑定的插件标识。"""

        return self._plugin

    @property
    def hooks(self) -> HookBus:
        """返回所属 Runtime 的钩子总线，用于构造需要触发钩子点的组件。"""

        return self._host.hooks

    @property
    def settings(self) -> Any:
        """返回宿主为本插件校验后的设置实例；插件未声明 settings 时为 None。"""

        return self._host._settings.get(self._plugin)

    def on(
        self, point: AnyPoint, handler: Callable[..., Any], /, **scope: Any
    ) -> Registration:
        """以插件身份在钩子点登记处理函数；参数见 `HookBus.on`。

        Returns:
            撤销本次登记的句柄；宿主关闭时自动注销。

        Raises:
            RuntimeError: 插件已经卸载。
        """

        with self._registering():
            return self._host.hooks.on(point, handler, owner=self._plugin, **scope)

    def provide(self, name: str, value: Any) -> Registration:
        """以插件身份提供一项具名值。

        descriptor.provides 声明的名称是单例 capability：只能在 setup 期提供，参与装配排序并可被装饰。
        其他名称随时可以提供，读取方在使用时查找，例如 Graph 冻结时查找输入 selector。

        Args:
            name: 带命名空间的名称。
            value: 提供的值。

        Returns:
            撤销本次提供的句柄；宿主关闭时自动撤销。capability 只能在 setup 期或关闭期间撤销。

        Raises:
            RuntimeError: 在 setup 期之外提供已声明的 capability，或插件已经卸载。
            ValueError: 名称已被提供，或是其他插件声明的 capability。
        """

        with self._registering():
            return self._host._provide(self._plugin, name, value)

    def wrap(self, capability: str, decorator: Callable[[Any], Any]) -> None:
        """装饰一项已在 descriptor.wraps 声明的单例 capability。

        decorator 接收当前实现并返回替代实现；后装配的装饰者位于外层。

        Args:
            capability: 单例 capability 名称。
            decorator: ``decorator(inner) -> outer``，不得返回 None。

        Raises:
            RuntimeError: 宿主已结束 setup 阶段。
            TypeError: decorator 不可调用或返回 None。
            ValueError: capability 未在 wraps 中声明，或已被本插件装饰过。
        """

        self._host._wrap(self._plugin, capability, decorator)

    def require(self, name: str) -> Any:
        """取得具名值；capability 返回经全部装饰者装饰后的实现。

        Args:
            name: capability 或具名值名称。

        Returns:
            当前提供的值。

        Raises:
            LookupError: 名称尚未被提供。
        """

        return self._host.require(name)

    def defer(self, cleanup: Callable[[], object]) -> None:
        """登记关闭或启动回滚时执行的清理函数；全部插件的清理按登记的逆序执行。

        setup 中途失败时，失败前登记的清理同样执行，因此资源创建后应立即登记清理。

        Args:
            cleanup: 无参数的同步可调用对象，返回值被忽略。

        Raises:
            TypeError: cleanup 不可调用。
            RuntimeError: 宿主已经关闭，或插件已经卸载。
        """

        with self._registering():
            self._host._defer(self._plugin, cleanup)

    @contextmanager
    def _registering(self) -> Iterator[None]:
        """持有宿主锁并确认插件仍处于安装状态，使登记不与卸载交错。

        Raises:
            RuntimeError: 运行期安装的插件已经卸载。
        """

        with self._host._lock:
            if not self._live:
                raise RuntimeError(f"plugin {self._plugin!r} is uninstalled")
            yield


class _FunctionPlugin:
    """把 ``setup(api)`` 函数适配为插件。

    Attributes:
        descriptor: 插件声明。
        _setup: 被装饰的 setup 函数。
    """

    __slots__ = ("_setup", "descriptor")

    def __init__(
        self, descriptor: PluginDescriptor, setup: Callable[[ExtensionAPI], None]
    ) -> None:
        """保存声明与 setup 函数。"""

        self.descriptor = descriptor
        self._setup = setup

    def setup(self, api: ExtensionAPI) -> None:
        """调用被装饰的函数。

        Args:
            api: 绑定当前插件身份的扩展接口。
        """

        self._setup(api)

    def __repr__(self) -> str:
        """返回包含插件 ID 的表示。"""

        return f"plugin({self.descriptor.id!r})"


def plugin(
    id: str,
    *,
    version: str = "1.0.0",
    provides: tuple[str, ...] = (),
    wraps: tuple[str, ...] = (),
    requires_capabilities: tuple[str, ...] = (),
    settings: type | None = None,
) -> Callable[[Callable[[ExtensionAPI], None]], Plugin]:
    """把 ``setup(api)`` 函数声明为插件的装饰器。

    函数插件与类插件能力相同；需要在关闭时释放的资源通过 ``api.defer`` 登记。

    Args:
        id: 含 ``/`` 的命名空间插件标识。
        version: 插件自身的简单语义化版本。
        provides: setup 中必须提供的单例 capability。
        wraps: setup 中必须装饰的单例 capability。
        requires_capabilities: 必须由其他插件提供的单例 capability。
        settings: 插件设置的冻结 dataclass 类型；None 表示没有设置。

    Returns:
        接收 setup 函数并返回插件对象的装饰器。

    Raises:
        TypeError: 被装饰对象不可调用，或声明参数类型不符。
        ValueError: 声明参数不符合 PluginDescriptor 校验。
    """

    descriptor = PluginDescriptor(
        id,
        version,
        provides=provides,
        wraps=wraps,
        requires_capabilities=requires_capabilities,
        settings=settings,
    )

    def decorate(setup: Callable[[ExtensionAPI], None]) -> Plugin:
        """把 setup 函数包装为插件。"""

        if not callable(setup):
            raise TypeError("plugin setup must be callable")
        return _FunctionPlugin(descriptor, setup)

    return decorate


class PluginHost:
    """解析插件依赖，管理单例 capability、装饰链、钩子总线与插件清理。

    启动失败或关闭时先逆序注销各插件的钩子与具名值，再逆序执行 defer 登记的清理函数；单项失败不跳过后续清理。
    关闭时抛出首个失败，其余失败写入 ``interlace.plugins.host`` 的 ERROR 日志；启动回滚时重新抛出原始
    启动失败，清理失败全部写入日志。宿主运行期间可经 `install` 安装只登记钩子、具名值与清理函数的插件，
    并单独卸载。

    Attributes:
        API_VERSION: 宿主支持的扩展 SPI 主版本字符串。
        hooks: 本宿主所属 Runtime 唯一的钩子总线。
        _plugins: 按依赖顺序排列的 (声明, 插件) 对。
        _providers: 已声明的单例 capability 到提供者插件标识的映射。
        _capabilities: 单例 capability 名称到 (提供者标识, 经装饰后的实现) 的映射。
        _wrapped: 已完成装饰的 (插件标识, capability) 集合。
        _apis: 插件标识到其扩展接口的映射。
        _entered: 已进入 setup、需要逆序注销登记的插件标识，按 setup 顺序保存。
        _cleanups: defer 登记的 (插件标识, 清理函数)，按登记顺序保存，逆序执行。
        _frozen: setup 阶段是否已结束；结束后拒绝提供 capability 与 wrap。
        _closed: 是否已开始关闭或启动回滚；开始后允许撤销 capability，拒绝再次启动与 defer。
        _settings: 插件标识到已校验设置实例的映射，只含声明了 settings 的插件。
        _installed: 运行期安装插件的标识到 (声明, 扩展接口) 的映射，按安装顺序保存。
        _lock: 串行化安装、卸载、登记与关闭的可重入锁。
    """

    API_VERSION = "5"

    def __init__(
        self,
        plugins: Iterable[Plugin],
        *,
        hooks: HookBus | None = None,
        settings: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        """校验插件声明与设置，并按依赖关系确定装配顺序。

        Args:
            plugins: 待装配的插件。
            hooks: 所属 Runtime 的钩子总线；None 时新建。
            settings: 插件标识到该插件设置覆盖值的映射；未列出的插件使用默认设置。

        Raises:
            TypeError: plugins 不可迭代，插件缺少 PluginDescriptor 或 setup，hooks 不是 HookBus，
                settings 不是映射，或设置值类型不符。
            ValueError: 插件重复、SPI 版本不兼容、单例 capability 冲突、装饰或依赖的 capability 无人提供、
                依赖缺失、循环依赖，设置指向未知插件或未声明设置的插件，或设置值不合法。
        """

        if hooks is None:
            hooks = HookBus()
        elif not isinstance(hooks, HookBus):
            raise TypeError("hooks must be a HookBus")
        self.hooks = hooks
        candidates = _collect(plugins)
        self._providers = _index_providers(candidates)
        self._plugins = _order(candidates, self._providers)
        self._capabilities: dict[str, tuple[str, Any]] = {}
        self._wrapped: set[tuple[str, str]] = set()
        self._apis = {
            descriptor.id: ExtensionAPI(self, descriptor.id)
            for descriptor, _ in self._plugins
        }
        self._entered: list[str] = []
        self._cleanups: list[tuple[str, Callable[[], object]]] = []
        self._frozen = False
        self._closed = False
        if settings is None:
            settings = {}
        elif not isinstance(settings, Mapping):
            raise TypeError("plugin settings must be a mapping")
        unknown = set(settings) - set(self._apis)
        if unknown:
            raise ValueError(
                f"settings for unknown plugins {sorted(unknown, key=str)!r}"
            )
        self._settings: dict[str, Any] = {}
        for descriptor, _ in self._plugins:
            value = _settings_for(descriptor, settings.get(descriptor.id))
            if value is not None:
                self._settings[descriptor.id] = value
        self._installed: dict[str, tuple[PluginDescriptor, ExtensionAPI]] = {}
        self._lock = RLock()

    @property
    def descriptors(self) -> tuple[PluginDescriptor, ...]:
        """返回声明快照：启动插件按装配顺序在前，运行期安装的插件按安装顺序在后。"""

        with self._lock:
            installed = tuple(descriptor for descriptor, _ in self._installed.values())
        return (*(descriptor for descriptor, _ in self._plugins), *installed)

    def start(self) -> PluginHost:
        """依次 setup 全部插件并核对声明；已启动时直接返回。

        任一步骤失败时执行回滚，回滚中的清理失败写入日志，随后重新抛出原始失败，宿主进入关闭状态。

        Returns:
            当前宿主。

        Raises:
            RuntimeError: 宿主已经关闭，或插件未兑现声明的 provides 或 wraps。
            BaseException: 插件 setup 抛出的原始异常。
        """

        if self._closed:
            raise RuntimeError("plugin host is closed")
        if self._frozen:
            return self
        try:
            for descriptor, item in self._plugins:
                # setup 即使中途失败也可能已经登记钩子或清理函数，因此先纳入回滚范围。
                self._entered.append(descriptor.id)
                item.setup(self._apis[descriptor.id])
                self._ensure_fulfilled(descriptor)
            self._frozen = True
        except BaseException:
            self._frozen = True
            self._closed = True
            failure = self._cleanup()
            if failure is not None:
                _logger.error("plugin host rollback failed", exc_info=failure)
            raise
        return self

    def require(self, name: str) -> Any:
        """按名称取得具名值；capability 返回经全部装饰者装饰后的实现。

        Args:
            name: capability 或具名值名称。

        Returns:
            当前提供的值。

        Raises:
            LookupError: 名称尚未被提供。
        """

        name = require_non_empty_string(name, "provided name")
        if name in self._providers:
            try:
                return self._capabilities[name][1]
            except KeyError as exc:
                raise LookupError(
                    f"required capability {name!r} is unavailable"
                ) from exc
        return self.hooks.lookup(name)

    def close(self) -> None:
        """逆序注销插件登记并执行清理函数；重复调用不产生作用。

        Raises:
            BaseException: 清理中出现的首个失败；其余失败已写入日志。
        """

        with self._lock:
            if self._closed:
                return
            self._closed = True
            failure = self._cleanup()
        if failure is not None:
            raise failure

    def install(
        self, plugin: Plugin, *, settings: Mapping[str, Any] | None = None
    ) -> Registration:
        """在宿主运行期间安装插件，返回卸载该插件的句柄。

        运行期安装的插件只能登记钩子、具名值与清理函数，不能提供或装饰单例 capability；依赖的
        capability 必须已由启动插件提供。setup 失败时注销其已有登记并执行其清理，再重新抛出原始失败。
        卸载先注销该插件的登记，再逆序执行它登记的清理；宿主关闭时一并卸载。已发布的
        ``runtime.started`` 不会补发给运行期安装的插件。

        Args:
            plugin: 待安装的插件。
            settings: 插件设置的覆盖值；None 表示全部使用默认值。

        Returns:
            卸载插件的句柄；重复调用或宿主关闭后调用不产生作用，卸载清理的首个失败抛给调用方。

        Raises:
            TypeError: 插件缺少 PluginDescriptor 或 setup，或设置值类型不符。
            ValueError: SPI 版本不兼容，插件声明了 provides 或 wraps，标识已被占用，依赖的 capability
                无人提供，或设置不合法。
            RuntimeError: 宿主尚未启动或已经关闭。
            BaseException: 插件 setup 抛出的原始异常。
        """

        ((descriptor, item),) = _collect((plugin,)).values()
        if descriptor.provides or descriptor.wraps:
            raise ValueError(
                f"installed plugin {descriptor.id!r} cannot provide or wrap capabilities"
            )
        values = _settings_for(descriptor, settings)
        with self._lock:
            if self._closed:
                raise RuntimeError("plugin host is closed")
            if not self._frozen:
                raise RuntimeError("plugin host has not started")
            if descriptor.id in self._apis or descriptor.id in self._installed:
                raise ValueError(f"duplicate plugin {descriptor.id!r}")
            for capability in descriptor.requires_capabilities:
                if capability not in self._capabilities:
                    raise ValueError(
                        f"missing required capability {capability!r} "
                        f"for plugin {descriptor.id!r}"
                    )
            api = ExtensionAPI(self, descriptor.id)
            self._installed[descriptor.id] = (descriptor, api)
            if values is not None:
                self._settings[descriptor.id] = values
            try:
                item.setup(api)
            except BaseException:
                failure = self._release(descriptor.id)
                if failure is not None:
                    _logger.error("plugin install rollback failed", exc_info=failure)
                raise
        return Registration(partial(self._uninstall, descriptor.id, api))

    def _uninstall(self, plugin: str, api: ExtensionAPI) -> None:
        """卸载仍处于安装状态的同一个插件实例；宿主已关闭或该实例已卸载时不产生作用。

        Raises:
            BaseException: 卸载清理中的首个失败；其余失败已写入日志。
        """

        with self._lock:
            installed = self._installed.get(plugin)
            if self._closed or installed is None or installed[1] is not api:
                return
            failure = self._release(plugin)
        if failure is not None:
            raise failure

    def _release(self, plugin: str) -> BaseException | None:
        """注销运行期安装插件的登记并逆序执行其清理；调用方持有宿主锁。

        Returns:
            首个失败，全部成功时为 None；其余失败已写入 ERROR 日志。
        """

        _, api = self._installed.pop(plugin)
        api._live = False
        self._settings.pop(plugin, None)
        cleanups = [cleanup for owner, cleanup in self._cleanups if owner == plugin]
        self._cleanups = [entry for entry in self._cleanups if entry[0] != plugin]
        return _run_all(
            (partial(self.hooks.detach_owner, plugin), *reversed(cleanups)),
            "plugin uninstall failed",
        )

    def _provide(self, plugin: str, name: str, value: Any) -> Registration:
        """以插件身份提供具名值；自身声明的 capability 走 setup 期单例路径。

        Raises:
            RuntimeError: 在 setup 期之外提供已声明的 capability。
            ValueError: 名称已被提供，或是其他插件声明的 capability。
        """

        name = require_namespaced(name, "provided name")
        declarer = self._providers.get(name)
        if declarer is None:
            return self.hooks.provide(name, value, owner=plugin)
        if declarer != plugin:
            raise ValueError(f"capability {name!r} is declared by {declarer!r}")
        self._ensure_registering()
        if name in self._capabilities:
            raise ValueError(f"capability {name!r} is already provided")
        self._capabilities[name] = (plugin, value)
        return Registration(lambda: self._remove_capability(plugin, name), owner=plugin)

    def _wrap(
        self, plugin: str, capability: str, decorator: Callable[[Any], Any]
    ) -> None:
        """以插件身份装饰单例 capability 的当前实现。

        Raises:
            RuntimeError: 宿主已结束 setup 阶段。
            TypeError: decorator 不可调用或返回 None。
            ValueError: capability 未在 wraps 中声明，或本插件已装饰过。
        """

        self._ensure_registering()
        capability = require_non_empty_string(capability, "capability")
        if capability not in self._descriptor(plugin).wraps:
            raise ValueError(
                f"plugin {plugin!r} did not declare wrapping {capability!r}"
            )
        if (plugin, capability) in self._wrapped:
            raise ValueError(f"plugin {plugin!r} already wrapped {capability!r}")
        if not callable(decorator):
            raise TypeError("capability decorator must be callable")
        owner, inner = self._capabilities[capability]
        outer = decorator(inner)
        if outer is None:
            raise TypeError(f"decorator of {capability!r} returned None")
        self._capabilities[capability] = (owner, outer)
        self._wrapped.add((plugin, capability))

    def _descriptor(self, plugin: str) -> PluginDescriptor:
        """返回插件声明。"""

        return next(item for item, _ in self._plugins if item.id == plugin)

    def _ensure_fulfilled(self, descriptor: PluginDescriptor) -> None:
        """确认插件 setup 后兑现了声明的全部 provides 与 wraps。

        Raises:
            RuntimeError: 存在未提供或未装饰的 capability。
        """

        provided = {
            capability
            for capability, (owner, _) in self._capabilities.items()
            if owner == descriptor.id
        }
        wrapped = {
            capability for owner, capability in self._wrapped if owner == descriptor.id
        }
        missing = (set(descriptor.provides) - provided) | (
            set(descriptor.wraps) - wrapped
        )
        if missing:
            raise RuntimeError(
                f"plugin {descriptor.id!r} did not fulfil declared capabilities: "
                f"{sorted(missing)!r}"
            )

    def _remove_capability(self, plugin: str, capability: str) -> None:
        """撤销插件自己登记的单例 capability；已被替换为其他提供者时不产生作用。

        Raises:
            RuntimeError: 宿主已冻结且尚未开始关闭或回滚。
        """

        if self._frozen and not self._closed:
            raise RuntimeError("capabilities are frozen while the host runs")
        if self._capabilities.get(capability, (None,))[0] == plugin:
            del self._capabilities[capability]

    def _ensure_registering(self) -> None:
        """只在 setup 阶段允许提供 capability 与 wrap。

        Raises:
            RuntimeError: 宿主已冻结或已关闭。
        """

        if self._frozen or self._closed:
            raise RuntimeError("capability registrations are frozen")

    def _defer(self, plugin: str, cleanup: Callable[[], object]) -> None:
        """登记插件的清理函数。

        Raises:
            TypeError: cleanup 不可调用。
            RuntimeError: 宿主已经关闭。
        """

        if not callable(cleanup):
            raise TypeError("cleanup must be callable")
        with self._lock:
            if self._closed:
                raise RuntimeError("plugin host is closed")
            self._cleanups.append((plugin, cleanup))

    def _cleanup(self) -> BaseException | None:
        """逆序注销各插件的钩子与具名值，再逆序执行清理函数，最后清空 capability。

        运行期安装的插件晚于启动插件 setup，因此先于启动插件注销。

        Returns:
            首个清理失败，全部成功时为 None；其余失败已写入 ERROR 日志。
        """

        for _, api in self._installed.values():
            api._live = False
        owners = (*self._entered, *self._installed)
        cleanups = [cleanup for _, cleanup in reversed(self._cleanups)]
        self._cleanups.clear()
        failure = _run_all(
            (
                *(
                    partial(self.hooks.detach_owner, owner)
                    for owner in reversed(owners)
                ),
                *cleanups,
            ),
            "plugin host cleanup failed",
        )
        self._entered.clear()
        self._installed.clear()
        self._capabilities.clear()
        self._wrapped.clear()
        return failure


def _settings_for(
    descriptor: PluginDescriptor, values: Mapping[str, Any] | None
) -> Any:
    """按插件声明构造设置实例；未声明设置的插件返回 None。

    Raises:
        TypeError: 设置值类型不符。
        ValueError: 插件未声明设置却收到设置，或设置值不合法。
    """

    if descriptor.settings is None:
        if values is not None:
            raise ValueError(f"plugin {descriptor.id!r} declares no settings")
        return None
    return build_settings(descriptor.settings, values)


def _run_all(
    actions: Iterable[Callable[[], object]], message: str
) -> BaseException | None:
    """依次执行全部动作，单项失败不跳过后续动作。

    Returns:
        首个失败，全部成功时为 None；其余失败以 message 写入 ERROR 日志。
    """

    failure: BaseException | None = None
    for action in actions:
        try:
            action()
        except BaseException as exc:
            if failure is None:
                failure = exc
            else:
                _logger.error(message, exc_info=exc)
    return failure


def _collect(plugins: Iterable[Plugin]) -> dict[str, tuple[PluginDescriptor, Plugin]]:
    """按插件标识收集插件，校验声明类型、setup、唯一性与 SPI 版本。

    Raises:
        TypeError: plugins 不可迭代，插件缺少 PluginDescriptor 或 setup。
        ValueError: 插件标识重复或 SPI 版本与宿主不一致。
    """

    try:
        values = tuple(plugins)
    except TypeError as exc:
        raise TypeError("plugins must be iterable") from exc
    candidates: dict[str, tuple[PluginDescriptor, Plugin]] = {}
    for item in values:
        descriptor = getattr(item, "descriptor", None)
        if not isinstance(descriptor, PluginDescriptor):
            raise TypeError("plugin must define a PluginDescriptor")
        if not callable(getattr(item, "setup", None)):
            raise TypeError("plugin must define setup()")
        if descriptor.id in candidates:
            raise ValueError(f"duplicate plugin {descriptor.id!r}")
        if descriptor.api_version != PluginHost.API_VERSION:
            raise ValueError(
                f"plugin {descriptor.id!r} requires API {descriptor.api_version}, "
                f"host provides {PluginHost.API_VERSION}"
            )
        candidates[descriptor.id] = (descriptor, item)
    return candidates


def _index_providers(
    candidates: dict[str, tuple[PluginDescriptor, Plugin]],
) -> dict[str, str]:
    """建立单例 capability 到提供者标识的索引，并校验冲突与缺失。

    Returns:
        单例 capability 名称到提供者插件标识的映射。

    Raises:
        ValueError: capability 被多个插件提供，或装饰、依赖的 capability 无人提供。
    """

    providers: dict[str, str] = {}
    for descriptor, _ in candidates.values():
        for capability in descriptor.provides:
            existing = providers.get(capability)
            if existing is not None:
                raise ValueError(
                    f"capability {capability!r} is already provided by "
                    f"{existing!r}; {descriptor.id!r} also declares it"
                )
            providers[capability] = descriptor.id
    for descriptor, _ in candidates.values():
        for label, capabilities in (
            ("required", descriptor.requires_capabilities),
            ("wrapped", descriptor.wraps),
        ):
            for capability in capabilities:
                if capability not in providers:
                    raise ValueError(
                        f"missing {label} capability {capability!r} "
                        f"for plugin {descriptor.id!r}"
                    )
    return providers


def _order(
    candidates: dict[str, tuple[PluginDescriptor, Plugin]],
    providers: dict[str, str],
) -> tuple[tuple[PluginDescriptor, Plugin], ...]:
    """按 capability 提供者、装饰者与依赖方拓扑排序，同层保持传入顺序。

    装饰者排在提供者之后；依赖某 capability 的插件排在其提供者与全部装饰者之后。

    Raises:
        ValueError: 存在循环的 capability 依赖。
    """

    wrappers: dict[str, list[str]] = {}
    for descriptor, _ in candidates.values():
        for capability in descriptor.wraps:
            wrappers.setdefault(capability, []).append(descriptor.id)
    ordered: list[tuple[PluginDescriptor, Plugin]] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(plugin_id: str) -> None:
        """深度优先访问插件依赖，先追加依赖再追加自身。"""

        if plugin_id in visiting:
            raise ValueError(f"cyclic plugin dependency involving {plugin_id!r}")
        if plugin_id in visited:
            return
        descriptor, item = candidates[plugin_id]
        visiting.add(plugin_id)
        for capability in descriptor.wraps:
            visit(providers[capability])
        for capability in descriptor.requires_capabilities:
            visit(providers[capability])
            for wrapper in wrappers.get(capability, ()):
                visit(wrapper)
        visiting.remove(plugin_id)
        visited.add(plugin_id)
        ordered.append((descriptor, item))

    for plugin_id in candidates:
        visit(plugin_id)
    return tuple(ordered)


CAP_EVENT_BUS = "interlace.runtime/event-bus"
CAP_TASK_BACKEND = "interlace.runtime/task-backend"
CAP_GRAPH_EXECUTOR = "interlace.runtime/graph-executor"
CAP_EXECUTION_FACTORY = "interlace.runtime/execution-factory"
CAP_EVENT_ROUTER = "interlace.runtime/event-router"
CAP_GRAPH_WORKER = "interlace.runtime/graph-worker"
