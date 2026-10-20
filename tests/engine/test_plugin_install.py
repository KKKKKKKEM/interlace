"""运行期安装与卸载插件的契约测试。"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from interlace import Runtime
from interlace.plugins import CAP_EVENT_BUS, NoticePoint, PluginHost, plugin

TRACE: NoticePoint[str] = NoticePoint("example/trace")


def tracer(
    plugin_id: str,
    seen: list[str],
    events: list[str],
    apis: list[Any] | None = None,
) -> Any:
    """构造登记 TRACE、具名值与两项清理的插件。

    Args:
        plugin_id: 插件标识。
        seen: TRACE 处理函数追加通知值的列表。
        events: 追加 ``setup``/``cleanup`` 记录的列表。
        apis: 追加 setup 收到的扩展接口的列表；None 时不保留。

    Returns:
        函数插件；具名值 ``<plugin_id>-value`` 是本插件对象第几次 setup。
    """

    if apis is None:
        apis = []

    @plugin(plugin_id)
    def setup(api: Any) -> None:
        """登记钩子、具名值与先后两项清理。"""

        apis.append(api)
        events.append(f"setup:{plugin_id}")
        api.on(TRACE, lambda value: seen.append(f"{plugin_id}:{value}"))
        api.provide(f"{plugin_id}-value", len(apis))
        api.defer(lambda: events.append(f"cleanup-first:{plugin_id}"))
        api.defer(lambda: events.append(f"cleanup-second:{plugin_id}"))

    return setup


def test_uninstall_withdraws_registrations_and_runs_own_cleanups_in_reverse() -> None:
    """卸载只撤销该插件的登记与清理，扩展接口随即拒绝新的登记，宿主其余部分照常运行。"""

    seen: list[str] = []
    events: list[str] = []
    with Runtime() as runtime:
        apis: list[Any] = []
        installed = tracer("example/installed", seen, events, apis)
        other = tracer("example/other", seen, events)
        handle = runtime.plugin_host.install(installed)
        runtime.plugin_host.install(other)
        assert [d.id for d in runtime.plugin_host.descriptors][-2:] == [
            "example/installed",
            "example/other",
        ]

        runtime.hooks.notify(TRACE, "before")
        handle()
        runtime.hooks.notify(TRACE, "after")

        assert seen == [
            "example/installed:before",
            "example/other:before",
            "example/other:after",
        ]
        assert events[-2:] == [
            "cleanup-second:example/installed",
            "cleanup-first:example/installed",
        ]
        with pytest.raises(LookupError):
            runtime.hooks.lookup("example/installed-value")
        assert runtime.hooks.lookup("example/other-value") == 1
        assert "example/installed" not in [
            d.id for d in runtime.plugin_host.descriptors
        ]
        with pytest.raises(RuntimeError, match="uninstalled"):
            apis[0].on(TRACE, seen.append)
        with pytest.raises(RuntimeError, match="uninstalled"):
            apis[0].defer(lambda: None)


def test_stale_handle_does_not_uninstall_a_reinstalled_plugin() -> None:
    """同一标识重新安装后，旧句柄再次调用不影响新实例。"""

    seen: list[str] = []
    events: list[str] = []
    with Runtime() as runtime:
        item = tracer("example/again", seen, events)
        first = runtime.plugin_host.install(item)
        first()
        runtime.plugin_host.install(item)
        first()
        runtime.hooks.notify(TRACE, "alive")

    assert seen == ["example/again:alive"]
    assert events.count("cleanup-first:example/again") == 2


def test_close_uninstalls_installed_plugins_before_startup_plugins() -> None:
    """宿主关闭时运行期安装的插件一并卸载，其清理先于更早登记的启动插件清理执行。"""

    seen: list[str] = []
    events: list[str] = []
    runtime = Runtime(plugins=[tracer("example/startup", seen, events)])
    handle = runtime.plugin_host.install(tracer("example/late", seen, events))
    runtime.close()
    handle()

    assert events[-4:] == [
        "cleanup-second:example/late",
        "cleanup-first:example/late",
        "cleanup-second:example/startup",
        "cleanup-first:example/startup",
    ]
    with pytest.raises(RuntimeError, match="closed"):
        runtime.plugin_host.install(tracer("example/after", seen, events))


def test_failed_install_rolls_back_and_keeps_the_id_available() -> None:
    """setup 失败时注销已有登记、执行已登记清理并重新抛出原始异常，标识仍可再次安装。"""

    events: list[str] = []

    @plugin("example/broken")
    def broken(api: Any) -> None:
        """登记后失败。"""

        api.provide("example/broken-value", 1)
        api.defer(lambda: events.append("cleanup"))
        raise KeyError("setup failed")

    with Runtime() as runtime:
        with pytest.raises(KeyError, match="setup failed"):
            runtime.plugin_host.install(broken)
        assert events == ["cleanup"]
        with pytest.raises(LookupError):
            runtime.hooks.lookup("example/broken-value")
        runtime.plugin_host.install(tracer("example/broken", [], events))


def test_install_rejects_capability_declarations_conflicts_and_missing_capabilities() -> (
    None
):
    """运行期插件不能参与单例 capability 装配，不得占用已有标识，依赖必须已被提供。"""

    noop: Any = lambda api: None  # noqa: E731
    with Runtime() as runtime:
        host = runtime.plugin_host
        with pytest.raises(ValueError, match="cannot provide or wrap"):
            host.install(plugin("example/p", provides=("example/cap",))(noop))
        with pytest.raises(ValueError, match="cannot provide or wrap"):
            host.install(plugin("example/w", wraps=(CAP_EVENT_BUS,))(noop))
        with pytest.raises(ValueError, match="duplicate plugin"):
            host.install(plugin("interlace.core/local-runtime")(noop))
        host.install(plugin("example/once")(noop))
        with pytest.raises(ValueError, match="duplicate plugin"):
            host.install(plugin("example/once")(noop))
        with pytest.raises(ValueError, match="missing required capability"):
            host.install(
                plugin("example/needs", requires_capabilities=("example/absent",))(noop)
            )
        host.install(
            plugin("example/uses-bus", requires_capabilities=(CAP_EVENT_BUS,))(noop)
        )

    with pytest.raises(RuntimeError, match="not started"):
        PluginHost(()).install(plugin("example/early")(noop))


def test_uninstall_raises_first_cleanup_failure_and_logs_the_rest(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """卸载执行全部清理，把首个失败抛给句柄调用方，其余失败写入日志。"""

    @plugin("example/failing")
    def failing(api: Any) -> None:
        """登记两项会失败的清理。"""

        api.defer(lambda: (_ for _ in ()).throw(RuntimeError("first registered")))
        api.defer(lambda: (_ for _ in ()).throw(RuntimeError("last registered")))

    with Runtime() as runtime:
        handle = runtime.plugin_host.install(failing)
        with (
            caplog.at_level(logging.ERROR, logger="interlace.plugins.host"),
            pytest.raises(RuntimeError, match="last registered"),
        ):
            handle()

    assert [
        record.exc_info[1].args[0] for record in caplog.records if record.exc_info
    ] == ["first registered"]
