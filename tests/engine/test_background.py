"""Background 后台 Notice 投递的契约测试。"""

from __future__ import annotations

import logging
from threading import Event as ThreadEvent
from typing import Any

import pytest

from interlace import Runtime
from interlace.plugins import Background, HookBus, NoticePoint, plugin

NOTICE: NoticePoint[int] = NoticePoint("test/background")


def test_trigger_does_not_wait_and_overflow_is_dropped_and_counted() -> None:
    """触发方不等待慢速处理函数；队列满时丢弃并计数，关闭时按入队顺序排空已接受的值。"""

    release = ThreadEvent()
    started = ThreadEvent()
    seen: list[int] = []

    def slow(value: int) -> None:
        """首个值阻塞到测试放行。"""

        if not seen:
            started.set()
            release.wait(5)
        seen.append(value)

    observer = Background(slow, maxsize=2)
    bus = HookBus()
    bus.on(NOTICE, observer)
    bus.notify(NOTICE, 0)
    assert started.wait(5)
    for value in (1, 2, 3, 4):
        bus.notify(NOTICE, value)
    assert observer.dropped == 2
    release.set()
    observer.close()
    assert seen == [0, 1, 2]

    bus.notify(NOTICE, 5)
    observer.close()
    assert seen == [0, 1, 2]
    assert observer.dropped == 3


def test_handler_failures_are_logged_and_do_not_stop_delivery(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """处理函数失败与返回 awaitable 都写入日志，后续值继续处理。"""

    seen: list[int] = []

    async def coroutine() -> None:
        """不应被等待的协程。"""

    def handler(value: int) -> Any:
        """按值失败、返回协程或记录。"""

        if value == 0:
            raise RuntimeError("boom")
        if value == 1:
            return coroutine()
        seen.append(value)
        return None

    observer = Background(handler)
    with caplog.at_level(logging.ERROR, logger="interlace.plugins.background"):
        for value in (0, 1, 2):
            observer(value)
        observer.close()

    assert seen == [2]
    assert [
        str(record.exc_info[1]) for record in caplog.records if record.exc_info
    ] == [
        "boom",
        "background handler returned an awaitable",
    ]


def test_plugin_defers_close_so_uninstall_drains_pending_values() -> None:
    """插件以 defer 登记 close，卸载时先注销登记再排空后台队列。"""

    seen: list[int] = []

    @plugin("example/background")
    def observing(api: Any) -> None:
        """登记后台观察者。"""

        observer = Background(seen.append)
        api.defer(observer.close)
        api.on(NOTICE, observer)

    with Runtime() as runtime:
        handle = runtime.plugin_host.install(observing)
        for value in range(100):
            runtime.hooks.notify(NOTICE, value)
        handle()
        assert seen == list(range(100))
        runtime.hooks.notify(NOTICE, 100)
    assert seen == list(range(100))


@pytest.mark.parametrize(
    ("handler", "maxsize", "error"),
    [
        (None, 1, TypeError),
        (lambda value: None, True, TypeError),
        (lambda value: None, 0, ValueError),
    ],
)
def test_background_rejects_invalid_arguments(
    handler: Any, maxsize: Any, error: type[Exception]
) -> None:
    """不可调用、协程函数与非正整数容量在构造时拒绝。"""

    with pytest.raises(error):
        Background(handler, maxsize=maxsize)


def test_background_rejects_coroutine_functions() -> None:
    """协程函数不能作为后台处理函数。"""

    async def handler(value: int) -> None:
        """异步处理函数。"""

    with pytest.raises(TypeError, match="synchronous"):
        Background(handler)
