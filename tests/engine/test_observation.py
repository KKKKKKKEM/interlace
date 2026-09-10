"""运行时观察者注册身份和卸载隔离的契约测试。"""

import pytest

from interlace import Runtime
from interlace.engine.observation import (
    ObservationHub,
    RuntimeEvent,
    RuntimeEventKind,
)


def test_duplicate_observer_registrations_detach_independently() -> None:
    """同一回调的重复注册各自交付，卸载其中一个不影响其他注册。"""

    hub = ObservationHub()
    events: list[RuntimeEvent] = []
    observer = events.append
    first = hub.attach(observer)
    second = hub.attach(observer)
    event = RuntimeEvent(RuntimeEventKind.NODE_STARTED)

    hub.publish(event)
    assert events == [event, event]
    first.detach()
    hub.publish(event)
    assert events == [event, event, event]
    first.detach()
    hub.publish(event)
    assert events == [event, event, event, event]
    second.detach()
    hub.publish(event)
    assert len(events) == 4


def test_old_observer_handle_does_not_detach_later_registration() -> None:
    """已卸载句柄再次使用时，不移除同一回调后来建立的新注册。"""

    hub = ObservationHub()
    events: list[RuntimeEvent] = []
    observer = events.append
    old = hub.attach(observer)
    old.detach()
    hub.attach(observer)
    old.detach()
    event = RuntimeEvent(RuntimeEventKind.NODE_FINISHED)

    hub.publish(event)
    assert events == [event]


def test_runtime_observer_rolls_back_router_if_worker_registration_fails() -> None:
    """第二个角色拒绝观察者时必须撤销第一个角色的注册。"""

    runtime = Runtime()
    try:
        original = runtime.worker.observe_runtime

        def reject(observer):
            """拒绝测试观察者注册。

            Args:
                observer: 待注册的观察者。

            Raises:
                RuntimeError: 模拟替换角色注册失败。
            """

            del observer
            raise RuntimeError("worker registration failed")

        runtime.worker.observe_runtime = reject
        with pytest.raises(RuntimeError, match="worker registration failed"):
            runtime.observe_runtime(lambda event: None)
        assert runtime.router._observations._observers == ()
        runtime.worker.observe_runtime = original
    finally:
        runtime.close()
