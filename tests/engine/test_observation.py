"""运行时观察者注册身份和卸载隔离的契约测试。"""

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
