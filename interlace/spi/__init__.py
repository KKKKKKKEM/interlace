"""Runtime 可独立替换和组合的窄角色协议。"""

from .runtime import (
    Delivery,
    DeliveryOutcome,
    DeliveryResult,
    Emit,
    EventBus,
    EventHandler,
    ExecutionFactory,
    GraphExecutor,
    HookableGraphExecutor,
    LocalTaskPublisher,
    TaskBackend,
    TaskConsumer,
    TaskPublisher,
    Work,
    WorkHandler,
)
from .roles import (
    ExecutionHost,
    GraphCatalog,
    HookHost,
    ObservationHost,
    PolicyHost,
    RouterRole,
    WorkerRole,
)
from ..engine.execution_resources import ExecutionNotifier, OutputStore
from ..engine.slots import SlotLease, SlotProvider
from ..engine.events import ContextFactory

__all__ = [
    "ContextFactory",
    "Delivery",
    "DeliveryOutcome",
    "DeliveryResult",
    "Emit",
    "EventBus",
    "EventHandler",
    "ExecutionFactory",
    "ExecutionHost",
    "ExecutionNotifier",
    "GraphCatalog",
    "GraphExecutor",
    "HookHost",
    "HookableGraphExecutor",
    "LocalTaskPublisher",
    "ObservationHost",
    "OutputStore",
    "PolicyHost",
    "RouterRole",
    "SlotLease",
    "SlotProvider",
    "TaskBackend",
    "TaskConsumer",
    "TaskPublisher",
    "Work",
    "WorkHandler",
    "WorkerRole",
]
