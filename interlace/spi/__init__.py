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
    RouterRole,
    WorkerRole,
)
from ..engine.events import ContextFactory
from ..engine.execution import ExecutionLimits
from ..engine.execution_resources import ExecutionNotifier, OutputStore
from ..engine.policies import InputSelector
from ..engine.slots import SlotLease, SlotProvider

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
    "ExecutionLimits",
    "ExecutionNotifier",
    "GraphCatalog",
    "GraphExecutor",
    "InputSelector",
    "LocalTaskPublisher",
    "OutputStore",
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
