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
    SlotLease,
    SlotProvider,
    TaskBackend,
    TaskConsumer,
    TaskPublisher,
    Work,
    WorkHandler,
)
from .roles import GraphCatalog, RouterRole, WorkerRole
from ..engine.execution_resources import ExecutionNotifier, OutputStore
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
    "ExecutionNotifier",
    "GraphExecutor",
    "HookableGraphExecutor",
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
    "GraphCatalog",
]
