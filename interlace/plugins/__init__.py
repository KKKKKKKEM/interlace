"""插件与统一扩展机制：两种钩子点、具名值与单例 capability 装饰链。

插件是 ``setup(api)``；``api`` 只有 on、provide、wrap、require、defer 五个动作。
核心钩子点位于子模块 `interlace.plugins.points`。
"""

from ..engine.extension import (
    HookBus,
    HookCaller,
    NoticePoint,
    Registration,
    WrapPoint,
)
from .host import (
    CAP_EVENT_BUS,
    CAP_EVENT_ROUTER,
    CAP_EXECUTION_FACTORY,
    CAP_GRAPH_EXECUTOR,
    CAP_GRAPH_WORKER,
    CAP_TASK_BACKEND,
    ExtensionAPI,
    Plugin,
    PluginDescriptor,
    PluginHost,
    plugin,
)

__all__ = [
    "CAP_EVENT_BUS",
    "CAP_EVENT_ROUTER",
    "CAP_EXECUTION_FACTORY",
    "CAP_GRAPH_EXECUTOR",
    "CAP_GRAPH_WORKER",
    "CAP_TASK_BACKEND",
    "ExtensionAPI",
    "HookBus",
    "HookCaller",
    "NoticePoint",
    "Plugin",
    "PluginDescriptor",
    "PluginHost",
    "Registration",
    "WrapPoint",
    "plugin",
]
