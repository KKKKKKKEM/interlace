"""Engine 层的核心钩子点。"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .extension import NoticePoint, WrapPoint
from .observation import RuntimeEvent

if TYPE_CHECKING:
    from .hooks import NodeCall, NodeInspection, Outputs

NODE_CALL: WrapPoint[NodeCall, Outputs] = WrapPoint(
    "interlace.node/call", scope=("graph", "node")
)
"""包裹一次 Node 调用的流式中间件。

处理函数签名为 ``fn(call, call_next) -> Outputs``：call_next(call) 返回内层输出的迭代器；处理函数可返回
Output、Iterable[Output]（含生成器）或 None。不调用 call_next 即短路；在迭代 call_next 结果时捕获业务异常
即恢复；逐项产出即改写输出；抛出 StopGraph 停止 Graph。传给 call_next 的 NodeCall 只能通过
``with_inputs`` 替换输入。执行控制异常与 Engine 契约错误不能被吞掉。处理函数必须同步，
需要等待 awaitable 时使用 ``call.wait()``。
"""

NODE_INSPECT: NoticePoint[NodeInspection] = NoticePoint(
    "interlace.node/inspect", scope=("graph", "node")
)
"""实际 Node 调用边界的同步采集：输入在全部中间件之后，原始输出在中间件改写之前；失败被隔离。"""

RUNTIME_EVENT: NoticePoint[RuntimeEvent] = NoticePoint(
    "interlace.runtime/event", scope=("kind", "graph")
)
"""只读生命周期事实，包括 Runtime 启动与关闭；scope 的 kind 取 RuntimeEventKind，graph 为关联 Graph 名或 None。"""

__all__ = ["NODE_CALL", "NODE_INSPECT", "RUNTIME_EVENT"]
