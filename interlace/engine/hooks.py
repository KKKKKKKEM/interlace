"""Node 调用中间件使用的调用参数、采集记录与停止信号。"""

from __future__ import annotations

import enum
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, TypeVar

from .core import Node, Output
from .events import Context

Outputs = Output | Iterable[Output] | None

T = TypeVar("T")


def _no_waiter(awaitable: Awaitable[Any]) -> Any:
    """未绑定执行时拒绝等待，并关闭协程避免未等待告警。"""

    close = getattr(awaitable, "close", None)
    if callable(close):
        close()
    raise RuntimeError("NodeCall.wait requires a call created by the Engine")


@dataclass(frozen=True, slots=True)
class NodeCall:
    """一次 Node 调用中允许中间件查看和替换的参数。

    Attributes:
        graph: Graph 的注册名称。
        node_id: Graph 内的节点 ID。
        node: 被调用的 Node 实例。
        inputs: 传给 Node 的只读端口输入映射。
        context: 本次 firing 的 Context，Node 与全部中间件共用同一实例。
        _waiter: Engine 绑定的等待器，在本次 execution 的取消与超时约束下等待 awaitable；
            不参与比较与表示，with_inputs 保留它。
    """

    graph: str
    node_id: str
    node: Node
    inputs: Mapping[str, Any]
    context: Context
    _waiter: Callable[[Awaitable[Any]], Any] = field(
        default=_no_waiter, compare=False, repr=False
    )

    def with_inputs(self, inputs: Mapping[str, Any]) -> NodeCall:
        """保留调用身份，只替换传给 Node 的输入。

        Args:
            inputs: 新的端口输入；端口集合与值类型由 Engine 在调用前校验。

        Returns:
            身份不变、输入为只读副本的新 NodeCall。

        Raises:
            TypeError: inputs 不是 Mapping。
        """

        if not isinstance(inputs, Mapping):
            raise TypeError("node call inputs must be a mapping")
        return replace(self, inputs=MappingProxyType(dict(inputs)))

    def wait(self, awaitable: Awaitable[T]) -> T:
        """在本次 execution 的取消与超时约束下同步等待 awaitable，供同步中间件调用异步接口。

        Args:
            awaitable: 需要等待的协程或 awaitable。

        Returns:
            awaitable 的结果。

        Raises:
            ExecutionControlError: 等待期间检测到取消或超时。
            RuntimeError: 调用不是由 Engine 创建。
        """

        result: T = self._waiter(awaitable)
        return result


class HookPhase(str, enum.Enum):
    """实际 Node 调用边界的采集阶段。

    Attributes:
        ENTER: Node 调用前，值为 None。
        EXIT: 一项原始输出离开 Node 之后，值为该 Output。
        ERROR: Node 调用或迭代失败之后，值为异常。
    """

    ENTER = "enter"
    EXIT = "exit"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class NodeInspection:
    """NODE_INSPECT 通知的一次采集记录。

    Attributes:
        call: 实际传给 Node 的调用参数，输入位于全部中间件转换之后。
        phase: 采集阶段。
        value: ENTER 为 None，EXIT 为原始 Output，ERROR 为异常。
    """

    call: NodeCall
    phase: HookPhase
    value: Any


class StopGraph(BaseException):
    """由 NODE_CALL 中间件抛出：停止后续 Node，并把携带结果追加为 Graph 的终端输出。

    继承 BaseException，避免被业务代码的 `except Exception` 吞掉。携带的 Output 按停止节点声明的
    输出端口与类型校验，但不沿 Edge 路由。

    Attributes:
        outputs: 信号携带的 Output，按交付顺序排列。
    """

    def __init__(self, *outputs: Output) -> None:
        """保存信号携带的输出。

        Args:
            *outputs: 按交付顺序排列的 Output，由 Engine 校验。
        """

        super().__init__(*outputs)
        self.outputs = tuple(outputs)
