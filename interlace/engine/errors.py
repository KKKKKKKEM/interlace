"""Interlace 精简内核的公共错误。"""

from __future__ import annotations


class InterlaceError(Exception):
    """所有 Interlace 公共错误的基类。"""


class GraphError(InterlaceError):
    """Graph 定义、冻结或引用不合法。"""


class GraphFrozenError(GraphError):
    """调用方尝试修改已经冻结的 Graph。"""


class GraphValidationError(GraphError):
    """Graph 在冻结时未满足静态约束。"""


class InterlaceRuntimeError(InterlaceError):
    """Runtime 注册、路由或生命周期操作失败。"""


class UnknownGraphError(InterlaceRuntimeError):
    """Runtime 中不存在指定的 Graph 注册名。"""


class ExecutionError(InterlaceRuntimeError):
    """一次 Graph 或 Node 执行失败。

    异常实例可以 pickle 往返；附加字段随实例状态一起恢复。

    Attributes:
        graph: 失败的 Graph 注册名或定义名；未知时为 None。
        node: 失败的 Node ID；不对应具体节点时为 None。
        event: 触发本次跨图执行的领域事件；直接调用时为 None。
    """

    def __init__(
        self,
        message: str,
        *,
        graph: str | None = None,
        node: str | None = None,
        event: object | None = None,
    ) -> None:
        """保存失败位置和可选触发事件。

        Args:
            message: 面向调用方的错误说明。
            graph: 失败的 Graph 注册名或定义名；未知时为 None。
            node: 失败的 Node ID；不对应具体节点时为 None。
            event: 触发跨图执行的领域事件；直接调用时为 None。
        """

        super().__init__(message)
        self.graph = graph
        self.node = node
        self.event = event


class ExecutionControlError(ExecutionError):
    """执行因调用方配置的控制条件而终止。"""


class ExecutionCancelledError(ExecutionControlError):
    """执行收到协作式取消请求。"""


class ExecutionTimeoutError(ExecutionControlError, TimeoutError):
    """整张 Graph 超过允许的总执行时长。"""


class NodeTimeoutError(ExecutionControlError, TimeoutError):
    """一次 Node firing 超过允许的执行时长。"""


class StepLimitExceededError(ExecutionControlError):
    """Graph 尝试执行超过允许步数的 Node firing。"""


class QueueLimitExceededError(ExecutionControlError):
    """Graph 内等待消费的输入 token 总数超过允许上限。"""


class HookExecutionError(ExecutionError):
    """Hook 返回非法数据、修改调用身份或在非法阶段发出信号。"""


class IncompleteInputsError(ExecutionError):
    """Graph 静止时仍存在无法满足输入策略的数据。"""


class PolicySelectionError(ExecutionError):
    """输入选择器返回了违反 selector 契约的结果，或在选择时抛出异常。"""


class InvalidOutputError(ExecutionError, TypeError):
    """Node 返回了不符合输出协议的值。"""


class PortValueTypeError(ExecutionError, TypeError):
    """实际端口值不满足 Ports 声明的类型。"""


class EventDispatchError(InterlaceRuntimeError):
    """事件订阅者或目标 Graph 执行失败。

    异常实例可以 pickle 往返，事件与原始异常需要各自可 pickle。

    Attributes:
        event: 投递失败的领域事件。
        cause: handler 或目标 Graph 抛出的原始异常。
    """

    def __init__(self, event: object, cause: BaseException) -> None:
        """保存投递失败的事件与原始异常。

        Args:
            event: 投递失败的领域事件。
            cause: handler 或目标 Graph 抛出的原始异常。
        """

        super().__init__(f"failed to dispatch event: {cause}")
        self.event = event
        self.cause = cause

    def __reduce__(
        self,
    ) -> tuple[type[EventDispatchError], tuple[object, BaseException]]:
        """按构造参数重建异常，避免默认实现只传入格式化后的消息。"""

        return type(self), (self.event, self.cause)


class RuntimeClosedError(InterlaceRuntimeError):
    """调用方在 Runtime 关闭后继续提交工作。"""
