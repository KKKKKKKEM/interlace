"""execution-local 的确定性 keyed join Node。"""

from __future__ import annotations

from collections import deque
from collections.abc import Hashable, Mapping
from dataclasses import dataclass
from typing import Any

from ..engine import Context, Node, Output, Ports
from ..engine.errors import ExecutionError, IncompleteInputsError
from ..engine.policies import LONGEST_POLICY, PolicyRef


class KeyedJoinOverflowError(ExecutionError):
    """单次 execution 内未配对的 keyed join 输入超过 max_pending。"""


@dataclass(frozen=True, slots=True)
class KeyedValue:
    """携带领域关联键的一项输入值。

    Attributes:
        key: 关联左右输入的领域键，必须可哈希。
        value: 参与配对的业务值。
    """

    key: Hashable
    value: Any

    def __post_init__(self) -> None:
        """在构造时校验 key 可哈希，提前暴露非法关联键。"""

        hash(self.key)


@dataclass(frozen=True, slots=True)
class KeyedPair:
    """同一领域键下已经配对的左右输入。

    Attributes:
        key: 两侧共同的领域键。
        left: 来自 left 端口的值。
        right: 来自 right 端口的值。
    """

    key: Hashable
    left: Any
    right: Any


class KeyedJoin(Node):
    """按 key 配对乱序输入；同 key 的重复值分别保持 FIFO。

    未配对值保存在当前 execution 的 Context.state 中，不跨 execution 共享。数据流静止时
    仍有未配对值则以 IncompleteInputsError 使 execution 失败。

    Attributes:
        input_ports: left 与 right 两个 KeyedValue 输入端口。
        output_ports: 输出 KeyedPair 的 joined 端口。
        input_policy: 内建 longest 策略，每次 firing 只消费一个值，并优先消费积压较多的一侧，
            使交错到达的左右输入及时配对。
        max_pending: 单次 execution 内允许暂存的未配对值总数上限。
    """

    input_ports = Ports(left=KeyedValue, right=KeyedValue)
    output_ports = Ports(joined=KeyedPair)
    input_policy = PolicyRef(LONGEST_POLICY)

    def __init__(self, *, max_pending: int = 10_000) -> None:
        """校验未配对值上限。

        Args:
            max_pending: 单次 execution 内允许暂存的未配对值总数上限，默认 10000。

        Raises:
            ValueError: max_pending 不是大于零的 int。
        """

        if type(max_pending) is not int or max_pending < 1:
            raise ValueError("max_pending must be an integer greater than zero")
        self.max_pending = max_pending

    def execute(self, inputs: Mapping[str, Any], context: Context) -> Output | None:
        """把本次到达的值与对侧同 key 的最早暂存值配对，没有对侧值时暂存。

        Args:
            inputs: 仅含 left 或 right 一个端口的输入映射。
            context: 保存本次 execution 配对状态的上下文。

        Returns:
            配对成功时返回 joined 端口上的 KeyedPair；暂存时返回 None。

        Raises:
            KeyedJoinOverflowError: 暂存后未配对值总数会超过 max_pending。
        """

        state = context.state("interlace.keyed-join")
        buffers = state.setdefault("buffers", {"left": {}, "right": {}})
        pending = state.setdefault("pending", 0)
        if not state.get("finalizer_registered"):
            state["finalizer_registered"] = True

            def validate_complete() -> None:
                """静止时仍有未配对值则抛出 IncompleteInputsError。"""

                if state["pending"]:
                    keys = {
                        side: tuple(side_buffers)
                        for side, side_buffers in buffers.items()
                        if side_buffers
                    }
                    raise IncompleteInputsError(
                        f"keyed join stopped with {state['pending']} unmatched values: "
                        f"{keys!r}"
                    )

            context.on_quiescence(validate_complete)
        port, item = next(iter(inputs.items()))
        other = "right" if port == "left" else "left"
        other_queue = buffers[other].get(item.key)
        if other_queue:
            counterpart = other_queue.popleft()
            state["pending"] = pending - 1
            if not other_queue:
                del buffers[other][item.key]
            left, right = (
                (item.value, counterpart)
                if port == "left"
                else (
                    counterpart,
                    item.value,
                )
            )
            return Output(KeyedPair(item.key, left, right), "joined")

        if pending >= self.max_pending:
            raise KeyedJoinOverflowError(
                f"keyed join exceeded max_pending={self.max_pending}"
            )
        buffers[port].setdefault(item.key, deque()).append(item.value)
        state["pending"] = pending + 1
        return None
