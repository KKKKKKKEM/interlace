"""循环 Graph：普通 Edge 回到上游，Node 停止反馈后自然结束。"""

from __future__ import annotations

from interlace import Graph, Node, Output, Runtime
from interlace.engine import Edge


class Counter(Node):
    """通过回边递增计数并在达到边界时结束的示例节点。

    Attributes:
        inputs: 输入端口，value 为当前计数，可来自入口或 again 回边。
        outputs: 输出端口，again 继续循环，done 输出最终计数。
    """

    inputs = {"value": int}
    outputs = {"again": int, "done": int}

    def execute(self, inputs, context):
        """value 小于 3 时加一并经 again 回送，否则从 done 输出。

        Args:
            inputs: 按输入端口名索引的值，value 为当前计数。
            context: Node 执行上下文；本示例不使用。

        Returns:
            未达到边界时返回 again 端口上的递增值，否则返回 done 端口上的最终计数。
        """

        del context
        value = inputs["value"]
        if value < 3:
            return Output(value + 1, "again")
        return Output(value, "done")


def run(value: int = 0) -> int:
    """沿自环递增，直到 Counter 从 done 端口退出。

    Args:
        value: 循环起始计数。

    Returns:
        done 端口输出的最终计数；起始值小于 3 时为 3，否则为起始值本身。
    """

    graph = Graph.compose(
        nodes={"counter": Counter()},
        entrypoint="counter",
        edges=[Edge("counter", "counter", "again", "value")],
    )
    with Runtime() as runtime:
        runtime.register("cycle", graph)
        return runtime.run("cycle", value)[0].value


if __name__ == "__main__":
    print(run())
