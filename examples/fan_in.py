"""分支与汇聚：一个值产生两个输出，再由默认 ALL 策略的 Node 合并。"""

from __future__ import annotations

from interlace import Graph, Node, Output, Runtime


class Split(Node):
    """将同一输入分发到两条计算分支的示例节点。

    Attributes:
        inputs: 输入端口，value 为待分发的整数。
        outputs: 输出端口，left 发送原值，right 发送原值加一。
    """

    inputs = {"value": int}
    outputs = {"left": int, "right": int}

    def execute(self, inputs, context):
        """把 value 分发到 left，把 value + 1 分发到 right。

        Args:
            inputs: 按输入端口名索引的值，value 为待分发整数。
            context: Node 执行上下文；本示例不使用。

        Returns:
            分别从 left 和 right 端口发送的两项 Output。
        """

        del context
        value = inputs["value"]
        return Output(value, "left"), Output(value + 1, "right")


class Add(Node):
    """等待两路输入并输出相加结果的示例节点；默认 ALL 策略在 left 与 right 都有值时才触发。

    Attributes:
        inputs: 输入端口，left 与 right 分别接收两条分支的整数。
        outputs: 输出端口，total 为两路输入之和。
    """

    inputs = {"left": int, "right": int}
    outputs = {"total": int}

    def execute(self, inputs, context):
        """把 left 与 right 相加后从 total 输出。

        Args:
            inputs: 按输入端口名索引的值，同时包含 left 与 right。
            context: Node 执行上下文；本示例不使用。

        Returns:
            total 端口上左右输入之和的 Output。
        """

        del context
        return Output(inputs["left"] + inputs["right"], "total")


def run(value: int = 2) -> int:
    """执行 Split → Add；Add 只在两个端口均到达后运行。

    Args:
        value: 送入 Split 的整数。

    Returns:
        total 端口输出的和，即 value * 2 + 1。
    """

    graph = (
        Graph(entrypoint="split")
        .add(split=Split(), add=Add())
        .connect("split", "add", source_port="left", target_port="left")
        .connect("split", "add", source_port="right", target_port="right")
    )
    with Runtime() as runtime:
        runtime.register("fan-in", graph)
        return runtime.run("fan-in", value)[0].value


if __name__ == "__main__":
    print(run())
