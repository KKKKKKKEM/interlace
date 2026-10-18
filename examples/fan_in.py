"""分支与汇聚：一个值产生两个输出，再由 ALL Node 合并。"""

from __future__ import annotations

from interlace import Edge, Graph, InputPolicy, Node, Output, Ports, Runtime


class Split(Node):
    """将同一输入分发到两条计算分支的示例节点。

    Attributes:
        input_ports: 输入端口，value 为待分发的整数。
        output_ports: 输出端口，left 发送原值，right 发送原值加一。
    """

    input_ports = Ports(value=int)
    output_ports = Ports(left=int, right=int)

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
    """等待两路输入并输出相加结果的示例节点。

    Attributes:
        input_ports: 输入端口，left 与 right 分别接收两条分支的整数。
        output_ports: 输出端口，total 为两路输入之和。
        input_policy: ALL 策略，left 与 right 都有 token 时才触发执行。
    """

    input_ports = Ports(left=int, right=int)
    output_ports = Ports(total=int)
    input_policy = InputPolicy.ALL

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

    graph = Graph.compose(
        nodes={"split": Split(), "add": Add()},
        entrypoint="split",
        edges=[
            Edge("split", "add", "left", "left"),
            Edge("split", "add", "right", "right"),
        ],
    )
    with Runtime() as runtime:
        runtime.register("fan-in", graph)
        return runtime.run("fan-in", value)[0].value


if __name__ == "__main__":
    print(run())
