"""组合引擎 Context，实现领域工厂与跨节点爬取数据传递；不访问网络。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from interlace import Context, Graph, Node, Output, Ports, Runtime


@dataclass
class CrawlData:
    """沿当前图的线性数据流传递的领域数据。

    Attributes:
        seed: 当前任务的页码，由调用方提供。
        request: 当前图构造的请求 URL，构造前为 None；由当前任务拥有。
    """

    seed: int
    request: str | None = None


class AirContext(Context):
    """通过组合复用引擎执行能力，提供当前任务的领域视图。

    Attributes:
        data: 借用当前输入的领域数据；上下文重建不复制它，跨节点由 Output 传递。
    """

    def __init__(self, base: Context, data: CrawlData) -> None:
        """使用领域自己的构造参数组合执行能力。

        Args:
            base: 引擎为当前节点调用绑定的上下文。
            data: 当前输入携带的爬取数据。
        """

        super().__init__(parent=base)
        self.data = data

    @classmethod
    def make(cls, base: Context, inputs: Mapping[str, Any]) -> AirContext:
        """验证当前领域输入并按业务构造签名创建上下文。

        Args:
            base: 引擎绑定的执行能力。
            inputs: 当前节点调用在 Hook enter 前的只读输入映射。

        Returns:
            引用当前任务数据的独立领域上下文。

        Raises:
            TypeError: 输入未携带 CrawlData。
            ValueError: 页码不是正整数。
        """

        data = inputs["data"]
        if not isinstance(data, CrawlData):
            raise TypeError("data must be CrawlData")
        if type(data.seed) is not int or data.seed < 1:
            raise ValueError("seed must be a positive page number")
        return cls(base, data)

    @property
    def seed(self) -> int:
        """读取当前任务的页码。

        Returns:
            当前输入数据持有的页码。
        """

        return self.data.seed

    @property
    def request(self) -> str | None:
        """读取上游节点构造的请求地址。

        Returns:
            当前任务的请求 URL，尚未构造时为 None。
        """

        return self.data.request


class BuildRequest(Node):
    """通过领域上下文构造请求地址。"""

    input_ports = Ports(data=CrawlData)  # 当前任务拥有的领域数据。
    output_ports = Ports(data=CrawlData)  # 携带请求地址的同一任务数据。

    def execute(self, inputs: Mapping[str, Any], context: Context) -> Output:
        """构造 URL 后通过 Output 交付当前任务数据。

        Args:
            inputs: 当前任务的数据端口。
            context: 工厂创建的 AirContext。

        Returns:
            携带请求地址的领域数据。
        """

        assert isinstance(context, AirContext)
        context.checkpoint()
        context.data.request = f"https://example.com/news?page={context.seed}"
        return Output(context.data, "data")


class ReadRequest(Node):
    """用新的上下文读取沿 Edge 到达的数据。"""

    input_ports = Ports(data=CrawlData)  # 上游交付的领域数据。
    output_ports = Ports(url=str)  # 本示例的终端请求地址。

    def execute(self, inputs: Mapping[str, Any], context: Context) -> Output:
        """输出上游构造的 URL，不执行网络请求。

        Args:
            inputs: 上游交付的领域数据。
            context: 重新创建但借用同一任务数据的 AirContext。

        Returns:
            上游构造的请求地址。
        """

        assert isinstance(context, AirContext)
        assert context.request is not None
        return Output(context.request, "url")


def main() -> None:
    """用标准 Runtime 装配领域工厂并运行两次独立任务。"""

    graph = (
        Graph(entrypoint="build", context_factory=AirContext.make)
        .add(build=BuildRequest(), read=ReadRequest())
        .connect("build", "read", source_port="data", target_port="data")
    )
    with Runtime() as runtime:
        runtime.register("air", graph)
        for page in (1, 2):
            print(runtime.run("air", CrawlData(seed=page))[0].value)


if __name__ == "__main__":
    main()
