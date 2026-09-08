"""具名节点工厂与图文档编译；只允许应用显式登记的构造能力。"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from threading import RLock
from typing import Any, TypeVar

from pydantic import BaseModel

from interlace import Graph, Node
from interlace.engine.policies import PolicyRegistry

from .models import GraphDefinition, Model

C = TypeVar("C", bound=BaseModel)


@dataclass(frozen=True)
class _NodeType:
    """保存一个不可变工厂登记。

    Attributes:
        title: 编辑器显示名称。
        model: 用于生成表单和验证配置的模型。
        factory: 接收验证后模型并返回 Node 的构造函数。
    """

    title: str
    model: type[BaseModel]
    factory: Callable[[Any], Node]


class NodeCatalog:
    """为可视化编辑提供显式节点目录。

    Attributes:
        _types: 名称到工厂登记的映射。
        _lock: 保护登记和目录快照。
        policies: 编译图时使用的输入策略注册表，由调用方管理。
    """

    def __init__(self, *, policies: PolicyRegistry | None = None) -> None:
        """创建目录，可注入与应用一致的 selector 注册表。

        Args:
            policies: 自定义输入策略；None 使用标准策略。
        """

        self._types: dict[str, _NodeType] = {}
        self._lock = RLock()
        self.policies = policies

    def register(
        self,
        name: str,
        factory: Callable[[C], Node],
        *,
        config: type[C],
        title: str | None = None,
    ) -> NodeCatalog:
        """登记节点工厂与 Pydantic 配置模型。

        Args:
            name: 唯一且非空的节点类型名称。
            factory: 接收配置模型的节点工厂，资源由应用注入并管理。
            config: 声明可编辑配置的 Pydantic 模型。
            title: 可读名称，默认使用登记名称。

        Returns:
            当前目录，便于连续登记。

        Raises:
            ValueError: 名称为空或重复。
            TypeError: 配置模型或工厂类型非法。
        """

        if not name.strip():
            raise ValueError("node type name must not be empty")
        if not isinstance(config, type) or not issubclass(config, BaseModel):
            raise TypeError("config must be a Pydantic model")
        if not callable(factory):
            raise TypeError("factory must be callable")
        with self._lock:
            if name in self._types:
                raise ValueError(f"duplicate node type: {name}")
            self._types[name] = _NodeType(title or name, config, factory)
        return self

    def register_node(
        self, name: str, node: type[Node], *, title: str | None = None
    ) -> NodeCatalog:
        """登记无构造参数的节点类。

        Args:
            name: 唯一类型名称。
            node: 可无参数构造的节点类。
            title: 编辑器显示名称。

        Returns:
            当前目录。
        """

        return self.register(name, lambda config: node(), config=Model, title=title)

    def describe(self) -> list[dict[str, Any]]:
        """返回表单所需元数据，不为浏览目录提前构造节点。

        Returns:
            节点类型、名称及 JSON Schema 集合。
        """

        with self._lock:
            return [
                {
                    "type": name,
                    "title": item.title,
                    "schema": item.model.model_json_schema(),
                }
                for name, item in self._types.items()
            ]

    def create(self, name: str, config: dict[str, Any]) -> Node:
        """校验配置并调用已登记工厂。

        Args:
            name: 节点目录中的类型名称。
            config: 用户提供的配置数据。

        Returns:
            工厂返回的 Node。

        Raises:
            ValueError: 类型未登记或配置非法。
            TypeError: 工厂未返回 Node。
        """

        with self._lock:
            item = self._types.get(name)
        if item is None:
            raise ValueError(f"unknown node type: {name}")
        node = item.factory(item.model.model_validate(config))
        if not isinstance(node, Node):
            raise TypeError("node factory must return a Node")
        return node

    def compile(self, definition: GraphDefinition) -> Graph:
        """通过标准 Graph 构建和冻结流程编译文档。

        Args:
            definition: 待验证的图文档。

        Returns:
            已通过核心校验的冻结 Graph。

        Raises:
            ValueError: 类型或配置非法。
            GraphError: 图结构、端口或可达性非法。
        """

        graph = Graph(entrypoint=definition.entrypoint)
        for item in definition.nodes:
            graph.add(item.id, self.create(item.type, item.config))
        for edge in definition.edges:
            graph.connect(**edge.model_dump())
        return graph.freeze(self.policies)


def describe_graph(name: str, graph: Graph) -> dict[str, Any]:
    """将公开冻结元数据转为可视化结构，不读取构造器私有状态。

    Args:
        name: 图注册名称。
        graph: 已冻结图。

    Returns:
        可用于画布和端口检查的 JSON 对象。
    """

    return {
        "name": name,
        "entrypoint": graph.entrypoint,
        "nodes": [
            {
                "id": node_id,
                "type": type(node).__name__,
                "inputs": {
                    port: typ.__name__
                    for port, typ in graph.spec_for(node_id).input_ports.items()
                },
                "outputs": {
                    port: typ.__name__
                    for port, typ in graph.spec_for(node_id).output_ports.items()
                },
                "policy": graph.spec_for(node_id).input_policy.ref.name,
            }
            for node_id, node in graph.nodes.items()
        ],
        "edges": [
            {
                "source": edge.source,
                "target": edge.target,
                "source_port": edge.source_port,
                "target_port": edge.target_port,
            }
            for edge in graph.edges
        ],
    }
