"""可冻结的 typed Graph 定义。"""

from __future__ import annotations

import inspect
import math
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, TypeVar, overload

from .core import (
    InputPolicy,
    Node,
    Ports,
    require_non_empty_string,
)
from .errors import GraphError, GraphFrozenError, GraphValidationError
from .events import Context, ContextFactory
from .policies import BoundPolicy, PolicyRef, PolicyRegistry

_G = TypeVar("_G", bound="Graph")  # 快捷构建保留调用方 Graph 子类的返回类型。


@dataclass(frozen=True, slots=True)
class Edge:
    """连接源 Node output port 和目标 Node input port。

    Attributes:
        source: 有向边连接的源节点 ID。
        target: 有向边连接的目标节点 ID。
        source_port: 有向边连接的源输出端口。
        target_port: 有向边连接的目标输入端口。
    """

    source: str
    target: str
    source_port: str = "default"
    target_port: str = "default"

    def __post_init__(self) -> None:
        """校验节点 ID 和端口名称。"""

        require_non_empty_string(self.source, "edge source")
        require_non_empty_string(self.target, "edge target")
        require_non_empty_string(self.source_port, "edge source_port")
        require_non_empty_string(self.target_port, "edge target_port")


@dataclass(frozen=True, slots=True)
class NodeSpec:
    """Graph 冻结时保存的单个 Node 执行元数据。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
        input_policy: 仅依据端口和 token 数量生效的输入策略。
        timeout: 超时秒数，None 表示不限制。
    """

    input_ports: Ports
    output_ports: Ports
    input_policy: BoundPolicy
    timeout: float | None


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    """一张冻结 Graph 的单次执行子图。

    Attributes:
        graph: 计划所属的已冻结 Graph 定义。
        nodes: 复制并冻结的待执行节点 ID 集合。
        entrypoint: 本次执行的唯一入口，缺省使用原图入口。
        edges: 保留的原图连接，始终保持原图顺序。
        outputs: 所有没有计划内下游的输出端口，使用节点 ID 与端口名二元组。
        cut_outputs: outputs 中因裁剪而成为出口的端口。
        boundary_edges: 源节点被选择但未保留的原图连接，包括部分扇出裁剪。
        _outgoing: 从派生边建立的只读下游索引。
    """

    graph: Graph
    nodes: frozenset[str]
    entrypoint: str
    edges: tuple[Edge, ...]
    outputs: frozenset[tuple[str, str]] = field(init=False)
    cut_outputs: frozenset[tuple[str, str]] = field(init=False)
    boundary_edges: tuple[Edge, ...] = field(init=False)
    _outgoing: Mapping[tuple[str, str], tuple[Edge, ...]] = field(
        init=False, repr=False
    )

    def __init__(
        self,
        graph: Graph,
        nodes: Iterable[str],
        *,
        entrypoint: str | None = None,
        edges: Iterable[Edge] | None = None,
        outputs: Iterable[tuple[str, str]] | None = None,
    ) -> None:
        """校验所选节点形成严格子图，并从冻结 Graph 派生全部执行元数据。

        Args:
            graph: 已通过类型、策略和可达性校验的冻结 Graph。
            nodes: 待执行节点 ID；复制为不可变集合后保存。
            entrypoint: 明确的执行入口，None 使用原图入口。
            edges: 保留的原图连接，None 保留所选节点间全部连接。
            outputs: 预期的完整出口集合，None 不额外约束；不用于过滤输出。

        Raises:
            TypeError: Graph 类型或节点 ID 集合不符合契约。
            GraphValidationError: Graph 未冻结，或节点选择缺失入口、不可达或缺少 ALL 输入。
        """

        if not isinstance(graph, Graph):
            raise TypeError("execution plan graph must be a Graph")
        if not graph.frozen:
            raise GraphValidationError("execution plan requires a frozen Graph")
        if isinstance(nodes, (str, bytes)):
            raise TypeError("plan nodes must be an iterable of node IDs")
        try:
            selected = frozenset(nodes)
        except TypeError as exc:
            raise TypeError("plan nodes must be an iterable of node IDs") from exc
        if not selected:
            raise GraphValidationError("execution plan must contain at least one node")
        if any(
            not isinstance(node_id, str) or not node_id.strip() for node_id in selected
        ):
            raise TypeError("plan node IDs must be non-empty strings")
        unknown = selected - set(graph.nodes)
        if unknown:
            raise GraphValidationError(
                f"execution plan references unknown nodes: {sorted(unknown)!r}"
            )
        entrypoint = (
            graph.entrypoint
            if entrypoint is None
            else require_non_empty_string(entrypoint, "plan entrypoint")
        )
        if entrypoint not in selected:
            raise GraphValidationError(
                f"execution plan must contain entrypoint {entrypoint!r}"
            )

        available = tuple(
            edge
            for edge in graph.edges
            if edge.source in selected and edge.target in selected
        )
        if edges is None:
            retained = available
        else:
            requested = tuple(edges)
            if any(not isinstance(edge, Edge) for edge in requested):
                raise TypeError("plan edges must contain Edge instances")
            invalid = set(requested) - set(available)
            if invalid:
                raise GraphValidationError(
                    "plan edges must be original edges between selected nodes: "
                    f"{sorted(map(repr, invalid))!r}"
                )
            requested_edges = set(requested)
            retained = tuple(edge for edge in available if edge in requested_edges)
        adjacency: dict[str, set[str]] = defaultdict(set)
        incoming_ports: dict[str, set[str]] = defaultdict(set)
        outgoing: dict[tuple[str, str], list[Edge]] = defaultdict(list)
        for edge in retained:
            adjacency[edge.source].add(edge.target)
            incoming_ports[edge.target].add(edge.target_port)
            outgoing[(edge.source, edge.source_port)].append(edge)

        reached: set[str] = set()
        pending = [entrypoint]
        while pending:
            current = pending.pop()
            if current not in reached:
                reached.add(current)
                pending.extend(adjacency.get(current, ()))
        unreachable = selected - reached
        if unreachable:
            raise GraphValidationError(
                "execution plan nodes are unreachable from entrypoint using original "
                f"edges: {sorted(unreachable)!r}"
            )
        for node_id in selected:
            spec = graph.spec_for(node_id)
            if (
                node_id == entrypoint
                or spec.input_policy.ref.name != "interlace.core/all"
            ):
                continue
            missing = set(spec.input_ports) - incoming_ports[node_id]
            if missing:
                raise GraphValidationError(
                    f"execution plan leaves ALL node {node_id!r} without inputs: "
                    f"{sorted(missing)!r}"
                )

        terminal = frozenset(
            (node_id, port)
            for node_id in selected
            for port in graph.spec_for(node_id).output_ports
            if not outgoing.get((node_id, port))
        )
        if outputs is not None:
            expected = tuple(outputs)
            if any(
                not isinstance(value, tuple)
                or len(value) != 2
                or any(not isinstance(part, str) or not part.strip() for part in value)
                for value in expected
            ):
                raise TypeError("plan outputs must contain (node_id, port) tuples")
            expected_ports = frozenset(expected)
            if terminal != expected_ports:
                raise GraphValidationError(
                    "plan output boundary differs: "
                    f"unexpected={sorted(terminal - expected_ports)!r}, "
                    f"missing={sorted(expected_ports - terminal)!r}"
                )
        object.__setattr__(self, "graph", graph)
        object.__setattr__(self, "nodes", selected)
        object.__setattr__(self, "entrypoint", entrypoint)
        object.__setattr__(self, "edges", retained)
        object.__setattr__(self, "outputs", terminal)
        object.__setattr__(
            self,
            "cut_outputs",
            frozenset(value for value in terminal if graph.outgoing_for(*value)),
        )
        object.__setattr__(
            self,
            "boundary_edges",
            tuple(
                edge
                for edge in graph.edges
                if edge.source in selected and edge not in retained
            ),
        )
        object.__setattr__(
            self,
            "_outgoing",
            MappingProxyType({key: tuple(value) for key, value in outgoing.items()}),
        )

    def outgoing_for(self, node_id: str, port: str) -> tuple[Edge, ...]:
        """返回计划内指定 output port 的有序下游连接。

        Args:
            node_id: Graph 内绑定的节点 ID。
            port: 需要读取或输出的端口名称。

        Returns:
            从指定节点出发的有向边元组。
        """

        return self._outgoing.get((node_id, port), ())

    def describe(self) -> dict[str, Any]:
        """返回独立的可 JSON 编码诊断快照，不运行节点或读取执行状态。

        Returns:
            入口及输入类型、所选节点和边、原有出口、裁剪出口与被裁剪连接。
            出口来自端口声明，不保证每个出口在实际运行中都会产生结果。
        """

        def connection(edge: Edge) -> dict[str, str]:
            """把一条不可变连接转换为诊断字段。

            Args:
                edge: 原图连接。

            Returns:
                新建的连接描述。
            """
            return {
                "source": edge.source,
                "source_port": edge.source_port,
                "target": edge.target,
                "target_port": edge.target_port,
            }

        return {
            "entrypoint": self.entrypoint,
            "inputs": {
                port: f"{kind.__module__}.{kind.__qualname__}"
                for port, kind in self.graph.spec_for(
                    self.entrypoint
                ).input_ports.items()
            },
            "nodes": [node for node in self.graph.nodes if node in self.nodes],
            "edges": [connection(edge) for edge in self.edges],
            "outputs": sorted(self.outputs),
            "original_outputs": sorted(self.outputs - self.cut_outputs),
            "cut_outputs": sorted(self.cut_outputs),
            "boundary_edges": [connection(edge) for edge in self.boundary_edges],
        }


class Graph:
    """描述 Node 通过 typed Edge 传递数据的静态有向图。

    Attributes:
        _entrypoint: Graph 入口节点 ID。
        _context_factory: 构造时固定的同步领域 Context 工厂，不保存当前执行状态。
        _nodes: Graph 内节点 ID 到节点实例的绑定。
        _edges: 保持声明顺序的有向边集合。
        _edge_set: 用于拒绝重复边的集合。
        _outgoing: 按源节点组织的下游边索引。
        _node_specs: 冻结时保存的节点端口、策略和超时快照。
        _frozen: 是否已完成冻结，冻结后不再接受定义修改。
    """

    def __init__(
        self,
        *,
        entrypoint: str | None = None,
        context_factory: ContextFactory = Context.make,
    ) -> None:
        """创建处于构建状态的空 Graph。

        Args:
            entrypoint: 可选的入口 Node ID，也可稍后用 entry() 设置。
            context_factory: 接收基础 Context 和只读输入的同步工厂，默认 Context.make。

        Raises:
            TypeError: context_factory 不可调用或声明为异步函数、生成器函数。
        """

        if not callable(context_factory) or any(
            check(callback)
            for callback in (
                context_factory,
                getattr(context_factory, "__call__", None),
            )
            for check in (
                inspect.iscoroutinefunction,
                inspect.isasyncgenfunction,
                inspect.isgeneratorfunction,
            )
        ):
            raise TypeError("context_factory must be a synchronous callable")
        self._context_factory = context_factory
        self._entrypoint = (
            None
            if entrypoint is None
            else require_non_empty_string(entrypoint, "graph entrypoint")
        )
        self._nodes: dict[str, Node] = {}
        self._edges: list[Edge] = []
        self._edge_set: set[Edge] = set()
        self._outgoing: dict[tuple[str, str], tuple[Edge, ...]] = {}
        self._node_specs: dict[str, NodeSpec] = {}
        self._frozen = False

    @classmethod
    def compose(
        cls: type[_G],
        *,
        nodes: Mapping[str, Node],
        entrypoint: str,
        edges: Iterable[Edge] = (),
        context_factory: ContextFactory = Context.make,
    ) -> _G:
        """按显式入口、节点映射和有序 Edge 集合组装可继续修改的 Graph。

        复制节点绑定和边集合，不复制 Node 行为。通过 add/connect 构建，
        完整类型、可达性和输入策略校验仍在 freeze 或 Runtime.register 时执行。

        Args:
            nodes: Graph 内节点 ID 到 Node 实例的映射，可为空以便后续补全。
            entrypoint: 显式指定的入口节点 ID，不根据边或映射顺序推断。
            edges: 按声明顺序消费一次的 Edge iterable，默认没有连线。
            context_factory: 本图每次 Node firing 使用的同步 Context 工厂。

        Returns:
            尚未冻结、可继续调用 add/connect 的 Graph。

        Raises:
            TypeError: 节点映射、Node、Edge、入口 ID 或 Context 工厂类型不合法。
            ValueError: 节点 ID 或入口 ID 为空。
            GraphError: Edge 重复。
        """

        if not isinstance(nodes, Mapping):
            raise TypeError("compose nodes must be a mapping of node IDs to Nodes")
        graph = cls(
            entrypoint=require_non_empty_string(entrypoint, "graph entrypoint"),
            context_factory=context_factory,
        )
        for node_id, node in nodes.items():
            graph.add(node_id, node)
        for edge in edges:
            if not isinstance(edge, Edge):
                raise TypeError("compose edges must contain only Edge instances")
            graph.connect(
                edge.source,
                edge.target,
                source_port=edge.source_port,
                target_port=edge.target_port,
            )
        return graph

    @property
    def context_factory(self) -> ContextFactory:
        """返回本图固定使用的节点上下文工厂。

        Returns:
            从基础执行能力和当前输入创建 Context 的同步可调用对象。
        """

        return self._context_factory

    @property
    def entrypoint(self) -> str:
        """返回入口 Node ID。

        Returns:
            Graph 入口节点 ID。

        Raises:
            GraphError: Graph 尚未设置入口。
        """

        if self._entrypoint is None:
            raise GraphError("graph has no entrypoint")
        return self._entrypoint

    @property
    def frozen(self) -> bool:
        """返回 Graph 是否已经冻结。

        Returns:
            是否已完成冻结，冻结后不再接受定义修改。
        """

        return self._frozen

    @property
    def nodes(self) -> Mapping[str, Node]:
        """返回只读的 Node binding。

        Returns:
            节点绑定 ID 到节点实例的只读映射。
        """

        return MappingProxyType(self._nodes)

    @property
    def edges(self) -> tuple[Edge, ...]:
        """返回按定义顺序排列的 Edge。

        Returns:
            保持声明顺序的有向边元组。
        """

        return tuple(self._edges)

    def entry(self, node_id: str) -> Graph:
        """设置当前 Graph 的唯一入口。

        Args:
            node_id: 作为入口的 Node ID。

        Returns:
            当前 Graph，便于链式构建。
        """

        self._ensure_mutable()
        self._entrypoint = require_non_empty_string(node_id, "graph entrypoint")
        return self

    @overload
    def add(self, node_id: str, node: Node, /) -> Graph:
        """将节点绑定加入当前 Graph，节点 ID 属于此次绑定。

        Args:
            node_id: Graph 内绑定的节点 ID。
            node: 节点实例或作用域中的节点 ID，以接口类型为准。

        Returns:
            本次操作得到的 Graph 实例。
        """
        ...

    @overload
    def add(self, **nodes: Node) -> Graph:
        """将节点绑定加入当前 Graph，节点 ID 属于此次绑定。

        Args:
            **nodes: 待绑定的节点 ID 与节点实例。

        Returns:
            本次操作得到的 Graph 实例。
        """
        ...

    def add(self, *args: object, **nodes: Node) -> Graph:
        """把一个或多个 Node 行为绑定到 Graph 中的位置。

        Args:
            args: 单个 Node 的 ID 和可复用 Node 行为。
            nodes: 以关键字名称作为 Node ID 的一组 Node 行为。

        Returns:
            当前 Graph，便于链式构建。

        Raises:
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        self._ensure_mutable()
        if args and nodes:
            raise TypeError("add accepts either (node_id, node) or keyword nodes")
        bindings: tuple[tuple[object, object], ...]
        if args:
            if len(args) != 2:
                raise TypeError("add expects a node_id and node")
            bindings = ((args[0], args[1]),)
        else:
            if not nodes:
                raise TypeError("add requires at least one node")
            bindings = tuple(nodes.items())

        validated: list[tuple[str, Node]] = []
        for node_id, node in bindings:
            node_id = require_non_empty_string(node_id, "node_id")
            if not isinstance(node, Node):
                raise TypeError(f"node {node_id!r} must be a Node")
            if node_id in self._nodes:
                raise GraphError(f"duplicate node {node_id!r}")
            validated.append((node_id, node))
        self._nodes.update(validated)
        return self

    def connect(
        self,
        source: str,
        target: str,
        *,
        source_port: str = "default",
        target_port: str = "default",
    ) -> Graph:
        """增加一条端口到端口的有向连接。

        Args:
            source: 源 Node ID。
            target: 目标 Node ID。
            source_port: 源 output port。
            target_port: 目标 input port。

        Returns:
            当前 Graph，便于链式构建。
        """

        self._ensure_mutable()
        edge = Edge(source, target, source_port, target_port)
        if edge in self._edge_set:
            raise GraphError(f"duplicate edge {edge!r}")
        self._edges.append(edge)
        self._edge_set.add(edge)
        return self

    def plan(
        self,
        *,
        include: Iterable[str],
        entrypoint: str | None = None,
        edges: Iterable[Edge] | None = None,
        outputs: Iterable[tuple[str, str]] | None = None,
        policies: PolicyRegistry | None = None,
    ) -> ExecutionPlan:
        """选择由原有节点和边组成的严格执行子图。

        未选节点不会执行，也不会自动连接其前后节点。首次创建计划时会
        冻结 Graph，保证计划所引用的定义之后不再变化。

        Args:
            include: 执行计划中需要保留的节点 ID 集合。
            entrypoint: 所选节点中的执行入口，None 使用原图入口。
            edges: 明确保留的原图连接，None 使用所选节点间全部原有连接。
            outputs: 预期完整出口集合，None 不额外校验；不改变输出交付。
            policies: 注册并绑定输入选择策略的容器。

        Returns:
            本次操作得到的 ExecutionPlan 实例。

        Raises:
            GraphValidationError: Graph 定义不符合冻结或执行约束。
            TypeError: 参数类型或接口实现不符合当前契约。
        """

        if not self._frozen:
            self.freeze(policies)
        return ExecutionPlan(
            self, include, entrypoint=entrypoint, edges=edges, outputs=outputs
        )

    def freeze(self, policies: PolicyRegistry | None = None) -> Graph:
        """校验并冻结 Graph。

        Args:
            policies: 注册并绑定输入选择策略的容器。

        Returns:
            已冻结的当前 Graph。

        Raises:
            GraphValidationError: 节点、端口、策略、连接或可达性不合法。
        """

        if self._frozen:
            return self
        if policies is None:
            policies = PolicyRegistry()
        if not isinstance(policies, PolicyRegistry):
            raise TypeError("policies must be a PolicyRegistry or None")
        self._validate_structure(policies)
        outgoing: dict[tuple[str, str], list[Edge]] = defaultdict(list)
        for edge in self._edges:
            outgoing[(edge.source, edge.source_port)].append(edge)
        self._outgoing = {key: tuple(edges) for key, edges in outgoing.items()}
        self._frozen = True
        return self

    def spec_for(self, node_id: str) -> NodeSpec:
        """返回冻结后的 Node 执行元数据。

        Args:
            node_id: Graph 内绑定的节点 ID。

        Returns:
            指定节点在冻结时保存的执行元数据。
        """

        self._ensure_frozen()
        return self._node_specs[node_id]

    def _execution_timeouts(self) -> Mapping[str, float | None]:
        """返回冻结后的 Node timeout 快照。

        Returns:
            各冻结节点的超时映射，单位秒。
        """

        self._ensure_frozen()
        return MappingProxyType(
            {node_id: spec.timeout for node_id, spec in self._node_specs.items()}
        )

    def outgoing_for(self, node_id: str, port: str) -> tuple[Edge, ...]:
        """返回指定 output port 的有序下游连接。

        Args:
            node_id: 源 Node ID。
            port: 源 output port。

        Returns:
            按定义顺序排列的 Edge。
        """

        self._ensure_frozen()
        return self._outgoing.get((node_id, port), ())

    def _validate_structure(self, policies: PolicyRegistry) -> None:
        """执行冻结前的完整静态校验。

        Args:
            policies: 注册并绑定输入选择策略的容器。

        Raises:
            GraphValidationError: Graph 定义不符合冻结或执行约束。
        """

        if not self._nodes:
            raise GraphValidationError("graph must contain at least one node")
        if self._entrypoint not in self._nodes:
            raise GraphValidationError("graph entrypoint must reference a node")

        specs: dict[str, NodeSpec] = {}
        for node_id, node in self._nodes.items():
            inputs = node.input_ports
            outputs = node.output_ports
            policy = node.input_policy
            timeout = node.timeout
            if not isinstance(inputs, Ports) or not isinstance(outputs, Ports):
                raise GraphValidationError(f"node {node_id!r} ports must be Ports")
            if not isinstance(policy, (InputPolicy, PolicyRef)):
                raise GraphValidationError(
                    f"node {node_id!r} input_policy must be InputPolicy or PolicyRef"
                )
            if timeout is not None:
                if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
                    raise GraphValidationError(
                        f"node {node_id!r} timeout must be a number or None"
                    )
                if not math.isfinite(timeout) or timeout <= 0:
                    raise GraphValidationError(
                        f"node {node_id!r} timeout must be finite and greater than zero"
                    )
            try:
                bound_policy = policies.bind(policy)
            except (TypeError, ValueError) as exc:
                raise GraphValidationError(
                    f"node {node_id!r} has invalid input policy: {exc}"
                ) from exc
            self._validate_policy(node_id, inputs, bound_policy)
            specs[node_id] = NodeSpec(inputs, outputs, bound_policy, timeout)

        adjacency: dict[str, set[str]] = defaultdict(set)
        incoming_ports: dict[str, set[str]] = defaultdict(set)
        for edge in self._edges:
            if edge.source not in self._nodes or edge.target not in self._nodes:
                raise GraphValidationError(f"edge references unknown node: {edge!r}")
            source_ports = specs[edge.source].output_ports
            target_ports = specs[edge.target].input_ports
            if edge.source_port not in source_ports:
                raise GraphValidationError(
                    f"node {edge.source!r} has no output port {edge.source_port!r}"
                )
            if edge.target_port not in target_ports:
                raise GraphValidationError(
                    f"node {edge.target!r} has no input port {edge.target_port!r}"
                )
            if not Ports.is_type_compatible(
                source_ports[edge.source_port], target_ports[edge.target_port]
            ):
                raise GraphValidationError(
                    f"incompatible edge {edge.source}.{edge.source_port} -> "
                    f"{edge.target}.{edge.target_port}"
                )
            adjacency[edge.source].add(edge.target)
            incoming_ports[edge.target].add(edge.target_port)

        reachable = self._reachable(adjacency)
        unreachable = set(self._nodes) - reachable
        if unreachable:
            raise GraphValidationError(
                f"nodes are unreachable from entrypoint: {sorted(unreachable)!r}"
            )
        for node_id, spec in specs.items():
            if (
                node_id == self.entrypoint
                or spec.input_policy.ref.name != "interlace.core/all"
            ):
                continue
            missing = set(spec.input_ports) - incoming_ports[node_id]
            if missing:
                raise GraphValidationError(
                    f"ALL node {node_id!r} has no incoming edge for inputs: "
                    f"{sorted(missing)!r}"
                )
        self._node_specs = specs

    @staticmethod
    def _validate_policy(
        node_id: str,
        ports: Ports,
        policy: BoundPolicy,
    ) -> None:
        """校验策略和声明端口是否匹配。

        Args:
            node_id: 用于错误定位的 Node ID。
            ports: Node 的 input ports。
            policy: Node 的输入策略。

        Raises:
            GraphValidationError: Graph 定义不符合冻结或执行约束。
        """

        declared = tuple(ports)
        if policy.on_start:
            if declared:
                raise GraphValidationError(
                    f"node {node_id!r} on_start policy requires zero inputs"
                )
            return
        if not declared:
            raise GraphValidationError(
                f"zero-input node {node_id!r} must use InputPolicy.ON_START"
            )

    def _reachable(self, adjacency: Mapping[str, set[str]]) -> set[str]:
        """计算从入口可达的 Node。

        Args:
            adjacency: Node ID 到直接下游的邻接表。

        Returns:
            包含入口的可达 Node ID 集合。
        """

        reached: set[str] = set()
        pending = [self.entrypoint]
        while pending:
            node_id = pending.pop()
            if node_id in reached:
                continue
            reached.add(node_id)
            pending.extend(adjacency.get(node_id, set()))
        return reached

    def _ensure_mutable(self) -> None:
        """拒绝冻结后的修改。

        Raises:
            GraphFrozenError: 尝试修改已经冻结的 Graph。
        """

        if self._frozen:
            raise GraphFrozenError("graph is frozen")

    def _ensure_frozen(self) -> None:
        """拒绝在冻结前读取执行快照。"""

        if not self._frozen:
            raise GraphError("graph is not frozen")
