"""可冻结的 typed Graph 定义与单次执行计划。"""

from __future__ import annotations

import inspect
import math
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from threading import RLock
from types import MappingProxyType
from typing import Any, TypeVar, overload

from .._validation import require_non_empty_string
from .core import ALL_POLICY, InputPolicy, Node, Ports
from .errors import GraphError, GraphFrozenError, GraphValidationError
from .events import Context, ContextFactory
from .policies import BoundPolicy, InputSelector, bind_policy

_G = TypeVar("_G", bound="Graph")  # 快捷构建保留调用方 Graph 子类的返回类型。


@dataclass(frozen=True, slots=True)
class Edge:
    """连接源 Node output port 和目标 Node input port。

    Attributes:
        source: 源节点 ID。
        target: 目标节点 ID。
        source_port: 源节点的输出端口，默认 ``default``。
        target_port: 目标节点的输入端口，默认 ``default``。
    """

    source: str
    target: str
    source_port: str = "default"
    target_port: str = "default"

    def __post_init__(self) -> None:
        """校验节点 ID 和端口名称都是非空字符串。"""

        require_non_empty_string(self.source, "edge source")
        require_non_empty_string(self.target, "edge target")
        require_non_empty_string(self.source_port, "edge source_port")
        require_non_empty_string(self.target_port, "edge target_port")


@dataclass(frozen=True, slots=True)
class NodeSpec:
    """Graph 冻结时保存的单个 Node 执行元数据。

    Attributes:
        input_ports: 冻结时读取的输入端口声明。
        output_ports: 冻结时读取的输出端口声明。
        input_policy: 冻结时绑定的策略与 selector 快照；没有输入端口的源节点为 None。
        timeout: 单次 firing 超时秒数，None 表示不限制。
    """

    input_ports: Ports
    output_ports: Ports
    input_policy: BoundPolicy | None
    timeout: float | None


_Outgoing = Mapping[
    tuple[str, str], tuple[Edge, ...]
]  # (节点 ID, 输出端口) 到有序下游边。


def _outgoing_index(edges: Iterable[Edge]) -> dict[tuple[str, str], tuple[Edge, ...]]:
    """按 (源节点, 源端口) 分组边，组内保持输入顺序。"""

    outgoing: dict[tuple[str, str], list[Edge]] = defaultdict(list)
    for edge in edges:
        outgoing[(edge.source, edge.source_port)].append(edge)
    return {key: tuple(value) for key, value in outgoing.items()}


def _reachable(entrypoint: str, edges: Iterable[Edge]) -> set[str]:
    """返回沿 edges 从 entrypoint 可达的节点 ID，包含入口本身。"""

    adjacency: dict[str, set[str]] = defaultdict(set)
    for edge in edges:
        adjacency[edge.source].add(edge.target)
    reached: set[str] = set()
    pending = [entrypoint]
    while pending:
        node_id = pending.pop()
        if node_id not in reached:
            reached.add(node_id)
            pending.extend(adjacency.get(node_id, ()))
    return reached


def _starved_all_node(
    specs: Mapping[str, NodeSpec],
    nodes: Iterable[str],
    entrypoint: str,
    edges: Iterable[Edge],
) -> tuple[str, list[str]] | None:
    """按 nodes 顺序查找首个有输入端口缺少入边的非入口 ALL 节点。

    Returns:
        节点 ID 与排序后的缺失端口；全部满足时返回 None。
    """

    incoming: dict[str, set[str]] = defaultdict(set)
    for edge in edges:
        incoming[edge.target].add(edge.target_port)
    for node_id in nodes:
        spec = specs[node_id]
        policy = spec.input_policy
        if node_id == entrypoint or policy is None or policy.policy.name != ALL_POLICY:
            continue
        missing = set(spec.input_ports) - incoming[node_id]
        if missing:
            return node_id, sorted(missing)
    return None


class ExecutionPlan:
    """一张冻结 Graph 的单次执行子图。

    构造参数是选择请求，属性是校验后的派生结果；实例不可变，按身份比较和哈希。
    需要不同选择时构造新的计划。

    Attributes:
        __slots__: 固定实例字段，全部通过只读属性暴露。
        _graph: 计划所属的冻结 Graph。
        _nodes: 所选节点 ID 的不可变集合。
        _entrypoint: 本次执行的入口节点 ID。
        _edges: 保留的原图连接，按原图顺序。
        _outputs: 没有计划内下游的 (节点 ID, 输出端口)。
        _cut_outputs: _outputs 中在原图有下游、因裁剪成为出口的端口。
        _boundary_edges: 源节点被选中但未保留的原图连接，按原图顺序。
        _outgoing: 由保留连接建立的只读下游索引。
    """

    __slots__ = (
        "_boundary_edges",
        "_cut_outputs",
        "_edges",
        "_entrypoint",
        "_graph",
        "_nodes",
        "_outgoing",
        "_outputs",
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
            graph: 已冻结的 Graph。
            nodes: 待执行节点 ID；复制为不可变集合后保存。
            entrypoint: 所选节点中的执行入口，None 使用原图入口。
            edges: 需要保留的原图连接，None 保留所选节点间全部连接。
            outputs: 预期的完整出口集合，None 不额外约束；不用于过滤输出。

        Raises:
            TypeError: graph 不是 Graph，nodes 是字符串或含非字符串成员，edges 含非 Edge，
                或 outputs 含非 (节点 ID, 端口) 二元组。
            ValueError: entrypoint 为空。
            GraphValidationError: Graph 未冻结；选择为空、含未知节点或不含入口；edges 不是
                所选节点间的原图连接；节点不可达；ALL 节点缺少输入；出口与 outputs 不符。
        """

        if not isinstance(graph, Graph):
            raise TypeError("execution plan graph must be a Graph")
        if not graph.frozen:
            raise GraphValidationError("execution plan requires a frozen Graph")
        selected = _select_nodes(graph, nodes)
        entrypoint = (
            graph.entrypoint
            if entrypoint is None
            else require_non_empty_string(entrypoint, "plan entrypoint")
        )
        if entrypoint not in selected:
            raise GraphValidationError(
                f"execution plan must contain entrypoint {entrypoint!r}"
            )
        retained = _retain_edges(graph, selected, edges)
        unreachable = selected - _reachable(entrypoint, retained)
        if unreachable:
            raise GraphValidationError(
                "execution plan nodes are unreachable from entrypoint using original "
                f"edges: {sorted(unreachable)!r}"
            )
        starved = _starved_all_node(
            graph._node_specs,
            (node_id for node_id in graph.nodes if node_id in selected),
            entrypoint,
            retained,
        )
        if starved is not None:
            raise GraphValidationError(
                f"execution plan leaves ALL node {starved[0]!r} without inputs: "
                f"{starved[1]!r}"
            )
        outgoing = _outgoing_index(retained)
        terminal = frozenset(
            (node_id, port)
            for node_id in selected
            for port in graph.spec_for(node_id).output_ports
            if (node_id, port) not in outgoing
        )
        if outputs is not None:
            _check_expected_outputs(terminal, outputs)
        retained_set = set(retained)
        self._graph = graph
        self._nodes = selected
        self._entrypoint = entrypoint
        self._edges = retained
        self._outputs = terminal
        self._cut_outputs = frozenset(
            value for value in terminal if graph.outgoing_for(*value)
        )
        self._boundary_edges = tuple(
            edge
            for edge in graph.edges
            if edge.source in selected and edge not in retained_set
        )
        self._outgoing: _Outgoing = MappingProxyType(outgoing)

    def __repr__(self) -> str:
        """返回包含入口与有序节点 ID 的调试表示。"""

        return (
            f"ExecutionPlan(entrypoint={self._entrypoint!r}, "
            f"nodes={sorted(self._nodes)!r})"
        )

    @property
    def graph(self) -> Graph:
        """返回计划所属的冻结 Graph。"""

        return self._graph

    @property
    def nodes(self) -> frozenset[str]:
        """返回所选节点 ID 的不可变集合。"""

        return self._nodes

    @property
    def entrypoint(self) -> str:
        """返回本次执行的入口节点 ID。"""

        return self._entrypoint

    @property
    def edges(self) -> tuple[Edge, ...]:
        """返回保留的原图连接，按原图顺序。"""

        return self._edges

    @property
    def outputs(self) -> frozenset[tuple[str, str]]:
        """返回所有没有计划内下游的 (节点 ID, 输出端口)。

        出口来自端口声明，不保证运行时一定产生输出。
        """

        return self._outputs

    @property
    def cut_outputs(self) -> frozenset[tuple[str, str]]:
        """返回 outputs 中在原图有下游、因裁剪才成为出口的端口。"""

        return self._cut_outputs

    @property
    def boundary_edges(self) -> tuple[Edge, ...]:
        """返回源节点被选中但未保留的原图连接，包括部分扇出裁剪，按原图顺序。"""

        return self._boundary_edges

    def outgoing_for(self, node_id: str, port: str) -> tuple[Edge, ...]:
        """返回计划内指定 output port 的有序下游连接。

        Args:
            node_id: 源节点 ID。
            port: 源输出端口。

        Returns:
            计划保留的下游连接；没有时为空元组。
        """

        return self._outgoing.get((node_id, port), ())

    def describe(self) -> dict[str, Any]:
        """返回独立的可 JSON 编码诊断快照，不运行节点或读取执行状态。

        Returns:
            入口及输入类型、所选节点和边、原有出口、裁剪出口与被裁剪连接。
            出口来自端口声明，不保证每个出口在实际运行中都会产生结果。
        """

        def connection(edge: Edge) -> dict[str, str]:
            """把一条连接转换为新的诊断字典。"""

            return {
                "source": edge.source,
                "source_port": edge.source_port,
                "target": edge.target,
                "target_port": edge.target_port,
            }

        return {
            "entrypoint": self._entrypoint,
            "inputs": {
                port: f"{kind.__module__}.{kind.__qualname__}"
                for port, kind in self._graph.spec_for(
                    self._entrypoint
                ).input_ports.items()
            },
            "nodes": [node for node in self._graph.nodes if node in self._nodes],
            "edges": [connection(edge) for edge in self._edges],
            "outputs": sorted(self._outputs),
            "original_outputs": sorted(self._outputs - self._cut_outputs),
            "cut_outputs": sorted(self._cut_outputs),
            "boundary_edges": [connection(edge) for edge in self._boundary_edges],
        }


def _select_nodes(graph: Graph, nodes: Iterable[str]) -> frozenset[str]:
    """把节点选择复制为非空 frozenset，并拒绝非法 ID 与未知节点。"""

    if isinstance(nodes, (str, bytes)):
        raise TypeError("plan nodes must be an iterable of node IDs")
    try:
        selected = frozenset(nodes)
    except TypeError as exc:
        raise TypeError("plan nodes must be an iterable of node IDs") from exc
    if not selected:
        raise GraphValidationError("execution plan must contain at least one node")
    if any(not isinstance(node_id, str) or not node_id.strip() for node_id in selected):
        raise TypeError("plan node IDs must be non-empty strings")
    unknown = selected.difference(graph.nodes)
    if unknown:
        raise GraphValidationError(
            f"execution plan references unknown nodes: {sorted(unknown)!r}"
        )
    return selected


def _retain_edges(
    graph: Graph,
    selected: frozenset[str],
    edges: Iterable[Edge] | None,
) -> tuple[Edge, ...]:
    """返回按原图顺序保留的所选节点间连接；edges 为 None 时保留全部。"""

    available = tuple(
        edge
        for edge in graph.edges
        if edge.source in selected and edge.target in selected
    )
    if edges is None:
        return available
    requested = tuple(edges)
    if any(not isinstance(edge, Edge) for edge in requested):
        raise TypeError("plan edges must contain Edge instances")
    requested_set = set(requested)
    invalid = requested_set.difference(available)
    if invalid:
        raise GraphValidationError(
            "plan edges must be original edges between selected nodes: "
            f"{sorted(map(repr, invalid))!r}"
        )
    return tuple(edge for edge in available if edge in requested_set)


def _check_expected_outputs(
    terminal: frozenset[tuple[str, str]],
    outputs: Iterable[tuple[str, str]],
) -> None:
    """校验调用方声明的完整出口与派生出口一致。"""

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


def _require_sync_factory(factory: ContextFactory) -> ContextFactory:
    """拒绝不可调用或声明为协程、异步生成器、生成器函数的 Context 工厂。"""

    if not callable(factory) or any(
        check(callback)
        for callback in (factory, type(factory).__call__)
        for check in (
            inspect.iscoroutinefunction,
            inspect.isasyncgenfunction,
            inspect.isgeneratorfunction,
        )
    ):
        raise TypeError("context_factory must be a synchronous callable")
    return factory


class Graph:
    """描述 Node 通过 typed Edge 传递数据的静态有向图。

    构建方法与 freeze 在同一把锁内检查并修改状态，并发构建与冻结不会让未校验的定义进入冻结图。

    Attributes:
        _lock: 串行化构建修改与冻结的可重入锁。
        _entrypoint: 入口节点 ID；未设置时为 None。
        _context_factory: 构造时固定的同步 Context 工厂。
        _nodes: 节点 ID 到 Node 实例的绑定，按加入顺序。
        _edges: 按声明顺序保存的边，dict 键同时用于拒绝重复边。
        _frozen_edges: 冻结时生成的边元组；冻结前为空。
        _outgoing: 冻结时建立的 (源节点, 源端口) 下游边索引。
        _node_specs: 冻结时保存的节点端口、策略和超时快照。
        _frozen: 是否已冻结；冻结后拒绝修改。
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
            TypeError: context_factory 不可调用或声明为异步函数、生成器函数；entrypoint 不是字符串。
            ValueError: entrypoint 为空。
        """

        self._lock = RLock()
        self._context_factory = _require_sync_factory(context_factory)
        self._entrypoint = (
            None
            if entrypoint is None
            else require_non_empty_string(entrypoint, "graph entrypoint")
        )
        self._nodes: dict[str, Node] = {}
        self._edges: dict[Edge, None] = {}
        self._frozen_edges: tuple[Edge, ...] = ()
        self._outgoing: _Outgoing = {}
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
            GraphError: 节点 ID 或 Edge 重复。
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
        """返回本图每次 Node firing 使用的同步 Context 工厂。"""

        return self._context_factory

    @property
    def entrypoint(self) -> str:
        """返回入口 Node ID。

        Raises:
            GraphError: Graph 尚未设置入口。
        """

        if self._entrypoint is None:
            raise GraphError("graph has no entrypoint")
        return self._entrypoint

    @property
    def frozen(self) -> bool:
        """返回 Graph 是否已经冻结。"""

        return self._frozen

    @property
    def nodes(self) -> Mapping[str, Node]:
        """返回节点 ID 到 Node 实例的只读视图，按加入顺序。"""

        return MappingProxyType(self._nodes)

    @property
    def edges(self) -> tuple[Edge, ...]:
        """返回按声明顺序排列的 Edge；冻结后返回同一个缓存元组。"""

        return self._frozen_edges if self._frozen else tuple(self._edges)

    def entry(self, node_id: str) -> Graph:
        """设置当前 Graph 的唯一入口。

        Args:
            node_id: 作为入口的 Node ID；冻结时才校验其存在。

        Returns:
            当前 Graph，便于链式构建。

        Raises:
            GraphFrozenError: Graph 已冻结。
            TypeError: node_id 不是字符串。
            ValueError: node_id 为空。
        """

        with self._lock:
            self._ensure_mutable()
            self._entrypoint = require_non_empty_string(node_id, "graph entrypoint")
        return self

    @overload
    def add(self, node_id: str, node: Node, /) -> Graph:
        """以位置参数绑定单个 Node。"""
        ...

    @overload
    def add(self, **nodes: Node) -> Graph:
        """以关键字名称作为 ID 绑定一组 Node。"""
        ...

    def add(self, *args: object, **nodes: Node) -> Graph:
        """把一个或多个 Node 行为绑定到 Graph 中的位置。

        一次调用中的绑定全部通过校验后才加入 Graph。

        Args:
            args: 单个 Node 的 ID 和 Node 实例。
            nodes: 以关键字名称作为 Node ID 的一组 Node 实例。

        Returns:
            当前 Graph，便于链式构建。

        Raises:
            GraphFrozenError: Graph 已冻结。
            TypeError: 参数形式不合法、节点 ID 不是字符串或值不是 Node。
            ValueError: 节点 ID 为空。
            GraphError: 节点 ID 已存在。
        """

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

        with self._lock:
            self._ensure_mutable()
            validated: dict[str, Node] = {}
            for raw_id, node in bindings:
                node_id = require_non_empty_string(raw_id, "node_id")
                if not isinstance(node, Node):
                    raise TypeError(f"node {node_id!r} must be a Node")
                if node_id in self._nodes:
                    raise GraphError(f"duplicate node {node_id!r}")
                validated[node_id] = node
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
        """增加一条端口到端口的有向连接；节点与端口在冻结时校验。

        Args:
            source: 源 Node ID。
            target: 目标 Node ID。
            source_port: 源 output port。
            target_port: 目标 input port。

        Returns:
            当前 Graph，便于链式构建。

        Raises:
            GraphFrozenError: Graph 已冻结。
            TypeError: 任一 ID 或端口不是字符串。
            ValueError: 任一 ID 或端口为空。
            GraphError: 相同连接已存在。
        """

        edge = Edge(source, target, source_port, target_port)
        with self._lock:
            self._ensure_mutable()
            if edge in self._edges:
                raise GraphError(f"duplicate edge {edge!r}")
            self._edges[edge] = None
        return self

    def plan(
        self,
        *,
        include: Iterable[str],
        entrypoint: str | None = None,
        edges: Iterable[Edge] | None = None,
        outputs: Iterable[tuple[str, str]] | None = None,
        selectors: Mapping[str, InputSelector] | None = None,
    ) -> ExecutionPlan:
        """选择由原有节点和边组成的严格执行子图。

        未选节点不会执行，也不会自动连接其前后节点。Graph 尚未冻结时先用 selectors
        冻结，保证计划引用的定义之后不再变化；已冻结的 Graph 保留冻结时绑定的策略，
        此时传入 selectors 会被拒绝。

        Args:
            include: 计划保留的节点 ID 集合。
            entrypoint: 所选节点中的执行入口，None 使用原图入口。
            edges: 明确保留的原图连接，None 使用所选节点间全部原有连接。
            outputs: 预期完整出口集合，None 不额外校验；不改变输出交付。
            selectors: 冻结未冻结 Graph 时使用的具名 selector 快照，None 只使用内建策略。

        Returns:
            新建的 ExecutionPlan。

        Raises:
            ValueError: Graph 已冻结且 selectors 不为 None，或 entrypoint 为空。
            TypeError: selectors 不是映射，或计划参数类型不合法。
            GraphValidationError: 冻结校验失败，或节点选择不满足 ExecutionPlan 约束。
        """

        with self._lock:
            if not self._frozen:
                self.freeze(selectors)
            elif selectors is not None:
                raise ValueError(
                    "selectors cannot be applied to an already frozen Graph"
                )
        return ExecutionPlan(
            self, include, entrypoint=entrypoint, edges=edges, outputs=outputs
        )

    def freeze(self, selectors: Mapping[str, InputSelector] | None = None) -> Graph:
        """校验并冻结 Graph；已冻结时直接返回，不重新绑定策略。

        Args:
            selectors: 按名称查找具名输入策略 selector 的快照，None 只使用内建策略；
                Runtime 传入 ``hooks.provided()``。

        Returns:
            已冻结的当前 Graph。

        Raises:
            TypeError: selectors 不是映射。
            GraphValidationError: 节点、端口、策略、连接或可达性不合法。
        """

        with self._lock:
            if self._frozen:
                return self
            if selectors is None:
                selectors = {}
            if not isinstance(selectors, Mapping):
                raise TypeError("selectors must be a mapping or None")
            self._node_specs = self._validate_structure(
                MappingProxyType(dict(selectors))
            )
            self._frozen_edges = tuple(self._edges)
            self._outgoing = MappingProxyType(_outgoing_index(self._frozen_edges))
            self._frozen = True
        return self

    def spec_for(self, node_id: str) -> NodeSpec:
        """返回冻结后的 Node 执行元数据。

        Args:
            node_id: Graph 内绑定的节点 ID。

        Returns:
            指定节点在冻结时保存的执行元数据。

        Raises:
            GraphError: Graph 尚未冻结。
            KeyError: 节点 ID 不存在。
        """

        self._ensure_frozen()
        return self._node_specs[node_id]

    def _execution_timeouts(self) -> Mapping[str, float | None]:
        """返回冻结时各节点的超时秒数只读快照。"""

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
            按定义顺序排列的 Edge；没有时为空元组。

        Raises:
            GraphError: Graph 尚未冻结。
        """

        self._ensure_frozen()
        return self._outgoing.get((node_id, port), ())

    def _validate_structure(
        self, selectors: Mapping[str, InputSelector]
    ) -> dict[str, NodeSpec]:
        """执行冻结前的完整静态校验，返回各节点的执行元数据。

        Raises:
            GraphValidationError: 节点、端口、策略、连接、可达性或 ALL 输入不合法。
        """

        if not self._nodes:
            raise GraphValidationError("graph must contain at least one node")
        if self._entrypoint not in self._nodes:
            raise GraphValidationError("graph entrypoint must reference a node")
        specs = {
            node_id: self._node_spec(node_id, node, selectors)
            for node_id, node in self._nodes.items()
        }
        self._validate_edges(specs)
        entrypoint = self.entrypoint
        unreachable = self._nodes.keys() - _reachable(entrypoint, self._edges)
        if unreachable:
            raise GraphValidationError(
                f"nodes are unreachable from entrypoint: {sorted(unreachable)!r}"
            )
        starved = _starved_all_node(specs, specs, entrypoint, self._edges)
        if starved is not None:
            raise GraphValidationError(
                f"ALL node {starved[0]!r} has no incoming edge for inputs: "
                f"{starved[1]!r}"
            )
        return specs

    @staticmethod
    def _node_spec(
        node_id: str, node: Node, selectors: Mapping[str, InputSelector]
    ) -> NodeSpec:
        """读取并校验单个 Node 的端口、超时与输入策略，生成冻结快照。"""

        inputs = Graph._ports(node_id, "inputs", node.inputs)
        outputs = Graph._ports(node_id, "outputs", node.outputs)
        policy = node.input_policy
        timeout = node.timeout
        if not isinstance(policy, InputPolicy):
            raise GraphValidationError(
                f"node {node_id!r} input_policy must be InputPolicy"
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
        if not inputs:
            return NodeSpec(inputs, outputs, None, timeout)
        try:
            bound_policy = bind_policy(policy, selectors)
        except (TypeError, ValueError) as exc:
            raise GraphValidationError(
                f"node {node_id!r} has invalid input policy: {exc}"
            ) from exc
        return NodeSpec(inputs, outputs, bound_policy, timeout)

    @staticmethod
    def _ports(node_id: str, label: str, declared: object) -> Ports:
        """把节点声明的端口映射转换为冻结快照中的不可变端口集合。"""

        if not isinstance(declared, Mapping) or not all(
            isinstance(name, str) for name in declared
        ):
            raise GraphValidationError(
                f"node {node_id!r} {label} must be a mapping of port names to types"
            )
        try:
            return Ports(**declared)
        except (TypeError, ValueError) as exc:
            raise GraphValidationError(f"node {node_id!r} {label}: {exc}") from exc

    def _validate_edges(self, specs: Mapping[str, NodeSpec]) -> None:
        """校验每条边引用已有节点、已声明端口，且源类型兼容目标类型。"""

        for edge in self._edges:
            if edge.source not in specs or edge.target not in specs:
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

    def _ensure_mutable(self) -> None:
        """Graph 已冻结时抛出 GraphFrozenError；调用方须持有 _lock。"""

        if self._frozen:
            raise GraphFrozenError("graph is frozen")

    def _ensure_frozen(self) -> None:
        """Graph 尚未冻结时抛出 GraphError。"""

        if not self._frozen:
            raise GraphError("graph is not frozen")
