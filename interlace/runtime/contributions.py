"""插件 Hook 贡献在目标 Graph 注册前的延迟绑定。"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from ..engine.graph import Graph
from ..engine.hooks import HookHandle, HookPhase, NodeHook


@dataclass(eq=False, slots=True)
class _HookContribution:
    """一项插件 Hook 贡献及其在执行器上的安装句柄。

    Attributes:
        hook: 节点 Hook 对象或单阶段回调。
        phase: 函数 Hook 对应阶段，Hook 对象为 None。
        graph: 目标 Graph 注册名，None 作用于全部 Graph。
        node: 目标 Graph 内的节点 ID，None 作用于整张 Graph。
        handle: 已安装到执行器的句柄，尚未安装或已撤销时为 None。
    """

    hook: NodeHook | Callable[..., object]
    phase: HookPhase | str | None
    graph: str | None
    node: str | None
    handle: HookHandle | None = None


class _DeferredHooks:
    """保存目标 Graph 尚未注册的 Hook 贡献，并在注册时安装到执行器。

    本类不加锁，调用方在 Worker 状态锁内使用。

    Attributes:
        _install: 把一项贡献安装到执行器并返回句柄的回调。
        _pending: 按目标 Graph 注册名等待安装的贡献，列表保持贡献顺序。
    """

    def __init__(self, install: Callable[[_HookContribution], HookHandle]) -> None:
        """保存执行器安装回调。"""

        self._install = install
        self._pending: dict[str, list[_HookContribution]] = {}

    def defer(self, contribution: _HookContribution) -> None:
        """登记目标 Graph 尚未注册的贡献。"""

        assert contribution.graph is not None
        self._pending.setdefault(contribution.graph, []).append(contribution)

    def install(self, contribution: _HookContribution) -> None:
        """立即安装目标 Graph 已注册或作用于全部 Graph 的贡献。"""

        contribution.handle = self._install(contribution)

    def bind(self, name: str, graph: Graph) -> None:
        """为即将注册的 Graph 安装全部延迟贡献，任一失败时回滚本次已安装项。

        Raises:
            ValueError: 某项贡献指向 Graph 中不存在的节点。
        """

        pending = tuple(self._pending.get(name, ()))
        for contribution in pending:
            if contribution.node is not None and contribution.node not in graph.nodes:
                raise ValueError(f"graph {name!r} has no node {contribution.node!r}")
        installed: list[_HookContribution] = []
        try:
            for contribution in pending:
                self.install(contribution)
                installed.append(contribution)
        except BaseException:
            for contribution in reversed(installed):
                assert contribution.handle is not None
                contribution.handle.detach()
                contribution.handle = None
            raise
        self._pending.pop(name, None)

    def withdraw(self, contribution: _HookContribution) -> None:
        """撤销尚未安装的登记或已经安装的执行器句柄；重复调用无效果。"""

        if contribution.graph is not None:
            pending = self._pending.get(contribution.graph)
            if pending is not None and contribution in pending:
                pending.remove(contribution)
                if not pending:
                    del self._pending[contribution.graph]
        if contribution.handle is not None:
            contribution.handle.detach()
            contribution.handle = None
