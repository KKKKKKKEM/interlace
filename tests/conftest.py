"""跨测试目录共享的装配夹具。"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from interlace.plugins import CAP_EVENT_ROUTER, CAP_GRAPH_WORKER, Plugin, plugin


@pytest.fixture
def roles_plugin() -> Callable[[Any, Any], Plugin]:
    """返回把现成 Router 与 Worker 作为插件提供给 Runtime 的工厂。

    Returns:
        接收 router 与 worker 并返回函数插件的工厂；角色由 Runtime 关闭，底层组件仍归调用方。
    """

    def make(router: Any, worker: Any) -> Plugin:
        """创建只提供两个角色的插件。

        Args:
            router: 满足 RouterRole 的事件角色。
            worker: 满足 WorkerRole 的执行角色。

        Returns:
            提供两个角色能力的函数插件。
        """

        @plugin("test/roles", provides=(CAP_EVENT_ROUTER, CAP_GRAPH_WORKER))
        def roles(api: Any) -> None:
            """提供注入的角色。

            Args:
                api: 绑定插件身份的扩展接口。
            """

            api.provide(CAP_EVENT_ROUTER, router)
            api.provide(CAP_GRAPH_WORKER, worker)

        return roles

    return make
