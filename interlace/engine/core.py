"""Graph 节点模型：Ports、InputPolicy、Node 和 Output。"""

from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, get_origin

from .._validation import require_non_empty_string

if TYPE_CHECKING:
    from .events import Context
    from .policies import PolicyRef


class InputPolicy(str, enum.Enum):
    """Node 的固定输入触发策略。

    Attributes:
        ALL: 所有声明输入端口都有 token 时触发。
        ANY: 任一输入端口有 token 时触发。
        ON_START: Graph 开始时触发一次，不消费端口 token。
    """

    ALL = "all"
    ANY = "any"
    ON_START = "on_start"


class Ports(Mapping[str, type[Any]]):
    """保存不可变的端口名称与 Python 类型映射。

    Attributes:
        __slots__: 实例允许保存的字段名称，限制动态增加属性。
        _types: 端口名称到数据类型的只读映射。
    """

    __slots__ = ("_types",)

    def __init__(self, /, **ports: type[Any]) -> None:
        """按关键字参数的声明顺序创建端口集合。

        ``self`` 是仅限位置参数，因此 ``self`` 也可以作为端口名称。

        Args:
            **ports: 端口名称到普通 Python 类的映射。

        Raises:
            TypeError: 端口类型不是普通 Python 类。
            ValueError: 端口名称为空或全空白。
        """

        normalized: dict[str, type[Any]] = {}
        for name, data_type in ports.items():
            require_non_empty_string(name, "port name")
            normalized[name] = self.require_type(
                data_type,
                f"port {name!r} type",
            )
        self._types = MappingProxyType(normalized)

    @staticmethod
    def require_type(value: object, label: str) -> type[Any]:
        """校验端口声明使用普通 Python class。

        Args:
            value: 待校验的端口数据类型。
            label: 校验失败时用于指明字段的说明名称。

        Returns:
            原样返回通过校验的类。

        Raises:
            TypeError: value 是 Any、泛型别名、Protocol 或不是类。
        """

        if (
            value is Any
            or not isinstance(value, type)
            or get_origin(value) is not None
            or getattr(value, "_is_protocol", False)
        ):
            raise TypeError(f"{label} must be a Python class")
        return value

    @staticmethod
    def is_type_compatible(
        source_type: type[Any],
        target_type: type[Any],
    ) -> bool:
        """判断源端口类型能否安全传给目标端口，即源类型是目标类型的子类。

        Args:
            source_type: 源端口声明的数据类型。
            target_type: 目标端口要求的数据类型。

        Returns:
            source_type 是 target_type 的子类时返回 True。

        Raises:
            TypeError: 任一参数不是普通 Python 类。
        """

        source_type = Ports.require_type(source_type, "source_type")
        target_type = Ports.require_type(target_type, "target_type")
        return issubclass(source_type, target_type)

    def __getitem__(self, port: str) -> type[Any]:
        """返回指定端口声明的数据类型。

        Args:
            port: 端口名称。

        Returns:
            该端口声明的 Python 类。

        Raises:
            KeyError: 端口未声明。
        """

        return self._types[port]

    def __iter__(self) -> Iterator[str]:
        """按声明顺序遍历端口名称。"""

        return iter(self._types)

    def __len__(self) -> int:
        """返回端口数量。"""

        return len(self._types)


@dataclass(frozen=True, slots=True)
class Output:
    """Node 在当前 Graph 内从命名端口产生的一项值。

    Attributes:
        value: 输出携带的数据值，执行器按端口声明校验类型。
        port: 发出该值的输出端口名称，默认 ``default``。
    """

    value: Any
    port: str = "default"

    def __post_init__(self) -> None:
        """校验输出端口名称。"""

        require_non_empty_string(self.port, "output port")


class Node(ABC):
    """统一声明同步或异步行为的图节点。

    execute 可使用 def 或 async def。默认执行器在 Graph 工作线程调用普通方法，
    将返回的 Awaitable 交给后台事件循环等待，再逐项处理最终输出。

    Attributes:
        input_ports: 节点声明的输入端口及其类型。
        output_ports: 节点声明的输出端口及其类型。
        input_policy: 仅依据端口和 token 数量生效的输入策略。
        timeout: 超时秒数，None 表示不限制。
    """

    input_ports = Ports(default=object)
    output_ports = Ports(default=object)
    input_policy: InputPolicy | PolicyRef = InputPolicy.ALL
    timeout: float | None = None

    @abstractmethod
    def execute(
        self,
        inputs: Mapping[str, Any],
        context: Context,
    ) -> Output | Iterable[Output] | Awaitable[Output | Iterable[Output] | None] | None:
        """执行一次节点行为，直接返回结果或返回可等待的结果。

        Args:
            inputs: 本次 firing 按输入端口名称选中的只读值映射。
            context: Graph 的 Context 工厂为本次 firing 返回的上下文。

        Returns:
            Output、惰性 Iterable[Output]、None，或得到这些结果的 Awaitable。
            每项输出必须符合声明端口契约；不接受异步生成器。
        """
