"""Graph 节点模型：Node、Output、输入策略与内部端口集合。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Iterable, Iterator, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, ClassVar, get_origin

from .._validation import require_namespaced, require_non_empty_string

if TYPE_CHECKING:
    from .events import Context

ALL_POLICY = "interlace.core/all"  # 全部输入端口都有 token 时触发的内建 selector 名称。
ANY_POLICY = "interlace.core/any"  # 首个非空端口优先的内建 selector 名称。
LONGEST_POLICY = (
    "interlace.core/longest"  # 可用 token 最多的端口优先的内建 selector 名称。
)


def _freeze_config(value: Any) -> Any:
    """递归深复制策略配置，并把映射、列表/元组、集合分别转为只读映射、元组、frozenset。"""

    if isinstance(value, Mapping):
        return MappingProxyType(
            {deepcopy(key): _freeze_config(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_config(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze_config(item) for item in value)
    return deepcopy(value)


@dataclass(frozen=True, slots=True)
class InputPolicy:
    """按名称引用的输入选择策略及其只读参数。

    内建策略为 ``InputPolicy.ALL``（默认）、``InputPolicy.ANY`` 与 ``InputPolicy.LONGEST``；
    自定义策略以命名空间名称引用 Runtime 中提供的同名 selector。没有输入端口的节点在入口处触发一次，
    不需要也不读取输入策略。按值比较 name 与 config；config 冻结为只读映射，无法稳定哈希，因此实例不可哈希。

    Attributes:
        name: selector 的命名空间名称。
        config: 构造时深复制并递归冻结的策略参数映射。
        ALL: 全部输入端口都有 token 时各消费一个。
        ANY: 每次只消费声明顺序中第一个非空端口。
        LONGEST: 每次只消费可用 token 最多的端口，相同时按声明顺序。
    """

    name: str
    config: Mapping[str, Any] = field(default_factory=dict)

    ALL: ClassVar[InputPolicy]
    ANY: ClassVar[InputPolicy]
    LONGEST: ClassVar[InputPolicy]
    __hash__: ClassVar[None] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        """校验名称并冻结配置副本。

        Raises:
            TypeError: name 不是字符串或 config 不是映射。
            ValueError: name 未带命名空间。
        """

        require_namespaced(self.name, "input policy name")
        if not isinstance(self.config, Mapping):
            raise TypeError("input policy config must be a mapping")
        object.__setattr__(self, "config", _freeze_config(self.config))


InputPolicy.ALL = InputPolicy(ALL_POLICY)
InputPolicy.ANY = InputPolicy(ANY_POLICY)
InputPolicy.LONGEST = InputPolicy(LONGEST_POLICY)


class Ports(Mapping[str, type[Any]]):
    """Graph 冻结时由节点声明的端口映射生成的不可变端口集合。

    节点以普通映射声明端口，例如 ``inputs = {"url": str}``；本类型只在冻结快照中使用。

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
        inputs: 输入端口名称到普通 Python 类的映射，按声明顺序排列；默认一个 ``default`` 端口，
            空映射表示没有输入的源节点，作为入口时在启动时触发一次。
        outputs: 输出端口名称到普通 Python 类的映射；默认一个 ``default`` 端口。
        input_policy: 仅依据端口和 token 数量生效的输入策略，默认 ``InputPolicy.ALL``。
        timeout: 超时秒数，None 表示不限制。
    """

    inputs: Mapping[str, type[Any]] = MappingProxyType({"default": object})
    outputs: Mapping[str, type[Any]] = MappingProxyType({"default": object})
    input_policy: InputPolicy = InputPolicy.ALL
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
