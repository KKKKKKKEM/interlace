"""插件设置声明、宿主校验与 JSON Schema 的契约测试。"""

from __future__ import annotations

import dataclasses
from typing import Any, Literal

import pytest

from interlace import Runtime
from interlace.plugins import PluginDescriptor, plugin, settings_schema


@dataclasses.dataclass(frozen=True, slots=True)
class Tuning:
    """测试用插件设置。

    Attributes:
        mode: 运行模式。
        limit: 并发上限。
        ratio: 采样比例。
        verbose: 是否输出详细日志。
    """

    mode: Literal["fast", "safe"] = "safe"
    limit: int = dataclasses.field(
        default=4, metadata={"minimum": 1, "maximum": 8, "title": "Limit"}
    )
    ratio: float = 0.5
    verbose: bool = False


def recorder(plugin_id: str, seen: dict[str, Any], **kwargs: Any) -> Any:
    """构造把 setup 收到的设置记录到 seen 的函数插件。

    Args:
        plugin_id: 插件标识。
        seen: 插件标识到 ``api.settings`` 的记录。
        **kwargs: 传给 `plugin` 的其余声明。

    Returns:
        函数插件。
    """

    @plugin(plugin_id, **kwargs)
    def setup(api: Any) -> None:
        """记录设置。"""

        seen[plugin_id] = api.settings

    return setup


def test_host_passes_validated_settings_and_defaults_to_plugins() -> None:
    """宿主按声明校验配置并交给插件：int 提升为 float，未覆盖字段取默认值，未声明设置时为 None。"""

    seen: dict[str, Any] = {}
    with Runtime(
        plugins=[
            recorder("example/tuned", seen, settings=Tuning),
            recorder("example/default", seen, settings=Tuning),
            recorder("example/plain", seen),
        ],
        settings={"example/tuned": {"mode": "fast", "limit": 8, "ratio": 1}},
    ) as runtime:
        runtime.plugin_host.install(
            recorder("example/late", seen, settings=Tuning), settings={"limit": 2}
        )

    assert seen["example/tuned"] == Tuning(mode="fast", limit=8, ratio=1.0)
    assert type(seen["example/tuned"].ratio) is float
    assert seen["example/default"] == Tuning()
    assert seen["example/plain"] is None
    assert seen["example/late"] == Tuning(limit=2)


@pytest.mark.parametrize(
    ("settings", "error", "match"),
    [
        ({"example/absent": {}}, ValueError, "unknown plugins"),
        ({"example/plain": {}}, ValueError, "declares no settings"),
        ({"example/tuned": {"other": 1}}, ValueError, "unknown settings"),
        ({"example/tuned": {"limit": 9}}, ValueError, "at most 8"),
        ({"example/tuned": {"limit": 0}}, ValueError, "at least 1"),
        ({"example/tuned": {"mode": "slow"}}, ValueError, "one of"),
        ({"example/tuned": {"limit": True}}, TypeError, "must be int"),
        ({"example/tuned": {"ratio": float("nan")}}, ValueError, "finite"),
        ({"example/tuned": ["limit"]}, TypeError, "mapping"),
        (["example/tuned"], TypeError, "mapping"),
    ],
)
def test_invalid_settings_fail_before_any_plugin_setup(
    settings: Any, error: type[Exception], match: str
) -> None:
    """任何非法配置都在 Runtime 可用前失败，且不执行任何插件 setup。"""

    seen: dict[str, Any] = {}
    with pytest.raises(error, match=match):
        Runtime(
            plugins=[
                recorder("example/tuned", seen, settings=Tuning),
                recorder("example/plain", seen),
            ],
            settings=settings,
        )
    assert seen == {}


def test_failed_install_settings_do_not_run_setup() -> None:
    """运行期安装的非法设置在 setup 前失败，标识不被占用。"""

    seen: dict[str, Any] = {}
    with Runtime() as runtime:
        late = recorder("example/late", seen, settings=Tuning)
        with pytest.raises(ValueError, match="at least 1"):
            runtime.plugin_host.install(late, settings={"limit": 0})
        with pytest.raises(ValueError, match="declares no settings"):
            runtime.plugin_host.install(recorder("example/plain", seen), settings={})
        assert seen == {}
        runtime.plugin_host.install(late)
    assert seen == {"example/late": Tuning()}


@dataclasses.dataclass
class Mutable:
    """未冻结的设置类型。

    Attributes:
        value: 任意值。
    """

    value: int = 1


@dataclasses.dataclass(frozen=True)
class NoDefault:
    """缺少默认值的设置类型。

    Attributes:
        value: 无默认值的字段。
    """

    value: int


@dataclasses.dataclass(frozen=True)
class Nested:
    """含不受支持字段类型的设置类型。

    Attributes:
        value: 列表字段。
    """

    value: list[int] = dataclasses.field(default_factory=list)


@dataclasses.dataclass(frozen=True)
class BadDefault:
    """默认值越界的设置类型。

    Attributes:
        value: 默认值低于 minimum 的字段。
    """

    value: int = dataclasses.field(default=0, metadata={"minimum": 1})


@dataclasses.dataclass(frozen=True)
class RangeOnText:
    """把范围元数据用于字符串的设置类型。

    Attributes:
        value: 带 maximum 的字符串字段。
    """

    value: str = dataclasses.field(default="", metadata={"maximum": 3})


@pytest.mark.parametrize(
    ("settings_type", "error", "match"),
    [
        (dict, TypeError, "dataclass type"),
        (Mutable, TypeError, "frozen"),
        (NoDefault, ValueError, "default"),
        (Nested, TypeError, "bool, int, float, str or Literal"),
        (BadDefault, ValueError, "at least 1"),
        (RangeOnText, ValueError, "int or float only"),
    ],
)
def test_descriptor_rejects_invalid_settings_types(
    settings_type: type, error: type[Exception], match: str
) -> None:
    """设置类型声明在插件声明构造时校验。"""

    with pytest.raises(error, match=match):
        PluginDescriptor("example/bad", settings=settings_type)


def test_settings_schema_follows_declaration_order_and_metadata() -> None:
    """Schema 按字段声明顺序给出类型、默认值、候选值与元数据，并拒绝未声明的键。"""

    assert settings_schema(Tuning) == {
        "type": "object",
        "title": "Tuning",
        "properties": {
            "mode": {"type": "string", "default": "safe", "enum": ["fast", "safe"]},
            "limit": {
                "type": "integer",
                "default": 4,
                "title": "Limit",
                "minimum": 1,
                "maximum": 8,
            },
            "ratio": {"type": "number", "default": 0.5},
            "verbose": {"type": "boolean", "default": False},
        },
        "additionalProperties": False,
    }
    assert list(settings_schema(Tuning)["properties"]) == [
        "mode",
        "limit",
        "ratio",
        "verbose",
    ]
