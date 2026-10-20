"""插件设置声明：以冻结 dataclass 声明默认值，宿主据此校验配置并生成 JSON Schema。

设置只允许 bool、int、float、str 与由这些标量组成的 Literal；配置是可序列化的普通数据，
由宿主保存、校验并交给插件，而不是隐藏在插件构造参数里。字段元数据支持 ``title``、
``description``，数值字段另支持 ``minimum`` 与 ``maximum``。
"""

from __future__ import annotations

import dataclasses
import math
import typing
from collections.abc import Mapping
from typing import Any, Literal

_JSON_TYPES: dict[type, str] = {
    bool: "boolean",
    int: "integer",
    float: "number",
    str: "string",
}
_TEXT_META = ("title", "description")
_RANGE_META = ("minimum", "maximum")


@dataclasses.dataclass(frozen=True, slots=True)
class _Spec:
    """一个设置字段的已解析规格。

    Attributes:
        field: dataclass 字段。
        kind: 标量类型；Literal 字段为各候选值共同的类型。
        choices: Literal 的候选值；非 Literal 字段为 None。
    """

    field: dataclasses.Field[Any]
    kind: type
    choices: tuple[Any, ...] | None


def _specs(settings_type: type) -> tuple[_Spec, ...]:
    """解析并校验设置类型的全部字段声明。

    Raises:
        TypeError: 不是冻结的 dataclass 类型，字段类型不受支持，或元数据类型不符。
        ValueError: 字段没有默认值、默认值非法，或元数据取值不合法。
    """

    if not isinstance(settings_type, type) or not dataclasses.is_dataclass(
        settings_type
    ):
        raise TypeError("plugin settings must be a dataclass type")
    if not settings_type.__dataclass_params__.frozen:  # type: ignore[attr-defined]
        raise TypeError("plugin settings dataclass must be frozen")
    hints = typing.get_type_hints(settings_type)
    result: list[_Spec] = []
    for field in dataclasses.fields(settings_type):
        spec = _spec(field, hints[field.name])
        if (
            field.default is dataclasses.MISSING
            or field.default_factory is not dataclasses.MISSING
        ):
            raise ValueError(f"setting {field.name!r} must have a plain default value")
        _check_meta(spec)
        _validate(spec, field.default)
        result.append(spec)
    return tuple(result)


def _spec(field: dataclasses.Field[Any], hint: Any) -> _Spec:
    """把字段的类型注解解析为规格。"""

    if typing.get_origin(hint) is Literal:
        choices = typing.get_args(hint)
        kinds = {type(choice) for choice in choices}
        if len(kinds) != 1 or not kinds <= set(_JSON_TYPES):
            raise TypeError(
                f"setting {field.name!r} Literal must hold one kind of "
                "bool, int, float or str values"
            )
        return _Spec(field, kinds.pop(), choices)
    if hint not in _JSON_TYPES:
        raise TypeError(
            f"setting {field.name!r} must be bool, int, float, str or Literal"
        )
    return _Spec(field, hint, None)


def _check_meta(spec: _Spec) -> None:
    """校验字段元数据的键、类型与适用范围。"""

    name, meta = spec.field.name, spec.field.metadata
    unknown = set(meta) - {*_TEXT_META, *_RANGE_META}
    if unknown:
        raise ValueError(f"setting {name!r} has unknown metadata {sorted(unknown)!r}")
    for key in _TEXT_META:
        if key in meta and not isinstance(meta[key], str):
            raise TypeError(f"setting {name!r} {key} must be a string")
    for key in _RANGE_META:
        if key not in meta:
            continue
        if spec.kind not in (int, float) or spec.choices is not None:
            raise ValueError(f"setting {name!r} {key} applies to int or float only")
        bound = meta[key]
        if type(bound) not in (int, float) or not math.isfinite(bound):
            raise TypeError(f"setting {name!r} {key} must be a finite number")
    if "minimum" in meta and "maximum" in meta and meta["minimum"] > meta["maximum"]:
        raise ValueError(f"setting {name!r} minimum exceeds maximum")


def _validate(spec: _Spec, value: Any) -> Any:
    """校验并规范化单个值：拒绝 bool 冒充数字，int 可提升为 float。

    Raises:
        TypeError: 值的类型不符。
        ValueError: 值不在候选值或范围内。
    """

    name, kind = spec.field.name, spec.kind
    if kind is float and type(value) is int:
        value = float(value)
    if type(value) is not kind:
        raise TypeError(f"setting {name!r} must be {kind.__name__}")
    if spec.choices is not None:
        if value not in spec.choices:
            raise ValueError(f"setting {name!r} must be one of {spec.choices!r}")
        return value
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"setting {name!r} must be finite")
    meta = spec.field.metadata
    if "minimum" in meta and value < meta["minimum"]:
        raise ValueError(f"setting {name!r} must be at least {meta['minimum']}")
    if "maximum" in meta and value > meta["maximum"]:
        raise ValueError(f"setting {name!r} must be at most {meta['maximum']}")
    return value


def check_settings_type(settings_type: type) -> None:
    """校验设置类型的声明；插件声明构造时调用。

    Raises:
        TypeError: 不是冻结的 dataclass 类型，字段类型或元数据类型不受支持。
        ValueError: 字段缺少默认值、默认值或元数据取值不合法。
    """

    _specs(settings_type)


def build_settings(settings_type: type, values: Mapping[str, Any] | None = None) -> Any:
    """由默认值与用户配置构造设置实例；任何非法配置都在构造时失败。

    Args:
        settings_type: 已声明的设置 dataclass 类型。
        values: 用户覆盖的字段值；None 表示全部使用默认值。

    Returns:
        设置 dataclass 的实例。

    Raises:
        TypeError: values 不是映射，或某个值的类型不符。
        ValueError: values 含未声明的键，或某个值超出候选值与范围。
    """

    if values is None:
        values = {}
    elif not isinstance(values, Mapping):
        raise TypeError("plugin settings values must be a mapping")
    specs = _specs(settings_type)
    unknown = set(values) - {spec.field.name for spec in specs}
    if unknown:
        raise ValueError(f"unknown settings {sorted(unknown, key=str)!r}")
    return settings_type(
        **{
            spec.field.name: _validate(spec, values[spec.field.name])
            for spec in specs
            if spec.field.name in values
        }
    )


def settings_schema(settings_type: type) -> dict[str, Any]:
    """生成设置类型的 JSON Schema（object），字段按声明顺序排列，供界面渲染配置表单。

    Args:
        settings_type: 已声明的设置 dataclass 类型。

    Returns:
        含 properties 与每项 default 的 JSON Schema 字典；不允许未声明的键。

    Raises:
        TypeError: 设置类型声明不合法。
        ValueError: 设置类型声明不合法。
    """

    properties: dict[str, Any] = {}
    for spec in _specs(settings_type):
        field = spec.field
        prop: dict[str, Any] = {
            "type": _JSON_TYPES[spec.kind],
            "default": field.default,
        }
        if spec.choices is not None:
            prop["enum"] = list(spec.choices)
        for key in (*_TEXT_META, *_RANGE_META):
            if key in field.metadata:
                prop[key] = field.metadata[key]
        properties[field.name] = prop
    return {
        "type": "object",
        "title": settings_type.__name__,
        "properties": properties,
        "additionalProperties": False,
    }
