"""跨包共享的参数校验辅助函数，不属于公共 API。"""

from __future__ import annotations

import math


def require_non_empty_string(value: object, label: str) -> str:
    """校验一个值是非空字符串。

    Args:
        value: 待校验的值，不能是空串或全空白。
        label: 校验失败时用于指明字段的说明名称。

    Returns:
        通过校验的原始字符串。

    Raises:
        TypeError: value 不是字符串。
        ValueError: value 为空串或全空白。
    """

    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string")
    if not value.strip():
        raise ValueError(f"{label} must not be empty")
    return value


def require_namespaced(value: object, label: str) -> str:
    """校验带 ``/`` 命名空间且不含空白的标识，例如 ``"acme/name"``。

    Args:
        value: 待校验的值。
        label: 校验失败时用于指明字段的说明名称。

    Returns:
        通过校验的原始字符串。

    Raises:
        TypeError: value 不是字符串。
        ValueError: value 为空、缺少命名空间、以 ``/`` 开头或结尾，或包含空白。
    """

    value = require_non_empty_string(value, label)
    if "/" not in value or value.startswith("/") or value.endswith("/"):
        raise ValueError(f"{label} must be namespaced, for example 'acme/name'")
    if any(char.isspace() for char in value):
        raise ValueError(f"{label} must not contain whitespace")
    return value


def validate_timeout(timeout: float | None) -> None:
    """校验等待接口使用的超时参数。

    Args:
        timeout: 等待时限，单位秒；None 表示不限制。

    Raises:
        TypeError: timeout 不是数字或 None。
        ValueError: timeout 为负数或非有限值。
    """

    if timeout is None:
        return
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise TypeError("timeout must be a number or None")
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("timeout must be a finite non-negative number")
