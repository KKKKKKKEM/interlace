"""Graph execution 领域配置的快照规则。"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from .._validation import require_non_empty_string


def snapshot_options(options: Mapping[str, Any] | None) -> dict[str, Any]:
    """复制执行配置，避免调用方在提交后修改配置。

    Args:
        options: 使用领域命名空间键的配置映射，None 表示空配置。

    Returns:
        与输入独立的深复制字典。

    Raises:
        TypeError: 配置不是映射，或配置值无法深复制。
        ValueError: 配置键不是非空字符串。
    """

    if options is None:
        return {}
    if not isinstance(options, Mapping):
        raise TypeError("options must be a mapping or None")
    for key in options:
        require_non_empty_string(key, "option key")
    return deepcopy(dict(options))
