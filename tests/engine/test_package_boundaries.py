"""包依赖方向的契约测试：按源码静态导入检查各层只依赖允许的下层。"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import interlace

# 包根目录，即 interlace/。
ROOT = Path(interlace.__file__).parent

# 各层允许导入的其他层；"" 表示顶层 interlace 包，_validation 是共享的私有校验工具。
ALLOWED: dict[str, frozenset[str]] = {
    "": frozenset({"engine", "runtime"}),
    "engine": frozenset({"_validation"}),
    "spi": frozenset({"_validation", "engine"}),
    "adapters": frozenset({"_validation", "engine", "spi"}),
    "plugins": frozenset({"_validation", "engine", "spi"}),
    "nodes": frozenset({"_validation", "engine"}),
    "runtime": frozenset({"_validation", "engine", "spi", "plugins", "adapters"}),
    "service": frozenset({"", "engine", "plugins", "spi"}),
}


def _layer(module: str) -> str:
    """返回模块所属的第一级子包名；顶层包与 _validation 之外的顶层模块返回其名称。

    Args:
        module: 以 ``interlace`` 开头的完整模块名。
    """

    parts = module.split(".")
    return parts[1] if len(parts) > 1 else ""


def _imports(path: Path) -> set[str]:
    """解析文件中全部指向 interlace 的导入，相对导入按文件位置还原为绝对模块名。

    Args:
        path: interlace 包内的源码文件。

    Returns:
        被导入的 interlace 模块全名集合。
    """

    module = ".".join(path.relative_to(ROOT.parent).with_suffix("").parts)
    package = module.removesuffix(".__init__")
    if path.name != "__init__.py":
        package = package.rpartition(".")[0]
    found: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = package.split(".")[: len(package.split(".")) - node.level + 1]
                found.add(".".join([*base, *([node.module] if node.module else [])]))
            elif node.module:
                found.add(node.module)
    return {name for name in found if name.split(".")[0] == "interlace"}


def _sources() -> list[Path]:
    """返回包内全部 Python 源码文件。"""

    return sorted(ROOT.rglob("*.py"))


@pytest.mark.parametrize(
    "path", _sources(), ids=lambda path: str(path.relative_to(ROOT))
)
def test_package_imports_follow_dependency_direction(path: Path) -> None:
    """每个模块只导入本层及允许的下层；核心各层不导入 service，plugins 不反向依赖 runtime。

    Args:
        path: 待检查的源码文件。
    """

    module = ".".join(path.relative_to(ROOT.parent).with_suffix("").parts)
    own = _layer(module.removesuffix(".__init__"))
    if own == "_validation":
        assert _imports(path) == set()
        return
    allowed = ALLOWED[own] | {own}
    violations = sorted(name for name in _imports(path) if _layer(name) not in allowed)
    assert violations == []
