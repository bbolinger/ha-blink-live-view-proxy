"""Tests that every name the package imports from its own modules exists.

Written after 0.7.0-rc.4 shipped with `__init__.py` importing CONF_CLIP_RECORDING
and DEFAULT_CLIP_RECORDING from `.const`, which defines neither. That is an
ImportError on the first setup of the integration, and every check in this suite
passed anyway: nothing here imports the package, because importing it pulls in
Home Assistant, so the tests read the modules as text or lift single functions
out by AST and the module-level import statement never runs. `compileall` is no
help either, since a missing name in a from-import is a runtime failure and not
a syntax error.

So this resolves relative imports the way Python would, without executing
anything: parse every module, collect what it takes from its siblings, and check
each name is actually defined there. No Home Assistant, no network. Run from the
repo root:

    python tests/test_internal_imports.py
"""

from __future__ import annotations

import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
PACKAGE = ROOT / "custom_components" / "blink_liveview_proxy"

CHECKS = 0
FAILURES: list[str] = []


def check(condition: bool, name: str) -> None:
    global CHECKS
    CHECKS += 1
    if not condition:
        FAILURES.append(name)
        print(f"  FAIL {name}")


def defined_names(tree: ast.Module) -> set[str]:
    """Top-level names a module binds, by any means a from-import can reach."""
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                for sub in ast.walk(target):
                    if isinstance(sub, ast.Name):
                        names.add(sub.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            # A re-export is a real binding: `from .const import DOMAIN` in one
            # module makes DOMAIN importable from that module too.
            for alias in node.names:
                names.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(node, ast.If):
            # TYPE_CHECKING blocks and version guards still bind at module level.
            for inner in ast.walk(node):
                if isinstance(inner, (ast.Import, ast.ImportFrom)):
                    for alias in inner.names:
                        names.add(alias.asname or alias.name.split(".")[0])
    return names


def main() -> int:
    modules = sorted(PACKAGE.glob("*.py"))
    check(bool(modules), "the package has modules to check")

    trees = {path.stem: ast.parse(path.read_text()) for path in modules}
    exports = {stem: defined_names(tree) for stem, tree in trees.items()}

    pairs = 0
    for stem, tree in sorted(trees.items()):
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or node.level != 1:
                continue
            if node.module is None or node.module not in exports:
                # `from . import x` and imports of subpackages are out of scope.
                continue
            for alias in node.names:
                if alias.name == "*":
                    continue
                pairs += 1
                check(
                    alias.name in exports[node.module],
                    f"{stem}.py: .{node.module} defines {alias.name}",
                )

    check(pairs > 0, "at least one relative import was found to check")
    print(f"\nchecked {pairs} imported names across {len(modules)} modules")

    print(f"\n{CHECKS - len(FAILURES)}/{CHECKS} checks passed")
    if FAILURES:
        print("\nfailed:")
        for name in FAILURES:
            print(f"  {name}")
        return 1
    print("all passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
