"""A ``LazyModule`` defers only a module that ``import orcapod`` does not load (ITL-1349).

Every attribute read through a ``LazyModule`` is a ``__getattr__`` call. On a module that
``import orcapod`` loads anyway (pyarrow, polars, numpy, pandas, asyncio) the wrapper defers
nothing and costs a call per attribute: 42.9 million of them in one warm workshop-scale
``job.run()``. Such a module is imported directly; the wrapper is kept for modules a plain
``import orcapod`` never loads (networkx, deltalake) and for the optional extras.
"""

from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path

import orcapod

SOURCE_ROOT = Path(orcapod.__file__).parent

# Modules of the optional extras (``orcapod[postgresql]``, ``[ray]``, ``[spiraldb]``):
# ``import orcapod`` loads psycopg when it is installed, but the library must import
# without them, so they stay wrapped.
OPTIONAL_EXTRAS = frozenset({"psycopg", "ray", "spiral"})


def _lazy_modules() -> dict[str, list[str]]:
    """Every module name given to ``LazyModule`` in the library, with where it is."""
    found: dict[str, list[str]] = {}
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        if path.name == "lazy_module.py":
            continue
        for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "LazyModule"
            ):
                assert node.args and isinstance(node.args[0], ast.Constant), (
                    f"{path}:{node.lineno}: LazyModule takes a literal module name"
                )
                where = f"{path.relative_to(SOURCE_ROOT)}:{node.lineno}"
                found.setdefault(str(node.args[0].value), []).append(where)
    return found


def _loaded_by_import_orcapod() -> set[str]:
    """The modules a fresh interpreter holds after ``import orcapod``."""
    script = "import json, sys; import orcapod; print(json.dumps(sorted(sys.modules)))"
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )
    return set(json.loads(result.stdout.splitlines()[-1]))


def test_no_lazy_module_wraps_a_module_import_orcapod_loads() -> None:
    lazy = _lazy_modules()
    loaded = _loaded_by_import_orcapod()
    wrapped_but_loaded = {
        name: lazy[name]
        for name in sorted(lazy)
        if name in loaded and name not in OPTIONAL_EXTRAS
    }
    assert not wrapped_but_loaded, (
        "import these directly, `import orcapod` loads them anyway: "
        f"{wrapped_but_loaded}"
    )


def test_the_scan_finds_the_lazy_modules_that_remain() -> None:
    # Guards the scan itself: a refactor that hid every call from it would pass the
    # test above vacuously.
    assert "networkx" in _lazy_modules()
