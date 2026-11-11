"""Run pytest with a stand-in for starfix 0.5's ordered ``hash_table`` (ITL-897).

starfix-python 0.5.0 (hash format 2, ITL-898) adds keyword-only ``ordered_columns`` and
``ordered_struct_fields`` to ``ArrowDigester.hash_table``. Until it is on PyPI, this script wraps
the installed 0.4 ``hash_table`` in this process and, through a temporary ``.pth`` hook in this
worktree's venv (removed when the run ends), in every child Python process the tests start:
called without the new keywords or with both false it is
unchanged; called with either of them it returns a stand-in digest that depends on the format-1
digest, on the flags, and on the top-level column order and the struct field order at every
depth, which is the behaviour the 0.5 options promise. It is NOT the format-2 byte layout, so
digests and golden values under it are meaningless; only equalities and inequalities are.

Run: ``uv run python superpowers/scripts/itl_897/starfix05_shim.py [pytest args]``
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from starfix import ArrowDigester

_released = ArrowDigester.hash_table


def _order(arrow_type: pa.DataType, ordered_struct_fields: bool) -> Any:
    if pa.types.is_struct(arrow_type):
        fields = [
            [f.name, _order(f.type, ordered_struct_fields)]
            for f in arrow_type  # pyright: ignore[reportGeneralTypeIssues]
        ]
        return fields if ordered_struct_fields else sorted(fields)
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return [_order(arrow_type.value_type, ordered_struct_fields)]
    return None


def _hash_table(
    table: pa.Table,
    *,
    include_metadata: bool = False,
    ordered_columns: bool = False,
    ordered_struct_fields: bool = False,
) -> bytes:
    base = _released(table, include_metadata=include_metadata)
    if not (ordered_columns or ordered_struct_fields):
        return base
    columns = [[f.name, _order(f.type, ordered_struct_fields)] for f in table.schema]
    names = [n for n, _ in columns]
    if len(set(names)) != len(names):
        raise ValueError(
            "duplicate node path"
        )  # format 2 rejects these with options on
    order = json.dumps(
        {
            "columns": columns if ordered_columns else sorted(columns),
            "flags": [ordered_columns, ordered_struct_fields],
        }
    ).encode()
    return b"\x00\x00\x02" + hashlib.sha256(base + order).digest()


def install() -> None:
    """Replace ``ArrowDigester.hash_table`` with the stand-in in this process."""
    ArrowDigester.hash_table = staticmethod(_hash_table)  # type: ignore[method-assign]


_ENV = "ITL897_STARFIX05_STANDIN"
_PTH_NAME = "itl897_starfix05_standin.pth"


def _pth_path() -> Path:
    import sysconfig

    return Path(sysconfig.get_paths()["purelib"]) / _PTH_NAME


def _cover_child_processes() -> Path:
    """Make every child Python process of this venv install the stand-in too.

    Writes a ``.pth`` hook into this worktree's own venv that installs the stand-in only when
    the environment variable ``ITL897_STARFIX05_STANDIN`` names this script's directory. Child
    processes inherit the variable even when a test resets ``PYTHONPATH`` (the doc samples
    do). The caller removes the hook when the run ends.
    """
    hook = _pth_path()
    hook.write_text(
        "import os, sys; "
        f"d = os.environ.get({_ENV!r}); "
        "d and (sys.path.insert(0, d) or __import__('starfix05_shim').install())\n"
    )
    os.environ[_ENV] = str(Path(__file__).resolve().parent)
    return hook


if __name__ == "__main__":
    install()
    hook = _cover_child_processes()
    try:
        code = pytest.main(sys.argv[1:] or ["-q", "-p", "no:cacheprovider"])
    finally:
        hook.unlink(missing_ok=True)
    sys.exit(code)
