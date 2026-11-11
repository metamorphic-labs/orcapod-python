"""Run pytest with a stand-in for starfix 0.5's ordered ``hash_table`` (ITL-897).

starfix-python 0.5.0 (hash format 2, ITL-898) adds keyword-only ``ordered_columns`` and
``ordered_struct_fields`` to ``ArrowDigester.hash_table``. Until it is on PyPI, this script wraps
the installed 0.4 ``hash_table`` in its own process only: called without the new keywords it is
unchanged; called with either of them it returns a stand-in digest that depends on the format-1
digest, on the flags, and on the top-level column order and the struct field order at every
depth, which is the behaviour the 0.5 options promise. It is NOT the format-2 byte layout, so
digests and golden values under it are meaningless; only equalities and inequalities are.

Run: ``uv run python superpowers/scripts/itl_897/starfix05_shim.py [pytest args]``
"""

from __future__ import annotations

import hashlib
import json
import sys
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
        raise ValueError("duplicate node path")  # format 2 rejects these with options on
    order = json.dumps(
        {
            "columns": columns if ordered_columns else sorted(columns),
            "flags": [ordered_columns, ordered_struct_fields],
        }
    ).encode()
    return b"\x00\x00\x02" + hashlib.sha256(base + order).digest()


ArrowDigester.hash_table = staticmethod(_hash_table)  # type: ignore[method-assign]


if __name__ == "__main__":
    sys.exit(pytest.main(sys.argv[1:] or ["-q", "-p", "no:cacheprovider"]))
