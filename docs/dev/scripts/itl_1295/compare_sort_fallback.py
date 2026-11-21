"""ITL-1295 part 2: the sort fallback's row order, before (main) and after (this branch).

usage:
    uv run python docs/dev/scripts/itl_1295/compare_sort_fallback.py audit
    uv run pytest -p compare_sort_fallback <test files>   (with this folder on PYTHONPATH;
        writes ``COMPARE_SORT_FALLBACK_OUT``, default ``sort_fallback_calls.json``)

``sort_by_tag_columns`` falls back to Polars when Arrow cannot sort (a list or a struct
tag). Before, Polars sorted the whole table (``old_order`` below, the code of main at
3d3dec21, given a row-number column); now it orders prepared keys and the rows are
taken (``arrow_utils._tag_order_by_polars``). Both are computed on the same table:

* as a pytest plugin, on every call the tests make (``_tag_order_by_polars`` wrapped);
* ``audit``: tables of three tag columns (the type under test in a list and in a
  struct, ``int64``, ``large_string``), in two tag orders, for every audit type.

Per call: ``same`` (one permutation), ``moved`` (rows ordered differently: listed with
the reason found: a null inside a list or struct; for a table Arrow sorts itself,
whether the new order is Arrow's), ``main raised`` (Polars refused or panicked)
or ``main wrong`` (main's sort ran but changed a value in ANY column of the table, a
key or not: the changed columns are listed; the order is not compared). Each call also
records the test file that made it.

The plugin run selects every test file that sorts, heads or previews:
``grep -rlE "sort_by_tags|\.head\(|_repr_html_|preview" tests`` (``test_*.py`` files).
"""

from __future__ import annotations

import decimal
import json
import os
import struct
import sys
from collections import Counter
from typing import Any, cast

import numpy as np
import polars as pl
import pyarrow as pa

from orcapod.utils import arrow_utils

_ROW = "__compare_row"


def old_order(table: pa.Table, keys: list[str]) -> list[Any]:
    """Main's fallback (3d3dec21) with a row-number column: the order it gave."""
    numbered = table.append_column(
        _ROW, pa.array(np.arange(table.num_rows), pa.int64())
    )
    sorted_table = (
        pl.DataFrame(numbered)
        .sort(by=keys, descending=False, nulls_last=True, maintain_order=True)
        .to_arrow()
    )
    return sorted_table.column(_ROW).to_pylist()


def _null_inside(value: Any) -> bool:
    """Whether a list element or a struct field, at any depth, is null."""
    if isinstance(value, list):
        return any(v is None or _null_inside(v) for v in value)
    if isinstance(value, dict):
        return any(v is None or _null_inside(v) for v in value.values())
    return False


def arrow_order(table: pa.Table, keys: list[str]) -> list[Any] | None:
    """Arrow's own order (a struct of scalars), or ``None`` where Arrow cannot sort."""
    numbered = table.append_column(
        _ROW, pa.array(np.arange(table.num_rows), pa.int64())
    )
    try:
        ordered = numbered.sort_by([(k, "ascending") for k in keys])
    except (pa.ArrowNotImplementedError, pa.ArrowTypeError):
        return None
    return ordered.column(_ROW).to_pylist()


def _bits(array: "pa.Array[Any]") -> "pa.Array[Any]":
    """An array with every float viewed as its bits, so a NaN equals itself."""
    if pa.types.is_floating(array.type):
        width = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}[
            array.type.bit_width
        ]
        return array.view(width)
    if isinstance(array, (pa.ListArray, pa.LargeListArray)):
        return pa.LargeListArray.from_arrays(
            cast("pa.Int64Array", array.offsets.cast(pa.int64())),
            _bits(array.values),
            mask=array.is_null(),
        )
    if isinstance(array, pa.StructArray):
        return pa.StructArray.from_arrays(
            [_bits(array.field(i)) for i in range(array.type.num_fields)],
            names=[f.name for f in array.type],
            mask=array.is_null(),
        )
    if isinstance(array, pa.DictionaryArray):
        return array.dictionary_decode()
    return array


def compare(table: pa.Table, keys: list[str], new: list[Any]) -> dict[str, Any]:
    """Classify one call."""
    types = ", ".join(f"{k}: {table.schema.field(k).type}" for k in keys)
    try:
        old = old_order(table, keys)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:  # a Polars panic is a BaseException
        return {"types": types, "result": "main raised", "error": type(error).__name__}
    try:
        restored = arrow_utils.restore_schema_nullability(
            pl.DataFrame(table).to_arrow(), table.schema
        )
        # Every column, not only the keys: main's sort carried the whole table through
        # Polars, so a data column could change while every key stayed equal.
        changed = [
            name
            for name in table.column_names
            if not _bits(restored.column(name).combine_chunks()).equals(
                _bits(table.column(name).combine_chunks())
            )
        ]
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        changed = list(table.column_names)
    if changed:
        return {
            "types": types,
            "result": "main wrong",
            "same order": old == new,
            "keys changed": any(name in keys for name in changed),
            "changed columns": changed,
        }
    if old == new:
        return {"types": types, "result": "same"}
    rows = table.select(keys).to_pylist()
    moved = [i for i, (a, b) in enumerate(zip(old, new, strict=True)) if a != b]
    why = (
        "null inside a list or struct"
        if any(_null_inside(rows[old[i]]) or _null_inside(rows[new[i]]) for i in moved)
        else "UNEXPLAINED"
    )
    arrow = arrow_order(table, keys)
    if arrow is not None:
        why += "; now Arrow's own order" if arrow == new else "; NOT Arrow's order"
    return {
        "types": types,
        "result": "moved",
        "why": why,
        "main": [rows[i] for i in old],
        "now": [rows[i] for i in new],
    }


# ── the pytest plugin ─────────────────────────────────────────────────────────

_CALLS: list[dict[str, Any]] = []
_TEST: list[str] = [""]


def pytest_runtest_setup(item: Any) -> None:
    _TEST[0] = item.nodeid


def pytest_configure(config: Any) -> None:
    original = arrow_utils._tag_order_by_polars  # pyright: ignore[reportPrivateUsage]

    def wrapped(table: pa.Table, keys: list[str]) -> Any:
        new = original(table, keys)
        _CALLS.append(
            {**compare(table, keys, new.to_pylist()), "test": _TEST[0].split("::")[0]}
        )
        return new

    arrow_utils._tag_order_by_polars = wrapped  # pyright: ignore[reportPrivateUsage]


def pytest_unconfigure(config: Any) -> None:
    out = os.environ.get("COMPARE_SORT_FALLBACK_OUT", "sort_fallback_calls.json")
    with open(out, "w") as handle:
        json.dump(_CALLS, handle, default=str, indent=1)
    print(
        f"\nsort fallback calls: {len(_CALLS)}; {dict(Counter(c['result'] for c in _CALLS))}"
    )


# ── the audit ─────────────────────────────────────────────────────────────────


def _f32(bits: int) -> float:
    return struct.unpack("<f", struct.pack("<I", bits))[0]


def audit_types() -> dict[str, "pa.Array[Any]"]:
    """Five values of each audit type, one null, out of order, two equal."""
    day = 86_400_000
    return {
        "int64": pa.array([3, 1, None, 2, 1], pa.int64()),
        "date64 off midnight": pa.array([day + 5, 3, None, day + 1, 3], pa.date64()),
        "date32": pa.array([5, 1, None, 3, 1], pa.date32()),
        "time64[ns]": pa.array([5, 1_000_001, None, 1_000_000, 5], pa.time64("ns")),
        "timestamp[ns, UTC]": pa.array([5, 1, None, 3, 1], pa.timestamp("ns", "UTC")),
        "duration[ns]": pa.array([5, -1, None, 3, -1], pa.duration("ns")),
        "decimal128": pa.array(
            [
                decimal.Decimal("2.5"),
                decimal.Decimal("-1"),
                None,
                decimal.Decimal("0.1"),
                decimal.Decimal("-1"),
            ],
            pa.decimal128(10, 2),
        ),
        "decimal256": pa.array(
            [
                decimal.Decimal("2.5"),
                decimal.Decimal("-1"),
                None,
                decimal.Decimal("1e30"),
                decimal.Decimal("-1"),
            ],
            pa.decimal256(40, 2),
        ),
        "float32 (sNaN, -0.0)": pa.array(
            [_f32(0x7FA00000), -0.0, None, 0.0, 1.5], pa.float32()
        ),
        "float64 NaN": pa.array([float("nan"), 2.0, None, -1.0, 2.0], pa.float64()),
        "float16": pa.array(
            np.array([1.5, -0.0, 0.0, 2.0, 1.5], np.float16), pa.float16()
        ),
        "large_string (é)": pa.array(["é", "é", None, "b", "é"], pa.large_string()),
        "large_binary": pa.array(
            [b"\x02", b"\x00", None, b"\x01", b"\x00"], pa.large_binary()
        ),
        "fixed_size_binary": pa.array([b"bb", b"aa", None, b"ab", b"aa"], pa.binary(2)),
        "bool": pa.array([True, False, None, True, False], pa.bool_()),
        "uint64 max": pa.array([2**64 - 1, 0, None, 5, 0], pa.uint64()),
        # Since ITL-1264 (PR 455) no stream carries a dictionary-encoded column; this
        # row measures the sort helper on a table directly, which no stream reaches.
        "dictionary<string>": pa.DictionaryArray.from_arrays(
            pa.array([0, 1, None, 2, 1], pa.int32()), pa.array(["c", "a", "b"])
        ),
    }


def _lists(values: "pa.Array[Any]", with_null_inside: bool) -> "pa.Array[Any]":
    """Lists over ``values``: per row, its value then the next row's; the last row null.

    Without nulls inside, a null value is replaced by the first row's (never null).
    """
    count = len(values)
    order = [[i, (i + 1) % count] for i in range(count)]
    if not with_null_inside:
        order = [[i if values[i].is_valid else 0 for i in pair] for pair in order]
    flat = pa.array([i for pair in order for i in pair], pa.int64())
    offsets = pa.array(np.arange(count + 1) * 2, pa.int64())
    mask = pa.array([False] * (count - 1) + [True], pa.bool_())
    return pa.LargeListArray.from_arrays(offsets, values.take(flat), mask=mask)  # pyright: ignore[reportArgumentType]


def audit_tables(values: "pa.Array[Any]") -> list[tuple[str, pa.Table, list[str]]]:
    """Tables with three tags (list or struct of the type, int64, large_string)."""
    count = len(values)
    other_int = pa.array([1, 1, 0, 1, 1][:count], pa.int64())
    other_str = pa.array(["b", "a", "a", "b", "a"][:count], pa.large_string())
    data = pa.array(np.arange(count), pa.int64())
    tables = []
    for name, tag in (
        ("list, nulls inside", _lists(values, True)),
        ("list, no null inside", _lists(values, False)),
        ("struct", pa.StructArray.from_arrays([values, other_int], names=["v", "i"])),
        (
            "list of struct",
            pa.LargeListArray.from_arrays(
                pa.array(np.arange(count + 1), pa.int64()),  # pyright: ignore[reportArgumentType]
                pa.StructArray.from_arrays([values, other_int], names=["v", "i"]),
            ),
        ),
    ):
        table = pa.table({"t": tag, "n": other_int, "s": other_str, "x": data})
        for keys in (["t", "n", "s"], ["s", "t", "n"], ["n", "s", "t"]):
            tables.append((f"{name}, keys {keys}", table, keys))
    return tables


def audit() -> None:
    results: Counter[str] = Counter()
    for type_name, values in audit_types().items():
        for label, table, keys in audit_tables(values):
            new = arrow_utils._tag_order_by_polars(table, keys).to_pylist()  # pyright: ignore[reportPrivateUsage]
            outcome = compare(table, keys, new)
            results[outcome["result"]] += 1
            line = f"{type_name:22} {label:40} {outcome['result']}"
            if outcome["result"] == "moved":
                line += f" ({outcome['why']})"
            if outcome["result"] == "main raised":
                line += f" ({outcome['error']})"
            print(line)
    print(dict(results))


if __name__ == "__main__":
    if sys.argv[1:] == ["audit"]:
        audit()
    else:
        print(__doc__)
