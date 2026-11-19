"""What would ordering a raw stream's identity rows by VALUE take? (ITL-1227, PR 453)

The developer is weighing a value order (tag columns first, then the other columns in
name order) instead of ordering the rows by a per-row hash. This script measures, on
pyarrow and Polars as locked:

1. which column types an identity table can hold that each library can sort;
2. which rows stay tied after sorting by every sortable column;
3. the sort semantics a frozen hash would depend on (strings, nulls, NaN, -0.0);
4. the cost per row of per-row hashing, of a value sort, and of the hybrid (a row hash
   only for the rows of a tie group).

Run: ``uv run python docs/dev/scripts/itl_1227_value_order.py``.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import pathlib
import time
import uuid
import warnings
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc

warnings.simplefilter("ignore")

import orcapod as op  # noqa: E402
from orcapod.contexts import get_default_context  # noqa: E402

CTX = get_default_context()
CONVERTER = CTX.type_converter
HASHER = CTX.arrow_hasher


@dataclasses.dataclass
class Point:
    """A user class (an exact class) for the logical-type column."""

    x: int
    y: float


def arrow_columns() -> dict[str, Any]:
    """One four-row column per Arrow type, with a null where the type allows one."""
    f16 = pa.array(np.array([1.0, -0.0, np.nan, 0.0], np.float16))
    cols: dict[str, Any] = {
        **{
            f"int{w}": pa.array([3, None, -1, 3], getattr(pa, f"int{w}")())
            for w in (8, 16, 32, 64)
        },
        **{
            f"uint{w}": pa.array([3, None, 1, 3], getattr(pa, f"uint{w}")())
            for w in (8, 16, 32, 64)
        },
        "float16": f16,
        "float32 NaN -0.0": pa.array([1.0, -0.0, float("nan"), 0.0], pa.float32()),
        "float64 NaN -0.0 null": pa.array(
            [float("nan"), -0.0, None, 0.0], pa.float64()
        ),
        "bool": pa.array([True, None, False, True]),
        "string": pa.array(["b", None, "é", "a"], pa.string()),
        "large_string": pa.array(["b", None, "é", "a"], pa.large_string()),
        "binary": pa.array([b"b", None, b"\xff", b"a"], pa.binary()),
        "large_binary": pa.array([b"b", None, b"\xff", b"a"], pa.large_binary()),
        "fixed_size_binary": pa.array([b"bb", None, b"aa", b"ab"], pa.binary(2)),
        "date32": pa.array([datetime.date(2020, 1, 2), None] * 2, pa.date32()),
        "date64": pa.array([datetime.date(2020, 1, 2), None] * 2, pa.date64()),
        "time64[ns]": pa.array([1, None, 1001, 5], pa.time64("ns")),
        "timestamp[us, UTC]": pa.array([1, None, 3, 2], pa.timestamp("us", "UTC")),
        "duration[ns]": pa.array([1, None, -3, 2], pa.duration("ns")),
        "decimal128(10,2)": pa.array(
            [decimal.Decimal("1.50"), None, decimal.Decimal("-2.25"), None],
            pa.decimal128(10, 2),
        ),
        "decimal256(76,40)": pa.array(
            [decimal.Decimal("1.5"), None, decimal.Decimal("-2.25"), None],
            pa.decimal256(76, 40),
        ),
        "null": pa.array([None] * 4, pa.null()),
        "large_list<int64>": pa.array(
            [[2, None], None, [1], [2]], pa.large_list(pa.int64())
        ),
        "large_list<large_list<int64>>": pa.array(
            [[[2]], None, [[1, None]], [[2]]], pa.large_list(pa.large_list(pa.int64()))
        ),
        "fixed_size_list<int64,2>": pa.array(
            [[1, 2], None, [0, 5], [1, 2]], pa.list_(pa.int64(), 2)
        ),
        "struct<a:int64,b:string>": pa.array(
            [{"a": 1, "b": "x"}, None, {"a": 0, "b": "y"}, {"a": 1, "b": "x"}],
            pa.struct([("a", pa.int64()), ("b", pa.string())]),
        ),
        "map<string,int64>": pa.array(
            [[("k", 1)], None, [("j", 2)], [("k", 1)]], pa.map_(pa.string(), pa.int64())
        ),
    }
    return cols


def logical_columns() -> dict[str, Any]:
    """One column per Orcapod logical type, built by the data context's converter."""
    values: dict[str, tuple[Any, list[Any]]] = {
        "pathlib.Path": (pathlib.Path, [pathlib.Path("/b"), pathlib.Path("/a")]),
        "uuid.UUID": (uuid.UUID, [uuid.UUID(int=2), uuid.UUID(int=1)]),
        "numpy.ndarray": (np.ndarray, [np.array([2, 1]), np.array([1])]),
        "pandas.DataFrame": (
            pd.DataFrame,
            [pd.DataFrame({"a": [2]}), pd.DataFrame({"a": [1]})],
        ),
        "polars.DataFrame": (
            pl.DataFrame,
            [pl.DataFrame({"a": [2]}), pl.DataFrame({"a": [1]})],
        ),
        "user class (dataclass)": (Point, [Point(2, 0.5), Point(1, 1.5)]),
    }
    try:
        import torch

        values["torch.Tensor"] = (
            torch.Tensor,
            [torch.tensor([2.0]), torch.tensor([1.0])],
        )
    except ImportError:  # pragma: no cover
        pass
    out: dict[str, Any] = {}
    for name, (python_type, items) in values.items():
        try:
            table = CONVERTER.python_dicts_to_arrow_table(
                [{"c": v} for v in items], python_schema={"c": python_type}
            )
            out[f"{name} [{table.schema.field('c').type}]"] = table.column(
                "c"
            ).combine_chunks()
        except Exception as error:  # noqa: BLE001
            out[f"{name}"] = error
    for name, python_type in (("op.File", op.File), ("op.Directory", op.Directory)):
        try:
            arrow_type = CONVERTER.python_type_to_arrow_type(python_type)
            out[f"{name} [{arrow_type}]"] = pa.array([None, None], arrow_type)
        except Exception as error:  # noqa: BLE001
            out[name] = error
    return out


def try_sort(name: str, column: Any) -> tuple[str, str]:
    """Sort a one-column table with pyarrow and with Polars; report each outcome."""
    if isinstance(column, Exception):
        return f"build failed: {type(column).__name__}", ""
    table = pa.table({"c": column})
    results = []
    try:
        pc.sort_indices(table, sort_keys=[("c", "ascending")], null_placement="at_end")
        results.append("ok")
    except Exception as error:  # noqa: BLE001
        results.append(f"{type(error).__name__}: {str(error)[:60]}")
    try:
        frame = pl.from_arrow(table)
        assert isinstance(frame, pl.DataFrame)
        frame.with_row_index("i").sort("c", nulls_last=True, maintain_order=True)
        results.append("ok")
    except BaseException as error:  # noqa: BLE001  (a Polars panic is a BaseException)
        results.append(f"{type(error).__name__}: {str(error)[:60]}")
    return results[0], results[1]


def semantics() -> None:
    """The sort semantics a frozen hash would rest on."""
    print("\n## 3. Semantics (pyarrow sort_indices / Polars sort)")
    values = pa.array(["b", "B", "é", "e", "a", None, "Z", ""], pa.large_string())
    t = pa.table({"c": values})
    pa_order = t.take(
        pc.sort_indices(t, sort_keys=[("c", "ascending")], null_placement="at_end")
    )
    frame = pl.from_arrow(t)
    assert isinstance(frame, pl.DataFrame)
    pl_order = frame.sort("c", nulls_last=True)
    print("strings pyarrow:", pa_order.column("c").to_pylist())
    print("strings polars: ", pl_order["c"].to_list())
    print("(byte order of UTF-8 = code point order: no locale in either)")
    floats = pa.array(
        [float("nan"), 1.0, -0.0, None, 0.0, float("-inf"), -1.0], pa.float64()
    )
    t = pa.table({"c": floats, "i": pa.array(range(len(floats)))})
    pa_sorted = t.take(
        pc.sort_indices(t, sort_keys=[("c", "ascending")], null_placement="at_end")
    )
    frame = pl.from_arrow(t)
    assert isinstance(frame, pl.DataFrame)
    pl_sorted = frame.sort("c", nulls_last=True, maintain_order=True)
    print(
        "floats pyarrow (value, input index):",
        list(zip(pa_sorted["c"].to_pylist(), pa_sorted["i"].to_pylist())),
    )
    print(
        "floats polars  (value, input index):",
        list(zip(pl_sorted["c"].to_list(), pl_sorted["i"].to_list())),
    )
    # -0.0 against 0.0 and NaN payloads: are they ties (kept in input order)?
    bits = np.array(
        [0x7FF8000000000001, 0x7FF8000000000000, 0x8000000000000000, 0x0], np.uint64
    )
    t = pa.table({"c": pa.array(bits.view(np.float64)), "i": pa.array([0, 1, 2, 3])})
    for label, rows in (("as given", [0, 1, 2, 3]), ("reversed", [3, 2, 1, 0])):
        tt = t.take(rows)
        order = tt.take(pc.sort_indices(tt, sort_keys=[("c", "ascending")]))[
            "i"
        ].to_pylist()
        print(f"NaN payloads and +/-0.0, {label}: pyarrow order of input ids {order}")


def tied_rows() -> None:
    """Which rows stay tied after a sort over every sortable column (name order)."""
    print("\n## 2. Ties after sorting by every sortable column")
    cases = {
        "identical rows": pa.table({"k": [1, 1], "x": [5, 5]}),
        "-0.0 vs 0.0 in a data column": pa.table(
            {"k": [1, 1], "x": pa.array([-0.0, 0.0])}
        ),
        "NaN payloads": pa.table(
            {
                "k": [1, 1],
                "x": pa.array(
                    np.array([0x7FF8000000000001, 0x7FF8000000000000], np.uint64).view(
                        np.float64
                    )
                ),
            }
        ),
        "differ only in a map column": pa.table(
            {
                "k": [1, 1],
                "m": pa.array(
                    [[("a", 1)], [("a", 2)]], pa.map_(pa.string(), pa.int64())
                ),
            }
        ),
        "differ only in an extension (ndarray) column": None,
    }
    arrays = logical_columns()
    nd = next(v for k, v in arrays.items() if k.startswith("numpy.ndarray"))
    cases["differ only in an extension (ndarray) column"] = pa.table(
        {"k": [1, 1], "a": nd}
    )
    for name, table in cases.items():
        assert table is not None
        sortable = []
        for col in sorted(table.column_names):
            ok, _ = try_sort(col, table.column(col))
            if ok == "ok":
                sortable.append(col)
        idx = pc.sort_indices(table, sort_keys=[(c, "ascending") for c in sortable])
        st = table.take(idx)
        keys = st.select(sortable)
        rows = [tuple(map(repr, r.values())) for r in keys.to_pylist()]
        tied = len(rows) - len(set(rows))
        try:
            digests = {
                HASHER.hash_table(table.slice(i, 1)).to_hex()
                for i in range(table.num_rows)
            }
            distinct = str(len(digests))
        except ValueError as error:
            distinct = f"unhashable ({str(error)[:50]})"
        print(
            f"{name:<45} sortable={sortable} tied_pairs={tied} distinct_row_hashes={distinct}"
        )


def costs(n: int = 20000) -> None:
    """Per-row hashing, value sort and the hybrid, on a typical identity table."""
    print(
        f"\n## 4. Cost on a {n}-row identity table (two tags, two data, system tags, source info)"
    )
    rng = np.random.default_rng(7)
    table = pa.table(
        {
            "k": pa.array(rng.integers(0, 100, n)),
            "j": pa.array([f"s{i}" for i in range(n)], pa.large_string()),
            "x": pa.array(rng.random(n)),
            "y": pa.array(rng.integers(0, 10, n)),
            "_tag_record_id::abc": pa.array(
                [f"row_{i}" for i in range(n)], pa.large_string()
            ),
            "_tag_source_id::abc": pa.array(["src"] * n, pa.large_string()),
            "_source_x": pa.array(
                [f"src::row_{i}::x" for i in range(n)], pa.large_string()
            ),
        }
    )

    def per_row_hash(t: pa.Table) -> pa.Table:
        digests = [
            HASHER.hash_table(t.slice(i, 1)).to_prefixed_digest()
            for i in range(t.num_rows)
        ]
        return t.take(
            pa.array(sorted(range(t.num_rows), key=digests.__getitem__), pa.int64())
        )

    def value_sort(t: pa.Table) -> pa.Table:
        tags = [c for c in ("j", "k") if c in t.column_names]
        rest = sorted(
            c
            for c in t.column_names
            if c not in tags
            and not pa.types.is_map(t.schema.field(c).type)
            and not pa.types.is_large_list(t.schema.field(c).type)
        )
        return t.take(
            pc.sort_indices(
                t,
                sort_keys=[(c, "ascending") for c in tags + rest],
                null_placement="at_end",
            )
        )

    def hybrid(t: pa.Table) -> pa.Table:
        s = value_sort(t)
        # Ties: rows equal in every column the sort used.
        keys = sorted(
            c
            for c in s.column_names
            if not pa.types.is_map(s.schema.field(c).type)
            and not pa.types.is_large_list(s.schema.field(c).type)
        )
        frame = pl.from_arrow(s.select(keys))
        assert isinstance(frame, pl.DataFrame)
        dup = frame.is_duplicated().to_numpy()
        if not dup.any():
            return s
        # Hash only the rows of tie groups, and order each group by its digests.
        ids = frame.with_row_index("i").filter(pl.Series(dup))["i"].to_list()
        _ = [HASHER.hash_table(s.slice(i, 1)).to_prefixed_digest() for i in ids]
        return s

    def timed(fn: Callable[[pa.Table], pa.Table], t: pa.Table) -> float:
        best = float("inf")
        for _ in range(3):
            start = time.perf_counter()
            fn(t)
            best = min(best, time.perf_counter() - start)
        return best

    whole = timed(lambda t: (HASHER.hash_table(t), t)[1], table)
    print(
        f"one table hash (today's final step):     {whole * 1e3:8.1f} ms  ({whole / n * 1e6:6.2f} us/row)"
    )
    for label, fn in (
        ("per-row hashing + sort", per_row_hash),
        ("value sort", value_sort),
        ("hybrid, no ties", hybrid),
    ):
        t = (
            timed(fn, table)
            if label != "per-row hashing + sort"
            else timed(fn, table.slice(0, 2000)) * n / 2000
        )
        print(f"{label:<40} {t * 1e3:8.1f} ms  ({t / n * 1e6:6.2f} us/row)")
    one_group = pa.table({"k": pa.array([1] * 2000), "x": pa.array([1.0] * 2000)})
    # Rows tied in every sortable column, differing in a list column (pyarrow cannot
    # sort a list): every row is in the one tie group.
    one_group = one_group.append_column(
        "m", pa.array([[i] for i in range(2000)], pa.large_list(pa.int64()))
    )
    t = timed(hybrid, one_group)
    print(
        f"{'hybrid, one tie group of 2000 rows':<40} {t * 1e3:8.1f} ms  ({t / 2000 * 1e6:6.2f} us/row)"
    )


def main() -> None:
    print(f"pyarrow {pa.__version__}, polars {pl.__version__}")
    print("\n## 1. Can each type be sorted? (pyarrow sort_indices | Polars sort)")
    columns: dict[str, Any] = {**arrow_columns(), **logical_columns()}
    for name, column in columns.items():
        pa_result, pl_result = try_sort(name, column)
        short = name.split(" [")[0] + (" [ext]" if "extension<" in name else "")
        print(
            f"{short[:34]:<34} pyarrow: {pa_result[:58]:<58} polars: {pl_result[:70]}"
        )
    tied_rows()
    semantics()
    costs()


if __name__ == "__main__":
    main()
