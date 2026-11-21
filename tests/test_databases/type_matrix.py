"""The supported-data-types matrix: what each database keeps of each Arrow type (ITL-1334).

Every row is a column type, every column a database. A cell is measured, never declared:
a table holding two columns of the type (the second with a null when the type is
nullable) is written with ``add_records``, flushed and read back with
``get_all_records``, then compared with what was written, up to the large variants every
Orcapod table uses (``arrow_utils.normalize_to_large_types``). The outcome is one of:

- ``KEPT``: read back equal, type (with nested nullability) and values.
- ``KEPT VIA RECORDED TYPE``: read back equal through Delta's recorded-type mechanism
  (``databases/delta_arrow_types.py``: the type is recorded in the field metadata, the
  values written in a form Delta keeps, the type restored on read).
- ``REFUSED``: the write raised an error naming the column, before anything was stored.
- ``SILENT``: anything else (another type, other values, an error that does not name the
  column, a failed read). The test fails on any silent cell.

The databases: in-memory, Delta Lake (local directory), the connector database over
SQLite, over PostgreSQL and over SpiralDB. No PostgreSQL server and no SpiralDB is used:
the PostgreSQL cell comes from the connector's own storability rule
(``unstorable_column_reason`` and ``validate_records``, over a mocked connection), so it
is ``KEPT BY RULE`` or ``REFUSED``, never a measured read; the SpiralDB cell is measured
through the disk fake (``tests/test_databases/fake_pyspiral.py``). No database tells tag
columns from data columns: the connector databases tell only the record-id key, which
Orcapod always writes as ``large_binary``.
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime as dt
import decimal
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa

import orcapod
from orcapod.contexts import get_default_context
from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    InMemoryArrowDatabase,
    PostgreSQLConnector,
    SQLiteConnector,
    SpiralDBConnector,
)
from orcapod.databases.delta_arrow_types import _is_recorded  # pyright: ignore[reportPrivateUsage]
from orcapod.utils.arrow_utils import normalize_to_large_types

KEPT = "KEPT"
RECORDED = "KEPT VIA RECORDED TYPE"
KEPT_BY_RULE = "KEPT BY RULE"
REFUSED = "REFUSED"
SILENT = "SILENT"

DATABASES = ("in-memory", "Delta", "SQLite", "PostgreSQL", "SpiralDB")


@dataclasses.dataclass(frozen=True)
class Cell:
    """One measured outcome: its kind and what was seen."""

    kind: str
    detail: str = ""


# ---------------------------------------------------------------------------
# The rows
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class MatrixExact:
    """An exact user class (an ordinary dataclass) with two fields."""

    n: int
    s: str


@orcapod.dataclass
class MatrixStructural:
    """A structural user class with two fields."""

    n: int
    s: str


@dataclasses.dataclass(frozen=True)
class Row:
    """One row of the matrix: a label, a group, and how to build its two columns."""

    label: str
    group: str
    build: Callable[
        [Path], tuple["pa.Field[Any]", "pa.Field[Any]", list[Any], list[Any]]
    ]
    provisional: bool = False


def _plain(
    arrow_type: pa.DataType, values: list[Any], nullable: bool = True
) -> Callable[[Path], tuple[Any, Any, list[Any], list[Any]]]:
    """Two columns of ``arrow_type``; the second has a null in the middle if nullable."""

    def build(_root: Path) -> tuple[Any, Any, list[Any], list[Any]]:
        second = [values[0], None, values[2]] if nullable else list(reversed(values))
        return (
            pa.field("x", arrow_type, nullable=nullable),
            pa.field("y", arrow_type, nullable=nullable),
            values,
            second,
        )

    return build


def _logical(
    python_type: Any, make: Callable[[Path], list[Any]]
) -> Callable[[Path], tuple[Any, Any, list[Any], list[Any]]]:
    """Two columns of a logical (or library) type, typed by the data context's converter:
    ``x`` of ``T`` and ``y`` of ``T | None`` with a null."""

    def build(root: Path) -> tuple[Any, Any, list[Any], list[Any]]:
        values = make(root)
        return ("logical", python_type, values, [values[0], None, values[2]])  # pyright: ignore[reportReturnType]

    return build


D = decimal.Decimal
_TS = [
    dt.datetime(2026, 1, 1, 0, 0, 1),
    dt.datetime(2026, 1, 2),
    dt.datetime(2026, 3, 4),
]
_TSZ = [t.replace(tzinfo=dt.timezone.utc) for t in _TS]
_DATES = [dt.date(2026, 1, 1), dt.date(2026, 1, 2), dt.date(1999, 12, 31)]
_TIMES = [dt.time(1, 2, 3), dt.time(4, 5, 6), dt.time(23, 59, 59)]
_DURATIONS = [dt.timedelta(seconds=1), dt.timedelta(seconds=2), dt.timedelta(days=1)]


def _files(root: Path) -> list[Any]:
    out = []
    for i in range(3):
        path = root / f"f{i}.txt"
        path.write_text(f"file {i}")
        out.append(orcapod.File(path))
    return out


def _directories(root: Path) -> list[Any]:
    out = []
    for i in range(3):
        path = root / f"d{i}"
        path.mkdir(exist_ok=True)
        (path / "a.txt").write_text(f"dir {i}")
        out.append(orcapod.Directory(path))
    return out


def _arrays(_root: Path) -> list[Any]:
    import numpy as np

    return [np.arange(3), np.arange(4.0), np.zeros((2, 2), dtype=np.int8)]


def _pandas_frames(_root: Path) -> list[Any]:
    import pandas as pd

    return [pd.DataFrame({"a": [1, 2]}), pd.DataFrame({"b": ["x"]}), pd.DataFrame()]


def _pandas_series(_root: Path) -> list[Any]:
    import pandas as pd

    return [pd.Series([1, 2]), pd.Series(["x"]), pd.Series([1.5])]


def _polars_frames(_root: Path) -> list[Any]:
    import polars as pl

    return [pl.DataFrame({"a": [1, 2]}), pl.DataFrame({"b": ["x"]}), pl.DataFrame()]


def _polars_series(_root: Path) -> list[Any]:
    import polars as pl

    return [pl.Series("a", [1, 2]), pl.Series("b", ["x"]), pl.Series("c", [1.5])]


def _tensors(_root: Path) -> list[Any]:
    import torch

    return [torch.arange(3), torch.zeros(2, 2), torch.ones(1, dtype=torch.float16)]


def _upaths(root: Path) -> list[Any]:
    from upath import UPath

    return [UPath(f"memory://matrix/{i}") for i in range(3)]


ROWS: tuple[Row, ...] = (
    *(
        Row(str(t), "integer", _plain(t, [1, 2, 100]))
        for t in (pa.int8(), pa.int16(), pa.int32(), pa.int64())
    ),
    *(
        Row(str(t), "integer", _plain(t, [1, 2, 200]))
        for t in (pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64())
    ),
    Row("halffloat (float16)", "float", _plain(pa.float16(), [1.5, 2.0, 0.25])),
    Row("float (float32)", "float", _plain(pa.float32(), [1.5, 2.0, 0.25])),
    Row("double (float64)", "float", _plain(pa.float64(), [1.5, 2.0, 0.1])),
    Row(
        "decimal128(10, 2)",
        "decimal",
        _plain(pa.decimal128(10, 2), [D("1.25"), D("-2.50"), D("0.01")]),
    ),
    Row(
        "decimal128(38, 4)",
        "decimal",
        _plain(
            pa.decimal128(38, 4),
            [D("1.2500"), D("-2.5000"), D("12345678901234567890.1234")],
        ),
    ),
    Row(
        "decimal256(40, 2)",
        "decimal",
        _plain(
            pa.decimal256(40, 2),
            [D("1.25"), D("-2.50"), D("12345678901234567890123456789012345678.01")],
        ),
    ),
    Row(
        "decimal256(76, 10)",
        "decimal",
        _plain(pa.decimal256(76, 10), [D("1.25"), D("-2.5"), D("0.0000000001")]),
    ),
    Row("bool", "boolean", _plain(pa.bool_(), [True, False, True])),
    Row("null", "null", _plain(pa.null(), [None, None, None])),
    Row("date32", "temporal", _plain(pa.date32(), _DATES)),
    Row("date64", "temporal", _plain(pa.date64(), _DATES)),
    Row("time32[s]", "temporal", _plain(pa.time32("s"), _TIMES)),
    Row("time32[ms]", "temporal", _plain(pa.time32("ms"), _TIMES)),
    Row("time64[us]", "temporal", _plain(pa.time64("us"), _TIMES)),
    Row("time64[ns]", "temporal", _plain(pa.time64("ns"), _TIMES)),
    *(
        Row(f"timestamp[{u}]", "temporal", _plain(pa.timestamp(u), _TS))
        for u in ("s", "ms", "us", "ns")
    ),
    *(
        Row(
            f"timestamp[{u}, tz=UTC]",
            "temporal",
            _plain(pa.timestamp(u, tz="UTC"), _TSZ),
        )
        for u in ("s", "ms", "us", "ns")
    ),
    *(
        Row(
            f"timestamp[{u}, tz=Europe/Berlin]",
            "temporal",
            _plain(pa.timestamp(u, tz="Europe/Berlin"), _TSZ),
        )
        for u in ("s", "ms", "us", "ns")
    ),
    *(
        Row(f"duration[{u}]", "temporal", _plain(pa.duration(u), _DURATIONS))
        for u in ("s", "ms", "us", "ns")
    ),
    Row("string", "string and binary", _plain(pa.string(), ["a", "bc", ""])),
    Row(
        "large_string", "string and binary", _plain(pa.large_string(), ["a", "bc", ""])
    ),
    Row("binary", "string and binary", _plain(pa.binary(), [b"a", b"bc", b""])),
    Row(
        "large_binary",
        "string and binary",
        _plain(pa.large_binary(), [b"a", b"bc", b""]),
    ),
    Row(
        "fixed_size_binary[4]",
        "string and binary",
        _plain(pa.binary(4), [b"abcd", b"efgh", b"\x00\x01\x02\x03"]),
    ),
    Row("list<int64>", "list", _plain(pa.list_(pa.int64()), [[1, 2], [], [3, None]])),
    Row(
        "list<int64 not null item>",
        "list",
        _plain(
            pa.list_(pa.field("item", pa.int64(), nullable=False)), [[1, 2], [], [3]]
        ),
    ),
    Row(
        "large_list<int64>",
        "list",
        _plain(pa.large_list(pa.int64()), [[1, 2], [], [3, None]]),
    ),
    Row(
        "large_list<int64 not null item>",
        "list",
        _plain(
            pa.large_list(pa.field("item", pa.int64(), nullable=False)),
            [[1, 2], [], [3]],
        ),
    ),
    Row(
        "fixed_size_list<int64>[2]",
        "list",
        _plain(pa.list_(pa.int64(), 2), [[1, 2], [3, None], [5, 6]]),
    ),
    Row(
        "fixed_size_list<int64 not null item>[2]",
        "list",
        _plain(
            pa.list_(pa.field("item", pa.int64(), nullable=False), 2),
            [[1, 2], [3, 4], [5, 6]],
        ),
    ),
    Row(
        "large_list<float16>",
        "list",
        _plain(pa.large_list(pa.float16()), [[1.5], [], [0.25, None]]),
    ),
    Row(
        "struct<a: int32, b: string not null>",
        "struct and map",
        _plain(
            pa.struct(
                [pa.field("a", pa.int32()), pa.field("b", pa.string(), nullable=False)]
            ),
            [{"a": 1, "b": "x"}, {"a": None, "b": "y"}, {"a": 3, "b": ""}],
        ),
    ),
    Row(
        "struct<f: fixed_size_list<int64>[2]>",
        "struct and map",
        _plain(
            pa.struct([pa.field("f", pa.list_(pa.int64(), 2))]),
            [{"f": [1, 2]}, {"f": None}, {"f": [5, 6]}],
        ),
    ),
    Row(
        "map<string, int64>",
        "struct and map",
        _plain(
            pa.map_(pa.string(), pa.int64()), [[("a", 1)], [], [("b", 2), ("c", None)]]
        ),
    ),
    Row("int64 not null", "nullability", _plain(pa.int64(), [1, 2, 3], nullable=False)),
    Row(
        "string not null",
        "nullability",
        _plain(pa.string(), ["a", "b", "c"], nullable=False),
    ),
    Row("File", "Orcapod logical type", _logical(orcapod.File, _files)),
    Row("Directory", "Orcapod logical type", _logical(orcapod.Directory, _directories)),
    Row(
        "Path",
        "Orcapod logical type",
        _logical(Path, lambda r: [Path("/a"), Path("b/c"), Path(".")]),
    ),
    Row("UPath", "Orcapod logical type", _logical(Any, _upaths)),  # resolved below
    Row(
        "uuid.UUID",
        "Orcapod logical type",
        _logical(uuid.UUID, lambda r: [uuid.UUID(int=i + 1) for i in range(3)]),
    ),
    Row(
        "list[Path]",
        "Orcapod logical type",
        _logical(list[Path], lambda r: [[Path("/a")], [], [Path("b"), Path("c")]]),
    ),
    Row(
        "set[int]",
        "Orcapod logical type",
        _logical(set[int], lambda r: [{1, 2}, set(), {3}]),
    ),
    Row(
        "set[uuid.UUID]",
        "Orcapod logical type",
        _logical(
            set[uuid.UUID], lambda r: [{uuid.UUID(int=1)}, set(), {uuid.UUID(int=2)}]
        ),
    ),
    Row(
        "dict[str, int]",
        "Orcapod logical type",
        _logical(dict[str, int], lambda r: [{"a": 1}, {}, {"b": 2}]),
    ),
    Row(
        "user class, exact (dataclass)",
        "Orcapod logical type",
        _logical(
            MatrixExact,
            lambda r: [MatrixExact(1, "a"), MatrixExact(2, "b"), MatrixExact(3, "")],
        ),
    ),
    Row(
        "user class, structural (orcapod.dataclass)",
        "Orcapod logical type",
        _logical(
            MatrixStructural,
            lambda r: [
                MatrixStructural(1, "a"),
                MatrixStructural(2, "b"),
                MatrixStructural(3, ""),
            ],
        ),
    ),
    Row(
        "numpy.ndarray", "library value type", _logical(Any, _arrays), provisional=True
    ),
    Row(
        "pandas.DataFrame",
        "library value type",
        _logical(Any, _pandas_frames),
        provisional=True,
    ),
    Row(
        "pandas.Series",
        "library value type",
        _logical(Any, _pandas_series),
        provisional=True,
    ),
    Row(
        "polars.DataFrame",
        "library value type",
        _logical(Any, _polars_frames),
        provisional=True,
    ),
    Row(
        "polars.Series",
        "library value type",
        _logical(Any, _polars_series),
        provisional=True,
    ),
    Row(
        "torch.Tensor", "library value type", _logical(Any, _tensors), provisional=True
    ),
)


def _python_type(label: str, declared: Any) -> Any:
    """The Python type of a logical row (the library and UPath types imported late)."""
    if declared is not Any:
        return declared
    if label == "UPath":
        from upath import UPath

        return UPath
    if label == "numpy.ndarray":
        import numpy as np

        return np.ndarray
    if label.startswith("pandas."):
        import pandas as pd

        return getattr(pd, label.split(".")[1])
    if label.startswith("polars."):
        import polars as pl

        return getattr(pl, label.split(".")[1])
    if label == "torch.Tensor":
        import torch

        return torch.Tensor
    raise AssertionError(label)


def build_table(row: Row, root: Path) -> pa.Table:
    """The table written for ``row``: a non-null ``large_binary`` id, then ``x`` and ``y``."""
    first, second, xs, ys = row.build(root)
    ids = pa.array([b"r1", b"r2", b"r3"], pa.large_binary())
    id_field = pa.field("id", pa.large_binary(), nullable=False)
    if first == "logical":
        python_type = _python_type(row.label, second)
        converter = get_default_context().type_converter
        table = converter.python_dicts_to_arrow_table(
            [{"x": x, "y": y} for x, y in zip(xs, ys)],
            python_schema={"x": python_type, "y": python_type | None},
        )
        return pa.Table.from_arrays(
            [ids, table.column("x"), table.column("y")],
            schema=pa.schema(
                [id_field, table.schema.field("x"), table.schema.field("y")]
            ),
        )
    return pa.Table.from_arrays(
        [ids, pa.array(xs, first.type), pa.array(ys, second.type)],
        schema=pa.schema([id_field, first, second]),
    )


# ---------------------------------------------------------------------------
# The databases
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _database(name: str, root: Path) -> Iterator[Any]:
    if name == "in-memory":
        yield InMemoryArrowDatabase()
    elif name == "Delta":
        yield DeltaTableDatabase(root / "delta")
    elif name == "SQLite":
        yield ConnectorArrowDatabase(SQLiteConnector(str(root / "store.db")))
    elif name == "SpiralDB":
        from tests.test_databases import fake_pyspiral

        with fake_pyspiral.install(root / "spiral"):
            fake_pyspiral.FakeSpiral().project("p")
            yield ConnectorArrowDatabase(SpiralDBConnector("p"))
    elif name == "PostgreSQL":
        with patch("psycopg.connect") as connect:
            connect.return_value = MagicMock()
            yield ConnectorArrowDatabase(
                PostgreSQLConnector("postgresql://localhost/m")
            )
    else:
        raise AssertionError(name)


def _names_a_column(error: BaseException) -> bool:
    text = str(error)
    return any(f"'{c}'" in text or f'"{c}"' in text for c in ("x", "y"))


def _expected_field(field: "pa.Field[Any]") -> "pa.Field[Any]":
    return field.with_type(normalize_to_large_types(field.type, keep_null=True))


def _describe(field: "pa.Field[Any]") -> str:
    nested = _nested_flags(field.type)
    return f"{field.type}{'' if field.nullable else ' not null'}{nested}"


def _nested_flags(arrow_type: pa.DataType) -> str:
    """The non-null nested fields, which ``str(type)`` may not show."""
    flags = []
    for i in range(getattr(arrow_type, "num_fields", 0)):
        child = arrow_type.field(i)
        if not child.nullable:
            flags.append(child.name)
        inner = _nested_flags(child.type)
        if inner:
            flags.append(f"{child.name}{inner}")
    return f" [non-null: {', '.join(flags)}]" if flags else ""


def _same_field(a: "pa.Field[Any]", b: "pa.Field[Any]") -> bool:
    return (
        a.name == b.name
        and a.nullable == b.nullable
        and a.type == b.type
        and _nested_flags(a.type) == _nested_flags(b.type)
    )


def measure(row: Row, database: str, root: Path) -> Cell:
    """Write ``row``'s table to ``database`` and say what came back."""
    root.mkdir(parents=True, exist_ok=True)
    try:
        written = build_table(row, root)
    except Exception as exc:  # noqa: BLE001  # a row Orcapod cannot build is reported
        return Cell(SILENT, f"cannot build the column: {type(exc).__name__}: {exc}")
    with _database(database, root) as db:
        path = ("matrix", "t")
        if database == "PostgreSQL":
            try:
                db.require_storable(written.schema)
                db._connector.validate_records(written)  # pyright: ignore[reportPrivateUsage]
            except Exception as exc:  # noqa: BLE001
                if _names_a_column(exc):
                    return Cell(REFUSED, f"{type(exc).__name__}")
                return Cell(
                    SILENT,
                    f"refused without naming the column: {type(exc).__name__}: {exc}",
                )
            return Cell(KEPT_BY_RULE, "storability rule only (no server)")
        try:
            db.add_records(path, written, record_id_column="id", flush=True)
        except Exception as exc:  # noqa: BLE001
            if _names_a_column(exc):
                return Cell(REFUSED, f"{type(exc).__name__}")
            return Cell(
                SILENT,
                f"write failed without naming the column: {type(exc).__name__}: {str(exc)[:160]}",
            )
        try:
            back = db.get_all_records(path, record_id_column="id")
        except Exception as exc:  # noqa: BLE001
            return Cell(
                SILENT, f"written, read failed: {type(exc).__name__}: {str(exc)[:160]}"
            )
    if back is None:
        return Cell(SILENT, "written, read back nothing")
    back = back.sort_by("id")
    problems = []
    for name in ("x", "y"):
        exact = written.schema.field(name)
        got = back.schema.field(name)
        # The type written, or its large variant (every Orcapod table is large).
        if _same_field(exact, got):
            want = exact
        else:
            want = _expected_field(exact)
            if not _same_field(want, got):
                problems.append(f"{name}: {_describe(exact)} -> {_describe(got)}")
                continue
        expected = written.column(name)
        if want.type != exact.type:
            expected = expected.cast(want.type)
        if not back.column(name).combine_chunks().equals(expected.combine_chunks()):
            problems.append(f"{name}: values differ")
    if problems:
        return Cell(SILENT, "; ".join(problems))
    if database == "Delta" and _is_recorded(written.schema.field("x").type):
        return Cell(RECORDED)
    return Cell(KEPT)


def measure_all(root: Path) -> dict[tuple[str, str], Cell]:
    """Every cell of the matrix, keyed by (row label, database)."""
    out: dict[tuple[str, str], Cell] = {}
    for i, row in enumerate(ROWS):
        for database in DATABASES:
            out[(row.label, database)] = measure(
                row, database, root / f"r{i}" / database
            )
    return out


# ---------------------------------------------------------------------------
# The page
# ---------------------------------------------------------------------------

PAGE = Path(__file__).resolve().parents[2] / "docs" / "reference" / "database-types.md"

_CELL_TEXT = {
    KEPT: "kept",
    RECORDED: "kept (recorded type)",
    KEPT_BY_RULE: "kept (by rule)",
    REFUSED: "refused",
    SILENT: "**not kept**",
}

_INTRODUCTION = """\
# Data types per database

<!--
Generated by `uv run python scripts/database_type_matrix.py` from the measurements of
tests/test_databases/type_matrix.py; tests/test_databases/test_type_matrix.py fails when
this page and the measurements differ. Do not edit by hand.
-->

Each database Orcapod stores into keeps some Arrow types exactly and refuses the others.
None of them gives a column back as another type: a type a database cannot keep is
refused, with an error naming the column, before anything of the table is written. This
page lists every type for every database. It is generated from a test that writes and
reads each type through each database, so it cannot drift from the code.

What a cell says:

- **kept**: read back exactly, with its Arrow type (nested field names and nullability
  included) and its values. Text, binary and list columns may come back in their large
  variants (`large_string`, `large_binary`, `large_list`), the types every Orcapod table
  uses.
- **kept (recorded type)**: kept by `DeltaTableDatabase` through a recorded type. Delta Lake
  has no column type for it, so the column's Arrow type is recorded in its field metadata,
  its values are written in a form Delta keeps (an integer form, `float32` for
  `float16`, a `large_list` for a fixed-size list) and the type is restored on read.
- **kept (by rule)**: PostgreSQL stores the column, by the connector's own rule. No
  PostgreSQL server is used to build this page, so this cell is the rule, not a measured
  read.
- **refused**: writing the column raises an error naming it (`UnstorableColumnError` for
  a connector database), and nothing of the table is written.

How it is measured: a table holding two columns of the type (the second nullable, with a
null row) is written with `add_records`, flushed and read back with `get_all_records`.
The SpiralDB column is measured through the test suite's disk fake of SpiralDB, which
models what SpiralDB does to a column (no SpiralDB is used). No database tells tag
columns from data columns, so a type is kept or refused the same way in either role.
The library value types (NumPy, pandas, Polars, torch) are
[provisional](../concepts/arrays.md): their stored form may change in a later release.

The databases: `InMemoryArrowDatabase` (in-memory), `DeltaTableDatabase` (Delta), and
`ConnectorArrowDatabase` over `SQLiteConnector`, `PostgreSQLConnector` and
`SpiralDBConnector`. See [The database](../concepts/pipeline-job.md#the-database) for how a
job uses one.

Three refusals depend on the values as well as the type:

- SpiralDB cannot store a null struct, so a struct column with a null row is refused
  (the rows below have one); a struct column with no null row is kept.
- Delta has no `null` type: a new column holding only nulls with no type is refused (the
  `null` row below), while a `null`-typed value is kept in a column the table already
  stores with a type.
- A fixed-size list or fixed-size binary read back by Delta with another length or width
  than its type is refused on read, never padded or cut.
"""


def render(cells: dict[tuple[str, str], Cell]) -> str:
    """The page: the introduction, then one table per group of rows."""
    lines = [_INTRODUCTION]
    groups: dict[str, list[Row]] = {}
    for row in ROWS:
        groups.setdefault(row.group, []).append(row)
    header = "| Type | " + " | ".join(DATABASES) + " |"
    rule = "|---|" + "---|" * len(DATABASES)
    for group, rows in groups.items():
        lines.append(f"## {group[0].upper()}{group[1:]}\n")
        lines.append(header)
        lines.append(rule)
        for row in rows:
            label = f"`{row.label}`" + (" (provisional)" if row.provisional else "")
            texts = [_CELL_TEXT[cells[(row.label, db)].kind] for db in DATABASES]
            lines.append(f"| {label} | " + " | ".join(texts) + " |")
        lines.append("")
    return "\n".join(lines).rstrip("\n") + "\n"
