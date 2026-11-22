"""The supported-data-types matrix: what each database keeps of each Arrow type (ITL-1334).

Every row is a column type, every column a database. A cell is measured, never declared:
a table holding two columns of the type (the second with a null when the type is
nullable) is written with ``add_records`` and flushed; the same type is appended as slices
of a larger table (three rows over two chunks, then the null row alone at a nonzero
offset); the same columns with other field metadata (the column's own, and a nested
field's) are appended, which must be refused naming the column; and every batch is read
back with ``get_all_records``, then compared exactly with what was written (every nested field's name, nullability and metadata, a map's
``keys_sorted``, values bit for bit), up to the large variants every Orcapod table uses
(``arrow_utils.normalize_to_large_types``). The outcome is one of:

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
from typing import Any, cast
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
    arrow_type: pa.DataType,
    values: list[Any],
    nullable: bool = True,
    metadata: dict[str, str] | None = None,
) -> Callable[[Path], tuple[Any, Any, list[Any], list[Any]]]:
    """Two columns of ``arrow_type``; the second has a null in the middle if nullable."""

    def build(_root: Path) -> tuple[Any, Any, list[Any], list[Any]]:
        second = [values[0], None, values[2]] if nullable else list(reversed(values))
        return (
            pa.field("x", arrow_type, nullable=nullable, metadata=metadata),
            pa.field("y", arrow_type, nullable=nullable, metadata=metadata),
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


# ``pa.map_`` with key and item fields (which pyarrow takes; its stubs take types only).
map_of_fields: Any = pa.map_

D = decimal.Decimal
_NAN = float("nan")
_TS = [
    dt.datetime(2026, 1, 1, 0, 0, 1),
    dt.datetime(2026, 1, 2),
    dt.datetime(2026, 3, 4),
]
_DATES = [dt.date(2026, 1, 1), dt.date(2026, 1, 2), dt.date(1999, 12, 31)]
_TIMES = [dt.time(1, 2, 3), dt.time(4, 5, 6), dt.time(23, 59, 59)]
_DURATIONS = [dt.timedelta(seconds=1), dt.timedelta(seconds=2), dt.timedelta(days=1)]
_PER_SECOND = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}


def _ticks(unit: str, seconds: list[int]) -> list[int]:
    """``seconds`` as integer ticks of ``unit``, each off the whole second by a
    different number of ticks (the last ``unit`` digit included), so a conversion
    that loses precision (nanoseconds to microseconds) changes a value."""
    per_second = _PER_SECOND[unit]
    offsets = [0, 0, 0] if unit == "s" else [1, per_second // 3 + 7, per_second - 1]
    return [s * per_second + o for s, o in zip(seconds, offsets)]


_TS_SECONDS = [int(t.replace(tzinfo=dt.timezone.utc).timestamp()) for t in _TS]
_TIME_SECONDS = [t.hour * 3600 + t.minute * 60 + t.second for t in _TIMES]
_DURATION_SECONDS = [int(d.total_seconds()) for d in _DURATIONS]


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
    # -0.0 and NaN in both columns: values are compared bit for bit.
    Row("halffloat (float16)", "float", _plain(pa.float16(), [-0.0, 2.0, _NAN])),
    Row("float (float32)", "float", _plain(pa.float32(), [-0.0, 2.0, _NAN])),
    Row("double (float64)", "float", _plain(pa.float64(), [-0.0, 0.1, _NAN])),
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
    Row("time32[s]", "temporal", _plain(pa.time32("s"), _ticks("s", _TIME_SECONDS))),
    Row("time32[ms]", "temporal", _plain(pa.time32("ms"), _ticks("ms", _TIME_SECONDS))),
    Row("time64[us]", "temporal", _plain(pa.time64("us"), _ticks("us", _TIME_SECONDS))),
    Row("time64[ns]", "temporal", _plain(pa.time64("ns"), _ticks("ns", _TIME_SECONDS))),
    *(
        Row(
            f"timestamp[{u}]",
            "temporal",
            _plain(pa.timestamp(u), _ticks(u, _TS_SECONDS)),
        )
        for u in ("s", "ms", "us", "ns")
    ),
    *(
        Row(
            f"timestamp[{u}, tz=UTC]",
            "temporal",
            _plain(pa.timestamp(u, tz="UTC"), _ticks(u, _TS_SECONDS)),
        )
        for u in ("s", "ms", "us", "ns")
    ),
    *(
        Row(
            f"timestamp[{u}, tz=Europe/Berlin]",
            "temporal",
            _plain(pa.timestamp(u, tz="Europe/Berlin"), _ticks(u, _TS_SECONDS)),
        )
        for u in ("s", "ms", "us", "ns")
    ),
    *(
        Row(
            f"duration[{u}]",
            "temporal",
            _plain(pa.duration(u), _ticks(u, _DURATION_SECONDS)),
        )
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
        "struct<> (no fields)",
        "struct and map",
        _plain(pa.struct([]), [{}, {}, {}]),
    ),
    Row(
        "uuid (Arrow's canonical extension type)",
        "struct and map",
        _plain(pa.uuid(), [uuid.UUID(int=i + 1).bytes for i in range(3)]),
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
    Row(
        "fixed_size_list<int16>[0]",
        "list",
        _plain(pa.list_(pa.int16(), 0), [[], [], []]),
    ),
    Row(
        "large_list<named: int16 {unit: m}>",
        "names and metadata",
        _plain(
            pa.large_list(pa.field("named", pa.int16(), metadata={"unit": "m"})),
            [[1, 2], [], [3, None]],
        ),
    ),
    Row(
        "struct<a: int32 {unit: s}>",
        "names and metadata",
        _plain(
            pa.struct([pa.field("a", pa.int32(), metadata={"unit": "s"})]),
            [{"a": 1}, {"a": None}, {"a": 3}],
        ),
    ),
    Row(
        "map<string, int64>, keys sorted",
        "names and metadata",
        _plain(
            pa.map_(pa.string(), pa.int64(), keys_sorted=True),  # pyright: ignore[reportCallIssue]  # the stubs lack keys_sorted
            [[("a", 1), ("b", 2)], [], [("c", None)]],
        ),
    ),
    Row(
        "map<k: string, v: int64>, keys sorted",
        "names and metadata",
        _plain(
            pa.map_(  # pyright: ignore[reportCallIssue]  # pyarrow takes key/item fields
                pa.field("k", pa.string(), nullable=False),
                pa.field("v", pa.int64()),
                keys_sorted=True,
            ),
            [[("a", 1), ("b", 2)], [], [("c", None)]],
        ),
    ),
    Row(
        "map<string, int64 {unit: m}> (item metadata)",
        "names and metadata",
        _plain(
            map_of_fields(
                pa.field("key", pa.string(), nullable=False),
                pa.field("value", pa.int64(), metadata={"unit": "m"}),
            ),
            [[("a", 1), ("b", 2)], [], [("c", None)]],
        ),
    ),
    Row(
        "map<string {unit: m}, int64> (key metadata)",
        "names and metadata",
        _plain(
            map_of_fields(
                pa.field("key", pa.string(), nullable=False, metadata={"unit": "m"}),
                pa.field("value", pa.int64()),
            ),
            [[("a", 1), ("b", 2)], [], [("c", None)]],
        ),
    ),
    Row(
        "map<string, struct<a: int64 {unit: m}>> (metadata inside the item)",
        "names and metadata",
        _plain(
            pa.map_(
                pa.string(),
                pa.struct([pa.field("a", pa.int64(), metadata={"unit": "m"})]),
            ),
            [[("a", {"a": 1})], [], [("c", {"a": None})]],
        ),
    ),
    Row(
        "int64 {unit: s} (field metadata)",
        "names and metadata",
        _plain(pa.int64(), [1, 2, 3], metadata={"unit": "s"}),
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
def open_database(name: str, root: Path) -> Iterator[Any]:
    """The database ``name`` (one of ``DATABASES``), its files under ``root``."""
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


def expected_field(field: "pa.Field[Any]") -> "pa.Field[Any]":
    return field.with_type(normalize_to_large_types(field.type, keep_null=True))


def field_difference(a: "pa.Field[Any]", b: "pa.Field[Any]", path: str) -> str | None:
    """The first difference between two fields, at any depth, or ``None``.

    Everything that makes a field counts: its name, nullability and metadata, and its
    type's kind and parameters, with every child field compared the same way (a list's
    item, a struct's children, a map's key and item and whether its keys are sorted).
    Arrow's own type equality ignores child names and metadata, so it is not used.
    """
    if a.name != b.name:
        return f"{path}: name {a.name!r} -> {b.name!r}"
    if a.nullable != b.nullable:
        return f"{path}: nullable {a.nullable} -> {b.nullable}"
    if dict(a.metadata or {}) != dict(b.metadata or {}):
        return f"{path}: metadata {dict(a.metadata or {})} -> {dict(b.metadata or {})}"
    return _type_difference(a.type, b.type, path)


def _type_difference(a: Any, b: Any, path: str) -> str | None:
    t = pa.types
    if isinstance(a, pa.ExtensionType) or isinstance(b, pa.ExtensionType):
        if not (
            isinstance(a, pa.ExtensionType)
            and isinstance(b, pa.ExtensionType)
            and a.extension_name == b.extension_name
            and a.__arrow_ext_serialize__() == b.__arrow_ext_serialize__()
        ):
            return f"{path}: {a} -> {b}"
        return _type_difference(a.storage_type, b.storage_type, path)
    kinds = (
        t.is_struct,
        t.is_map,
        t.is_fixed_size_list,
        t.is_large_list,
        t.is_list,
    )
    kind_a = next((k for k in kinds if k(a)), None)
    kind_b = next((k for k in kinds if k(b)), None)
    if kind_a is not kind_b or (kind_a is None and a != b):
        return f"{path}: {a} -> {b}"
    if kind_a is None:
        return None
    if t.is_map(a):
        if a.keys_sorted != b.keys_sorted:
            return f"{path}: keys_sorted {a.keys_sorted} -> {b.keys_sorted}"
        return field_difference(a.key_field, b.key_field, f"{path}.key") or (
            field_difference(a.item_field, b.item_field, f"{path}.item")
        )
    if t.is_fixed_size_list(a) and a.list_size != b.list_size:
        return f"{path}: list size {a.list_size} -> {b.list_size}"
    if t.is_struct(a):
        if a.num_fields != b.num_fields:
            return f"{path}: {a} -> {b}"
        for i in range(a.num_fields):
            found = field_difference(
                a.field(i), b.field(i), f"{path}.{a.field(i).name}"
            )
            if found:
                return found
        return None
    return field_difference(a.value_field, b.value_field, f"{path}[]")


def _lengths(array: Any) -> list[int]:
    """The item count of each non-null row of a list or map array (from its offsets:
    pyarrow's ``list_value_length`` has no kernel for a map)."""
    import numpy as np

    counts = np.diff(array.offsets.to_numpy(zero_copy_only=False))
    return counts[array.is_valid().to_numpy(zero_copy_only=False)].tolist()


def _items(array: Any) -> list[Any]:
    """The items of the non-null rows of a list or map array, in row order: the values
    of a list, the keys and the items of a map (whose arrays have no ``flatten``)."""
    import numpy as np

    offsets = array.offsets.to_numpy(zero_copy_only=False)
    rows = np.flatnonzero(array.is_valid().to_numpy(zero_copy_only=False))
    indices = pa.array(
        np.concatenate(
            [np.arange(offsets[r], offsets[r + 1]) for r in rows] or [np.zeros(0)]
        ).astype(np.int64)
    )
    if pa.types.is_map(array.type):
        return [array.keys.take(indices), array.items.take(indices)]
    return [array.values.take(indices)]


def _same_values(a: Any, b: Any) -> bool:
    """Whether two arrays of one type hold the same values, bit for bit.

    Nulls must be in the same rows. A float is compared by its bits (``-0.0`` is not
    ``0.0``, a NaN equals the same NaN); nested values are compared child by child, and
    an extension array by its storage.
    """
    if isinstance(a, pa.ChunkedArray):
        a = a.combine_chunks()
    if isinstance(b, pa.ChunkedArray):
        b = b.combine_chunks()
    if len(a) != len(b) or not a.is_null().equals(b.is_null()):
        return False
    t = pa.types
    if isinstance(a.type, pa.ExtensionType):
        return _same_values(a.storage, b.storage)
    if t.is_floating(a.type):
        bits = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}[a.type.bit_width]
        return a.view(bits).equals(b.view(bits))
    if t.is_struct(a.type):
        return all(_same_values(x, y) for x, y in zip(a.flatten(), b.flatten()))
    if t.is_list(a.type) or t.is_large_list(a.type) or t.is_map(a.type):
        return _lengths(a) == _lengths(b) and all(
            _same_values(x, y) for x, y in zip(_items(a), _items(b))
        )
    if t.is_fixed_size_list(a.type):
        return _same_values(a.flatten(), b.flatten())
    return a.equals(b)


_CHANGED = {b"orcapod:matrix": b"changed"}


def _with_changed_metadata(field: "pa.Field[Any]") -> "pa.Field[Any]":
    return field.with_metadata(cast(Any, {**(field.metadata or {}), **_CHANGED}))


def _nested_changed(arrow_type: Any) -> Any:
    """``arrow_type`` with its first nested field's metadata changed, or ``None``."""
    t = pa.types
    if isinstance(arrow_type, pa.BaseExtensionType):
        return None
    if t.is_struct(arrow_type):
        fields = list(arrow_type)
        if not fields:
            return None
        return pa.struct([_with_changed_metadata(fields[0]), *fields[1:]])
    if t.is_map(arrow_type):
        return pa.map_(  # pyright: ignore[reportCallIssue]  # pyarrow takes key/item fields
            arrow_type.key_field,
            _with_changed_metadata(arrow_type.item_field),
            keys_sorted=arrow_type.keys_sorted,
        )
    if t.is_large_list(arrow_type):
        return pa.large_list(_with_changed_metadata(arrow_type.value_field))
    if t.is_list(arrow_type):
        return pa.list_(_with_changed_metadata(arrow_type.value_field))
    if t.is_fixed_size_list(arrow_type):
        return pa.list_(
            _with_changed_metadata(arrow_type.value_field), arrow_type.list_size
        )
    return None


def _attribute_changes(rows: pa.Table) -> list[tuple[str, pa.Table]]:
    """``rows`` (new ids) with other field attributes on ``x`` and ``y``: the columns'
    own metadata, then, for a nested type, its first nested field's metadata."""
    ids = pa.array([b"c1", b"c2", b"c3"], pa.large_binary())
    rows = rows.set_column(0, rows.schema.field("id"), ids)
    out = []
    own = rows
    for name in ("x", "y"):
        index = own.schema.get_field_index(name)
        own = own.set_column(
            index, _with_changed_metadata(own.schema.field(name)), own.column(index)
        )
    out.append(("column metadata", own))
    nested = rows
    for name in ("x", "y"):
        index = nested.schema.get_field_index(name)
        field = nested.schema.field(name)
        changed = _nested_changed(field.type)
        if changed is None:
            return out
        # Built from the values: the append must be refused, so they are never read.
        column = pa.array(nested.column(index).to_pylist(), changed)
        nested = nested.set_column(index, field.with_type(changed), column)
    out.append(("nested field metadata", nested))
    return out


def measure(row: Row, database: str, root: Path) -> Cell:
    """Write ``row``'s table to ``database`` and say what came back."""
    root.mkdir(parents=True, exist_ok=True)
    try:
        written = build_table(row, root)
    except Exception as exc:  # noqa: BLE001  # a row Orcapod cannot build is reported
        return Cell(SILENT, f"cannot build the column: {type(exc).__name__}: {exc}")
    with open_database(database, root) as db:
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
                # A refusal writes nothing: the table still reads back empty.
                try:
                    db.flush()
                    stored = db.get_all_records(path, record_id_column="id")
                except Exception as read_exc:  # noqa: BLE001
                    return Cell(SILENT, f"refused, then unreadable: {read_exc}")
                if stored is not None and stored.num_rows:
                    return Cell(SILENT, "refused, but rows were stored")
                return Cell(REFUSED, f"{type(exc).__name__}")
            return Cell(
                SILENT,
                f"write failed without naming the column: {type(exc).__name__}: {str(exc)[:160]}",
            )
        # The same type appended as slices (a list's offsets then start past zero):
        # three rows over two chunks, then the null row alone at a nonzero offset. The
        # stored type must take them, and every batch reads back exactly (on Delta,
        # through the merged record).
        doubled = pa.concat_tables([written, written])
        for ids, rows in (
            ([b"s1", b"s2", b"s3"], doubled.slice(1, 3)),
            ([b"s4"], doubled.slice(4, 1)),
        ):
            appended = rows.set_column(
                0, written.schema.field("id"), pa.array(ids, pa.large_binary())
            )
            try:
                db.add_records(path, appended, record_id_column="id", flush=True)
            except Exception as exc:  # noqa: BLE001
                return Cell(
                    SILENT,
                    f"first write kept, the same type appended as a slice failed: "
                    f"{type(exc).__name__}: {str(exc)[:160]}",
                )
            written = pa.concat_tables([written, appended])
        # The same columns with other field attributes appended: the table holds one
        # type per column, so the append must be refused naming the column (the rows
        # read back below are then those already written).
        for where, changed in _attribute_changes(written.slice(0, 3)):
            try:
                db.add_records(path, changed, record_id_column="id", flush=True)
            except Exception as exc:  # noqa: BLE001
                if not _names_a_column(exc):
                    return Cell(
                        SILENT,
                        f"an append with other {where} failed without naming the "
                        f"column: {type(exc).__name__}: {str(exc)[:160]}",
                    )
                db.flush()
                continue
            return Cell(SILENT, f"an append with other {where} was accepted")
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
        want = exact
        difference = field_difference(exact, got, name)
        if difference is not None:
            want = expected_field(exact)
            if field_difference(want, got, name) is not None:
                problems.append(difference)
                continue
        expected = written.column(name)
        if want.type != exact.type:
            expected = expected.cast(want.type)
        if not _same_values(back.column(name), expected):
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
For every type on this page, each database either gives the column back exactly or refuses
it, with an error naming the column, before anything of the table is written: none gives
it back as another type. The page is generated from a test that writes and reads each
type through each database, so it cannot drift from the code. The rows are
representative, not every Arrow type: dictionary-encoded columns are refused by every
stream before they reach a database, and union, run-end-encoded and list-view types are
not covered.

What a cell says:

- **kept**: read back exactly: the Arrow type with the name, nullability and metadata of
  the column and of every nested field (a list's item, a struct's children, a map's key
  and item, and whether its keys are sorted), and the values bit for bit (a float's
  `-0.0` and NaN included). Text, binary and list columns may come back in their large
  variants (`large_string`, `large_binary`, `large_list`), the types every Orcapod table
  uses.
- **kept (recorded type)**: kept by `DeltaTableDatabase` through a recorded type. Delta
  Lake has no column type for it, or gives it back changed (it names every list item
  `element` and drops a map's sorted keys), so the column's Arrow type is recorded in its
  field metadata, its values are written in a form Delta keeps (an integer form,
  `float32` for `float16`, a `large_list` for a fixed-size list) and the type is restored
  on read, nested fields matched by name.
- **kept (by rule)**: PostgreSQL stores the column, by the connector's own rule. No
  PostgreSQL server is used to build this page, so this cell is the rule, not a measured
  read: it does not show that the server keeps every value (a NaN, a time zone).
- **refused**: writing the column raises an error naming it (`UnstorableColumnError` for
  a connector database), and nothing of the table is written.

How it is measured: a table holding two columns of the type (the second nullable, with a
null row) is written with `add_records` and flushed. The same type is then appended as
slices of a larger table (three rows over two chunks, then the null row alone), and the
same columns with other field metadata (the column's own, and a nested field's) are
appended, which every database must refuse, naming the column. Every batch written is read
back with `get_all_records`.
The SpiralDB column is measured through the test suite's disk fake of SpiralDB, which
models what SpiralDB does to a column (no SpiralDB is used). No database tells tag
columns from data columns, so a type is kept or refused the same way in either role.
The library value types (NumPy, pandas, Polars, torch) are
[provisional](../concepts/arrays.md): their stored form may change in a later release.

The databases: `InMemoryArrowDatabase` (in-memory), `DeltaTableDatabase` (Delta), and
`ConnectorArrowDatabase` over `SQLiteConnector`, `PostgreSQLConnector` and
`SpiralDBConnector`. See [The database](../concepts/pipeline-job.md#the-database) for how a
job uses one.

Some refusals depend on the values or names as well as the type:

- SpiralDB cannot store a null struct, so a struct column with a null row is refused
  (the rows below have one); a struct column with no null row is kept.
- SQLite reads a NaN back as NULL and `-0.0` as `0.0`, so a `double` column holding one
  is refused (the float rows below hold both); one without them is kept.
- Delta has no `null` type: a new column, or a new nested field, holding only nulls with
  no type is refused (the `null` row below), while a `null`-typed value is kept where the
  table already stores a type.
- Delta and SpiralDB give a map's key and item fields back as `key` and `value`, so a map
  with other names is refused. SpiralDB does not keep metadata on a map's key or item
  field, so a map with such metadata is refused there.
- A fixed-size list or fixed-size binary read back by Delta with another length or width
  than its type is refused on read, never padded or cut.

A table holds one type per column. An append whose columns differ from the held ones
(stored, or still pending) in field metadata (the column's own, or a nested field's: a
struct child, a list item, a map key or item), in a nested field's name, or in whether a
map's keys are sorted is refused by every database, naming the column and the field,
before anything is written: either side's rows would otherwise read back with the other
side's attributes. A change of nullability is left to each database's own rule.

Appending to a Delta table may evolve a struct column (fields added, omitted or in
another order): every value keeps its field's name, and an omitted field reads as null
in the appended rows. An append is refused, naming the column and the field, when it
would add a non-nullable struct field (the stored rows have no value for it) or add a
struct field inside a map (Delta cannot). A nested field the append makes nullable reads
as nullable afterwards, as Delta stores it.

An append matches columns by name, exactly as Arrow matches them (`a` and `A` are two
columns), never by position. Rows whose columns are the held table's columns in another
order, each of the held type (a logical type compared by its storage type), are put in
the held order (the rows still pending, else the stored table) by every database before
they are queued and before the checks that compare them with the held columns (field
attributes, declared nulls, schema compatibility), whether the table was flushed in
between or not; the checks that look at the rows alone (a storable type, the record id)
come first. Rows still queued are matched again, by name, to the stored table when they
are read with it or written. A table keeps the column
order the database gave its first write: the first write's order, except in SpiralDB,
which stores and reads a table's key columns first and then its other columns sorted by
name, whatever the order written (measured on SpiralDB itself). An append that also
adds, omits or retypes a column keeps each database's own rule. The order of a struct's
fields is not covered: a struct whose fields are in another order is another type here.
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
