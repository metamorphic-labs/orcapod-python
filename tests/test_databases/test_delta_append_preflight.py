"""What a Delta flush would refuse is refused when the rows are added (ITL-1334).

``add_records`` checks the rows against the stored table before buffering anything
(``_append_refusal``): the refusal names the column, nothing is queued, and the stored
rows read back unchanged. The flush repeats the check against the version it loads,
since another writer may commit in between; rows it refuses leave the queue, so no
later flush fails on them again.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod.databases import DeltaTableDatabase
from orcapod.errors import InputValidationError, UnstorableColumnError
from tests.test_databases.type_matrix import map_of_fields

PATH = ("t",)
I64 = pa.int64()


def _rows(ids: list[bytes], **columns: tuple[Any, list[Any]]) -> pa.Table:
    """``id`` then each column, given as (type or field, values)."""
    fields = [pa.field("id", pa.large_binary())]
    arrays = [pa.array(ids, pa.large_binary())]
    for name, (kind, values) in columns.items():
        field = (
            kind.with_name(name) if isinstance(kind, pa.Field) else pa.field(name, kind)
        )
        fields.append(field)
        arrays.append(pa.array(values, field.type))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _struct(*fields: Any) -> Any:
    """A struct of exactly the fields given, in their order, each with its own name,
    type, nullability and metadata."""
    return pa.struct(list(fields))


A_NOT_NULL = pa.field("a", I64, nullable=False)
B = pa.field("b", I64)

# (stored rows, appended rows, what the refusal names): two columns where it matters.
REFUSED: dict[str, tuple[pa.Table, pa.Table, str]] = {
    "a non-nullable struct field omitted": (
        _rows([b"1", b"2"], x=(_struct(A_NOT_NULL, B), [{"a": 1, "b": 2}] * 2)),
        _rows([b"3", b"4"], x=(_struct(B), [{"b": 3}, {"b": None}])),
        r"'x': x\.a is stored as non-nullable, and the rows omit it",
    ),
    "a non-nullable struct field omitted in a list": (
        _rows([b"1"], x=(pa.large_list(_struct(A_NOT_NULL, B)), [[{"a": 1, "b": 2}]])),
        _rows([b"3"], x=(pa.large_list(_struct(B)), [[{"b": 3}]])),
        r"'x': x\[\]\.a is stored as non-nullable",
    ),
    "a non-nullable struct field omitted in a map item": (
        _rows(
            [b"1"],
            x=(
                pa.map_(pa.large_string(), _struct(A_NOT_NULL, B)),
                [[("k", {"a": 1, "b": 2})]],
            ),
        ),
        _rows([b"3"], x=(pa.map_(pa.large_string(), _struct(B)), [[("k", {"b": 3})]])),
        r"'x': x\.item\.a is stored as non-nullable",
    ),
    "a non-nullable column omitted": (
        _rows(
            [b"1", b"2"],
            x=(pa.field("x", I64, nullable=False), [1, 2]),
            y=(I64, [1, 2]),
        ),
        _rows([b"3", b"4"], y=(I64, [3, 4])),
        r"'x': x is stored as non-nullable, and the rows omit it",
    ),
    "a null in a non-nullable column": (
        _rows(
            [b"1", b"2"],
            x=(pa.field("x", I64, nullable=False), [1, 2]),
            y=(I64, [1, 2]),
        ),
        _rows([b"3", b"4"], x=(I64, [3, None]), y=(I64, [3, 4])),
        r"'x': x is stored as non-nullable, and the rows hold a null",
    ),
    "another type": (
        _rows([b"1", b"2"], x=(I64, [1, 2]), y=(I64, [1, 2])),
        _rows([b"3", b"4"], x=(pa.float64(), [3.0, 4.0]), y=(I64, [3, 4])),
        r"'x': it is stored as int64 but the new rows have double",
    ),
    "a timestamp unit the stored column does not have": (
        _rows([b"1", b"2"], x=(pa.timestamp("us"), [1, 2])),
        _rows([b"3", b"4"], x=(pa.timestamp("ms"), [3, 4])),
        r"'x': it is stored as timestamp\[us\] but the new rows have timestamp\[ms\]",
    ),
    "a map key type beside null items": (
        _rows([b"1"], x=(pa.map_(pa.large_string(), I64), [[("a", 1)]])),
        _rows([b"3"], x=(pa.map_(I64, pa.null()), [[(5, None)]])),
        r"'x': it is stored as map<large_string, int64> but the new rows have "
        r"map<int64, int64>",
    ),
    "float16 into a float32 column": (
        _rows([b"1", b"2"], x=(pa.float32(), [1.0, 2.0])),
        _rows([b"3", b"4"], x=(pa.float16(), [3.0, 4.0])),
        r"'x': it is stored as float but the new rows have halffloat",
    ),
}


@pytest.mark.parametrize("case", sorted(REFUSED))
def test_an_append_the_flush_would_refuse_is_refused_when_added(
    tmp_path: Path, case: str
) -> None:
    """The refusal names the column (and the field), nothing is queued, a later flush
    has nothing to fail on, and the stored rows read back unchanged, on the same and
    on a fresh database object."""
    first, second, match = REFUSED[case]
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, first, record_id_column="id", flush=True)
    with pytest.raises(InputValidationError, match=match):
        db.add_records(PATH, second, record_id_column="id")
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]  # nothing queued
    db.flush()
    for reader in (db, DeltaTableDatabase(tmp_path)):
        back = reader.get_all_records(PATH, record_id_column="id")
        assert back is not None
        assert back.sort_by("id").to_pylist() == first.sort_by("id").to_pylist()


# The controls: what the flush takes is not refused. (stored rows, appended rows, the
# columns compared, the rows read back in id order.)
KEPT: dict[str, tuple[pa.Table, pa.Table, list[str], list[dict[str, Any]]]] = {
    "a nullable struct field omitted": (
        _rows([b"1"], x=(_struct(pa.field("a", I64), B), [{"a": 1, "b": 2}])),
        _rows([b"3"], x=(_struct(B), [{"b": 3}])),
        ["x"],
        [{"x": {"a": 1, "b": 2}}, {"x": {"a": None, "b": 3}}],
    ),
    "a nullable column omitted": (
        _rows([b"1"], x=(I64, [1]), y=(I64, [1])),
        _rows([b"3"], y=(I64, [3])),
        ["x", "y"],
        [{"x": 1, "y": 1}, {"x": None, "y": 3}],
    ),
    "a nullable type without nulls into a non-nullable column": (
        _rows([b"1"], x=(pa.field("x", I64, nullable=False), [1])),
        _rows([b"3"], x=(I64, [3])),
        ["x"],
        [{"x": 1}, {"x": 3}],
    ),
    "a new float16 column": (
        _rows([b"1"], y=(I64, [1])),
        _rows([b"3", b"4"], y=(I64, [3, 4]), x=(pa.float16(), [1.5, None])),
        ["x", "y"],
        [{"x": None, "y": 1}, {"x": 1.5, "y": 3}, {"x": None, "y": 4}],
    ),
    "a null item of a map takes its type from the stored map": (
        _rows([b"1"], x=(pa.map_(pa.large_string(), I64), [[("a", 1)]])),
        _rows([b"3"], x=(pa.map_(pa.large_string(), pa.null()), [[("b", None)]])),
        ["x"],
        [{"x": [("a", 1)]}, {"x": [("b", None)]}],
    ),
    "the same timestamp unit": (
        _rows([b"1"], x=(pa.timestamp("us"), [1])),
        _rows([b"3"], x=(pa.timestamp("us"), [3])),
        ["x"],
        [
            {"x": dt.datetime(1970, 1, 1, 0, 0, 0, 1)},
            {"x": dt.datetime(1970, 1, 1, 0, 0, 0, 3)},
        ],
    ),
}


@pytest.mark.parametrize("case", sorted(KEPT))
def test_an_append_the_flush_takes_is_kept(tmp_path: Path, case: str) -> None:
    """Both batches read back on a fresh object, value for value (a null kept as a
    null), with the stored column's type: a new ``float16`` column as ``float16``."""
    first, second, columns, expected = KEPT[case]
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, first, record_id_column="id", flush=True)
    db.add_records(PATH, second, record_id_column="id", flush=True)
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="id")
    assert back is not None
    assert back.sort_by("id").select(columns).to_pylist() == expected
    if case == "a new float16 column":
        assert back.schema.field("x").type == pa.float16()


@pytest.mark.parametrize(
    "arrow_type",
    [
        pa.struct([]),
        pa.struct([pa.field("e", pa.struct([]))]),
        pa.large_list(pa.struct([])),
        map_of_fields(
            pa.field("key", pa.large_string(), False), pa.field("value", pa.struct([]))
        ),
    ],
    ids=["top level", "in a struct", "in a list", "in a map item"],
)
def test_a_struct_with_no_fields_is_refused_before_buffering(
    tmp_path: Path, arrow_type: Any
) -> None:
    """A Parquet file stores values in leaf columns only, so Delta cannot write it."""
    db = DeltaTableDatabase(tmp_path)
    with pytest.raises(UnstorableColumnError, match="'x'.*a struct with no fields"):
        db.add_records(
            PATH,
            pa.table(
                {
                    "id": pa.array([b"1"], pa.large_binary()),
                    "x": pa.nulls(1, arrow_type),
                }
            ),
            record_id_column="id",
        )
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize(
    "conflicting",
    [
        _rows([b"9"], x=(pa.float64(), [9.0])),
        _rows([b"9"], x=(pa.field("x", I64, metadata={"unit": "s"}), [9])),
    ],
    ids=["another type", "other field metadata"],
)
def test_the_flush_refuses_rows_another_writer_made_conflict(
    tmp_path: Path, conflicting: pa.Table
) -> None:
    """Rows queued while nothing was stored; another writer then stores the column
    otherwise. The flush refuses them against the version it loads, drops them from
    the queue, and says so; the other writer's rows stand."""
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, _rows([b"1", b"2"], x=(I64, [1, 2])), record_id_column="id")
    DeltaTableDatabase(tmp_path).add_records(
        PATH, conflicting, record_id_column="id", flush=True
    )
    with pytest.raises(InputValidationError, match="'x'") as raised:
        db.flush()
    refusal = raised.value
    assert any("no longer queued" in note for note in getattr(refusal, "__notes__", []))
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
    db.flush()  # nothing left to refuse
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="id")
    assert back is not None and back.column("id").to_pylist() == [b"9"]


def test_null_parts_typed_partly_by_each_schema_append(tmp_path: Path) -> None:
    """A ``null`` part needs a type from the stored table or a pending batch, not every
    part from one of them; each is written as typed nulls."""
    db = DeltaTableDatabase(tmp_path)
    db.add_records(
        PATH,
        _rows([b"1"], x=(_struct(pa.field("a", I64)), [{"a": 1}])),
        record_id_column="id",
        flush=True,
    )
    db.add_records(
        PATH,
        _rows([b"2"], x=(_struct(pa.field("a", pa.null()), B), [{"a": None, "b": 2}])),
        record_id_column="id",
    )
    db.add_records(
        PATH,
        _rows(
            [b"3"],
            x=(
                _struct(pa.field("a", pa.null()), pa.field("b", pa.null())),
                [{"a": None, "b": None}],
            ),
        ),
        record_id_column="id",
        schema_handling="merge",
        flush=True,
    )
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="id")
    assert back is not None
    assert back.schema.field("x").type == _struct(pa.field("a", I64), B)
    assert back.sort_by("id").column("x").to_pylist() == [
        {"a": 1, "b": None},
        {"a": None, "b": 2},
        {"a": None, "b": None},
    ]


@pytest.mark.parametrize(
    "appended",
    [
        _struct(pa.field("a", pa.null()), B),
        pa.large_list(_struct(pa.field("a", pa.null()), B)),
    ],
    ids=["top level", "in a list"],
)
def test_a_null_child_of_an_evolving_struct_appends(
    tmp_path: Path, appended: Any
) -> None:
    """Delta's MERGE panicked on a ``null`` child of a struct that gains a field
    (pre-existing); the child is written as typed nulls of its stored type."""
    stored_type = _struct(pa.field("a", I64))
    in_list = pa.types.is_large_list(appended)
    db = DeltaTableDatabase(tmp_path)
    db.add_records(
        PATH,
        _rows(
            [b"1"],
            x=(
                pa.large_list(stored_type) if in_list else stored_type,
                [[{"a": 1}]] if in_list else [{"a": 1}],
            ),
        ),
        record_id_column="id",
        flush=True,
    )
    value = {"a": None, "b": 2}
    db.add_records(
        PATH,
        _rows([b"2"], x=(appended, [[value]] if in_list else [value])),
        record_id_column="id",
        flush=True,
    )
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="id")
    assert back is not None
    rows: list[Any] = back.sort_by("id").column("x").to_pylist()
    if in_list:
        rows = [cast(list[Any], r)[0] for r in rows]
    assert rows == [{"a": 1, "b": None}, {"a": None, "b": 2}]


def test_a_null_path_no_schema_types_is_refused_naming_every_path(
    tmp_path: Path,
) -> None:
    """Every ``null`` part no schema types is named (``x.b``, ``x.c``), not only the
    first; ``x.a``, which the stored table types, is not."""
    db = DeltaTableDatabase(tmp_path)
    db.add_records(
        PATH,
        _rows([b"1"], x=(_struct(pa.field("a", I64)), [{"a": 1}])),
        record_id_column="id",
        flush=True,
    )
    with pytest.raises(UnstorableColumnError, match=r"'x\.b', 'x\.c'"):
        db.add_records(
            PATH,
            _rows(
                [b"2"],
                x=(
                    _struct(*(pa.field(n, pa.null()) for n in "abc")),
                    [{"a": None, "b": None, "c": None}],
                ),
            ),
            record_id_column="id",
        )


def test_the_check_runs_on_the_rows_coerced_to_the_pending_batch(
    tmp_path: Path,
) -> None:
    """``schema_handling="coerce"`` casts the rows to the pending batch's types before
    they are checked against the stored table, so ``int32`` rows behind a stored and a
    pending ``int64`` column append; without it, the same rows are refused."""
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, _rows([b"1"], x=(I64, [1])), record_id_column="id", flush=True)
    db.add_records(PATH, _rows([b"2"], x=(I64, [2])), record_id_column="id")
    with pytest.raises(ValueError, match="Schema mismatch"):
        db.add_records(PATH, _rows([b"3"], x=(pa.int32(), [3])), record_id_column="id")
    db.add_records(
        PATH,
        _rows([b"3"], x=(pa.int32(), [3])),
        record_id_column="id",
        schema_handling="coerce",
        flush=True,
    )
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="id")
    assert back is not None
    assert back.schema.field("x").type == I64
    assert back.sort_by("id").column("x").to_pylist() == [1, 2, 3]


@pytest.mark.parametrize(
    ("values", "kept"),
    [([None, 7], 7), ([7, None], None)],
    ids=["null first", "null last"],
)
def test_values_are_checked_on_the_rows_kept_after_duplicates(
    tmp_path: Path, values: list[Any], kept: Any
) -> None:
    """A repeated id keeps its last row: a null in a dropped row is not refused, a null
    in the kept row is, before anything is queued."""
    db = DeltaTableDatabase(tmp_path)
    db.add_records(
        PATH,
        _rows([b"1"], x=(pa.field("x", I64, nullable=False), [1])),
        record_id_column="id",
        flush=True,
    )
    repeated = _rows([b"2", b"2"], x=(I64, values))
    if kept is None:
        with pytest.raises(InputValidationError, match="'x'.*hold a null"):
            db.add_records(PATH, repeated, record_id_column="id")
        assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
        expected = [1]
    else:
        db.add_records(PATH, repeated, record_id_column="id", flush=True)
        expected = [1, kept]
    db.flush()
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="id")
    assert back is not None
    assert back.sort_by("id").column("x").to_pylist() == expected


def test_rows_skip_duplicates_skips_are_not_checked(tmp_path: Path) -> None:
    """``skip_duplicates=True`` skips a stored id; its null is never written, so it is
    not refused."""
    db = DeltaTableDatabase(tmp_path)
    db.add_records(
        PATH,
        _rows([b"1"], x=(pa.field("x", I64, nullable=False), [1])),
        record_id_column="id",
        flush=True,
    )
    db.add_records(
        PATH,
        _rows([b"1", b"2"], x=(I64, [None, 2])),
        record_id_column="id",
        skip_duplicates=True,
        flush=True,
    )
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="id")
    assert back is not None
    assert back.sort_by("id").column("x").to_pylist() == [1, 2]


def test_a_flush_refusal_drops_what_the_handle_cached_of_the_table(
    tmp_path: Path,
) -> None:
    """A handle that read the table, then had its queued rows refused against a
    version another writer committed, reads that version afterwards: its rows and
    its new column, not the version it had cached."""
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, _rows([b"1"], x=(I64, [1])), record_id_column="id", flush=True)
    assert db.get_all_records(PATH, record_id_column="id") is not None  # cached
    db.add_records(
        PATH,
        _rows([b"2"], x=(I64, [2]), y=(pa.float64(), [2.0])),
        record_id_column="id",
    )
    DeltaTableDatabase(tmp_path).add_records(
        PATH,
        _rows([b"9"], x=(I64, [9]), y=(I64, [9])),
        record_id_column="id",
        flush=True,
    )
    with pytest.raises(InputValidationError, match="'y'"):
        db.flush()
    back = db.get_all_records(PATH, record_id_column="id")
    assert back is not None
    assert back.sort_by("id").select(["x", "y"]).to_pylist() == [
        {"x": 1, "y": None},
        {"x": 9, "y": 9},
    ]


def test_a_refused_merge_leaves_the_queued_rows_as_they_were(tmp_path: Path) -> None:
    """``schema_handling="merge"`` re-types the queued rows only when the new rows are
    accepted: a refused merge leaves them as queued, so the flush writes them."""
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, _rows([b"1"], x=(I64, [1])), record_id_column="id", flush=True)
    db.add_records(PATH, _rows([b"2"], x=(pa.null(), [None])), record_id_column="id")
    with pytest.raises(InputValidationError, match="'x': it is stored as int64"):
        db.add_records(
            PATH,
            _rows([b"3"], x=(pa.float64(), [3.0])),
            record_id_column="id",
            schema_handling="merge",
        )
    pending = db._pending_batches["t"]  # pyright: ignore[reportPrivateUsage]
    assert pending.schema.field("x").type == pa.null()
    assert pending.select(["__record_id", "x"]).to_pylist() == [
        {"__record_id": b"2", "x": None}
    ]
    db.flush()
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="id")
    assert back is not None
    assert back.sort_by("id").column("x").to_pylist() == [1, None]
