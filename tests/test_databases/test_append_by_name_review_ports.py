"""Review findings on PR 491 (ITL-1367) and PR 496 (ITL-1370), ported from the reviewers'
reproducer scripts so a regression of the same kind is caught here.

Each test names its review round and finding. A finding that was declined or documented as
a limit is pinned as it stands (equal to main), so a later change is noticed.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pyarrow as pa
import pytest

from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.function_pod import FunctionPod
from orcapod.core.sources import DictSource
from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    InMemoryArrowDatabase,
    SQLiteConnector,
)
from tests.test_databases.type_matrix import open_database

PATH = ("t",)


# ── PR 491, Codex round 3, finding 1: a logical type inside a struct or a list ─────


class _Unit(pa.ExtensionType):
    """An ``int64`` in a unit: two units are one type to pyarrow's own equality."""

    def __init__(self, unit: str) -> None:
        self.unit = unit
        super().__init__(pa.int64(), "test.ports.unit")

    def __arrow_ext_serialize__(self) -> bytes:
        return self.unit.encode()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: bytes) -> Any:
        return cls(serialized.decode())


def _unit_rows(n: int, unit: str, nesting: str, order: tuple[str, ...]) -> pa.Table:
    values: Any = pa.ExtensionArray.from_storage(
        _Unit(unit), pa.array([10 * n], pa.int64())
    )
    if nesting == "struct":
        values = pa.StructArray.from_arrays(
            [values, pa.array([1000 * n])], names=["measure", "other"]
        )
    elif nesting == "list":
        values = pa.LargeListArray.from_arrays(pa.array([0, 1], pa.int64()), values)
    columns = {
        "rid": pa.array([bytes([n])], pa.large_binary()),
        "a": values,
        "b": pa.array([100 * n]),
    }
    return pa.table({name: columns[name] for name in order})


@pytest.mark.parametrize("nesting", ["top", "struct", "list"])
@pytest.mark.parametrize("database", ["in-memory", "Delta"])
def test_a_permuted_append_of_another_logical_type_is_refused_at_every_depth(
    tmp_path: Path, database: str, nesting: str
) -> None:
    """Held rows in metres, an append in seconds with the columns permuted: refused
    as on main (in-memory: ``ArrowInvalid``; Delta: ``Schema mismatch``), never read
    back as metres (PR 491 Codex round 3, finding 1)."""
    db = (
        InMemoryArrowDatabase()
        if database == "in-memory"
        else DeltaTableDatabase(tmp_path)
    )
    db.add_records(
        PATH, _unit_rows(1, "m", nesting, ("rid", "a", "b")), record_id_column="rid"
    )
    permuted = _unit_rows(2, "s", nesting, ("b", "rid", "a"))
    error: type[BaseException] = (
        pa.ArrowInvalid if database == "in-memory" else ValueError
    )
    with pytest.raises(error):
        db.add_records(PATH, permuted, record_id_column="rid")


# ── PR 491, Codex round 4, finding 1, and CodeRabbit (utils.py:254) ─────────────────


def _contained(n: int, unit: str, kind: str) -> Any:
    """An ``int64`` in ``unit`` inside a container the append comparison does not
    handle, or a ``string_view`` inside a struct."""
    if kind == "string view in a struct":
        return pa.array([{"x": str(n)}], pa.struct([("x", pa.string_view())]))
    values = pa.ExtensionArray.from_storage(_Unit(unit), pa.array([10 * n], pa.int64()))
    index = pa.array([0], pa.int8())
    if kind == "dictionary":
        return pa.DictionaryArray.from_arrays(index, values)
    if kind == "dense union":
        return pa.UnionArray.from_dense(
            index, pa.array([0], pa.int32()), [values], field_names=["measure"]
        )
    if kind == "sparse union":
        return pa.UnionArray.from_sparse(index, [values], field_names=["measure"])
    if kind == "list view":
        list_view: Any = pa.ListViewArray
        return list_view.from_arrays(pa.array([0]), pa.array([1]), values)
    run_end: Any = pa.RunEndEncodedArray
    return run_end.from_arrays(pa.array([1], pa.int32()), values)


@pytest.mark.parametrize("flushed", [False, True], ids=["pending", "committed"])
@pytest.mark.parametrize(
    "kind",
    [
        "dictionary",
        "dense union",
        "sparse union",
        "list view",
        "run-end encoded",
        "string view in a struct",
    ],
)
def test_a_permuted_append_of_an_unhandled_type_is_refused_as_on_main(
    kind: str, flushed: bool
) -> None:
    """A column whose type tree holds a type the append comparison does not handle is
    never reordered: a permuted append (here of another unit, or an equal
    ``string_view`` struct) is refused as main refuses it, never read under the held
    type (PR 491 Codex round 4, finding 1; CodeRabbit utils.py:254)."""

    def rows(n: int, unit: str, order: tuple[str, ...]) -> pa.Table:
        columns = {
            "rid": pa.array([bytes([n])], pa.large_binary()),
            "a": _contained(n, unit, kind),
            "b": pa.array([100 * n]),
        }
        return pa.table({name: columns[name] for name in order})

    db = InMemoryArrowDatabase()
    db.add_records(
        PATH, rows(1, "m", ("rid", "a", "b")), record_id_column="rid", flush=flushed
    )
    with pytest.raises(pa.ArrowInvalid):
        db.add_records(PATH, rows(2, "s", ("b", "rid", "a")), record_id_column="rid")
        db.flush()


# ── PR 491, Codex round 3, finding 2 (requeue form) ───────────────────────────────


def _abc(n: int, order: tuple[str, ...] = ("rid", "a", "b"), a: Any = None) -> pa.Table:
    columns = {
        "rid": pa.array([bytes([n])], pa.large_binary()),
        "a": pa.array([10 * n]) if a is None else a,
        "b": pa.array([100 * n]),
    }
    return pa.table({name: columns[name] for name in order})


@pytest.mark.parametrize("skip_duplicates", [False, True])
@pytest.mark.parametrize(
    ("schema_handling", "a"),
    [
        ("merge", pa.nulls(1)),
        ("merge", pa.array([30])),
        ("coerce", pa.nulls(1)),
        ("coerce", pa.array([30], pa.int32())),
        ("coerce", pa.array([30])),
    ],
    ids=["merge null", "merge same", "coerce null", "coerce int32", "coerce same"],
)
def test_a_batch_after_a_raced_flush_is_stored_by_name(
    tmp_path: Path, schema_handling: str, a: Any, skip_duplicates: bool
) -> None:
    """A permuted batch whose flush races another writer, then a further permuted batch
    relying on ``schema_handling``: every row stored is stored under its names (PR 491
    Codex round 3, finding 2, the requeue form).

    On a canonical table the raced batch commits by append (ITL-1377's decision;
    main's MERGE failed and requeued it), so nothing is pending for the last batch:
    an ``int32`` batch is then refused against the stored ``int64`` column, exactly as
    main refuses it whenever nothing is pending (``coerce`` casts to the pending
    batch only). Every other case stores every row, as main does."""
    from orcapod.errors import InputValidationError

    db = DeltaTableDatabase(tmp_path)
    kwargs: dict[str, Any] = {
        "record_id_column": "rid",
        "schema_handling": schema_handling,
        "skip_duplicates": skip_duplicates,
    }
    db.add_records(PATH, _abc(1), flush=True, **kwargs)
    db.add_records(PATH, _abc(2, ("b", "rid", "a")), **kwargs)
    snapshot = db._snapshot_for_commit

    def interleave(*args: Any) -> Any:
        taken = snapshot(*args)
        DeltaTableDatabase(tmp_path).add_records(PATH, _abc(4), flush=True, **kwargs)
        return taken

    with patch.object(db, "_snapshot_for_commit", side_effect=interleave):
        try:
            db.flush()
        except RuntimeError:
            pass  # main: the MERGE conflicts and the batch is requeued
    expected = {1: (10, 100), 2: (20, 200), 4: (40, 400)}
    third = _abc(3, ("b", "rid", "a"), a)
    if a.type == pa.int32():
        with pytest.raises(InputValidationError, match="stored as int64"):
            db.add_records(PATH, third, **kwargs)
    else:
        db.add_records(PATH, third, **kwargs)
        expected[3] = (a.to_pylist()[0], 300)
    db.flush()
    table = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="rid")
    assert table is not None
    rows = {row["rid"][0]: (row["a"], row["b"]) for row in table.to_pylist()}
    assert rows == expected


# ── PR 491, Codex round 3, finding 4: a queued row under a locked database ───────────


def test_a_queued_row_is_read_while_another_connection_locks_the_database(
    tmp_path: Path,
) -> None:
    """A point lookup of a queued row answers from memory, so another connection's
    exclusive lock does not make it fail (PR 491 Codex round 3, finding 4)."""
    path = tmp_path / "store.db"
    db = ConnectorArrowDatabase(SQLiteConnector(str(path)))
    db.add_records(PATH, _abc(1), record_id_column="rid", flush=True)
    db.add_records(PATH, _abc(2, ("b", "rid", "a")), record_id_column="rid")
    with sqlite3.connect(path) as other:
        other.execute("BEGIN EXCLUSIVE")
        row = db.get_record_by_id(PATH, bytes([2]), record_id_column="rid")
        other.rollback()
    assert row is not None
    assert row.select(["rid", "a", "b"]).to_pylist() == [
        {"rid": bytes([2]), "a": 20, "b": 200}
    ]


# ── PR 491, Codex round 3, D: a schema change between the flush's check and its write ─


def test_a_rename_between_the_flush_check_and_the_write_is_as_on_main(
    tmp_path: Path,
) -> None:
    """Documented limit, pre-existing and equal to main: another connection that
    renames ``a`` to ``A`` after the flush checked the table and before it writes is
    not seen; SQLite resolves the written ``a`` without case, so the value lands under
    ``A`` (PR 491 Codex round 3, D). Pinned so a change is noticed."""
    path = tmp_path / "store.db"
    ConnectorArrowDatabase(SQLiteConnector(str(path))).add_records(
        PATH, _abc(1), record_id_column="rid", flush=True
    )
    connector = SQLiteConnector(str(path))
    db = ConnectorArrowDatabase(connector)
    db.add_records(PATH, _abc(2), record_id_column="rid")
    upsert = connector.upsert_records

    def rename_then_write(*args: Any, **kwargs: Any) -> Any:
        with sqlite3.connect(path) as other:
            other.execute('ALTER TABLE "t" RENAME COLUMN a TO A')
        return upsert(*args, **kwargs)

    with patch.object(connector, "upsert_records", side_effect=rename_then_write):
        db.flush()
    table = ConnectorArrowDatabase(SQLiteConnector(str(path))).get_all_records(
        PATH, record_id_column="rid"
    )
    assert table is not None
    assert sorted(cast(list[int], table.column("A").to_pylist())) == [10, 20]


# ── PR 491, CodeRabbit (connector:488), declined ───────────────────────────────────


def test_a_column_with_field_metadata_is_stored_by_name_on_spiraldb(
    tmp_path: Path,
) -> None:
    """Declined: a connector flush compares the connector's column info, which carries
    no field metadata, so a column with field metadata is not reordered there;
    SpiralDB matches the write by name itself. Pinned: every value under its name
    (PR 491 CodeRabbit, connector_arrow_database.py:488)."""
    field = pa.field("a", pa.int64(), metadata={"unit": "m"})

    def rows(n: int, order: tuple[str, ...]) -> pa.Table:
        table = pa.Table.from_arrays(
            [
                pa.array([bytes([n])], pa.large_binary()),
                pa.array([10 * n]),
                pa.array([100 * n]),
            ],
            schema=pa.schema(
                [pa.field("rid", pa.large_binary()), field, pa.field("b", pa.int64())]
            ),
        )
        return table.select(list(order))

    with open_database("SpiralDB", tmp_path) as db:
        db.add_records(
            PATH, rows(1, ("rid", "a", "b")), record_id_column="rid", flush=True
        )
        db.add_records(
            PATH, rows(2, ("b", "rid", "a")), record_id_column="rid", flush=True
        )
        table = db.get_all_records(PATH, record_id_column="rid")
        assert table is not None
        got = sorted(table.select(["rid", "a", "b"]).to_pylist(), key=str)
        assert got == [
            {"rid": bytes([1]), "a": 10, "b": 100},
            {"rid": bytes([2]), "a": 20, "b": 200},
        ]


# ── PR 491, CodeRabbit (in_memory:270), declined ───────────────────────────────────


@pytest.mark.parametrize("order", [("rid", "a", "b"), ("b", "rid", "a")])
def test_a_string_append_to_a_large_string_batch_is_refused_in_memory_as_on_main(
    order: tuple[str, ...],
) -> None:
    """Declined: the in-memory database refuses a ``string`` append to a
    ``large_string`` pending batch in any column order, as main does; the reorder
    does not change that verdict (PR 491 CodeRabbit, in_memory_databases.py:270)."""

    def rows(n: int, text: Any, column_order: tuple[str, ...]) -> pa.Table:
        columns = {
            "rid": pa.array([bytes([n])], pa.large_binary()),
            "a": pa.array([str(n)], text),
            "b": pa.array([n]),
        }
        return pa.table({name: columns[name] for name in column_order})

    db = InMemoryArrowDatabase()
    db.add_records(
        PATH, rows(1, pa.large_string(), ("rid", "a", "b")), record_id_column="rid"
    )
    with pytest.raises(pa.ArrowInvalid):
        db.add_records(PATH, rows(2, pa.string(), order), record_id_column="rid")


# ── PR 496, Codex, finding 4: the pod wrapper's cache hit ───────────────────────────


def _outputs(x: int) -> tuple[int, int]:
    return x + 1, x + 7


def test_a_cached_pods_hit_presents_the_outputs_as_its_first_call_does() -> None:
    """The pod wrapper uses the same per-input lookup as the cached data function: a
    hit gives the outputs in the declared order ``z, a``, as the first call and main
    do (PR 496 Codex, finding 4, the pod form)."""
    database = InMemoryArrowDatabase()
    pod = CachedFunctionPod(
        FunctionPod.from_fn(_outputs, output_keys=["z", "a"]),  # pyright: ignore[reportArgumentType]  # a FunctionPod works at run time
        result_database=database,
    )
    source = DictSource(
        data=[{"k": 1, "x": 1}, {"k": 2, "x": 2}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="src",
    )
    first = [data for _, data in pod.process(source).iter_data()]
    database.flush()
    second = [data for _, data in pod.process(source).iter_data()]
    assert all(d.get_meta_value(pod.RESULT_COMPUTED_FLAG) is False for d in second)
    for miss, hit in zip(first, second, strict=True):
        assert tuple(hit.keys()) == tuple(miss.keys()) == ("z", "a")
        assert [hit[k] for k in hit.keys()] == [miss[k] for k in miss.keys()]
        columns = hit.as_table(columns={"source": True}).column_names
        assert columns == miss.as_table(columns={"source": True}).column_names
