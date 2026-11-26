"""An append matches columns by name, whatever their order (ITL-1367).

Every database puts rows whose columns are the held table's columns in another order,
each field equal to the held one, into the held order: the in-memory database when the
rows are queued (the pending batch, else the stored table); Delta when they are queued,
against its pending batch only (it matches a stored table by name itself); a connector
database when they are queued, against its pending batch only, and at a flush or a
combined read, against the stored table read then. The table keeps the order its
database gave the first write. ``a`` and ``c`` are both ``int64``, so a positional match
would swap their values: every test reads the values back by name. Appends that add,
lack or retype a column, a field that differs in any way, and a name that repeats
exactly, keep the database's own rule.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pyarrow as pa
import pytest

from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    ExtensionAwareDatabase,
    InMemoryArrowDatabase,
    SQLiteConnector,
)
from orcapod.databases.utils import declared_null_path, in_held_column_order
from orcapod.errors import (
    InputValidationError,
    NullInNonNullableColumnError,
    UnstorableColumnError,
)
from tests.test_databases.test_connector_arrow_database import MockDBConnector
from tests.test_databases.type_matrix import open_database

PATH = ("t",)
ID = "id"
TYPES: dict[str, Any] = {
    ID: pa.large_binary(),
    "a": pa.int64(),
    "c": pa.int64(),
    "b": pa.large_string(),
}
FIRST_ORDERS = {
    "id a c b": [ID, "a", "c", "b"],
    "b c id a": ["b", "c", ID, "a"],
}
SECOND_ORDERS = {
    "c a b id": ["c", "a", "b", ID],
    "id c a b": [ID, "c", "a", "b"],
    "b id a c": ["b", ID, "a", "c"],
}
DATABASES = ("in-memory", "Delta", "SQLite")
DURABLE = ("Delta", "SQLite")


def _row(n: int) -> dict[str, Any]:
    return {ID: f"r{n}".encode(), "a": n, "c": 100 + n, "b": f"b{n}"}


def _rows(order: list[str], ns: list[int]) -> pa.Table:
    """Rows ``ns`` with the columns in ``order``; ``a`` is ``n``, ``c`` is ``100 + n``."""
    rows = [_row(n) for n in ns]
    return pa.table(
        {name: pa.array([r[name] for r in rows], TYPES[name]) for name in order}
    )


def _given_order(database: str, first: list[str], stored: bool = True) -> list[str]:
    """The order a database gives a table first written in ``first``.

    The first write's order.
    """
    return first


def _check(table: pa.Table | None, first: list[str], ns: list[int]) -> None:
    """The rows ``ns``, read by name, in the order ``first``."""
    assert table is not None
    assert table.column_names == first
    got = sorted(table.select([ID, "a", "c", "b"]).to_pylist(), key=lambda r: r["a"])
    assert got == [_row(n) for n in ns]


def _add(db: Any, table: pa.Table, flush: bool = False) -> None:
    db.add_records(PATH, table, record_id_column=ID, flush=flush)


@pytest.mark.parametrize("database", DATABASES)
@pytest.mark.parametrize("first", FIRST_ORDERS)
@pytest.mark.parametrize("second", SECOND_ORDERS)
@pytest.mark.parametrize("when", ["after a flush", "same pending batch"])
def test_an_append_in_another_order_is_matched_by_name(
    tmp_path: Path, database: str, first: str, second: str, when: str
) -> None:
    first_order, second_order = FIRST_ORDERS[first], SECOND_ORDERS[second]
    with ExitStack() as stack:
        db = stack.enter_context(open_database(database, tmp_path))
        _add(db, _rows(first_order, [1, 2]), flush=when == "after a flush")
        _add(db, _rows(second_order, [3, 4]))
        # Before the flush: the pending rows (and the stored ones) read by name.
        _check(
            db.get_all_records(PATH, record_id_column=ID),
            _given_order(database, first_order, stored=when == "after a flush"),
            [1, 2, 3, 4],
        )
        db.flush()
        _check(
            db.get_all_records(PATH, record_id_column=ID),
            _given_order(database, first_order),
            [1, 2, 3, 4],
        )


@pytest.mark.parametrize("database", DURABLE)
@pytest.mark.parametrize("first", FIRST_ORDERS)
@pytest.mark.parametrize("second", SECOND_ORDERS)
def test_an_append_in_another_order_after_reopening_is_matched_by_name(
    tmp_path: Path, database: str, first: str, second: str
) -> None:
    first_order, second_order = FIRST_ORDERS[first], SECOND_ORDERS[second]
    with open_database(database, tmp_path) as db:
        _add(db, _rows(first_order, [1, 2]), flush=True)
    with open_database(database, tmp_path) as db:
        _add(db, _rows(second_order, [3, 4]))
        _add(db, _rows(first_order, [5]))
        db.flush()
    with open_database(database, tmp_path) as db:
        _check(
            db.get_all_records(PATH, record_id_column=ID),
            _given_order(database, first_order),
            [1, 2, 3, 4, 5],
        )


@pytest.mark.parametrize("database", DATABASES)
def test_a_new_table_keeps_the_order_the_database_gave_its_first_write(
    tmp_path: Path, database: str
) -> None:
    """The first write's order is kept, whichever order it is; nothing is sorted by
    Orcapod."""
    with open_database(database, tmp_path) as db:
        for n, order in enumerate([["c", "b", "a", ID], [ID, "a", "b", "c"]]):
            path = (f"t{n}",)
            db.add_records(path, _rows(order, [1]), record_id_column=ID, flush=True)
            table = db.get_all_records(path, record_id_column=ID)
            assert table is not None
            assert table.column_names == _given_order(database, order)


# Today's verdict for an append that is reordered AND retypes ``a`` (``int32`` where
# ``int64`` is held), measured on main 321284ae with the same fake: (where it is refused,
# the exception). The rows are not reordered (another type), so the verdict is the
# database's own; the stored rows are the first write's either way.
RETYPE_VERDICTS: dict[tuple[str, str], tuple[str, type[BaseException]]] = {
    ("in-memory", "after a flush"): ("flush", pa.ArrowInvalid),
    ("in-memory", "same pending batch"): ("add", pa.ArrowInvalid),
    ("Delta", "after a flush"): ("add", InputValidationError),
    ("Delta", "same pending batch"): ("add", ValueError),
    ("SQLite", "after a flush"): ("add", UnstorableColumnError),
    ("SQLite", "same pending batch"): ("add", UnstorableColumnError),
}


@pytest.mark.parametrize("database", DATABASES)
@pytest.mark.parametrize("when", ["after a flush", "same pending batch"])
def test_an_append_that_also_retypes_a_column_keeps_todays_rule(
    tmp_path: Path, database: str, when: str
) -> None:
    """Another type is not a reordering: the rows are left as given, and the
    database's own rule refuses them where it does today; the stored rows stay."""
    stage, error = RETYPE_VERDICTS[(database, when)]
    retyped = _rows(["c", "a", "b", ID], [3]).set_column(
        1, "a", pa.array([3], pa.int32())
    )
    with open_database(database, tmp_path) as db:
        _add(db, _rows([ID, "a", "c", "b"], [1]), flush=when == "after a flush")
        if stage == "add":
            with pytest.raises(error):
                _add(db, retyped)
            db.flush()
        else:
            _add(db, retyped)
            with pytest.raises(error):
                db.flush()
        table = db.get_all_records(PATH, record_id_column=ID)
        assert table is not None
        assert table.schema.field("a").type == pa.int64()
        assert table.select([ID, "a", "c"]).to_pylist() == [
            {ID: b"r1", "a": 1, "c": 101}
        ]


def test_a_table_created_again_in_another_order_is_read_as_it_is_now(
    tmp_path: Path,
) -> None:
    """A table dropped and created again in another order by another connection:
    this handle's next append is matched to the new order by name, values right."""
    with open_database("SQLite", tmp_path) as db:
        _add(db, _rows([ID, "a", "c", "b"], [1]), flush=True)
        _add(db, _rows(["c", "a", "b", ID], [2]), flush=True)
        with sqlite3.connect(tmp_path / "store.db") as other:
            other.execute('DROP TABLE "t"')
        other_db = ConnectorArrowDatabase(SQLiteConnector(str(tmp_path / "store.db")))
        _add(other_db, _rows(["b", ID, "c", "a"], [3]), flush=True)
        _add(db, _rows([ID, "a", "c", "b"], [4]), flush=True)
        _check(
            db.get_all_records(PATH, record_id_column=ID),
            ["b", ID, "c", "a"],
            [3, 4],
        )


def test_rows_queued_before_a_table_is_created_again_are_read_with_it_by_name(
    tmp_path: Path,
) -> None:
    """Rows queued in the old stored order, then the table dropped and created again
    in another order by another connection: a read before the flush, and the flush,
    combine the queued rows with what is stored now, by name."""
    with open_database("SQLite", tmp_path) as db:
        _add(db, _rows([ID, "a", "c", "b"], [1]), flush=True)
        _add(db, _rows(["b", ID, "c", "a"], [2]))  # queued as [id, a, c, b]
        with sqlite3.connect(tmp_path / "store.db") as other:
            other.execute('DROP TABLE "t"')
        other_db = ConnectorArrowDatabase(SQLiteConnector(str(tmp_path / "store.db")))
        _add(other_db, _rows(["b", ID, "c", "a"], [3]), flush=True)
        _check(
            db.get_all_records(PATH, record_id_column=ID), ["b", ID, "c", "a"], [2, 3]
        )
        db.flush()
        _check(
            other_db.get_all_records(PATH, record_id_column=ID),
            ["b", ID, "c", "a"],
            [2, 3],
        )


# ── The shared step ────────────────────────────────────────────────────────────


def _held(*names: str) -> list[Any]:
    return [pa.field(n, pa.int64()) for n in names]


def _table(*names: str) -> pa.Table:
    return pa.Table.from_arrays(
        [pa.array([i], pa.int64()) for i in range(len(names))], names=list(names)
    )


def test_the_step_reorders_the_held_columns_by_name() -> None:
    given = _table("c", "a", "b")
    out = in_held_column_order(given, _held("a", "b", "c"))
    assert out.column_names == ["a", "b", "c"]
    assert out.to_pylist() == [{"a": 1, "b": 2, "c": 0}]


def test_the_step_keeps_the_given_fields() -> None:
    """The rows are reordered and their fields kept as given: metadata, and a
    nullability the held column does not have (the append checks judge it)."""
    field = pa.field("c", pa.int64(), nullable=False, metadata={"unit": "m"})
    given = pa.Table.from_arrays(
        [pa.array([1], pa.int64()), pa.array([2], pa.int64())],
        schema=pa.schema([field, pa.field("a", pa.int64())]),
    )
    held = [pa.field("a", pa.int64()), field.with_nullable(True)]
    out = in_held_column_order(given, held)
    assert out.column_names == ["a", "c"]
    assert out.schema.field("c").equals(field, check_metadata=True)


def test_the_step_matches_names_exactly() -> None:
    """``a`` and ``A`` are two columns, as Arrow matches names exactly."""
    out = in_held_column_order(_table("A", "a", "c"), _held("a", "c", "A"))
    assert out.column_names == ["a", "c", "A"]
    assert out.to_pylist() == [{"A": 0, "a": 1, "c": 2}]


def _annotated(name: str, ext_type: Any) -> Any:
    """A field held as its storage type, annotated with its logical type (a stored
    Delta column, a column the extension-aware wrapper encoded)."""
    return pa.field(
        name,
        pa.string(),
        metadata={
            b"ARROW:extension:name": ext_type.extension_name.encode(),
            b"ARROW:extension:metadata": ext_type.__arrow_ext_serialize__(),
        },
    )


class _Unit(pa.ExtensionType):
    """An extension type whose metadata is a unit: two units over ``int64`` are one
    type to pyarrow's own equality, two types here."""

    def __init__(self, unit: str) -> None:
        self.unit = unit
        super().__init__(pa.int64(), "test.itl1367.unit")

    def __arrow_ext_serialize__(self) -> bytes:
        return self.unit.encode()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: bytes) -> Any:
        return cls(serialized.decode())


def _unit_column(unit: str, nest: str) -> Any:
    """``a``: an ``int64`` in ``unit``, at the top, in a struct or in a list."""
    array = pa.ExtensionArray.from_storage(_Unit(unit), pa.array([1], pa.int64()))
    if nest == "struct":
        return pa.StructArray.from_arrays([array], names=["m"])
    if nest == "list":
        return pa.LargeListArray.from_arrays(pa.array([0, 1], pa.int64()), array)
    return array


@pytest.mark.parametrize("nest", ["top", "struct", "list"])
def test_the_step_matches_a_logical_type_only_to_itself_at_every_depth(nest: str):
    """One rule for "may reorder": every field equal at every depth. A logical type
    is equal only to its own name and metadata, also inside a struct or a list; an
    annotation in field metadata only to the same annotation. Anything else is left
    as given, so the database's own refusal applies (codex review of PR 491, round
    3)."""
    held_table = pa.table({"b": [0], "a": _unit_column("m", nest)})
    held = list(held_table.schema)
    same = pa.table({"a": _unit_column("m", nest), "b": [1]})
    assert in_held_column_order(same, held).column_names == ["b", "a"]
    other = pa.table({"a": _unit_column("s", nest), "b": [1]})
    assert in_held_column_order(other, held) is other


def test_the_step_matches_a_field_annotation_only_to_itself() -> None:
    from tests.test_databases.test_extension_aware_database import (
        _make_converter_with_type,
    )

    _, one = _make_converter_with_type("test.itl1367.one")
    _, two = _make_converter_with_type("test.itl1367.two")
    a = pa.field("a", pa.int64())

    def annotated(ext_type: Any) -> pa.Table:
        return pa.Table.from_arrays(
            [pa.array(["x"]), pa.array([1])],
            schema=pa.schema([_annotated("text", ext_type), a]),
        )

    given = annotated(one)
    held_one = [a, _annotated("text", one)]
    assert in_held_column_order(given, held_one).column_names == ["a", "text"]
    assert in_held_column_order(given, [a, _annotated("text", two)]) is given
    assert in_held_column_order(given, [a, pa.field("text", pa.string())]) is given
    live = pa.table(
        {
            "text": pa.ExtensionArray.from_storage(
                one, pa.array(["x"], pa.large_string())
            ),
            "a": [1],
        }
    )
    assert (
        in_held_column_order(live, held_one) is live
    )  # a live type is not its storage


def test_the_step_compares_types_in_their_large_variants() -> None:
    given = pa.table({"b": pa.array(["x"], pa.string()), "a": [1]})
    out = in_held_column_order(
        given, [pa.field("a", pa.int64()), pa.field("b", pa.large_string())]
    )
    assert out.column_names == ["a", "b"]


@pytest.mark.parametrize(
    ("given", "held"),
    [
        pytest.param(_table("c", "a"), None, id="nothing held"),
        pytest.param(_table("c", "a"), _held("a", "b", "c"), id="a column missing"),
        pytest.param(_table("c", "a", "b"), _held("a", "c"), id="a column added"),
        pytest.param(_table("c", "A"), _held("a", "c"), id="a name in another case"),
        pytest.param(_table("c", "a", "a"), _held("a", "c", "a"), id="a repeated name"),
        pytest.param(
            _table("c", "a"),
            [pa.field("a", pa.int64()), pa.field("c", pa.int32())],
            id="a type differs",
        ),
        pytest.param(
            pa.table(
                {
                    "s": pa.array(
                        [{"y": 1, "x": 2}],
                        pa.struct([("y", pa.int64()), ("x", pa.int64())]),
                    ),
                    "a": [1],
                }
            ),
            [
                pa.field("a", pa.int64()),
                pa.field("s", pa.struct([("x", pa.int64()), ("y", pa.int64())])),
            ],
            id="struct children in another order",
        ),
    ],
)
def test_the_step_leaves_anything_else_as_given(
    given: pa.Table, held: list[Any] | None
) -> None:
    assert in_held_column_order(given, held) is given


@pytest.mark.parametrize("database", ["in-memory"])
@pytest.mark.parametrize("when", ["after a flush", "same pending batch"])
def test_names_differing_in_case_only_are_two_columns(
    tmp_path: Path, database: str, when: str
) -> None:
    """The in-memory database holds ``a`` and ``A`` apart; an append of them in another
    order is matched by exact name. (Delta refuses them: below.)"""
    with open_database(database, tmp_path) as db:
        _add(db, _table_with_id(b"r1", [ID, "a", "A"]), flush=when == "after a flush")
        _add(db, _table_with_id(b"r2", ["A", ID, "a"]))
        db.flush()
        table = db.get_all_records(PATH, record_id_column=ID)
        assert table is not None
        assert sorted(table.select([ID, "a", "A"]).to_pylist(), key=str) == [
            {ID: b"r1", "a": 1, "A": 2},
            {ID: b"r2", "a": 1, "A": 2},
        ]


@pytest.mark.parametrize("when", ["after a flush", "same pending batch"])
def test_delta_refuses_names_differing_only_in_case(tmp_path: Path, when: str) -> None:
    """Delta column names are case-insensitive (deltalake 1.5.1 and later refuses
    such a table): ``a`` and ``A`` in one table are refused before anything is
    queued, in one batch and as a column added beside a held one (ITL-1403)."""
    db = DeltaTableDatabase(tmp_path)
    with pytest.raises(UnstorableColumnError, match="differ only in case") as info:
        _add(db, _table_with_id(b"r1", [ID, "a", "A"]))
    assert info.value.column == "A"
    _add(db, _table_with_id(b"r1", [ID, "a"]), flush=when == "after a flush")
    with pytest.raises(InputValidationError, match="'a' and 'A' differ only in case"):
        # ``merge``: a new column beside a pending batch is otherwise refused by the
        # schema check before the name is looked at.
        db.add_records(
            PATH,
            _table_with_id(b"r2", ["A", ID]),
            record_id_column=ID,
            schema_handling="merge",
        )
    db.flush()
    table = db.get_all_records(PATH, record_id_column=ID)
    assert table is not None
    assert table.select([ID, "a"]).to_pylist() == [{ID: b"r1", "a": 1}]


def _table_with_id(rid: bytes, order: list[str]) -> pa.Table:
    columns = {
        ID: pa.array([rid], pa.large_binary()),
        "a": pa.array([1], pa.int64()),
        "A": pa.array([2], pa.int64()),
    }
    return pa.table({name: columns[name] for name in order})


@pytest.mark.parametrize("stored", [True, False], ids=["committed", "pending"])
def test_a_logical_column_is_matched_by_name_through_the_extension_database(
    tmp_path: Path, stored: bool
) -> None:
    """A logical column appended in another order through the extension-aware
    wrapper, the held rows pending or committed: every value is read back by name
    (Delta matches a stored table by name itself; a pending batch is aligned)."""
    from tests.test_databases.test_extension_aware_database import (
        _make_converter_with_type,
    )

    converter, ext_type = _make_converter_with_type("test.itl1367.extension")

    def rows(n: int, order: list[str]) -> pa.Table:
        columns = {
            ID: pa.array([bytes([n])], pa.large_binary()),
            "text": pa.ExtensionArray.from_storage(
                ext_type, pa.array([str(n)], pa.large_string())
            ),
            "n": pa.array([n], pa.int64()),
        }
        return pa.table({name: columns[name] for name in order})

    db = ExtensionAwareDatabase(DeltaTableDatabase(tmp_path / "delta"), converter)
    _add(db, rows(1, [ID, "text", "n"]), flush=stored)
    _add(db, rows(2, ["n", ID, "text"]))
    pending = db.get_record_by_id(PATH, bytes([2]), record_id_column=ID)
    assert pending is not None
    db.flush()
    committed = db.get_record_by_id(PATH, bytes([2]), record_id_column=ID)
    assert committed is not None
    row = {ID: bytes([2]), "text": "2", "n": 2}
    assert pending.select([ID, "text", "n"]).to_pylist() == [row]
    assert committed.select([ID, "text", "n"]).to_pylist() == [row]


# ── Delta: the commit a reordered append takes ─────────────────────────────────


def _race_rows(n: int, value: int, order: tuple[str, ...]) -> pa.Table:
    columns = {
        "rid": pa.array([bytes([n])], pa.large_binary()),
        "a": pa.array([value], pa.int64()),
        "b": pa.array([1000 + value], pa.int64()),
    }
    return pa.table({name: columns[name] for name in order})


@pytest.mark.parametrize("skip_duplicates", [True, False])
@pytest.mark.parametrize(
    ("stale_order", "other_order"),
    [(("a", "b", "rid"), ("b", "rid", "a")), (("b", "rid", "a"), ("a", "b", "rid"))],
)
def test_a_reordered_append_is_stored_once_across_writers(
    tmp_path: Path,
    skip_duplicates: bool,
    stale_order: tuple[str, ...],
    other_order: tuple[str, ...],
) -> None:
    """Two handles append the same new record id with different values, each in
    another order than the stored table. The second commits after the first has
    taken its snapshot of the stored ids. Every commit to an existing table is a
    MERGE (ITL-1377): the stale commit fails, is retried against the table as it is
    then, and leaves the id alone, so it keeps the first writer's row."""
    stale, other = DeltaTableDatabase(tmp_path), DeltaTableDatabase(tmp_path)
    stale.add_records(
        PATH, _race_rows(1, 10, ("rid", "a", "b")), record_id_column="rid", flush=True
    )
    snapshot = stale._snapshot_for_commit

    def interleave(*args: Any) -> Any:
        taken = snapshot(*args)
        other.add_records(
            PATH,
            _race_rows(2, 20, other_order),
            record_id_column="rid",
            flush=True,
            skip_duplicates=skip_duplicates,
        )
        return taken

    with patch.object(
        stale, "_snapshot_for_commit", side_effect=interleave
    ) as snapshots:
        stale.add_records(
            PATH,
            _race_rows(2, 99, stale_order),
            record_id_column="rid",
            flush=True,
            skip_duplicates=skip_duplicates,
        )
    # The first attempt lost to the other writer; the second committed nothing.
    assert snapshots.call_count == 2
    table = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="rid")
    assert table is not None
    assert sorted(table.select(["rid", "a"]).to_pylist(), key=str) == [
        {"rid": bytes([1]), "a": 10},
        {"rid": bytes([2]), "a": 20},
    ]


def test_an_all_duplicate_append_looks_up_no_order(tmp_path: Path) -> None:
    """With ``skip_duplicates`` and nothing left to write, a stored table whose
    recorded type cannot be read does not fail an append that writes nothing (Delta
    looks no stored order up at all)."""
    from deltalake import write_deltalake

    rows = _rows([ID, "a", "c", "b"], [1]).rename_columns(
        ["__record_id", "a", "c", "b"]
    )
    schema = rows.schema.set(
        2, rows.schema.field(2).with_metadata({b"orcapod:arrow_type": b"invalid"})
    )
    write_deltalake(tmp_path / "t", pa.Table.from_arrays(rows.columns, schema=schema))
    db = DeltaTableDatabase(tmp_path)
    db.add_records(
        PATH,
        _rows(["b", ID, "c", "a"], [1]),
        record_id_column=ID,
        skip_duplicates=True,
        flush=True,
    )


# ── Lookups ────────────────────────────────────────────────────────────────────


def test_an_aligned_append_looks_the_stored_table_up_once(tmp_path: Path) -> None:
    """The connector database looks a table up once per flushed append, at the
    flush, as main does: an append looks nothing up when it is queued."""
    path = str(tmp_path / "store.db")
    ConnectorArrowDatabase(SQLiteConnector(path)).add_records(
        PATH, _rows([ID, "a", "c", "b"], [1]), record_id_column=ID, flush=True
    )
    connector = SQLiteConnector(path)
    db = ConnectorArrowDatabase(connector)
    with (
        patch.object(
            connector, "get_table_names", wraps=connector.get_table_names
        ) as names,
        patch.object(
            connector, "get_column_info", wraps=connector.get_column_info
        ) as info,
    ):
        _add(db, _rows([ID, "a", "c", "b"], [2]), flush=True)
        assert (names.call_count, info.call_count) == (1, 1)
        _add(db, _rows(["b", ID, "c", "a"], [3]))
        assert (names.call_count, info.call_count) == (1, 1)
        db.flush()
        assert (names.call_count, info.call_count) == (2, 2)
    _check(
        db.get_all_records(PATH, record_id_column=ID), [ID, "a", "c", "b"], [1, 2, 3]
    )


# ── Nothing about a stored table is trusted across calls ───────────────────────


def _abc(n: int, order: tuple[str, ...] = ("rid", "a", "b")) -> pa.Table:
    columns = {
        "rid": pa.array([bytes([n])], pa.large_binary()),
        "a": pa.array([10 * n], pa.int64()),
        "b": pa.array([100 * n], pa.int64()),
    }
    return pa.table({name: columns[name] for name in order})


def _sqlite(path: Path) -> ConnectorArrowDatabase:
    return ConnectorArrowDatabase(SQLiteConnector(str(path)))


def _recreate_with_text_a(path: Path) -> None:
    with sqlite3.connect(path) as other:
        other.execute('DROP TABLE "t"')
    _sqlite(path).add_records(
        PATH,
        pa.table(
            {
                "rid": pa.array([bytes([1])], pa.large_binary()),
                "a": pa.array(["10"], pa.large_string()),
                "b": pa.array([100], pa.int64()),
            }
        ),
        record_id_column="rid",
        flush=True,
    )


CHANGES: dict[str, Any] = {
    "rename": lambda c: c.execute('ALTER TABLE "t" RENAME COLUMN a TO A'),
    "add": lambda c: c.execute('ALTER TABLE "t" ADD COLUMN extra INTEGER'),
    "drop": lambda c: c.execute('ALTER TABLE "t" DROP COLUMN b'),
}


@pytest.mark.parametrize("change", [*CHANGES, "retype"])
@pytest.mark.parametrize("order", [("rid", "a", "b"), ("b", "rid", "a")])
def test_a_table_changed_between_queue_and_flush_is_refused_as_on_main(
    tmp_path: Path, change: str, order: tuple[str, ...]
) -> None:
    """Another connection renames, adds, drops or retypes a column after rows are
    queued: the flush checks the table as it is then, by exact name and type, and
    refuses as main does; the stored rows are untouched (codex review of PR 491,
    round 2: a remembered table order let such rows through)."""
    path = tmp_path / "store.db"
    _sqlite(path).add_records(PATH, _abc(1), record_id_column="rid", flush=True)
    db = _sqlite(path)
    db.add_records(PATH, _abc(2, order), record_id_column="rid")
    if change == "retype":
        _recreate_with_text_a(path)
    else:
        with sqlite3.connect(path) as other:
            CHANGES[change](other)
    before = _sqlite(path).get_all_records(PATH, record_id_column="rid")
    with pytest.raises(ValueError, match="Schema mismatch"):
        db.flush()
    after = _sqlite(path).get_all_records(PATH, record_id_column="rid")
    assert before is not None and after is not None
    assert after.equals(before)
    assert after.num_rows == 1


def test_a_table_dropped_and_written_again_by_the_same_handle_takes_the_new_order(
    tmp_path: Path,
) -> None:
    """The handle that wrote the dropped table writes the replacement in the order of
    its new first write, as main does: nothing of the dropped table is remembered."""
    path = tmp_path / "store.db"
    db = _sqlite(path)
    db.add_records(PATH, _abc(1), record_id_column="rid", flush=True)
    with sqlite3.connect(path) as other:
        other.execute('DROP TABLE "t"')
    db.add_records(PATH, _abc(2, ("b", "rid", "a")), record_id_column="rid", flush=True)
    table = _sqlite(path).get_all_records(PATH, record_id_column="rid")
    assert table is not None
    assert table.column_names == ["b", "rid", "a"]
    assert table.to_pylist() == [{"b": 200, "rid": bytes([2]), "a": 20}]


def test_a_queued_row_is_read_from_memory(tmp_path: Path) -> None:
    """A point lookup of a queued row answers from memory, in the order the row is
    queued, with no lookup of the stored table (as main): the stored table is not
    even listed. Presentation order is a reader's business (ITL-1370)."""
    with open_database("SQLite", tmp_path) as db:
        rows = pa.table(
            {
                "rid": pa.array([bytes([1])], pa.large_binary()),
                "z": pa.array([10]),
                "a": pa.array([100]),
            }
        )
        db.add_records(PATH, rows, record_id_column="rid", flush=True)
        second = rows.set_column(0, "rid", pa.array([bytes([2])], pa.large_binary()))
        db.add_records(PATH, second, record_id_column="rid")
        with patch.object(
            db._connector, "get_table_names", side_effect=AssertionError("listed")
        ):
            queued = db.get_record_by_id(PATH, bytes([2]), record_id_column="rid")
        assert queued is not None
        assert queued.column_names == ["rid", "z", "a"]
        db.flush()
        committed = db.get_record_by_id(PATH, bytes([2]), record_id_column="rid")
        assert committed is not None
        assert committed.select(["rid", "z", "a"]).to_pylist() == queued.to_pylist()


@pytest.mark.parametrize("schema_handling", ["error", "merge", "coerce"])
@pytest.mark.parametrize("skip_duplicates", [False, True])
@pytest.mark.parametrize("race", [False, True], ids=["alone", "with another writer"])
def test_a_refused_reordered_append_leaves_the_next_batch_committed(
    tmp_path: Path, schema_handling: str, skip_duplicates: bool, race: bool
) -> None:
    """A reordered append the ITL-1334 check refuses (other field metadata) queues
    nothing, and leaves nothing behind: the next aligned batch is committed, by the
    MERGE every commit to an existing table is (ITL-1377), also when another writer
    commits an unrelated id meanwhile."""
    from deltalake import DeltaTable

    db = DeltaTableDatabase(tmp_path)
    kwargs: dict[str, Any] = {
        "record_id_column": "rid",
        "schema_handling": schema_handling,
        "skip_duplicates": skip_duplicates,
    }
    db.add_records(PATH, _abc(1), flush=True, **kwargs)
    bad = _abc(2, ("b", "rid", "a"))
    bad = bad.set_column(
        2, bad.schema.field("a").with_metadata({"units": "wrong"}), bad["a"]
    )
    with pytest.raises(InputValidationError):
        db.add_records(PATH, bad, **kwargs)
    if race:
        snapshot = db._snapshot_for_commit

        def interleave(*args: Any) -> Any:
            taken = snapshot(*args)
            DeltaTableDatabase(tmp_path).add_records(
                PATH, _abc(3), flush=True, **kwargs
            )
            return taken

        with patch.object(db, "_snapshot_for_commit", side_effect=interleave):
            db.add_records(PATH, _abc(2), flush=True, **kwargs)
    else:
        db.add_records(PATH, _abc(2), flush=True, **kwargs)
    assert list(DeltaTable(tmp_path / "t").history())[0]["operation"] == "MERGE"
    table = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="rid")
    assert table is not None
    expected = [1, 2, 3] if race else [1, 2]
    assert sorted(cast(list[int], table.column("a").to_pylist())) == [
        10 * n for n in expected
    ]


@pytest.mark.parametrize("backend", ["in-memory", "Delta"])
@pytest.mark.parametrize("stored", [False, True], ids=["pending", "committed"])
def test_two_logical_types_over_one_storage_type_are_not_reordered(
    tmp_path: Path, backend: str, stored: bool
) -> None:
    """Through the extension-aware wrapper, a column of one logical type held and a
    permuted append of another logical type over the same storage type: not taken
    for one, and refused before anything is buffered as another logical type
    identifier, naming both (ITL-1371; on main it was refused later, or, committed
    to Delta, taken by name and left the table unreadable)."""
    from tests.test_databases.test_extension_aware_database import (
        _make_converter_with_type,
    )

    one = _make_converter_with_type("test.itl1367.held")
    two = _make_converter_with_type("test.itl1367.other")

    def rows(n: int, pair: Any, order: list[str]) -> pa.Table:
        text = pa.ExtensionArray.from_storage(
            pair[1], pa.array([str(n)], pa.large_string())
        )
        return _abc(n).append_column(pa.field("text", pair[1]), text).select(order)

    inner = (
        InMemoryArrowDatabase()
        if backend == "in-memory"
        else DeltaTableDatabase(tmp_path / "delta")
    )
    ExtensionAwareDatabase(inner, one[0]).add_records(
        PATH,
        rows(1, one, ["rid", "a", "b", "text"]),
        record_id_column="rid",
        flush=stored,
    )
    second = ExtensionAwareDatabase(inner, two[0])
    permuted = rows(2, two, ["text", "b", "rid", "a"])
    with pytest.raises(InputValidationError) as refused:
        second.add_records(PATH, permuted, record_id_column="rid")
    assert "'test.itl1367.other;'" in str(refused.value)
    assert "'test.itl1367.held;'" in str(refused.value)
    inner.flush()
    stored_rows = inner.get_all_records(PATH, record_id_column="rid")
    assert stored_rows is not None
    assert stored_rows.column("rid").to_pylist() == [bytes([1])]


@pytest.mark.parametrize(
    ("schema_handling", "a"),
    [
        ("merge", pa.nulls(1)),
        ("coerce", pa.nulls(1)),
        ("coerce", pa.array([30], pa.int32())),
    ],
    ids=["merge null", "coerce null", "coerce int32"],
)
def test_a_delta_batch_queued_in_another_order_keeps_its_order(
    tmp_path: Path, schema_handling: str, a: Any
) -> None:
    """Delta aligns an append only to its own pending batch, never to the stored
    table: a batch queued in another order than the stored table stays in its order,
    so a later append in that order, relying on ``schema_handling`` to merge or coerce
    a column, behaves as on main (codex review of PR 491, round 3)."""
    db = DeltaTableDatabase(tmp_path)
    kwargs: dict[str, Any] = {
        "record_id_column": "rid",
        "schema_handling": schema_handling,
    }
    db.add_records(PATH, _abc(1), flush=True, **kwargs)
    db.add_records(PATH, _abc(2, ("b", "rid", "a")), **kwargs)
    third = _abc(3, ("b", "rid", "a")).set_column(2, "a", a)
    db.add_records(PATH, third, **kwargs)
    db.flush()
    table = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="rid")
    assert table is not None
    got = sorted(table.select(["rid", "a", "b"]).to_pylist(), key=str)
    assert got == [
        {"rid": bytes([1]), "a": 10, "b": 100},
        {"rid": bytes([2]), "a": 20, "b": 200},
        {"rid": bytes([3]), "a": a.to_pylist()[0], "b": 300},
    ]


def test_a_delta_table_created_again_in_another_order_takes_the_batch(
    tmp_path: Path,
) -> None:
    """A Delta table created again in another order while this handle holds the old
    one: an aligned batch is committed by the MERGE every commit to an existing
    table is (ITL-1377), also with another writer committing an unrelated id
    meanwhile."""
    import shutil

    from deltalake import DeltaTable, write_deltalake

    order = ("b", "__record_id", "a")

    def rows(n: int) -> pa.Table:
        return _abc(n).rename_columns(["__record_id", "a", "b"]).select(order)

    db = DeltaTableDatabase(tmp_path)
    db.add_records(
        PATH,
        rows(1).select(["__record_id", "a", "b"]),
        flush=True,
        record_id_column="__record_id",
    )
    shutil.rmtree(tmp_path / "t")
    write_deltalake(tmp_path / "t", rows(1), mode="overwrite")
    db.add_records(PATH, rows(2), record_id_column="__record_id")
    snapshot = db._snapshot_for_commit

    def interleave(*args: Any) -> Any:
        taken = snapshot(*args)
        DeltaTableDatabase(tmp_path).add_records(
            PATH, rows(3), flush=True, record_id_column="__record_id"
        )
        return taken

    with patch.object(db, "_snapshot_for_commit", side_effect=interleave):
        db.flush()
    history = [entry["operation"] for entry in DeltaTable(tmp_path / "t").history()]
    assert history == ["MERGE", "MERGE", "WRITE"]
    table = DeltaTableDatabase(tmp_path).get_all_records(
        PATH, record_id_column="__record_id"
    )
    assert table is not None
    assert sorted(cast(list[int], table.column("a").to_pylist())) == [10, 20, 30]


def _extension_values(type_: Any) -> Any:
    return pa.ExtensionArray.from_storage(type_, pa.array([1], pa.int64()))


def _container_columns() -> dict[str, tuple[Any, Any]]:
    """A column of each container kind the comparison does not handle, held and
    appended: the appended one holds an extension type with other metadata (or, for
    a plain list view, an item field with other metadata)."""
    held_ext, other_ext = _Unit("m"), _Unit("s")
    offsets = pa.array([0], pa.int32())
    sizes = pa.array([1], pa.int32())

    def list_view(value: Any, large: bool) -> Any:
        kind: Any = pa.LargeListViewArray if large else pa.ListViewArray
        return kind.from_arrays(
            pa.array([0], pa.int64() if large else pa.int32()),
            pa.array([1], pa.int64() if large else pa.int32()),
            value,
        )

    def run_end() -> Any:
        encoded: Any = pa.RunEndEncodedArray
        return encoded.from_arrays(pa.array([1], pa.int32()), pa.array([1]))

    def view_struct() -> Any:
        return pa.array([{"x": "a"}], pa.struct([("x", pa.string_view())]))

    def interval() -> Any:
        return pa.array([(1, 0, 0)], pa.month_day_nano_interval())

    def union(value: Any, mode: str) -> Any:
        types = pa.array([0], pa.int8())
        if mode == "dense":
            return pa.UnionArray.from_dense(types, pa.array([0], pa.int32()), [value])
        return pa.UnionArray.from_sparse(types, [value])

    list_view_array: Any = pa.ListViewArray
    plain_item = list_view_array.from_arrays(offsets, sizes, pa.array([1]))
    marked_item = plain_item.cast(
        pa.list_view(pa.field("item", pa.int64(), metadata={"unit": "s"}))
    )
    return {
        "list_view": (
            list_view(_extension_values(held_ext), False),
            list_view(_extension_values(other_ext), False),
        ),
        "large_list_view": (
            list_view(_extension_values(held_ext), True),
            list_view(_extension_values(other_ext), True),
        ),
        "dense union": (
            union(_extension_values(held_ext), "dense"),
            union(_extension_values(other_ext), "dense"),
        ),
        "sparse union": (
            union(_extension_values(held_ext), "sparse"),
            union(_extension_values(other_ext), "sparse"),
        ),
        "list view item metadata": (plain_item, marked_item),
        "dictionary": (
            pa.array(["x"]).dictionary_encode(),
            pa.array(["x"]).dictionary_encode(),
        ),
        "run-end encoded": (run_end(), run_end()),
        "interval": (interval(), interval()),
        # Equal on both sides: a view type is not eligible even though the large
        # normalization would turn it into ``large_string`` (CodeRabbit on PR 491).
        "string view in a struct": (view_struct(), view_struct()),
    }


@pytest.mark.parametrize("kind", list(_container_columns()))
def test_a_column_of_a_type_the_comparison_does_not_handle_is_never_reordered(
    kind: str,
) -> None:
    """Eligibility is positive (codex review of PR 491, round 4): a column whose type
    tree holds anything but the handled leaf types, structs, maps, the three list
    kinds and extension types over those is not eligible, even when equal, so the
    table is left as given and the database's own rule applies."""
    held_values, given_values = _container_columns()[kind]
    held = list(pa.table({"b": [0], "a": held_values}).schema)
    given = pa.table({"a": given_values, "b": [1]})
    assert in_held_column_order(given, held) is given


class _KeyNotNullSQLite(SQLiteConnector):
    """A SQLite connector whose reads give the primary key ``NOT NULL``, as a
    PostgreSQL connector does (the CI failure of PR 491's PostgreSQL job)."""

    def iter_batches(self, query: str, params: Any = None, batch_size: int = 1000):  # type: ignore[override]
        for batch in super().iter_batches(query, params, batch_size):
            schema = batch.schema
            index = schema.get_field_index("__record_id")
            if index >= 0:
                schema = schema.set(index, schema.field(index).with_nullable(False))
            yield pa.RecordBatch.from_arrays(batch.columns, schema=schema)


@pytest.mark.parametrize("order", [["id", "a", "c", "b"], ["b", "c", "id", "a"]])
def test_queued_rows_combine_with_a_stored_key_read_back_not_null(
    tmp_path: Path, order: list[str]
) -> None:
    """A store reads its primary key back ``NOT NULL`` while the queued rows declare
    it nullable: a read that combines them takes the column nullable, the arrays
    unchanged, in either column order; a read before and after the flush give the
    same rows (as the PostgreSQL CI job checks against a live server)."""
    db = ConnectorArrowDatabase(_KeyNotNullSQLite(str(tmp_path / "store.db")))
    _add(db, _rows([ID, "a", "c", "b"], [1, 2]), flush=True)
    _add(db, _rows(order, [3, 4]))
    before = db.get_all_records(PATH, record_id_column=ID)
    _check(before, [ID, "a", "c", "b"], [1, 2, 3, 4])
    db.flush()
    _check(
        db.get_all_records(PATH, record_id_column=ID), [ID, "a", "c", "b"], [1, 2, 3, 4]
    )


def _declared(order: list[str], ns: list[int], *, not_null: bool) -> pa.Table:
    """Rows ``ns`` with the id and ``a`` declared ``NOT NULL``, or nullable."""
    table = _rows(order, ns)
    schema = pa.schema(
        f.with_nullable(not (not_null and f.name in (ID, "a"))) for f in table.schema
    )
    return pa.Table.from_arrays(table.columns, schema=schema)


def _check_lookups(db: Any, ns: list[int]) -> None:
    """The rows ``ns`` by one id, by several ids and by column values."""
    several = db.get_records_by_ids(
        PATH, [_row(n)[ID] for n in reversed(ns)], record_id_column=ID
    )
    assert sorted(several[ID].to_pylist()) == [_row(n)[ID] for n in ns]
    for n in ns:
        one = db.get_record_by_id(PATH, _row(n)[ID], record_id_column=ID)
        valued = db.get_records_with_column_value(
            PATH, {"a": n, "c": 100 + n}, record_id_column=ID
        )
        assert one.to_pylist() == valued.to_pylist() == [_row(n)]


@contextmanager
def _connector_database(kind: str, tmp_path: Path) -> Iterator[Any]:
    if kind == "SQLite, key read NOT NULL":
        yield ConnectorArrowDatabase(_KeyNotNullSQLite(str(tmp_path / "store.db")))
    elif kind == "writable mock":
        yield ConnectorArrowDatabase(MockDBConnector())
    else:
        with open_database(kind, tmp_path) as db:
            yield db


CONNECTOR_DATABASES = ("SQLite", "SQLite, key read NOT NULL")


@pytest.mark.parametrize("kind", CONNECTOR_DATABASES)
@pytest.mark.parametrize("order", [["id", "a", "c", "b"], ["b", "c", "id", "a"]])
@pytest.mark.parametrize("stored", [True, False])
@pytest.mark.parametrize("queued", [True, False])
def test_lookups_before_a_flush_combine_rows_of_differing_nullability(
    tmp_path: Path, kind: str, order: list[str], stored: bool, queued: bool
) -> None:
    """Stored and queued rows that declare the key and a data column ``NOT NULL`` or
    nullable read together, whole and by id and by value (ITL-1381)."""
    with _connector_database(kind, tmp_path) as db:
        _add(db, _declared([ID, "a", "c", "b"], [1, 2], not_null=stored), flush=True)
        _add(db, _declared(order, [3, 4], not_null=queued))
        rows = db.get_all_records(PATH, record_id_column=ID)
        _check(rows, _given_order(kind, [ID, "a", "c", "b"]), [1, 2, 3, 4])
        _check_lookups(db, [1, 2, 3, 4])


@pytest.mark.parametrize("kind", CONNECTOR_DATABASES)
@pytest.mark.parametrize("order", [["id", "a", "c", "b"], ["b", "c", "id", "a"]])
@pytest.mark.parametrize("not_null", [(True, False, True), (False, True, False)])
def test_queued_batches_of_differing_nullability_are_one_batch(
    tmp_path: Path, kind: str, order: list[str], not_null: tuple[bool, ...]
) -> None:
    """Batches queued by several calls that declare the key and a data column
    ``NOT NULL`` and nullable are one nullable batch, before and after the flush
    (ITL-1381): the second call raised ``ArrowInvalid``."""
    with _connector_database(kind, tmp_path) as db:
        first = [ID, "a", "c", "b"]
        for n, flag in enumerate(not_null, start=1):
            _add(db, _declared(order if n > 1 else first, [n], not_null=flag))
        _check(db.get_all_records(PATH, record_id_column=ID), first, [1, 2, 3])
        _check_lookups(db, [1, 2, 3])
        assert db.get_all_records(PATH, record_id_column=ID).schema.field("a").nullable
        db.flush()
        _check(
            db.get_all_records(PATH, record_id_column=ID),
            _given_order(kind, first),
            [1, 2, 3],
        )
        _check_lookups(db, [1, 2, 3])


@pytest.mark.parametrize("stored", [True, False])
@pytest.mark.parametrize("first", [True, False])
@pytest.mark.parametrize("skip", [True, False])
def test_a_refused_batch_leaves_the_queued_rows_to_be_flushed(
    tmp_path: Path, stored: bool, first: bool, skip: bool
) -> None:
    """Queued batches of differing nullability are combined unless the stored column is
    ``NOT NULL`` (CodeRabbit on PR 539): the second is then refused when it is queued,
    in either order, so a flush never drops rows of a valid batch with it."""
    order = [ID, "a", "c", "b"]
    with _connector_database("SQLite", tmp_path) as db:
        _add(db, _declared(order, [1], not_null=stored), flush=True)
        _add(db, _declared(order, [2], not_null=first))
        second = _declared(["c", ID, "b", "a"], [3], not_null=not first)
        if stored:
            with pytest.raises(InputValidationError, match="stored NOT NULL"):
                db.add_records(PATH, second, record_id_column=ID, skip_duplicates=skip)
            # The refused batch changed neither the rows nor the duplicate policy.
            assert not any(db._pending_skip_existing.values())
            # A batch whose rows are all queued already has nothing to combine.
            again = _declared(order, [2], not_null=not first)
            db.add_records(PATH, again, record_id_column=ID, skip_duplicates=True)
            ns = [1, 2]
        else:
            db.add_records(PATH, second, record_id_column=ID, skip_duplicates=skip)
            ns = [1, 2, 3]
        _check(db.get_all_records(PATH, record_id_column=ID), order, ns)
        db.flush()
        _check(db.get_all_records(PATH, record_id_column=ID), order, ns)


def _int_list(values: list[int | None], *, declared: bool) -> Any:
    item = pa.field("item", pa.int64(), nullable=not declared)
    offsets = pa.array(range(len(values) + 1), pa.int64())
    return pa.LargeListArray.from_arrays(
        offsets, pa.array(values, pa.int64()), type=pa.large_list(item)
    )


def _null_case(place: str, n: int, *, null: bool, not_null: bool) -> pa.Table:
    """Row ``n`` whose value at ``place`` (declared ``NOT NULL`` there, at the top level
    only with ``not_null``) is null with ``null``; built without Arrow checking it."""
    value = [None if null else n]
    other = pa.array([n], pa.int64())
    nullable = not not_null
    if place == "column":
        column = pa.array(value, pa.int64())
    elif place == "struct":
        column = pa.StructArray.from_arrays(
            [other, pa.array(value, pa.int64())],
            fields=[pa.field("o", pa.int64()), pa.field("x", pa.int64(), False)],
        )
    elif place == "list":
        column = _int_list(value, declared=True)
    else:
        column = pa.StructArray.from_arrays(
            [other, _int_list(value, declared=True)],
            fields=[
                pa.field("o", pa.int64()),
                pa.field("xs", pa.large_list(pa.field("item", pa.int64(), False))),
            ],
        )
    ids = pa.array([f"r{n}".encode()], pa.large_binary())
    return pa.Table.from_arrays(
        [ids, column, other],
        schema=pa.schema(
            [
                pa.field(ID, pa.large_binary()),
                pa.field("v", column.type, nullable=nullable),
                pa.field("w", pa.int64()),
            ]
        ),
    )


NULL_PATHS = {
    "column": "v",
    "struct": "v.x",
    "list": "v[]",
    "list in struct": "v.xs[]",
}
NULL_CASES = [
    *[(kind, "column") for kind in CONNECTOR_DATABASES],
    *[("writable mock", place) for place in NULL_PATHS if place != "column"],
]


@pytest.mark.parametrize(("kind", "place"), NULL_CASES)
@pytest.mark.parametrize("violating_first", [True, False])
@pytest.mark.parametrize("existing", [True, False])
@pytest.mark.parametrize("skip", [True, False])
@pytest.mark.parametrize("run_async", [True, False])
def test_rows_are_checked_against_their_own_declaration_before_they_are_queued(
    tmp_path: Path,
    kind: str,
    place: str,
    violating_first: bool,
    existing: bool,
    skip: bool,
    run_async: bool,
) -> None:
    """Review of PR 539: a null declared ``NOT NULL``, combined with a valid nullable
    batch, was stored. It is refused when queued, whichever batch comes first, and the
    valid batch is stored alone. Queued from this thread or from an event loop's tasks
    (a connector database has no async path of its own)."""

    def queue(db: Any, violating: bool) -> None:
        rows = _null_case(
            place, 3 if violating else 2, null=violating, not_null=violating
        )
        if violating:
            with pytest.raises(NullInNonNullableColumnError) as refused:
                db.add_records(PATH, rows, record_id_column=ID, skip_duplicates=skip)
            assert refused.value.column == NULL_PATHS[place]
        else:
            db.add_records(PATH, rows, record_id_column=ID, skip_duplicates=skip)

    async def queue_async(db: Any) -> None:
        for violating in (violating_first, not violating_first):
            await asyncio.create_task(asyncio.to_thread(queue, db, violating))

    with _connector_database(kind, tmp_path) as db:
        if existing:
            _add(db, _null_case(place, 1, null=False, not_null=False), flush=True)
        if run_async and not kind.startswith("SQLite"):
            asyncio.run(queue_async(db))
        elif run_async:

            async def on_loop() -> None:
                # A SQLite connection stays on its own thread.
                for violating in (violating_first, not violating_first):
                    queue(db, violating)

            asyncio.run(on_loop())
        else:
            for violating in (violating_first, not violating_first):
                queue(db, violating)
        ids = [b"r1", b"r2"] if existing else [b"r2"]
        for flush in (False, True):
            if flush:
                db.flush()
            rows = db.get_all_records(PATH, record_id_column=ID)
            assert sorted(rows.column(ID).to_pylist()) == ids
            assert rows.column("v").null_count == 0


@pytest.mark.parametrize("violating_first", [True, False])
def test_a_postgresql_database_checks_the_declaration_before_anything_is_sent(
    tmp_path: Path, violating_first: bool
) -> None:
    with open_database("PostgreSQL", tmp_path) as db:
        valid = _null_case("column", 2, null=False, not_null=False)
        violating = _null_case("column", 3, null=True, not_null=True)
        if not violating_first:
            _add(db, valid)
        with pytest.raises(NullInNonNullableColumnError, match="'v'"):
            _add(db, violating)
        if violating_first:
            _add(db, valid)
        rows = db.get_all_records(PATH, record_id_column=ID)
        assert rows is not None
        assert rows[ID].to_pylist() == [b"r2"]


@pytest.mark.parametrize("kind", [*CONNECTOR_DATABASES, "PostgreSQL"])
@pytest.mark.parametrize("id_type", [pa.binary(), pa.large_binary()])
@pytest.mark.parametrize("null_in", ["v", "the id"])
@pytest.mark.parametrize("queued", [True, False])
def test_each_record_id_type_gets_the_declared_null_error(
    tmp_path: Path, kind: str, id_type: Any, null_in: str, queued: bool
) -> None:
    """Review of PR 539: with a ``binary`` record id the rows are normalized, and the
    normalization raised a plain ``ValueError`` first."""

    def rows(n: int, *, null: bool) -> pa.Table:
        table = _null_case("column", n, null=null and null_in == "v", not_null=null)
        ids = pa.array([None if null and null_in == "the id" else b"r%d" % n], id_type)
        field = pa.field(ID, id_type, nullable=not null)
        return pa.Table.from_arrays(
            [ids, *table.columns[1:]], schema=table.schema.set(0, field)
        )

    with _connector_database(kind, tmp_path) as db:
        if queued:
            _add(db, rows(2, null=False))
        with pytest.raises(NullInNonNullableColumnError) as refused:
            _add(db, rows(3, null=True))
        assert refused.value.column == ("v" if null_in == "v" else "__record_id")
        rows_read = db.get_all_records(PATH, record_id_column=ID)
        held = [] if rows_read is None else rows_read[ID].to_pylist()
        assert held == ([b"r2"] if queued else [])


def test_the_declared_null_rule_reads_only_the_values_of_the_rows() -> None:
    """A value under a null struct, outside a slice, or at a nullable place never
    counts; a map's values do."""

    def path(array: Any, nullable: bool = True) -> str | None:
        field = pa.field("c", array.type, nullable=nullable)
        return declared_null_path(
            pa.Table.from_arrays([array], schema=pa.schema([field]))
        )

    ints = pa.array([1, None, 3])
    x = pa.field("x", pa.int64(), nullable=False)
    assert (path(ints), path(ints, False)) == (None, "c")
    assert path(pa.StructArray.from_arrays([ints], fields=[x])) == "c.x"
    hidden = pa.array([False, True, False])
    masked = pa.StructArray.from_arrays([ints], fields=[x], mask=hidden)
    assert (path(masked), path(masked, False)) == (None, "c")
    lists = _int_list([1, None], declared=True)
    assert (path(lists), path(lists.slice(0, 1)), path(lists.slice(1))) == (
        "c[]",
        None,
        "c[]",
    )
    assert path(_int_list([1, None], declared=False)) is None
    # The pyarrow stubs do not know the ``type`` argument of ``from_arrays``.
    arrow: Any = pa
    fixed = arrow.FixedSizeListArray.from_arrays(
        pa.array([1, None, 3, 4]), type=pa.list_(pa.field("item", pa.int64(), False), 2)
    )
    assert (path(fixed), path(fixed.slice(1))) == ("c[]", None)
    maps = arrow.MapArray.from_arrays(
        pa.array([0, 1, 2], pa.int32()),
        pa.array(["a", "b"]),
        pa.array([1, None]),
        type=arrow.map_(pa.string(), x),
    )
    assert (path(maps), path(maps.slice(0, 1)), path(maps.slice(1))) == (
        "c{}",
        None,
        "c{}",
    )
    # A key is never null, but a field nested in it can be (CodeRabbit on PR 539).
    for declared in (True, False):
        child = pa.field("x", pa.int64(), nullable=not declared)
        keys = pa.StructArray.from_arrays([pa.array([1, None])], fields=[child])
        keyed = arrow.MapArray.from_arrays(
            pa.array([0, 1, 2], pa.int32()),
            keys,
            pa.array([1, 2]),
            type=arrow.map_(pa.field("key", keys.type, nullable=False), pa.int64()),
        )
        found = "c{key}.x" if declared else None
        assert path(keyed) == path(keyed.slice(1)) == found
        assert path(keyed.slice(0, 1)) is None


# ── PostgreSQL (a live server: the PostgreSQL CI job) ──────────────────────────


@pytest.fixture
def postgresql_database() -> Iterator[ConnectorArrowDatabase]:
    psycopg = pytest.importorskip("psycopg")
    from orcapod.databases import PostgreSQLConnector

    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    dbname = os.environ.get("PGDATABASE", "testdb")
    user = os.environ.get("PGUSER", "postgres")
    password = os.environ.get("PGPASSWORD", "postgres")
    base_dsn = (
        f"host={host} port={port} dbname={dbname} user={user} password={password}"
    )
    schema = f"test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(base_dsn, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
    connector = PostgreSQLConnector(f"{base_dsn} options=-csearch_path={schema}")
    try:
        yield ConnectorArrowDatabase(connector)
    finally:
        connector.close()
        with psycopg.connect(base_dsn, autocommit=True) as admin:
            admin.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


@pytest.mark.postgres
@pytest.mark.parametrize("first", FIRST_ORDERS)
@pytest.mark.parametrize("second", SECOND_ORDERS)
@pytest.mark.parametrize("when", ["after a flush", "same pending batch"])
def test_postgresql_matches_an_append_in_another_order_by_name(
    postgresql_database: ConnectorArrowDatabase, first: str, second: str, when: str
) -> None:
    db = postgresql_database
    first_order, second_order = FIRST_ORDERS[first], SECOND_ORDERS[second]
    _add(db, _rows(first_order, [1, 2]), flush=when == "after a flush")
    _add(db, _rows(second_order, [3, 4]))
    _check(db.get_all_records(PATH, record_id_column=ID), first_order, [1, 2, 3, 4])
    db.flush()
    _check(db.get_all_records(PATH, record_id_column=ID), first_order, [1, 2, 3, 4])
