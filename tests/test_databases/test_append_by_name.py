"""An append matches columns by name, whatever their order (ITL-1367).

Every database puts rows whose columns are the held table's columns in another order
(each with the held type) into the held order before anything is checked or buffered:
the pending batch's order, else the stored table's. The table keeps the order of its
first write. ``a`` and ``c`` are both ``int64``, so a positional match would swap their
values: every test reads the values back by name. Appends that add, lack or retype a
column, and a name that repeats exactly, keep the database's own rule.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pyarrow as pa
import pytest

from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    ExtensionAwareDatabase,
    SpiralDBConnector,
    SQLiteConnector,
)
from orcapod.databases.utils import in_held_column_order
from orcapod.errors import InputValidationError, UnstorableColumnError
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
DATABASES = ("in-memory", "Delta", "SQLite", "SpiralDB")
DURABLE = ("Delta", "SQLite", "SpiralDB")


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

    The first write's order, except in SpiralDB once rows are stored: SpiralDB
    stores and reads a table's columns in its own order, the key columns, then the
    value columns sorted by name, whatever the order written (observed live,
    ITL-1367; its fake does the same). Rows still pending keep the first write's.
    """
    if database == "SpiralDB" and stored:
        return [ID, *sorted(n for n in first if n != ID)]
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
    Orcapod (SpiralDB orders a table's columns its own way)."""
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
    ("SpiralDB", "after a flush"): ("flush", ValueError),
    ("SpiralDB", "same pending batch"): ("add", pa.ArrowInvalid),
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


def test_a_spiraldb_table_deleted_and_created_again_in_another_order(
    tmp_path: Path,
) -> None:
    with open_database("SpiralDB", tmp_path) as db:
        _add(db, _rows([ID, "a", "c", "b"], [1]), flush=True)
        db._connector.delete_table("t")  # the connector's own drop
        other_db = ConnectorArrowDatabase(SpiralDBConnector("p"))
        _add(other_db, _rows(["b", ID, "c", "a"], [3]), flush=True)
        _add(db, _rows([ID, "a", "c", "b"], [4]), flush=True)
        _check(
            db.get_all_records(PATH, record_id_column=ID),
            _given_order("SpiralDB", ["b", ID, "c", "a"]),
            [3, 4],
        )


# ── The shared step ────────────────────────────────────────────────────────────


def _held(*names: str) -> list[tuple[str, Any]]:
    return [(n, pa.int64()) for n in names]


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
    """Metadata and nullability go on to the append checks as given."""
    field = pa.field("c", pa.int64(), nullable=False, metadata={"unit": "m"})
    given = pa.Table.from_arrays(
        [pa.array([1], pa.int64()), pa.array([2], pa.int64())],
        schema=pa.schema([field, pa.field("a", pa.int64())]),
    )
    out = in_held_column_order(given, _held("a", "c"))
    assert out.schema.field("c").equals(field, check_metadata=True)


def test_the_step_matches_names_exactly() -> None:
    """``a`` and ``A`` are two columns, as Arrow matches names exactly."""
    out = in_held_column_order(_table("A", "a", "c"), _held("a", "c", "A"))
    assert out.column_names == ["a", "c", "A"]
    assert out.to_pylist() == [{"A": 0, "a": 1, "c": 2}]


def test_the_step_compares_a_logical_type_by_its_storage() -> None:
    """A database holding a logical column reports its storage type."""
    from tests.test_databases.test_extension_aware_database import (
        _make_converter_with_type,
    )

    _, ext_type = _make_converter_with_type("test.itl1367.storage")
    text = pa.ExtensionArray.from_storage(ext_type, pa.array(["x"], pa.large_string()))
    given = pa.table({"text": text, "a": [1]})
    out = in_held_column_order(given, [("a", pa.int64()), ("text", pa.string())])
    assert out.column_names == ["a", "text"]


def test_the_step_compares_types_in_their_large_variants() -> None:
    given = pa.table({"b": pa.array(["x"], pa.string()), "a": [1]})
    out = in_held_column_order(given, [("a", pa.int64()), ("b", pa.large_string())])
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
            [("a", pa.int64()), ("c", pa.int32())],
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
                ("a", pa.int64()),
                ("s", pa.struct([("x", pa.int64()), ("y", pa.int64())])),
            ],
            id="struct children in another order",
        ),
    ],
)
def test_the_step_leaves_anything_else_as_given(
    given: pa.Table, held: list[tuple[str, Any]] | None
) -> None:
    assert in_held_column_order(given, held) is given


@pytest.mark.parametrize("database", ["in-memory", "Delta"])
@pytest.mark.parametrize("when", ["after a flush", "same pending batch"])
def test_names_differing_in_case_only_are_two_columns(
    tmp_path: Path, database: str, when: str
) -> None:
    """The in-memory and Delta databases hold ``a`` and ``A`` apart; an append of them
    in another order is matched by exact name."""
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
    """Delta stores a logical column as its storage type; an append of it in another
    order is put in the held order whether the held rows are committed or pending."""
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
    assert pending.column_names == committed.column_names
    assert committed.to_pylist() == [{ID: bytes([2]), "text": "2", "n": 2}]


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
def test_a_reordered_append_is_committed_as_insert_if_missing_across_writers(
    tmp_path: Path,
    skip_duplicates: bool,
    stale_order: tuple[str, ...],
    other_order: tuple[str, ...],
) -> None:
    """Two handles append the same new record id with different values, each in
    another order than the stored table. The second commits after the first has
    taken its snapshot of the stored ids. As on main (where rows in another order
    than the stored table are committed by MERGE), the stale commit fails and the id
    keeps one row: the reorder does not turn the commit into a blind append."""
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

    with patch.object(stale, "_snapshot_for_commit", side_effect=interleave):
        with pytest.raises(RuntimeError) as raised:
            stale.add_records(
                PATH,
                _race_rows(2, 99, stale_order),
                record_id_column="rid",
                flush=True,
                skip_duplicates=skip_duplicates,
            )
    assert type(raised.value.__cause__).__name__ == "CommitFailedError"
    table = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="rid")
    assert table is not None
    assert sorted(table.select(["rid", "a"]).to_pylist(), key=str) == [
        {"rid": bytes([1]), "a": 10},
        {"rid": bytes([2]), "a": 20},
    ]


def test_an_all_duplicate_append_looks_up_no_order(tmp_path: Path) -> None:
    """With ``skip_duplicates`` and nothing left to write, the stored order is not
    looked up: a stored table whose recorded type cannot be read does not fail an
    append that writes nothing."""
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
    """The connector database looks a table up once per flushed append, as main
    does: an append on a fresh handle shares its lookup with the flush, and an
    append to a table the handle has seen looks nothing up before the flush."""
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
