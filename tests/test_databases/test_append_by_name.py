"""An append matches columns by name, whatever their order (ITL-1367).

Every database puts rows whose columns are the held table's columns in another order
(each with the held type) into the held order before anything is checked or buffered:
the pending batch's order, else the stored table's. The table keeps the order of its
first write. ``a`` and ``c`` are both ``int64``, so a positional match would swap their
values: every test reads the values back by name. Appends that add, lack or retype a
column, and names that repeat or differ in case only, keep the database's own rule.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.databases import ConnectorArrowDatabase
from orcapod.databases.utils import in_held_column_order
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


@pytest.mark.parametrize("database", DATABASES)
@pytest.mark.parametrize("when", ["after a flush", "same pending batch"])
def test_an_append_that_also_retypes_a_column_keeps_todays_rule(
    tmp_path: Path, database: str, when: str
) -> None:
    """Another type is not a reordering: the rows are left as given, for the
    database's own rule. Whatever that rule does, no value is matched by position."""
    retyped = _rows(["c", "a", "b", ID], [3]).set_column(
        1, "a", pa.array([3], pa.int32())
    )
    with open_database(database, tmp_path) as db:
        _add(db, _rows([ID, "a", "c", "b"], [1]), flush=when == "after a flush")
        try:
            _add(db, retyped, flush=True)
        except Exception:  # noqa: BLE001  # today's rule refused it: nothing swapped
            return
        table = db.get_all_records(PATH, record_id_column=ID)
        assert table is not None
        got = sorted(table.select([ID, "a", "c"]).to_pylist(), key=lambda r: r["a"])
        assert got == [
            {ID: b"r1", "a": 1, "c": 101},
            {ID: b"r3", "a": 3, "c": 103},
        ]


def test_a_table_created_again_in_another_order_is_read_as_it_is_now(
    tmp_path: Path,
) -> None:
    """The connector database reads the stored order on every append, so a table
    dropped and created again in another order is matched to its new order."""
    with open_database("SQLite", tmp_path) as db:
        _add(db, _rows([ID, "a", "c", "b"], [1]), flush=True)
        _add(db, _rows(["c", "a", "b", ID], [2]), flush=True)
        with sqlite3.connect(tmp_path / "store.db") as other:
            other.execute('DROP TABLE "t"')
        _add(db, _rows(["b", ID, "c", "a"], [3]), flush=True)
        _add(db, _rows([ID, "a", "c", "b"], [4]), flush=True)
        _check(
            db.get_all_records(PATH, record_id_column=ID),
            ["b", ID, "c", "a"],
            [3, 4],
        )


def test_a_spiraldb_table_deleted_and_created_again_in_another_order(
    tmp_path: Path,
) -> None:
    with open_database("SpiralDB", tmp_path) as db:
        _add(db, _rows([ID, "a", "c", "b"], [1]), flush=True)
        db._connector.delete_table("t")  # the connector's own drop
        _add(db, _rows(["b", ID, "c", "a"], [3]), flush=True)
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
        pytest.param(
            _table("A", "a", "c"), _held("a", "c", "A"), id="names differing in case"
        ),
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


def test_names_differing_in_case_only_keep_todays_rule() -> None:
    """The in-memory database holds ``a`` and ``A`` apart; a reordered append of them
    is not reordered and is refused, as it is today."""
    with open_database("in-memory", Path(".")) as db:
        _add(db, _table_with_id([ID, "a", "A"]))
        with pytest.raises(pa.ArrowInvalid):
            _add(db, _table_with_id(["A", ID, "a"]))


def _table_with_id(order: list[str]) -> pa.Table:
    columns = {
        ID: pa.array([uuid.uuid4().bytes], pa.large_binary()),
        "a": pa.array([1], pa.int64()),
        "A": pa.array([2], pa.int64()),
    }
    return pa.table({name: columns[name] for name in order})


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
