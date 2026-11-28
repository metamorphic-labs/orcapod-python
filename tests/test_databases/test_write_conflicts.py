"""Write conflicts are checked when rows are added (ITL-1386, ITL-1487)."""

from pathlib import Path
from sqlite3 import IntegrityError

import pyarrow as pa
import pytest

from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    InMemoryArrowDatabase,
    SQLiteConnector,
)
from tests.test_databases.test_connector_arrow_database import MockDBConnector

PATH = ("records",)


@pytest.mark.parametrize("backend", ["delta", "memory", "sqlite", "connector"])
def test_add_refuses_stored_id_without_buffering_other_rows(
    tmp_path: Path, backend: str
) -> None:
    """Both add methods reject a stored key, leaving the entire add unapplied."""
    with SQLiteConnector(":memory:") as sqlite:
        db = {
            "delta": DeltaTableDatabase(tmp_path),
            "memory": InMemoryArrowDatabase(),
            "sqlite": ConnectorArrowDatabase(sqlite),
            "connector": ConnectorArrowDatabase(MockDBConnector()),
        }[backend]
        db.add_record(PATH, b"x", pa.table({"v": [1]}), flush=True)

        with pytest.raises(ValueError, match="already exist.*stored"):
            db.add_record(PATH, b"x", pa.table({"v": [2]}))
        rows = pa.table({"id": [b"x", b"y"], "v": [2, 3]})
        with pytest.raises(ValueError, match="already exist.*stored"):
            db.add_records(PATH, rows, record_id_column="id")
        db.flush()

        stored = db.get_all_records(PATH, record_id_column="id")
        assert stored is not None
        assert stored.to_pylist() == [{"id": b"x", "v": 1}]


@pytest.mark.parametrize("backend", ["delta", "sqlite", "connector"])
@pytest.mark.parametrize("skip_duplicates", [False, True])
def test_conflict_at_flush_keeps_first_commit(
    tmp_path: Path, backend: str, skip_duplicates: bool
) -> None:
    """A write accepted before another handle commits the key loses at flush."""
    path = tmp_path / "store.db"
    with SQLiteConnector(str(path)) as first, SQLiteConnector(str(path)) as second:
        mock = MockDBConnector()
        a, b = {
            "delta": (DeltaTableDatabase(tmp_path), DeltaTableDatabase(tmp_path)),
            "sqlite": (ConnectorArrowDatabase(first), ConnectorArrowDatabase(second)),
            "connector": (ConnectorArrowDatabase(mock), ConnectorArrowDatabase(mock)),
        }[backend]
        a.add_record(PATH, b"x", pa.table({"v": [1]}), skip_duplicates=skip_duplicates)
        b.add_record(PATH, b"x", pa.table({"v": [2]}), flush=True)
        a.flush()
        stored = b.get_all_records(PATH)
        assert stored is not None
        assert stored.to_pylist() == [{"v": 2}]


def test_delta_add_sees_id_committed_since_its_last_read(tmp_path: Path) -> None:
    """The strict add checks the current store, including for a warm handle."""
    a, b = DeltaTableDatabase(tmp_path), DeltaTableDatabase(tmp_path)
    a.add_record(PATH, b"initial", pa.table({"v": [0]}), flush=True)
    assert a.get_record_by_id(PATH, b"x") is None
    b.add_record(PATH, b"x", pa.table({"v": [1]}), flush=True)
    with pytest.raises(ValueError, match="already exist.*stored"):
        a.add_record(PATH, b"x", pa.table({"v": [2]}))


def test_strict_sqlite_insert_reports_a_constraint_error() -> None:
    """Strict writes skip a lost ID race and still report other SQL errors."""
    with SQLiteConnector(":memory:") as sqlite:
        db = ConnectorArrowDatabase(sqlite)
        schema = pa.schema([pa.field("v", pa.int64(), nullable=False)])
        db.add_record(PATH, b"x", pa.table({"v": [1]}, schema=schema), flush=True)
        with pytest.raises(IntegrityError, match="NOT NULL"):
            db.add_record(
                PATH, b"y", pa.table({"v": [None]}, schema=schema), flush=True
            )
        db.add_record(
            PATH,
            b"y",
            pa.table({"v": [None]}, schema=schema),
            skip_duplicates=True,
            flush=True,
        )
        stored = db.get_all_records(PATH)
        assert stored is not None and stored.to_pylist() == [{"v": 1}]
