"""Batched database reads preserve the rows of the existing read paths (ITL-1451)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    InMemoryArrowDatabase,
    NoOpArrowDatabase,
    SpiralDBConnector,
    SQLiteConnector,
    delta_lake_databases,
)
from orcapod.protocols.database_protocols import (
    STORED_RECORD_ID_COLUMN,
    ArrowDatabaseProtocol,
)
from tests.test_databases import fake_pyspiral

_PATH = ("records",)
_SIZE = 1_050


@pytest.fixture(params=["delta", "memory", "sqlite"])
def database(request: pytest.FixtureRequest, tmp_path: Path) -> ArrowDatabaseProtocol:
    if request.param == "delta":
        return DeltaTableDatabase(str(tmp_path))
    if request.param == "sqlite":
        return ConnectorArrowDatabase(SQLiteConnector(tmp_path / "store.sqlite"))
    return InMemoryArrowDatabase()


def _rows(start: int, stop: int) -> pa.Table:
    return pa.table(
        {
            "rid": pa.array(
                [str(i).encode() for i in range(start, stop)], pa.large_binary()
            ),
            "group": pa.array([i % 10 for i in range(start, stop)], pa.int64()),
            "text": pa.array([str(i) for i in range(start, stop)], pa.large_string()),
        }
    )


def test_large_membership_matches_existing_reads(
    database: ArrowDatabaseProtocol,
) -> None:
    database.add_records(_PATH, _rows(0, _SIZE), record_id_column="rid", flush=True)
    scoped = database.at("scope")
    scoped.add_records(_PATH, _rows(0, _SIZE), record_id_column="rid", flush=True)
    scoped.add_records(
        _PATH, _rows(_SIZE, _SIZE + 2), record_id_column="rid", flush=False
    )

    ids = [str(i).encode() for i in reversed(range(_SIZE + 2))]
    result = scoped.get_records_by_column(
        _PATH,
        STORED_RECORD_ID_COLUMN,
        [*ids, "2", b"missing", "1051", None],
        record_id_column="rid",
    )
    assert result is not None
    assert set(result["rid"].to_pylist()) == set(ids)
    assert result.num_rows == len(ids)
    assert result.schema.names == ["rid", "group", "text"]

    table = scoped.get_all_records(_PATH, record_id_column="rid")
    assert table is not None
    for column, values in [
        ("group", [9, 4, 4, -1, None]),
        ("text", ["1050", "2", "0", "2", "missing"]),
    ]:
        selected = scoped.get_records_by_column(
            _PATH, column, values, record_id_column="rid"
        )
        assert selected is not None
        assert selected.schema.equals(table.schema, check_metadata=True)
        assert sorted(selected.to_pylist(), key=lambda row: row["rid"]) == sorted(
            (row for row in table.to_pylist() if row[column] in values),
            key=lambda row: row["rid"],
        )
    arrow_ids = pa.array([b"0", b"1051", b"1051"], type=pa.large_binary())
    arrow_matches = scoped.get_records_by_column(
        _PATH, STORED_RECORD_ID_COLUMN, arrow_ids, record_id_column="rid"
    )
    assert arrow_matches is not None
    assert set(arrow_matches["rid"].to_pylist()) == {b"0", b"1051"}
    assert arrow_matches.num_rows == 2

    assert scoped.get_records_by_column(_PATH, "group", []) is None
    absent = scoped.get_records_by_column(_PATH, "group", [-1], record_id_column="rid")
    assert absent is not None
    assert absent.num_rows == 0
    assert absent.schema.equals(table.schema, check_metadata=True)
    assert scoped.get_records_by_column(("absent",), "group", [1]) is None


def test_committed_collision_obeys_the_existing_read_overlay(
    database: ArrowDatabaseProtocol,
) -> None:
    database.add_record(_PATH, b"one", pa.table({"status": ["stored"]}), flush=True)
    database.add_record(_PATH, b"one", pa.table({"status": ["pending"]}))
    database.add_record(_PATH, b"two", pa.table({"status": ["pending"]}))
    view = database.get_all_records(_PATH, record_id_column="rid")
    actual = database.get_records_by_column(_PATH, "status", ["pending"], "rid")
    assert view is not None
    assert actual is not None
    assert set(actual["rid"].to_pylist()) == {
        row["rid"] for row in view.to_pylist() if row["status"] == "pending"
    }


def test_discard_store_returns_no_matches() -> None:
    assert NoOpArrowDatabase().get_records_by_column(_PATH, "group", [1]) is None


def test_fake_uses_one_scan_for_many_record_ids(tmp_path: Path) -> None:
    with fake_pyspiral.install(tmp_path):
        database = ConnectorArrowDatabase(SpiralDBConnector("p"))
        database.add_records(_PATH, _rows(0, 41), record_id_column="rid", flush=True)
        ids = [str(i).encode() for i in range(0, 40, 2)] + [b"missing"]
        fake_pyspiral.scan_log.clear()
        previous_rows = {
            row["text"].to_pylist()[0]
            for record_id in ids
            if (row := database.get_record_by_id(_PATH, record_id)) is not None
        }
        assert len(fake_pyspiral.scan_log) == len(ids)

        fake_pyspiral.scan_log.clear()
        result = database.get_records_by_column(
            _PATH, STORED_RECORD_ID_COLUMN, [*reversed(ids), ids[0]]
        )
        assert result is not None
        assert set(result["text"].to_pylist()) == previous_rows
        assert result.num_rows == len(previous_rows)
        assert len(fake_pyspiral.scan_log) == 1

        assert database.get_records_by_column(_PATH, "group", []) is None
        assert len(fake_pyspiral.scan_log) == 1


def test_delta_reuses_the_loaded_rows_across_lookup_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = DeltaTableDatabase(str(tmp_path))
    writer.add_records(_PATH, _rows(0, 100), record_id_column="rid", flush=True)
    whole_reads = 0
    original = delta_lake_databases.delta_to_pyarrow_dataset

    def count_read(*args: Any, **kwargs: Any):
        nonlocal whole_reads
        whole_reads += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(delta_lake_databases, "delta_to_pyarrow_dataset", count_read)
    previous = DeltaTableDatabase(str(tmp_path))
    for _ in range(3):
        assert previous.get_all_records(_PATH) is not None
    assert whole_reads == 3

    whole_reads = 0
    batched = DeltaTableDatabase(str(tmp_path))
    for group in (1, 5, 3):
        hits = batched.get_records_by_column(_PATH, "group", [group, 999])
        assert hits is not None
        assert hits["group"].to_pylist() == [group] * 10
    assert whole_reads == 1
