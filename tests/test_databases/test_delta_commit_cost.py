"""What a ``DeltaTableDatabase`` commit costs, and that its snapshot stays exact (ITL-1173).

After the rows of a table are loaded for lookups (ITL-919), each commit of this database
extends them with the rows of the data files the commit added. These tests pin:

- the cost: an append commit loads the table once and never builds the table's dataset
  (which lists every data file); counted, not timed;
- the result: after any sequence of commits (appends, MERGEs, schema evolution, another
  writer's commits, a partitioned table) the extended rows equal a full read of the
  table, and lookups through this database answer as a fresh database's do.

The snapshot's rows are compared directly (``_stored_rows``): they are the subject.
"""

from __future__ import annotations

import random
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import deltalake
import pyarrow as pa
import pyarrow.dataset as pa_dataset
import pytest

from orcapod.databases import DeltaTableDatabase
from orcapod.databases import delta_lake_databases

PATH = ("table",)


def _rows(ids: list[str], *, extra: bool = False) -> pa.Table:
    n = len(ids)
    table = pa.table(
        {
            "__record_id": pa.array([i.encode() for i in ids], pa.large_binary()),
            "hash": pa.array([f"h{i}".encode() for i in ids], pa.large_binary()),
            "context": pa.array(["ctx"] * n, pa.large_string()),
            "value": pa.array(
                [None if k % 5 == 0 else k for k in range(n)], pa.int64()
            ),
            "items": pa.array(
                [[k, k + 1] for k in range(n)], pa.large_list(pa.int64())
            ),
            "point": pa.array(
                [{"x": float(k), "label": f"p{k}"} for k in range(n)],
                pa.struct([("x", pa.float64()), ("label", pa.large_string())]),
            ),
            "at": pa.array(
                [datetime(2026, 1, 1, tzinfo=timezone.utc)] * n,
                pa.timestamp("us", "UTC"),
            ),
        }
    )
    if extra:
        table = table.append_column(
            "added", pa.array([f"a{i}" for i in ids], pa.large_string())
        )
    return table


def _data_files(table_dir: str) -> list[str]:
    """The data files of the table's current version, as its add actions name them."""
    actions = pa.table(deltalake.DeltaTable(table_dir).get_add_actions(flatten=False))
    return cast(list[str], actions.column("path").to_pylist())


def _sorted(table: pa.Table) -> pa.Table:
    return table.sort_by("__record_id")


def _assert_snapshot_exact(db: DeltaTableDatabase, root: Path) -> None:
    """The extended rows equal a full read, and lookups agree with a fresh database."""
    ours = db._stored_rows(PATH)
    fresh_db = DeltaTableDatabase(root)
    fresh = fresh_db._stored_rows(PATH)
    assert ours is not None and fresh is not None
    assert ours.table.schema == fresh.table.schema
    assert _sorted(ours.table).equals(_sorted(fresh.table))
    for record_id in cast(list[bytes], fresh.table["__record_id"].to_pylist()):
        got = db.get_record_by_id(PATH, record_id)
        want = fresh_db.get_record_by_id(PATH, record_id)
        assert got is not None and want is not None
        assert got.equals(want)
        by_value = db.get_records_with_column_value(
            PATH, {"hash": b"h" + record_id, "context": "ctx"}
        )
        assert by_value is not None and by_value.num_rows == 1


@pytest.fixture
def files_read(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """Record the data files of every dataset the snapshot extension reads directly."""
    read: list[list[str]] = []
    real = pa_dataset.FileSystemDataset

    def recording(fragments, *args, **kwargs):
        fragments = list(fragments)
        read.append([fragment.path for fragment in fragments])
        return real(fragments, *args, **kwargs)

    monkeypatch.setattr(
        delta_lake_databases,
        "pa_ds",
        types.SimpleNamespace(FileSystemDataset=recording),
    )
    return read


@pytest.fixture
def counts(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Count table loads and whole-table dataset builds by the Delta database."""
    counted = {"table_loads": 0, "datasets": 0}
    original_init = deltalake.DeltaTable.__init__
    original_dataset = delta_lake_databases.delta_to_pyarrow_dataset

    def counting_init(self, *args, **kwargs):
        counted["table_loads"] += 1
        original_init(self, *args, **kwargs)

    def counting_dataset(*args, **kwargs):
        counted["datasets"] += 1
        return original_dataset(*args, **kwargs)

    monkeypatch.setattr(deltalake.DeltaTable, "__init__", counting_init)
    monkeypatch.setattr(
        delta_lake_databases, "delta_to_pyarrow_dataset", counting_dataset
    )
    return counted


def test_append_commit_loads_once_and_reads_only_its_files(
    tmp_path: Path, counts: dict[str, int], files_read: list[list[str]]
) -> None:
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, _rows(["r0", "r1"]), flush=True)
    assert db.get_record_by_id(PATH, b"r0") is not None  # loads the rows once

    table_dir = str(tmp_path / "table")
    for commit in range(1, 21):
        files_before = set(_data_files(table_dir))
        counts["table_loads"] = counts["datasets"] = 0
        files_read.clear()
        db.add_records(
            PATH, _rows([f"r{commit}_{k}" for k in range(3)]), skip_duplicates=True
        )
        db.flush()
        # Lookups between commits are answered from the extended rows.
        assert db.get_record_by_id(PATH, f"r{commit}_1".encode()) is not None
        assert db.get_record_by_id(PATH, b"r0") is not None
        assert counts == {"table_loads": 1, "datasets": 0}, commit
        added = set(_data_files(table_dir)) - files_before
        assert len(added) == 1
        assert files_read == [sorted(added)], commit

    _assert_snapshot_exact(db, tmp_path)


def test_snapshot_equals_a_full_read_after_mixed_commits(tmp_path: Path) -> None:
    """Appends, MERGEs (a stored id), schema evolution and another writer's commits."""
    rng = random.Random(1173)
    db = DeltaTableDatabase(tmp_path)
    other = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, _rows(["s0", "s1", "s2"]), flush=True)
    db._stored_rows(PATH)
    stored = ["s0", "s1", "s2"]
    for step in range(30):
        new = [f"n{step}_{k}" for k in range(rng.randint(1, 4))]
        kind = rng.choice(["append", "append", "merge", "other"])
        if kind == "merge":
            db.add_records(
                PATH, _rows([rng.choice(stored), *new]), skip_duplicates=True
            )
            db.flush()
        elif kind == "other":
            other.add_records(PATH, _rows(new), flush=True)
            db.flush()  # sees the other commit at its next commit
            db.add_records(PATH, _rows([f"{n}_ours" for n in new]), flush=True)
            new += [f"{n}_ours" for n in new]
        else:
            db.add_records(PATH, _rows(new), flush=True)
        stored += new
        _assert_snapshot_exact(db, tmp_path)

    # Schema evolution: the rows are read again, then extended as before.
    db.add_records(PATH, _rows(["e0", "e1"], extra=True), flush=True)
    _assert_snapshot_exact(db, tmp_path)
    db.add_records(PATH, _rows(["e2"], extra=True), flush=True)
    _assert_snapshot_exact(db, tmp_path)


def test_partitioned_table_reads_its_dataset(
    tmp_path: Path, counts: dict[str, int]
) -> None:
    """Partition values are not in the data files: the table's dataset is read.

    Orcapod's own commits to a partitioned table fail before this point (its id read
    uses a one-column schema), so the extension is driven directly, after a commit
    by another writer.
    """
    table_dir = str(tmp_path / "table")
    deltalake.write_deltalake(table_dir, _rows(["p0", "p1"]), partition_by=["context"])
    db = DeltaTableDatabase(tmp_path)
    assert db.get_record_by_id(PATH, b"p0") is not None
    base = db._snapshots["table"]
    base.ids = {b"p0", b"p1"}
    deltalake.write_deltalake(
        table_dir, _rows(["q0", "q1"]), mode="append", partition_by=["context"]
    )
    counts["datasets"] = 0

    extended = db._extend_snapshot(
        base, deltalake.DeltaTable(table_dir), {b"q0", b"q1"}
    )

    assert counts["datasets"] == 1
    assert extended.rows is not None
    fresh = DeltaTableDatabase(tmp_path)._stored_rows(PATH)
    assert fresh is not None
    assert _sorted(extended.rows.table).equals(_sorted(fresh.table))
