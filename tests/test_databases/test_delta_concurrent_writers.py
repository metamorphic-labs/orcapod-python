"""Two writers committing the same new record id store it once (ITL-1377, ITL-1384).

A ``DeltaTableDatabase`` decides which pending ids are new against the table version it
loads for a commit. Another writer can commit between that load and the commit. Every
commit to an existing table is therefore a MERGE, which Delta refuses when rows were
added after the load; the flush then loads the table again and retries. A create fails
when another writer created the table first, and is retried the same way.

The interleavings are deterministic: the other writer commits inside the first writer's
flush, after it loaded the table (``_snapshot_for_commit``) or before it creates the
table (``deltalake.write_deltalake``). Two processes are checked as well.
"""

from __future__ import annotations

import multiprocessing
import sys
from pathlib import Path
from typing import Any, Literal
from unittest.mock import patch

import deltalake
import pyarrow as pa
import pytest
from deltalake.exceptions import CommitFailedError

from orcapod.databases import DeltaTableDatabase
from orcapod.databases import delta_lake_databases

PATH = ("t",)

SchemaHandling = Literal["error", "merge", "coerce"]


BOUND = 4


@pytest.fixture(autouse=True)
def _no_wait_between_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    """The random wait before a retry only spaces attempts; tests skip it, and use a
    small bound so that reaching it takes a few commits."""
    monkeypatch.setattr(delta_lake_databases, "_sleep_before_retry", lambda _: None)
    monkeypatch.setattr(delta_lake_databases, "_COMMIT_ATTEMPTS", BOUND)


def _records(n: int, value: int, *, reordered: bool = False) -> pa.Table:
    """One row with record id ``bytes([n])``; ``reordered`` puts its columns in another order."""
    table = pa.table(
        {
            "rid": pa.array([bytes([n])], pa.large_binary()),
            "a": pa.array([value], pa.int64()),
            "z": pa.array([100 + n], pa.int64()),
        }
    )
    return table.select(["z", "rid", "a"]) if reordered else table


def _stored(root: Path) -> list[tuple[bytes, int]]:
    """Every stored ``(record id, a)``, read by a fresh database, sorted."""
    table = DeltaTableDatabase(root).get_all_records(PATH, record_id_column="rid")
    assert table is not None
    return sorted(zip(table["rid"].to_pylist(), table["a"].to_pylist()))


def _operations(root: Path) -> list[str]:
    """The table's commit operations, oldest first."""
    history = deltalake.DeltaTable(str(root / "t")).history()
    return [str(entry["operation"]) for entry in reversed(history)]


def _commit_after_load(
    one: DeltaTableDatabase, commit: Any, *, every_attempt: bool = False
) -> Any:
    """Patch ``one`` so ``commit()`` runs after ``one`` loads the table for a commit."""
    real = one._snapshot_for_commit
    done: list[bool] = []

    def interleaved(*args: Any) -> Any:
        loaded = real(*args)
        if every_attempt or not done:
            done.append(True)
            commit()
        return loaded

    return patch.object(one, "_snapshot_for_commit", side_effect=interleaved)


@pytest.mark.parametrize("flushed", [False, True], ids=["pending", "flushed"])
@pytest.mark.parametrize("reordered", [False, True], ids=["same-order", "other-order"])
@pytest.mark.parametrize("skip_duplicates", [True, False])
@pytest.mark.parametrize("schema_handling", ["error", "merge", "coerce"])
def test_an_id_another_writer_committed_meanwhile_is_stored_once(
    tmp_path: Path,
    schema_handling: SchemaHandling,
    skip_duplicates: bool,
    reordered: bool,
    flushed: bool,
) -> None:
    """The later writer retries, sees the id and leaves it: the first writer's row stays."""
    one, two = DeltaTableDatabase(tmp_path), DeltaTableDatabase(tmp_path)
    one.add_records(PATH, _records(1, 10), record_id_column="rid", flush=True)

    def other_writer() -> None:
        two.add_records(
            PATH,
            _records(2, 99, reordered=reordered),
            record_id_column="rid",
            skip_duplicates=skip_duplicates,
            schema_handling=schema_handling,
            flush=True,
        )

    with _commit_after_load(one, other_writer):
        one.add_records(
            PATH,
            _records(2, 20),
            record_id_column="rid",
            skip_duplicates=skip_duplicates,
            schema_handling=schema_handling,
            flush=flushed,
        )
        if not flushed:
            one.flush()

    assert _stored(tmp_path) == [(b"\x01", 10), (b"\x02", 99)]
    # The later writer's retry inserted nothing, so it committed nothing.
    assert _operations(tmp_path) == ["WRITE", "MERGE"]
    # Both handles answer from what is committed.
    for db in (one, two):
        row = db.get_record_by_id(PATH, b"\x02")
        assert row is not None and row["a"].to_pylist() == [99]


@pytest.mark.parametrize("reordered", [False, True], ids=["same-order", "other-order"])
def test_disjoint_ids_are_both_committed(tmp_path: Path, reordered: bool) -> None:
    """A conflict over another id is retried, and both rows are stored."""
    one, two = DeltaTableDatabase(tmp_path), DeltaTableDatabase(tmp_path)
    one.add_records(PATH, _records(1, 10), record_id_column="rid", flush=True)

    def other_writer() -> None:
        two.add_records(
            PATH,
            _records(3, 30, reordered=reordered),
            record_id_column="rid",
            flush=True,
        )

    with _commit_after_load(one, other_writer):
        one.add_records(PATH, _records(2, 20), record_id_column="rid", flush=True)

    assert _stored(tmp_path) == [(b"\x01", 10), (b"\x02", 20), (b"\x03", 30)]
    assert _operations(tmp_path) == ["WRITE", "MERGE", "MERGE"]
    row = one.get_record_by_id(PATH, b"\x03")
    assert row is not None and row["a"].to_pylist() == [30]


@pytest.mark.parametrize("same_id", [True, False], ids=["same-id", "other-id"])
def test_a_table_another_writer_created_meanwhile_keeps_its_rows(
    tmp_path: Path, same_id: bool
) -> None:
    """The later create fails and is retried as a MERGE; no committed row is lost (ITL-1384)."""
    one, two = DeltaTableDatabase(tmp_path), DeltaTableDatabase(tmp_path)
    real = deltalake.write_deltalake
    created: list[bool] = []

    def interleaved(*args: Any, **kwargs: Any) -> None:
        if not created:
            created.append(True)
            two.add_records(
                PATH,
                _records(1 if same_id else 2, 99),
                record_id_column="rid",
                flush=True,
            )
        real(*args, **kwargs)

    one.add_records(PATH, _records(1, 10), record_id_column="rid")
    with patch.object(deltalake, "write_deltalake", side_effect=interleaved):
        one.flush()

    if same_id:
        assert _stored(tmp_path) == [(b"\x01", 99)]
        assert _operations(tmp_path) == ["WRITE"]
    else:
        assert _stored(tmp_path) == [(b"\x01", 10), (b"\x02", 99)]
        assert _operations(tmp_path) == ["WRITE", "MERGE"]


def test_a_writer_that_keeps_losing_stops_after_the_bound(tmp_path: Path) -> None:
    """After ``_COMMIT_ATTEMPTS`` lost commits the flush raises main's error; rows stay queued."""
    one, two = DeltaTableDatabase(tmp_path), DeltaTableDatabase(tmp_path)
    one.add_records(PATH, _records(1, 10), record_id_column="rid", flush=True)
    others = iter(range(10, 10 + BOUND))

    def other_writer() -> None:
        two.add_records(
            PATH, _records(next(others), 0), record_id_column="rid", flush=True
        )

    one.add_records(PATH, _records(2, 20), record_id_column="rid")
    with _commit_after_load(one, other_writer, every_attempt=True) as loads:
        with pytest.raises(RuntimeError) as info:
            one.flush()
    assert isinstance(info.value.__cause__, CommitFailedError)
    assert loads.call_count == BOUND
    # The batch is still queued, and the next flush commits it.
    one.flush()
    stored = _stored(tmp_path)
    assert (b"\x02", 20) in stored
    assert len(stored) == 2 + BOUND


def test_a_writer_that_wins_after_losing_commits_within_the_bound(
    tmp_path: Path,
) -> None:
    """Losing every attempt but the last still commits."""
    one, two = DeltaTableDatabase(tmp_path), DeltaTableDatabase(tmp_path)
    one.add_records(PATH, _records(1, 10), record_id_column="rid", flush=True)
    losses = BOUND - 1
    others = iter(range(10, 10 + losses))

    def other_writer() -> None:
        other = next(others, None)
        if other is not None:
            two.add_records(
                PATH, _records(other, 0), record_id_column="rid", flush=True
            )

    one.add_records(PATH, _records(2, 20), record_id_column="rid")
    with _commit_after_load(one, other_writer, every_attempt=True) as loads:
        one.flush()
    assert loads.call_count == BOUND
    assert (b"\x02", 20) in _stored(tmp_path)


def _write_ids(root: str, start: Any, ids: list[int], value: int) -> None:
    """Commit each id in ``ids`` alone, once ``start`` lets every process go."""
    db = DeltaTableDatabase(root)
    start.wait()
    for n in ids:
        db.add_records(PATH, _records(n, value), record_id_column="rid", flush=True)


@pytest.mark.skipif(sys.platform == "win32", reason="spawned writers on one table")
def test_writers_in_other_processes_store_each_id_once(tmp_path: Path) -> None:
    """Three processes commit the same ids one at a time: each id is stored once."""
    context = multiprocessing.get_context("spawn")
    start = context.Barrier(3)
    ids = list(range(1, 13))
    processes = [
        context.Process(target=_write_ids, args=(str(tmp_path), start, ids, value))
        for value in (1, 2, 3)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=120)
        assert process.exitcode == 0
    stored = _stored(tmp_path)
    assert [rid for rid, _ in stored] == [bytes([n]) for n in ids]


def test_writers_creating_one_table_at_once_store_an_id_once(tmp_path: Path) -> None:
    """Four writers create the same table at the same moment, all with one id.

    deltalake 1.6 commits a create that lost the race on top of the winner's, as an
    append, unless the create is committed without retries; the losers then load the
    table and MERGE, which finds the id stored (ITL-1384, ITL-1377).
    """
    import threading

    for trial in range(10):
        root = tmp_path / str(trial)
        start = threading.Barrier(4)
        errors: list[BaseException] = []

        def write(value: int) -> None:
            db = DeltaTableDatabase(root)
            db.add_records(PATH, _records(1, value), record_id_column="rid")
            start.wait()
            try:
                db.flush()
            except BaseException as e:  # noqa: BLE001  # reported below
                errors.append(e)

        threads = [threading.Thread(target=write, args=(v,)) for v in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert errors == []
        stored = _stored(root)
        assert [rid for rid, _ in stored] == [b"\x01"], trial
