"""Reads through a ``DeltaTableDatabase`` handle reflect every write of its family (ITL-864).

Contract: a read through any handle of one ``at()`` family returns exactly the rows a
flush would leave, pending and stored together, with no duplicates and no missing
rows. Delta's flush inserts only rows whose record id is not stored yet, so on an id
overlap the stored row is the one returned.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import cast
from pathlib import Path

import deltalake
import pyarrow as pa
import pytest

from orcapod.databases import DeltaTableDatabase

PATH = ("table",)
SCOPE = "scope"


def _rows(
    ids: list[str], values: list[int], groups: list[int] | None = None
) -> pa.Table:
    """Build a record table keyed by ``__record_id`` with ``int64`` columns ``v`` and ``g``."""
    return pa.table(
        {
            "__record_id": pa.array([i.encode() for i in ids], type=pa.large_binary()),
            "v": pa.array(values, type=pa.int64()),
            "g": pa.array(
                groups if groups is not None else [0] * len(ids), type=pa.int64()
            ),
        }
    )


def _values(table: pa.Table | None) -> list[int] | None:
    """Return the sorted ``v`` values of ``table``, or ``None`` when there is no table."""
    if table is None:
        return None
    return sorted(cast(list[int], table.column("v").to_pylist()))


Read = Callable[[DeltaTableDatabase, tuple[str, ...]], "pa.Table | None"]

# Each read method, called for rows ``a=1`` and ``b=2`` (both in group 0).
READS: dict[str, Read] = {
    "get_all_records": lambda db, path: db.get_all_records(path),
    "get_record_by_id": lambda db, path: db.get_record_by_id(path, b"b"),
    "get_records_by_ids": lambda db, path: db.get_records_by_ids(path, [b"a", b"b"]),
    "get_records_with_column_value": lambda db, path: db.get_records_with_column_value(
        path, {"g": 0}
    ),
}
EXPECTED = {
    "get_all_records": [1, 2],
    "get_record_by_id": [2],
    "get_records_by_ids": [1, 2],
    "get_records_with_column_value": [1, 2],
}


@pytest.mark.parametrize("flusher", ["writer", "root", "reader"])
@pytest.mark.parametrize("method", list(READS))
def test_sibling_handle_sees_rows_flushed_by_another_handle(
    tmp_path: Path, method: str, flusher: str
) -> None:
    """A handle that already loaded the table sees rows a sibling handle wrote and flushed."""
    root = DeltaTableDatabase(tmp_path)
    reader = root.at(SCOPE)
    writer = root.at(SCOPE)
    read = READS[method]

    reader.add_records(PATH, _rows(["a"], [1]), flush=True)
    assert _values(reader.get_all_records(PATH)) == [1]  # reader loads its snapshot

    writer.add_records(PATH, _rows(["b"], [2]))
    assert _values(read(reader, PATH)) == EXPECTED[method]
    assert _values(read(root, (SCOPE, *PATH))) == EXPECTED[method]

    {"writer": writer, "root": root, "reader": reader}[flusher].flush()

    assert _values(read(reader, PATH)) == EXPECTED[method]
    assert _values(read(writer, PATH)) == EXPECTED[method]
    assert _values(read(root, (SCOPE, *PATH))) == EXPECTED[method]
    assert _values(read(root.at(SCOPE), PATH)) == EXPECTED[method]


@pytest.fixture
def stored_and_pending(tmp_path: Path) -> DeltaTableDatabase:
    """A handle with stored rows ``a=1 (g=0)``, ``b=2 (g=1)`` and pending row ``c=3 (g=1)``."""
    db = DeltaTableDatabase(tmp_path).at(SCOPE)
    db.add_records(PATH, _rows(["a", "b"], [1, 2], [0, 1]), flush=True)
    db.add_records(PATH, _rows(["c"], [3], [1]))
    return db


def test_get_all_records_returns_stored_and_pending(
    stored_and_pending: DeltaTableDatabase,
) -> None:
    assert _values(stored_and_pending.get_all_records(PATH)) == [1, 2, 3]


def test_get_record_by_id_finds_stored_and_pending(
    stored_and_pending: DeltaTableDatabase,
) -> None:
    assert _values(stored_and_pending.get_record_by_id(PATH, b"a")) == [1]
    assert _values(stored_and_pending.get_record_by_id(PATH, b"c")) == [3]


def test_get_records_by_ids_returns_stored_and_pending(
    stored_and_pending: DeltaTableDatabase,
) -> None:
    assert _values(stored_and_pending.get_records_by_ids(PATH, [b"a", b"c"])) == [1, 3]


def test_get_records_with_column_value_returns_stored_and_pending(
    stored_and_pending: DeltaTableDatabase,
) -> None:
    db = stored_and_pending
    assert _values(db.get_records_with_column_value(PATH, {"g": 1})) == [2, 3]
    assert _values(db.get_records_with_column_value(PATH, {"g": 0})) == [1]


def test_reads_with_no_match_return_none(
    stored_and_pending: DeltaTableDatabase,
) -> None:
    db = stored_and_pending
    assert db.get_records_with_column_value(PATH, {"g": 9}) is None
    assert db.get_records_by_ids(PATH, [b"zz"]) is None
    assert db.get_record_by_id(PATH, b"zz") is None


@pytest.mark.parametrize(
    ("read", "expected_ids"),
    [
        (
            lambda db: db.get_all_records(PATH, record_id_column="rid"),
            [b"a", b"b", b"c"],
        ),
        (
            lambda db: db.get_records_by_ids(
                PATH, [b"a", b"c"], record_id_column="rid"
            ),
            [b"a", b"c"],
        ),
        (
            lambda db: db.get_records_with_column_value(
                PATH, [("g", 1)], record_id_column="rid"
            ),
            [b"b", b"c"],
        ),
    ],
    ids=["get_all_records", "get_records_by_ids", "get_records_with_column_value"],
)
def test_record_id_column_names_the_merged_ids(
    stored_and_pending: DeltaTableDatabase,
    read: Callable[[DeltaTableDatabase], pa.Table | None],
    expected_ids: list[bytes],
) -> None:
    """With ``record_id_column`` the ids of stored and pending rows come back under that name."""
    table = read(stored_and_pending)
    assert table is not None
    assert "__record_id" not in table.column_names
    assert sorted(cast(list[bytes], table.column("rid").to_pylist())) == expected_ids


@pytest.mark.parametrize("skip_duplicates", [False, True])
def test_pending_row_with_stored_id_reads_as_flush_keeps_it(
    tmp_path: Path, skip_duplicates: bool
) -> None:
    """A pending row whose id is already stored is never returned; the stored row is."""
    db = DeltaTableDatabase(tmp_path).at(SCOPE)
    db.add_records(PATH, _rows(["a"], [1]), flush=True)
    db.add_records(PATH, _rows(["a", "d"], [10, 4]), skip_duplicates=skip_duplicates)

    for when in ("before flush", "after flush"):
        assert _values(db.get_all_records(PATH)) == [1, 4], when
        assert _values(db.get_record_by_id(PATH, b"a")) == [1], when
        assert _values(db.get_records_by_ids(PATH, [b"a", b"d"])) == [1, 4], when
        assert _values(db.get_records_with_column_value(PATH, {"g": 0})) == [1, 4], when
        db.flush()


@pytest.fixture
def fail_id_refresh_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the next stored-ID refresh fail once, as a transient storage error would.

    ``DeltaTable.to_pyarrow_table`` is the backend call the stored-ID refresh makes; the
    read methods scan through ``to_pyarrow_dataset``, so only the refresh fails.
    """
    original = deltalake.DeltaTable.to_pyarrow_table
    calls = {"n": 0}

    def to_pyarrow_table(self: deltalake.DeltaTable) -> pa.Table:
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("transient storage error")
        return original(self)

    monkeypatch.setattr(deltalake.DeltaTable, "to_pyarrow_table", to_pyarrow_table)


@pytest.mark.usefixtures("fail_id_refresh_once")
@pytest.mark.parametrize("method", list(READS))
def test_failed_id_refresh_is_retried_by_the_next_read(
    tmp_path: Path, method: str
) -> None:
    """After a failed stored-ID refresh, a pending row with a stored id is still not returned."""
    db = DeltaTableDatabase(tmp_path).at(SCOPE)
    db.add_records(PATH, _rows(["b"], [2]), flush=True)
    db.add_records(PATH, _rows(["a", "b"], [1, 20]))

    with pytest.raises(OSError, match="transient storage error"):
        _ = db.get_all_records(PATH)

    assert _values(READS[method](db, PATH)) == EXPECTED[method]


@pytest.mark.usefixtures("fail_id_refresh_once")
def test_get_records_by_ids_handles_failed_id_refresh_like_a_failed_read(
    stored_and_pending: DeltaTableDatabase,
) -> None:
    """A failed stored-ID refresh follows the method's stored-read error behaviour."""
    db = stored_and_pending

    assert db.get_records_by_ids(PATH, [b"a", b"c"]) is None
    assert _values(db.get_records_by_ids(PATH, [b"a", b"c"])) == [1, 3]
