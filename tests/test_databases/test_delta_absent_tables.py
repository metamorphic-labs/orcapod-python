"""A ``DeltaTableDatabase`` remembers a table it found absent until its next flush (ITL-1173).

Until a table exists, a job's per-row lookups would each try to open it again. The family
of a database (every handle from ``at()``) keeps the record keys it found absent and
answers lookups for them without opening the table, until any flush of the family (a
commit) or a flush of that table. ``table_exists`` always checks again. A table another
process creates is therefore seen after this database's next flush, as rows another
process appends to an already-loaded table are; a flush to a table believed absent
loads it first and never replaces it.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

import deltalake
import pyarrow as pa
import pytest

from orcapod.databases import DeltaTableDatabase

PATH = ("table",)


def _rows(ids: list[str], values: list[int]) -> pa.Table:
    return pa.table(
        {
            "__record_id": pa.array([i.encode() for i in ids], type=pa.large_binary()),
            "v": pa.array(values, type=pa.int64()),
        }
    )


@pytest.fixture
def table_opens(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record every attempt to open a Delta table (its URI)."""
    opens: list[str] = []
    original = deltalake.DeltaTable.__init__

    def counting_init(self, table_uri, *args, **kwargs):
        opens.append(str(table_uri))
        original(self, table_uri, *args, **kwargs)

    monkeypatch.setattr(deltalake.DeltaTable, "__init__", counting_init)
    return opens


def _values(table: pa.Table | None) -> list[int]:
    return [] if table is None else sorted(cast(list[int], table["v"].to_pylist()))


def test_lookups_on_an_absent_table_open_it_once(
    tmp_path: Path, table_opens: list[str]
) -> None:
    db = DeltaTableDatabase(tmp_path)
    for i in range(50):
        assert db.get_record_by_id(PATH, f"r{i}".encode()) is None
        assert db.get_records_with_column_value(PATH, {"v": i}) is None
        assert db.get_all_records(PATH) is None
    assert len(table_opens) == 1


def test_lookups_on_an_absent_table_open_it_once_across_the_family(
    tmp_path: Path, table_opens: list[str]
) -> None:
    root = DeltaTableDatabase(tmp_path)
    scoped = root.at("scope")
    for i in range(20):
        assert scoped.get_record_by_id(PATH, f"r{i}".encode()) is None
        assert root.get_record_by_id(("scope", *PATH), f"r{i}".encode()) is None
    assert len(table_opens) == 1


def test_a_family_flush_forgets_absent_tables(
    tmp_path: Path, table_opens: list[str]
) -> None:
    db = DeltaTableDatabase(tmp_path)
    assert db.get_all_records(PATH) is None
    db.flush()
    assert db.get_all_records(PATH) is None
    assert len(table_opens) == 2


def test_create_after_miss_is_read_back(tmp_path: Path) -> None:
    """The family's own create replaces the absent entry, through any handle."""
    root = DeltaTableDatabase(tmp_path)
    scoped = root.at("scope")
    assert root.get_all_records(("scope", *PATH)) is None
    assert scoped.get_all_records(PATH) is None

    scoped.add_records(PATH, _rows(["a", "b"], [1, 2]), flush=True)

    assert _values(scoped.get_all_records(PATH)) == [1, 2]
    assert _values(root.get_all_records(("scope", *PATH))) == [1, 2]
    assert _values(root.get_record_by_id(("scope", *PATH), b"a")) == [1]


def test_pending_rows_of_an_absent_table_are_read(tmp_path: Path) -> None:
    db = DeltaTableDatabase(tmp_path)
    assert db.get_all_records(PATH) is None
    db.add_records(PATH, _rows(["a"], [1]))
    assert _values(db.get_record_by_id(PATH, b"a")) == [1]
    assert _values(db.get_all_records(PATH)) == [1]


def test_table_another_database_creates_is_seen_after_the_next_flush(
    tmp_path: Path,
) -> None:
    ours = DeltaTableDatabase(tmp_path)
    assert ours.get_all_records(PATH) is None

    other = DeltaTableDatabase(tmp_path)  # stands in for another process
    other.add_records(PATH, _rows(["x"], [10]), flush=True)

    # Remembered as absent until this database flushes.
    assert ours.get_all_records(PATH) is None
    ours.flush()
    assert _values(ours.get_all_records(PATH)) == [10]


def test_table_exists_checks_again(tmp_path: Path) -> None:
    ours = DeltaTableDatabase(tmp_path)
    assert not ours.table_exists(PATH)

    DeltaTableDatabase(tmp_path).add_records(PATH, _rows(["x"], [10]), flush=True)

    assert ours.table_exists(PATH)
    assert _values(ours.get_all_records(PATH)) == [10]


def test_flush_to_a_table_believed_absent_keeps_the_other_rows(
    tmp_path: Path,
) -> None:
    """A flush loads the table first: rows another database committed are kept."""
    ours = DeltaTableDatabase(tmp_path)
    assert ours.get_all_records(PATH) is None

    DeltaTableDatabase(tmp_path).add_records(
        PATH, _rows(["x", "shared"], [10, 11]), flush=True
    )

    ours.add_records(PATH, _rows(["a", "shared"], [1, 2]), skip_duplicates=True)
    ours.flush()

    stored = deltalake.DeltaTable(str(tmp_path / "table")).to_pyarrow_table()
    by_id = dict(
        zip(stored["__record_id"].to_pylist(), stored["v"].to_pylist(), strict=True)
    )
    assert by_id == {b"x": 10, b"shared": 11, b"a": 1}
    assert _values(ours.get_all_records(PATH)) == [1, 10, 11]
