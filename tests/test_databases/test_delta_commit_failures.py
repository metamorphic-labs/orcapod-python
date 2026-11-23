"""How a failed Delta commit is classified at the flush (ITL-1403).

The flush is the point of truth: a commit deltalake refuses for the rows themselves
would be refused the same way at every later flush, so it is raised once and the batch
leaves the queue; anything else keeps its rows queued, so nothing is dropped on a guess.

Each permanent form is provoked here with deltalake itself, so an upgrade that changes
an exception type or one of ``PERMANENT_DELTA_ERROR_PREFIXES`` fails this file.
"""

from __future__ import annotations

import os
import stat
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import deltalake
import pyarrow as pa
import pytest

from orcapod.databases import DeltaTableDatabase
from orcapod.databases import delta_lake_databases as dld
from orcapod.errors import InputValidationError

PATH = ("t",)


def _raised(action: Callable[[], Any]) -> BaseException:
    try:
        action()
    except Exception as error:  # noqa: BLE001  # the error is the subject
        return error
    raise AssertionError("deltalake did not refuse")


def _merge(table: deltalake.DeltaTable, rows: pa.Table, evolve: bool = True) -> Any:
    return (
        table.merge(
            rows,
            "t.id = s.id",
            source_alias="s",
            target_alias="t",
            merge_schema=evolve,
        )
        .when_not_matched_insert_all()
        .execute()
    )


def _metadata_change(root: Path) -> BaseException:
    def table(value: str) -> pa.Table:
        field = pa.field("x", pa.int64(), metadata={"k": value})
        return pa.Table.from_arrays(
            [pa.array([1]), pa.array([1])],
            schema=pa.schema([pa.field("id", pa.int64()), field]),
        )

    deltalake.write_deltalake(str(root), table("1"))
    return _raised(lambda: _merge(deltalake.DeltaTable(str(root)), table("2")))


def _type_coercion(root: Path) -> BaseException:
    deltalake.write_deltalake(
        str(root), pa.table({"id": [1], "s": pa.array([{"x": 1}])})
    )
    grown = pa.table({"id": [2], "s": pa.array([{"x": 2, "y": 3}])})
    return _raised(lambda: _merge(deltalake.DeltaTable(str(root)), grown, evolve=False))


def _invalid_data(root: Path) -> BaseException:
    schema = pa.schema(
        [pa.field("id", pa.int64()), pa.field("v", pa.int64(), nullable=False)]
    )
    deltalake.write_deltalake(
        str(root), pa.Table.from_arrays([pa.array([1]), pa.array([1])], schema=schema)
    )
    nulls = pa.table({"id": [2], "v": pa.array([None], pa.int64())})
    return _raised(lambda: _merge(deltalake.DeltaTable(str(root)), nulls))


def _unsupported_cast(root: Path) -> BaseException:
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("s", pa.struct([pa.field("a", pa.int64(), nullable=False)])),
        ]
    )
    deltalake.write_deltalake(
        str(root),
        pa.Table.from_arrays(
            [pa.array([1]), pa.array([{"a": 1}], schema.field("s").type)], schema=schema
        ),
    )
    loosened = pa.table(
        {"id": [2], "s": pa.array([{"a": None}], pa.struct([("a", pa.int64())]))}
    )
    return _raised(
        lambda: _merge(deltalake.DeltaTable(str(root)), loosened, evolve=False)
    )


def _duplicate_name(root: Path) -> BaseException:
    return _raised(
        lambda: deltalake.write_deltalake(
            str(root), pa.table({"id": [1], "a": [1], "A": [2]})
        )
    )


PERMANENT: dict[str, Callable[[Path], BaseException]] = {
    "SchemaMismatchError (field metadata)": _metadata_change,
    "DeltaError 'type_coercion' (DataFusion cannot insert)": _type_coercion,
    "DeltaError 'Invalid data found' (a null in a non-nullable column)": _invalid_data,
    "DeltaError 'Unsupported CAST' (a nested field loosened, no evolution)": _unsupported_cast,
    "Exception 'Duplicate field name' (case-insensitive names)": _duplicate_name,
}


@pytest.mark.parametrize("case", sorted(PERMANENT))
def test_each_permanent_form_of_deltalake_is_classified_permanent(
    tmp_path: Path, case: str
) -> None:
    error = PERMANENT[case](tmp_path / "t")
    assert dld._permanent_commit_failure(error), (type(error), str(error))  # pyright: ignore[reportPrivateUsage]


def test_the_prefixes_are_the_documented_three() -> None:
    assert dld.PERMANENT_DELTA_ERROR_PREFIXES == (
        "Generic DeltaTable error: type_coercion",
        "Generic DeltaTable error: External error: Invalid data found",
        "Generic DeltaTable error: This feature is not implemented: Unsupported CAST",
    )


def test_a_concurrent_commit_is_not_permanent(tmp_path: Path) -> None:
    from deltalake.exceptions import CommitFailedError

    root = str(tmp_path / "t")
    deltalake.write_deltalake(root, pa.table({"id": [1]}))
    stale = deltalake.DeltaTable(root)
    _merge(deltalake.DeltaTable(root), pa.table({"id": [2]}))
    error = _raised(lambda: _merge(stale, pa.table({"id": [3]})))
    assert isinstance(error, CommitFailedError)
    assert not dld._permanent_commit_failure(error)  # pyright: ignore[reportPrivateUsage]


@pytest.mark.skipif(sys.platform == "win32" or os.geteuid() == 0, reason="permissions")
def test_a_storage_failure_keeps_the_rows_queued(tmp_path: Path) -> None:
    """A failure that may pass (here an unwritable table directory) is not dropped:
    the rows stay queued, and a later flush, once the store is writable, commits them."""
    db = DeltaTableDatabase(tmp_path)
    rows = lambda rid: pa.table({"rid": pa.array([rid], pa.large_binary()), "v": [1]})  # noqa: E731
    db.add_records(PATH, rows(b"1"), record_id_column="rid", flush=True)
    db.add_records(PATH, rows(b"2"), record_id_column="rid", skip_duplicates=True)
    table_dir = tmp_path / "t"
    os.chmod(table_dir, stat.S_IRUSR | stat.S_IXUSR)
    try:
        with pytest.raises(Exception) as info:  # noqa: B017  # main's error, unchanged
            db.flush()
        assert not isinstance(info.value, InputValidationError)
        assert db._pending_batches  # pyright: ignore[reportPrivateUsage]
    finally:
        os.chmod(table_dir, stat.S_IRWXU)
    db.flush()
    stored = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="rid")
    assert stored is not None
    assert sorted(cast(list[bytes], stored.column("rid").to_pylist())) == [b"1", b"2"]


def test_a_type_coercion_refusal_is_raised_once_through_orcapod(tmp_path: Path) -> None:
    """A struct gaining a field with schema evolution off: accepted when added (nothing
    Orcapod can tell for certain), refused by Delta at the commit, raised once."""
    db = DeltaTableDatabase(tmp_path, allow_schema_evolution=False)
    rows = lambda rid, value: pa.table(  # noqa: E731
        {"rid": pa.array([rid], pa.large_binary()), "s": pa.array([value])}
    )
    db.add_records(PATH, rows(b"1", {"x": 1}), record_id_column="rid", flush=True)
    db.add_records(PATH, rows(b"2", {"x": 2, "y": 3}), record_id_column="rid")
    with pytest.raises(InputValidationError, match="type_coercion") as info:
        db.flush()
    assert "1 rows queued for it were not written" in str(info.value)
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
    db.flush()  # a no-op
