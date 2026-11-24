"""Column names deltalake's MERGE reserves are refused on Delta when rows are added.

Every commit to an existing Delta table is a MERGE (ITL-1377). deltalake 1.6.6's MERGE
planner names columns of its own (``crates/core/src/operations/merge/mod.rs``); a table
holding a top-level column with one of those names takes its first write, and every MERGE
into it then fails, so its rows would stay queued forever (review of PR 511). Like a
backtick in a name (ITL-1375), such a name is refused for certain: when rows are added,
and for a table that already holds one.

The names are provoked with deltalake itself, for the names ``CANDIDATES`` enumerates
only: an upgrade that drops or renames one of them, or starts reserving another of them,
fails this file. A name an upgrade newly reserves outside that list is not detected here;
``CANDIDATES`` is read from deltalake 1.6.6's merge planner and is to be read again on an
upgrade.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import cast

import deltalake
import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase
from orcapod.databases.delta_lake_databases import MERGE_RESERVED_COLUMNS
from orcapod.errors import InputValidationError, UnstorableColumnError

PATH = ("t",)

# Every ``__delta_rs_*`` name deltalake 1.6.6's merge planner writes, and names near
# them: the ones a MERGE refuses must be exactly ``MERGE_RESERVED_COLUMNS``. Only these
# names are probed (review of PR 511).
CANDIDATES = sorted(
    MERGE_RESERVED_COLUMNS
    | {
        "__delta_rs_target_row_ordinal_in_file",
        "__delta_rs_match_cardinality_class",
        "__delta_rs_update_expanded",
        "__delta_rs_path",
        "__delta_rs_c_x",
        "__delta_rs_target_c_x",
        "_change_type",
    }
)


def _merge_fails(root: Path, name: str) -> bool:
    deltalake.write_deltalake(str(root), pa.table({"id": [1], name: [1]}))
    try:
        (
            deltalake.DeltaTable(str(root))
            .merge(
                pa.table({"id": [2], name: [2]}),
                "t.id = s.id",
                source_alias="s",
                target_alias="t",
                merge_schema=True,
                streamed_exec=False,
            )
            .when_not_matched_insert_all()
            .execute()
        )
    except Exception:  # noqa: BLE001  # the refusal is the subject
        return True
    return False


@pytest.mark.parametrize("name", CANDIDATES)
def test_deltalake_merge_refuses_exactly_the_reserved_names(
    tmp_path: Path, name: str
) -> None:
    assert _merge_fails(tmp_path / "t", name) == (name in MERGE_RESERVED_COLUMNS)


def _rows(rid: bytes, name: str) -> pa.Table:
    return pa.table({"rid": pa.array([rid], pa.large_binary()), name: [1]})


@pytest.mark.parametrize("name", sorted(MERGE_RESERVED_COLUMNS))
def test_a_reserved_name_is_refused_when_rows_are_added(
    tmp_path: Path, name: str
) -> None:
    db = DeltaTableDatabase(tmp_path)
    with pytest.raises(UnstorableColumnError, match="MERGE reserves") as info:
        db.add_records(PATH, _rows(b"1", name), record_id_column="rid", flush=True)
    assert info.value.column == name
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
    assert not (tmp_path / "t" / "_delta_log").exists()


@pytest.mark.parametrize("with_column", [True, False], ids=["holding it", "without it"])
def test_a_stored_table_holding_a_reserved_name_takes_no_append(
    tmp_path: Path, with_column: bool
) -> None:
    """A table written before this rule: an append is refused when added, naming the
    column, with nothing queued, never left to fail at every flush."""
    name = "__delta_rs_source"
    deltalake.write_deltalake(
        str(tmp_path / "t"),
        pa.table(
            {"__record_id": pa.array([b"1"], pa.large_binary()), "v": [1], name: [1]}
        ),
    )
    db = DeltaTableDatabase(tmp_path)
    rows = pa.table({"rid": pa.array([b"2"], pa.large_binary()), "v": [2]})
    if with_column:
        rows = rows.append_column(name, pa.array([2]))
    with pytest.raises(
        (InputValidationError, UnstorableColumnError), match="MERGE reserves"
    ):
        db.add_records(PATH, rows, record_id_column="rid")
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
    db.flush()  # a no-op


@function_pod(output_keys=["__delta_rs_source"])
def _reserved(v: int) -> int:
    return v


@pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
def test_a_job_writing_a_reserved_name_stops_at_its_first_write(
    tmp_path: Path, use_async: bool
) -> None:
    source = ArrowTableSource(
        pa.Table.from_arrays(
            [pa.array([1, 2], pa.int64()), pa.array([1, 2], pa.int64())],
            schema=pa.schema(
                [
                    pa.field("k", pa.int64(), nullable=False),
                    pa.field("v", pa.int64(), nullable=False),
                ]
            ),
        ),
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="s",
        record_id_columns=["k"],
    )
    with pytest.raises(UnstorableColumnError, match="MERGE reserves"):
        database = DeltaTableDatabase(tmp_path)
        job = PipelineJob(name="itl1377", database=database)
        with job:
            _reserved.pod(source, label="fn")
        if use_async:
            asyncio.run(job.run_async())
        else:
            job.run()


def test_a_nested_field_with_a_reserved_name_is_kept(tmp_path: Path) -> None:
    """Only a top-level column collides with the planner's columns."""
    db = DeltaTableDatabase(tmp_path)
    for rid in (b"1", b"2"):
        db.add_records(
            PATH,
            pa.table(
                {
                    "rid": pa.array([rid], pa.large_binary()),
                    "s": pa.array([{"__delta_rs_source": 1}]),
                }
            ),
            record_id_column="rid",
            flush=True,
        )
    stored = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="rid")
    assert stored is not None and stored.num_rows == 2


@pytest.mark.parametrize("rebuilt", [True, False], ids=["rebuilt", "unchanged"])
@pytest.mark.parametrize("name", ["a`b", "__delta_rs_source"])
def test_a_cached_table_holding_such_a_name_is_checked_again_when_rebuilt(
    tmp_path: Path, name: str, rebuilt: bool
) -> None:
    """Review of PR 511: the database cached a handle of a table holding such a name;
    the table is rebuilt without it. The refusal is checked again against the version
    stored now: the rows are stored. Unchanged, they are still refused, nothing
    queued."""
    root = str(tmp_path / "t")
    deltalake.write_deltalake(
        root,
        pa.table(
            {"__record_id": pa.array([b"1"], pa.large_binary()), "v": [1], name: [1]}
        ),
    )
    db = DeltaTableDatabase(tmp_path)
    db.get_all_records(PATH)  # the handle is cached
    assert db._delta_table_cache  # pyright: ignore[reportPrivateUsage]
    rows = pa.table({"rid": pa.array([b"2", b"3"], pa.large_binary()), "v": [2, 3]})
    if not rebuilt:
        with pytest.raises(InputValidationError, match="Delta|MERGE|backtick"):
            db.add_records(PATH, rows, record_id_column="rid")
        assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
        return
    current = deltalake.DeltaTable(root).to_pyarrow_table().drop_columns([name])
    deltalake.write_deltalake(root, current, mode="overwrite", schema_mode="overwrite")
    db.add_records(PATH, rows, record_id_column="rid", flush=True)
    stored = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="rid")
    assert stored is not None
    assert sorted(cast(list[bytes], stored.column("rid").to_pylist())) == [
        b"1",
        b"2",
        b"3",
    ]
