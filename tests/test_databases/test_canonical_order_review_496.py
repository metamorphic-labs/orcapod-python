"""Cases from the independent review of PR 496 (ITL-1370), each against main's behaviour.

Nothing a user can observe on main may differ except the canonical order of a new table
and the two presentation changes the developer approved. Each test pins one case the
review found; the reproducers are named in the PR.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

import orcapod.databases.utils as database_utils
from orcapod.core.data_function import CachedDataFunction, PythonDataFunction
from orcapod.core.datagrams import Data
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline.logging_observer import LoggingObserver
from orcapod.pipeline.status_observer import StatusObserver

PATH = ("t",)


def _rows(n: int, order: tuple[str, ...], z_type: Any) -> pa.Table:
    columns = {
        "z": pa.array([str(n)], pa.large_string()).cast(z_type),
        "rid": pa.array([bytes([n])], pa.large_binary()),
        "a": pa.array([n], pa.int64()),
    }
    return pa.table({name: columns[name] for name in order})


@pytest.mark.parametrize(
    ("database", "z_type"),
    [
        ("in-memory", pa.dictionary(pa.int32(), pa.string())),
        ("in-memory", pa.string_view()),
        ("Delta", pa.dictionary(pa.int32(), pa.string())),
        ("Delta", pa.string_view()),
    ],
    ids=["in-memory dictionary", "in-memory view", "Delta dictionary", "Delta view"],
)
def test_a_writer_that_keeps_its_order_matches_itself(
    tmp_path: Path, database: str, z_type: Any
) -> None:
    """Two batches with the same column order and a type the append comparison does
    not handle: both are written, as on main. Such rows are never sorted, so the
    second batch meets the first as given (review finding 2)."""
    db = (
        InMemoryArrowDatabase()
        if database == "in-memory"
        else DeltaTableDatabase(tmp_path)
    )
    order = ("z", "rid", "a")
    db.add_records(PATH, _rows(1, order, z_type), record_id_column="rid")
    db.add_records(PATH, _rows(2, order, z_type), record_id_column="rid")
    db.flush()
    table = db.get_all_records(PATH, record_id_column="rid")
    assert table is not None
    assert sorted(table.column("a").to_pylist()) == [1, 2]  # pyright: ignore[reportArgumentType]  # int64 column


def test_a_delta_table_main_created_with_a_merge_reserved_name_still_appends(
    tmp_path: Path,
) -> None:
    """A Delta table created before ITL-1370 in a non-canonical order, with a column
    named ``__delta_rs_source`` (which delta-rs's MERGE cannot handle): a batch in the
    stored order is appended by WRITE, as main does (review finding 3)."""

    def rows(n: int) -> pa.Table:
        return pa.table(
            {
                "z": [n],
                "rid": pa.array([bytes([n])], pa.large_binary()),
                "__delta_rs_source": [n + 10],
            }
        )

    with patch.object(
        database_utils,
        "canonical_column_order",
        lambda records, record_id: records,
        create=True,
    ):
        DeltaTableDatabase(tmp_path).add_records(
            PATH, rows(1), record_id_column="rid", flush=True
        )
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, rows(2), record_id_column="rid", flush=True)
    history = [entry["operation"] for entry in DeltaTable(tmp_path / "t").history()]
    assert history == ["WRITE", "WRITE"]
    table = db.get_all_records(PATH, record_id_column="rid")
    assert table is not None
    assert table.column_names == ["z", "rid", "__delta_rs_source"]
    assert sorted(table.column("z").to_pylist()) == [1, 2]  # pyright: ignore[reportArgumentType]  # int64 column


def _outputs(x: int) -> tuple[int, int]:
    return x + 1, x + 7


def test_a_cache_hit_presents_the_outputs_as_the_first_call_does() -> None:
    """A cached function declaring outputs ``z, a``: a hit gives the keys, values,
    source info and table columns in the order the miss gave them, sync and async,
    as main does (review finding 4)."""
    fn = CachedDataFunction(
        PythonDataFunction(_outputs, output_keys=["z", "a"]),  # pyright: ignore[reportArgumentType]  # a PythonDataFunction works at run time
        result_database=InMemoryArrowDatabase(),
    )

    def view(data: Any) -> tuple[Any, ...]:
        keys = tuple(data.keys())
        columns = data.as_table(columns={"source": True}).column_names
        return keys, tuple(data[k] for k in keys), tuple(columns)

    for x in (1, 2):
        item = Data({"x": x})
        miss, hit = fn.call(item), fn.call(item)
        assert miss is not None and hit is not None
        assert view(hit) == view(miss)
        assert view(hit)[0] == ("z", "a")

    async def both() -> tuple[Any, Any]:
        item = Data({"x": 3})
        return await fn.async_call(item), await fn.async_call(item)

    miss, hit = asyncio.run(both())
    assert miss is not None and hit is not None
    assert view(hit) == view(miss)


@pytest.mark.parametrize("kind", ["logs", "status"])
def test_an_observer_presents_the_tags_of_every_node_in_name_order(kind: str) -> None:
    """Node tables with different tags: the combined table lists the observer's own
    columns, then every tag by name, after the union (review finding 6)."""
    db = InMemoryArrowDatabase()
    name, id_column = (
        ("execution_logs", "_log_id")
        if kind == "logs"
        else ("execution_status", "_status_id")
    )
    for node, tag in [("node1", "z"), ("node2", "a")]:
        db.add_record(
            (node, name), node.encode(), pa.table({id_column: [node], tag: [1]})
        )
    db.flush()
    table = (
        LoggingObserver(log_database=db).get_logs()
        if kind == "logs"
        else StatusObserver(status_database=db).get_status()
    )
    assert table is not None
    assert table.column_names == [id_column, "a", "z"]


def test_an_external_empty_delta_table_presents_its_order_before_and_after_a_flush(
    tmp_path: Path,
) -> None:
    """An existing zero-row Delta table in the order ``z, rid, A``: rows queued in
    that order read in that order before and after the flush, as on main (review
    finding 7)."""
    items = pa.table(
        {
            "z": [1, 2],
            "rid": pa.array([b"\x01", b"\x02"], pa.large_binary()),
            "A": [10, 20],
        }
    )
    write_deltalake(
        str(tmp_path / "t"), items.rename_columns(["z", "__record_id", "A"]).slice(0, 0)
    )
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, items, record_id_column="rid")
    pending = db.get_all_records(PATH, record_id_column="rid")
    db.flush()
    committed = db.get_all_records(PATH, record_id_column="rid")
    assert pending is not None and committed is not None
    assert pending.column_names == committed.column_names == ["z", "rid", "A"]


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "ITL-1377: on a canonical Delta table a batch commits by plain append, as an "
        "ordinary writer's batch does on main, so a competing writer's record id is "
        "stored twice; ITL-1377 makes every Delta append conflict-safe (developer, "
        "2026-11-22). PR 496 must not merge before ITL-1377's fix."
    ),
)
@pytest.mark.parametrize("skip_duplicates", [True, False])
def test_a_competing_record_id_on_a_canonical_delta_table_keeps_one_row(
    tmp_path: Path, skip_duplicates: bool
) -> None:
    def rows(n: int, value: int, order: tuple[str, ...]) -> pa.Table:
        table = pa.table(
            {
                "rid": pa.array([bytes([n])], pa.large_binary()),
                "a": [value],
                "z": [100 + n],
            }
        )
        return table.select(list(order))

    one, two = DeltaTableDatabase(tmp_path), DeltaTableDatabase(tmp_path)
    one.add_records(
        PATH, rows(1, 10, ("rid", "a", "z")), record_id_column="rid", flush=True
    )
    one.add_records(
        PATH,
        rows(2, 20, ("z", "rid", "a")),
        record_id_column="rid",
        skip_duplicates=skip_duplicates,
    )
    snapshot = one._snapshot_for_commit

    def interleave(*args: Any) -> Any:
        taken = snapshot(*args)
        two.add_records(
            PATH,
            rows(2, 99, ("rid", "a", "z")),
            record_id_column="rid",
            skip_duplicates=skip_duplicates,
            flush=True,
        )
        return taken

    with patch.object(one, "_snapshot_for_commit", side_effect=interleave):
        try:
            one.flush()
        except RuntimeError:
            pass
    table = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="rid")
    assert table is not None
    assert sorted(table.column("rid").to_pylist()) == [bytes([1]), bytes([2])]  # pyright: ignore[reportArgumentType]  # binary column
