"""A slot bound to a narrower source presents each column once (ITL-1420).

A source slot declared from a table whose ``x`` is ``int | None``, bound to the same
table with ``x: int`` (a narrower source: its rows are given in the declared form). With
tags declared ``j, k`` and exactly one of them a data-tag, the declared form placed the
data-tag both with the tags and with the data, so one column came out twice and another
was lost: the slot's ``as_table()`` and ``head()`` raised and its typed zero-row table
(what an operator's description is derived from) had the duplicate. A job's run read
the rows another way and was not affected.

Every arrangement is checked: two tags and two data columns, no data-tag, either one
or both, in both tag orders; the slot's table, head and typed zero-row table, and a job
run on each orchestrator.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.core.streams.base import typed_zero_row_table
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource

ROWS = [
    {"j": 3, "k": 5, "x": 1, "y": 7.5},
    {"j": 4, "k": 6, "x": 2, "y": 8.5},
]

# ``tag_only_columns``: which tags are tags only (the others are data-tags too).
ROLES: dict[str, bool | tuple[str, ...]] = {
    "no_data_tag": True,
    "k_data_tag": ("j",),
    "j_data_tag": ("k",),
    "both_data_tags": (),
}
ORDERS = [("j", "k"), ("k", "j")]


def _source(nullable_x: bool, order: tuple[str, str], roles: Any) -> ArrowTableSource:  # pyright: ignore[reportExplicitAny]  # a ROLES value
    """The table ``x, k, j, y`` (not laid out in tag order), ``x`` nullable or not."""
    table = pa.table(
        {
            "x": pa.array([1, 2], pa.int64()),
            "k": pa.array([5, 6], pa.int64()),
            "j": pa.array([3, 4], pa.int64()),
            "y": pa.array([7.5, 8.5], pa.float64()),
        }
    )
    table = table.cast(
        pa.schema([f.with_nullable(f.name == "x" and nullable_x) for f in table.schema])
    )
    return ArrowTableSource(
        table, tag_columns=list(order), tag_only_columns=roles, source_id="s"
    )


def _bound_job(order: tuple[str, str], roles: Any) -> PipelineJob:  # pyright: ignore[reportExplicitAny]  # a ROLES value
    """The slot declared with ``x: int | None``, bound to the table with ``x: int``."""
    job = PipelineJob(name="declared_data_tags", database=InMemoryArrowDatabase())
    with job:
        _source(True, order, roles).select_data_columns(["y", "x"], label="selected")
    slot = next(label for label in job.nodes if label != "selected")
    job.bind(sources={slot: _source(False, order, roles)})
    return job


def _slot(job: PipelineJob) -> Any:  # pyright: ignore[reportExplicitAny]  # a job node
    return job.nodes[next(label for label in job.nodes if label != "selected")]


def _once(table: pa.Table) -> None:
    names = table.column_names
    assert len(set(names)) == len(names), names
    assert {"j", "k", "x", "y"} <= set(names)


@pytest.mark.parametrize("roles", list(ROLES), ids=list(ROLES))
@pytest.mark.parametrize("order", ORDERS, ids=["j-k", "k-j"])
def test_the_slot_presents_each_column_once(order: tuple[str, str], roles: str) -> None:
    """The slot's table, head and typed zero-row table hold every column once, the
    table and head hold the rows, and the zero-row table has the table's columns."""
    slot = _slot(_bound_job(order, ROLES[roles]))
    full = slot.as_table(columns={"system_tags": True, "source": True})
    head = slot.head(5).as_table()
    zero = typed_zero_row_table(slot)
    assert zero is not None
    for table in (full, head, zero):
        _once(table)
    assert set(full.column_names) <= set(zero.column_names)
    for table in (full, head):
        rows = table.select(["j", "k", "x", "y"]).to_pylist()
        assert sorted(rows, key=lambda row: row["j"]) == ROWS


@pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
@pytest.mark.parametrize("roles", list(ROLES), ids=list(ROLES))
@pytest.mark.parametrize("order", ORDERS, ids=["j-k", "k-j"])
def test_a_run_gives_the_bound_rows(
    order: tuple[str, str], roles: str, orchestrator: type
) -> None:
    """A job over the slot gives the bound table's rows, on either orchestrator."""
    job = _bound_job(order, ROLES[roles])
    job.run(orchestrator=orchestrator())
    table = job.nodes["selected"].as_table()
    assert sorted(table.to_pylist(), key=lambda row: row["j"]) == ROWS
