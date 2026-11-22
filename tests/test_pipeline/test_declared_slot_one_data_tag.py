"""A declared slot over a narrower source with one data-tag among two tags (ITL-1366, e10).

A source slot declared from a source whose ``x`` is ``int | None``, bound to the same
table with ``x: int`` (a narrower source, given as the declared schema), feeding an
operator. On ``main`` the run raises ``ValueError`` ("Duplicate column name") where the
source's rows are rebuilt for its successor, when exactly one of the two tags is a
data-tag and the tags are declared ``j, k``. On the sync path the branch hands on the
declared table itself (F over ``_DeclaredForm.apply`` of the bound table), and the run
completes with the rows the bind describes (spec row e10; provisional, coordinator
decision 2026-11-22, pending the developer). The async path still carries rows until
ITL-1347 stage 5 and raises as ``main`` does.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource

ROWS = [
    {"j": 3, "k": 5, "x": 1, "y": 7.5},
    {"j": 4, "k": 6, "x": 2, "y": 8.5},
]

ROLES: dict[str, bool | tuple[str, ...]] = {
    "no_data_tag": True,
    "k_data_tag": ("j",),
    "j_data_tag": ("k",),
    "both_data_tags": (),
}


def _source(nullable_x: bool, order: tuple[str, str], roles: Any) -> ArrowTableSource:
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


def _bound_job(order: tuple[str, str], roles: Any) -> PipelineJob:
    """The slot declared with ``x: int | None``, bound to the table with ``x: int``."""
    job = PipelineJob(name="declared_one_data_tag", database=InMemoryArrowDatabase())
    with job:
        _source(True, order, roles).select_data_columns(["y", "x"], label="selected")
    slot = next(label for label in job.nodes if label != "selected")
    job.bind(sources={slot: _source(False, order, roles)})
    return job


def _rows(job: PipelineJob) -> list[dict[str, Any]]:
    table = job.nodes["selected"].as_table()
    return sorted(table.to_pylist(), key=lambda row: row["j"])


@pytest.mark.parametrize("roles", list(ROLES), ids=list(ROLES))
@pytest.mark.parametrize("order", [("j", "k"), ("k", "j")], ids=["j-k", "k-j"])
def test_the_sync_run_completes_with_the_rows_the_bind_describes(
    order: tuple[str, str], roles: str
) -> None:
    """Every arrangement runs on the sync path and gives the bound table's rows."""
    job = _bound_job(order, ROLES[roles])
    job.run()
    assert _rows(job) == ROWS


@pytest.mark.parametrize("roles", list(ROLES), ids=list(ROLES))
@pytest.mark.parametrize("order", [("j", "k"), ("k", "j")], ids=["j-k", "k-j"])
def test_the_async_run_is_as_on_main(order: tuple[str, str], roles: str) -> None:
    """The async path carries rows until stage 5: it raises where ``main`` raises."""
    job = _bound_job(order, ROLES[roles])
    raises_on_main = order == ("j", "k") and roles in ("k_data_tag", "j_data_tag")
    if raises_on_main:
        with pytest.raises(ValueError, match="Duplicate column name"):
            job.run(orchestrator=AsyncPipelineOrchestrator())
    else:
        job.run(orchestrator=AsyncPipelineOrchestrator())
        assert _rows(job) == ROWS
