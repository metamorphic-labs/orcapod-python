"""Binding several sources, and sources in another column arrangement (ITL-1236).

``PipelineJob.bind(sources=...)`` takes several sources in one call. Every source is
validated before anything changes, so one refused source leaves every binding as it was
(the ``bind`` docstring). A source whose columns are the slot's in another order (tag
order, table layout) is accepted, and the bind changes no node's hash or columns
(CLAUDE.md, "Important implementation details", ITL-1112).
"""

from __future__ import annotations

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import SourceSpecMismatchError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

from ..test_core.operators.several_items import (
    DATA_TAGS,
    LAYOUTS,
    MAIN,
    MIXED,
    PLAIN,
    TAGS_FIRST,
    Arrangement,
    P,
    make_source,
    presentation,
    rows_fingerprint,
)


@function_pod(output_keys="total")
def total(d1: int, p1: int) -> int:
    return d1 + p1


def _job(role: str) -> PipelineJob:
    """A job over slots ``s`` (``MAIN``) and ``p`` (``P``), joined by ``total``."""
    job = PipelineJob(name="bind", database=InMemoryArrowDatabase())
    with job:
        total.pod(
            make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, role)),
            make_source(P, Arrangement(P.tags, TAGS_FIRST, role)),
            label="total",
        )
    return job


def _slot_names(job: PipelineJob) -> tuple[str, str]:
    return ("s", "p") if {"s", "p"} <= set(job.nodes) else ("", "")


def _snapshot(job: PipelineJob) -> dict[str, object]:
    return {
        name: (node.pipeline_hash(), presentation(node))
        for name, node in job.nodes.items()
    }


@pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
@pytest.mark.parametrize("role", [PLAIN, DATA_TAGS, MIXED])
def test_several_sources_bound_in_one_call(role: str, orchestrator: type) -> None:
    job = _job(role)
    assert _slot_names(job) == ("s", "p")
    before = _snapshot(job)
    arrangement = Arrangement(MAIN.tags, TAGS_FIRST, role)
    # Both replacements hold other rows than the slots' sources, so the result shows
    # that both were bound.
    sources: dict[str, StreamProtocol] = {
        "s": make_source(MAIN, arrangement, source_id="s_new", rows=[0, 1]),
        "p": make_source(
            P, Arrangement(P.tags, TAGS_FIRST, role), source_id="p_new", rows=[1, 3]
        ),
    }
    job.bind(sources=sources)
    for name, source in sources.items():
        assert job.nodes[name].bound_source is source
    assert _snapshot(job) == before
    job.run(orchestrator=orchestrator())
    rows = sorted(
        (tag["t1"], tag["t2"], data["total"])
        for tag, data in job.nodes["total"].iter_data()
    )
    # MAIN's rows 0 and 1 are (b, 2, d1=10) and (a, 1, d1=20); P's rows 1 and 3 are
    # (b, 1, p1=8) and (b, 2, p1=6). Only (b, 2) matches: with all of P, (a, 1) would
    # match too.
    assert rows == [("b", 2, 16)]


@pytest.mark.parametrize("refused", ["s", "p"])
def test_one_refused_source_leaves_every_binding_as_it_was(refused: str) -> None:
    """Two sources in one call, one of them refused: neither is bound."""
    job = _job(PLAIN)
    bound_before = {name: job.nodes[name].bound_source for name in ("s", "p")}
    sources: dict[str, StreamProtocol] = {
        "s": make_source(MAIN, source_id="s_new"),
        "p": make_source(P, source_id="p_new"),
    }
    # The other slot's source does not fit: other column names.
    sources[refused] = make_source(P if refused == "s" else MAIN, source_id="wrong")
    with pytest.raises(SourceSpecMismatchError):
        job.bind(sources=sources)
    assert {name: job.nodes[name].bound_source for name in ("s", "p")} == bound_before


def _total_rows(job: PipelineJob) -> list[tuple[str, ...]]:
    return rows_fingerprint(list(job.nodes["total"].iter_data()), source_info=False)


def _total_values(job: PipelineJob) -> list[tuple[str, str]]:
    """The rows' tag and data values only: the bound source has another source id."""
    return sorted(
        (repr(sorted(tag.as_dict().items())), repr(sorted(data.as_dict().items())))
        for tag, data in job.nodes["total"].iter_data()
    )


def _bind_in(
    role: str,
    order: tuple[str, ...],
    layout: str,
    orchestrator: type = SyncPipelineOrchestrator,
) -> PipelineJob:
    """Bind ``MAIN`` in another arrangement to slot ``s``: nothing moves, all reads.

    Returns:
        The job, run on ``orchestrator``.
    """
    job = _job(role)
    before = _snapshot(job)
    # The columns of the table, not their order: CLAUDE.md promises the declared names,
    # roles and types; the column order within the table is a convenience only.
    table_before = sorted(job.nodes["s"].as_table().column_names)
    job.bind(
        sources={
            "s": make_source(MAIN, Arrangement(order, layout, role), source_id="s_new")
        }
    )
    assert _snapshot(job) == before, (order, layout)
    assert sorted(job.nodes["s"].as_table().column_names) == table_before, (
        order,
        layout,
    )
    # A mixed-role source placed its data-tag twice: the slot's table and head
    # repeated a column and lost another (ITL-1245, ITL-1420).
    head = job.nodes["s"].head(5).as_table().column_names
    assert len(set(head)) == len(head), (order, layout)
    job.run(orchestrator=orchestrator())
    return job


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("order", MAIN.tag_orders(), ids="-".join)
@pytest.mark.parametrize("role", [PLAIN, DATA_TAGS, MIXED])
def test_a_source_in_another_arrangement_is_bound_as_the_slot_declares(
    role: str, order: tuple[str, ...], layout: str
) -> None:
    """Both orchestrators: the same rows, with the values of the slot's own source."""
    reference = _job(role)
    reference.run()
    sync = _bind_in(role, order, layout, SyncPipelineOrchestrator)
    asynchronous = _bind_in(role, order, layout, AsyncPipelineOrchestrator)
    assert _total_rows(asynchronous) == _total_rows(sync)
    assert _total_values(sync) == _total_values(reference)
    assert len(_total_values(sync)) == 6
