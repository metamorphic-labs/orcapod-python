"""ITL-1347 stage 1: a successor's input reuses the table its operator node kept.

An operator node's ``execute`` returns its rows as ``RowsWithTable``, carrying the table
``_rows_to_table`` built for the output the node keeps. The sync orchestrator rebuilds a
successor's input with ``_materialize_upstream``, which now takes that table instead of
building it from the rows again. The governing rule of the spec
(``docs/dev/specs/itl-1347-job-table-transport.md``) is that nothing a run shows may change.
So every successor's input must equal, byte for byte and in identity, the input built from
the same rows as a plain list (the path before stage 1). The test checks that on every
edge of a job that uses every operator kind, over several tag orders, table layouts and tag
roles. These tests are about the orchestrator's internal hand-off, so they wrap it.
"""

from __future__ import annotations

import warnings
from collections.abc import Iterator
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators.static_output_pod import (
    RowsWithTable,
    StaticOutputOperatorPod,
)
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import ProvisionalHashWarning
from orcapod.operators import MergeJoin, Rejoin
from orcapod.pipeline import SyncPipelineOrchestrator
from tests.test_core.operators.several_items import (
    MAIN,
    P,
    Q,
    Arrangement,
    arrangements,
    make_source,
)

ALL_COLUMNS = {
    "meta": True,
    "source": True,
    "system_tags": True,
    "context": True,
    "content_hash": True,
}


@function_pod(output_keys="d1_doubled")
def doubled(d1: int) -> int:
    return 2 * d1


def build_job(arrangement: Arrangement) -> Any:
    """A job in which every operator kind feeds a successor."""
    main = make_source(MAIN, arrangement)
    partner_p = make_source(P)
    partner_q = make_source(Q)
    job = PipelineJob(name="kept_table_reuse", database=InMemoryArrowDatabase())
    with job:
        joined = main.join(partner_p, label="joined")
        mapped = joined.map_data({"p1": "p1_renamed"}, label="mapped")
        no_nulls = mapped.drop_nulls(["d2"], label="no_nulls")
        no_nulls.select_data_columns(["d1", "p1_renamed"], label="after_no_nulls")
        grouped = main.group_by(["t1"], label="grouped")
        grouped.select_data_columns(["d1"], label="after_grouped")
        batched = main.select_data_columns(["d1"]).batch(batch_size=2, label="batched")
        batched.select_data_columns(["d1"], label="after_batched")
        tagged = main.add_tags(k=1, label="tagged")
        doubled.pod(tagged.select_data_columns(["d1"]), label="doubled")
        semi = main.semi_join(partner_q, label="semi")
        semi.select_data_columns(["d3"], label="after_semi")
        merged = MergeJoin()(
            main.select_data_columns(["d1"]),
            partner_p.map_data({"p1": "d1"}).select_data_columns(["d1"]),
        )
        merged.select_data_columns(["d1"], label="after_merged")
        rejoined = Rejoin()(
            main.select_data_columns(["d1"]), main.select_data_columns(["d3"])
        )
        rejoined.select_data_columns(["d1"], label="after_rejoined")
        nothing = main.polars_filter(d1=999, label="nothing")
        nothing.select_data_columns(["d1"], label="after_nothing")
    return job


def _identity(stream: Any) -> tuple[Any, ...]:
    return (
        type(stream),
        stream.producer,
        tuple(id(u) for u in stream.upstreams),
        stream.pipeline_hash(),
        stream.content_hash(),
        stream.keys(),
        stream.output_schema(),
        stream.data_context_key,
    )


@pytest.fixture
def recorded_edges(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    """Wrap the orchestrator's input rebuild; check each input against the old path."""
    original = SyncPipelineOrchestrator._materialize_as_stream
    reused: list[str] = []

    def checked(buf: Any, upstream_node: Any) -> Any:
        built = original(buf, upstream_node)
        plain = StaticOutputOperatorPod._materialize_upstream(  # pyright: ignore[reportPrivateUsage]  # the path before stage 1
            list(buf), upstream_node
        )
        assert _identity(built) == _identity(plain)
        built_table = built.as_table(columns=ALL_COLUMNS).combine_chunks()
        plain_table = plain.as_table(columns=ALL_COLUMNS).combine_chunks()
        assert built_table.schema.equals(plain_table.schema, check_metadata=True)
        assert built_table.equals(plain_table, check_metadata=True)
        if isinstance(buf, RowsWithTable):
            reused.append(upstream_node.label)
        return built

    monkeypatch.setattr(
        SyncPipelineOrchestrator, "_materialize_as_stream", staticmethod(checked)
    )
    yield reused


@pytest.mark.parametrize("arrangement", arrangements(MAIN), ids=lambda a: a.id)
def test_every_successor_input_equals_the_rebuilt_one(
    arrangement: Arrangement, recorded_edges: list[str]
) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ProvisionalHashWarning)
        job = build_job(arrangement)
        job.run()
    # Every operator node with rows handed its kept table on; the one with no rows
    # (``nothing``) took the plain path, which gives its zero-row input as before.
    assert {
        "joined",
        "mapped",
        "no_nulls",
        "grouped",
        "batched",
        "tagged",
        "semi",
    } <= set(recorded_edges)
    assert "nothing" not in recorded_edges


def test_a_held_operator_node_takes_the_plain_path(
    recorded_edges: list[str],
) -> None:
    """A second run holds every node: buffers are plain lists, inputs as before."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ProvisionalHashWarning)
        job = build_job(arrangements(MAIN)[0])
        job.run()
        recorded_edges.clear()
        # Clear one function node so the second run has something to run, fed by a
        # held operator node.
        job.nodes["doubled"].clear_cache()
        job.run()
    assert recorded_edges == []


def test_execute_returns_rows_equal_to_the_plain_list() -> None:
    """The rows an operator node returns compare equal to the plain list of its rows."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ProvisionalHashWarning)
        job = build_job(arrangements(MAIN)[0])
        job.run()
    node = job.nodes["joined"]
    upstreams = [
        StaticOutputOperatorPod._materialize_upstream(list(u.iter_data()), u)  # pyright: ignore[reportPrivateUsage]  # the orchestrator's input form
        for u in node.upstreams
    ]
    rows = node.execute(*upstreams)
    assert isinstance(rows, RowsWithTable)
    assert rows == list(rows)
    table, tag_keys, data_tags = rows.row_table
    assert table.num_rows == len(rows)
    assert tag_keys == node.keys()[0]
    assert isinstance(data_tags, tuple)
