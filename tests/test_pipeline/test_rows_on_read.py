"""Outputs held as rows, built where a successor reads them (ITL-1347; Codex on PR #485).

On every route where building a node's output from its whole table is not provably
``main``'s result, the node hands on its rows (``RowsOnRead``), built by ``main``'s row
path where the sync orchestrator gives them to a successor (``at_edge``). So the
order, the error and the moment it surfaces are ``main``'s: a narrower bind, a source
whose whole-table build is unavailable (its rows read once), a held node. A leaf
nobody reads is never built.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.operators import Batch
from orcapod.pipeline.dag import OrcaDAG
from orcapod.pipeline.observer import NoOpObserver
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource, DictSource
from orcapod.types import CacheMode
from tests.test_core.operators.test_row_built_form import (
    _NESTED,  # pyright: ignore[reportPrivateUsage]  # the shared provenance fixtures
    _NodeEvents,  # pyright: ignore[reportPrivateUsage]
    _source_table,  # pyright: ignore[reportPrivateUsage]
    _with_source_x,  # pyright: ignore[reportPrivateUsage]
)

_UNCONVERTIBLE = pa.large_list(_NESTED)


def _unconvertible() -> ArrowTableStream:
    """Tokens ``[None, [[]]]``: the whole-table build is unavailable, the rows raise."""
    return _with_source_x([[None, [[]]], [None, [[]]]], _UNCONVERTIBLE)


def _plain() -> DictSource:
    return DictSource(
        [{"k": 1, "x": 1}, {"k": 2, "x": 2}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="s",
    )


def test_a_fallback_source_reads_its_rows_once() -> None:
    """A rebind after the source node ended does not change what it handed on.

    Its rows were read once, when it ran: the successor's edge raises the original
    rows' ``ArrowTypeError``, as on ``main``.
    """
    job = PipelineJob(name="read_once", database=InMemoryArrowDatabase())
    with job:
        Batch(batch_size=2)(_plain().select_data_columns(["x"]), label="batched")
    job.bind(sources={"s": _unconvertible()})
    node = job.nodes["s"]

    class RebindsAtEnd(NoOpObserver):
        def contextualize(self, *identity_path: str) -> RebindsAtEnd:
            return self

        def on_node_end(self, node_label: str, node_hash: str) -> None:
            if node_label == "s":
                node.bound_source = _plain()

    with pytest.raises(pa.ArrowTypeError, match="Expected bytes"):
        job.run(observer=RebindsAtEnd())


def test_a_fallback_source_nobody_reads_completes() -> None:
    """A source whose rows cannot be built, as a leaf of a direct sync run: it completes.

    ``main`` never rebuilt a leaf's rows; neither does the branch (``RowsOnRead``).
    """
    job = PipelineJob(name="leaf", database=InMemoryArrowDatabase())
    with job:
        _plain().select_data_columns(["x"], label="selected")
    job.bind(sources={"s": _unconvertible()})
    dag: OrcaDAG[Any] = OrcaDAG()
    dag.add_node(job.nodes["s"])
    observer = _NodeEvents()
    SyncPipelineOrchestrator().run(dag, observer=observer, materialize_results=False)
    assert observer.events == [("start", "s"), ("end", "s")]


def test_a_narrower_raw_bind_keeps_the_slots_system_tag_order() -> None:
    """A raw stream whose system tags are record id then source id, bound narrower.

    ``main`` gives each row the slot's order (source id, then record id); so do the
    kept and the raw stored tables (Codex on PR #485, round 3).
    """
    from orcapod.system_constants import constants

    table = _source_table()
    source_id = next(n for n in table.column_names if "source_id" in n)
    record_id = next(n for n in table.column_names if "record_id" in n)
    others = [n for n in table.column_names if n not in (source_id, record_id)]
    reversed_pair = table.select([*others, record_id, source_id])
    raw = ArrowTableStream(reversed_pair, tag_columns=["k"])

    database = InMemoryArrowDatabase()
    job = PipelineJob(name="reversed_pair", database=database)
    with job:
        declared = DictSource(
            [{"k": 1, "x": 1}, {"k": 2, "x": None}],
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="s",
        )
        Batch(batch_size=2)(declared, label="batched")
    job.nodes["batched"].attach_databases(
        pipeline_database=database.at("log"), cache_mode=CacheMode.LOG
    )
    job.bind(sources={"s": raw})
    job.run()
    node = job.nodes["batched"]
    kept = node.as_table(columns={"system_tags": True}).column_names
    system_tags = [n for n in kept if n.startswith(constants.SYSTEM_TAG_PREFIX)]
    assert ["source_id" in n for n in system_tags] == [True, False]
    stored = node.get_all_records(columns={"system_tags": True})
    assert stored is not None
    stored_tags = [
        n for n in stored.column_names if n.startswith(constants.SYSTEM_TAG_PREFIX)
    ]
    assert ["source_id" in n for n in stored_tags] == [True, False]


def test_a_narrower_bind_is_described_from_its_declared_schema() -> None:
    """A narrower bind's description never places columns: its types are undeclared.

    With one data-tag among two tags (ITL-1366's input), the description does not
    raise; ``arrow_types_declared`` is false (Codex on PR #485, round 3).
    """
    table = pa.table(
        {
            "x": pa.array([1, 2], pa.int64()),
            "k": pa.array([5, 6], pa.int64()),
            "j": pa.array([3, 4], pa.int64()),
            "y": pa.array([7.5, 8.5], pa.float64()),
        }
    )

    def source(nullable_x: bool) -> ArrowTableSource:
        fields = [f.with_nullable(f.name == "x" and nullable_x) for f in table.schema]
        return ArrowTableSource(
            table.cast(pa.schema(fields)),
            tag_columns=["j", "k"],
            tag_only_columns=("j",),
            source_id="s",
        )

    job = PipelineJob(name="narrower_description", database=InMemoryArrowDatabase())
    with job:
        source(True).select_data_columns(["y", "x"], label="selected")
    slot = next(label for label in job.nodes if label != "selected")
    job.bind(sources={slot: source(False)})
    node = job.nodes[slot]
    schema = node.arrow_schema()
    assert not node.arrow_types_declared
    assert set(node.keys()[0]) <= set(schema.names)


def test_a_narrower_bind_channel_raises_as_its_rows_do() -> None:
    """The source node's channel carries its rows: ITL-1366's input raises as on main.

    ``open_channel()`` reads the node's output, the rows built when read, which raise
    the duplicate-column error ``main``'s row path raises.
    """
    import asyncio

    table = pa.table(
        {
            "x": pa.array([1, 2], pa.int64()),
            "k": pa.array([5, 6], pa.int64()),
            "j": pa.array([3, 4], pa.int64()),
            "y": pa.array([7.5, 8.5], pa.float64()),
        }
    )

    def source(nullable_x: bool) -> ArrowTableSource:
        fields = [f.with_nullable(f.name == "x" and nullable_x) for f in table.schema]
        return ArrowTableSource(
            table.cast(pa.schema(fields)),
            tag_columns=["j", "k"],
            tag_only_columns=("j",),
            source_id="s",
        )

    job = PipelineJob(name="narrower_channel", database=InMemoryArrowDatabase())
    with job:
        source(True).select_data_columns(["y", "x"], label="selected")
    slot = next(label for label in job.nodes if label != "selected")
    job.bind(sources={slot: source(False)})
    node: Any = job.nodes[slot]
    with pytest.raises(ValueError, match="Duplicate column name"):
        asyncio.run(node.open_channel().collect())


def _replay_job(database: InMemoryArrowDatabase, *, with_successor: bool) -> Any:
    """``source -> select x`` with the select node replaying from ``database``."""
    job = PipelineJob(name="replay_odd", database=InMemoryArrowDatabase())
    with job:
        selected = _plain().select_data_columns(["x"], label="selected")
        if with_successor:
            Batch(batch_size=2)(selected, label="batched")
    job.nodes["selected"].attach_databases(
        pipeline_database=database, cache_mode=CacheMode.REPLAY
    )
    return job


@pytest.mark.parametrize("with_successor", [False, True], ids=["leaf", "successor"])
def test_replay_of_a_table_the_whole_table_build_cannot_use(
    with_successor: bool,
) -> None:
    """REPLAY of a stored table with unconvertible tokens hands on its rows.

    The node ends as usual; a leaf completes; a successor's edge raises the rows'
    ``ArrowTypeError``, as ``main``'s row rebuild does (Codex on PR #485, round 3).
    """
    logged = InMemoryArrowDatabase()
    job = PipelineJob(name="replay_odd", database=InMemoryArrowDatabase())
    with job:
        _plain().select_data_columns(["x"], label="selected")
    node: Any = job.nodes["selected"]
    node.attach_databases(pipeline_database=logged, cache_mode=CacheMode.LOG)
    job.run()
    path = node._stored_table_path  # pyright: ignore[reportPrivateUsage]  # the table to seed
    stored = logged.get_all_records(path, record_id_column="__record_id")
    assert stored is not None
    position = stored.schema.get_field_index("_source_x")
    odd = stored.set_column(
        position,
        pa.field("_source_x", _UNCONVERTIBLE, nullable=True),
        pa.array([[None, [[]]]] * stored.num_rows, _UNCONVERTIBLE),
    )
    seeded = InMemoryArrowDatabase()
    seeded.add_records(path, odd, record_id_column="__record_id")

    replay = _replay_job(seeded, with_successor=with_successor)
    observer = _NodeEvents()
    if with_successor:
        with pytest.raises(pa.ArrowTypeError, match="Expected bytes"):
            replay.run(observer=observer)
        assert ("end", "selected") in observer.events
        assert ("start", "batched") not in observer.events
    else:
        replay.run(observer=observer)
        assert ("end", "selected") in observer.events
