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


# ── round 4: the boundary of the eager route, and the lazy output itself ──────


DELIVERED: list[int] = []


def _deliver(x: int, ctx: Any) -> None:
    DELIVERED.append(x)


def _leaf_run(job: PipelineJob, label: str) -> _NodeEvents:
    """Run only ``label`` (a source node), as a leaf of a direct sync DAG."""
    dag: OrcaDAG[Any] = OrcaDAG()
    dag.add_node(job.nodes[label])
    observer = _NodeEvents()
    SyncPipelineOrchestrator().run(dag, observer=observer, materialize_results=False)
    return observer


def test_a_bound_tap_stream_delivers_every_row_as_main_does() -> None:
    """A side-effect stream bound to a slot is not a library source kind: its rows.

    The node iterates it as ``main`` did, delivering every row, and a leaf completes
    (Codex on PR #485, round 4: the eager route read ``as_table()`` instead).
    """
    from orcapod.side_effects import tap_pod

    tap: Any = tap_pod(_deliver)
    DELIVERED.clear()
    raw = _with_source_x([[None, [[]]], [None, [[]]]], _UNCONVERTIBLE)
    tapped: Any = tap.process(raw)
    job = PipelineJob(name="tap_bound", database=InMemoryArrowDatabase())
    with job:
        _plain().select_data_columns(["x"], label="selected")
    job.bind(sources={"s": tapped})
    observer = _leaf_run(job, "s")
    assert DELIVERED == [1, 2]
    assert observer.events == [("start", "s"), ("end", "s")]


@pytest.mark.parametrize(
    "make",
    [
        pytest.param(lambda: _plain().select_data_columns(["x"]), id="operator"),
        pytest.param(
            lambda: ArrowTableStream(_source_table(), tag_columns=["k"]), id="raw"
        ),
    ],
)
def test_a_bound_stream_that_is_not_a_source_kind_hands_on_its_rows(make: Any) -> None:
    """Only a library source kind whose table is its rows (``TABLE_IS_ROWS``) is eager."""
    from orcapod.core.operators.static_output_pod import RowsOnRead

    job = PipelineJob(name="bound_kinds", database=InMemoryArrowDatabase())
    with job:
        _plain().select_data_columns(["x"], label="selected")
    job.bind(sources={"s": make()})
    assert isinstance(job.nodes["s"].execute(), RowsOnRead)


def test_a_library_source_kind_is_eager() -> None:
    from orcapod.core.operators.static_output_pod import RowsOnRead

    job = PipelineJob(name="eager_kind", database=InMemoryArrowDatabase())
    with job:
        _plain().select_data_columns(["x"], label="selected")
    assert not isinstance(job.nodes["s"].execute(), RowsOnRead)


def test_an_empty_output_is_derived_at_the_edge() -> None:
    """A source with no row: its zero-row output is derived where the successor reads.

    An observer that rebinds the source when it ends changes what the successor
    receives, as on ``main`` (Codex on PR #485, round 4).
    """

    def empty(mark: str) -> ArrowTableSource:
        field = pa.field("x", pa.int64(), nullable=False, metadata={"mark": mark})
        table = pa.table({"k": pa.array([], pa.int64()), "x": pa.array([], pa.int64())})
        table = pa.Table.from_arrays(
            table.columns,
            schema=pa.schema([pa.field("k", pa.int64(), nullable=False), field]),
        )
        return ArrowTableSource(table, tag_columns=["k"], source_id="s")

    job = PipelineJob(name="empty_edge", database=InMemoryArrowDatabase())
    with job:
        empty("before").select_data_columns(["x"], label="selected")
    slot = next(label for label in job.nodes if label != "selected")
    node = job.nodes[slot]

    class RebindsAtEnd(NoOpObserver):
        def contextualize(self, *identity_path: str) -> RebindsAtEnd:
            return self

        def on_node_end(self, node_label: str, node_hash: str) -> None:
            if node_label == slot:
                node.bound_source = empty("after")

    job.run(observer=RebindsAtEnd())
    kept = job.nodes["selected"].as_table()
    assert kept.schema.field("x").metadata == {b"mark": b"after"}


def test_an_operator_with_an_empty_result_is_not_called_again() -> None:
    """An empty result's zero-row form is derived at the edge, never by a second call.

    An operator that raises on empty input completes as a leaf, as on ``main``
    (Codex on PR #485, round 4).
    """
    from orcapod.operators import SelectDataColumns

    calls: list[int] = []

    class EmptyThenRaises(SelectDataColumns):
        def unary_static_process(self, stream: Any) -> Any:
            rows = stream.as_table().num_rows
            calls.append(rows)
            if rows == 0:
                raise ValueError("cannot process empty input")
            result: Any = super().unary_static_process(stream)
            return result.polars_filter(x=-1)

    job = PipelineJob(name="empty_result", database=InMemoryArrowDatabase())
    with job:
        EmptyThenRaises(["x"])(_plain(), label="emptied")
    observer = _NodeEvents()
    job.run(observer=observer)
    assert ("end", "emptied") in observer.events
    assert 0 not in calls


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
def test_returned_function_rows_keep_their_meta(asynchronous: bool) -> None:
    """``node_outputs[node].iter_data()`` gives the node's own rows, meta included."""
    from orcapod import function_pod
    from orcapod.pipeline import AsyncPipelineOrchestrator

    @function_pod(output_keys="y")
    def plus(x: int) -> int:
        return x + 1

    job = PipelineJob(name="returned_rows", database=InMemoryArrowDatabase())
    with job:
        plus.pod(_plain(), label="plus")
    dag: OrcaDAG[Any] = OrcaDAG()
    dag.add_edge(job.nodes["s"], job.nodes["plus"])
    orchestrator: Any = (
        AsyncPipelineOrchestrator() if asynchronous else SyncPipelineOrchestrator()
    )
    result = orchestrator.run(dag)
    rows = list(result.node_outputs[job.nodes["plus"]].iter_data())
    assert rows
    assert all("__computed" in data.keys(columns={"meta": True}) for _, data in rows)


def test_a_lazy_output_is_described_and_opened_without_reading() -> None:
    """Describing an unread output, or opening its channel, builds nothing.

    The first read of the channel raises what the rows' build raises (Codex on PR
    #485, round 4).
    """
    import asyncio

    from orcapod.core.operators.static_output_pod import RowsOnRead
    from orcapod.protocols.core_protocols import StreamProtocol

    job = PipelineJob(name="lazy_out", database=InMemoryArrowDatabase())
    with job:
        _plain().select_data_columns(["x"], label="selected")
    job.bind(sources={"s": _unconvertible()})
    out: Any = job.nodes["s"].execute()
    assert isinstance(out, RowsOnRead)
    assert isinstance(out, StreamProtocol)
    assert not out.arrow_types_declared
    assert "x" in out.arrow_schema().names
    channel = out.open_channel()
    assert out._stream is None  # pyright: ignore[reportPrivateUsage]  # nothing built
    with pytest.raises(pa.ArrowTypeError, match="Expected bytes"):
        asyncio.run(channel.collect())


# ── round 5 ──────────────────────────────────────────────────────────────────


def _subclass_overriding(member: str) -> type:
    """An ``ArrowTableSource`` subclass that overrides ``member`` (reading as before)."""
    from orcapod.core.sources import ArrowTableSource as Base

    def through(self: Any, *args: Any, **kwargs: Any) -> Any:
        return getattr(super(cls, self), member)(*args, **kwargs)

    cls = type(f"Overrides_{member}", (Base,), {member: through})
    return cls


@pytest.mark.parametrize(
    "member",
    ["iter_data", "as_table", "__iter__", "async_iter_data", "keys", "output_schema"],
)
def test_a_subclass_that_overrides_a_reader_is_not_a_table_is_rows_kind(
    member: str,
) -> None:
    """``TABLE_IS_ROWS`` is not inherited past an override of how rows are read."""
    from orcapod.core.sources.base import table_is_rows

    table = _source_table().select(["k", "x"])
    cls = _subclass_overriding(member)
    source = cls(table, tag_columns=["k"], tag_only_columns=True, source_id="s")
    assert not table_is_rows(source)
    plain = ArrowTableSource(
        table, tag_columns=["k"], tag_only_columns=True, source_id="s"
    )
    assert table_is_rows(plain)
    # Exact, never by inspection: a subclass that overrides nothing is not proven.
    subclass = type("Plain", (ArrowTableSource,), {})
    assert not table_is_rows(
        subclass(table, tag_columns=["k"], tag_only_columns=True, source_id="s")
    )


def test_a_filtering_subclass_delivers_only_its_rows() -> None:
    """A subclass whose ``iter_data`` filters is read through it, as on ``main``."""

    class AboveTen(ArrowTableSource):
        def iter_data(self) -> Any:
            return (row for row in super().iter_data() if row[1]["x"] > 10)

    table = pa.table({"k": pa.array([1, 2]), "x": pa.array([10, 20])})
    job = PipelineJob(name="filtering", database=InMemoryArrowDatabase())
    with job:
        ArrowTableSource(
            table, tag_columns=["k"], tag_only_columns=True, source_id="s"
        ).select_data_columns(["x"], label="selected")
    job.bind(
        sources={
            "s": AboveTen(
                table, tag_columns=["k"], tag_only_columns=True, source_id="s"
            )
        }
    )
    job.run()
    assert job.nodes["selected"].as_table()["x"].to_pylist() == [20]


def test_an_operator_result_keeps_its_own_column_order() -> None:
    """A result laid out ``y, x`` against a prediction ``x, y`` is kept, stored and
    handed on as ``y, x``, as ``main`` built it from the result's rows."""
    from orcapod.operators import SelectDataColumns

    class Reordered(SelectDataColumns):
        def unary_static_process(self, stream: Any) -> Any:
            out: Any = super().unary_static_process(stream)
            table = out.as_table(columns={"source": True, "system_tags": True})
            names = [n for n in table.column_names if n not in ("x", "y")]
            plain = [n for n in names if not n.startswith("_source_")]
            source = ["_source_y", "_source_x"]
            rest = [n for n in names if n.startswith("_source_") and n not in source]
            return ArrowTableStream(
                table.select([*plain, "y", "x", *source, *rest]), tag_columns=["k"]
            )

    database = InMemoryArrowDatabase()
    job = PipelineJob(name="reordered", database=database)
    with job:
        source = DictSource(
            [{"k": 1, "x": 1, "y": 2}, {"k": 2, "x": 3, "y": 4}],
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="s",
        )
        Batch(batch_size=2)(Reordered(["x", "y"])(source, label="reordered"), label="b")
    job.nodes["reordered"].attach_databases(
        pipeline_database=database.at("log"), cache_mode=CacheMode.LOG
    )
    job.run()
    expected = ["k", "y", "x", "_source_y", "_source_x"]
    node = job.nodes["reordered"]
    assert node.as_table(columns={"source": True}).column_names == expected
    stored = node.get_all_records(columns={"source": True})
    assert stored is not None
    assert [n for n in stored.column_names if not n.startswith("__")] == expected
    assert job.nodes["b"].as_table(columns={"source": True}).column_names == expected


# ── final check: the eager route is positive and exact ────────────────────────


def _only_above_ten(rows: Any) -> Any:
    return (row for row in rows if row[1]["x"] > 10)


class _FilteringStream(ArrowTableStream):
    def iter_data(self) -> Any:
        return _only_above_ten(super().iter_data())


class _ReadsThroughAProperty(ArrowTableSource):
    """Reads through a ``_stream`` property that hands on a filtering stream."""

    @property
    def _stream(self) -> Any:
        return getattr(self, "saved_stream")

    @_stream.setter
    def _stream(self, base: Any) -> None:
        self.saved_stream = _FilteringStream(
            base.as_table(columns={"source": True, "system_tags": True}),
            tag_columns=["k"],
        )


def _bound_selection(source: Any) -> list[int]:
    """Bind ``source`` to a slot feeding a selection; return what the sync run keeps."""
    table = pa.table({"k": pa.array([1, 2]), "x": pa.array([10, 20])})
    job = PipelineJob(name="exact", database=InMemoryArrowDatabase())
    with job:
        ArrowTableSource(
            table, tag_columns=["k"], tag_only_columns=True, source_id="s"
        ).select_data_columns(["x"], label="selected")
    job.bind(sources={"s": source})
    job.run()
    return job.nodes["selected"].as_table()["x"].to_pylist()


@pytest.mark.parametrize("how", ["instance_reader", "property", "plain_subclass"])
def test_only_an_exact_unshadowed_library_source_is_eager(how: str) -> None:
    """An ``iter_data`` replaced on the instance and a ``_stream`` property both read
    the rows the reader gives (``[20]``, as ``main``); a plain subclass is not proven
    either, and gives ``main``'s rows (Codex on PR #485, final check)."""
    from orcapod.core.sources.base import table_is_rows

    table = pa.table({"k": pa.array([1, 2]), "x": pa.array([10, 20])})
    kwargs: dict[str, Any] = {
        "tag_columns": ["k"],
        "tag_only_columns": True,
        "source_id": "s",
    }
    if how == "instance_reader":
        source: Any = ArrowTableSource(table, **kwargs)
        read = source.iter_data
        source.iter_data = lambda: _only_above_ten(read())
        expected = [20]
    elif how == "property":
        source = _ReadsThroughAProperty(table, **kwargs)
        expected = [20]
    else:
        source = type("Plain", (ArrowTableSource,), {})(table, **kwargs)
        expected = [10, 20]
    assert not table_is_rows(source)
    assert _bound_selection(source) == expected


def test_an_exact_library_source_is_eager() -> None:
    """The exact class, with nothing replaced on the instance, takes the eager route."""
    from orcapod.core.sources.base import table_is_rows

    table = pa.table({"k": pa.array([1, 2]), "x": pa.array([10, 20])})
    source = ArrowTableSource(
        table, tag_columns=["k"], tag_only_columns=True, source_id="s"
    )
    source.as_table()
    list(source.iter_data())
    assert table_is_rows(source)
    assert _bound_selection(source) == [10, 20]


def _swapped(stream: Any, first: str, second: str, tags: list[str]) -> Any:
    """``stream``'s table with ``first`` and ``second`` swapped, over ``tags``."""
    from orcapod.core.streams.base import ZERO_ROW_INPUT_COLUMNS

    table = stream.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
    names = list(table.column_names)
    a, b = names.index(first), names.index(second)
    names[a], names[b] = names[b], names[a]
    return ArrowTableStream(table.select(names), tag_columns=tags)


@pytest.mark.parametrize("replay", [False, True], ids=["log", "replay"])
@pytest.mark.parametrize("layout", ["data", "tags"])
def test_a_composed_operator_result_keeps_its_own_order(
    layout: str, replay: bool
) -> None:
    """An operator whose ``static_process`` returns ANOTHER operator's result keeps,
    stores and hands on the order of the rows that result yields, to any depth: data
    ``y, x`` (laid out so) and tags ``b, a`` (declared so), as ``main`` built them from
    the rows; REPLAY of the stored table too (Codex on PR #485, final check)."""
    from orcapod.operators import SelectDataColumns

    class Reordered(SelectDataColumns):
        def unary_static_process(self, stream: Any) -> Any:
            if layout == "data":
                return _swapped(stream, "x", "y", ["a", "b"])
            return _swapped(stream, "x", "x", ["b", "a"])

    class Composed(SelectDataColumns):
        def unary_static_process(self, stream: Any) -> Any:
            return Reordered(["x", "y"]).process(stream)

    database = InMemoryArrowDatabase()
    job = PipelineJob(name="composed", database=database)
    with job:
        source = DictSource(
            [{"a": 1, "b": 5, "x": 1, "y": 2}, {"a": 2, "b": 6, "x": 3, "y": 4}],
            tag_columns=["a", "b"],
            tag_only_columns=True,
            source_id="s",
        )
        Batch(batch_size=2)(Composed(["x", "y"])(source, label="composed"), label="b")
    node: Any = job.nodes["composed"]
    node.attach_databases(
        pipeline_database=database.at("log"), cache_mode=CacheMode.LOG
    )
    job.run()
    if replay:
        node.attach_databases(
            pipeline_database=database.at("log"), cache_mode=CacheMode.REPLAY
        )
        node.clear_cache()
        job.run()
    # ``main`` 321284ae, measured: the stored table keeps its tags in the slot's order,
    # and REPLAY hands on the stored table's order.
    data = ["a", "b", "y", "x", "_source_y", "_source_x"]
    stored_tags = ["a", "b", "x", "y", "_source_x", "_source_y"]
    result_tags = ["b", "a", "x", "y", "_source_x", "_source_y"]
    if layout == "data":
        kept, in_store = data, data
    else:
        kept, in_store = (stored_tags if replay else result_tags), stored_tags
    assert node.as_table(columns={"source": True}).column_names == kept
    stored = node.get_all_records(columns={"source": True})
    assert stored is not None
    assert [n for n in stored.column_names if not n.startswith("__")] == in_store
    assert job.nodes["b"].as_table(columns={"source": True}).column_names == kept


def test_a_result_of_another_stream_kind_takes_mains_row_path() -> None:
    """A result that is not exactly an ``ArrowTableStream`` is read as ``main`` read
    it: its rows (here, the ones its iterator yields), never its table."""
    from orcapod.core.streams.base import ZERO_ROW_INPUT_COLUMNS
    from orcapod.operators import SelectDataColumns

    class Filtering(SelectDataColumns):
        def unary_static_process(self, stream: Any) -> Any:
            table = stream.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
            return _FilteringStream(table, tag_columns=["k"])

    database = InMemoryArrowDatabase()
    job = PipelineJob(name="other_kind", database=database)
    with job:
        source = ArrowTableSource(
            pa.table({"k": pa.array([1, 2]), "x": pa.array([10, 20])}),
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="s",
        )
        Batch(batch_size=2)(Filtering(["x"])(source, label="filtered"), label="b")
    node: Any = job.nodes["filtered"]
    node.attach_databases(
        pipeline_database=database.at("log"), cache_mode=CacheMode.LOG
    )
    job.run()
    assert node.as_table()["x"].to_pylist() == [20]
    stored = node.get_all_records()
    assert stored is not None
    assert stored["x"].to_pylist() == [20]
    assert job.nodes["b"].as_table()["x"].to_pylist() == [[20]]


# ── CodeRabbit on 352ce7b9: every wrapper proven, and the rows' data context ─────


@pytest.mark.parametrize("how", ["subclass", "instance_reader"])
def test_a_dynamic_stream_that_is_not_proven_stops_the_chain(how: str) -> None:
    """A ``DynamicPodStream`` subclass, or one whose ``iter_data`` is replaced on the
    instance, inside an operator's result is read through its own rows (``[20]``),
    as ``main`` read them: the chain is followed only through proven wrappers."""
    from orcapod.core.operators.static_output_pod import DynamicPodStream
    from orcapod.operators import SelectDataColumns

    class FilteringDynamic(DynamicPodStream):
        def iter_data(self) -> Any:
            return _only_above_ten(super().iter_data())

    class Inner(SelectDataColumns):
        pass

    class Outer(SelectDataColumns):
        def unary_static_process(self, stream: Any) -> Any:
            if how == "subclass":
                return FilteringDynamic(Inner(["x", "y"]), (stream,))
            inner: Any = Inner(["x", "y"]).process(stream)
            read = inner.iter_data
            inner.iter_data = lambda: _only_above_ten(read())
            return inner

    database = InMemoryArrowDatabase()
    job = PipelineJob(name="unproven", database=database)
    with job:
        source = ArrowTableSource(
            pa.table({"k": [1, 2], "x": [10, 20], "y": [1, 2], "z": [5, 6]}),
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="s",
        )
        Batch(batch_size=2)(Outer(["x", "y"])(source, label="outer"), label="b")
    node: Any = job.nodes["outer"]
    node.attach_databases(
        pipeline_database=database.at("log"), cache_mode=CacheMode.LOG
    )
    job.run()
    assert node.as_table()["x"].to_pylist() == [20]
    stored = node.get_all_records()
    assert stored is not None
    assert stored["x"].to_pylist() == [20]
    assert job.nodes["b"].as_table()["x"].to_pylist() == [[20]]


OTHER_CONTEXT = "std:v1:test-rows-on-read-other"


def test_a_result_in_another_data_context_hands_on_its_own_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An operator node over a stream of another data context hands on the result's
    rows as ``main`` returned them (in their own data context); what it keeps is in its
    own data context, as on ``main``."""
    import dataclasses

    import orcapod.contexts as contexts
    from orcapod.core.nodes.operator_node import OperatorJobNode
    from orcapod.operators import SelectDataColumns
    from orcapod.system_constants import constants

    other = dataclasses.replace(
        contexts.get_default_context(), context_key=OTHER_CONTEXT
    )
    registry = contexts._get_registry()  # pyright: ignore[reportPrivateUsage]  # the test registers a second context
    original = registry.get_context

    def get_context(key: Any = None) -> contexts.DataContext:  # pyright: ignore[reportExplicitAny]  # the registry's own argument
        return other if key == OTHER_CONTEXT else original(key)

    monkeypatch.setattr(registry, "get_context", get_context)

    def stream() -> ArrowTableStream:
        table = pa.table(
            {
                "k": ["a", "b"],
                "x": [1, 2],
                "y": [3, 4],
                constants.CONTEXT_KEY: pa.array([OTHER_CONTEXT] * 2, pa.large_string()),
            }
        )
        return ArrowTableStream(table, tag_columns=["k"], data_context=other)

    node = OperatorJobNode(SelectDataColumns(["x"]), [stream()])
    output: Any = node.execute(stream())
    assert [data.data_context_key for _, data in output.iter_data()] == [
        OTHER_CONTEXT
    ] * 2
    kept = node.as_table(columns={"context": True})[constants.CONTEXT_KEY]
    assert kept.to_pylist() == [node.data_context_key] * 2


# ── Codex round 7: the source's data context, system tags, retained lifetime ───


def test_a_source_in_another_data_context_hands_on_its_own_rows() -> None:
    """An exact ``CachedSource`` with its own data context: the rows a sync run returns
    report the source's context, as ``main`` returned them, and the edge table is built
    in the slot's context, as ``main`` rebuilt it."""
    import dataclasses

    from orcapod.contexts import get_default_context
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.operators.static_output_pod import at_edge
    from orcapod.sources import CachedSource
    from orcapod.system_constants import constants

    custom = dataclasses.replace(get_default_context(), context_key="review:custom")
    inner = DictSource(
        [{"k": 1, "x": 10}, {"k": 2, "x": 20}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="s",
        data_context=custom,
    )
    source = CachedSource(
        inner,
        cache_database=InMemoryArrowDatabase(),
        source_id="cached",
        data_context=custom,
    )
    slot = SourceJobNode.from_stream(source)
    dag: OrcaDAG[Any] = OrcaDAG()
    dag.add_node(slot)
    output: Any = SyncPipelineOrchestrator().run(dag).node_outputs[slot]
    assert [
        (tag.data_context_key, data.data_context_key)
        for tag, data in output.iter_data()
    ] == [("review:custom", "review:custom")] * 2
    edge = at_edge(output).as_table(columns={"context": True})
    assert edge[constants.CONTEXT_KEY].to_pylist() == [slot.data_context_key] * 2


class _AddSystemTag:
    """``SelectDataColumns`` whose result carries one more system tag, ``int16``."""

    @staticmethod
    def operator(values: list[int | None], exact: bool) -> Any:
        from orcapod.operators import SelectDataColumns
        from orcapod.system_constants import constants

        class Unproven(ArrowTableStream):
            """Reads as ``ArrowTableStream``; not proven: ``main``'s row path."""

        kind = ArrowTableStream if exact else Unproven

        class AddSystemTag(SelectDataColumns):
            def unary_static_process(self, stream: Any) -> Any:
                out: Any = super().unary_static_process(stream)
                table = out.as_table(columns={"source": True, "system_tags": True})
                table = table.append_column(
                    f"{constants.SYSTEM_TAG_PREFIX}region",
                    pa.array(values[: table.num_rows], pa.int16()),
                )
                return kind(table, tag_columns=["k"])

        return AddSystemTag(["x"])


def _system_tag_job(values: list[int | None], exact: bool) -> tuple[Any, Any]:
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="system_tag", database=database)
    with job:
        source = DictSource(
            [{"k": 1, "x": 1}, {"k": 2, "x": 2}],
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="s",
        )
        _AddSystemTag.operator(values, exact)(source, label="marked")
    job.nodes["marked"].attach_databases(
        pipeline_database=database.at("log"), cache_mode=CacheMode.LOG
    )
    return job, database


def test_a_narrow_system_tag_is_stored_as_the_rows_give() -> None:
    """A result over an exact ``ArrowTableStream`` with an ``int16`` system tag: kept,
    stored and record-hashed as ``main``'s rows give it (``int64``, non-null), the
    same as ``main``'s own row path (an unproven stream) gives."""
    from orcapod.system_constants import constants

    name = f"{constants.SYSTEM_TAG_PREFIX}region"
    stored: dict[bool, Any] = {}
    for exact in (True, False):
        job, database = _system_tag_job([1, 2], exact)
        job.run()
        node: Any = job.nodes["marked"]
        kept = node.as_table(columns={"system_tags": True}).schema.field(name)
        assert (kept.type, kept.nullable) == (pa.int64(), False)
        stored[exact] = database.at("log").get_all_records(
            node._stored_table_path,  # pyright: ignore[reportPrivateUsage]  # the raw LOG table
            record_id_column="_record_hash",
        )
    for table in stored.values():
        field = table.schema.field(name)
        assert (field.type, field.nullable) == (pa.int64(), False)
    assert (
        stored[True]["_record_hash"].to_pylist()
        == stored[False]["_record_hash"].to_pylist()
    )


def test_a_null_system_tag_raises_at_the_operator_as_main_does() -> None:
    """With a null among its values, ``main``'s rows cannot make one table: the
    operator raises ``ArrowInvalid`` before its end event, as on ``main``."""
    job, _ = _system_tag_job([None, 2], exact=True)
    observer = _NodeEvents()
    with pytest.raises(pa.ArrowInvalid):
        job.run(observer=observer)
    assert ("start", "marked") in observer.events
    assert ("end", "marked") not in observer.events


def test_a_source_table_with_other_system_tags_is_not_its_rows() -> None:
    """The source route's guard: an exact ``ArrowTableSource`` whose system tag is a
    narrow integer has a table that is not its rows (they give ``int64``); a stock
    source's system tags are what the rows give."""
    from orcapod.core.streams.base import system_tags_as_rows_give

    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
            pa.field("region", pa.int16(), nullable=False),
        ]
    )
    narrow = ArrowTableSource(
        pa.Table.from_pylist(
            [{"k": 1, "x": 10, "region": 1}, {"k": 2, "x": 20, "region": 2}],
            schema=schema,
        ),
        tag_columns=["k"],
        tag_only_columns=True,
        system_tag_columns=["region"],
        source_id="s",
    )
    assert not system_tags_as_rows_give(
        narrow.as_table(columns={"system_tags": True}).schema
    )
    stock = _plain()
    assert system_tags_as_rows_give(
        stock.as_table(columns={"system_tags": True}).schema
    )


def test_a_retained_output_keeps_only_its_rows_once_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With results retained, an output held as its rows drops its built stream once
    every successor has read it (``main`` retained only the rows): no built stream
    outlives the run, its rows are its own, and a later read builds the table again."""
    import gc
    import weakref

    from orcapod import function_pod
    from orcapod.core.operators.static_output_pod import (
        RowsOnRead,
        StaticOutputOperatorPod,
    )

    @function_pod(output_keys="y")
    def plus_one(x: int) -> int:
        return x + 1

    job = PipelineJob(name="retained", database=InMemoryArrowDatabase())
    with job:
        computed: Any = plus_one.pod(
            _plain().select_data_columns(["x"]), label="computed"
        )
        computed.select_data_columns(["y"], label="second")
        computed.select_data_columns(["y"], label="third")

    built: list[weakref.ref[Any]] = []
    original = StaticOutputOperatorPod._materialize_upstream  # pyright: ignore[reportPrivateUsage]  # the row path, watched

    def watched(*args: Any, **kwargs: Any) -> Any:
        stream = original(*args, **kwargs)
        built.append(weakref.ref(stream))
        return stream

    monkeypatch.setattr(
        StaticOutputOperatorPod, "_materialize_upstream", staticmethod(watched)
    )
    graph = job._node_graph  # pyright: ignore[reportPrivateUsage]  # the job's graph
    assert graph is not None
    result = SyncPipelineOrchestrator().run(graph)
    output: Any = result.node_outputs[job.nodes["computed"]]
    assert isinstance(output, RowsOnRead)
    gc.collect()
    assert built
    assert all(ref() is None for ref in built)
    assert len(list(output.iter_data())) == 2
    assert output.as_table().num_rows == 2


@pytest.mark.parametrize(
    "kind",
    [pa.time64("ns"), pa.decimal256(76, 40)],
    ids=["time64ns", "decimal256"],
)
def test_a_lazy_output_describes_itself_without_converting_python_types(
    kind: pa.DataType,
) -> None:
    """Types with no Python type of their own are described from the node, never raised."""
    values = (
        pa.array([1, 2]).cast(kind)
        if pa.types.is_decimal(kind)
        else pa.array([1, 2], kind)
    )
    base = pa.table({"k": pa.array([1, 2], pa.int64()), "x": values})
    base = base.cast(pa.schema([f.with_nullable(False) for f in base.schema]))
    source = ArrowTableSource(
        base, tag_columns=["k"], tag_only_columns=True, source_id="s"
    )
    raw = ArrowTableStream(
        source.as_table(columns={"source": True, "system_tags": True}),
        tag_columns=["k"],
    )
    job = PipelineJob(name="lazy_types", database=InMemoryArrowDatabase())
    with job:
        source.select_data_columns(["x"], label="selected")
    job.bind(sources={"s": raw})
    out: Any = job.nodes["s"].execute()
    assert out.arrow_schema().field("x").type == kind
