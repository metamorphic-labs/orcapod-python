"""Operators that gather rows accept only rows of one data context (ITL-1359).

``GroupBy``, ``Batch`` and the merged lists of ``MergeJoin`` refuse members of different
data contexts and give an entry its members' one context, on a plain stream, on the async
path and in a job. One data context ships, so a copy of it under another key stands in
for a second one.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Callable, Sequence
from typing import Any, cast

import pyarrow as pa
import pytest

import orcapod.contexts as contexts
from orcapod.channels import Channel
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.operators import Batch, GroupBy, MapData, MergeJoin
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.sources import ArrowTableSource, CachedSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline import PipelineJob
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.types import CacheMode, ColumnConfig

from .several_items import Row, async_rows

DEFAULT = contexts.get_default_context_key()
OTHER = "std:v1:test-gathered-other"
CONTEXTS = [DEFAULT, OTHER]
TAGS = [("h", "g"), ("g", "h")]
WITH_CONTEXT = ColumnConfig(context=True)

GROUP_BY: Callable[[], StaticOutputOperatorPod] = lambda: GroupBy(["h"])  # noqa: E731
BATCH: Callable[[], StaticOutputOperatorPod] = lambda: Batch(2)  # noqa: E731
GATHER = [
    pytest.param(GROUP_BY, id="GroupBy"),
    pytest.param(BATCH, id="Batch"),
    pytest.param(Batch, id="Batch-all"),
]


@pytest.fixture(autouse=True)
def other_context(monkeypatch: pytest.MonkeyPatch) -> contexts.DataContext:
    """A copy of the default data context, registered under another key."""
    other = dataclasses.replace(contexts.get_default_context(), context_key=OTHER)
    registry = contexts._get_registry()  # pyright: ignore[reportPrivateUsage]  # the test registers a second context
    original = registry.get_context

    def get_context(key: str | None = None) -> contexts.DataContext:
        return other if key == OTHER else original(key)

    monkeypatch.setattr(registry, "get_context", get_context)
    return other


def _columns(tags: tuple[str, ...], data_first: bool) -> pa.Table:
    """Four rows: two tags and two data columns, in the arrangement asked for."""
    tag_values = {
        "h": pa.array(["a", "a", "b", "b"], pa.large_string()),
        "g": pa.array([1, 2, 3, 4], pa.int64()),
    }
    data = {
        "x": pa.array([0.5, 0.25, 0.125, 0.75], pa.float64()),
        "n": pa.array([1, 2, 3, 4], pa.int64()),
    }
    ordered = {name: tag_values[name] for name in tags}
    columns = {**data, **ordered} if data_first else {**ordered, **data}
    return pa.Table.from_arrays(list(columns.values()), names=list(columns))


def _stream(
    keys: Sequence[str] | None,
    context: str = DEFAULT,
    *,
    tags: tuple[str, ...] = TAGS[0],
    data_first: bool = False,
    rows: Sequence[int] = (0, 1, 2, 3),
) -> ArrowTableStream:
    """A raw stream whose rows are of ``keys`` (``None``: no context column)."""
    table = (
        _columns(tags, data_first)
        .append_column("source_id::o", pa.array(["s"] * 4, pa.large_string()))
        .append_column(
            "record_id::o", pa.array(["r1", "r2", "r3", "r4"], pa.large_string())
        )
    )
    if keys is not None:
        table = table.append_column(
            constants.CONTEXT_KEY, pa.array(keys, pa.large_string())
        )
    return ArrowTableStream(
        table.take(pa.array(rows, pa.int64())),
        tag_columns=tags,
        system_tag_columns=["source_id::o", "record_id::o"],
        data_context=context,
    )


def _rows_in_contexts(keys: Sequence[str]) -> list[Row]:
    """The four rows as datagrams, row ``i`` in the data context ``keys[i]``."""
    return [
        next(_stream([key] * 4, key, rows=[i]).iter_data())
        for i, key in enumerate(keys)
    ]


def _async_over(op: StaticOutputOperatorPod, rows: list[Row]) -> list[Row]:
    """Run ``op`` over a channel of datagrams, which keep their own data contexts."""

    async def run() -> list[Row]:
        incoming: Channel[Row] = Channel()
        outgoing: Channel[Row] = Channel()
        for row in rows:
            await incoming.writer.send(row)
        await incoming.writer.close()
        await op.async_execute(
            [incoming.reader],
            outgoing.writer,
            input_pipeline_hashes=[_stream(None).pipeline_hash()],
        )
        return await outgoing.reader.collect()

    return asyncio.run(run())


def _contexts(stream: StreamProtocol) -> list[str]:
    """The data context each row of ``stream``'s table names."""
    table = stream.as_table(columns=WITH_CONTEXT)
    return cast("list[str]", table.column(constants.CONTEXT_KEY).to_pylist())


def _refused(operator: str, tags: str) -> Any:
    """Expect the refusal: the operator, both contexts and one row's tags."""
    return pytest.raises(
        InputValidationError,
        match=rf"^{operator} .*\['{DEFAULT}', '{OTHER}'\].*tags .*{tags}",
    )


# --- One input: GroupBy and Batch ----------------------------------------------------


@pytest.mark.parametrize("data_first", [False, True])
@pytest.mark.parametrize("tags", TAGS)
@pytest.mark.parametrize("make_op", GATHER)
@pytest.mark.parametrize("reverse", [False, True])
def test_members_of_two_contexts_are_refused(
    make_op: Callable[[], StaticOutputOperatorPod],
    tags: tuple[str, ...],
    data_first: bool,
    reverse: bool,
) -> None:
    keys = [DEFAULT, OTHER, DEFAULT, OTHER]
    op = make_op()
    name = type(op).__name__
    rows = [3, 2, 1, 0] if reverse else [0, 1, 2, 3]
    for context in CONTEXTS:  # whatever context the stream itself reports
        stream = _stream(keys, context, tags=tags, data_first=data_first, rows=rows)
        for read in (
            lambda s: op.static_process(s),
            lambda s: make_op()(s).as_table(),
            lambda s: list(make_op()(s).iter_data()),
        ):
            with _refused(name, "'g': "):
                read(stream)


@pytest.mark.parametrize("make_op", GATHER)
@pytest.mark.parametrize("reverse", [False, True])
def test_members_of_two_contexts_are_refused_async(
    make_op: Callable[[], StaticOutputOperatorPod], reverse: bool
) -> None:
    rows = _rows_in_contexts([DEFAULT, OTHER, DEFAULT, OTHER])
    with _refused(type(make_op()).__name__, "'g': "):
        _async_over(make_op(), rows[::-1] if reverse else rows)


@pytest.mark.parametrize("key", CONTEXTS)
@pytest.mark.parametrize("stated", [True, False])
@pytest.mark.parametrize("make_op", GATHER)
def test_the_entry_takes_its_members_one_context(
    make_op: Callable[[], StaticOutputOperatorPod], stated: bool, key: str
) -> None:
    """Also when the table names no context (the default fills it, whatever the
    stream's own)."""
    stream = _stream([key] * 4 if stated else None, key)
    result = make_op().static_process(stream)
    assert set(_contexts(result)) == {key}
    assert {d.data_context_key for _, d in result.iter_data()} == {key}
    assert {t.data_context_key for t, _ in result.iter_data()} == {key}
    assert set(_contexts(make_op()(stream))) == {key}
    rows = async_rows(make_op(), stream)
    assert len(rows) == result.as_table().num_rows
    assert {d.data_context_key for _, d in rows} == {key}


def test_a_gathered_stream_is_gathered_again_in_its_context() -> None:
    """Through an operator's lazy stream, which reports its pod's default context."""
    grouped = GroupBy(["h"])(_stream(None, OTHER))
    assert grouped.data_context_key == DEFAULT
    result = Batch()(grouped)
    assert _contexts(result) == [OTHER]
    assert [d.data_context_key for _, d in result.iter_data()] == [OTHER]


@pytest.mark.parametrize("make_op", [GATHER[0], GATHER[1]])
def test_entries_of_different_contexts_are_each_accepted(
    make_op: Callable[[], StaticOutputOperatorPod],
) -> None:
    """The rule is per gathered entry: each names its own context in the table."""
    keys = [DEFAULT, DEFAULT, OTHER, OTHER]
    assert _contexts(make_op().static_process(_stream(keys))) == [DEFAULT, OTHER]
    with _refused("Batch", "'g': 3"):
        Batch().static_process(_stream(keys))


def test_rows_a_batch_drops_are_not_members() -> None:
    keys = [DEFAULT, DEFAULT, DEFAULT, OTHER]
    stream = _stream(keys)
    assert _contexts(Batch(3, drop_partial_batch=True).static_process(stream)) == [
        DEFAULT
    ]
    with _refused("Batch", "'g': 4"):
        Batch(4).static_process(stream)


@pytest.mark.parametrize("make_op", GATHER)
def test_no_row_gathers_nothing(
    make_op: Callable[[], StaticOutputOperatorPod],
) -> None:
    result = make_op().static_process(_stream([DEFAULT, OTHER] * 2, OTHER, rows=[]))
    assert result.as_table().num_rows == 0
    assert result.data_context_key == OTHER


@pytest.mark.parametrize("make_op", GATHER)
def test_one_context_changes_nothing_of_the_result(
    make_op: Callable[[], StaticOutputOperatorPod],
) -> None:
    """A stated default context gives the table an unstated one gives."""
    every = ColumnConfig.all()
    assert (
        make_op()
        .static_process(_stream([DEFAULT] * 4))
        .as_table(columns=every)
        .equals(make_op().static_process(_stream(None)).as_table(columns=every))
    )


# --- Two inputs: MergeJoin -----------------------------------------------------------


@pytest.mark.parametrize("data_first", [False, True])
@pytest.mark.parametrize("tags", TAGS)
@pytest.mark.parametrize("stated", [True, False])
@pytest.mark.parametrize("pair", [(DEFAULT, OTHER), (OTHER, DEFAULT)])
def test_merge_join_refuses_rows_of_two_contexts(
    pair: tuple[str, str], stated: bool, tags: tuple[str, ...], data_first: bool
) -> None:
    streams = [
        _stream([key] * 4 if stated else None, key, tags=tags, data_first=data_first)
        for key in pair
    ]
    for read in (
        lambda: MergeJoin().static_process(*streams),
        lambda: MergeJoin()(*streams).as_table(),
        lambda: async_rows(MergeJoin(), *streams),
    ):
        with _refused("MergeJoin", "'g': "):
            read()


@pytest.mark.parametrize("key", CONTEXTS)
@pytest.mark.parametrize("stated", [True, False])
def test_merge_join_takes_its_rows_one_context(key: str, stated: bool) -> None:
    keys = [key] * 4 if stated else None
    streams = [_stream(keys, key), _stream(keys, key, tags=TAGS[1], data_first=True)]
    result = MergeJoin().static_process(*streams)
    assert _contexts(result) == [key] * 4
    assert {d.data_context_key for _, d in result.iter_data()} == {key}
    rows = async_rows(MergeJoin(), *streams)
    assert len(rows) == 4
    assert {d.data_context_key for _, d in rows} == {key}
    assert {t.data_context_key for t, _ in rows} == {key}


def test_merge_join_decides_per_joined_row() -> None:
    """Only matched rows are members, and each output row has its own two."""
    keys = [DEFAULT, OTHER, OTHER, DEFAULT]
    left, right = _stream(keys), _stream(keys, rows=[3, 1, 0])
    result = MergeJoin().static_process(left, right)
    by_row = dict(
        zip(result.as_table().column("g").to_pylist(), _contexts(result), strict=True)
    )
    assert by_row == {1: DEFAULT, 2: OTHER, 4: DEFAULT}
    disagreeing = _stream([DEFAULT, DEFAULT, OTHER, OTHER], rows=[0, 2])
    assert MergeJoin().static_process(left, disagreeing).as_table().num_rows == 2
    with _refused("MergeJoin", "'g': 4"):
        MergeJoin().static_process(left, _stream([OTHER] * 4, rows=[1, 2, 3]))


def test_merge_join_without_a_merged_column_gathers_nothing() -> None:
    left = _stream(None)
    right = MapData({"x": "y", "n": "m"}).static_process(_stream(None, OTHER))
    assert MergeJoin().static_process(left, right).as_table().num_rows == 4


# --- In a job ------------------------------------------------------------------------


def _source(context: str, name: str) -> ArrowTableSource:
    return ArrowTableSource(
        _columns(TAGS[0], False),
        tag_columns=list(TAGS[0]),
        tag_only_columns=True,
        source_id=name,
        data_context=context,
    )


def _cached(name: str) -> CachedSource:
    """A source whose rows report ``OTHER``."""
    return CachedSource(
        _source(OTHER, name),
        cache_database=InMemoryArrowDatabase(),
        data_context=OTHER,
    )


@pytest.mark.parametrize("make_op", GATHER)
def test_a_source_is_read_as_its_rows(
    make_op: Callable[[], StaticOutputOperatorPod],
) -> None:
    """A source reports its own data context; its rows are of the one they report, which
    is the default for an in-memory source and the source's own for a cached one."""
    for source in (_source(OTHER, "s"), _cached("s")):
        (key,) = {data.data_context_key for _, data in source.iter_data()}
        result = make_op().static_process(source)
        assert set(_contexts(result)) == {key}
        assert {d.data_context_key for _, d in result.iter_data()} == {key}
        assert {d.data_context_key for _, d in async_rows(make_op(), source)} == {key}


def test_merge_join_refuses_sources_whose_rows_are_of_two_contexts() -> None:
    for streams in (
        [_source(OTHER, "first"), _cached("second")],
        [_cached("first"), _source(OTHER, "second")],
    ):
        for read in (
            lambda: MergeJoin().static_process(*streams),
            lambda: async_rows(MergeJoin(), *streams),
        ):
            with _refused("MergeJoin", "'g': "):
                read()


@pytest.mark.parametrize("make_op", GATHER)
@pytest.mark.parametrize("reverse", [False, True])
def test_a_job_node_refuses_a_mix_and_stores_nothing(
    make_op: Callable[[], StaticOutputOperatorPod], reverse: bool
) -> None:
    keys = [DEFAULT, OTHER, DEFAULT, OTHER]
    stream = _stream(keys, rows=[3, 2, 1, 0] if reverse else [0, 1, 2, 3])
    database = InMemoryArrowDatabase()
    node = OperatorJobNode(
        make_op(),
        [stream],
        pipeline_database=database,
        cache_mode=CacheMode.LOG,
    )
    with _refused(type(make_op()).__name__, "'g': "):
        node.execute(stream)
    database.flush()
    assert database.list_table_paths(()) == []
    accepted = _stream([DEFAULT] * 4)
    OperatorJobNode(
        make_op(),
        [accepted],
        pipeline_database=database,
        cache_mode=CacheMode.LOG,
    ).execute(accepted)
    database.flush()
    assert len(database.list_table_paths(())) == 1


@pytest.mark.parametrize("cached_first", [False, True])
def test_an_async_job_refuses_a_mix_and_stores_nothing(cached_first: bool) -> None:
    """A source hands an async node its rows in their own data context."""
    database = InMemoryArrowDatabase()
    sources = [_source(DEFAULT, "first"), _cached("second")]
    job = PipelineJob(name="mixed", database=database)
    with job:
        _ = MergeJoin()(*(sources[::-1] if cached_first else sources), label="merged")
    with pytest.raises(Exception) as caught:  # noqa: PT011  # checked below, in a group
        asyncio.run(job.run_async())
    assert "MergeJoin gathers several rows" in _messages(caught.value)
    database.flush()
    assert database.list_table_paths(()) == []


def _messages(error: BaseException) -> str:
    """Every message of an error and of the errors it groups."""
    if isinstance(error, BaseExceptionGroup):
        return " ".join(_messages(inner) for inner in error.exceptions)
    return str(error)


@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("make_op", GATHER)
def test_a_job_over_one_context_runs(
    make_op: Callable[[], StaticOutputOperatorPod], use_async: bool
) -> None:
    job = PipelineJob(name="one", database=InMemoryArrowDatabase())
    with job:
        _ = make_op()(_source(DEFAULT, "s"), label="gathered")
        _ = MergeJoin()(_source(DEFAULT, "s"), _source(DEFAULT, "t"), label="merged")
    if use_async:
        asyncio.run(job.run_async())
    else:
        job.run()
    expected = make_op()(_source(DEFAULT, "s"))
    actual = job.nodes["gathered"]
    assert sorted(map(str, actual.as_table().to_pylist())) == sorted(
        map(str, expected.as_table().to_pylist())
    )
    assert set(_contexts(actual)) == {DEFAULT}
    assert job.nodes["merged"].as_table().num_rows == 4
