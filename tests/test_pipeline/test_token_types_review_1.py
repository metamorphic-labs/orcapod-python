"""Findings of the independent review of PR #601 (ITL-1491), each as a test.

The rule under test: the Arrow type of a source-info token column is determined by
the pipeline and kept on every route; no route types a token again from its value
where an Arrow type exists; and inputs that declare one token column with two types
are refused, on the sync and on the async route alike.

Every job has sources with two tags (both data-tags) and two data columns, in two tag
orders, and runs on both orchestrators.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.channels import Channel
from orcapod.core.datagrams import Data
from orcapod.core.operators import Join, MergeJoin
from orcapod.core.operators.static_output_pod import (
    SourceInfoTypesDisagree,
    StaticOutputOperatorPod,
    concatenate_batches,
)
from orcapod.core.record_batch_channels import OutputChannel
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.core.streams.base import ZERO_ROW_INPUT_COLUMNS
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.system_constants import constants

ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}
TAG_ORDERS = {"k_then_g": ("k", "g"), "g_then_k": ("g", "k")}
TAG_ORDER = pytest.mark.parametrize("tags", list(TAG_ORDERS))

TOKEN = pa.large_string()
TOKENS = pa.large_list(TOKEN)
# A hand-built token type no value gives: a fixed-size list whose item field has its
# own name and is not nullable.
FIXED = pa.list_(pa.field("member", TOKEN, nullable=False), 2)
FIXED_PLAIN = pa.list_(TOKEN, 2)

calls: Counter[str] = Counter()


@pytest.fixture(autouse=True)
def _reset() -> None:
    """Clear the call counts."""
    calls.clear()


@function_pod(output_keys="total")
def add_two(x: int, y: int) -> int:
    """Return ``x + y``."""
    calls["add_two"] += 1
    return x + y


@function_pod(output_keys="total")
def add_three(x: int, y: int, z: int) -> int:
    """Return ``x + y + z``."""
    calls["add_three"] += 1
    return x + y + z


@function_pod(output_keys="total")
def add_pairs(x: list[int], w: int, v: int) -> int:
    """Return the sum of the merged ``x`` and the two other columns."""
    calls["add_pairs"] += 1
    return sum(x) + w + v


# (first data column, second data column) of each source.
DATA = {"a": ("x", "w"), "b": ("y", "v"), "c": ("z", "u")}


def _source(
    source_id: str,
    tags: tuple[str, ...],
    *,
    data: tuple[str, str] | None = None,
    keys: tuple[int, int] = (1, 2),
    data_tags: bool = True,
) -> ArrowTableSource:
    """Two rows keyed ``k`` and ``g``, with the source's two data columns."""
    first, second = data or DATA[source_id]
    columns = {
        "k": pa.array(list(keys), pa.int64()),
        "g": pa.array(["m", "n"], pa.large_string()),
        first: pa.array([10, 20], pa.int64()),
        second: pa.array([100, 200], pa.int64()),
    }
    names = [*tags, first, second]
    schema = pa.schema(pa.field(n, columns[n].type, nullable=False) for n in names)
    return ArrowTableSource(
        pa.Table.from_arrays([columns[n] for n in names], schema=schema),
        tag_columns=list(tags),
        tag_only_columns=not data_tags,
        source_id=source_id,
    )


def _with_tokens(
    source: ArrowTableSource,
    tags: tuple[str, ...],
    tokens: dict[str, tuple[list[Any], pa.DataType]],
    *,
    data_tags: bool = True,
) -> ArrowTableStream:
    """``source``'s rows as a raw stream with the named token columns replaced."""
    table = source.as_table(columns={"source": True, "system_tags": True})
    for name, (values, kind) in tokens.items():
        column = f"{constants.SOURCE_PREFIX}{name}"
        table = table.set_column(
            table.schema.get_field_index(column),
            pa.field(column, kind, nullable=True),
            pa.array(values, kind),
        )
    return ArrowTableStream(
        table, tag_columns=list(tags), data_tags=list(tags) if data_tags else []
    )


def _stored(database: InMemoryArrowDatabase, function: str) -> pa.Table:
    """The pipeline table (``pdb_v2``) of the function named ``function``."""
    database.flush()
    (path,) = [
        p for p in database.list_sources() if p[-1] == "pdb_v2" and function in p
    ]
    records = database.get_all_records(path)
    assert records is not None
    return records


def _instance_ids(stored: pa.Table) -> list[bytes]:
    """The stored instance ids, sorted."""
    return sorted(
        cast("list[bytes]", stored.column("__pipeline_instance_id").to_pylist())
    )


def _async_rows(operator: Any, *streams: Any) -> list[Any]:
    """The rows ``operator.async_execute`` yields over the streams' rows."""

    async def run() -> list[Any]:
        channels: list[Channel[Any]] = [Channel(buffer_size=16) for _ in streams]
        output: Channel[Any] = Channel(buffer_size=16)
        for stream, channel in zip(streams, channels):
            for row in stream.iter_data():
                await channel.writer.send(row)
            await channel.writer.close()
        await operator.async_execute(
            [channel.reader for channel in channels],
            output.writer,
            input_pipeline_hashes=[s.pipeline_hash() for s in streams],
        )
        return await output.reader.collect()

    return asyncio.run(run())


# ---------------------------------------------------------------------------
# P1-2: the async Join keeps the declared type of a merged data-tag's token
# ---------------------------------------------------------------------------

K_TOKENS = {
    "a": ([["a1", "a2"], ["a3", "a4"]], FIXED),
    "b": ([["b1", "b2"], ["b3", "b4"]], FIXED),
}


def _join_job(
    database: InMemoryArrowDatabase, tags: tuple[str, ...], inputs: int
) -> PipelineJob:
    """``Join(a, b[, c]) -> add``; ``k`` and ``g`` are data-tags of ``a`` and ``b``.

    ``c`` has plain tags, so it merges no token and leaves the first two inputs'
    merged token pending until the join's last step.
    """
    job = PipelineJob(name="merged", database=database)
    with job:
        sources = [_source("a", tags), _source("b", tags)]
        if inputs == 3:
            sources.append(_source("c", tags, data_tags=False))
        function = add_two if inputs == 2 else add_three
        function.pod(Join()(*sources, label="joined"), label="f")
    job.bind(
        sources={
            name: _with_tokens(_source(name, tags), tags, {"k": K_TOKENS[name]})
            for name in ("a", "b")
        }
    )
    return job


@TAG_ORDER
@pytest.mark.parametrize("inputs", [2, 3])
def test_a_join_keeps_the_declared_type_of_a_merged_data_tag_token(
    inputs: int, tags: str
) -> None:
    """Both routes store the merged token with the type both inputs declare.

    ``k`` is a data-tag on two inputs, with fixed-size-list tokens. The join merges
    the two tokens position by position into the same fixed-size list (item field
    ``member``, not nullable). The async route rebuilt the merged token from its
    Python value as a ``large_list`` (review of PR #601, P1-2), also when a third
    input left the merged token pending until the last step.
    """
    function = "add_two" if inputs == 2 else "add_three"
    seen = {}
    for name, orchestrator in ORCHESTRATORS.items():
        database = InMemoryArrowDatabase()
        job = _join_job(database, TAG_ORDERS[tags], inputs)
        _ = job.run(orchestrator=orchestrator())
        stored = _stored(database, function).sort_by("k")
        field = stored.schema.field("_source_k")
        assert field.type.equals(FIXED, check_metadata=True), (name, field.type)
        merged = cast("list[list[str]]", stored.column("_source_k").to_pylist())
        assert [len(token) for token in merged] == [2, 2]
        assert all(part.startswith("join_v1:") for token in merged for part in token)
        assert len({part for token in merged for part in token}) == 4
        assert stored.schema.field("_source_g").type == TOKEN
        seen[name] = (merged, _instance_ids(stored))
    assert seen["sync"] == seen["async"]


@TAG_ORDER
@pytest.mark.parametrize("inputs", [2, 3])
@pytest.mark.parametrize("first", list(ORCHESTRATORS))
def test_switching_routes_over_one_store_adds_no_row_after_a_join(
    first: str, inputs: int, tags: str
) -> None:
    """A second session on the other route finds every instance: no write error."""
    function = "add_two" if inputs == 2 else "add_three"
    second = next(name for name in ORCHESTRATORS if name != first)
    database = InMemoryArrowDatabase()
    stored = []
    for route in (first, second):
        job = _join_job(database, TAG_ORDERS[tags], inputs)
        _ = job.run(orchestrator=ORCHESTRATORS[route]())
        table = _stored(database, function)
        assert table.num_rows == 2, route
        stored.append(_instance_ids(table))
    assert stored[0] == stored[1]
    assert calls[function] == 2


# ---------------------------------------------------------------------------
# Q2 / P2-3: inputs that declare one token column with two types are refused
# ---------------------------------------------------------------------------

# Pairs of declarations of one token column; the values fit both.
TWO_TYPES = {
    "fixed_and_large": (FIXED_PLAIN, TOKENS),
    "item_nullability": (
        pa.large_list(pa.field("item", TOKEN, nullable=False)),
        TOKENS,
    ),
    "list_and_string": (TOKENS, TOKEN),
}
SIDES = pytest.mark.parametrize("swapped", [False, True], ids=["a_b", "b_a"])


def _values(kind: pa.DataType, prefix: str) -> list[Any]:
    """Two rows of tokens of ``kind``."""
    if kind == TOKEN:
        return [f"{prefix}1", f"{prefix}2"]
    return [[f"{prefix}1", f"{prefix}2"], [f"{prefix}3", f"{prefix}4"]]


def _two_typed(
    column: str, case: str, swapped: bool, tags: tuple[str, ...], keys: tuple[int, int]
) -> tuple[ArrowTableStream, ArrowTableStream]:
    """Two inputs that declare ``_source_<column>`` with the case's two types.

    For ``column == "x"`` both inputs have the data columns ``x`` and ``w`` / ``v``
    (``x`` collides under ``MergeJoin``); otherwise their own data columns.
    """
    first, second = TWO_TYPES[case]
    if swapped:
        first, second = second, first
    data = {"a": ("x", "w"), "b": ("x", "v")} if column == "x" else DATA
    streams = []
    for name, kind, source_keys in (("a", first, (1, 2)), ("b", second, keys)):
        source = _source(name, tags, data=data[name], keys=source_keys)
        streams.append(
            _with_tokens(source, tags, {column: (_values(kind, name), kind)})
        )
    return streams[0], streams[1]


def _refusal(run: Any) -> str:
    """The message of the ``InputValidationError`` ``run`` raises.

    The async ``Join`` runs in a task group, which wraps the error it raises.
    """
    with pytest.raises((InputValidationError, ExceptionGroup)) as refused:
        run()
    error: BaseException = refused.value
    while isinstance(error, ExceptionGroup):
        (error,) = error.exceptions
    assert isinstance(error, InputValidationError)
    return str(error)


def _names_both_types(message: str, column: str, case: str) -> bool:
    """Whether a refusal names the column and both declared types."""
    from orcapod.utils import arrow_utils

    return f"'_source_{column}'" in message and all(
        arrow_utils.describe_arrow_type(kind) in message for kind in TWO_TYPES[case]
    )


@TAG_ORDER
@SIDES
@pytest.mark.parametrize("keys", [(1, 2), (7, 8)], ids=["keys_match", "no_match"])
@pytest.mark.parametrize("case", list(TWO_TYPES))
def test_merge_join_refuses_two_declarations_of_a_colliding_token_column(
    case: str, keys: tuple[int, int], swapped: bool, tags: str
) -> None:
    """``MergeJoin`` refuses, on both routes, inputs whose colliding column's token
    types differ, naming the column and both types, whether or not a key matches.

    On the base such a job completed: the sync edge had typed both token columns
    again from their values (a fixed-size list became a ``large_list``), so the
    declarations no longer differed when the operator saw them (review of PR
    #601, P2-3; the refusal is the developer's decision of November 27, 2026).
    """
    order = TAG_ORDERS[tags]
    left, right = _two_typed("x", case, swapped, order, keys)
    sync = _refusal(lambda: MergeJoin()(left, right).as_table())
    assert sync.startswith("MergeJoin") and _names_both_types(sync, "x", case)
    assert _refusal(lambda: _async_rows(MergeJoin(), left, right)) == sync
    for orchestrator in ORCHESTRATORS.values():
        job = PipelineJob(name="two_types", database=InMemoryArrowDatabase())
        with job:
            merged = MergeJoin()(
                _source("a", order, data=("x", "w")),
                _source("b", order, data=("x", "v"), keys=keys),
                label="merged",
            )
            add_pairs.pod(merged, label="f")
        job.bind(sources={"a": left, "b": right})
        refused = _refusal(lambda: job.run(orchestrator=orchestrator()))
        assert _names_both_types(refused, "x", case)
    assert calls["add_pairs"] == 0


@TAG_ORDER
@SIDES
@pytest.mark.parametrize("keys", [(1, 2), (7, 8)], ids=["keys_match", "no_match"])
@pytest.mark.parametrize("case", list(TWO_TYPES))
def test_join_refuses_two_declarations_of_a_merged_data_tag_token(
    case: str, keys: tuple[int, int], swapped: bool, tags: str
) -> None:
    """``Join`` refuses, on both routes, inputs whose shared data-tag's token types
    differ, naming the column and both types, whether or not a key matches.

    The sync route silently took the first input's type and the async route
    rebuilt the token from its value (review of PR #601, P1-2 and P2-3).
    """
    order = TAG_ORDERS[tags]
    left, right = _two_typed("k", case, swapped, order, keys)
    # The commutative Join names its inputs in no fixed order: both types are named.
    for refused in (
        _refusal(lambda: Join()(left, right).as_table()),
        _refusal(lambda: _async_rows(Join(), left, right)),
    ):
        assert refused.startswith("Join") and _names_both_types(refused, "k", case)
    for orchestrator in ORCHESTRATORS.values():
        job = PipelineJob(name="two_types", database=InMemoryArrowDatabase())
        with job:
            joined = Join()(
                _source("a", order), _source("b", order, keys=keys), label="joined"
            )
            add_two.pod(joined, label="f")
        job.bind(sources={"a": left, "b": right})
        refused = _refusal(lambda: job.run(orchestrator=orchestrator()))
        assert _names_both_types(refused, "k", case)
    assert calls["add_two"] == 0


# ---------------------------------------------------------------------------
# P2-4: record batches: item metadata is carried, declarations compared strictly
# ---------------------------------------------------------------------------

WITH_ITEM_METADATA = {
    "large_list": pa.large_list(pa.field("item", TOKEN, metadata={"m": "v"})),
    "fixed_size_list": pa.list_(pa.field("item", TOKEN, metadata={"m": "v"}), 2),
}


@TAG_ORDER
@pytest.mark.parametrize("kind", list(WITH_ITEM_METADATA))
def test_a_channel_carries_a_token_columns_item_metadata(kind: str, tags: str) -> None:
    """A row-built batch whose token column's item field has metadata is accepted.

    A token column keeps the type its stream declares, item-field metadata
    included, so the batch a node builds from such rows carries it. The channel
    refused it as "carrying field metadata" (review of PR #601, P2-4).
    """
    order = TAG_ORDERS[tags]
    declared = WITH_ITEM_METADATA[kind]
    stream = _with_tokens(
        _source("a", order), order, {"x": ([["p", "q"], ["r", "s"]], declared)}
    )
    built, tag_keys, data_tags = StaticOutputOperatorPod._rows_to_table(  # pyright: ignore[reportPrivateUsage]  # the batch a node builds from its rows
        list(stream.iter_data())
    )
    assert built.schema.field("_source_x").type.equals(declared, check_metadata=True)
    assert (tag_keys, data_tags) == (order, order)

    async def send() -> pa.Table:
        output = OutputChannel(stream)
        for batch in built.combine_chunks().to_batches(max_chunksize=1):
            await output.send(batch)
        await output.close()
        return output.result().as_table(columns={"source": True, "system_tags": True})

    result = asyncio.run(send())
    assert result.schema.field("_source_x").type.equals(declared, check_metadata=True)
    assert result.column("_source_x").to_pylist() == [["p", "q"], ["r", "s"]]


DIFFERING_DECLARATIONS = {
    "item_name": (
        pa.large_list(pa.field("member", TOKEN)),
        pa.large_list(pa.field("another", TOKEN)),
    ),
    "item_nullability": (
        pa.large_list(pa.field("item", TOKEN, nullable=False)),
        TOKENS,
    ),
    "item_metadata": (
        pa.large_list(pa.field("item", TOKEN, metadata={"m": "v"})),
        TOKENS,
    ),
    "list_kind": (pa.list_(TOKEN, 1), TOKENS),
    "depth": (pa.large_list(TOKENS), TOKENS),
}


@pytest.mark.parametrize("swapped", [False, True], ids=["as_listed", "swapped"])
@pytest.mark.parametrize("case", list(DIFFERING_DECLARATIONS))
def test_batches_declaring_two_token_types_are_refused_in_either_order(
    case: str, swapped: bool
) -> None:
    """Two batches of one stream declare one token type, compared strictly.

    A difference in the item field's name, nullability or metadata, in the list
    kind or in the depth is refused whichever batch comes first. Batches whose
    item fields had two names were put together under the first batch's name, so
    the order of the batches decided the result's type (review of PR #601, P2-4).
    """
    first, second = DIFFERING_DECLARATIONS[case]
    if swapped:
        first, second = second, first

    def batch(kind: pa.DataType, key: int) -> pa.RecordBatch:
        value: Any = [[]] if kind == pa.large_list(TOKENS) else ["t"]
        return pa.RecordBatch.from_arrays(
            [pa.array([key], pa.int64()), pa.array([value], kind)],
            schema=pa.schema([pa.field("k", pa.int64()), pa.field("_source_x", kind)]),
        )

    with pytest.raises(SourceInfoTypesDisagree, match="_source_x"):
        _ = concatenate_batches([batch(first, 1), batch(second, 2)])
    same = concatenate_batches([batch(first, 1), batch(first, 2)])
    assert same is not None
    assert same.schema.field("_source_x").type.equals(first, check_metadata=True)


@TAG_ORDER
def test_the_whole_table_form_drops_a_token_fields_own_metadata(tags: str) -> None:
    """The whole-table form equals the table built from rows for field metadata.

    A row builds each source-info field anew, so the field's own metadata is not
    carried; the whole-table form kept it, so the two forms of one node output
    differed (review of PR #601, P2-4). Item-field metadata, part of the column's
    type, is kept by both.
    """
    order = TAG_ORDERS[tags]
    kind = WITH_ITEM_METADATA["large_list"]
    stream = _with_tokens(
        _source("a", order), order, {"x": ([["p"], ["q", "r"]], kind)}
    )
    table = stream.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
    index = table.schema.get_field_index("_source_x")
    table = table.set_column(
        index,
        table.schema.field(index).with_metadata({"top": "level"}),
        table.column(index),
    )
    marked = ArrowTableStream(table, tag_columns=list(order), data_tags=list(order))
    built, _, _ = StaticOutputOperatorPod._rows_to_table(list(marked.iter_data()))  # pyright: ignore[reportPrivateUsage]  # the rows' form
    assert table.schema.field("_source_x").metadata == {b"top": b"level"}
    formed = StaticOutputOperatorPod._row_built_form(  # pyright: ignore[reportPrivateUsage]  # the whole-table form
        table, marked.keys()
    )
    assert formed.schema.equals(built.schema, check_metadata=True)
    assert not formed.schema.field("_source_x").metadata
    assert formed.schema.field("_source_x").type.equals(kind, check_metadata=True)


# ---------------------------------------------------------------------------
# P2-5: a row made with with_columns_of gains no token column
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("order", [("y", "z"), ("z", "y")])
def test_rows_built_by_hand_and_by_taking_columns_make_one_table(
    order: tuple[str, str],
) -> None:
    """A hand-built row and the same row made with ``with_columns_of`` concatenate.

    Neither row has a token for ``x`` or ``w``. The composed row gained null
    ``_source_x`` and ``_source_w`` columns the hand-built row does not have, so
    the two could not be built into one table (review of PR #601, P2-5).
    """
    from orcapod.core.datagrams import Tag

    tokens = {"y": "from-y", "z": ["z1", "z2"]}
    direct = Data(
        {**{name: 2 for name in order}, "x": 1, "w": 3},
        source_info={name: tokens[name] for name in order},
    )
    other = Data({name: 2 for name in order}, source_info={n: tokens[n] for n in order})
    composed = Data({"x": 1, "w": 3}).with_columns_of(other, list(order))
    expected = [f"_source_{name}" for name in order]
    for row in (direct, composed):
        shown = row.as_table(columns={"source": True})
        assert [n for n in shown.column_names if n.startswith("_source_")] == expected
    assert composed.as_table(columns={"source": True}).schema.equals(
        direct.as_table(columns={"source": True}).schema
    )
    built, _, _ = StaticOutputOperatorPod._rows_to_table(  # pyright: ignore[reportPrivateUsage]  # the sync edge's build
        [(Tag({"k": 1}), direct), (Tag({"k": 2}), composed)]
    )
    assert built.num_rows == 2
    assert [n for n in built.column_names if n.startswith("_source_")] == expected
    # For the instance id each data column has a token column, null when missing.
    assert composed.source_info_table().column_names == [
        *expected,
        "_source_x",
        "_source_w",
    ]
    assert composed.source_info_table().equals(
        direct.source_info_table().select(composed.source_info_table().column_names)
    )


# ---------------------------------------------------------------------------
# P2-6: a function pod's stream outside a job keeps its input's token types
# ---------------------------------------------------------------------------

NESTED = pa.large_list(TOKENS)
STREAM_TOKENS = {
    "k": ([["p", "q"], ["r", "s"]], FIXED),
    "g": ([[None, ["a"]], [[]]], NESTED),
}


@TAG_ORDER
def test_a_function_stream_outside_a_job_keeps_fallen_through_token_types(
    tags: str,
) -> None:
    """``pod(stream).as_table(columns={"source": True})`` keeps the tokens' types.

    Two data-tags fall through the function with a fixed-size-list token and a
    nested token holding a null and an empty list. The stream's table converted
    every source-info token from its Python value and raised ``ValueError:
    Unsupported Python type: <class 'list'>`` (review of PR #601, P2-6; it failed on
    the base too).
    """
    order = TAG_ORDERS[tags]
    stream = _with_tokens(_source("a", order), order, STREAM_TOKENS)

    @function_pod(output_keys="total")
    def add(x: int, w: int) -> int:
        return x + w

    out = add.pod(stream)
    table = out.as_table(columns={"source": True}).sort_by("k")
    assert table.column("total").to_pylist() == [110, 220]
    for name, (values, kind) in STREAM_TOKENS.items():
        field = table.schema.field(f"_source_{name}")
        assert field.type.equals(kind, check_metadata=True), field.type
        assert table.column(f"_source_{name}").to_pylist() == values
    assert table.schema.field("_source_total").type == TOKEN
    # The rows and the table agree.
    for _, data in out.iter_data():
        row = data.as_table(columns={"source": True})
        for name, (_, kind) in STREAM_TOKENS.items():
            assert row.schema.field(f"_source_{name}").type.equals(
                kind, check_metadata=True
            )


# ---------------------------------------------------------------------------
# P3: the Python schema of a token column follows its declared type
# ---------------------------------------------------------------------------


@TAG_ORDER
def test_the_python_schema_of_a_token_follows_its_columns_type(tags: str) -> None:
    """``schema(columns={"source": True})`` agrees with ``arrow_schema``.

    A token ``[None]`` in a column declared two list levels deep shows one level;
    the Python schema said ``list[str]`` while the Arrow schema had two levels
    (review of PR #601, P3). A null token in a list column is a list too.
    """
    order = TAG_ORDERS[tags]
    stream = _with_tokens(
        _source("a", order, data_tags=False),
        order,
        {"x": ([[None], [["t"]]], NESTED), "w": ([None, ["u"]], TOKENS)},
        data_tags=False,
    )
    for _, data in stream.iter_data():
        schema = data.schema(columns={"source": True})
        arrow = data.arrow_schema(columns={"source": True})
        assert schema["_source_x"] == list[list[str]]
        assert schema["_source_w"] == list[str]
        assert arrow.field("_source_x").type == NESTED
        assert arrow.field("_source_w").type == TOKENS
