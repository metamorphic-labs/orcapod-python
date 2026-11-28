"""Findings of the second independent review of PR #601 (ITL-1491), each as a test.

The streams here satisfy the token-shape rule unless a test says otherwise: a
data-tag key ``k`` that is a list has a list token or a plain string token. Every
stream has two tags and two data columns; the operators are run directly on both
routes (sync call, async row channels), in two tag orders.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.channels import Channel
from orcapod.core.operators import Batch, Join, MergeJoin
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator

STR = pa.large_string()
TOKENS = pa.large_list(STR)
NESTED = pa.large_list(TOKENS)
INTS = pa.large_list(pa.int64())
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}
TAG_ORDERS = {"k_then_g": ("k", "g"), "g_then_k": ("g", "k")}
TAG_ORDER = pytest.mark.parametrize("tags", list(TAG_ORDERS))
OPERATORS = pytest.mark.parametrize(
    "operator", [Join, MergeJoin], ids=["join", "merge"]
)


def _keyed(
    data: tuple[str, str],
    keys: list[Any],
    tokens: list[Any],
    token_type: pa.DataType,
    tags: tuple[str, ...],
    *,
    key_type: pa.DataType = INTS,
    data_tag: bool = True,
) -> ArrowTableStream:
    """A raw stream: tags ``k`` (a list key) and ``g``, two integer data columns.

    ``k`` is a data-tag with the given token column unless ``data_tag`` is false.
    """
    first, second = data
    columns: dict[str, Any] = {
        "k": pa.array(keys, key_type),
        "g": pa.array(["m"] * len(keys), STR),
        first: pa.array(list(range(len(keys))), pa.int64()),
        second: pa.array([10 * i for i in range(len(keys))], pa.int64()),
    }
    names = [*tags, first, second]
    if data_tag:
        columns["_source_k"] = pa.array(tokens, token_type)
        names.append("_source_k")
    return ArrowTableStream(
        pa.table({name: columns[name] for name in names}),
        tag_columns=list(tags),
        data_tags=["k"] if data_tag else [],
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


def _outcome(run: Any) -> tuple[str, Any]:
    """What ``run`` gives: ``("rows", tokens of k)`` or ``(error type, message)``."""
    try:
        rows = run()
    except (Exception, ExceptionGroup) as error:  # noqa: BLE001  # the outcome is compared
        while isinstance(error, ExceptionGroup):
            (error,) = error.exceptions
        return (type(error).__name__, str(error))
    tokens = []
    for _, data in rows:
        table = data.as_table(columns={"source": True})
        tokens.append(
            {
                field.name: (field.type, table.column(field.name).to_pylist())
                for field in table.schema
                if field.name.startswith("_source_")
            }
        )
    return ("rows", tokens)


def _both_routes(
    operator: Any, *streams: Any
) -> tuple[tuple[str, Any], tuple[str, Any]]:
    """The outcome of ``operator`` over ``streams`` on the sync and the async route."""
    return (
        _outcome(lambda: list(operator()(*streams).iter_data())),
        _outcome(lambda: _async_rows(operator(), *streams)),
    )


def _refused_alike(
    outcomes: tuple[tuple[str, Any], tuple[str, Any]], column: str, *kinds: pa.DataType
) -> None:
    """Both routes refuse with ``InputValidationError`` naming the column and types."""
    from orcapod.utils import arrow_utils

    for kind, message in outcomes:
        assert kind == "InputValidationError", (kind, message)
        assert f"'{column}'" in message
        for token_type in kinds:
            assert arrow_utils.describe_arrow_type(token_type) in message


# ---------------------------------------------------------------------------
# 3c. MergeJoin checks a merged data-tag's token types before it renames one
# ---------------------------------------------------------------------------


@TAG_ORDER
@pytest.mark.parametrize("swapped", [False, True], ids=["list_left", "list_right"])
@pytest.mark.parametrize("match", [True, False], ids=["keys_match", "no_match"])
@OPERATORS
def test_a_list_token_against_a_string_token_of_a_shared_data_tag_is_refused(
    operator: Any, match: bool, swapped: bool, tags: str
) -> None:
    """One input declares a list token for the shared data-tag, the other a string.

    Both are valid over the list-valued key. ``MergeJoin`` renamed the second
    input's token column before it compared the inputs' types, so it completed
    when no key matched and raised a ``ValueError`` about merging a list with a
    scalar when one did (second review of PR #601, finding 3c). Both operators
    refuse on both routes, whether or not a key matches.
    """
    order = TAG_ORDERS[tags]
    listed = _keyed(("x", "w"), [[1], [2]], [["a"], ["b"]], TOKENS, order)
    plain = _keyed(
        ("y", "v"), [[1], [2]] if match else [[7], [8]], ["c", "d"], STR, order
    )
    inputs = (plain, listed) if swapped else (listed, plain)
    outcomes = _both_routes(operator, *inputs)
    _refused_alike(outcomes, "_source_k", TOKENS, STR)
    assert outcomes[0][1].startswith(operator.__name__)


# ---------------------------------------------------------------------------
# 3a. A Join of three inputs decides from every input's declaration
# ---------------------------------------------------------------------------


@TAG_ORDER
def test_a_three_input_join_refuses_two_declarations_when_no_row_matches(
    tags: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Three inputs with disjoint keys; the third declares another token type.

    The sync path compares the inputs' token columns before it joins. The async
    chain compared a step's two sides when each had sent a row, and the first
    step's result has no row here, so the last step never compared and the join
    completed with no row (second review of PR #601, finding 3a). Each input's
    first row now gives its declaration to all the others.
    """
    order = TAG_ORDERS[tags]
    inputs = [
        _keyed(("a", "b"), [[1]], [["m"]], TOKENS, order),
        _keyed(("p", "q"), [[2]], [["n"]], TOKENS, order),
        _keyed(("c", "d"), [[3]], ["o"], STR, order),
    ]
    hashes = sorted(
        (stream.pipeline_hash() for stream in inputs), key=lambda h: h.to_hex()
    )
    for stream, pipeline_hash in zip(inputs, hashes, strict=True):
        monkeypatch.setattr(stream, "pipeline_hash", lambda h=pipeline_hash: h)
    for rotation in range(3):
        rotated = inputs[rotation:] + inputs[:rotation]
        assert Join().order_input_streams(*rotated) == inputs
        _refused_alike(_both_routes(Join, *rotated), "_source_k", TOKENS, STR)


@function_pod(output_keys="total")
def add_two(x: int, y: int) -> int:
    """Return ``x + y``."""
    return x + y


@TAG_ORDER
@pytest.mark.parametrize("empty", ["a", "b"])
def test_a_join_job_with_an_empty_input_refuses_two_declarations_on_both_routes(
    empty: str, tags: str
) -> None:
    """In a job the declarations are the nodes': an input with no row has one too.

    One slot is bound to a stream with no rows that declares a string token, the
    other to rows with list tokens. Both orchestrators refuse.
    """
    order = TAG_ORDERS[tags]

    def source(name: str, data: tuple[str, str]) -> ArrowTableSource:
        names = [*order, *data]
        columns = {
            "k": pa.array([[1], [2]], INTS),
            "g": pa.array(["m", "n"], STR),
            data[0]: pa.array([10, 20], pa.int64()),
            data[1]: pa.array([1, 2], pa.int64()),
        }
        schema = pa.schema(pa.field(n, columns[n].type, nullable=False) for n in names)
        return ArrowTableSource(
            pa.Table.from_arrays([columns[n] for n in names], schema=schema),
            tag_columns=list(order),
            tag_only_columns=False,
            source_id=name,
        )

    def bound(name: str, data: tuple[str, str]) -> ArrowTableStream:
        table = source(name, data).as_table(
            columns={"source": True, "system_tags": True}
        )
        if name == empty:
            return ArrowTableStream(
                table.slice(0, 0), tag_columns=list(order), data_tags=list(order)
            )
        table = table.set_column(
            table.schema.get_field_index("_source_k"),
            pa.field("_source_k", TOKENS),
            pa.array([["p"], ["q"]], TOKENS),
        )
        return ArrowTableStream(table, tag_columns=list(order), data_tags=list(order))

    for orchestrator in ORCHESTRATORS.values():
        job = PipelineJob(name="empty_input", database=InMemoryArrowDatabase())
        with job:
            joined = Join()(source("a", ("x", "w")), source("b", ("y", "v")), label="j")
            add_two.pod(joined, label="f")
        job.bind(sources={"a": bound("a", ("x", "w")), "b": bound("b", ("y", "v"))})
        kind, message = _outcome(lambda: job.run(orchestrator=orchestrator()) and [])
        assert kind == "InputValidationError", (kind, message)
        assert "'_source_k'" in message


# ---------------------------------------------------------------------------
# 3b. Item-field differences between two inputs: the routes agree (ITL-1519)
# ---------------------------------------------------------------------------

ITEM_FIELDS = {
    "names": (
        pa.large_list(pa.field("a", STR)),
        pa.large_list(pa.field("b", STR)),
    ),
    "metadata": (
        pa.large_list(pa.field("member", STR, metadata={"m": "a"})),
        pa.large_list(pa.field("member", STR, metadata={"m": "b"})),
    ),
}


@TAG_ORDER
@pytest.mark.parametrize("swapped", [False, True], ids=["as_listed", "swapped"])
@pytest.mark.parametrize("case", list(ITEM_FIELDS))
@OPERATORS
def test_item_field_differences_of_a_merged_token_give_one_result_on_both_routes(
    operator: Any, case: str, swapped: bool, tags: str
) -> None:
    """Two inputs' list tokens differ only in the item field's name or metadata.

    No refusal is added for this (the treatment of a list layer's item field is
    ITL-1519). What must hold today: the sync and the async route give the same
    rows, with the same token column type, item field included.
    """
    order = TAG_ORDERS[tags]
    first, second = ITEM_FIELDS[case]
    if swapped:
        first, second = second, first
    left = _keyed(("x", "w"), [[1], [2]], [["a"], ["b"]], first, order)
    right = _keyed(("y", "v"), [[1], [2]], [["c"], ["d"]], second, order)
    sync, asynchronous = _both_routes(operator, left, right)
    assert sync[0] == asynchronous[0] == "rows", (sync, asynchronous)
    assert len(sync[1]) == len(asynchronous[1]) == 2
    key = lambda row: repr(sorted((n, v) for n, (_, v) in row.items()))  # noqa: E731
    for mine, theirs in zip(
        sorted(sync[1], key=key), sorted(asynchronous[1], key=key), strict=True
    ):
        assert mine.keys() == theirs.keys()
        for name in mine:
            assert mine[name][0].equals(theirs[name][0], check_metadata=True), name
            assert mine[name][1] == theirs[name][1]


# ---------------------------------------------------------------------------
# 4. A null inner list in a merged token is refused, not a guard's RuntimeError
# ---------------------------------------------------------------------------


@TAG_ORDER
@pytest.mark.parametrize("side", ["both", "left", "right"])
@OPERATORS
def test_a_null_inner_list_in_a_merged_token_is_refused_naming_the_column(
    operator: Any, side: str, tags: str
) -> None:
    """A two-level token ``[None, ["a"]]`` over a list-of-lists data-tag key.

    ``Join`` and ``MergeJoin`` reached their guard's ``RuntimeError`` on such a
    token (second review of PR #601, finding 4; the base stored the characters of
    an internal marker). The developer then decided (November 27, 2026) that a
    token's list is never null at any level, so the stream that holds one is
    refused where it is built, naming the token column and the level, and the
    operators never see it. Their refusal stays as a guard for rows that are in
    no stream (``test_token_types_review_3.py``).
    """
    from orcapod.errors import InputValidationError

    order = TAG_ORDERS[tags]
    keys = [[[1], [2]], [[3], [4]]]
    key_type = pa.large_list(INTS)
    whole = [[["p"], ["a"]], [["q"], ["b"]]]
    holed = [[None, ["a"]], [["q"], ["b"]]]
    for data, tokens in (
        (("x", "w"), holed if side != "right" else whole),
        (("y", "v"), holed if side != "left" else whole),
    ):
        if tokens is whole:
            continue
        with pytest.raises(InputValidationError) as refused:
            _ = _keyed(data, keys, tokens, NESTED, order, key_type=key_type)
        message = str(refused.value)
        assert "'_source_k'" in message and "'k'" in message
        assert "list level 1" in message
    # Without the null, the same inputs merge, with the declared two levels.
    full = _keyed(("x", "w"), keys, whole, NESTED, order, key_type=key_type)
    other = _keyed(("y", "v"), keys, whole, NESTED, order, key_type=key_type)
    sync, asynchronous = _both_routes(operator, full, other)
    assert sync[0] == asynchronous[0] == "rows"
    assert len(sync[1]) == len(asynchronous[1]) == 2
    assert all(row["_source_k"][0] == NESTED for row in sync[1] + asynchronous[1])


# ---------------------------------------------------------------------------
# 5. The async Join folds only the tokens it merged
# ---------------------------------------------------------------------------

PENDING_LOOKALIKE = "\x00join-pending-members\x00[]"


@TAG_ORDER
@pytest.mark.parametrize("inputs", [2, 3])
def test_a_token_that_looks_like_the_joins_pending_marker_is_left_alone(
    inputs: int, tags: str
) -> None:
    """A data column's own token that starts like the join's internal marker.

    The join never merged that column, so its token is the user's and is kept. The
    async route folded every carried token with that prefix into a ``join_v1:``
    digest, changing provenance on one route only (second review of PR #601,
    finding 5; on the base too).
    """
    order = TAG_ORDERS[tags]

    def stream(data: tuple[str, str], token: str) -> ArrowTableStream:
        table = pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                "g": pa.array(["m", "n"], STR),
                data[0]: pa.array([10, 20], pa.int64()),
                data[1]: pa.array([1, 2], pa.int64()),
                f"_source_{data[0]}": pa.array([token, "plain"], STR),
                f"_source_{data[1]}": pa.array(["t0", PENDING_LOOKALIKE], STR),
            }
        )
        return ArrowTableStream(table, tag_columns=list(order))

    streams = [
        stream(("x", "w"), PENDING_LOOKALIKE),
        stream(("y", "v"), "s::0::y"),
        stream(("z", "u"), PENDING_LOOKALIKE),
    ][:inputs]
    sync, asynchronous = _both_routes(Join, *streams)
    assert sync[0] == asynchronous[0] == "rows", (sync, asynchronous)
    key = lambda row: repr(sorted((n, v) for n, (_, v) in row.items()))  # noqa: E731
    assert sorted(sync[1], key=key) == sorted(asynchronous[1], key=key)
    seen = {
        token
        for row in asynchronous[1]
        for _, values in row.values()
        for token in values
    }
    assert PENDING_LOOKALIKE in seen
    assert not any(token.startswith("join_v1:") for token in seen)


# ---------------------------------------------------------------------------
# 7. MergeJoin reports the first mismatch in name order, whatever the hash seed
# ---------------------------------------------------------------------------

_SEEDED = """
import warnings
warnings.simplefilter("ignore")
import pyarrow as pa
from orcapod.core.operators.merge_join import _output_types
from orcapod.errors import InputValidationError
s = pa.large_string()
left = {"k": pa.int64(), "x": pa.list_(pa.int64(), 2), "_source_x": pa.list_(s, 2),
        "y": pa.int8(), "_source_y": s}
right = {"k": pa.int64(), "x": pa.large_list(pa.int64()), "_source_x": pa.large_list(s),
         "y": pa.int16(), "_source_y": s}
try:
    _output_types(left, right)
except InputValidationError as error:
    print(str(error).split("column ")[1].split(":")[0])
"""


def test_merge_join_reports_the_first_mismatch_in_name_order_under_any_hash_seed() -> (
    None
):
    """Three columns disagree; the one first in name order is reported.

    ``MergeJoin`` compared its inputs' columns in the order of a set of names, so
    which disagreement it reported followed ``PYTHONHASHSEED`` (second review of
    PR #601, finding 7; on the base too). A set of names is held sorted.
    """
    reported = set()
    for seed in ("0", "1", "2", "3", "7"):
        result = subprocess.run(
            [sys.executable, "-c", _SEEDED],
            env={**os.environ, "PYTHONHASHSEED": seed},
            capture_output=True,
            text=True,
            check=True,
        )
        reported.add(result.stdout.strip())
    assert reported == {"'_source_x'"}


# ---------------------------------------------------------------------------
# 8. Ordinary sources can declare two token types for one column
# ---------------------------------------------------------------------------


def _ordinary_inputs(
    operator: Any, tags: tuple[str, ...], keys: tuple[int, int] = (1, 2)
) -> tuple[Any, Any, Any, Any]:
    """A ``Batch(1)`` of a source of scalars, and a source that holds lists.

    Both give the same list-valued data (``k`` and ``g`` for ``Join``, where they
    are data-tags; the colliding column ``x`` for ``MergeJoin``). The batch's
    tokens are lists of tokens; the source's are plain strings.

    Returns:
        The batched stream, the list-holding source, and the two root sources.
    """
    plain_tags = operator is MergeJoin

    def source(name: str, columns: dict[str, Any], data: tuple[str, ...]) -> Any:
        names = [*tags, *data]
        schema = pa.schema(pa.field(n, columns[n].type, nullable=False) for n in names)
        return ArrowTableSource(
            pa.Table.from_arrays([columns[n] for n in names], schema=schema),
            tag_columns=list(tags),
            tag_only_columns=plain_tags,
            source_id=name,
        )

    scalars = source(
        "scalars",
        {
            "k": pa.array([1, 2], pa.int64()),
            "g": pa.array(["m", "n"], STR),
            "x": pa.array([10, 20], pa.int64()),
            "w": pa.array([1, 2], pa.int64()),
        },
        ("x", "w"),
    )
    second = ("x", "v") if plain_tags else ("y", "v")
    lists = source(
        "lists",
        {
            "k": pa.array([[key] for key in keys], INTS),
            "g": pa.array([["m"], ["n"]], pa.large_list(STR)),
            second[0]: pa.array([[30], [40]], INTS),
            second[1]: pa.array([[3], [4]], INTS),
        },
        second,
    )
    return Batch(batch_size=1)(scalars), lists, scalars, lists


@TAG_ORDER
@pytest.mark.parametrize("swapped", [False, True], ids=["batch_first", "source_first"])
@pytest.mark.parametrize("keys", [(1, 2), (7, 8)], ids=["keys_match", "no_match"])
@OPERATORS
def test_a_batch_of_scalars_against_a_source_of_lists_is_refused_on_both_routes(
    operator: Any, keys: tuple[int, int], swapped: bool, tags: str
) -> None:
    """Two token types for one column, from ordinary sources (no hand-built token).

    ``Batch(1)`` over a source of scalars gives list data with LIST tokens (one
    token per member). A source that already holds the same list-shaped data has
    plain STRING tokens (one per value). Their data types are equal, their token
    types are not: ``Join`` on the shared data-tags and ``MergeJoin`` on the
    colliding column refuse, on both routes, naming the token column and both
    types (second review of PR #601, finding 8; the refusal is the developer's
    decision of November 27, 2026).
    """
    order = TAG_ORDERS[tags]
    batched, lists, _, _ = _ordinary_inputs(operator, order, keys)
    inputs = (lists, batched) if swapped else (batched, lists)
    column = "_source_x" if operator is MergeJoin else "_source_"
    outcomes = _both_routes(operator, *inputs)
    for kind, message in outcomes:
        assert kind == "InputValidationError", (kind, message)
        assert message.startswith(operator.__name__) and column in message
        assert "large_list<item: large_string>" in message
        assert "large_string in" in message or "and large_string" in message
    if operator is MergeJoin:
        assert outcomes[0][1] == outcomes[1][1]


@function_pod(output_keys="n")
def count_lists(x: list[int], y: list[int]) -> int:
    """Return how many values the two lists hold."""
    return len(x) + len(y)


@TAG_ORDER
@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_job_joining_a_batch_of_scalars_with_a_source_of_lists_is_refused(
    orchestrator: str, tags: str
) -> None:
    """The same case in a job, on each orchestrator: refused, nothing computed.

    Before ITL-1491 this job failed too, with ``ValueError: cannot merge the source
    info of a list with that of a scalar`` when a key matched.
    """
    order = TAG_ORDERS[tags]
    _, _, scalars, lists = _ordinary_inputs(Join, order)
    job = PipelineJob(name="ordinary", database=InMemoryArrowDatabase())
    with job:
        joined = Join()(Batch(batch_size=1)(scalars), lists, label="joined")
        count_lists.pod(joined, label="f")
    kind, message = _outcome(
        lambda: job.run(orchestrator=ORCHESTRATORS[orchestrator]()) and []
    )
    assert kind == "InputValidationError", (kind, message)
    assert message.startswith("Join") and "_source_" in message
    assert "large_list<item: large_string>" in message
