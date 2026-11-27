"""Findings of the third independent review of PR #601 (ITL-1491), each as a test.

They are about the record-batch join kernels, the guards against a null inner
list, the order of the columns a refusal names, and the rule that a token's list
is never null at any level. The findings about the async orchestrator's check of
held rows went with that check, which was taken out of the PR (ITL-1534).
"""

from __future__ import annotations

import asyncio
from typing import Any

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.operators import Join, MergeJoin
from orcapod.core.operators.join import NullTokenListError, null_token_list_error
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from tests.test_core.operators.test_streaming_joins import (
    _join_kernel,  # pyright: ignore[reportPrivateUsage]  # the shared kernel fixtures
    _merge_kernel,  # pyright: ignore[reportPrivateUsage]
    _sent,  # pyright: ignore[reportPrivateUsage]
)
from tests.test_pipeline.test_token_types_review_2 import (
    NESTED,
    _keyed,  # pyright: ignore[reportPrivateUsage]  # the shared stream fixtures
)

STR = pa.large_string()
TOKENS = pa.large_list(STR)


def _unwrapped(error: BaseException) -> BaseException:
    while isinstance(error, ExceptionGroup):
        (error,) = error.exceptions
    return error


# ---------------------------------------------------------------------------
# 3. The record-batch join kernels name the column of a null inner list
# ---------------------------------------------------------------------------


KEY_OF_LISTS = pa.large_list(pa.large_list(pa.int64()))
KEYS = [[[1], [2]], [[3], [4]]]
WHOLE = [[["p"], ["a"]], [["q"], ["b"]]]
HOLED = [None, ["a"]]


def _merge_inputs(tags: tuple[str, ...]) -> tuple[ArrowTableStream, ArrowTableStream]:
    """Two valid streams that share the data-tag ``k``, a list of lists."""
    left = _keyed(("x", "w"), KEYS, WHOLE, NESTED, tags, key_type=KEY_OF_LISTS)
    right = _keyed(("y", "v"), KEYS, WHOLE, NESTED, tags, key_type=KEY_OF_LISTS)
    return left, right


def _rows_with_a_null_inner_list(stream: ArrowTableStream) -> list[Any]:
    """The stream's rows, the token of ``k`` replaced by one no stream can hold."""
    return [
        (tag, data.with_source_info(k=pa.scalar(HOLED, type=NESTED)))
        for tag, data in stream.iter_data()
    ]


def _over_row_channels(operator: Any, streams: Any, rows: Any) -> list[Any]:
    """``operator.async_execute`` over bare row channels fed ``rows``."""

    async def run() -> list[Any]:
        channels: list[Channel[Any]] = [Channel(buffer_size=16) for _ in rows]
        output: Channel[Any] = Channel(buffer_size=16)
        for channel, sent in zip(channels, rows):
            for row in sent:
                await channel.writer.send(row)
            await channel.writer.close()
        await operator.async_execute(
            [channel.reader for channel in channels],
            output.writer,
            input_pipeline_hashes=[s.pipeline_hash() for s in streams],
        )
        return await output.reader.collect()

    return asyncio.run(run())


@pytest.mark.parametrize(
    ("operator", "kernel"),
    [(Join, _join_kernel), (MergeJoin, _merge_kernel)],
    ids=["join", "merge"],
)
@pytest.mark.parametrize("tags", [("k", "g"), ("g", "k")], ids=["k_g", "g_k"])
def test_a_kernel_guards_against_a_null_inner_list_naming_the_column(
    operator: Any, kernel: Any, tags: tuple[str, ...]
) -> None:
    """The kernels merge a shared data-tag's tokens with ``Join``'s routine. A
    null where a list of tokens is declared raised the routine's own blank
    ``NullTokenListError`` out of ``push`` (third review of PR #601, finding 3).

    No stream holds such a token (a token's list is never null at any level), so
    this is a guard: the batches are built by hand, which a channel does not
    check. The kernel words the refusal as its operator does."""
    left, right = _merge_inputs(tags)
    with pytest.raises(InputValidationError, match="list level 1"):
        _ = _keyed(
            ("x", "w"), KEYS, [HOLED, WHOLE[1]], NESTED, tags, key_type=KEY_OF_LISTS
        )

    def holed(table: pa.Table) -> pa.Table:
        index = table.schema.get_field_index("_source_k")
        return table.set_column(
            index,
            table.schema.field(index),
            pa.array([HOLED] * table.num_rows, NESTED),
        )

    pushing = kernel([left, right])
    with pytest.raises(InputValidationError) as refused:
        for batch in holed(_sent(left)).to_batches():
            pushing.push(0, batch)
        for batch in _sent(right).to_batches():
            pushing.push(1, batch)
        pushing.finish()
    assert str(refused.value) == str(
        null_token_list_error(operator.__name__, "_source_k", NESTED)
    )
    assert "'_source_k'" in str(refused.value)


@pytest.mark.parametrize("operator", [Join, MergeJoin], ids=["join", "merge"])
@pytest.mark.parametrize("side", [0, 1], ids=["left", "right"])
@pytest.mark.parametrize("tags", [("k", "g"), ("g", "k")], ids=["k_g", "g_k"])
def test_the_row_channel_operators_guard_against_a_null_inner_list(
    operator: Any, side: int, tags: tuple[str, ...]
) -> None:
    """Rows built by hand and sent over a bare row channel are in no stream, so a
    null inner list can still reach the merge of a shared data-tag's tokens. The
    operator refuses with ``InputValidationError`` naming the token column."""
    streams = _merge_inputs(tags)
    rows = [list(stream.iter_data()) for stream in streams]
    rows[side] = _rows_with_a_null_inner_list(streams[side])
    with pytest.raises((InputValidationError, ExceptionGroup)) as refused:
        _ = _over_row_channels(operator(), streams, rows)
    error = _unwrapped(refused.value)
    assert isinstance(error, InputValidationError), repr(error)
    assert "'_source_k'" in str(error)
    # The same rows without the null merge on that route.
    merged = _over_row_channels(
        operator(), streams, [list(s.iter_data()) for s in streams]
    )
    assert len(merged) == 2


def test_the_merge_routine_names_what_it_found() -> None:
    """The guard under ``Join``, ``MergeJoin`` and both kernels: the routine that
    merges two token columns raises its own named error, with a message, for a
    null where a list of tokens is declared; each caller words it for the user."""
    table = pa.table(
        {
            "k": pa.array(KEYS, KEY_OF_LISTS),
            "_source_k": pa.array([HOLED, WHOLE[1]], NESTED),
            "_merged_k": pa.array(WHOLE, NESTED),
        }
    )
    for left, right in (("_source_k", "_merged_k"), ("_merged_k", "_source_k")):
        with pytest.raises(NullTokenListError, match="null where a list of tokens"):
            _ = Join._merge_source_columns(table, left, right)  # pyright: ignore[reportPrivateUsage]  # the guard is the subject
    for operator in ("Join", "MergeJoin"):
        worded = null_token_list_error(operator, "_source_k", NESTED)
        assert isinstance(worded, InputValidationError)
        assert str(worded).startswith(operator) and "'_source_k'" in str(worded)


# ---------------------------------------------------------------------------
# 4. The violating columns are named in name order
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("columns", [("x", "y"), ("y", "x")], ids=["x_y", "y_x"])
def test_a_stream_names_its_violating_columns_in_name_order(
    columns: tuple[str, str],
) -> None:
    """Two token columns break the rule; whatever the table's column order, the
    refusal lists them by name (third review of PR #601, finding 4a)."""
    lists = pa.large_list(pa.int64())
    held: dict[str, Any] = {"k": pa.array([1, 2], pa.int64())}
    for name in columns:
        held[name] = pa.array([[1], [2]], lists)
    for name in columns:
        held[f"_source_{name}"] = pa.array([None, ["a"]], TOKENS)
    with pytest.raises(InputValidationError) as refused:
        _ = ArrowTableStream(pa.table(held), tag_columns=["k"])
    message = str(refused.value)
    assert message.index("'_source_x'") < message.index("'_source_y'")


# ---------------------------------------------------------------------------
# 5. A token's list is never null at any level (developer, November 27, 2026)
# ---------------------------------------------------------------------------

INTS = pa.large_list(pa.int64())
TWO = pa.large_list(TOKENS)
THREE = pa.large_list(TWO)
FIXED_OF_LISTS = pa.list_(TOKENS, 2)

# name: (token type, tokens of two rows, data type, data, the level of the null list)
NULL_LISTS: dict[str, tuple[pa.DataType, list[Any], pa.DataType, list[Any], int]] = {
    "level_1_of_2": (
        TWO,
        [[["a"], ["b"]], [None, ["c"]]],
        pa.large_list(INTS),
        [[[1], [2]], [[3], [4]]],
        1,
    ),
    "level_1_of_3": (
        THREE,
        [[[["a"]]], [None]],
        pa.large_list(pa.large_list(INTS)),
        [[[[1]]], [[[2]]]],
        1,
    ),
    "level_2_of_3": (
        THREE,
        [[[["a"]]], [[None, ["b"]]]],
        pa.large_list(pa.large_list(INTS)),
        [[[[1]]], [[[2], [3]]]],
        2,
    ),
    "level_1_of_fixed": (
        FIXED_OF_LISTS,
        [[["a"], ["b"]], [["c"], None]],
        pa.list_(INTS, 2),
        [[[1], [2]], [[3], [4]]],
        1,
    ),
}

# The same shapes with a null only where the element is a string: valid.
NULL_STRINGS: dict[str, tuple[pa.DataType, list[Any], pa.DataType, list[Any]]] = {
    "string_of_2": (
        TWO,
        [[["a", None], []], [[None]]],
        pa.large_list(INTS),
        [[[1, 2], []], [[3]]],
    ),
    "string_of_3": (
        THREE,
        [[[[None]]], [[["a", None], []]]],
        pa.large_list(pa.large_list(INTS)),
        [[[[1]]], [[[2, 3], []]]],
    ),
    "string_of_fixed": (
        FIXED_OF_LISTS,
        [[[None], []], [["c", None], [None]]],
        pa.list_(INTS, 2),
        [[[1], []], [[3, 4], [5]]],
    ),
}


def _shaped(
    token_type: pa.DataType,
    tokens: list[Any],
    data_type: pa.DataType,
    data: list[Any],
    role: str,
    layout: str,
) -> ArrowTableStream:
    """Tags ``g`` and ``k``; data ``x`` (the checked column, a data-tag in that
    role) and ``w``; in two table layouts."""
    rows = len(data)
    columns: dict[str, Any] = {
        "g": pa.array([f"g{i}" for i in range(rows)], STR),
        "k": pa.array(list(range(rows)), pa.int64()),
        "x": pa.array(data, data_type),
        "w": pa.array([10 * i for i in range(rows)], pa.int64()),
        "_source_x": pa.array(tokens, token_type),
        "_source_w": pa.array([f"s::row_{i}::w" for i in range(rows)], STR),
    }
    names = list(columns) if layout == "tags_first" else list(reversed(columns))
    data_tag = role == "data_tag"
    return ArrowTableStream(
        pa.table({name: columns[name] for name in names}),
        tag_columns=["g", "k", "x"] if data_tag else ["g", "k"],
        data_tags=["x"] if data_tag else [],
    )


ROLES = pytest.mark.parametrize("role", ["data", "data_tag"])
LAYOUTS = pytest.mark.parametrize("layout", ["tags_first", "reversed"])


@ROLES
@LAYOUTS
@pytest.mark.parametrize("case", list(NULL_LISTS))
def test_a_stream_with_a_null_list_inside_a_token_is_refused(
    case: str, role: str, layout: str
) -> None:
    """A token's list is never null at any level; a null is allowed only at the
    innermost level, where the element is a string. The stream is refused where it
    is built, naming the token column, the data column, both types and the level."""
    token_type, tokens, data_type, data, level = NULL_LISTS[case]
    with pytest.raises(InputValidationError) as refused:
        _ = _shaped(token_type, tokens, data_type, data, role, layout)
    message = str(refused.value)
    assert "'_source_x'" in message and "'x'" in message
    assert str(token_type) in message or "large_list" in message
    assert f"list level {level}" in message
    # With no rows the same types make a stream: the rule reads validity only.
    empty = _shaped(token_type, [], data_type, [], role, layout)
    assert empty.as_table().num_rows == 0


@ROLES
@LAYOUTS
@pytest.mark.parametrize("case", list(NULL_STRINGS))
def test_a_null_string_inside_a_token_is_valid_at_any_depth(
    case: str, role: str, layout: str
) -> None:
    token_type, tokens, data_type, data = NULL_STRINGS[case]
    stream = _shaped(token_type, tokens, data_type, data, role, layout)
    shown = stream.as_table(columns={"source": True})
    assert shown.column("_source_x").to_pylist() == tokens
    assert [d.source_info()["x"] for _, d in stream.iter_data()] == tokens


def test_a_table_cut_into_chunks_is_checked_in_every_chunk() -> None:
    """The null list is in the second chunk of the token column, behind a slice."""
    token_type, tokens, data_type, data, _ = NULL_LISTS["level_1_of_2"]
    one = pa.table(
        {"x": pa.array(data, data_type), "_source_x": pa.array(tokens, token_type)}
    )
    chunked = pa.concat_tables([one.slice(0, 1), one.slice(1, 1)])
    assert chunked.column("_source_x").num_chunks == 2
    with pytest.raises(InputValidationError, match="list level 1"):
        _ = ArrowTableStream(chunked)
    # The valid first row alone, as a slice of the same table, is a stream.
    assert ArrowTableStream(one.slice(0, 1)).as_table().num_rows == 1
