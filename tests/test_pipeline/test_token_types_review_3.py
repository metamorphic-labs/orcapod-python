"""Findings of the third independent review of PR #601 (ITL-1491), each as a test.

They are about the rows a run holds rather than runs (the async orchestrator sends
them on as they are, the sync one builds them into a stream where a successor
reads them) and about the record-batch join kernels.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.datagrams import Data, Tag
from orcapod.core.operators import Join, MergeJoin
from orcapod.core.operators.join import NullTokenListError, null_token_list_error
from orcapod.core.operators.static_output_pod import rows_on_read
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline.async_orchestrator import (
    _send_held,  # pyright: ignore[reportPrivateUsage]  # the held-row check is the subject
)
from orcapod.pipeline.dag import OrcaDAG
from orcapod.utils import arrow_utils, schema_utils
from tests.test_core.operators.test_streaming_joins import (
    _join_kernel,  # pyright: ignore[reportPrivateUsage]  # the shared kernel fixtures
    _merge_kernel,  # pyright: ignore[reportPrivateUsage]
    _sent,  # pyright: ignore[reportPrivateUsage]
)
from tests.test_pipeline.test_rows_on_read import (
    _held_job,  # pyright: ignore[reportPrivateUsage]  # the shared held-node fixtures
    _stored_by_after,  # pyright: ignore[reportPrivateUsage]
    _unconvertible,  # pyright: ignore[reportPrivateUsage]
)
from tests.test_pipeline.test_token_types_review_2 import (
    NESTED,
    _keyed,  # pyright: ignore[reportPrivateUsage]  # the shared stream fixtures
)

STR = pa.large_string()
TOKENS = pa.large_list(STR)
ROUTES = pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])


def _sent_on(rows: list[Any], *, read: bool = True) -> list[Any]:
    """Send held rows as the async orchestrator does; return what was sent."""

    async def run() -> list[Any]:
        channel: Channel[Any] = Channel(buffer_size=len(rows) + 1)
        await _send_held(rows, channel.writer, read=read)
        return await channel.reader.collect()

    return asyncio.run(run())


def _unwrapped(error: BaseException) -> BaseException:
    while isinstance(error, ExceptionGroup):
        (error,) = error.exceptions
    return error


# ---------------------------------------------------------------------------
# 1. Held rows whose token type has a null leaf
# ---------------------------------------------------------------------------

NULL_LEAF_TOKENS = {
    "null_scalar": (pa.scalar(None, type=pa.null()), STR, None),
    "list_of_null": (pa.scalar([None], type=pa.large_list(pa.null())), TOKENS, [None]),
}


def _stored_over_held_null_leaf(kind: str, asynchronous: bool) -> pa.Table:
    """Run the function node after a held node whose rows hold ``kind`` tokens."""
    given, _, _ = NULL_LEAF_TOKENS[kind]
    held, after, _ = _held_job()
    rows: list[Any] = [
        (tag, data.with_source_info(x=given))
        for tag, data in _unconvertible().iter_data()
    ]
    dag: OrcaDAG[Any] = OrcaDAG()
    dag.add_edge(held, after)
    if asynchronous:
        AsyncPipelineOrchestrator().run(dag, held_rows={held: rows})
    else:
        SyncPipelineOrchestrator().run(dag, held={held: rows_on_read(rows, held)})
    return _stored_by_after(after)


@pytest.mark.parametrize("kind", list(NULL_LEAF_TOKENS))
def test_held_rows_with_a_null_typed_token_are_stored_alike_on_both_routes(
    kind: str,
) -> None:
    """A token whose Arrow type has a ``null`` leaf is a valid token of a held row.

    ``with_source_info`` given a null-typed scalar, or a list of nulls typed
    ``large_list<null>``, keeps that type on the row. A stream reads a ``null``
    leaf as ``large_string`` before it applies the token-shape rule, so the sync
    orchestrator stores the rows. The async orchestrator gave the rule the row's
    raw types and refused them (third review of PR #601, finding 1). Both read the
    same normalized types now, and store the same rows.
    """
    _, stored_type, token = NULL_LEAF_TOKENS[kind]
    sync = _stored_over_held_null_leaf(kind, False)
    asynchronous = _stored_over_held_null_leaf(kind, True)
    random = {"__data_id", "__pipeline_record_id", "__recorded_at", "__pipeline_run_id"}
    kept = sorted(name for name in sync.column_names if name not in random)
    assert kept == sorted(n for n in asynchronous.column_names if n not in random)
    left, right = sync.select(kept).sort_by("k"), asynchronous.select(kept).sort_by("k")
    assert left.schema.equals(right.schema)
    assert left.to_pylist() == right.to_pylist()
    assert left.num_rows == 2
    assert left.schema.field("_source_x").type == stored_type
    assert left.column("_source_x").to_pylist() == [token, token]


def test_the_rule_reads_a_table_and_a_row_through_one_normalization() -> None:
    """Every spelling of a string leaf, and a null leaf, is one token type to the
    rule, for a table's columns and for a row's alike."""
    for token_type, data_type in [
        (pa.null(), pa.int64()),
        (pa.string(), pa.int64()),
        (pa.string_view(), pa.int64()),
        (pa.large_list(pa.null()), pa.large_list(pa.int64())),
        (pa.list_(pa.string()), pa.list_(pa.int64())),
    ]:
        assert schema_utils.token_shape_fault(token_type, data_type) is None
    fault = schema_utils.token_shape_fault(pa.list_(pa.null()), pa.int64())
    assert fault == (TOKENS, pa.int64())


# ---------------------------------------------------------------------------
# 2. What the held-row check reads
# ---------------------------------------------------------------------------


def _list_rows(count: int) -> list[Any]:
    """Rows of one stream: tags ``k``, ``g``; list data ``x``, ``y`` with list tokens."""
    table = pa.table(
        {
            "g": pa.array(["m"] * count, STR),
            "k": pa.array(range(count), pa.int64()),
            "y": pa.array([[2, 3]] * count, pa.large_list(pa.int64())),
            "x": pa.array([[1, 2]] * count, pa.large_list(pa.int64())),
            "_source_x": pa.array([["a", None]] * count, TOKENS),
            "_source_y": pa.array([["b", "c"]] * count, TOKENS),
        }
    )
    return list(ArrowTableStream(table, tag_columns=["k", "g"]).iter_data())


def test_the_held_row_check_builds_no_table(monkeypatch: pytest.MonkeyPatch) -> None:
    """The check reads the types a row holds and the null counts of its list-typed
    token columns: it joins no table of the row's columns (third review of PR #601,
    finding 2: one combined one-row table was built per row)."""
    stacked: list[int] = []
    stack = arrow_utils.hstack_tables

    def counted(*tables: pa.Table) -> pa.Table:
        stacked.append(len(tables))
        return stack(*tables)

    rows = _list_rows(5)
    monkeypatch.setattr(arrow_utils, "hstack_tables", counted)
    assert len(_sent_on(rows)) == 5
    assert stacked == []


def test_the_held_row_check_makes_no_arrow_of_a_row_built_from_values() -> None:
    """A row built from Python values holds no Arrow table, and the check makes
    none: its data types come from its schema, a list token's type from the list
    it is (a ``None`` there is a plain string token, which fits any data)."""
    data = Data({"x": [1], "y": 2}, source_info={"x": ["t"], "y": None})
    assert len(_sent_on([(Tag({"k": 1}), data)])) == 1
    assert data._data_table is None  # pyright: ignore[reportPrivateUsage]  # no Arrow was made
    assert data._source_info_table is None  # pyright: ignore[reportPrivateUsage]
    # The same rule still refuses such a row: a list token over a scalar.
    bad = Data({"x": 1, "y": 2}, source_info={"x": ["t"], "y": "s"})
    with pytest.raises(InputValidationError, match="'_source_x'"):
        _sent_on([(Tag({"k": 1}), bad)])
    assert bad._data_table is None  # pyright: ignore[reportPrivateUsage]


def test_the_held_row_check_judges_each_layout_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The rows of one stream share one layout of token and data types: the type
    rule is applied once per token column, not once per row."""
    judged: list[tuple[pa.DataType, pa.DataType]] = []
    fits = schema_utils.token_layers_fit

    def counted(token_type: pa.DataType, data_type: pa.DataType) -> bool:
        judged.append((token_type, data_type))
        return fits(token_type, data_type)

    rows = _list_rows(40)
    monkeypatch.setattr(schema_utils, "token_layers_fit", counted)
    assert len(_sent_on(rows)) == 40
    assert len(judged) == 2


def test_rows_with_plain_string_tokens_have_nothing_to_check() -> None:
    """A plain string token fits any data and may be null: such rows say so."""
    table = pa.table(
        {
            "k": pa.array([1, 2], pa.int64()),
            "x": pa.array([[1], [2]], pa.large_list(pa.int64())),
            "y": pa.array([3, 4], pa.int64()),
            "_source_x": pa.array(["a", None], STR),
            "_source_y": pa.array(["b", "c"], STR),
        }
    )
    for _, data in ArrowTableStream(table, tag_columns=["k"]).iter_data():
        assert data.token_layout() == ()
    for _, data in _list_rows(2):
        assert [entry[:2] for entry in data.token_layout()] == [
            ("_source_y", "y"),
            ("_source_x", "x"),
        ]
        # A changed row gives its own layout, not the one it was read with.
        assert [entry[:2] for entry in data.select("x").token_layout()] == [
            ("_source_x", "x")
        ]
        renamed = data.rename({"x": "z"}).token_layout()
        assert sorted(entry[:2] for entry in renamed) == [
            ("_source_y", "y"),
            ("_source_z", "z"),
        ]


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
# 4. Held rows: the order of the columns named, and rows nobody reads
# ---------------------------------------------------------------------------


def _row_with_tokens(position: int, x: Any, y: Any) -> Any:
    table = pa.table(
        {
            "x": pa.array([[1]], pa.large_list(pa.int64())),
            "y": pa.array([[2]], pa.large_list(pa.int64())),
            "_source_x": pa.array([x], TOKENS),
            "_source_y": pa.array([y], TOKENS),
        }
    )
    return Tag({"k": position}), Data(table)


@pytest.mark.parametrize("first_fault", ["x", "y"])
def test_both_routes_name_the_violating_columns_in_name_order(
    first_fault: str,
) -> None:
    """Row 1 holds a null list token in one column and row 2 in the other. The
    async check named the columns in the order it met them and the sync stream in
    its column order (third review of PR #601, finding 4a). Both name them sorted."""
    one = _row_with_tokens(1, None, ["b"])
    other = _row_with_tokens(2, ["a"], None)
    rows = [one, other] if first_fault == "x" else [other, one]
    seed = ArrowTableStream(
        pa.table({"k": [0], "y": [[2]], "x": [[1]]}), tag_columns=["k"]
    )
    with pytest.raises(InputValidationError) as on_async:
        _sent_on(rows)
    with pytest.raises(InputValidationError) as on_sync:
        cast("Any", rows_on_read(rows, seed)).built()
    assert str(on_async.value) == str(on_sync.value)
    message = str(on_async.value)
    assert message.index("'_source_x'") < message.index("'_source_y'")


def _bad_held_rows() -> list[Any]:
    """The held node's rows with a null token in their list-typed token column."""
    rows: list[Any] = []
    for tag, data in _unconvertible().iter_data():
        table = data.as_table().append_column(
            "_source_x", pa.array([None], pa.large_list(pa.large_list(TOKENS)))
        )
        rows.append((tag, Data(table)))
    return rows


def _held_outcome(
    asynchronous: bool, *, successor: str, materialize: bool
) -> tuple[str, str]:
    """Run a job whose held node holds rows that break the token-shape rule.

    Args:
        asynchronous: The orchestrator.
        successor: ``"none"`` (the held node is terminal), ``"held"`` (its only
            successor is held too) or ``"runs"`` (a function node reads it).
        materialize: ``materialize_results``.

    Returns:
        ``("completed", "")``, or the error's type name and message.
    """
    held, after, _ = _held_job()
    dag: OrcaDAG[Any] = OrcaDAG()
    rows = _bad_held_rows()
    held_rows: dict[Any, list[Any]] = {held: rows}
    if successor == "none":
        dag.add_node(held)
    else:
        dag.add_edge(held, after)
        if successor == "held":
            held_rows[after] = []
    try:
        if asynchronous:
            AsyncPipelineOrchestrator().run(
                dag, held_rows=held_rows, materialize_results=materialize
            )
        else:
            SyncPipelineOrchestrator().run(
                dag,
                held={n: rows_on_read(r, n) for n, r in held_rows.items()},
                materialize_results=materialize,
            )
    except (Exception, ExceptionGroup) as error:  # noqa: BLE001  # the outcome is compared
        error = _unwrapped(error)
        return type(error).__name__, str(error)
    return "completed", ""


@pytest.mark.parametrize("successor", ["none", "held"])
def test_held_rows_nobody_reads_are_not_checked_on_either_route(
    successor: str,
) -> None:
    """The sync orchestrator builds a held node's rows where a successor reads
    them, so rows nobody reads are never built or checked. The async orchestrator
    checked every held node's rows (third review of PR #601, finding 4b). Neither
    checks rows that no node that runs will read."""
    outcomes = [
        _held_outcome(asynchronous, successor=successor, materialize=False)
        for asynchronous in (False, True)
    ]
    assert outcomes == [("completed", ""), ("completed", "")]


@ROUTES
def test_held_rows_a_running_node_reads_are_still_checked(asynchronous: bool) -> None:
    kind, message = _held_outcome(asynchronous, successor="runs", materialize=False)
    assert kind == "InputValidationError" and "'_source_x'" in message


def test_materialized_terminal_held_rows_give_one_outcome_on_both_routes() -> None:
    """With ``materialize_results`` both routes do the same with a terminal held
    node's rows."""
    sync = _held_outcome(False, successor="none", materialize=True)
    asynchronous = _held_outcome(True, successor="none", materialize=True)
    assert sync == asynchronous


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


@pytest.mark.parametrize("case", list(NULL_LISTS))
@pytest.mark.parametrize("built_from", ["table", "values"])
def test_held_rows_with_a_null_list_inside_a_token_are_refused_alike(
    case: str, built_from: str
) -> None:
    """Rows handed on without being built into a stream are held to the same rule:
    the async check and the sync stream of them give the same error."""
    token_type, tokens, data_type, data, level = NULL_LISTS[case]
    rows: list[Any] = []
    for position, (token, value) in enumerate(zip(tokens, data)):
        if built_from == "table":
            row = Data(
                pa.table(
                    {
                        "x": pa.array([value], data_type),
                        "w": pa.array([position], pa.int64()),
                        "_source_x": pa.array([token], token_type),
                    }
                )
            )
        elif pa.types.is_fixed_size_list(token_type) or case == "level_1_of_3":
            # ``[None]`` alone is typed from its value as a list of strings, in
            # which the null is a string: valid.
            pytest.skip("a row built from values is typed from its values")
        else:
            row = Data({"x": value, "w": position}, source_info={"x": token})
        rows.append((Tag({"k": position}), row))
    with pytest.raises(InputValidationError) as on_async:
        _sent_on(rows)
    assert f"list level {level}" in str(on_async.value)
    if built_from == "table":
        seed = ArrowTableStream(
            pa.table(
                {
                    "k": pa.array([0], pa.int64()),
                    "x": pa.array([data[0]], data_type),
                    "w": pa.array([0], pa.int64()),
                }
            ),
            tag_columns=["k"],
        )
        with pytest.raises(InputValidationError) as on_sync:
            cast("Any", rows_on_read(rows, seed)).built()
        assert str(on_async.value) == str(on_sync.value)
    # Rows nobody reads are not checked.
    assert len(_sent_on(rows, read=False)) == len(rows)


def test_the_outermost_null_level_is_named_whatever_the_row_order() -> None:
    """One column with a null list at level 1 in one row and at level 2 in
    another: both routes name level 1, in either row order."""
    data_type = pa.large_list(pa.large_list(INTS))

    def row(position: int, token: Any) -> Any:
        table = pa.table(
            {
                "x": pa.array([[[[1]], [[2]]]], data_type),
                "_source_x": pa.array([token], THREE),
            }
        )
        return Tag({"k": position}), Data(table)

    deep, shallow = row(1, [[None], [["a"]]]), row(2, [None, [["b"]]])
    seed = ArrowTableStream(
        pa.table({"k": [0], "x": pa.array([[[[1]], [[2]]]], data_type)}),
        tag_columns=["k"],
    )
    messages = set()
    for rows in ([deep, shallow], [shallow, deep]):
        with pytest.raises(InputValidationError) as on_async:
            _sent_on(rows)
        with pytest.raises(InputValidationError) as on_sync:
            cast("Any", rows_on_read(rows, seed)).built()
        messages |= {str(on_async.value), str(on_sync.value)}
    (message,) = messages
    assert "list level 1" in message
