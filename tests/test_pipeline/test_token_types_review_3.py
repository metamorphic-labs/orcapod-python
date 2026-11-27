"""Findings of the third independent review of PR #601 (ITL-1491), each as a test.

They are about the rows a run holds rather than runs (the async orchestrator sends
them on as they are, the sync one builds them into a stream where a successor
reads them) and about the record-batch join kernels.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.datagrams import Data, Tag
from orcapod.core.operators import Join, MergeJoin
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
    _both_routes,  # pyright: ignore[reportPrivateUsage]  # the shared stream fixtures
    _keyed,  # pyright: ignore[reportPrivateUsage]
)

STR = pa.large_string()
TOKENS = pa.large_list(STR)
ROUTES = pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])


class _Sink:
    """A writable channel that keeps what it is sent."""

    def __init__(self) -> None:
        self.rows: list[Any] = []

    async def send(self, row: Any) -> None:
        self.rows.append(row)

    async def close(self) -> None:
        pass


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
    sink = _Sink()
    asyncio.run(_send_held(rows, sink))
    assert len(sink.rows) == 5
    assert stacked == []


def test_the_held_row_check_makes_no_arrow_of_a_row_built_from_values() -> None:
    """A row built from Python values holds no Arrow table, and the check makes
    none: its data types come from its schema, a list token's type from the list
    it is (a ``None`` there is a plain string token, which fits any data)."""
    data = Data({"x": [1], "y": 2}, source_info={"x": ["t"], "y": None})
    sink = _Sink()
    asyncio.run(_send_held([(Tag({"k": 1}), data)], sink))
    assert len(sink.rows) == 1
    assert data._data_table is None  # pyright: ignore[reportPrivateUsage]  # no Arrow was made
    assert data._source_info_table is None  # pyright: ignore[reportPrivateUsage]
    # The same rule still refuses such a row: a list token over a scalar.
    bad = Data({"x": 1, "y": 2}, source_info={"x": ["t"], "y": "s"})
    with pytest.raises(InputValidationError, match="'_source_x'"):
        asyncio.run(_send_held([(Tag({"k": 1}), bad)], _Sink()))
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
    asyncio.run(_send_held(rows, _Sink()))
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


@pytest.mark.parametrize(
    ("operator", "kernel"),
    [(Join, _join_kernel), (MergeJoin, _merge_kernel)],
    ids=["join", "merge"],
)
@pytest.mark.parametrize("tags", [("k", "g"), ("g", "k")], ids=["k_g", "g_k"])
def test_a_kernel_refuses_a_null_inner_list_as_its_operator_does(
    operator: Any, kernel: Any, tags: tuple[str, ...]
) -> None:
    """The kernels merge a shared data-tag's tokens with ``Join``'s routine. A
    null where a list of tokens is declared raised the routine's own blank
    ``NullTokenListError`` out of ``push`` (third review of PR #601, finding 3).
    The kernel words it as its operator does on the sync and row-channel routes."""
    key_type = pa.large_list(pa.large_list(pa.int64()))
    keys = [[[1], [2]], [[3], [4]]]
    left = _keyed(
        ("x", "w"),
        keys,
        [[None, ["a"]], [["q"], ["b"]]],
        NESTED,
        tags,
        key_type=key_type,
    )
    right = _keyed(
        ("y", "v"),
        keys,
        [[["b"], ["c"]], [["d"], ["e"]]],
        NESTED,
        tags,
        key_type=key_type,
    )
    sync, row_channels = _both_routes(operator, left, right)
    assert sync == row_channels and sync[0] == "InputValidationError"
    pushing = kernel([left, right])
    with pytest.raises(InputValidationError) as refused:
        for position, stream in enumerate((left, right)):
            for batch in _sent(stream).to_batches():
                pushing.push(position, batch)
        pushing.finish()
    assert str(refused.value) == sync[1]
    assert "'_source_k'" in str(refused.value)


# ---------------------------------------------------------------------------
# 4. Held rows: the order of the columns named, and rows nobody reads
# ---------------------------------------------------------------------------


def _row_with_tokens(position: int, x: Any, y: Any) -> tuple[Tag, Data]:
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
        asyncio.run(_send_held(rows, _Sink()))
    with pytest.raises(InputValidationError) as on_sync:
        rows_on_read(rows, seed).built()
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
