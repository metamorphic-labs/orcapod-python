"""CodeRabbit's final pass on PR #510 (head 365f2a08): one regression test per finding.

Stages 1-2 and the differential harness (ITL-1347). Each test fails without its fix.
"""

from __future__ import annotations

import asyncio
import decimal
import importlib.util
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.streams import ArrowTableStream
from orcapod.sources import ArrowTableSource, DictSource
from orcapod.types import Schema


def _plain(values: tuple[int, int] = (1, 2), source_id: str = "s") -> DictSource:
    return DictSource(
        [{"k": 1, "x": values[0]}, {"k": 2, "x": values[1]}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id=source_id,
    )


# ── compare.py:140: set mode compares raw Arrow values ─────────────────────────

_SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "dev"
    / "scripts"
    / "itl_1347"
    / "compare.py"
)


def _compare_module() -> Any:
    spec = importlib.util.spec_from_file_location("itl_1347_compare_510", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


compare = _compare_module()

_SET_KINDS = [
    ("time64_ns", pa.time64("ns"), [1, 9], [2, 9]),
    ("timestamp_ns", pa.timestamp("ns"), [1, 9], [2, 9]),
    (
        "decimal256",
        pa.decimal256(76, 40),
        [decimal.Decimal("1." + "0" * 39 + "1"), None],
        [decimal.Decimal("1." + "0" * 39 + "2"), None],
    ),
    ("negative_zero", pa.float64(), [0.0, 1.0], [-0.0, 1.0]),
    (
        "long_string",
        pa.large_string(),
        ["x" * 5000 + "a", "b"],
        ["x" * 5000 + "c", "b"],
    ),
]


@pytest.mark.parametrize(
    ("kind", "before", "after"),
    [case[1:] for case in _SET_KINDS],
    ids=[case[0] for case in _SET_KINDS],
)
def test_set_mode_tells_raw_values_apart(
    kind: Any, before: list[Any], after: list[Any]
) -> None:
    """Rows differing in one raw value are different rows in set mode, also where
    Python loses precision; the same rows in another order, with their duplicates,
    are the same multiset."""
    first = pa.table({"v": pa.array(before, kind), "k": [1, 2]})
    second = pa.table({"v": pa.array(after, kind), "k": [1, 2]})
    assert compare._rows_as_multiset(first) != compare._rows_as_multiset(second)
    reordered = first.take([1, 0])
    assert compare._rows_as_multiset(first) == compare._rows_as_multiset(reordered)
    doubled = pa.concat_tables([first, first])
    assert compare._rows_as_multiset(doubled) != compare._rows_as_multiset(first)


# ── source_node.py:497: inspecting opened_on takes no binding ─────────────────


def test_inspecting_a_source_channel_s_origin_takes_no_binding() -> None:
    """Reading ``opened_on`` of an unread source-node channel runs nothing: a rebind
    before the first read is what the channel delivers (``open_channel``'s
    first-read guarantee)."""
    from orcapod.core.nodes.source_node import SourceJobNode

    node: Any = SourceJobNode.from_stream(_plain((1, 2)))
    channel = node.open_channel()
    assert channel.opened_on is None
    node.bound_source = _plain((7, 8))
    batches = asyncio.run(channel.collect())
    assert pa.Table.from_batches(batches)["x"].to_pylist() == [7, 8]


# ── source_node.py:1330: an unbound slot declared in Arrow types ──────────────


@pytest.mark.parametrize("declared", ["arrow", "python"])
def test_an_unbound_slot_states_its_types_as_the_protocol_says(declared: str) -> None:
    """An unbound slot declared with explicit Arrow schemas answers
    ``arrow_types_declared`` ``True`` (its rows will have those types: a bind refuses
    any other); one declared in Python types answers ``False``."""
    from orcapod.core.nodes.source_node import SourceNode

    if declared == "arrow":
        node = SourceNode(
            "slot",
            tag_schema=pa.schema([pa.field("k", pa.int64(), nullable=False)]),
            data_schema=pa.schema([pa.field("x", pa.int64(), nullable=False)]),
        )
    else:
        node = SourceNode(
            "slot", tag_schema=Schema({"k": int}), data_schema=Schema({"x": int})
        )
    assert node.arrow_types_declared is (declared == "arrow")


# ── source_node.py:1884: a subclass reads its rows without a bound source ─────


def test_a_source_job_node_subclass_with_rows_of_its_own_needs_no_binding() -> None:
    """A ``SourceJobNode`` subclass that yields its own rows, with no bound source,
    hands them on from ``execute``, as ``main`` iterated them; the unbound error is
    for an exact ``SourceJobNode``."""
    from orcapod.core.nodes.source_node import SourceJobNode, UnboundSourceError
    from orcapod.core.operators.static_output_pod import at_edge

    rows = list(_plain((5, 6)).iter_data())

    class OwnRows(SourceJobNode):
        def iter_data(self) -> Any:
            return iter(rows)

    template: Any = SourceJobNode.from_stream(_plain())
    node: Any = OwnRows.from_stream(_plain())
    node._bound_source = None  # pyright: ignore[reportPrivateUsage]  # no binding
    assert at_edge(node.execute()).as_table()["x"].to_pylist() == [5, 6]
    template._bound_source = None  # pyright: ignore[reportPrivateUsage]  # no binding
    with pytest.raises(UnboundSourceError):
        template.execute()


# ── static_output_pod.py:1126: token types settle across an input's batches ───


class _Unchecked:
    """An input channel that delivers its batches without the send check."""

    def __init__(self, stream: Any, batches: list[pa.RecordBatch]) -> None:
        self.stream = stream
        self._batches = batches
        self.closed = False

    def __aiter__(self) -> Any:
        async def gen() -> Any:
            for batch in self._batches:
                yield batch

        return gen()

    async def close(self) -> None:
        self.closed = True


def _token_batch(tokens: Any, kind: pa.DataType) -> pa.RecordBatch:
    table = _plain().as_table(columns={"source": True, "system_tags": True})
    table = table.append_column("z", pa.array([1, 2]))
    table = table.append_column(
        "_source_z", pa.array(["s::row_0::z", "s::row_1::z"], pa.large_string())
    )
    position = table.schema.get_field_index("_source_x")
    table = table.set_column(
        position, pa.field("_source_x", kind), pa.array(tokens, kind)
    )
    return table.to_batches()[0]


def test_a_row_by_row_operator_refuses_batches_whose_tokens_cannot_settle() -> None:
    """Two batches of one input whose ``_source_x`` tokens settle to different types:
    the sync path refuses that input; the record-batch form fails its output, also
    when the operator drops ``x``."""
    from orcapod.core.operators.static_output_pod import SourceInfoTypesDisagree
    from orcapod.core.record_batch_channels import OutputChannel
    from orcapod.operators import SelectDataColumns

    strings = _token_batch(["s::row_0::x", "s::row_1::x"], pa.large_string())
    lists = _token_batch([["a"], ["b"]], pa.large_list(pa.large_string()))
    description = ArrowTableStream(pa.Table.from_batches([strings]), tag_columns=["k"])
    op = SelectDataColumns(["z"])

    async def go() -> None:
        output = OutputChannel(op(description))
        channel = _Unchecked(description, [strings, lists])
        await op.async_execute_batches([channel], output)  # pyright: ignore[reportArgumentType]

    with pytest.raises(SourceInfoTypesDisagree):
        asyncio.run(go())


# ── static_output_pod.py:1219: a cancelled close closes the others too ────────


class _Closing:
    def __init__(self, raises: BaseException | None = None) -> None:
        self.raises = raises
        self.closed = False

    async def close(self) -> None:
        self.closed = True
        if self.raises is not None:
            raise self.raises


@pytest.mark.parametrize("operator_failed", [False, True])
def test_a_cancelled_close_still_closes_every_input(operator_failed: bool) -> None:
    """One input's close is cancelled: every other input is still closed; the
    cancellation then propagates, unless the operator's own error is in flight,
    which keeps its precedence."""
    from orcapod.core.operators.static_output_pod import close_inputs

    inputs = [_Closing(), _Closing(asyncio.CancelledError()), _Closing()]
    error = RuntimeError("operator failed") if operator_failed else None

    async def go() -> None:
        await close_inputs(inputs, error)  # pyright: ignore[reportArgumentType]

    if operator_failed:
        asyncio.run(go())
    else:
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(go())
    assert all(channel.closed for channel in inputs)


# ── streams/base.py:550 (1): no second read after a failed preparation ────────


def test_a_failed_batch_preparation_is_raised_again_without_a_second_read() -> None:
    """``keys()`` fails after the table was read: a later read raises the same error
    and never reads the stream again."""
    from orcapod.core.streams.base import CompleteStreamFeed

    reads: list[int] = []
    base = _plain()

    class Failing:
        def as_table(self, **kwargs: Any) -> Any:
            reads.append(1)
            return base.as_table(**kwargs)

        def keys(self, **kwargs: Any) -> Any:
            raise RuntimeError("keys failed")

    feed = CompleteStreamFeed(Failing())  # pyright: ignore[reportArgumentType]
    for _ in range(2):
        with pytest.raises(RuntimeError, match="keys failed"):
            asyncio.run(feed.next())
    assert reads == [1]


# ── streams/base.py:550 (2): system tags the rows retype take the row path ────


def _with_system_tag(column: pa.Array[Any]) -> ArrowTableStream:
    table = pa.table({"k": [1, 2], "x": [10, 20], "e": column})
    return ArrowTableStream(table, tag_columns=["k"], system_tag_columns=["e"])


@pytest.mark.parametrize(
    "column",
    [
        pa.array([1, 2], pa.int16()),
        pa.array([1, 2], pa.int32()),
        pa.array(["a", "b"], pa.string()),
    ],
    ids=["int16", "int32", "string"],
)
def test_a_channel_on_a_stream_gives_the_rows_system_tag_types(
    column: pa.Array[Any],
) -> None:
    """A channel opened on an exact ``ArrowTableStream`` whose system tag rows would
    retype delivers what the rows build (``main``'s row path), e.g. ``int16`` as
    ``int64``."""
    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
    from orcapod.core.streams.base import ZERO_ROW_INPUT_COLUMNS

    stream = _with_system_tag(column)
    delivered = pa.Table.from_batches(asyncio.run(stream.open_channel().collect()))
    built = StaticOutputOperatorPod._materialize_upstream(  # pyright: ignore[reportPrivateUsage]  # main's row path
        list(stream.iter_data()), stream
    ).as_table(columns=ZERO_ROW_INPUT_COLUMNS)
    (name,) = [n for n in delivered.column_names if n.startswith("_tag")]
    assert delivered.schema.field(name).type == built.schema.field(name).type
    assert delivered[name].to_pylist() == built[name].to_pylist()


def test_a_channel_on_a_stream_whose_rows_cannot_make_one_table_raises() -> None:
    """A string system tag holding a null and a value: the rows cannot make one
    table; the channel raises what ``main``'s row build raises."""
    stream = _with_system_tag(pa.array(["a", None], pa.large_string()))
    with pytest.raises(pa.ArrowInvalid):
        asyncio.run(stream.open_channel().collect())


def test_an_arrow_table_source_channel_takes_the_whole_table() -> None:
    """The ordinary case keeps the fast path: a source's own system tags are what the
    rows give."""
    source = ArrowTableSource(
        pa.table({"k": [1, 2], "x": [10, 20]}),
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="s",
    )
    delivered = pa.Table.from_batches(asyncio.run(source.open_channel().collect()))
    assert delivered["x"].to_pylist() == [10, 20]
