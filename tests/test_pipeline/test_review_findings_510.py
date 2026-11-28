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


# ── streams/base.py:550 (2): raw stream system-tag fields on channels ─────────


def _with_system_tag(column: pa.Array[Any]) -> ArrowTableStream:
    table = pa.table({"k": [1, 2], "x": [10, 20], "e": column})
    return ArrowTableStream(table, tag_columns=["k"], system_tag_columns=["e"])


@pytest.mark.parametrize(
    "column",
    [
        pa.array(["a", "b"], pa.string()),
        pa.array(["a", "b"], pa.large_string()),
    ],
    ids=["string", "large_string"],
)
def test_a_channel_on_a_raw_stream_keeps_the_declared_system_tag_field(
    column: pa.Array[Any],
) -> None:
    """Both the channel and the row-built table carry the stream's declared field."""
    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
    from orcapod.core.streams.base import ZERO_ROW_INPUT_COLUMNS

    stream = _with_system_tag(column)
    delivered = pa.Table.from_batches(asyncio.run(stream.open_channel().collect()))
    built = StaticOutputOperatorPod._materialize_upstream(  # pyright: ignore[reportPrivateUsage]  # main's row path
        list(stream.iter_data()), stream
    ).as_table(columns=ZERO_ROW_INPUT_COLUMNS)
    (name,) = [n for n in delivered.column_names if n.startswith("_tag")]
    expected = stream.as_table(columns=ZERO_ROW_INPUT_COLUMNS).schema.field(name)
    assert delivered.schema.field(name).equals(expected, check_metadata=True)
    assert built.schema.field(name).equals(expected, check_metadata=True)
    assert delivered[name].to_pylist() == built[name].to_pylist()


def _arrow_table_source() -> ArrowTableSource:
    return ArrowTableSource(
        pa.table({"k": [1, 2], "j": ["a", "b"], "x": [10, 20], "y": [0.5, 1.5]}),
        tag_columns=["k", "j"],
        tag_only_columns=True,
        source_id="s",
    )


def _dict_source() -> DictSource:
    return DictSource(
        [{"k": 1, "j": "a", "x": 10, "y": 0.5}, {"k": 2, "j": "b", "x": 20, "y": 1.5}],
        tag_columns=["j", "k"],
        tag_only_columns=True,
        source_id="s",
    )


@pytest.mark.parametrize(
    "make", [_arrow_table_source, _dict_source], ids=["arrow_table", "dict"]
)
def test_a_library_source_channel_takes_the_whole_table(
    make: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ordinary case keeps the fast path: a library source's own system tags are
    what the rows give, so its channel never builds a table from rows (CodeRabbit on
    PR #510: the values alone cannot tell the two routes apart)."""
    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod

    builds: list[int] = []
    original = StaticOutputOperatorPod._materialize_upstream  # pyright: ignore[reportPrivateUsage]  # the row path, watched

    def watched(rows: Any, upstream: Any) -> Any:
        builds.append(1)
        return original(rows, upstream)

    monkeypatch.setattr(
        StaticOutputOperatorPod, "_materialize_upstream", staticmethod(watched)
    )
    source = make()
    delivered = pa.Table.from_batches(asyncio.run(source.open_channel().collect()))
    assert delivered["x"].to_pylist() == [10, 20]
    assert delivered["y"].to_pylist() == [0.5, 1.5]
    assert builds == []


# ── job.py:1003 (declined): no route for the old ``run(held_rows=)`` override ─


def test_a_sync_orchestrator_override_of_the_old_signature_is_not_supported() -> None:
    """Declined: no compatibility shim (pre-v0.1.0). A ``SyncPipelineOrchestrator``
    subclass whose ``run`` takes the old ``held_rows`` and not ``held`` fails with
    ``TypeError`` before it runs; the override is updated instead."""
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

    calls: list[int] = []

    class OldSignature(SyncPipelineOrchestrator):
        def run(  # pyright: ignore[reportIncompatibleMethodOverride]  # the old contract
            self,
            graph: Any,
            *,
            observer: Any = None,
            materialize_results: bool = True,
            run_id: str | None = None,
            pipeline_uri: str = "",
            load_only: bool = False,
            held_rows: Any = None,
        ) -> Any:
            calls.append(1)
            return super().run(graph, observer=observer)

    job = PipelineJob(name="old_override", database=InMemoryArrowDatabase())
    with job:
        _plain().select_data_columns(["x"], label="selected")
    with pytest.raises(TypeError, match="held"):
        job.run(orchestrator=OldSignature())
    assert calls == []
