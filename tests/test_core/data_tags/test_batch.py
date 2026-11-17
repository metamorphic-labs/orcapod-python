"""``Batch`` on data-tags (ITL-1017).

Developer decision, November 16, 2026 (gap audit Q5): "Batch always simply takes
anything and puts into list." ``Batch`` keeps every tag, list-valued, as in v0.3. A
data-tag becomes the same list in both roles and stays a data-tag; its source
info is one token per member, a list of the shape of its data column (nothing is merged,
so no token is folded). System tags fold as ``Batch`` already folds them.

Every operator is driven through its public call, ``op(stream)``, and read through the
stream's public methods; the async path is covered by
``tests/test_core/operators/test_async_sync_equivalence.py``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

import orcapod as op
from orcapod.core.operators import Batch, GroupBy, PolarsFilter
from orcapod.core.data_tags import check_namespaces, stream_data_tags
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants

ROWS = [
    {"s": "s1", "x": 20, "y": 5},
    {"s": "s1", "x": 10, "y": 4},
    {"s": "s2", "x": 5, "y": 1},
]


def _source(**kw: Any) -> op.sources.DictSource:
    """Tags ``s`` and ``x``, both data-tags unless ``tag_only_columns`` says otherwise."""
    return op.sources.DictSource(ROWS, tag_columns=["s", "x"], source_id="B", **kw)


def _no_rows(stream: StreamProtocol) -> StreamProtocol:
    return PolarsFilter(constraints={"y": -1})(stream)


def _source_info(stream: StreamProtocol) -> list[dict[str, Any]]:
    table = stream.as_table(columns={"source": True})
    names = [c for c in table.column_names if c.startswith(constants.SOURCE_PREFIX)]
    return table.select(names).to_pylist()


def test_a_data_tag_is_the_same_list_in_both_roles() -> None:
    batched = Batch(batch_size=2)(_source())
    assert batched.keys() == (("s", "x"), ("s", "x", "y"))
    assert stream_data_tags(batched) == ("s", "x")
    tag_schema, data_schema = batched.output_schema()
    assert tag_schema["x"] == data_schema["x"] == list[int]
    assert tag_schema["s"] == data_schema["s"] == list[str]
    # One physical column, in input order.
    assert batched.as_table().column_names == ["s", "x", "y"]
    rows = [(tag.as_dict(), data.as_dict()) for tag, data in batched.iter_data()]
    assert [(t["x"], d["x"], d["y"]) for t, d in rows] == [
        ([20, 10], [20, 10], [5, 4]),
        ([5], [5], [1]),
    ]
    assert all(t["s"] == d["s"] for t, d in rows)


def test_source_info_is_one_token_per_member() -> None:
    """A data-tag's source info is listed like any data column, never folded."""
    batched = Batch(batch_size=2)(_source())
    assert _source_info(batched) == [
        {
            "_source_s": ["B::row_0::s", "B::row_1::s"],
            "_source_x": ["B::row_0::x", "B::row_1::x"],
            "_source_y": ["B::row_0::y", "B::row_1::y"],
        },
        {
            "_source_s": ["B::row_2::s"],
            "_source_x": ["B::row_2::x"],
            "_source_y": ["B::row_2::y"],
        },
    ]


def test_system_tags_fold_to_scalars() -> None:
    table = Batch(batch_size=2)(_source()).as_table(columns={"system_tags": True})
    system_tags = [
        f for f in table.schema if f.name.startswith(constants.SYSTEM_TAG_PREFIX)
    ]
    assert len(system_tags) == 2
    assert all(not str(f.type).startswith("large_list") for f in system_tags)


def test_a_batch_of_one_is_a_list_of_one() -> None:
    batched = Batch(batch_size=1)(_source())
    assert stream_data_tags(batched) == ("s", "x")
    assert [data["x"] for _, data in batched.iter_data()] == [[20], [10], [5]]
    assert _source_info(batched)[0]["_source_x"] == ["B::row_0::x"]


def test_drop_partial_batch_drops_the_incomplete_batch() -> None:
    batched = Batch(batch_size=2, drop_partial_batch=True)(_source())
    assert stream_data_tags(batched) == ("s", "x")
    assert [(tag["x"], data["x"]) for tag, data in batched.iter_data()] == [
        ([20, 10], [20, 10])
    ]


def test_drop_partial_batch_can_leave_no_rows_with_the_types_of_rows() -> None:
    """Too few rows for one batch: zero rows, typed like a result with rows (ITL-912)."""
    empty = Batch(batch_size=5, drop_partial_batch=True)(_source())
    full = Batch(batch_size=2)(_source())
    assert stream_data_tags(empty) == ("s", "x")
    table = empty.as_table(all_info=True)
    assert table.num_rows == 0
    assert table.schema == full.as_table(all_info=True).schema


@pytest.mark.parametrize(
    "make", [Batch, lambda: Batch(batch_size=2)], ids=["all", "batch_2"]
)
def test_no_input_rows_gives_the_types_of_rows(make: Callable[[], Batch]) -> None:
    """The zero-row table is the operator applied to a zero-row input (ITL-912)."""
    empty = make()(_no_rows(_source()))
    full = make()(_source())
    assert stream_data_tags(empty) == ("s", "x")
    assert empty.output_schema() == full.output_schema()
    # The filter is in the empty input's pipeline only, so system-tag names differ.
    columns = {"source": True, "context": True, "content_hash": True}
    table = empty.as_table(columns=columns)
    assert table.num_rows == 0
    assert table.column_names.count("x") == 1
    assert table.schema == full.as_table(columns=columns).schema
    assert empty.head().table.schema == full.head().table.schema


def test_batch_after_group_by_nests_the_lists() -> None:
    """The key stays a data-tag, now a list; the non-key data lists nest one level."""
    batched = Batch()(GroupBy(["s"])(_source()))
    assert batched.keys() == (("s",), ("s", "x", "y"))
    assert stream_data_tags(batched) == ("s",)
    tag_schema, data_schema = batched.output_schema()
    assert tag_schema["s"] == data_schema["s"] == list[str]
    assert data_schema["x"] == list[list[int]]
    [(tag, data)] = list(batched.iter_data())
    assert tag["s"] == data["s"] == ["s1", "s2"]
    assert data["x"] == [[10, 20], [5]]
    [info] = _source_info(batched)
    # The key's tokens are the group's merged tokens, listed: none is folded again.
    assert len(info["_source_s"]) == 2
    assert info["_source_x"] == [["B::row_1::x", "B::row_0::x"], ["B::row_2::x"]]


def test_batch_after_batch_nests_the_data_tag() -> None:
    batched = Batch()(Batch(batch_size=2)(_source()))
    assert stream_data_tags(batched) == ("s", "x")
    tag_schema, data_schema = batched.output_schema()
    assert tag_schema["x"] == data_schema["x"] == list[list[int]]
    [(tag, data)] = list(batched.iter_data())
    assert tag["x"] == data["x"] == [[20, 10], [5]]
    assert _source_info(batched)[0]["_source_x"] == [
        ["B::row_0::x", "B::row_1::x"],
        ["B::row_2::x"],
    ]


def test_a_nullable_data_tag_is_one_type_in_both_roles() -> None:
    rows = [{"x": 1, "y": 1}, {"x": None, "y": 2}]
    source = op.sources.DictSource(
        rows, tag_columns=["x"], source_id="N", data_schema={"x": int | None, "y": int}
    )
    batched = Batch()(source)
    tag_schema, data_schema = batched.output_schema()
    assert tag_schema["x"] == data_schema["x"] == list[int | None]
    [(tag, data)] = list(batched.iter_data())
    assert tag["x"] == data["x"] == [1, None]


@pytest.mark.parametrize(
    "make_input",
    [
        _source,
        lambda: _no_rows(_source()),
        lambda: GroupBy(["s"])(_source()),
        lambda: Batch(batch_size=2)(_source()),
    ],
    ids=["source", "no_rows", "after_group_by", "after_batch"],
)
@pytest.mark.parametrize(
    "make_op",
    [Batch, lambda: Batch(batch_size=2), lambda: Batch(2, drop_partial_batch=True)],
    ids=["all", "batch_2", "drop_partial"],
)
def test_prediction_matches_the_result(
    make_input: Callable[[], StreamProtocol], make_op: Callable[[], Batch]
) -> None:
    """The predicted schema, data-tags included, equals ``static_process``'s."""
    stream = make_input()
    batch = make_op()
    result = batch.static_process(stream)
    for columns in (None, {"system_tags": True}):
        assert batch.output_schema(stream, columns=columns) == result.output_schema(
            columns=columns
        )
    assert batch(stream).keys() == result.keys()
    assert stream_data_tags(batch(stream)) == stream_data_tags(result)
    # Namespace check: a data-tag has one type in both halves.
    tag_schema, data_schema = batch.output_schema(stream)
    check_namespaces(tag_schema, data_schema, stream_data_tags(result))


def test_plain_tags_are_unchanged() -> None:
    plain = Batch(batch_size=2)(_source(tag_only_columns=True))
    assert plain.keys() == (("s", "x"), ("y",))
    assert stream_data_tags(plain) == ()
    assert set(_source_info(plain)[0]) == {"_source_y"}


def test_mixed_tags_keep_only_the_data_tag_ones_data_tag() -> None:
    batched = Batch(batch_size=2)(_source(tag_only_columns=["s"]))
    assert batched.keys() == (("s", "x"), ("x", "y"))
    assert stream_data_tags(batched) == ("x",)


# ── a function pod after Batch ──────────────────────────────────────────────


@op.function_pod(output_keys=["total"])
def total(y: list[int]) -> int:
    return sum(y)


def test_the_pod_call_lets_batched_data_tags_fall_through() -> None:
    out = total.pod(Batch(batch_size=2)(_source()))
    assert out.keys() == (("s", "x"), ("s", "x", "total"))
    assert stream_data_tags(out) == ("s", "x")
    rows = [(tag["x"], data["x"], data["total"]) for tag, data in out.iter_data()]
    assert sorted(rows) == [([5], [5], 1), ([20, 10], [20, 10], 9)]


CALLS: list[list[int]] = []


@op.function_pod(output_keys=["total"])
def counted_total(y: list[int]) -> int:
    CALLS.append(y)
    return sum(y)


@pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
def test_a_job_with_batch_runs_and_a_second_run_computes_nothing(
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
) -> None:
    store = InMemoryArrowDatabase()

    def build(job: PipelineJob) -> None:
        with job:
            counted_total.pod(Batch(batch_size=2)(_source()), label="total")

    CALLS.clear()
    job = PipelineJob(database=store)
    build(job)
    job.run(orchestrator=orchestrator())
    assert sorted(CALLS) == [[1], [5, 4]]
    node = job.nodes["total"]
    assert stream_data_tags(node) == ("s", "x")
    table = node.as_table()
    assert table.column_names.count("x") == 1
    assert sorted(r["total"] for r in table.to_pylist()) == [1, 9]

    CALLS.clear()
    job2 = PipelineJob(database=store)
    build(job2)
    job2.run(orchestrator=orchestrator())
    assert CALLS == []
    assert sorted(r["total"] for r in job2.nodes["total"].as_table().to_pylist()) == [
        1,
        9,
    ]
