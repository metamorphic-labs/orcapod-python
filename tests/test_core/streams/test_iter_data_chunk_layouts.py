"""Regression tests for ITL-816.

``ArrowTableStream.iter_data`` must yield every row, with the tag paired to the
right data and the right source info, whatever the chunk layout of the tag
columns, the data columns and the source-info columns of the backing table.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.streams import ArrowTableStream
from orcapod.system_constants import constants

SOURCE_X = f"{constants.SOURCE_PREFIX}x"


def _chunked(values: list, sizes: list[int], arrow_type: pa.DataType) -> pa.ChunkedArray:
    """Cut ``values`` into a ``ChunkedArray`` with chunks of the given sizes."""
    assert sum(sizes) == len(values)
    chunks = []
    start = 0
    for size in sizes:
        chunks.append(pa.array(values[start : start + size], type=arrow_type))
        start += size
    return pa.chunked_array(chunks, type=arrow_type)


def _chunked_stream(
    tag_sizes: list[int],
    data_sizes: list[int],
    source_sizes: list[int] | None = None,
    with_tags: bool = True,
) -> ArrowTableStream:
    """Build a stream whose tag, data and source-info columns are chunked independently.

    Row ``i`` holds ``id=i``, ``grp="g{i}"``, ``x=i*10``, ``y="v{i}"`` and (when
    ``source_sizes`` is given) ``_source_x="src::{i}"``, so a mis-paired row is
    always detectable.
    """
    n = sum(data_sizes)
    columns: dict[str, pa.ChunkedArray] = {}
    if with_tags:
        columns["id"] = _chunked(list(range(n)), tag_sizes, pa.int64())
        columns["grp"] = _chunked([f"g{i}" for i in range(n)], tag_sizes, pa.large_string())
    columns["x"] = _chunked([i * 10 for i in range(n)], data_sizes, pa.int64())
    columns["y"] = _chunked([f"v{i}" for i in range(n)], data_sizes, pa.large_string())
    if source_sizes is not None:
        columns[SOURCE_X] = _chunked(
            [f"src::{i}" for i in range(n)], source_sizes, pa.large_string()
        )
    table = pa.table(columns)
    return ArrowTableStream(table, tag_columns=["id", "grp"] if with_tags else [])


LAYOUTS = [
    pytest.param([6], [1, 1, 1, 1, 1, 1], id="tag-single-data-many"),
    pytest.param([2, 2, 2], [6], id="tag-many-data-single"),
    pytest.param([4, 2], [1, 5], id="unequal-boundaries-same-count"),
    pytest.param([3, 3], [2, 2, 2], id="fewer-tag-chunks"),
    pytest.param([1, 2, 3], [5, 1], id="fewer-data-chunks"),
    pytest.param([4, 2], [2, 2, 1, 1], id="reporter-shape"),
    pytest.param([0, 6], [3, 0, 3], id="empty-chunks"),
    # Result-cache hits return data whose columns carry zero-length chunks
    # (layouts like [1, 0, 0, ...]) while the tag side has one chunk per row.
    pytest.param([1] * 6, [1, 0, 0, 1, 0, 1, 0, 0, 1, 1, 0, 1], id="zero-length-data-chunks"),
    pytest.param([1, 0, 0, 1, 0, 1, 0, 0, 1, 1, 0, 1], [1] * 6, id="zero-length-tag-chunks"),
    pytest.param([0, 1, 0, 2, 0, 3], [3, 0, 0, 2, 1, 0], id="zero-length-chunks-both-sides"),
    pytest.param([2, 4], [2, 4], id="same-layout-control"),
]


@pytest.mark.parametrize("tag_sizes,data_sizes", LAYOUTS)
class TestIterDataChunkLayouts:
    def test_layout_is_actually_as_described(self, tag_sizes, data_sizes):
        """Guard the fixture: the stream's backing table really is chunked unevenly."""
        stream = _chunked_stream(tag_sizes, data_sizes)
        assert [len(c) for c in stream._table.column("id").chunks] == tag_sizes
        assert [len(c) for c in stream._table.column("x").chunks] == data_sizes

    def test_yields_every_row(self, tag_sizes, data_sizes):
        stream = _chunked_stream(tag_sizes, data_sizes)
        n = sum(data_sizes)
        assert stream.as_table().num_rows == n
        assert len(list(stream.iter_data())) == stream.as_table().num_rows

    def test_tag_pairs_with_its_own_data(self, tag_sizes, data_sizes):
        stream = _chunked_stream(tag_sizes, data_sizes)
        pairs = list(stream.iter_data())
        assert [tag["id"] for tag, _ in pairs] == list(range(sum(data_sizes)))
        for tag, data in pairs:
            row = tag["id"]
            assert tag["grp"] == f"g{row}"
            assert data["x"] == row * 10
            assert data["y"] == f"v{row}"

    def test_iteration_matches_as_table(self, tag_sizes, data_sizes):
        stream = _chunked_stream(tag_sizes, data_sizes)
        expected = stream.as_table().to_pylist()
        actual = [{**tag.as_dict(), **data.as_dict()} for tag, data in stream.iter_data()]
        assert actual == expected

    def test_cached_second_iteration_is_identical(self, tag_sizes, data_sizes):
        stream = _chunked_stream(tag_sizes, data_sizes)
        first = [(t.as_dict(), d.as_dict()) for t, d in stream.iter_data()]
        second = [(t.as_dict(), d.as_dict()) for t, d in stream.iter_data()]
        assert len(first) == sum(data_sizes)
        assert first == second

    def test_content_hash_column_has_one_hash_per_row(self, tag_sizes, data_sizes):
        """``as_table(content_hash=True)`` appends one hash per iterated row."""
        stream = _chunked_stream(tag_sizes, data_sizes)
        table = stream.as_table(columns={"content_hash": True})
        assert table.num_rows == sum(data_sizes)


SOURCE_LAYOUTS = [
    pytest.param([6], [6], [2, 2, 2], id="source-many-rest-single"),
    pytest.param([2, 2, 2], [2, 2, 2], [6], id="source-single-rest-many"),
    pytest.param([4, 2], [1, 5], [3, 3], id="all-three-differ"),
    pytest.param([3, 3], [3, 3], [3, 3], id="same-layout-multi-chunk"),
]


@pytest.mark.parametrize("tag_sizes,data_sizes,source_sizes", SOURCE_LAYOUTS)
def test_source_info_pairs_with_its_own_row(tag_sizes, data_sizes, source_sizes):
    """Per-row source info follows the row, not the position inside a batch."""
    stream = _chunked_stream(tag_sizes, data_sizes, source_sizes)
    pairs = list(stream.iter_data())
    assert len(pairs) == sum(data_sizes)
    for tag, data in pairs:
        row = tag["id"]
        assert data["x"] == row * 10
        assert data.source_info()["x"] == f"src::{row}"


@pytest.mark.parametrize("data_sizes", [[6], [1, 5], [2, 2, 2]])
def test_stream_without_tag_columns_yields_every_row(data_sizes):
    n = sum(data_sizes)
    table = pa.table(
        {
            "x": _chunked([i * 10 for i in range(n)], data_sizes, pa.int64()),
            "y": _chunked([f"v{i}" for i in range(n)], data_sizes[::-1], pa.large_string()),
        }
    )
    stream = ArrowTableStream(table)
    pairs = list(stream.iter_data())
    assert len(pairs) == stream.as_table().num_rows == n
    assert [(d["x"], d["y"]) for _, d in pairs] == [(i * 10, f"v{i}") for i in range(n)]
    assert all(t.as_dict() == {} for t, _ in pairs)


def test_empty_stream_yields_nothing():
    table = pa.table(
        {
            "id": pa.array([], type=pa.int64()),
            "x": pa.array([], type=pa.int64()),
        }
    )
    stream = ArrowTableStream(table, tag_columns=["id"])
    assert list(stream.iter_data()) == []
    assert stream.as_table().num_rows == 0
