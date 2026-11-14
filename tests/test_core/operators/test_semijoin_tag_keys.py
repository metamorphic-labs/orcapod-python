"""SemiJoin keys on the shared tag columns only (ITL-1088).

Developer decision, November 13, 2026: "Semijoin should key on shared tags only". The
match keys are exactly the names that are tag columns of both inputs. A name shared as a
data column, or a tag on one side and a data column on the other, is ignored, and the
right input's data columns are never read. With no shared tag, every left row is
returned. Before, every shared name, tag or data, was a match key.
"""

from __future__ import annotations

from collections.abc import Callable

import pyarrow as pa
import pytest

from orcapod.core.operators import PolarsFilter, SemiJoin
from orcapod.core.sources import ArrowTableSource
from orcapod.protocols.core_protocols import StreamProtocol
from tests.test_core.operators.test_async_sync_equivalence import (
    _source,  # pyright: ignore[reportPrivateUsage]  # shared test helper
    assert_equivalent,
    assert_same_provenance,
    async_rows,
    sync_rows,
)


def left_source() -> ArrowTableSource:
    """Tag ``id`` 1..3, data ``y`` 1..3 and ``x``."""
    return _source(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "y": pa.array([1, 2, 3], pa.int64()),
            "x": pa.array([10, 20, 30], pa.int64()),
        },
        tags=("id",),
        source_id="left",
    )


def right_same_tag_other_y() -> ArrowTableSource:
    """Tag ``id`` 1..3 and data ``y`` that equals the left's only for ``id`` 1 and 3."""
    return _source(
        {"id": pa.array([1, 2, 3], pa.int64()), "y": pa.array([1, 9, 3], pa.int64())},
        tags=("id",),
        source_id="right_y",
    )


def right_tag_y() -> ArrowTableSource:
    """Tag ``y`` (the left's data column name) and data ``z``."""
    return _source(
        {"y": pa.array([1, 3], pa.int64()), "z": pa.array([0, 0], pa.int64())},
        tags=("y",),
        source_id="right_tag_y",
    )


def right_nothing_shared() -> ArrowTableSource:
    """Tag ``other`` and data ``w``: no name in common with the left."""
    return _source(
        {"other": pa.array([7], pa.int64()), "w": pa.array([0], pa.int64())},
        tags=("other",),
        source_id="right_nothing",
    )


def right_unequal_data_of_another_type() -> ArrowTableSource:
    """Tag ``id`` 1 and 3; data ``y`` and ``x`` of other types and values than the left's."""
    return _source(
        {
            "id": pa.array([3, 1], pa.int64()),
            "y": pa.array(["no", "match"], pa.large_string()),
            "x": pa.array([None, 0.5], pa.float64()),
        },
        tags=("id",),
        source_id="right_other_data",
        nullable=("x",),
    )


def left_ids(stream: StreamProtocol) -> list[int | None]:
    """The ``id`` of each result row, in order."""
    return stream.as_table().column("id").to_pylist()


CASES = [
    # (right input, ids kept)
    pytest.param(
        right_same_tag_other_y, [1, 2, 3], id="shared_data_column_is_not_a_key"
    ),
    pytest.param(right_tag_y, [1, 2, 3], id="tag_on_right_data_on_left_is_not_a_key"),
    pytest.param(right_nothing_shared, [1, 2, 3], id="nothing_shared"),
    pytest.param(
        right_unequal_data_of_another_type, [1, 3], id="right_data_never_read"
    ),
]


@pytest.mark.parametrize(("make_right", "kept"), CASES)
def test_semijoin_keys_on_shared_tags_only(
    make_right: Callable[[], ArrowTableSource], kept: list[int]
) -> None:
    """The result is the left rows whose shared tag values match; data plays no part."""
    left = left_source()
    result = SemiJoin()(left, make_right())
    assert left_ids(result) == kept
    assert result.output_schema() == left.output_schema()
    assert result.as_table().schema == left.as_table().schema


@pytest.mark.parametrize(("make_right", "kept"), CASES)
def test_async_rows_equal_sync_rows(
    make_right: Callable[[], ArrowTableSource], kept: list[int]
) -> None:
    """The async path keys on the same columns and yields the sync rows."""
    left, right = left_source(), make_right()
    expected = sync_rows(SemiJoin(), left, right)
    actual = async_rows(SemiJoin(), left, right)
    assert len(expected) == len(kept)
    assert_equivalent(expected, actual)
    assert_same_provenance(expected, actual)


def test_filter_a_stream_by_a_subset_of_itself() -> None:
    """The commonest use: every data column name is shared, and only the tags decide."""
    source = left_source()
    subset = PolarsFilter(constraints={"id": 2})(left_source())
    result = SemiJoin()(source, subset)
    assert left_ids(result) == [2]
    assert_equivalent(
        sync_rows(SemiJoin(), source, subset), async_rows(SemiJoin(), source, subset)
    )
