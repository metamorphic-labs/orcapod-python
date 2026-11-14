"""SemiJoin over a shared tag key that is nullable on one input only (ITL-829, second case).

Developer decision, November 13, 2026: when a shared tag key is ``T`` on one input and
``T | None`` on the other, SemiJoin drops the rows with a null in that key on the
nullable side and semi-joins. The result is exactly ``DropNulls(<those keys>)`` applied
to the nullable side followed by the semi join, schema included, so a left key that was
nullable comes out ``T``. A key nullable on both sides is not narrowed, and a shared data
column is never narrowed.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.operators import DropNulls, SemiJoin
from orcapod.core.sources import ArrowTableSource
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from tests.test_core.operators.test_async_sync_equivalence import (
    _source,  # pyright: ignore[reportPrivateUsage]  # shared test helper
    assert_equivalent,
    assert_same_provenance,
    async_rows,
    sync_rows,
)

FULL = {"source": True, "system_tags": True}


def left_source(nullable: tuple[str, ...]) -> ArrowTableSource:
    """Tags ``a`` and ``b`` (int64), data ``x``; a null in each nullable tag."""
    return _source(
        {
            "a": pa.array([1, 1, 2, 2, 3], pa.int64()),
            "b": pa.array([1, None if "b" in nullable else 5, 1, 2, 2], pa.int64()),
            "x": pa.array([10, 20, 30, 40, 50], pa.int64()),
        },
        tags=("a", "b"),
        source_id="left",
        nullable=nullable,
    )


def right_source(nullable: tuple[str, ...]) -> ArrowTableSource:
    """Tags ``a`` and ``b`` (int64), data ``z``; a null in each nullable tag."""
    return _source(
        {
            "a": pa.array([1, 2, None if "a" in nullable else 9, 3], pa.int64()),
            "b": pa.array([1, 2, 1, None if "b" in nullable else 7], pa.int64()),
            "z": pa.array([100, 200, 300, 400], pa.int64()),
        },
        tags=("a", "b"),
        source_id="right",
        nullable=nullable,
    )


def narrowed(stream: StreamProtocol, names: tuple[str, ...]) -> StreamProtocol:
    """``DropNulls(names)(stream)``, or the stream itself when there are no names."""
    return DropNulls(names)(stream) if names else stream


# (left nullable tags, right nullable tags, names DropNulls takes on left, on right)
MIXED = [
    pytest.param(("b",), (), ("b",), (), id="left_nullable"),
    pytest.param((), ("b",), (), ("b",), id="right_nullable"),
    pytest.param(("b",), ("a",), ("b",), ("a",), id="composite_crossed"),
    pytest.param(("a", "b"), ("b",), ("a",), (), id="composite_one_shared_nullable"),
]


@pytest.mark.parametrize(("left_null", "right_null", "left_drop", "right_drop"), MIXED)
def test_result_equals_drop_nulls_then_semijoin(
    left_null: tuple[str, ...],
    right_null: tuple[str, ...],
    left_drop: tuple[str, ...],
    right_drop: tuple[str, ...],
) -> None:
    """Rows, Arrow schema, system tags and source info equal the explicit composition."""
    left, right = left_source(left_null), right_source(right_null)
    result = SemiJoin()(left, right)
    expected = SemiJoin()(narrowed(left, left_drop), narrowed(right, right_drop))

    assert result.output_schema() == expected.output_schema()
    assert result.as_table(columns=FULL).equals(
        expected.as_table(columns=FULL), check_metadata=True
    )
    assert result.as_table(columns=FULL).num_rows > 0


@pytest.mark.parametrize(("left_null", "right_null", "left_drop", "right_drop"), MIXED)
def test_prediction_matches_computed_schema(
    left_null: tuple[str, ...],
    right_null: tuple[str, ...],
    left_drop: tuple[str, ...],
    right_drop: tuple[str, ...],
) -> None:
    """``output_schema()`` without computing equals the computed stream's schema."""
    left, right = left_source(left_null), right_source(right_null)
    op = SemiJoin()
    predicted = op(left, right).output_schema(columns=FULL)
    # Schema-prediction check: compared against the computed stream (CLAUDE.md).
    computed = op.static_process(left, right).output_schema(columns=FULL)
    assert predicted == computed


def test_left_nullable_key_narrows_to_t() -> None:
    """A left key nullable against a non-nullable right key comes out ``T``."""
    result = SemiJoin()(left_source(("b",)), right_source(()))
    tag_schema, _ = result.output_schema()
    assert tag_schema["b"] is int
    assert not result.as_table().schema.field("b").nullable
    assert result.as_table().column("x").to_pylist() == [10, 40]


def test_right_nullable_key_keeps_left_schema() -> None:
    """A right key nullable against a non-nullable left key leaves the left schema."""
    left = left_source(())
    result = SemiJoin()(left, right_source(("b",)))
    assert result.output_schema() == left.output_schema()
    assert result.as_table().schema == left.as_table().schema
    assert result.as_table().column("x").to_pylist() == [10, 40]


def test_key_nullable_on_both_sides_is_not_narrowed() -> None:
    """Both nullable: as before, the key stays ``T | None`` and a null matches nothing."""
    left = left_source(("b",))
    result = SemiJoin()(left, right_source(("b",)))
    tag_schema, _ = result.output_schema()
    assert tag_schema["b"] == int | None
    assert result.as_table().schema.field("b").nullable
    assert result.output_schema() == left.output_schema()
    assert result.as_table().column("x").to_pylist() == [10, 40]


@pytest.mark.parametrize(("left_null", "right_null", "left_drop", "right_drop"), MIXED)
def test_async_rows_equal_sync_rows(
    left_null: tuple[str, ...],
    right_null: tuple[str, ...],
    left_drop: tuple[str, ...],
    right_drop: tuple[str, ...],
) -> None:
    """The async path yields the sync rows: types, nullability, tags, hashes, sources."""
    left, right = left_source(left_null), right_source(right_null)
    expected = sync_rows(SemiJoin(), left, right)
    actual = async_rows(SemiJoin(), left, right)
    assert expected
    assert_equivalent(expected, actual)
    assert_same_provenance(expected, actual)


def test_shared_data_column_nullability_mismatch_still_raises() -> None:
    """Only tag keys are narrowed; a shared data column keeps today's error."""
    left = _source(
        {"a": pa.array([1, 2], pa.int64()), "y": pa.array([1, 2], pa.int64())},
        tags=("a",),
        source_id="l",
    )
    right = _source(
        {"a": pa.array([1, 2], pa.int64()), "y": pa.array([1, None], pa.int64())},
        tags=("a",),
        source_id="r",
        nullable=("y",),
    )
    with pytest.raises(InputValidationError, match="'y'"):
        _ = SemiJoin()(left, right)


def test_other_type_mismatch_still_raises() -> None:
    """Narrowing removes only ``None``: ``int`` against ``str | None`` still raises."""
    left = _source(
        {"a": pa.array([1, 2], pa.int64()), "x": pa.array([1, 2], pa.int64())},
        tags=("a",),
        source_id="l",
    )
    right = _source(
        {"a": pa.array(["1", None], pa.large_string()), "z": pa.array([1, 2])},
        tags=("a",),
        source_id="r",
        nullable=("a",),
    )
    with pytest.raises(InputValidationError, match="'a'"):
        _ = SemiJoin()(left, right)
