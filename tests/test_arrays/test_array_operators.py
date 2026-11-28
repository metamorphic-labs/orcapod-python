"""Array columns pass through every operator unchanged (ITL-888).

Each source holds one row per entry of ``ARRAYS``, so a single column carries
every dtype and shape class at once. Operators are called on streams and read
back through the stream's public methods.
"""

from __future__ import annotations

from typing import Any

import pytest

from orcapod.operators import (
    Batch,
    DropDataColumns,
    GroupBy,
    Join,
    MapData,
    MergeJoin,
    PolarsFilter,
    SelectDataColumns,
    SemiJoin,
)
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.sources import DictSource
from tests.test_arrays.array_cases import (
    ARRAYS,
    assert_arrays_identical,
)
from tests.test_core.operators.test_async_sync_equivalence import (
    async_rows,
    sync_rows,
)

# PolarsFilter is built here on purpose; the warning of every construction is tested in
# test_polars_filter_warning.py (ITL-1296).
pytestmark = pytest.mark.filterwarnings("ignore::orcapod.errors.PolarsFilterWarning")

NAMES = sorted(ARRAYS)
GROUP = {name: f"g{i % 3}" for i, name in enumerate(NAMES)}


def arrays_source(source_id: str = "arrays", shift: int = 0) -> DictSource:
    """One row per array; ``shift`` rotates which array each tag gets."""
    return DictSource(
        data=[
            {
                "k": name,
                "g": GROUP[name],
                "x": ARRAYS[NAMES[(i + shift) % len(NAMES)]],
                "n": i,
            }
            for i, name in enumerate(NAMES)
        ],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id=source_id,
    )


def expected_x(name: str, shift: int = 0) -> Any:
    return ARRAYS[NAMES[(NAMES.index(name) + shift) % len(NAMES)]]


def fields(datagram: Any) -> dict[str, Any]:
    """A tag's or data's values, typed loosely for the assertions below."""
    return dict(datagram.as_dict())


def rows(stream: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for tag, data in stream.iter_data():
        out[str(tag.as_dict()["k"])] = {**tag.as_dict(), **data.as_dict()}
    return out


def other_source() -> DictSource:
    return DictSource(
        data=[{"k": name, "y": i} for i, name in enumerate(NAMES)],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="other",
    )


def test_join() -> None:
    """``Join`` passes array columns through unchanged."""
    got = rows(Join()(arrays_source(), other_source()))
    assert sorted(got) == NAMES
    for name in NAMES:
        assert_arrays_identical(got[name]["x"], expected_x(name))


def test_semijoin() -> None:
    """``SemiJoin`` keeps the matching rows with their arrays unchanged."""
    keep = NAMES[::2]
    right = DictSource(
        data=[{"k": name, "y": 0} for name in keep],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="r",
    )
    got = rows(SemiJoin()(arrays_source(), right))
    assert sorted(got) == keep
    for name in keep:
        assert_arrays_identical(got[name]["x"], expected_x(name))


def test_map_data() -> None:
    """``MapData`` renames an array column and keeps its values."""
    got = rows(MapData({"x": "z"})(arrays_source()))
    for name in NAMES:
        assert "x" not in got[name]
        assert_arrays_identical(got[name]["z"], expected_x(name))


def test_select_and_drop_data_columns() -> None:
    """Selecting or dropping data columns keeps the array column unchanged."""
    selected = rows(SelectDataColumns(["x"])(arrays_source()))
    dropped = rows(DropDataColumns(["n"])(arrays_source()))
    for got in (selected, dropped):
        for name in NAMES:
            assert "n" not in got[name]
            assert_arrays_identical(got[name]["x"], expected_x(name))


def test_polars_filter_on_tag() -> None:
    """``PolarsFilter`` on a tag keeps the matching rows with their arrays unchanged."""
    got = rows(PolarsFilter(constraints={"g": "g1"})(arrays_source()))
    expected = [n for n in NAMES if GROUP[n] == "g1"]
    assert sorted(got) == expected
    for name in expected:
        assert_arrays_identical(got[name]["x"], expected_x(name))


def test_batch() -> None:
    """``Batch`` collects array values into lists, in row order, with none lost."""
    out = list(Batch(batch_size=4)(arrays_source()).iter_data())
    seen: list[str] = []
    for tag, data in out:
        names = list(fields(tag)["k"])
        xs = fields(data)["x"]
        assert len(names) == len(xs)
        for name, x in zip(names, xs, strict=True):
            assert_arrays_identical(x, expected_x(name))
        seen.extend(names)
    assert sorted(seen) == NAMES


def test_group_by() -> None:
    """``GroupBy`` collects each group's arrays into a list."""
    out = list(GroupBy(by=["g"])(arrays_source()).iter_data())
    assert sorted(fields(t)["g"] for t, _ in out) == ["g0", "g1", "g2"]
    for tag, data in out:
        # GroupBy drops the non-key tag ``k`` (ITL-1070); it still orders the
        # members, so the group's names come in ascending order.
        assert set(fields(tag)) == {"g"}
        names = sorted(n for n in NAMES if GROUP[n] == fields(tag)["g"])
        for name, x in zip(names, fields(data)["x"], strict=True):
            assert_arrays_identical(x, expected_x(name))


@pytest.mark.parametrize("shift", [0, 1])
def test_merge_join_colliding_array_column(shift: int) -> None:
    """``MergeJoin`` merges a colliding array column into a list holding both sides' arrays."""
    left = arrays_source("left")
    right = arrays_source("right", shift=shift)
    got = rows(MergeJoin()(left, right))
    assert sorted(got) == NAMES
    for name in NAMES:
        merged = list(got[name]["x"])
        assert len(merged) == 2
        # The first argument's value first (ITL-1281).
        assert_arrays_identical(merged[0], expected_x(name))
        assert_arrays_identical(merged[1], expected_x(name, shift))


def grouped_arrays_source(source_id: str, shift: int = 0) -> StreamProtocol:
    """``arrays_source`` grouped on ``g``, so ``x`` becomes ``list[np.ndarray]``."""
    return GroupBy(by=["g"])(arrays_source(source_id, shift))


def assert_merged_groups(
    got: dict[str, Any], second_shift: int, first_shift: int = 0
) -> None:
    """Each group's merged ``x`` holds the first argument's member list, then the
    second's, in that order (``MergeJoin`` is ordered, ITL-1281)."""
    assert sorted(got) == ["g0", "g1", "g2"]
    for group, merged in got.items():
        names = [n for n in NAMES if GROUP[n] == group]
        expected = [
            [expected_x(n, first_shift) for n in names],
            [expected_x(n, second_shift) for n in names],
        ]
        assert len(merged) == len(expected) == 2, group
        for members, want in zip(merged, expected, strict=True):
            assert len(members) == len(want), group
            for one, other in zip(members, want, strict=True):
                assert_arrays_identical(one, other)


@pytest.mark.parametrize("shift", [0, 1])
def test_merge_join_colliding_grouped_array_column(shift: int) -> None:
    """``MergeJoin`` merges colliding lists of arrays (after ``GroupBy``), in argument order."""
    left = grouped_arrays_source("left")
    right = grouped_arrays_source("right", shift)
    out = MergeJoin()(left, right)
    got = {fields(t)["g"]: fields(d)["x"] for t, d in out.iter_data()}
    assert_merged_groups(got, shift)
    # The swapped call holds the same pairs, reversed: MergeJoin is ordered (ITL-1281).
    swapped = MergeJoin()(right, left)
    swapped_got = {fields(t)["g"]: fields(d)["x"] for t, d in swapped.iter_data()}
    assert_merged_groups(swapped_got, 0, first_shift=shift)


def test_merge_join_colliding_grouped_array_column_async_equals_sync() -> None:
    """The async ``MergeJoin`` merges lists of arrays into the same data as the sync path."""
    left = grouped_arrays_source("left")
    right = grouped_arrays_source("right", 1)
    expected = {fields(t)["g"]: d for t, d in sync_rows(MergeJoin(), left, right)}
    # The async path is under test: this helper drives ``async_execute`` over channels.
    actual = {fields(t)["g"]: d for t, d in async_rows(MergeJoin(), left, right)}
    # Data only: after GroupBy or Batch, MergeJoin's async system tag column names
    # differ from the sync ones on main too, for any column type (reported separately).
    assert sorted(actual) == sorted(expected)
    for group, data in actual.items():
        assert data.arrow_schema() == expected[group].arrow_schema()
        assert data.content_hash() == expected[group].content_hash()
        assert data.source_info() == expected[group].source_info()
    assert_merged_groups({g: fields(d)["x"] for g, d in actual.items()}, 1)
