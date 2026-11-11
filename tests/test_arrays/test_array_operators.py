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
from orcapod.sources import DictSource
from tests.test_arrays.array_cases import (
    ARRAYS,
    assert_arrays_identical,
    assert_same_arrays_any_order,
)

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
        source_id="other",
    )


def test_join() -> None:
    got = rows(Join()(arrays_source(), other_source()))
    assert sorted(got) == NAMES
    for name in NAMES:
        assert_arrays_identical(got[name]["x"], expected_x(name))


@pytest.mark.xfail(
    strict=True,
    reason="ITL-847: SemiJoin's Acero hash join refuses nested non-key columns, and the "
    "array format is a struct",
)
def test_semijoin() -> None:
    keep = NAMES[::2]
    right = DictSource(
        data=[{"k": name, "y": 0} for name in keep], tag_columns=["k"], source_id="r"
    )
    got = rows(SemiJoin()(arrays_source(), right))
    assert sorted(got) == keep
    for name in keep:
        assert_arrays_identical(got[name]["x"], expected_x(name))


def test_map_data() -> None:
    got = rows(MapData({"x": "z"})(arrays_source()))
    for name in NAMES:
        assert "x" not in got[name]
        assert_arrays_identical(got[name]["z"], expected_x(name))


def test_select_and_drop_data_columns() -> None:
    selected = rows(SelectDataColumns(["x"])(arrays_source()))
    dropped = rows(DropDataColumns(["n"])(arrays_source()))
    for got in (selected, dropped):
        for name in NAMES:
            assert "n" not in got[name]
            assert_arrays_identical(got[name]["x"], expected_x(name))


def test_polars_filter_on_tag() -> None:
    got = rows(PolarsFilter(constraints={"g": "g1"})(arrays_source()))
    expected = [n for n in NAMES if GROUP[n] == "g1"]
    assert sorted(got) == expected
    for name in expected:
        assert_arrays_identical(got[name]["x"], expected_x(name))


def test_batch() -> None:
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
    out = list(GroupBy(by=["g"])(arrays_source()).iter_data())
    assert sorted(fields(t)["g"] for t, _ in out) == ["g0", "g1", "g2"]
    for tag, data in out:
        # GroupBy moves the non-key tag ``k`` into the data as a list.
        names = list(fields(data)["k"])
        assert sorted(names) == sorted(n for n in NAMES if GROUP[n] == fields(tag)["g"])
        for name, x in zip(names, fields(data)["x"], strict=True):
            assert_arrays_identical(x, expected_x(name))


@pytest.mark.parametrize("shift", [0, 1])
def test_merge_join_colliding_array_column(shift: int) -> None:
    left = arrays_source("left")
    right = arrays_source("right", shift=shift)
    got = rows(MergeJoin()(left, right))
    assert sorted(got) == NAMES
    for name in NAMES:
        assert_same_arrays_any_order(
            list(got[name]["x"]), [expected_x(name), expected_x(name, shift)]
        )
