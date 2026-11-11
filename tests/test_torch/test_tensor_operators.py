"""Tensor columns pass through every operator unchanged (ITL-871).

Each source holds one row per entry of ``TENSORS``, so a single column carries
every dtype and shape class at once. Operators are called on streams and read
back through the stream's public methods.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("torch")

from orcapod.operators import (  # noqa: E402
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
from orcapod.sources import DictSource  # noqa: E402
from tests.test_torch.tensor_cases import (  # noqa: E402
    TENSORS,
    assert_same_tensors_any_order,
    assert_tensors_identical,
)

NAMES = sorted(TENSORS)
GROUP = {name: f"g{i % 3}" for i, name in enumerate(NAMES)}


def tensors_source(source_id: str = "tensors", shift: int = 0) -> DictSource:
    """One row per tensor; ``shift`` rotates which tensor each tag gets."""
    return DictSource(
        data=[
            {
                "k": name,
                "g": GROUP[name],
                "x": TENSORS[NAMES[(i + shift) % len(NAMES)]],
                "n": i,
            }
            for i, name in enumerate(NAMES)
        ],
        tag_columns=["k", "g"],
        source_id=source_id,
    )


def expected_x(name: str, shift: int = 0) -> Any:
    return TENSORS[NAMES[(NAMES.index(name) + shift) % len(NAMES)]]


def fields(datagram: Any) -> dict[str, Any]:
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
    got = rows(Join()(tensors_source(), other_source()))
    assert sorted(got) == NAMES
    for name in NAMES:
        assert_tensors_identical(got[name]["x"], expected_x(name))


def test_semijoin() -> None:
    keep = NAMES[::2]
    right = DictSource(
        data=[{"k": name, "y": 0} for name in keep], tag_columns=["k"], source_id="r"
    )
    got = rows(SemiJoin()(tensors_source(), right))
    assert sorted(got) == keep
    for name in keep:
        assert_tensors_identical(got[name]["x"], expected_x(name))


def test_map_data() -> None:
    got = rows(MapData({"x": "z"})(tensors_source()))
    for name in NAMES:
        assert "x" not in got[name]
        assert_tensors_identical(got[name]["z"], expected_x(name))


def test_select_and_drop_data_columns() -> None:
    selected = rows(SelectDataColumns(["x"])(tensors_source()))
    dropped = rows(DropDataColumns(["n"])(tensors_source()))
    for got in (selected, dropped):
        for name in NAMES:
            assert "n" not in got[name]
            assert_tensors_identical(got[name]["x"], expected_x(name))


def test_polars_filter_on_tag() -> None:
    got = rows(PolarsFilter(constraints={"g": "g1"})(tensors_source()))
    expected = [n for n in NAMES if GROUP[n] == "g1"]
    assert sorted(got) == expected
    for name in expected:
        assert_tensors_identical(got[name]["x"], expected_x(name))


def test_batch() -> None:
    out = list(Batch(batch_size=4)(tensors_source()).iter_data())
    seen: list[str] = []
    for tag, data in out:
        names = list(fields(tag)["k"])
        xs = fields(data)["x"]
        assert len(names) == len(xs)
        for name, x in zip(names, xs, strict=True):
            assert_tensors_identical(x, expected_x(name))
        seen.extend(names)
    assert sorted(seen) == NAMES


def test_group_by() -> None:
    out = list(GroupBy(by=["g"])(tensors_source()).iter_data())
    assert sorted(fields(t)["g"] for t, _ in out) == ["g0", "g1", "g2"]
    for tag, data in out:
        # GroupBy moves the non-key tag ``k`` into the data as a list.
        names = list(fields(data)["k"])
        assert sorted(names) == sorted(n for n in NAMES if GROUP[n] == fields(tag)["g"])
        for name, x in zip(names, fields(data)["x"], strict=True):
            assert_tensors_identical(x, expected_x(name))


@pytest.mark.parametrize("shift", [0, 1])
def test_merge_join_colliding_tensor_column(shift: int) -> None:
    left = tensors_source("left")
    right = tensors_source("right", shift=shift)
    got = rows(MergeJoin()(left, right))
    assert sorted(got) == NAMES
    for name in NAMES:
        assert_same_tensors_any_order(
            list(got[name]["x"]), [expected_x(name), expected_x(name, shift)]
        )


def test_merge_join_order_does_not_depend_on_input_order() -> None:
    forward = rows(MergeJoin()(tensors_source("a"), tensors_source("b", shift=1)))
    backward = rows(MergeJoin()(tensors_source("b", shift=1), tensors_source("a")))
    for name in NAMES:
        f, b = list(forward[name]["x"]), list(backward[name]["x"])
        assert len(f) == len(b) == 2
        for x, y in zip(f, b, strict=True):
            assert_tensors_identical(x, y)
