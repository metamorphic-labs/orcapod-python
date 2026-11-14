"""Referential tags through operators (ITL-1023).

Specification "Orcapod v0.4 — Referential Tags" (ITL-1004): ``Join`` section 9 and
tests 15.4; ``GroupBy`` section 8 and tests 15.3; select and drop sections 7 and 10 and
tests 15.5; row-preserving operators section 10. Operators the slice does not cover raise
"not supported with referential tags yet" instead of giving a wrong answer.

Every operator is driven through its public call, ``op(stream)``, and read through the
stream's public methods.
"""

from __future__ import annotations

import warnings
from collections.abc import Mapping, Sequence
from typing import Any

import pytest

import orcapod as op
from orcapod.core import referential_tags
from orcapod.core.operators import (
    Batch,
    DropDataColumns,
    DropNulls,
    DropTagColumns,
    GroupBy,
    Index,
    Join,
    MapData,
    MapTags,
    MergeJoin,
    Pick,
    PolarsFilter,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.core.referential_tags import (
    ReferenceMaterializationWarning,
    stream_references,
)
from orcapod.errors import InputValidationError
from orcapod.utils import arrow_utils


def _ref(rows: Sequence[Mapping[str, Any]], tags: list[str], sid: str, **kw: Any):
    return op.sources.DictSource(rows, tag_columns=tags, source_id=sid, **kw)


def _a(**kw: Any):
    return _ref([{"x": 1, "a": 0.5}, {"x": 2, "a": 0.6}], ["x"], "A", **kw)


def _b(**kw: Any):
    return _ref([{"x": 1, "b": 10}, {"x": 3, "b": 30}], ["x"], "B", **kw)


def _rows(stream) -> list[dict[str, object]]:
    return sorted(stream.as_table().to_pylist(), key=lambda r: repr(sorted(r.items())))


# ── Join (section 9) ────────────────────────────────────────────────────────


def test_join_plain_with_plain_is_unchanged() -> None:
    joined = Join()(_a(tag_only_columns=True), _b(tag_only_columns=True))
    assert joined.keys() == (("x",), ("a", "b")) or joined.keys() == (
        ("x",),
        ("b", "a"),
    )
    assert stream_references(joined) == ()
    assert _rows(joined) == [{"x": 1, "a": 0.5, "b": 10}]


@pytest.mark.parametrize("plain_side", ["a", "b"])
def test_join_plain_with_referential_gives_a_referential_tag(plain_side: str) -> None:
    a = _a(tag_only_columns=True) if plain_side == "a" else _a()
    b = _b(tag_only_columns=True) if plain_side == "b" else _b()
    joined = Join()(a, b)
    tags, data = joined.keys()
    assert tags == ("x",) and set(data) == {"x", "a", "b"}
    assert stream_references(joined) == ("x",)
    table = joined.as_table(columns={"source": True})
    assert table.column_names.count("x") == 1
    # The data column comes from the referential side, with its source info.
    side = "B" if plain_side == "a" else "A"
    assert table.column("_source_x").to_pylist() == [f"{side}::row_0::x"]


def test_join_referential_with_referential_merges_one_data_column() -> None:
    joined = Join()(_a(), _b())
    assert stream_references(joined) == ("x",)
    table = joined.as_table(columns={"source": True})
    assert table.column_names.count("x") == 1
    assert [{k: r[k] for k in ("x", "a", "b")} for r in table.to_pylist()] == [
        {"x": 1, "a": 0.5, "b": 10}
    ]
    # Both inputs' provenance is kept, folded into one merged token: the merged value
    # is scalar, so is its source info (developer decision, November 13, 2026, ITL-1017).
    (token,) = table.column("_source_x").to_pylist()
    assert token == arrow_utils.fold_source_info_tokens(["A::row_0::x", "B::row_0::x"])


def test_join_is_commutative_with_references() -> None:
    assert (
        Join()(_a(), _b()).as_table(columns={"source": True}).to_pylist()
        == Join()(_b(), _a()).as_table(columns={"source": True}).to_pylist()
    )


def test_join_rejects_other_data_collisions() -> None:
    other = _ref([{"k": 1, "a": 0.9}], ["k"], "C")  # data ``a`` again, not a reference
    with pytest.raises(InputValidationError, match="overlapping"):
        Join()(_a(), other)
    # Data-only ``x`` on one side, referential ``x`` on the other: not equated by a tag.
    data_only = _ref([{"k": 1, "x": 1}], ["k"], "D")
    with pytest.raises(InputValidationError):
        Join()(_a(), data_only)


def test_join_rejects_a_plain_tag_named_like_a_data_column() -> None:
    plain_tag = _ref([{"x": 1, "c": 2}], ["x"], "P", tag_only_columns=True)
    data_only = _ref([{"k": 1, "x": 1}], ["k"], "D")
    with pytest.raises(InputValidationError):
        Join()(plain_tag, data_only)


def test_join_rejects_disagreeing_reference_types() -> None:
    as_text = _ref([{"x": "1", "b": 10}], ["x"], "T")
    with pytest.raises(InputValidationError):
        Join()(_a(), as_text)


def test_join_many_to_many_keeps_row_alignment() -> None:
    left = _ref(
        [{"x": 1, "u": 1, "a": 0.1}, {"x": 1, "u": 2, "a": 0.2}], ["x", "u"], "L"
    )
    right = _ref([{"x": 1, "v": 1, "b": 1}, {"x": 1, "v": 2, "b": 2}], ["x", "v"], "R")
    joined = Join()(left, right)
    rows = joined.as_table().to_pylist()
    assert len(rows) == 4
    assert all(r["x"] == 1 for r in rows)
    for tag, data in joined.iter_data():
        assert tag["x"] == data["x"]


# ── GroupBy (section 8) ─────────────────────────────────────────────────────


GROUP_ROWS = [
    {"s": "s1", "x": 20, "y": 5},
    {"s": "s1", "x": 10, "y": 4},
    {"s": "s2", "x": 5, "y": 1},
]


def test_group_by_referential_key_stays_scalar_in_both_roles() -> None:
    grouped = GroupBy(["x"])(_ref(GROUP_ROWS, ["s", "x"], "G"))
    tag_schema, data_schema = grouped.output_schema()
    assert tag_schema["x"] is int and data_schema["x"] is int
    # The non-key ``s`` is dropped in both roles (ITL-1070).
    assert stream_references(grouped) == ("x",)
    for tag, data in grouped.iter_data():
        assert tag["x"] == data["x"] and isinstance(data["x"], int)


def test_group_by_referential_non_key_keeps_its_data_as_a_list() -> None:
    grouped = GroupBy(["s"])(_ref(GROUP_ROWS, ["s", "x"], "G"))
    # A non-key tag loses its tag role (ITL-1070); a referential one keeps its data
    # column, aggregated like every other data column (developer, November 13, 2026).
    tags, data = grouped.keys()
    assert tags == ("s",) and set(data) == {"s", "x", "y"}
    assert stream_references(grouped) == ("s",)
    _, data_schema = grouped.output_schema()
    assert data_schema["x"] == list[int]
    rows = {tag["s"]: data for tag, data in grouped.iter_data()}
    # ``x`` orders the members, and ``y`` follows that order.
    assert rows["s1"]["x"] == [10, 20]
    assert rows["s1"]["y"] == [4, 5]
    assert grouped.as_table().column_names.count("x") == 1


def test_group_by_prediction_matches_the_result() -> None:
    source = _ref(GROUP_ROWS, ["s", "x"], "G")
    for by in (["s"], ["x"]):
        group_by = GroupBy(by)
        # Schema prediction checked against static_process (CLAUDE.md standing rule).
        assert (
            group_by.output_schema(source)
            == group_by.static_process(source).output_schema()
        )


# ── select and drop (sections 7 and 10) ─────────────────────────────────────


def test_selecting_away_the_referenced_data_leaves_a_plain_tag() -> None:
    source = _ref([{"x": 1, "y": 2}], ["x"], "S")
    with pytest.warns(ReferenceMaterializationWarning):
        selected = SelectDataColumns(["y"])(source)
    assert selected.keys() == (("x",), ("y",))
    assert stream_references(selected) == ()
    assert selected.as_table().to_pylist() == [{"x": 1, "y": 2}]
    assert "_source_x" not in selected.as_table(columns={"source": True}).column_names


def test_dropping_the_referenced_data_leaves_a_plain_tag() -> None:
    source = _ref([{"x": 1, "y": 2}], ["x"], "S")
    with pytest.warns(ReferenceMaterializationWarning):
        dropped = DropDataColumns(["x"])(source)
    assert dropped.keys() == (("x",), ("y",))


def test_silent_materialization_changes_nothing_but_the_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _ref([{"x": 1, "y": 2}], ["x"], "S")
    with pytest.warns(ReferenceMaterializationWarning):
        warned = SelectDataColumns(["y"])(source)
    monkeypatch.setattr(referential_tags, "reference_materialization", "silent")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        silent = SelectDataColumns(["y"])(source)
    assert silent.content_hash() == warned.content_hash()
    assert silent.pipeline_hash() == warned.pipeline_hash()
    assert silent.as_table().to_pylist() == warned.as_table().to_pylist()


@pytest.mark.parametrize(
    "drop", [lambda: DropTagColumns(["x"]), lambda: SelectTagColumns(["k"])]
)
def test_removing_a_referential_tag_keeps_its_data(drop) -> None:
    source = _ref([{"k": 1, "x": 1, "y": 2}], ["k", "x"], "S", tag_only_columns=["k"])
    result = drop()(source)
    assert result.keys() == (("k",), ("x", "y"))
    assert stream_references(result) == ()
    ((tag, data),) = list(result.iter_data())
    assert tag.as_dict() == {"k": 1}
    assert data.as_dict() == {"x": 1, "y": 2}
    assert data.source_info()["x"] == "S::row_0::x"
    assert data.source_info()["x"] == "S::row_0::x"


# ── row-preserving operators (section 10) ───────────────────────────────────


def _rows_source():
    return _ref(
        [
            {"x": 1, "y": None, "l": [1, 2], "d": {"q": 1}},
            {"x": 2, "y": 3, "l": [3], "d": {"q": 2}},
        ],
        ["x"],
        "R",
    )


@pytest.mark.parametrize(
    "make_op",
    [
        lambda: PolarsFilter(constraints={"x": 2}),
        lambda: DropNulls(),
        lambda: DropNulls(["y"]),
        lambda: Index("l", 0, out="first"),
        lambda: Pick("d", "q", out="q"),
    ],
    ids=["filter", "drop_nulls_all", "drop_nulls", "index", "pick"],
)
def test_row_preserving_operators_keep_references(make_op) -> None:
    result = make_op()(_rows_source())
    assert stream_references(result) == ("x",)
    assert result.as_table().column_names.count("x") == 1
    for tag, data in result.iter_data():
        assert tag["x"] == data["x"]


def test_semijoin_keeps_the_left_references() -> None:
    result = SemiJoin()(_a(), _b())
    assert stream_references(result) == ("x",)
    assert [r["x"] for r in result.as_table().to_pylist()] == [1]


@pytest.mark.parametrize("make_op", [lambda: Index("x", 0), lambda: Pick("x", "q")])
def test_in_place_extraction_of_a_referenced_column_raises(make_op) -> None:
    source = _ref([{"x": [1], "y": 2}], ["x"], "R")
    with pytest.raises(InputValidationError, match="referential tag"):
        make_op()(source)


def test_extraction_named_like_a_tag_raises() -> None:
    source = _ref([{"k": 1, "l": [1]}], ["k"], "R")
    with pytest.raises(InputValidationError, match="tag"):
        Index("l", 0, out="k")(source)


# ── not supported yet: raise, never a wrong answer ──────────────────────────


@pytest.mark.parametrize(
    "call, issue",
    [
        (lambda: Batch(batch_size=2)(_a()), "ITL-1017"),
        (lambda: MergeJoin()(_a(), _b()), "ITL-1018"),
    ],
    ids=["batch", "merge_join"],
)
def test_uncovered_operators_raise(call, issue: str) -> None:
    with pytest.raises(NotImplementedError, match=issue):
        call()


# ── renames (developer decision, November 12, 2026) ─────────────────────────


def test_renames_that_leave_references_alone_work() -> None:
    renamed = MapData({"a": "z"})(_a())
    assert renamed.keys() == (("x",), ("x", "z"))
    assert stream_references(renamed) == ("x",)
    retagged = MapTags({"k": "kk"})(
        _ref([{"k": 1, "x": 1, "y": 2}], ["k", "x"], "M", tag_only_columns=["k"])
    )
    assert retagged.keys() == (("kk", "x"), ("x", "y"))
    assert stream_references(retagged) == ("x",)


def test_map_tags_renames_a_referential_tag_in_both_roles() -> None:
    renamed = MapTags({"x": "z"})(_a())
    assert renamed.keys() == (("z",), ("z", "a"))
    assert stream_references(renamed) == ("z",)
    table = renamed.as_table(columns={"source": True})
    assert "x" not in table.column_names and table.column_names.count("z") == 1
    assert table.column("_source_z").to_pylist() == ["A::row_0::x", "A::row_1::x"]
    # The prediction agrees with the result (checked against static_process).
    op_ = MapTags({"x": "z"})
    assert op_.output_schema(_a()) == op_.static_process(_a()).output_schema()


def test_map_tags_tag_only_leaves_a_plain_tag_and_the_data_column() -> None:
    renamed = MapTags({"x": "z"}, tag_only=True)(_a())
    assert renamed.keys() == (("z",), ("x", "a"))
    assert stream_references(renamed) == ()
    for tag, data in renamed.iter_data():
        assert tag["z"] == data["x"]
    # ``tag_only`` enters the identity only when set.
    assert (
        MapTags({"x": "z"}).content_hash()
        != MapTags({"x": "z"}, tag_only=True).content_hash()
    )


def test_map_data_is_a_data_only_move() -> None:
    renamed = MapData({"x": "z"})(_a())
    assert renamed.keys() == (("x",), ("z", "a"))
    assert stream_references(renamed) == ()
    for tag, data in renamed.iter_data():
        assert tag["x"] == data["z"]
    assert renamed.as_table(columns={"source": True}).column(
        "_source_z"
    ).to_pylist() == ["A::row_0::x", "A::row_1::x"]


def test_map_data_dropping_an_unmapped_reference_leaves_a_plain_tag() -> None:
    renamed = MapData({"a": "z"}, drop_unmapped=True)(_a())
    assert renamed.keys() == (("x",), ("z",))


def test_map_tags_dropping_an_unmapped_reference_keeps_its_data() -> None:
    source = _ref([{"k": 1, "x": 1, "y": 2}], ["k", "x"], "M")
    renamed = MapTags({"k": "kk"}, drop_unmapped=True)(source)
    assert renamed.keys() == (("kk",), ("kk", "x", "y"))
    assert stream_references(renamed) == ("kk",)


def test_map_tags_on_plain_tags_is_unchanged() -> None:
    plain = _a(tag_only_columns=True)
    assert MapTags({"x": "z"})(plain).keys() == (("z",), ("a",))


def test_uncovered_operators_still_work_on_plain_tags() -> None:
    plain = _a(tag_only_columns=True)
    assert Batch(batch_size=2)(plain).keys() == (("x",), ("a",))


def test_derived_source_over_a_referential_stream_raises() -> None:
    with pytest.raises(NotImplementedError, match="ITL-1019"):
        op.sources.DerivedSource(origin=_a(), tags_as_data={"x": "x_copy"})
