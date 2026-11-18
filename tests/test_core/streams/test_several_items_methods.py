"""Stream methods given several items equal their operators; several column groups (ITL-1236).

Every stream method that builds an operator from several items (names, a mapping,
keywords, predicates) gives the stream that operator gives: same rows, hashes and
column order, for plain tags, data-tags and a mix, on a stream with three tags and three
data columns. ``polars_filter`` merges ``constraint_map`` and keyword constraints, the
keywords winning. Several ``ColumnConfig`` groups together give the ``all_info``
columns of those groups, in the same order.
"""

from __future__ import annotations

import itertools
from collections.abc import Callable
from dataclasses import dataclass

import polars as pl
import pytest

from orcapod.core.operators import (
    AddTags,
    Batch,
    DropColumns,
    DropDataColumns,
    DropNulls,
    DropTagColumns,
    Join,
    MapData,
    MapTags,
    PolarsFilter,
    SelectColumns,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
    TagBy,
)
from orcapod.core.sources import ArrowTableSource
from orcapod.protocols.core_protocols import StreamProtocol

from ..operators.several_items import (
    LAYOUTS,
    MAIN,
    P,
    ROLES,
    TAGS_FIRST,
    Arrangement,
    fingerprint,
    make_source,
    presentation,
)

pytestmark = pytest.mark.filterwarnings(
    "ignore::orcapod.core.data_tags.DataTagMaterializationWarning"
)

# The stream methods are on the stream classes, not on ``StreamProtocol``.
Build = Callable[[ArrowTableSource], StreamProtocol]


@dataclass(frozen=True)
class MethodCase:
    """A stream method call and the operator call it must equal."""

    label: str
    method: Build
    operator: Build


CASES = [
    MethodCase(
        "select_tag_columns",
        lambda s: s.select_tag_columns(["t3", "t1"]),
        lambda s: SelectTagColumns(["t3", "t1"])(s),
    ),
    MethodCase(
        "select_data_columns",
        lambda s: s.select_data_columns(["d3", "d1"]),
        lambda s: SelectDataColumns(["d3", "d1"])(s),
    ),
    MethodCase(
        "drop_tag_columns",
        lambda s: s.drop_tag_columns(["t3", "t1"]),
        lambda s: DropTagColumns(["t3", "t1"])(s),
    ),
    MethodCase(
        "drop_data_columns",
        lambda s: s.drop_data_columns(["d3", "d1"]),
        lambda s: DropDataColumns(["d3", "d1"])(s),
    ),
    MethodCase(
        "select_columns_positional",
        lambda s: s.select_columns("d3", "t2", "d1"),
        lambda s: SelectColumns(["d3", "t2", "d1"])(s),
    ),
    MethodCase(
        "drop_columns_positional",
        lambda s: s.drop_columns("d3", "t2"),
        lambda s: DropColumns(["d3", "t2"])(s),
    ),
    MethodCase(
        "map_tags",
        lambda s: s.map_tags({"t3": "u3", "t1": "u1"}),
        lambda s: MapTags({"t3": "u3", "t1": "u1"})(s),
    ),
    MethodCase(
        "map_data",
        lambda s: s.map_data({"d3": "e3", "d1": "e1"}),
        lambda s: MapData({"d3": "e3", "d1": "e1"})(s),
    ),
    MethodCase(
        "drop_nulls",
        lambda s: s.drop_nulls(["d2", "t2"]),
        lambda s: DropNulls(["d2", "t2"])(s),
    ),
    MethodCase(
        "polars_filter_predicates",
        lambda s: s.polars_filter(pl.col("d1") > 10, pl.col("t3") == 1),
        lambda s: PolarsFilter(predicates=[pl.col("d1") > 10, pl.col("t3") == 1])(s),
    ),
    MethodCase(
        "polars_filter_constraints",
        lambda s: s.polars_filter(t1="a", t2=1),
        lambda s: PolarsFilter(constraints={"t1": "a", "t2": 1})(s),
    ),
    # Keyword constraints win over the map's value for the same name.
    MethodCase(
        "polars_filter_constraint_map_and_keywords",
        lambda s: s.polars_filter(constraint_map={"t1": "a", "t2": 2}, t2=1),
        lambda s: PolarsFilter(constraints={"t1": "a", "t2": 1})(s),
    ),
    MethodCase(
        "add_tags",
        lambda s: s.add_tags(x=1, y="z"),
        lambda s: AddTags({"x": 1, "y": "z"})(s),
    ),
    MethodCase(
        "tag_by",
        lambda s: s.tag_by(["d3", "d1"]),
        lambda s: TagBy(["d3", "d1"])(s),
    ),
]


@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("case", CASES, ids=lambda case: case.label)
def test_a_stream_method_with_several_items_equals_its_operator(
    case: MethodCase, role: str
) -> None:
    source = make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, role))
    by_method, by_operator = case.method(source), case.operator(source)
    assert presentation(by_method) == presentation(by_operator)
    assert fingerprint(by_method) == fingerprint(by_operator)


@pytest.mark.parametrize("role", ROLES)
def test_join_and_semi_join_methods_equal_their_operators(role: str) -> None:
    def build(spec):
        return make_source(spec, Arrangement(spec.tags, TAGS_FIRST, role))

    main, partner = build(MAIN), build(P)
    assert fingerprint(main.join(partner)) == fingerprint(Join()(main, partner))
    assert fingerprint(partner.join(main)) == fingerprint(Join()(main, partner))
    assert fingerprint(main.semi_join(partner)) == fingerprint(
        SemiJoin()(main, partner)
    )


def test_polars_filter_keywords_win_over_the_constraint_map() -> None:
    kept = make_source(MAIN).polars_filter(constraint_map={"t1": "a", "t2": 2}, t2=1)
    assert sorted(data["d1"] for _, data in kept.iter_data()) == [20, 50]  # pyright: ignore[reportArgumentType]


COLUMN_GROUPS = ("meta", "context", "source", "system_tags", "content_hash")


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("role", ROLES)
def test_several_column_groups_keep_the_all_info_column_order(
    role: str, layout: str
) -> None:
    """Every combination of ``ColumnConfig`` groups: the all_info columns, filtered."""
    source = make_source(MAIN, Arrangement(MAIN.tags, layout, role))
    partner = make_source(P, Arrangement(P.tags, layout, role))
    for stream in (source, Join()(source, partner), Batch(batch_size=2)(source)):
        full = stream.as_table(all_info=True).column_names
        for n in range(1, len(COLUMN_GROUPS) + 1):
            for groups in itertools.combinations(COLUMN_GROUPS, n):
                columns = stream.as_table(
                    columns=dict.fromkeys(groups, True)
                ).column_names
                assert len(set(columns)) == len(columns), groups
                assert columns == [c for c in full if c in columns], groups
