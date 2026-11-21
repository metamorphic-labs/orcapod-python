"""Stream methods given several items equal their operators; several column groups (ITL-1236).

Every stream method that builds an operator from several items (names, a mapping,
keywords, predicates) gives the stream that operator gives: same rows, hashes and
column order, for plain tags, data-tags and a mix, on a stream with three tags and three
data columns, with its items in the order given and reversed; a method that takes data
columns is also given a data-tag. ``polars_filter`` merges ``constraint_map`` and keyword constraints, the
keywords winning. Several ``ColumnConfig`` groups together give the ``all_info``
columns of those groups, in the same order.
"""

from __future__ import annotations

import itertools
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

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
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants

from ..operators.several_items import (
    DATA_TAGS,
    LAYOUTS,
    MAIN,
    MIXED,
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
Build = Callable[[ArrowTableSource, Sequence[Any]], StreamProtocol]

# What the order of the items does.
ORDER_FREE = "order_free"  # nothing but, at most, the column order
ORDER_IS_IDENTITY = "order_is_identity"  # the same rows, another operator (hash)

# Roles in which every column a case names is a data-tag or data column, never plain.
DATA_TAG_ROLES = (DATA_TAGS, MIXED)


@dataclass(frozen=True)
class MethodCase:
    """A stream method call and the operator call it must equal, given its items.

    ``items`` are the several items in the order given (pairs for a mapping or
    keywords); every case is also run with them reversed. ``order`` says what that
    changes; ``roles`` are the roles of the source in which the case applies.
    """

    label: str
    items: tuple[Any, ...]
    method: Build
    operator: Build
    order: str = ORDER_FREE
    roles: tuple[str, ...] = ROLES


CASES = [
    MethodCase(
        "select_tag_columns",
        ("t3", "t1"),
        lambda s, i: s.select_tag_columns(list(i)),
        lambda s, i: SelectTagColumns(list(i))(s),
    ),
    MethodCase(
        "select_data_columns",
        ("d3", "d1"),
        lambda s, i: s.select_data_columns(list(i)),
        lambda s, i: SelectDataColumns(list(i))(s),
    ),
    MethodCase(
        "select_data_columns_with_a_data_tag",
        ("t3", "d1"),
        lambda s, i: s.select_data_columns(list(i)),
        lambda s, i: SelectDataColumns(list(i))(s),
        roles=DATA_TAG_ROLES,
    ),
    MethodCase(
        "drop_tag_columns",
        ("t3", "t1"),
        lambda s, i: s.drop_tag_columns(list(i)),
        lambda s, i: DropTagColumns(list(i))(s),
    ),
    MethodCase(
        "drop_data_columns",
        ("d3", "d1"),
        lambda s, i: s.drop_data_columns(list(i)),
        lambda s, i: DropDataColumns(list(i))(s),
    ),
    MethodCase(
        "drop_data_columns_with_a_data_tag",
        ("t3", "d1"),
        lambda s, i: s.drop_data_columns(list(i)),
        lambda s, i: DropDataColumns(list(i))(s),
        roles=DATA_TAG_ROLES,
    ),
    MethodCase(
        "select_columns_positional",
        ("d3", "t2", "d1"),
        lambda s, i: s.select_columns(*i),
        lambda s, i: SelectColumns(list(i))(s),
    ),
    MethodCase(
        "drop_columns_positional",
        ("d3", "t2"),
        lambda s, i: s.drop_columns(*i),
        lambda s, i: DropColumns(list(i))(s),
    ),
    MethodCase(
        "map_tags",
        (("t3", "u3"), ("t1", "u1")),
        lambda s, i: s.map_tags(dict(i)),
        lambda s, i: MapTags(dict(i))(s),
    ),
    MethodCase(
        "map_data",
        (("d3", "e3"), ("d1", "e1")),
        lambda s, i: s.map_data(dict(i)),
        lambda s, i: MapData(dict(i))(s),
    ),
    MethodCase(
        "map_data_with_a_data_tag",
        (("t3", "e3"), ("d1", "e1")),
        lambda s, i: s.map_data(dict(i)),
        lambda s, i: MapData(dict(i))(s),
        roles=DATA_TAG_ROLES,
    ),
    MethodCase(
        "drop_nulls",
        ("d2", "t2"),
        lambda s, i: s.drop_nulls(list(i)),
        lambda s, i: DropNulls(list(i))(s),
    ),
    # The order of the predicates belongs to the filter (docs/concepts/operators.md).
    MethodCase(
        "polars_filter_predicates",
        (pl.col("d1") > 10, pl.col("t3") == 1),
        lambda s, i: s.polars_filter(*i),
        lambda s, i: PolarsFilter(predicates=list(i))(s),
        order=ORDER_IS_IDENTITY,
    ),
    MethodCase(
        "polars_filter_constraints",
        (("t1", "a"), ("t2", 1)),
        lambda s, i: s.polars_filter(**dict(i)),
        lambda s, i: PolarsFilter(constraints=dict(i))(s),
    ),
    # Keyword constraints win over the map's value for the same name.
    MethodCase(
        "polars_filter_constraint_map_and_keywords",
        (("t1", "a"), ("t2", 2)),
        lambda s, i: s.polars_filter(constraint_map=dict(i), t2=1),
        lambda s, i: PolarsFilter(constraints={**dict(i), "t2": 1})(s),
    ),
    # The new tags are a set (ITL-1269): identity and placement in name order.
    MethodCase(
        "add_tags",
        (("x", 1), ("y", "z")),
        lambda s, i: s.add_tags(**dict(i)),
        lambda s, i: AddTags(dict(i))(s),
    ),
    # TagBy takes data columns only; a data-tag is refused (already a tag).
    MethodCase(
        "tag_by",
        ("d3", "d1"),
        lambda s, i: s.tag_by(list(i)),
        lambda s, i: TagBy(list(i))(s),
    ),
]


@pytest.mark.parametrize(
    ("case", "role"),
    [
        pytest.param(case, role, id=f"{case.label}-{role}")
        for case in CASES
        for role in case.roles
    ],
)
def test_a_stream_method_with_several_items_equals_its_operator(
    case: MethodCase, role: str
) -> None:
    """In the order given and reversed: the method equals its operator.

    Reversing the items keeps the rows; it changes nothing else for an order-free
    argument, and the operator's identity (content and pipeline hash) for one whose
    order is part of it.
    """
    source = make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, role))
    results = []
    for items in (case.items, tuple(reversed(case.items))):
        by_method, by_operator = (
            case.method(source, items),
            case.operator(source, items),
        )
        assert presentation(by_method) == presentation(by_operator)
        assert fingerprint(by_method) == fingerprint(by_operator)
        results.append(by_method)
    forward, backward = results
    assert fingerprint(forward)["rows"] == fingerprint(backward)["rows"]
    if case.order == ORDER_FREE:
        assert fingerprint(forward) == fingerprint(backward)
        assert presentation(forward) == presentation(backward)
    else:
        assert forward.content_hash() != backward.content_hash()
        assert forward.pipeline_hash() != backward.pipeline_hash()


@pytest.mark.parametrize("role", ROLES)
def test_tag_by_refuses_a_data_tag_in_either_order(role: str) -> None:
    """``TagBy`` gives a data column the tag role: a data-tag already has it."""
    source = make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, role))
    for names in (["t3", "d1"], ["d1", "t3"]):
        with pytest.raises(InputValidationError, match="t3"):
            TagBy(names)(source)
        with pytest.raises(InputValidationError, match="t3"):
            source.tag_by(names)


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
CONTENT_HASH_COLUMN = "_content_hash"


def _group_of(name: str) -> str | None:
    """The ``ColumnConfig`` group of a column name, or ``None`` for a user column.

    Derived from the column naming convention (CLAUDE.md, "Column naming
    conventions"), not from the stream under test.
    """
    if name.startswith(constants.META_PREFIX):
        return "meta"
    if name == constants.CONTEXT_KEY:
        return "context"
    if name.startswith(constants.SOURCE_PREFIX):
        return "source"
    if name.startswith(constants.SYSTEM_TAG_PREFIX):
        return "system_tags"
    if name == CONTENT_HASH_COLUMN:
        return "content_hash"
    return None


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("role", ROLES)
def test_several_column_groups_give_exactly_their_columns_in_the_all_info_order(
    role: str, layout: str
) -> None:
    """Every combination of groups: the user columns and those groups' columns, in order.

    The expected list is the ``all_info`` table's columns kept by their group, so a
    missing requested column and an unrequested group both fail.
    """
    source = make_source(MAIN, Arrangement(MAIN.tags, layout, role))
    partner = make_source(P, Arrangement(P.tags, layout, role))
    for stream in (source, Join()(source, partner), Batch(batch_size=2)(source)):
        full = stream.as_table(all_info=True).column_names
        present = {_group_of(name) for name in full} - {None}
        # Every group but meta shows up in all_info for these streams.
        assert present == {"context", "source", "system_tags", "content_hash"}
        for n in range(1, len(COLUMN_GROUPS) + 1):
            for groups in itertools.combinations(COLUMN_GROUPS, n):
                expected = [
                    name
                    for name in full
                    if _group_of(name) is None or _group_of(name) in groups
                ]
                columns = stream.as_table(
                    columns=dict.fromkeys(groups, True)
                ).column_names
                assert columns == expected, groups
