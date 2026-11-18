"""Operators with several items: several columns, names, mappings and inputs (ITL-1236).

Every argument that takes several items is tested here with two or more, in more than
one order, in each role (plain tag, data, data-tag), on inputs with three tags and three
data columns in several arrangements (``several_items``), on the sync and the async path.

What is asserted:

* **Arrangement.** An operator's result does not depend on the arrangement of its input:
  the order of the tag columns, the physical layout of the table, for each role. The
  rows (values, system tags, row hashes, source info), the stream's content and pipeline
  hashes and the column names are the same (``fingerprint``); only the column order
  follows the input.
* **Order of a set argument.** Column names to select, drop or narrow, and the entries
  of a rename mapping, name a set: their order changes nothing, not even the column
  order of the result, nor the operator's identity.
* **Order of the inputs of a commutative operator** (``Join``, ``MergeJoin``) changes
  nothing, with two, three and four inputs.

``GroupBy`` is not here: its arrangement and ``by`` order are tested by ITL-1226
(``test_group_by_member_order.py``). The order of ``AddTags``'s names is the order of
the new tags and part of its identity, and is tested as such below.
"""

from __future__ import annotations

import itertools
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import polars as pl
import pyarrow as pa
import pytest

from orcapod.core.operators import (
    AddTags,
    Batch,
    DropColumns,
    DropDataColumns,
    DropNulls,
    DropTagColumns,
    Join,
    MapColumns,
    MapData,
    MapTags,
    MergeJoin,
    PolarsFilter,
    SelectColumns,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
    TagBy,
)
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.sources import ArrowTableSource
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol

from .several_items import (
    DATA_TAGS,
    MAIN,
    MIXED,
    P,
    PLAIN,
    Q,
    R,
    ROLES,
    TAGS_FIRST,
    Arrangement,
    Spec,
    arrangements,
    async_rows,
    fingerprint,
    make_source,
    presentation,
    rows_fingerprint,
)

# Selecting or dropping the data role of a data-tag warns that the tag is materialized;
# that warning is tested in the data-tag tests, not here.
pytestmark = pytest.mark.filterwarnings(
    "ignore::orcapod.core.data_tags.DataTagMaterializationWarning"
)


@dataclass(frozen=True)
class UnaryCase:
    """An operator applied to ``MAIN``, and the roles it supports."""

    label: str
    make_op: Callable[[], StaticOutputOperatorPod]
    roles: tuple[str, ...] = ROLES


UNARY_CASES = [
    UnaryCase("select_tag_columns", lambda: SelectTagColumns(["t3", "t1"])),
    UnaryCase("select_data_columns", lambda: SelectDataColumns(["d3", "d1"])),
    UnaryCase("drop_tag_columns", lambda: DropTagColumns(["t3", "t1"])),
    UnaryCase("drop_data_columns", lambda: DropDataColumns(["d3", "d1"])),
    UnaryCase("select_columns", lambda: SelectColumns(["d3", "t2", "d1", "t1"])),
    UnaryCase("drop_columns", lambda: DropColumns(["d3", "t2"])),
    UnaryCase("map_tags", lambda: MapTags({"t3": "u3", "t1": "u1"})),
    UnaryCase(
        "map_tags_drop_unmapped",
        lambda: MapTags({"t3": "u3", "t1": "u1"}, drop_unmapped=True),
    ),
    UnaryCase("map_tags_tag_only", lambda: MapTags({"t3": "u3"}, tag_only=True)),
    UnaryCase("map_data", lambda: MapData({"d3": "e3", "d1": "e1"})),
    UnaryCase(
        "map_data_drop_unmapped",
        lambda: MapData({"d3": "e3", "d1": "e1"}, drop_unmapped=True),
    ),
    # MapColumns is refused with data-tags (ITL-1022).
    UnaryCase(
        "map_columns", lambda: MapColumns({"d3": "e3", "t1": "u1"}), roles=(PLAIN,)
    ),
    UnaryCase(
        "polars_filter_constraints",
        lambda: PolarsFilter(constraints={"t1": "a", "t2": 1}),
    ),
    UnaryCase(
        "polars_filter_predicates",
        lambda: PolarsFilter(predicates=[pl.col("d1") > 10, pl.col("t3") == 1]),
    ),
    UnaryCase("drop_nulls_named", lambda: DropNulls(["d2", "t2"])),
    UnaryCase("drop_nulls_all", DropNulls),
    UnaryCase("batch", lambda: Batch(batch_size=4)),
    UnaryCase("batch_all", Batch),
    UnaryCase("add_tags", lambda: AddTags({"x": 1, "y": "z"})),
    UnaryCase("tag_by", lambda: TagBy(["d3", "d1"])),
]


def _unary_params() -> list[object]:
    return [
        pytest.param(case, role, id=f"{case.label}/{role}")
        for case in UNARY_CASES
        for role in case.roles
    ]


def _assert_all_equal(results: dict[str, dict[str, object]]) -> None:
    """Assert every arrangement's fingerprint equals the first one's, naming the field."""
    (first_id, first), *others = results.items()
    for arrangement_id, result in others:
        for field, value in first.items():
            assert result[field] == value, (
                f"{field} differs between {first_id} and {arrangement_id}"
            )


# ── arrangement: one input ──────────────────────────────────────────────────


@pytest.mark.parametrize(("case", "role"), _unary_params())
def test_unary_result_does_not_depend_on_the_input_arrangement(
    case: UnaryCase, role: str
) -> None:
    results = {
        arrangement.id: fingerprint(case.make_op()(make_source(MAIN, arrangement)))
        for arrangement in arrangements(MAIN, roles=(role,))
    }
    _assert_all_equal(results)


@pytest.mark.parametrize(("case", "role"), _unary_params())
def test_unary_async_rows_equal_sync_rows_in_every_arrangement(
    case: UnaryCase, role: str
) -> None:
    for arrangement in arrangements(MAIN, roles=(role,)):
        source = make_source(MAIN, arrangement)
        sync = rows_fingerprint(list(case.make_op()(source).iter_data()))
        assert rows_fingerprint(async_rows(case.make_op(), source)) == sync, (
            arrangement.id
        )


# ── order of a set argument ─────────────────────────────────────────────────


@dataclass(frozen=True)
class OrderCase:
    """An operator built from each permutation of its names or mapping entries."""

    label: str
    items: tuple[Any, ...]
    make_op: Callable[[Any], StaticOutputOperatorPod]
    roles: tuple[str, ...] = ROLES


ORDER_CASES = [
    OrderCase("select_tag_columns", ("t1", "t2", "t3"), SelectTagColumns),
    OrderCase("select_data_columns", ("d1", "d2", "d3"), SelectDataColumns),
    OrderCase("drop_tag_columns", ("t1", "t3"), DropTagColumns),
    OrderCase("drop_data_columns", ("d1", "d3"), DropDataColumns),
    # Tag and data names in one call.
    OrderCase("select_columns", ("t1", "d3", "t3", "d1"), SelectColumns),
    OrderCase("drop_columns", ("t1", "d3", "d1"), DropColumns),
    OrderCase("drop_nulls", ("d2", "t2", "d1"), DropNulls),
    OrderCase("map_tags", (("t1", "u1"), ("t2", "u2"), ("t3", "u3")), MapTags),
    # A swap: each target is another entry's source.
    OrderCase("map_tags_swap", (("t1", "t3"), ("t3", "t1")), MapTags, roles=(PLAIN,)),
    OrderCase("map_data", (("d1", "e1"), ("d2", "e2"), ("d3", "e3")), MapData),
    OrderCase("map_data_swap", (("d1", "d3"), ("d3", "d1")), MapData),
    OrderCase(
        "map_data_drop_unmapped",
        (("d1", "e1"), ("d3", "e3")),
        lambda items: MapData(items, drop_unmapped=True),
    ),
    OrderCase(
        "map_columns",
        (("t1", "u1"), ("d2", "e2"), ("t3", "u3")),
        MapColumns,
        roles=(PLAIN,),
    ),
    OrderCase(
        "polars_filter_constraints",
        (("t1", "a"), ("t2", 1), ("t3", 1)),
        lambda items: PolarsFilter(constraints=dict(items)),
    ),
]


def _make(case: OrderCase, items: tuple[Any, ...]) -> StaticOutputOperatorPod:
    """Build the operator from ``items``: a dict of pairs, or a list of names."""
    if isinstance(case.items[0], tuple):
        return case.make_op(dict(items))
    return case.make_op(list(items))


def _order_params() -> list[object]:
    return [
        pytest.param(case, role, id=f"{case.label}/{role}")
        for case in ORDER_CASES
        for role in case.roles
    ]


@pytest.mark.parametrize(("case", "role"), _order_params())
def test_the_order_of_a_set_argument_changes_nothing(
    case: OrderCase, role: str
) -> None:
    """Every permutation: same rows, hashes, column names AND column order."""
    arrangement = Arrangement(MAIN.tags, TAGS_FIRST, role)
    results: dict[str, dict[str, object]] = {}
    for items in itertools.permutations(case.items):
        op = _make(case, items)
        stream = op(make_source(MAIN, arrangement))
        results[repr(items)] = {
            **fingerprint(stream),
            "presentation": presentation(stream),
            "operator_identity": op.content_hash().to_string(),
            "async_rows": rows_fingerprint(
                async_rows(op, make_source(MAIN, arrangement))
            ),
        }
    _assert_all_equal(results)


def test_polars_filter_predicates_in_any_order_choose_the_same_rows() -> None:
    """Two predicates in either order keep the same rows (identity: see ITL-1236 doc)."""
    predicates = (pl.col("d1") > 10, pl.col("t3") == 1, pl.col("d3") != "y")
    rows = {
        repr(order): rows_fingerprint(
            list(PolarsFilter(predicates=list(order))(make_source(MAIN)).iter_data())
        )
        for order in itertools.permutations(predicates)
    }
    assert len({repr(v) for v in rows.values()}) == 1


@pytest.mark.parametrize("role", ROLES)
def test_add_tags_order_is_the_order_of_the_new_tags_and_its_identity(
    role: str,
) -> None:
    source = make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, role))
    xy = AddTags({"x": 1, "y": "z"})(source)
    yx = AddTags({"y": "z", "x": 1})(source)
    assert xy.keys()[0] == ("t1", "t2", "t3", "x", "y")
    assert yx.keys()[0] == ("t1", "t2", "t3", "y", "x")
    assert xy.content_hash() != yx.content_hash()
    assert xy.pipeline_hash() != yx.pipeline_hash()
    # Same rows: the values of each row do not depend on the order.
    assert rows_fingerprint(list(xy.iter_data())) == rows_fingerprint(
        list(yx.iter_data())
    )


# ── roles: names of several roles in one call ───────────────────────────────


@dataclass(frozen=True)
class RoleCase:
    """An operator naming columns of several roles, on ``MIXED``, and its result keys.

    In ``MIXED`` ``t2`` is a plain tag and ``t1``, ``t3`` are data-tags.
    """

    label: str
    make_op: Callable[[], StaticOutputOperatorPod]
    keys: tuple[tuple[str, ...], tuple[str, ...]]


ROLE_CASES = [
    # Dropping a data-tag's tag role keeps it as data; a plain tag is removed.
    RoleCase(
        "drop_tag_columns",
        lambda: DropTagColumns(["t3", "t2"]),
        (("t1",), ("t1", "t3", "d1", "d2", "d3")),
    ),
    # Dropping a data-tag's data role leaves a plain tag.
    RoleCase(
        "drop_data_columns",
        lambda: DropDataColumns(["t3", "d1"]),
        (("t1", "t2", "t3"), ("t1", "d2", "d3")),
    ),
    RoleCase(
        "select_tag_columns",
        lambda: SelectTagColumns(["t3", "t2"]),
        # t1 lost its tag role: plain data, after the data-tag t3 (ITL-1205).
        (("t2", "t3"), ("t3", "t1", "d1", "d2", "d3")),
    ),
    RoleCase(
        "select_data_columns",
        lambda: SelectDataColumns(["d2", "t3"]),
        (("t1", "t2", "t3"), ("t3", "d2")),
    ),
    # Both roles at once: a data-tag kept or removed whole, a plain tag, data.
    RoleCase(
        "select_columns",
        lambda: SelectColumns(["d2", "t3", "t2"]),
        (("t2", "t3"), ("t3", "d2")),
    ),
    RoleCase(
        "drop_columns",
        lambda: DropColumns(["t3", "t2", "d1"]),
        (("t1",), ("t1", "d2", "d3")),
    ),
    # A data-tag renamed in both roles; a plain tag renamed.
    RoleCase(
        "map_tags",
        lambda: MapTags({"t3": "u3", "t2": "u2"}),
        (("t1", "u2", "u3"), ("t1", "u3", "d1", "d2", "d3")),
    ),
    # A data-only move of a data-tag: the tag keeps its name, plain.
    RoleCase(
        "map_data",
        lambda: MapData({"t3": "e3", "d1": "e1"}),
        (("t1", "t2", "t3"), ("t1", "e3", "e1", "d2", "d3")),
    ),
    RoleCase(
        "drop_nulls",
        lambda: DropNulls(["t3", "t2", "d2"]),
        (("t1", "t2", "t3"), ("t1", "t3", "d1", "d2", "d3")),
    ),
    RoleCase(
        "polars_filter",
        lambda: PolarsFilter(constraints={"t3": 1, "t2": 2, "d3": "w"}),
        (("t1", "t2", "t3"), ("t1", "t3", "d1", "d2", "d3")),
    ),
]


@pytest.mark.parametrize("case", ROLE_CASES, ids=lambda case: case.label)
def test_names_of_several_roles_in_one_call(case: RoleCase) -> None:
    source = make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, MIXED))
    result = case.make_op()(source)
    assert result.keys() == case.keys
    assert rows_fingerprint(async_rows(case.make_op(), source)) == rows_fingerprint(
        list(result.iter_data())
    )


def _sorted_values(stream: StreamProtocol, name: str, role: str = "data") -> list[Any]:
    """The values of column ``name`` of every row, sorted."""
    index = 0 if role == "tag" else 1
    values: list[Any] = [row[index][name] for row in stream.iter_data()]
    return sorted(values)


def test_drop_nulls_of_several_roles_drops_a_row_with_a_null_in_any() -> None:
    source = make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, MIXED))
    kept = DropNulls(["t3", "t2", "d2"])(source)
    assert _sorted_values(kept, "d1") == [10, 30, 40, 50]


def test_polars_filter_constraints_of_several_roles_are_all_applied() -> None:
    source = make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, MIXED))
    kept = PolarsFilter(constraints={"t1": "a", "t2": 1, "t3": 1})(source)
    assert [data["d1"] for _, data in kept.iter_data()] == [50]


@pytest.mark.xfail(
    strict=True,
    reason="ITL-1242: MapTags refuses a swap of data-tags that it accepts for plain tags",
    raises=InputValidationError,
)
@pytest.mark.parametrize("role", [DATA_TAGS, MIXED])
def test_map_tags_swaps_data_tags_as_it_swaps_plain_tags(role: str) -> None:
    source = make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, role))
    swapped = MapTags({"t1": "t3", "t3": "t1"})(source)
    plain = MapTags({"t1": "t3", "t3": "t1"})(
        make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, PLAIN))
    )
    assert swapped.keys()[0] == plain.keys()[0]
    assert _sorted_values(swapped, "t1", "tag") == _sorted_values(plain, "t1", "tag")


# ── several inputs: Join, MergeJoin, SemiJoin ───────────────────────────────


JOIN_INPUTS: dict[str, tuple[Spec, ...]] = {
    "2": (MAIN, P),
    "3": (MAIN, P, Q),
    "4": (MAIN, P, Q, R),
}


def _join_params() -> list[object]:
    return [
        pytest.param(n, role, id=f"join_{n}/{role}")
        for n in JOIN_INPUTS
        for role in ROLES
    ]


def _sources(
    specs: tuple[Spec, ...],
    order_index: int,
    layout: str,
    role: str,
    rows: list[int] | None = None,
) -> list[ArrowTableSource]:
    """Each spec in its ``order_index``-th tag order, in ``layout`` and ``role``."""
    return [
        make_source(
            spec,
            Arrangement(
                spec.tag_orders()[order_index % len(spec.tag_orders())], layout, role
            ),
            rows=rows,
        )
        for spec in specs
    ]


@pytest.mark.parametrize(("n", "role"), _join_params())
def test_join_result_does_not_depend_on_the_order_of_its_inputs(
    n: str, role: str
) -> None:
    """Every permutation of the inputs: same rows, hashes, system-tag names, order."""
    specs = JOIN_INPUTS[n]
    results: dict[str, dict[str, object]] = {}
    for permutation in itertools.permutations(range(len(specs))):
        sources = _sources(specs, 0, TAGS_FIRST, role)
        ordered = [sources[i] for i in permutation]
        stream = Join()(*ordered)
        results[repr(permutation)] = {
            **fingerprint(stream),
            "presentation": presentation(stream),
            "system_tag_order": stream.keys(columns={"system_tags": True})[0],
            "async_rows": rows_fingerprint(async_rows(Join(), *ordered)),
        }
    _assert_all_equal(results)


@pytest.mark.parametrize(("n", "role"), _join_params())
def test_join_result_does_not_depend_on_the_arrangement_of_its_inputs(
    n: str, role: str
) -> None:
    specs = JOIN_INPUTS[n]
    results: dict[str, dict[str, object]] = {}
    for order_index in range(3):
        for layout in ("tags_first", "data_first_reversed", "interleaved"):
            sources = _sources(specs, order_index, layout, role)
            stream = Join()(*sources)
            results[f"{order_index}/{layout}"] = {
                **fingerprint(stream),
                "async_rows": rows_fingerprint(async_rows(Join(), *sources)),
            }
    _assert_all_equal(results)


def test_join_of_four_inputs_matches_on_every_shared_tag() -> None:
    sources = _sources(JOIN_INPUTS["4"], 0, TAGS_FIRST, PLAIN)
    joined = Join()(*sources)
    rows = sorted(
        (
            tag["t1"],
            tag["t2"],
            tag["t3"],
            data["d1"],
            data["p1"],
            data["q2"],
            data["r1"],
        )
        for tag, data in joined.iter_data()
    )
    # MAIN x P on (t1, t2), x Q on (t2, t3), x R on t1: every MAIN row has one match.
    assert rows == [
        ("a", 1, 1, 50, 9, 400, -2),
        ("a", 1, 2, 20, 9, 100, -2),
        ("a", 2, 1, 30, 7, 200, -2),
        ("b", 1, 2, 40, 8, 100, -1),
        ("b", 2, 1, 10, 6, 200, -1),
        ("b", 2, 2, 60, 6, 300, -1),
    ]


# MergeJoin: two inputs with the same columns; every data column collides. The rows
# of ``MAIN`` without a null (``NON_NULL_ROWS``), and other values in every data column,
# so no merged pair ties: a tie is ITL-1241, a null ``MERGE_NULL_ISSUE``.
NON_NULL_ROWS = [0, 2, 3, 4]
MAIN_OTHER = Spec(
    source_id="s2",
    tags=MAIN.tags,
    data=MAIN.data,
    columns={
        **{tag: MAIN.columns[tag] for tag in MAIN.tags},
        "d1": pa.array([60, 55, 45, 35, 25, 15], pa.int64()),
        "d2": pa.array([9.5, None, 7.5, 6.5, 5.5, 1.5], pa.float64()),
        "d3": pa.array(["a", "b", "c", "d", "e", "f"], pa.large_string()),
    },
    mixed_plain=MAIN.mixed_plain,
)


@pytest.mark.parametrize("role", ROLES)
def test_merge_join_result_does_not_depend_on_the_order_of_its_inputs(
    role: str,
) -> None:
    """Three colliding data columns: either input order, any tag order.

    Swapping the inputs changes nothing, not even the column order. Another tag order
    of the inputs changes only the column order.
    """
    results: dict[str, dict[str, object]] = {}
    for order_index in range(3):
        left, right = _sources(
            (MAIN, MAIN_OTHER), order_index, TAGS_FIRST, role, rows=NON_NULL_ROWS
        )
        ab, ba = MergeJoin()(left, right), MergeJoin()(right, left)
        assert presentation(ab) == presentation(ba)
        for label, inputs, stream in (
            ("ab", (left, right), ab),
            ("ba", (right, left), ba),
        ):
            results[f"{order_index}/{label}"] = {
                **fingerprint(stream),
                "async_rows": rows_fingerprint(async_rows(MergeJoin(), *inputs)),
            }
    _assert_all_equal(results)


@pytest.mark.xfail(
    strict=True,
    reason="ITL-1241: MergeJoin orders tied values' source info by input position",
)
@pytest.mark.parametrize("role", ROLES)
def test_merge_join_of_tied_values_does_not_depend_on_the_order_of_its_inputs(
    role: str,
) -> None:
    """Two inputs with equal values in three colliding columns (ITL-1241)."""
    arrangement = Arrangement(MAIN.tags, TAGS_FIRST, role)
    left = make_source(MAIN, arrangement, source_id="s", rows=NON_NULL_ROWS)
    right = make_source(MAIN, arrangement, source_id="s2", rows=NON_NULL_ROWS)
    assert fingerprint(MergeJoin()(left, right)) == fingerprint(
        MergeJoin()(right, left)
    )


MERGE_NULL_ISSUE = "ITL-1236 defect 3, issue pending"


@pytest.mark.xfail(
    strict=True,
    reason=f"{MERGE_NULL_ISSUE}: MergeJoin raises TypeError on a null in a merged column",
    raises=TypeError,
)
@pytest.mark.parametrize("role", ROLES)
def test_merge_join_of_a_nullable_column_holding_nulls(role: str) -> None:
    """``d2`` holds a null in rows 1 and 5 of ``MAIN``; the merge must not crash."""
    arrangement = Arrangement(MAIN.tags, TAGS_FIRST, role)
    left = make_source(MAIN, arrangement)
    right = make_source(MAIN_OTHER, arrangement)
    assert fingerprint(MergeJoin()(left, right)) == fingerprint(
        MergeJoin()(right, left)
    )


@pytest.mark.parametrize("role", ROLES)
def test_semijoin_matches_on_every_shared_tag_in_any_arrangement(role: str) -> None:
    """The right input's tag order and layout never change which rows are kept."""
    results: dict[str, dict[str, object]] = {}
    for order_index in range(3):
        for layout in ("tags_first", "data_first_reversed", "interleaved"):
            left, right = _sources((MAIN, P), order_index, layout, role)
            stream = SemiJoin()(left, right)
            results[f"{order_index}/{layout}"] = {
                **fingerprint(stream),
                "async_rows": rows_fingerprint(async_rows(SemiJoin(), left, right)),
            }
    _assert_all_equal(results)
    left, right = _sources((MAIN, P), 0, TAGS_FIRST, role)
    kept = SemiJoin()(
        left, make_source(P, Arrangement(P.tags, TAGS_FIRST, role), rows=[0, 1])
    )
    # P's first two rows are (a, 2) and (b, 1): matched on t1 AND t2.
    assert _sorted_values(kept, "d1") == [30, 40]
