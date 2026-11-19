"""No operator result and no stream depends on the order of what is fed into it (ITL-1271).

Developer, November 18, 2026: "every operator outcomes and all streams are not sensitive
to tag / column orders fed into them UNLESS it is somehow explicitly expected". The places
where an order is meant to count are ``order_rules.ORDER_EXCEPTIONS``; nothing else may
depend on one.

Every operator exported by ``orcapod.operators``, every source and stream kind exported by
``orcapod.sources`` and ``orcapod.streams``, the function pod forms and ``SideEffectPod``
have a case here: ``test_every_exported_operator_has_a_case`` and
``test_every_exported_source_and_stream_kind_has_a_case`` fail for a new one without.

Each case is built over the inputs of ``several_items`` (three tags, three data columns) in
each role of the tags (plain, data-tags, mixed) and compared across:

* three arrangements of its input: the declared tag order with the tags first, the
  reversed tag order with the data first and reversed, a rotation of the tags interleaved
  with the data;
* the orders of the items of each argument that names several (identity, reversed and
  every other permutation of up to three items);
* the orders of the inputs of an operator that declares itself commutative.

What must be equal (``identity``): the content and pipeline hash, the operator's own
identity, the system-tag column names, the output schema as sets, and the rows as a
multiset (values, system tags and so record ids, tag and data hashes, source info), on the
sync and the async path; in a job also the nodes' hashes, the stored table paths and
stored rows, a ``DerivedSource`` over a node, and a saved and loaded job. What may follow
the input (presentation, ``column_placement``): the column order. Which rows ``head(n)``
shows is presentation that must not depend on tag order either (ITL-1270).

A case owned by an open issue is ``xfail(strict=True)`` naming it
(``order_rules.KNOWN_VIOLATIONS``): when the issue lands, remove the mark.
"""

from __future__ import annotations

import asyncio
import inspect
import itertools
import json
import re
import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.csv as pa_csv
import pytest

import orcapod
from orcapod import PipelineJob, function_pod, operators, sources, streams
from orcapod.channels import Channel
from orcapod.core.nodes import SourceJobNode
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.operators import (
    AddTags,
    Batch,
    DropColumns,
    DropDataColumns,
    DropNulls,
    DropTagColumns,
    GroupBy,
    Index,
    Join,
    MapColumns,
    MapData,
    MapTags,
    MergeJoin,
    Pick,
    PolarsFilter,
    SelectColumns,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
    TagBy,
)
from orcapod.side_effects import SideEffectPod
from orcapod.sources import (
    ArrowTableSource,
    CachedSource,
    CSVSource,
    DataFrameSource,
    DeltaTableSource,
    DerivedSource,
    DictSource,
    ListSource,
    PollingSource,
    SQLiteTableSource,
)
from orcapod.types import CacheMode, Cursor

from .operators.several_items import (
    DATA_FIRST_REVERSED,
    DATA_TAGS,
    INTERLEAVED,
    MAIN,
    MIXED,
    PLAIN,
    ROLES,
    TAGS_FIRST,
    Arrangement,
    P,
    Q,
    Spec,
    _layout,
    make_source,
)
from .order_rules import KNOWN_VIOLATIONS, ORDER_EXCEPTIONS

pytestmark = [
    pytest.mark.filterwarnings(
        "ignore::orcapod.core.data_tags.DataTagMaterializationWarning"
    ),
    pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning"),
]

# (tag order index, layout): the three tag orders (declared, reversed, a rotation) with the
# tags first, and the declared order in the two other layouts. One axis varies at a time,
# so a dependence on both cannot cancel out.
ARRANGEMENTS = (
    (0, TAGS_FIRST),
    (1, TAGS_FIRST),
    (2, TAGS_FIRST),
    (0, DATA_FIRST_REVERSED),
    (0, INTERLEAVED),
)
LAYOUTS = (TAGS_FIRST, DATA_FIRST_REVERSED, INTERLEAVED)


def xfail(issue: str) -> pytest.MarkDecorator:
    """The strict expected-failure mark of a case owned by ``issue``.

    Only a failed comparison is the expected failure (``raises=AssertionError``): an
    error while building, running or loading fails the test as usual.
    """
    return pytest.mark.xfail(
        strict=True,
        raises=AssertionError,
        reason=f"{issue}: {KNOWN_VIOLATIONS[issue]}",
    )


# ── inputs ──────────────────────────────────────────────────────────────────

# MergeJoin merges every data column: the rows of MAIN without a null and other values,
# so no merged pair ties (a tie is ITL-1241, a null ITL-1243).
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
# ``dict[str, int]`` (as the type converter stores it) and ``list[int]`` columns for
# Pick and Index.
_KV = pa.large_list(pa.struct([("key", pa.large_string()), ("value", pa.int64())]))
NESTED = Spec(
    source_id="n",
    tags=MAIN.tags,
    data=("d1", "d4", "d5"),
    columns={
        **{tag: MAIN.columns[tag] for tag in MAIN.tags},
        "d1": MAIN.columns["d1"],
        "d4": pa.array(
            [
                [{"key": "speed", "value": i}, {"key": "size", "value": 9 - i}]
                for i in range(6)
            ],
            _KV,
        ),
        "d5": pa.array(
            [[i, i + 1, i + 2] for i in range(6)], pa.large_list(pa.int64())
        ),
    },
    mixed_plain=MAIN.mixed_plain,
)


def source(
    spec: Spec, role: str, order: int, layout: str, rows: list[int] | None = None
) -> ArrowTableSource:
    """``spec`` with its ``order``-th tag order, in ``layout`` and ``role``."""
    orders = spec.tag_orders()
    return make_source(
        spec, Arrangement(orders[order % len(orders)], layout, role), rows=rows
    )


def table(spec: Spec, order: int, layout: str) -> pa.Table:
    """The table of ``spec`` laid out as ``make_source`` lays it out."""
    names = _layout(spec, spec.tag_orders()[order], layout)
    columns = {name: spec.columns[name] for name in names}
    schema = pa.schema(
        pa.field(name, column.type, nullable=column.null_count > 0)
        for name, column in columns.items()
    )
    return pa.table(list(columns.values()), schema=schema)


def tag_only(spec: Spec, role: str) -> bool | list[str]:
    """``tag_only_columns`` of ``role``."""
    return {PLAIN: True, DATA_TAGS: [], MIXED: [spec.mixed_plain]}[role]


# ── what is compared ────────────────────────────────────────────────────────

# A function result's source-info token, and its stored ``_source_<output>`` column, hold a
# random record id (uuid7), new on each computation: not an order effect.
_RANDOM_ID = re.compile(r"::[0-9a-f]{32}::")


def _masked(value: Any) -> str:
    return _RANDOM_ID.sub("::<uuid7>::", repr(value))


def row_identity(rows: Sequence[tuple[Any, Any]]) -> list[tuple[str, ...]]:
    """Each row's values, system tags, tag and data hashes and source info; sorted."""
    return sorted(
        (
            repr(sorted(tag.as_dict().items())),
            repr(sorted(data.as_dict().items())),
            repr(sorted(tag.system_tags().items())),
            tag.content_hash().to_string(),
            data.content_hash().to_string(),
            _masked(sorted(data.source_info().items())),
        )
        for tag, data in rows
    )


def identity(stream: Any) -> dict[str, Any]:
    """What must not depend on any order: hashes, names, schema as sets, rows."""
    tag_keys, _ = stream.keys()
    all_tags, _ = stream.keys(columns={"system_tags": True})
    tag_schema, data_schema = stream.output_schema()
    return {
        "content_hash": stream.content_hash().to_string(),
        "pipeline_hash": stream.pipeline_hash().to_string(),
        "system_tag_names": sorted(k for k in all_tags if k not in tag_keys),
        "output_schema": (
            sorted((k, repr(t)) for k, t in tag_schema.items()),
            sorted((k, repr(t)) for k, t in data_schema.items()),
        ),
        "rows": row_identity(list(stream.iter_data())),
    }


async def _run_async(producer: Any, upstreams: Sequence[Any]) -> list[Any]:
    channels = [Channel(buffer_size=64) for _ in upstreams]
    output = Channel(buffer_size=64)
    for upstream, channel in zip(upstreams, channels):
        for row in upstream.iter_data():
            await channel.writer.send(row)
        await channel.writer.close()
    readers = [channel.reader for channel in channels]
    if "input_pipeline_hashes" in inspect.signature(producer.async_execute).parameters:
        await producer.async_execute(
            readers,
            output.writer,
            input_pipeline_hashes=[s.pipeline_hash() for s in upstreams],
        )
    else:
        await producer.async_execute(readers, output.writer)
    return await output.reader.collect()


def with_async(stream: Any) -> dict[str, Any]:
    """``identity`` plus the async rows, which must equal the sync rows."""
    result = identity(stream)
    rows = asyncio.run(_run_async(stream.producer, list(stream.upstreams)))
    assert row_identity(rows) == result["rows"], "async rows differ from sync rows"
    return result


def assert_all_equal(results: dict[str, dict[str, Any]]) -> None:
    """Every variant's result equals the first one's, naming the element that differs."""
    (first_id, first), *others = results.items()
    for variant_id, result in others:
        # An element only one variant has (an extra stored table) is a difference too.
        assert result.keys() == first.keys(), (
            f"elements differ: {first_id} vs {variant_id}: "
            f"{sorted(result.keys() ^ first.keys())}"
        )
        for name, value in first.items():
            assert result[name] == value, f"{name} differs: {first_id} vs {variant_id}"


# ── operators ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class OperatorCase:
    """An exported operator over ``inputs``, built from its argument's items.

    Attributes:
        label: The case's id.
        make: Builds the operator from the argument's items (``None`` for no argument).
        items: The items of the argument that names several, permuted by the test.
        inputs: The input specs.
        rows: The input rows to keep (``None``: every row).
        roles: The tag roles the operator accepts.
        arrangement_issue: The open issue whose cases fail across arrangements.
        argument_issue: The open issue whose cases fail across argument orders.
    """

    label: str
    make: Callable[[Any], StaticOutputOperatorPod]
    items: tuple[Any, ...] | None = None
    inputs: tuple[Spec, ...] = (MAIN,)
    rows: list[int] | None = None
    roles: tuple[str, ...] = ROLES
    arrangement_issue: str | None = None
    argument_issue: str | None = None

    def build(self, items: Sequence[Any] | None = None) -> StaticOutputOperatorPod:
        return self.make(self.items if items is None else items)

    def sources(self, role: str, order: int, layout: str) -> list[Any]:
        return [source(spec, role, order, layout, self.rows) for spec in self.inputs]


def _names(cls: Callable[..., Any]) -> Callable[[Any], Any]:
    return lambda items: cls(list(items))


def _mapping(cls: Callable[..., Any]) -> Callable[[Any], Any]:
    return lambda items: cls(dict(items))


def _plain(make: Callable[[], Any]) -> Callable[[Any], Any]:
    return lambda _items: make()


# Every name of ``orcapod.operators`` that is an operator: one or more cases each.
OPERATOR_CASES: dict[str, tuple[OperatorCase, ...]] = {
    "Join": (OperatorCase("join", _plain(Join), inputs=(MAIN, P, Q)),),
    "MergeJoin": (
        OperatorCase(
            "merge_join",
            _plain(MergeJoin),
            inputs=(MAIN, MAIN_OTHER),
            rows=NON_NULL_ROWS,
        ),
    ),
    "SemiJoin": (OperatorCase("semi_join", _plain(SemiJoin), inputs=(MAIN, P)),),
    "MapTags": (
        OperatorCase(
            "map_tags", _mapping(MapTags), (("t1", "u1"), ("t2", "u2"), ("t3", "u3"))
        ),
    ),
    "MapData": (
        OperatorCase(
            "map_data", _mapping(MapData), (("d1", "e1"), ("d2", "e2"), ("d3", "e3"))
        ),
    ),
    # MapColumns is refused with data-tags (ITL-1022).
    "MapColumns": (
        OperatorCase(
            "map_columns",
            _mapping(MapColumns),
            (("t1", "u1"), ("d2", "e2"), ("t3", "u3")),
            roles=(PLAIN,),
        ),
    ),
    "Batch": (OperatorCase("batch", _plain(lambda: Batch(batch_size=4))),),
    "GroupBy": (
        OperatorCase("group_by_t1_t2", _names(GroupBy), ("t1", "t2")),
        # Two tags outside the key order the members, in name order (ITL-1226).
        OperatorCase("group_by_t1", _plain(lambda: GroupBy("t1"))),
    ),
    "SelectTagColumns": (
        OperatorCase("select_tag_columns", _names(SelectTagColumns), ("t1", "t3")),
    ),
    "SelectDataColumns": (
        OperatorCase("select_data_columns", _names(SelectDataColumns), ("d1", "d3")),
    ),
    "DropTagColumns": (
        OperatorCase("drop_tag_columns", _names(DropTagColumns), ("t1", "t3")),
    ),
    "DropDataColumns": (
        OperatorCase("drop_data_columns", _names(DropDataColumns), ("d1", "d3")),
    ),
    "SelectColumns": (
        OperatorCase("select_columns", _names(SelectColumns), ("t1", "d3", "t3", "d1")),
    ),
    "DropColumns": (OperatorCase("drop_columns", _names(DropColumns), ("t1", "d3")),),
    "PolarsFilter": (
        OperatorCase(
            "polars_filter_constraints",
            lambda items: PolarsFilter(constraints=dict(items)),
            (("t1", "a"), ("t2", 1)),
        ),
        OperatorCase(
            "polars_filter_predicates",
            lambda items: PolarsFilter(predicates=list(items)),
            (pl.col("d1") > 10, pl.col("t3") == 1),
            argument_issue="ITL-1247",
        ),
    ),
    "DropNulls": (
        OperatorCase("drop_nulls_named", _names(DropNulls), ("d2", "t2", "d1")),
        OperatorCase("drop_nulls_all", _plain(DropNulls)),
    ),
    "AddTags": (
        OperatorCase(
            "add_tags",
            _mapping(AddTags),
            (("x", 1), ("y", "z")),
        ),
    ),
    "Pick": (
        OperatorCase("pick", _plain(lambda: Pick("d4", "speed")), inputs=(NESTED,)),
    ),
    "Index": (OperatorCase("index", _plain(lambda: Index("d5", 1)), inputs=(NESTED,)),),
    "TagBy": (OperatorCase("tag_by", _names(TagBy), ("d3", "d1")),),
}


def _operator_names() -> set[str]:
    return {
        name
        for name in operators.__all__
        if inspect.isclass(obj := getattr(operators, name))
        and issubclass(obj, StaticOutputOperatorPod)
    }


def test_every_exported_operator_has_a_case() -> None:
    """A new operator fails here until it has a case in ``OPERATOR_CASES``."""
    assert _operator_names() == set(OPERATOR_CASES)


def _operator_params(issue_of: Callable[[OperatorCase], str | None]) -> list[Any]:
    return [
        pytest.param(
            case,
            role,
            id=f"{case.label}/{role}",
            marks=[xfail(issue)] if (issue := issue_of(case)) else [],
        )
        for cases in OPERATOR_CASES.values()
        for case in cases
        for role in case.roles
    ]


@pytest.mark.parametrize(
    ("case", "role"), _operator_params(lambda c: c.arrangement_issue)
)
def test_an_operator_does_not_depend_on_the_arrangement_of_its_inputs(
    case: OperatorCase, role: str
) -> None:
    """Tag order and table layout change no hash, name, schema or row; sync and async."""
    assert_all_equal(
        {
            f"{order}/{layout}": with_async(
                case.build()(*case.sources(role, order, layout))
            )
            for order, layout in ARRANGEMENTS
        }
    )


def _permutations(items: tuple[Any, ...]) -> list[tuple[Any, ...]]:
    """Every permutation of up to three items; else identity, reversed and a rotation."""
    if len(items) <= 3:
        return list(itertools.permutations(items))
    return [items, tuple(reversed(items)), items[1:] + items[:1]]


@pytest.mark.parametrize(
    ("case", "role"),
    [
        param
        for param in _operator_params(lambda c: c.argument_issue)
        if param.values[0].items is not None
    ],
)
def test_the_order_of_an_argument_that_names_several_changes_nothing(
    case: OperatorCase, role: str
) -> None:
    """Names, mapping entries, keywords, predicates: their order changes nothing.

    Not even the result's column order, nor the operator's own identity.
    """
    assert case.items is not None
    results = {}
    for items in _permutations(case.items):
        op = case.build(items)
        stream = op(*case.sources(role, 0, TAGS_FIRST))
        results[repr(items)] = {
            **with_async(stream),
            "operator_identity": op.content_hash().to_string(),
            "keys": stream.keys(),
        }
    assert_all_equal(results)


def _commutative_cases() -> list[Any]:
    return [
        pytest.param(case, role, id=f"{case.label}/{role}")
        for cases in OPERATOR_CASES.values()
        for case in cases
        for role in case.roles
        if len(case.inputs) > 1
        and isinstance(case.build().argument_symmetry([]), frozenset)
    ]


@pytest.mark.parametrize(("case", "role"), _commutative_cases())
def test_the_order_of_the_inputs_of_a_commutative_operator_changes_nothing(
    case: OperatorCase, role: str
) -> None:
    inputs = case.sources(role, 0, TAGS_FIRST)
    assert_all_equal(
        {
            repr([s.source_id for s in order]): with_async(case.build()(*order))
            for order in itertools.permutations(inputs)
        }
    )


def test_the_commutative_cases_are_the_joins() -> None:
    """``Join`` and ``MergeJoin`` declare themselves commutative; ``SemiJoin`` does not."""
    assert {p.values[0].label for p in _commutative_cases()} == {"join", "merge_join"}
    two = [source(MAIN, PLAIN, 0, TAGS_FIRST), source(P, PLAIN, 0, TAGS_FIRST)]
    assert isinstance(SemiJoin().argument_symmetry(two), tuple)


# ── sources, streams and pods ───────────────────────────────────────────────


@function_pod(output_keys="out")
def combine(d1: int, d3: str) -> str:
    """Join a number and a string; reads two data columns."""
    return f"{d3}{d1}"


@function_pod(output_keys="joined")
def across(d1: int, p1: int) -> int:
    """Combine a column of ``MAIN`` with one of ``P``: a pod over two inputs."""
    return d1 * 100 + p1


def notify(ctx: Any, d1: int, d3: str) -> None:
    """A side effect that does nothing outside Orcapod."""


class _OneBatch:
    """A polling implementation that serves one table, once."""

    def __init__(self, batch: pa.Table) -> None:
        self._batch = batch

    def identity(self) -> Any:
        return ("order_invariance_polling", 1)

    def to_config(self) -> dict[str, Any] | None:
        return None

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> _OneBatch:
        raise NotImplementedError

    def schema(self) -> Any:
        return None

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        return cursor is None or cursor.value == 0

    async def fetch(self, cursor: Cursor[int] | None = None) -> tuple[Cursor[int], Any]:
        if cursor is not None and cursor.value > 0:
            return Cursor(value=1), {}
        return Cursor(value=1), self._batch

    async def close(self) -> None:
        pass


@dataclass(frozen=True)
class StreamCase:
    """A source, stream or pod form built in one arrangement.

    Attributes:
        label: The case's id.
        build: ``(role, tag order index, layout, tmp_path) -> stream``.
        roles: The tag roles it is built in.
        issue: The open issue whose cases fail across arrangements, per role.
        with_async: Whether its producer has an async path to compare.
    """

    label: str
    build: Callable[[str, int, str, Path], Any]
    roles: tuple[str, ...] = ROLES
    issue: dict[str, str] = field(default_factory=dict)
    with_async: bool = False


def _arrow_source(role: str, order: int, layout: str, _tmp: Path, **kw: Any) -> Any:
    return ArrowTableSource(
        table(MAIN, order, layout),
        tag_columns=list(MAIN.tag_orders()[order]),
        tag_only_columns=tag_only(MAIN, role),
        **kw,
    )


def _dict_source(role: str, order: int, layout: str, _tmp: Path) -> Any:
    return DictSource(
        table(MAIN, order, layout).to_pylist(),
        tag_columns=list(MAIN.tag_orders()[order]),
        tag_only_columns=tag_only(MAIN, role),
    )


def _data_frame_source(role: str, order: int, layout: str, _tmp: Path) -> Any:
    return DataFrameSource(
        pl.from_arrow(table(MAIN, order, layout)),
        tag_columns=list(MAIN.tag_orders()[order]),
        tag_only_columns=tag_only(MAIN, role),
    )


def _list_source(_role: str, order: int, _layout: str, _tmp: Path) -> Any:
    rows = table(MAIN, 0, TAGS_FIRST).to_pylist()
    tags = MAIN.tag_orders()[order]
    return ListSource(
        "d1",
        [row["d1"] for row in rows],
        tag_function=lambda _element, i: {t: rows[i][t] for t in tags},
        expected_tag_keys=list(tags),
    )


def _csv_source(role: str, order: int, layout: str, tmp: Path) -> Any:
    path = tmp / "rows.csv"  # one path: the location-based default id is the same
    pa_csv.write_csv(table(MAIN, order, layout), path)
    return CSVSource(
        str(path),
        tag_columns=list(MAIN.tag_orders()[order]),
        tag_only_columns=tag_only(MAIN, role),
    )


def _delta_source(role: str, order: int, layout: str, tmp: Path) -> Any:
    from deltalake import write_deltalake

    path = tmp / f"delta_{order}_{layout}"
    write_deltalake(str(path), table(MAIN, order, layout))
    return DeltaTableSource(
        path,
        tag_columns=list(MAIN.tag_orders()[order]),
        tag_only_columns=tag_only(MAIN, role),
        source_id="delta",
    )


_SQL_TYPES = {
    "t1": "TEXT",
    "t2": "INTEGER",
    "t3": "INTEGER",
    "d1": "INTEGER",
    "d2": "REAL",
    "d3": "TEXT",
}


def _sqlite_source(role: str, order: int, layout: str, tmp: Path) -> Any:
    # The primary key (the record-id key order, an exception) is declared in one order.
    path = tmp / f"rows_{order}_{layout}.sqlite"
    rows = table(MAIN, order, layout)
    with sqlite3.connect(path) as conn:
        columns = ", ".join(f"{n} {_SQL_TYPES[n]}" for n in rows.column_names)
        conn.execute(f"CREATE TABLE m ({columns}, PRIMARY KEY (t1, t2, t3))")
        conn.executemany(
            f"INSERT INTO m VALUES ({', '.join('?' for _ in rows.column_names)})",
            [tuple(r.values()) for r in rows.to_pylist()],
        )
    conn.close()
    return SQLiteTableSource(
        path,
        "m",
        tag_columns=list(MAIN.tag_orders()[order]),
        tag_only_columns=tag_only(MAIN, role),
        source_id="sqlite",
    )


def _polling_source(_role: str, order: int, layout: str, _tmp: Path) -> Any:
    return PollingSource(
        _OneBatch(table(MAIN, order, layout)),
        tag_columns=list(MAIN.tag_orders()[order]),
        record_id_columns=list(MAIN.tags),
    )


def _cached_source(role: str, order: int, layout: str, _tmp: Path) -> Any:
    return CachedSource(
        source(MAIN, role, order, layout), cache_database=InMemoryArrowDatabase()
    )


def _derived_source(role: str, order: int, layout: str, _tmp: Path) -> Any:
    return DerivedSource(
        source(MAIN, role, order, layout), tags_as_data={"t1": "x1", "t3": "x3"}
    )


def _raw_stream(role: str, order: int, layout: str, _tmp: Path) -> Any:
    tags = list(MAIN.tag_orders()[order])
    data_tags = {
        PLAIN: [],
        DATA_TAGS: tags,
        MIXED: [t for t in tags if t != MAIN.mixed_plain],
    }[role]
    return ArrowTableStream(
        table(MAIN, order, layout), tag_columns=tags, data_tags=data_tags
    )


def _pod_call(role: str, order: int, layout: str, _tmp: Path) -> Any:
    return combine.pod(source(MAIN, role, order, layout))


def _pod_process(role: str, order: int, layout: str, _tmp: Path) -> Any:
    selected = source(MAIN, role, order, layout).select_data_columns(["d1", "d3"])
    return combine.pod.process(selected)


def _pod_two_inputs(role: str, order: int, layout: str, _tmp: Path) -> Any:
    return across.pod(source(MAIN, role, order, layout), source(P, role, order, layout))


def _side_effect(role: str, order: int, layout: str, _tmp: Path) -> Any:
    selected = source(MAIN, role, order, layout).select_data_columns(["d1", "d3"])
    return SideEffectPod(notify)(selected)


_DATA_TAG_ROLES_1246 = {DATA_TAGS: "ITL-1246", MIXED: "ITL-1246"}
_EVERY_ROLE_1227 = {role: "ITL-1227" for role in ROLES}

STREAM_CASES: tuple[StreamCase, ...] = (
    StreamCase(
        "arrow_table_source_given_id", lambda *a: _arrow_source(*a, source_id="s")
    ),
    StreamCase("arrow_table_source_default_id", _arrow_source),
    StreamCase("dict_source", _dict_source),
    StreamCase("data_frame_source", _data_frame_source),
    StreamCase("list_source", _list_source, roles=(PLAIN,)),
    StreamCase("csv_source", _csv_source),
    StreamCase("delta_table_source", _delta_source),
    StreamCase("sqlite_table_source", _sqlite_source),
    # PollingSource takes no tag_only_columns: its tags are plain.
    StreamCase("polling_source", _polling_source, roles=(PLAIN,)),
    StreamCase("cached_source", _cached_source),
    StreamCase("derived_source_tags_as_data", _derived_source),
    StreamCase("raw_arrow_table_stream", _raw_stream, issue=_EVERY_ROLE_1227),
    StreamCase(
        "function_pod_call", _pod_call, issue=_DATA_TAG_ROLES_1246, with_async=True
    ),
    StreamCase("function_pod_process", _pod_process, with_async=True),
    StreamCase(
        "function_pod_two_inputs",
        _pod_two_inputs,
        issue=_DATA_TAG_ROLES_1246,
        with_async=True,
    ),
    # Its async path delivers: tested in a job (test_a_job_...).
    StreamCase("side_effect_pod", _side_effect),
)

# The kinds of ``orcapod.sources`` and ``orcapod.streams`` and the case that covers each,
# or why none applies. A new kind fails ``test_every_exported_source_and_stream_kind_has_a_case``.
KIND_CASES: dict[str, str] = {
    "ArrowTableSource": "arrow_table_source_given_id",
    "DictSource": "dict_source",
    "DataFrameSource": "data_frame_source",
    "ListSource": "list_source",
    "CSVSource": "csv_source",
    "DeltaTableSource": "delta_table_source",
    "SQLiteTableSource": "sqlite_table_source",
    "PollingSource": "polling_source",
    "CachedSource": "cached_source",
    "DerivedSource": "derived_source_tags_as_data",  # plain: test_a_job_...
    "ArrowTableStream": "raw_arrow_table_stream",
}
NOT_APPLICABLE: dict[str, str] = {
    "RootSource": "the abstract base of every source",
    "StreamBase": "the abstract base of every stream",
    "StreamView": "a view of rows a stream already has; it orders nothing",
    "SourceProxy": "stands in for a source a loaded job cannot rebuild; it reads nothing",
    "DBTableSource": "the base of the database sources; SQLiteTableSource is its case",
    "PostgreSQLTableSource": "needs a PostgreSQL server; a DBTableSource like SQLite",
    "SpiralDBTableSource": "needs a SpiralDB project; reads through the same builder",
}


def _kind_names() -> set[str]:
    from orcapod.core.sources.base import RootSource
    from orcapod.core.streams.base import StreamBase
    from orcapod.core.streams.view import StreamView

    names = set()
    for module in (sources, streams):
        for name in module.__all__:
            obj = getattr(module, name)
            if inspect.isclass(obj) and issubclass(
                obj, (RootSource, StreamBase, StreamView)
            ):
                names.add(name)
    return names


def test_every_exported_source_and_stream_kind_has_a_case() -> None:
    """A new source or stream kind fails here until it has a case (or a reason)."""
    assert _kind_names() == set(KIND_CASES) | set(NOT_APPLICABLE)
    assert not set(KIND_CASES) & set(NOT_APPLICABLE)
    labels = {case.label for case in STREAM_CASES}
    assert set(KIND_CASES.values()) <= labels


def _stream_params() -> list[Any]:
    return [
        pytest.param(
            case,
            role,
            id=f"{case.label}/{role}",
            marks=[xfail(case.issue[role])] if role in case.issue else [],
        )
        for case in STREAM_CASES
        for role in case.roles
    ]


@pytest.mark.parametrize(("case", "role"), _stream_params())
def test_a_source_stream_or_pod_does_not_depend_on_the_arrangement_of_its_input(
    case: StreamCase, role: str, tmp_path: Path
) -> None:
    """Tag order and table layout change no hash, source id, name, schema or row."""
    results = {}
    for order, layout in ARRANGEMENTS:
        stream = case.build(role, order, layout, tmp_path)
        result = with_async(stream) if case.with_async else identity(stream)
        if hasattr(stream, "source_id"):
            result["source_id"] = stream.source_id
        results[f"{order}/{layout}"] = result
    assert_all_equal(results)


@pytest.mark.parametrize("role", ROLES)
def test_the_order_of_a_function_pods_inputs_changes_nothing(role: str) -> None:
    """``pod(a, b)`` joins its inputs: their order is not part of anything."""
    main, other = source(MAIN, role, 0, TAGS_FIRST), source(P, role, 0, TAGS_FIRST)
    assert_all_equal(
        {
            "(main, p)": with_async(across.pod(main, other)),
            "(p, main)": with_async(across.pod(other, main)),
        }
    )


@pytest.mark.parametrize("role", ROLES)
def test_the_order_of_tags_as_data_items_changes_nothing(role: str) -> None:
    origin = source(MAIN, role, 0, TAGS_FIRST)
    items = (("t1", "z1"), ("t2", "y2"), ("t3", "x3"))
    results = {}
    for order in itertools.permutations(items):
        derived = DerivedSource(origin, tags_as_data=dict(order))
        results[repr(order)] = {
            **identity(derived),
            "source_id": derived.source_id,
            "keys": derived.keys(),
        }
    assert_all_equal(results)


def test_the_order_of_tag_only_columns_changes_nothing() -> None:
    results = {}
    for order in itertools.permutations(("t1", "t3")):
        built = ArrowTableSource(
            table(MAIN, 0, TAGS_FIRST),
            tag_columns=list(MAIN.tags),
            tag_only_columns=list(order),
        )
        results[repr(order)] = {**identity(built), "source_id": built.source_id}
    assert_all_equal(results)


@pytest.mark.parametrize("role", [PLAIN, MIXED])
def test_the_order_of_a_raw_streams_data_tags_changes_nothing(role: str) -> None:
    tags = list(MAIN.tags)
    results = {
        repr(order): identity(
            ArrowTableStream(
                table(MAIN, 0, TAGS_FIRST), tag_columns=tags, data_tags=order
            )
        )
        for order in (["t1", "t3"], ["t3", "t1"])
    }
    assert_all_equal(results)


# ── presentation ────────────────────────────────────────────────────────────


def _shown(stream: Any) -> list[Any]:
    """The tags of the rows ``head(3)`` sorted by tags shows, in order."""
    stream.as_table()  # compute, so head has rows to show
    tag_keys, _ = stream.keys()
    shown = stream.head(3, columns={"sort_by_tags": True}).as_table()
    return [sorted(row.items()) for row in shown.select(list(tag_keys)).to_pylist()]


@pytest.mark.parametrize(
    "role", [pytest.param(r, marks=[xfail("ITL-1270")]) for r in ROLES]
)
def test_which_rows_head_shows_does_not_depend_on_tag_order(role: str) -> None:
    shown = {order: _shown(source(MAIN, role, order, TAGS_FIRST)) for order in range(3)}
    assert shown[0] == shown[1] == shown[2]


# ── exceptions: where order is meant to count ───────────────────────────────


def test_record_id_columns_order_counts() -> None:
    """``record_id_columns`` is the explicit key order: record ids follow it."""
    ids = set()
    for key in (["t1", "t2", "t3"], ["t3", "t2", "t1"]):
        built = _arrow_source(PLAIN, 0, TAGS_FIRST, Path(), record_id_columns=key)
        ids.add(tuple(sorted(r[2] for r in identity(built)["rows"])))
    assert len(ids) == 2


def test_the_docs_list_every_exception() -> None:
    """The section of ``docs/concepts/identity.md`` lists exactly ``ORDER_EXCEPTIONS``."""
    page = Path(orcapod.__file__).parents[2] / "docs" / "concepts" / "identity.md"
    section = page.read_text().split("## Order never counts, except where you give it")[
        1
    ]
    section = section.split("\n## ", 1)[0]
    table = section.split("| Order that counts | Why |\n|---|---|\n", 1)[1]
    listed = table.split("\n\n", 1)[0].splitlines()
    assert listed == [f"| {e.what} | {e.reason} |" for e in ORDER_EXCEPTIONS]


def _issues_on_marks() -> set[str]:
    """The issue of every expected-failure mark on a test of this module."""
    issues = set()
    for name, test in list(globals().items()):
        if not (name.startswith("test_") and callable(test)):
            continue
        for mark in getattr(test, "pytestmark", []):
            marks = [mark]
            if mark.name == "parametrize":
                # A ``pytest.param`` carries its own marks.
                marks = [
                    m for param in mark.args[1] for m in getattr(param, "marks", ())
                ]
            issues |= {
                m.kwargs["reason"].split(":", 1)[0] for m in marks if m.name == "xfail"
            }
    return issues


def test_every_known_violation_is_used() -> None:
    """``KNOWN_VIOLATIONS`` is exactly the issues the generated marks name.

    An issue left in it after its last mark is gone is stale, even if this file still
    mentions it; a mark naming an issue missing from it cannot be built.
    """
    assert _issues_on_marks() == set(KNOWN_VIOLATIONS)


# ── a job: stored tables, saved and loaded ─────────────────────────────────


class PrerequisiteFailed(Exception):
    """A failed check that comes before a comparison across arrangements.

    Async against sync, loaded against saved. It is not an ``AssertionError``, so an
    expected-failure mark, which takes ``AssertionError`` only, never hides it.
    """


_VOLATILE = {"__pod_ts", "__data_id", "executed_at", "pipeline_run_id"}
JOB_OPERATORS: dict[str, Callable[[Any, Any, Any], Any]] = {
    "join": lambda s, p, q: Join()(s, p, q, label="join"),
    "group_by": lambda s, p, q: GroupBy(["t1", "t2"])(s, label="group_by"),
    "batch": lambda s, p, q: Batch(batch_size=4)(s, label="batch"),
}
# The job comparisons: the declared tag order with the tags first, the reversed tag order
# with the tags first, and the declared order with the data first and reversed.
JOB_ARRANGEMENTS = ((0, TAGS_FIRST), (1, TAGS_FIRST), (0, DATA_FIRST_REVERSED))


def _job(
    role: str, order: int, layout: str, run_async: bool, tmp: Path, *, pod: bool
) -> dict[str, Any]:
    """A job in one arrangement: what must not depend on it.

    Args:
        role: The tag role of the sources.
        order: The tag order index.
        layout: The table layout.
        run_async: Run with ``run_async`` rather than ``run``.
        tmp: Where the job is saved.
        pod: Build only the function pod node (``True``), or only the operators and
            the side effect (``False``).

    Returns:
        Each node's hashes and rows, ``as_source`` of a node, every stored table's rows
        (volatile columns left out) and the loaded job's node hashes. A loaded node
        whose hashes differ from the saved job's fails here, in every role (a source
        slot's content hash excepted: a loaded slot over an in-memory table is unbound).
    """
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="order", database=database)
    with job:
        main = source(MAIN, role, order, layout)
        if pod:
            combine.pod(main, label="pod")
        else:
            other_p, other_q = (
                source(P, role, order, layout),
                source(Q, role, order, layout),
            )
            for build in JOB_OPERATORS.values():
                build(main, other_p, other_q)
            SideEffectPod(notify)(
                main.select_data_columns(["d1", "d3"]), label="effect"
            )
    if not pod:
        for label in JOB_OPERATORS:
            job.nodes[label].attach_databases(
                pipeline_database=database.at("ops"), cache_mode=CacheMode.LOG
            )
    if run_async:
        asyncio.run(job.run_async())
    else:
        job.run()
    out: dict[str, Any] = {}
    for label in sorted(job.nodes):
        node = job.nodes[label]
        out[f"{label}.hashes"] = (
            node.content_hash().to_string(),
            node.pipeline_hash().to_string(),
        )
        out[f"{label}.rows"] = row_identity(list(node.iter_data()))
    derived = job.nodes["pod" if pod else "join"].as_source()
    out["as_source"] = (
        derived.content_hash().to_string(),
        derived.pipeline_hash().to_string(),
        derived.source_id,
    )
    for path in database.list_table_paths():
        stored = database.get_all_records(path)
        if stored is None:
            continue
        keep = [c for c in stored.column_names if c not in _VOLATILE]
        out[f"stored[{'/'.join(path)}]"] = sorted(
            _masked(sorted(row.items())) for row in stored.select(keep).to_pylist()
        )
    saved = tmp / f"job_{role}_{order}_{layout}_{run_async}_{pod}.json"
    job.save(saved)
    loaded = PipelineJob.load(saved, database=database)
    out["loaded"] = {
        label: (node.content_hash().to_string(), node.pipeline_hash().to_string())
        for label, node in sorted(loaded.nodes.items())
    }
    # The loaded job is the job that was saved, not only equal across arrangements:
    # every node's pipeline hash, and the content hash of every node but the source
    # slots (an in-memory table is not saved, so a loaded source slot is unbound).
    for label, node in job.nodes.items():
        content, pipeline = out["loaded"][label]
        if pipeline != out[f"{label}.hashes"][1] or (
            not isinstance(node, SourceJobNode) and content != out[f"{label}.hashes"][0]
        ):
            raise PrerequisiteFailed(
                f"loaded {label}: hashes differ from the saved job"
            )
    return out


def _jobs_by_arrangement(role: str, tmp: Path, *, pod: bool) -> dict[str, Any]:
    """``_job`` in every ``JOB_ARRANGEMENTS``; the async run must equal the sync run."""
    results = {}
    for order, layout in JOB_ARRANGEMENTS:
        sync = _job(role, order, layout, False, tmp, pod=pod)
        if _job(role, order, layout, True, tmp, pod=pod) != sync:
            raise PrerequisiteFailed(f"async differs from sync in {order}/{layout}")
        results[f"{order}/{layout}"] = sync
    return results


@pytest.mark.parametrize("role", ROLES)
def test_a_jobs_operators_and_side_effect_do_not_depend_on_the_arrangement(
    role: str, tmp_path: Path
) -> None:
    """Node hashes, stored paths and rows, as_source and a loaded job; sync and async.

    The loaded job must have the saved job's pipeline hash at every node and content
    hash at every node but the source slots, which are left out because an in-memory
    table is not saved, so a loaded source slot is unbound and has another content hash.
    """
    assert_all_equal(_jobs_by_arrangement(role, tmp_path, pod=False))


@pytest.mark.parametrize(
    "role",
    [pytest.param(r, marks=[xfail("ITL-1246")] if r != PLAIN else []) for r in ROLES],
)
def test_a_jobs_function_pod_does_not_depend_on_the_arrangement(
    role: str, tmp_path: Path
) -> None:
    """The function pod node, alone in its job: the same elements as above.

    Building, running, the async run and loading are checked before the comparison and
    fail as usual; only the comparison across arrangements is ITL-1246's.
    """
    results = _jobs_by_arrangement(role, tmp_path, pod=True)
    assert_all_equal(results)


def test_a_saved_job_records_the_declared_order_and_loads_the_same_hashes(
    tmp_path: Path,
) -> None:
    """The saved file keeps each source's declared ``tag_columns``: placement only.

    Each loaded job has the saved job's hashes, and the two orders have the same.
    """
    database = InMemoryArrowDatabase()
    hashes = []
    for order in (0, 1):
        job = PipelineJob(name="order", database=database)
        with job:
            combine.pod(source(MAIN, PLAIN, order, TAGS_FIRST), label="pod")
        job.run()
        original = job.nodes["pod"].content_hash()
        path = tmp_path / f"job_{order}.json"
        job.save(path)
        saved = json.loads(path.read_text())
        assert saved["bindings"]["sources"]["s"]["tag_columns"] == list(
            MAIN.tag_orders()[order]
        )
        loaded = PipelineJob.load(path, database=database)
        assert loaded.nodes["pod"].content_hash() == original
        hashes.append(original)
    assert hashes[0] == hashes[1]
