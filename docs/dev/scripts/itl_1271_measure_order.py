"""Measure what depends on the order of tags, columns, argument items and inputs (ITL-1271).

Every operator exported by ``orcapod.operators``, the function pod forms, ``SideEffectPod``,
every source kind, a raw ``ArrowTableStream``, and the nodes of a job (sync and async,
stored, saved and loaded) are built over the inputs of ``tests/test_core/operators/
several_items.py`` (three tags, three data columns) in:

* every tag order of ``MAIN.tag_orders()`` (declared, reversed, one rotation);
* every physical table layout (tags first, data first reversed, interleaved);
* each role of the tags (all plain, all data-tags, mixed), measured separately;
* every order of the items of each multi-item argument;
* every order of the inputs of an operator that declares itself commutative.

For each element (``F`` = a hash, an identity or a stored value: freeze relevant;
``P`` = presentation only) the script prints the number of distinct values across each
axis; 1 means the element does not depend on that order. Run with
``uv run python docs/dev/scripts/itl_1271_measure_order.py [subject-substring ...]``.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import sqlite3
import sys
import tempfile
import traceback
import warnings
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa

warnings.simplefilter("ignore")

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))  # the shared inputs live in the test package

import orcapod as op  # noqa: E402
from orcapod.channels import Channel  # noqa: E402
from orcapod.core.streams import ArrowTableStream  # noqa: E402
from orcapod.databases import InMemoryArrowDatabase  # noqa: E402
from orcapod.operators import (  # noqa: E402
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
from orcapod.side_effects import SideEffectPod  # noqa: E402
from orcapod.sources import (  # noqa: E402
    ArrowTableSource,
    CachedSource,
    CSVSource,
    DataFrameSource,
    DeltaTableSource,
    DerivedSource,
    DictSource,
    ListSource,
    SQLiteTableSource,
)
from orcapod.types import CacheMode  # noqa: E402
from tests.test_core.operators.several_items import (  # noqa: E402
    DATA_TAGS,
    LAYOUTS,
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

TMP = Path(tempfile.mkdtemp(prefix="itl1271_"))
ORDERS = MAIN.tag_orders()

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
# A ``dict[str, int]`` column, as the type converter stores it.
KV = pa.large_list(pa.struct([("key", pa.large_string()), ("value", pa.int64())]))
NESTED = Spec(
    source_id="n",
    tags=MAIN.tags,
    data=("d1", "d4", "d5"),
    columns={
        **{tag: MAIN.columns[tag] for tag in MAIN.tags},
        "d1": MAIN.columns["d1"],
        "d4": pa.array(
            [
                [{"key": "speed", "value": i}, {"key": "size", "value": 10 - i}]
                for i in range(6)
            ],
            KV,
        ),
        "d5": pa.array(
            [[i, i + 1, i + 2] for i in range(6)], pa.large_list(pa.int64())
        ),
    },
    mixed_plain=MAIN.mixed_plain,
)


# ── what is compared ────────────────────────────────────────────────────────


def _v(value: Any) -> str:
    return repr(value)


def _random_id(token: Any) -> Any:
    """A function result's token holds a random (uuid7) record id: not an order effect."""
    if isinstance(token, str) and ":python.function." in token:
        parts = token.split("::")
        if len(parts) == 3:
            return f"{parts[0]}::<uuid7>::{parts[2]}"
    return token


def row_elements(rows: Sequence[tuple[Any, Any]], prefix: str = "") -> dict[str, Any]:
    """Each row's parts, keyed by its values, as sorted lists (row order is not compared)."""
    parts: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for tag, data in rows:
        key = _v(sorted(tag.as_dict().items())) + _v(sorted(data.as_dict().items()))
        parts["F rows: values"].append((key, ""))
        parts["F rows: system tags (record ids)"].append(
            (key, _v(sorted(tag.system_tags().items())))
        )
        parts["F rows: tag hash"].append((key, tag.content_hash().to_string()))
        parts["F rows: data hash"].append((key, data.content_hash().to_string()))
        parts["F rows: source info"].append(
            (key, _v(sorted((k, _random_id(v)) for k, v in data.source_info().items())))
        )
    return {prefix + k: sorted(v) for k, v in parts.items()}


def stream_elements(
    stream: Any, async_rows: Sequence[Any] | None = None
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    out["F content hash"] = stream.content_hash().to_string()
    out["F pipeline hash"] = stream.pipeline_hash().to_string()
    tag_keys, data_keys = stream.keys()
    all_tags, _ = stream.keys(columns={"system_tags": True})
    out["F system-tag column names"] = sorted(k for k in all_tags if k not in tag_keys)
    tag_schema, data_schema = stream.output_schema()
    out["F output_schema (as sets)"] = (
        sorted((k, _v(t)) for k, t in tag_schema.items()),
        sorted((k, _v(t)) for k, t in data_schema.items()),
    )
    rows = list(stream.iter_data())
    out.update(row_elements(rows))
    if async_rows is not None:
        async_part = row_elements(async_rows)
        sync_part = row_elements(rows)
        out["F async rows == sync rows"] = async_part == sync_part
        out["F async rows"] = async_part
    out["P keys() order"] = (tuple(tag_keys), tuple(data_keys))
    out["P as_table() column order"] = tuple(
        stream.as_table(all_info=True).column_names
    )
    head = stream.head(3, columns={"sort_by_tags": True}).as_table()
    shown = head.select([k for k in tag_keys if k in head.column_names]).to_pylist()
    out["P head(3, sorted) rows shown"] = [sorted(r.items()) for r in shown]
    return out


async def _async(producer: Any, upstreams: Sequence[Any]) -> list[Any]:
    channels = [Channel(buffer_size=256) for _ in upstreams]
    output = Channel(buffer_size=256)
    for stream, channel in zip(upstreams, channels):
        for row in stream.iter_data():
            await channel.writer.send(row)
        await channel.writer.close()
    readers = [c.reader for c in channels]
    try:
        await producer.async_execute(
            readers,
            output.writer,
            input_pipeline_hashes=[s.pipeline_hash() for s in upstreams],
        )
    except TypeError:
        await producer.async_execute(readers, output.writer)
    return await output.reader.collect()


def async_rows_of(stream: Any) -> list[Any] | None:
    producer = getattr(stream, "producer", None)
    if producer is None or not hasattr(producer, "async_execute"):
        return None
    return asyncio.run(_async(producer, list(stream.upstreams)))


# ── subjects ────────────────────────────────────────────────────────────────

# A subject builds, for one role, one tag-order index and one layout, a dict of elements.
# ``arg_orders`` gives, for one role, a dict {argument order label: elements} measured on
# the declared arrangement; ``input_orders`` the same for the order of input streams.


class Subject:
    def __init__(
        self,
        name: str,
        build: Callable[[str, int, str], dict[str, Any]],
        roles: Sequence[str] = ROLES,
        layouts: Sequence[str] = LAYOUTS,
        arg_orders: Callable[[str], dict[str, dict[str, Any]]] | None = None,
        input_orders: Callable[[str], dict[str, dict[str, Any]]] | None = None,
    ) -> None:
        self.name = name
        self.build = build
        self.roles = roles
        self.layouts = layouts
        self.arg_orders = arg_orders
        self.input_orders = input_orders


def src(spec: Spec, role: str, order: int, layout: str, **kw: Any) -> ArrowTableSource:
    orders = spec.tag_orders()
    return make_source(
        spec, Arrangement(orders[order % len(orders)], layout, role), **kw
    )


def op_elements(make_op: Callable[[], Any], inputs: Sequence[Any]) -> dict[str, Any]:
    operator = make_op()
    stream = operator(*inputs)
    out = stream_elements(stream, async_rows_of(stream))
    out["F operator identity (content hash)"] = operator.content_hash().to_string()
    return out


def unary(
    name: str,
    make_op: Callable[..., Any],
    items: Sequence[Any] | None = None,
    spec: Spec = MAIN,
    roles: Sequence[str] = ROLES,
    as_mapping: bool = False,
    as_kwargs: bool = False,
) -> Subject:
    """An operator over ``spec``; ``items`` are the argument's items, permuted."""

    def build_op(its: Sequence[Any] | None) -> Callable[[], Any]:
        if its is None:
            return make_op
        if as_mapping:
            return lambda: make_op(dict(its))
        return lambda: make_op(list(its))

    def build(role: str, order: int, layout: str) -> dict[str, Any]:
        return op_elements(build_op(items), [src(spec, role, order, layout)])

    def by_argument_order(role: str) -> dict[str, dict[str, Any]]:
        assert items is not None
        return {
            _v(perm): op_elements(build_op(perm), [src(spec, role, 0, TAGS_FIRST)])
            for perm in itertools.permutations(items)
        }

    permuted = items is not None and len(items) > 1
    return Subject(
        name, build, roles=roles, arg_orders=by_argument_order if permuted else None
    )


def several(
    name: str,
    make_op: Callable[[], Any],
    specs: Sequence[Spec],
    commutative: bool,
    rows: list[int] | None = None,
) -> Subject:
    def inputs(role: str, order: int, layout: str) -> list[Any]:
        return [src(s, role, order, layout, rows=rows) for s in specs]

    def build(role: str, order: int, layout: str) -> dict[str, Any]:
        return op_elements(make_op, inputs(role, order, layout))

    def by_input_order(role: str) -> dict[str, dict[str, Any]]:
        base = inputs(role, 0, TAGS_FIRST)
        return {
            _v([s.source_id for s in perm]): op_elements(make_op, list(perm))
            for perm in itertools.permutations(base)
        }

    return Subject(name, build, input_orders=by_input_order if commutative else None)


OPERATOR_SUBJECTS: dict[str, list[Subject]] = {
    "Join": [several("Join(MAIN, P, Q)", Join, (MAIN, P, Q), commutative=True)],
    "MergeJoin": [
        several(
            "MergeJoin(MAIN, MAIN_OTHER)",
            MergeJoin,
            (MAIN, MAIN_OTHER),
            commutative=True,
            rows=NON_NULL_ROWS,
        )
    ],
    "SemiJoin": [several("SemiJoin(MAIN, P)", SemiJoin, (MAIN, P), commutative=False)],
    "MapTags": [
        unary(
            "MapTags",
            MapTags,
            (("t1", "u1"), ("t2", "u2"), ("t3", "u3")),
            as_mapping=True,
        )
    ],
    "MapData": [
        unary(
            "MapData",
            MapData,
            (("d1", "e1"), ("d2", "e2"), ("d3", "e3")),
            as_mapping=True,
        )
    ],
    "MapColumns": [
        unary(
            "MapColumns",
            MapColumns,
            (("t1", "u1"), ("d2", "e2"), ("t3", "u3")),
            as_mapping=True,
            roles=(PLAIN,),
        )
    ],
    "Batch": [unary("Batch(4)", lambda: Batch(batch_size=4))],
    "GroupBy": [
        unary("GroupBy(by t1,t2)", GroupBy, ("t1", "t2")),
        # Two non-key tags order the members (ITL-1226).
        unary("GroupBy(by t1)", lambda: GroupBy("t1")),
    ],
    "SelectTagColumns": [unary("SelectTagColumns", SelectTagColumns, ("t1", "t3"))],
    "SelectDataColumns": [unary("SelectDataColumns", SelectDataColumns, ("d1", "d3"))],
    "DropTagColumns": [unary("DropTagColumns", DropTagColumns, ("t1", "t3"))],
    "DropDataColumns": [unary("DropDataColumns", DropDataColumns, ("d1", "d3"))],
    "SelectColumns": [unary("SelectColumns", SelectColumns, ("t1", "d3", "t3", "d1"))],
    "DropColumns": [unary("DropColumns", DropColumns, ("t1", "d3"))],
    "PolarsFilter": [
        unary(
            "PolarsFilter(constraints)",
            lambda c: PolarsFilter(constraints=c),
            (("t1", "a"), ("t2", 1)),
            as_mapping=True,
        ),
        unary(
            "PolarsFilter(predicates)",
            lambda p: PolarsFilter(predicates=p),
            (pl.col("d1") > 10, pl.col("t3") == 1),
        ),
    ],
    "DropNulls": [
        unary("DropNulls(d2,t2,d1)", DropNulls, ("d2", "t2", "d1")),
        unary("DropNulls()", DropNulls),
    ],
    "AddTags": [
        unary("AddTags(x,y)", AddTags, (("x", 1), ("y", "z")), as_mapping=True)
    ],
    "Pick": [unary("Pick(d4.speed)", lambda: Pick("d4", "speed"), spec=NESTED)],
    "Index": [unary("Index(d5[1])", lambda: Index("d5", 1), spec=NESTED)],
    "TagBy": [unary("TagBy(d3,d1)", TagBy, ("d3", "d1"))],
}


# Function pods and side effects ----------------------------------------------------


@op.function_pod(output_keys="out")
def combine(d1: int, d3: str) -> str:
    return f"{d3}{d1}"


SENT: list[Any] = []


def notify(ctx: Any, d1: int, d3: str) -> None:
    SENT.append((d1, d3))


def pod_call(role: str, order: int, layout: str) -> dict[str, Any]:
    stream = combine.pod(src(MAIN, role, order, layout))
    return stream_elements(stream, async_rows_of(stream))


def pod_process(role: str, order: int, layout: str) -> dict[str, Any]:
    selected = src(MAIN, role, order, layout).select_data_columns(["d1", "d3"])
    stream = combine.pod.process(selected)
    return stream_elements(stream, async_rows_of(stream))


def side_effect(role: str, order: int, layout: str) -> dict[str, Any]:
    pod = SideEffectPod(notify)
    stream = pod(src(MAIN, role, order, layout).select_data_columns(["d1", "d3"]))
    return stream_elements(stream, async_rows_of(stream))


# Sources ---------------------------------------------------------------------------


def _table(spec: Spec, order: int, layout: str) -> pa.Table:
    names = _layout(spec, spec.tag_orders()[order], layout)
    cols = {n: spec.columns[n] for n in names}
    schema = pa.schema(
        pa.field(n, c.type, nullable=c.null_count > 0) for n, c in cols.items()
    )
    return pa.table(list(cols.values()), schema=schema)


def _tag_only(spec: Spec, role: str) -> bool | list[str]:
    return {PLAIN: True, DATA_TAGS: [], MIXED: [spec.mixed_plain]}[role]


def source_elements(source: Any) -> dict[str, Any]:
    out = stream_elements(source)
    out["F source_id"] = source.source_id
    return out


def arrow_source(default_id: bool) -> Callable[[str, int, str], dict[str, Any]]:
    def build(role: str, order: int, layout: str) -> dict[str, Any]:
        return source_elements(
            ArrowTableSource(
                _table(MAIN, order, layout),
                tag_columns=list(MAIN.tag_orders()[order]),
                tag_only_columns=_tag_only(MAIN, role),
                source_id=None if default_id else "s",
            )
        )

    return build


def dict_source(default_id: bool) -> Callable[[str, int, str], dict[str, Any]]:
    def build(role: str, order: int, layout: str) -> dict[str, Any]:
        return source_elements(
            DictSource(
                _table(MAIN, order, layout).to_pylist(),
                tag_columns=list(MAIN.tag_orders()[order]),
                tag_only_columns=_tag_only(MAIN, role),
                source_id=None if default_id else "s",
            )
        )

    return build


def frame_source(default_id: bool) -> Callable[[str, int, str], dict[str, Any]]:
    def build(role: str, order: int, layout: str) -> dict[str, Any]:
        return source_elements(
            DataFrameSource(
                pl.from_arrow(_table(MAIN, order, layout)),
                tag_columns=list(MAIN.tag_orders()[order]),
                tag_only_columns=_tag_only(MAIN, role),
                source_id=None if default_id else "s",
            )
        )

    return build


def list_source(default_id: bool) -> Callable[[str, int, str], dict[str, Any]]:
    rows = MAIN_ROWS = _table(MAIN, 0, TAGS_FIRST).to_pylist()

    def build(role: str, order: int, layout: str) -> dict[str, Any]:
        tags = MAIN.tag_orders()[order]

        def tag_function(element: Any, index: int) -> dict[str, Any]:
            return {t: MAIN_ROWS[index][t] for t in tags}

        return source_elements(
            ListSource(
                "d1",
                [r["d1"] for r in rows],
                tag_function=tag_function,
                expected_tag_keys=list(tags),
                source_id=None if default_id else "s",
            )
        )

    return build


def csv_source(role: str, order: int, layout: str) -> dict[str, Any]:
    import pyarrow.csv as pacsv

    path = TMP / "same.csv"  # one path, so the location-based default id is the same
    pacsv.write_csv(_table(MAIN, order, layout), path)
    return source_elements(
        CSVSource(
            str(path),
            tag_columns=list(MAIN.tag_orders()[order]),
            tag_only_columns=_tag_only(MAIN, role),
        )
    )


def delta_source(role: str, order: int, layout: str) -> dict[str, Any]:
    from deltalake import write_deltalake

    path = TMP / f"delta_{role}_{order}_{layout}"
    write_deltalake(str(path), _table(MAIN, order, layout))
    return source_elements(
        DeltaTableSource(
            path,
            tag_columns=list(MAIN.tag_orders()[order]),
            tag_only_columns=_tag_only(MAIN, role),
            source_id="delta_s",
        )
    )


def _sqlite(order: int, layout: str, pk: Sequence[str]) -> Path:
    path = TMP / f"db_{order}_{layout}_{'_'.join(pk)}.sqlite"
    if path.exists():
        return path
    table = _table(MAIN, order, layout)
    sqltype = {
        "t1": "TEXT",
        "t2": "INTEGER",
        "t3": "INTEGER",
        "d1": "INTEGER",
        "d2": "REAL",
        "d3": "TEXT",
    }
    con = sqlite3.connect(path)
    cols = ", ".join(f"{n} {sqltype[n]}" for n in table.column_names)
    con.execute(f"CREATE TABLE m ({cols}, PRIMARY KEY ({', '.join(pk)}))")
    rows = table.to_pylist()
    con.executemany(
        f"INSERT INTO m VALUES ({', '.join('?' for _ in table.column_names)})",
        [tuple(r[n] for n in table.column_names) for r in rows],
    )
    con.commit()
    con.close()
    return path


def sqlite_source(role: str, order: int, layout: str) -> dict[str, Any]:
    # The primary key is declared in one order (t1, t2, t3): its order is the record-id
    # key order, an explicit argument; the tag order varies.
    path = _sqlite(order, layout, MAIN.tags)
    return source_elements(
        SQLiteTableSource(
            path,
            "m",
            tag_columns=list(MAIN.tag_orders()[order]),
            tag_only_columns=_tag_only(MAIN, role),
            source_id="sqlite_s",
        )
    )


def cached_source(role: str, order: int, layout: str) -> dict[str, Any]:
    inner = src(MAIN, role, order, layout)
    return source_elements(CachedSource(inner, cache_database=InMemoryArrowDatabase()))


def derived_tags_as_data_items() -> tuple[tuple[str, str], ...]:
    return (("t1", "x1"), ("t3", "x3"))


def derived_tags_as_data(
    role: str, order: int, layout: str, items: Sequence[tuple[str, str]] | None = None
) -> dict[str, Any]:
    origin = src(MAIN, role, order, layout)
    return source_elements(
        DerivedSource(origin, tags_as_data=dict(items or derived_tags_as_data_items()))
    )


def raw_stream(role: str, order: int, layout: str) -> dict[str, Any]:
    tags = list(MAIN.tag_orders()[order])
    data_tags = {
        PLAIN: [],
        DATA_TAGS: tags,
        MIXED: [t for t in tags if t != MAIN.mixed_plain],
    }[role]
    return stream_elements(
        ArrowTableStream(
            _table(MAIN, order, layout), tag_columns=tags, data_tags=data_tags
        )
    )


# Further argument and input orders ---------------------------------------------------


def arrow_source_args(
    role: str,
    order: int,
    layout: str,
    tag_only_columns: bool | list[str] | None = None,
    record_id_columns: list[str] | None = None,
) -> dict[str, Any]:
    return source_elements(
        ArrowTableSource(
            _table(MAIN, order, layout),
            tag_columns=list(MAIN.tag_orders()[order]),
            tag_only_columns=_tag_only(MAIN, role)
            if tag_only_columns is None
            else tag_only_columns,
            source_id="s",
            record_id_columns=record_id_columns,
        )
    )


def tag_only_orders(role: str) -> dict[str, dict[str, Any]]:
    return {
        _v(perm): arrow_source_args(role, 0, TAGS_FIRST, tag_only_columns=list(perm))
        for perm in itertools.permutations(("t1", "t3"))
    }


def record_id_orders(role: str) -> dict[str, dict[str, Any]]:
    return {
        _v(perm): arrow_source_args(role, 0, TAGS_FIRST, record_id_columns=list(perm))
        for perm in [("t1", "t2", "t3"), ("t3", "t2", "t1"), ("t2", "t3", "t1")]
    }


def raw_data_tag_orders(role: str) -> dict[str, dict[str, Any]]:
    tags = list(MAIN.tags)
    return {
        _v(perm): stream_elements(
            ArrowTableStream(
                _table(MAIN, 0, TAGS_FIRST), tag_columns=tags, data_tags=list(perm)
            )
        )
        for perm in itertools.permutations(("t1", "t3"))
    }


@op.function_pod(output_keys="joined")
def across(d1: int, p1: int) -> int:
    return d1 * 100 + p1


def pod_two_inputs(
    role: str, order: int, layout: str, reverse: bool = False
) -> dict[str, Any]:
    inputs = [src(MAIN, role, order, layout), src(P, role, order, layout)]
    stream = across.pod(*(reversed(inputs) if reverse else inputs))
    return stream_elements(stream)


def pod_input_orders(role: str) -> dict[str, dict[str, Any]]:
    return {
        "(MAIN, P)": pod_two_inputs(role, 0, TAGS_FIRST),
        "(P, MAIN)": pod_two_inputs(role, 0, TAGS_FIRST, reverse=True),
    }


def job_input_orders(role: str) -> dict[str, dict[str, Any]]:
    out = {}
    for label, perm in (("(s, p, q)", (0, 1, 2)), ("(q, s, p)", (2, 0, 1))):
        JOIN_ORDER[0] = perm
        out[label] = job_elements(role, 0, TAGS_FIRST)
    JOIN_ORDER[0] = (0, 1, 2)
    return out


# Jobs ------------------------------------------------------------------------------

JOIN_ORDER: list[tuple[int, int, int]] = [(0, 1, 2)]
OP_IN_JOB = {
    "join": lambda s, p, q: Join()(
        *[(s, p, q)[i] for i in JOIN_ORDER[0]], label="join"
    ),
    "group_by": lambda s, p, q: GroupBy(["t1", "t2"])(s, label="group_by"),
    "tag_by": lambda s, p, q: TagBy(["d3", "d1"])(s, label="tag_by"),
    "batch": lambda s, p, q: Batch(batch_size=4)(s, label="batch"),
    "add_tags": lambda s, p, q: AddTags({"x": 1, "y": "z"})(s, label="add_tags"),
}


def _stored(db: InMemoryArrowDatabase) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for path in sorted(db.list_table_paths()):
        table = db.get_all_records(path)
        if table is None:
            continue
        out["/".join(path)] = table
    return out


def _flatten(obj: Any, prefix: str = "") -> dict[str, Any]:
    if isinstance(obj, dict):
        flat: dict[str, Any] = {}
        for k, v in obj.items():
            flat.update(_flatten(v, f"{prefix}/{k}"))
        return flat
    if isinstance(obj, list) and any(isinstance(v, (dict, list)) for v in obj):
        flat = {}
        for i, v in enumerate(obj):
            flat.update(_flatten(v, f"{prefix}[{i}]"))
        return flat
    return {prefix: obj}


def build_job(role: str, order: int, layout: str, async_run: bool) -> tuple[Any, Any]:
    db = InMemoryArrowDatabase()
    job = op.PipelineJob(name="m", database=db)
    with job:
        s = src(MAIN, role, order, layout)
        p = src(P, role, order, layout)
        q = src(Q, role, order, layout)
        combine.pod(s, label="pod")
        for build in OP_IN_JOB.values():
            build(s, p, q)
        SideEffectPod(notify)(s.select_data_columns(["d1", "d3"]), label="side_effect")
    for label in OP_IN_JOB:
        job.nodes[label].attach_databases(
            pipeline_database=db.at("ops"), cache_mode=CacheMode.LOG
        )
    if async_run:
        asyncio.run(job.run_async())
    else:
        job.run()
    return job, db


VOLATILE: dict[str, set[str]] = {}


def job_elements(role: str, order: int, layout: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for mode in ("sync", "async"):
        job, db = build_job(role, order, layout, async_run=mode == "async")
        stored = _stored(db)
        out[f"F {mode}: stored table paths"] = sorted(stored)
        for label in sorted(job.nodes):
            node = job.nodes[label]
            out[f"F {mode}: node[{label}] content hash"] = (
                node.content_hash().to_string()
            )
            out[f"F {mode}: node[{label}] pipeline hash"] = (
                node.pipeline_hash().to_string()
            )
            if label in ("pod", "join", "group_by", "side_effect"):
                out.update(
                    row_elements(
                        list(node.iter_data()), prefix=f"{mode}: node[{label}] "
                    )
                )
        for i, (path, table) in enumerate(sorted(stored.items())):
            key = path.split("/")[0] + "/.../" + path.split("/")[-1]
            volatile = VOLATILE.get(key, set())
            keep = [c for c in table.column_names if c not in volatile]
            out[f"F {mode}: stored rows [{key}]"] = sorted(
                _v(sorted((c, r[c]) for c in keep))
                for r in table.select(keep).to_pylist()
            )
            out[f"P {mode}: stored column order [{key}]"] = tuple(table.column_names)
        if mode == "sync":
            path = TMP / f"job_{role}_{order}_{layout}.json"
            job.save(path)
            flat = _flatten(json.loads(path.read_text()))
            volatile = VOLATILE.get("saved", set())
            out["F saved job file (normalized)"] = sorted(
                (k, _v(v)) for k, v in flat.items() if k not in volatile
            )
            loaded = op.PipelineJob.load(path, database=db)
            for label in sorted(loaded.nodes):
                node = loaded.nodes[label]
                out[f"F loaded: node[{label}] content hash"] = (
                    node.content_hash().to_string()
                )
                out[f"F loaded: node[{label}] pipeline hash"] = (
                    node.pipeline_hash().to_string()
                )
            out.update(
                row_elements(
                    list(loaded.nodes["pod"].as_source().iter_data()),
                    prefix="loaded: node[pod].as_source() ",
                )
            )
        for label in ("pod", "join"):
            derived = job.nodes[label].as_source()  # DerivedSource, plain
            out[f"F {mode}: DerivedSource(node[{label}]) content hash"] = (
                derived.content_hash().to_string()
            )
            out[f"F {mode}: DerivedSource(node[{label}]) pipeline hash"] = (
                derived.pipeline_hash().to_string()
            )
            out[f"F {mode}: DerivedSource(node[{label}]) source_id"] = derived.source_id
    return out


def learn_volatile() -> None:
    """Columns and saved-file entries that differ between two identical runs."""
    tables = [
        _stored(build_job(PLAIN, 0, TAGS_FIRST, async_run=False)[1]) for _ in range(2)
    ]
    for path, table in tables[0].items():
        other = tables[1].get(path)
        key = path.split("/")[0] + "/.../" + path.split("/")[-1]
        VOLATILE[key] = {
            c
            for c in table.column_names
            if other is None
            or c not in other.column_names
            or sorted(map(_v, table.column(c).to_pylist()))
            != sorted(map(_v, other.column(c).to_pylist()))
        }
    flats = []
    for i in range(2):
        job, _ = build_job(PLAIN, 0, TAGS_FIRST, async_run=False)
        path = TMP / f"volatile_{i}.json"
        job.save(path)
        flats.append(_flatten(json.loads(path.read_text())))
    VOLATILE["saved"] = {k for k in flats[0] if flats[0][k] != flats[1].get(k)}


# ── the run ─────────────────────────────────────────────────────────────────


def subjects() -> list[Subject]:
    out = [s for group in OPERATOR_SUBJECTS.values() for s in group]
    out += [
        Subject("pod(source) [function pod, call]", pod_call),
        Subject("pod.process(select) [function pod, process]", pod_process),
        Subject("SideEffectPod(select) [outside a job]", side_effect),
        Subject("ArrowTableSource(source_id given)", arrow_source(False)),
        Subject("ArrowTableSource(default source_id)", arrow_source(True)),
        Subject("DictSource(source_id given)", dict_source(False)),
        Subject("DictSource(default source_id)", dict_source(True)),
        Subject("DataFrameSource(source_id given)", frame_source(False)),
        Subject("DataFrameSource(default source_id)", frame_source(True)),
        Subject(
            "ListSource(source_id given)",
            list_source(False),
            roles=(PLAIN,),
            layouts=(TAGS_FIRST,),
        ),
        Subject(
            "ListSource(default source_id)",
            list_source(True),
            roles=(PLAIN,),
            layouts=(TAGS_FIRST,),
        ),
        Subject("CSVSource(default location id)", csv_source),
        Subject("DeltaTableSource", delta_source),
        Subject("SQLiteTableSource(pk t1,t2,t3)", sqlite_source),
        Subject("CachedSource(ArrowTableSource)", cached_source),
        Subject(
            "DerivedSource(tags_as_data t1,t3)",
            derived_tags_as_data,
            arg_orders=lambda role: {
                _v(perm): derived_tags_as_data(role, 0, TAGS_FIRST, perm)
                for perm in itertools.permutations(derived_tags_as_data_items())
            },
        ),
        Subject("raw ArrowTableStream", raw_stream),
        Subject(
            "ArrowTableSource tag_only_columns order",
            arrow_source_args,
            roles=(PLAIN,),
            layouts=(TAGS_FIRST,),
            arg_orders=tag_only_orders,
        ),
        Subject(
            "ArrowTableSource record_id_columns order [EXCEPTION: key order]",
            arrow_source_args,
            roles=(PLAIN,),
            layouts=(TAGS_FIRST,),
            arg_orders=record_id_orders,
        ),
        Subject(
            "raw ArrowTableStream data_tags order",
            raw_stream,
            roles=(PLAIN,),
            layouts=(TAGS_FIRST,),
            arg_orders=raw_data_tag_orders,
        ),
        Subject(
            "pod(MAIN, P) [function pod over two inputs]",
            pod_two_inputs,
            input_orders=pod_input_orders,
        ),
        Subject(
            "PipelineJob (pod, join, group_by, tag_by, batch, add_tags, side effect; "
            "DerivedSource plain = node.as_source)",
            job_elements,
            input_orders=job_input_orders,
        ),
    ]
    return out


def measure(subject: Subject) -> list[tuple[str, str, str, int, int, int, int]]:
    """Rows: (subject, role, element, by tag order, by layout, by argument order, by input order)."""
    report = []
    for role in subject.roles:
        cells: dict[str, dict[tuple[int, str], str]] = defaultdict(dict)
        for layout in subject.layouts:
            for order in range(len(ORDERS)):
                try:
                    elements = subject.build(role, order, layout)
                except Exception as exc:  # noqa: BLE001
                    elements = {"ERROR": f"{type(exc).__name__}: {exc}"}
                    traceback.print_exc(limit=3, file=sys.stderr)
                for key, value in elements.items():
                    cells[key][(order, layout)] = _v(value)
        extra: dict[str, dict[str, dict[str, str]]] = {}
        for axis, fn in (("arg", subject.arg_orders), ("input", subject.input_orders)):
            if fn is None:
                continue
            try:
                variants = fn(role)
            except Exception as exc:  # noqa: BLE001
                variants = {"ERROR": {"ERROR": f"{type(exc).__name__}: {exc}"}}
            per_key: dict[str, dict[str, str]] = defaultdict(dict)
            for label, elements in variants.items():
                for key, value in elements.items():
                    per_key[key][label] = _v(value)
            extra[axis] = per_key
        keys = list(cells) + [
            k for axis in extra.values() for k in axis if k not in cells
        ]
        for key in keys:
            c = cells.get(key, {})
            by_order = max(
                (
                    len({c[(o, lay)] for o in range(len(ORDERS)) if (o, lay) in c})
                    for lay in subject.layouts
                ),
                default=0,
            )
            by_layout = max(
                (
                    len({c[(o, lay)] for lay in subject.layouts if (o, lay) in c})
                    for o in range(len(ORDERS))
                ),
                default=0,
            )
            by_arg = len(set(extra.get("arg", {}).get(key, {}).values()))
            by_input = len(set(extra.get("input", {}).get(key, {}).values()))
            if key == "F async rows == sync rows":
                arrangements = [k for k, v in c.items() if v == "False"]
                if arrangements:
                    report.append(
                        (
                            subject.name,
                            role,
                            f"F ASYNC ROWS DIFFER FROM SYNC in {len(arrangements)} of "
                            f"{len(c)} arrangements",
                            0,
                            0,
                            0,
                            0,
                        )
                    )
                continue
            report.append(
                (subject.name, role, key, by_order, by_layout, by_arg, by_input)
            )
            if key == "ERROR":
                print(
                    f"  ERROR {subject.name}/{role}: {sorted(set(c.values()))[:1]}",
                    file=sys.stderr,
                )
    return report


def main(filters: Iterable[str]) -> None:
    filters = list(filters)
    chosen = [s for s in subjects() if not filters or any(f in s.name for f in filters)]
    if any("PipelineJob" in s.name for s in chosen):
        learn_volatile()
    print(
        "| subject | role | element | tag order | layout | argument order | input order |"
    )
    print("|---|---|---|---|---|---|---|")
    stable: list[str] = []
    for subject in chosen:
        print(f"... {subject.name}", file=sys.stderr, flush=True)
        rows = measure(subject)
        moved = [
            r for r in rows if max(r[3:]) > 1 or r[2] == "ERROR" or "ASYNC" in r[2]
        ]
        for name, role, key, *counts in moved:
            cells = " | ".join("**%d**" % n if n > 1 else str(n or "-") for n in counts)
            print(f"| {name} | {role} | {key} | {cells} |", flush=True)
        if not any(not r[2].startswith("P ") for r in moved):
            stable.append(subject.name)
    print(
        "\nNo freeze-relevant (F) element depends on any order:",
        ", ".join(stable) or "(none)",
    )


if __name__ == "__main__":
    main(sys.argv[1:])
