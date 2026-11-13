"""ITL-1004 audit, measurement 02: run every operator over a PROTOTYPE referential stream.

The prototype is today's ``ArrowTableStream`` with one physical column ``x`` listed both as
a tag and as a data column (the "name in both lists" representation). It is built by
setting the stream's private column lists after construction: this is a measurement of
what today's operator code does with such a stream, not a proposed implementation. Nothing
under ``src/`` is changed.

Run: uv run python superpowers/specs/itl-1004-audit/02_prototype_unified.py
Output: 02_prototype_unified.out
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
from _common import async_rows, describe, f64, i64, s, show, src

from orcapod import function_pod
from orcapod.operators import (
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
    PolarsFilter,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.system_constants import constants


def make_ref(source: Any, refs: list[str]) -> Any:
    """Turn tags ``refs`` of ``source`` into referential tags (prototype, private attrs).

    The physical column stays single. It is added to the data column list and given a
    source-info column, as a data column would have.
    """
    st = source._stream  # the source's inner ArrowTableStream
    names = st._table.column_names
    st._data_columns = tuple(c for c in names if c in refs or c in st._data_columns)
    st._data_schema = pa.schema(
        f for f in st._table.schema if f.name in st._data_columns
    )
    for r in refs:
        col = f"{constants.SOURCE_PREFIX}{r}"
        if col not in st._source_info_table.column_names:
            n = st._table.num_rows
            st._source_info_table = st._source_info_table.append_column(
                pa.field(col, pa.large_string(), nullable=False),
                pa.array(
                    [f"{source.source_id}::row_{i}::{r}" for i in range(n)],
                    pa.large_string(),
                ),
            )
    return source


def ref(
    columns: dict[str, pa.Array], tags: list[str], refs: list[str], sid: str
) -> Any:
    return make_ref(src(columns, tags, sid), refs)


R = ref(
    {
        "session": s("s1", "s1", "s2"),
        "x": i64(20, 10, 30),
        "y": f64(0.2, 0.1, 0.3),
    },
    ["session", "x"],
    ["x"],
    "R",
)
print("=== P0. The prototype stream R: tags session, x(ref); data x, y")
show("describe(R)", lambda: describe(R))
show("R.pipeline_hash()", lambda: R.pipeline_hash().to_string())
show(
    "R.as_table(all_info=True) columns", lambda: R.as_table(all_info=True).column_names
)

print("\n=== P1. Row filters (spec: preserve)")
show(
    "PolarsFilter(constraints={'x': 20})(R)",
    lambda: describe(PolarsFilter(constraints={"x": 20})(R)),
)
show("DropNulls(['x'])(R)", lambda: describe(DropNulls(["x"])(R)))
show("DropNulls()(R)", lambda: describe(DropNulls()(R)))
M = src({"x": i64(10, 30), "w": f64(1.0, 3.0)}, ["x"], "M")
show("SemiJoin()(R, M[tag x materialized])", lambda: describe(SemiJoin()(R, M)))

print("\n=== P2. Select / Drop (spec: removing the target materializes the tag)")
show("SelectDataColumns(['y'])(R)", lambda: describe(SelectDataColumns(["y"])(R)))
show("DropDataColumns(['x'])(R)", lambda: describe(DropDataColumns(["x"])(R)))
show(
    "DropTagColumns(['x'])(R)  (spec: data x kept)",
    lambda: describe(DropTagColumns(["x"])(R)),
)
show(
    "SelectTagColumns(['session'])(R)",
    lambda: describe(SelectTagColumns(["session"])(R)),
)

print("\n=== P3. Renames")
show("MapTags({'x': 'x2'})(R)", lambda: describe(MapTags({"x": "x2"})(R)))
show("MapData({'x': 'x2'})(R)", lambda: describe(MapData({"x": "x2"})(R)))
show(
    "MapTags({'session': 'sess'})(R)", lambda: describe(MapTags({"session": "sess"})(R))
)

print(
    "\n=== P4. GroupBy (spec: key scalar in both roles, non-key list in both, one order)"
)
show("GroupBy(['x'])(R)", lambda: describe(GroupBy(["x"])(R)))
show(
    "GroupBy(['x']).output_schema(R) (prediction)",
    lambda: GroupBy(["x"]).output_schema(R),
)
show("GroupBy(['session'])(R)", lambda: describe(GroupBy(["session"])(R)))
show(
    "GroupBy(['session']).output_schema(R)",
    lambda: GroupBy(["session"]).output_schema(R),
)
show("GroupBy(['session']) async rows", lambda: async_rows(GroupBy(["session"]), R))
show("Batch(2)(R)", lambda: describe(Batch(batch_size=2)(R)))

print("\n=== P5. Join")
show("Join().output_schema(R, M)", lambda: Join().output_schema(R, M))
show("Join()(R, M) [ref x + materialized x]", lambda: describe(Join()(R, M)))
show("Join async (R, M)", lambda: async_rows(Join(), R, M))
R2 = ref(
    {"x": i64(10, 20), "z": f64(5.0, 6.0)},
    ["x"],
    ["x"],
    "R2",
)
show("Join().output_schema(R, R2) [ref + ref]", lambda: Join().output_schema(R, R2))
show("Join()(R, R2)", lambda: describe(Join()(R, R2)))
N = src({"k": i64(1, 2), "x": i64(10, 20)}, ["k"], "N")
show("Join().output_schema(R, N[data x, no tag x])", lambda: Join().output_schema(R, N))
show("MergeJoin()(R, R2)", lambda: describe(MergeJoin()(R, R2)))

print("\n=== P6. Function pods")


@function_pod(output_keys="z")
def uses_x(x: int | None, y: float | None) -> float:
    return float(x or 0) + (y or 0.0)


@function_pod(output_keys="z")
def uses_y(y: float | None) -> float:
    return (y or 0.0) * 2


@function_pod(output_keys="x")
def writes_x(y: float | None) -> float:
    return (y or 0.0) * 2


show("uses_x.pod(R)", lambda: describe(uses_x.pod(R)))
show("uses_y.pod(R) (automatic select drops data x)", lambda: describe(uses_y.pod(R)))
show(
    "uses_y.pod.process(R) (strict: extra data x)",
    lambda: describe(uses_y.pod.process(R)),
)
show("writes_x.pod.output_schema(R)", lambda: writes_x.pod.output_schema(R))
show("writes_x.pod(R)", lambda: describe(writes_x.pod(R)))
show(
    "data hash of the row passed to uses_y: same with and without the reference?",
    lambda: (
        [
            d.content_hash().to_string()
            for _, d in SelectDataColumns(["y"])(R).iter_data()
        ]
        == [
            d.content_hash().to_string()
            for _, d in SelectDataColumns(["y"])(
                src(
                    {
                        "session": s("s1", "s1", "s2"),
                        "x": i64(20, 10, 30),
                        "y": f64(0.2, 0.1, 0.3),
                    },
                    ["session", "x"],
                    "R",
                )
            ).iter_data()
        ]
    ),
)

print("\n=== P7. Index over a grouped referential list")
GR = GroupBy(["session"])(R)
show("Index('x', 0)(GroupBy(session)(R))", lambda: describe(Index("x", 0)(GR)))
show("Index('x', 0, out='x0')(...)", lambda: describe(Index("x", 0, out="x0")(GR)))
