"""ITL-640 Q3 (iii): is every operator's Arrow schema the same in zero-row form
(``typed_zero_row_table``, ITL-912) as in its populated result, and what does the
Python prediction say about list item nullability (ITL-1136)?

Run: ``uv run python docs/dev/scripts/itl_640/q3iii_operators.py``

The async path is not measured here: ``tests/test_core/operators/test_async_sync_equivalence.py``
asserts, for every operator, that the async rows have the sync rows' Arrow types,
nullability (item level included) and column order, over int8, float32, string,
list and large_list with nullable and non-null items, a struct and a logical type.

The inputs have two tags (``k`` str, ``g`` str) and two data columns: ``x`` int8 with a
null, ``y`` large_list<int8 not null>.
"""

from __future__ import annotations

import warnings

import pyarrow as pa

from orcapod.contexts import get_default_context
from orcapod.core.operators import (
    AddTags,
    Batch,
    DropNulls,
    GroupBy,
    Join,
    MapData,
    MergeJoin,
    PolarsFilter,
    SelectDataColumns,
    SemiJoin,
)
from orcapod.core.streams.base import typed_zero_row_table
from orcapod.sources import ArrowTableSource

warnings.simplefilter("ignore")
ctx = get_default_context()
NN_ITEMS = pa.large_list(pa.field("item", pa.int8(), nullable=False))


def source(source_id: str, other: str = "z") -> ArrowTableSource:
    table = pa.Table.from_arrays(
        [
            pa.array(["r1", "r2", "r3"], pa.large_string()),
            pa.array(["g1", "g1", "g2"], pa.large_string()),
            pa.array([1, None, 3], pa.int8()),
            pa.array([[1], [2, 3], []], NN_ITEMS),
            pa.array([1.5, 2.5, 3.5], pa.float32()),
        ],
        schema=pa.schema(
            [
                pa.field("k", pa.large_string(), nullable=False),
                pa.field("g", pa.large_string(), nullable=False),
                pa.field("x", pa.int8()),
                pa.field("y", NN_ITEMS, nullable=False),
                pa.field(other, pa.float32(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(
        table, tag_columns=["k", "g"], tag_only_columns=True, source_id=source_id
    )


def user(stream, schema: pa.Schema) -> pa.Schema:
    tags, data = stream.keys()
    return pa.schema([schema.field(n) for n in dict.fromkeys([*tags, *data])])


def text(schema: pa.Schema) -> str:
    return "; ".join(f"{f.name}: {f.type}{'' if f.nullable else ' nn'}" for f in schema)


CASES = {
    "Batch()": lambda: Batch()(source("a")),
    "GroupBy('g')": lambda: GroupBy("g")(source("a")),
    "MergeJoin(a, b) over a null in x": lambda: MergeJoin()(source("a"), source("b")),
    "MergeJoin(a, b) on y only": lambda: MergeJoin()(
        SelectDataColumns(["y", "z"])(source("a")),
        SelectDataColumns(["y"])(source("b", "w")),
    ),
    "Join(a, b')": lambda: Join()(
        source("a"), source("b", "w").map_data({"x": "x2", "y": "y2"})
    ),
    "SemiJoin(a, b)": lambda: SemiJoin()(source("a"), source("b")),
    "SelectDataColumns(x, y)": lambda: SelectDataColumns(["y", "x"])(source("a")),
    "MapData": lambda: MapData({"x": "x1", "y": "y1"})(source("a")),
    "PolarsFilter": lambda: PolarsFilter(constraints={"g": "g1"})(source("a")),
    "DropNulls(x)": lambda: DropNulls(["x"])(source("a")),
    "AddTags": lambda: AddTags({"cohort": "A", "run": 3})(source("a")),
}

for label, make in CASES.items():
    try:
        stream = make()
        populated = user(stream, stream.as_table().schema)
        zero = typed_zero_row_table(stream)
        zero_user = None if zero is None else user(stream, zero.schema)
        equal = zero_user is not None and ctx.arrow_hasher.hash_schema(
            populated
        ) == ctx.arrow_hasher.hash_schema(zero_user)
        print(f"== {label}: zero-row == populated: {equal}")
        print(f"    populated: {text(populated)}")
        if not equal:
            print(f"    zero-row : {None if zero_user is None else text(zero_user)}")
        print(f"    Python   : {stream.output_schema()}")
        converted = ctx.type_converter.python_schema_to_arrow_schema(
            stream.output_schema()[0] + stream.output_schema()[1]
        )
        print(
            "    converter(Python) == populated:",
            ctx.arrow_hasher.hash_schema(user(stream, converted))
            == ctx.arrow_hasher.hash_schema(populated),
        )
    except Exception as exc:  # noqa: BLE001
        print(f"== {label}: FAILED {type(exc).__name__}: {exc}")
