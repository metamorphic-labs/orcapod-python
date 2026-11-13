"""ITL-1004 audit, measurement 01: what v0.3 code does today when a tag and a data column
share a name, anywhere it can be built through the public interface.

Run: uv run python superpowers/specs/itl-1004-audit/01_today_same_name.py
Output: 01_today_same_name.out
"""

from __future__ import annotations

import pyarrow as pa
from _common import async_rows, describe, f64, i64, s, show, src  # pyright: ignore[reportImplicitRelativeImport]  # run as a file from this folder

from orcapod import function_pod
from orcapod.core.streams import ArrowTableStream
from orcapod.operators import (
    Batch,
    GroupBy,
    Join,
    MapData,
    MapTags,
    MergeJoin,
    SemiJoin,
)
from orcapod.sources import DerivedSource

print("=== R1. A stream cannot name one column as both tag and data")
one = src({"x": i64(1, 2), "y": f64(0.1, 0.2)}, ["x"], "one")
show("ArrowTableSource(x, y; tag_columns=[x])", lambda: describe(one))

dup = pa.table([i64(1, 2), i64(1, 2), f64(0.1, 0.2)], names=["x", "x", "y"])
show(
    "ArrowTableStream over a table with two physical columns named x, tag_columns=[x]",
    lambda: describe(ArrowTableStream(dup, tag_columns=["x"])),
)

print("\n=== R2. Join: input A has TAG x, input B has DATA x (ITL-940 shape)")
a = src({"x": i64(1, 2), "a": f64(0.1, 0.2)}, ["x"], "A")
b = src({"k": i64(1, 2), "x": i64(1, 2), "b": f64(0.5, 0.6)}, ["k"], "B")
show("Join().output_schema(A, B)", lambda: Join().output_schema(a, b))
show("Join()(A, B) read", lambda: describe(Join()(a, b)))
show("Join async rows", lambda: async_rows(Join(), a, b))
show("MergeJoin()(A, B) read", lambda: describe(MergeJoin()(a, b)))
show("SemiJoin()(A, B) read", lambda: describe(SemiJoin()(a, b)))

print("\n=== R3. Join: both inputs have DATA x (the collision)")
c = src({"x": i64(1, 2), "u": i64(5, 6)}, ["u"], "C")
d = src({"x": i64(1, 2), "u": i64(5, 6), "v": f64(1.0, 2.0)}, ["u"], "D")
show("Join().output_schema(C, D)", lambda: Join().output_schema(c, d))
show("Join async rows", lambda: async_rows(Join(), c, d))
show(
    "MergeJoin()(C, D) read (colliding data -> list)",
    lambda: describe(MergeJoin()(c, d)),
)

print("\n=== R4. MapTags / MapData renaming onto a name of the other namespace")
e = src({"x": i64(1, 2), "y": f64(0.1, 0.2)}, ["x"], "E")
show("MapTags({x: y})(E)", lambda: describe(MapTags({"x": "y"})(e)))
show("MapData({y: x})(E)", lambda: describe(MapData({"y": "x"})(e)))


print("\n=== R5. Function pod output named like an input TAG")


@function_pod(output_keys="x")
def make_x(y: float | None) -> float:
    return (y or 0.0) * 2


show("make_x.pod.output_schema(E)", lambda: make_x.pod.output_schema(e))
show("make_x.pod(E) read", lambda: describe(make_x.pod(e)))

print("\n=== R6. DerivedSource(tags_as_data={x: x}) (same name)")
show(
    "DerivedSource(E, tags_as_data={'x': 'x'})",
    lambda: describe(DerivedSource(e, tags_as_data={"x": "x"})),
)
show(
    "DerivedSource(E, tags_as_data={'x': 'x_copy'})",
    lambda: describe(DerivedSource(e, tags_as_data={"x": "x_copy"})),
)

print("\n=== R7. GroupBy / Batch over the 'copy' shape (tag t_x, data x)")
g = src(
    {
        "t_unit": i64(1, 1, 2),
        "t_time": i64(30, 10, 20),
        "unit": i64(1, 1, 2),
        "time": i64(30, 10, 20),
        "amp": f64(0.3, 0.1, 0.2),
    },
    ["t_unit", "t_time"],
    "G",
)
show("GroupBy(by=[t_unit])(G)", lambda: describe(GroupBy(by=["t_unit"])(g)))
show("Batch(batch_size=2)(G)", lambda: describe(Batch(batch_size=2)(g)))

print("\n=== R8. Function pod with a tag name as a parameter (tags are not inputs)")


@function_pod(output_keys="z")
def uses_tag(x: int | None) -> int | None:
    return x


show("uses_tag.pod(E) (x is a tag of E)", lambda: describe(uses_tag.pod(e)))
_ = s
