"""ITL-1004 audit, measurement 06: consequences of "declared source tags are referential by
default" (developer, November 12, 2026), and today's materialized-with-materialized Join.

Uses the prototype of 02 for the referential side (private attributes set after
construction; nothing under ``src/`` changes).

Run: uv run python superpowers/specs/itl-1004-audit/06_default_referential.py
Output: 06_default_referential.out
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
from pathlib import Path
from typing import Any

import pyarrow as pa
from _common import async_rows, describe, f64, i64, s, show, src  # pyright: ignore[reportImplicitRelativeImport]  # run as a file from this folder

from orcapod import function_pod
from orcapod.config import DEFAULT_CONFIG
from orcapod.contexts import resolve_context
from orcapod.core.datagrams import Data
from orcapod.operators import Join
from orcapod.sources import ArrowTableSource, ListSource
from orcapod.types import Schema
from orcapod.utils.schema_utils import compute_source_schema_hash


def _load_make_ref() -> Any:
    spec = importlib.util.spec_from_file_location(
        "proto", Path(__file__).with_name("02_prototype_unified.py")
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    with contextlib.redirect_stdout(io.StringIO()):
        spec.loader.exec_module(mod)
    return mod.make_ref


make_ref = _load_make_ref()
ctx = resolve_context(None)

print("=== J1. Today: materialized with materialized Join (must stay byte-identical)")
A = src({"subject": s("m1", "m2"), "a": f64(0.1, 0.2)}, ["subject"], "A")
B = src({"subject": s("m1", "m2"), "b": f64(1.0, 2.0)}, ["subject"], "B")
show("Join()(A, B)", lambda: describe(Join()(A, B)))
show("Join()(A, B).pipeline_hash()", lambda: Join()(A, B).pipeline_hash().to_hex(16))
show(
    "system tag columns of Join()(A, B)",
    lambda: [c for c in Join()(A, B).keys(all_info=True)[0] if c.startswith("_tag")],
)
show(
    "sync rows == async rows",
    lambda: (
        sorted(
            (repr(t.as_dict()), repr(d.as_dict())) for t, d in Join()(A, B).iter_data()
        )
        == sorted((repr(t), repr(d)) for t, d in async_rows(Join(), A, B))
    ),
)

print(
    "\n=== H1. Source schema hash (names the system-tag columns) moves under the default"
)
tag = Schema({"subject": str, "x": int})
show(
    "compute_source_schema_hash: x materialized vs x referential",
    lambda: (
        compute_source_schema_hash(tag, Schema({"y": float}), ctx, DEFAULT_CONFIG),
        compute_source_schema_hash(
            tag, Schema({"x": int, "y": float}), ctx, DEFAULT_CONFIG
        ),
    ),
)


def cols() -> dict[str, Any]:
    return {"subject": s("m1", "m2"), "x": i64(10, 20), "y": f64(0.1, 0.2)}


V03 = src(cols(), ["subject", "x"], "S")  # explicit materialized = v0.3
REF = make_ref(src(cols(), ["subject", "x"], "S"), ["subject", "x"])  # default ref

print("\n=== H2. Function-result identity for a function that does not read the keys")
# The automatic select keeps only y; on the referential stream that is the row's Data
# without the referenced columns. Compare with the v0.3 row's Data (y only).
show(
    "Data hash after dropping referenced keys == v0.3 Data hash, per row",
    lambda: (
        [
            Data(d.as_table().drop_columns(["subject", "x"])).content_hash().to_hex(16)
            for _, d in REF.iter_data()
        ]
        == [d.content_hash().to_hex(16) for _, d in V03.iter_data()]
    ),
)
show(
    "the same, as values",
    lambda: [d.content_hash().to_hex(16) for _, d in V03.iter_data()],
)


@function_pod(output_keys="z")
def double_y(y: float | None) -> float:
    return (y or 0.0) * 2


show(
    "result table path (function uri) is independent of the input stream",
    lambda: double_y.pod.data_function.uri,
)

print("\n=== H3. A source whose columns are all tags (v0.3)")
show(
    "ArrowTableSource(subject, x; tag_columns=[subject, x])",
    lambda: describe(
        ArrowTableSource(
            pa.table({"subject": s("m1"), "x": i64(1)}),
            tag_columns=["subject", "x"],
            source_id="T",
        )
    ),
)

print("\n=== H4. ListSource: tags come from a tag function, not from columns")
show(
    "ListSource('weight', [20.5, 22.25])",
    lambda: describe(ListSource("weight", [20.5, 22.25])),
)
