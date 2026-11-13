"""ITL-1004 audit, measurement 04: which identities would see a referential binding.

Uses the prototype of 02 (one physical column ``x`` listed as tag and data, set through
private attributes after construction). Nothing under ``src/`` changes.

Run: uv run python superpowers/specs/itl-1004-audit/04_identity.py
Output: 04_identity.out
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

from _common import f64, i64, s, show, src

from orcapod.contexts import resolve_context
from orcapod.types import Schema

_spec = importlib.util.spec_from_file_location(
    "proto", Path(__file__).with_name("02_prototype_unified.py")
)
make_ref = None  # loaded lazily below so 02's prints do not run here


def _make_ref():
    import io
    import contextlib

    assert _spec is not None and _spec.loader is not None
    mod = importlib.util.module_from_spec(_spec)
    with contextlib.redirect_stdout(io.StringIO()):
        _spec.loader.exec_module(mod)
    return mod.make_ref


make_ref = _make_ref()
hasher = resolve_context(None).semantic_hasher


def cols():
    return {"session": s("s1", "s2"), "x": i64(10, 20), "y": f64(0.1, 0.2)}


TAGGED = src(cols(), ["session", "x"], "S")  # v0.3: x materialized tag only
UNTAGGED = src(cols(), ["session"], "S")  # v0.3: x data only
REF = make_ref(src(cols(), ["session", "x"], "S"), ["x"])  # prototype: x tag and data

print("=== I1. pipeline_hash of a root = hash((tag_schema, data_schema))")
for name, st in [
    ("x tag only", TAGGED),
    ("x data only", UNTAGGED),
    ("x referential", REF),
]:
    show(
        f"{name}: output_schema / pipeline_hash",
        lambda st=st: f"{st.output_schema()}\n{st.pipeline_hash().to_hex(16)}",
    )

print(
    "\n=== I2. Schema-pair hash: the three shapes are distinct, no extra identity term"
)
pairs = {
    "tag{x} data{y}": (Schema({"session": str, "x": int}), Schema({"y": float})),
    "tag{} data{x,y}": (Schema({"session": str}), Schema({"x": int, "y": float})),
    "tag{x} data{x,y}": (
        Schema({"session": str, "x": int}),
        Schema({"x": int, "y": float}),
    ),
}
show(
    "hash_object((tag_schema, data_schema))",
    lambda: "\n".join(
        f"{k}: {hasher.hash_object(v).to_hex(16)}" for k, v in pairs.items()
    ),
)

print(
    "\n=== I3. Root content_hash = (class, output_schema, source_id): sees the binding"
)
show(
    "content hashes (same source_id 'S')",
    lambda: "\n".join(
        f"{n}: {st.content_hash().to_hex(16)}"
        for n, st in [
            ("tag only", TAGGED),
            ("data only", UNTAGGED),
            ("referential", REF),
        ]
    ),
)

print("\n=== I4. Function-result identity: the Data hash a function would receive")
show(
    "per-row Data content hash, x untagged vs x referential (data columns x, y)",
    lambda: (
        [d.content_hash().to_hex(16) for _, d in UNTAGGED.iter_data()]
        == [d.content_hash().to_hex(16) for _, d in REF.iter_data()]
    ),
)
show(
    "per-row Tag content hash, x materialized vs x referential (tag columns session, x)",
    lambda: (
        [t.content_hash().to_hex(16) for t, _ in TAGGED.iter_data()]
        == [t.content_hash().to_hex(16) for t, _ in REF.iter_data()]
    ),
)

print(
    "\n=== I5. Record-id preimage input = tag.as_table(system_tags) + input data hash"
)
show(
    "tag table of row 0, x materialized vs x referential: same columns?",
    lambda: (
        next(iter(TAGGED.iter_data()))[0]
        .as_table(columns={"system_tags": True})
        .column_names,
        next(iter(REF.iter_data()))[0]
        .as_table(columns={"system_tags": True})
        .column_names,
    ),
)
show(
    "system tag column names (embed the source schema hash, computed at build)",
    lambda: (TAGGED.keys(all_info=True)[0][2:], REF.keys(all_info=True)[0][2:]),
)
_ = i64
