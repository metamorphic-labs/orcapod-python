"""ITL-640 Q1/Q2: does ``StarfixArrowHasher.hash_schema`` cover a logical type's whole
identifier, and does it ignore field metadata?

Run: ``uv run python docs/dev/scripts/itl_640/q1_q2_logical_types_and_metadata.py``

Q1. For every logical type a schema can hold, the field's Arrow type (from the data
context's converter) is hashed, and then the same field with only the extension
METADATA changed (name kept) and only the extension NAME changed (metadata kept): both
must hash differently from the original.

Q2. Field metadata and schema metadata that are not ``ARROW:extension:*`` are ignored:
this script shows it, so that nothing identity-bearing may be carried only that way.
"""

from __future__ import annotations

import dataclasses
import uuid
import warnings
from pathlib import Path

import numpy as np
import pyarrow as pa

import orcapod as op
from orcapod.contexts import get_default_context
from orcapod.logical_types.numpy_type import LogicalNumpyArray
from orcapod.utils.arrow_utils import normalize_extension_columns

warnings.simplefilter("ignore")

ctx = get_default_context()
conv = ctx.type_converter
hasher = ctx.arrow_hasher


@dataclasses.dataclass
class Exact:
    a: int
    b: str


@op.dataclass
class Structural:
    a: int
    b: str


@op.dataclass(name="Structural")
class StructuralOtherFields:
    a: int
    b: float


TYPES: dict[str, object] = {
    "Path": Path,
    "uuid.UUID": uuid.UUID,
    "File": op.File if hasattr(op, "File") else None,
    "np.ndarray": np.ndarray,
    "Exact (dataclass)": Exact,
    "Structural (orcapod.dataclass)": Structural,
    "list[Path]": list[Path],
    "set[int]": set[int],
    "set[Path]": set[Path],
    "list[Exact]": list[Exact],
}


def h(schema: pa.Schema) -> str:
    return hasher.hash_schema(schema).to_hex()[:16]


def ipc_field(field: pa.Field) -> pa.Field:
    """The field in IPC form: storage type plus ``ARROW:extension:*`` metadata."""
    return normalize_extension_columns(pa.schema([field]).empty_table()).schema.field(0)


def with_ext(field: pa.Field, *, name: bytes | None = None, meta: bytes | None = None):
    md = dict(field.metadata or {})
    if name is not None:
        md[b"ARROW:extension:name"] = name
    if meta is not None:
        md[b"ARROW:extension:metadata"] = meta
    return field.with_metadata(md)


print("Q1: logical types")
print(f"{'type':34} {'arrow type':48} {'hash':16}  meta-changed  name-changed")
for label, py_type in TYPES.items():
    if py_type is None:
        continue
    try:
        # Hashing a type registers a class a factory covers (a dataclass), as a
        # source or a function pod does before it converts the type.
        ctx.semantic_hasher.hash_object(py_type)
        arrow_type = conv.python_type_to_arrow_type(py_type)
    except Exception as exc:  # noqa: BLE001
        print(f"{label:34} NOT CONVERTIBLE: {type(exc).__name__}: {exc}")
        continue
    field = pa.field("x", arrow_type)
    base = h(pa.schema([field]))
    # Rebuild the field in IPC form, then change the extension metadata or name only.
    ipc = ipc_field(field)
    md = dict(ipc.metadata or {})
    if b"ARROW:extension:name" in md:
        same_ipc = h(pa.schema([ipc]))
        changed_meta = h(
            pa.schema(
                [with_ext(ipc, meta=md.get(b"ARROW:extension:metadata", b"") + b" ")]
            )
        )
        changed_name = h(
            pa.schema([with_ext(ipc, name=md[b"ARROW:extension:name"] + b"x")])
        )
        verdict = (
            f"{'DIFFERS' if changed_meta != base else 'SAME!!'}       "
            f"{'DIFFERS' if changed_name != base else 'SAME!!'}"
            f"   (ipc form hashes {'equal' if same_ipc == base else 'DIFFERENTLY'})"
        )
    else:
        verdict = "(no extension at top level)"
    print(f"{label:34} {str(arrow_type)[:48]:48} {base}  {verdict}")

print()
print("Q1b: two numpy ignore lists (two logical types, one extension name)")
t_strict = LogicalNumpyArray().get_arrow_extension_type()
t_ignore = LogicalNumpyArray(ignore=["memory_order"]).get_arrow_extension_type()
print("  strict :", t_strict, t_strict.__arrow_ext_serialize__())
print("  ignore :", t_ignore, t_ignore.__arrow_ext_serialize__())
try:
    print(
        "  hash_schema equal:",
        h(pa.schema([pa.field("x", t_strict)]))
        == h(pa.schema([pa.field("x", t_ignore)])),
    )
except Exception as exc:  # noqa: BLE001
    # A data context reads only columns stored under its own list (CLAUDE.md).
    print(f"  hash_schema of the other list raises {type(exc).__name__}")

print()
print("Q1c: structural classes, one name, other fields (identity records differ)")
ctx.semantic_hasher.hash_object(Structural)
ctx.semantic_hasher.hash_object(StructuralOtherFields)
a = conv.python_type_to_arrow_type(Structural)
b = conv.python_type_to_arrow_type(StructuralOtherFields)
print("  A:", a)
print("  B:", b)
print(
    "  hash_schema equal:",
    h(pa.schema([pa.field("x", a)])) == h(pa.schema([pa.field("x", b)])),
)

print()
print("Q1d: a logical type against its storage type")
for label, py_type in (
    ("Path", Path),
    ("uuid.UUID", uuid.UUID),
    ("set[int]", set[int]),
):
    t = conv.python_type_to_arrow_type(py_type)
    storage = t.storage_type if isinstance(t, pa.ExtensionType) else t
    print(
        f"  {label}: {t} vs storage {storage}: equal =",
        h(pa.schema([pa.field("x", t)])) == h(pa.schema([pa.field("x", storage)])),
    )

print()
print("Q2: field and schema metadata (not ARROW:extension:*)")
plain = pa.schema([pa.field("x", pa.int64())])
print(
    "  field metadata {'k': 'v'} ignored:",
    h(plain) == h(pa.schema([pa.field("x", pa.int64(), metadata={"k": "v"})])),
)
print(
    "  schema metadata {'pandas': ...} ignored:",
    h(plain) == h(plain.with_metadata({"pandas": "{}"})),
)
print(
    "  nested field metadata ignored:",
    h(pa.schema([pa.field("x", pa.large_list(pa.field("item", pa.int64())))]))
    == h(
        pa.schema(
            [
                pa.field(
                    "x",
                    pa.large_list(pa.field("item", pa.int64(), metadata={"k": "v"})),
                )
            ]
        )
    ),
)
