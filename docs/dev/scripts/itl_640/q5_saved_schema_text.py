"""ITL-640 Q5: does the saved schema text of a job rebuild the same Arrow type?

Run: ``uv run python docs/dev/scripts/itl_640/q5_saved_schema_text.py``

Each Arrow type is written with the saved-job writer (``_serialize_arrow_type``) and
read with the reader (``parse_arrow_type_string``); the two are compared with
``StarfixArrowHasher.hash_schema``. Top-level nullability is saved apart, as the
``nullable`` record of a saved schema pair, so it is not part of this text.
"""

from __future__ import annotations

import uuid
import warnings
from pathlib import Path

import pyarrow as pa

from orcapod.contexts import get_default_context
from orcapod.pipeline.serialization import (
    _serialize_arrow_type,
    parse_arrow_type_string,
)

warnings.simplefilter("ignore")
ctx = get_default_context()
conv = ctx.type_converter


def lt(py_type):
    ctx.semantic_hasher.hash_object(py_type)
    return conv.python_type_to_arrow_type(py_type)


TYPES = {
    "int8": pa.int8(),
    "uint64": pa.uint64(),
    "float32": pa.float32(),
    "ts[s]": pa.timestamp("s"),
    "ts[ns]": pa.timestamp("ns"),
    "ts[us, UTC]": pa.timestamp("us", tz="UTC"),
    "ts[ms, Europe/Berlin]": pa.timestamp("ms", tz="Europe/Berlin"),
    "date32": pa.date32(),
    "date64": pa.date64(),
    "decimal128(10,2)": pa.decimal128(10, 2),
    "decimal256(50,4)": pa.decimal256(50, 4),
    "large_list<int8 not null>": pa.large_list(
        pa.field("item", pa.int8(), nullable=False)
    ),
    "large_list<large_list<int64 not null>>": pa.large_list(
        pa.large_list(pa.field("item", pa.int64(), nullable=False))
    ),
    "fixed_size_list<int64>[2]": pa.list_(pa.int64(), 2),
    "struct<a: int32 not null, b: large_list<ts[ns]>>": pa.struct(
        [
            pa.field("a", pa.int32(), nullable=False),
            pa.field("b", pa.large_list(pa.timestamp("ns"))),
        ]
    ),
    "dictionary<int32, string>": pa.dictionary(pa.int32(), pa.string()),
    "map<string, int64>": pa.map_(pa.string(), pa.int64()),
    "Path": lt(Path),
    "uuid.UUID": lt(uuid.UUID),
    "list[Path]": lt(list[Path]),
    "set[int]": lt(set[int]),
    "dict[str, int]": lt(dict[str, int]),
}

for label, arrow_type in TYPES.items():
    text = _serialize_arrow_type(arrow_type)
    try:
        back = parse_arrow_type_string(text)
    except Exception as exc:  # noqa: BLE001
        print(
            f"{label:52} text {text!r}: READ FAILED {type(exc).__name__}: {exc}"[:220]
        )
        continue
    same = ctx.arrow_hasher.hash_schema(
        pa.schema([pa.field("x", arrow_type)])
    ) == ctx.arrow_hasher.hash_schema(pa.schema([pa.field("x", back)]))
    print(
        f"{label:52} {'same' if same else 'DIFFERS -> ' + str(back)}   text {text!r}"[
            :240
        ]
    )
