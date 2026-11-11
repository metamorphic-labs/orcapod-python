"""C2: the array hash's method label, and what extension metadata does to hashes.

Run: ``uv run python superpowers/specs/array-tensor-storage-experiments/c2_hash_labels_and_extension_metadata.py``

Claims tested:

* The array value token carries the method label ``sha256`` with no version, so a later
  change of what is digested cannot be told apart from today's by the token alone.
* ``StarfixArrowHasher`` strips every field metadata key except ``ARROW:extension:*`` and
  then hashes with ``include_metadata=True``. So the extension name and extension metadata
  (where a design would put dtype or shape) enter the table hash and the Arrow schema hash.
  Other field metadata does not.
* The pipeline hash of a stream depends on Python types only, so Arrow extension metadata
  does not reach it (contrast with b1).
"""

from __future__ import annotations

import json
from typing import Any, cast

import numpy as np
import pyarrow as pa

from orcapod.contexts import get_default_context
from orcapod.core.datagrams.datagram import Datagram
from orcapod.hashing.arrow_hashers import StarfixArrowHasher
from orcapod.sources import ArrowTableSource


def check(name: str, fn: Any) -> None:
    try:
        status, observed = fn()
        print(f"[{status}] {name}: {observed}")
    except Exception as exc:  # noqa: BLE001  (experiment: report every failure)
        print(f"[ERROR] {name}: {type(exc).__name__}: {exc}")


CTX = get_default_context()
assert isinstance(CTX.arrow_hasher, StarfixArrowHasher)
HASHER = cast(StarfixArrowHasher, CTX.arrow_hasher)


def token_label() -> tuple[str, str]:
    arr = np.arange(3.0)
    h = CTX.semantic_hasher.hash_object(arr)
    table = Datagram({"x": arr}).as_table()
    processed = HASHER._process_table_columns(table)  # pyright: ignore[reportPrivateUsage]  # experiment inspects the hashed column
    token = processed.column("x")[0].as_py()
    prefix = token[: token.index(b"sha256:") + len(b"sha256:")]
    return (
        "INFO",
        f"semantic hash method={h.method!r}; hashed token prefix={prefix!r} "
        "(no version in either)",
    )


def table_with(meta: dict[bytes, bytes]) -> pa.Table:
    field = pa.field("x", pa.large_binary(), nullable=False, metadata=meta)
    return pa.table(
        {"x": pa.array([b"abc"], pa.large_binary())}, schema=pa.schema([field])
    )


def metadata_in_table_hash() -> tuple[str, str]:
    base = {b"ARROW:extension:name": b"demo.ndarray"}
    meta_a = {
        **base,
        b"ARROW:extension:metadata": json.dumps({"shape": [2, 3]}).encode(),
    }
    meta_b = {
        **base,
        b"ARROW:extension:metadata": json.dumps({"shape": [3, 2]}).encode(),
    }
    other = {**meta_a, b"comment": b"anything"}
    h_a = HASHER.hash_table(table_with(meta_a))
    h_b = HASHER.hash_table(table_with(meta_b))
    h_other = HASHER.hash_table(table_with(other))
    h_none = HASHER.hash_table(table_with({}))
    return (
        "INFO",
        f"same storage values: ext metadata shape [2,3] vs [3,2] -> same table hash: "
        f"{h_a == h_b}; extra non-extension key -> same as without: {h_a == h_other}; "
        f"no extension metadata -> same as with: {h_a == h_none}",
    )


def metadata_in_schema_hash() -> tuple[str, str]:
    base = {b"ARROW:extension:name": b"demo.ndarray"}
    s_a = table_with({**base, b"ARROW:extension:metadata": b'{"dtype":"<f4"}'}).schema
    s_b = table_with({**base, b"ARROW:extension:metadata": b'{"dtype":"<f8"}'}).schema
    return (
        "INFO",
        f"Arrow schema hash differs for dtype <f4 vs <f8 in extension metadata: "
        f"{HASHER.hash_schema(s_a) != HASHER.hash_schema(s_b)}",
    )


def pipeline_hash_uses_python_types() -> tuple[str, str]:
    base = {b"ARROW:extension:name": b"demo.ndarray"}

    def source(meta: bytes) -> ArrowTableSource:
        field = pa.field(
            "x", pa.large_binary(), metadata={**base, b"ARROW:extension:metadata": meta}
        )
        schema = pa.schema([pa.field("k", pa.large_string()), field])
        table = pa.table(
            {"k": ["a"], "x": pa.array([b"abc"], pa.large_binary())}, schema=schema
        )
        return ArrowTableSource(table, tag_columns=["k"])

    a = source(b'{"dtype":"<f4"}')
    b = source(b'{"dtype":"<f8"}')
    return (
        "INFO",
        f"two sources differing only in extension metadata: same output_schema "
        f"{a.output_schema() == b.output_schema()}; same pipeline_hash "
        f"{a.pipeline_hash() == b.pipeline_hash()}",
    )


if __name__ == "__main__":
    check("array hash method label", token_label)
    check("extension metadata in the table hash", metadata_in_table_hash)
    check("extension metadata in the Arrow schema hash", metadata_in_schema_hash)
    check("extension metadata and the pipeline hash", pipeline_hash_uses_python_types)
