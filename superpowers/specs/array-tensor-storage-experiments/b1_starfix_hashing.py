"""B1: can the starfix hasher hash each candidate's schema and values today?

Run:
    uv run python superpowers/specs/array-tensor-storage-experiments/b1_starfix_hashing.py

Claims tested, per candidate (see ``b0_common`` for the candidates):

* ``starfix.ArrowDigester.hash_schema`` / ``hash_table`` on the live extension type.
* The same on the storage form produced by Orcapod's
  ``normalize_extension_columns`` (storage type plus ``ARROW:extension:*`` field
  metadata), with ``include_metadata=True``.
* Orcapod's default-context ``arrow_hasher`` (``StarfixArrowHasher``) on the live table.
* Whether shape, dtype or memory layout enter the *schema* hash (which feeds
  ``pipeline_hash``) or only the value hash.
* ``FixedShapeTensorType`` is not a ``pa.ExtensionType`` subclass, so every
  ``isinstance(t, pa.ExtensionType)`` check in Orcapod misses it.
"""

from __future__ import annotations

from typing import Any, cast

import json

import numpy as np
import pyarrow as pa
from b0_common import ENCODERS, checking, fst_type, report, sample_table  # pyright: ignore[reportImplicitRelativeImport]  # run as a script: sys.path[0] is this directory
from starfix import ArrowDigester

from orcapod.contexts import get_default_context
from orcapod.hashing.arrow_hashers import StarfixArrowHasher
from orcapod.utils.arrow_utils import normalize_extension_columns

HASHER = cast(StarfixArrowHasher, get_default_context().arrow_hasher)


def _short(digest: object) -> str:
    raw = getattr(digest, "digest", digest)
    return raw.hex()[:16] if isinstance(raw, bytes | bytearray) else str(raw)[:16]


def starfix_checks(name: str) -> None:
    table = sample_table(name)
    ext_type = table.schema.field("x").type
    report(
        "INFO",
        f"{name}: isinstance(type, pa.ExtensionType)",
        isinstance(ext_type, pa.ExtensionType),
    )
    with checking(f"{name}: starfix hash_schema(live ext)"):
        report(
            "OK",
            f"{name}: starfix hash_schema(live ext)",
            _short(ArrowDigester.hash_schema(table.schema)),
        )
    with checking(f"{name}: starfix hash_table(live ext)"):
        report(
            "OK",
            f"{name}: starfix hash_table(live ext)",
            _short(ArrowDigester.hash_table(table)),
        )
    norm = normalize_extension_columns(table)
    report(
        "INFO",
        f"{name}: normalize_extension_columns field type",
        f"{norm.schema.field('x').type} meta={bool(norm.schema.field('x').metadata)}",
    )
    with checking(f"{name}: starfix hash_schema(storage+meta)"):
        report(
            "OK",
            f"{name}: starfix hash_schema(storage+meta)",
            _short(ArrowDigester.hash_schema(norm.schema, include_metadata=True)),
        )
    with checking(f"{name}: starfix hash_table(storage+meta)"):
        report(
            "OK",
            f"{name}: starfix hash_table(storage+meta)",
            _short(ArrowDigester.hash_table(norm, include_metadata=True)),
        )
    with checking(f"{name}: orcapod arrow_hasher.hash_schema"):
        report(
            "OK",
            f"{name}: orcapod arrow_hasher.hash_schema",
            _short(HASHER.hash_schema(table.schema)),
        )
    with checking(f"{name}: orcapod arrow_hasher.hash_table"):
        report(
            "OK",
            f"{name}: orcapod arrow_hasher.hash_table",
            _short(HASHER.hash_table(table)),
        )


def fst_storage_form(table: pa.Table) -> pa.Table:
    """Storage form of a table's ``arrow.fixed_shape_tensor`` columns, built by hand.

    ``normalize_extension_columns`` skips them (not a ``pa.ExtensionType``), so this
    emulates what IPC/Parquet write: storage type plus ``ARROW:extension:*`` metadata
    (JSON ``shape`` / ``permutation`` / ``dim_names`` as in the canonical spec).
    """
    cols, fields = [], []
    for field, col in zip(table.schema, table.columns, strict=True):
        t = field.type
        if (
            isinstance(t, pa.BaseExtensionType)
            and t.extension_name == "arrow.fixed_shape_tensor"
        ):
            t = fst_type(t)
            params: dict[str, list[Any]] = {"shape": list(t.shape)}
            if t.permutation is not None:
                params["permutation"] = list(t.permutation)
            if t.dim_names is not None:
                params["dim_names"] = list(t.dim_names)
            meta = {
                b"ARROW:extension:name": b"arrow.fixed_shape_tensor",
                b"ARROW:extension:metadata": json.dumps(params).encode(),
            }
            storage = [cast("pa.ExtensionArray[Any]", c).storage for c in col.chunks]
            cols.append(pa.chunked_array(storage, t.storage_type))  # pyright: ignore[reportCallIssue, reportArgumentType]  # pyarrow-stubs have no overload for a non-literal DataType
            fields.append(pa.field(field.name, t.storage_type, field.nullable, meta))
        else:
            cols.append(col)
            fields.append(field)
    return pa.Table.from_arrays(cols, schema=pa.schema(fields))


def storage_form(table: pa.Table) -> pa.Table:
    return fst_storage_form(normalize_extension_columns(table))


def _schema_hash(arr: pa.Array[Any]) -> str:
    """Arrow schema hash of a one-column table in its storage form."""
    t = storage_form(pa.table({"x": arr}))
    return _short(ArrowDigester.hash_schema(t.schema, include_metadata=True))


def schema_sensitivity() -> None:
    a23 = [np.zeros((2, 3), np.float32)]
    a32 = [np.zeros((3, 2), np.float32)]
    a23_64 = [np.zeros((2, 3), np.float64)]
    for name, enc in ENCODERS.items():
        with checking(f"{name}: schema hash shape (2,3) vs (3,2)"):
            h1, h2 = _schema_hash(enc(a23)), _schema_hash(enc(a32))
            report("INFO", f"{name}: schema hash differs by shape", h1 != h2)
        with checking(f"{name}: schema hash float32 vs float64"):
            h1, h2 = _schema_hash(enc(a23)), _schema_hash(enc(a23_64))
            report("INFO", f"{name}: schema hash differs by dtype", h1 != h2)


def fst_layout() -> None:
    """A permuted (transposed) batch and its C-order copy: same arrays, different FST."""
    stack = np.arange(12, dtype=np.float32).reshape(2, 3, 2).transpose(0, 2, 1)
    copy = np.ascontiguousarray(stack)  # same values, shape (2, 2, 3)
    with checking("fst: permuted batch vs C copy"):
        av = pa.FixedShapeTensorArray.from_numpy_ndarray(stack)
        ac = pa.FixedShapeTensorArray.from_numpy_ndarray(copy)
        report(
            "INFO",
            "fst: permuted vs C copy",
            f"permutation {fst_type(av.type).permutation} vs "
            f"{fst_type(ac.type).permutation}; "
            f"types equal={av.type == ac.type}; storage equal={av.storage.equals(ac.storage)}; "
            f"decoded equal={np.array_equal(av.to_numpy_ndarray(), ac.to_numpy_ndarray())}",
        )
    with checking("fst: Fortran-ordered single tensor"):
        f = np.asfortranarray(np.arange(6, dtype=np.float32).reshape(2, 3))
        pa.FixedShapeTensorArray.from_numpy_ndarray(f[np.newaxis])
        report("OK", "fst: Fortran-ordered single tensor", "accepted")
    with checking("fst: permutation None == [0,1]"):
        t1 = pa.fixed_shape_tensor(pa.float32(), [2, 3])
        t2 = pa.fixed_shape_tensor(pa.float32(), [2, 3], permutation=[0, 1])
        ipc1 = pa.schema([pa.field("x", t1)]).serialize().to_pybytes()
        ipc2 = pa.schema([pa.field("x", t2)]).serialize().to_pybytes()
        report(
            "INFO",
            "fst: permutation None vs [0,1] (from_numpy_ndarray always sets it)",
            f"type equal={t1 == t2}; IPC schema bytes equal={ipc1 == ipc2}; "
            f"so a stored schema / schema hash differs by construction path",
        )


def encode_fst_storage() -> pa.Array[Any]:
    return pa.array([list(range(6))], pa.list_(pa.float32(), 6))


def fst_storage_values() -> None:
    """ITL-848: starfix cannot hash fixed_size_list values even in storage form."""
    with checking("fst: starfix hash_table(storage+meta)"):
        t = storage_form(sample_table("fst"))
        report(
            "OK",
            "fst: starfix hash_table(storage+meta)",
            _short(ArrowDigester.hash_table(t, include_metadata=True)),
        )
    with checking("fst: starfix hash_schema(storage+meta)"):
        t = storage_form(sample_table("fst"))
        report(
            "OK",
            "fst: starfix hash_schema(storage+meta)",
            _short(ArrowDigester.hash_schema(t.schema, include_metadata=True)),
        )


def pipeline_schema_types() -> None:
    """pipeline_hash hashes Python types, not Arrow schemas: what does np.ndarray give?"""
    from numpy.typing import NDArray

    from orcapod.types import Schema

    sh = get_default_context().semantic_hasher
    for label, tp in [
        ("np.ndarray", np.ndarray),
        ("NDArray[np.float32]", NDArray[np.float32]),
    ]:
        with checking(f"semantic hash of Schema({{'x': {label}}})"):
            report(
                "INFO",
                f"semantic hash of Schema({{'x': {label}}})",
                sh.hash_object(Schema({"x": tp})).to_string()[:40],
            )


def npy_schema_blind() -> None:
    """With today's .npy format the schema never says dtype or shape."""
    lt = sample_table("npy").schema.field("x").type
    with checking("npy: schema carries dtype/shape"):
        norm = normalize_extension_columns(
            pa.table({"x": ENCODERS["npy"]([np.zeros(3, np.int8)])})
        )
        report(
            "INFO",
            "npy: storage-form field metadata (int8 vs float32 identical?)",
            norm.schema.field("x").metadata
            == normalize_extension_columns(sample_table("npy"))
            .schema.field("x")
            .metadata,
        )
        report("INFO", "npy: live type", lt)


def value_identity() -> None:
    """Do equal arrays in different byte order / memory layout hash the same?"""
    base = np.arange(6, dtype=np.float32).reshape(2, 3)
    wide = np.zeros((2, 6), dtype=np.float32)
    wide[:, ::2] = base
    view = wide[:, ::2]  # a strided, non-contiguous view equal to base
    assert not view.flags.c_contiguous and np.array_equal(view, base)
    variants = {
        "big-endian": base.astype(">f4"),
        "Fortran order": np.asfortranarray(base),
        "non-contiguous view": view,
    }
    for name, enc in ENCODERS.items():
        with checking(f"{name}: value hash identity"):
            ref = HASHER.hash_table(pa.table({"x": enc([base])}))
            parts = []
            for label, v in variants.items():
                h = HASHER.hash_table(pa.table({"x": enc([v])}))
                parts.append(f"{label}={'same' if h == ref else 'DIFFERENT'}")
            report("INFO", f"{name}: hash vs C-order little-endian", "; ".join(parts))


if __name__ == "__main__":
    for cand in ENCODERS:
        starfix_checks(cand)
    schema_sensitivity()
    fst_layout()
    fst_storage_values()
    pipeline_schema_types()
    value_identity()
    npy_schema_blind()
