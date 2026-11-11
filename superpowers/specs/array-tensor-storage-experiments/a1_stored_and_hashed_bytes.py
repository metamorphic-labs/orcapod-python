"""A1: what today's NumPy support stores and what it hashes.

Run: ``uv run python superpowers/specs/array-tensor-storage-experiments/a1_stored_and_hashed_bytes.py``

Claims tested:

* The stored value is exactly ``np.save(arr, allow_pickle=False)`` bytes, header included,
  in a ``large_binary`` column with extension name ``numpy.ndarray``.
* A datagram's content hash is starfix over a token column
  ``b"numpy:ndarray::sha256:" + sha256(np.save(np.load(stored)))``, so the hash commits
  to the whole ``.npy`` file, header included.
* Hash stability across processes, memory layout (C, Fortran, non-contiguous views),
  byte order, dtype width, ``.npy`` header version, 0-d, empty and large arrays.
"""

from __future__ import annotations

from typing import Any

import hashlib
import io
import json
import subprocess
import sys
import time
import tracemalloc

import numpy as np
import numpy.typing as npt
import pyarrow as pa
from starfix import ArrowDigester

from orcapod.contexts import get_default_context
from orcapod.core.datagrams.datagram import Datagram


def check(name, fn):
    try:
        status, observed = fn()
        print(f"[{status}] {name}: {observed}")
    except Exception as exc:  # noqa: BLE001  (experiment: report every failure)
        print(f"[ERROR] {name}: {type(exc).__name__}: {exc}")


def npy_bytes(arr: npt.NDArray[Any]) -> bytes:
    buf = io.BytesIO()
    np.save(buf, arr, allow_pickle=False)
    return buf.getvalue()


def datagram_hash(arr) -> str:
    return Datagram({"x": arr}).content_hash().to_hex()


def stored(arr) -> tuple[pa.Field[Any], bytes]:
    table = Datagram({"x": arr}).as_table()
    field = table.schema.field("x")
    col = table.column("x").combine_chunks()
    storage = col.storage if isinstance(col, pa.ExtensionArray) else col
    return field, storage[0].as_py()


ARR = np.arange(6, dtype=np.float64).reshape(2, 3)


def stored_is_npy():
    field, raw = stored(ARR)
    same = raw == npy_bytes(ARR)
    header_len = int.from_bytes(raw[8:10], "little")
    header = raw[10 : 10 + header_len].decode("latin1").strip()
    return (
        "OK" if same else "DEFECT",
        f"field type={field.type!r}; storage==np.save bytes: {same}; "
        f"magic={raw[:8]!r}; header={header!r}; total={len(raw)} bytes "
        f"for {ARR.nbytes} data bytes",
    )


def reproduce_hash():
    ctx = get_default_context()
    digest = hashlib.sha256(npy_bytes(ARR)).digest()
    token = b"numpy:ndarray::sha256:" + digest
    # The datagram field is non-nullable; nullability is part of the starfix hash.
    schema = pa.schema([pa.field("x", pa.large_binary(), nullable=False)])
    table = pa.table({"x": pa.array([token], type=pa.large_binary())}, schema=schema)
    manual = ArrowDigester.hash_table(table, include_metadata=False)
    via_hasher = ctx.arrow_hasher.hash_table(Datagram({"x": ARR}).as_table()).digest
    dg = Datagram({"x": ARR}).content_hash()
    ok = manual == via_hasher
    return (
        "OK" if ok else "DEFECT",
        "starfix over a non-null large_binary column holding "
        "b'numpy:ndarray::sha256:' + sha256(np.save(arr)) == arrow_hasher(datagram "
        f"table): {ok}; "
        f"datagram.content_hash method={dg.method!r}, "
        f"equals arrow_hasher digest: {dg.digest == via_hasher}",
    )


CHILD = """
import numpy as np
from orcapod.core.datagrams.datagram import Datagram
print(Datagram({"x": np.arange(6, dtype=np.float64).reshape(2, 3)}).content_hash().to_hex())
"""


def cross_process():
    outs = {
        subprocess.run(
            [sys.executable, "-c", CHILD], capture_output=True, text=True, check=True
        ).stdout.strip()
        for _ in range(2)
    }
    outs.add(datagram_hash(ARR))
    return (
        "OK" if len(outs) == 1 else "DEFECT",
        f"distinct hashes over 3 processes: {len(outs)}",
    )


def layout_c_vs_f():
    c = np.ascontiguousarray(ARR)
    f = np.asfortranarray(ARR)
    same = datagram_hash(c) == datagram_hash(f)
    back = stored(f)[1]
    rt = np.load(io.BytesIO(back))
    return (
        "INFO",
        f"equal values, C vs Fortran order -> same hash: {same}; "
        f"Fortran input stored with fortran_order=True: {b'True' in back[:128]}; "
        f"read-back flags F_CONTIGUOUS={rt.flags.f_contiguous}",
    )


def noncontiguous():
    big = np.arange(24, dtype=np.float64).reshape(4, 6)
    view = big[::2, 1:4]  # neither C nor F contiguous
    copy = view.copy()
    t_view = big.T  # F-contiguous view of a C array
    t_copy = np.ascontiguousarray(big.T)
    return (
        "INFO",
        f"strided view vs C copy same hash: {datagram_hash(view) == datagram_hash(copy)}; "
        f"transposed view (F-contig) vs C copy same hash: "
        f"{datagram_hash(t_view) == datagram_hash(t_copy)}",
    )


def byte_order():
    le = np.array([1.0, 2.0], dtype="<f4")
    be = np.array([1.0, 2.0], dtype=">f4")
    rt = np.load(io.BytesIO(stored(be)[1]))
    return (
        "INFO",
        f"<f4 vs >f4 equal values same hash: {datagram_hash(le) == datagram_hash(be)}; "
        f"big-endian read back as dtype {rt.dtype.str!r}",
    )


def dtype_width():
    return (
        "INFO",
        "int32 vs int64 [1,2,3] same hash: "
        f"{datagram_hash(np.array([1, 2, 3], np.int32)) == datagram_hash(np.array([1, 2, 3], np.int64))}",
    )


def header_version():
    # An array that arrives from a v2.0 .npy file is re-encoded by np.save on store,
    # so the stored and hashed header is v1.0 regardless of the source file.
    buf = io.BytesIO()
    np.lib.format.write_array(buf, ARR, version=(2, 0))
    from_v2 = np.load(io.BytesIO(buf.getvalue()))
    same = datagram_hash(from_v2) == datagram_hash(ARR)
    # np.save itself picks v3.0 for non-latin1 field names.
    s = np.zeros(2, dtype=[("é中", "<i4")])
    raw = npy_bytes(s)
    return (
        "INFO",
        f"array loaded from a v2.0 file hashes as from v1.0: {same}; "
        f"np.save version for non-latin1 field name: {raw[6]}.{raw[7]}; "
        f"for ARR: {npy_bytes(ARR)[6]}.{npy_bytes(ARR)[7]}",
    )


def zero_dim_and_empty():
    out = []
    for arr in (
        np.array(3.5),
        np.zeros((0,), np.float64),
        np.zeros((0, 3), np.float64),
        np.zeros((3, 0), np.float64),
    ):
        rt = np.load(io.BytesIO(stored(arr)[1]))
        out.append(
            f"shape {arr.shape}: rt shape {rt.shape}, hash {datagram_hash(arr)[:12]}"
        )
    distinct = len({datagram_hash(np.zeros(s)) for s in [(0,), (0, 3), (3, 0)]})
    return (
        "INFO",
        "; ".join(out) + f"; distinct hashes for empty shapes: {distinct}/3",
    )


def large():
    arr = np.ones((32 * 1024 * 1024,), dtype=np.float32)  # 128 MiB
    tracemalloc.start()
    t0 = time.perf_counter()
    h = datagram_hash(arr)
    dt = time.perf_counter() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return (
        "INFO",
        f"128 MiB float32: datagram content hash in {dt:.2f}s, traced peak "
        f"{peak / arr.nbytes:.1f}x the array size (hash {h[:12]})",
    )


def handler_direct():
    ctx = get_default_context()
    h = ctx.semantic_hasher.hash_object(ARR)
    return (
        "INFO",
        f"semantic_hasher.hash_object(ndarray) method={h.method!r}, digest == "
        f"sha256(.npy): {h.digest == hashlib.sha256(npy_bytes(ARR)).digest()}",
    )


if __name__ == "__main__":
    print(f"numpy {np.__version__}, pyarrow {pa.__version__}")
    print(json.dumps({"datagram_hash_ARR": datagram_hash(ARR)}))
    check("stored bytes", stored_is_npy)
    check("hashed bytes", reproduce_hash)
    check("semantic hasher on a bare ndarray", handler_direct)
    check("cross-process", cross_process)
    check("C vs Fortran", layout_c_vs_f)
    check("non-contiguous", noncontiguous)
    check("byte order", byte_order)
    check("dtype width", dtype_width)
    check(".npy header version", header_version)
    check("0-d and empty", zero_dim_and_empty)
    check("large array", large)
