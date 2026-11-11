"""C1: prototype of a layout-independent canonical array digest (not Orcapod code).

Run: ``uv run python superpowers/specs/array-tensor-storage-experiments/c1_canonical_digest_prototype.py``

Today's hash is SHA-256 of the ``.npy`` file (see a1), so it depends on memory layout and
byte order. This prototype hashes the logical value instead:

    sha256( canonical_dtype_string + shape + C-order little-endian buffer )

Claims tested:

* The canonical digest is equal for C/Fortran order, strided views and big/little endian
  copies of the same values, and differs for different dtype, shape or values.
* Its cost on a 128 MiB array relative to today's ``.npy`` digest.
* It does not depend on NumPy: the same bytes can be produced from any buffer that exposes
  dtype, shape and C-order little-endian data (the torch side is checked in the b* scripts).
"""

from __future__ import annotations

from typing import Any

import hashlib
import io
import json
import time

import numpy as np
import numpy.typing as npt


def canonical_dtype(dt: np.dtype[Any]) -> str:
    """A byte-order-free dtype name; structured dtypes are not handled by this prototype."""
    if dt.fields is not None:
        raise TypeError("structured dtypes are out of scope for this prototype")
    return dt.newbyteorder("<").str.lstrip("<|=")  # e.g. 'f4', 'i8', 'b1', 'M8[ns]'


def canonical_digest(arr: npt.NDArray[Any]) -> str:
    le = arr.astype(arr.dtype.newbyteorder("<"), copy=False)
    buf = np.ascontiguousarray(le)  # copies only when not already C-contiguous
    header = json.dumps(
        {"dtype": canonical_dtype(arr.dtype), "shape": list(arr.shape)},
        separators=(",", ":"),
    ).encode()
    h = hashlib.sha256()
    h.update(len(header).to_bytes(4, "little"))
    h.update(header)
    if buf.size:  # hashlib reads any C-contiguous buffer; skip empty ones
        h.update(buf)  # pyright: ignore[reportArgumentType]  # ndarray implements the buffer protocol; numpy's stubs do not declare it
    return h.hexdigest()


def npy_digest(arr: npt.NDArray[Any]) -> str:
    b = io.BytesIO()
    np.save(b, arr, allow_pickle=False)
    return hashlib.sha256(b.getvalue()).hexdigest()


def report(name, ok, detail=""):
    print(f"[{'OK' if ok else 'FAIL'}] {name}{': ' + detail if detail else ''}")


if __name__ == "__main__":
    a = np.arange(24, dtype=np.float32).reshape(4, 6)
    base = np.zeros((4, 12), dtype=np.float32)
    base[:, ::2] = a
    strided = base[:, ::2]  # equal values, non-contiguous view
    variants = {
        "C": a,
        "Fortran": np.asfortranarray(a),
        "strided view": strided,
        "big-endian": a.astype(">f4"),
        "transposed twice": a.T.T,
    }
    canon = {k: canonical_digest(v) for k, v in variants.items()}
    npy = {k: npy_digest(v) for k, v in variants.items()}
    report(
        "canonical digest equal across layout and byte order",
        len(set(canon.values())) == 1,
        f"{len(set(canon.values()))} distinct over {len(canon)} variants",
    )
    report(
        "today's .npy digest over the same variants (for contrast)",
        True,
        f"{len(set(npy.values()))} distinct over {len(npy)} variants",
    )
    report(
        "differs for dtype",
        canonical_digest(a) != canonical_digest(a.astype(np.float64)),
    )
    report(
        "differs for shape", canonical_digest(a) != canonical_digest(a.reshape(6, 4))
    )
    report(
        "differs for empty shapes (0,3) vs (3,0)",
        canonical_digest(np.zeros((0, 3))) != canonical_digest(np.zeros((3, 0))),
    )
    big = np.ones(32 * 1024 * 1024, dtype=np.float32)
    for name, fn, arr in (
        ("canonical, C input", canonical_digest, big),
        (".npy today, C input", npy_digest, big),
        ("canonical, big-endian input", canonical_digest, big.astype(">f4")),
    ):
        t0 = time.perf_counter()
        fn(arr)
        print(f"[INFO] {name} on 128 MiB: {time.perf_counter() - t0:.3f}s")
