"""B4: which dtypes and layouts can each candidate represent, and is anything changed?

Run:
    uv run python superpowers/specs/array-tensor-storage-experiments/b4_dtype_strictness.py

For each NumPy dtype / layout case and each candidate, the array is encoded,
decoded *from the stored column alone* (dtype from the stored type or metadata,
not from the input), and compared with the input on dtype (including byte
order), shape, values and memory order. Results:

* ``exact``: dtype, shape and values identical (memory order reported separately).
* ``CHANGED``: values equal but dtype or shape differ (an implicit conversion).
* ``rejected``: the encoder raised (type cannot be represented).

The flattened candidates in ``b0_common`` store little-endian C-order values
(a design choice); this script reports where that loses the original byte
order. ``fst`` is pyarrow's own ``FixedShapeTensorArray.from_numpy_ndarray`` /
``to_numpy_ndarray``.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import numpy.typing as npt
import pyarrow as pa
from b0_common import ENCODERS, checking, decode_stored, report  # pyright: ignore[reportImplicitRelativeImport]  # run as a script: sys.path[0] is this directory


def cases() -> dict[str, npt.NDArray[Any]]:
    base = np.arange(6).reshape(2, 3)
    return {
        "bool": (base % 2).astype(bool),
        "int8": base.astype(np.int8),
        "uint64": base.astype(np.uint64),
        "float16": base.astype(np.float16),
        "float32": base.astype(np.float32),
        "float64 >f8 (big-endian)": base.astype(">f8"),
        "complex64": (base + 1j).astype(np.complex64),
        "datetime64[ns]": base.astype("datetime64[ns]"),
        "timedelta64[s]": base.astype("timedelta64[s]"),
        "U5 (fixed unicode)": np.array(
            [["a", "bb", "c"], ["d", "e", "fffff"]], dtype="U5"
        ),
        "S3 (fixed bytes)": np.array(
            [[b"a", b"bb", b"c"], [b"d", b"e", b"f"]], dtype="S3"
        ),
        "structured [a<i4,b<f8]": np.zeros((2, 3), dtype=[("a", "<i4"), ("b", "<f8")]),
        "object": np.array([[1, "a", None]] * 2, dtype=object),
        "0-d float32": np.array(3.5, dtype=np.float32),
        "empty (0,3) float32": np.zeros((0, 3), dtype=np.float32),
        "Fortran-order float32": np.asfortranarray(base.astype(np.float32)),
        "non-contiguous view": np.arange(24, dtype=np.float32).reshape(4, 6)[::2, ::2],
    }


def run_case(name: str, label: str, a: npt.NDArray[Any]) -> None:
    try:
        col = ENCODERS[name]([a, a])
    except Exception as exc:  # noqa: BLE001
        report(
            "INFO",
            f"{name}: {label}",
            f"rejected: {type(exc).__name__}: {str(exc)[:120]}",
        )
        return
    with checking(f"{name}: {label}"):
        got = decode_stored(name, col)[0]
        same_dtype = got.dtype == a.dtype
        same_shape = got.shape == a.shape
        try:
            values = np.array_equal(got.astype(a.dtype), a)
        except (TypeError, ValueError):
            values = False
        order = "F" if got.flags.f_contiguous and not got.flags.c_contiguous else "C"
        in_order = "F" if a.flags.f_contiguous and not a.flags.c_contiguous else "C"
        exact = same_dtype and same_shape and values
        verdict = "exact" if exact else ("CHANGED" if values else "VALUES DIFFER")
        report(
            "OK" if exact else ("INFO" if values else "FAIL"),
            f"{name}: {label}",
            f"{verdict}; dtype {a.dtype.str}->{got.dtype.str}; shape {a.shape}->{got.shape}; "
            f"order {in_order}->{order}",
        )


def fst_byte_order() -> None:
    """FST takes no big-endian input; np.stack silently makes the batch native-endian."""
    a = np.arange(12, dtype=np.float32).reshape(2, 2, 3).astype(">f4")
    with checking("fst: from_numpy_ndarray(big-endian batch)"):
        pa.FixedShapeTensorArray.from_numpy_ndarray(a)
        report("INFO", "fst: from_numpy_ndarray(big-endian batch)", "accepted")
    stacked = np.stack([a[0], a[1]])
    report(
        "INFO",
        "numpy: np.stack of two >f4 arrays",
        f"dtype {a.dtype.str} -> {stacked.dtype.str} (stack converts byte order itself)",
    )


if __name__ == "__main__":
    for label, arr in cases().items():
        for cand in ENCODERS:
            run_case(cand, label, arr)
    fst_byte_order()
