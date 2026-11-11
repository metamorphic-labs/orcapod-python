"""A2: dtype coverage and failure modes of today's ``numpy.ndarray`` logical type.

Run: ``uv run python superpowers/specs/array-tensor-storage-experiments/a2_dtype_coverage.py``

For each dtype the script builds a ``DictSource`` with one array column, reads the value
back through ``iter_data`` and compares dtype, shape and buffer. It also computes the
datagram content hash. Then it checks structured field order (ITL-465), object dtype,
masked arrays, ``np.generic`` scalars, ndarray subclasses and bit-level float identity.
"""

from __future__ import annotations

from typing import Any

import warnings

import numpy as np

from orcapod.core.datagrams.datagram import Datagram
from orcapod.sources import DictSource


def check(name, fn):
    try:
        status, observed = fn()
        print(f"[{status}] {name}: {observed}")
    except Exception as exc:  # noqa: BLE001  (experiment: report every failure)
        print(f"[ERROR] {name}: {type(exc).__name__}: {str(exc)[:300]}")


def roundtrip(arr):
    src = DictSource(data=[{"k": "a", "x": arr}], tag_columns=["k"], source_id="s")
    (_, data), *_ = list(src.iter_data())
    return src, data["x"]


def same(a, b) -> bool:
    return (
        type(b) is np.ndarray
        and a.dtype == b.dtype
        and a.shape == b.shape
        and np.asarray(a).tobytes() == b.tobytes()
    )


def dtype_case(arr):
    def run():
        src, back = roundtrip(arr)
        h = Datagram({"x": arr}).content_hash().to_hex()[:12]
        ok = same(arr, back)
        return (
            "OK" if ok else "DEFECT",
            f"round trip exact: {ok} (back dtype {getattr(back, 'dtype', None)}, "
            f"type {type(back).__name__}); data schema {dict(src.output_schema()[1])}; "
            f"hash {h}",
        )

    return run


CASES = {
    "bool": np.array([True, False]),
    "int8": np.array([-1, 2], np.int8),
    "uint64": np.array([2**63], np.uint64),
    "float16": np.array([1.5], np.float16),
    "float32": np.array([1.5], np.float32),
    "longdouble": np.array([1.5], np.longdouble),
    "complex64": np.array([1 + 2j], np.complex64),
    "complex128": np.array([1 + 2j], np.complex128),
    "datetime64[ns]": np.array(["2026-11-10T01:02:03"], "M8[ns]"),
    "datetime64[D]": np.array(["2026-11-10"], "M8[D]"),
    "timedelta64[s]": np.array([5], "m8[s]"),
    "unicode U5": np.array(["ab", "cde"], "U5"),
    "bytes S3": np.array([b"ab"], "S3"),
    "void V4": np.array([b"\x00\x01\x02\x03"], "V4"),
    "structured": np.array([(1, 2.0)], dtype=[("a", "<i4"), ("b", "<f8")]),
    "nested structured": np.zeros(1, dtype=[("p", [("x", "<f4"), ("y", "<f4")])]),
    "structured with subarray": np.zeros(1, dtype=[("v", "<f4", (3,))]),
    "big-endian >i4": np.array([1, 2], ">i4"),
}


def stringdtype():
    if not hasattr(np.dtypes, "StringDType"):
        return ("INFO", "np.dtypes.StringDType absent in this NumPy")
    arr = np.array(["a", "bcd"], dtype=np.dtypes.StringDType())
    return dtype_case(arr)()


def field_order():
    a = np.array([(1, 2.0)], dtype=[("a", "<i4"), ("b", "<f8")])
    b = np.array([(2.0, 1)], dtype=[("b", "<f8"), ("a", "<i4")])
    ha = Datagram({"x": a}).content_hash()
    hb = Datagram({"x": b}).content_hash()
    return (
        "INFO",
        f"equal records, field order (a,b) vs (b,a) -> same hash: {ha == hb} "
        "(ITL-465 asks for order-independent hashing)",
    )


def object_dtype():
    out = []
    for label, arr in (
        ("object", np.array([1, "a"], dtype=object)),
        ("struct with object field", np.zeros(1, dtype=[("o", "O")])),
    ):
        try:
            roundtrip(arr)
            out.append(f"{label}: accepted")
        except Exception as exc:  # noqa: BLE001
            out.append(f"{label}: {type(exc).__name__}: {str(exc)[:80]}")
    return ("INFO", "; ".join(out))


def masked():
    m = np.ma.array([1.0, 2.0, 3.0], mask=[False, True, False])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _, back = roundtrip(m)
    h_m = Datagram({"x": m}).content_hash()
    h_plain = Datagram({"x": np.array([1.0, 2.0, 3.0])}).content_hash()
    return (
        "DEFECT" if not isinstance(back, np.ma.MaskedArray) else "OK",
        f"masked array read back as {type(back).__name__}, values {back.tolist()}; "
        f"same hash as the unmasked data: {h_m == h_plain}; warnings: "
        f"{[str(w.message)[:60] for w in caught]}",
    )


def generic_scalar():
    out = []
    for val in (np.float32(1.5), np.int64(3), np.bool_(True)):
        try:
            src, back = roundtrip(val)
            arrow_type = src.as_table().schema.field("x").type
            out.append(
                f"{type(val).__name__}: schema {dict(src.output_schema()[1])}, Arrow "
                f"{arrow_type}, back {type(back).__name__} {back!r}"
            )
        except Exception as exc:  # noqa: BLE001
            out.append(f"{type(val).__name__}: {type(exc).__name__}: {str(exc)[:100]}")
    return ("INFO", "; ".join(out))


class MyArray(np.ndarray[Any, Any]):
    pass


def subclasses():
    out = []
    for label, arr in (
        ("np.matrix", np.matrix([[1.0, 2.0]])),
        ("np.recarray", np.rec.array([(1, 2.0)], dtype=[("a", "<i4"), ("b", "<f8")])),
        ("user subclass", np.arange(3.0).view(MyArray)),
    ):
        try:
            src, back = roundtrip(arr)
            out.append(
                f"{label}: schema {dict(src.output_schema()[1])}, back "
                f"{type(back).__name__}"
            )
        except Exception as exc:  # noqa: BLE001
            out.append(f"{label}: {type(exc).__name__}: {str(exc)[:100]}")
    return ("INFO", "; ".join(out))


def float_bits():
    nan_a = np.array([np.nan])
    nan_b = np.array([np.nan]).view(np.uint64)
    nan_b[0] |= 1
    nan_b = nan_b.view(np.float64)
    z = Datagram({"x": np.array([0.0])}).content_hash()
    nz = Datagram({"x": np.array([-0.0])}).content_hash()
    na = Datagram({"x": nan_a}).content_hash()
    nb = Datagram({"x": nan_b}).content_hash()
    return (
        "INFO",
        f"0.0 vs -0.0 same hash: {z == nz}; two NaN payloads same hash: {na == nb} "
        "(hash is over bits, not values)",
    )


def schema_ignores_dtype_and_shape():
    a = DictSource(
        data=[{"k": "a", "x": np.zeros((2, 3), np.float32)}], tag_columns=["k"]
    )
    b = DictSource(data=[{"k": "a", "x": np.zeros((5,), np.int64)}], tag_columns=["k"])
    return (
        "INFO",
        f"float32 (2,3) and int64 (5,) columns: same output_schema: "
        f"{a.output_schema() == b.output_schema()}; same pipeline_hash: "
        f"{a.pipeline_hash() == b.pipeline_hash()}",
    )


if __name__ == "__main__":
    print(f"numpy {np.__version__}")
    for label, arr in CASES.items():
        check(f"dtype {label}", dtype_case(arr))
    check("dtype StringDType (NumPy 2 variable-width)", stringdtype)
    check("structured field order", field_order)
    check("object dtype", object_dtype)
    check("masked array", masked)
    check("np.generic scalars", generic_scalar)
    check("ndarray subclasses", subclasses)
    check("float bit identity", float_bits)
    check("schema carries no dtype or shape", schema_ignores_dtype_and_shape)
