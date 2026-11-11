"""A1b: are the stored ``.npy`` bytes and the content hash the same on NumPy 1.26 and 2.x?

Run: ``uv run python superpowers/specs/array-tensor-storage-experiments/a1b_numpy_versions.py``

The script re-runs itself twice as a child: once in the locked environment (NumPy 2.x)
and once with ``uv run --with numpy==1.26.4``. Each child prints, for a fixed set of
arrays, the SHA-256 of ``np.save`` bytes and the Orcapod datagram content hash. The parent
compares the two and reports any array whose bytes or hash differ, or a child that cannot
start. It also checks that bytes written by one version load in the other.
"""

from __future__ import annotations

from typing import Any

import base64
import io
import json
import subprocess
import sys


def arrays():
    import numpy as np

    return {
        "f8_2x3": np.arange(6, dtype=np.float64).reshape(2, 3),
        "f8_fortran": np.asfortranarray(np.arange(6, dtype=np.float64).reshape(2, 3)),
        "f4_be": np.array([1.0, 2.0], dtype=">f4"),
        "bool": np.array([True, False]),
        "i1": np.array([-1, 2], dtype=np.int8),
        "u8": np.array([1, 2], dtype=np.uint64),
        "c16": np.array([1 + 2j]),
        "m8_ns": np.array([1, 2], dtype="m8[ns]"),
        "M8_D": np.array(["2026-11-10"], dtype="M8[D]"),
        "U5": np.array(["ab", "cde"], dtype="U5"),
        "S3": np.array([b"ab"], dtype="S3"),
        "zero_d": np.array(3.5),
        "empty_0x3": np.zeros((0, 3)),
        "struct": np.zeros(2, dtype=[("a", "<i4"), ("b", "<f8")]),
        "struct_nested": np.zeros(1, dtype=[("p", [("x", "<f4"), ("y", "<f4")])]),
        "subarray_field": np.zeros(1, dtype=[("v", "<f4", (3,))]),
    }


def child() -> None:
    import hashlib

    import numpy as np

    out: dict[str, Any] = {"numpy": np.__version__, "npy": {}, "orcapod": {}, "raw": {}}
    try:
        from orcapod.core.datagrams.datagram import Datagram
    except Exception as exc:  # noqa: BLE001
        Datagram = None
        out["orcapod_import_error"] = f"{type(exc).__name__}: {exc}"
    for name, arr in arrays().items():
        buf = io.BytesIO()
        np.save(buf, arr, allow_pickle=False)
        raw = buf.getvalue()
        out["npy"][name] = hashlib.sha256(raw).hexdigest()
        out["raw"][name] = base64.b64encode(raw).decode()
        if Datagram is not None:
            try:
                out["orcapod"][name] = Datagram({"x": arr}).content_hash().to_hex()
            except Exception as exc:  # noqa: BLE001
                out["orcapod"][name] = f"ERROR {type(exc).__name__}: {exc}"
    print("JSON:" + json.dumps(out))


def run(extra: list[str]) -> dict[str, Any] | str:
    cmd = ["uv", "run", *extra, "python", __file__, "--child"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    for line in proc.stdout.splitlines():
        if line.startswith("JSON:"):
            return json.loads(line[5:])
    return f"exit {proc.returncode}: {proc.stderr.strip().splitlines()[-3:]}"


def parent() -> None:
    import numpy as np

    new = run([])
    old = run(["--with", "numpy==1.26.4"])
    for label, res in (("locked", new), ("numpy==1.26.4", old)):
        if isinstance(res, str):
            print(f"[ERROR] child {label} could not run: {res}")
            return
        print(
            f"[INFO] child {label}: numpy {res['numpy']}; orcapod import error: "
            f"{res.get('orcapod_import_error')}"
        )
    assert isinstance(new, dict) and isinstance(old, dict)
    npy_diff = [k for k in new["npy"] if new["npy"][k] != old["npy"][k]]
    print(
        f"[{'OK' if not npy_diff else 'DEFECT'}] np.save bytes identical on "
        f"{old['numpy']} and {new['numpy']} for {len(new['npy'])} arrays; differing: {npy_diff}"
    )
    hash_diff = [
        k for k in new["orcapod"] if new["orcapod"].get(k) != old["orcapod"].get(k)
    ]
    print(
        f"[{'OK' if not hash_diff else 'DEFECT'}] Orcapod datagram content hash identical "
        f"across versions; differing: {hash_diff}"
    )
    # Cross-read: bytes written by 1.26 load in this (2.x) process to an equal array.
    bad = []
    for name, ref in arrays().items():
        got = np.load(
            io.BytesIO(base64.b64decode(old["raw"][name])), allow_pickle=False
        )
        if (
            got.dtype != ref.dtype
            or got.shape != ref.shape
            or got.tobytes() != ref.tobytes()
        ):
            bad.append(name)
    print(
        f"[{'OK' if not bad else 'DEFECT'}] 1.26-written bytes load in {np.__version__} "
        f"with equal dtype, shape and buffer; mismatches: {bad}"
    )


if __name__ == "__main__":
    if "--child" in sys.argv:
        child()
    else:
        parent()
