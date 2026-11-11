"""B7: the path from today's stored ``.npy`` columns to each candidate.

Run:
    uv run python superpowers/specs/array-tensor-storage-experiments/b7_migration.py

Claims tested:

* Today's ``.npy`` column allows a different dtype and shape in every row. Each
  candidate fixes dtype (all) and shape (``fst``, ``flat``) per column, so a
  heterogeneous ``.npy`` column cannot be converted into one candidate column.
* Lossless conversion of a homogeneous ``.npy`` column into each candidate:
  which inputs survive exactly (dtype incl. byte order, shape, values, memory
  order) and which are changed or refused.
* Coexistence: old and new formats can sit side by side in one Parquet file or
  Delta table under different column names, and are told apart on read by
  ``ARROW:extension:name``. Even with the metadata gone (SQLite stores storage
  only), ``.npy`` bytes are recognisable by the ``\\x93NUMPY`` magic.
* Same column name, new storage type: appending new-format rows to a Delta
  table that holds ``.npy`` rows (what happens if the logical type keeps its
  name and hence its pipeline hash, so the same table is reused).
"""

from __future__ import annotations

from typing import Any

import io
import tempfile
import warnings
from pathlib import Path

import numpy as np
import numpy.typing as npt
import pyarrow as pa
import pyarrow.parquet as pq
from b0_common import ENCODERS, arrays_equal, checking, decode, decode_stored, report  # pyright: ignore[reportImplicitRelativeImport]  # run as a script: sys.path[0] is this directory

from orcapod.databases import DeltaTableDatabase


def npy_column(arrays: list[npt.NDArray[Any]]) -> pa.ExtensionArray[Any]:
    return ENCODERS["npy"](arrays)


def load_npy(col: pa.ExtensionArray[Any]) -> list[npt.NDArray[Any]]:
    return [np.load(io.BytesIO(v), allow_pickle=False) for v in col.storage.to_pylist()]


def heterogeneous() -> None:
    rows = [
        np.zeros((2, 3), np.float32),
        np.zeros((4,), np.float32),
        np.zeros((2, 3), np.int64),
    ]
    with checking("npy: heterogeneous rows in one column"):
        back = load_npy(npy_column(rows))
        report(
            "INFO",
            "npy: one column holds (2,3) f4, (4,) f4, (2,3) i8",
            f"round trip exact={arrays_equal(back, rows)}",
        )
    for name, enc in ENCODERS.items():
        if name == "npy":
            continue
        with checking(f"{name}: convert heterogeneous npy column"):
            col = enc(load_npy(npy_column(rows)))
            got = decode(name, col, rows[0])
            report(
                "INFO",
                f"{name}: heterogeneous column",
                f"encoded without error; exact={arrays_equal(got, rows)} "
                "(dtype and shape taken from row 0: 'row' casts the int64 row silently via pa.array, 'raw' reinterprets its bytes)",
            )


def homogeneous_conversion() -> None:
    cases = {
        "C float32": np.arange(6, dtype=np.float32).reshape(2, 3),
        "Fortran float32": np.asfortranarray(
            np.arange(6, dtype=np.float32).reshape(2, 3)
        ),
        "big-endian >f4": np.arange(6, dtype=">f4").reshape(2, 3),
        "0-d float64": np.array(1.5),
        "structured": np.zeros((2,), dtype=[("a", "<i4"), ("b", "<f8")]),
        "complex64": np.ones((2, 2), np.complex64),
    }
    for label, a in cases.items():
        old = npy_column([a, a])
        loaded = load_npy(old)
        for name, enc in ENCODERS.items():
            if name == "npy":
                continue
            try:
                new = enc(loaded)
            except Exception as exc:  # noqa: BLE001
                report(
                    "INFO",
                    f"{name}: from npy [{label}]",
                    f"refused: {type(exc).__name__}",
                )
                continue
            with checking(f"{name}: from npy [{label}]"):
                got = decode_stored(name, new)[0]
                exact = (
                    got.dtype == a.dtype
                    and got.shape == a.shape
                    and np.array_equal(got, a)
                )
                order_kept = got.flags.f_contiguous == a.flags.f_contiguous
                report(
                    "OK" if exact and order_kept else "INFO",
                    f"{name}: from npy [{label}]",
                    f"dtype {a.dtype.str}->{got.dtype.str}; exact={exact}; memory order kept={order_kept}",
                )


def coexistence(tmp: Path) -> None:
    arrays = [np.arange(6, dtype=np.float32).reshape(2, 3)]
    table = pa.table({"old_x": npy_column(arrays), "new_x": ENCODERS["raw"](arrays)})
    with checking("parquet: old and new columns side by side"):
        pq.write_table(table, tmp / "mixed.parquet")
        schema = pq.read_table(tmp / "mixed.parquet").schema
        report(
            "INFO",
            "parquet: column types on read",
            {
                f.name: f"{str(f.type)[:50]} / ext-name meta="
                f"{(f.metadata or {}).get(b'ARROW:extension:name')}"
                for f in schema
            },
        )
    with checking("npy magic sniffing on storage-only bytes"):
        raw = npy_column(arrays).storage[0].as_py()
        magic = raw[:6] == b"\x93NUMPY"
        report("INFO", "npy magic", f"storage bytes start with the .npy magic: {magic}")


def same_name_new_type(tmp: Path) -> None:
    db = DeltaTableDatabase(base_path=tmp / "delta")
    arrays = [np.arange(6, dtype=np.float32).reshape(2, 3)]
    old = pa.table(
        {"rid": pa.array([b"a"], pa.large_binary()), "x": npy_column(arrays)}
    )
    for name in ("raw", "flat"):
        new = pa.table(
            {"rid": pa.array([b"b"], pa.large_binary()), "x": ENCODERS[name](arrays)}
        )
        p = ("exp", f"same_name_{name}")
        with checking(f"delta: append {name} rows to npy table"):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                db.add_records(p, old, record_id_column="rid", flush=True)
                before = db.get_all_records(p, record_id_column="rid")
                try:
                    db.add_records(p, new, record_id_column="rid", flush=True)
                    write = "write accepted"
                except Exception as exc:  # noqa: BLE001
                    write = f"write raised {type(exc).__name__}"
                after = db.get_all_records(p, record_id_column="rid")
            report(
                "INFO",
                f"delta: append {name} rows to npy table",
                f"rows before={None if before is None else before.num_rows}; {write}; "
                f"rows after={'None (read failed, old rows invisible)' if after is None else after.num_rows}",
            )


if __name__ == "__main__":
    heterogeneous()
    homogeneous_conversion()
    with tempfile.TemporaryDirectory() as d:
        coexistence(Path(d))
        same_name_new_type(Path(d))
