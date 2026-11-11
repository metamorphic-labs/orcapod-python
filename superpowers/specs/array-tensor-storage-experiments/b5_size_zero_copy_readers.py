"""B5: size, zero-copy decode, and what other tools see, per candidate.

Run (DuckDB checks are skipped unless DuckDB is available):
    uv run --with duckdb python superpowers/specs/array-tensor-storage-experiments/b5_size_zero_copy_readers.py

Claims tested:

* Size: Arrow in-memory bytes and Parquet file size (uncompressed and the
  pyarrow default compression) for 4 rows of a 1000x1000 ``float32`` array
  (4,000,000 bytes of payload per row), per candidate.
* Zero-copy: can a stored row be turned into a NumPy array without copying the
  payload (``np.shares_memory`` against the Arrow buffer, or ``owndata``)?
* Readers: what Polars, pandas (via pyarrow) and DuckDB return when they read
  the Parquet file without Orcapod.
"""

from __future__ import annotations

from typing import Any, cast

import io
import tempfile
import warnings
from pathlib import Path

import numpy as np
import numpy.typing as npt
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from b0_common import ENCODERS, checking, report  # pyright: ignore[reportImplicitRelativeImport]  # run as a script: sys.path[0] is this directory

ROWS = 4
SHAPE = (1000, 1000)


def big_arrays() -> list[npt.NDArray[Any]]:
    rng = np.random.default_rng(0)
    return [rng.random(SHAPE, dtype=np.float32) for _ in range(ROWS)]


def size_checks(name: str, table: pa.Table, tmp: Path) -> None:
    with checking(f"{name}: size"):
        pq.write_table(table, tmp / f"{name}_none.parquet", compression="none")
        pq.write_table(table, tmp / f"{name}_snappy.parquet")
        payload = ROWS * int(np.prod(SHAPE)) * 4
        report(
            "INFO",
            f"{name}: size (payload {payload / 1e6:.1f} MB)",
            f"arrow nbytes={table.column('x').nbytes / 1e6:.2f} MB; "
            f"parquet uncompressed={(tmp / f'{name}_none.parquet').stat().st_size / 1e6:.2f} MB; "
            f"parquet snappy={(tmp / f'{name}_snappy.parquet').stat().st_size / 1e6:.2f} MB",
        )


def zero_copy(name: str, col: pa.Array[Any]) -> None:
    with checking(f"{name}: zero-copy decode of row 0"):
        # the storage layout differs per candidate, so the branches below use it untyped
        s: Any = col.storage if isinstance(col, pa.ExtensionArray) else col
        if name == "npy":
            raw = s.buffers()[2]  # large_binary data buffer
            copied = np.load(io.BytesIO(s[0].as_py()), allow_pickle=False)
            # zero-copy alternative: parse the header, then frombuffer at its offset
            mv = memoryview(raw)
            f = io.BytesIO(bytes(mv[:256]))
            np.lib.format.read_magic(f)
            shape, fortran, dtype = np.lib.format.read_array_header_1_0(f)
            view = np.frombuffer(
                raw, dtype=dtype, count=int(np.prod(shape)), offset=f.tell()
            )
            report(
                "INFO",
                f"{name}: zero-copy",
                f"np.load shares memory={np.shares_memory(copied, np.frombuffer(raw, np.uint8))}; "
                f"header-parse + frombuffer shares memory="
                f"{np.shares_memory(view, np.frombuffer(raw, np.uint8))}",
            )
        elif name == "fst":
            arr = cast("pa.FixedShapeTensorArray[Any]", col).to_numpy_ndarray()
            buf = np.frombuffer(s.values.buffers()[1], np.uint8)
            report(
                "INFO",
                f"{name}: zero-copy",
                f"to_numpy_ndarray shares memory={np.shares_memory(arr, buf)}",
            )
        elif name in ("flat", "vst", "row"):
            values = s.values if name == "flat" else s.field("data").values
            arr = values.to_numpy(zero_copy_only=True)
            buf = np.frombuffer(values.buffers()[1], np.uint8)
            report(
                "INFO",
                f"{name}: zero-copy",
                f"values.to_numpy(zero_copy_only=True) shares memory={np.shares_memory(arr, buf)}",
            )
        elif name == "raw":
            data = s.field("data")
            buf = data.buffers()[2]
            start = int(np.frombuffer(data.buffers()[1], np.int64)[data.offset])
            arr = np.frombuffer(
                buf, dtype=np.float32, count=int(np.prod(SHAPE)), offset=start
            )
            report(
                "INFO",
                f"{name}: zero-copy",
                f"frombuffer on data buffer shares memory={np.shares_memory(arr, np.frombuffer(buf, np.uint8))}",
            )


def readers(name: str, tmp: Path) -> None:
    path = tmp / f"{name}_small.parquet"
    small = pa.table(
        {"x": ENCODERS[name]([np.arange(6, dtype=np.float32).reshape(2, 3)])}
    )
    pq.write_table(small, path)
    with checking(f"{name}: polars read_parquet"):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            df = pl.read_parquet(path)
        report(
            "INFO",
            f"{name}: polars sees",
            f"{df.schema['x']}; value={str(df['x'][0])[:70]!r}; warnings={len(caught)}",
        )
    with checking(f"{name}: pandas via pyarrow"):
        pdf = pq.read_table(path).to_pandas()
        v = pdf["x"].iloc[0]
        report("INFO", f"{name}: pandas sees", f"{type(v).__name__}: {str(v)[:70]!r}")
    try:
        import duckdb  # pyright: ignore[reportMissingImports]  # optional: run with uv run --with duckdb
    except ImportError:
        report(
            "SKIP", f"{name}: duckdb", "duckdb not installed (run with --with duckdb)"
        )
        return
    with checking(f"{name}: duckdb"):
        rel = duckdb.sql(f"SELECT x FROM read_parquet('{path}')")
        report(
            "INFO",
            f"{name}: duckdb sees",
            f"{rel.types[0]}; value={str(rel.fetchall()[0][0])[:70]!r}",
        )


if __name__ == "__main__":
    arrays = big_arrays()
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        for cand, enc in ENCODERS.items():
            with checking(f"{cand}: encode"):
                col = enc(arrays)
                table = pa.table({"x": col})
                size_checks(cand, table, tmp)
                zero_copy(cand, col)
            readers(cand, tmp)
