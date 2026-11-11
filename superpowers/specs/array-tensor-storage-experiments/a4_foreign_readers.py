"""A4: what other tools see in a stored NumPy array column (ITL-874).

A ``PipelineJob`` stores the output of a ``-> np.ndarray`` pod in a ``DeltaTableDatabase``.
The Delta table that holds the array column (``.../_result/...``) is then read without
Orcapod.

Two settings, because importing Orcapod registers ``numpy.ndarray`` as an extension type with
pyarrow and polars for the whole process:

1. A clean subprocess (``python -I``) that never imports Orcapod, as any other tool would.
2. This process, after Orcapod is imported, as a user's own notebook would be.

Claims tested in each setting:

* ``deltalake`` gives an Arrow table: the column's Arrow type and field metadata
  (``ARROW:extension:name`` / ``ARROW:extension:metadata``).
* ``pyarrow.parquet.read_schema`` on a Delta data file: the same, from the Parquet footer.
* ``polars.read_delta``: the dtype polars gives the column.
* ``pandas``: the column dtype and the Python type of a value.
* A reader with only NumPy decodes each value with ``np.load(io.BytesIO(value),
  allow_pickle=False)`` and gets the original arrays back (dtype, shape, byte order, order).

Also: an in-memory stream's ``as_table()`` handed to ``polars.from_arrow`` and pandas.

Run: ``uv run python superpowers/specs/array-tensor-storage-experiments/a4_foreign_readers.py``
"""

from __future__ import annotations

from typing import Any, cast

import os

os.environ.setdefault(
    "RUST_LOG", "error"
)  # before deltalake loads: silence stats warnings

import io  # noqa: E402
import json  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
import tempfile  # noqa: E402
from pathlib import Path  # noqa: E402

import numpy as np  # noqa: E402
import numpy.typing as npt  # noqa: E402
import polars as pl  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from a3_common import describe, report, run_check, same_array  # noqa: E402  # pyright: ignore[reportImplicitRelativeImport]  # run as a script: sys.path[0] is this directory
from orcapod import PipelineJob, function_pod  # noqa: E402
from orcapod.databases import DeltaTableDatabase  # noqa: E402
from orcapod.sources import DictSource  # noqa: E402

X = {
    "a": np.arange(6, dtype=np.float32).reshape(2, 3),
    "b": np.asfortranarray(np.arange(6, dtype=np.float64).reshape(3, 2)),
    "c": np.array([1, -2, 3], dtype=">i4"),  # big-endian
}
COLUMN = "y"

# Runs in a clean interpreter: no Orcapod import, so no extension types are registered.
CLEAN_READER = r"""
import io, json, sys
import numpy as np, pyarrow.parquet as pq, polars as pl
from deltalake import DeltaTable
path = sys.argv[1]
out = {}
def field(f):
    meta = {k.decode(): v.decode()[:80] for k, v in (f.metadata or {}).items()}
    return f"type={f.type}, metadata={meta}"
try:
    t = DeltaTable(path).to_pyarrow_table()
    out["deltalake"] = field(t.schema.field("y"))
    raw = t.column("y").to_pylist()
    arrs = [np.load(io.BytesIO(v), allow_pickle=False) for v in raw]
    out["numpy"] = [
        [a.dtype.str, list(a.shape), bool(a.flags["F_CONTIGUOUS"] and a.ndim > 1),
         a.tolist()]
        for a in arrs
    ]
    out["header"] = repr(raw[0][: raw[0].index(b"\n") + 1])
    out["nbytes"] = [len(v) for v in raw]
except Exception as e:
    out["deltalake"] = f"{type(e).__name__}: {e}"
try:
    import glob
    files = sorted(glob.glob(path + "/*.parquet"))
    kinds = sorted({field(pq.read_schema(f).field("y")) for f in files})
    out["parquet"] = f"{len(files)} data files; distinct footers: {kinds}"
except Exception as e:
    out["parquet"] = f"{type(e).__name__}: {e}"
try:
    df = pl.read_delta(path)
    out["polars"] = f"dtype={df.schema['y']}, value type={type(df['y'][0]).__name__}"
except Exception as e:
    out["polars"] = f"{type(e).__name__}: {' '.join(str(e).split())[:200]}"
try:
    pdf = DeltaTable(path).to_pandas()
    out["pandas"] = f"dtype={pdf['y'].dtype}, value type={type(pdf['y'].iloc[0]).__name__}"
except Exception as e:
    out["pandas"] = f"{type(e).__name__}: {e}"
print(json.dumps(out))
"""


@function_pod(output_keys=COLUMN)
def identity(x: np.ndarray) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    return x


def source() -> DictSource:
    return DictSource(
        data=[{"k": k, "x": v} for k, v in X.items()],
        tag_columns=["k"],
        source_id="a4_foreign",
    )


def result_table_dir(store: Path) -> Path:
    """Return the Delta table directory that holds the pod's output column."""
    from deltalake import DeltaTable

    for log in sorted(store.rglob("_delta_log")):
        table = log.parent
        if COLUMN in DeltaTable(str(table)).schema().to_arrow().names:
            return table
    raise FileNotFoundError(f"no Delta table with column {COLUMN!r} under {store}")


def field_summary(field: pa.Field[Any]) -> str:
    meta = {k.decode(): v.decode()[:80] for k, v in (field.metadata or {}).items()}
    return f"type={field.type}, metadata={meta}"


def matches_inputs(decoded: list[npt.NDArray[Any]]) -> dict[str, str]:
    """Return the inputs that no decoded array equals (dtype, shape and values)."""
    return {
        k: describe(v)
        for k, v in X.items()
        if not any(same_array(d, v) for d in decoded)
    }


def clean_process(table_dir: Path) -> None:
    proc = subprocess.run(
        [sys.executable, "-I", "-c", CLEAN_READER, str(table_dir)],
        capture_output=True,
        text=True,
        env=dict(os.environ, RUST_LOG="error"),
        check=False,
    )
    if proc.returncode != 0:
        report("DEFECT", "clean reader process", proc.stderr.strip().splitlines()[-1])
        return
    out = json.loads(proc.stdout.strip().splitlines()[-1])
    for key in ("deltalake", "parquet", "polars", "pandas"):
        status = "INFO" if "Error" not in out[key] else "DEFECT"
        report(status, f"clean process (no Orcapod): {key}", out[key])
    if "numpy" in out:
        decoded = [
            np.asarray(values, dtype=np.dtype(dt)).reshape(shape)
            for dt, shape, _, values in out["numpy"]
        ]
        missing = matches_inputs(decoded)
        orders = ["F" if f else "C" for _, _, f, _ in out["numpy"]]
        dtypes = [dt for dt, *_ in out["numpy"]]
        report(
            "OK" if not missing else "DEFECT",
            "clean process (no Orcapod): numpy-only np.load of each value",
            f"dtypes={dtypes}, orders={orders}, unmatched inputs={missing}, "
            f"bytes per value={out['nbytes']}, first header={out['header']}",
        )


def this_process(table_dir: Path) -> None:
    from deltalake import DeltaTable

    run_check(
        "Orcapod imported: deltalake to_pyarrow_table() field",
        lambda: field_summary(
            DeltaTable(str(table_dir)).to_pyarrow_table().schema.field(COLUMN)
        ),
    )
    run_check(
        "Orcapod imported: pyarrow.parquet.read_schema field",
        lambda: (
            f"{len(list(table_dir.glob('*.parquet')))} data files; distinct footers: "
            + str(
                sorted(
                    {
                        field_summary(pq.read_schema(f).field(COLUMN))
                        for f in table_dir.glob("*.parquet")
                    }
                )
            )
        ),
    )

    def polars_delta() -> str:
        frame = pl.read_delta(str(table_dir))
        return f"dtype={frame.schema[COLUMN]}, value type={type(frame[COLUMN][0]).__name__}"

    run_check("Orcapod imported: polars.read_delta", polars_delta)

    def numpy_decode() -> tuple[str, str]:
        raw = DeltaTable(str(table_dir)).to_pyarrow_table().column(COLUMN)
        values = raw.to_pylist()
        decoded = [
            np.load(io.BytesIO(cast(bytes, v)), allow_pickle=False) for v in values
        ]
        missing = matches_inputs(decoded)
        return ("OK" if not missing else "DEFECT"), (
            f"to_pylist() value type={type(values[0]).__name__}; unmatched inputs={missing}"
        )

    run_check("Orcapod imported: np.load of each stored value", numpy_decode)

    def in_memory_polars() -> str:
        table = identity.pod(source()).as_table()
        frame = pl.from_arrow(table)
        assert isinstance(frame, pl.DataFrame)
        return (
            f"as_table() field: {field_summary(table.schema.field(COLUMN))}; "
            f"pl.from_arrow dtype={frame.schema[COLUMN]}"
        )

    run_check("in-memory as_table() -> polars.from_arrow", in_memory_polars)

    def in_memory_pandas() -> str:
        frame = identity.pod(source()).as_table().to_pandas()
        value = frame[COLUMN].iloc[0]
        return f"dtype={frame[COLUMN].dtype}, value type={type(value).__name__}"

    run_check("in-memory as_table().to_pandas()", in_memory_pandas)


def main() -> None:
    store = Path(tempfile.mkdtemp(prefix="itl874_a4_foreign_")) / "store"
    job = PipelineJob(name="foreign", store=DeltaTableDatabase(base_path=store))
    with job:
        identity.pod(source(), label="identity")
    job.run()
    table_dir = result_table_dir(store)
    report("INFO", "Delta table read", table_dir.relative_to(store).parts[:3])
    clean_process(table_dir)
    this_process(table_dir)


if __name__ == "__main__":
    main()
