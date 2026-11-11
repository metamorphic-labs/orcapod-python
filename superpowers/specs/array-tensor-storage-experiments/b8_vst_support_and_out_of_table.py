"""B8: what the locked stack supports for ``arrow.variable_shape_tensor``; out-of-table size.

Run:
    uv run python superpowers/specs/array-tensor-storage-experiments/b8_vst_support_and_out_of_table.py

Claims tested:

* The locked pyarrow has no built-in ``variable_shape_tensor`` (no factory, no
  type class), so the type can only be written as storage plus
  ``ARROW:extension:*`` field metadata, or through a Python-defined extension type.
* A field carrying only that metadata (no Python type registered) survives an
  Arrow IPC round trip and a Parquet round trip with the metadata intact.
* Polars reads such a field as its storage struct.
* Out-of-table storage (ITL-467): with arrays written to files and the table
  holding only a reference (path plus content hash), what the table costs per
  row. Everything about a real artifact store (lifecycle, garbage collection,
  remote URIs) is not verified here.
"""

from __future__ import annotations

from typing import cast

import hashlib
import tempfile
import warnings
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
from b0_common import checking, report  # pyright: ignore[reportImplicitRelativeImport]  # run as a script: sys.path[0] is this directory

VST_META = {
    b"ARROW:extension:name": b"arrow.variable_shape_tensor",
    b"ARROW:extension:metadata": b'{"dim_names":["H","W"]}',
}


def metadata_only_vst_table() -> pa.Table:
    storage_type = pa.struct(
        [("data", pa.list_(pa.float32())), ("shape", pa.list_(pa.int32(), 2))]
    )
    storage = pa.array(
        [
            {"data": [0, 1, 2, 3, 4, 5], "shape": [2, 3]},
            {"data": [7, 8], "shape": [1, 2]},
        ],
        storage_type,
    )
    field = pa.field("x", storage_type, metadata=VST_META)
    return pa.Table.from_arrays([storage], schema=pa.schema([field]))


def support() -> None:
    report(
        "INFO",
        "pyarrow version / built-in variable_shape_tensor",
        f"{pa.__version__} / factory={hasattr(pa, 'variable_shape_tensor')}, "
        f"type class={hasattr(pa, 'VariableShapeTensorType')}",
    )
    report(
        "INFO",
        "pyarrow built-in fixed_shape_tensor",
        f"factory={hasattr(pa, 'fixed_shape_tensor')}, "
        f"is pa.ExtensionType subclass={issubclass(pa.FixedShapeTensorType, pa.ExtensionType)}",
    )


def metadata_roundtrips(tmp: Path) -> None:
    # b0_common registers a Python VST type on import; drop it so this checks
    # what a reader with no definition of the type does.
    pa.unregister_extension_type("arrow.variable_shape_tensor")
    table = metadata_only_vst_table()
    with checking("vst metadata-only: IPC round trip"):
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, table.schema) as w:
            w.write_table(table)
        back = ipc.open_stream(sink.getvalue()).read_all()
        f = back.schema.field("x")
        report(
            "OK" if f.metadata == VST_META else "FAIL",
            "vst metadata-only: IPC",
            f"type={f.type}; metadata kept={f.metadata == VST_META}",
        )
    with checking("vst metadata-only: Parquet round trip"):
        pq.write_table(table, tmp / "vst.parquet")
        f = pq.read_table(tmp / "vst.parquet").schema.field("x")
        report(
            "OK" if f.metadata == VST_META else "FAIL",
            "vst metadata-only: Parquet",
            f"type={f.type}; metadata kept={f.metadata == VST_META}",
        )
    with checking("vst metadata-only: polars"):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            df = cast(pl.DataFrame, pl.from_arrow(table))
        report(
            "INFO",
            "vst metadata-only: polars dtype",
            f"{df.schema['x']}; warnings={len(caught)}",
        )


def out_of_table(tmp: Path) -> None:
    rng = np.random.default_rng(0)
    arrays = [rng.random((1000, 1000), dtype=np.float32) for _ in range(4)]
    store = tmp / "artifacts"
    store.mkdir()
    refs = []
    with checking("out-of-table: reference table size"):
        for a in arrays:
            payload = np.ascontiguousarray(a).tobytes()
            digest = hashlib.sha256(payload).hexdigest()
            np.save(store / f"{digest}.npy", a)
            refs.append(
                {
                    "uri": f"artifacts/{digest}.npy",
                    "sha256": digest,
                    "shape": list(a.shape),
                    "dtype": a.dtype.str,
                }
            )
        table = pa.Table.from_pylist(
            refs,
            schema=pa.schema(
                [
                    ("uri", pa.large_string()),
                    ("sha256", pa.large_string()),
                    ("shape", pa.large_list(pa.int64())),
                    ("dtype", pa.large_string()),
                ]
            ),
        )
        pq.write_table(table, tmp / "refs.parquet")
        files = sum(p.stat().st_size for p in store.iterdir())
        report(
            "INFO",
            "out-of-table: 4 x 1000x1000 float32",
            f"reference parquet={(tmp / 'refs.parquet').stat().st_size} bytes; "
            f"artifact files={files / 1e6:.2f} MB; table rows stay small and hashable by starfix "
            "(strings and int lists only)",
        )


if __name__ == "__main__":
    support()
    with tempfile.TemporaryDirectory() as d:
        metadata_roundtrips(Path(d))
        out_of_table(Path(d))
