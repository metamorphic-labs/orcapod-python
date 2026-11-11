"""B3: does each candidate survive each storage backend available locally?

Run:
    uv run python superpowers/specs/array-tensor-storage-experiments/b3_databases.py

Claims tested, per candidate:

* Parquet (``pyarrow.parquet`` write/read): Arrow type preserved, arrays equal.
* Arrow IPC file: same.
* ``InMemoryArrowDatabase``, ``DeltaTableDatabase`` and ``ConnectorArrowDatabase``
  over ``SQLiteConnector``: ``add_records`` then ``get_all_records``. Reported raw,
  and wrapped in ``ExtensionAwareDatabase`` with the default context's type
  converter (the wrapper pipelines use to re-wrap extension columns on read).
* After reading, the arrays are decoded from whatever comes back (extension or
  storage) and compared with the input.
"""

from __future__ import annotations


import tempfile
import warnings
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
from b0_common import ENCODERS, arrays_equal, checking, decode, report, sample_arrays  # pyright: ignore[reportImplicitRelativeImport]  # run as a script: sys.path[0] is this directory

from orcapod.contexts import get_default_context
from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    ExtensionAwareDatabase,
    InMemoryArrowDatabase,
    SQLiteConnector,
)

CONVERTER = get_default_context().type_converter


def table_for(name: str) -> pa.Table:
    return pa.table(
        {
            "rid": pa.array([b"r0", b"r1"], pa.large_binary()),
            "x": ENCODERS[name](sample_arrays()),
        }
    )


def _verdict(name: str, label: str, original: pa.Table, back: pa.Table | None) -> None:
    if back is None:
        report("FAIL", f"{name}: {label}", "no records returned")
        return
    back = back.sort_by("rid") if "rid" in back.column_names else back
    otype = original.schema.field("x").type
    btype = back.schema.field("x").type
    meta = back.schema.field("x").metadata or {}
    ext_name = meta.get(b"ARROW:extension:name", b"").decode() or None
    got = decode(name, back.column("x"), sample_arrays()[0])
    equal = arrays_equal(got, sample_arrays())
    report(
        "OK" if btype == otype and equal else ("INFO" if equal else "FAIL"),
        f"{name}: {label}",
        f"type kept={btype == otype}; returned={btype}; field ext-name meta={ext_name}; "
        f"arrays equal={equal}",
    )


def file_formats(name: str, tmp: Path) -> None:
    original = table_for(name)
    with checking(f"{name}: parquet"):
        pq.write_table(original, tmp / f"{name}.parquet")
        _verdict(name, "parquet", original, pq.read_table(tmp / f"{name}.parquet"))
    with checking(f"{name}: arrow ipc"):
        with ipc.new_file(str(tmp / f"{name}.arrow"), original.schema) as w:
            w.write_table(original)
        _verdict(
            name,
            "arrow ipc",
            original,
            ipc.open_file(str(tmp / f"{name}.arrow")).read_all(),  # pyright: ignore[reportArgumentType]  # pyarrow-stubs omit the str path pyarrow accepts
        )


def storage_only(table: pa.Table) -> pa.Table:
    """Strip extension identity: storage arrays, no field metadata."""
    cols = []
    for col in table.columns:
        chunks = [
            c.storage if isinstance(c, pa.ExtensionArray) else c for c in col.chunks
        ]
        cols.append(pa.chunked_array(chunks))
    return pa.table(dict(zip(table.column_names, cols, strict=True)))


def databases(name: str, tmp: Path) -> None:
    original = table_for(name)
    makers = {
        "in-memory": lambda: InMemoryArrowDatabase(),
        "delta": lambda: DeltaTableDatabase(base_path=tmp / f"delta_{name}"),
        "sqlite": lambda: ConnectorArrowDatabase(
            SQLiteConnector(tmp / f"{name}.sqlite")
        ),
    }
    for db_name, make in makers.items():
        for wrapped in (False, True):
            label = f"{db_name}{' + ExtensionAwareDatabase' if wrapped else ''}"
            with checking(f"{name}: {label}"):
                db = make()
                if wrapped:
                    db = ExtensionAwareDatabase(db, CONVERTER)  # pyright: ignore[reportArgumentType]  # src has two TypeConverterProtocol classes; the context's converter satisfies both at runtime
                path = ("exp", f"{name}_{int(wrapped)}")
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        db.add_records(
                            path, original, record_id_column="rid", flush=True
                        )
                        back = db.get_all_records(path, record_id_column="rid")
                except ValueError as exc:
                    if '"category"' in str(exc):
                        report(
                            "INFO",
                            f"{name}: {label}",
                            "throwaway type has no orcapod 'category' metadata, so the "
                            "converter cannot reconstruct it (expected; a real logical type "
                            "would register a factory)",
                        )
                        continue
                    raise
                _verdict(name, label, original, back)
        # Storage layout alone, without extension identity: does the backend keep it?
        with checking(f"{name}: {db_name} storage-only"):
            db = make()
            plain = storage_only(original)
            path = ("exp", f"{name}_plain")
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                db.add_records(path, plain, record_id_column="rid", flush=True)
                back = db.get_all_records(path, record_id_column="rid")
            if back is None:
                report("FAIL", f"{name}: {db_name} storage-only", "no records returned")
                continue
            back = back.sort_by("rid")
            got = (
                decode(name, back.column("x"), sample_arrays()[0])
                if name != "fst"
                else None
            )
            report(
                "INFO",
                f"{name}: {db_name} storage-only",
                f"in={plain.schema.field('x').type}; out={back.schema.field('x').type}; "
                f"same={plain.schema.field('x').type == back.schema.field('x').type}"
                + (
                    ""
                    if got is None
                    else f"; arrays equal={arrays_equal(got, sample_arrays())}"
                ),
            )


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        for cand in ENCODERS:
            file_formats(cand, tmp)
            databases(cand, tmp)
