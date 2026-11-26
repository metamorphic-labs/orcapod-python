"""Which appends of another logical type identifier each database accepts (ITL-1371).

usage: PYTHONPATH=. uv run python docs/dev/scripts/itl_1371_logical_identity_appends.py

A table holds a column ``x`` of logical type ``probe.a`` (metadata ``m``), at the top
level, as a struct child, as a list item or as a map item; a second batch appends ``x``
with the same storage type and the same, or another, identifier, either as a real Arrow
extension array or as the annotated physical form (storage type with
``ARROW:extension:name`` / ``ARROW:extension:metadata`` field metadata), against the
pending batch, the committed table, and (Delta, SpiralDB fake) a reopened database.
Prints ``accepted`` or ``refused: <error>`` per case.
"""

from __future__ import annotations

import itertools
import os
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Any

import pyarrow as pa

from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.databases.connector_arrow_database import ConnectorArrowDatabase
from orcapod.databases.spiraldb_connector import SpiralDBConnector
from orcapod.databases.sqlite_connector import SQLiteConnector

NAME = b"ARROW:extension:name"
META = b"ARROW:extension:metadata"


def _probe_class(name: str) -> Any:
    """An extension type named ``name`` whose metadata is any bytes."""

    class Probe(pa.ExtensionType):
        def __init__(self, meta: bytes) -> None:
            self._meta = meta
            super().__init__(pa.large_string(), name)

        def __arrow_ext_serialize__(self) -> bytes:
            return self._meta

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: bytes) -> Any:
            return cls(serialized)

    return Probe


PROBES: dict[str, Any] = {name: _probe_class(name) for name in ("probe.a", "probe.b")}


IDENTIFIERS = {
    "same": ("probe.a", b"m"),
    "other name": ("probe.b", b"m"),
    "other metadata": ("probe.a", b"n"),
    "plain": None,
}


def leaf(identifier: tuple[str, bytes] | None, form: str, values: list[str]):
    """The leaf field and array of ``x``."""
    storage = pa.array(values, pa.large_string())
    if identifier is None:
        return pa.field("v", pa.large_string()), storage
    if form == "extension":
        ext = PROBES[identifier[0]](identifier[1])
        return pa.field("v", ext), pa.ExtensionArray.from_storage(ext, storage)
    meta = {NAME: identifier[0].encode(), META: identifier[1]}
    return pa.field("v", pa.large_string(), metadata=meta), storage


def column(identifier, form: str, depth: str, values: list[str]):
    field, array = leaf(identifier, form, values)
    n = len(values)
    if depth == "top":
        return field.with_name("x"), array
    if depth == "struct":
        arr = pa.StructArray.from_arrays([array], fields=[field])
        return pa.field("x", arr.type), arr
    if depth == "list":
        offsets = pa.array(list(range(n + 1)), pa.int64())
        typ = pa.large_list(field.with_name("item"))
        arr = pa.LargeListArray.from_arrays(offsets, array, type=typ)
        return pa.field("x", typ), arr
    if depth == "map value":
        # The stubs type pa.map_'s arguments as types and from_arrays' offsets as
        # int64; pyarrow takes key and item fields, and int32 offsets for a map.
        offsets: Any = pa.array(list(range(n + 1)), pa.int32())
        keys = pa.array([f"k{i}" for i in range(n)], pa.large_string())
        typ: Any = pa.map_(
            pa.field("key", pa.large_string(), nullable=False),  # pyright: ignore[reportArgumentType]
            field.with_name("value"),  # pyright: ignore[reportArgumentType]
        )
        from_arrays: Any = pa.MapArray.from_arrays
        arr = from_arrays(offsets, keys, array, type=typ)
        return pa.field("x", typ), arr
    raise AssertionError(depth)


def table(identifier, form, depth, ids: list[bytes]):
    field, arr = column(identifier, form, depth, [i.decode() for i in ids])
    return pa.Table.from_arrays(
        [pa.array(ids, pa.large_binary()), arr],
        schema=pa.schema([pa.field("id", pa.large_binary()), field]),
    )


def open_db(kind: str, root: Path):
    if kind == "in-memory":
        return InMemoryArrowDatabase()
    if kind == "Delta":
        return DeltaTableDatabase(root / "delta")
    if kind == "SQLite":
        return ConnectorArrowDatabase(SQLiteConnector(str(root / "s.db")))
    if kind == "SpiralDB":
        return ConnectorArrowDatabase(SpiralDBConnector("p"))
    raise AssertionError(kind)


def case(kind, state, form, depth, change, root: Path) -> str:
    path = ("t",)
    db = open_db(kind, root)
    base = IDENTIFIERS["same"]
    try:
        db.add_records(
            path,
            table(base, form, depth, [b"1"]),
            record_id_column="id",
            flush=state != "pending",
        )
    except Exception as e:  # noqa: BLE001
        return f"first write refused: {type(e).__name__}"
    if state == "reopened":
        db = open_db(kind, root)
    incoming = table(IDENTIFIERS[change], form, depth, [b"2"])
    try:
        db.add_records(path, incoming, record_id_column="id")
        db.flush()
    except Exception as e:  # noqa: BLE001
        return f"refused: {type(e).__name__}: {(str(e).splitlines() or [''])[0][:110]}"
    return "accepted"


def main() -> None:
    warnings.simplefilter("ignore")
    kinds = ["in-memory", "Delta", "SQLite", "SpiralDB"]
    with tempfile.TemporaryDirectory() as tmp:
        from tests.test_databases import fake_pyspiral  # pyright: ignore[reportAttributeAccessIssue]  # historical: ran on the pyspiral fake, removed in ITL-1186

        with fake_pyspiral.install(Path(tmp) / "spiral"):
            fake_pyspiral.FakeSpiral().project("p")
            for i, (kind, state, form, depth, change) in enumerate(
                itertools.product(
                    kinds,
                    ("pending", "committed", "reopened"),
                    ("extension", "annotated"),
                    ("top", "struct", "list", "map value"),
                    IDENTIFIERS,
                )
            ):
                if state == "reopened" and kind == "in-memory":
                    continue
                root = Path(tmp) / str(i)
                root.mkdir()
                if kind == "SpiralDB":
                    fake_pyspiral.FakeSpiral().project("p")
                print(
                    f"{kind:9} {state:9} {form:9} {depth:9} {change:14} "
                    f"{case(kind, state, form, depth, change, root)}"
                )


if __name__ == "__main__":
    main()
    sys.stdout.flush()
    os._exit(0)
