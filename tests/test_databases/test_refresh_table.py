"""Every database implements ``refresh_table``, a required protocol member (ITL-1408).

A function node calls it unconditionally on each of its stored tables at the start of
every run and at every stored read. ``DeltaTableDatabase`` looks at the store again;
the in-memory, connector-backed and no-op databases read the store on every read and
do nothing. Protocols are internal: a capability is a required method, never an
optional protocol plus a check.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pytest

import orcapod.databases as databases
from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    ExtensionAwareDatabase,
    InMemoryArrowDatabase,
    NoOpArrowDatabase,
    SQLiteConnector,
)
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

FACTORIES: dict[str, Callable[[Path], ArrowDatabaseProtocol]] = {
    "in_memory": lambda _: InMemoryArrowDatabase(),
    "delta": lambda path: DeltaTableDatabase(path / "delta"),
    "connector": lambda _: ConnectorArrowDatabase(SQLiteConnector(":memory:")),
    "noop": lambda _: NoOpArrowDatabase(),
    "extension_aware": lambda _: ExtensionAwareDatabase.from_config(
        InMemoryArrowDatabase().to_config()
    ),
}

DATABASE_CLASSES = (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    ExtensionAwareDatabase,
    InMemoryArrowDatabase,
    NoOpArrowDatabase,
)


def test_every_exported_database_class_is_covered() -> None:
    """A new database class the library exports must be added here."""
    exported = {
        getattr(databases, name)
        for name in databases.__all__
        if inspect.isclass(getattr(databases, name))
        and hasattr(getattr(databases, name), "add_record")
    }
    assert exported == set(DATABASE_CLASSES)


@pytest.mark.parametrize("cls", DATABASE_CLASSES, ids=lambda c: c.__name__)
def test_each_class_defines_refresh_table(cls: type) -> None:
    """Defined on the class itself: no attribute lookup decides whether it exists."""
    assert "refresh_table" in vars(cls)
    assert callable(vars(cls)["refresh_table"])


@pytest.mark.parametrize("kind", sorted(FACTORIES))
def test_each_database_conforms_and_refreshes_any_path(
    tmp_path: Path, kind: str
) -> None:
    """Conforms to the protocol; refreshing a table that does not exist is harmless."""
    database = FACTORIES[kind](tmp_path)
    assert isinstance(database, ArrowDatabaseProtocol)
    database.refresh_table(("no", "such", "table"))
    database.at("scoped").refresh_table(("no", "such", "table"))


def _rows(ids: list[int]) -> pa.Table:
    return pa.table(
        {
            "__rid": pa.array([str(i).encode() for i in ids], pa.large_binary()),
            "x": pa.array(ids, pa.int64()),
        }
    )


def test_delta_refresh_sees_another_handles_commit(tmp_path: Path) -> None:
    """The real refresh: a warm handle reads another handle's commit after it."""
    warm = DeltaTableDatabase(tmp_path / "store")
    other = DeltaTableDatabase(tmp_path / "store")
    path = ("t",)
    warm.add_records(path, _rows([1]), record_id_column="__rid")
    warm.flush()
    stored = warm.get_all_records(path)
    assert stored is not None and stored.num_rows == 1
    other.add_records(path, _rows([2]), record_id_column="__rid")
    other.flush()
    stale = warm.get_all_records(path)
    assert stale is not None and stale.num_rows == 1  # the loaded version
    warm.refresh_table(path)
    fresh = warm.get_all_records(path)
    assert fresh is not None
    assert sorted(fresh.column("x").to_pylist(), key=lambda v: v or 0) == [1, 2]


def test_extension_aware_forwards_to_the_wrapped_database(tmp_path: Path) -> None:
    """The wrapper refreshes the Delta database it wraps."""
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter

    inner = DeltaTableDatabase(tmp_path / "store")
    wrapped = ExtensionAwareDatabase(inner, UniversalTypeConverter())
    refreshed: list[tuple[str, ...]] = []
    original = inner.refresh_table

    def record(record_path: tuple[str, ...]) -> None:
        refreshed.append(record_path)
        original(record_path)

    inner.refresh_table = record  # pyright: ignore[reportAttributeAccessIssue]  # instance spy
    wrapped.refresh_table(("t",))
    assert refreshed == [("t",)]
