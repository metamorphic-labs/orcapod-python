"""Backend parameters for shared pipeline store tests."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path

import pytest

from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    InMemoryArrowDatabase,
    SpiralDBConnector,
    SQLiteConnector,
)
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from tests.test_databases import fake_pyspiral

from orcapod.errors import UnstorableColumnError

STORE_KINDS = ("in_memory", "delta", "sqlite", "spiral")

_SPIRAL_RESERVED_COLUMNS = pytest.param(
    "spiral",
    marks=pytest.mark.xfail(
        strict=True,
        raises=UnstorableColumnError,
        reason="TODO-ITL: SpiralDB reserves the '__' prefix on value columns.",
    ),
)
_SQLITE_EXECUTOR_INFO = pytest.param(
    "sqlite",
    marks=pytest.mark.xfail(
        strict=True,
        raises=UnstorableColumnError,
        reason="TODO-ITL: SQLite cannot store the function executor's list of structs.",
    ),
)

function_store_cases = pytest.mark.parametrize(
    "pipeline_store_factory",
    ("in_memory", "delta", _SQLITE_EXECUTOR_INFO, _SPIRAL_RESERVED_COLUMNS),
    indirect=True,
)
cache_store_cases = pytest.mark.parametrize(
    "pipeline_store_factory",
    ("in_memory", "delta", "sqlite", _SPIRAL_RESERVED_COLUMNS),
    indirect=True,
)


@contextmanager
def open_stores(
    kind: str, root: Path
) -> Iterator[Callable[[str], ArrowDatabaseProtocol]]:
    """Open stores of one kind, with named namespaces for isolation and reopening."""
    with ExitStack() as resources:
        if kind == "spiral":
            resources.enter_context(fake_pyspiral.install(root / "spiral"))
        memory: dict[str, InMemoryArrowDatabase] = {}

        def open_store(name: str) -> ArrowDatabaseProtocol:
            """Open a store and register any connector for teardown."""
            if kind == "in_memory":
                if name not in memory:
                    memory[name] = InMemoryArrowDatabase()
                return memory[name]
            if kind == "delta":
                return DeltaTableDatabase(root / name)
            if kind == "sqlite":
                connector = SQLiteConnector(root / f"{name}.db")
            elif kind == "spiral":
                connector = SpiralDBConnector("p", dataset=name)
            else:
                raise AssertionError(f"Unknown test store: {kind}")
            resources.callback(connector.close)
            return ConnectorArrowDatabase(connector)

        yield open_store
