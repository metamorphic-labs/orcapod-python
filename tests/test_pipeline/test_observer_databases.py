"""Observer setup rejects SpiralDB stores before any recording can start."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, cast
from unittest.mock import Mock, patch

import pytest

from orcapod import databases
from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    ExtensionAwareDatabase,
    InMemoryArrowDatabase,
    NoOpArrowDatabase,
    SpiralDBConnector,
    SQLiteConnector,
)
from orcapod.errors import SpiralDBObserverNotSupportedError
from orcapod.pipeline import serialization
from orcapod.pipeline.composite_observer import CompositeObserver
from orcapod.pipeline.logging_observer import LoggingObserver
from orcapod.pipeline.status_observer import StatusObserver
from orcapod.semantic_types.universal_converter import UniversalTypeConverter
from tests.test_databases import fake_spiral_sdk

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol


class _DerivedConnectorDatabase(ConnectorArrowDatabase):
    """A connector database subclass must be checked too."""


class _DerivedExtensionWrapper(ExtensionAwareDatabase):
    """An extension wrapper subclass must be unwrapped too."""


class _DerivedSpiralConnector(SpiralDBConnector):
    """A SpiralDB connector subclass still uses the same store."""


@pytest.fixture
def spiral_connector(tmp_path: Path) -> Iterator[SpiralDBConnector]:
    """Open a real connector backed by the fake SpiralDB SDK."""
    with (
        fake_spiral_sdk.install(tmp_path / "spiral"),
        SpiralDBConnector("p") as connector,
    ):
        yield connector


@pytest.fixture
def capture_install() -> Iterator[Mock]:
    """Track capture setup without replacing pytest's streams."""
    with patch("orcapod.pipeline.logging_observer.install_capture_streams") as install:
        yield install


def _wrap(db: ArrowDatabaseProtocol) -> ExtensionAwareDatabase:
    """Wrap a store in the extension-aware read layer."""
    return ExtensionAwareDatabase(db, UniversalTypeConverter())


@pytest.fixture(
    params=[
        "root",
        "scoped",
        "wrapped",
        "scoped-then-wrapped",
        "wrapped-then-scoped",
        "doubly-wrapped",
    ]
)
def store_view(
    request: pytest.FixtureRequest,
) -> Callable[[ArrowDatabaseProtocol], ArrowDatabaseProtocol]:
    """Build each supported arrangement of scopes and extension wrappers."""
    views: dict[str, Callable[[ArrowDatabaseProtocol], ArrowDatabaseProtocol]] = {
        "root": lambda db: db,
        "scoped": lambda db: db.at("observer"),
        "wrapped": _wrap,
        "scoped-then-wrapped": lambda db: _wrap(db.at("observer")),
        "wrapped-then-scoped": lambda db: _wrap(db).at("observer"),
        "doubly-wrapped": lambda db: _wrap(_wrap(db.at("observer"))),
    }
    return views[cast(str, request.param)]


_OBSERVERS = [StatusObserver, LoggingObserver]
_REFUSAL = r"Observers cannot record to a SpiralDB store in this version.*another database.*Delta.*ITL-1540"


@pytest.mark.parametrize("observer_class", _OBSERVERS)
def test_spiral_store_is_refused_at_setup(
    observer_class: type[StatusObserver] | type[LoggingObserver],
    spiral_connector: SpiralDBConnector,
    store_view: Callable[[ArrowDatabaseProtocol], ArrowDatabaseProtocol],
    capture_install: Mock,
) -> None:
    db = store_view(ConnectorArrowDatabase(spiral_connector))
    with (
        patch.object(spiral_connector, "get_table_names") as get_table_names,
        patch.object(spiral_connector, "create_table_if_not_exists") as create_table,
        patch.object(spiral_connector, "upsert_records") as upsert,
    ):
        with pytest.raises(SpiralDBObserverNotSupportedError, match=_REFUSAL):
            _ = observer_class(db)
        get_table_names.assert_not_called()
        create_table.assert_not_called()
        upsert.assert_not_called()

    capture_install.assert_not_called()
    assert spiral_connector.get_table_names() == []


@pytest.mark.parametrize("observer_class", _OBSERVERS)
def test_spiral_store_subclasses_are_refused(
    observer_class: type[StatusObserver] | type[LoggingObserver],
    tmp_path: Path,
    capture_install: Mock,
) -> None:
    with (
        fake_spiral_sdk.install(tmp_path / "spiral"),
        _DerivedSpiralConnector("p") as connector,
    ):
        db = _DerivedExtensionWrapper(
            _DerivedConnectorDatabase(connector), UniversalTypeConverter()
        )
        with pytest.raises(SpiralDBObserverNotSupportedError, match=_REFUSAL):
            _ = observer_class(db)

    capture_install.assert_not_called()


@pytest.mark.parametrize("observer_class", _OBSERVERS)
def test_spiral_store_resolved_from_config_is_refused(
    observer_class: type[StatusObserver] | type[LoggingObserver],
    spiral_connector: SpiralDBConnector,
    monkeypatch: pytest.MonkeyPatch,
    capture_install: Mock,
) -> None:
    db = _wrap(ConnectorArrowDatabase(spiral_connector).at("observer"))
    # ConnectorArrowDatabase deserialization is tracked in ITL-1533.
    resolver = Mock(return_value=db)
    monkeypatch.setattr(serialization, "resolve_database_from_config", resolver)

    config = {"database": {"type": "connector_arrow_database"}}
    with pytest.raises(SpiralDBObserverNotSupportedError, match=_REFUSAL):
        # The observer config signatures use bare dicts.
        _ = observer_class.from_config(config)  # pyright: ignore[reportUnknownMemberType]

    resolver.assert_called_once_with(config["database"], db_registry=None)
    capture_install.assert_not_called()


@pytest.mark.parametrize("observer_class", _OBSERVERS)
@pytest.mark.parametrize("backend", ["memory", "delta", "sqlite", "noop"])
def test_other_stores_are_accepted(
    observer_class: type[StatusObserver] | type[LoggingObserver],
    backend: str,
    tmp_path: Path,
    store_view: Callable[[ArrowDatabaseProtocol], ArrowDatabaseProtocol],
    capture_install: Mock,
) -> None:
    with SQLiteConnector() as sqlite:
        roots = {
            "memory": InMemoryArrowDatabase(),
            "delta": DeltaTableDatabase(tmp_path / "delta"),
            "sqlite": ConnectorArrowDatabase(sqlite),
            "noop": NoOpArrowDatabase(),
        }
        observer = observer_class(store_view(roots[backend]))
        composite = CompositeObserver(observer).contextualize("node")
        composite.on_run_start("test-run")
        composite.on_run_end("test-run")

    assert capture_install.call_count == int(observer_class is LoggingObserver)


def test_exported_database_classes_have_been_considered() -> None:
    """Keep new public database types visible to the stopgap's maintainers."""
    assert set(databases.__all__) == {
        # Roots kept as-is by the observer check.
        "InMemoryArrowDatabase",
        "DeltaTableDatabase",
        "NoOpArrowDatabase",
        # The wrapper is unwrapped, then the connector decides the result.
        "ExtensionAwareDatabase",
        "ConnectorArrowDatabase",
        # Connectors: SpiralDB is refused; SQLite and PostgreSQL pass.
        "SpiralDBConnector",
        "SQLiteConnector",
        "PostgreSQLConnector",
    }
