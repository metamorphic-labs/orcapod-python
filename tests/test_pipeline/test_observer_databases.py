"""Observer setup rejects SpiralDB stores before any recording can start."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, cast, final
from unittest.mock import Mock, patch

import pytest
from typing_extensions import override

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


@final
class _DelegatingConnectorDatabase(ConnectorArrowDatabase):
    """Implement the database interface through a delegate without a connector."""

    # Deliberately exercise a subclass without the base class's stored connector.
    def __init__(self) -> None:  # pyright: ignore[reportMissingSuperCall]
        self._delegate = InMemoryArrowDatabase()

    @override
    def __getattribute__(self, name: str) -> object:
        if not name.startswith("_"):
            return cast(object, getattr(self._delegate, name))
        return cast(object, super().__getattribute__(name))


@final
class _DatabaseWithCustomAttributeLookup(InMemoryArrowDatabase):
    """Expose the database interface without allowing dynamic class lookup."""

    @override
    def __getattribute__(self, name: str) -> object:
        if name == "__class__":
            raise AssertionError("The setup check must use the actual class.")
        return cast(object, super().__getattribute__(name))


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
_REFUSAL = (
    r"^Observers cannot record to a SpiralDB store in this version\. "
    + r"Give the observer another database, for example Delta\.$"
)


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
def test_unclassifiable_connector_database_is_accepted(
    observer_class: type[StatusObserver] | type[LoggingObserver],
    capture_install: Mock,
) -> None:
    observer = observer_class(_DelegatingConnectorDatabase())
    _ = observer.contextualize("node")
    assert capture_install.call_count == int(observer_class is LoggingObserver)


@pytest.mark.parametrize("observer_class", _OBSERVERS)
def test_other_database_attribute_hooks_are_not_called(
    observer_class: type[StatusObserver] | type[LoggingObserver],
    capture_install: Mock,
) -> None:
    _ = observer_class(_DatabaseWithCustomAttributeLookup())
    assert capture_install.call_count == int(observer_class is LoggingObserver)


@pytest.mark.parametrize("observer_class", _OBSERVERS)
def test_wrapper_without_stored_database_is_accepted(
    observer_class: type[StatusObserver] | type[LoggingObserver],
    capture_install: Mock,
) -> None:
    _ = observer_class(ExtensionAwareDatabase.__new__(ExtensionAwareDatabase))
    assert capture_install.call_count == int(observer_class is LoggingObserver)


@pytest.mark.parametrize("logging_first", [True, False])
@pytest.mark.parametrize("nested", [True, False])
def test_composite_refusal_does_not_install_capture(
    logging_first: bool,
    nested: bool,
    spiral_connector: SpiralDBConnector,
    monkeypatch: pytest.MonkeyPatch,
    capture_install: Mock,
) -> None:
    spiral = _wrap(ConnectorArrowDatabase(spiral_connector).at("observer"))
    stores = {"memory": InMemoryArrowDatabase(), "spiral": spiral}

    def resolve(
        config: dict[str, str], db_registry: object = None
    ) -> ArrowDatabaseProtocol:
        assert db_registry is None
        return stores[config["type"]]

    monkeypatch.setattr(serialization, "resolve_database_from_config", resolve)
    logging_config: dict[str, object] = {
        "type": "logging",
        "database": {"type": "memory"},
    }
    status_config: dict[str, object] = {
        "type": "status",
        "database": {"type": "spiral"},
    }
    if nested:
        logging_config = {"type": "composite", "observers": [logging_config]}
        status_config = {"type": "composite", "observers": [status_config]}
    children = [logging_config, status_config]
    if not logging_first:
        children.reverse()

    with pytest.raises(SpiralDBObserverNotSupportedError, match=r"SpiralDB store"):
        _ = CompositeObserver.from_config({"observers": children})

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


def test_every_database_class_exported_from_orcapod_databases_is_classified() -> None:
    """Require an explicit classification for names exported through ``__all__``."""
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
