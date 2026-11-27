"""Database validation shared by observers that record run events."""

from __future__ import annotations

from inspect import getattr_static
from types import MemberDescriptorType
from typing import TYPE_CHECKING, cast

from orcapod.databases import (
    ConnectorArrowDatabase,
    ExtensionAwareDatabase,
    SpiralDBConnector,
)
from orcapod.errors import SpiralDBObserverNotSupportedError

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol


def _stored_reference(instance: object, name: str) -> object:
    """Read an instance field or native slot without calling attribute hooks."""
    value = cast(object, getattr_static(instance, name, None))
    if type(value) is MemberDescriptorType:
        try:
            return cast(object, value.__get__(instance, type(instance)))
        except AttributeError:
            return None
    return value


def require_observer_database(database: ArrowDatabaseProtocol) -> None:
    """Validate the observer's database without contacting the store.

    Args:
        database: The database passed to the observer.

    Raises:
        SpiralDBObserverNotSupportedError: If the database is backed by SpiralDB.
    """
    # Stopgap (ITL-1540): update this check by hand for a new standalone SpiralDB
    # database or a new wrapper. ITL-1539 will decide the protocol property that
    # every database should declare and wrappers should forward. Use the known
    # classes' stored references without invoking subclass attribute hooks.
    backend: object = database
    seen: set[int] = set()
    while issubclass(type(backend), ExtensionAwareDatabase):
        if id(backend) in seen:
            return
        seen.add(id(backend))
        backend = _stored_reference(backend, "_db")
    if not issubclass(type(backend), ConnectorArrowDatabase):
        return

    connector = _stored_reference(backend, "_connector")
    if issubclass(type(connector), SpiralDBConnector):
        raise SpiralDBObserverNotSupportedError(
            "Observers cannot record to a SpiralDB store in this version. "
            + "Give the observer another database, for example Delta."
        )
