"""Database validation shared by observers that record run events."""

from __future__ import annotations

from typing import TYPE_CHECKING

from orcapod.databases import (
    ConnectorArrowDatabase,
    ExtensionAwareDatabase,
    SpiralDBConnector,
)
from orcapod.errors import SpiralDBObserverNotSupportedError

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol


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
    # classes' stored references directly until then.
    while isinstance(database, ExtensionAwareDatabase):
        database = database._db  # pyright: ignore[reportPrivateUsage]
    if isinstance(database, ConnectorArrowDatabase) and isinstance(
        database._connector,  # pyright: ignore[reportPrivateUsage]
        SpiralDBConnector,
    ):
        raise SpiralDBObserverNotSupportedError(
            "Observers cannot record to a SpiralDB store in this version. "
            + "Give the observer another database, for example Delta (ITL-1540)."
        )
