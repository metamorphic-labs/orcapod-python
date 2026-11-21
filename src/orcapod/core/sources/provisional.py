"""The provisional sources: identities not frozen with data context v1 (ITL-1314).

Data context v1 freezes every hash at the 0.4.0 release. A provisional source is outside
that freeze: its default source id and its content hash (and so the system-tag values,
source-info tokens, stored paths and record ids downstream of it) may still change in a
later release. No golden value depends on one: a golden case that uses a provisional
source is refused by the golden tool, since only provisional operators may be declared.

``PROVISIONAL_SOURCES`` is the one list. Each listed source also sets the class attribute
``PROVISIONAL = True`` (declared on ``RootSource``); a test checks that the list and the
markers agree. Constructing a provisional source, also through ``from_config``, issues
``ProvisionalHashWarning`` once per source kind per process and notes the use
(``utils/provisional.warn_provisional_use``). The mark enters no hash, identifier, config
or stored path. A source leaves the list when its identity is decided; its golden values
are then added.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from orcapod.utils.provisional import warn_provisional_use

PROVISIONAL_SOURCES: Mapping[str, str] = MappingProxyType(
    {
        # Its default source id hashes the implementation's own identity(), and its
        # content hash leaves out the source id and the declared schema (ITL-1316).
        "PollingSource": "ITL-1316",
    }
)
"""The provisional sources: class name to the issue that decides its identity."""


def warn_provisional_source(name: str) -> None:
    """Note the use of a provisional source and warn, once per source kind per process.

    Args:
        name: The provisional source's class name.
    """
    warn_provisional_use(
        name,
        f"{name} is provisional: its source id and hashes are not frozen with data "
        "context v1 and may change in a later release. Then the system tags, stored "
        f"paths and record ids of its rows, and of every step downstream of {name}, "
        "change, and those results are computed again instead of reused (nothing "
        "stored is lost or wrong). See 'Provisional sources' in "
        "docs/concepts/sources.md. To silence: warnings.filterwarnings('ignore', "
        "category=orcapod.errors.ProvisionalHashWarning).",
    )
