"""The joined table-name rule of the relational connectors (ITL-1498).

``ConnectorArrowDatabase`` names a table by its full record path. SQLite, PostgreSQL and a
plain SpiralDB connector use this one rule, the one ``ConnectorArrowDatabase`` used before
naming moved into the connectors (``DBConnectorProtocol.table_name``, ``table_paths``):
each component sanitized (anything but letters, digits and ``_`` becomes ``_``), joined
with ``__``, and ``t_`` in front of a name that starts with a digit. The rule loses
information, so a listing gives back paths in the name's form.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa


def joined_table_name(path: Sequence[str]) -> str:
    """Return the table name of ``path`` under the joined rule.

    Args:
        path: The full record path components.
    """
    parts = [re.sub(r"[^a-zA-Z0-9_]", "_", part) for part in path]
    name = "__".join(parts)
    if name and name[0].isdigit():
        name = "t_" + name
    return name


def joined_table_paths(
    names: Iterable[str], prefix: tuple[str, ...]
) -> list[tuple[str, ...]]:
    """Return the paths, under ``prefix``, of tables named by the joined rule.

    The tables whose name starts with the name ``prefix`` maps to. The components after
    ``prefix`` are the rest of the name split at ``__``, in the name's form: a component
    that held ``__`` comes back as two, and a table of another path whose name has the
    same beginning is listed as well. With an empty prefix, a name that got the ``t_``
    prefix keeps it.

    Args:
        names: The table names, stored and pending.
        prefix: Leading full path components.

    Returns:
        The full paths, sorted.
    """
    stem = joined_table_name(prefix) if prefix else ""
    found: set[tuple[str, ...]] = set()
    for name in names:
        if not stem:
            found.add(tuple(name.split("__")))
        elif name == stem:
            found.add(prefix)
        elif name.startswith(stem + "__"):
            found.add(prefix + tuple(name[len(stem) + 2 :].split("__")))
    return sorted(found)


def whole_table(batches: Iterable[pa.RecordBatch]) -> pa.Table | None:
    """Return the batches of a whole-table read as one table, ``None`` for none.

    What a relational connector's ``read_candidates`` gives: the whole table, which
    ``ConnectorArrowDatabase`` then filters (ITL-1502).
    """
    import pyarrow as pa

    found = list(batches)
    return pa.Table.from_batches(found) if found else None
