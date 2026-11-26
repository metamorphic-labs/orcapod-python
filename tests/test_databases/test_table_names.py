"""The relational connectors' table names did not change when naming moved (ITL-1498).

``ConnectorArrowDatabase`` used to name tables and list paths itself; the connectors do
now (``DBConnectorProtocol.table_name``, ``has_table``, ``table_paths``). The rule it used
is kept below as the oracle, verbatim, and every name and listing must equal it.
"""

from __future__ import annotations

import random
import re
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.databases import ConnectorArrowDatabase
from orcapod.databases.sqlite_connector import SQLiteConnector
from orcapod.databases.table_names import joined_table_name, joined_table_paths


def _old_path_to_table_name(record_path: tuple[str, ...]) -> str:
    """``ConnectorArrowDatabase._path_to_table_name`` before ITL-1498, verbatim."""
    parts = [re.sub(r"[^a-zA-Z0-9_]", "_", part) for part in record_path]
    name = "__".join(parts)
    if name and name[0].isdigit():
        name = "t_" + name
    return name


def _old_list_table_paths(
    names: set[str],
    pending_keys: list[str],
    path_prefix: tuple[str, ...],
    prefix: tuple[str, ...],
) -> list[tuple[str, ...]]:
    """``ConnectorArrowDatabase.list_table_paths`` before ITL-1498, verbatim."""
    full_prefix = path_prefix + prefix
    names = set(names)
    names.update(_old_path_to_table_name(tuple(key.split("/"))) for key in pending_keys)
    stem = _old_path_to_table_name(full_prefix) if full_prefix else ""
    found: set[tuple[str, ...]] = set()
    for name in names:
        if not stem:
            found.add(tuple(name.split("__")))
        elif name == stem:
            found.add(prefix)
        elif name.startswith(stem + "__"):
            found.add(prefix + tuple(name[len(stem) + 2 :].split("__")))
    return sorted(found)


_ALPHABET = ["a", "b", "1", "_", "__", "-", ".", "é", "A", ":", " "]


def _component(rng: random.Random) -> str:
    return "".join(rng.choice(_ALPHABET) for _ in range(rng.randint(1, 4)))


def _path(rng: random.Random) -> tuple[str, ...]:
    return tuple(_component(rng) for _ in range(rng.randint(1, 4)))


def test_names_are_the_old_rule() -> None:
    rng = random.Random(1498)
    for _ in range(20_000):
        path = _path(rng)
        assert joined_table_name(path) == _old_path_to_table_name(path)


@pytest.mark.parametrize("seed", range(40))
def test_listings_are_the_old_rule(seed: int) -> None:
    rng = random.Random(seed)
    stored = {_old_path_to_table_name(_path(rng)) for _ in range(rng.randint(0, 12))}
    pending = ["/".join(_path(rng)) for _ in range(rng.randint(0, 4))]
    for path_prefix in ((), _path(rng)[:1]):
        for prefix in ((), _path(rng)[:1], _path(rng)[:2]):
            full = [
                p[len(path_prefix) :]
                for p in joined_table_paths(
                    {
                        *stored,
                        *(joined_table_name(tuple(k.split("/"))) for k in pending),
                    },
                    path_prefix + prefix,
                )
            ]
            assert sorted(set(full)) == _old_list_table_paths(
                stored, pending, path_prefix, prefix
            )


def test_a_database_on_sqlite_names_and_lists_as_before(tmp_path: Path) -> None:
    """Through ``ConnectorArrowDatabase``: stored and pending tables, with a base path."""
    connector = SQLiteConnector(str(tmp_path / "db.sqlite"))
    db = ConnectorArrowDatabase(connector)
    rows = pa.table({"x": pa.array([1], pa.int64())})
    stored_paths = [("fn-1", "v0", "rdb_v1"), ("fn.1", "v0"), ("2nd", "x__y")]
    for path in stored_paths:
        db.add_record(path, b"r", rows)
    db.flush()
    db.add_record(("fn-1", "pending"), b"r", rows)
    names = set(connector.get_table_names())
    assert names == {_old_path_to_table_name(p) for p in stored_paths}
    for prefix in ((), ("fn-1",), ("fn.1",), ("2nd",)):
        assert db.list_table_paths(prefix) == _old_list_table_paths(
            names, ["fn-1/pending"], (), prefix
        )
    scoped = db.at("fn-1")
    assert scoped.list_table_paths() == _old_list_table_paths(
        names, ["fn-1/pending"], ("fn-1",), ()
    )
    for path in stored_paths:
        assert db.table_exists(path)
    assert not db.table_exists(("absent",))
