"""Read a job's STORED tables directly, and name what links them (ITL-1421, round two).

``node.get_all_records()`` is a convenience view: it joins a function node's pipeline
record with its result and drops the pipeline record's own key (the pipeline entry id),
``__node_content_hash`` and ``__pipeline_base_entry_id``. These helpers read every stored
table as it is, through the job's database (``list_table_paths`` and
``get_all_records(path, record_id_column=...)``), so every column is visible.

Tables of one job (database scoped by the job name):

- function node, pipeline store: ``<job>/<function uri>/schema:<pipeline hash>/pdb_v1``
- function node, result store: ``<job>/_result/<function uri>/rev1/rdb_v1``
- operator node with ``CacheMode.LOG``: ``<job>/<operator uri>/schema:<pipeline hash>/odb_v1``

A source job node stores nothing; its rows are read from the node (``as_table``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

KEY = "__record_id"
"""The name under which the stored primary key of every table is read here."""


def is_labelled(value: bytes) -> bool:
    """A stored hash is ``b"<label>:" + digest`` with a lower-case ASCII label; a result
    id (a 16-byte uuid7) is not, even when one of its bytes happens to be ``:``."""
    label, sep, _ = value.partition(b":")
    return (
        bool(sep)
        and 2 <= len(label) <= 32
        and all(c in b"abcdefghijklmnopqrstuvwxyz0123456789_" for c in label)
    )


def hash_text(value: bytes | str | None) -> str | None:
    """One text form for a hash stored as a prefixed binary or as ``label:hex`` text.

    Stored tables keep ``b"<label>:" + digest``; in-memory tables keep
    ``"<label>:<hex>"``. Both become ``"<label>:<hex>"``.
    """
    if value is None or isinstance(value, str):
        return value
    label, _, digest = value.partition(b":")
    return f"{label.decode()}:{digest.hex()}"


def short_hash(value: Any, n: int = 10) -> str:
    """A short display form of a hash, a uuid or a token."""
    if isinstance(value, bytes):
        text = hash_text(value) if is_labelled(value) else value.hex()
    else:
        text = str(value)
    if text is None:
        return "None"
    label, sep, rest = text.rpartition(":")
    if not sep:  # a uuid7 (a result id): its leading digits are a timestamp
        return f"…{rest[-n:]}" if len(rest) > n else rest
    if rest.startswith("000002"):  # starfix's digest header
        rest = rest[6:]
    return f"{label}{sep}{rest[:n]}…" if len(rest) > n else text


def system_tag_columns(table: Any) -> list[str]:
    if table is None:
        return []
    return sorted(c for c in table.column_names if c.startswith("_tag_"))


def user_tags(node: Any) -> list[str]:
    return list(node.keys()[0])


def parse_token(token: str) -> tuple[str, str, str]:
    """Split ``<source id>::<record id>::<column>`` at its first and last ``::``."""
    head, _, column = token.rpartition("::")
    source, _, record = head.partition("::")
    return source, record, column


@dataclass
class NodeTables:
    """Every table of one job node: in memory, and each stored table found for it."""

    label: str
    kind: str
    memory: pa.Table
    # A stored table is ``None`` when the node has none; typed ``Any`` for the scripts.
    pipeline: Any = None
    pipeline_path: tuple[str, ...] = ()
    result: Any = None
    result_path: tuple[str, ...] = ()
    cache: Any = None
    cache_path: tuple[str, ...] = ()
    content_hash: str = ""
    pipeline_hash: str = ""

    def own(self, table: Any) -> Any:
        """The rows of a shared stored table that belong to THIS node.

        A pipeline or operator table is shared by every node of one pipeline hash
        under the job's scope; ``__node_content_hash`` names the node.
        """
        if table is None or "__node_content_hash" not in table.column_names:
            return table
        mask = [
            hash_text(v) == self.content_hash
            for v in table["__node_content_hash"].to_pylist()
        ]
        return table.filter(pa.array(mask))


@dataclass
class StoredJob:
    job: Any
    database: Any
    nodes: dict[str, NodeTables] = field(default_factory=dict)
    paths: list[tuple[str, ...]] = field(default_factory=list)

    def __getitem__(self, label: str) -> NodeTables:
        return self.nodes[label]

    def label_of_pipeline_hash(self, hex_digest: str) -> str | None:
        for label, n in self.nodes.items():
            if n.pipeline_hash.endswith(hex_digest):
                return label
        return None


def read_job(job: Any, database: Any) -> StoredJob:
    """Read every node's in-memory table and every stored table of ``job``."""
    out = StoredJob(job, database, paths=sorted(database.list_table_paths()))
    for label in sorted(job.nodes):
        node = job.nodes[label]
        kind = type(node).__name__
        n = NodeTables(
            label,
            kind,
            node.as_table(all_info=True),
            content_hash=node.content_hash().to_string(),
            pipeline_hash=node.pipeline_hash().to_string(),
        )
        ident = tuple(node.node_identity_path) if kind != "SourceJobNode" else ()
        for path in out.paths:
            if ident and path[1:-1] == ident and path[-1] == "pdb_v1":
                n.pipeline_path = path
                n.pipeline = database.get_all_records(path, record_id_column=KEY)
            if ident and path[1:-1] == ident and path[-1] == "odb_v1":
                n.cache_path = path
                n.cache = database.get_all_records(path, record_id_column=KEY)
            # <job>/_result/<function uri>/<revision>/rdb_v1
            if (
                kind == "FunctionJobNode"
                and path[1] == "_result"
                and path[2:-2] == tuple(node.node_uri)
                and path[-1] == "rdb_v1"
            ):
                n.result_path = path
                n.result = database.get_all_records(path, record_id_column=KEY)
        out.nodes[label] = n
    return out


def rows(table: pa.Table | None) -> list[dict[str, Any]]:
    return [] if table is None else table.to_pylist()


def explain_system_tag_name(name: str, stored: StoredJob) -> str:
    """Read a system-tag column name: the source's schema hash, then one extension per
    multi-input or folding step, each naming the INPUT stream by its pipeline hash."""
    parts = name.split("::")
    words = [f"{parts[0]} of the source with schema hash {parts[1][:8]}…"]
    for ext in parts[2:]:
        digest, _, position = ext.partition(":")
        label = stored.label_of_pipeline_hash(digest) or "?"
        if position:
            words.append(f"joined as input {position} = {label!r} ({digest[:8]}…)")
        else:
            words.append(f"folded after {label!r} ({digest[:8]}…)")
    return "; ".join(words)


def log_operators(job: Any, database: Any, name: str) -> None:
    """Give every operator job node a stored cache (``CacheMode.LOG``), so its rows are
    in the store too. A job's default (``OFF``) keeps operator rows in memory only."""
    from orcapod.types import CacheMode

    for label in sorted(job.nodes):
        node = job.nodes[label]
        if type(node).__name__ == "OperatorJobNode":
            node.attach_databases(
                pipeline_database=database.at(name), cache_mode=CacheMode.LOG
            )


def by_system_tags(table: pa.Table) -> dict[tuple[Any, ...], dict[str, Any]]:
    """Index a table's rows by their system-tag values (column names sorted)."""
    cols = system_tag_columns(table)
    out: dict[tuple[Any, ...], dict[str, Any]] = {}
    for r in table.to_pylist():
        out[tuple(r[c] for c in cols)] = r
    return out
