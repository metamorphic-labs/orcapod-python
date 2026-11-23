"""``node.trace()`` and ``node.why_missing()``: per-row provenance of a job (ITL-1432).

The procedure is the one of the provenance showcase design (ITL-1421, round two,
``docs/dev/specs/itl-1421-provenance-showcase.md``):

- Within a run, every row a node holds names the input rows it came from by its SYSTEM
  TAGS: a row-wise operator and a function pod keep their values and names, a join keeps
  each input's values under names extended with ``::<pipeline hash of that input>:<its
  position>`` (the argument index for ``MergeJoin``; the canonical rank, by pipeline hash
  with ties in argument order, for ``Join``). So the input row of an output row is the row
  of the input whose system-tag values are the output row's, under the mapped names.
- A function node's stored PIPELINE entry for that row (its system tags, and the result
  its output token names) says which result the row was served and whether this entry
  computed it (``__computed``); its stored RESULT says which function, which body, which
  executor and when. The input row's ``_content_hash`` must be the entry's
  ``__input_data_hash``, and the result's value must be the value the node holds.
- A folding operator (``GroupBy``, ``Batch``) folds its members' system tags into one
  value. Its members are found with the operator's own rule (``fold_members``) and
  CHECKED: every group must fold to exactly one held output row and back; otherwise
  ``ProvenanceInconsistencyError``. Member ``i`` fills position ``i`` of every list, and
  each member's value must be the list's element ``i``.
- The SOURCE INFO token ``<source id>::<record id>::<column>`` of a value names the source
  cell it came from; the token's record id is the record-id system tag of that source.
  A token is matched EXACTLY first; an element of a list token (``Index`` after a fold)
  or an access path (``Pick``) only when no exact match exists. Every link is checked by
  value: the input cell must hold the value the output cell holds.

Everything is read from what is held or stored: the rows the job's nodes hold
(``as_table`` of a job node that is not stale) and the function nodes' stored tables
(``FunctionJobNode._stored_provenance``). A source is never read: a source cell is known
from the token of the row read from it. Only a source that keeps its rows in memory, in
an ``ArrowTableStream`` it built itself, with the base ``as_table`` and ``iter_data`` (no
override), offers a held snapshot, read without calling the source, to check values and
to say whether it holds a record. Every other source (a cached, polling, derived or
user-defined one) is reported as not read. Nothing is computed, no user code is called,
nothing is written, and an ambiguous link is refused, never guessed.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

from orcapod.errors import ProvenanceInconsistencyError
from orcapod.system_constants import constants
from orcapod.types.explanation import Step, plain_repr
from orcapod.types.provenance import (
    Absence,
    FunctionRun,
    SourceCell,
    Trace,
    TraceStep,
)
from orcapod.utils.arrow_utils import fold_system_tag_values

if TYPE_CHECKING:
    import pyarrow as pa

_ROW_HASH = "_content_hash"
"""The name under which a held row's content hash is read here."""

# The pipeline entry columns of the run id and the time an entry was recorded
# (ITL-1433, decided 2026-11-23). Shown when the stored table has them.
# TODO(ITL-1433): import the constants defined beside the pipeline meta schema.
_RUN_ID_COL = "__pipeline_run_id"
_RECORDED_AT_COL = "__recorded_at"

_COLUMNS = {"system_tags": True, "source": True, "content_hash": _ROW_HASH}

Row = dict[str, Any]
Link = tuple["_Cell", "int | None"]


# ---------------------------------------------------------------------------
# Values, as Orcapod compares them
# ---------------------------------------------------------------------------


def same_value(a: Any, b: Any) -> bool:
    """Whether two cell values are one value, by Orcapod's strict rules.

    A ``bool`` equals only a ``bool`` (never ``0`` or ``1``); every NaN equals every
    NaN and ``0.0`` equals ``-0.0``, as ``GroupBy`` and the joins match keys; lists and
    mappings compare element by element; values of unrelated types are never equal.

    Args:
        a: A value.
        b: A value.

    Returns:
        Whether they are one value.
    """
    if isinstance(a, bool) or isinstance(b, bool):
        return type(a) is type(b) and a == b
    if isinstance(a, float) and isinstance(b, float):
        return (math.isnan(a) and math.isnan(b)) or a == b
    if isinstance(a, list) and isinstance(b, list):
        items_a, items_b = cast(list[Any], a), cast(list[Any], b)
        return len(items_a) == len(items_b) and all(
            same_value(x, y) for x, y in zip(items_a, items_b)
        )
    if isinstance(a, dict) and isinstance(b, dict):
        map_a, map_b = cast(dict[Any, Any], a), cast(dict[Any, Any], b)
        return map_a.keys() == map_b.keys() and all(
            same_value(map_a[k], map_b[k]) for k in map_a
        )
    numbers = isinstance(a, (int, float)) and isinstance(b, (int, float))
    if type(a) is not type(b) and not numbers:
        return False
    try:
        return bool(a == b)
    except Exception:  # an incomparable value is not equal
        return False


# ---------------------------------------------------------------------------
# Node kinds and what a node holds
# ---------------------------------------------------------------------------


def _kind(node: Any) -> str:
    from orcapod.core.nodes.function_node import FunctionJobNode
    from orcapod.core.nodes.source_node import SourceNodeBase

    if isinstance(node, SourceNodeBase):
        return "source"
    if isinstance(node, FunctionJobNode):
        return "function"
    return "operator"


def _folding(node: Any) -> Any:
    """The node's folding operator (``GroupBy`` or ``Batch``), or ``None``."""
    from orcapod.core.operators.batch import Batch
    from orcapod.core.operators.group_by import GroupBy

    producer = getattr(node, "producer", None)
    return producer if isinstance(producer, (GroupBy, Batch)) else None


def _source_id(node: Any) -> str:
    """The id a source node's tokens and system tags carry."""
    bound = getattr(node, "bound_source", None)
    found = getattr(bound, "source_id", None)
    return found if isinstance(found, str) else node.label


def _library_class(obj: Any) -> bool:
    """Whether ``obj`` is an instance of an Orcapod class itself, not of a subclass
    defined elsewhere: only such an object's methods are called (no user code)."""
    return type(obj).__module__.startswith("orcapod.")


def _keys(node: Any) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """A node's tag and data column names, without calling a source.

    A job's source node answers from the schema it declares (``SourceNodeBase``),
    never from its bound source, whose ``output_schema`` may be user code.
    """
    if _kind(node) == "source":
        from orcapod.core.nodes.source_node import SourceNodeBase

        tags, data = SourceNodeBase.output_schema(node)
        return tuple(tags), tuple(data)
    tags, data = node.keys()
    return tuple(tags), tuple(data)


def _source_ids_of(node: Any) -> set[str]:
    """The ids a source node's rows can carry: its own, and that of the source a
    ``CachedSource`` wraps (which keeps the wrapped source's tokens and system tags)."""
    from orcapod.core.sources.cached_source import CachedSource

    ids = {_source_id(node)}
    bound = getattr(node, "bound_source", None)
    while type(bound) is CachedSource:
        bound = vars(bound).get("_source")
        found = getattr(bound, "source_id", None) if _library_class(bound) else None
        if isinstance(found, str):
            ids.add(found)
    return ids


def _source_snapshot(node: Any) -> pa.Table | None:
    """The rows a source keeps in memory, read without calling it; else ``None``.

    Only a source whose class keeps the base ``as_table`` and ``iter_data`` and that holds
    its rows in an ``ArrowTableStream`` it built itself: reading that stream runs no user
    code, no I/O and writes nothing. A cached, polling, derived or user-defined source
    (one that overrides how it reads) is not read.
    """
    from orcapod.core.sources.base import RootSource
    from orcapod.core.streams.arrow_table_stream import ArrowTableStream

    from orcapod.core.nodes.source_node import SourceNodeBase

    bound = getattr(node, "bound_source", None)
    if not isinstance(bound, RootSource) or not _library_class(bound):
        return None
    cls = type(bound)
    if (
        cls.as_table is not RootSource.as_table
        or cls.iter_data is not RootSource.iter_data
    ):
        return None
    stream = vars(bound).get("_stream")
    if type(stream) is not ArrowTableStream:
        return None
    table = stream.as_table(columns=_COLUMNS)
    # The job's source node presents the system tags under the names of its declared
    # schema: one source-id and one record-id column, matched by prefix.
    tag_schema, _ = SourceNodeBase.output_schema(node, columns={"system_tags": True})
    renamed: list[str] = []
    for name in table.column_names:
        for prefix in (
            constants.SYSTEM_TAG_SOURCE_ID_PREFIX,
            constants.SYSTEM_TAG_RECORD_ID_PREFIX,
        ):
            if name.startswith(prefix):
                presented = [c for c in tag_schema if c.startswith(prefix)]
                if len(presented) != 1:
                    return None
                name = presented[0]
        renamed.append(name)
    return table.rename_columns(renamed)


def _system_tags(row: Mapping[str, Any]) -> list[str]:
    return sorted(c for c in row if c.startswith(constants.SYSTEM_TAG_PREFIX))


def _source_id_column(record_column: str) -> str:
    """The source-id system-tag column paired with a record-id one.

    Args:
        record_column: A record-id system-tag column name, with any extensions.

    Returns:
        The source-id column name with the same extensions.
    """
    return record_column.replace(
        constants.SYSTEM_TAG_RECORD_ID_PREFIX, constants.SYSTEM_TAG_SOURCE_ID_PREFIX, 1
    )


def _record_ids(row: Mapping[str, Any], source: str | set[str]) -> set[Any]:
    """The record ids of ``source`` (an id, or any of several) a row names, under any
    extended column name."""
    sources = {source} if isinstance(source, str) else source
    return {
        row[c]
        for c in row
        if c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
        and row.get(_source_id_column(c)) in sources
    }


def _names_source(row: Mapping[str, Any], sources: set[str]) -> bool:
    """Whether a row's source-id system tags name one of ``sources`` (not a fold)."""
    return any(
        row[c] in sources
        for c in row
        if c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
    )


def _hash_text(value: Any) -> str | None:
    """A stored ``b"<label>:" + digest`` as the ``<label>:<hex>`` text of a held row."""
    if value is None or isinstance(value, str):
        return value
    label, _, digest = bytes(value).partition(b":")
    return f"{label.decode()}:{digest.hex()}"


def _parse_token(token: str) -> tuple[str, str, str]:
    """Split ``<source id>::<record id>::<column>`` at its first and last ``::``."""
    sep = constants.BLOCK_SEPARATOR
    head, _, column = token.rpartition(sep)
    source, _, record = head.partition(sep)
    return source, record, column


def _positions(node: Any) -> list[int]:
    """The position each input of a combining node has in its system-tag names.

    ``MergeJoin`` is ordered: the argument index. Every other combining operator orders
    its inputs canonically by pipeline hash, ties in argument order.
    """
    from orcapod.core.operators.merge_join import MergeJoin

    upstreams = list(node.upstreams)
    if isinstance(getattr(node, "producer", None), MergeJoin):
        return list(range(len(upstreams)))
    hexes = [u.pipeline_hash().to_hex() for u in upstreams]
    ranked = sorted(range(len(hexes)), key=lambda i: hexes[i])
    position = [0] * len(hexes)
    for rank, index in enumerate(ranked):
        position[index] = rank
    return position


def _column_map(
    out_columns: Sequence[str], node: Any, index: int, up_columns: Sequence[str]
) -> dict[str, str]:
    """Map the output's system-tag columns to those of input ``index`` they come from.

    A column passes unchanged, or gains one extension naming that input by its pipeline
    hash: ``::<hash>:<position>`` (a join: the position must be that input's own) or
    ``::<hash>`` (a fold).
    """
    sep = constants.BLOCK_SEPARATOR
    upstream = node.upstreams[index]
    digest = upstream.pipeline_hash().to_hex()
    position = str(_positions(node)[index]) if len(node.upstreams) > 1 else None
    mapping: dict[str, str] = {}
    for out in out_columns:
        for up in up_columns:
            if out == up:
                mapping[out] = up
                break
            if not out.startswith(up + sep):
                continue
            extension = out[len(up) + len(sep) :]
            if sep in extension:
                continue
            hash_part, colon, at = extension.partition(constants.FIELD_SEPARATOR)
            if not digest.startswith(hash_part):
                continue
            if colon and at != position:
                continue
            mapping[out] = up
            break
    return mapping


_Lookups = dict[Any, Any]
"""Indexes built once per call: column maps, and rows by their system-tag values."""


def _input_rows(
    row: Row, node: Any, index: int, up_rows: list[Row], lookups: _Lookups
) -> list[Row]:
    """The rows of input ``index`` that ``row`` names by its system tags (row-wise).

    The input's rows are indexed once per call by the mapped system-tag values, so a
    trace through many rows does not scan them again for each row.
    """
    if not up_rows:
        return []
    out_columns, up_columns = tuple(_system_tags(row)), tuple(_system_tags(up_rows[0]))
    map_key = ("map", id(node), index, out_columns, up_columns)
    if map_key not in lookups:
        lookups[map_key] = _column_map(out_columns, node, index, up_columns)
    mapping: dict[str, str] = lookups[map_key]
    if not mapping:
        return []
    ordered = sorted(mapping.items())
    up_names = tuple(u for _, u in ordered)
    index_key = ("rows", id(up_rows), up_names)
    if index_key not in lookups:
        by_values: dict[tuple[Any, ...], list[Row]] = {}
        for r in up_rows:
            by_values.setdefault(tuple(r[u] for u in up_names), []).append(r)
        lookups[index_key] = (up_rows, by_values)  # keeps the list alive with its id
    _, by_values = lookups[index_key]
    return list(by_values.get(tuple(row[o] for o, _ in ordered), []))


def _one(rows: list[Row], node: Any, what: str) -> Row:
    """The one row of ``rows``; several rows count as one only when identical."""
    if not rows:
        raise ProvenanceInconsistencyError(f"node {node.label!r}: {what}: no row")
    hashes = {r.get(_ROW_HASH) for r in rows}
    if len(rows) > 1 and (len(hashes) > 1 or None in hashes):
        raise ProvenanceInconsistencyError(
            f"node {node.label!r}: {what}: {len(rows)} different rows match and nothing "
            "tells them apart; the link is ambiguous"
        )
    return rows[0]


@dataclass
class _Reader:
    """What one call reads, each node once."""

    tables: dict[int, pa.Table | None] = field(default_factory=dict)
    rows: dict[int, list[Row] | None] = field(default_factory=dict)
    stored: dict[int, tuple[list[Row], dict[bytes, Row]]] = field(default_factory=dict)
    folds: dict[int, dict[int, list[Row]] | None] = field(default_factory=dict)
    lookups: _Lookups = field(default_factory=dict)

    def table(self, node: Any) -> pa.Table | None:
        """The rows the node holds, with system tags, tokens and row hashes.

        ``None`` when the node holds no run (it is stale or never ran), and for a source
        that offers no held snapshot (``_source_snapshot``).
        """
        key = id(node)
        if key not in self.tables:
            if _kind(node) == "source":
                self.tables[key] = _source_snapshot(node)
            elif node.is_stale:
                self.tables[key] = None
            else:
                self.tables[key] = node.as_table(columns=_COLUMNS)
        return self.tables[key]

    def held(self, node: Any) -> list[Row] | None:
        key = id(node)
        if key not in self.rows:
            table = self.table(node)
            self.rows[key] = None if table is None else table.to_pylist()
        return self.rows[key]

    def provenance(self, node: Any) -> tuple[list[Row], dict[bytes, Row]]:
        """A function node's own stored entries, and its results by id."""
        key = id(node)
        if key not in self.stored:
            entries, results = node._stored_provenance()  # pyright: ignore[reportPrivateUsage]  # the provenance reader of the node, ITL-1432
            entry_rows: list[Row] = [] if entries is None else entries.to_pylist()
            by_id: dict[bytes, Row] = {}
            if results is not None:
                for result in results.to_pylist():
                    by_id[bytes(result[constants.DATA_RECORD_ID])] = result
            self.stored[key] = (entry_rows, by_id)
        return self.stored[key]

    def fold(self, node: Any) -> dict[int, list[Row]] | None:
        """A folding node's members, by the ``id`` of each held output row.

        Every group the operator's rule forms from the held input must fold to exactly
        one held output row, and every held output row must be one group's fold.

        Returns:
            The members of each output row, in member order; ``None`` when the node or
            its input holds no run (or the input is a source offering no snapshot).

        Raises:
            ProvenanceInconsistencyError: If the groups and the held rows disagree.
        """
        key = id(node)
        if key in self.folds:
            return self.folds[key]
        rows = self.held(node)
        upstream = node.upstreams[0]
        up_table = self.table(upstream)
        up_rows = self.held(upstream)
        folding = _folding(node)
        if (
            rows is None
            or up_table is None
            or up_rows is None
            or not _library_class(folding)
        ):
            # Not held, or a fold rule that is user code: not read.
            self.folds[key] = None
            return None
        mapping = _column_map(
            _system_tags(rows[0]) if rows else [],
            node,
            0,
            _system_tags(up_rows[0]) if up_rows else [],
        )
        assigned: dict[int, list[Row]] = {}
        ordered = sorted(mapping.items())
        by_folded: dict[tuple[Any, ...], list[Row]] = {}
        for r in rows:
            by_folded.setdefault(tuple(r[o] for o, _ in ordered), []).append(r)
        for members in folding.fold_members(up_table, list(_keys(upstream)[0])):
            folded = {
                column: fold_system_tag_values(
                    [up_rows[i][column] for i in members], label=folding.fold_label
                )
                for column in set(mapping.values())
            }
            found = (
                by_folded.get(tuple(folded[u] for _, u in ordered), [])
                if mapping
                else []
            )
            if len(found) != 1 or id(found[0]) in assigned:
                raise ProvenanceInconsistencyError(
                    f"node {node.label!r}: folding the system tags of the members of a "
                    f"{type(folding).__name__} group does not give exactly one row the "
                    f"node holds ({len(found)} match); the rows {upstream.label!r} "
                    "holds are not the ones the node folded"
                )
            assigned[id(found[0])] = [up_rows[i] for i in members]
        if len(assigned) != len(rows):
            raise ProvenanceInconsistencyError(
                f"node {node.label!r}: {len(rows) - len(assigned)} of the rows it holds "
                f"are no fold of the rows {upstream.label!r} holds"
            )
        self.folds[key] = assigned
        return assigned


# ---------------------------------------------------------------------------
# trace
# ---------------------------------------------------------------------------


def _tags_of(node: Any, row: Row) -> tuple[tuple[str, Any], ...]:
    return tuple((t, row[t]) for t in _keys(node)[0] if t in row)


def _datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None


@dataclass(frozen=True)
class _Cell:
    """A cell being traced: the row, the column, and optionally one element of it."""

    row: Row
    column: str
    element: int | None = None

    @property
    def value(self) -> Any:
        value = self.row.get(self.column)
        if self.element is None:
            return value
        return cast(list[Any], value)[self.element]

    @property
    def token(self) -> Any:
        token = self.row.get(constants.SOURCE_PREFIX + self.column)
        if self.element is None or not isinstance(token, list):
            return token
        return cast(list[Any], token)[self.element]


def _entries_for(node: Any, row: Row, column: str, reader: _Reader) -> list[Row]:
    """The node's stored entries for a held row: its system tags and its result id.

    The entries are indexed once per call by those system-tag values.
    """
    entries, _ = reader.provenance(node)
    token = row.get(constants.SOURCE_PREFIX + column)
    result_hex = _parse_token(token)[1] if isinstance(token, str) else None
    columns = tuple(_system_tags(row))
    key = ("entries", id(node), columns)
    if key not in reader.lookups:
        by_tags: dict[tuple[Any, ...], list[Row]] = {}
        for e in entries:
            by_tags.setdefault(tuple(e.get(c) for c in columns), []).append(e)
        reader.lookups[key] = by_tags
    found: list[Row] = reader.lookups[key].get(tuple(row[c] for c in columns), [])
    return [
        e
        for e in found
        if result_hex is None or bytes(e[constants.DATA_RECORD_ID]).hex() == result_hex
    ]


def _latest(entries: list[Row], node: Any) -> Row:
    """The entry of the latest attempt; entries for different inputs are refused."""
    inputs = {bytes(e[constants.INPUT_DATA_HASH_COL]) for e in entries}
    if len(inputs) > 1:
        raise ProvenanceInconsistencyError(
            f"node {node.label!r}: {len(inputs)} stored entries for different inputs "
            "have the row's system tags and result; the link is ambiguous"
        )
    return max(entries, key=lambda e: e.get("__pipeline_recomputation_index") or 0)


def _function_run(
    node: Any, entry: Row, row: Row, column: str, reader: _Reader
) -> FunctionRun:
    """The run behind an entry; the stored value must be the value the node holds."""
    _, results = reader.provenance(node)
    result = results.get(bytes(entry[constants.DATA_RECORD_ID]))
    if result is None:
        raise ProvenanceInconsistencyError(
            f"node {node.label!r}: the stored entry names a result the result "
            "database does not hold"
        )
    if column in result and not same_value(result[column], row.get(column)):
        raise ProvenanceInconsistencyError(
            f"node {node.label!r}: the stored result holds {column}="
            f"{result[column]!r}, the node holds {row.get(column)!r}"
        )
    var = f"{constants.META_PREFIX}pf_var_"
    execution = f"{constants.META_PREFIX}pf_exec_"
    return FunctionRun(
        function=str(result.get(f"{var}function_name")),
        version=str(result.get(f"{var}function_version")),
        body_hash=_hash_text(result.get(f"{var}function_content_hash")) or "",
        executor=str(result.get(f"{execution}executor_type")),
        python_version=str(result.get(f"{execution}python_version")),
        computed_at=_datetime(result.get(f"{constants.META_PREFIX}pod_ts")),
        computed_here=bool(entry.get(f"{constants.META_PREFIX}computed")),
        result_id=bytes(entry[constants.DATA_RECORD_ID]).hex(),
        entry_id=_hash_text(entry.get("__pipeline_entry_id")) or "",
        run_id=entry.get(_RUN_ID_COL),
        recorded_at=_datetime(entry.get(_RECORDED_AT_COL)),
    )


class _Tracer:
    def __init__(self) -> None:
        self.reader = _Reader()
        self.edges: set[tuple[str, str]] = set()

    def unread(
        self, node: Any, column: str, member: int | None, status: str
    ) -> TraceStep:
        """A step at a node whose rows are not held (``not_computed``) or not read
        (``unavailable``)."""
        return TraceStep(
            node=node.label,
            kind=_kind(node),
            operator=None,
            column=column,
            value=None,
            tags=(),
            member=member,
            status=status,
        )

    def leaf(
        self,
        source_node: Any,
        token: str,
        value: Any,
        *,
        held: bool,
        member: int | None,
        input_hash: str | None = None,
    ) -> TraceStep:
        """A source cell named by a token, checked against a held snapshot if any.

        Args:
            source_node: The job's source node.
            token: The cell's source-info token.
            value: The value the run held for the cell (``held``), or ``None``.
            held: Whether ``value`` is a value the run held; if not, the value is read
                from the snapshot, and the step is ``unavailable`` without one.
            member: The cell's position in a folded or merged list, or ``None``.
            input_hash: For a function that read the source directly, the input data
                hash its stored entry holds: the snapshot row must have that hash.
        """
        source_id, record_id, source_column = _parse_token(token)
        snapshot = self.reader.held(source_node)
        status = "traced"
        if snapshot is not None:
            key = ("records", id(source_node))
            if key not in self.reader.lookups:
                by_record: dict[tuple[Any, Any], list[Row]] = {}
                for r in snapshot:
                    for c in r:
                        if c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX):
                            pair = (r.get(_source_id_column(c)), r[c])
                            by_record.setdefault(pair, []).append(r)
                self.reader.lookups[key] = by_record
            rows = self.reader.lookups[key].get((source_id, record_id), [])
            if len(rows) != 1:
                raise ProvenanceInconsistencyError(
                    f"source {source_id!r} holds {len(rows)} rows with record id "
                    f"{record_id!r}"
                )
            if input_hash is not None and rows[0].get(_ROW_HASH) != input_hash:
                raise ProvenanceInconsistencyError(
                    f"source {source_id!r} record {record_id!r} is not the input the "
                    "stored entry was computed from (its content hash differs)"
                )
            if source_column in rows[0]:
                if not held:
                    value = rows[0][source_column]
                elif not same_value(rows[0][source_column], value):
                    raise ProvenanceInconsistencyError(
                        f"source {source_id!r} record {record_id!r} holds "
                        f"{source_column}={rows[0][source_column]!r}; the run held "
                        f"{value!r}"
                    )
        elif not held:
            status = "unavailable"
            value = None
        cell = SourceCell(source_node.label, source_id, record_id, source_column, value)
        return TraceStep(
            source_node.label,
            "source",
            None,
            source_column,
            value,
            (),
            member,
            status=status,
            source=cell,
        )

    def source_leaves(self, node: Any, index: int, cell: _Cell) -> list[TraceStep]:
        """The leaves of a cell for its input ``index``, a source node: the cell's
        tokens that name that source, each checked against the row's system tags."""
        upstream = node.upstreams[index]
        source_ids = _source_ids_of(upstream)
        token, value = cell.token, cell.value
        pairs: list[tuple[str, Any, int | None]] = []
        if isinstance(token, list):
            tokens = cast(list[Any], token)
            values = (
                cast(list[Any], value)
                if isinstance(value, list)
                else [None] * len(tokens)
            )
            pairs = [
                (t, v, k)
                for k, (t, v) in enumerate(zip(tokens, values))
                if isinstance(t, str)
            ]
        elif isinstance(token, str):
            pairs = [(token, value, None)]
        out: list[TraceStep] = []
        for t, v, k in pairs:
            sid, record_id, _ = _parse_token(t)
            if sid not in source_ids:
                continue
            if record_id not in _record_ids(cell.row, sid):
                raise ProvenanceInconsistencyError(
                    f"node {node.label!r}: the token {t!r} names a record the row's "
                    "system tags do not"
                )
            self.edges.add((upstream.label, node.label))
            out.append(self.leaf(upstream, t, v, held=True, member=k))
        return out

    def step(self, node: Any, cell: _Cell, member: int | None = None) -> TraceStep:
        kind = _kind(node)
        tags = _tags_of(node, cell.row)
        if kind == "source":
            token = cell.token
            if not isinstance(token, str):
                return self.unread(node, cell.column, member, "unavailable")
            return self.leaf(node, token, cell.value, held=True, member=member)
        if kind == "function":
            return self.function_step(node, cell, member, tags)
        producer = getattr(node, "producer", None)
        operator = type(producer).__name__ if producer is not None else None
        if _folding(node) is not None:
            return self.fold_step(node, cell, member, tags, operator)
        inputs: list[TraceStep] = []
        for index, upstream in enumerate(node.upstreams):
            if _kind(upstream) == "source":
                inputs.extend(self.source_leaves(node, index, cell))
                continue
            up_rows = self.reader.held(upstream)
            if up_rows is None:
                inputs.append(self.unread(upstream, cell.column, None, "not_computed"))
                continue
            matched = _input_rows(cell.row, node, index, up_rows, self.reader.lookups)
            if not matched:
                continue
            for up_cell, link_member in self.links(node, upstream, cell, matched):
                self.edges.add((upstream.label, node.label))
                inputs.append(self.step(upstream, up_cell, link_member))
        return TraceStep(
            node.label,
            kind,
            operator,
            cell.column,
            cell.value,
            tags,
            member,
            inputs=tuple(inputs),
        )

    def links(
        self, node: Any, upstream: Any, cell: _Cell, matched: list[Row]
    ) -> list[Link]:
        """The input cells one row-wise output cell came from, each checked by value.

        The token is matched exactly first; then as one element of a list token (an
        ``Index`` after a fold); then a list token's elements as the input's tokens
        (``MergeJoin``); only then as an access path (``Pick``).
        """
        token, value = cell.token, cell.value
        if token is None:
            return []
        prefix = constants.SOURCE_PREFIX
        data_columns = list(_keys(upstream)[1])

        def resolve(make: Callable[[Row, str], Link | None], what: str) -> list[Link]:
            found: list[Link] = []
            for column in data_columns:
                good = [(r, link) for r in matched if (link := make(r, column))]
                if not good:
                    continue
                row = _one(
                    [r for r, _ in good],
                    node,
                    f"{what} of {cell.column!r} in {upstream.label!r}",
                )
                found.append(next(link for r, link in good if r is row))
            return found

        def exact(r: Row, column: str) -> Link | None:
            if r.get(prefix + column) != token:
                return None
            up = _Cell(r, column)
            return (up, None) if same_value(up.value, value) else None

        found = resolve(exact, "the input cell")
        if found:
            return found
        if isinstance(token, str):

            def element(r: Row, column: str) -> Link | None:
                tokens = r.get(prefix + column)
                if not isinstance(tokens, list):
                    return None
                items = cast(list[Any], tokens)
                at = [k for k, t in enumerate(items) if t == token]
                if len(at) > 1:
                    chosen = getattr(node.producer, "i", None)
                    if not isinstance(chosen, int):
                        raise ProvenanceInconsistencyError(
                            f"node {node.label!r}: the token of {cell.column!r} is "
                            f"several elements of {column!r}; the link is ambiguous"
                        )
                    k = chosen % len(items)
                    at = [k] if k in at else []
                if len(at) != 1:
                    return None
                up = _Cell(r, column, at[0])
                return (up, None) if same_value(up.value, value) else None

            found = resolve(element, "the list element")
            if found:
                return found

            def path(r: Row, column: str) -> Link | None:
                up_token = r.get(prefix + column)
                if not isinstance(up_token, str):
                    return None
                if token.startswith(up_token + "[") or token.startswith(up_token + "."):
                    return (_Cell(r, column), None)
                return None

            return resolve(path, "the accessed value")
        if isinstance(token, list):
            values = cast(list[Any], value) if isinstance(value, list) else []
            pieces: list[Link] = []
            for k, piece_token in enumerate(cast(list[Any], token)):

                def piece(
                    r: Row, column: str, piece_token: Any = piece_token, k: int = k
                ) -> Link | None:
                    if r.get(prefix + column) != piece_token:
                        return None
                    up = _Cell(r, column)
                    if k < len(values) and not same_value(up.value, values[k]):
                        return None
                    return (up, k)

                pieces.extend(resolve(piece, f"element {k}"))
            return pieces
        return []

    def function_step(
        self,
        node: Any,
        cell: _Cell,
        member: int | None,
        tags: tuple[tuple[str, Any], ...],
    ) -> TraceStep:
        row, column = cell.row, cell.column
        upstream = node.upstreams[0]
        has_store = node._pipeline_database is not None  # pyright: ignore[reportPrivateUsage]  # a node with no database stores no entry
        entries = _entries_for(node, row, column, self.reader) if has_store else []
        if has_store and not entries:
            raise ProvenanceInconsistencyError(
                f"node {node.label!r}: no stored pipeline entry for the row "
                f"{dict(tags)!r}; the database holds other runs"
            )
        if _kind(upstream) == "source":
            # The function read the source directly: its input tokens are in the entry.
            run: FunctionRun | None = None
            leaves: list[TraceStep] = []
            if entries:
                entry = _latest(entries, node)
                run = _function_run(node, entry, row, column, self.reader)
                for name in sorted(
                    c for c in entry if c.startswith(constants.SOURCE_PREFIX)
                ):
                    token = entry[name]
                    if isinstance(token, str):
                        self.edges.add((upstream.label, node.label))
                        leaves.append(
                            self.leaf(
                                upstream,
                                token,
                                None,
                                held=False,
                                member=None,
                                input_hash=_hash_text(
                                    entry[constants.INPUT_DATA_HASH_COL]
                                ),
                            )
                        )
            return TraceStep(
                node.label,
                "function",
                None,
                column,
                cell.value,
                tags,
                member,
                function=run,
                inputs=tuple(leaves),
            )
        up_rows = self.reader.held(upstream)
        if up_rows is None:
            return TraceStep(
                node.label,
                "function",
                None,
                column,
                cell.value,
                tags,
                member,
                inputs=(self.unread(upstream, column, None, "not_computed"),),
            )
        matched = _input_rows(row, node, 0, up_rows, self.reader.lookups)
        if has_store:
            pairs = [
                (r, e)
                for r in matched
                for e in entries
                if _hash_text(e[constants.INPUT_DATA_HASH_COL]) == r.get(_ROW_HASH)
            ]
            up_row = _one(
                [r for r, _ in pairs], node, f"the input row in {upstream.label!r}"
            )
            entry = _latest(
                [e for r, e in pairs if r.get(_ROW_HASH) == up_row.get(_ROW_HASH)], node
            )
            for name, stored in entry.items():
                if (
                    name.startswith(constants.SOURCE_PREFIX)
                    and name in up_row
                    and up_row[name] != stored
                ):
                    raise ProvenanceInconsistencyError(
                        f"node {node.label!r}: the stored entry's input token {name} "
                        f"is {stored!r}, the input row's is {up_row[name]!r}"
                    )
            run = _function_run(node, entry, row, column, self.reader)
        else:
            up_row = _one(matched, node, f"the input row in {upstream.label!r}")
            run = None
        self.edges.add((upstream.label, node.label))
        inputs = tuple(
            self.step(upstream, _Cell(up_row, c)) for c in sorted(_keys(upstream)[1])
        )
        return TraceStep(
            node.label,
            "function",
            None,
            column,
            cell.value,
            tags,
            member,
            function=run,
            inputs=inputs,
        )

    def fold_step(
        self,
        node: Any,
        cell: _Cell,
        member: int | None,
        tags: tuple[tuple[str, Any], ...],
        operator: str | None,
    ) -> TraceStep:
        row, column = cell.row, cell.column
        upstream = node.upstreams[0]
        assignment = self.reader.fold(node)
        if assignment is None:
            status = (
                "unavailable"
                if _kind(upstream) == "source" or not _library_class(_folding(node))
                else "not_computed"
            )
            return TraceStep(
                node.label,
                "operator",
                operator,
                column,
                cell.value,
                tags,
                member,
                inputs=(self.unread(upstream, column, None, status),),
            )
        members = assignment[id(row)]
        values = row.get(column)
        tokens = row.get(constants.SOURCE_PREFIX + column)
        folding = _folding(node)
        if not isinstance(values, list) and column in getattr(folding, "by", ()):
            return self.key_step(node, cell, member, tags, operator, members)
        wanted = range(len(members)) if cell.element is None else [cell.element]
        inputs: list[TraceStep] = []
        for i in wanted:
            m = members[i]
            if not isinstance(values, list) or not same_value(
                cast(list[Any], values)[i], m.get(column)
            ):
                raise ProvenanceInconsistencyError(
                    f"node {node.label!r}: element {i} of {column!r} is not the value "
                    f"its member in {upstream.label!r} holds"
                )
            member_token = m.get(constants.SOURCE_PREFIX + column)
            if isinstance(tokens, list) and cast(list[Any], tokens)[i] != member_token:
                raise ProvenanceInconsistencyError(
                    f"node {node.label!r}: member {i} of {column!r} has token "
                    f"{cast(list[Any], tokens)[i]!r}, its input row {member_token!r}"
                )
            self.edges.add((upstream.label, node.label))
            inputs.append(self.step(upstream, _Cell(m, column), member=i))
        return TraceStep(
            node.label,
            "operator",
            operator,
            column,
            cell.value,
            tags,
            member,
            inputs=tuple(inputs),
        )

    def key_step(
        self,
        node: Any,
        cell: _Cell,
        member: int | None,
        tags: tuple[tuple[str, Any], ...],
        operator: str | None,
        members: list[Row],
    ) -> TraceStep:
        """A ``GroupBy`` key that is also data (a data-tag): one scalar, the value every
        member holds, whose token is the fold of the members' tokens in member order."""
        row, column = cell.row, cell.column
        upstream = node.upstreams[0]
        folding = _folding(node)
        prefix = constants.SOURCE_PREFIX
        for m in members:
            if not same_value(m.get(column), row.get(column)):
                raise ProvenanceInconsistencyError(
                    f"node {node.label!r}: the key {column!r} is "
                    f"{row.get(column)!r}, a member in {upstream.label!r} holds "
                    f"{m.get(column)!r}"
                )
        token = row.get(prefix + column)
        member_tokens = [m.get(prefix + column) for m in members]
        if token is not None and token != fold_system_tag_values(
            member_tokens, label=folding.fold_label
        ):
            raise ProvenanceInconsistencyError(
                f"node {node.label!r}: the token of the key {column!r} is not the fold "
                "of its members' tokens"
            )
        inputs: list[TraceStep] = []
        for i, m in enumerate(members):
            self.edges.add((upstream.label, node.label))
            inputs.append(self.step(upstream, _Cell(m, column), member=i))
        return TraceStep(
            node.label,
            "operator",
            operator,
            column,
            cell.value,
            tags,
            member,
            inputs=tuple(inputs),
        )


def _select_row(node: Any, rows: list[Row], row: Mapping[str, Any]) -> Row:
    """The one held row whose values are the selector's (``same_value``).

    Args:
        node: The node, for the messages.
        rows: The rows the node holds.
        row: Column values that must select exactly one row.

    Returns:
        The selected row.

    Raises:
        ValueError: If a selector column is not a column of the rows, or the values
            select no row or several rows.
    """
    known = set(rows[0]) if rows else set()
    unknown = sorted(k for k in row if k not in known)
    if rows and unknown:
        raise ValueError(
            f"node {node.label!r} has no column {unknown[0]!r} to select a row by"
        )
    found = [r for r in rows if all(same_value(r[k], v) for k, v in row.items())]
    if len(found) != 1:
        given = ", ".join(f"{k}={plain_repr(v)}" for k, v in sorted(row.items()))
        what = "no row" if not found else f"{len(found)} rows"
        raise ValueError(
            f"node {node.label!r} holds {what} with {given}; give columns that select "
            "one row"
        )
    return found[0]


def trace(node: Any, row: Mapping[str, Any], column: str) -> Trace:
    """Trace one cell of a job node back to the source cells it was made from.

    Args:
        node: A job node (function, operator or source node).
        row: Column values that select exactly one row the node holds (its tags,
            usually; system-tag columns may be given too), compared by Orcapod's strict
            value rules (``same_value``: a ``bool`` is never ``0``; NaN equals NaN).
        column: The data column of the cell.

    Returns:
        The trace. When the node holds no run its root has status ``not_computed``; a
        source node holds no rows unless its source keeps a snapshot in memory.

    Raises:
        ValueError: If ``column`` is not a data column of the node, or ``row`` names a
            column the node does not have or does not select exactly one row.
        ProvenanceInconsistencyError: If a link cannot be proven from what the job
            holds and stores, or is ambiguous.
    """
    if column not in _keys(node)[1]:
        raise ValueError(f"{column!r} is not a data column of node {node.label!r}")
    tracer = _Tracer()
    rows = tracer.reader.held(node)
    if rows is None:
        status = "unavailable" if _kind(node) == "source" else "not_computed"
        return Trace(root=tracer.unread(node, column, None, status))
    root = tracer.step(node, _Cell(_select_row(node, rows, row), column))
    return Trace(root=root, edges=tuple(sorted(tracer.edges)))


# ---------------------------------------------------------------------------
# why_missing
# ---------------------------------------------------------------------------


def _ancestry(node: Any) -> dict[int, Any]:
    found: dict[int, Any] = {}
    todo = [node]
    while todo:
        current = todo.pop()
        if id(current) in found:
            continue
        found[id(current)] = current
        todo.extend(u for u in getattr(current, "upstreams", ()) if u is not None)
    return found


def _source_ids(node: Any) -> list[str]:
    return sorted(
        {_source_id(n) for n in _ancestry(node).values() if _kind(n) == "source"}
    )


def _topological(nodes: list[Any]) -> list[Any]:
    depth: dict[int, int] = {}

    def of(n: Any) -> int:
        if id(n) not in depth:
            ups = [u for u in getattr(n, "upstreams", ()) if u is not None]
            depth[id(n)] = 1 + max((of(u) for u in ups), default=-1)
        return depth[id(n)]

    return sorted(nodes, key=lambda n: (of(n), n.label))


def _key_text(values: Mapping[str, Any]) -> str:
    return ", ".join(f"{k}={plain_repr(v)}" for k, v in sorted(values.items()))


class _NotHeld(Exception):
    """A node on the way holds no run (``not_computed``), or reads a source that is not
    read here (``unavailable``)."""

    def __init__(self, status: str = "not_computed") -> None:
        super().__init__(status)
        self.status = status


def why_missing(node: Any, source: str, record_id: str) -> Absence:
    """Say whether rows of one source record reach a job node, and if not, why.

    Args:
        node: A job node.
        source: The source id (as in source-info tokens and the source-id system tag).
        record_id: The record id (``row_<n>``, or the key columns as JSON).

    Returns:
        The answer about ``node``.

    Raises:
        ValueError: If no source with that id feeds the node.
        ProvenanceInconsistencyError: If a group's members cannot be proven.
    """
    ancestry = _ancestry(node)
    # A source is named by its own id, or by the id of the source a cache wraps.
    sources = [
        n
        for n in ancestry.values()
        if _kind(n) == "source" and source in _source_ids_of(n)
    ]
    if not sources:
        raise ValueError(f"no source with id {source!r} feeds node {node.label!r}")
    ids: set[str] = set().union(*(_source_ids_of(n) for n in sources))
    path = _topological(
        [n for n in ancestry.values() if any(id(s) in _ancestry(n) for s in sources)]
    )
    labels = tuple(n.label for n in path)
    on_path = {id(n) for n in path}
    reader = _Reader()
    # Per node: the held rows from the record; ``None`` for a source not read.
    carried: dict[int, list[Row] | None] = {}
    try:
        for n in path:
            carried[id(n)] = _carried(n, ids, record_id, reader, carried, on_path)
    except _NotHeld as unread:
        because = (
            "a node on the way holds no run"
            if unread.status == "not_computed"
            else "a step on the way reads a source that is not read here"
        )
        return Absence(
            node.label,
            source,
            record_id,
            True,
            unread.status,
            None,
            None,
            (),
            None,
            because,
            labels,
        )

    def holds(n: Any) -> bool:
        """Proven: the node holds a row from the record."""
        return bool(carried.get(id(n)))

    def may_hold(n: Any) -> bool:
        """Not disproven: it holds a row, or it is a source that is not read."""
        return holds(n) or (_kind(n) == "source" and carried.get(id(n)) is None)

    if _kind(node) == "source" and carried.get(id(node)) is None:
        return Absence(
            node.label,
            source,
            record_id,
            True,
            "unavailable",
            None,
            None,
            (),
            None,
            f"source {source!r} is not read here, so whether it holds the record is "
            "not known",
            labels,
        )
    present = tuple(n.label for n in path if holds(n))
    missing = not holds(node)
    function_node = next((n for n in path if _kind(n) == "function"), None)
    reached = _reached(function_node, ids, record_id, reader, carried)

    removed_by: Step | None = None
    because = ""
    edges: set[tuple[str, str]] = set()
    if missing:
        removed_by, because, blamed = _blame(
            node, source, record_id, reader, carried, on_path, may_hold
        )
        edges |= blamed
    for n in path:
        if holds(n):
            for u in n.upstreams:
                if id(u) in on_path and holds(u):
                    edges.add((u.label, n.label))
    return Absence(
        node=node.label,
        source=source,
        record_id=record_id,
        missing=missing,
        status="read",
        reached_function=reached,
        function_node=function_node.label if function_node is not None else None,
        present_in=present,
        removed_by=removed_by,
        because=because if missing else "",
        path=labels,
        edges=tuple(sorted(edges)),
    )


def _carried(
    n: Any,
    source: set[str],
    record_id: str,
    reader: _Reader,
    carried: dict[int, list[Row] | None],
    on_path: set[int],
) -> list[Row] | None:
    """The rows ``n`` holds from the record (``None``: a source not read)."""
    if _kind(n) == "source":
        snapshot = reader.held(n)
        if snapshot is None:
            return None
        return [r for r in snapshot if record_id in _record_ids(r, source)]
    rows = reader.held(n)
    if rows is None:
        raise _NotHeld
    kept: dict[int, Row] = {}
    if _folding(n) is not None:
        upstream = n.upstreams[0]
        assignment = reader.fold(n)
        up = carried.get(id(upstream))
        if assignment is None:
            # A fold read straight from a source that offers no snapshot: its members
            # are not held, and the folded rows cannot name the record.
            raise _NotHeld(
                "unavailable" if _kind(upstream) == "source" else "not_computed"
            )
        hits = None if up is None else {id(r) for r in up}
        for r in rows:
            members = assignment[id(r)]
            if any(
                (record_id in _record_ids(m, source))
                if hits is None
                else (id(m) in hits)
                for m in members
            ):
                kept[id(r)] = r
        return list(kept.values())
    for index, upstream in enumerate(n.upstreams):
        if id(upstream) not in on_path:
            continue
        up = carried.get(id(upstream))
        if up is None or _kind(upstream) == "source":
            for r in rows:
                if record_id in _record_ids(r, source):
                    kept[id(r)] = r
            continue
        for r in rows:
            if id(r) not in kept and _input_rows(r, n, index, up, reader.lookups):
                kept[id(r)] = r
    return list(kept.values())


def _reached(
    function_node: Any,
    source: set[str],
    record_id: str,
    reader: _Reader,
    carried: dict[int, list[Row] | None],
) -> bool | None:
    """The stored answer of the first function node on the way.

    Entries that name the source by its own system tags answer for every run. After a
    fold they name groups, by folded system tags: an entry for a group that holds the
    record now proves it reached the function; otherwise the stored answer is
    unavailable (``None``), never a guess.
    """
    if function_node is None or function_node._pipeline_database is None:  # pyright: ignore[reportPrivateUsage]
        return None
    entries, _ = reader.provenance(function_node)
    if not entries:
        return None
    if any(_names_source(e, source) for e in entries):
        return any(record_id in _record_ids(e, source) for e in entries)
    up = carried.get(id(function_node.upstreams[0]))
    if not up:
        return None
    columns = _system_tags(entries[0])
    keys = {tuple(r.get(c) for c in columns) for r in up}
    return (
        True if any(tuple(e.get(c) for c in columns) in keys for e in entries) else None
    )


def _blame(
    node: Any,
    source: str,
    record_id: str,
    reader: _Reader,
    carried: dict[int, list[Row] | None],
    on_path: set[int],
    holds: Callable[[Any], bool],
) -> tuple[Step | None, str, set[tuple[str, str]]]:
    """Why the record's rows do not reach ``node``: walk back from ``node`` to the step
    that received a row from the record and passed none on."""
    current = node
    while True:
        ups = [u for u in current.upstreams if id(u) in on_path]
        if any(holds(u) for u in ups):
            break
        lacking = [u for u in _topological(ups) if _kind(u) != "source"]
        if not lacking:
            break
        current = lacking[0]
    holding = [u for u in current.upstreams if id(u) in on_path and holds(u)]
    if not holding:
        if any(
            carried.get(id(s)) == [] for s in current.upstreams if _kind(s) == "source"
        ):
            return None, f"source {source!r} holds no record {record_id!r}", set()
        return None, "no step on the way passes it on", set()
    edges = {(u.label, current.label) for u in holding}
    position = list(current.upstreams).index(holding[0])
    producer = getattr(current, "producer", None)
    if _kind(current) == "function":
        step = Step(
            operator=type(producer).__name__ if producer is not None else "FunctionPod",
            label=current.label,
            text="no row was kept for this input (the function raised, or the run skipped it)",
        )
        return step, step.text, edges
    others = [
        ", ".join(_source_ids(u))
        for i, u in enumerate(current.upstreams)
        if i != position
    ]
    text = None
    if (
        producer is not None
        and _library_class(producer)
        and hasattr(producer, "rows_dropped_text")
    ):
        text = producer.rows_dropped_text(
            position, sorted(others), tuple(current.upstreams)
        )
    if carried.get(id(holding[0])) is None:
        # Straight after a source that is not read: whether the source holds the
        # record is not known, so the step is not blamed.
        because = (
            f"{current.label!r} holds no row from it, and source {source!r} is not read "
            "here, so whether it holds the record is not known"
        )
        if text is not None:
            because += f"; {current.label!r} keeps: {text}"
        return None, because, edges
    step = Step(
        operator=type(producer).__name__,
        label=current.label,
        text=text
        or (
            "its rows are not passed on"
            if producer is None or _library_class(producer)
            else "its rows are not passed on (a user-defined operator, not described)"
        ),
    )
    because = step.text
    held_up = carried[id(holding[0])] or []
    if len(current.upstreams) > 1 and held_up:
        carried_row = held_up[0]
        for i, other in enumerate(current.upstreams):
            if i == position:
                continue
            shared = [
                t
                for t in other.keys()[0]
                if t in carried_row and t in holding[0].keys()[0]
            ]
            other_rows = reader.held(other) or []
            key = {t: carried_row[t] for t in shared}
            if key and not any(
                all(t in o and same_value(o[t], v) for t, v in key.items())
                for o in other_rows
            ):
                because = (
                    f"no row with {_key_text(key)} in {other.label!r} "
                    f"(reads {', '.join(_source_ids(other))})"
                )
    return step, because, edges
