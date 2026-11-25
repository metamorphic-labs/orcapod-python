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
    # A subclass of a built-in type is compared as its base value (read through the
    # base type's own slots), so no subclass hook (__eq__, __int__, ...) ever runs.
    a, b = _plain_builtin(a), _plain_builtin(b)
    if isinstance(a, bool) or isinstance(b, bool):
        return type(a) is type(b) and a == b
    if isinstance(a, float) and isinstance(b, float):
        return (math.isnan(a) and math.isnan(b)) or a == b
    for kind in (list, tuple):
        if isinstance(a, kind) and isinstance(b, kind):
            items_a, items_b = cast(Sequence[Any], a), cast(Sequence[Any], b)
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


def _plain_builtin(value: Any) -> Any:
    """``value`` as its built-in base value when it is an instance of a subclass of
    ``int``, ``float``, ``str``, ``bytes``, ``list``, ``tuple`` or ``dict`` (read
    through the base type's slots, so no subclass method runs; a container's items
    stay as they are); any other value unchanged."""
    kind = type(value)
    if kind in (int, float, str, bytes, bool, list, tuple, dict):
        return value
    if isinstance(value, list):
        return list.__getitem__(cast(list[Any], value), slice(None))
    if isinstance(value, tuple):
        return tuple.__getitem__(cast(tuple[Any, ...], value), slice(None))
    if isinstance(value, dict):
        return dict(dict.items(cast(dict[Any, Any], value)))
    if isinstance(value, int):
        return int.__int__(value)
    if isinstance(value, float):
        return float.__float__(value)
    if isinstance(value, str):
        return str.__str__(value)
    if isinstance(value, bytes):
        return b"".join((value,))
    return value


# ---------------------------------------------------------------------------
# Node kinds and what a node holds
# ---------------------------------------------------------------------------


def _lists_hold(row: Row, members: list[Row], node: Any) -> bool:
    """Whether every list-valued data column of a fold's output ``row`` holds the
    members' values of that column, one element per member, in member order."""
    for column in _keys(node)[1]:
        value = row.get(column)
        if not isinstance(value, list) or not all(column in m for m in members):
            continue
        if not same_value(value, [m[column] for m in members]):
            return False
    return True


def _kind(node: Any) -> str:
    """A node's kind for provenance: ``"source"``, ``"function"`` or ``"operator"``.

    Decided by the node's class, never by its label.

    Args:
        node: A job node.

    Returns:
        The kind.
    """
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
    # Exact types: a subclass may fold otherwise (``fold`` then reports it unread).
    return producer if type(producer) in (GroupBy, Batch) else None


def _library_class(obj: Any) -> bool:
    """Whether ``obj`` is an instance of an Orcapod class itself, not of a subclass
    defined elsewhere: only such an object's methods are called (no user code)."""
    return type(obj).__module__.startswith("orcapod.")


def _keys(node: Any) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """A node's tag and data column names, from what it declares or holds.

    Never a schema prediction, which can reach a bound source's own ``output_schema``
    (user code): a source node answers from the schema it declares
    (``SourceNodeBase``); an operator node from the ``ArrowTableStream`` its run holds;
    a function node from the tag and data rows its run holds. A node that holds none
    has no columns here.
    """
    from orcapod.core.streams.arrow_table_stream import ArrowTableStream

    kind = _kind(node)
    if kind == "source":
        from orcapod.core.nodes.source_node import SourceNodeBase

        tags, data = SourceNodeBase.output_schema(node)
        return tuple(tags), tuple(data)
    if kind == "function":
        pair = next(iter(node._held_pairs()), None)  # pyright: ignore[reportPrivateUsage]  # the rows the node's run holds
        if pair is None:
            return (), ()
        tag, data = pair
        return tuple(tag.keys()), tuple(data.keys())
    stream = vars(node).get("_cached_output_stream")
    if type(stream) is not ArrowTableStream:
        return (), ()
    tags, data = stream.keys()
    return tuple(tags), tuple(data)


def _merge_slot(node: Any, index: int) -> int | None:
    """The element of a merged list that input ``index`` fills, or ``None``.

    ``MergeJoin`` merges a colliding column into ``[first argument's value, second
    argument's value]``: element ``i`` comes from argument ``i``, nothing else.
    """
    from orcapod.core.operators.merge_join import MergeJoin

    if type(getattr(node, "producer", None)) is MergeJoin:
        return index
    return None


def _static_source_ids(node: Any) -> set[str]:
    """The ids a source node's rows can carry, from stored structure only (no method or
    property of the source is called): the node's label (a slot is named by its
    source's name), and the id built from the source's stored name and its class's
    kind and version (``<kind>_v<N>:<name>``, ITL-1425); a ``CachedSource`` presents
    the id of the source it wraps, so that source's is read through it."""
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.source_ids import build_source_id

    ids = {node.label}
    bound = getattr(node, "bound_source", None)
    while type(bound) is CachedSource:
        bound = vars(bound).get("_source")
    if bound is not None:
        name = vars(bound).get("_source_name")
        kind = getattr(type(bound), "SOURCE_KIND", None)
        version = getattr(type(bound), "SOURCE_VERSION", None)
        if isinstance(name, str) and isinstance(kind, str) and isinstance(version, int):
            ids.add(build_source_id(kind, version, name))
    return ids


def _source_name(node: Any) -> str | None:
    """A source node's source name (stored, read through any ``CachedSource``; no method
    or property of the source is called), or ``None``."""
    from orcapod.core.sources.cached_source import CachedSource

    bound = getattr(node, "bound_source", None)
    while type(bound) is CachedSource:
        bound = vars(bound).get("_source")
    name = vars(bound).get("_source_name") if bound is not None else None
    return name if isinstance(name, str) else None


def _declared_system_tags(node: Any) -> list[str]:
    """A source node's system-tag columns, from the schema it declares."""
    from orcapod.core.nodes.source_node import SourceNodeBase

    tags, _ = SourceNodeBase.output_schema(node, columns={"system_tags": True})
    return sorted(c for c in tags if c.startswith(constants.SYSTEM_TAG_PREFIX))


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
    form = vars(node).get("_declared_form")
    if form is not None:
        # A narrower bind: the job reads the rows in the declared form (nullable
        # fields, the slot's system-tag names), so the snapshot is read in it too:
        # the same values and row hashes the job's rows have.
        raw = stream.as_table(
            columns={"system_tags": True, "source": True, "context": True}
        )
        declared = node._declared_stream(bound, form, raw)  # pyright: ignore[reportPrivateUsage]  # the node's own declared form, library code
        return declared.as_table(columns=_COLUMNS)
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


def _pairs(row: Mapping[str, Any], columns: Sequence[str]) -> set[tuple[Any, Any]]:
    """The ``(source id, record id)`` pairs a row holds in the given system-tag
    columns, each record-id column with the source-id column of the same name."""
    return {
        (row.get(_source_id_column(c)), row[c])
        for c in columns
        if c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX) and c in row
    }


def _positional_pairs(
    row: Row, node: Any, index: int, source_node: Any, reader: _Reader
) -> set[tuple[Any, Any]]:
    """The ``(source id, record id)`` a row of ``node`` holds for its input ``index``,
    a source node: read in the system-tag columns at that input's position (never by
    source id, which two inputs can share)."""
    mapping = _column_map(
        _system_tags(row),
        node,
        index,
        _declared_system_tags(source_node),
        _slot(row, node, index, reader),
    )
    return _pairs(row, list(mapping))


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
    if type(getattr(node, "producer", None)) is MergeJoin:
        return list(range(len(upstreams)))
    hexes = [u.pipeline_hash().to_hex() for u in upstreams]
    ranked = sorted(range(len(hexes)), key=lambda i: hexes[i])
    position = [0] * len(hexes)
    for rank, index in enumerate(ranked):
        position[index] = rank
    return position


def _column_map(
    out_columns: Sequence[str],
    node: Any,
    index: int,
    up_columns: Sequence[str],
    position: str | frozenset[str] | None = None,
) -> dict[str, str]:
    """Map the output's system-tag columns to those of input ``index`` they come from.

    A column passes unchanged, or gains one extension naming that input by its pipeline
    hash: ``::<hash>:<position>`` (a join: the position must be that input's own) or
    ``::<hash>`` (a fold).

    Args:
        out_columns: The output's system-tag columns.
        node: The node.
        index: The input.
        up_columns: The input's system-tag columns.
        position: The input's position in the names: one (``_slot`` resolved it for a
            row), or a set (any of tied inputs' positions, for a mapping of names
            only); by default the input's canonical position.
    """
    sep = constants.BLOCK_SEPARATOR
    upstream = node.upstreams[index]
    digest = upstream.pipeline_hash().to_hex()
    if position is None:
        position = str(_positions(node)[index]) if len(node.upstreams) > 1 else None
    allowed = {position} if isinstance(position, str) else position
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
            if colon and (allowed is None or at not in allowed):
                continue
            mapping[out] = up
            break
    return mapping


_Lookups = dict[Any, Any]
"""Indexes built once per call: column maps, and rows by their system-tag values."""


def _tied(node: Any, index: int) -> list[int]:
    """The inputs of a commutative join with the pipeline hash of input ``index``
    (itself included): their slots in the system-tag names are canonical, decided by
    the values they hold, not by argument order."""
    from orcapod.core.operators import Join

    if type(getattr(node, "producer", None)) is not Join or len(node.upstreams) < 2:
        return [index]
    hexes = [u.pipeline_hash().to_hex() for u in node.upstreams]
    return [j for j, h in enumerate(hexes) if h == hexes[index]]


def _tied_positions(node: Any, index: int) -> frozenset[str] | None:
    """The positions tied inputs share (any of them), or ``None`` when not tied."""
    tied = _tied(node, index)
    if len(tied) < 2:
        return None
    positions = _positions(node)
    return frozenset(str(positions[j]) for j in tied)


def _fits(node: Any, index: int, row: Row, position: str, reader: Any) -> bool:
    """Whether the system tags of ``row`` at ``position`` can be input ``index``'s: a
    held row of that input has those values, or, for a source, they name a record of
    it (its held snapshot, or else its stored ids)."""
    upstream = node.upstreams[index]
    if _kind(upstream) == "source":
        mapping = _column_map(
            _system_tags(row), node, index, _declared_system_tags(upstream), position
        )
        pairs = _pairs(row, list(mapping))
        if not pairs:
            return False
        held = reader.held_pairs(upstream)
        if held is not None:
            return pairs <= held
        ids = _static_source_ids(upstream)
        return all(sid in ids for sid, _ in pairs)
    up_rows = reader.held(upstream)
    if not up_rows:
        return False
    mapping = _column_map(
        _system_tags(row), node, index, _system_tags(up_rows[0]), position
    )
    if not mapping:
        return False
    return any(all(r[u] == row[o] for o, u in mapping.items()) for r in up_rows)


def _slot(row: Row, node: Any, index: int, reader: Any) -> str | None:
    """The position of input ``index`` in the system-tag names of ``row``.

    The canonical position, except for inputs of a commutative ``Join`` with tied
    pipeline hashes: their slots hold the tied inputs in an order decided by values
    (source and record ids), so each input's slot is the one whose values only it
    can have. A tie no values decide is refused.

    Raises:
        ProvenanceInconsistencyError: If tied inputs cannot be told apart.
    """
    tied = _tied(node, index)
    if len(tied) < 2:
        return None
    key = ("slot", id(node), id(row), index)
    if key in reader.lookups:
        return reader.lookups[key]
    positions = _positions(node)
    slots = sorted(str(positions[j]) for j in tied)
    fits = {j: [p for p in slots if _fits(node, j, row, p, reader)] for j in tied}
    # Search the assignments of slots to inputs that fit (each input its own slot),
    # stopping at a second one: one is the answer, none or several are refused.
    found: list[tuple[str, ...]] = []
    chosen: list[str] = []

    def search(k: int) -> None:
        if len(found) > 1:
            return
        if k == len(tied):
            found.append(tuple(chosen))
            return
        for slot in fits[tied[k]]:
            if slot not in chosen:
                chosen.append(slot)
                search(k + 1)
                chosen.pop()

    search(0)
    if len(found) != 1:
        raise ProvenanceInconsistencyError(
            f"node {node.label!r}: inputs {sorted(node.upstreams[j].label for j in tied)} "
            f"have one pipeline hash and {'several' if found else 'no'} ways to hold this "
            "row's values; "
            "the link is ambiguous"
        )
    for k, j in enumerate(tied):
        reader.lookups[("slot", id(node), id(row), j)] = found[0][k]
    return reader.lookups[key]


def _input_rows(
    row: Row, node: Any, index: int, up_rows: list[Row], reader: _Reader
) -> list[Row]:
    """The rows of input ``index`` that ``row`` names by its system tags (row-wise).

    The input's rows are indexed once per call by the mapped system-tag values, so a
    trace through many rows does not scan them again for each row.
    """
    if not up_rows:
        return []
    lookups = reader.lookups
    out_columns, up_columns = tuple(_system_tags(row)), tuple(_system_tags(up_rows[0]))
    position = _slot(row, node, index, reader)
    map_key = ("map", id(node), index, out_columns, up_columns, position)
    if map_key not in lookups:
        lookups[map_key] = _column_map(out_columns, node, index, up_columns, position)
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
    found = by_values.get(tuple(row[o] for o, _ in ordered), [])
    return [r for r in found if _tags_agree(node, row, r)]


def _passes_rows(node: Any, index: int) -> bool:
    """Whether the rows of input ``index`` flow on into the node's rows.

    A ``SemiJoin``'s right input only chooses which left rows pass: its rows never
    flow on, so nothing of it is matched against the node's rows (which carry the
    left input's system tags, maybe under the very names the right input has).
    """
    from orcapod.core.operators import SemiJoin

    return not (type(getattr(node, "producer", None)) is SemiJoin and index != 0)


def _tags_agree(node: Any, row: Row, up_row: Row) -> bool:
    """Whether an input row holds the user tags an output row keeps from it.

    A step that keeps a tag keeps its value: a tag of both rows must be equal. A
    renaming step's renamed names are not compared (a rename changes them).
    """
    from orcapod.core.operators import MapColumns, MapTags

    producer: Any = getattr(node, "producer", None)
    renamed: set[str] = set()
    if type(producer) in (MapTags, MapColumns):
        name_map = cast(dict[str, str], producer.name_map)
        renamed = set(name_map) | set(name_map.values())
    return all(
        same_value(row[t], up_row[t])
        for t in _keys(node)[0]
        if t in row and t in up_row and t not in renamed
    )


def _one(rows: list[Row], node: Any, what: str) -> Row:
    """The one row of ``rows``; several rows count as one only when identical in
    every value (data, tags, system tags, tokens), never by a data hash alone."""
    if not rows:
        raise ProvenanceInconsistencyError(f"node {node.label!r}: {what}: no row")
    hashes = {r.get(_ROW_HASH) for r in rows}
    identical = all(same_value(r, rows[0]) for r in rows[1:])
    if len(rows) > 1 and (len(hashes) > 1 or None in hashes or not identical):
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
    pair_sets: dict[int, set[tuple[Any, Any]] | None] = field(default_factory=dict)
    entry_rows: dict[int, list[Row]] = field(default_factory=dict)

    def held_pairs(self, node: Any) -> set[tuple[Any, Any]] | None:
        """The ``(source id, record id)`` pairs of the rows ``node`` holds, built once
        per call; ``None`` when its rows are not held."""
        key = id(node)
        if key not in self.pair_sets:
            rows = self.held(node)
            self.pair_sets[key] = (
                None
                if rows is None
                else {p for r in rows for p in _pairs(r, _system_tags(r))}
            )
        return self.pair_sets[key]

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
        """The rows ``node`` holds, as dictionaries, read once per call.

        Args:
            node: A job node.

        Returns:
            ``None`` when its rows are not read (no run, or a source whose rows are not
            held); ``[]`` for a completed run with no rows.
        """
        key = id(node)
        if key not in self.rows:
            table = self.table(node)
            self.rows[key] = None if table is None else table.to_pylist()
        return self.rows[key]

    def entries(self, node: Any) -> list[Row]:
        """A function node's own stored entries, without reading its results (what
        ``why_missing``'s stored answer needs); from ``provenance`` when already read."""
        key = id(node)
        if key in self.stored:
            return self.stored[key][0]
        if key not in self.entry_rows:
            entries, _ = node._stored_provenance(results=False)  # pyright: ignore[reportPrivateUsage]  # the provenance reader of the node, ITL-1432
            self.entry_rows[key] = [] if entries is None else entries.to_pylist()
        return self.entry_rows[key]

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
        if rows and (
            set(mapping) != set(_system_tags(rows[0]))
            or set(mapping.values()) != set(_system_tags(up_rows[0]) if up_rows else [])
            or len(set(mapping.values())) != len(mapping)
        ):
            # Every system-tag column must take part (source id and record id alike),
            # or a group would be trusted on part of its fold.
            raise ProvenanceInconsistencyError(
                f"node {node.label!r}: its system-tag columns do not map one to one "
                f"onto those of {upstream.label!r}, so its folds cannot be checked"
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
            member_rows = [up_rows[i] for i in members]
            if len(found) > 1:
                # Several held rows fold to these system tags (two cached versions of
                # one record): the one whose lists hold these members' values, one
                # element per member; still refused when several do.
                found = [
                    r
                    for r in found
                    if id(r) not in assigned and _lists_hold(r, member_rows, node)
                ]
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


PathStep = int | str
"""One level of a path into a cell's value: a list position, or a mapping key."""


def _at(value: Any, path: tuple[PathStep, ...]) -> Any:
    """The part of ``value`` at ``path`` (list positions and mapping keys, in order).

    Raises:
        ProvenanceInconsistencyError: If the path does not lead into the value.
    """
    for step in path:
        if isinstance(step, int) and isinstance(value, list):
            items = cast(list[Any], value)
            if not -len(items) <= step < len(items):
                raise ProvenanceInconsistencyError(
                    f"position {step} is outside a list of {len(items)}"
                )
            value = items[step]
        elif isinstance(step, str) and isinstance(value, dict):
            mapping = cast(dict[Any, Any], value)
            if step not in mapping:
                raise ProvenanceInconsistencyError(f"key {step!r} is not in the value")
            value = mapping[step]
        elif isinstance(step, str) and isinstance(value, list):
            # An Arrow map reads as a list of entries, ``{"key": k, "value": v}`` or
            # ``(k, v)``.
            found = [
                entry
                for entry in cast(list[Any], value)
                if (isinstance(entry, dict) and entry.get("key") == step)
                or (isinstance(entry, tuple) and len(entry) == 2 and entry[0] == step)
            ]
            if len(found) != 1:
                raise ProvenanceInconsistencyError(
                    f"key {step!r} is in {len(found)} entries of the value"
                )
            entry = found[0]
            value = entry["value"] if isinstance(entry, dict) else entry[1]
        else:
            raise ProvenanceInconsistencyError(
                f"the path step {step!r} does not lead into a {type(value).__name__}"
            )
    return value


@dataclass(frozen=True)
class _Cell:
    """A cell being traced: a row, a column, and a path into the column's value.

    The path's outer levels are the lists that operators built (a fold's members, a
    ``MergeJoin``'s arguments); the levels below them are positions or keys inside the
    source's own value (``Index``, ``Pick``). Walking upstream, each operator consumes
    the level it built and passes the rest on unchanged.
    """

    row: Row
    column: str
    path: tuple[PathStep, ...] = ()

    @property
    def whole(self) -> Any:
        """The column's whole value in the row."""
        return self.row.get(self.column)

    @property
    def whole_token(self) -> Any:
        """The column's whole source-info token in the row."""
        return self.row.get(constants.SOURCE_PREFIX + self.column)

    @property
    def value(self) -> Any:
        """The traced part of the value."""
        return _at(self.whole, self.path)

    @property
    def token(self) -> Any:
        """The token of the traced part: the token lists are followed as far as the
        path's list positions go (below them, a value inside one source cell)."""
        token = self.whole_token
        for step in self.path:
            if isinstance(step, int) and isinstance(token, list):
                token = cast(list[Any], token)[step]
            else:
                break
        return token


def _list_depth(cell: _Cell) -> int:
    """How many of a path's leading positions the column's token lists follow (the
    levels operators built); the rest of the path is inside one source cell."""
    token = cell.whole_token
    depth = 0
    for step in cell.path:
        if isinstance(step, int) and isinstance(token, list):
            token = cast(list[Any], token)[step]
            depth += 1
        else:
            break
    return depth


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
    """The one stored entry behind a held row; several are refused.

    The entries given have the row's system tags and name its result (and, past a
    function's input, the input row's hash). Entries for different inputs make the
    link ambiguous. Several entries for one input and one result (async execution
    can record two copies of an identical input, one reusing the other's result) each
    say something different (entry id, computed or reused, run, time recorded), and
    the held row does not record which one served it: never guessed (the highest
    index is not necessarily the one), refused.
    """
    inputs = {bytes(e[constants.INPUT_DATA_HASH_COL]) for e in entries}
    if len(inputs) > 1:
        raise ProvenanceInconsistencyError(
            f"node {node.label!r}: {len(inputs)} stored entries for different inputs "
            "have the row's system tags and result; the link is ambiguous"
        )
    distinct: dict[Any, Row] = {
        _hash_text(e.get("__pipeline_entry_id")) or id(e): e for e in entries
    }
    if len(distinct) > 1:
        raise ProvenanceInconsistencyError(
            f"node {node.label!r}: {len(distinct)} stored entries for one input have "
            "the row's system tags and result; which one served the row is not "
            "recorded"
        )
    return next(iter(distinct.values()))


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
    if column not in result:
        raise ProvenanceInconsistencyError(
            f"node {node.label!r}: the stored result holds no {column!r}"
        )
    if not same_value(result[column], row.get(column)):
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


def _input_columns(node: Any, index: int, column: str) -> list[str] | None:
    """The columns of input ``index`` that output ``column`` of a row-wise operator
    comes from, by the operator's own semantics (its stored arguments), never by
    matching tokens or values (they only verify a link).

    - Select, drop, filter, ``DropNulls``, ``AddTags``, ``TagBy``, ``Join``,
      ``Rejoin``, ``MergeJoin`` (a column of one input) and ``Index`` / ``Pick``
      (a column they do not select from): the same name, from an input that has it;
    - ``SemiJoin``: the same name, from its left input only;
    - ``MapData``, ``MapColumns`` and ``MapTags``: the renames each applies to the
      input's columns (its own plan, ``_plan_group``: a ``strict=False`` entry that
      names no column of its role is no rename); a name renamed away is no longer
      that column.

    Only these exact classes: a subclass may do anything, so its link is unavailable.

    Returns:
        The input columns (empty when this input gives none); ``None`` for an operator
        whose column mapping is not known here, so the link is unavailable.
    """
    from orcapod.core.operators import (
        AddTags,
        DropColumns,
        DropDataColumns,
        DropNulls,
        DropTagColumns,
        Filter,
        Index,
        Join,
        MapColumns,
        MapData,
        MapTags,
        MergeJoin,
        Pick,
        PolarsFilter,
        Rejoin,
        SelectColumns,
        SelectDataColumns,
        SelectTagColumns,
        SemiJoin,
        TagBy,
    )
    from orcapod.core.operators.mappers import _plan_group  # pyright: ignore[reportPrivateUsage]  # the mappers' own rename plan, ITL-1432

    producer: Any = getattr(node, "producer", None)
    kind = type(producer)
    tags, data_columns = _keys(node.upstreams[index])
    data = [c for c in data_columns if not c.startswith(constants.META_PREFIX)]
    same = [column] if column in data else []
    if kind is SemiJoin:
        return same if index == 0 else []
    if kind in (MapData, MapColumns, MapTags):
        name_map = cast(dict[str, str], producer.name_map)
        if kind is MapTags:
            # A tag rename reaches the data only for a data-tag, in both roles.
            renamed = (
                {}
                if producer.tag_only
                else _plan_group([c for c in tags if c in data], name_map, False)[0]
            )
            dropped: list[str] = []
        else:
            renamed, dropped = _plan_group(data, name_map, producer.drop_unmapped)
        old = {new: previous for previous, new in renamed.items()}
        if column in old:
            return [old[column]]
        if column in renamed or column in dropped:
            return []
        return same
    if kind in (
        Filter,
        PolarsFilter,
        DropNulls,
        SelectDataColumns,
        SelectTagColumns,
        SelectColumns,
        DropDataColumns,
        DropTagColumns,
        DropColumns,
        AddTags,
        TagBy,
        Join,
        Rejoin,
        MergeJoin,
        Index,
        Pick,
    ):
        return same
    return None


def _input_order(node: Any) -> list[int]:
    """The order a trace lists a row-wise node's inputs in: the argument order for an
    ordered operator (``MergeJoin``); for a commutative one (``Join``, ``Rejoin``) by
    pipeline hash, then by label, so swapping its arguments changes nothing."""
    from orcapod.core.operators import Join, Rejoin

    upstreams = list(node.upstreams)
    if type(getattr(node, "producer", None)) in (Join, Rejoin):
        return sorted(
            range(len(upstreams)),
            key=lambda i: (upstreams[i].pipeline_hash().to_hex(), upstreams[i].label),
        )
    return list(range(len(upstreams)))


def _merged_key(node: Any, column: str) -> bool:
    """Whether ``column`` is a data-tag several inputs of a join share: one key, with
    one merged token, whose value is every input's (as a tag join key)."""
    from orcapod.core.operators import Join, MergeJoin, Rejoin

    if type(getattr(node, "producer", None)) not in (Join, MergeJoin, Rejoin):
        return False
    sharing = [
        u for u in node.upstreams if column in _keys(u)[0] and column in _keys(u)[1]
    ]
    return len(sharing) > 1


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
        within: tuple[PathStep, ...] = (),
        input_hash: str | None = None,
        token_column: str | None = None,
    ) -> TraceStep:
        """A source cell named by a token, checked against a held snapshot if any.

        Args:
            source_node: The job's source node.
            token: The cell's source-info token.
            value: The value the run held for the traced part (``held``), or ``None``.
            held: Whether ``value`` is a value the run held; if not, the value is read
                from the snapshot, and the step is ``unavailable`` without one.
            member: The cell's position in a folded or merged list, or ``None``.
            within: The path inside the source cell's own value (``Index``, ``Pick``).
            input_hash: For a function that read the source directly, the input data
                hash its stored entry holds: the snapshot row must have that hash.
            token_column: For such a function, the source-info column the stored token
                was read from: the snapshot row's own token in that column must be
                the stored token (a token naming another cell of the same record
                is refused).
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
            if source_column not in rows[0]:
                # A token naming a column the source does not have links nothing (a
                # present column holding null is a value, compared below).
                raise ProvenanceInconsistencyError(
                    f"source {source_id!r} record {record_id!r} has no column "
                    f"{source_column!r}, which a stored token names"
                )
            if token_column is not None and token_column in rows[0]:
                own = rows[0][token_column]
                if own != token:
                    raise ProvenanceInconsistencyError(
                        f"source {source_id!r} record {record_id!r}: the stored token "
                        f"{token_column} is {token!r}, the source row's is {own!r}"
                    )
            stored = _at(rows[0][source_column], within)
            if not held:
                value = stored
            elif not same_value(stored, value):
                raise ProvenanceInconsistencyError(
                    f"source {source_id!r} record {record_id!r} holds "
                    f"{source_column}={stored!r}; the run held {value!r}"
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

    def to_source(
        self,
        node: Any,
        index: int,
        row: Row,
        token: Any,
        value: Any,
        within: tuple[PathStep, ...],
        member: int | None,
    ) -> list[TraceStep]:
        """The source cell that input ``index`` (a source node) gave a row of ``node``.

        ``token`` is the source cell's token. The input must be the one whose system
        tags, at its own position, name that token's record, and it must have the
        token's column; ``within`` is the path inside the source cell's value.
        """
        upstream = node.upstreams[index]
        if not isinstance(token, str):
            return []
        sid, record_id, source_column = _parse_token(token)
        if (sid, record_id) not in _positional_pairs(
            row, node, index, upstream, self.reader
        ) or source_column not in set(_keys(upstream)[1]):
            # The operator maps the cell to this input, yet the token names another
            # record or column: what the node holds contradicts the mapping.
            raise ProvenanceInconsistencyError(
                f"node {node.label!r}: the token {token!r} is not a cell of input "
                f"{upstream.label!r}, which the operator takes it from"
            )
        self.edges.add((upstream.label, node.label))
        return [
            self.leaf(upstream, token, value, held=True, member=member, within=within)
        ]

    def step(self, node: Any, cell: _Cell, member: int | None = None) -> TraceStep:
        """The step at ``node`` for ``cell``, and the steps below it.

        Args:
            node: The node holding ``cell``'s row.
            cell: A row the node holds, a column and a path into its value.
            member: The cell's position in its parent's folded or merged list.

        Returns:
            A source step (a leaf); a function, fold or row-wise operator step with the
            steps of the input cells it came from. A step whose input is not held is
            ``not_computed``; one whose link cannot be proven (a source not read, an
            operator that is not one of Orcapod's own) is ``unavailable``.
        """
        kind = _kind(node)
        tags = _tags_of(node, cell.row)
        if kind == "source":
            token = cell.token
            if not isinstance(token, str):
                return self.unread(node, cell.column, member, "unavailable")
            within = cell.path[_list_depth(cell) :]
            return self.leaf(
                node, token, cell.value, held=True, member=member, within=within
            )
        if kind == "function":
            return self.function_step(node, cell, member, tags)
        producer = getattr(node, "producer", None)
        operator = type(producer).__name__ if producer is not None else None
        if _folding(node) is not None:
            return self.fold_step(node, cell, member, tags, operator)
        if _merged_key(node, cell.column) and not cell.path:
            self.verify_merged_key(node, cell)
        inputs: list[TraceStep] = []
        for index in _input_order(node):
            upstream = node.upstreams[index]
            inputs.extend(self.input_steps(node, index, upstream, cell))
        if not inputs and cell.whole_token is not None:
            # No input claims the cell: its origin is not known here.
            inputs.append(self.unread(node, cell.column, None, "unavailable"))
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

    def input_steps(
        self, node: Any, index: int, upstream: Any, cell: _Cell
    ) -> list[TraceStep]:
        """The steps at input ``index`` of a row-wise operator that ``cell`` came from.

        Each operator consumes only the path level it built:

        - ``Index`` and ``Pick`` select inside their column: the input cell is that
          column, with the position or key put in front of the path;
        - a ``MergeJoin`` column of both inputs is ``[first, second]``: its outer level
          is the argument, so input ``i`` gives element ``i`` only;
        - any other column passes unchanged (the input's column whose whole token is the
          cell's), and so does the path.

        Every input cell is checked: it holds the value the cell holds.
        """
        from orcapod.core.operators.index import Index
        from orcapod.core.operators.pick import Pick

        producer: Any = getattr(node, "producer", None)
        column = cell.column
        if type(producer) in (Index, Pick) and column == (
            producer.out or producer.column
        ):
            selected = producer.i if type(producer) is Index else producer.key
            return self.from_input(
                node, index, upstream, cell, producer.column, selected
            )
        slot = _merge_slot(node, index)
        if slot is not None and self.merged(node, column):
            if cell.path and cell.path[0] != slot:
                return []  # this element is the other argument's
            whole = cell.whole_token
            if not isinstance(whole, list) or len(cast(list[Any], whole)) <= slot:
                return []
            piece = _Cell(cell.row, column, (slot, *cell.path[1:]))
            return self.passed_on(node, index, upstream, piece, slot, drop_front=True)
        return self.passed_on(node, index, upstream, cell, None, drop_front=False)

    def verify_merged_key(self, node: Any, cell: _Cell) -> None:
        """A key several inputs of a join share has one token: for ``Join`` and
        ``MergeJoin`` the fold of the inputs' own tokens (``join_v1``, sorted), for
        ``Rejoin`` the one token its inputs share (it keeps its origin row's columns).
        Rebuild it and compare. Inputs whose token is not known here (not held) leave
        it unchecked."""
        from orcapod.core.operators.join import _fold_members  # pyright: ignore[reportPrivateUsage]  # the join's own fold of merged tokens, ITL-1432

        tokens: list[str | None] = []
        sep = constants.BLOCK_SEPARATOR
        for index, upstream in enumerate(node.upstreams):
            up_tags, up_data = _keys(upstream)
            if cell.column not in up_tags or cell.column not in up_data:
                continue
            if _kind(upstream) == "source":
                pairs = _positional_pairs(cell.row, node, index, upstream, self.reader)
                if len(pairs) != 1:
                    return
                ((sid, record_id),) = pairs
                tokens.append(f"{sid}{sep}{record_id}{sep}{cell.column}")
                continue
            up_rows = self.reader.held(upstream)
            if up_rows is None:
                return
            matched = _input_rows(cell.row, node, index, up_rows, self.reader)
            found = {r.get(constants.SOURCE_PREFIX + cell.column) for r in matched}
            if len(found) != 1:
                return
            tokens.append(found.pop())
        from orcapod.core.operators import Rejoin

        if type(getattr(node, "producer", None)) is Rejoin:
            # Rejoin keeps its origin row's columns: the inputs' common token.
            if set(tokens) != {cell.whole_token}:
                raise ProvenanceInconsistencyError(
                    f"node {node.label!r}: the token of {cell.column!r} is not the "
                    "token its inputs share"
                )
            return
        if cell.whole_token != _fold_members(tokens):
            raise ProvenanceInconsistencyError(
                f"node {node.label!r}: the token of the key {cell.column!r} is not the "
                "join's fold of its inputs' tokens"
            )

    def merged(self, node: Any, column: str) -> bool:
        """Whether ``column`` is a column of both inputs of a ``MergeJoin`` (merged
        into ``[first, second]``)."""
        return not _merged_key(node, column) and all(
            column in _keys(u)[1] for u in node.upstreams
        )

    def from_input(
        self,
        node: Any,
        index: int,
        upstream: Any,
        cell: _Cell,
        input_column: str,
        selected: PathStep,
    ) -> list[TraceStep]:
        """``Index`` or ``Pick``: the cell is part of the input's ``input_column``."""
        if input_column not in _keys(upstream)[1]:
            return []
        # The output token is the input cell's token with the selection appended
        # (``x[0]``, ``x['a']``), or, when the input token is a list (one per member),
        # its element at the position.
        suffix = f"[{selected!r}]" if isinstance(selected, str) else f"[{selected}]"
        if _kind(upstream) == "source":
            token = cell.token
            if token is None:
                return []
            base = token[: -len(suffix)] if isinstance(token, str) else None
            if (
                base is None
                or not cast(str, token).endswith(suffix)
                or _parse_token(base)[2] != input_column
            ):
                raise ProvenanceInconsistencyError(
                    f"node {node.label!r}: the token {token!r} of {cell.column!r} is "
                    f"not {input_column + suffix!r}, which the operator selects"
                )
            within = (selected, *cell.path[_list_depth(cell) :])
            return self.to_source(node, index, cell.row, base, cell.value, within, None)
        up_rows = self.reader.held(upstream)
        if up_rows is None:
            return [self.unread(upstream, cell.column, None, "not_computed")]
        matched = _input_rows(cell.row, node, index, up_rows, self.reader)
        good: list[_Cell] = []
        for r in matched:
            whole = r.get(input_column)
            step_at = selected
            if isinstance(selected, int) and isinstance(whole, list):
                size = len(cast(list[Any], whole))
                step_at = selected + size if selected < 0 else selected
            up = _Cell(r, input_column, (step_at, *cell.path))
            input_token = up.whole_token
            expected = (
                cast(list[Any], input_token)[cast(int, step_at)]
                if isinstance(input_token, list)
                else f"{input_token}{suffix}"
            )
            if cell.whole_token != expected:
                continue  # the held output token is not the selection's
            try:
                if same_value(up.value, cell.value):
                    good.append(up)
            except ProvenanceInconsistencyError:
                continue
        if not good:
            if matched:
                raise ProvenanceInconsistencyError(
                    f"node {node.label!r}: {cell.column!r} is a part of "
                    f"{input_column!r} of {upstream.label!r}, whose held row holds "
                    "another value or token"
                )
            return []
        chosen = _one(
            [c.row for c in good],
            node,
            f"the input of {cell.column!r} in {upstream.label!r}",
        )
        up_cell = next(c for c in good if c.row is chosen)
        self.edges.add((upstream.label, node.label))
        return [self.step(upstream, up_cell)]

    def passed_on(
        self,
        node: Any,
        index: int,
        upstream: Any,
        cell: _Cell,
        slot: int | None,
        *,
        drop_front: bool,
    ) -> list[TraceStep]:
        """A column passed on from input ``index``: the input's column whose whole token
        is the cell's (one element of a merged list, ``slot``, when ``drop_front``)."""
        whole = cell.whole_token
        if drop_front:
            whole = cast(list[Any], whole)[cast(int, slot)]
        path = cell.path[1:] if drop_front else cell.path
        columns = _input_columns(node, index, cell.column)
        if columns is None:
            return [self.unread(node, cell.column, None, "unavailable")]
        if not columns:
            return []
        # A key several inputs share has one merged token: each input's own token is
        # not the cell's, so the link is verified by the value alone.
        key = _merged_key(node, cell.column)
        if whole is None and not key:
            return []
        if _kind(upstream) == "source":
            if key:
                pairs = _positional_pairs(cell.row, node, index, upstream, self.reader)
                if len(pairs) != 1:
                    return []
                ((sid, record_id),) = pairs
                sep = constants.BLOCK_SEPARATOR
                return self.to_source(
                    node,
                    index,
                    cell.row,
                    f"{sid}{sep}{record_id}{sep}{columns[0]}",
                    cell.value,
                    path,
                    slot,
                )
            token = whole
            depth = 0
            while (
                depth < len(path)
                and isinstance(path[depth], int)
                and isinstance(token, list)
            ):
                token = cast(list[Any], token)[cast(int, path[depth])]
                depth += 1
            if isinstance(token, str) and _parse_token(token)[2] not in columns:
                # A source token keeps the source's own column name (a rename does not
                # change it): it must be the column the operator maps from.
                raise ProvenanceInconsistencyError(
                    f"node {node.label!r}: the token {token!r} of {cell.column!r} "
                    f"names another column than {columns!r}, which the operator maps "
                    "it from"
                )
            return self.to_source(
                node, index, cell.row, token, cell.value, path[depth:], slot
            )
        up_rows = self.reader.held(upstream)
        if up_rows is None:
            return [self.unread(upstream, cell.column, None, "not_computed")]
        matched = _input_rows(cell.row, node, index, up_rows, self.reader)
        prefix = constants.SOURCE_PREFIX
        found: list[TraceStep] = []
        for up_column in columns:
            good: list[_Cell] = []
            for r in matched:
                if not key and r.get(prefix + up_column) != whole:
                    continue  # the token verifies the link
                up = _Cell(r, up_column, path)
                try:
                    if same_value(up.value, cell.value):
                        good.append(up)
                except ProvenanceInconsistencyError:
                    continue
            if not good:
                if any(up_column in r for r in matched):
                    raise ProvenanceInconsistencyError(
                        f"node {node.label!r}: {cell.column!r} comes from "
                        f"{up_column!r} of {upstream.label!r}, whose held row has "
                        "another token or value"
                    )
                continue
            chosen = _one(
                [c.row for c in good],
                node,
                f"the input of {cell.column!r} in {upstream.label!r}",
            )
            up_cell = next(c for c in good if c.row is chosen)
            self.edges.add((upstream.label, node.label))
            found.append(self.step(upstream, up_cell, slot))
        return found

    def fallen_through(
        self,
        node: Any,
        cell: _Cell,
        member: int | None,
        tags: tuple[tuple[str, Any], ...],
    ) -> TraceStep:
        """A data-tag that falls through a function pod: traced to the input row with
        the row's system tags, verified by its token and value."""
        upstream = node.upstreams[0]
        if _kind(upstream) == "source":
            token = cell.token
            if isinstance(token, str) and _parse_token(token)[2] != cell.column:
                raise ProvenanceInconsistencyError(
                    f"node {node.label!r}: the token {token!r} of the data-tag "
                    f"{cell.column!r} names another column; a data-tag falls through "
                    "under its own name"
                )
            inputs = self.to_source(
                node,
                0,
                cell.row,
                token,
                cell.value,
                cell.path[_list_depth(cell) :],
                None,
            )
        else:
            up_rows = self.reader.held(upstream)
            if up_rows is None:
                inputs = [self.unread(upstream, cell.column, None, "not_computed")]
            else:
                matched = [
                    r
                    for r in _input_rows(cell.row, node, 0, up_rows, self.reader)
                    if r.get(constants.SOURCE_PREFIX + cell.column) == cell.whole_token
                ]
                up_row = _one(matched, node, f"the input row in {upstream.label!r}")
                up_cell = _Cell(up_row, cell.column, cell.path)
                if not same_value(up_cell.value, cell.value):
                    raise ProvenanceInconsistencyError(
                        f"node {node.label!r}: the data-tag {cell.column!r} is not the "
                        f"value its input row in {upstream.label!r} holds"
                    )
                self.edges.add((upstream.label, node.label))
                inputs = [self.step(upstream, up_cell)]
        return TraceStep(
            node.label,
            "function",
            None,
            cell.column,
            cell.value,
            tags,
            member,
            inputs=tuple(inputs),
        )

    def function_step(
        self,
        node: Any,
        cell: _Cell,
        member: int | None,
        tags: tuple[tuple[str, Any], ...],
    ) -> TraceStep:
        row, column = cell.row, cell.column
        upstream = node.upstreams[0]
        up_tags, up_data = _keys(upstream)
        if column in up_tags and column in up_data:
            # A data-tag of the input falls through the function unchanged: it is
            # the input's cell, not a result (its token names the source cell).
            return self.fallen_through(node, cell, member, tags)
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
                # The entry's own system tags name the record it was computed for;
                # every input token it stores must name that record.
                own = _pairs(entry, _system_tags(entry))
                for name in sorted(
                    c for c in entry if c.startswith(constants.SOURCE_PREFIX)
                ):
                    token = entry[name]
                    if isinstance(token, str):
                        source_id, record_id, _ = _parse_token(token)
                        if (source_id, record_id) not in own:
                            raise ProvenanceInconsistencyError(
                                f"node {node.label!r}: the stored entry's input token "
                                f"{name} names {source_id!r} {record_id!r}, not the "
                                f"record its system tags name"
                            )
                        self.edges.add((upstream.label, node.label))
                        leaves.append(
                            self.leaf(
                                upstream,
                                token,
                                None,
                                held=False,
                                member=None,
                                token_column=name,
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
        matched = _input_rows(row, node, 0, up_rows, self.reader)
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
        """A ``GroupBy`` or ``Batch`` row: its lists' outer level is the members.

        A path's first position is a member (only that member is traced, with the rest
        of the path); without a path every member is traced. Each member must hold the
        list's element and token at its position.
        """
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
        if cell.path:
            first = cell.path[0]
            if not isinstance(first, int):
                raise ProvenanceInconsistencyError(
                    f"node {node.label!r}: {column!r} is a list of members; "
                    f"{first!r} is not a position"
                )
            wanted = [first + len(members) if first < 0 else first]
            rest = cell.path[1:]
        else:
            wanted = list(range(len(members)))
            rest = ()
        # One element per member, in the value and in its parallel token list.
        if not isinstance(values, list) or len(cast(list[Any], values)) != len(members):
            raise ProvenanceInconsistencyError(
                f"node {node.label!r}: {column!r} does not hold one element per member "
                f"of its group ({len(members)} members)"
            )
        if isinstance(tokens, list) and len(cast(list[Any], tokens)) != len(members):
            raise ProvenanceInconsistencyError(
                f"node {node.label!r}: the tokens of {column!r} are not one per member "
                f"of its group ({len(members)} members)"
            )
        if any(not -len(members) <= i < len(members) for i in wanted):
            raise ProvenanceInconsistencyError(
                f"node {node.label!r}: position {wanted[0]} is outside its group "
                f"of {len(members)} members"
            )
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
            inputs.append(self.step(upstream, _Cell(m, column, rest), member=i))
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
            inputs.append(self.step(upstream, _Cell(m, column, cell.path), member=i))
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


def _snapshot(value: Any) -> Any:
    """A copy of a selector value's lists, tuples and mappings, at every depth, read
    through the base types' own methods (``_plain_builtin``: no subclass method is
    called); other values as given."""
    value = _plain_builtin(value)
    if type(value) is list:
        return [_snapshot(v) for v in cast(list[Any], value)]
    if type(value) is tuple:
        return tuple(_snapshot(v) for v in cast(tuple[Any, ...], value))
    if type(value) is dict:
        return {k: _snapshot(v) for k, v in cast(dict[Any, Any], value).items()}
    return value


def _select_rows(
    node: Any, rows: list[Row], row: Mapping[str, Any], known: set[str]
) -> list[Row]:
    """The held rows whose values are the selector's (``same_value``), in tag order.

    Args:
        node: The node, for the messages.
        rows: The rows the node holds.
        row: Column values; none selects every row.
        known: The node's column names (of its held table, which a run with no rows
            still has).

    Returns:
        The matching rows (any number), ordered by the node's tags (then by every
        column), so the order never follows the order the rows were held in.

    Raises:
        ValueError: If a selector column is not a column of the node.
    """
    unknown = sorted(k for k in row if k not in known)
    if unknown:
        raise ValueError(
            f"node {node.label!r} has no column {unknown[0]!r} to select a row by"
        )
    found = [r for r in rows if all(same_value(r[k], v) for k, v in row.items())]
    tags = list(_keys(node)[0])

    def place(value: Any) -> tuple[int, Any]:
        # A total order, as GroupBy's: values, then NaN, then null; inside a list
        # (a Batch's list-valued tag) element by element, by the same rule.
        if value is None:
            return (2, 0)
        if isinstance(value, float) and value != value:
            return (1, 0)
        if isinstance(value, list):
            return (0, tuple(place(v) for v in cast(list[Any], value)))
        return (0, value)

    def typed(r: Row) -> tuple[Any, ...]:
        return tuple(place(r.get(t)) for t in tags)

    def text(r: Row) -> tuple[str, ...]:
        return tuple(repr(r.get(k)) for k in (*tags, *sorted(r)))

    try:
        return sorted(found, key=lambda r: (typed(r), text(r)))
    except TypeError:  # a tag type with no order (a struct): by its text
        return sorted(found, key=text)


def trace(node: Any, row: Mapping[str, Any], column: str) -> Trace:
    """Trace cells of a job node back to the source cells they were made from.

    Args:
        node: A job node (function, operator or source node).
        row: Column values selecting the held rows to trace (its tags, usually;
            system-tag columns may be given too), compared by Orcapod's strict value
            rules (``same_value``: a ``bool`` is never ``0``; NaN equals NaN). Any
            number of rows may match; none given selects every held row.
        column: The data column of the cells.

    Returns:
        The trace, one root per matching row (none: status ``no_match``). When the
        node holds no run its one root has status ``not_computed``; a source node
        holds no rows unless its source keeps a snapshot in memory.

    Raises:
        ValueError: If ``column`` is not a data column of the node, or ``row`` names a
            column the node does not have.
        ProvenanceInconsistencyError: If a link cannot be proven from what the job
            holds and stores, or is ambiguous.
    """
    tracer = _Tracer()
    # A snapshot, as given: shown in the header and matched by value, so a caller
    # changing its list afterwards changes neither the match nor the header.
    row = {name: _snapshot(value) for name, value in row.items()}
    selector = tuple(row.items())
    rows = tracer.reader.held(node)
    if rows is None:
        status = "unavailable" if _kind(node) == "source" else "not_computed"
        return Trace(
            node=node.label,
            column=column,
            selector=selector,
            roots=(tracer.unread(node, column, None, status),),
            matched=None,
        )
    if column not in _keys(node)[1]:
        table = tracer.reader.table(node)
        # A completed function run with no rows holds no row to read its columns
        # from: its data columns are the held table's columns that are not its
        # input's tags (a function passes its input's tags) nor system columns (a
        # data column may start with "_": only the reserved prefixes are system).
        tags = set(_keys(node.upstreams[0])[0]) if _kind(node) == "function" else set()
        held_column = (
            not rows
            and _kind(node) == "function"
            and table is not None
            and column in table.column_names
            and column not in tags
            and not column.startswith(
                (
                    constants.SYSTEM_TAG_PREFIX,
                    constants.SOURCE_PREFIX,
                    constants.META_PREFIX,
                    constants.CONTEXT_KEY,
                    _ROW_HASH,
                )
            )
        )
        if not held_column:
            raise ValueError(f"{column!r} is not a data column of node {node.label!r}")
    table = tracer.reader.table(node)
    if rows:
        known = set(rows[0])
    elif table is not None:
        known = set(table.column_names)
    else:
        known = {*_keys(node)[0], *_keys(node)[1]}
    found = _select_rows(node, rows, row, known)
    roots = tuple(tracer.step(node, _Cell(r, column)) for r in found)
    return Trace(
        node=node.label,
        column=column,
        selector=selector,
        roots=roots,
        matched=len(found),
        edges=tuple(sorted(tracer.edges)),
    )


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


def _source_labels(node: Any) -> list[str]:
    """The labels of the source nodes ``node`` reads, sorted."""
    return sorted({n.label for n in _ancestry(node).values() if _kind(n) == "source"})


def _consumers(source_node: Any, nodes: Sequence[Any]) -> list[tuple[Any, int]]:
    """The nodes of ``nodes`` reading ``source_node`` directly, with its input index."""
    return [
        (n, i)
        for n in nodes
        for i, u in enumerate(getattr(n, "upstreams", ()))
        if u is source_node
    ]


def _emitted_ids(source_node: Any, nodes: Sequence[Any], reader: _Reader) -> set[Any]:
    """The source ids a source node's rows carry, read from held rows only: its
    snapshot, and its direct consumers' rows at its input position. A cache keeps the
    ids of the source it wraps."""
    ids: set[Any] = set()
    snapshot = reader.held(source_node)
    if snapshot is not None:
        for r in snapshot:
            ids |= {sid for sid, _ in _pairs(r, _system_tags(r))}
    for consumer, index in _consumers(source_node, nodes):
        if not _passes_rows(consumer, index):
            continue  # its rows carry the other input's ids
        if _folding(consumer) is not None:
            continue  # its rows hold folds, not ids
        if _kind(consumer) != "source" and consumer.is_stale:
            continue
        for r in reader.held(consumer) or []:
            ids |= {
                sid
                for sid, _ in _positional_pairs(r, consumer, index, source_node, reader)
            }
    return ids


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

    def __init__(self, status: str = "not_computed", because: str = "") -> None:
        super().__init__(status)
        self.status = status
        self.because = because or (
            "a node on the way holds no run"
            if status == "not_computed"
            else "a step on the way reads a source that is not read here"
        )


def why_missing(node: Any, source: str, record_id: str) -> Absence:
    """Say whether rows of one source record reach a job node, and if not, why.

    A source is named by the id its rows carry (as in source-info tokens and the
    source-id system tag), or by its source node's label. Rows are matched to a source
    by the system tags at that source's input position, never by id alone: two inputs
    can carry one id (a cache keeps the id of the source it wraps).

    Args:
        node: A job node.
        source: A source node's label, a source's name or its full id; it must name
            exactly one source (several are refused, never combined).
        record_id: The record id (``row_<n>``, or the key columns as JSON).

    Returns:
        The answer about ``node``.

    Raises:
        ValueError: If no source with that label, name or id feeds the node, or it
            names several.
        ProvenanceInconsistencyError: If a group's members cannot be proven.
    """
    ancestry = _ancestry(node)
    reader = _Reader()
    if _kind(node) == "source" and reader.held(node) is None:
        # A source node whose rows are not held: nothing here says which records,
        # or which ids, it holds.
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
            f"source {node.label!r} is not read here, so whether it holds the record "
            "is not known",
            (node.label,),
        )
    all_nodes = list(ancestry.values())
    # One rule: the source nodes whose label, name or full id is ``source``. Exactly
    # one is the answer's source; several are refused, never combined (two bindings'
    # rows are never evidence for one another).
    candidates: list[tuple[Any, set[Any]]] = []
    for n in all_nodes:
        if _kind(n) != "source":
            continue
        ids = (_static_source_ids(n) - {n.label}) | _emitted_ids(n, all_nodes, reader)
        if source == n.label or source == _source_name(n) or source in ids:
            candidates.append((n, ids))
    if not candidates:
        raise ValueError(f"no source with id {source!r} feeds node {node.label!r}")
    if len(candidates) > 1:
        named = "; ".join(
            f"node {n.label!r} (id {', '.join(sorted(map(str, ids))) or 'unknown'})"
            for n, ids in sorted(candidates, key=lambda c: c[0].label)
        )
        raise ValueError(
            f"{source!r} names several sources feeding node {node.label!r}: {named}; "
            "give the node label or the full id of one of them"
        )
    chosen, chosen_ids = candidates[0]
    sources: list[Any] = [chosen]
    # A node label or source name asks about every id the slot's rows carry (a slot
    # can carry several: a stream of two sources' rows); a full id asks about that id
    # only, so another id's record of the same number is never taken for it.
    slot_wide = source in (chosen.label, _source_name(chosen))
    if slot_wide and source in chosen_ids and chosen_ids != {source}:
        # Two meanings at one node: the slot (every id it carries) and one of its ids.
        # Refused, never one picked.
        carried_ids = ", ".join(sorted(map(str, chosen_ids)))
        raise ValueError(
            f"{source!r} is both the label of source node {chosen.label!r} (every id "
            f"its rows carry: {carried_ids}) and one of those full source ids, so it "
            "has two meanings here; give the slot's source another name (so its "
            "label is none of its ids), or ask by one of its other ids"
        )
    ids = (chosen_ids or {source}) if slot_wide else {source}
    wanted: dict[int, set[tuple[Any, Any]]] = {
        id(chosen): {(sid, record_id) for sid in ids}
    }
    path = _topological(
        [n for n in all_nodes if any(id(s) in _ancestry(n) for s in sources)]
    )
    labels = tuple(n.label for n in path)
    on_path = {id(n) for n in path}
    # Per node: the held rows from the record; ``None`` for a source not read.
    carried: dict[int, list[Row] | None] = {}
    try:
        for n in path:
            carried[id(n)] = _carried(n, wanted, reader, carried, on_path)
    except _NotHeld as unread:
        because = unread.because
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
    reached, recorded = _reached(function_node, sources, wanted, reader, carried)

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
            for index, u in enumerate(n.upstreams):
                if id(u) in on_path and holds(u) and _passes_rows(n, index):
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
        entry_recorded=recorded,
    )


def _carried(
    n: Any,
    wanted: dict[int, set[tuple[Any, Any]]],
    reader: _Reader,
    carried: dict[int, list[Row] | None],
    on_path: set[int],
) -> list[Row] | None:
    """The rows ``n`` holds from the record (``None``: a source not read).

    ``wanted`` gives, per matching source node, the ``(source id, record id)`` pairs
    that name the record.
    """
    if _kind(n) == "source":
        snapshot = reader.held(n)
        if snapshot is None:
            return None
        pairs = wanted.get(id(n), set())
        return [r for r in snapshot if _pairs(r, _system_tags(r)) & pairs]
    rows = reader.held(n)
    if rows is None:
        raise _NotHeld
    if (
        _kind(n) == "operator"
        and _folding(n) is None
        and _input_columns(n, 0, "") is None
    ):
        # An operator that is not one of Orcapod's own (a user operator, or a subclass
        # of one, which may do anything): which rows come from which is not known.
        raise _NotHeld(
            "unavailable",
            f"{n.label!r} is not one of Orcapod's own operators, so which of its rows "
            "come from the record is not known",
        )
    kept: dict[int, Row] = {}
    if _folding(n) is not None:
        upstream = n.upstreams[0]
        assignment = reader.fold(n)
        if assignment is None:
            # A fold read straight from a source that offers no snapshot (or with a
            # user fold rule): its members are not held.
            raise _NotHeld(
                "unavailable"
                if _kind(upstream) == "source" or not _library_class(_folding(n))
                else "not_computed"
            )
        hits = {id(r) for r in carried.get(id(upstream)) or []}
        for r in rows:
            if any(id(m) in hits for m in assignment[id(r)]):
                kept[id(r)] = r
        return list(kept.values())
    for index, upstream in enumerate(n.upstreams):
        if id(upstream) not in on_path or not _passes_rows(n, index):
            continue
        if _kind(upstream) == "source":
            pairs = wanted.get(id(upstream), set())
            for r in rows:
                if _positional_pairs(r, n, index, upstream, reader) & pairs:
                    kept[id(r)] = r
            continue
        up = carried.get(id(upstream)) or []
        for r in rows:
            if id(r) not in kept and _input_rows(r, n, index, up, reader):
                kept[id(r)] = r
    return list(kept.values())


def _source_columns(
    source_node: Any,
    node: Any,
    names: Sequence[str],
    reader: _Reader,
    tied: list[bool] | None = None,
) -> set[str] | None:
    """The system-tag columns of ``node`` (named ``names``) that come from
    ``source_node``, followed through the column names of the held nodes between.

    Args:
        source_node: The source node.
        node: The node whose columns are named.
        names: The node's system-tag columns.
        reader: The call's reader.
        tied: When given, ``True`` is appended when the way crosses a commutative
            ``Join`` whose input is tied with another (same pipeline hash): the
            columns then include the other input's slots too, and names alone cannot
            tell the two apart.

    Returns:
        The column names; ``None`` when a node between holds no run.
    """
    if node is source_node:
        return set(_declared_system_tags(source_node))
    found: set[str] = set()
    for index, upstream in enumerate(node.upstreams):
        if id(source_node) not in _ancestry(upstream) or not _passes_rows(node, index):
            continue
        if upstream is source_node:
            up_columns: set[str] | None = set(_declared_system_tags(source_node))
        else:
            table = reader.table(upstream)
            if table is None:
                return None
            up_names = _system_tags(dict.fromkeys(table.column_names))
            up_columns = _source_columns(source_node, upstream, up_names, reader, tied)
        if up_columns is None:
            return None
        positions = _tied_positions(node, index)
        if positions is not None and tied is not None:
            tied.append(True)
        found |= set(_column_map(names, node, index, sorted(up_columns), positions))
    return found


def _reached(
    function_node: Any,
    sources: list[Any],
    wanted: dict[int, set[tuple[Any, Any]]],
    reader: _Reader,
    carried: dict[int, list[Row] | None],
) -> tuple[bool | None, bool | None]:
    """The stored answer of the first function node on the way.

    A function node stores an entry for each input it ran successfully (or served from
    its store). The entries' system-tag columns that come from each matching source (by
    position) name its records directly. After a fold they hold folded values: an entry
    for a group that holds the record now proves it reached the function.

    A stored entry proves the input reached the function. No entry proves nothing: the
    function may have raised on the input (a failure stores no entry) or never received
    it. So the answer is never "did not reach".

    Returns:
        ``(reached, recorded)``: ``reached`` is ``True`` when a stored entry proves the
        input reached the function, else ``None`` (unknown); ``recorded`` is ``True``
        when such an entry is stored, ``False`` when the entries name the source but
        none this record, ``None`` when the entries cannot tell (a fold not held, tied
        inputs, no entries).
    """
    if function_node is None or function_node._pipeline_database is None:  # pyright: ignore[reportPrivateUsage]
        return None, None
    entries = reader.entries(function_node)
    if not entries:
        return None, None
    names = _system_tags(entries[0])
    direct = False
    unknown = False
    crosses_tie = False
    for source_node in sources:
        tied: list[bool] = []
        columns = _source_columns(source_node, function_node, names, reader, tied)
        crosses_tie = crosses_tie or bool(tied)
        if not columns:
            continue
        pairs = wanted[id(source_node)]
        ids = {sid for sid, _ in pairs}
        named = [_pairs(e, sorted(columns)) for e in entries]
        if any(sid in ids for found in named for sid, _ in found):
            direct = True
            if any(found & pairs for found in named):
                if not tied:
                    return True, True
                # The columns include a tied input's slots: a stored entry names the
                # record, but maybe through the other input; it cannot tell.
                unknown = True
    # The held run's witness first: an entry for an input row the run holds from the
    # record (a group that holds it, say) proves receipt, whatever the direct names.
    up = carried.get(id(function_node.upstreams[0]))
    if up and not crosses_tie:
        # A witness is an entry for the very input row the run holds: its system tags
        # AND its input data hash. Through a Join of tied inputs the system tags are
        # canonical by value and cannot name the binding, so no witness is taken.
        keys = {(tuple(r.get(c) for c in names), r.get(_ROW_HASH)) for r in up}
        if any(
            (
                tuple(e.get(c) for c in names),
                _hash_text(e.get(constants.INPUT_DATA_HASH_COL)),
            )
            in keys
            for e in entries
        ):
            return True, True
    if unknown:
        return None, None
    if direct:
        return None, False
    return None, None


def _missing_key_text(
    node: Any, position: int, holding: Any, carried_row: Row, reader: _Reader
) -> str | None:
    """Which other inputs of a join prove the carried row's key absent.

    Only an input whose held rows lack the key proves it; an input whose rows are not
    held (an unread source) is unknown, never a miss. Every proving input is named, in
    the join's canonical order (by pipeline hash, then label); when none proves it, the
    inputs not read are named. Column names come from what the nodes declare or hold
    (no source is called).

    Returns:
        The reason in words, or ``None`` when no input shares a key with the row.
    """
    proving: list[str] = []
    unread: list[str] = []
    null_keys: dict[str, None] = {}
    holding_tags = set(_keys(holding)[0])
    # Each operator's own equality: Rejoin matches a null with a null in a shared
    # column (outside the system tags); Join, MergeJoin and SemiJoin match a null key
    # with nothing, so a null key is the reason, not a peer lacking a row.
    nulls_match = type(getattr(node, "producer", None)).__name__ == "Rejoin"
    for i in _input_order(node):
        if i == position:
            continue
        other = node.upstreams[i]
        shared = [t for t in _keys(other)[0] if t in carried_row and t in holding_tags]
        if not shared:
            continue
        key = {t: carried_row[t] for t in shared}
        if not nulls_match and any(v is None for v in key.values()):
            null_keys.update((t, None) for t, v in key.items() if v is None)
            continue
        rows = reader.held(other)
        if rows is None:
            unread.append(other.label)
            continue
        if not any(
            all(t in o and same_value(o[t], v) for t, v in key.items()) for o in rows
        ):
            proving.append(
                f"no row with {_key_text(key)} in {other.label!r} "
                f"(reads {', '.join(_source_labels(other))})"
            )
    if null_keys:
        names = ", ".join(sorted(null_keys))
        return f"its key {names} is null, and a null key matches nothing in a join"
    if proving:
        return "; ".join(proving)
    if unread:
        names = ", ".join(repr(label) for label in unread)
        return (
            "no held input proves the key missing; the rows of "
            f"{names} are not read here"
        )
    return None


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
    if _kind(node) == "source":
        return None, f"source {node.label!r} holds no record {record_id!r}", set()
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
    # By identity: node equality compares identities, which can reach a source.
    position = next(i for i, u in enumerate(current.upstreams) if u is holding[0])
    producer = getattr(current, "producer", None)
    if _kind(current) == "function" and carried.get(id(holding[0])) is None:
        # A function straight after a source that is not read: that it received the
        # record is not established, so it is not blamed.
        return (
            None,
            f"{current.label!r} holds no row from it, and source {source!r} is not read "
            "here, so whether it holds the record is not known",
            edges,
        )
    if _kind(current) == "function":
        step = Step(
            operator=type(producer).__name__ if producer is not None else "FunctionPod",
            label=current.label,
            text="no row was kept for this input (the function raised, or the run skipped it)",
        )
        return step, step.text, edges
    others = [
        ", ".join(_source_labels(u))
        for i, u in enumerate(current.upstreams)
        if i != position
    ]
    text = None
    names = ", ".join(plain_repr(name) for name in sorted(others))
    if not _passes_rows(current, position):
        # A SemiJoin's right input: its rows only choose and never flow on (in
        # explain()'s words, built here from what the nodes hold: no input's keys()).
        text = f"its rows only choose rows of {names} and do not flow on"
    elif type(producer).__name__ == "SemiJoin":
        text = f"rows without a match in {names} are dropped"
    elif (
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
    if len(current.upstreams) > 1 and held_up and _passes_rows(current, position):
        # One reason per held row from the record (a cache can hold several versions
        # of it), each once, in text order: never the order the rows were held in. A
        # SemiJoin's right input is explained by its row-flow rule alone.
        reasons = {
            reason
            for row in held_up
            if (reason := _missing_key_text(current, position, holding[0], row, reader))
            is not None
        }
        if reasons:
            because = "; ".join(sorted(reasons))
    return step, because, edges
