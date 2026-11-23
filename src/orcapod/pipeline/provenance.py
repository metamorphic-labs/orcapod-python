"""``node.trace()`` and ``node.why_missing()``: per-row provenance of a job (ITL-1432).

The procedure is the one of the provenance showcase design (ITL-1421, round two,
``docs/dev/specs/itl-1421-provenance-showcase.md``):

- Within a run, every row a node holds names the input rows it came from by its SYSTEM
  TAGS: a row-wise operator and a function pod keep their values and names, a join keeps
  each input's values under names extended with ``::<pipeline hash of that input>:<its
  position>``. So the input row of an output row is the row of the input whose system-tag
  values are the output row's, under the mapped names.
- A function node's stored PIPELINE entry for that row (its system tags, and the result
  its output token names) says which result the row was served and whether this entry
  computed it (``__computed``); its stored RESULT says which function, which body, which
  executor and when. The input row's ``_content_hash`` must be the entry's
  ``__input_data_hash``.
- A folding operator (``GroupBy``, ``Batch``) folds its members' system tags into one
  value. Its members are found with the operator's own rule (``fold_members``) and
  CHECKED: folding their system tags must give the row's folded values; otherwise
  ``ProvenanceInconsistencyError``. Member ``i`` fills position ``i`` of every list.
- The SOURCE INFO token ``<source id>::<record id>::<column>`` of a value names the source
  cell it came from; the token's record id is the record-id system tag of that source.

Everything is read: the rows the job's nodes hold (``as_table``) and the function nodes'
stored tables (``FunctionJobNode._stored_provenance``). Nothing is computed, no user code
is called, nothing is written, and a ``PollingSource`` is never read (it would poll).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
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

Row = dict[str, Any]


# ---------------------------------------------------------------------------
# Node kinds and the rows a node holds
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


def _system_tags(row: Mapping[str, Any]) -> list[str]:
    return sorted(c for c in row if c.startswith(constants.SYSTEM_TAG_PREFIX))


def _source_id_column(record_column: str) -> str:
    return record_column.replace(
        constants.SYSTEM_TAG_RECORD_ID_PREFIX, constants.SYSTEM_TAG_SOURCE_ID_PREFIX, 1
    )


def _record_ids(row: Mapping[str, Any], source: str) -> set[Any]:
    """The record ids of ``source`` a row names, under any extended column name."""
    return {
        row[c]
        for c in row
        if c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
        and row.get(_source_id_column(c)) == source
    }


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


def _flat(value: Any) -> list[str]:
    if isinstance(value, list):
        out: list[str] = []
        for item in cast(list[Any], value):
            out.extend(_flat(item))
        return out
    return [] if value is None else [str(value)]


@dataclass
class _Reader:
    """What one call reads, each node once."""

    tables: dict[int, pa.Table | None] = field(default_factory=dict)
    rows: dict[int, list[Row] | None] = field(default_factory=dict)
    stored: dict[int, tuple[list[Row], dict[bytes, Row]]] = field(default_factory=dict)

    def table(self, node: Any) -> pa.Table | None:
        """The rows the node holds, with system tags, tokens and row hashes.

        ``None`` when the node holds no run (it is stale, or never ran), or for a
        source this tool must not read (a ``PollingSource`` would poll).
        """
        key = id(node)
        if key in self.tables:
            return self.tables[key]
        table: pa.Table | None
        if _kind(node) == "source":
            from orcapod.core.sources.polling_source import PollingSource

            bound = getattr(node, "bound_source", None)
            if bound is None or isinstance(bound, PollingSource):
                table = None
            else:
                table = node.as_table(
                    columns={
                        "system_tags": True,
                        "source": True,
                        "content_hash": _ROW_HASH,
                    }
                )
        elif node.is_stale:
            table = None
        else:
            table = node.as_table(
                columns={"system_tags": True, "source": True, "content_hash": _ROW_HASH}
            )
        self.tables[key] = table
        return table

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


# ---------------------------------------------------------------------------
# How an output row names its input rows
# ---------------------------------------------------------------------------


def _column_map(
    out_columns: Sequence[str], upstream: Any, up_columns: Sequence[str]
) -> dict[str, str]:
    """Map the output's system-tag columns to those of ``upstream`` they come from.

    A column passes unchanged, or gains one extension naming ``upstream`` by its
    pipeline hash: ``::<hash>:<position>`` (a join) or ``::<hash>`` (a fold).
    """
    sep = constants.BLOCK_SEPARATOR
    digest = upstream.pipeline_hash().to_hex()
    mapping: dict[str, str] = {}
    for out in out_columns:
        for up in up_columns:
            if out == up:
                mapping[out] = up
                break
            if out.startswith(up + sep):
                extension = out[len(up) + len(sep) :]
                if sep in extension:
                    continue
                if digest.startswith(extension.split(constants.FIELD_SEPARATOR)[0]):
                    mapping[out] = up
                    break
    return mapping


def _input_rows(row: Row, upstream: Any, up_rows: list[Row]) -> list[Row]:
    """The rows of ``upstream`` that ``row`` names by its system tags (row-wise)."""
    if not up_rows:
        return []
    mapping = _column_map(_system_tags(row), upstream, _system_tags(up_rows[0]))
    if not mapping:
        return []
    return [r for r in up_rows if all(r[u] == row[o] for o, u in mapping.items())]


def _fold_groups(
    node: Any, folding: Any, up_table: pa.Table, up_rows: list[Row]
) -> list[tuple[list[int], dict[str, str]]]:
    """Each group or batch of the node: its member row numbers and its folded values."""
    upstream = node.upstreams[0]
    tag_columns = list(upstream.keys()[0])
    groups = folding.fold_members(up_table, tag_columns)
    columns = _system_tags(up_rows[0]) if up_rows else []
    return [
        (
            members,
            {
                column: fold_system_tag_values(
                    [up_rows[i][column] for i in members], label=folding.fold_label
                )
                for column in columns
            },
        )
        for members in groups
    ]


def _members(node: Any, row: Row, reader: _Reader) -> list[Row] | None:
    """The members of a folded row, in member order, proven by refolding."""
    folding = _folding(node)
    upstream = node.upstreams[0]
    up_table = reader.table(upstream)
    up_rows = reader.held(upstream)
    if up_table is None or up_rows is None:
        return None
    mapping = _column_map(
        _system_tags(row), upstream, _system_tags(up_rows[0]) if up_rows else []
    )
    found = [
        members
        for members, folded in _fold_groups(node, folding, up_table, up_rows)
        if mapping and all(folded[u] == row[o] for o, u in mapping.items())
    ]
    if len(found) != 1:
        raise ProvenanceInconsistencyError(
            f"node {node.label!r}: folding the system tags of the members of "
            f"{type(folding).__name__} does not give the row's folded values "
            f"({len(found)} groups match); the rows {upstream.label!r} holds are not "
            "the ones the node folded"
        )
    return [up_rows[i] for i in found[0]]


# ---------------------------------------------------------------------------
# trace
# ---------------------------------------------------------------------------


def _tags_of(node: Any, row: Row) -> tuple[tuple[str, Any], ...]:
    return tuple((t, row[t]) for t in node.keys()[0] if t in row)


def _datetime(value: Any) -> datetime | None:
    return value if isinstance(value, datetime) else None


def _function_run(
    node: Any, row: Row, column: str, up_row: Row | None, reader: _Reader
) -> FunctionRun | None:
    """The stored entry and result behind a function node's row."""
    if node._pipeline_database is None:  # pyright: ignore[reportPrivateUsage]  # a node with no database stores no entry
        return None
    entries, results = reader.provenance(node)
    token = row.get(constants.SOURCE_PREFIX + column)
    result_hex = _parse_token(token)[1] if isinstance(token, str) else None
    columns = _system_tags(row)
    candidates = [
        e
        for e in entries
        if all(e.get(c) == row[c] for c in columns)
        and (
            result_hex is None or bytes(e[constants.DATA_RECORD_ID]).hex() == result_hex
        )
    ]
    if up_row is not None:
        candidates = [
            e
            for e in candidates
            if _hash_text(e[constants.INPUT_DATA_HASH_COL]) == up_row.get(_ROW_HASH)
        ]
    if not candidates:
        raise ProvenanceInconsistencyError(
            f"node {node.label!r}: no stored pipeline entry for the row "
            f"{dict(_tags_of(node, row))!r} and its input; the database holds other runs"
        )
    entry = max(candidates, key=lambda e: e.get("__pipeline_recomputation_index") or 0)
    if up_row is not None:
        for name, value in entry.items():
            if (
                name.startswith(constants.SOURCE_PREFIX)
                and name in up_row
                and up_row[name] != value
            ):
                raise ProvenanceInconsistencyError(
                    f"node {node.label!r}: the stored entry's input token {name} "
                    f"is {value!r}, the input row's is {up_row[name]!r}"
                )
    result = results.get(bytes(entry[constants.DATA_RECORD_ID]))
    if result is None:
        raise ProvenanceInconsistencyError(
            f"node {node.label!r}: the stored entry names a result the result "
            "database does not hold"
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


def _token_columns(up_row: Row, up_node: Any, tokens: list[str]) -> list[str]:
    """The data columns of an input row whose tokens a cell's tokens came from."""
    found: list[str] = []
    for column in up_node.keys()[1]:
        up_tokens = _flat(up_row.get(constants.SOURCE_PREFIX + column))
        if any(
            t == u or t.startswith(u + "[") or t.startswith(u + ".")
            for t in tokens
            for u in up_tokens
        ):
            found.append(column)
    return sorted(found)


class _Tracer:
    def __init__(self) -> None:
        self.reader = _Reader()
        self.edges: set[tuple[str, str]] = set()

    def not_computed(self, node: Any, column: str, member: int | None) -> TraceStep:
        return TraceStep(
            node=node.label,
            kind=_kind(node),
            operator=None,
            column=column,
            value=None,
            tags=(),
            member=member,
            status="not_computed",
        )

    def step(
        self, node: Any, row: Row, column: str, member: int | None = None
    ) -> TraceStep:
        kind = _kind(node)
        value = row.get(column)
        tags = _tags_of(node, row)
        token = row.get(constants.SOURCE_PREFIX + column)
        if kind == "source":
            cell = None
            if isinstance(token, str):
                source_id, record_id, source_column = _parse_token(token)
                cell = SourceCell(
                    node.label, source_id, record_id, source_column, value
                )
            return TraceStep(
                node.label, "source", None, column, value, tags, member, source=cell
            )
        if kind == "function":
            upstream = node.upstreams[0]
            up_rows = self.reader.held(upstream)
            if up_rows is None:
                return TraceStep(
                    node.label,
                    kind,
                    None,
                    column,
                    value,
                    tags,
                    member,
                    inputs=(self.not_computed(upstream, column, None),),
                )
            matched = _input_rows(row, upstream, up_rows)
            if not matched:
                raise ProvenanceInconsistencyError(
                    f"node {node.label!r}: {upstream.label!r} holds no row with the "
                    "system tags of the traced row"
                )
            up_row, run = matched[0], None
            if node._pipeline_database is not None:  # pyright: ignore[reportPrivateUsage]  # a node with no database stores no entry
                # The input row whose content hash a stored entry names.
                for candidate in matched:
                    try:
                        run = _function_run(node, row, column, candidate, self.reader)
                    except ProvenanceInconsistencyError:
                        continue
                    up_row = candidate
                    break
                if run is None:
                    run = _function_run(node, row, column, matched[0], self.reader)
            self.edges.add((upstream.label, node.label))
            fn_inputs = tuple(
                self.step(upstream, up_row, c) for c in sorted(upstream.keys()[1])
            )
            return TraceStep(
                node.label,
                kind,
                None,
                column,
                value,
                tags,
                member,
                function=run,
                inputs=fn_inputs,
            )
        operator = (
            type(node.producer).__name__
            if getattr(node, "producer", None) is not None
            else None
        )
        folding = _folding(node)
        if folding is not None:
            upstream = node.upstreams[0]
            members = _members(node, row, self.reader)
            if members is None:
                return TraceStep(
                    node.label,
                    kind,
                    operator,
                    column,
                    value,
                    tags,
                    member,
                    inputs=(self.not_computed(upstream, column, None),),
                )
            self.edges.add((upstream.label, node.label))
            tokens = row.get(constants.SOURCE_PREFIX + column)
            inputs = []
            for i, m in enumerate(members):
                if (
                    isinstance(tokens, list)
                    and i < len(tokens)
                    and tokens[i] != m.get(constants.SOURCE_PREFIX + column)
                ):
                    raise ProvenanceInconsistencyError(
                        f"node {node.label!r}: member {i} of {column!r} has token "
                        f"{tokens[i]!r}, its input row {m.get(constants.SOURCE_PREFIX + column)!r}"
                    )
                inputs.append(self.step(upstream, m, column, member=i))
            return TraceStep(
                node.label,
                kind,
                operator,
                column,
                value,
                tags,
                member,
                inputs=tuple(inputs),
            )
        tokens = _flat(token)
        inputs: list[TraceStep] = []
        for upstream in node.upstreams:
            up_rows = self.reader.held(upstream)
            if up_rows is None:
                inputs.append(self.not_computed(upstream, column, None))
                continue
            matched = _input_rows(row, upstream, up_rows)
            if not matched:
                continue
            up_row = matched[0]
            columns = (
                _token_columns(up_row, upstream, tokens)
                if tokens
                else ([column] if column in upstream.keys()[1] else [])
            )
            for c in columns:
                self.edges.add((upstream.label, node.label))
                inputs.append(self.step(upstream, up_row, c))
        return TraceStep(
            node.label,
            kind,
            operator,
            column,
            value,
            tags,
            member,
            inputs=tuple(inputs),
        )


def _select_row(node: Any, rows: list[Row], row: Mapping[str, Any]) -> Row:
    known = set(rows[0]) if rows else set()
    unknown = sorted(k for k in row if k not in known)
    if unknown:
        raise ValueError(
            f"node {node.label!r} has no column {unknown[0]!r} to select a row by"
        )
    found = [r for r in rows if all(r[k] == v for k, v in row.items())]
    if len(found) != 1:
        given = ", ".join(f"{k}={plain_repr(v)}" for k, v in sorted(row.items()))
        what = "no row" if not found else f"{len(found)} rows"
        raise ValueError(
            f"node {node.label!r} holds {what} with {given}; give columns that select one row"
        )
    return found[0]


def trace(node: Any, row: Mapping[str, Any], column: str) -> Trace:
    """Trace one cell of a job node back to the source cells it was made from.

    Args:
        node: A job node (function, operator or source node).
        row: Column values that select exactly one row the node holds (its tags,
            usually; system-tag columns may be given too).
        column: The data column of the cell.

    Returns:
        The trace. When the node holds no run, its root has status ``not_computed``.

    Raises:
        ValueError: If ``column`` is not a data column of the node, or ``row`` names a
            column the node does not have or does not select exactly one row.
        ProvenanceInconsistencyError: If a link cannot be proven from what the job
            holds and stores.
    """
    if column not in node.keys()[1]:
        raise ValueError(f"{column!r} is not a data column of node {node.label!r}")
    tracer = _Tracer()
    rows = tracer.reader.held(node)
    if rows is None:
        return Trace(root=tracer.not_computed(node, column, None))
    root = tracer.step(node, _select_row(node, rows, row), column)
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
    ids = set()
    for n in _ancestry(node).values():
        if _kind(n) == "source":
            bound = getattr(n, "bound_source", None)
            ids.add(getattr(bound, "source_id", None) or n.label)
    return sorted(ids)


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


def why_missing(node: Any, source: str, record_id: str) -> Absence:
    """Say whether rows of one source record reach a job node, and if not, why.

    Args:
        node: A job node.
        source: The source id (as in source-info tokens and the source-id system tag).
        record_id: The record id (``row_<n>``, or the key columns as JSON).

    Returns:
        The answer.

    Raises:
        ValueError: If no source with that id feeds the node.
        ProvenanceInconsistencyError: If a group's members cannot be proven.
    """
    ancestry = _ancestry(node)
    sources = [
        n
        for n in ancestry.values()
        if _kind(n) == "source"
        and getattr(getattr(n, "bound_source", None), "source_id", n.label) == source
    ]
    if not sources:
        raise ValueError(f"no source with id {source!r} feeds node {node.label!r}")
    on_path = [
        n for n in ancestry.values() if any(id(s) in _ancestry(n) for s in sources)
    ]
    path = _topological(on_path)
    reader = _Reader()
    carried: dict[int, list[Row]] = {}
    not_computed = False
    for n in path:
        rows = reader.held(n)
        if _kind(n) == "source":
            if rows is None:
                # Not read (a polling source): its record is taken to exist.
                carried[id(n)] = [{}]
            else:
                carried[id(n)] = [
                    r for r in rows if record_id in _record_ids(r, source)
                ]
            continue
        if rows is None:
            not_computed = True
            break
        ups = [u for u in n.upstreams if id(u) in carried]
        folding = _folding(n)
        kept: list[Row] = []
        if folding is not None:
            upstream = n.upstreams[0]
            up_table = reader.table(upstream)
            up_rows = reader.held(upstream) or []
            hits = {id(r) for r in carried.get(id(upstream), [])}
            if up_table is not None and hits:
                for members, folded in _fold_groups(n, folding, up_table, up_rows):
                    if not any(id(up_rows[i]) in hits for i in members):
                        continue
                    for r in rows:
                        mapping = _column_map(_system_tags(r), upstream, list(folded))
                        if mapping and all(
                            folded[u] == r[o] for o, u in mapping.items()
                        ):
                            kept.append(r)
        else:
            for upstream in ups:
                up_carried = carried[id(upstream)]
                if up_carried == [{}]:  # an unread source: its rows by record id
                    kept.extend(r for r in rows if record_id in _record_ids(r, source))
                    continue
                for r in rows:
                    named = _input_rows(r, upstream, up_carried)
                    if named:
                        kept.append(r)
        carried[id(n)] = kept

    function_node = next((n for n in path if _kind(n) == "function"), None)
    reached: bool | None = None
    if function_node is not None and function_node._pipeline_database is not None:  # pyright: ignore[reportPrivateUsage]
        entries, _ = reader.provenance(function_node)
        reached = (
            any(record_id in _record_ids(e, source) for e in entries)
            if entries
            else None
        )
        if entries == []:
            reached = None
    present = tuple(n.label for n in path if carried.get(id(n)))
    labels = tuple(n.label for n in path)
    if not_computed:
        return Absence(
            node.label,
            source,
            record_id,
            True,
            "not_computed",
            reached,
            function_node.label if function_node is not None else None,
            present,
            None,
            "a node on the way holds no run",
            labels,
        )
    missing = not carried.get(id(node))
    removed_by: Step | None = None
    because = ""
    for n in path:
        if _kind(n) == "source" or carried.get(id(n)):
            continue
        ups = [u for u in n.upstreams if carried.get(id(u))]
        if not ups:
            continue
        position = list(n.upstreams).index(ups[0])
        producer = getattr(n, "producer", None)
        others = [
            ", ".join(_source_ids(u))
            for i, u in enumerate(n.upstreams)
            if i != position
        ]
        text = None
        if producer is not None and hasattr(producer, "rows_dropped_text"):
            text = producer.rows_dropped_text(
                position, sorted(others), tuple(n.upstreams)
            )
        if _kind(n) == "function":
            removed_by = Step(
                operator=type(producer).__name__
                if producer is not None
                else "FunctionPod",
                label=n.label,
                text="no row was kept for this input (the function raised, or the run skipped it)",
            )
            because = removed_by.text
        else:
            removed_by = Step(
                operator=type(producer).__name__,
                label=n.label,
                text=text or "its rows are not passed on",
            )
            because = removed_by.text
            if len(n.upstreams) > 1:
                carried_row = carried[id(ups[0])][0]
                for i, other in enumerate(n.upstreams):
                    if i == position:
                        continue
                    shared = [
                        t
                        for t in other.keys()[0]
                        if t in carried_row and t in ups[0].keys()[0]
                    ]
                    other_rows = reader.held(other) or []
                    key = {t: carried_row[t] for t in shared}
                    if key and not any(
                        all(o[t] == v for t, v in key.items()) for o in other_rows
                    ):
                        because = f"no row with {_key_text(key)} in {other.label!r} (reads {', '.join(_source_ids(other))})"
        break
    if missing and removed_by is None:
        source_node = sources[0]
        if carried.get(id(source_node)) == []:
            because = f"source {source!r} holds no record {record_id!r}"
        else:
            because = "no step on the way passes it on"
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
    )
