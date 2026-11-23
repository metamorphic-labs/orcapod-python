"""Scenario 1 by hand: from ONE output cell back to the exact source records and columns.

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.04_trace_by_hand

Pipeline (the tutorial's, Part 8): units, sessions (CSV) -> select -> join -> firing_rate
[rates] -> group_by(subject_id, session) -> select -> mean_rate [session_means].

Public API only: ``as_table(columns=...)``, ``get_all_records(columns=...)``,
``node.node_uri``, ``SourceJobNode.bound_source.source_id``. Nothing is computed by the
trace; it reads what the run stored. The same trace is then repeated on the job LOADED from
its saved file (``run(load_only=True)``), which shows it needs no live objects.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from ._common import workshop_job

from orcapod import PipelineJob
from orcapod.databases import DeltaTableDatabase


def parse_token(token: str) -> tuple[str, str, str]:
    """Split ``<source id>::<record id>::<column>`` at its first and last ``::``."""
    head, _, column = token.rpartition("::")
    source, _, record = head.partition("::")
    return source, record, column


def function_nodes_by_uri(job: Any) -> dict[str, list[Any]]:
    """A function node's token names its function as ``":".join(node.node_uri)``."""
    out: dict[str, list[Any]] = {}
    for label in sorted(job.nodes):
        node = job.nodes[label]
        if type(node).__name__ == "FunctionJobNode":
            out.setdefault(":".join(node.node_uri), []).append(node)
    return out


def source_nodes_by_id(job: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for label in sorted(job.nodes):
        node = job.nodes[label]
        if type(node).__name__ == "SourceJobNode" and node.bound_source is not None:
            out[node.bound_source.source_id] = node
    return out


def trace(job, token: str, depth: int = 0, *, fn=None, src=None, stats=None) -> None:
    """Print the provenance tree of one source-info token, recursively."""
    fn = fn if fn is not None else function_nodes_by_uri(job)
    src = src if src is not None else source_nodes_by_id(job)
    pad = "  " * depth
    source, record, column = parse_token(token)
    if source in fn:
        # A function output: the record id is the result row's ``__data_id`` (hex).
        hits = []
        for node in fn[source]:
            records = node.get_all_records(columns={"source": True, "meta": True})
            if records is None:
                continue
            ids = [b.hex() for b in records.column("__data_id").to_pylist()]
            hits += [
                (node, records.slice(i, 1)) for i, x in enumerate(ids) if x == record
            ]
        value = hits[0][1].column(column)[0].as_py() if hits else None
        print(
            f"{pad}{column} = {value!r}  <- result {record[:12]}… of "
            f"{source.split(':')[0]} ({len(hits)} pipeline record(s) point at it)"
        )
        if stats is not None:
            stats["shared"] += len(hits) > 1
        node, row = hits[0]  # several: identical inputs; all lead to equal values
        for name in sorted(
            c
            for c in row.column_names
            if c.startswith("_source_") and c != f"_source_{column}"
        ):
            tokens = row.column(name)[0].as_py()
            for t in tokens if isinstance(tokens, list) else [tokens]:
                trace(job, t, depth + 1, fn=fn, src=src, stats=stats)
        return
    # A source cell: find the row by its record-id system tag.
    node = src[source]
    table = node.as_table(columns={"system_tags": True})
    rid = [c for c in table.column_names if c.startswith("_tag_record_id")][0]
    i = table.column(rid).to_pylist().index(record)
    tags = {k: table.column(k)[i].as_py() for k in node.keys()[0]}
    print(
        f"{pad}{column} = {table.column(column)[i].as_py()!r}  <- source "
        f"{source!r} record {record} {tags}"
    )
    if stats is not None:
        stats["sources"].add((source, record, column))


def cell_token(job, label: str, column: str, **tags) -> str:
    table = job.nodes[label].as_table(columns={"source": True})
    for row in table.to_pylist():
        if all(row[k] == v for k, v in tags.items()):
            print(f"{label}{tags}: {column} = {row[column]!r}")
            return row[f"_source_{column}"]
    raise LookupError(tags)


root = Path(tempfile.mkdtemp())
job, _ = workshop_job(root)
job.run()
token = cell_token(
    job, "session_means", "mean_rate_hz", subject_id="mouse_03", session="s2"
)
stats: dict[str, Any] = {"shared": 0, "sources": set()}
trace(job, token, stats=stats)
print(
    f"\nreached {len(stats['sources'])} distinct source cells; "
    f"{stats['shared']} result(s) were shared by several pipeline records"
)

print("\n== Same trace on the job loaded from its file (load_only run, same store)")
job.save(root / "job.json")
loaded = PipelineJob.load(
    root / "job.json", database=DeltaTableDatabase(root / "store")
)
loaded.run(load_only=True)
stats2: dict[str, Any] = {"shared": 0, "sources": set()}
trace(
    loaded,
    cell_token(
        loaded, "session_means", "mean_rate_hz", subject_id="mouse_03", session="s2"
    ),
    stats=stats2,
)
print("same source cells reached:", stats2["sources"] == stats["sources"])

print("\n== Check: recomputing the cell from the source cells the trace reached")
u = job.nodes["units"].as_table(columns={"system_tags": True})
s = job.nodes["sessions"].as_table(columns={"system_tags": True})


def value(t, src, rec, col):
    rid = [c for c in t.column_names if c.startswith("_tag_record_id")][0]
    return t.column(col)[t.column(rid).to_pylist().index(rec)].as_py()


spikes = sorted(r for (src, r, c) in stats["sources"] if c == "n_spikes")
(dur_rec,) = {r for (src, r, c) in stats["sources"] if c == "duration_s"}
duration = value(s, "sessions", dur_rec, "duration_s")
rates = [value(u, "units", r, "n_spikes") / duration for r in spikes]
print("mean of n_spikes/duration over the traced records:", sum(rates) / len(rates))

print("\n== Where the chain is ambiguous: one stored result, several pipeline records")
records = job.nodes["rates"].get_all_records(columns={"source": True, "meta": True})
ids = [b.hex() for b in records.column("__data_id").to_pylist()]
shared = sorted({x for x in ids if ids.count(x) > 1})
for x in shared:
    rows = [r for r, i in zip(records.to_pylist(), ids) if i == x]
    print(f"result {x[:12]}… (rate_hz = {rows[0]['rate_hz']}) is pointed at by:")
    for r in rows:
        print(
            f"   {r['subject_id']} {r['session']} {r['probe']} unit {r['unit']}: "
            f"{r['_source_n_spikes']}, {r['_source_duration_s']}"
        )
print(
    "A token names the RESULT (memoized by input data hash), not the input row: two"
    " input rows with equal values share one result, here inside ONE group, so the"
    " group key does not tell them apart. The grouped row lists the token twice, once"
    " per member, in GroupBy's member order (non-key tags in name order, then record"
    " id): position i of the list is the i-th member of the group in that order. A tool"
    " resolves it by that rule, or reports every candidate; the hand trace above takes"
    " the first."
)
