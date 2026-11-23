"""Scenario 2: "this input was bad: what does it touch?" Forward impact of ONE source
record, per node, on a job after ``job.run()``.

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.06_forward_impact

Two methods, compared:
- EXACT, operator-aware: a row-wise step (select, join, function pod) keeps the source's
  record-id system tag, so a dependent row carries the bad record id in the column of
  that source; a ``GroupBy`` row depends on it when its key equals a dependent input
  row's key (the key is the operator's own argument); a node after the group keeps the
  group's folded system tags, so it matches them by value.
- GENERIC, token matching: follow source-info tokens forward (a function output's token
  in the list tokens of a grouped row). It needs no operator knowledge, but a memoized
  result shared by two input rows (equal input values) makes it over-include.
Public API only; reads the nodes' tables, computes nothing new.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from ._common import workshop_job

root = Path(tempfile.mkdtemp())
job, _ = workshop_job(root)
job.run()

BAD = ("units", "row_16")  # mouse_02 s1 imec0 unit 0


def rows(label):
    return job.nodes[label].as_table(all_info=True).to_pylist()


def record_column(row, source):
    for name, value in row.items():
        if name.startswith("_tag_source_id") and value == source:
            return name.replace("_tag_source_id", "_tag_record_id", 1)
    return None


def flatten(value):
    if isinstance(value, list):
        for v in value:
            yield from flatten(v)
    elif value is not None:
        yield value


exact: dict[str, list[dict[str, Any]]] = {}
for label in ["units", "SelectDataColumns_3", "Join", "rates"]:
    exact[label] = [
        r for r in rows(label) if (c := record_column(r, BAD[0])) and r[c] == BAD[1]
    ]
keys = {(r["subject_id"], r["session"]) for r in exact["rates"]}
exact["GroupBy"] = [
    r for r in rows("GroupBy") if (r["subject_id"], r["session"]) in keys
]
folded = {
    tuple(sorted((k, v) for k, v in r.items() if k.startswith("_tag")))
    for r in exact["GroupBy"]
}
exact["session_means"] = [
    r
    for r in rows("session_means")
    if tuple(sorted((k, v) for k, v in r.items() if k.startswith("_tag"))) in folded
]

print(f"Forward impact of {BAD[0]} record {BAD[1]} (exact, operator-aware):")
for label, hits in exact.items():
    tags = sorted(
        {
            tuple(
                v
                for k, v in r.items()
                if k in ("subject_id", "session", "probe", "unit")
            )
            for r in hits
        }
    )
    print(f"  {label:20s} {len(hits)} row(s) {tags}")

print("\nGeneric token matching from the bad record's tokens:")
tokens = {f"{BAD[0]}::{BAD[1]}::n_spikes"}
for label in ["rates", "GroupBy", "session_means"]:
    node = job.nodes[label]
    if type(node).__name__ == "FunctionJobNode":
        # A function output row does not carry its inputs' tokens: only the node's
        # stored record links them to the output token.
        recs = node.get_all_records(columns={"source": True}).to_pylist()
        outs = sorted(c for c in recs[0] if c.startswith("_source_"))
        out_col = [c for c in outs if c[len("_source_") :] in node.keys()[1]][0]
        hit = [
            r
            for r in recs
            if tokens & set(flatten([r[c] for c in outs if c != out_col]))
        ]
        out = {r[out_col] for r in hit}
        shared = [r for r in recs if r[out_col] in out]
        print(
            f"  {label}: {len(hit)} stored record(s) read a dependent token; their"
            f" result(s) are pointed at by {len(shared)} record(s)"
        )
        tokens |= out
    else:
        hit = [
            r
            for r in rows(label)
            if tokens
            & set(flatten([v for k, v in r.items() if k.startswith("_source_")]))
        ]
        print(f"  {label}: {len(hit)} row(s)")
        tokens |= {
            t
            for r in hit
            for k, v in r.items()
            if k.startswith("_source_")
            for t in flatten(v)
        }
print(
    "  -> at rates, the result the bad record produced is shared with a second input"
    " row (equal values): following its token marks that row's dependents too. Both"
    " rows are in one group here, so the answer is still right; across groups the"
    " token path over-includes, and only the operator-aware rule is exact."
)
