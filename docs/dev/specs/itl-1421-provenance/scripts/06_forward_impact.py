"""Scenario 2: "this was bad: what does it touch?" Forward impact, per node, on a job
after ``job.run()``, using the pipeline store, the result store and the system tags.

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.06_forward_impact

Two different questions, two different answers:
  (a) a bad INPUT VALUE (source record units/row_16 was wrong). What depends on it is
      what carries its record id: row-wise steps and joins keep the record-id system tag
      (column of source 'units'); the function node's PIPELINE store has one entry per
      input row, keyed by those system tags, so the entries it TRIGGERED are exact; a
      GroupBy row depends on it when its key tags are the entry's (checked: refolding the
      members' record ids gives the group's fold); after the group, the folded system tags
      name the group row, row-wise and in the next pipeline store.
  (b) a bad COMPUTATION (the result row of that entry is suspect: wrong environment,
      broken body). What depends on it is every pipeline entry SERVED that result: the
      RESULT store's ``__record_id`` = the pipeline store's ``__data_id``. A memoized
      result is served to every entry with equal input values, so (b) can be wider
      than (a).
Round one followed source-info tokens forward and so answered neither exactly: it marked
the entry that shares the result (question b's answer) while asking question a.
Public API and direct reads of the stored tables; nothing is computed.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from orcapod.databases import DeltaTableDatabase
from orcapod.utils.arrow_utils import fold_system_tag_values

from ._common import workshop_job
from ._store import KEY, read_job, rows, short_hash, system_tag_columns

root = Path(tempfile.mkdtemp())
db = DeltaTableDatabase(root / "store")
job, _ = workshop_job(root, database=db)
job.run()
S = read_job(job, db)
SOURCE, RECORD = "units", "row_16"  # mouse_02 s1 imec0 unit 0
KEYS = ("subject_id", "session")


def record_id_of(row: dict[str, Any], source: str) -> Any:
    """The value of the record-id system tag of ``source`` in a row (any extension)."""
    for c in row:
        if c.startswith("_tag_source_id") and row[c] == source:
            return row[c.replace("_tag_source_id", "_tag_record_id", 1)]
    return None


def describe(r: dict[str, Any]) -> str:
    return " ".join(
        str(r[k]) for k in ("subject_id", "session", "probe", "unit") if k in r
    )


def sysv(r: dict[str, Any], cols: list[str]) -> tuple[Any, ...]:
    return tuple(r[c] for c in cols)


print(f"(a) Forward impact of a bad input value: {SOURCE} {RECORD}")
hit: dict[str, list[dict[str, Any]]] = {}
for label in ["units", "SelectDataColumns_3", "Join"]:
    hit[label] = [r for r in rows(S[label].memory) if record_id_of(r, SOURCE) == RECORD]
    print(
        f"  {label:20s} system tags (record id of '{SOURCE}' = {RECORD}): {[describe(r) for r in hit[label]]}"
    )
rates = S["rates"]
pipe = rows(rates.own(rates.pipeline))
entries = [r for r in pipe if record_id_of(r, SOURCE) == RECORD]
print(
    f"  rates PIPELINE store: entries triggered by it: {[(describe(r), short_hash(r[KEY])) for r in entries]}"
)
results = {r["__data_id"] for r in entries}
print(
    f"  rates RESULT store:   the result(s) those entries were served: {[short_hash(x) for x in results]}"
)
hit["rates"] = [r for r in rows(rates.memory) if record_id_of(r, SOURCE) == RECORD]
groups = {tuple(r[k] for k in KEYS) for r in entries}
gcols = system_tag_columns(S["GroupBy"].memory)
rcols = system_tag_columns(rates.memory)
hit["GroupBy"] = []
for g in rows(S["GroupBy"].memory):
    if tuple(g[k] for k in KEYS) not in groups:
        continue
    members = sorted(
        (r for r in rows(rates.memory) if all(r[k] == g[k] for k in KEYS)),
        key=lambda r: (r["probe"], r["unit"]),
    )
    rid_col = [
        c
        for c, rc in zip(gcols, rcols)
        if c.startswith("_tag_record_id")
        and members[0][rc.replace("record_id", "source_id", 1)] == SOURCE
    ][0]
    rc = rcols[gcols.index(rid_col)]
    assert g[rid_col] == fold_system_tag_values(
        [m[rc] for m in members], label="group_v1"
    )
    assert RECORD in [m[rc] for m in members]
    hit["GroupBy"].append(g)
print(
    f"  GroupBy              key tags {sorted(groups)}; {RECORD} is in the refolded member list: True"
)
folded = {sysv(g, gcols) for g in hit["GroupBy"]}
hit["SelectDataColumns_2"] = [
    r for r in rows(S["SelectDataColumns_2"].memory) if sysv(r, gcols) in folded
]
means = S["session_means"]
mpipe = [
    r
    for r in rows(means.own(means.pipeline))
    if sysv(r, system_tag_columns(means.pipeline)) in folded
]
print(
    f"  session_means PIPELINE store: entries with those folded system tags: {[(r['subject_id'], r['session'], short_hash(r[KEY])) for r in mpipe]}"
)
hit["session_means"] = [r for r in rows(means.memory) if sysv(r, gcols) in folded]
print("  per node (rows):", {k: len(v) for k, v in hit.items()})

print("\n(b) Forward impact of a bad computation: the result row_16's entry was served")
(bad_result,) = results
served = [r for r in pipe if r["__data_id"] == bad_result]
print(
    f"  rates RESULT {short_hash(bad_result)} <- rates PIPELINE entries served it: {[(describe(r), record_id_of(r, SOURCE), 'computed' if r['__computed'] else 'reused') for r in served]}"
)
groups_b = {tuple(r[k] for k in KEYS) for r in served}
print(
    f"  -> groups {sorted(groups_b)}; then as in (a). Here both entries are in one group, so (a) and (b) reach the same session_means row."
)

print("\nRound one's method: follow source-info tokens forward")
tokens = {f"{SOURCE}::{RECORD}::n_spikes"}
tok_entries = [
    r for r in pipe if tokens & {r["_source_n_spikes"], r["_source_duration_s"]}
]
out_tokens = {
    next(x for x in rows(rates.result) if x[KEY] == r["__data_id"])["_source_rate_hz"]
    for r in tok_entries
}
tok_rows = [r for r in rows(rates.memory) if r["_source_rate_hz"] in out_tokens]
print(
    f"  rates rows carrying a dependent output token: {[describe(r) for r in tok_rows]}"
)
print(
    f"  -> {len(tok_rows)} rows: it includes {describe(tok_rows[-1])} (units {record_id_of(tok_rows[-1], SOURCE)}), which (a) excludes and (b) includes."
)
