"""Scenario 1 by hand: from ONE output cell back to the exact source cells, using every
stored table: the PIPELINE store (why and from what), the RESULT store (which function,
which computation) and the SOURCE INFO (the ultimate sources).

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.04_trace_by_hand

Pipeline (the tutorial's, Part 8): units, sessions (CSV) -> select -> join -> firing_rate
[rates] -> group_by(subject_id, session) -> select -> mean_rate [session_means].

The procedure, per step (the step names are printed):
  P  pipeline store: the function node's pipeline record whose SYSTEM TAGS are the row's
     (its key is hash(system tags + input data hash)); it names the input entry that
     triggered the lookup, the result it was served (``__data_id``) and whether this
     entry computed it (``__computed``).
  R  result store: the result row ``__record_id = __data_id``: the function (name,
     version, content hash, module, git), how it ran (executor, Python), when (``__pod_ts``)
     and the value; ``__input_data_hash`` must equal the pipeline record's.
  U  upstream entry: the row of the node's input with the same system-tag values (a
     row-wise operator keeps them); its ``_content_hash`` must equal ``__input_data_hash``.
  G  GroupBy: the members are the input rows with the group's KEY TAGS; refolding their
     system tags in member order must give the group's folded system tags (exact
     membership, and member i is list position i).
  S  source info: the pipeline record's input tokens ``<source>::<record>::<column>``
     name the ultimate source cells; the record-id system tags name the same rows.
Nothing is computed by the trace. It is repeated on the job LOADED from its file.
"""

from __future__ import annotations

import contextlib
import io
import tempfile
from pathlib import Path
from typing import Any

from orcapod import PipelineJob
from orcapod.databases import DeltaTableDatabase
from orcapod.utils.arrow_utils import fold_system_tag_values

from ._common import workshop_job
from ._store import (
    KEY,
    StoredJob,
    hash_text,
    parse_token,
    read_job,
    rows,
    short_hash,
    system_tag_columns,
)


def sys_values(row: dict[str, Any], cols: list[str]) -> tuple[Any, ...]:
    return tuple(row[c] for c in cols)


def pipeline_record(S: StoredJob, label: str, row: dict[str, Any]) -> dict[str, Any]:
    """P: the node's own pipeline record whose system tags are the row's."""
    n = S[label]
    cols = system_tag_columns(n.pipeline)
    hits = [
        r
        for r in rows(n.own(n.pipeline))
        if sys_values(r, cols) == sys_values(row, cols)
    ]
    assert len(hits) == 1, hits
    return hits[0]


def result_row(S: StoredJob, label: str, rec: dict[str, Any]) -> dict[str, Any]:
    """R: the result row the pipeline record was served."""
    (res,) = [r for r in rows(S[label].result) if r[KEY] == rec["__data_id"]]
    assert res["__input_data_hash"] == rec["__input_data_hash"]
    return res


def upstream_row(S: StoredJob, label: str, rec: dict[str, Any]) -> dict[str, Any]:
    """U: the input row with the record's system tags; its content hash is checked."""
    t = S[label].memory
    cols = system_tag_columns(t)
    (row,) = [r for r in rows(t) if sys_values(r, cols) == sys_values(rec, cols)]
    assert row["_content_hash"] == hash_text(rec["__input_data_hash"]), label
    return row


def group_members(
    S: StoredJob,
    group: dict[str, Any],
    keys: list[str],
    member_input: str,
    order: list[str],
) -> list[dict[str, Any]]:
    """G: the input rows with the group's key tags, in member order, checked by refold."""
    t = S[member_input].memory
    members = sorted(
        (r for r in rows(t) if all(r[k] == group[k] for k in keys)),
        key=lambda r: tuple(r[c] for c in order),
    )
    gcols, mcols = system_tag_columns(S["GroupBy"].memory), system_tag_columns(t)
    for gc, mc in zip(gcols, mcols):
        assert group[gc] == fold_system_tag_values(
            [m[mc] for m in members], label="group_v1"
        ), gc
    return members


def source_cell(
    S: StoredJob, token: str, rec: dict[str, Any]
) -> tuple[str, str, str, Any]:
    """S: resolve a source token; the record-id system tag of that source agrees."""
    source, record, column = parse_token(token)
    cols = system_tag_columns(S["Join"].memory)
    named = {
        rec[c]
        for c in cols
        if c.startswith("_tag_record_id")
        and rec[c.replace("record_id", "source_id", 1)] == source
    }
    assert named == {record}, (named, record)
    t = S[source].memory  # the source job node, labelled by its source
    rid = [c for c in system_tag_columns(t) if c.startswith("_tag_record_id")][0]
    (row,) = [r for r in rows(t) if r[rid] == record]
    return source, record, column, row[column]


def fn_desc(res: dict[str, Any]) -> str:
    return (
        f"{res['__pf_var_function_name']} {res['__pf_var_function_version']} "
        f"body {short_hash(res['__pf_var_function_content_hash'], 8)} "
        f"ran {res['__pod_ts']:%H:%M:%S.%f} on {res['__pf_exec_executor_type']}"
    )


def trace(S: StoredJob, subject: str, session: str) -> set[tuple[str, str, str, Any]]:
    cells: set[tuple[str, str, str, Any]] = set()
    (out,) = [
        r
        for r in rows(S["session_means"].memory)
        if (r["subject_id"], r["session"]) == (subject, session)
    ]
    print(f"session_means {subject} {session}: mean_rate_hz = {out['mean_rate_hz']!r}")
    p = pipeline_record(S, "session_means", out)
    print(
        f" P session_means entry {short_hash(p[KEY])} (by its 4 folded system tags) -> result {short_hash(p['__data_id'])}, computed by this entry: {p['__computed']}"
    )
    r = result_row(S, "session_means", p)
    print(f" R result: {fn_desc(r)}; value {r['mean_rate_hz']!r}")
    u = upstream_row(S, "SelectDataColumns_2", p)
    g = upstream_row(S, "GroupBy", p)  # the select kept the system tags and the content
    print(
        f" U input entry: the SelectDataColumns_2 / GroupBy row with those system tags; content hash = __input_data_hash: True ({len(u['rate_hz'])} rates)"
    )
    members = group_members(S, g, ["subject_id", "session"], "rates", ["probe", "unit"])
    print(
        f" G {len(members)} members = 'rates' rows with subject_id={subject!r}, session={session!r}; refold of their system tags = the group's: True"
    )
    for i, m in enumerate(members):
        pm = pipeline_record(S, "rates", m)
        rm = result_row(S, "rates", pm)
        assert (
            g["_source_rate_hz"][i] == rm["_source_rate_hz"]
            and g["rate_hz"][i] == rm["rate_hz"]
        )
        sharers = [
            x
            for x in rows(S["rates"].own(S["rates"].pipeline))
            if x["__data_id"] == pm["__data_id"]
        ]
        print(
            f"   [{i}] {m['probe']} unit {m['unit']}: P rates entry {short_hash(pm[KEY])} -> R result {short_hash(pm['__data_id'])} rate_hz={rm['rate_hz']:.4f} (computed by this entry: {pm['__computed']}; result served to {len(sharers)} entr{'y' if len(sharers) == 1 else 'ies'})"
        )
        for col in ("_source_n_spikes", "_source_duration_s"):
            cell = source_cell(S, pm[col], pm)
            cells.add(cell)
            print(f"       S {col[8:]} = {cell[3]!r} <- {cell[0]} {cell[1]}")
    return cells


def token_only(S: StoredJob, subject: str, session: str) -> list[str]:
    """Round one's method: follow output tokens to the FIRST pipeline record holding them."""
    p = pipeline_record(
        S,
        "session_means",
        next(
            r
            for r in rows(S["session_means"].memory)
            if (r["subject_id"], r["session"]) == (subject, session)
        ),
    )
    reached = []
    for tok in p["_source_rate_hz"]:
        rec = parse_token(tok)[1]
        hits = [
            x
            for x in rows(S["rates"].own(S["rates"].pipeline))
            if x["__data_id"].hex() == rec
        ]
        reached.append(parse_token(hits[0]["_source_n_spikes"])[1])
    return reached


root = Path(tempfile.mkdtemp())
db = DeltaTableDatabase(root / "store")
job, _ = workshop_job(root, database=db)
job.run()
S = read_job(job, db)

print("== 1. mouse_03 s2 (the demo cell)")
cells = trace(S, "mouse_03", "s2")
spikes = [v for (src, _, col, v) in cells if col == "n_spikes"]
(duration,) = {v for (src, _, col, v) in cells if col == "duration_s"}
print(
    f"reached {len(cells)} source cells; recomputed from them: {sum(s / duration for s in spikes) / len(spikes)!r}"
)

print("\n== 2. mouse_02 s1: the group with a SHARED memoized result")
cells2 = trace(S, "mouse_02", "s1")
units_rows = sorted(
    {rec for (src, rec, col, _) in cells2 if src == "units"}, key=lambda x: int(x[4:])
)
print(
    f"reached units rows (exact, via pipeline records): {units_rows} ({len(units_rows)} distinct)"
)
old = token_only(S, "mouse_02", "s1")
print(
    f"round one's token-only path reaches:            {sorted(set(old), key=lambda x: (
            int(x[4:])
        ))} ({len(set(old))} distinct; {old.count('row_16')}x row_16)"
)

print("\n== 3. The same trace on the job LOADED from its file (load_only run)")
job.save(root / "job.json")
db2 = DeltaTableDatabase(root / "store")
loaded = PipelineJob.load(root / "job.json", database=db2)
loaded.run(load_only=True)
L = read_job(loaded, db2)
with contextlib.redirect_stdout(io.StringIO()):
    same1 = trace(L, "mouse_03", "s2") == cells
    same2 = trace(L, "mouse_02", "s1") == cells2
print(
    "loaded job reaches the same source cells: mouse_03 s2",
    same1,
    "; mouse_02 s1",
    same2,
)
