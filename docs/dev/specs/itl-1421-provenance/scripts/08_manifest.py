"""Scenario 4: "reproduce and prove a result": a provenance manifest and its check.

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.08_manifest

Proves:
- NODE level: per node its kind, ``content_hash()``, ``pipeline_hash()``, a source's
  ``source_id``, a function node's uri, the data context are readable through public API,
  and the loaded job gives the same. They do NOT prove the data: a source's content hash
  is an ADDRESS, so editing a value in the CSV moves no node hash.
- ROW level, from the stores: the manifest of ONE cell is the chain of PIPELINE entries
  the trace of scenario 1 visits (entry id = hash of the input's system tags and input
  data hash), each with the RESULT it was served (function body hash, executor, time) and
  the source cells its input tokens name, with their row hashes. Re-checking it against a
  later run needs no computing beyond operators: per entry, the current input row with
  the same system tags must have the same content hash (= the entry's
  ``__input_data_hash``).
- After the CSV edit and a re-run into the SAME store, the pipeline store keeps both
  entries of the edited unit: same system tags, new input data hash, new entry id; and the
  group's entry likewise (its folded system tags unchanged, its input hash new). The check
  names exactly those two steps.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from orcapod import PipelineJob
from orcapod.databases import DeltaTableDatabase

from ._common import workshop_job
from ._store import (
    KEY,
    StoredJob,
    hash_text,
    read_job,
    rows,
    short_hash,
    system_tag_columns,
)


def node_manifest(job: Any) -> dict[str, Any]:
    nodes = {}
    for label in sorted(job.nodes):
        node = job.nodes[label]
        entry = {
            "kind": type(node).__name__,
            "content_hash": node.content_hash().to_string(),
            "pipeline_hash": node.pipeline_hash().to_string(),
            "data_context": node.data_context_key,
        }
        if entry["kind"] == "SourceJobNode":
            entry["source_id"] = node.bound_source.source_id
        if entry["kind"] == "FunctionJobNode":
            entry["function"] = ":".join(node.node_uri)
        nodes[label] = entry
    return nodes


def sysv(r: dict[str, Any], cols: list[str]) -> tuple[Any, ...]:
    return tuple(r[c] for c in cols)


def cell_manifest(S: StoredJob, subject: str, session: str) -> list[dict[str, Any]]:
    """The pipeline entries one session_means cell depends on, with their results."""
    out = []
    (row,) = [
        r
        for r in rows(S["session_means"].memory)
        if (r["subject_id"], r["session"]) == (subject, session)
    ]
    todo = [("session_means", row)]
    while todo:
        label, r = todo.pop()
        n = S[label]
        cols = system_tag_columns(n.pipeline)
        (p,) = [
            x
            for x in rows(n.own(n.pipeline))
            if sysv(x, cols) == sysv(r, cols)
            and hash_text(x["__output_data_hash"]) == r["_content_hash"]
        ]
        (res,) = [x for x in rows(n.result) if x[KEY] == p["__data_id"]]
        tokens = [p[c] for c in p if c.startswith("_source_")]
        out.append(
            {
                "node": label,
                "system_tags": dict(zip(cols, sysv(p, cols))),
                "entry": p[KEY],
                "input_data_hash": hash_text(p["__input_data_hash"]),
                "result": p["__data_id"],
                "body": hash_text(res["__pf_var_function_content_hash"]),
                "executor": res["__pf_exec_executor_type"],
                "inputs": tokens,
            }
        )
        if (
            label == "session_means"
        ):  # the group's members (key tags; scenario 1 checks the fold)
            todo += [
                ("rates", m)
                for m in rows(S["rates"].memory)
                if (m["subject_id"], m["session"]) == (subject, session)
            ]
    return out


def verify(manifest: list[dict[str, Any]], S: StoredJob) -> list[str]:
    """Per entry: the CURRENT input row with the same system tags has the same hash."""
    upstream = {"session_means": "SelectDataColumns_2", "rates": "Join"}
    problems = []
    for e in manifest:
        t = S[upstream[e["node"]]].memory
        cols = list(e["system_tags"])
        now = [r for r in rows(t) if sysv(r, cols) == tuple(e["system_tags"].values())]
        if [r["_content_hash"] for r in now] != [e["input_data_hash"]]:
            rid = [
                v for c, v in e["system_tags"].items() if c.startswith("_tag_record_id")
            ]
            problems.append(
                f"{e['node']} entry {short_hash(e['entry'])} (record ids {[short_hash(v, 8) for v in rid]}): input now {[short_hash(r['_content_hash'], 8) for r in now]}, was {short_hash(e['input_data_hash'], 8)}"
            )
    return problems


root = Path(tempfile.mkdtemp())
db = DeltaTableDatabase(root / "store")
job, data_dir = workshop_job(root, database=db)
job.run()
m1 = node_manifest(job)
print("== Node manifest")
for label in ("units", "rates", "session_means"):
    print(f"  {label}: {m1[label]}")
job.save(root / "job.json")
loaded = PipelineJob.load(
    root / "job.json", database=DeltaTableDatabase(root / "store")
)
loaded.run(load_only=True)
print("  loaded job gives the same node manifest:", node_manifest(loaded) == m1)

print(
    "\n== Cell manifest of session_means mouse_02 s1 (pipeline entries + results + inputs)"
)
S = read_job(job, db)
cm = cell_manifest(S, "mouse_02", "s1")
for e in cm[:2]:
    print(
        f"  {e['node']:13s} entry {short_hash(e['entry'])} input {short_hash(e['input_data_hash'], 8)} result {short_hash(e['result'])} body {short_hash(e['body'], 8)} inputs {[i if isinstance(i, str) else f'{len(i)} tokens' for i in e['inputs']]}"
    )
print(f"  … {len(cm)} entries in all (1 mean + 8 rates)")
print("  verify against the same run:", verify(cm, S) or "OK")

print(
    "\n== Edit one value in units.csv (row_16), rebuild the job over the file, run into the SAME store"
)
csv = data_dir / "units.csv"
lines = csv.read_text().splitlines()
fields = lines[17].split(",")  # row_16 (mouse_02 s1 imec0 unit 0)
fields[-1] = str(int(fields[-1]) + 1)
lines[17] = ",".join(fields)
csv.write_text("\n".join(lines) + "\n")
edited, _ = workshop_job(root, database=db, regenerate=False)
edited.run()
print("  run summary:", edited.last_run)
m2 = node_manifest(edited)
print(
    "  node hashes unchanged after the edit:",
    {k: (v["content_hash"], v["pipeline_hash"]) for k, v in m2.items()}
    == {k: (v["content_hash"], v["pipeline_hash"]) for k, v in m1.items()},
)
E = read_job(edited, db)
for label in ("rates", "session_means"):
    pipe = rows(E[label].own(E[label].pipeline))
    cols = system_tag_columns(E[label].pipeline)
    seen: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for p in pipe:
        seen.setdefault(sysv(p, cols), []).append(p)
    twice = [v for v in seen.values() if len(v) > 1]
    print(
        f"  {label} PIPELINE store: {len(pipe)} entries; system tags with two entries: {len(twice)} -> {[(v[0]['subject_id'], v[0]['session'], v[0].get('probe'), v[0].get('unit'), [short_hash(x['__input_data_hash'], 8) for x in v]) for v in twice]}"
    )
print("  verify the old cell manifest against the new run:")
for line in verify(cm, E):
    print("   ", line)
