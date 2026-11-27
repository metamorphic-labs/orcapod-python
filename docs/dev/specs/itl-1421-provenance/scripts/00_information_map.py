"""The information map: every stored table of one tutorial run, every column, and every
link between them, each link CHECKED on the real rows.

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.00_information_map

The pipeline is the tutorial's (Part 8), with every operator node given a stored cache
(``CacheMode.LOG``) so that its rows are in the store as well:

  units ─ SelectDataColumns_3 ┐
                              ├ Join ─ rates (firing_rate) ─ GroupBy ─ SelectDataColumns_2
  sessions ─ SelectDataColumns_1 ┘                                     ─ session_means (mean_rate)

The tables are read DIRECTLY (``database.list_table_paths()``,
``database.get_all_records(path, record_id_column="__record_id")``), not through
``node.get_all_records()``, which drops the pipeline record's key, its base entry id and
``__node_content_hash``. The only non-reading call is ``node.compute_pipeline_entry_id``
(public), used to prove what the pipeline record's key is made of.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from orcapod.databases import DeltaTableDatabase
from orcapod.utils.arrow_utils import fold_system_tag_values

from ._common import workshop_job
from ._store import (
    KEY,
    by_system_tags,
    explain_system_tag_name,
    hash_text,
    log_operators,
    parse_token,
    read_job,
    rows,
    short_hash,
    system_tag_columns,
)

root = Path(tempfile.mkdtemp())
db = DeltaTableDatabase(root / "store")
job, _ = workshop_job(root, database=db)
log_operators(job, db, "workshop")
job.run()
S = read_job(job, db)


def example(value: Any) -> str:
    if isinstance(value, bytes):
        return repr(short_hash(value, 12))
    if isinstance(value, list):
        inner = ", ".join(example(v) for v in value[:2])
        return f"[{inner}{', …' if len(value) > 2 else ''}] ({len(value)})"
    text = repr(value)
    if "::" in text and len(text) > 60:  # a function output token: shorten the uri
        src, rec, col = parse_token(value)
        text = repr(f"{src.split(':')[0]}:<uri>::{rec[:10]}…::{col}")
    if text.startswith("'group_v1:"):
        text = text[:20] + "…'"
    return text


def show_table(title: str, path: tuple[str, ...], table: Any) -> None:
    shown = "/".join(p if len(p) < 24 else p[:16] + "…" for p in path)
    print(f"\n### {title}: {shown}  ({table.num_rows} rows)")
    for f in table.schema:
        name = f.name
        if name.startswith("_tag_"):
            name = name.split("::")[0] + "::…" + f" [{len(f.name.split('::')) - 2} ext]"
        print(f"  {name:38s} {str(f.type):42s} {example(table[f.name][0].as_py())}")


print("== 1. Every stored table of the run (list_table_paths)")
for label, n in S.nodes.items():
    if n.pipeline is not None:
        show_table(f"{label} PIPELINE (pdb_v1)", n.pipeline_path, n.pipeline)
    if n.result is not None:
        show_table(f"{label} RESULT (rdb_v1)", n.result_path, n.result)
    if n.cache is not None:
        show_table(f"{label} OPERATOR CACHE (odb_v1)", n.cache_path, n.cache)
print(f"\n{len(S.paths)} stored tables in all; source nodes store nothing.")

print("\n== 2. In-memory node tables (as_table(all_info=True)): columns per node")
for label, n in S.nodes.items():
    groups = {
        "tags": [c for c in n.memory.column_names if not c.startswith("_")],
        "system tags": system_tag_columns(n.memory),
        "other": [
            c
            for c in n.memory.column_names
            if c.startswith("_") and not c.startswith("_tag_")
        ],
    }
    print(
        f"  {label:20s} {n.kind:16s} {n.memory.num_rows:3d} rows; "
        f"{len(groups['system tags'])} system-tag cols; {groups['other']}"
    )

print("\n== 3. System-tag column NAMES: which upstream each name records")
for label in ["units", "Join", "rates", "GroupBy", "session_means"]:
    print(f"  {label}:")
    for c in system_tag_columns(S[label].memory):
        print(f"    {explain_system_tag_name(c, S)}")


def check(text: str, ok: int, total: int) -> None:
    print(f"  [{'OK' if ok == total else 'NO'}] {text}: {ok} of {total}")


print("\n== 4. Links, checked on the rows")
rates, means = S["rates"], S["session_means"]
r_pipe = rows(rates.own(rates.pipeline))
r_res = {r[KEY]: r for r in rows(rates.result)}
print(" function node 'rates': pipeline record -> result")
check(
    "pipeline.__node_content_hash = node.content_hash() (the table is shared by "
    "every node of this pipeline hash in the job's scope)",
    len(r_pipe),
    rates.pipeline.num_rows,
)
check(
    "pipeline.__data_id = result.__record_id (the result row it was served)",
    sum(r["__data_id"] in r_res for r in r_pipe),
    len(r_pipe),
)
check(
    "pipeline.__input_data_hash = result.__input_data_hash of that result",
    sum(
        r["__input_data_hash"] == r_res[r["__data_id"]]["__input_data_hash"]
        for r in r_pipe
    ),
    len(r_pipe),
)
check(
    "result._source_rate_hz = '<uri>::<hex of result.__record_id>::rate_hz'",
    sum(parse_token(r["_source_rate_hz"])[1] == k.hex() for k, r in r_res.items()),
    len(r_res),
)
print(
    f"  {len(r_pipe)} pipeline records -> {len(set(r['__data_id'] for r in r_pipe))} "
    f"distinct results ({len(r_res)} result rows)"
)
shared = [
    d
    for d in {r["__data_id"] for r in r_pipe}
    if sum(x["__data_id"] == d for x in r_pipe) > 1
]
for d in shared:
    recs = [r for r in r_pipe if r["__data_id"] == d]
    print(
        f"  shared result {short_hash(d)}: pointed at by {len(recs)} pipeline records:"
    )
    for r in recs:
        uid = [
            r[c]
            for c in system_tag_columns(rates.pipeline)
            if c.startswith("_tag_record_id")
            and r[c.replace("record_id", "source_id", 1)] == "units"
        ][0]
        print(
            f"    entry {short_hash(r[KEY])} {r['subject_id']} {r['session']} "
            f"{r['probe']} unit {r['unit']}: units {uid}, __computed={r['__computed']}"
        )

print(" 'rates' pipeline record -> its upstream entry (the Join row)")
join_mem = by_system_tags(S["Join"].memory)
cols = system_tag_columns(rates.pipeline)
check(
    "pipeline record's system tags = one Join row's system tags (same names: a "
    "function pod passes them through)",
    sum(tuple(r[c] for c in cols) in join_mem for r in r_pipe),
    len(r_pipe),
)
check(
    "pipeline.__input_data_hash = that Join row's _content_hash",
    sum(
        hash_text(r["__input_data_hash"])
        == join_mem[tuple(r[c] for c in cols)]["_content_hash"]
        for r in r_pipe
    ),
    len(r_pipe),
)
check(
    "pipeline._source_<input> = that Join row's _source_<input> (the input tokens)",
    sum(
        all(
            r[t] == join_mem[tuple(r[c] for c in cols)][t]
            for t in ("_source_n_spikes", "_source_duration_s")
        )
        for r in r_pipe
    ),
    len(r_pipe),
)
rates_mem = by_system_tags(rates.memory)
check(
    "pipeline.__output_data_hash = the node's output row _content_hash",
    sum(
        hash_text(r["__output_data_hash"])
        == rates_mem[tuple(r[c] for c in cols)]["_content_hash"]
        for r in r_pipe
    ),
    len(r_pipe),
)
node = job.nodes["rates"]
keys = {r[KEY] for r in r_pipe}
base = {r["__pipeline_base_entry_id"] for r in r_pipe}
made = [
    (node.compute_pipeline_entry_id(t, d), node.compute_base_entry_id(t, d))
    for t, d in job.nodes["Join"].iter_data()
]
check(
    "pipeline.__record_id = hash(input SYSTEM TAGS + input data hash + recomputation "
    "index); __pipeline_base_entry_id = the same without the index",
    sum(k in keys and b in base for k, b in made),
    len(made),
)

print(" Join row -> source rows (system tags and tokens agree)")
units_mem = {
    r[
        [
            c
            for c in system_tag_columns(S["units"].memory)
            if c.startswith("_tag_record_id")
        ][0]
    ]: r
    for r in rows(S["units"].memory)
}
jrows = rows(S["Join"].memory)
ucol = [
    c
    for c in system_tag_columns(S["Join"].memory)
    if c.startswith("_tag_record_id")
    and jrows[0][c.replace("record_id", "source_id", 1)] == "units"
][0]
check(
    "Join row's units record-id system tag = the record id in its _source_n_spikes token",
    sum(parse_token(r["_source_n_spikes"])[1] == r[ucol] for r in jrows),
    len(jrows),
)
check(
    "that record id names a units row with the same n_spikes value",
    sum(units_mem[r[ucol]]["n_spikes"] == r["n_spikes"] for r in jrows),
    len(jrows),
)

print(" GroupBy row -> its member rows of 'rates' (key tags + fold)")
gb = rows(S["GroupBy"].memory)
gcols = system_tag_columns(S["GroupBy"].memory)
rcols = system_tag_columns(rates.memory)
ok_fold = ok_tokens = 0
for g in gb:
    members = sorted(
        (
            r
            for r in rows(rates.memory)
            if (r["subject_id"], r["session"]) == (g["subject_id"], g["session"])
        ),
        key=lambda r: (r["probe"], r["unit"]),
    )
    refold = {
        gc: fold_system_tag_values([m[rc] for m in members], label="group_v1")
        for gc, rc in zip(gcols, rcols)
    }
    ok_fold += all(g[gc] == refold[gc] for gc in gcols)
    ok_tokens += g["_source_rate_hz"] == [m["_source_rate_hz"] for m in members]
check(
    "each folded system tag = fold of the members' values, members = 'rates' rows with "
    "the group's KEY TAGS, in member order (non-key tags in name order: probe, unit)",
    ok_fold,
    len(gb),
)
check(
    "the grouped row's token list = the members' tokens in that same order (row-aligned)",
    ok_tokens,
    len(gb),
)

print(" 'session_means' pipeline record -> its upstream entry")
m_pipe = rows(means.own(means.pipeline))
sel2 = by_system_tags(S["SelectDataColumns_2"].memory)
mcols = system_tag_columns(means.pipeline)
check(
    "pipeline record's (folded) system tags = one SelectDataColumns_2 row = one GroupBy row",
    sum(
        tuple(r[c] for c in mcols) in sel2
        and tuple(r[c] for c in mcols) in by_system_tags(S["GroupBy"].memory)
        for r in m_pipe
    ),
    len(m_pipe),
)
check(
    "pipeline.__input_data_hash = that row's _content_hash",
    sum(
        hash_text(r["__input_data_hash"])
        == sel2[tuple(r[c] for c in mcols)]["_content_hash"]
        for r in m_pipe
    ),
    len(m_pipe),
)

print(" operator caches (odb_v1) = the operator's in-memory rows")
for label in [
    "SelectDataColumns_1",
    "SelectDataColumns_3",
    "Join",
    "GroupBy",
    "SelectDataColumns_2",
]:
    n = S[label]
    own = n.own(n.cache)
    mem = by_system_tags(n.memory)
    data = [c for c in n.memory.column_names if not c.startswith("_")]
    check(
        f"{label}: cache row (by system tags) has the in-memory row's tags and data",
        sum(
            all(
                r[c] == mem.get(tuple(r[s] for s in system_tag_columns(own)), {}).get(c)
                for c in data
            )
            for r in rows(own)
        ),
        n.memory.num_rows,
    )

print("\n== 5. What the convenience view drops")
view = job.nodes["rates"].get_all_records(all_info=True)
dropped = sorted(
    set(rates.pipeline.column_names) | set(rates.result.column_names) - {KEY}
)
dropped = [c for c in dropped if c not in view.column_names]
print("  node.get_all_records() lacks:", dropped)
