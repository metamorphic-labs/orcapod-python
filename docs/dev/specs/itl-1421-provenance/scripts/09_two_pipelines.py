"""Scenario 5: two pipelines: where do they share work, where do they first diverge, why?

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.09_two_pipelines

Pipeline A is the tutorial's. Pipeline B inserts ``filter(probe == "imec0")`` before the
group-by. Proves, with public API (``content_hash``, ``pipeline_hash``, ``upstreams``,
``identity_structure``, ``get_all_records``):
- the nodes the two share are those with equal content hashes (labels may differ);
- the first divergence is a node of B whose content hash A lacks while every upstream's
  content hash A has; why: its own identity (the operator and its argument) is new;
- under one job name the shared function node's results are reused (B's run reads them
  from the store); under another job name nothing is shared.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from ._common import workshop_job

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.operators import Filter, GroupBy, SelectDataColumns
from orcapod.sources import CSVSource

root = Path(tempfile.mkdtemp())
store = DeltaTableDatabase(root / "store")
a, data_dir = workshop_job(root, database=store, name="A")
a.run()


@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    return n_spikes / duration_s


@function_pod(output_keys="mean_rate_hz")
def mean_rate(rate_hz: list[float]) -> float:
    return sum(rate_hz) / len(rate_hz)


# The same job name: a job's name is the top level of where its results are stored,
# so only jobs of one name share results (tutorial Part 6).
b = PipelineJob(name="A", database=store)
with b:
    units = CSVSource(
        str(data_dir / "units.csv"),
        tag_columns=["subject_id", "session", "probe", "unit"],
        tag_only_columns=True,
        source_id="units",
    )
    sessions = CSVSource(
        str(data_dir / "sessions.csv"),
        tag_columns=["subject_id", "session"],
        tag_only_columns=True,
        source_id="sessions",
    )
    rates = firing_rate.pod(
        units.select_data_columns(["n_spikes"]),
        sessions.select_data_columns(["duration_s"]),
        label="rates_b",
    )
    imec0 = Filter({"probe": "imec0"})(rates)
    grouped = GroupBy(["subject_id", "session"])(imec0)
    mean_rate.pod(SelectDataColumns(["rate_hz"])(grouped), label="imec0_means")
b.run()

in_a = {a.nodes[k].content_hash(): k for k in a.nodes}
print("node in B -> same node in A (by content hash):")
for label in sorted(b.nodes):
    print(f"  {label:20s} -> {in_a.get(b.nodes[label].content_hash(), '— (new)')}")

print("\nfirst divergence:")
for label in sorted(b.nodes):
    node = b.nodes[label]
    if node.content_hash() in in_a:
        continue
    ups = [u for u in node.upstreams]
    if all(u.content_hash() in in_a for u in ups):
        print(
            f"  {label} ({type(node).__name__}) over "
            f"{[in_a[u.content_hash()] for u in ups]}"
        )
        op = node.producer
        print(f"  node identity: {node.identity_structure()!r}"[:200])
        print(f"  its operator's identity: {op.identity_structure()!r}")

recs = b.nodes["rates_b"].get_all_records(columns={"meta": True}).to_pylist()
ids_a = {
    r["__data_id"]
    for r in a.nodes["rates"].get_all_records(columns={"meta": True}).to_pylist()
}
print(
    f"\nresult ids shared by A's and B's rates records: "
    f"{len(ids_a & {r['__data_id'] for r in recs})} of "
    f"{len({r['__data_id'] for r in recs})} distinct results"
)
print("B's run summary:", b.last_run)
print(
    "A differently named job over the same store shares nothing: the job name is the"
    " top of every stored path."
)

print("\n== Row level, from the stores (round two)")
from orcapod.utils.arrow_utils import fold_system_tag_values  # noqa: E402

from ._store import read_job, rows, system_tag_columns  # noqa: E402

SA, SB = read_job(a, store), read_job(b, store)
ra, rb = SA["rates"], SB["rates_b"]
print(
    f"  rates PIPELINE store: A and B use the same table: {ra.pipeline_path == rb.pipeline_path};"
    f" entries under the shared node content hash: {ra.own(ra.pipeline).num_rows}"
    f" (B wrote no new entry: same input system tags and input hashes)"
)
pa_, pb_ = SA["session_means"], SB["imec0_means"]
print(
    f"  session_means vs imec0_means PIPELINE tables: same table: {pa_.pipeline_path == pb_.pipeline_path}"
)


def members(S, group_input, gb_label, subject, session):
    """The group's members, by key tags, checked by refolding their system tags."""
    t = S[group_input].memory
    ms = sorted(
        (r for r in rows(t) if (r["subject_id"], r["session"]) == (subject, session)),
        key=lambda r: (r["probe"], r["unit"]),
    )
    (g,) = [
        r
        for r in rows(S[gb_label].memory)
        if (r["subject_id"], r["session"]) == (subject, session)
    ]
    gcols, mcols = system_tag_columns(S[gb_label].memory), system_tag_columns(t)
    assert all(
        g[gc] == fold_system_tag_values([m[mc] for m in ms], label="group_v1")
        for gc, mc in zip(gcols, mcols)
    )
    rid = [
        c
        for c in mcols
        if c.startswith("_tag_record_id")
        and ms[0][c.replace("record_id", "source_id", 1)] == "units"
    ][0]
    return {m[rid] for m in ms}


filt = [k for k in b.nodes if k.startswith("Filter")][0]
gb_b = [k for k in b.nodes if k.startswith("GroupBy")][0]
ma = members(SA, "rates", "GroupBy", "mouse_01", "s1")
mb = members(SB, filt, gb_b, "mouse_01", "s1")
print(f"  group mouse_01 s1: A's members (units record ids) {sorted(ma)}")
print(f"                     B's members                    {sorted(mb)}")
dropped = ma - mb


def units_ids(t):
    """The units record ids of a table (record ids are per source: sessions has row_0 too)."""
    out = set()
    for r in rows(t):
        for c in system_tag_columns(t):
            if c.startswith("_tag_source_id") and r[c] == "units":
                out.add(r[c.replace("_tag_source_id", "_tag_record_id", 1)])
    return out


in_rates_b = units_ids(rb.memory) & dropped
in_filter = units_ids(SB[filt].memory) & dropped
print(
    f"  {sorted(dropped)}: in B's rates (shared entries): {sorted(in_rates_b) == sorted(dropped)}; in B's {filt}: {sorted(in_filter)} -> the row-level divergence is at {filt}"
)
