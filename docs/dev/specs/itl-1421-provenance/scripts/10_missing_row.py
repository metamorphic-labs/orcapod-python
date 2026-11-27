"""Scenario 6: "why is this row missing?" Which step removed a source record, and why.

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.10_missing_row

Pipeline: units -> filter(probe == "imec0") -> join sessions (sessions restricted to
mouse_01..03 by a filter) -> firing_rate. Asks about two units records, one removed by the
probe filter, one by the join (no session row). Proves, with public API, on a job after
``job.run()``: no step records what it dropped (scripts 01), but every intermediate node's
rows are readable, so the first node whose rows lack the record id, and that node's
operator identity (the condition, the join), answer the question. For a join, the reason
"no match" is confirmed by looking for the record's key in the other input.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from ._common import workshop_job

from orcapod import PipelineJob, function_pod
from orcapod.sources import CSVSource

root = Path(tempfile.mkdtemp())
_, data_dir = workshop_job(root)  # writes the dataset


@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    return n_spikes / duration_s


from orcapod.databases import InMemoryArrowDatabase  # noqa: E402

job = PipelineJob(name="missing", database=InMemoryArrowDatabase())
with job:
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
    kept_sessions = sessions.polars_filter(
        __import__("polars").col("subject_id") != "mouse_04"
    )
    firing_rate.pod(
        units.filter({"probe": "imec0"}).select_data_columns(["n_spikes"]),
        kept_sessions.select_data_columns(["duration_s"]),
        label="rates",
    )
job.run()


def record_ids(node, source):
    t = node.as_table(all_info=True).to_pylist()
    out = set()
    for r in t:
        for k, v in r.items():
            if k.startswith("_tag_source_id") and v == source:
                out.add(r[k.replace("_tag_source_id", "_tag_record_id", 1)])
    return out


def path_to(node, target):
    """Nodes from ``node`` down to the source node ``target``, nearest first."""
    if node is target:
        return [node]
    for up in node.upstreams:
        p = path_to(up, target)
        if p:
            return [node] + p
    return []


def why_missing(record):
    chain = list(reversed(path_to(job.nodes["rates"], job.nodes["units"])))
    print(f"units record {record}:")
    for below, node in zip(chain, chain[1:]):
        if record in record_ids(below, "units") and record not in record_ids(
            node, "units"
        ):
            op = node.producer
            print(
                f"  present in {below.label}, missing from {node.label} "
                f"({type(op).__name__ if op else type(node).__name__})"
            )
            if op is not None:
                print(f"  operator identity: {op.identity_structure()!r}")
            if type(op).__name__ == "Join":
                row = [
                    r
                    for r in below.as_table(all_info=True).to_pylist()
                    if record in r.values()
                ][0]
                others = [u for u in node.upstreams if u is not below]
                keys = ("subject_id", "session")
                found = any(
                    all(o[k] == row[k] for k in keys)
                    for u in others
                    for o in u.as_table().to_pylist()
                )
                print(
                    f"  the other input has a row with "
                    f"{ {k: row[k] for k in keys} }: {found}"
                )
            return
    print("  not missing")


why_missing("row_1")  # mouse_01 s1 imec0 unit 1: kept
why_missing("row_4")  # mouse_01 s1 imec1 unit 0: removed by the filter
why_missing("row_48")  # mouse_04 s1 imec0 unit 0: removed by the join

print("\n== The function node's PIPELINE store (round two)")
# The pipeline store has one entry per input row the function node ever received, keyed
# by the row's system tags: a record id with no entry never reached the function; one
# with an entry reached it (then the result store holds what it was served).
node = job.nodes["rates"]
records = node.get_all_records(all_info=True)
pipe = [] if records is None else records.to_pylist()
for rec in ("row_1", "row_4", "row_48"):
    hits = [
        r
        for r in pipe
        for k, v in r.items()
        if k.startswith("_tag_source_id")
        and v == "units"
        and r[k.replace("_tag_source_id", "_tag_record_id", 1)] == rec
    ]
    print(f"  units {rec}: pipeline entries naming it: {len(hits)}")
print(
    "  An entry-less record never reached the function: the cause is upstream (above)."
    " This answer comes from the store alone, so it also holds in a later process"
    " (a loaded job) without re-running the operators; WHICH step dropped it still needs"
    " the operators' rows (held, cached with LOG, or re-derived by a load-only run)."
)
