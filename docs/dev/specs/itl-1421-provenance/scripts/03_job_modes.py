"""Audit 3: per-row lineage of a job node (b) after ``job.run()``, (c) on the job loaded
from its saved file with the store (``run(load_only=True)``), (d) through ``as_source()``.
Mode (a), a plain stream, is scripts 01 and 02.

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.03_job_modes

Proves:
- (b) and (c) present the same provenance columns and values for function and operator
  nodes (the loaded run reads stored results, operators recompute over them);
- a function node's stored record (``get_all_records``) joins the pipeline record (tags,
  system tags, the INPUT columns' source-info tokens) with the result (output values,
  output tokens, ``__data_id`` = the token's record id, ``__input_data_hash``,
  ``__output_data_hash``, function and run metadata): the one place an output is linked
  to its inputs' tokens;
- (d) ``as_source()`` of a function node is a new root WITHOUT lineage: no system tags,
  and its output source-info tokens are null; the rows can be re-joined to the stored
  records by tags and ``_content_hash`` == ``__output_data_hash``.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from ._common import short, workshop_job

from orcapod import PipelineJob
from orcapod.databases import DeltaTableDatabase

root = Path(tempfile.mkdtemp())
job, _ = workshop_job(root)
job.run()
job.save(root / "job.json")
loaded = PipelineJob.load(
    root / "job.json", database=DeltaTableDatabase(root / "store")
)
loaded.run(load_only=True)


def lineage(table):
    cols = sorted(c for c in table.column_names if c.startswith(("_tag", "_source_")))
    rows = sorted(
        tuple(str(table.column(c)[i].as_py()) for c in cols)
        for i in range(table.num_rows)
    )
    return cols, rows


print("== (b) after job.run() vs (c) loaded, run(load_only=True)")
for label in ["units", "Join", "rates", "GroupBy", "session_means"]:
    b = job.nodes[label].as_table(all_info=True)
    c = loaded.nodes[label].as_table(all_info=True)
    cols, rows_b = lineage(b)
    same = lineage(c) == (cols, rows_b)
    kind = type(job.nodes[label]).__name__
    print(
        f"  {label:14s} {kind:16s} rows={b.num_rows:3d} provenance columns="
        f"{len(cols)} same in (c): {same}"
    )
    # Function output tokens name a fresh result id per computation: the loaded run
    # reads the stored ones, so they are equal, not re-minted.

print("\n== The stored record of a function node: where an output meets its inputs")
rec = job.nodes["rates"].get_all_records(all_info=True)
print("  columns:", [short(c) for c in rec.column_names if not c.startswith("__pf")])
row = rec.slice(0, 1).to_pylist()[0]
print("  input tokens :", row["_source_n_spikes"], row["_source_duration_s"])
print("  output token :", row["_source_rate_hz"])
print(
    "  __data_id hex == token record id:",
    row["__data_id"].hex() == row["_source_rate_hz"].split("::")[1],
)
print(
    "  pipeline record columns are NOT shown by get_all_records: __node_content_hash,"
    " the pipeline entry ids (dropped on purpose)."
)

print("\n== (d) as_source(): a new root")
src = job.nodes["session_means"].as_source()
t = src.as_table(all_info=True)
print("  columns:", t.column_names)
print("  system tags:", [c for c in t.column_names if c.startswith("_tag")] or "none")
print("  output tokens:", set(t.column("_source_mean_rate_hz").to_pylist()))
stored = job.nodes["session_means"].get_all_records(all_info=True)
by = {
    (r["subject_id"], r["session"], r["__output_data_hash"]): r["_source_mean_rate_hz"]
    for r in stored.to_pylist()
}
matched = 0
for r in t.to_pylist():
    prefix, _, hexdigest = r["_content_hash"].partition(":")
    digest = prefix.encode() + b":" + bytes.fromhex(hexdigest)
    matched += (r["subject_id"], r["session"], digest) in by
print(
    f"  re-joined to the stored records by tags + _content_hash: {matched} of {t.num_rows}"
)
