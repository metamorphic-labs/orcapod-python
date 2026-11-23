"""Scenario 4: "reproduce and prove a result": a provenance manifest and its check.

Run from docs/dev/specs/itl-1421-provenance: uv run python -m scripts.08_manifest

Proves:
- every item of a manifest is readable today through public API: per node its kind,
  ``content_hash()``, ``pipeline_hash()``, a source's ``source_id``, a function node's
  ``node_uri`` (name, output schema hash, major, data function type), the data context,
  and per row the data ``_content_hash``; the stored records add the function's content
  hash, git hash, Python version and timestamps;
- the job loaded from its file gives the same manifest;
- node hashes alone do NOT prove the data: a source's content hash is an ADDRESS (source
  id, schema, key columns), so editing a value in the CSV leaves every node hash as it
  was. The proof needs the row hashes of the source records the result read (the trace of
  scenario 1): the edited row's ``_content_hash`` differs.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from ._common import workshop_job

from orcapod import PipelineJob
from orcapod.databases import DeltaTableDatabase


def manifest(job: Any) -> dict[str, Any]:
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


def row_hashes(job, label):
    t = job.nodes[label].as_table(columns={"content_hash": True})
    return sorted(t.column("_content_hash").to_pylist())


root = Path(tempfile.mkdtemp())
job, data_dir = workshop_job(root)
job.run()
m1 = manifest(job)
print("manifest entries:")
for label in ("units", "rates", "session_means"):
    print(f"  {label}: {m1[label]}")
rec = job.nodes["rates"].get_all_records(columns={"meta": True}).to_pylist()[0]
print(
    "  + per result:",
    {
        k: rec[k]
        for k in (
            "__pf_var_function_content_hash",
            "__pf_exec_python_version",
            "__pod_ts",
        )
    },
)

job.save(root / "job.json")
loaded = PipelineJob.load(
    root / "job.json", database=DeltaTableDatabase(root / "store")
)
loaded.run(load_only=True)
print("\nloaded job gives the same node manifest:", manifest(loaded) == m1)
print(
    "and the same output row hashes:",
    row_hashes(loaded, "session_means") == row_hashes(job, "session_means"),
)

print("\n== Edit one value in units.csv, build the job again over the file")
before_units = row_hashes(job, "units")
csv = data_dir / "units.csv"
lines = csv.read_text().splitlines()
fields = lines[17].split(",")  # row_16 (mouse_02 s1 imec0 unit 0)
fields[-1] = str(int(fields[-1]) + 1)
lines[17] = ",".join(fields)
csv.write_text("\n".join(lines) + "\n")
edited, _ = workshop_job(
    root, database=DeltaTableDatabase(root / "store2"), regenerate=False
)
edited.run()
m2 = manifest(edited)
print(
    "node hashes unchanged after the edit:",
    {k: (v["content_hash"], v["pipeline_hash"]) for k, v in m2.items()}
    == {k: (v["content_hash"], v["pipeline_hash"]) for k, v in m1.items()},
)
after_units = row_hashes(edited, "units")
print("source row hashes that differ:", len(set(after_units) - set(before_units)))
