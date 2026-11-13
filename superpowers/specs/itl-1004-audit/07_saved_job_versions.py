"""ITL-1004 audit, measurement 07: the version information a saved job holds today.

Saves a small job (a ``DictSource`` and one function pod, in-memory store) and prints every
field that could identify the Orcapod version that wrote it.

Run: uv run python superpowers/specs/itl-1004-audit/07_saved_job_versions.py
Output: 07_saved_job_versions.out
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource


@function_pod(output_keys="doubled")
def double(v: int) -> int:
    return v * 2


def version_fields(obj: Any, path: str = "") -> list[str]:
    """Every key path whose name mentions a version or a context, with its value."""
    found: list[str] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            here = f"{path}.{key}" if path else str(key)
            if any(w in str(key).lower() for w in ("version", "context")):
                found.append(f"{here} = {json.dumps(value)[:80]}")
            found.extend(version_fields(value, here))
    elif isinstance(obj, list):
        for i, value in enumerate(obj):
            found.extend(version_fields(value, f"{path}[{i}]"))
    return found


job = PipelineJob(name="versions", store=InMemoryArrowDatabase())
with job:
    double.pod(DictSource(data=[{"k": 1, "v": 2}], tag_columns=["k"], source_id="src"))
_ = job.run()

with tempfile.TemporaryDirectory() as tmp:
    path = Path(tmp) / "job.json"
    job.save(str(path))
    saved = json.loads(path.read_text())

print("=== V1. Top-level keys of a saved job file")
print(f"    {sorted(saved)}")
print("\n=== V2. Every field naming a version or a data context")
for line in version_fields(saved):
    print(f"    {line}")
print("\n=== V3. A source config inside the job (bindings.sources)")
for name, config in saved["bindings"]["sources"].items():
    print(f"    {name}: keys {sorted(config)}")
    print(f"    {name}.config keys: {sorted(config.get('config', {}))}")
