"""Round trips (ITL-1388 question 6): saved again == saved first? loaded hashes == live?

Usage: ``python roundtrip.py <workdir> <job> <situation>``; reads ``<job>_job.json``,
the reader's ``out/<job>.<situation>.resaved.json`` and ``expected.json``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def diff(a, b, path=""):
    out = []
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                out.append(f"{path}/{k}: only in {'first' if k in a else 'second'}")
            else:
                out += diff(a[k], b[k], f"{path}/{k}")
    elif a != b:
        out.append(f"{path}: {str(a)[:90]!r} -> {str(b)[:90]!r}")
    return out


w = Path(sys.argv[1])
job, sit = sys.argv[2], sys.argv[3]
first = json.loads((w / f"{job}_job.json").read_text())
second = json.loads((w / "out" / f"{job}.{sit}.resaved.json").read_text())
d = diff(first, second)
print(f"# {w.name}/{job} ({sit}): {len(d)} differences between saved and saved-again")
for line in d[:40]:
    print("  ", line)
rec = json.loads((w / "out" / f"{job}.{sit}.json").read_text())
live = json.loads((w / "expected.json").read_text())[job]
print("# hashes and paths: loaded (before a run) vs live")
for label in sorted(live):
    lv, ld = live[label], rec["before"].get(label, {})
    for attr in ("content_hash", "pipeline_hash", "node_identity_path"):
        if lv.get(attr) != ld.get(attr) and not str(lv.get(attr)).startswith("ERROR"):
            print(
                f"   {label}.{attr}: live {str(lv.get(attr))[:60]} loaded {str(ld.get(attr))[:60]}"
            )
