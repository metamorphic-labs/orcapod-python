"""ITL-1295: how many pipeline rows each node stores after runs and reuse runs.

usage: uv run python docs/dev/scripts/itl_1295/measure_reuse_rows.py

``one`` (``inc``) and ``two`` (``dbl``) over a source of 2 rows tagged ``k`` (int64
control, ``date64`` off midnight). Three jobs over one in-memory database run the same
graph. After each, the stored pipeline rows of each node (count and ``k``) and the run
summary's computed / reused counts are printed.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import measure_read_path as m  # noqa: E402  # pyright: ignore[reportImplicitRelativeImport]  # a sibling script

from orcapod.databases import InMemoryArrowDatabase  # noqa: E402
from orcapod.pipeline import SyncPipelineOrchestrator  # noqa: E402

for name in ("int64 (control)", "date64 off midnight"):
    db = InMemoryArrowDatabase()
    print(name)
    for attempt in range(3):
        job = m.build(m.CASES[name], db)
        summary = job.run(orchestrator=SyncPipelineOrchestrator())
        line = []
        for pod, label in (("inc", "one"), ("dbl", "two")):
            [table] = [
                db.get_all_records(p)
                for p in db.list_table_paths()
                if p[1] == pod and p[-1] == "pdb_v1"
            ]
            assert table is not None
            node = summary.nodes[label]
            line.append(
                f"{label}: computed {node.computed} reused {node.reused}, "
                + f"stored {table.num_rows} rows k={m.exact(table.column('k'))}"
            )
        print(f"  job {attempt + 1}: " + " | ".join(line))
