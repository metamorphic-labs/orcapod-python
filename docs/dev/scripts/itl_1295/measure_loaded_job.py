"""ITL-1295: a saved and loaded job reads its function node's stored results.

usage: uv run python docs/dev/scripts/itl_1295/measure_loaded_job.py

Runs node ``one`` of ``measure_read_path.py`` over an in-memory database, saves the job,
loads it over the same database and reads ``one``'s stored view (``as_source``) and
``get_all_records()``; ``k`` is compared with the input exactly.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import measure_read_path as m  # noqa: E402  # pyright: ignore[reportImplicitRelativeImport]  # a sibling script

from orcapod import PipelineJob  # noqa: E402
from orcapod.databases import InMemoryArrowDatabase  # noqa: E402
from orcapod.pipeline import SyncPipelineOrchestrator  # noqa: E402

for name in (
    "int64 (control)",
    "date64 off midnight",
    "decimal256(76,40)",
    "large_list<date64>",
):
    values = m.CASES[name]
    db = InMemoryArrowDatabase()
    job = m.build(values, db)
    job.run(orchestrator=SyncPipelineOrchestrator())
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "job.json"
        job.save(path)
        loaded = PipelineJob.load(path, database=db)
        out = []
        for read, action in (
            (
                "as_source",
                lambda: loaded.nodes["one"].as_source().as_table().column("k"),
            ),
            (
                "get_all_records",
                lambda: loaded.nodes["one"].get_all_records().column("k"),
            ),
        ):
            try:
                out.append(f"{read}: {m.verdict(m.exact(action()), m.exact(values))}")
            except BaseException as error:  # noqa: BLE001
                out.append(f"{read}: {type(error).__name__}: {str(error)[:60]}")
        print(f"{name:22} " + " | ".join(out))
