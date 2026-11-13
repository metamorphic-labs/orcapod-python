"""Name the callers of the Arrow hasher during one read of a SpiralDBTableSource (ITL-1061).

uv run python superpowers/specs/itl-1061/spiral_hash_callers.py
"""

from __future__ import annotations

import sys
import tempfile
import traceback
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import rerun_counts  # noqa: E402  # pyright: ignore[reportImplicitRelativeImport]  # a sibling script, run from this folder's path

from orcapod.hashing import arrow_hashers  # noqa: E402
from tests.test_databases import fake_pyspiral  # noqa: E402

callers: Counter[str] = Counter()
original = arrow_hashers.StarfixArrowHasher.hash_table


def counted(self, table):
    frames = traceback.extract_stack(limit=6)[:-1]
    callers[
        " <- ".join(
            f"{Path(f.filename).name}:{f.lineno}:{f.name}" for f in frames[::-1][:3]
        )
    ] += 1
    return original(self, table)


arrow_hashers.StarfixArrowHasher.hash_table = counted

with tempfile.TemporaryDirectory() as tmp, fake_pyspiral.install(Path(tmp)):
    sessions, experiments = rerun_counts.spiral_sources(Path(tmp))
    callers.clear()
    rows = list(experiments.iter_data())
    print(f"one read of experiments: {len(rows)} rows")
    for where, n in callers.most_common():
        print(f"{n:>7}  {where}")

    # The same source inside a job: what the source node's execute hashes on a re-run.
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase

    job = PipelineJob(name="ws", store=InMemoryArrowDatabase())
    with job:
        rerun_counts.build(sessions, experiments, rerun_counts.make_pods())
    job.run()
    callers.clear()
    job.run()
    print("second job.run(): hash_table callers (top 8)")
    for where, n in callers.most_common(8):
        print(f"{n:>7}  {where}")
