"""Profile a first and a second sync job.run() over the Spiral fake (ITL-1061).

    uv run python superpowers/specs/itl-1061/profile_runs.py

Prints, per run, the wall time and the cumulative time of the functions the counters
point at (no counters installed here, so the times are not inflated by them). Take the
times as shares, not absolutes: the machine is shared.
"""

from __future__ import annotations

import cProfile
import os
import pstats
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import rerun_counts  # noqa: E402  # pyright: ignore[reportImplicitRelativeImport]  # a sibling script, run from this folder's path

from orcapod import PipelineJob  # noqa: E402
from orcapod.databases import InMemoryArrowDatabase  # noqa: E402
from tests.test_databases import fake_pyspiral  # noqa: E402

WATCH = (
    ("sync_orchestrator.py", "run"),
    ("source_node.py", "execute"),
    ("spiraldb_table_source.py", "_key_tokens"),
    ("stream_builder.py", "attach_provenance"),
    ("arrow_table_stream.py", "iter_data"),
    ("tag_data.py", "__init__"),
    ("operator_node.py", "execute"),
    ("static_output_pod.py", "_rows_to_table"),
    ("static_output_pod.py", "_materialize_to_stream"),
    ("function_node.py", "execute"),
    ("function_node.py", "compute_base_entry_id"),
    ("function_node.py", "get_cached_results"),
    ("function_node.py", "_process_data_internal"),
    ("arrow_utils.py", "hstack_tables"),
)


def show(profile: cProfile.Profile, title: str, wall: float) -> None:
    stats = pstats.Stats(profile)
    print(f"### {title}: wall {wall:.2f} s, load {os.getloadavg()[0]:.1f}")
    rows = []
    for (path, _line, name), (_cc, nc, _tt, ct, _callers) in stats.stats.items():  # pyright: ignore[reportAttributeAccessIssue]  # pstats keeps its table in an undeclared attribute
        for file, func in WATCH:
            if path.endswith(file) and name == func:
                rows.append((ct, nc, f"{file}:{func}"))
    for ct, nc, where in sorted(rows, reverse=True):
        print(f"{ct:8.2f} s cum  {nc:>8} calls  {where}")
    print()


with tempfile.TemporaryDirectory() as tmp, fake_pyspiral.install(Path(tmp)):
    sources = rerun_counts.spiral_sources(Path(tmp))
    job = PipelineJob(name="ws", store=InMemoryArrowDatabase())
    with job:
        rerun_counts.build(sources[0], sources[1], rerun_counts.make_pods())
    for i in (1, 2):
        profile = cProfile.Profile()
        start = time.perf_counter()
        profile.enable()
        job.run()
        profile.disable()
        show(profile, f"job.run() #{i}", time.perf_counter() - start)
