"""Measure work retained by a job, independently of function execution cost.

Run with ``uv run python bench/bench_pipeline_cached_rerun.py --rows 1000``.
Counters, rather than timing thresholds, distinguish reuse from fast recomputation.
"""

from __future__ import annotations

import argparse
import json
import time
from unittest.mock import patch

from orcapod import PipelineJob, function_pod
from orcapod.core.operators.group_by import GroupBy
from orcapod.databases import InMemoryArrowDatabase
from orcapod.hashing.arrow_hashers import StarfixArrowHasher
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.sources import DictSource


def measure(rows: int, grouped: bool, asynchronous: bool) -> None:
    """Print cold and warm run times and counts for one independent job."""
    counts = {"functions": 0, "arrow_hashes": 0, "group_by": 0}

    @function_pod(output_keys="y")
    def increment(x: int) -> int:
        counts["functions"] += 1
        return x + 1

    @function_pod(output_keys="y")
    def total(x: list[int]) -> int:
        counts["functions"] += 1
        return sum(x)

    source = DictSource(
        data=[{"g": i // 100, "k": i, "x": i} for i in range(rows)],
        tag_columns=["g", "k"],
        source_id="benchmark",
    )
    with PipelineJob(name="cache_benchmark", store=InMemoryArrowDatabase()) as job:
        if grouped:
            total.pod(source.group_by(["g"]).drop_tag_columns(["k"]), label="result")
        else:
            increment.pod(source, label="result")

    original_hash = StarfixArrowHasher.hash_table
    original_group = GroupBy.unary_static_process

    def hash_table(self, *args, **kwargs):
        counts["arrow_hashes"] += 1
        return original_hash(self, *args, **kwargs)

    def group(self, *args, **kwargs):
        counts["group_by"] += 1
        return original_group(self, *args, **kwargs)

    with (
        patch.object(StarfixArrowHasher, "hash_table", hash_table),
        patch.object(GroupBy, "unary_static_process", group),
    ):
        for run in ("cold", "warm", "warm_again"):
            counts.update(functions=0, arrow_hashes=0, group_by=0)
            started = time.perf_counter()
            summary = job.run(
                orchestrator=AsyncPipelineOrchestrator() if asynchronous else None
            )
            elapsed = time.perf_counter() - started
            if not summary.ok:
                raise RuntimeError(str(summary))
            print(
                json.dumps(
                    {
                        "rows": rows,
                        "grouped": grouped,
                        "async": asynchronous,
                        "run": run,
                        "seconds": elapsed,
                        **counts,
                    }
                ),
                flush=True,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1000)
    options = parser.parse_args()
    if options.rows < 1:
        parser.error("--rows must be positive")
    for is_grouped in (False, True):
        for is_async in (False, True):
            measure(options.rows, is_grouped, is_async)
