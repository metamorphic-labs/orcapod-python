"""One ITL-1173 measurement in a fresh interpreter; prints one JSON line.

Usage: uv run python docs/dev/scripts/itl_1173/bench_one.py <mode> <N> <commit_every> <dbdir> [--rerun] [--profile out.prof]
  mode: delta | memory | noop | nojob
  commit_every: rows per commit, or 0 for "one commit at node end"
"""

import json
import os
import sys
import time
from typing import Any

import orcapod as op
from orcapod import sources


def table_stats(root: str) -> dict[str, dict[str, int]]:
    """Commits (versions), data files and bytes per Delta table under root."""
    out = {}
    for dirpath, dirnames, filenames in os.walk(root):
        if "_delta_log" in dirnames:
            log = os.path.join(dirpath, "_delta_log")
            versions = [f for f in os.listdir(log) if f.endswith(".json")]
            parquet = [f for f in filenames if f.endswith(".parquet")]
            size = sum(os.path.getsize(os.path.join(dirpath, f)) for f in parquet)
            rel = os.path.relpath(dirpath, root)
            out[rel] = {
                "versions": len(versions),
                "files": len(parquet),
                "bytes": size,
            }
            dirnames.remove("_delta_log")
    return out


def main() -> None:
    mode, n, every, dbdir = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
    rerun = "--rerun" in sys.argv
    prof = (
        sys.argv[sys.argv.index("--profile") + 1] if "--profile" in sys.argv else None
    )

    source = sources.DictSource(
        [{"key": i, "value": i} for i in range(n)], tag_columns="key"
    )

    @op.function_pod("result")
    def double(value: int) -> int:
        return 2 * value

    result: dict[str, Any] = {"mode": mode, "n": n, "every": every, "rerun": rerun}
    if mode == "nojob":
        t0 = time.perf_counter()
        double.pod(source).run()
        result["run_s"] = time.perf_counter() - t0
        print(json.dumps(result))
        return

    if mode == "delta":
        # commit_every_rows must be < batch_size; 0 = one commit at node end.
        rows_per_commit = every if every else n + 1
        database = op.databases.DeltaTableDatabase(
            dbdir, batch_size=max(1000, rows_per_commit + 1)
        )
    else:
        rows_per_commit = every if every else n + 1
        database = (
            op.databases.NoOpArrowDatabase()
            if mode == "noop"
            else op.databases.InMemoryArrowDatabase()
        )

    job = op.PipelineJob("pipeline_v1", database=database)
    with job:
        double.pod(source)

    profiler = None
    if prof:
        import cProfile

        profiler = cProfile.Profile()
        profiler.enable()
    t0 = time.perf_counter()
    job.run(commit_every_rows=rows_per_commit)
    result["run_s"] = time.perf_counter() - t0
    if profiler and prof:
        profiler.disable()
        profiler.dump_stats(prof)
    # Same job again in the same process (state rule: held, nothing runs).
    t0 = time.perf_counter()
    job.run(commit_every_rows=rows_per_commit)
    result["same_job_second_run_s"] = time.perf_counter() - t0
    if mode == "delta":
        result["tables"] = table_stats(dbdir)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
