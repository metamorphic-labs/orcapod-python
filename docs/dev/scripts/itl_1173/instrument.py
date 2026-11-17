"""ITL-1173: inclusive wall time of named code paths during one job run (no cProfile).

Usage: uv run python docs/dev/scripts/itl_1173/instrument.py <delta|memory|noop> <N> <commit_every (0 = node end)> <dbdir>
Prints a JSON object: {name: [calls, inclusive seconds]} plus run_s.
"""

import json
import sys
import time
from typing import Any
from collections import defaultdict

import deltalake
import deltalake.writer.writer as dl_writer

import orcapod as op
from orcapod import sources
from orcapod.core import cached_function_pod, data_function, result_cache
from orcapod.core.nodes import function_node
from orcapod.databases import delta_lake_databases as dldb
from orcapod.databases import in_memory_databases as imdb
from orcapod.hashing import arrow_hashers

STATS: dict[str, list[float]] = defaultdict(lambda: [0, 0.0])
_depth: dict[str, int] = defaultdict(int)


def wrap(owner, attr, name):
    orig = getattr(owner, attr)
    is_static = isinstance(owner.__dict__.get(attr), staticmethod)
    is_class = isinstance(owner.__dict__.get(attr), classmethod)
    func = owner.__dict__[attr].__func__ if (is_static or is_class) else orig

    def timed(*a, **k):
        # Count only the outermost call of a name (recursion / re-entry).
        _depth[name] += 1
        t0 = time.perf_counter()
        try:
            return func(*a, **k)
        finally:
            _depth[name] -= 1
            if _depth[name] == 0:
                s = STATS[name]
                s[0] += 1
                s[1] += time.perf_counter() - t0

    if is_static:
        setattr(owner, attr, staticmethod(timed))
    elif is_class:
        setattr(owner, attr, classmethod(timed))
    else:
        setattr(owner, attr, timed)


FJN = function_node.FunctionJobNode
D = dldb.DeltaTableDatabase
M = imdb.InMemoryArrowDatabase
for owner, attr, name in [
    (FJN, "execute", "node.execute"),
    (FJN, "compute_base_entry_id", "hash.base_entry_id"),
    (FJN, "compute_pipeline_entry_id", "hash.pipeline_entry_id"),
    (FJN, "_process_data_internal", "row.process_data_internal"),
    (FJN, "_add_pipeline_record", "row.add_pipeline_record"),
    (FJN, "_commit_pending_writes", "commit.node_commit"),
    (FJN, "get_cached_results", "node.get_cached_results"),
    (
        cached_function_pod.CachedFunctionPod,
        "process_data",
        "row.cached_pod.process_data",
    ),
    (result_cache.ResultCache, "lookup", "row.result_cache.lookup"),
    (result_cache.ResultCache, "store", "row.result_cache.store"),
    (data_function.PythonDataFunction, "call", "row.user_function_call"),
    (arrow_hashers.StarfixArrowHasher, "hash_table", "hash.arrow_hash_table(all)"),
    (D, "get_records_with_column_value", "db.get_records_with_column_value"),
    (D, "get_record_by_id", "db.get_record_by_id"),
    (D, "get_records_by_ids", "db.get_records_by_ids"),
    (D, "add_records", "db.add_records"),
    (D, "_flush_by_key", "commit.flush_by_key"),
    (D, "_extend_snapshot", "commit.extend_snapshot"),
    (D, "_snapshot_for_commit", "commit.snapshot_for_commit"),
    (D, "_scan_ids", "db.scan_ids"),
    (D, "_scan_rows", "db.scan_rows"),
    (M, "get_records_with_column_value", "db.get_records_with_column_value"),
    (M, "add_records", "db.add_records"),
    (M, "flush", "commit.memory_flush"),
    (M, "_filter_committed", "mem.filter_committed(index)"),
    (M, "_filter_existing_records", "mem.filter_existing_records"),
]:
    wrap(owner, attr, name)

# deltalake internals, as called from Orcapod's module.
_orig_write = deltalake.write_deltalake


def _timed_write(*a, **k):
    t0 = time.perf_counter()
    try:
        return _orig_write(*a, **k)
    finally:
        s = STATS["commit.write_deltalake"]
        s[0] += 1
        s[1] += time.perf_counter() - t0


dldb.deltalake.write_deltalake = _timed_write  # module attribute used by dldb
_orig_dt_init = deltalake.DeltaTable.__init__


def _timed_dt_init(self, *a, **k):
    t0 = time.perf_counter()
    try:
        return _orig_dt_init(self, *a, **k)
    finally:
        s = STATS["delta.DeltaTable()"]
        s[0] += 1
        s[1] += time.perf_counter() - t0


deltalake.DeltaTable.__init__ = _timed_dt_init
_orig_merge = deltalake.DeltaTable.merge


def _timed_merge(self, *a, **k):
    t0 = time.perf_counter()
    try:
        return _orig_merge(self, *a, **k)
    finally:
        s = STATS["commit.merge(builder)"]
        s[0] += 1
        s[1] += time.perf_counter() - t0


deltalake.DeltaTable.merge = _timed_merge
_ = dl_writer

# Per-commit sequence: (table kind, seconds) for every _flush_by_key call, in order.
SEQ: list[tuple[str, float]] = []
_inner_flush = D._flush_by_key


def _seq_flush(self, record_key):
    t0 = time.perf_counter()
    try:
        return _inner_flush(self, record_key)
    finally:
        kind = "result" if "/_result/" in record_key else "pipeline"
        SEQ.append((kind, time.perf_counter() - t0))


D._flush_by_key = _seq_flush


def main() -> None:
    mode, n, every, dbdir = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
    source = sources.DictSource(
        [{"key": i, "value": i} for i in range(n)], tag_columns="key"
    )

    @op.function_pod("result")
    def double(value: int) -> int:
        return 2 * value

    rows = every if every else n + 1
    if mode == "delta":
        database = op.databases.DeltaTableDatabase(
            dbdir, batch_size=max(1000, rows + 1)
        )
    else:
        database = (
            op.databases.NoOpArrowDatabase()
            if mode == "noop"
            else op.databases.InMemoryArrowDatabase()
        )
    job = op.PipelineJob("pipeline_v1", database=database)
    with job:
        double.pod(source)
    STATS.clear()
    t0 = time.perf_counter()
    job.run(commit_every_rows=rows)
    out: dict[str, Any] = {
        "run_s": time.perf_counter() - t0,
        "mode": mode,
        "n": n,
        "every": every,
    }
    if mode == "memory":
        out["stats_first"] = {
            k: [int(v[0]), round(v[1], 4)] for k, v in sorted(STATS.items())
        }
        STATS.clear()
        job2 = op.PipelineJob("pipeline_v1", database=database)
        with job2:
            double.pod(source)
        t0 = time.perf_counter()
        job2.run(commit_every_rows=rows)
        out["rerun_fresh_job_same_db_s"] = time.perf_counter() - t0
    out["stats"] = {k: [int(v[0]), round(v[1], 4)] for k, v in sorted(STATS.items())}
    for kind in ("result", "pipeline"):
        seq = [s for k, s in SEQ if k == kind]
        if seq:
            # Mean commit time per decile of the commit sequence.
            dec = max(1, len(seq) // 10)
            out[f"commit_curve_{kind}_ms"] = [
                round(1000 * sum(seq[i : i + dec]) / len(seq[i : i + dec]), 1)
                for i in range(0, len(seq), dec)
            ]
            out[f"commits_{kind}"] = len(seq)
    SEQ.clear()
    print(json.dumps(out))


if __name__ == "__main__":
    main()
