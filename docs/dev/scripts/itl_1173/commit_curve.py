"""ITL-1173: raw deltalake cost per commit against the table version.

Appends 300 batches of 100 rows (the shape of a function node's pipeline table) and
times, at selected versions: a fresh table load, an append given the URI, a load plus
``to_pyarrow_dataset``, and ``update_incremental`` of a kept handle. Then times appends
written through a kept handle.

Usage: uv run python docs/dev/scripts/itl_1173/commit_curve.py <empty dir>
"""

from __future__ import annotations

import json
import os
import sys
import time

import deltalake
import pyarrow as pa

SAMPLED = (1, 2, 10, 50, 99, 100, 101, 150, 200, 250, 299, 300)


def batch(k: int) -> pa.Table:
    ids = range(k * 100, k * 100 + 100)
    return pa.table(
        {
            "__record_id": pa.array(
                [i.to_bytes(32, "big") for i in ids], pa.large_binary()
            ),
            "h1": pa.array([i.to_bytes(40, "big") for i in ids], pa.large_binary()),
            "h2": pa.array([i.to_bytes(40, "big") for i in ids], pa.large_binary()),
            "v": pa.array(ids, pa.int64()),
            "ctx": pa.array(["std:v1:default"] * 100, pa.large_string()),
        }
    )


def ms(seconds: float) -> float:
    return round(1e3 * seconds, 1)


def main() -> None:
    d = sys.argv[1]
    if os.path.exists(d) and os.listdir(d):
        sys.exit(f"{d} is not empty: give an empty or missing directory")
    deltalake.write_deltalake(d, batch(0), mode="overwrite")
    handle = deltalake.DeltaTable(d)
    for k in range(1, 301):
        t0 = time.perf_counter()
        loaded = deltalake.DeltaTable(d)
        t_load = time.perf_counter() - t0
        t0 = time.perf_counter()
        deltalake.write_deltalake(d, batch(k), mode="append")
        t_write = time.perf_counter() - t0
        t0 = time.perf_counter()
        _ = deltalake.DeltaTable(d).to_pyarrow_dataset()
        t_dataset = time.perf_counter() - t0
        t0 = time.perf_counter()
        handle.update_incremental()
        t_incremental = time.perf_counter() - t0
        if k in SAMPLED:
            print(
                json.dumps(
                    {
                        "version": loaded.version() + 1,
                        "load_ms": ms(t_load),
                        "write_append_uri_ms": ms(t_write),
                        "load+to_pyarrow_dataset_ms": ms(t_dataset),
                        "update_incremental_ms": ms(t_incremental),
                    }
                ),
                flush=True,
            )
    kept = deltalake.DeltaTable(d)
    times: list[float] = []
    for k in range(301, 321):
        t0 = time.perf_counter()
        deltalake.write_deltalake(kept, batch(k), mode="append")
        times.append(time.perf_counter() - t0)
    print(
        "append through a kept handle, min / mean ms:",
        ms(min(times)),
        ms(sum(times) / len(times)),
    )
    print(
        "checkpoints:",
        sorted(f for f in os.listdir(f"{d}/_delta_log") if not f.endswith(".json")),
        flush=True,
    )
    os._exit(0)  # the dataset reads above can hang interpreter exit (ITL-892)


if __name__ == "__main__":
    main()
