"""Lookup cost of finding stored entries without a stored base entry id (ITL-1466, round 6).

A function node's run asks, for each input row, whether its entry (tags, system
tags, input data hash) is stored. Three ways, timed on a pipeline table of N rows
with N inputs, all present:

  a. stored id: the base entry id kept as a plain column; hash each input's
     preimage, then an is_in filter on the column (today's way);
  b. join: no stored id; join the inputs' (system tags, input hash) with the same
     columns of the table (Polars inner join);
  c. rehash: no stored id; hash the preimage of every stored row.

Runs on the library as it is. Run: uv run python docs/dev/specs/itl-1466/time_entry_lookup.py
"""

import os
import sys
import time

import polars as pl
import pyarrow as pa

from orcapod.contexts import get_default_arrow_hasher

HASHER = get_default_arrow_hasher()
SID, RID, IDH = "_tag::source_id:x", "_tag::record_id:x", "__input_data_hash"


def table(n: int) -> pa.Table:
    return pa.table(
        {
            "s": pa.array([f"s{i % 97}" for i in range(n)], pa.large_string()),
            SID: pa.array(["dict_v1:units"] * n, pa.large_string()),
            RID: pa.array([f"row_{i}" for i in range(n)], pa.large_string()),
            IDH: pa.array(
                [b"arrow_v1:" + i.to_bytes(32, "big") for i in range(n)],
                pa.large_binary(),
            ),
        }
    )


def entry_id(row: pa.Table) -> bytes:
    return HASHER.hash_table(row).to_prefixed_digest()


def run(n: int) -> dict[str, float]:
    stored = table(n)
    inputs = table(n)
    out: dict[str, float] = {}

    t = time.perf_counter()
    input_ids = [entry_id(inputs.slice(i, 1)) for i in range(n)]
    out["hash inputs (all ways but b)"] = time.perf_counter() - t

    stored_ids = stored.append_column(
        "__pipeline_base_entry_id", pa.array(input_ids, pa.large_binary())
    )
    t = time.perf_counter()
    found = pl.from_arrow(stored_ids).filter(
        pl.col("__pipeline_base_entry_id").is_in(input_ids)
    )
    out["a. is_in on stored id"] = time.perf_counter() - t
    assert found.height == n

    t = time.perf_counter()
    keys = ["s", SID, RID, IDH]
    joined = pl.from_arrow(stored).join(pl.from_arrow(inputs), on=keys, how="semi")
    out["b. join on columns"] = time.perf_counter() - t
    assert joined.height == n

    if n <= 100_000:
        t = time.perf_counter()
        _ = [entry_id(stored.slice(i, 1)) for i in range(n)]
        out["c. rehash stored rows"] = time.perf_counter() - t
    return out


if __name__ == "__main__":
    print(sys.version.split()[0], os.cpu_count(), "cpus")
    for n in (10_000, 100_000, 1_000_000):
        for name, secs in run(n).items():
            print(f"N={n:>9,}  {name:<30} {secs:8.3f} s")
