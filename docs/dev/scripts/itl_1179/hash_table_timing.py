"""Time ``hash_table`` on a 1e6-row table (ITL-1179 stage 1, proof C).

Run under each starfix version, one process at a time::

    uv run python docs/dev/scripts/itl_1179/hash_table_timing.py
    uv run --with starfix==0.5.0 python docs/dev/scripts/itl_1179/hash_table_timing.py

Prints the best of three wall-clock times for starfix's ``ArrowDigester.hash_table`` and for
Orcapod's ``StarfixArrowHasher.hash_table`` (which adds the semantic preprocessing; a flat
table skips it, a nested column goes through the Python visitor).
"""

from __future__ import annotations

import importlib.metadata
import time

import numpy as np
import pyarrow as pa
from starfix import ArrowDigester

from orcapod.contexts import get_default_context

N = 1_000_000


def _best(fn, repeat: int = 3) -> float:
    times = []
    for _ in range(repeat):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    return min(times)


def main() -> None:
    rng = np.random.default_rng(0)
    flat = pa.table(
        {
            "i": pa.array(rng.integers(-(2**40), 2**40, N), pa.int64()),
            "f": pa.array(rng.standard_normal(N), pa.float64()),
            "s": pa.array([f"row-{i % 1000}" for i in range(N)], pa.large_string()),
        }
    )
    lists = pa.table(
        {"l": pa.array([[i, i + 1] for i in range(N)], pa.large_list(pa.int64()))}
    )
    hasher = get_default_context().arrow_hasher
    print(
        f"starfix {importlib.metadata.version('starfix')}, pyarrow {pa.__version__}, {N} rows"
    )
    print(
        f"  starfix hash_table, flat (int64, float64, large_string): {_best(lambda: (
                ArrowDigester.hash_table(flat)
            )):.3f} s"
    )
    print(
        f"  starfix hash_table, large_list<int64>:                  {_best(lambda: (
                ArrowDigester.hash_table(lists)
            )):.3f} s"
    )
    print(
        f"  orcapod hash_table, flat:                               {_best(lambda: (
                hasher.hash_table(flat)
            )):.3f} s"
    )
    print(
        f"  orcapod hash_table, large_list<int64> (visitor):        {_best(lambda: (
                hasher.hash_table(lists)
            ), 1):.3f} s"
    )


if __name__ == "__main__":
    main()
