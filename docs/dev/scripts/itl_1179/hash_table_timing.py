"""Time ``hash_table`` on a 1e6-row table (ITL-1179 stage 1, proof C).

Run under each starfix version, one process at a time::

    uv run python docs/dev/scripts/itl_1179/hash_table_timing.py
    uv run --with starfix==0.5.0 python docs/dev/scripts/itl_1179/hash_table_timing.py

Prints the best of three wall-clock times for starfix's ``ArrowDigester.hash_table`` and for
Orcapod's ``StarfixArrowHasher.hash_table`` (which adds the semantic preprocessing; a flat
table skips it, a nested column goes through the Python visitor).

The numbers in ``README.md`` were taken at commit 937a68f7 (before Orcapod passed starfix's
float options). On a later checkout the 0.5.0 run works through ``_starfix_compat``, which
drops the two options starfix 0.5.0 does not have; the tables here hold no ``-0.0`` and no
NaN, so the options change no digest.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
from starfix import ArrowDigester

from orcapod.contexts import get_default_context

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
from docs.dev.scripts.itl_1179._starfix_compat import use_installed_starfix  # noqa: E402  # the repository root goes on the path first

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
    version = use_installed_starfix()
    print(f"starfix {version}, pyarrow {pa.__version__}, {N} rows")
    timings = [
        (
            "starfix hash_table, flat (int64, float64, large_string)",
            _best(lambda: ArrowDigester.hash_table(flat)),
        ),
        (
            "starfix hash_table, large_list<int64>",
            _best(lambda: ArrowDigester.hash_table(lists)),
        ),
        ("orcapod hash_table, flat", _best(lambda: hasher.hash_table(flat))),
        (
            "orcapod hash_table, large_list<int64> (visitor)",
            _best(lambda: hasher.hash_table(lists), 1),
        ),
    ]
    for label, seconds in timings:
        print(f"  {label + ':':<56} {seconds:.3f} s")


if __name__ == "__main__":
    main()
