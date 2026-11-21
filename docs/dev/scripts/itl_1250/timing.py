"""ITL-1250 (d): the time of one GroupBy, in the current checkout.

usage: uv run python docs/dev/scripts/itl_1250/timing.py <rows> <groups>

A source with tags g (the key, ``groups`` distinct values) and t (a non-key tag that
orders the members), data v (float64) and w (large_string); record ids by position.
Times ``GroupBy("g")(source).as_table()`` once, after building the source. Run it on
two checkouts (``main`` and this branch) to compare, one process at a time.
"""

from __future__ import annotations

import sys
import time

import numpy as np
import pyarrow as pa

import orcapod as op
from orcapod.core.operators import GroupBy


def main() -> None:
    rows, groups = int(float(sys.argv[1])), int(float(sys.argv[2]))
    rng = np.random.default_rng(0)
    table = pa.Table.from_arrays(
        [
            pa.array(rng.integers(0, groups, rows), pa.int64()),
            pa.array(rng.permutation(rows), pa.int64()),
            pa.array(rng.random(rows), pa.float64()),
            pa.array([f"w{i}" for i in range(rows)], pa.large_string()),
        ],
        names=["g", "t", "v", "w"],
    )
    source = op.sources.ArrowTableSource(
        table, tag_columns=["g", "t"], tag_only_columns=True, source_id="s"
    )
    _ = source.as_table()
    start = time.perf_counter()
    out = GroupBy("g")(source).as_table()
    elapsed = time.perf_counter() - start
    print(
        f"rows={rows:.0e} groups={groups:.0e} out={out.num_rows} seconds={elapsed:.2f}"
    )


if __name__ == "__main__":
    main()
