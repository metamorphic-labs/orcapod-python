"""ITL-1004 audit, measurement 05: migration from tags_as_data, and today's output-name checks.

Run: uv run python superpowers/specs/itl-1004-audit/05_migration_and_collisions.py
Output: 05_migration_and_collisions.out
"""

from __future__ import annotations

import pyarrow as pa
from _common import describe, f64, i64, show, src  # pyright: ignore[reportImplicitRelativeImport]  # run as a file from this folder

from orcapod.core.datagrams import Data
from orcapod.operators import GroupBy, Index, Pick

print("=== M1. Function-result identity follows the data COLUMN NAME (Arrow hash)")
same = Data(pa.table({"spike_time": i64(10)}))
renamed = Data(pa.table({"spike_time_us": i64(10)}))
show(
    "Data{spike_time: 10} hash == Data{spike_time_us: 10} hash",
    lambda: same.content_hash() == renamed.content_hash(),
)

print("\n=== M2. Index / Pick: out= equal to an existing TAG name (today)")
g = GroupBy(["u"])(
    src({"u": i64(1, 1), "t": i64(5, 6), "v": f64(0.1, 0.2)}, ["u", "t"], "G")
)
show("grouped stream", lambda: describe(g))
show(
    "Index('v', 0, out='t')(g)  (t is a tag)",
    lambda: describe(Index("v", 0, out="t")(g)),
)
st = src(
    {
        "u": i64(1, 2),
        "d": pa.array([{"t": 1}, {"t": 2}], pa.struct([("t", pa.int64())])),
    },
    ["u"],
    "S",
)
show(
    "Pick('d', 't', out='u')(st)  (u is a tag)",
    lambda: describe(Pick("d", "t", out="u")(st)),
)
