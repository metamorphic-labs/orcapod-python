"""ITL-1187: does changing delta.dataSkippingStatsColumns on an existing table apply to old files?

Creates a table without the property (deltalake keeps statistics for ``v`` and ``s`` by
default), sets the property to name only ``s``, appends once more, and prints for each data
file the columns it has a minimum for. Seen with deltalake 1.0.2: the first file keeps ``v``
and ``s``; the file appended after the change has ``s`` only. A change applies to files
written afterwards; earlier files are not rewritten.

Usage: uv run python docs/dev/scripts/itl_1187/alter_property.py <empty or missing directory>
"""

import os
import sys

import deltalake
import pyarrow as pa


def rows(lo: int, hi: int) -> pa.Table:
    """Rows with an int ``v``, a string ``s`` and a binary ``h``."""
    ids = list(range(lo, hi))
    return pa.Table.from_arrays(
        [
            pa.array(ids, pa.int64()),
            pa.array([f"s{i}" for i in ids], pa.large_string()),
            pa.array([b"h"] * len(ids), pa.large_binary()),
        ],
        names=["v", "s", "h"],
    )


def main(directory: str) -> None:
    """Run the experiment in ``directory`` and print the statistics of each file."""
    if os.path.exists(directory) and os.listdir(directory):
        sys.exit(f"{directory} is not empty: give an empty or missing directory")
    path = os.path.join(directory, "t")
    deltalake.write_deltalake(path, rows(0, 5))
    deltalake.DeltaTable(path).alter.set_table_properties(
        {"delta.dataSkippingStatsColumns": "`s`"}
    )
    deltalake.write_deltalake(path, rows(5, 10), mode="append")

    table = deltalake.DeltaTable(path)
    print("configuration:", table.metadata().configuration)
    adds = pa.table(table.get_add_actions(flatten=True)).to_pylist()
    for add in sorted(adds, key=lambda a: str(a["modification_time"])):
        with_min = sorted(
            str(k)[len("min.") :]
            for k, v in add.items()
            if str(k).startswith("min.") and v is not None
        )
        print(f"{add['path']}: statistics for {with_min}")


if __name__ == "__main__":
    main(sys.argv[1])
