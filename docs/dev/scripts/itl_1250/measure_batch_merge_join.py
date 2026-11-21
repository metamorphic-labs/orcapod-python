"""ITL-1250 (f), ITL-1281: do Batch and MergeJoin lose data values through Python?

Each case puts two values of one Arrow type into one list, by ``Batch`` (both rows in
one batch) and by ``MergeJoin`` (one row on each side, same key), and prints the input
and the list, read exactly: temporal values as integers, floats as bit patterns.
"""

from __future__ import annotations

from typing import Any, TypeAlias, cast

import pyarrow as pa

from orcapod.core.operators import Batch, MergeJoin
from orcapod.core.streams import ArrowTableStream

Column: TypeAlias = "pa.Array[Any]"


def _exact(array: Column) -> list[Any]:
    if pa.types.is_floating(array.type):
        width = cast(int, array.type.bit_width)
        unsigned = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}[width]
        return [None if b is None else hex(b) for b in array.view(unsigned).to_pylist()]
    return array.cast(pa.int64()).to_pylist()


def _list_values(table: pa.Table) -> Column:
    lists = cast("pa.LargeListArray[Any]", table.column("v").combine_chunks())
    return lists.values


CASES: dict[str, Column] = {
    "time64[ns]": pa.array([1, 1001], pa.int64()).cast(pa.time64("ns")),
    "date64": pa.array([1, 86_400_001], pa.int64()).cast(pa.date64()),
    "float32 sNaN,-NaN": pa.array([0x7F800001, 0xFFC00000], pa.uint32()).view(
        pa.float32()
    ),
}


def main() -> None:
    for name, values in CASES.items():
        rows = pa.Table.from_arrays(
            [pa.array([0, 1], pa.int64()), values], names=["r", "v"]
        )
        batched = Batch(batch_size=2)(
            ArrowTableStream(rows, tag_columns=["r"])
        ).as_table()
        key = pa.array([1], pa.int64())
        left = pa.Table.from_arrays([key, values.slice(0, 1)], names=["k", "v"])
        right = pa.Table.from_arrays([key, values.slice(1, 1)], names=["k", "v"])
        merged = MergeJoin()(
            ArrowTableStream(left, tag_columns=["k"]),
            ArrowTableStream(right, tag_columns=["k"]),
        ).as_table()
        print(
            f"{name}: input {_exact(values)}  Batch {_exact(_list_values(batched))}"
            + f"  MergeJoin {_exact(_list_values(merged))}"
        )


if __name__ == "__main__":
    main()
