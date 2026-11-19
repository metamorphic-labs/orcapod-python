"""ITL-1250: are the data values inside GroupBy's lists the input's, bit for bit?

Run in a checkout: on ``main`` at ``a026d31f`` the time64[ns], date64 and float32
signaling-NaN cases print CHANGED; with ITL-1250 every case prints ``same``. Each case
groups two rows into one group and compares the data list with the input column,
read exactly (temporal values as integers, floats as bit patterns).
"""

from __future__ import annotations

import decimal
from typing import Any, TypeAlias, cast

import pyarrow as pa

from orcapod.core.operators import GroupBy
from orcapod.core.streams import ArrowTableStream

Column: TypeAlias = "pa.Array[Any]"


def _exact(array: Column) -> list[Any]:
    """The values read exactly: unsigned bits for a float, integers for a temporal."""
    if pa.types.is_floating(array.type):
        width = cast(int, array.type.bit_width)
        unsigned = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}[width]
        return [None if b is None else hex(b) for b in array.view(unsigned).to_pylist()]
    if pa.types.is_temporal(array.type):
        return array.cast(pa.int64()).to_pylist()
    if pa.types.is_large_list(array.type):
        lists = cast("pa.LargeListArray[Any]", array)
        values = _exact(lists.values)
        offsets = cast(list[int], lists.offsets.to_pylist())
        return [values[offsets[i] : offsets[i + 1]] for i in range(len(lists))]
    return [None if v is None else str(v) for v in array.to_pylist()]


def _bits(values: list[int], unsigned: pa.DataType, floating: pa.DataType) -> Column:
    return pa.array(values, unsigned).view(floating)


def cases() -> list[tuple[str, Column]]:
    tiny = decimal.Decimal("1." + "0" * 39 + "1")
    return [
        (
            "time64[ns] 1 ns, 1001 ns",
            pa.array([1, 1001], pa.int64()).cast(pa.time64("ns")),
        ),
        (
            "date64 1 ms, 1 day + 1 ms",
            pa.array([1, 86_400_001], pa.int64()).cast(pa.date64()),
        ),
        (
            "timestamp[ns] 1, 1001",
            pa.array([1, 1001], pa.int64()).cast(pa.timestamp("ns")),
        ),
        (
            "duration[ns] 1, 1001",
            pa.array([1, 1001], pa.int64()).cast(pa.duration("ns")),
        ),
        (
            "float32 signaling NaN, -NaN",
            _bits([0x7F800001, 0xFFC00000], pa.uint32(), pa.float32()),
        ),
        (
            "float16 signaling NaN, -0",
            _bits([0x7C01, 0x8000], pa.uint16(), pa.float16()),
        ),
        (
            "float64 payload NaN, -0",
            _bits([0x7FF8000000000001, 0x8000000000000000], pa.uint64(), pa.float64()),
        ),
        (
            "decimal256(76,40)",
            pa.array([tiny, decimal.Decimal(-3)], pa.decimal256(76, 40)),
        ),
        (
            "large_list<time64[ns]>",
            pa.array([[1, 2], [1001]], pa.large_list(pa.int64())).cast(
                pa.large_list(pa.time64("ns"))
            ),
        ),
    ]


def main() -> None:
    for name, values in cases():
        table = pa.Table.from_arrays(
            [pa.array([1, 1], pa.int64()), pa.array([0, 1], pa.int64()), values],
            names=["g", "r", "v"],
        )
        out = GroupBy("g")(ArrowTableStream(table, tag_columns=["g", "r"])).as_table()
        grouped = cast("pa.LargeListArray[Any]", out.column("v").combine_chunks())
        before, after = _exact(values), _exact(grouped.values)
        verdict = "same" if before == after else "CHANGED"
        print(f"{name}\n  input:   {before}\n  grouped: {after}  {verdict}")


if __name__ == "__main__":
    main()
