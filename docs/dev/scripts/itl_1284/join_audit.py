"""ITL-1284: which Arrow types ``Join`` and ``SemiJoin`` change or fail on, in this checkout.

usage: uv run python docs/dev/scripts/itl_1284/join_audit.py

For each Arrow type T (two values) and each operator, two roles: ``data`` (a non-key
data column of T on the left input, for ``Join`` also on the right) and ``key`` (a
shared tag of T, the only key). Each runs on the sync path (``op(left, right)``) and the
async path (``async_execute`` over channels). The values out are compared with the
values in exactly: temporal values as their integers, floats as their bits, decimals as
text, logical types through their storage, lists and structs element by element. A
line is printed for every case that is not exact (``CHANGED`` or ``ERROR``), then a
count. A Polars panic is a ``BaseException``, so it is caught and printed too.

Measured on ``main`` at ``1015c268`` (Polars 1.41.2, PyArrow 23.0.1), the sync path
truncates ``date64`` to midnight (top level, in a list, in a struct) and panics on
``decimal256``; with ITL-1284 every case is exact, and a key type with no rule (a map,
an interval, a union) is refused with ``InputValidationError`` on both paths.
"""

from __future__ import annotations

import asyncio
import decimal
import uuid
from pathlib import Path
from typing import Any, TypeAlias, cast

import pyarrow as pa

from orcapod.channels import Channel
from orcapod.contexts import get_default_context
from orcapod.core.operators import Join, SemiJoin
from orcapod.core.streams import ArrowTableStream
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol

Column: TypeAlias = "pa.Array[Any]"
Row: TypeAlias = "tuple[TagProtocol, DataProtocol]"
CTX = get_default_context()
D = decimal.Decimal


def exact(array: "Column | pa.ChunkedArray[Any]") -> list[Any]:
    """Return a column's values exactly, as plain Python values."""
    if isinstance(array, pa.ChunkedArray):
        array = array.combine_chunks()
    if isinstance(array, pa.ExtensionArray):
        return exact(array.storage)
    if isinstance(array, pa.DictionaryArray):
        return exact(array.dictionary_decode())
    arrow_type = array.type
    if pa.types.is_large_list(arrow_type) or pa.types.is_list(arrow_type):
        lists = cast("pa.LargeListArray[Any]", array)
        values = exact(lists.values)
        offsets = cast(list[int], lists.offsets.to_pylist())
        return [
            values[offsets[i] : offsets[i + 1]] if lists[i].is_valid else None
            for i in range(len(lists))
        ]
    if pa.types.is_fixed_size_list(arrow_type):
        fixed = cast("pa.FixedSizeListArray[Any]", array)
        return exact(fixed.cast(pa.large_list(arrow_type.value_type)))
    if pa.types.is_struct(arrow_type):
        struct = cast("pa.StructArray", array)
        fields = [exact(struct.field(i)) for i in range(arrow_type.num_fields)]
        return [
            tuple(f[j] for f in fields) if struct[j].is_valid else None
            for j in range(len(struct))
        ]
    if pa.types.is_map(arrow_type):
        return [repr(x) for x in array.to_pylist()]
    if pa.types.is_floating(arrow_type):
        unsigned = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}
        bits = cast(
            list[int | None], array.view(unsigned[arrow_type.bit_width]).to_pylist()
        )
        return [None if b is None else hex(b) for b in bits]
    if pa.types.is_interval(arrow_type):
        return [repr(x) for x in array.to_pylist()]
    if pa.types.is_temporal(arrow_type):
        width = cast(int, arrow_type.bit_width)
        return array.view(pa.int32() if width == 32 else pa.int64()).to_pylist()
    if pa.types.is_decimal(arrow_type):
        return [None if x is None else str(x) for x in array.to_pylist()]
    return array.to_pylist()


def ints(values: list[int], arrow_type: pa.DataType) -> Column:
    return pa.array(values, pa.int64()).cast(arrow_type)


def floats(bits: list[int], width: int) -> Column:
    unsigned = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}[width]
    floating = {16: pa.float16(), 32: pa.float32(), 64: pa.float64()}[width]
    return pa.array(bits, unsigned).view(floating)


def logical(python_type: type, storage: list[Any]) -> Column:
    arrow_type = cast(
        pa.BaseExtensionType, CTX.type_converter.python_type_to_arrow_type(python_type)
    )
    return pa.ExtensionArray.from_storage(
        arrow_type, pa.array(storage, arrow_type.storage_type)
    )


def cases() -> dict[str, Column]:
    dec256 = pa.decimal256(76, 40)
    date64s = ints([1, 86_400_001], pa.date64())
    return {
        "int8": pa.array([-128, 127], pa.int8()),
        "uint64 max": pa.array([2**64 - 1, 0], pa.uint64()),
        "float16 (1.0, -0.0)": floats([0x3C00, 0x8000], 16),
        "float32 (sNaN, 1.0)": floats([0x7F800001, 0x3F800000], 32),
        "float64 (payload NaN, -0.0)": floats(
            [0x7FF8000000000001, 0x8000000000000000], 64
        ),
        "bool": pa.array([True, False]),
        "large_string (precomposed and decomposed é)": pa.array(
            ["\u00e9", "e\u0301"], pa.large_string()
        ),
        "fixed_size_binary(3)": pa.array([b"abc", b"\x00\x00\x00"], pa.binary(3)),
        "date32": pa.array([0, 1], pa.int32()).cast(pa.date32()),
        "date64 off midnight": date64s,
        "time32[s]": pa.array([1, 2], pa.int32()).cast(pa.time32("s")),
        "time64[ns]": ints([1, 1001], pa.time64("ns")),
        "timestamp[ns, America/New_York]": ints(
            [1, 1001], pa.timestamp("ns", "America/New_York")
        ),
        "duration[ns]": ints([1, 1001], pa.duration("ns")),
        "decimal128(38,10)": pa.array(
            [D("1.0000000001"), D("-2")], pa.decimal128(38, 10)
        ),
        "decimal256(76,40)": pa.array([D("1." + "0" * 39 + "1"), D("-3")], dec256),
        "large_list<date64>": pa.LargeListArray.from_arrays(
            pa.array([0, 1, 2], pa.int64()), date64s
        ),
        "large_list<time64[ns]>": pa.LargeListArray.from_arrays(
            pa.array([0, 1, 2], pa.int64()), ints([1, 1001], pa.time64("ns"))
        ),
        "large_list<float32 sNaN>": pa.LargeListArray.from_arrays(
            pa.array([0, 1, 2], pa.int64()), floats([0x7F800001, 0], 32)
        ),
        "struct<a: date64>": pa.StructArray.from_arrays([date64s], names=["a"]),
        "large_list<decimal256>": pa.array(
            [[D(1)], [D(2)]], pa.large_list(pa.decimal256(76, 2))
        ),
        "struct<a: decimal256>": pa.StructArray.from_arrays(
            [pa.array([D(1), D(2)], dec256)], names=["a"]
        ),
        "fixed_size_list<decimal256, 1>": pa.FixedSizeListArray.from_arrays(
            pa.array([D(1), D(2)], dec256), 1
        ),
        "logical Path": logical(Path, ["a", "b"]),
        "logical UUID": logical(
            uuid.UUID, [uuid.UUID(int=1).bytes, uuid.UUID(int=2).bytes]
        ),
        "map<large_string, int64>": pa.array(
            [[("a", 1)], [("b", 2)]], pa.map_(pa.large_string(), pa.int64())
        ),
        "month_day_nano_interval": pa.array(
            [(1, 2, 3), (4, 5, 6)], pa.month_day_nano_interval()
        ),
    }


async def _run_async(
    operator: Join | SemiJoin, streams: list[ArrowTableStream]
) -> list[Row]:
    channels: list[Channel[Row]] = [Channel(buffer_size=64) for _ in streams]
    output: Channel[Row] = Channel(buffer_size=64)
    for stream, channel in zip(streams, channels):
        for row in stream.iter_data():
            await channel.writer.send(row)
        await channel.writer.close()
    await operator.async_execute(
        [c.reader for c in channels],
        output.writer,
        input_pipeline_hashes=[s.pipeline_hash() for s in streams],
    )
    return await output.reader.collect()


def _column_of_rows(rows: list[Row], name: str) -> list[Any]:
    out: list[Any] = []
    for tag, data in rows:
        table = tag.as_table() if name in tag.keys() else data.as_table()
        out.extend(exact(table.column(name)))
    return out


def check(operator_name: str, role: str, values: Column, path: str) -> str:
    """Return ``same``, or what changed, or the error raised."""
    numbers = pa.array([1, 2], pa.int64())
    if role == "data":
        left = pa.Table.from_arrays([numbers, values], names=["k", "y"])
        right = pa.Table.from_arrays(
            [numbers, pa.array([0, 0], pa.int64())], names=["k", "z"]
        )
        name = "y"
    else:
        left = pa.Table.from_arrays(
            [values, pa.array([0, 1], pa.int64())], names=["k", "x"]
        )
        right = pa.Table.from_arrays(
            [values, pa.array([2, 3], pa.int64())], names=["k", "z"]
        )
        name = "k"
    streams = [
        ArrowTableStream(left, tag_columns=["k"]),
        ArrowTableStream(right, tag_columns=["k"]),
    ]
    operator = Join() if operator_name == "Join" else SemiJoin()
    try:
        if path == "sync":
            got = exact(operator(*streams).as_table().column(name))
        else:
            got = _column_of_rows(asyncio.run(_run_async(operator, streams)), name)
    except BaseException as error:  # noqa: BLE001  # a Polars panic is a BaseException
        while isinstance(error, BaseExceptionGroup) and len(error.exceptions) == 1:
            error = error.exceptions[0]  # the async path's task group
        return f"ERROR {type(error).__name__}: {str(error)[:90]}"
    want = exact(values)
    if sorted(map(repr, got)) == sorted(map(repr, want)):
        return "same"
    return f"CHANGED {want} -> {got}"


def main() -> None:
    total = exact_count = 0
    for name, values in cases().items():
        for operator_name in ("Join", "SemiJoin"):
            for role in ("data", "key"):
                for path in ("sync", "async"):
                    total += 1
                    result = check(operator_name, role, values, path)
                    if result == "same":
                        exact_count += 1
                    else:
                        print(f"{name:32} {operator_name:8} {role:4} {path:5} {result}")
    print(f"{exact_count} of {total} cases exact")


if __name__ == "__main__":
    main()
