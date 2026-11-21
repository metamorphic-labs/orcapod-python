"""ITL-1250 (a): GroupBy against Polars group_by, per key type, in the current checkout.

For each Arrow key type, a table of rows with a key ``k`` and a data column ``v`` (the
row number) is grouped by ``GroupBy`` and by ``polars.DataFrame.group_by`` on the key's
storage (``float16`` as ``float32``). The partition (the sets of ``v``) of each is
printed. On ``main`` at ``a026d31f``, ``time64[ns]`` and ``date64`` off midnight DIFFER
(main merges keys that are one Python value); with ITL-1250 every type Polars reads is
the same. Polars cannot read ``decimal256`` (it panics), which the script reports.
"""

from __future__ import annotations

import decimal
import uuid
from pathlib import Path
from typing import Any, TypeAlias, cast

import polars as pl
import pyarrow as pa

from orcapod.contexts import get_default_context
from orcapod.core.operators import GroupBy
from orcapod.core.streams import ArrowTableStream

Column: TypeAlias = "pa.Array[Any]"
Partition: TypeAlias = "list[list[int]]"
CTX = get_default_context()

NAN = {16: 0x7E00, 32: 0x7FC00000, 64: 0x7FF8000000000000}
NEG_NAN = {16: 0xFE00, 32: 0xFFC00000, 64: 0xFFF8000000000000}
PAYLOAD_NAN = {16: 0x7E01, 32: 0x7FC00001, 64: 0x7FF8000000000001}
SIGNALING_NAN = {16: 0x7C01, 32: 0x7F800001, 64: 0x7FF0000000000001}
NEG_ZERO = {16: 0x8000, 32: 0x80000000, 64: 0x8000000000000000}
ONE = {16: 0x3C00, 32: 0x3F800000, 64: 0x3FF0000000000000}


def _floats(width: int, bits: list[int | None]) -> Column:
    unsigned = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}[width]
    floating = {16: pa.float16(), 32: pa.float32(), 64: pa.float64()}[width]
    return pa.array(bits, unsigned).view(floating)


def _ints(values: list[int | None], arrow_type: pa.DataType) -> Column:
    return pa.array(values, pa.int64()).cast(arrow_type)


def _logical(python_type: type, storage: list[Any]) -> Column:
    arrow_type = cast(
        pa.BaseExtensionType, CTX.type_converter.python_type_to_arrow_type(python_type)
    )
    return pa.ExtensionArray.from_storage(
        arrow_type, pa.array(storage, arrow_type.storage_type)
    )


def cases() -> list[tuple[str, Column]]:
    out: list[tuple[str, Column]] = []
    for bits in (8, 16, 32, 64):
        for signed in (True, False):
            arrow_type = getattr(pa, f"{'int' if signed else 'uint'}{bits}")()
            low = -(2 ** (bits - 1)) if signed else 0
            high = 2 ** (bits - 1) - 1 if signed else 2**bits - 1
            out.append(
                (
                    str(arrow_type),
                    pa.array([low, high, low, 0, None, high, None], arrow_type),
                )
            )
    for w in (16, 32, 64):
        bits_list: list[int | None] = [
            NAN[w],
            NEG_NAN[w],
            PAYLOAD_NAN[w],
            SIGNALING_NAN[w],
            NEG_ZERO[w],
            0,
            ONE[w],
            None,
            ONE[w],
        ]
        out.append(
            (
                f"float{w} (nan, -nan, payload, signaling, -0, 0, 1, null, 1)",
                _floats(w, bits_list),
            )
        )
    out.append(("bool", pa.array([True, False, True, None, None], pa.bool_())))
    out.append(
        (
            "large_string",
            pa.array(["a", "é", "", "a", None, "é", "é"], pa.large_string()),
        )
    )
    out.append(
        ("large_binary", pa.array([b"a", b"\x00", b"", b"a", None], pa.large_binary()))
    )
    out.append(
        (
            "fixed_size_binary(4)",
            pa.array([b"abcd", b"abce", b"abcd", None], pa.binary(4)),
        )
    )
    out.append(("date32", pa.array([0, 1, 0, None], pa.int32()).cast(pa.date32())))
    out.append(
        (
            "date64 (0 ms, 1 ms, 1 day, 1 day + 1 ms)",
            _ints([0, 1, 86_400_000, 86_400_001, None], pa.date64()),
        )
    )
    out.append(
        ("time32[s]", pa.array([0, 1, 1, None], pa.int32()).cast(pa.time32("s")))
    )
    out.append(
        ("time32[ms]", pa.array([0, 1, 1, None], pa.int32()).cast(pa.time32("ms")))
    )
    out.append(("time64[us]", _ints([0, 1, 1, None], pa.time64("us"))))
    out.append(
        (
            "time64[ns] (1, 2, 1000, 1001 ns)",
            _ints([1, 2, 1000, 1001, None], pa.time64("ns")),
        )
    )
    for unit in ("s", "ms", "us", "ns"):
        out.append(
            (f"timestamp[{unit}]", _ints([0, 1, 1000, 1001, None], pa.timestamp(unit)))
        )
        out.append(
            (
                f"timestamp[{unit}, UTC]",
                _ints([0, 1, 1000, 1001, None], pa.timestamp(unit, "UTC")),
            )
        )
        out.append(
            (
                f"timestamp[{unit}, America/New_York]",
                _ints([0, 1, 1000, 1001, None], pa.timestamp(unit, "America/New_York")),
            )
        )
        out.append(
            (f"duration[{unit}]", _ints([0, 1, 1000, 1001, None], pa.duration(unit)))
        )
    d = decimal.Decimal
    out.append(
        (
            "decimal128(38,10)",
            pa.array(
                [d("1.0000000001"), d("1.0000000002"), d("1"), d("1.0000000000"), None],
                pa.decimal128(38, 10),
            ),
        )
    )
    out.append(
        (
            "decimal256(76,40)",
            pa.array(
                [d("1." + "0" * 39 + "1"), d("1." + "0" * 39 + "2"), d("1"), None],
                pa.decimal256(76, 40),
            ),
        )
    )
    out.append(("null", pa.array([None, None, None], pa.null())))
    out.append(("logical Path", _logical(Path, ["a", "b", "a", None])))
    u1, u2 = uuid.UUID(int=1).bytes, uuid.UUID(int=2).bytes
    out.append(("logical UUID", _logical(uuid.UUID, [u1, u2, u1, None])))
    return out


def partition_group_by(key: Column) -> Partition:
    table = pa.Table.from_arrays(
        [key, pa.array(range(len(key)), pa.int64())], names=["k", "v"]
    )
    out = GroupBy("k")(ArrowTableStream(table, tag_columns=["k"])).as_table()
    return sorted(sorted(vs) for vs in cast(Partition, out.column("v").to_pylist()))


def partition_polars(key: Column) -> Partition:
    storage = key.storage if isinstance(key, pa.ExtensionArray) else key
    if pa.types.is_float16(storage.type):
        storage = storage.cast(pa.float32())
    table = pa.Table.from_arrays(
        [storage, pa.array(range(len(key)), pa.int64())], names=["k", "v"]
    )
    frame = cast(pl.DataFrame, pl.from_arrow(table))
    grouped = frame.group_by("k").agg(pl.col("v"))
    return sorted(sorted(vs) for vs in cast(Partition, grouped["v"].to_list()))


def main() -> None:
    for name, key in cases():
        results: list[str] = []
        for grouping in (partition_group_by, partition_polars):
            try:
                results.append(str(grouping(key)))
            except BaseException as error:  # noqa: BLE001  # Polars panics on decimal256
                results.append(f"ERROR {type(error).__name__}: {str(error)[:100]}")
        verdict = "same" if results[0] == results[1] else "DIFFER"
        print(
            f"{verdict:6} {name}\n       GroupBy: {results[0]}\n       Polars:  {results[1]}"
        )


if __name__ == "__main__":
    main()
