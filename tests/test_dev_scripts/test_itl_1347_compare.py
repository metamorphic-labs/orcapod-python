"""The ITL-1347 differential's calibration (``docs/dev/scripts/itl_1347/compare.py``).

Two baseline dumps of one checkout decide which columns are volatile (left out of the
comparison). A column is volatile only when its values differ as a multiset; one that
holds the same values in another row order is reported as row-order variation, so later
drift in it is still found (CodeRabbit on PR #510).
"""

from __future__ import annotations

import decimal
import importlib.util
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "dev"
    / "scripts"
    / "itl_1347"
    / "compare.py"
)


def _compare_module() -> Any:
    spec = importlib.util.spec_from_file_location("itl_1347_compare", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


compare = _compare_module()


@pytest.mark.parametrize(
    ("kind", "values"),
    [
        (pa.int64(), [1, 2, 3]),
        (pa.large_string(), ["a", "b", "c"]),
        (pa.float64(), [0.5, -0.0, float("nan")]),
        (pa.large_list(pa.int64()), [[1], [2, 3], []]),
    ],
    ids=["int", "string", "float", "list"],
)
def test_a_reordered_deterministic_column_is_not_volatile(
    kind: Any, values: list[Any]
) -> None:
    """The reviewer's example, over several types: two baseline runs return a table's
    rows in another order; its deterministic columns hold the same values, so they are
    row-order variation, not volatile."""
    first = pa.table({"v": pa.array(values, kind), "run_id": ["r1", "r1", "r1"]})
    second = pa.table({"v": pa.array(values[::-1], kind), "run_id": ["r2", "r2", "r2"]})
    volatile, reordered = compare.calibrate_tables({"t": first}, {"t": second})
    assert volatile == {"t": ["run_id"]}
    assert reordered == {"t": ["v"]}


def test_a_value_differing_as_a_multiset_is_volatile() -> None:
    """A run-made value (another multiset) is volatile; an equal column is neither."""
    first = pa.table({"same": [1, 2], "stamp": [10, 11]})
    second = pa.table({"same": [1, 2], "stamp": [20, 21]})
    volatile, reordered = compare.calibrate_tables({"t": first}, {"t": second})
    assert volatile == {"t": ["stamp"]}
    assert reordered == {}


def test_negative_zero_and_nan_payloads_count_as_values() -> None:
    """Bit for bit: ``-0.0`` against ``0.0`` is a difference of values, not of order."""
    first = pa.table({"v": pa.array([0.0, 1.0], pa.float64())})
    second = pa.table({"v": pa.array([-0.0, 1.0], pa.float64())})
    volatile, _ = compare.calibrate_tables({"t": first}, {"t": second})
    assert volatile == {"t": ["v"]}


def _float_bits(bits: int, width: int) -> float:
    import struct

    if width == 64:
        return struct.unpack("<d", struct.pack("<Q", bits))[0]
    return struct.unpack("<f", struct.pack("<I", bits))[0]


# (id, Arrow type, first run's values, second run's values in place)
_CHANGED_IN_PLACE = [
    ("time64_ns", pa.time64("ns"), [1, 9], [2, 9]),
    ("timestamp_ns", pa.timestamp("ns"), [1, 9], [2, 9]),
    ("timestamp_ns_utc", pa.timestamp("ns", tz="UTC"), [1, 9], [2, 9]),
    ("duration_ns", pa.duration("ns"), [1, 9], [2, 9]),
    (
        "decimal256",
        pa.decimal256(76, 40),
        [decimal.Decimal("1." + "0" * 39 + "1"), None],
        [decimal.Decimal("1." + "0" * 39 + "2"), None],
    ),
    ("negative_zero", pa.float64(), [0.0, 1.0], [-0.0, 1.0]),
    (
        "nan_payload",
        pa.float64(),
        [_float_bits(0x7FF8000000000001, 64), 1.0],
        [_float_bits(0x7FF8000000000002, 64), 1.0],
    ),
    (
        "long_string",
        pa.large_string(),
        ["x" * 5000 + "a", "b"],
        ["x" * 5000 + "c", "b"],
    ),
]


@pytest.mark.parametrize(
    ("kind", "before", "after"),
    [case[1:] for case in _CHANGED_IN_PLACE],
    ids=[case[0] for case in _CHANGED_IN_PLACE],
)
def test_a_value_changed_in_place_is_a_change_not_a_reorder(
    kind: Any, before: list[Any], after: list[Any]
) -> None:
    """Rows in place, one raw value changed: volatile, never row-order variation, also
    for types whose Python form loses precision (nanosecond times, ``decimal256``, a
    NaN payload, ``-0.0``) or whose text form is shortened (a long string)."""
    first = pa.table({"v": pa.array(before, kind)})
    second = pa.table({"v": pa.array(after, kind)})
    volatile, reordered = compare.calibrate_tables({"t": first}, {"t": second})
    assert volatile == {"t": ["v"]}
    assert reordered == {}


@pytest.mark.parametrize(
    ("kind", "before"),
    [(case[1], case[2]) for case in _CHANGED_IN_PLACE],
    ids=[case[0] for case in _CHANGED_IN_PLACE],
)
def test_the_same_raw_values_reordered_are_a_reorder(
    kind: Any, before: list[Any]
) -> None:
    """The same raw values in another row order: row-order variation, for each type."""
    first = pa.table({"v": pa.array(before, kind)})
    second = pa.table({"v": pa.array(before[::-1], kind)})
    volatile, reordered = compare.calibrate_tables({"t": first}, {"t": second})
    assert volatile == {}
    assert reordered == {"t": ["v"]}
