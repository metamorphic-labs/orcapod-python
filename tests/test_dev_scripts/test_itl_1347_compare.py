"""The ITL-1347 differential's calibration (``docs/dev/scripts/itl_1347/compare.py``).

Two baseline dumps of one checkout decide which columns are volatile (left out of the
comparison). A column is volatile only when its values differ as a multiset; one that
holds the same values in another row order is reported as row-order variation, so later
drift in it is still found (CodeRabbit on PR #510).
"""

from __future__ import annotations

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
