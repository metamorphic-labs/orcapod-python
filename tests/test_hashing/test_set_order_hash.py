"""A plain ``set[T]`` value hashes the same in every process (ITL-905).

A ``set[T]`` of a non-logical element type is stored as ``large_list<T>``. Python's set
iteration order depends on ``PYTHONHASHSEED`` for str and bytes elements, so the stored
order, and the row hash, must not come from iteration order.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from typing import Any

import numpy as np
import pytest

from orcapod.sources import DictSource

_ROW_HASH_SCRIPT = textwrap.dedent(
    """
    from orcapod.sources import DictSource

    source = DictSource(
        data=[{"k": "r", "s": {"apple", "banana", "cherry", "date", "elder"}}],
        tag_columns=["k"],
        data_schema={"k": str, "s": set[str]},
        source_id="set_order",
    )
    _, data = next(iter(source.iter_data()))
    print(data.content_hash().to_string())
    """
)


def _row_hash_in_subprocess(seed: str) -> str:
    """Return the row hash of a fixed ``set[str]`` computed under ``PYTHONHASHSEED=seed``."""
    env = {**os.environ, "PYTHONHASHSEED": seed}
    result = subprocess.run(
        [sys.executable, "-c", _ROW_HASH_SCRIPT],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _source(value: set[str]) -> DictSource:
    """Return a one-row ``DictSource`` whose ``s`` column holds ``value``."""
    return DictSource(
        data=[{"k": "r", "s": value}],
        tag_columns=["k"],
        data_schema={"k": str, "s": set[str]},
        source_id="set_order",
    )


class TestPlainSetOrder:
    """A plain ``set[T]`` is stored sorted, never in iteration order."""

    def test_row_hash_is_independent_of_hash_seed(self):
        """Two processes with different hash seeds give one row hash."""
        assert _row_hash_in_subprocess("1") == _row_hash_in_subprocess("2")

    def test_stored_elements_are_sorted(self):
        """``str`` elements are stored sorted."""
        table = _source({"cherry", "apple", "banana"}).as_table()
        assert table.column("s").to_pylist() == [["apple", "banana", "cherry"]]

    def test_int_set_elements_are_sorted(self):
        """``int`` elements are stored in numeric order."""
        source = DictSource(
            data=[{"k": "r", "s": {300, -1, 7}}],
            tag_columns=["k"],
            data_schema={"k": str, "s": set[int]},
        )
        assert source.as_table().column("s").to_pylist() == [[-1, 7, 300]]

    def test_numpy_nan_element_is_rejected(self):
        """A NaN that is not a Python ``float`` (``numpy.float32``) has no order either."""
        # NumPy scalars are outside the declared ``DataValue`` type; DictSource accepts
        # them at runtime, which is the case under test.
        rows: list[dict[str, Any]] = [
            {"k": "r", "s": {np.float32("nan"), np.float32(1.0)}}
        ]
        with pytest.raises(ValueError, match="NaN"):
            DictSource(
                data=rows,
                tag_columns=["k"],
                data_schema={"k": str, "s": set[float]},
            ).as_table()
