"""The SpiralDB fake's own decoding of what it is given (CodeRabbit on PR 595)."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from tests.test_databases import fake_spiral_sdk as fake


def test_a_run_end_encoded_slice_expands_only_its_own_rows() -> None:
    """Every slice decodes to its own values, and a slice of one row of a run of two
    billion values builds one index, not one per value of the run."""
    small = pa.RunEndEncodedArray.from_arrays(
        pa.array([3, 5, 9], pa.int32()), pa.array(["a", "b", "c"])
    )
    for offset in range(10):
        for length in range(10 - offset):
            piece: Any = small.slice(offset, length)
            assert fake._decoded(piece).to_pylist() == piece.to_pylist()  # pyright: ignore[reportPrivateUsage]
    huge = pa.RunEndEncodedArray.from_arrays(
        pa.array([1, 2**31 - 1], pa.int32()), pa.array(["a", "b"])
    )
    last = huge.slice(2**31 - 2, 1)
    assert fake._run_indices(last) == [1]  # pyright: ignore[reportPrivateUsage]
    assert fake._decoded(last).to_pylist() == ["b"]  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("offset", [0, 1])
def test_a_zero_width_fixed_size_list_of_dictionaries_keeps_its_rows(
    offset: int,
) -> None:
    """A fixed-size list of size 0 has no values to count its rows by: it is rebuilt
    from its buffers, rows and nulls kept."""
    arrow_type = pa.list_(pa.dictionary(pa.int8(), pa.string()), 0)
    given: Any = pa.array([[], None, [], []], arrow_type).slice(offset)
    got = fake._deep_decoded(given)  # pyright: ignore[reportPrivateUsage]
    assert len(got) == len(given)
    assert got.to_pylist() == given.to_pylist()
    assert got.type == pa.list_(pa.string(), 0)
