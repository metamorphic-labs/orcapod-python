"""Order is part of a pandas value's hash identity (ITL-897).

Developer decision, November 11, 2026: order is identity wherever the type's own equality
depends on order. pandas' ``equals`` and ``assert_frame_equal`` / ``assert_series_equal`` treat
column order, MultiIndex level order and struct field order as significant, so the hash must too.

Hashes are taken through the data context's semantic hasher, the entry point every stored key
goes through. The tests assert only distinctness and stability, not a hash label or digest: the
mechanism is not decided yet (ITL-898).
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.logical_types import LogicalPandasDataFrame, LogicalPandasSeries


def _hash(value: Any) -> str:
    return get_default_context().semantic_hasher.hash_object(value).to_string()


def _frame() -> pd.DataFrame:
    return pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})


def _multi_index() -> pd.MultiIndex:
    return pd.MultiIndex.from_tuples([(0, "p"), (1, "q")], names=["i", "j"])


def _struct_frame(order: list[str]) -> pd.DataFrame:
    types = {"u": pa.int64(), "v": pa.large_string()}
    struct = pa.struct([(name, types[name]) for name in order])
    array = pa.array([{"u": 1, "v": "x"}], struct)
    return pd.DataFrame({"s": pd.Series(array, dtype=pd.ArrowDtype(struct))})


ORDER_PAIRS: dict[str, tuple[Any, Any]] = {
    "column order": (_frame(), _frame()[["b", "a"]]),
    "MultiIndex level order (DataFrame)": (
        _frame().set_index(_multi_index()),
        _frame().set_index(_multi_index()).swaplevel(),
    ),
    "MultiIndex level order (Series)": (
        pd.Series([1, 2], index=_multi_index(), name="v"),
        pd.Series([1, 2], index=_multi_index(), name="v").swaplevel(),
    ),
    "struct field order": (_struct_frame(["u", "v"]), _struct_frame(["v", "u"])),
}


class TestOrderIsIdentity:
    @pytest.mark.parametrize("name", list(ORDER_PAIRS))
    def test_pandas_itself_distinguishes_the_pair(self, name: str) -> None:
        """Precondition: pandas equality says the two values differ."""
        first, second = ORDER_PAIRS[name]
        assert not first.equals(second)

    @pytest.mark.parametrize("name", list(ORDER_PAIRS))
    def test_values_differing_only_in_order_hash_differently(self, name: str) -> None:
        first, second = ORDER_PAIRS[name]
        assert _hash(first) != _hash(second)


class TestStability:
    def test_equal_frames_hash_equal(self) -> None:
        assert _hash(_frame()) == _hash(_frame())

    def test_reordered_back_hashes_like_the_original(self) -> None:
        assert _hash(_frame()[["b", "a"]][["a", "b"]]) == _hash(_frame())

    @pytest.mark.parametrize(
        "value",
        [
            _frame(),
            _frame()[["b", "a"]],
            _frame().set_index(_multi_index()).swaplevel(),
        ],
        ids=["ab", "ba", "swapped-levels"],
    )
    def test_frame_hash_survives_storage(self, value: pd.DataFrame) -> None:
        """A frame read back from storage keeps its order and so its cache key."""
        logical = LogicalPandasDataFrame()
        back = logical.storage_to_python(logical.python_to_storage(value))
        assert list(back.columns) == list(value.columns)
        assert list(back.index.names) == list(value.index.names)
        assert _hash(back) == _hash(value)

    def test_series_hash_survives_storage(self) -> None:
        value = pd.Series([1, 2], index=_multi_index(), name="v").swaplevel()
        logical = LogicalPandasSeries()
        back = logical.storage_to_python(logical.python_to_storage(value))
        assert list(back.index.names) == ["j", "i"]
        assert _hash(back) == _hash(value)
