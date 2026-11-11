"""A pandas value's hash identity follows pandas' own equality (ITL-897, ITL-887).

Developer decisions, November 11, 2026: order is identity wherever the type's own equality
depends on order, and ``pandas_dataframe_v1`` / ``pandas_series_v1`` settle in one hash move
every difference pandas' equality recognises. The oracle is ``assert_frame_equal`` /
``assert_series_equal`` with default arguments, with two exceptions: ``flags`` (storage drops it)
and ``-0.0`` vs ``0.0`` (bytes differ; an extra computation, never a wrong result).

Hashes are taken through the data context's semantic hasher, the entry point every stored key
goes through.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

from orcapod.contexts import get_default_context
from orcapod.logical_types import LogicalPandasDataFrame, LogicalPandasSeries


def _content_hash(value: Any) -> Any:
    return get_default_context().semantic_hasher.hash_object(value)


def _hash(value: Any) -> str:
    return _content_hash(value).to_string()


def _frame() -> pd.DataFrame:
    return pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})


def _multi_index() -> pd.MultiIndex:
    return pd.MultiIndex.from_tuples([(0, "p"), (1, "q")], names=["i", "j"])


def _struct_frame(order: list[str]) -> pd.DataFrame:
    types = {"u": pa.int64(), "v": pa.large_string()}
    struct = pa.struct([(name, types[name]) for name in order])
    array = pa.array([{"u": 1, "v": "x"}], struct)
    return pd.DataFrame({"s": pd.Series(array, dtype=pd.ArrowDtype(struct))})


def _with_columns_name(df: pd.DataFrame, name: str) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.rename(name)
    return out


def _with_attrs(value: Any) -> Any:
    out = value.copy()
    out.attrs = {"unit": "mV"}
    return out


def _categorical(categories: list[str], ordered: bool = False) -> pd.DataFrame:
    values = pd.Categorical(["x", "y"], categories=categories, ordered=ordered)
    return pd.DataFrame({"a": values})


def _timestamp_labels(tz: str | None) -> pd.DataFrame:
    labels = pd.DatetimeIndex(pd.to_datetime(["2020-01-01", "2020-01-02"]))
    return pd.DataFrame(
        [[1, 2]], columns=labels if tz is None else labels.tz_localize(tz)
    )


# Pairs that assert_frame_equal / assert_series_equal (defaults) tell apart: must hash apart.
DISTINCT: dict[str, tuple[Any, Any]] = {
    # orders
    "column order": (_frame(), _frame()[["b", "a"]]),
    "MultiIndex level order": (
        _frame().set_index(_multi_index()),
        _frame().set_index(_multi_index()).swaplevel(),
    ),
    "struct field order": (_struct_frame(["u", "v"]), _struct_frame(["v", "u"])),
    # labels and names
    "column label type 0 vs '0'": (
        pd.DataFrame({0: [1, 2]}),
        pd.DataFrame({"0": [1, 2]}),
    ),
    "MultiIndex columns vs flat labels": (
        pd.DataFrame(
            [[1, 2]], columns=pd.MultiIndex.from_tuples([("a", "x"), ("a", "y")])
        ),
        pd.DataFrame([[1, 2]], columns=pd.Index(["('a', 'x')", "('a', 'y')"])),
    ),
    "columns.name": (_frame(), _with_columns_name(_frame(), "features")),
    "index name": (_frame(), _frame().rename_axis("id")),
    "MultiIndex level names": (
        _frame().set_index(pd.MultiIndex.from_tuples([(0, "p"), (1, "q")])),
        _frame().set_index(_multi_index()),
    ),
    "index dtype int64 vs float64": (
        pd.DataFrame({"a": [1, 2]}, index=pd.Index([0, 1], dtype="int64")),
        pd.DataFrame({"a": [1, 2]}, index=pd.Index([0.0, 1.0], dtype="float64")),
    ),
    # dtypes (ITL-887 among them)
    "categorical vs object string": (
        pd.DataFrame({"a": ["x", "y"]}),
        _categorical(["x", "y"]),
    ),
    "categorical category order": (_categorical(["x", "y"]), _categorical(["y", "x"])),
    "categorical unused category": (
        _categorical(["x", "y"]),
        _categorical(["x", "y", "z"]),
    ),
    "categorical ordered flag": (
        _categorical(["x", "y"]),
        _categorical(["x", "y"], ordered=True),
    ),
    "Int64 vs int64": (
        pd.DataFrame({"a": pd.array([1, 2], dtype="int64")}),
        pd.DataFrame({"a": pd.array([1, 2], dtype="Int64")}),
    ),
    "boolean vs bool": (
        pd.DataFrame({"a": [True, False]}),
        pd.DataFrame({"a": pd.array([True, False], dtype="boolean")}),
    ),
    "string vs object": (
        pd.DataFrame({"a": ["x", "y"]}),
        pd.DataFrame({"a": pd.array(["x", "y"], dtype="string")}),
    ),
    "int64 vs int32": (
        pd.DataFrame({"a": np.array([1, 2], dtype="int64")}),
        pd.DataFrame({"a": np.array([1, 2], dtype="int32")}),
    ),
    "row order": (_frame(), _frame().iloc[::-1]),
    "values": (_frame(), pd.DataFrame({"a": [1, 3], "b": ["x", "y"]})),
    # labels outside str/int that work on v0.2.0
    "Timestamp labels naive vs UTC": (
        _timestamp_labels(None),
        _timestamp_labels("UTC"),
    ),
    "Timestamp vs str labels": (
        _timestamp_labels(None),
        pd.DataFrame(
            [[1, 2]], columns=pd.Index(["2020-01-01 00:00:00", "2020-01-02 00:00:00"])
        ),
    ),
    "period column freq": (
        pd.DataFrame({"p": pd.period_range("2020-01", periods=2, freq="M")}),
        pd.DataFrame({"p": pd.period_range("2020Q1", periods=2, freq="Q")}),
    ),
    "interval column closed": (
        pd.DataFrame({"i": pd.interval_range(0, 2, closed="right")}),
        pd.DataFrame({"i": pd.interval_range(0, 2, closed="left")}),
    ),
    # Series
    "Series MultiIndex level order": (
        pd.Series([1, 2], index=_multi_index(), name="v"),
        pd.Series([1, 2], index=_multi_index(), name="v").swaplevel(),
    ),
    "Series name": (pd.Series([1, 2], name="a"), pd.Series([1, 2], name="b")),
    "Series name None vs 'x'": (pd.Series([1, 2]), pd.Series([1, 2], name="x")),
    "Series name 0 vs '0'": (pd.Series([1, 2], name=0), pd.Series([1, 2], name="0")),
    "Series dtype int64 vs Int64": (
        pd.Series([1, 2], dtype="int64"),
        pd.Series([1, 2], dtype="Int64"),
    ),
    "Series categorical vs object": (
        pd.Series(["x", "y"]),
        pd.Series(pd.Categorical(["x", "y"])),
    ),
}

# Pairs that assert_frame_equal / assert_series_equal (defaults) call equal: must hash equal.
EQUAL: dict[str, tuple[Any, Any]] = {
    "copy": (_frame(), _frame().copy()),
    "reordered back": (_frame(), _frame()[["b", "a"]][["a", "b"]]),
    "RangeIndex vs Index[int64]": (
        _frame(),
        pd.DataFrame(
            {"a": [1, 2], "b": ["x", "y"]}, index=pd.Index([0, 1], dtype="int64")
        ),
    ),
    "attrs": (_frame(), _with_attrs(_frame())),
    "dict key order in an object column": (
        pd.DataFrame({"s": [{"u": 1, "v": "x"}]}),
        pd.DataFrame({"s": [{"v": "x", "u": 1}]}),
    ),
    "chunked copy": (_frame(), pd.concat([_frame().iloc[:1], _frame().iloc[1:]])),
    "Series attrs": (pd.Series([1, 2]), _with_attrs(pd.Series([1, 2]))),
}


def _assert_fn(value: Any) -> Callable[[Any, Any], None]:
    return assert_series_equal if isinstance(value, pd.Series) else assert_frame_equal


class TestFollowsPandasEquality:
    @pytest.mark.parametrize("name", list(DISTINCT))
    def test_pandas_distinguishes_the_pair(self, name: str) -> None:
        """Precondition: pandas' assert function says the two values differ."""
        first, second = DISTINCT[name]
        with pytest.raises(AssertionError):
            _assert_fn(first)(first, second)

    @pytest.mark.parametrize("name", list(DISTINCT))
    def test_distinct_values_hash_differently(self, name: str) -> None:
        first, second = DISTINCT[name]
        assert _hash(first) != _hash(second)

    @pytest.mark.parametrize("name", list(EQUAL))
    def test_pandas_calls_the_pair_equal(self, name: str) -> None:
        first, second = EQUAL[name]
        _assert_fn(first)(first, second)

    @pytest.mark.parametrize("name", list(EQUAL))
    def test_equal_values_hash_equal(self, name: str) -> None:
        first, second = EQUAL[name]
        assert _hash(first) == _hash(second)


class TestLabels:
    def test_dataframe_label(self) -> None:
        assert _content_hash(_frame()).method == "pandas_dataframe_v1"

    def test_series_label(self) -> None:
        assert _content_hash(pd.Series([1, 2])).method == "pandas_series_v1"

    def test_frame_and_its_one_column_series_differ(self) -> None:
        assert _hash(pd.DataFrame({"a": [1, 2]})) != _hash(pd.Series([1, 2], name="a"))


class TestOutsideTheGrammar:
    def test_unsupported_label_type_raises(self) -> None:
        class Label:
            def __hash__(self) -> int:
                return 1

        with pytest.raises(ValueError, match="Label"):
            _hash(pd.DataFrame({Label(): [1]}))

    def test_unsupported_dtype_raises_naming_the_column(self) -> None:
        frame = pd.DataFrame({"s": pd.arrays.SparseArray([0, 1])})
        with pytest.raises(ValueError, match="'s'"):
            _hash(frame)

    def test_date_label_hashes(self) -> None:
        assert _hash(pd.DataFrame([[1]], columns=pd.Index([dt.date(2020, 1, 1)])))


STORABLE_FRAMES: dict[str, Any] = {
    "ab": _frame(),
    "ba": _frame()[["b", "a"]],
    "swapped levels": _frame().set_index(_multi_index()).swaplevel(),
    "categorical": _categorical(["y", "x", "z"], ordered=True),
    "nullable": pd.DataFrame({"a": pd.array([1, None], dtype="Int64")}),
    "columns.name": _with_columns_name(_frame(), "features"),
    "int labels": pd.DataFrame({0: [1], 1: ["x"]}),
    "Timestamp labels": _timestamp_labels("UTC"),
    "period": pd.DataFrame({"p": pd.period_range("2020-01", periods=2, freq="M")}),
    "interval": pd.DataFrame({"i": pd.interval_range(0, 2)}),
}


class TestStorage:
    @pytest.mark.parametrize("name", list(STORABLE_FRAMES))
    def test_frame_hash_survives_storage(self, name: str) -> None:
        """A frame read back from storage keeps its cache key."""
        value = STORABLE_FRAMES[name]
        logical = LogicalPandasDataFrame()
        back = logical.storage_to_python(logical.python_to_storage(value))
        assert_frame_equal(back, value)
        assert _hash(back) == _hash(value)

    @pytest.mark.parametrize(
        "value",
        [
            pd.Series([1, 2], index=_multi_index(), name="v").swaplevel(),
            pd.Series([1, 2]),
            pd.Series(pd.Categorical(["x", "y"]), name=0),
        ],
        ids=["swapped levels", "unnamed", "categorical int name"],
    )
    def test_series_hash_survives_storage(self, value: pd.Series) -> None:
        logical = LogicalPandasSeries()
        back = logical.storage_to_python(logical.python_to_storage(value))
        assert_series_equal(back, value)
        assert _hash(back) == _hash(value)


class TestDescriptorGrammar:
    """Pins of the descriptor grammar.

    Uses the non-public descriptor functions because the test is about the grammar itself: a
    change here is a new hash definition and needs a new label.
    """

    def test_frame_descriptor(self) -> None:
        from orcapod.hashing.pandas_value_hash import pandas_frame_descriptor

        frame = pd.DataFrame(
            {0: pd.array([1, None], dtype="Int64"), "c": pd.Categorical(["x", "y"])},
            index=pd.Index([5, 6], name="id"),
        )
        frame.columns = frame.columns.rename("features")
        assert pandas_frame_descriptor(frame) == {
            "kind": "pandas.dataframe",
            "height": 2,
            "columns": [
                [{"int": 0}, {"masked": "Int64"}],
                [
                    "c",
                    {
                        "categorical": {
                            "categories": [{"numpy": "object"}, ["x", "y"]],
                            "ordered": False,
                        }
                    },
                ],
            ],
            "columns_names": ["features"],
            "index": [["id", {"numpy": "int64"}]],
        }

    def test_series_descriptor(self) -> None:
        from orcapod.hashing.pandas_value_hash import pandas_series_descriptor

        series = pd.Series(
            pd.array(["x", None], dtype="string"),
            index=pd.MultiIndex.from_tuples([(0, 1.5), (1, 2.5)], names=["i", None]),
            name=("a", 1),
        )
        assert pandas_series_descriptor(series) == {
            "kind": "pandas.series",
            "height": 2,
            "name": {"tuple": ["a", {"int": 1}]},
            "dtype": {"string": "python"},
            "index": [
                ["i", {"numpy": "int64"}],
                [None, {"numpy": "float64"}],
            ],
        }

    def test_label_grammar(self) -> None:
        from orcapod.hashing.pandas_value_hash import pandas_label_descriptor

        assert pandas_label_descriptor("a") == "a"
        assert pandas_label_descriptor(None) is None
        assert pandas_label_descriptor(True) == {"bool": True}
        assert pandas_label_descriptor(np.int8(3)) == {"int": 3}
        assert pandas_label_descriptor(0.5) == {"float": "0x1.0000000000000p-1"}
        assert pandas_label_descriptor(pd.Timestamp("2020-01-01", tz="UTC")) == {
            "timestamp": [1577836800000000000, "s", "UTC"]
        }
        assert pandas_label_descriptor(dt.date(2020, 1, 2)) == {"date": "2020-01-02"}
        assert pandas_label_descriptor(pd.Timedelta(seconds=1)) == {
            "timedelta": [1000000000, "ns"]
        }
        assert pandas_label_descriptor(pd.Period("2020-01", freq="M")) == {
            "period": [600, "M"]
        }
        assert pandas_label_descriptor(pd.NaT) == {"nat": "datetime"}

    def test_dtype_grammar(self) -> None:
        from orcapod.hashing.pandas_value_hash import pandas_dtype_descriptor

        assert pandas_dtype_descriptor(np.dtype("datetime64[ns]")) == {
            "numpy": "datetime64[ns]"
        }
        assert pandas_dtype_descriptor(pd.DatetimeTZDtype("ns", "UTC")) == {
            "datetimetz": ["ns", "UTC"]
        }
        assert pandas_dtype_descriptor(pd.StringDtype("pyarrow")) == {
            "string": "pyarrow"
        }
        assert pandas_dtype_descriptor(pd.BooleanDtype()) == {"masked": "boolean"}
        assert pandas_dtype_descriptor(pd.PeriodDtype("M")) == {"period": "M"}
        assert pandas_dtype_descriptor(pd.PeriodDtype("Q")) == {"period": "Q-DEC"}
        assert pandas_dtype_descriptor(pd.IntervalDtype("int64", "left")) == {
            "interval": [{"numpy": "int64"}, "left"]
        }
        # The Arrow type itself, struct field order included, is in the ordered digest.
        assert (
            pandas_dtype_descriptor(
                pd.ArrowDtype(
                    pa.list_(pa.struct([("v", pa.string()), ("u", pa.int8())]))
                )
            )
            == "arrow"
        )
