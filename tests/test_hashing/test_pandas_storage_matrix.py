"""A pandas value hashes the same before and after storage, over a matrix of values (ITL-897).

``hash(read(write(v))) == hash(v)`` is the rule that makes a stored pandas value a cache hit.
It fails whenever storage changes a property the hash counts, and such properties are easy to
miss one at a time: an ``Int64`` index reads back ``int64``, mixed column labels read back as
text. So this module checks the rule over the product of index kinds, column label kinds and
column dtypes, for frames and Series.

A value storage does not accept (the write or the read raises) is outside the rule and is
left out; each test asserts that most of its matrix was accepted, so it cannot pass empty.
"""

from __future__ import annotations

import datetime as dt
import itertools
import warnings
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa

from orcapod.contexts import get_default_context
from orcapod.logical_types.pandas_type import (
    LogicalPandasDataFrame,
    LogicalPandasSeries,
)

# Index kinds: two rows each.
INDEXES: dict[str, pd.Index] = {
    "range": pd.RangeIndex(2),
    "int64": pd.Index([5, 6], dtype="int64"),
    "int32 named": pd.Index([5, 6], dtype="int32", name="id"),
    "object ints": pd.Index([5, 6], dtype=object),
    "float64": pd.Index([0.5, 1.5]),
    "object strings": pd.Index(["p", "q"], dtype=object),
    "string": pd.Index(["p", "q"], dtype="string"),
    "string[pyarrow]": pd.Index(["p", "q"], dtype="string[pyarrow]"),
    "Int64": pd.Index([5, 6], dtype="Int64"),
    "Int64 with NA": pd.Index(pd.array([5, None], dtype="Int64")),
    "Float64": pd.Index([0.5, 1.5], dtype="Float64"),
    "boolean": pd.Index([True, False], dtype="boolean"),
    "bool": pd.Index([True, False]),
    "arrow int64": pd.Index(pd.array([5, 6], dtype="int64[pyarrow]")),
    "categorical": pd.CategoricalIndex(["p", "q"]),
    "categorical, object int categories": pd.CategoricalIndex(
        [1, 2], categories=pd.Index([1, 2], dtype=object)
    ),
    "categorical, string dtype categories": pd.CategoricalIndex(
        ["p", "q"], categories=pd.Index(["p", "q"], dtype="string")
    ),
    "datetime": pd.DatetimeIndex(["2020-01-01", "2020-01-02"]),
    "datetime s": pd.DatetimeIndex(["2020-01-01", "2020-01-02"]).as_unit("s"),
    "datetime UTC": pd.DatetimeIndex(["2020-01-01", "2020-01-02"], tz="UTC"),
    "timedelta": pd.TimedeltaIndex(["1s", "2s"]),
    "period": pd.period_range("2020-01", periods=2, freq="M"),
    "interval": pd.interval_range(0, 2),
    "int name": pd.Index([5, 6], name=7),
    "float name": pd.Index([5, 6], name=1.5),
    "tuple name": pd.Index([5, 6], name=("a", 1)),
    "timestamp name": pd.Index([5, 6], name=pd.Timestamp("2020-01-01")),
    "multi": pd.MultiIndex.from_tuples([(0, "p"), (1, "q")], names=["i", "j"]),
    "multi unnamed": pd.MultiIndex.from_tuples([(0, "p"), (1, "q")]),
    "multi Int64": pd.MultiIndex.from_arrays(
        [pd.array([5, 6], dtype="Int64"), pd.array(["p", "q"], dtype="string")]
    ),
    "multi mixed names": pd.MultiIndex.from_tuples(
        [(0, "p"), (1, "q")], names=[1.5, "j"]
    ),
}

# The index kinds crossed with every column dtype: one of each way storage treats an index
# (kept, restored as NumPy, restored from a dictionary, named as text, several levels).
# Every index kind is crossed with every column axis and every Series name.
INDEXES_FOR_DTYPES: dict[str, pd.Index] = {
    name: INDEXES[name]
    for name in (
        "range",
        "object ints",
        "string",
        "Int64 with NA",
        "boolean",
        "categorical, object int categories",
        "datetime UTC",
        "period",
        "int name",
        "tuple name",
        "multi Int64",
        "multi mixed names",
    )
}

# Column axes: two labels each.
COLUMN_AXES: dict[str, pd.Index] = {
    "strings": pd.Index(["a", "b"], dtype=object),
    "string dtype": pd.Index(["a", "b"], dtype="string"),
    "ints": pd.Index([0, 1]),
    "int32": pd.Index([0, 1], dtype="int32"),
    "object ints": pd.Index([0, 1], dtype=object),
    "floats": pd.Index([0.5, 1.5]),
    "arrow int8": pd.Index([0, 1], dtype="int8[pyarrow]"),
    "arrow double": pd.Index([0.5, 1.5], dtype="double[pyarrow]"),
    "mixed int and str": pd.Index([0, "b"], dtype=object),
    "mixed int and float": pd.Index([1, 2.5], dtype=object),
    "None and str": pd.Index([None, "b"], dtype=object),
    "bools": pd.Index([True, False]),
    "timestamps": pd.DatetimeIndex(["2020-01-01", "2020-01-02"]),
    "timestamps s": pd.DatetimeIndex(["2020-01-01", "2020-01-02"]).as_unit("s"),
    "timestamps UTC": pd.DatetimeIndex(["2020-01-01", "2020-01-02"], tz="UTC"),
    "named": pd.Index(["a", "b"], dtype=object, name="features"),
    "int name": pd.Index(["a", "b"], dtype=object, name=3),
    "multi": pd.MultiIndex.from_tuples([("a", 1), ("a", 2)]),
    "multi named": pd.MultiIndex.from_tuples([("a", 1), ("b", 2)], names=["x", 2]),
    "multi int32": pd.MultiIndex.from_arrays(
        [pd.Index([1, 2], dtype="int32"), pd.Index(["x", "y"], dtype=object)]
    ),
}

# Column values: two rows each.
COLUMN_VALUES: dict[str, Any] = {
    "int64": np.array([1, 2], dtype="int64"),
    "int8": np.array([1, 2], dtype="int8"),
    "float64 with nan": np.array([1.5, np.nan]),
    "bool": np.array([True, False]),
    "Int64 with NA": pd.array([1, None], dtype="Int64"),
    "boolean with NA": pd.array([True, None], dtype="boolean"),
    "string with NA": pd.array(["x", None], dtype="string"),
    "string[pyarrow]": pd.array(["x", "y"], dtype="string[pyarrow]"),
    "object strings": pd.Series(["x", None], dtype=object),
    "object ints": pd.Series([1, 2], dtype=object),
    "object ints with None": pd.Series([1, None], dtype=object),
    "object bools with None": pd.Series([True, None], dtype=object),
    "object bytes": pd.Series([b"x", b"y"], dtype=object),
    "object dates": pd.Series([dt.date(2020, 1, 1), None], dtype=object),
    "object datetimes": pd.Series([dt.datetime(2020, 1, 1), None], dtype=object),
    "object lists": pd.Series([[1, None], [2]], dtype=object),
    "object nested lists": pd.Series([[[1, None]], [[2]]], dtype=object),
    "object dicts": pd.Series([{"u": 1, "v": [1, None]}, {"u": 2, "v": []}]),
    "categorical": pd.Categorical(["x", "y"], categories=["y", "x", "z"]),
    "ordered categorical": pd.Categorical(["x", "y"], ordered=True),
    "categorical, object int categories": pd.Categorical(
        [1, 2], categories=pd.Index([2, 1, 3], dtype=object)
    ),
    "categorical, int32 categories": pd.Categorical(
        [1, 2], categories=pd.Index([1, 2], dtype="int32")
    ),
    "categorical, float categories": pd.Categorical([0.5, None], categories=[0.5, 1.5]),
    "categorical, object bool categories": pd.Categorical(
        [True, False], categories=pd.Index([True, False], dtype=object)
    ),
    "categorical, string dtype categories": pd.Categorical(
        ["x", "y"], categories=pd.Index(["x", "y"], dtype="string")
    ),
    "categorical, datetime categories": pd.Categorical(
        pd.to_datetime(["2020-01-01", "2020-01-02"])
    ),
    "categorical, Int64 categories": pd.Categorical(
        [1, 2], categories=pd.Index([1, 2], dtype="Int64")
    ),
    "datetime": pd.to_datetime(["2020-01-01", None]),
    "datetime UTC": pd.to_datetime(["2020-01-01", "2020-01-02"]).tz_localize("UTC"),
    "timedelta": pd.to_timedelta(["1s", None]),
    "period": pd.period_range("2020-01", periods=2, freq="M"),
    "interval": pd.arrays.IntervalArray.from_breaks([0, 1, 2]),
    "arrow int64": pd.array([1, None], dtype="int64[pyarrow]"),
    "arrow struct": pd.Series(
        pa.array([{"u": 1, "v": "x"}, None]),
        dtype=pd.ArrowDtype(pa.struct([("u", pa.int64()), ("v", pa.string())])),
    ),
}

SERIES_NAMES: dict[str, Any] = {
    "none": None,
    "str": "s",
    "int": 3,
    "float": 1.5,
    "tuple": ("a", 1),
    "timestamp": pd.Timestamp("2020-01-01"),
}

ATTRS: dict[str, dict[Any, Any]] = {
    "empty": {},
    "json": {"unit": "mV", "n": [1, 2]},
    "int key and tuple": {1: "x", "t": (1, 2)},
}


def _hash(value: Any) -> str:
    """The semantic hash of ``value`` under the default data context, as text."""
    return get_default_context().semantic_hasher.hash_object(value).to_string()


def _stored_and_read(value: Any) -> Any:
    """``value`` after the storage round trip of its logical type."""
    logical = (
        LogicalPandasSeries()
        if isinstance(value, pd.Series)
        else LogicalPandasDataFrame()
    )
    return logical.storage_to_python(logical.python_to_storage(value))


def _unstable(values: dict[str, Any]) -> tuple[list[str], int]:
    """Check the round-trip rule on every value storage accepts.

    Returns:
        The names of the values whose hash changed through storage (or changed again on a
        second round trip), and how many values storage accepted.
    """
    unstable: list[str] = []
    accepted = 0
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for name, value in values.items():
            try:
                back = _stored_and_read(value)
                again = _stored_and_read(back)
            except (ValueError, TypeError, NotImplementedError, pa.ArrowException):
                continue  # Storage does not accept this value.
            accepted += 1
            if not (_hash(value) == _hash(back) == _hash(again)):
                unstable.append(name)
    return unstable, accepted


def _frame(index: pd.Index, columns: pd.Index, values: Any) -> pd.DataFrame:
    """A two-row, two-column frame: ``values`` and a plain ``int64`` column."""
    frame = pd.DataFrame(
        {"first": values, "second": np.array([1, 2], dtype="int64")}
    ).set_axis(index, axis=0)
    return frame.set_axis(columns, axis=1)


class TestFrameHashSurvivesStorage:
    """Frames over index kinds, column axes and column dtypes, two at a time."""

    def test_index_kinds_by_column_axes(self) -> None:
        """Every index kind with every column axis."""
        values = {
            f"index {i} / columns {c}": _frame(index, columns, COLUMN_VALUES["int64"])
            for (i, index), (c, columns) in itertools.product(
                INDEXES.items(), COLUMN_AXES.items()
            )
        }
        unstable, accepted = _unstable(values)
        assert unstable == []
        assert accepted > len(values) * 3 // 4

    def test_index_kinds_by_column_dtypes(self) -> None:
        """Index kinds of every treatment with every column dtype."""
        values = {
            f"index {i} / values {v}": _frame(index, COLUMN_AXES["strings"], column)
            for (i, index), (v, column) in itertools.product(
                INDEXES_FOR_DTYPES.items(), COLUMN_VALUES.items()
            )
        }
        unstable, accepted = _unstable(values)
        assert unstable == []
        assert accepted > len(values) * 3 // 4

    def test_column_axes_by_column_dtypes(self) -> None:
        """Every column axis with every column dtype."""
        values = {
            f"columns {c} / values {v}": _frame(INDEXES["range"], columns, column)
            for (c, columns), (v, column) in itertools.product(
                COLUMN_AXES.items(), COLUMN_VALUES.items()
            )
        }
        unstable, accepted = _unstable(values)
        assert unstable == []
        assert accepted > len(values) * 3 // 4

    def test_attrs(self) -> None:
        """Frames and Series with ``attrs`` storage keeps as JSON."""
        values: dict[str, Any] = {}
        for name, attrs in ATTRS.items():
            frame = _frame(INDEXES["range"], COLUMN_AXES["strings"], [1, 2])
            frame.attrs = attrs
            series = pd.Series([1, 2], name="s")
            series.attrs = attrs
            values[f"frame attrs {name}"] = frame
            values[f"series attrs {name}"] = series
        unstable, accepted = _unstable(values)
        assert unstable == []
        assert accepted == len(values)


class TestSeriesHashSurvivesStorage:
    """Series over index kinds, names and dtypes, two at a time."""

    def test_index_kinds_by_names(self) -> None:
        """Every index kind with every kind of Series name."""
        values = {
            f"index {i} / name {n}": pd.Series([1, 2], index=index, name=series_name)
            for (i, index), (n, series_name) in itertools.product(
                INDEXES.items(), SERIES_NAMES.items()
            )
        }
        unstable, accepted = _unstable(values)
        assert unstable == []
        assert accepted > len(values) * 3 // 4

    def test_index_kinds_by_dtypes(self) -> None:
        """Index kinds of every treatment with every dtype."""
        values = {
            f"index {i} / values {v}": pd.Series(column, name="s").set_axis(index)
            for (i, index), (v, column) in itertools.product(
                INDEXES_FOR_DTYPES.items(), COLUMN_VALUES.items()
            )
        }
        unstable, accepted = _unstable(values)
        assert unstable == []
        assert accepted > len(values) * 3 // 4

    def test_names_by_dtypes(self) -> None:
        """Every kind of Series name with every dtype."""
        values = {
            f"name {n} / values {v}": pd.Series(column, name=series_name)
            for (n, series_name), (v, column) in itertools.product(
                SERIES_NAMES.items(), COLUMN_VALUES.items()
            )
        }
        unstable, accepted = _unstable(values)
        assert unstable == []
        assert accepted > len(values) * 3 // 4
