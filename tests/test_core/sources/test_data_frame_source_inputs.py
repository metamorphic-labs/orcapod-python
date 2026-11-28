"""What a ``DataFrameSource`` accepts: data frames only (ITL-1330).

A pandas DataFrame, a Polars DataFrame, or an object exporting an Arrow C stream or a C
array of a struct builds as before. Anything else (a dict, a list, a tuple, a generator,
a string, a NumPy array, a Polars or pandas Series, a Polars LazyFrame) is refused when
the source is built, with a ``TypeError`` that names the type given and the remedy.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from orcapod.core.sources import DataFrameSource, DictSource
from orcapod.errors import NaiveDatetimeError, SubMicrosecondDatetimeError

# ---------------------------------------------------------------------------
# Refused inputs
# ---------------------------------------------------------------------------

_COLUMNS: dict[str, list[Any]] = {
    "g": ["a", "b"],
    "k": [1, 2],
    "x": [1.5, 2.5],
    "y": ["p", "q"],
}
_ROWS = [
    {"g": "a", "k": 1, "x": 1.5, "y": "p"},
    {"g": "b", "k": 2, "x": 2.5, "y": "q"},
]
_SCHEMA = pa.schema(
    [
        pa.field("g", pa.large_string(), nullable=False),
        pa.field("k", pa.int64(), nullable=False),
        pa.field("x", pa.float64(), nullable=False),
        pa.field("y", pa.large_string(), nullable=False),
    ]
)

# name -> (builds the refused input, its type as the error names it, text of its remedy)
REFUSED: dict[str, tuple[Callable[[], Any], str, str]] = {
    "dict_of_lists": (lambda: dict(_COLUMNS), "dict", "DictSource"),
    "empty_dict": (lambda: {}, "dict", "DictSource"),
    "list_of_dicts": (lambda: list(_ROWS), "list", "DictSource"),
    "list_of_tuples": (
        lambda: [("a", 1, 1.5, "p"), ("b", 2, 2.5, "q")],
        "list",
        "DictSource",
    ),
    "list_of_lists": (lambda: [["a", "b"], ["p", "q"]], "list", "ListSource"),
    "empty_list": (lambda: [], "list", "ListSource"),
    "tuple_of_dicts": (lambda: tuple(_ROWS), "tuple", "DictSource"),
    "generator_of_dicts": (lambda: (row for row in _ROWS), "generator", "DictSource"),
    "none": (lambda: None, "NoneType", "DictSource"),
    "str": (lambda: "abc", "str", "DictSource"),
    "set": (lambda: {1, 2}, "set", "ListSource"),
    # Typed columns in a Python container are still a Python container.
    "dict_of_numpy_arrays": (
        lambda: {name: np.array(values) for name, values in _COLUMNS.items()},
        "dict",
        "pl.DataFrame(",
    ),
    "dict_of_polars_series": (
        lambda: {name: pl.Series(values) for name, values in _COLUMNS.items()},
        "dict",
        "pl.DataFrame(",
    ),
    "dict_of_pandas_series": (
        lambda: {name: pd.Series(values) for name, values in _COLUMNS.items()},
        "dict",
        "pl.DataFrame(",
    ),
    "dict_of_arrow_arrays": (
        lambda: {name: pa.array(values) for name, values in _COLUMNS.items()},
        "dict",
        "pl.DataFrame(",
    ),
    "list_of_polars_series": (
        lambda: [pl.Series(name, values) for name, values in _COLUMNS.items()],
        "list",
        "pl.DataFrame(",
    ),
    "list_of_arrow_record_batches": (
        lambda: pa.Table.from_pydict(_COLUMNS).to_batches(),
        "list",
        "pl.DataFrame(",
    ),
    # Not a container of Python values, and not a data frame either.
    "polars_series": (
        lambda: pl.Series("x", [1.5, 2.5]),
        "polars.Series",
        "to_frame()",
    ),
    "polars_struct_series": (
        lambda: pl.DataFrame(_COLUMNS).to_struct("row"),
        "polars.Series",
        "to_frame()",
    ),
    "polars_lazy_frame": (
        lambda: pl.DataFrame(_COLUMNS).lazy(),
        "polars.LazyFrame",
        "collect()",
    ),
    "pandas_series": (
        lambda: pd.Series([1.5, 2.5], name="x"),
        "pandas.Series",
        "to_frame()",
    ),
    "numpy_2d": (lambda: np.array([[1, 2], [3, 4]]), "numpy.ndarray", "pl.DataFrame("),
    "numpy_1d": (lambda: np.array([1, 2]), "numpy.ndarray", "pl.DataFrame("),
    "numpy_structured": (
        lambda: np.array([(1, 1.5), (2, 2.5)], dtype=[("k", "i8"), ("x", "f8")]),
        "numpy.ndarray",
        "pl.DataFrame(",
    ),
}

# Every way the other arguments may accompany the refused input.
ARGUMENTS: dict[str, dict[str, Any]] = {
    "bare": {},
    "tags": {"tag_columns": ["g", "k"]},
    "tags_reversed_tag_only": {"tag_columns": ["k", "g"], "tag_only_columns": True},
    "one_tag_only": {"tag_columns": ["g", "k"], "tag_only_columns": ["k"]},
    "keys": {"tag_columns": ["g", "k"], "record_id_columns": ["k", "g"]},
    "schema": {"schema": _SCHEMA},
    "all": {
        "tag_columns": ["g", "k"],
        "tag_only_columns": True,
        "record_id_columns": ["g", "k"],
        "schema": _SCHEMA,
        "source_id": "named",
    },
    # Arguments that are themselves invalid: the input is refused first.
    "unknown_tag": {"tag_columns": ["missing"]},
    "unknown_key": {"record_id_columns": ["missing"]},
}


@pytest.mark.parametrize("arguments", list(ARGUMENTS), ids=list(ARGUMENTS))
@pytest.mark.parametrize("kind", list(REFUSED), ids=list(REFUSED))
def test_an_input_that_is_not_a_data_frame_is_refused(
    kind: str, arguments: str
) -> None:
    """The error is a ``TypeError`` naming the type given and its remedy, whatever the
    other arguments are (also invalid ones: the input is refused first)."""
    make, type_name, remedy = REFUSED[kind]
    data = make()

    with pytest.raises(TypeError) as info:
        DataFrameSource(data, **ARGUMENTS[arguments])

    message = str(info.value)
    assert "DataFrameSource accepts a data frame only" in message
    assert f"got {type_name}" in message
    assert remedy in message
    assert (
        "pl.DataFrame(" in message or "to_frame()" in message or "collect()" in message
    )


@pytest.mark.parametrize("kind", ["dict_of_lists", "list_of_dicts", "list_of_tuples"])
def test_the_refusal_of_a_python_container_points_to_both_sources(kind: str) -> None:
    """A dict or a list names ``DictSource`` and ``ListSource``, and says that a caller
    who wants Polars' own typing passes ``pl.DataFrame(...)``."""
    with pytest.raises(TypeError) as info:
        DataFrameSource(REFUSED[kind][0](), tag_columns=["g"])
    message = str(info.value)
    assert "DictSource" in message
    assert "ListSource" in message
    assert "pl.DataFrame(" in message


def test_a_generator_is_refused_without_being_read() -> None:
    """Refusing reads nothing: a generator given by mistake is still whole."""
    rows: Any = (row for row in _ROWS)
    with pytest.raises(TypeError, match="got generator"):
        DataFrameSource(rows, tag_columns=["g", "k"])
    assert list(rows) == _ROWS


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (datetime(2026, 1, 2, 3, 4, 5), NaiveDatetimeError),
        (
            pd.Timestamp("2026-01-02T03:04:05.123456789Z"),
            SubMicrosecondDatetimeError,
        ),
    ],
    ids=["naive", "sub_microsecond"],
)
@pytest.mark.parametrize("shape", ["dict_of_lists", "list_of_dicts"])
def test_the_reported_defect_is_refused_and_its_remedy_follows_the_rule(
    shape: str, value: Any, error: type[Exception]
) -> None:
    """The values of the report: no source is built from them by ``DataFrameSource``, and
    the source the error points to applies the datetime rule to them."""
    rows = [{"id": 1, "v": value, "w": value}, {"id": 2, "v": value, "w": value}]
    data: Any = (
        rows if shape == "list_of_dicts" else {k: [r[k] for r in rows] for k in rows[0]}
    )
    with pytest.raises(TypeError, match="DictSource"):
        DataFrameSource(data, tag_columns=["id"], tag_only_columns=True)
    with pytest.raises(error):
        DictSource(
            rows,
            tag_columns=["id"],
            tag_only_columns=True,
            data_schema={"id": int, "v": datetime, "w": datetime},
        )


# ---------------------------------------------------------------------------
# Supported inputs build as before
# ---------------------------------------------------------------------------


class _ArrowStreamExporter:
    """An object of another library that exports an Arrow C stream."""

    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        return self.table.__arrow_c_stream__(requested_schema)


class _ArrowArrayExporter:
    """An object of another library that exports an Arrow C array of a struct."""

    def __init__(self, batch: pa.RecordBatch) -> None:
        self.batch = batch

    def __arrow_c_array__(self, requested_schema: Any = None) -> Any:
        return self.batch.__arrow_c_array__(requested_schema)


# Two tag columns and two data columns, in two table layouts.
LAYOUTS: dict[str, tuple[str, ...]] = {
    "tags_first": ("g", "k", "x", "y"),
    "mixed": ("y", "k", "x", "g"),
}
# Two declared tag orders, each in the tag-only and the data-tag role (and one of each).
ROLES: dict[str, dict[str, Any]] = {
    "gk_tag_only": {"tag_columns": ["g", "k"], "tag_only_columns": True},
    "kg_tag_only": {"tag_columns": ["k", "g"], "tag_only_columns": True},
    "gk_data_tags": {"tag_columns": ["g", "k"]},
    "kg_data_tags": {"tag_columns": ["k", "g"]},
    "gk_one_of_each": {"tag_columns": ["g", "k"], "tag_only_columns": ["k"]},
}


def _arrow(layout: str) -> pa.Table:
    return pa.table({name: _COLUMNS[name] for name in LAYOUTS[layout]})


def _batch(layout: str) -> pa.RecordBatch:
    return _arrow(layout).to_batches()[0]


SUPPORTED: dict[str, Callable[[str], Any]] = {
    "polars": lambda layout: pl.DataFrame(
        {name: _COLUMNS[name] for name in LAYOUTS[layout]}
    ),
    "pandas": lambda layout: pd.DataFrame(
        {name: _COLUMNS[name] for name in LAYOUTS[layout]}
    ),
    "arrow_table": _arrow,
    "arrow_record_batch": _batch,
    "arrow_reader": lambda layout: _arrow(layout).to_reader(),
    "arrow_struct_array": lambda layout: _batch(layout).to_struct_array(),
    "c_stream_exporter": lambda layout: _ArrowStreamExporter(_arrow(layout)),
    "c_array_exporter": lambda layout: _ArrowArrayExporter(_batch(layout)),
}

# The default source id, measured before ITL-1330: one value for all eight supported
# inputs, both layouts and every role (the id hashes the source's table, in no column
# order, and no role enters it), so it pins what each input builds.
DEFAULT_SOURCE_ID = (
    "data_frame_v1:arrow_v1:"
    "000002cbaa91da0d861f3fdd888ca2e07246810db637d95c3768f8eaf58be891669542"
)


@pytest.mark.parametrize("role", list(ROLES), ids=list(ROLES))
@pytest.mark.parametrize("layout", list(LAYOUTS), ids=list(LAYOUTS))
@pytest.mark.parametrize("kind", list(SUPPORTED), ids=list(SUPPORTED))
def test_a_supported_input_builds_as_before(kind: str, layout: str, role: str) -> None:
    """Rows, roles, types and the default source id of every supported input."""
    arguments = ROLES[role]
    source = DataFrameSource(SUPPORTED[kind](layout), **arguments)

    tags = tuple(arguments["tag_columns"])
    tag_only = arguments.get("tag_only_columns", ())
    plain = set(tags) if tag_only is True else set(tag_only)
    data_tags = tuple(name for name in tags if name not in plain)
    others = tuple(name for name in LAYOUTS[layout] if name not in tags)

    tag_keys, data_keys = source.keys()
    assert set(tag_keys) == set(tags)
    assert data_keys == data_tags + others

    table = source.as_table()
    assert table.num_rows == 2
    for name in LAYOUTS[layout]:
        assert table.schema.field(name) == _SCHEMA.field(name)
        assert table.column(name).to_pylist() == _COLUMNS[name]

    assert source.source_id == DEFAULT_SOURCE_ID


@pytest.mark.parametrize("role", list(ROLES), ids=list(ROLES))
@pytest.mark.parametrize("kind", list(SUPPORTED), ids=list(SUPPORTED))
def test_a_supported_input_with_keys_and_a_schema_builds(kind: str, role: str) -> None:
    """With ``record_id_columns`` (two, in two orders) and a ``schema=``, the record ids
    are the keys in key order, whatever the input kind."""
    for keys in (["g", "k"], ["k", "g"]):
        source = DataFrameSource(
            SUPPORTED[kind]("tags_first"),
            schema=_SCHEMA,
            record_id_columns=keys,
            source_id="named",
            **ROLES[role],
        )
        table = source.as_table(columns={"system_tags": True})
        record_ids = next(
            table.column(name).to_pylist()
            for name in table.column_names
            if name.startswith("_tag_record_id")
        )
        rows = [{"g": "a", "k": 1}, {"g": "b", "k": 2}]
        assert record_ids == [
            "{" + ",".join(_json_entry(key, row[key]) for key in keys) + "}"
            for row in rows
        ]
        assert source.source_id == "data_frame_v1:named"


def _json_entry(name: str, value: object) -> str:
    return f'"{name}":"{value}"' if isinstance(value, str) else f'"{name}":{value}'
