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
from orcapod.errors import (
    InputValidationError,
    NaiveDatetimeError,
    SubMicrosecondDatetimeError,
)

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
    names = LAYOUTS[layout]
    return pa.table(
        {name: _COLUMNS[name] for name in names},
        schema=pa.schema([_SCHEMA.field(name) for name in names]),
    )


def _batch(layout: str) -> pa.RecordBatch:
    return _arrow(layout).to_batches()[0]


SUPPORTED: dict[str, Callable[[str], Any]] = {
    "polars": lambda layout: pl.DataFrame(
        {name: _COLUMNS[name] for name in LAYOUTS[layout]}
    ),
    "pandas": lambda layout: pd.DataFrame(
        {name: _COLUMNS[name] for name in LAYOUTS[layout]}
    ).astype({"g": "string", "y": "string"}),
    "pandas_arrow": lambda layout: pd.DataFrame(
        {name: _COLUMNS[name] for name in LAYOUTS[layout]}
    ).convert_dtypes(dtype_backend="pyarrow"),
    "arrow_table": _arrow,
    "arrow_record_batch": _batch,
    "arrow_reader": lambda layout: _arrow(layout).to_reader(),
    "arrow_struct_array": lambda layout: _batch(layout).to_struct_array(),
    "c_stream_exporter": lambda layout: _ArrowStreamExporter(_arrow(layout)),
    "c_array_exporter": lambda layout: _ArrowArrayExporter(_batch(layout)),
}

# The default source id, measured before ITL-1330: one value for all supported
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


def _frame_with_object_column(name: str, layout: str) -> tuple[pd.DataFrame, pa.Schema]:
    columns = (*LAYOUTS[layout], "origin", "key")
    fields = [
        *_SCHEMA,
        pa.field("origin", pa.large_string(), nullable=False),
        pa.field("key", pa.int64(), nullable=False),
    ]
    schema = pa.schema(fields)
    frame = pd.DataFrame(
        {**_COLUMNS, "origin": ["left", "right"], "key": [91, 92]}
    ).astype({"g": "string", "y": "string", "origin": "string"})
    return frame.loc[:, list(columns)].astype({name: object}), pa.schema(
        [schema.field(column) for column in columns]
    )


@pytest.mark.parametrize("layout", list(LAYOUTS))
@pytest.mark.parametrize("name", ["g", "k", "x", "y", "origin", "key"])
def test_pandas_object_column_requires_its_own_schema_field(
    layout: str, name: str
) -> None:
    """Every column role needs a declaration, even if other fields are declared."""
    frame, schema = _frame_with_object_column(name, layout)
    options: dict[str, Any] = {
        "tag_columns": ["g", "k"],
        "tag_only_columns": ["g"],
        "system_tag_columns": ["origin"],
        "record_id_columns": ["key"],
    }
    for declaration in (None, schema.remove(schema.get_field_index(name))):
        with pytest.raises(
            InputValidationError, match=rf"'{name}'.*dtype object"
        ) as info:
            DataFrameSource(frame, schema=declaration, **options)
        for remedy in ("schema=", "astype('string')", "pd.to_datetime", "DictSource"):
            assert remedy in str(info.value)

    actual = DataFrameSource(frame, schema=schema, **options)
    typed = frame.astype({"g": "string", "y": "string", "origin": "string"})
    for numeric in ("k", "x", "key"):
        typed[numeric] = typed[numeric].astype("float64" if numeric == "x" else "int64")
    expected = DataFrameSource(typed, schema=schema, **options)
    assert actual.as_table(all_info=True).equals(
        expected.as_table(all_info=True), check_metadata=True
    )
    assert actual.source_id == expected.source_id


@pytest.mark.parametrize("with_schema", [False, True])
@pytest.mark.parametrize("values", [["a", "b"], [1, 2]])
def test_pandas_categorical_with_object_categories_is_refused(
    with_schema: bool, values: list[Any]
) -> None:
    frame = pd.DataFrame(
        {"kind": pd.Categorical(values, categories=pd.Index(values, dtype=object))}
    )
    schema = pa.schema([("kind", pa.large_string())]) if with_schema else None
    with pytest.raises(InputValidationError, match=r"'kind'.*object-dtype categories"):
        DataFrameSource(frame, schema=schema)


@pytest.mark.parametrize("with_schema", [False, True])
def test_polars_object_column_is_refused(with_schema: bool) -> None:
    frame = pl.DataFrame({"origin": pl.Series(["left", "right"], dtype=pl.Object)})
    schema = (
        pa.schema([pa.field("origin", pa.large_string(), nullable=False)])
        if with_schema
        else None
    )
    with pytest.raises(
        InputValidationError, match=r"'origin'.*dtype Object.*DictSource"
    ):
        DataFrameSource(frame, schema=schema, system_tag_columns=["origin"])


def test_pandas_all_none_object_column_requires_a_nullable_type() -> None:
    frame = pd.DataFrame({"missing": pd.Series([None, None], dtype=object)})
    with pytest.raises(InputValidationError, match=r"'missing'.*dtype object"):
        DataFrameSource(frame)
    with pytest.raises(
        InputValidationError, match=r"'missing'.*None.*row 0.*non-nullable"
    ):
        DataFrameSource(
            frame, schema=pa.schema([pa.field("missing", pa.int64(), nullable=False)])
        )
    source = DataFrameSource(frame, schema=pa.schema([("missing", pa.int64())]))
    assert (
        source.as_table()
        .column("missing")
        .equals(pa.chunked_array([[None, None]], type=pa.int64()))
    )


@pytest.mark.parametrize("zone", [None, "UTC"])
def test_declared_pandas_object_timestamps_keep_nanoseconds(zone: Any) -> None:
    first = pd.Timestamp("2026-01-02T03:04:05.123456000", tz=zone)
    second = pd.Timestamp("2026-01-02T03:04:05.123456789", tz=zone)
    frame = pd.DataFrame({"when": pd.Series([first, second], dtype=object)})
    schema = pa.schema([("when", pa.timestamp("ns", tz=zone))])
    column = DataFrameSource(frame, schema=schema).as_table().column("when")
    assert column.type == schema.field("when").type
    assert column.cast(pa.int64()).to_pylist() == [first.value, second.value]

    with pytest.raises(
        InputValidationError, match=r"'when'.*123456789.*row 1.*timestamp"
    ):
        DataFrameSource(
            frame, schema=pa.schema([("when", pa.timestamp("us", tz=zone))])
        )


@pytest.mark.parametrize(
    ("values", "arrow_type", "first_invalid"),
    [
        ([1, 2.5, "also invalid"], pa.int64(), "2.5"),
        ([1, 2**53 + 1], pa.float64(), "9007199254740993"),
        ([["a"], ["b", 2]], pa.list_(pa.string()), r"\['b', 2\]"),
        (
            [[1], [None]],
            pa.list_(pa.field("item", pa.int64(), nullable=False)),
            r"\[None\]",
        ),
    ],
)
def test_declared_pandas_object_column_reports_first_inexact_value(
    values: list[Any], arrow_type: pa.DataType, first_invalid: str
) -> None:
    frame = pd.DataFrame({"payload": pd.Series(values, dtype=object)})
    with pytest.raises(
        InputValidationError, match=rf"'payload'.*{first_invalid}.*row 1"
    ):
        DataFrameSource(frame, schema=pa.schema([("payload", arrow_type)]))
