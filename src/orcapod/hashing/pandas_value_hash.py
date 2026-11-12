"""Content hashes of ``pandas.DataFrame`` and ``pandas.Series`` values.

A pandas value hashes as::

    sha256(canonical_json(descriptor) + b"\\x00" + arrow_hasher.hash_table_ordered(values_table))

under the ``ContentHash`` method label ``"pandas_dataframe_v1"`` (a frame) or
``"pandas_series_v1"`` (a Series); see ``orcapod.hashing.labelled_hash``. The label names this
whole procedure: the grammar, the values table and the ordered Arrow digest it embeds. It
changes only when the procedure would give an existing value a different digest (then
``pandas_dataframe_v2`` / ``pandas_series_v2``), and it never enters the computation: only the
digest bytes of ``hash_table_ordered`` are hashed, not that hash's own label. Such a change
also changes the identifier of the pandas logical types (``logical_types/pandas_type.py``).

The hash is strict (developer's decisions, November 11 and 12, 2026): everything about a
value that can be changed is part of its identity, ``attrs`` and ``-0.0`` versus ``0.0``
included. The one exception is a property the stored form does not restore, which never
counts, so that a value read back from a store hashes like the value that was written:
``flags``, the kind of a ``RangeIndex``, an index's ``freq``, unused MultiIndex levels, the
storage of a string dtype, and the ``object`` dtype of a column that reads back as another
dtype. ``PANDAS_IDENTITY_PROPERTIES`` lists every property and whether it is identity; it
is the one place that states the decision.

Hashing what storage restores
-----------------------------
An ``object`` column (or index level) whose values are not strings does not come back as
``object``: ``[1, 2]`` reads back ``int64`` and ``[1, None]`` reads back ``float64``. Such a
column is hashed in the form it reads back in: its Arrow column is converted to pandas the
way storage does, and when the result is no longer ``object``, that dtype is described and
its Arrow form is digested. An ``object`` column that reads back as ``object`` (strings,
bytes, dicts, lists, decimals, dates) is hashed as it is.

Values table
------------
``pa.Table.from_pandas(value, preserve_index=True)`` with its fields renamed by position:
``c0 .. cN-1`` for the columns in order, then ``i0 .. iM-1`` for the index levels in order.
Labels are not Arrow field names: they are in the descriptor, typed. A Series is hashed as its
one-column frame. The digest is the data context's Arrow hasher's ``hash_table_ordered``: the
same preprocessing as every Orcapod Arrow hash, with column order and struct field order at every
depth included (starfix's ordered options). It covers the Arrow types (with starfix's large /
non-large unification), the row count and the values; a categorical column is hashed by its
values.

Descriptor grammar
------------------
Canonical JSON, in a grammar Orcapod owns (never ``repr``, ``str(dtype)`` alone, or pyarrow's
``pandas`` schema metadata)::

    frame:  {"kind": "orcapod.pandas.dataframe", "height": <rows>, "attrs": <attrs>,
             "columns": [[<label>, <dtype>], ...],      in column order
             "columns_names": [<label>, ...],           columns.names, one per column level
             "index": [[<label>, <dtype>], ...]}        index levels in order: name, dtype
    series: {"kind": "orcapod.pandas.series", "height": <rows>, "attrs": <attrs>,
             "name": <label>, "dtype": <dtype>,
             "index": [[<label>, <dtype>], ...]}

    <label> := <str> | null | {"bool": b} | {"int": n} | {"float": <float.hex()>}
             | {"tuple": [<label>, ...]}
             | {"timestamp": [<epoch value>, <unit>, <tz or null>]} | {"date": <iso date>}
             | {"timedelta": [<value>, <unit>]} | {"period": [<ordinal>, <period alias>]}
             | {"nat": "datetime" | "timedelta"}
    <dtype> := {"numpy": <numpy dtype name>}
             | {"masked": "Int8" | ... | "UInt64" | "Float32" | "Float64" | "boolean"}
             | "string"                                 pd.StringDtype, any storage
             | {"datetimetz": [<unit>, <tz>]}
             | {"categorical": {"categories": [<dtype>, [<label>, ...]], "ordered": <bool>}}
             | {"period": <period alias>} | {"interval": [<subtype dtype>, <closed>]}
             | "arrow"                                  pd.ArrowDtype; the type is in the digest

``<attrs>`` is ``attrs`` as the JSON object it is stored as (``{}`` when it is empty or
cannot be written as JSON, which is when storage drops it). ``kind`` is the name of the
value's logical type.

A period alias is pandas' own period frequency string (``"M"``, ``"Q-DEC"``, as in
``Period.freqstr``). A label or dtype outside the grammar raises ``ValueError``.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

import numpy as np
import pandas as pd
import pyarrow as pa

from orcapod.hashing.labelled_hash import labelled_content_hash

if TYPE_CHECKING:
    from orcapod.protocols.hashing_protocols import ArrowHasherProtocol
    from orcapod.types import ContentHash


PANDAS_DATAFRAME_HASH_LABEL = "pandas_dataframe_v1"
"""``ContentHash.method`` of a ``pd.DataFrame`` value hash: the single definition."""

PANDAS_SERIES_HASH_LABEL = "pandas_series_v1"
"""``ContentHash.method`` of a ``pd.Series`` value hash: the single definition."""


@dataclass(frozen=True)
class PandasIdentityProperty:
    """One property of a pandas value and whether the ``v1`` hash depends on it.

    Attributes:
        name: A stable name for the property.
        identity: Whether two values that differ only in this property hash differently.
        carried_by: Which part of the hash sees the property: the ordered Arrow digest of
            the values table, the descriptor, or both; empty when ``identity`` is false.
        description: What the property is, in pandas terms. For a property that is not
            identity, it says why.
        option: Whether the property may be named in a logical type's ignore list. Only
            identity properties can be options.
    """

    name: str
    identity: bool
    carried_by: Literal["digest", "descriptor", "digest and descriptor", ""]
    description: str
    option: bool = False


PANDAS_IDENTITY_PROPERTIES: tuple[PandasIdentityProperty, ...] = (
    PandasIdentityProperty(
        "column_order", True, "digest", "order of the columns", option=True
    ),
    PandasIdentityProperty(
        "index_level_order", True, "digest", "order of the levels of a MultiIndex"
    ),
    PandasIdentityProperty(
        "struct_field_order",
        True,
        "digest",
        "order of the fields of a pd.ArrowDtype struct column, at every depth",
        option=True,
    ),
    PandasIdentityProperty(
        "values", True, "digest", "the values, their row order and the row count"
    ),
    PandasIdentityProperty(
        "arrow_type",
        True,
        "digest",
        "a column's Arrow type: integer and float width, datetime unit and timezone",
    ),
    PandasIdentityProperty("index_values", True, "digest", "the index values"),
    PandasIdentityProperty(
        "index_names", True, "descriptor", "the name of each index level, typed"
    ),
    PandasIdentityProperty(
        "index_dtype", True, "digest and descriptor", "the dtype of each index level"
    ),
    PandasIdentityProperty(
        "columns_names", True, "descriptor", "columns.names, one per column level"
    ),
    PandasIdentityProperty(
        "label_type",
        True,
        "descriptor",
        "the type of a column label (0 vs '0'; a MultiIndex tuple vs its text)",
    ),
    PandasIdentityProperty(
        "series_name", True, "descriptor", "a Series' name, typed", option=True
    ),
    PandasIdentityProperty(
        "value_kind", True, "descriptor", "a frame vs its one-column Series"
    ),
    PandasIdentityProperty(
        "pandas_dtype",
        True,
        "descriptor",
        "the pandas dtype where the Arrow values coincide: masked vs NumPy "
        "(Int64 vs int64, boolean vs bool), string vs object, period frequency, "
        "interval closed side",
    ),
    PandasIdentityProperty(
        "categorical_dtype", True, "descriptor", "categorical vs its plain values"
    ),
    PandasIdentityProperty(
        "categorical_categories",
        True,
        "descriptor",
        "the category list, unused categories included, and its order",
    ),
    PandasIdentityProperty(
        "categorical_ordered", True, "descriptor", "a categorical's ordered flag"
    ),
    PandasIdentityProperty(
        "signed_zero",
        True,
        "digest",
        "-0.0 vs 0.0 (pandas calls them equal; the values are hashed as bytes)",
    ),
    PandasIdentityProperty(
        "attrs",
        True,
        "descriptor",
        "the attrs dict, as the JSON object storage keeps",
        option=True,
    ),
    PandasIdentityProperty(
        "flags", False, "", "flags.allows_duplicate_labels: storage does not restore it"
    ),
    PandasIdentityProperty(
        "index_kind",
        False,
        "",
        "RangeIndex vs an int64 Index with the same values: storage restores an Index",
    ),
    PandasIdentityProperty(
        "index_freq",
        False,
        "",
        "freq of a DatetimeIndex or TimedeltaIndex: storage does not restore it",
    ),
    PandasIdentityProperty(
        "index_unused_levels",
        False,
        "",
        "unused levels of a MultiIndex: storage does not restore them",
    ),
    PandasIdentityProperty(
        "string_storage",
        False,
        "",
        "string[python] vs string[pyarrow]: storage restores string[python]",
    ),
    PandasIdentityProperty(
        "object_dtype",
        False,
        "",
        "object dtype of a column whose values read back as another dtype "
        "(object ints read back int64)",
    ),
    PandasIdentityProperty(
        "attrs_not_json",
        False,
        "",
        "attrs that cannot be written as JSON: storage drops them, so they hash as {}",
    ),
    PandasIdentityProperty(
        "dict_key_order",
        False,
        "",
        "key order of dicts in an object column: a dict is unordered",
    ),
)
"""Every property the ``v1`` hash was decided on, and whether it is identity.

This is the single statement of the decision (developer, November 11 and 12, 2026): the
hash is strict, so every property of a value counts, except a property the stored form does
not restore, which never counts and cannot be an option. Properties with ``option=True`` are
the ones a logical type's ignore list may name. The functions below implement exactly this
list; ``tests/test_hashing/test_pandas_value_identity.py`` checks every entry against a pair
of values. Changing an entry's ``identity`` changes existing hashes: it needs new labels and
a new logical type identifier.
"""

_MASKED_DTYPE_NAMES = frozenset(
    {
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Float32",
        "Float64",
        "boolean",
    }
)


def pandas_label_descriptor(label: Any) -> Any:
    """Describe a pandas label (column label, index or Series name, category) in the grammar.

    Args:
        label: The label.

    Returns:
        The label's grammar term; typed, so ``0`` and ``"0"`` differ.

    Raises:
        ValueError: If the label's type is outside the grammar.
    """
    if label is None or isinstance(label, str):
        return label
    if isinstance(label, (bool, np.bool_)):
        return {"bool": bool(label)}
    if isinstance(label, (int, np.integer)):
        return {"int": int(label)}
    if isinstance(label, (float, np.floating)):
        return {"float": float(label).hex()}
    if isinstance(label, tuple):
        return {"tuple": [pandas_label_descriptor(part) for part in label]}  # pyright: ignore[reportUnknownVariableType]
    if isinstance(label, (pd.Timestamp, dt.datetime, np.datetime64)):
        timestamp = pd.Timestamp(label)
        if not isinstance(timestamp, pd.Timestamp):  # NaT
            return {"nat": "datetime"}
        tz = None if timestamp.tz is None else str(timestamp.tz)
        return {"timestamp": [timestamp.value, timestamp.unit, tz]}
    if isinstance(label, dt.date):
        return {"date": label.isoformat()}
    if isinstance(label, (pd.Timedelta, dt.timedelta, np.timedelta64)):
        delta = pd.Timedelta(label)
        if not isinstance(delta, pd.Timedelta):  # NaT
            return {"nat": "timedelta"}
        return {"timedelta": [delta.value, delta.unit]}
    if isinstance(label, pd.Period):
        return {"period": [label.ordinal, label.freqstr]}
    raise ValueError(
        f"pandas label {label!r} of type {type(label).__name__} is outside the "
        "pandas_dataframe_v1 label grammar"
    )


def pandas_dtype_descriptor(dtype: Any) -> Any:
    """Describe a pandas dtype in the grammar.

    Args:
        dtype: A numpy dtype or a pandas extension dtype.

    Returns:
        The dtype's grammar term.

    Raises:
        ValueError: If the dtype is outside the grammar.
    """
    if isinstance(dtype, pd.CategoricalDtype):
        categories = dtype.categories
        return {
            "categorical": {
                "categories": [
                    pandas_dtype_descriptor(categories.dtype),
                    [pandas_label_descriptor(c) for c in categories],
                ],
                "ordered": bool(dtype.ordered),
            }
        }
    if isinstance(dtype, pd.ArrowDtype):
        return "arrow"
    if isinstance(dtype, pd.StringDtype):
        # The storage (python / pyarrow) is not kept by the stored form, so it is not
        # part of the term.
        return "string"
    if isinstance(dtype, pd.DatetimeTZDtype):
        return {"datetimetz": [dtype.unit, str(dtype.tz)]}
    if isinstance(dtype, pd.PeriodDtype):
        # The period alias ("M", "Q-DEC"), as in Period.freqstr; not the offset alias
        # ("ME", "QE-DEC") that dtype.freq.freqstr gives since pandas 2.2.
        return {"period": dtype.name.removeprefix("period[").removesuffix("]")}
    if isinstance(dtype, pd.IntervalDtype):
        return {"interval": [pandas_dtype_descriptor(dtype.subtype), dtype.closed]}
    if isinstance(dtype, np.dtype):
        return {"numpy": dtype.name}
    if getattr(dtype, "name", None) in _MASKED_DTYPE_NAMES:
        return {"masked": dtype.name}
    raise ValueError(
        f"pandas dtype {dtype!r} is outside the pandas_dataframe_v1 dtype grammar"
    )


def _column_descriptor(label: Any, dtype: Any) -> list[Any]:
    try:
        dtype_term = pandas_dtype_descriptor(dtype)
    except ValueError as exc:
        raise ValueError(f"column {label!r}: {exc}") from exc
    return [pandas_label_descriptor(label), dtype_term]


def _attrs_term(attrs: Any) -> Any:
    """Return ``attrs`` as the JSON object storage keeps, ``{}`` when storage drops it.

    pyarrow writes ``attrs`` as JSON and writes ``{}`` when it cannot, so a value read back
    has the JSON form: ``{1: "x"}`` reads back ``{"1": "x"}``, a tuple reads back a list.
    """
    try:
        return json.loads(json.dumps(attrs))
    except (TypeError, ValueError):
        return {}


def _restored_form(
    dtype: Any, column: pa.ChunkedArray[Any]
) -> tuple[Any, pa.ChunkedArray[Any]]:
    """Return the dtype and Arrow column a field has once read back from storage.

    Only an ``object`` field can change: its Arrow column is converted to pandas the way
    storage does, and when the result has another dtype (``[1, None]`` reads back
    ``float64``), that dtype and its Arrow form are returned.

    Args:
        dtype: The field's pandas dtype.
        column: The field's Arrow column in the values table.

    Returns:
        The dtype to describe and the Arrow column to digest.
    """
    if dtype != np.dtype(object):
        return dtype, column
    restored = column.to_pandas()
    if restored.dtype == np.dtype(object):
        return dtype, column
    return restored.dtype, pa.chunked_array([pa.Array.from_pandas(restored)])


def _values_table(df: pd.DataFrame) -> tuple[pa.Table, list[Any], list[Any]]:
    """Return the frame's values as an Arrow table, with the dtypes storage restores.

    The table has positional field names: ``c0 ..`` for the columns in order, then
    ``i0 ..`` for the index levels in order.

    Returns:
        The values table, the dtype of each column and the dtype of each index level, each
        field in the form it reads back from storage in.

    Raises:
        ValueError: If a column's dtype is outside the grammar (the error names the column)
            or a column cannot be converted to Arrow.
    """
    for label, dtype in zip(df.columns, df.dtypes):
        if dtype != np.dtype(object):
            # A dtype outside the grammar is reported by name, before Arrow refuses it.
            _column_descriptor(label, dtype)
    try:
        table = pa.Table.from_pandas(df, preserve_index=True)
    except pa.ArrowInvalid as exc:
        raise ValueError(
            "cannot convert to Arrow; check for columns with mixed or "
            f"non-Arrow-serialisable values. Original error: {exc}"
        ) from exc
    width = df.shape[1]
    index = df.index
    dtypes = [
        *df.dtypes,
        *(index.get_level_values(level).dtype for level in range(index.nlevels)),
    ]
    names = [f"c{position}" for position in range(width)] + [
        f"i{position}" for position in range(index.nlevels)
    ]
    restored = [
        _restored_form(dtype, table.column(position))
        for position, dtype in enumerate(dtypes)
    ]
    values = pa.table([column for _, column in restored], names=names)
    restored_dtypes = [dtype for dtype, _ in restored]
    return values, restored_dtypes[:width], restored_dtypes[width:]


def _index_descriptor(index: pd.Index, dtypes: list[Any]) -> list[Any]:
    return [
        [pandas_label_descriptor(name), pandas_dtype_descriptor(dtype)]
        for name, dtype in zip(index.names, dtypes)
    ]


def _frame_descriptor(
    df: pd.DataFrame, column_dtypes: list[Any], index_dtypes: list[Any]
) -> dict[str, Any]:
    return {
        "kind": "orcapod.pandas.dataframe",
        "height": len(df),
        "attrs": _attrs_term(df.attrs),
        "columns": [
            _column_descriptor(label, dtype)
            for label, dtype in zip(df.columns, column_dtypes)
        ],
        "columns_names": [pandas_label_descriptor(name) for name in df.columns.names],
        "index": _index_descriptor(df.index, index_dtypes),
    }


def _series_descriptor(
    series: pd.Series, dtype: Any, index_dtypes: list[Any]
) -> dict[str, Any]:
    return {
        "kind": "orcapod.pandas.series",
        "height": len(series),
        "attrs": _attrs_term(series.attrs),
        "name": pandas_label_descriptor(series.name),
        "dtype": pandas_dtype_descriptor(dtype),
        "index": _index_descriptor(series.index, index_dtypes),
    }


def pandas_frame_descriptor(df: pd.DataFrame) -> dict[str, Any]:
    """Return the descriptor of a DataFrame (see the module docstring).

    Args:
        df: The DataFrame.

    Returns:
        The descriptor as a JSON-serialisable dict.

    Raises:
        ValueError: If a label or dtype is outside the grammar (a dtype error names its
            column), or a column cannot be converted to Arrow.
    """
    _, column_dtypes, index_dtypes = _values_table(df)
    return _frame_descriptor(df, column_dtypes, index_dtypes)


def pandas_series_descriptor(series: pd.Series) -> dict[str, Any]:
    """Return the descriptor of a Series (see the module docstring).

    Args:
        series: The Series.

    Returns:
        The descriptor as a JSON-serialisable dict.

    Raises:
        ValueError: If a label or the dtype is outside the grammar, or the values cannot be
            converted to Arrow.
    """
    _, (dtype,), index_dtypes = _values_table(series.to_frame(name="c0"))
    return _series_descriptor(series, dtype, index_dtypes)


def pandas_frame_content_hash(
    df: pd.DataFrame, arrow_hasher: ArrowHasherProtocol
) -> ContentHash:
    """Return the ``pandas_dataframe_v1`` content hash of a DataFrame.

    Args:
        df: The DataFrame.
        arrow_hasher: The data context's Arrow hasher, whose ``hash_table_ordered`` digests
            the values table.

    Returns:
        A ``ContentHash`` with method ``"pandas_dataframe_v1"``.

    Raises:
        ValueError: If a label or dtype is outside the grammar, or a column cannot be converted
            to Arrow.
    """
    values, column_dtypes, index_dtypes = _values_table(df)
    descriptor = _frame_descriptor(df, column_dtypes, index_dtypes)
    digest = arrow_hasher.hash_table_ordered(values).digest
    return labelled_content_hash(PANDAS_DATAFRAME_HASH_LABEL, descriptor, digest)


def pandas_series_content_hash(
    series: pd.Series, arrow_hasher: ArrowHasherProtocol
) -> ContentHash:
    """Return the ``pandas_series_v1`` content hash of a Series.

    Args:
        series: The Series.
        arrow_hasher: The data context's Arrow hasher, whose ``hash_table_ordered`` digests
            the values table.

    Returns:
        A ``ContentHash`` with method ``"pandas_series_v1"``.

    Raises:
        ValueError: If a label or the dtype is outside the grammar, or the values cannot be
            converted to Arrow.
    """
    values, (dtype,), index_dtypes = _values_table(series.to_frame(name="c0"))
    descriptor = _series_descriptor(series, dtype, index_dtypes)
    digest = arrow_hasher.hash_table_ordered(values).digest
    return labelled_content_hash(PANDAS_SERIES_HASH_LABEL, descriptor, digest)
