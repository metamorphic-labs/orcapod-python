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

The identity is what pandas' own ``assert_frame_equal`` / ``assert_series_equal`` compare with
their default arguments (developer's decision, November 11, 2026), with two exceptions:
``flags`` is not part of it (storage does not keep it), and ``-0.0`` and ``0.0`` hash apart
(the values are hashed as bytes; this costs a recomputation, never a wrong result). ``attrs``
is not part of it, as in pandas.

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

    frame:  {"kind": "pandas.dataframe", "height": <rows>,
             "columns": [[<label>, <dtype>], ...],      in column order
             "columns_names": [<label>, ...],           columns.names, one per column level
             "index": [[<label>, <dtype>], ...]}        index levels in order: name, dtype
    series: {"kind": "pandas.series", "height": <rows>, "name": <label>, "dtype": <dtype>,
             "index": [[<label>, <dtype>], ...]}

    <label> := <str> | null | {"bool": b} | {"int": n} | {"float": <float.hex()>}
             | {"tuple": [<label>, ...]}
             | {"timestamp": [<epoch value>, <unit>, <tz or null>]} | {"date": <iso date>}
             | {"timedelta": [<value>, <unit>]} | {"period": [<ordinal>, <period alias>]}
             | {"nat": "datetime" | "timedelta"}
    <dtype> := {"numpy": <numpy dtype name>}
             | {"masked": "Int8" | ... | "UInt64" | "Float32" | "Float64" | "boolean"}
             | {"string": <storage>}
             | {"datetimetz": [<unit>, <tz>]}
             | {"categorical": {"categories": [<dtype>, [<label>, ...]], "ordered": <bool>}}
             | {"period": <period alias>} | {"interval": [<subtype dtype>, <closed>]}
             | "arrow"                                  pd.ArrowDtype; the type is in the digest

A period alias is pandas' own period frequency string (``"M"``, ``"Q-DEC"``, as in
``Period.freqstr``). A label or dtype outside the grammar raises ``ValueError``.
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any

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
        return {"string": dtype.storage}
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


def _index_descriptor(index: pd.Index) -> list[Any]:
    return [
        [
            pandas_label_descriptor(name),
            pandas_dtype_descriptor(index.get_level_values(level).dtype),
        ]
        for level, name in enumerate(index.names)
    ]


def _column_descriptor(label: Any, dtype: Any) -> list[Any]:
    try:
        dtype_term = pandas_dtype_descriptor(dtype)
    except ValueError as exc:
        raise ValueError(f"column {label!r}: {exc}") from exc
    return [pandas_label_descriptor(label), dtype_term]


def pandas_frame_descriptor(df: pd.DataFrame) -> dict[str, Any]:
    """Return the descriptor of a DataFrame (see the module docstring).

    Args:
        df: The DataFrame.

    Returns:
        The descriptor as a JSON-serialisable dict.

    Raises:
        ValueError: If a label or dtype is outside the grammar; a dtype error names its column.
    """
    return {
        "kind": "pandas.dataframe",
        "height": len(df),
        "columns": [
            _column_descriptor(label, dtype)
            for label, dtype in zip(df.columns, df.dtypes)
        ],
        "columns_names": [pandas_label_descriptor(name) for name in df.columns.names],
        "index": _index_descriptor(df.index),
    }


def pandas_series_descriptor(series: pd.Series) -> dict[str, Any]:
    """Return the descriptor of a Series (see the module docstring).

    Args:
        series: The Series.

    Returns:
        The descriptor as a JSON-serialisable dict.

    Raises:
        ValueError: If a label or the dtype is outside the grammar.
    """
    return {
        "kind": "pandas.series",
        "height": len(series),
        "name": pandas_label_descriptor(series.name),
        "dtype": pandas_dtype_descriptor(series.dtype),
        "index": _index_descriptor(series.index),
    }


def _values_table(df: pd.DataFrame) -> pa.Table:
    """Return the frame's values as an Arrow table with positional field names.

    Raises:
        ValueError: If a column cannot be converted to Arrow.
    """
    try:
        table = pa.Table.from_pandas(df, preserve_index=True)
    except pa.ArrowInvalid as exc:
        raise ValueError(
            "cannot convert to Arrow; check for columns with mixed or "
            f"non-Arrow-serialisable values. Original error: {exc}"
        ) from exc
    columns = [f"c{position}" for position in range(df.shape[1])]
    levels = [f"i{position}" for position in range(df.index.nlevels)]
    return table.rename_columns(columns + levels)


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
    descriptor = pandas_frame_descriptor(df)
    digest = arrow_hasher.hash_table_ordered(_values_table(df)).digest
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
    descriptor = pandas_series_descriptor(series)
    digest = arrow_hasher.hash_table_ordered(
        _values_table(series.to_frame(name="c0"))
    ).digest
    return labelled_content_hash(PANDAS_SERIES_HASH_LABEL, descriptor, digest)
