"""``polars.DataFrame`` and ``polars.Series`` logical types for orcapod.

Storage
-------
``LogicalPolarsDataFrame`` maps ``pl.DataFrame`` <-> Arrow ``large_binary`` using an Arrow
IPC stream as the storage envelope, the same envelope ``LogicalPandasDataFrame`` uses.
``LogicalPolarsSeries`` stores a ``pl.Series`` the same way, as a one-column frame named
after the Series (a Polars Series name is always a ``str``, so no sentinel is needed).

The frame is exported with ``compat_level=pl.CompatLevel.oldest()``, so strings and binaries
are written as ``large_string`` / ``large_binary`` (never the view types) whatever the
installed Polars defaults to, and its chunks are combined into a single record batch. Reading
the stream back with ``pl.from_arrow`` restores the original Polars schema exactly: column
order and names, integer width and signedness, float width, ``Categorical`` with its
``Categories`` (name, namespace, physical width), ``Enum`` with its
category order, ``Decimal`` precision and scale, ``Datetime`` unit and time zone, ``Duration``
unit, ``List``, ``Array`` shape, ``Struct`` field order, and ``Null``.

Dtypes that cannot round-trip exactly are rejected when the value is written, naming the
column and dtype: ``Object`` (Arrow export would silently store raw pointer bytes),
``Int128`` / ``UInt128`` (pyarrow cannot import them), and Polars extension dtypes (a logical
value nested inside a frame value is not supported).

Content hash
------------
The stored IPC bytes are not canonical (the bytes under null slots, and Polars' own field
metadata, vary for equal frames), so the content hash is computed from the frame itself by
``polars_frame_content_hash`` / ``polars_series_content_hash``, which the semantic hashers
``PolarsDataFrameHandler`` / ``PolarsSeriesHandler`` call. The hash is::

    sha256(descriptor + b"\\x00" + starfix_digest)

under the ``ContentHash`` method label ``"polars_dataframe_v1"`` (a frame) or
``"polars_series_v1"`` (a Series). Each label names its algorithm (descriptor grammar plus
digest) and changes only if that algorithm would give an existing value a different digest
(the hash label rule, ``docs/reference/hashing.md``).

``starfix_digest`` is ``starfix.ArrowDigester.hash_table`` of a hash-only Arrow projection of
the values (see ``_hash_projection``), taken in starfix's ordered mode
(``ordered_columns=True, ordered_struct_fields=True``), so column order and struct field
order at every depth are part of the digest. starfix hashes a dictionary-encoded column by
its values and tells a null struct from a struct whose fields are all null.

``descriptor`` is canonical JSON (keys sorted, separators ``","`` and ``":"``, UTF-8) that
carries what starfix does not see, in a grammar Orcapod owns (never Polars' ``repr`` or its
``_PL_*`` metadata)::

    frame:  {"kind": "polars.dataframe", "height": <rows>,
             "columns": [[<name>, <dtype>], ...]}   in column order
    series: {"kind": "polars.series", "name": <name>, "height": <rows>,
             "dtype": <dtype>}

    <dtype> := "int8" | "int16" | "int32" | "int64"
             | "uint8" | "uint16" | "uint32" | "uint64"
             | "float16" | "float32" | "float64"
             | "boolean" | "string" | "binary" | "date" | "time" | "null"
             | {"categorical": [<name>, <namespace>, <physical>]}   its Categories
             | {"enum": [<category>, ...]}          every declared category, in order
             | {"decimal": [<precision>, <scale>]}
             | {"datetime": [<unit>, <time zone or null>]}
             | {"duration": <unit>}
             | {"list": <dtype>}
             | {"array": [<dtype>, <size>]}         a multi-dimensional array nests
             | {"struct": [[<field>, <dtype>], ...]}   in field order

The identity is everything a pod can observe through the public Polars API as the frame's
logical content (developer's decision, November 11, 2026): column order and struct field
order, at every depth, are part of it. Physical properties are not: chunking, sortedness
flags (which storage does not keep), physical ``Categorical`` codes, Arrow layout. An
Enum's full enumeration of choices is part of its dtype: its categories in declared order,
including categories no row holds, so ``Enum(["a", "b"])``, ``Enum(["b", "a"])`` and
``Enum(["a", "b", "z"])`` hash differently over the same values. Any change to the grammar or
the digest is a new definition under new labels (``polars_dataframe_v2``,
``polars_series_v2``). A dtype outside the grammar raises ``ValueError`` naming the column.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import polars as pl
import pyarrow as pa
from starfix import ArrowDigester

from orcapod.logical_types.arrow_ipc import ipc_bytes_to_table, table_to_ipc_bytes
from orcapod.logical_types.base_logical_type import BaseLogicalType
from orcapod.logical_types.identifier import version_metadata
from orcapod.logical_types.registry import (
    make_arrow_extension_type,
    make_polars_extension_type,
)
from orcapod.types import ContentHash

if TYPE_CHECKING:
    from polars._typing import PolarsDataType

    from orcapod.logical_types.protocols import TypeConverterProtocol


POLARS_DATAFRAME_HASH_LABEL = "polars_dataframe_v1"
"""``ContentHash`` method label of a ``pl.DataFrame`` hash; names the whole definition."""

POLARS_SERIES_HASH_LABEL = "polars_series_v1"
"""``ContentHash`` method label of a ``pl.Series`` hash; names the whole definition."""

_IDENTIFIER_METADATA = version_metadata(1)
"""Extension metadata of the types in this module: version 1 of their stored layout and
value hash. Never stored without it, so there is no older layout to read."""

_UNSTORABLE_DTYPES: tuple[type[pl.DataType], ...] = (pl.Object, pl.Int128, pl.UInt128)

_SUPPORTED_DTYPES_TEXT = (
    "Supported dtypes are integers up to 64 bits, floats, String, Binary, Boolean, "
    "Categorical, Enum, Decimal, Date, Time, Datetime, Duration, Null, and List, Array or "
    "Struct of those."
)

# Grammar names of the dtypes that carry no parameters.
_PLAIN_DTYPE_NAMES: dict[object, str] = {
    pl.Int8: "int8",
    pl.Int16: "int16",
    pl.Int32: "int32",
    pl.Int64: "int64",
    pl.UInt8: "uint8",
    pl.UInt16: "uint16",
    pl.UInt32: "uint32",
    pl.UInt64: "uint64",
    pl.Float16: "float16",
    pl.Float32: "float32",
    pl.Float64: "float64",
    pl.Boolean: "boolean",
    pl.String: "string",
    pl.Binary: "binary",
    pl.Date: "date",
    pl.Time: "time",
    pl.Null: "null",
}


def _find_unstorable_dtype(dtype: PolarsDataType) -> PolarsDataType | None:
    """Return the first dtype inside ``dtype`` that cannot round-trip exactly.

    Walks ``List``, ``Array`` and ``Struct`` children.

    Args:
        dtype: A Polars dtype instance or class, possibly nested.

    Returns:
        The offending (possibly nested) dtype, or ``None`` when every part of ``dtype`` can be
        stored losslessly.
    """
    if isinstance(dtype, type):
        # A bare dtype class has no children to walk.
        unstorable = issubclass(dtype, (pl.BaseExtension, *_UNSTORABLE_DTYPES))
        return dtype if unstorable else None
    if isinstance(dtype, (pl.BaseExtension, *_UNSTORABLE_DTYPES)):
        return dtype
    if isinstance(dtype, (pl.List, pl.Array)):
        return _find_unstorable_dtype(dtype.inner)
    if isinstance(dtype, pl.Struct):
        for field in dtype.fields:
            found = _find_unstorable_dtype(field.dtype)
            if found is not None:
                return found
    return None


def check_polars_schema_storable(schema: pl.Schema) -> None:
    """Raise if any column of ``schema`` cannot be stored and read back exactly.

    Args:
        schema: The schema of the frame about to be stored.

    Raises:
        ValueError: If a column's dtype is, or contains, ``Object``, ``Int128``, ``UInt128``
            or a Polars extension dtype. The message names the column and its dtype.
    """
    for name, dtype in schema.items():
        bad = _find_unstorable_dtype(dtype)
        if bad is None:
            continue
        nested = "" if bad is dtype else f" (contains {bad})"
        raise ValueError(
            f"Polars value: column {name!r} has dtype {dtype}{nested}, which cannot be "
            f"stored and read back exactly. {_SUPPORTED_DTYPES_TEXT} Convert the column "
            "explicitly before returning the value."
        )


def _ordered_pairs(pairs: Iterable[tuple[str, Any]]) -> list[list[Any]]:
    """Encode named members (columns or struct fields) as an ordered list of pairs.

    The one place where column order and struct field order enter the descriptor: a JSON
    list keeps its order under ``sort_keys``, a JSON object would not.

    Args:
        pairs: ``(name, dtype descriptor)`` pairs in the value's own order.

    Returns:
        ``[[name, dtype descriptor], ...]`` in the same order.
    """
    return [[name, descriptor] for name, descriptor in pairs]


def polars_dtype_descriptor(dtype: PolarsDataType) -> Any:
    """Encode a Polars dtype in Orcapod's hash descriptor grammar.

    See the module docstring for the grammar.

    Args:
        dtype: The dtype to encode, possibly nested.

    Returns:
        A JSON-serialisable value: a ``str`` for a plain dtype, a one-key ``dict`` for a
        parameterised one. Struct fields are an ordered list of ``[name, dtype]`` pairs, so
        their order is part of the descriptor.

    Raises:
        ValueError: If ``dtype`` (or a dtype nested in it) is outside the grammar. The
            message names the offending dtype; callers add the column.
    """
    if isinstance(dtype, pl.Enum):
        return {"enum": dtype.categories.to_list()}
    if dtype is pl.Categorical:  # the bare class means the default ``Categories``
        dtype = pl.Categorical()
    if isinstance(dtype, pl.Categorical):
        categories = dtype.categories
        return {
            "categorical": [
                categories.name(),
                categories.namespace(),
                _PLAIN_DTYPE_NAMES[categories.physical()],
            ]
        }
    if isinstance(dtype, pl.Decimal):
        return {"decimal": [dtype.precision, dtype.scale]}
    if isinstance(dtype, pl.Datetime):
        return {"datetime": [dtype.time_unit, dtype.time_zone]}
    if isinstance(dtype, pl.Duration):
        return {"duration": dtype.time_unit}
    if isinstance(dtype, pl.List):
        return {"list": polars_dtype_descriptor(dtype.inner)}
    if isinstance(dtype, pl.Array):
        return {"array": [polars_dtype_descriptor(dtype.inner), dtype.size]}
    if isinstance(dtype, pl.Struct):
        return {
            "struct": _ordered_pairs(
                (field.name, polars_dtype_descriptor(field.dtype))
                for field in dtype.fields
            )
        }
    if not isinstance(dtype, pl.BaseExtension):
        dtype_class = dtype if isinstance(dtype, type) else type(dtype)
        name = _PLAIN_DTYPE_NAMES.get(dtype_class)
        if name is not None:
            return name
    raise ValueError(
        f"dtype {dtype} is outside Orcapod's hash grammar. {_SUPPORTED_DTYPES_TEXT}"
    )


def _hash_projection_type(arrow_type: pa.DataType) -> pa.DataType:
    """Map a nested Arrow type to the type starfix hashes it as.

    ``fixed_size_list`` (Polars ``Array``) becomes ``large_list``, a nested ``null`` becomes
    an all-null ``bool`` (starfix does not hash ``null``), and a dictionary becomes its value
    type. The descriptor records the real dtypes, so nothing is lost. Field names and
    nullability are kept.

    Args:
        arrow_type: The Arrow type of a column, as exported by Polars.

    Returns:
        The type to cast the column to before hashing.
    """
    if pa.types.is_null(arrow_type):
        return pa.bool_()
    if pa.types.is_dictionary(arrow_type):
        return _hash_projection_type(arrow_type.value_type)
    if (
        pa.types.is_fixed_size_list(arrow_type)
        or pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
    ):
        item = arrow_type.value_field
        return pa.large_list(item.with_type(_hash_projection_type(item.type)))
    if pa.types.is_struct(arrow_type):
        return pa.struct(
            [field.with_type(_hash_projection_type(field.type)) for field in arrow_type]
        )
    return arrow_type


def _hash_projection(df: pl.DataFrame) -> pa.Table:
    """Build the Arrow table whose starfix digest stands for the frame's values.

    Top-level ``Null`` columns are left out: their content is fully determined by their
    dtype and the row count, both in the descriptor.

    Args:
        df: The frame to hash. Its dtypes must already be inside the grammar.

    Returns:
        A table of the frame's non-``Null`` columns, cast with ``_hash_projection_type``.
    """
    table = df.to_arrow(compat_level=pl.CompatLevel.oldest())
    kept = [field for field in table.schema if not pa.types.is_null(field.type)]
    table = table.select([field.name for field in kept])
    target = pa.schema(
        [field.with_type(_hash_projection_type(field.type)) for field in kept]
    )
    return table.cast(target)


def _values_digest(df: pl.DataFrame) -> bytes:
    """The starfix digest of a frame's values: the only call into starfix.

    starfix runs in its ordered mode, so column order and struct field order at every depth
    are part of the digest, as they are part of the descriptor.

    Args:
        df: The frame to hash. Its dtypes must already be inside the grammar.

    Returns:
        ``ArrowDigester.hash_table`` of the projected table, with ``ordered_columns`` and
        ``ordered_struct_fields`` on.
    """
    return ArrowDigester.hash_table(
        _hash_projection(df), ordered_columns=True, ordered_struct_fields=True
    )


def _content_hash(
    descriptor: dict[str, Any], df: pl.DataFrame, method: str
) -> ContentHash:
    """Combine a descriptor and a frame's values into the content hash.

    The digest is ``sha256(descriptor_json + b"\x00" + starfix_digest)``, where
    ``descriptor_json`` is ``descriptor`` as canonical JSON (keys sorted, separators ``","``
    and ``":"``, UTF-8, non-ASCII kept) and ``starfix_digest`` is
    ``_values_digest(df)``.

    Args:
        descriptor: The frame or Series descriptor in the module's grammar.
        df: The frame whose values are hashed (a Series is passed as its one-column frame).
        method: The ``ContentHash`` method label naming the hash definition.

    Returns:
        A ``ContentHash`` with method ``method``.
    """
    descriptor_bytes = json.dumps(
        descriptor, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    values_digest = _values_digest(df)
    digest = hashlib.sha256(descriptor_bytes + b"\x00" + values_digest).digest()
    return ContentHash(method=method, digest=digest)


def polars_frame_content_hash(df: pl.DataFrame) -> ContentHash:
    """Content hash of a ``pl.DataFrame`` (see the module docstring for the scheme).

    Args:
        df: The frame to hash.

    Returns:
        A ``ContentHash`` with method ``"polars_dataframe_v1"``.

    Raises:
        ValueError: If a column's dtype is outside the descriptor grammar.
    """
    columns: list[tuple[str, Any]] = []
    for name, dtype in df.schema.items():
        try:
            columns.append((name, polars_dtype_descriptor(dtype)))
        except ValueError as error:
            raise ValueError(
                f"Polars value: column {name!r} cannot be hashed: {error}"
            ) from error
    descriptor = {
        "kind": "polars.dataframe",
        "height": df.height,
        "columns": _ordered_pairs(columns),
    }
    return _content_hash(descriptor, df, POLARS_DATAFRAME_HASH_LABEL)


def polars_series_content_hash(series: pl.Series) -> ContentHash:
    """Content hash of a ``pl.Series``: its name, dtype and values.

    Args:
        series: The Series to hash.

    Returns:
        A ``ContentHash`` with method ``"polars_series_v1"``; never equal to the hash of
        the one-column frame holding the same Series (the labels and the descriptors'
        ``"kind"`` differ).

    Raises:
        ValueError: If the dtype is outside the descriptor grammar.
    """
    try:
        dtype_descriptor = polars_dtype_descriptor(series.dtype)
    except ValueError as error:
        raise ValueError(
            f"Polars value: Series {series.name!r} cannot be hashed: {error}"
        ) from error
    descriptor = {
        "kind": "polars.series",
        "name": series.name,
        "height": series.len(),
        "dtype": dtype_descriptor,
    }
    return _content_hash(descriptor, series.to_frame(), POLARS_SERIES_HASH_LABEL)


def _frame_to_ipc_bytes(df: pl.DataFrame) -> bytes:
    """Serialise a frame to the storage envelope after checking its dtypes.

    Args:
        df: The frame to store.

    Returns:
        Arrow IPC stream bytes holding the frame, exported with
        ``compat_level=pl.CompatLevel.oldest()``, as one record batch.

    Raises:
        ValueError: If a column's dtype cannot round-trip exactly (see
            ``check_polars_schema_storable``).
    """
    check_polars_schema_storable(df.schema)
    table = df.to_arrow(compat_level=pl.CompatLevel.oldest()).combine_chunks()
    return table_to_ipc_bytes(table)


def _ipc_bytes_to_frame(storage_value: Any) -> pl.DataFrame:
    """Read the storage envelope back into a frame with its stored column names.

    ``pl.from_arrow`` renames a column called ``""`` to ``"column_0"`` (and then fails if a
    ``"column_0"`` exists too), so the table is read under positional names and the stored
    names are restored afterwards.

    Args:
        storage_value: Arrow IPC stream bytes (or a buffer) as stored in ``large_binary``.

    Returns:
        The frame, with the schema it was stored with.

    Raises:
        TypeError: If the stream does not decode to a ``pl.DataFrame``.
    """
    table = ipc_bytes_to_table(storage_value)
    names = table.column_names
    frame = pl.from_arrow(table.rename_columns([str(i) for i in range(len(names))]))
    if not isinstance(frame, pl.DataFrame):
        raise TypeError(
            f"stored Polars value did not decode to a pl.DataFrame (got {type(frame)!r})"
        )
    frame.columns = names
    return frame


class LogicalPolarsDataFrame(BaseLogicalType):
    """Logical type for ``pl.DataFrame``.

    Stores frames as Arrow ``large_binary`` holding an Arrow IPC stream (see the module
    docstring for the exact export settings and the rejected dtypes).

    The extension name ``"polars.dataframe"`` is library-qualified, following the
    ``"pandas.dataframe"`` and ``"numpy.ndarray"`` precedent: it names the Python type users
    annotate with, so a stored Arrow schema documents itself.

    Example:
        >>> lt = LogicalPolarsDataFrame()
        >>> df = pl.DataFrame({"x": [1, 2]}, schema={"x": pl.Int8})
        >>> dict(lt.storage_to_python(lt.python_to_storage(df)).schema)
        {'x': Int8}
    """

    _arrow_ext_class = make_arrow_extension_type(
        "polars.dataframe", pa.large_binary(), _IDENTIFIER_METADATA
    )
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type(
        "polars.dataframe", pa.large_binary(), _IDENTIFIER_METADATA.decode()
    )
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "polars.dataframe"
    python_type: type = pl.DataFrame

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the cached Arrow extension type for ``pl.DataFrame``.

        Returns:
            A ``pa.ExtensionType`` instance with extension name ``"polars.dataframe"`` and
            storage type ``pa.large_binary()``.
        """
        if LogicalPolarsDataFrame._arrow_ext is None:
            ext_class = LogicalPolarsDataFrame._arrow_ext_class
            LogicalPolarsDataFrame._arrow_ext = ext_class()  # pyright: ignore[reportCallIssue]  # synthesized subclass takes no arguments
        return LogicalPolarsDataFrame._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type for ``pl.DataFrame``.

        Returns:
            A ``pl.BaseExtension`` instance registered under ``"polars.dataframe"``.
        """
        if LogicalPolarsDataFrame._polars_ext is None:
            ext_class = LogicalPolarsDataFrame._polars_ext_class
            LogicalPolarsDataFrame._polars_ext = ext_class()  # pyright: ignore[reportCallIssue]  # synthesized subclass takes no arguments
        return LogicalPolarsDataFrame._polars_ext

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> bytes:
        """Serialise a ``pl.DataFrame`` to Arrow IPC stream bytes.

        Args:
            value: A ``pl.DataFrame`` whose dtypes can all round-trip exactly.
            converter: Ignored. Present for protocol conformance.

        Returns:
            Raw bytes of an Arrow IPC stream holding the frame as one record batch.

        Raises:
            TypeError: If ``value`` is not a ``pl.DataFrame``.
            ValueError: If a column's dtype cannot round-trip exactly (see
                ``check_polars_schema_storable``).
        """
        if not isinstance(value, pl.DataFrame):
            raise TypeError(
                f"LogicalPolarsDataFrame: expected a pl.DataFrame, got {type(value)!r}"
            )
        return _frame_to_ipc_bytes(value)

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> pl.DataFrame:
        """Reconstruct a ``pl.DataFrame`` from Arrow IPC stream bytes.

        Args:
            storage_value: Raw bytes as stored in Arrow ``large_binary``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            The ``pl.DataFrame`` with the schema it was stored with.

        Raises:
            TypeError: If the stored stream does not decode to a ``pl.DataFrame``.
        """
        return _ipc_bytes_to_frame(storage_value)


class LogicalPolarsSeries(BaseLogicalType):
    """Logical type for ``pl.Series``.

    Stores the Series as a one-column frame named after the Series, in the same Arrow IPC
    envelope as ``LogicalPolarsDataFrame``, so its name, dtype and values round-trip exactly
    under the same rules.

    The extension name ``"polars.series"`` is library-qualified, like ``"pandas.series"``.

    Example:
        >>> lt = LogicalPolarsSeries()
        >>> s = pl.Series("m", [1, 2], dtype=pl.UInt8)
        >>> back = lt.storage_to_python(lt.python_to_storage(s))
        >>> back.name, back.dtype
        ('m', UInt8)
    """

    _arrow_ext_class = make_arrow_extension_type(
        "polars.series", pa.large_binary(), _IDENTIFIER_METADATA
    )
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type(
        "polars.series", pa.large_binary(), _IDENTIFIER_METADATA.decode()
    )
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "polars.series"
    python_type: type = pl.Series

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the cached Arrow extension type for ``pl.Series``.

        Returns:
            A ``pa.ExtensionType`` instance with extension name ``"polars.series"`` and
            storage type ``pa.large_binary()``.
        """
        if LogicalPolarsSeries._arrow_ext is None:
            ext_class = LogicalPolarsSeries._arrow_ext_class
            LogicalPolarsSeries._arrow_ext = ext_class()  # pyright: ignore[reportCallIssue]  # synthesized subclass takes no arguments
        return LogicalPolarsSeries._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type for ``pl.Series``.

        Returns:
            A ``pl.BaseExtension`` instance registered under ``"polars.series"``.
        """
        if LogicalPolarsSeries._polars_ext is None:
            ext_class = LogicalPolarsSeries._polars_ext_class
            LogicalPolarsSeries._polars_ext = ext_class()  # pyright: ignore[reportCallIssue]  # synthesized subclass takes no arguments
        return LogicalPolarsSeries._polars_ext

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> bytes:
        """Serialise a ``pl.Series`` to Arrow IPC stream bytes.

        Args:
            value: A ``pl.Series`` whose dtype can round-trip exactly.
            converter: Ignored. Present for protocol conformance.

        Returns:
            Raw bytes of an Arrow IPC stream holding the Series as a one-column frame.

        Raises:
            TypeError: If ``value`` is not a ``pl.Series``.
            ValueError: If the dtype cannot round-trip exactly (see
                ``check_polars_schema_storable``).
        """
        if not isinstance(value, pl.Series):
            raise TypeError(
                f"LogicalPolarsSeries: expected a pl.Series, got {type(value)!r}"
            )
        return _frame_to_ipc_bytes(value.to_frame())

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> pl.Series:
        """Reconstruct a ``pl.Series`` from Arrow IPC stream bytes.

        Args:
            storage_value: Raw bytes as stored in Arrow ``large_binary``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            The ``pl.Series`` with the name, dtype and values it was stored with.

        Raises:
            TypeError: If the stored stream does not decode to a one-column frame.
        """
        frame = _ipc_bytes_to_frame(storage_value)
        if frame.width != 1:
            raise TypeError(
                "LogicalPolarsSeries: stored value has "
                f"{frame.width} columns, expected exactly 1"
            )
        return frame.to_series(0)
