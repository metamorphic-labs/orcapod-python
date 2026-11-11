"""``polars.DataFrame`` logical type for orcapod.

``LogicalPolarsDataFrame`` maps ``pl.DataFrame`` <-> Arrow ``large_binary`` using an Arrow
IPC stream as the storage envelope, the same envelope ``LogicalPandasDataFrame`` uses.

The frame is exported with ``compat_level=pl.CompatLevel.oldest()``, so strings and binaries
are written as ``large_string`` / ``large_binary`` (never the view types) whatever the
installed Polars defaults to, and its chunks are combined into a single record batch. Reading
the stream back with ``pl.from_arrow`` restores the original Polars schema exactly: column
order and names, integer width and signedness, float width, ``Categorical``, ``Enum`` with its
category order, ``Decimal`` precision and scale, ``Datetime`` unit and time zone, ``Duration``
unit, ``List``, ``Array`` shape, ``Struct`` field order, and ``Null``.

Dtypes that cannot round-trip exactly are rejected when the value is written, naming the
column and dtype: ``Object`` (Arrow export would silently store raw pointer bytes),
``Int128`` / ``UInt128`` (pyarrow cannot import them), and Polars extension dtypes (a logical
value nested inside a frame value is not supported).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import polars as pl
import pyarrow as pa

from orcapod.logical_types.arrow_ipc import ipc_bytes_to_table, table_to_ipc_bytes
from orcapod.logical_types.base_logical_type import BaseLogicalType
from orcapod.logical_types.registry import (
    make_arrow_extension_type,
    make_polars_extension_type,
)

if TYPE_CHECKING:
    from orcapod.logical_types.protocols import TypeConverterProtocol


_UNSTORABLE_DTYPES: tuple[type[pl.DataType], ...] = (pl.Object, pl.Int128, pl.UInt128)


def _find_unstorable_dtype(dtype: pl.DataType) -> pl.DataType | None:
    """Return the first dtype inside ``dtype`` that cannot round-trip exactly.

    Walks ``List``, ``Array`` and ``Struct`` children.

    Args:
        dtype: A Polars dtype, possibly nested.

    Returns:
        The offending (possibly nested) dtype, or ``None`` when every part of ``dtype`` can be
        stored losslessly.
    """
    if isinstance(dtype, pl.BaseExtension):
        return dtype
    if isinstance(dtype, _UNSTORABLE_DTYPES):
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
            f"LogicalPolarsDataFrame: column {name!r} has dtype {dtype}{nested}, which "
            "cannot be stored and read back exactly. Supported dtypes are integers up to "
            "64 bits, floats, String, Binary, Boolean, Categorical, Enum, Decimal, Date, "
            "Time, Datetime, Duration, Null, and List, Array or Struct of those. Convert "
            "the column explicitly before returning the frame."
        )


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
        >>> lt.storage_to_python(lt.python_to_storage(df)).schema
        Schema([('x', Int8)])
    """

    _arrow_ext_class = make_arrow_extension_type("polars.dataframe", pa.large_binary())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type(
        "polars.dataframe", pa.large_binary()
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
            LogicalPolarsDataFrame._arrow_ext = (
                LogicalPolarsDataFrame._arrow_ext_class()
            )
        return LogicalPolarsDataFrame._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type for ``pl.DataFrame``.

        Returns:
            A ``pl.BaseExtension`` instance registered under ``"polars.dataframe"``.
        """
        if LogicalPolarsDataFrame._polars_ext is None:
            LogicalPolarsDataFrame._polars_ext = (
                LogicalPolarsDataFrame._polars_ext_class()
            )
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
        check_polars_schema_storable(value.schema)
        table = value.to_arrow(compat_level=pl.CompatLevel.oldest()).combine_chunks()
        return table_to_ipc_bytes(table)

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
        frame = pl.from_arrow(ipc_bytes_to_table(storage_value))
        if not isinstance(frame, pl.DataFrame):
            raise TypeError(
                "LogicalPolarsDataFrame: stored value did not decode to a pl.DataFrame "
                f"(got {type(frame)!r})"
            )
        return frame
