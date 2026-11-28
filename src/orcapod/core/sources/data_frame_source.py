from __future__ import annotations

import math
from collections.abc import Collection, Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, cast

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa

from orcapod.core.sources.base import RootSource
from orcapod.core.data_tags import TagOnlyColumns, source_data_tags
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.core.sources.stream_builder import (
    normalize_record_id_columns,
    declare_native_custom_system_tags,
    require_custom_system_tags,
    require_custom_system_tags_values,
    require_supported_nullable_types,
)
from orcapod.errors import InputValidationError
from orcapod.system_constants import constants
from orcapod.utils import arrow_utils
from orcapod.utils.datetime_utils import instant_microseconds
from orcapod.utils.schema_utils import _normalize_column_list

if TYPE_CHECKING:
    from polars._typing import ArrowArrayExportable, ArrowStreamExportable


def _exports_arrow(data: object) -> bool:
    """Whether ``data`` exports an Arrow C stream or an Arrow C array."""
    return hasattr(data, "__arrow_c_stream__") or hasattr(data, "__arrow_c_array__")


def _type_name(kind: type) -> str:
    """The name a refusal gives a type: ``dict``, ``polars.Series``, ``numpy.ndarray``.

    A builtin is its own name; any other type is its top-level package and its
    qualified name, since the module a library keeps a class in changes between its
    versions.
    """
    package = kind.__module__.partition(".")[0]
    if package == "builtins":
        return kind.__qualname__
    return f"{package}.{kind.__qualname__}"


def _require_data_frame(data: object) -> None:
    """Refuse an input that is not a data frame (ITL-1330).

    A data frame is a pandas ``DataFrame``, a Polars ``DataFrame``, or an object that
    exports an Arrow C stream or an Arrow C array (a pyarrow ``Table``, ``RecordBatch``
    or ``RecordBatchReader``, a struct array, a frame of another library). Nothing else
    is: a Polars ``Series`` exports an Arrow C stream and is refused all the same.

    Nothing of ``data`` is read: the decision is made from its type.

    Raises:
        TypeError: When ``data`` is not a data frame. The message names the type given
            and the remedy.
    """
    if isinstance(data, (pd.DataFrame, pl.DataFrame)):
        return
    if not isinstance(data, pl.Series) and _exports_arrow(data):
        return
    if isinstance(data, (pl.Series, pd.Series)):
        remedy = "Make the series a data frame first: series.to_frame()."
    elif isinstance(data, pl.LazyFrame):
        remedy = "Collect the lazy frame first: frame.collect()."
    elif isinstance(data, np.ndarray):
        remedy = (
            "Build a data frame that names the array's columns first: "
            "pl.DataFrame(array, schema=[...])."
        )
    else:
        remedy = (
            "Python values go to DictSource (rows, as a list of dicts) or ListSource "
            "(a list of values), which type them by Orcapod's rules. To have Polars "
            "infer the types instead, build the data frame yourself and pass it: "
            "DataFrameSource(pl.DataFrame(data), ...)."
        )
    raise TypeError(
        "DataFrameSource accepts a data frame only (a pandas or Polars DataFrame, or "
        "an object that exports an Arrow C stream or C array, such as a pyarrow "
        f"Table); got {_type_name(type(data))}. {remedy}"
    )


def _require_declared_pandas_columns(
    frame: pd.DataFrame, schema: pa.Schema | None
) -> list[tuple[int, pa.Field[Any]]]:
    """Resolve object columns from dtype and schema without reading values."""
    columns = []
    for index, (label, dtype) in enumerate(frame.dtypes.items()):
        name = str(label)
        if (
            isinstance(dtype, pd.CategoricalDtype)
            and dtype.categories is not None
            and pd.api.types.is_object_dtype(dtype.categories.dtype)
        ):
            raise InputValidationError(
                f"Pandas categorical column {name!r} has object-dtype categories. "
                "Decode it to a typed dtype such as astype('string') before building "
                "a DataFrameSource."
            )
        if not pd.api.types.is_object_dtype(dtype):
            continue
        if schema is None or name not in schema.names:
            raise InputValidationError(
                f"Pandas column {name!r} has dtype object. Declare its type with "
                "schema=, convert it to a typed dtype (for example astype('string'), "
                "pd.to_datetime, or convert_dtypes()), or use DictSource for Python values."
            )
        columns.append((index, schema.field(name)))
    return columns


def _object_value_matches(value: Any, converted: Any, field: pa.Field[Any]) -> bool:
    """Check an Arrow conversion for changes to the original Python value."""
    if isinstance(value, np.datetime64):
        value = pd.Timestamp(value)
    if value is None or value is pd.NA or value is pd.NaT:
        return field.nullable and converted is None
    if isinstance(value, datetime) and isinstance(converted, datetime):
        instant = instant_microseconds(value)
        result = instant_microseconds(converted)
        if instant is not None and result is not None:
            return instant == result and getattr(value, "nanosecond", 0) == getattr(
                converted, "nanosecond", 0
            )
        return instant is None and result is None and value == converted
    if isinstance(value, np.timedelta64):
        value = pd.Timedelta(value)
    elif isinstance(value, (np.generic, np.ndarray)):
        value = value.tolist()
    kind = field.type
    if isinstance(kind, pa.StructType):
        if isinstance(value, tuple):
            value = dict(zip(converted, value, strict=True))
        return (
            isinstance(value, Mapping)
            and value.keys() <= converted.keys()
            and all(
                _object_value_matches(value.get(key), item, kind.field(key))
                for key, item in converted.items()
            )
        )
    if isinstance(kind, pa.MapType):
        pairs = list(value.items()) if isinstance(value, Mapping) else value
        if not isinstance(pairs, (list, tuple)):
            return False
        return len(pairs) == len(converted) and all(
            _object_value_matches(key, stored_key, kind.key_field)
            and _object_value_matches(item, stored_item, kind.item_field)
            for (key, item), (stored_key, stored_item) in zip(
                pairs, converted, strict=True
            )
        )
    if isinstance(value, (list, tuple)) and isinstance(converted, (list, tuple)):
        if not isinstance(
            kind,
            (
                pa.ListType,
                pa.LargeListType,
                pa.FixedSizeListType,
                pa.ListViewType,
                pa.LargeListViewType,
            ),
        ):
            return False
        return len(value) == len(converted) and all(
            _object_value_matches(item, result, kind.value_field)
            for item, result in zip(value, converted, strict=True)
        )
    if isinstance(value, float):
        if math.isnan(value):
            return isinstance(converted, float) and math.isnan(converted)
        if value == 0.0 and math.copysign(1.0, value) < 0:
            return isinstance(converted, float) and math.copysign(1.0, converted) < 0
    return bool(value == converted)


def _pandas_object_array(values: pd.Series, field: pa.Field[Any]) -> pa.Array[Any]:
    """Convert to the declared type and report the first incompatible value."""
    scalars = []
    for row, value in enumerate(values):
        try:
            missing = value is pd.NA or value is pd.NaT
            scalar = pa.scalar(None if missing else value, type=field.type)
            if not scalar.is_valid and not field.nullable:
                raise ValueError("the field is non-nullable")
            if not _object_value_matches(value, scalar.as_py(), field):
                raise ValueError("conversion would change the value")
        except (pa.ArrowException, TypeError, ValueError, OverflowError) as exc:
            raise InputValidationError(
                f"Pandas object column {field.name!r} cannot represent value {value!r} "
                f"at row {row} exactly as {field.type}: {exc}."
            ) from exc
        scalars.append(scalar)
    return pa.array(scalars, type=field.type)


def _pandas_to_arrow(frame: pd.DataFrame, schema: pa.Schema | None) -> pa.Table:
    """Keep the native conversion for typed columns and build object columns in Arrow."""
    columns = _require_declared_pandas_columns(frame, schema)
    if not columns:
        return pl.DataFrame(frame).to_arrow()
    objects = {
        index: _pandas_object_array(frame.iloc[:, index], field)
        for index, field in columns
    }
    typed = pl.DataFrame(
        frame.iloc[
            :, [index for index in range(len(frame.columns)) if index not in objects]
        ]
    ).to_arrow()
    typed_columns = iter(typed.columns)
    return pa.table(
        [
            objects[index] if index in objects else next(typed_columns)
            for index in range(len(frame.columns))
        ],
        names=[str(label) for label in frame.columns],
    )


class DataFrameSource(RootSource):
    """A source backed by a data frame.

    A data frame is a pandas ``DataFrame``, a Polars ``DataFrame``, or an object that
    exports an Arrow C stream or an Arrow C array of a struct (a pyarrow ``Table``,
    ``RecordBatch`` or ``RecordBatchReader``, a frame of another library). Its columns
    keep the types they have. It is converted to an Arrow table and enriched by
    ``SourceStreamBuilder`` (source-info, schema-hash, system tags).

    Data frames only (ITL-1330): a dict, a list or any other container of Python
    values, a NumPy array, a Polars or pandas ``Series`` and a Polars ``LazyFrame``
    are refused with a ``TypeError`` when the source is built. Python values go to
    ``DictSource`` or ``ListSource``, which type them by Orcapod's rules; a caller who
    wants Polars to infer the types passes ``pl.DataFrame(data)``.

    A pandas ``object`` column requires a field in ``schema``. Its values must fit
    the declared Arrow type exactly. Convert pandas categoricals with object
    categories and Polars ``Object`` columns to typed columns before building.

    Data-tags (ITL-1023): every declared tag column is a data-tag by default,
    also a data column of the same name. ``tag_only_columns`` (keyword-only) names the
    tags kept plain (tag only, not data, as in v0.3); ``True`` keeps all of them plain.

    ``hash_in_key_order`` (keyword-only, ITL-1272): when the source has key columns and
    no ``source_id``, hash its table for the default source id with the rows sorted by
    the key columns, in key order, so the same rows in any order give one default
    source id and one content hash. The rows are presented in the order given either
    way. Off by default: the rows are hashed in the order given. Refused with a
    ``source_id`` or without key columns.
    """

    TAKES_TAG_ONLY_COLUMNS: ClassVar[bool] = True

    # The config type key; also the kind in the kind part of the source id.
    SOURCE_KIND: ClassVar[str] = "data_frame"
    # The version of this kind's implementation, in the kind part (ITL-1425).
    SOURCE_VERSION: ClassVar[int] = 1
    TABLE_IS_ROWS: ClassVar[bool] = True

    def __init__(
        self,
        data: pd.DataFrame
        | pl.DataFrame
        | ArrowStreamExportable
        | ArrowArrayExportable,
        tag_columns: str | Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        source_id: str | None = None,
        schema: pa.Schema | None = None,
        *,
        tag_only_columns: TagOnlyColumns = (),
        record_id_columns: str | Collection[str] | None = None,
        hash_in_key_order: bool = False,
        **kwargs: Any,
    ) -> None:
        # Before anything is built: only a data frame is accepted (ITL-1330).
        _require_data_frame(data)
        arrow_input_schema = None
        if isinstance(data, pd.DataFrame):
            arrow_table = _pandas_to_arrow(data, schema)
        elif isinstance(data, pl.DataFrame):
            for name, dtype in data.schema.items():
                if dtype == pl.Object:
                    raise InputValidationError(
                        f"Polars column {name!r} has dtype Object. Convert it to a typed "
                        "dtype before building a DataFrameSource, or use DictSource "
                        "for Python values."
                    )
            arrow_table = data.to_arrow()
        else:
            # Materialize once before Polars drops Arrow field metadata.
            # Like Polars, prefer the array export when both are available.
            if hasattr(data, "__arrow_c_array__"):
                batch = pa.record_batch(cast("ArrowArrayExportable", data))
                arrow_input = pa.Table.from_batches([batch])
            else:
                arrow_input = pa.table(cast("ArrowStreamExportable", data))
            require_custom_system_tags(arrow_input, system_tag_columns)
            arrow_input_schema = arrow_input.schema
            # Polars rechunks native tables but preserves chunks on exporter inputs.
            arrow_table = cast(
                pl.DataFrame,
                pl.from_arrow(arrow_input, rechunk=isinstance(data, pa.Table)),
            ).to_arrow()

        tag_columns = _normalize_column_list(tag_columns)

        require_supported_nullable_types(
            pa.schema(
                [
                    field
                    for field in arrow_table.schema
                    if field.name.startswith(
                        (constants.META_PREFIX, constants.DATAGRAM_PREFIX)
                    )
                ]
            )
        )
        arrow_table = arrow_utils.drop_system_columns(arrow_table)

        missing = set(tag_columns) - set(arrow_table.column_names)
        if missing:
            raise ValueError(f"TagProtocol column(s) not found in data: {missing}")

        if schema is not None:
            require_supported_nullable_types(schema)
            require_custom_system_tags_values(arrow_table, system_tag_columns)
            require_custom_system_tags(arrow_table, system_tag_columns, schema=schema)
            arrow_table = arrow_table.cast(schema)
        elif arrow_input_schema is not None:
            arrow_table = arrow_utils.restore_schema_nullability(
                arrow_table, arrow_input_schema
            )
        else:
            arrow_table = declare_native_custom_system_tags(
                arrow_table, system_tag_columns
            )
            require_supported_nullable_types(arrow_table.schema)
            # Native frames carry no Arrow nullability declarations.
            inferred = arrow_utils.infer_schema_nullable(
                arrow_table.drop_columns(list(system_tag_columns))
            )
            arrow_table = arrow_utils.restore_schema_nullability(arrow_table, inferred)

        super().__init__(source_id=source_id, **kwargs)
        self._key_columns = normalize_record_id_columns(record_id_columns) or ()
        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            arrow_table,
            tag_columns=tag_columns,
            data_tags=source_data_tags(tag_columns, tag_only_columns),
            source_name=self._source_name,
            kind_part=self.kind_part(),
            record_id_columns=self._key_columns,
            hash_in_key_order=hash_in_key_order,
            system_tag_columns=system_tag_columns,
        )

        self._stream = result.stream
        self._tag_columns = result.tag_columns
        self._tag_only_columns = tuple(
            c for c in result.tag_columns if c not in result.data_tags
        )
        if self._source_name is None:
            self._source_name = result.source_name

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize metadata-only config (DataFrame is not serializable)."""
        return {
            "source_type": self.SOURCE_KIND,
            "tag_columns": list(self._tag_columns),
            **self._tag_only_config(),
            **self._source_id_config(),
            **self._identity_config(),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry=None) -> DataFrameSource:
        """Not supported — DataFrameSource cannot be reconstructed from config.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "DataFrameSource cannot be reconstructed from config — "
            "the original DataFrame is not serializable."
        )
