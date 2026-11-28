from __future__ import annotations

import logging
from collections.abc import Collection
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
    require_supported_nullable_types,
)
from orcapod.utils import arrow_utils, polars_data_utils
from orcapod.utils.schema_utils import _normalize_column_list

if TYPE_CHECKING:
    from polars._typing import ArrowArrayExportable, ArrowStreamExportable

logger = logging.getLogger(__name__)


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
        super().__init__(source_id=source_id, **kwargs)

        arrow_system_tag_flags: dict[str, bool] = {}
        # Keep pandas and Polars on their native paths even if they export Arrow.
        # For Arrow inputs, materialize once before Polars drops field metadata.
        if not isinstance(data, (pd.DataFrame, pl.DataFrame)):
            # Like Polars, prefer the array export when both are available.
            if hasattr(data, "__arrow_c_array__"):
                batch = pa.record_batch(cast("ArrowArrayExportable", data))
                arrow_input = pa.Table.from_batches([batch])
            else:
                arrow_input = pa.table(cast("ArrowStreamExportable", data))
            require_custom_system_tags(arrow_input, system_tag_columns)
            arrow_system_tag_flags = {
                name: arrow_input.schema.field(name).nullable
                for name in system_tag_columns
            }
            # Polars rechunks native tables but preserves chunks on exporter inputs.
            df = cast(
                pl.DataFrame,
                pl.from_arrow(arrow_input, rechunk=isinstance(data, pa.Table)),
            )
        else:
            df = pl.DataFrame(data)

        # Convert any Object-dtype columns to Arrow-compatible types.
        object_columns = [c for c in df.columns if df[c].dtype == pl.Object]
        if object_columns:
            logger.info(
                f"Converting {len(object_columns)} object column(s) to Arrow format"
            )
            sub_table = self.data_context.type_converter.python_dicts_to_arrow_table(
                df.select(object_columns).to_dicts()
            )
            df = df.with_columns([pl.from_arrow(c) for c in sub_table])

        tag_columns = _normalize_column_list(tag_columns)

        require_supported_nullable_types(
            polars_data_utils.get_system_columns(df).to_arrow().schema
        )
        df = polars_data_utils.drop_system_columns(df)

        missing = set(tag_columns) - set(df.columns)
        if missing:
            raise ValueError(f"TagProtocol column(s) not found in data: {missing}")

        arrow_table = df.to_arrow()
        if schema is not None:
            require_supported_nullable_types(schema)
            require_custom_system_tags(arrow_table, system_tag_columns, schema=schema)
            arrow_table = arrow_table.cast(schema)
        else:
            if not arrow_system_tag_flags:
                arrow_table = declare_native_custom_system_tags(
                    arrow_table, system_tag_columns
                )
                arrow_system_tag_flags = {name: False for name in system_tag_columns}
            # Polars defaults all fields to nullable=True; infer the correct
            # flags from the actual data before passing to the builder.
            require_supported_nullable_types(arrow_table.schema)
            inferred = arrow_utils.infer_schema_nullable(
                arrow_table.drop_columns(list(arrow_system_tag_flags))
            )
            arrow_table = arrow_table.cast(
                pa.schema(
                    [
                        field.with_nullable(arrow_system_tag_flags[field.name])
                        if field.name in arrow_system_tag_flags
                        else inferred.field(field.name)
                        for field in arrow_table.schema
                    ]
                )
            )

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
