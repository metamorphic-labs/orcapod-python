from __future__ import annotations

import logging
from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence, Sized
from itertools import chain
from typing import TYPE_CHECKING, Any, ClassVar

import polars as pl
import pyarrow as pa

from orcapod.core.sources.base import RootSource
from orcapod.core.data_tags import TagOnlyColumns, source_data_tags
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.core.sources.stream_builder import (
    normalize_record_id_columns,
    require_custom_system_tags,
    require_source_column_names,
    require_source_row_names,
    require_supported_nullable_types,
)
from orcapod.errors import InputValidationError
from orcapod.utils import arrow_utils, polars_data_utils
from orcapod.utils.schema_utils import _normalize_column_list

if TYPE_CHECKING:
    from polars._typing import FrameInitTypes

logger = logging.getLogger(__name__)


def _checked_frame_rows(
    rows: Iterator[Any], system_tag_columns: Collection[str]
) -> Iterator[Any]:
    """Inspect mapping rows at their original pace and stop on a reserved name."""
    try:
        first = next(rows)
    except StopIteration:
        return
    if not isinstance(first, Mapping):
        yield from chain((first,), rows)
        return
    for row in chain((first,), rows):
        if isinstance(row, Mapping):
            try:
                require_source_column_names(row.keys(), system_tag_columns)
            except InputValidationError:
                # Gather the remaining names for the diagnostic before inference
                # can drop the offending row's fields.
                try:
                    require_source_row_names(
                        (
                            item
                            for item in chain((row,), rows)
                            if isinstance(item, Mapping)
                        ),
                        system_tag_columns,
                    )
                except InputValidationError as error:
                    raise error from None
        yield row


def source_dataframe(
    data: FrameInitTypes, system_tag_columns: Collection[str] = ()
) -> pl.DataFrame:
    """Check mapping names before Polars infers a schema from a sample of rows.

    Args:
        data: Raw data accepted by the Polars DataFrame constructor.
        system_tag_columns: Columns nominated as custom system tags.

    Returns:
        The original input converted with Polars' normal inference.
    """
    if isinstance(data, Mapping):
        require_source_column_names(data.keys(), system_tag_columns)
    elif isinstance(data, Sequence):
        if data and isinstance(data[0], Mapping):
            require_source_row_names(data, system_tag_columns)
    elif (
        isinstance(data, Iterable)
        and not isinstance(data, Sized)
        and not hasattr(data, "__arrow_c_stream__")
    ):
        data = _checked_frame_rows(iter(data), system_tag_columns)
    return pl.DataFrame(data)


class DataFrameSource(RootSource):
    """A source backed by a Polars DataFrame (or any Polars-compatible data).

    The DataFrame is converted to an Arrow table and enriched by
    ``SourceStreamBuilder`` (source-info, schema-hash, system tags).

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
        data: FrameInitTypes,
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
        super().__init__(source_id=source_id, **kwargs)

        df = source_dataframe(data, system_tag_columns)
        require_source_column_names(df.columns, system_tag_columns)

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
            require_source_column_names(schema.names, system_tag_columns)
            require_custom_system_tags(arrow_table, system_tag_columns, schema=schema)
            arrow_table = arrow_table.cast(schema)
        else:
            # Polars defaults all fields to nullable=True; infer the correct
            # flags from the actual data before passing to the builder.
            require_supported_nullable_types(arrow_table.schema)
            arrow_table = arrow_table.cast(
                arrow_utils.infer_schema_nullable(arrow_table)
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
