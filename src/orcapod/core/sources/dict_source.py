from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import Any, ClassVar

import pyarrow as pa

from orcapod.core.sources.base import RootSource
from orcapod.core.data_tags import TagOnlyColumns, source_data_tags
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.core.sources.stream_builder import (
    normalize_record_id_columns,
    declare_native_custom_system_tags,
    require_supported_nullable_types,
)
from orcapod.types import DataValue, SchemaLike
from orcapod.utils import arrow_utils
from orcapod.utils.schema_utils import _normalize_column_list
from orcapod.utils.tensor_conversions import warn_source_tensor_conversions


class DictSource(RootSource):
    """A source backed by a collection of Python dictionaries.

    Each dict becomes one (tag, data) pair in the stream. The dicts are
    converted to an Arrow table via the data-context type converter, then
    enriched by ``SourceStreamBuilder`` (source-info, schema-hash, system tags).

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
    SOURCE_KIND: ClassVar[str] = "dict"
    # The version of this kind's implementation, in the kind part (ITL-1425).
    SOURCE_VERSION: ClassVar[int] = 1
    TABLE_IS_ROWS: ClassVar[bool] = True

    def __init__(
        self,
        data: Collection[Mapping[str, DataValue]],
        tag_columns: str | Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        data_schema: SchemaLike | pa.Schema | None = None,
        source_id: str | None = None,
        *,
        tag_only_columns: TagOnlyColumns = (),
        record_id_columns: str | Collection[str] | None = None,
        hash_in_key_order: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id=source_id, **kwargs)
        tag_columns = _normalize_column_list(tag_columns)

        # Accept either a Python SchemaLike (Mapping[str, type]) or a
        # pa.Schema directly via data_schema; route to the converter accordingly.
        rows = [dict(row) for row in data]
        if isinstance(data_schema, pa.Schema):
            require_supported_nullable_types(data_schema)
            arrow_table = self.data_context.type_converter.python_dicts_to_arrow_table(
                rows,
                arrow_schema=data_schema,
            )
        else:
            arrow_table = self.data_context.type_converter.python_dicts_to_arrow_table(
                rows,
                python_schema=data_schema,
            )
            if data_schema is None:
                arrow_table = declare_native_custom_system_tags(
                    arrow_table, system_tag_columns
                )
                # No explicit schema — infer nullable from actual values.
                # The type converter defaults all fields to nullable=True; derive
                # the correct flags here so the builder can trust the schema as-is.
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
        warn_source_tensor_conversions(self.source_id, rows)

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize metadata-only config (data is not serializable)."""
        return {
            "source_type": self.SOURCE_KIND,
            "tag_columns": list(self._tag_columns),
            **self._tag_only_config(),
            **self._source_id_config(),
            **self._identity_config(),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry=None) -> DictSource:
        """Not supported — DictSource data cannot be reconstructed from config.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "DictSource cannot be reconstructed from config — "
            "original data is not serializable."
        )
