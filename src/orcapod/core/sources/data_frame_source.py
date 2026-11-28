from __future__ import annotations

import logging
from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence, Sized
from dataclasses import fields, is_dataclass
from itertools import chain, repeat
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple

import polars as pl
import pyarrow as pa
from pydantic import BaseModel

from orcapod.core.sources.base import RootSource
from orcapod.core.data_tags import TagOnlyColumns, source_data_tags
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.core.sources.stream_builder import (
    normalize_record_id_columns,
    require_custom_system_tags,
    require_source_column_names,
    require_supported_nullable_types,
)
from orcapod.errors import InputValidationError
from orcapod.utils import arrow_utils, polars_data_utils
from orcapod.utils.schema_utils import _normalize_column_list, reserved_system_names

if TYPE_CHECKING:
    from polars._typing import FrameInitTypes

logger = logging.getLogger(__name__)


class _FrameRow(NamedTuple):
    """A row and its layout before Polars infers any columns."""

    value: Any
    names: tuple[str, ...]
    row_type: type
    non_column_names: tuple[str, ...] = ()

    @property
    def exposed_names(self) -> tuple[str, ...]:
        """Return declared and other field names exposed by the row."""
        return (*self.names, *self.non_column_names)


def _describe_model_row(value: BaseModel, index: int) -> _FrameRow:
    """Read the model's field names without running its serialization code."""
    model = type(value)
    declared = model.model_fields
    computed = model.model_computed_fields
    if not isinstance(computed, Mapping):
        # Older Pydantic releases exposed this on the instance.
        computed = value.model_computed_fields
    exposed = (
        set(computed)
        | {info.alias for info in computed.values() if info.alias is not None}
        | set(value.model_extra or ())
        | set(value.__pydantic_extra__ or ())
        | value.model_fields_set
        | vars(value).keys()
    )
    if model.model_config.get("serialize_by_alias"):
        exposed.update(
            field.serialization_alias
            for field in declared.values()
            if field.serialization_alias is not None
        )
    if not all(isinstance(name, str) for name in exposed):
        raise InputValidationError(f"Source row {index} needs string field names.")
    return _FrameRow(
        value, tuple(declared), model, tuple(sorted(exposed - declared.keys()))
    )


def _describe_frame_row(value: Any, index: int) -> _FrameRow | None:
    """Read top-level field names while preserving typed row values."""
    if value is None:
        return None
    if isinstance(value, Mapping):
        names = tuple(value)
        if not all(isinstance(name, str) for name in names):
            raise InputValidationError(f"Source row {index} needs string field names.")
        return _FrameRow(value if isinstance(value, dict) else dict(value), names, dict)
    if isinstance(value, BaseModel):
        return _describe_model_row(value, index)
    if is_dataclass(value) and not isinstance(value, type):
        return _FrameRow(
            value, tuple(field.name for field in fields(value)), type(value)
        )
    raise InputValidationError(
        f"Source row {index} ({type(value).__name__}) needs named fields. "
        "Use a mapping, dataclass or Pydantic model."
    )


def _checked_frame_rows(
    rows: Iterable[_FrameRow | None], system_tag_columns: Collection[str]
) -> Iterator[_FrameRow | None]:
    """Check every named layout before yielding the row to Polars."""
    first: _FrameRow | None = None
    expected: set[str] = set()
    for index, row in enumerate(rows):
        if row is not None:
            try:
                require_source_column_names(row.exposed_names, system_tag_columns)
            except InputValidationError as exc:
                raise InputValidationError(f"Source row {index}. {exc}") from exc
            if row.non_column_names:
                raise InputValidationError(
                    f"Source row {index} exposes extra or computed model fields "
                    f"{list(row.non_column_names)!r} outside its declared columns. "
                    "Use a mapping or a model without computed or extra fields."
                )
            if isinstance(row.value, BaseModel):
                model = type(row.value)
                if (
                    model.model_dump is not BaseModel.model_dump
                    or model.__pydantic_decorators__.model_serializers
                ):
                    raise InputValidationError(
                        f"Source row {index} has serialized field names that cannot be "
                        "verified from its declared model fields. Use a mapping."
                    )
            names = set(row.names)
            if first is None:
                first, expected = row, names
            if names != expected:
                raise InputValidationError(
                    f"Source row {index} has different field names. "
                    f"Missing {sorted(expected - names)!r}, extra {sorted(names - expected)!r}."
                )
            if row.row_type is not first.row_type or (
                row.row_type is not dict and row.names != first.names
            ):
                raise InputValidationError(
                    f"Source row {index} has a different layout "
                    f"({row.row_type.__name__} fields {row.names!r}). "
                    f"Use the same row layout ({first.row_type.__name__} fields "
                    f"{first.names!r}) or use mappings throughout."
                )
        yield row


def _require_materialized_row_names(
    rows: Sequence[_FrameRow | None], system_tag_columns: Collection[str]
) -> None:
    """Report all reserved names and the first row exposing one."""
    names = {name for row in rows if row is not None for name in row.exposed_names}
    try:
        require_source_column_names(names, system_tag_columns)
    except InputValidationError as exc:
        reserved = reserved_system_names(names) - set(system_tag_columns)
        first = next(
            index
            for index, row in enumerate(rows)
            if row is not None and not reserved.isdisjoint(row.exposed_names)
        )
        raise InputValidationError(f"Source row {first}. {exc}") from exc


def _frame_from_checked_rows(
    rows: Iterator[_FrameRow | None], *, materialized: bool
) -> pl.DataFrame:
    """Keep Polars' typed conversion for a single verified layout."""
    leading_nulls = 0
    for first in rows:
        if first is not None:
            break
        leading_nulls += 1
    else:
        return pl.DataFrame([None] * leading_nulls)

    all_rows = chain(repeat(None, leading_nulls), (first,), rows)
    if first.row_type is dict:
        values = (row.value if row is not None else None for row in all_rows)
        return pl.DataFrame(list(values) if materialized else values)

    null_positions: set[int] = set()

    def typed_values() -> Iterator[Any]:
        for index, row in enumerate(all_rows):
            if row is None:
                null_positions.add(index)
            else:
                yield row.value

    typed = typed_values()
    frame = pl.DataFrame(list(typed) if materialized else typed)
    if tuple(frame.columns) != first.names:
        raise InputValidationError(
            f"Source row {leading_nulls} declares fields {first.names!r}, "
            f"but Polars returned {frame.columns!r}. Use mappings for these rows."
        )
    if null_positions:
        indices = iter(range(frame.height))
        positions = [
            None if index in null_positions else next(indices)
            for index in range(frame.height + len(null_positions))
        ]
        frame = frame.select(
            pl.all().gather(pl.Series(positions, dtype=pl.get_index_type()))
        )
    return frame


def source_dataframe(
    data: FrameInitTypes, system_tag_columns: Collection[str] = ()
) -> pl.DataFrame:
    """Validate named row layouts before Polars can project or relabel fields.

    Args:
        data: Raw data accepted by the Polars DataFrame constructor.
        system_tag_columns: Columns nominated as custom system tags.

    Returns:
        The validated input converted with Polars' normal inference.
    """
    if isinstance(data, Mapping):
        require_source_column_names(data.keys(), system_tag_columns)
        return pl.DataFrame(data)
    elif isinstance(data, Sequence):
        if all(isinstance(column, pl.Series) for column in data):
            return pl.DataFrame(data)
        rows = [_describe_frame_row(row, index) for index, row in enumerate(data)]
        _require_materialized_row_names(rows, system_tag_columns)
        return _frame_from_checked_rows(
            _checked_frame_rows(rows, system_tag_columns), materialized=True
        )
    elif (
        isinstance(data, Iterable)
        and not isinstance(data, Sized)
        and not hasattr(data, "__arrow_c_stream__")
    ):
        return _frame_from_checked_rows(
            _checked_frame_rows(
                (_describe_frame_row(row, index) for index, row in enumerate(data)),
                system_tag_columns,
            ),
            materialized=False,
        )
    return pl.DataFrame(data)


class DataFrameSource(RootSource):
    """A source backed by a Polars DataFrame (or any Polars-compatible data).

    Python rows must be mappings with the same keys, or dataclass or Pydantic
    rows of one class with the same declared fields. Put extra or computed
    Pydantic fields in mappings, and use mappings for models with custom
    serialization. A ``None`` row keeps its position as a null row.

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
