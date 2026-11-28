from __future__ import annotations

import logging
import math
from collections.abc import (
    Callable,
    Collection,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
    Sized,
)
from dataclasses import fields, is_dataclass
from enum import Enum
from itertools import chain, repeat
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NamedTuple, cast

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
from pydantic import BaseModel
from typing_extensions import override

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
    kind: Literal["mapping", "model", "dataclass", "namedtuple", "series", "positional"]
    additional_names: tuple[str, ...] = ()

    @property
    def exposed_names(self) -> tuple[str, ...]:
        """Return declared and other field names exposed by the row."""
        return (*self.names, *self.additional_names)


class _FrameModelRow(BaseModel):
    """Present the original Pydantic layout and check any dump Polars uses.

    Polars reads declared field values directly for flat models and calls
    `model_dump` when it unpacks nested models. Forward both paths so validation
    keeps Polars' original type inference and does not run a serializer twice.
    """

    __slots__: ClassVar[tuple[str, ...]] = (
        "_original",
        "_names",
        "_system_tag_columns",
        "_dumped_fields",
    )
    if TYPE_CHECKING:
        _original: BaseModel
        _names: set[str]
        _system_tag_columns: Collection[str]
        _dumped_fields: dict[str, object] | None

    @classmethod
    def from_model(
        cls,
        value: BaseModel,
        names: tuple[str, ...],
        system_tag_columns: Collection[str],
    ) -> _FrameModelRow:
        """Wrap a model that has already been constructed and validated."""
        row = cls.model_construct()
        object.__setattr__(row, "_original", value)
        object.__setattr__(row, "_names", set(names))
        object.__setattr__(row, "_system_tag_columns", system_tag_columns)
        object.__setattr__(row, "_dumped_fields", None)
        return row

    @override
    def __getattribute__(self, name: str) -> Any:
        if name in ("__class__", "__dict__"):
            return getattr(self._original, name)
        return super().__getattribute__(name)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._original, name)

    @override
    def model_dump(self, **kwargs: object) -> dict[str, object]:
        """Validate the serialized field names before they reach inference."""
        dump = cast("Callable[..., object]", self._original.model_dump)
        values = dump(**kwargs)
        if not isinstance(values, dict):
            raise InputValidationError(
                "A serialized model row must return a dict with string field names."
            )
        fields = cast("dict[object, object]", values)
        names = {name for name in fields if isinstance(name, str)}
        if len(names) != len(fields):
            raise InputValidationError(
                "A serialized model row needs string field names."
            )
        require_source_column_names(names, self._system_tag_columns)
        if names != self._names:
            raise InputValidationError(
                f"Serialized model row has different fields. Missing "
                f"{sorted(self._names - names)!r}, extra "
                f"{sorted(names - self._names)!r}. Use a mapping."
            )
        result = cast("dict[str, object]", fields)
        object.__setattr__(self, "_dumped_fields", result)
        return result

    def _frame_fields(self) -> Mapping[str, object]:
        """Return the fields Polars read, including any serializer changes."""
        if self._dumped_fields is not None:
            return self._dumped_fields
        return vars(self._original)


def _describe_model_row(value: BaseModel) -> _FrameRow:
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
    aliases = {
        field.serialization_alias
        for name, field in declared.items()
        if model.model_config.get("serialize_by_alias")
        and field.serialization_alias is not None
        and field.serialization_alias != name
    }
    if not all(isinstance(name, str) for name in exposed):
        raise InputValidationError("needs string field names.")
    if aliases & declared.keys():
        require_source_column_names(aliases)
        raise InputValidationError(
            f"has serialization aliases {sorted(aliases)!r} that rename existing "
            "model fields. Use a mapping."
        )
    return _FrameRow(
        value,
        tuple(declared),
        "model",
        tuple(sorted((exposed - declared.keys()) | aliases)),
    )


def _describe_frame_row(value: Any) -> _FrameRow | None:
    """Read top-level field names while preserving typed row values."""
    if value is None:
        return None
    if isinstance(value, Mapping):
        names = tuple(value)
        if not all(isinstance(name, str) for name in names):
            raise InputValidationError("needs string field names.")
        return _FrameRow(
            value if isinstance(value, dict) else dict(value), names, "mapping"
        )
    if isinstance(value, BaseModel):
        return _describe_model_row(value)
    if is_dataclass(value) and not isinstance(value, type):
        return _FrameRow(
            value, tuple(field.name for field in fields(value)), "dataclass"
        )
    if isinstance(value, Sequence):
        names = getattr(value, "_fields", None)
        if isinstance(names, tuple) and all(isinstance(name, str) for name in names):
            return _FrameRow(value, names, "namedtuple")
    if isinstance(value, (pd.Series, pd.Index, pl.Series)):
        name = str(value.name) if value.name is not None else ""
        return _FrameRow(value, (name,) if name else (), "series")
    return _FrameRow(value, (), "positional")


def _describe_frame_rows(values: Iterable[Any]) -> Iterator[_FrameRow | None]:
    """Attach the input row's index to an error from its descriptor."""
    for index, value in enumerate(values):
        try:
            yield _describe_frame_row(value)
        except InputValidationError as exc:
            raise InputValidationError(f"Source row {index} {exc}") from exc


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
            names = set(row.exposed_names)
            if first is None:
                first, expected = row, names
            positional_tail = (
                first.kind == "namedtuple"
                and row.kind == "positional"
                and isinstance(row.value, (tuple, list, np.ndarray))
                and (
                    not isinstance(row.value, np.ndarray)
                    or (row.value.ndim == 1 and row.value.dtype.names is None)
                )
                and len(row.value) == len(first.names)
            )
            if positional_tail:
                yield row
                continue
            if names != expected:
                raise InputValidationError(
                    f"Source row {index} has different field names. "
                    f"Missing {sorted(expected - names)!r}, extra {sorted(names - expected)!r}. "
                    "Use the same field names in every row. For mappings, add missing "
                    "fields with None where appropriate and remove unintended fields."
                )
            if row.kind != first.kind or (
                row.kind == "namedtuple" and row.names != first.names
            ):
                raise InputValidationError(
                    f"Source row {index} has a different layout "
                    f"({type(row.value).__name__} fields {row.names!r}). "
                    f"Use the same row layout ({type(first.value).__name__} fields "
                    f"{first.names!r}) or use mappings throughout."
                )
        yield row


def _same_frame_value(supplied: Any, converted: Any) -> bool:
    """Compare converted values without rounding mixed numeric inputs."""
    if supplied is converted:
        return True
    if isinstance(supplied, np.generic):
        supplied = supplied.item()
    if isinstance(supplied, Enum):
        supplied = supplied.value
    if is_dataclass(supplied) and not isinstance(supplied, type):
        supplied = {
            field.name: getattr(supplied, field.name) for field in fields(supplied)
        }
    if isinstance(supplied, BaseModel):
        supplied = vars(supplied)
    if isinstance(supplied, Mapping) and isinstance(converted, Mapping):
        return all(
            _same_frame_value(value, converted.get(name)) and name in converted
            for name, value in supplied.items()
        ) and all(
            name in supplied or value is None for name, value in converted.items()
        )
    if isinstance(supplied, (list, tuple, np.ndarray)) and isinstance(converted, list):
        return len(supplied) == len(converted) and all(
            _same_frame_value(left, right) for left, right in zip(supplied, converted)
        )
    if isinstance(supplied, float):
        if math.isnan(supplied):
            return isinstance(converted, float) and math.isnan(converted)
        if supplied == 0 and math.copysign(1, supplied) < 0:
            return (
                isinstance(converted, float)
                and math.copysign(1, converted) < 0
                and converted == 0
            )
    equal = supplied == converted
    return isinstance(equal, (bool, np.bool_)) and bool(equal)


def _require_exact_mixed_rows(
    frame: pl.DataFrame,
    rows: list[tuple[int, _FrameRow, Any]],
) -> None:
    """Check a mixed batch against the values each input column supplied."""
    _, first, _ = rows[0]
    if all(type(row.value) is type(first.value) for _, row, _ in rows):
        return
    names = frame.columns
    values = [
        column.to_list() if column.dtype == pl.Object else column.to_arrow().to_pylist()
        for column in frame
    ]
    for (index, row, value), converted in zip(rows, zip(*values), strict=True):
        if isinstance(value, _FrameModelRow):
            supplied = value._frame_fields()
        elif row.kind == "dataclass":
            supplied = {name: getattr(value, name) for name in row.names}
        else:
            supplied = dict(zip(first.names, value, strict=True))
        for name, item in zip(names, converted, strict=True):
            if _same_frame_value(supplied[name], item):
                continue
            if row.kind == "dataclass" and row.names != first.names:
                raise InputValidationError(
                    f"Source row {index} has a different layout ({row.names!r}) "
                    f"from the first row ({first.names!r}), and conversion changed "
                    f"column {name!r}. Use mappings for these rows."
                )
            raise InputValidationError(
                f"Source row {index} column {name!r} could not be converted "
                "without losing its value. Use values that convert exactly to one type."
            )


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


def _frame_from_rows(
    data: Iterable[Any], system_tag_columns: Collection[str], *, materialized: bool
) -> pl.DataFrame:
    """Use the regular Polars conversion after checking row or column names."""
    rows = _describe_frame_rows(data)
    if materialized:
        described = list(rows)
        _require_materialized_row_names(described, system_tag_columns)
        rows = iter(described)

    leading_nulls = 0
    for first in rows:
        if first is not None:
            break
        leading_nulls += 1
    else:
        return pl.DataFrame([None] * leading_nulls)

    all_rows = chain(repeat(None, leading_nulls), (first,), rows)
    if first.kind == "series" or (materialized and isinstance(first.value, np.ndarray)):
        return _frame_from_columns(
            all_rows, system_tag_columns, materialized=materialized
        )

    checked = _checked_frame_rows(all_rows, system_tag_columns)
    if first.kind in ("mapping", "positional"):
        values = (row.value if row is not None else None for row in checked)
        return pl.DataFrame(list(values) if materialized else values)

    null_positions: set[int] = set()
    typed_rows: list[tuple[int, _FrameRow, Any]] = []

    def typed_values() -> Iterator[Any]:
        for index, row in enumerate(checked):
            if row is None:
                null_positions.add(index)
                continue
            value = row.value
            if isinstance(value, BaseModel):
                value = _FrameModelRow.from_model(
                    row.value, row.exposed_names, system_tag_columns
                )
            typed_rows.append((index, row, value))
            yield value

    typed = typed_values()
    frame = pl.DataFrame(list(typed) if materialized else typed)
    if first.kind == "model":
        expected = set(first.exposed_names)
        if set(frame.columns) != expected:
            raise InputValidationError(
                f"Source row {leading_nulls} exposes model fields {sorted(expected)!r}, "
                f"but Polars returned {frame.columns!r}. Use a mapping."
            )
    elif tuple(frame.columns) != first.names:
        raise InputValidationError(
            f"Source row {leading_nulls} declares fields {first.names!r}, "
            f"but Polars returned {frame.columns!r}. Use mappings for these rows."
        )
    _require_exact_mixed_rows(frame, typed_rows)
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


def _frame_from_columns(
    columns: Iterable[_FrameRow | None],
    system_tag_columns: Collection[str],
    *,
    materialized: bool,
) -> pl.DataFrame:
    """Check named columns and refuse named rows in a column-oriented sequence."""
    named_series: dict[int, str] = {}

    def checked_columns() -> Iterator[Any]:
        for index, column in enumerate(columns):
            if column is None:
                yield None
                continue
            if not materialized:
                require_source_column_names(column.exposed_names, system_tag_columns)
            if column.kind == "series" and column.names:
                named_series[index] = column.names[0]
            elif column.kind not in ("positional", "series"):
                raise InputValidationError(
                    f"Source row {index} declares fields {column.names!r} in a "
                    "column-oriented sequence. Pass named rows separately or use named columns."
                )
            yield column.value

    values = checked_columns()
    frame = pl.DataFrame(list(values) if materialized else values)
    for index, name in named_series.items():
        if index >= frame.width or name != frame.columns[index]:
            raise InputValidationError(
                f"Input column {index} is named {name!r}, but Polars "
                f"returned columns {frame.columns!r}. Use named columns."
            )
    return frame


def source_dataframe(
    data: FrameInitTypes, system_tag_columns: Collection[str] = ()
) -> pl.DataFrame:
    """Convert a polling batch, validating layouts before Polars infers columns.

    Args:
        data: Tabular or column input, or Python rows with a consistent named layout.
        system_tag_columns: Columns nominated as custom system tags.

    Returns:
        The validated input converted with Polars' normal inference.
    """
    if isinstance(data, pd.DataFrame):
        require_source_column_names(
            [str(name) for name in data.columns], system_tag_columns
        )
    elif isinstance(data, (pa.Table, pa.RecordBatch)):
        require_source_column_names(data.schema.names, system_tag_columns)
    elif isinstance(data, np.ndarray):
        require_source_column_names(data.dtype.names or (), system_tag_columns)
    elif isinstance(data, Mapping):
        require_source_column_names(data.keys(), system_tag_columns)
    elif isinstance(data, Sequence):
        return _frame_from_rows(data, system_tag_columns, materialized=True)
    elif (
        isinstance(data, Iterable)
        and not isinstance(data, Sized)
        and not hasattr(data, "__arrow_c_stream__")
    ):
        return _frame_from_rows(data, system_tag_columns, materialized=False)
    return pl.DataFrame(data)


class DataFrameSource(RootSource):
    """A source backed by a Polars DataFrame or other Polars-compatible data.

    The DataFrame is converted to an Arrow table and enriched by
    ``SourceStreamBuilder`` (source-info, schema-hash, system tags).
    Column names beginning with an underscore are reserved. For Python row
    inputs, the check applies to the columns inferred by Polars.

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

        if schema is not None:
            require_source_column_names(schema.names, system_tag_columns)
        if isinstance(data, pd.DataFrame):
            require_source_column_names(
                [str(name) for name in data.columns], system_tag_columns
            )
        elif isinstance(data, (pa.Table, pa.RecordBatch)):
            require_source_column_names(data.schema.names, system_tag_columns)
        df = pl.DataFrame(data)
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
