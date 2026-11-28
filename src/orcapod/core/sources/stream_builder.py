"""Compositional builder for enriching raw Arrow tables into source streams.

Extracts the enrichment pipeline that was previously embedded in
``ArrowTableSource.__init__``: dropping system columns, validating tags,
computing schema/table hashes, adding source-info provenance, adding system
tag columns, and wrapping the result in an ``ArrowTableStream``.
"""

from __future__ import annotations

import datetime
import json
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa

from orcapod.core.sources.source_ids import (
    build_source_id,
    hash_source_name,
    parse_kind_part,
    require_source_name,
)
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.errors import InputValidationError, RowIdentifierError
from orcapod.logical_types.identifier import LogicalTypeIdentifierError
from orcapod.system_constants import constants
from orcapod.types import ContentHash
from orcapod.utils import arrow_utils, schema_utils
from orcapod.utils.schema_utils import compute_source_schema_hash

if TYPE_CHECKING:
    from orcapod.config import OrcapodConfig
    from orcapod.contexts import DataContext
    from orcapod.protocols.hashing_protocols import ArrowHasherProtocol


def require_supported_nullable_types(schema: pa.Schema) -> None:
    """Refuse source columns containing the Arrow Null type, even with zero rows."""
    columns = sorted(
        (field.name, str(field.type))
        for field in schema
        if arrow_utils.contains_null_type(field.type)
    )
    if columns:
        names = ", ".join(f"{name!r} ({arrow_type})" for name, arrow_type in columns)
        raise InputValidationError(
            f"Source column(s) {names} contain Arrow type null. "
            "Please declare a supported nullable type."
        )


def require_system_tag_without_field_metadata(
    field: pa.Field[Any], *, stored_cache: bool = False
) -> None:
    """Refuse field metadata on a system tag before it can reach source rows."""
    # Stopgap (ITL-1561): re-evaluate what metadata a system-tag field may carry.
    # Extension metadata reaches the row hash now that rows use stream fields.
    if not field.metadata:
        return
    if stored_cache:
        raise InputValidationError(
            f"System-tag column {field.name!r}: the source cache was written with "
            "Arrow field metadata on a system tag. Rebuild the cache."
        )
    raise InputValidationError(
        f"Custom system-tag column {field.name!r} has Arrow field metadata. "
        "Remove the field metadata before building the source."
    )


def validate_source_table(table: pa.Table, *, stored_cache: bool = False) -> pa.Table:
    """Validate an enriched table presented by a source without the source builder.

    Args:
        table: Source fields and values, with system tags already named as such.
        stored_cache: Report the rebuild remedy for a persisted source cache.

    Returns:
        The same table, with fields and values unchanged.
    """
    require_supported_nullable_types(table.schema)
    require_custom_system_tags(
        table,
        [
            name
            for name in table.column_names
            if name.startswith(constants.SYSTEM_TAG_PREFIX)
        ],
        stored_cache=stored_cache,
    )
    return table


def require_custom_system_tags(
    table: pa.Table,
    names: Collection[str],
    *,
    schema: pa.Schema | None = None,
    stored_cache: bool = False,
) -> None:
    """Validate custom system-tag declarations and values.

    Args:
        table: The source's Arrow columns.
        names: The columns nominated as system tags.
        schema: The declared fields if the reader has yet to apply them.
        stored_cache: Report the rebuild remedy for a persisted source cache.

    Raises:
        ValueError: If a nominated column is missing from the source table.
        InputValidationError: If a nominated column has field metadata, can
            hold nulls or is not text.
    """
    if not names:
        return
    fields = table.schema if schema is None else schema
    available = set(fields.names).intersection(table.column_names)
    if missing := sorted(set(names) - available):
        raise ValueError(
            f"system_tag_columns not found in table: {missing}. "
            f"Available columns: {sorted(available)}"
        )
    for name in names:
        field = fields.field(name)
        require_system_tag_without_field_metadata(field, stored_cache=stored_cache)
        if schema is not None:
            require_system_tag_without_field_metadata(
                table.schema.field(name), stored_cache=stored_cache
            )
        has_nulls = table.column(name).null_count > 0
        if (
            field.nullable
            or has_nulls
            or not (
                pa.types.is_string(field.type)
                or pa.types.is_large_string(field.type)
                or pa.types.is_string_view(field.type)
            )
        ):
            raise InputValidationError(
                f"Custom system-tag column {name!r} has type {field.type} "
                f"({'nullable' if field.nullable else 'non-nullable'}"
                f"{', contains null values' if has_nulls else ''}). "
                "Declare a non-nullable string column with no null values "
                "and convert its values to text first."
            )


def declare_native_custom_system_tags(
    table: pa.Table, names: Collection[str]
) -> pa.Table:
    """Declare native custom system tags as required large strings.

    Args:
        table: A table inferred from native Python values.
        names: The columns nominated as custom system tags.

    Returns:
        The table with nominated fields declared as non-nullable large strings.

    Raises:
        InputValidationError: If a nominated column is not text or contains nulls.
    """
    if not names:
        return table
    require_custom_system_tags_values(table, names)
    schema = pa.schema(
        [
            pa.field(
                field.name,
                pa.large_string(),
                nullable=False,
                metadata=field.metadata,
            )
            if field.name in names
            else field
            for field in table.schema
        ],
        metadata=cast(dict[bytes | str, bytes | str] | None, table.schema.metadata),
    )
    return table.cast(schema)


def require_custom_system_tags_values(table: pa.Table, names: Collection[str]) -> None:
    """Validate values and types before declaring native custom system tags.

    Args:
        table: A table inferred from native Python values.
        names: The columns nominated as custom system tags.

    Raises:
        ValueError: If a nominated column is missing.
        InputValidationError: If a nominated column is not text or contains nulls.
    """
    if missing := sorted(set(names) - set(table.column_names)):
        raise ValueError(
            f"system_tag_columns not found in table: {missing}. "
            f"Available columns: {sorted(table.column_names)}"
        )
    for name in names:
        field = table.schema.field(name)
        require_system_tag_without_field_metadata(field)
        has_nulls = table.column(name).null_count > 0
        if has_nulls or not (
            pa.types.is_string(field.type)
            or pa.types.is_large_string(field.type)
            or pa.types.is_string_view(field.type)
        ):
            raise InputValidationError(
                f"Custom system-tag column {name!r} has type {field.type} "
                f"({'contains null values' if has_nulls else 'non-text values'}). "
                "Declare a non-nullable string column with no null values "
                "and convert its values to text first."
            )


def normalize_record_id_columns(
    record_id_columns: str | Collection[str] | None,
) -> tuple[str, ...] | None:
    """Normalize a source's ``record_id_columns`` argument (ITL-1154).

    Args:
        record_id_columns: ``None`` for the source's default (the primary key of a
            database table, the position otherwise), a column name or several key
            columns in key order, or ``()`` for the position.

    Returns:
        ``None``, or the key columns as a tuple (empty for the position).

    Raises:
        TypeError: If several key columns are given in a collection without an
            order (a set or a mapping): the key order is part of every row
            identifier, so it must be stated.
    """
    if record_id_columns is None:
        return None
    if isinstance(record_id_columns, str):
        return (record_id_columns,)
    if not isinstance(record_id_columns, Sequence):
        raise TypeError(
            "record_id_columns must list the key columns in key order (a list or "
            f"a tuple), not a {type(record_id_columns).__name__}: the order is part "
            "of every row identifier."
        )
    return tuple(record_id_columns)


def positional_row_identifier(row_index: int) -> str:
    """Return the row identifier of a row named by its position: ``row_<n>``.

    The identifier names a row position, not a row: rows supplied in another order
    under the same source id get other identifiers. Name key columns
    (``record_id_columns``) when row order is not fixed.

    Args:
        row_index: The row's position, from 0.

    Returns:
        ``"row_<n>"``.
    """
    return f"row_{row_index}"


_FRACTION_DIGITS = {"s": 0, "ms": 3, "us": 6, "ns": 9}
"""Fractional-second digits of a timestamp key value, fixed by the column's unit."""


def _timestamp_text(value: int, unit: str, *, utc: bool) -> str:
    """Return a timestamp key value as ISO-8601 text, from its stored integer.

    Formatted from the integer, never from a float or a library object, so it is
    exact at every unit. A time-zone-aware timestamp is written in UTC with ``Z``; a
    naive one without an offset, so the two never share a form.

    Args:
        value: The stored integer: units since the epoch (UTC for a zoned column).
        unit: The column's unit, ``s``, ``ms``, ``us`` or ``ns``.
        utc: Whether the column has a time zone.

    Returns:
        ``YYYY-MM-DDTHH:MM:SS[.fraction][Z]``, the year always four digits.

    Raises:
        OverflowError: If the moment lies outside years 1 to 9999.
    """
    digits = _FRACTION_DIGITS[unit]
    seconds, fraction = divmod(value, 10**digits)
    moment = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=seconds)
    # ``isoformat`` always writes four year digits; ``strftime("%Y")`` does not on
    # every platform.
    text = moment.isoformat(timespec="seconds")
    if digits:
        text += "." + str(fraction).zfill(digits)
    return text + ("Z" if utc else "")


def _key_values(column: "pa.ChunkedArray[Any]") -> list[object]:
    """Return a key column's values as JSON values (ITL-1154).

    Strings, integers and booleans as they are. A timestamp is ISO-8601 text
    (``_timestamp_text``) and a date ``YYYY-MM-DD``, both formatted from the stored
    integers. A null stays ``None`` (``row_identifiers`` refuses it).

    Args:
        column: A key column of an allowed type (``key_column_type_allowed``).

    Returns:
        One value per row.

    Raises:
        OverflowError: If a timestamp or date lies outside years 1 to 9999, which
            Arrow can store and ISO-8601 text here cannot write.
    """
    arrow_type = column.type
    if pa.types.is_timestamp(arrow_type):
        unit, utc = arrow_type.unit, arrow_type.tz is not None
        ints = column.cast(pa.int64()).to_pylist()
        return [None if v is None else _timestamp_text(v, unit, utc=utc) for v in ints]
    if pa.types.is_date32(arrow_type):
        days = column.cast(pa.int32()).to_pylist()
        epoch = datetime.date(1970, 1, 1)
        return [
            None if d is None else (epoch + datetime.timedelta(days=d)).isoformat()
            for d in days
        ]
    if pa.types.is_date64(arrow_type):
        millis = column.cast(pa.int64()).to_pylist()
        epoch = datetime.date(1970, 1, 1)
        return [
            None
            if m is None
            else (epoch + datetime.timedelta(milliseconds=m)).isoformat()
            for m in millis
        ]
    return cast("list[object]", column.to_pylist())


def key_row_identifier(key: Sequence[tuple[str, object]]) -> str:
    """Return the row identifier of a row named by its key columns.

    The canonical JSON object of the key columns in the declared key order, with no
    spaces: ``{"unit":"u1","time":1700000000}``. It handles composite keys and keeps
    value types apart (``17`` and ``"17"`` differ). This is the one definition of the
    form (ITL-1154).

    Args:
        key: ``(column, value)`` pairs in the declared key order; each value a
            ``str``, ``int`` or ``bool`` (``key_columns_problem`` checks the types).

    Returns:
        The JSON text.
    """
    return json.dumps(dict(key), separators=(",", ":"), ensure_ascii=False)


def key_column_type_allowed(arrow_type: pa.DataType) -> bool:
    """Whether a column of ``arrow_type`` may be a key column (ITL-1154).

    Allowed: strings, integers, booleans, timestamps and dates (written as ISO-8601
    text, ``_key_values``). Everything else (floats, bytes, times of day, durations,
    UUIDs, containers, logical types) is refused; more types can be added later
    without moving an existing identifier.

    Args:
        arrow_type: The column's Arrow type.

    Returns:
        True for a string, integer or boolean type.
    """
    return (
        pa.types.is_string(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_integer(arrow_type)
        or pa.types.is_boolean(arrow_type)
        or pa.types.is_timestamp(arrow_type)
        or pa.types.is_date32(arrow_type)
        or pa.types.is_date64(arrow_type)
    )


def row_identifiers(table: pa.Table, record_id_columns: Sequence[str]) -> list[str]:
    """Return each row's identifier: its key, or its position when no key is named.

    Args:
        table: The source's rows.
        record_id_columns: The key columns, in key order; empty for position.

    Returns:
        One identifier per row, in row order.

    Raises:
        RowIdentifierError: If a key column is missing or of a type that is not allowed, a
            key value is null, a timestamp or date key lies outside years 1 to 9999,
            or two rows have the same key. The message names the column or the key.
    """
    if not record_id_columns:
        return [positional_row_identifier(i) for i in range(table.num_rows)]
    missing = [c for c in record_id_columns if c not in table.column_names]
    if missing:
        raise RowIdentifierError(
            f"record_id_columns {missing} not found in table columns: "
            f"{table.column_names}"
        )
    for column in record_id_columns:
        arrow_type = table.schema.field(column).type
        if not key_column_type_allowed(arrow_type):
            raise RowIdentifierError(
                f"key column {column!r} has type {arrow_type}, which cannot identify "
                "a row: a key column must hold strings, integers, booleans, "
                "timestamps or dates. Name a string or integer key column in "
                "record_id_columns, or pass record_id_columns=() to identify rows by "
                "position."
            )
    columns: dict[str, list[object]] = {}
    for column in record_id_columns:
        try:
            columns[column] = _key_values(table.column(column))
        except OverflowError as exc:
            raise RowIdentifierError(
                f"key column {column!r} holds a {table.schema.field(column).type} "
                "value outside years 1 to 9999, which a row identifier cannot write"
            ) from exc
    identifiers: list[str] = []
    seen: dict[str, int] = {}
    for i in range(table.num_rows):
        key = [(c, columns[c][i]) for c in record_id_columns]
        nulls = [c for c, value in key if value is None]
        if nulls:
            raise RowIdentifierError(
                f"key column {nulls[0]!r} is null in row {i}: a key value identifies "
                "the row and cannot be null"
            )
        identifier = key_row_identifier(key)
        if identifier in seen:
            raise RowIdentifierError(
                f"key {identifier} appears in rows {seen[identifier]} and {i}: key "
                "values must be unique, so each row has its own identifier"
            )
        seen[identifier] = i
        identifiers.append(identifier)
    return identifiers


def require_hash_in_key_order_usable(
    hash_in_key_order: bool, *, source_name: str | None, key_columns: Sequence[str]
) -> None:
    """Refuse ``hash_in_key_order=True`` where it would mean nothing (ITL-1272).

    It orders the rows of the table a default source name is hashed from, so it needs
    key columns (there is no key order without them) and no given ``source_id``: the
    table hash is still computed then, but it does not determine a given name and
    enters no identity, so the argument would change nothing. Whole-argument checks,
    made before the table is read.

    Args:
        hash_in_key_order: The argument as given.
        source_name: The given source name (the ``source_id`` argument), or ``None``.
        key_columns: The resolved key columns, in key order; empty for the position.

    Raises:
        ValueError: If ``hash_in_key_order`` is true with a ``source_id`` or with no
            key columns.
    """
    if not hash_in_key_order:
        return
    if source_name is not None:
        raise ValueError(
            "hash_in_key_order=True orders the rows hashed into a default source id, "
            f"but a source_id ({source_name!r}) is given, which the table hash does "
            "not determine; pass one or the other"
        )
    if not key_columns:
        raise ValueError(
            "hash_in_key_order=True needs key columns: name them in "
            "record_id_columns (rows identified by position have no key order)"
        )


def _unhashable_columns(
    hasher: ArrowHasherProtocol, table: pa.Table
) -> list[tuple[str, pa.DataType]]:
    """Return the columns of ``table`` that ``hasher`` cannot hash, with their types.

    Each column is hashed on its own. A zero-row slice is not enough, because the hasher
    rejects a type only when it visits a value, so the whole column is probed. This runs
    only after hashing the whole table has failed.

    Args:
        hasher: The data context's Arrow hasher.
        table: The table whose hash failed.

    Returns:
        ``(column name, Arrow type)`` for every column whose hash raises
        ``ValueError``, in column order.
    """
    failing: list[tuple[str, pa.DataType]] = []
    for name in table.column_names:
        try:
            hasher.hash_table(table.select([name]))
        except ValueError:
            failing.append((name, table.schema.field(name).type))
    return failing


def _unhashable_table_error(
    hasher: ArrowHasherProtocol, table: pa.Table, exc: ValueError
) -> ValueError | None:
    """Return the error for a table the Arrow hasher refused, naming every column.

    The hasher (starfix) raises ``ValueError`` for a type it does not hash, at the
    table or at its schema, and names the first such column only.

    Args:
        hasher: The data context's Arrow hasher.
        table: The table whose hash, or whose schema's hash, failed.
        exc: The hasher's error.

    Returns:
        A ``ValueError`` naming every column the hasher refuses with its type, or
        ``None`` when no single column is refused, or when ``exc`` is a logical type
        identifier this installation cannot read, which names its column already
        (the caller re-raises ``exc``).
    """
    if isinstance(exc, LogicalTypeIdentifierError):
        return None
    unhashable = _unhashable_columns(hasher, table)
    if not unhashable:
        return None
    described = ", ".join(f"{name!r} of type {kind}" for name, kind in unhashable)
    noun = "column" if len(unhashable) == 1 else "columns"
    return ValueError(
        f"cannot hash {noun} {described}: the Arrow hasher refused "
        f"({exc}). Drop or convert before building the source."
    )


@dataclass(frozen=True)
class SourceStreamResult:
    """Artifacts produced by ``SourceStreamBuilder.build()``."""

    stream: ArrowTableStream
    schema_hash: str
    table_hash: ContentHash
    source_id: str
    source_name: str
    tag_columns: tuple[str, ...]
    system_tag_columns: tuple[str, ...]
    data_tags: tuple[str, ...] = ()


class SourceStreamBuilder:
    """Builds an enriched ``ArrowTableStream`` from a raw Arrow table.

    Args:
        data_context: Provides type_converter, semantic_hasher, arrow_hasher.
        config: Orcapod config (controls hash character counts).
    """

    def __init__(self, data_context: DataContext, config: OrcapodConfig) -> None:
        self._data_context = data_context
        self._config = config

    def build(
        self,
        table: pa.Table,
        tag_columns: Collection[str],
        source_name: str | None = None,
        record_id_columns: str | Collection[str] | None = (),
        system_tag_columns: Collection[str] = (),
        kind_part: str | None = None,
        data_tags: Collection[str] = (),
        *,
        hash_in_key_order: bool = False,
    ) -> SourceStreamResult:
        """Run the full enrichment pipeline.

        The builder trusts the incoming table's nullable flags as-is.
        Callers are responsible for setting nullable correctly before calling
        this method — use ``arrow_utils.infer_schema_nullable(table)`` when
        receiving a raw Arrow table whose schema has not been set deliberately
        (e.g. from Polars or plain ``pa.table({})``, which default all fields
        to ``nullable=True`` regardless of content).

        Args:
            table: Arrow table with nullable flags already set correctly.
            tag_columns: Column names forming the tag for each row.
            source_name: The source's name, the part of its source id after the
                kind part (ITL-1425). When not given, the default is
                ``<algorithm label>:<hex>`` of the table hash (``hash_source_name``),
                which changes when rows are reordered unless ``hash_in_key_order`` is
                given; pass an explicit value when the source id must survive
                reordering.
            record_id_columns: The key columns whose values identify a row, in key
                order: the row identifier is their canonical JSON
                (``key_row_identifier``), which follows the row regardless of its
                position (given a stable ``source_id``). Empty: the row's position,
                ``row_<n>``, stable only while row order is.
            system_tag_columns: Additional system-level tag columns.
            kind_part: The kind part of the source id, ``<kind>_v<N>``, for example
                ``dict_v1`` (``source_ids.kind_part``). Required: the source id is
                ``<kind part>:<name>`` (``source_ids.build_source_id``).
            data_tags: The tag columns that are data-tags: also data columns
                of the same name, stored once (ITL-1023). Empty (the builder's
                default) gives plain tags, exactly as in v0.3; sources pass their
                declared tags minus ``tag_only_columns``.
            hash_in_key_order: Hash the table with its rows sorted by the key
                columns, in key order (``arrow_utils.key_row_order``), so the same
                rows in any order give one table hash and one default source id
                (ITL-1272). The stream keeps the rows in the order given. Off by
                default: the rows are hashed in the order given, which the user may
                have chosen on purpose. Needs key columns and no ``source_id``.

        Returns:
            SourceStreamResult with enriched stream and metadata.

        Raises:
            ValueError: If the table has no columns ("Table is empty"), if
                tag_columns or a key column are not in the table, a key column's
                type is not allowed, a key value is null or repeated, or if
                ``kind_part`` is not given or is not ``<kind>_v<N>``, or if the
                source id it builds is not valid, or if ``hash_in_key_order`` is given with
                a ``source_name`` or without key columns.
            InputValidationError: If a column's type is, or contains, a
                dictionary: dictionary-encoded columns are not currently supported
                (ITL-1264, ITL-1277).
        """
        # Whole-argument checks first, before the table is read.
        key_columns = normalize_record_id_columns(record_id_columns) or ()
        if source_name is not None:
            require_source_name(source_name)
        require_hash_in_key_order_usable(
            hash_in_key_order, source_name=source_name, key_columns=key_columns
        )
        tag_columns_tuple = tuple(tag_columns)
        system_tag_columns_tuple = tuple(system_tag_columns)
        require_supported_nullable_types(table.schema)
        data_tags_tuple = tuple(c for c in tag_columns_tuple if c in data_tags)
        if unknown := set(data_tags) - set(tag_columns_tuple):
            raise ValueError(
                f"data_tags {sorted(unknown)} are not in tag_columns "
                f"{list(tag_columns_tuple)}"
            )
        # Tag-only columns: the tags that are not also data.
        tag_only_tuple = tuple(c for c in tag_columns_tuple if c not in data_tags_tuple)

        # A tag never has a meta column's name; the meta columns are dropped next,
        # so say so here rather than report the tag as missing (ITL-1080).
        schema_utils.require_names_not_meta(tag_columns_tuple)

        # Dictionary-encoded columns are not currently supported (ITL-1264,
        # ITL-1277). The stream refuses them too; checking first reports the
        # dictionary, not a key column error, and before anything is hashed. It runs
        # before the system columns are dropped: a hand-built meta, source-info or
        # system-tag column holding a dictionary is refused, not dropped silently.
        if found := schema_utils.dictionary_columns(table.schema):
            raise schema_utils.dictionary_columns_error(
                found, tag_columns_tuple, data_tags_tuple, system_tag_columns_tuple
            )
        # Nor are columns of a type the schema hash rejects (a union, a list view, a
        # run-end-encoded type; ITL-640): refused here first, as the stream refuses
        # them, with the same message.
        if found := schema_utils.schema_hash_rejected_columns(table.schema):
            raise schema_utils.schema_hash_rejected_columns_error(
                found, tag_columns_tuple, data_tags_tuple, system_tag_columns_tuple
            )

        # 1. Drop system columns from raw input.
        table = arrow_utils.drop_system_columns(table)
        require_custom_system_tags(table, system_tag_columns_tuple)

        # 2. Validate tag_columns. A table with no columns at all (e.g. a
        # DictSource built from no rows and no schema) gets its own error, since
        # "tag column not found" would point at the wrong problem.
        if not table.column_names:
            raise ValueError(
                "Table is empty: the input has no columns, so its tag and data "
                "columns cannot be found. To build a source with no rows, give it "
                "an explicit schema (for example data_schema= on DictSource)."
            )
        missing_tags = set(tag_columns_tuple) - set(table.column_names)
        if missing_tags:
            raise ValueError(
                f"tag_columns not found in table: {missing_tags}. "
                f"Available columns: {list(table.column_names)}"
            )

        # 3. The row identifiers (validates the key columns: a missing, null or
        # repeated key is refused here, before anything is hashed).
        identifiers = row_identifiers(table, key_columns)

        # 4. Compute schema hash from the tag/data Arrow schemas (ITL-640).
        # Nullable flags in the incoming table are trusted as-is — callers must
        # set them correctly before calling build().
        non_sys = arrow_utils.drop_system_columns(table)
        tag_schema = non_sys.select(list(tag_columns_tuple)).schema
        data_schema = non_sys.drop(list(tag_only_tuple)).schema
        try:
            schema_hash = compute_source_schema_hash(
                tag_schema,
                data_schema,
                self._data_context,
                self._config,
            )
        except ValueError as exc:
            # A type the Arrow hasher cannot hash fails here first, at its schema
            # (ITL-640): refused as the table hash below refuses it.
            error = _unhashable_table_error(self._data_context.arrow_hasher, table, exc)
            if error is None:
                raise
            raise error from exc

        # 5. Compute table hash for data identity: the rows in the order given, or,
        # with ``hash_in_key_order``, in key order (ITL-1272). The stream keeps the
        # rows in the order given either way.
        hasher = self._data_context.arrow_hasher
        hashed = (
            arrow_utils.key_row_order(table, key_columns)
            if hash_in_key_order
            else table
        )
        try:
            table_hash = hasher.hash_table(hashed)
        except ValueError as exc:
            error = _unhashable_table_error(hasher, table, exc)
            if error is None:
                raise
            raise error from exc

        # 6. Default name: the labelled table hash, at full length, after the kind
        # part (ITL-1425). A default built from a hash says which algorithm made it.
        if kind_part is None:
            raise ValueError(
                "build() needs the kind part of the source id (kind_part, "
                "'<kind>_v<N>'): every source id is '<kind part>:<name>'"
            )
        kind, version = parse_kind_part(kind_part)
        if source_name is None:
            source_name = hash_source_name(table_hash)
        source_id = build_source_id(kind, version, source_name)

        # 7. Attach the source-info tokens and the system tags.
        stream = self.attach_provenance(
            table,
            tag_columns=tag_columns_tuple,
            system_tag_columns=system_tag_columns_tuple,
            schema_hash=schema_hash,
            source_id=source_id,
            row_identifiers=identifiers,
            data_tags=data_tags_tuple,
        )

        return SourceStreamResult(
            stream=stream,
            schema_hash=schema_hash,
            table_hash=table_hash,
            source_id=source_id,
            source_name=source_name,
            tag_columns=tag_columns_tuple,
            system_tag_columns=system_tag_columns_tuple,
            data_tags=data_tags_tuple,
        )

    def attach_provenance(
        self,
        table: pa.Table,
        *,
        tag_columns: Sequence[str],
        system_tag_columns: Sequence[str],
        schema_hash: str,
        source_id: str,
        row_identifiers: Sequence[str],
        data_tags: Sequence[str] = (),
        with_source_info: bool = True,
    ) -> ArrowTableStream:
        """Add source-info and system-tag columns to ``table`` and wrap it as a stream.

        A row's record id is its row identifier itself, as text (``row_<n>`` or the
        key's canonical JSON), and its source id is the source's name: the two
        system-tag columns. Each source-info value is
        ``<source_id>::<record id>::<column>`` (for example ``_source_amplitude``
        holds ``<source_id>::row_3::amplitude``). A row keeps its record id wherever
        and however often it is read, as long as its identifier is the same
        (ITL-1154).

        Args:
            table: The rows, with nullable flags already set correctly.
            tag_columns: Column names forming the tag for each row.
            system_tag_columns: Additional system-level tag columns.
            schema_hash: The source's schema hash, which names its system-tag columns.
            source_id: The canonical source name.
            row_identifiers: One identifier per row, identifying the row within the
                source.
            data_tags: The tag columns that are also data columns (data-tags);
                they get source info like any data column.
            with_source_info: Whether to build the source-info tokens. ``False`` only
                for a read whose caller keeps no source-info column and hashes no row
                (ITL-1464): the tokens are one string per row and data column, many
                times the size of the rows. The stream then holds null tokens and
                must not be handed on.

        Returns:
            The stream over the enriched table.
        """
        if with_source_info:
            source_info = [
                f"{source_id}{constants.BLOCK_SEPARATOR}{identifier}"
                for identifier in row_identifiers
            ]
            table = arrow_utils.add_source_info(
                table,
                source_info,
                exclude_columns=[c for c in tag_columns if c not in data_tags],
            )
        table = arrow_utils.add_system_tag_columns(
            table,
            schema_hash,
            source_id,
            row_identifiers,
        )
        # Nullable flags are already correct — the caller set them.
        return ArrowTableStream(
            table=table,
            tag_columns=tag_columns,
            system_tag_columns=system_tag_columns,
            data_tags=data_tags,
        )
