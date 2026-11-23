from __future__ import annotations

import logging
import os
import random
import sys
import time
from collections import defaultdict
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from orcapod.databases._row_index import (
    KeyIndex,
    compact_tail,
    is_indexed_lookup,
    matching_rows,
    rows_at,
)
from orcapod.databases.utils import (
    appended_field_change,
    coerce_record_id,
    metadata_bytes,
)
from orcapod.errors import (
    InputValidationError,
    OrcapodDatabaseReadError,
    TableListingNotSupportedError,
    TypeIdentityError,
    UnstorableColumnError,
)
from orcapod.databases.delta_arrow_types import (
    ARROW_TYPE_KEY,
    first_column_out_of_stored_range,
    from_delta_layout,
    merged_type,
    rebased_offsets,
    recorded_type,
    to_delta_layout,
    with_recorded_type,
    written_field,
)
from orcapod.databases.delta_statistics import statistics_configuration
from orcapod.databases.storage_utils import (
    delta_to_pyarrow_dataset,
    is_cloud_uri,
    parse_base_path,
)
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import deltalake
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as pa_ds
    import pyarrow.fs as pa_fs
    from upath import UPath
else:
    deltalake = LazyModule("deltalake")
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")
    pc = LazyModule("pyarrow.compute")
    pa_ds = LazyModule("pyarrow.dataset")

# Module-level logger
logger = logging.getLogger(__name__)


# Directory that marks its parent directory as a Delta table.
_DELTA_LOG_DIRECTORY = "_delta_log"

# How many times a flush tries to commit a table's pending rows when other writers
# keep committing first; after the last attempt it raises the last conflict
# (ITL-1377).
_COMMIT_ATTEMPTS = 10


class _LostCommitRace(Exception):
    """Another writer committed to the table first; load it again and retry.

    Attributes:
        error: The error the lost commit raised, raised as itself when no attempt
            is left.
    """

    def __init__(self, error: Exception) -> None:
        super().__init__(str(error))
        self.error = error


def _sleep_before_retry(attempt: int) -> None:
    """Wait a random time before attempt ``attempt + 1``, longer after each loss.

    Writers that lost to each other start again at different moments, so they do
    not keep colliding. The wait only spaces attempts: what is committed is decided
    against the table each attempt loads.
    """
    time.sleep(random.uniform(0, min(0.5, 0.005 * 2**attempt)))


# deltalake storage option -> where s3fs takes the same setting.
_S3FS_OPTION_OF: dict[str, tuple[str, ...]] = {
    "aws_access_key_id": ("key",),
    "aws_secret_access_key": ("secret",),
    "aws_session_token": ("token",),
    "aws_endpoint_url": ("client_kwargs", "endpoint_url"),
    "aws_endpoint": ("client_kwargs", "endpoint_url"),
    "aws_region": ("client_kwargs", "region_name"),
    "aws_default_region": ("client_kwargs", "region_name"),
}


def _s3fs_options(storage_options: Mapping[str, str]) -> dict[str, Any]:
    """Translate deltalake storage options into ``s3fs`` constructor arguments.

    Covers the credentials, the endpoint and the region. Other options are not
    translated; ``s3fs`` then falls back on its default credential chain.

    Args:
        storage_options: Options in deltalake's form (``AWS_*`` keys, any case).

    Returns:
        Keyword arguments for ``s3fs.S3FileSystem``.
    """
    options: dict[str, Any] = {}
    for key, value in storage_options.items():
        target = _S3FS_OPTION_OF.get(key.lower())
        if target is None:
            continue
        if len(target) == 1:
            options[target[0]] = value
        else:
            options.setdefault(target[0], {})[target[1]] = value
    return options


def _list_local_directory(directory: str) -> list[str]:
    """Return the names of the subdirectories of a local directory.

    A symbolic link to a directory is not a subdirectory here: following one that
    points at an ancestor would send the table walk in circles.
    """
    with os.scandir(directory) as entries:
        return [entry.name for entry in entries if entry.is_dir(follow_symlinks=False)]


def _join_local_path(directory: str, name: str) -> str:
    """Return the path of the entry ``name`` of a local directory."""
    return os.path.join(directory, name)


def _join_cloud_path(directory: str, name: str) -> str:
    """Return the path of the entry ``name`` of a cloud directory."""
    return f"{directory}/{name}"


def _stored_component(directory_name: str) -> str:
    """Return the path component a local directory name stands for.

    The inverse of ``DeltaTableDatabase._sanitize_path_component``: on Windows a
    ``:`` is stored as ``!``. Cloud paths are stored unchanged and need no inverse.
    """
    if sys.platform == "win32":
        return directory_name.replace("!", ":")
    return directory_name


def _allows_http(storage_options: Mapping[str, str]) -> bool:
    """Return whether deltalake's ``AWS_ALLOW_HTTP`` option is switched on."""
    for key, value in storage_options.items():
        if key.lower() in ("aws_allow_http", "allow_http"):
            return str(value).strip().lower() in ("true", "1", "yes", "on")
    return False


def _comparable_storage_type(arrow_type: pa.DataType) -> pa.DataType:
    """Return ``arrow_type`` reduced to what the append check compares.

    Extension types are replaced by their storage type, a dictionary by its value
    type, and every nested field is made nullable, at every depth. The result is used only to compare a batch
    with a stored table (ITL-884); it is never stored, so dropping nested
    non-null flags here changes no schema or hash.

    Args:
        arrow_type: The Arrow type to reduce.

    Returns:
        The storage type with all nested fields nullable.
    """
    if isinstance(arrow_type, pa.ExtensionType):
        return _comparable_storage_type(arrow_type.storage_type)
    if pa.types.is_dictionary(arrow_type):
        # The Delta writer decodes a dictionary: its values are what is stored.
        return _comparable_storage_type(
            arrow_utils.normalize_to_large_types(arrow_type.value_type)
        )
    if pa.types.is_struct(arrow_type):
        return pa.struct(
            [pa.field(f.name, _comparable_storage_type(f.type)) for f in arrow_type]
        )
    if pa.types.is_large_list(arrow_type):
        field = arrow_type.value_field
        return pa.large_list(pa.field(field.name, _comparable_storage_type(field.type)))
    if pa.types.is_list(arrow_type):
        field = arrow_type.value_field
        return pa.list_(pa.field(field.name, _comparable_storage_type(field.type)))
    if pa.types.is_fixed_size_list(arrow_type):
        field = arrow_type.value_field
        return pa.list_(
            pa.field(field.name, _comparable_storage_type(field.type)),
            arrow_type.list_size,
        )
    if pa.types.is_map(arrow_type):
        key, item = arrow_type.key_field, arrow_type.item_field
        # pyarrow accepts key and item Fields here; its stubs only list types.
        return pa.map_(  # pyright: ignore[reportCallIssue]
            pa.field(key.name, _comparable_storage_type(key.type), nullable=False),
            pa.field(item.name, _comparable_storage_type(item.type)),
            keys_sorted=arrow_type.keys_sorted,
        )
    return arrow_type


def _without_null_types(arrow_type: pa.DataType) -> pa.DataType | None:
    """Return ``arrow_type`` without the parts typed ``null``, or ``None`` if none is left.

    A ``null``-typed value fits a column of any type, and Delta has no ``null``
    type, so the storability check (``delta_unstorable_reason``) leaves it out at
    every depth: a
    ``null`` struct field is dropped (its siblings are still compared, inside lists
    and maps too), and a type that is ``null``, or whose list items or map keys or
    values are entirely ``null``, is not compared.

    Args:
        arrow_type: A type already reduced by ``_comparable_storage_type``.

    Returns:
        The type to compare, or ``None`` when it should not be compared.
    """
    if pa.types.is_null(arrow_type):
        return None
    if pa.types.is_struct(arrow_type):
        kept = [
            pa.field(f.name, t)
            for f in arrow_type
            if (t := _without_null_types(f.type)) is not None
        ]
        return pa.struct(kept) if kept else None
    if pa.types.is_large_list(arrow_type) or pa.types.is_list(arrow_type):
        item = _without_null_types(arrow_type.value_type)
        if item is None:
            return None
        field = pa.field(arrow_type.value_field.name, item)
        return (
            pa.large_list(field)
            if pa.types.is_large_list(arrow_type)
            else pa.list_(field)
        )
    if pa.types.is_fixed_size_list(arrow_type):
        item = _without_null_types(arrow_type.value_type)
        if item is None:
            return None
        return pa.list_(
            pa.field(arrow_type.value_field.name, item), arrow_type.list_size
        )
    if pa.types.is_map(arrow_type):
        key = _without_null_types(arrow_type.key_type)
        value = _without_null_types(arrow_type.item_type)
        if key is None or value is None:
            return None
        # pyarrow accepts key and item Fields here; its stubs only list types.
        return pa.map_(  # pyright: ignore[reportCallIssue]
            pa.field(arrow_type.key_field.name, key, nullable=False),
            pa.field(arrow_type.item_field.name, value),
            keys_sorted=arrow_type.keys_sorted,
        )
    return arrow_type


def _as_delta_writer_stores(schema: pa.Schema) -> pa.Schema:
    """Return ``schema`` with the types the Delta writer would store it as.

    Applies the conversion ``deltalake.write_deltalake`` and ``DeltaTable.merge``
    apply to incoming data (unsigned to signed integers, every timestamp to
    microseconds, fixed-size binary to binary), then Delta's own type mapping.
    Calling the writer's conversion instead of copying it keeps the append check
    in step with what the writer accepts; ``test_identical_schema_appends``
    guards it across ``deltalake`` versions.

    Args:
        schema: The schema of the rows about to be written.

    Returns:
        The schema as the Delta table would store it.
    """
    from arro3.core import Schema as Arro3Schema
    from deltalake.writer._conversion import (  # pyright: ignore[reportPrivateUsage]  # the writer's own conversion, see the docstring
        _convert_arro3_schema_to_delta,  # pyright: ignore[reportPrivateUsage]
    )

    converted = _convert_arro3_schema_to_delta(Arro3Schema.from_arrow(schema))
    return pa.schema(
        deltalake.Schema.from_arrow(converted).to_arrow()  # pyright: ignore[reportArgumentType]  # arro3 schema; pyarrow reads it through the C data interface
    )


def _conformed_to_stored(
    delta_table: deltalake.DeltaTable, written: pa.Table, table_uri: str
) -> pa.Table:
    """Return ``written`` with each struct, at every depth, in the stored layout.

    Delta's MERGE matches the fields of a struct by POSITION, so a struct appended with
    its fields in another order would store each value under another field's name. Each
    column the table stores is given the stored struct layout, by name: the stored
    fields in their order (a field the rows omit is filled with nulls), then the rows'
    new fields (schema evolution), at every depth of lists and maps. Logical
    (extension) columns, whose layout their type fixes, are left as they are.

    Args:
        delta_table: The loaded stored table.
        written: The rows about to be appended, as written (``to_delta_layout``).
        table_uri: The table, for the error message.

    Returns:
        ``written`` in the stored layout.

    Raises:
        InputValidationError: The rows omit a struct field the table stores as
            non-nullable.
    """
    stored = _stored_arrow_schema(delta_table)
    for index, field in enumerate(written.schema):
        position = stored.get_field_index(field.name)
        if position < 0 or isinstance(field.type, pa.ExtensionType):
            continue
        column = written.column(index)
        try:
            conformed = pa.chunked_array(
                [
                    _conformed_array(chunk, stored.field(position).type)
                    for chunk in column.chunks
                ]
            )
        except ValueError as exc:
            raise InputValidationError(
                f"Cannot append to the stored table {table_uri!r}: column "
                f"{field.name!r}: {exc}. The stored rows are unchanged and nothing "
                "was written."
            ) from exc
        if conformed.type != column.type or _struct_orders(conformed.type) != (
            _struct_orders(column.type)
        ):
            written = written.set_column(
                index, field.with_type(conformed.type), conformed
            )
    return written


def _struct_orders(arrow_type: pa.DataType) -> tuple[Any, ...]:
    """The field names of every struct in ``arrow_type``, in order (Arrow's type
    equality compares them, but this states the intent)."""
    if pa.types.is_struct(arrow_type):
        return tuple((f.name, _struct_orders(f.type)) for f in arrow_type)
    return tuple(
        _struct_orders(arrow_type.field(i).type) for i in range(arrow_type.num_fields)
    )


def _conformed_array(array: Any, stored: Any) -> Any:
    """Return ``array`` with every struct in the stored layout (``_conformed_to_stored``).

    Raises:
        ValueError: The array omits a struct field ``stored`` has as non-nullable.
    """
    t = pa.types
    if t.is_null(array.type) and not t.is_null(stored):
        # A ``null``-typed part where the table stores a type: typed nulls, since
        # Delta's MERGE panics on a ``null`` child of an evolving struct (ITL-1334).
        return pa.nulls(len(array), stored)
    mask = array.is_null() if array.null_count else None
    if t.is_struct(array.type) and t.is_struct(stored):
        own = {array.type.field(i).name: i for i in range(array.type.num_fields)}
        fields, children = [], []
        for stored_field in stored:
            if stored_field.name in own:
                i = own[stored_field.name]
                child = _conformed_array(array.field(i), stored_field.type)
                fields.append(array.type.field(i).with_type(child.type))
                children.append(child)
            elif stored_field.nullable:
                fields.append(stored_field)
                children.append(pa.nulls(len(array), stored_field.type))
            else:
                raise ValueError(
                    f"the rows omit the struct field {stored_field.name!r}, stored as "
                    "non-nullable"
                )
        stored_names = {f.name for f in stored}
        for i in range(array.type.num_fields):
            own_field = array.type.field(i)
            if own_field.name not in stored_names:
                fields.append(own_field)
                children.append(array.field(i))
        return pa.StructArray.from_arrays(children, fields=fields, mask=mask)
    if t.is_map(array.type) and t.is_map(stored):
        offsets, start, length = rebased_offsets(array)
        keys = _conformed_array(array.keys.slice(start, length), stored.key_type)
        items = _conformed_array(array.items.slice(start, length), stored.item_type)
        return pa.MapArray.from_arrays(
            offsets,
            keys,
            items,
            type=cast(
                Any,
                pa.map_(  # pyright: ignore[reportCallIssue]  # pyarrow takes fields
                    array.type.key_field.with_type(keys.type),
                    array.type.item_field.with_type(items.type),
                    keys_sorted=array.type.keys_sorted,
                ),
            ),
            mask=mask,
        )
    # A fixed-size list is written as a large_list (``to_delta_layout``), so only the
    # variable-size lists appear here.
    lists = (t.is_list, t.is_large_list)
    if any(k(array.type) for k in lists) and any(k(stored) for k in lists):
        # cast: the type guards above narrow the types in the stubs' terms.
        list_type: Any = array.type
        offsets, start, length = rebased_offsets(array)
        values = _conformed_array(
            array.values.slice(start, length), cast(Any, stored).value_type
        )
        if values.type == list_type.value_type and _struct_orders(values.type) == (
            _struct_orders(list_type.value_type)
        ):
            return array
        value_field = list_type.value_field.with_type(values.type)
        if t.is_large_list(array.type):
            return pa.LargeListArray.from_arrays(
                offsets, values, type=pa.large_list(value_field), mask=mask
            )
        return pa.ListArray.from_arrays(
            offsets, values, type=pa.list_(value_field), mask=mask
        )
    return array


def _with_merged_records(
    delta_table: deltalake.DeltaTable, original: pa.Table, written: pa.Table
) -> pa.Table:
    """Return ``written`` with each recorded column's record describing the column
    after the append (ITL-1334).

    A column recorded on either side (stored or incoming) is recorded as the merge of
    its stored type (its record, else its stored type) and the incoming rows' type
    (``merged_type``): an append that evolves a struct (a field added, omitted or in
    another order) keeps every stored field and its values, which a read then matches
    by name. Columns the stored table does not have, and logical (extension) columns,
    are left as they are.

    Args:
        delta_table: The loaded stored table.
        original: The rows about to be appended, with their own types.
        written: ``original`` as it is written (``to_delta_layout``).

    Returns:
        ``written`` with the merged records.
    """
    stored = _stored_arrow_schema(delta_table)
    stored_records = {
        f.name: recorded_type(f.metadata) for f in delta_table.schema().fields
    }
    for index, field in enumerate(written.schema):
        position = stored.get_field_index(field.name)
        own = original.schema.field(field.name).type
        if position < 0 or isinstance(own, pa.ExtensionType):
            continue
        stored_record = stored_records.get(field.name)
        incoming_record = recorded_type(field.metadata)
        if stored_record is None and incoming_record is None:
            continue
        base = (
            stored_record
            if stored_record is not None
            else arrow_utils.normalize_to_large_types(stored.field(position).type)
        )
        incoming = (
            incoming_record
            if incoming_record is not None
            else arrow_utils.normalize_to_large_types(own)
        )
        written = written.set_column(
            index,
            with_recorded_type(field, merged_type(base, incoming)),
            written.column(index),
        )
    return written


_EXTENSION_KEYS = (b"ARROW:extension:name", b"ARROW:extension:metadata")


def _metadata(metadata: Mapping[Any, Any] | None) -> dict[bytes, bytes]:
    """A field's own metadata as bytes: without the record a Delta column carries
    (``ARROW_TYPE_KEY``) and the extension name and metadata Delta keeps for a logical
    column (they are its type)."""
    as_bytes = metadata_bytes(metadata)
    for key in (ARROW_TYPE_KEY, *_EXTENSION_KEYS):
        as_bytes.pop(key, None)
    return as_bytes


def _is_logical(field: "pa.Field[Any]") -> bool:
    """Whether ``field`` is a logical (extension) column: as given, or as Delta stores
    it (its storage type, with the extension name in its metadata)."""
    return isinstance(field.type, pa.ExtensionType) or (
        _EXTENSION_KEYS[0] in metadata_bytes(field.metadata)
    )


def _evolution_change(
    stored: Any, incoming: Any, path: str, *, in_map: bool = False
) -> str | None:
    """Say why a struct of ``incoming`` cannot evolve the stored one (ITL-1334).

    Walks both types by struct field NAME and through lists and maps: a struct field
    ``incoming`` adds may not be non-nullable (the stored rows have no value for it,
    and Delta then cannot read the table), nor be inside a map (Delta's merge cannot
    add it); a field it omits is filled with nulls (``_conformed_to_stored``), so it
    may not be one stored as non-nullable.

    Args:
        stored: The type the table holds, large-normalized.
        incoming: The appended rows' type, large-normalized.
        path: Where in the column this is, for the reason.
        in_map: Whether ``path`` is inside a map.

    Returns:
        The reason, or ``None``.
    """
    t = pa.types
    if t.is_struct(stored) and t.is_struct(incoming):
        known = {f.name: f for f in stored}
        given = {f.name for f in incoming}
        for field in stored:
            if field.name not in given and not field.nullable:
                return (
                    f"{path}.{field.name} is stored as non-nullable, and the rows "
                    "omit it"
                )
        for field in incoming:
            here = f"{path}.{field.name}"
            if field.name not in known:
                if in_map:
                    return f"{here} is a new struct field inside a map, which Delta cannot add"
                if not field.nullable:
                    return (
                        f"{here} is a new non-nullable struct field, which the stored "
                        "rows have no value for"
                    )
                continue
            found = _evolution_change(
                known[field.name].type, field.type, here, in_map=in_map
            )
            if found is not None:
                return found
        return None
    if t.is_map(stored) and t.is_map(incoming):
        return _evolution_change(
            stored.key_type, incoming.key_type, f"{path}.key", in_map=True
        ) or _evolution_change(
            stored.item_type, incoming.item_type, f"{path}.item", in_map=True
        )
    lists = (t.is_list, t.is_large_list, t.is_fixed_size_list)
    if any(k(stored) for k in lists) and any(k(incoming) for k in lists):
        # cast: the type guards above narrow ``stored`` in the stubs' terms.
        return _evolution_change(
            cast(Any, stored).value_type,
            incoming.value_type,
            f"{path}[]",
            in_map=in_map,
        )
    return None


def _column_change(held: "pa.Field[Any]", field: "pa.Field[Any]") -> str | None:
    """Say why rows of column ``field`` cannot be appended where ``held`` is held.

    The column's own field metadata and its nested field tree must be the held ones
    (``appended_field_change``): the table holds one type per column, so either
    side's rows would read back with the other's. A struct may evolve, within
    ``_evolution_change``. A logical column's type is its extension's, which the type
    check compares; its own field metadata is compared here. Nullability is not
    compared: the append check and Delta decide it, and whether a change is refused
    belongs to the nullability audit (ITL-842).

    Args:
        held: The column as the table holds it (its record, else its stored type,
            with its field metadata; or a pending batch's column).
        field: The column of the rows to append.

    Returns:
        The reason, naming the path, or ``None``.
    """
    if _is_logical(field) or _is_logical(held):
        if _metadata(held.metadata) == _metadata(field.metadata):
            return None
        return f"{field.name} has other field metadata than the stored column"
    return appended_field_change(
        held.with_metadata(cast(Any, _metadata(held.metadata))),
        field.with_metadata(cast(Any, _metadata(field.metadata))),
    ) or _evolution_change(
        arrow_utils.normalize_to_large_types(held.type, keep_null=True),
        arrow_utils.normalize_to_large_types(field.type, keep_null=True),
        field.name,
    )


def _held_columns(delta_table: deltalake.DeltaTable) -> dict[str, "pa.Field[Any]"]:
    """Each stored column as the table holds it: its recorded type, else its stored
    type, with its field metadata as stored. Read from the log; no data is read."""
    stored = _stored_arrow_schema(delta_table)
    fields: dict[str, pa.Field[Any]] = {}
    for delta_field in delta_table.schema().fields:
        record = recorded_type(delta_field.metadata)
        fields[delta_field.name] = pa.field(
            delta_field.name,
            record
            if record is not None
            else stored.field(stored.get_field_index(delta_field.name)).type,
            nullable=delta_field.nullable,
            metadata=cast(Any, metadata_bytes(delta_field.metadata)) or None,
        )
    return fields


def _non_null_violation(
    held: Mapping[str, "pa.Field[Any]"], rows: pa.Table
) -> tuple[str, bool] | None:
    """The first column ``held`` declares non-nullable that ``rows`` omit or hold a
    null in: Delta's writer refuses such a batch (ITL-1334).

    Args:
        held: Columns by name: a stored table's, a pending batch's, or the rows' own.
        rows: The rows to append.

    Returns:
        ``(column, omitted)``: ``omitted`` is ``True`` when the rows lack the column,
        ``False`` when they hold a null in it; ``None`` when there is no such column.
    """
    for name, field in held.items():
        if field.nullable:
            continue
        index = rows.schema.get_field_index(name)
        if index < 0:
            return name, True
        if rows.column(index).null_count:
            return name, False
    return None


def _append_refusal(
    delta_table: deltalake.DeltaTable, rows: pa.Table
) -> tuple[str, str] | None:
    """The first column of ``rows`` the stored table cannot take, and why (ITL-1334).

    One rule for what an append is refused for, checked before the rows are buffered
    and again at flush against the version then loaded:

    - a column change (``_column_change``): other field attributes, a struct that
      adds a non-nullable field or one inside a map, or omits a non-nullable one;
    - a column stored as non-nullable that the rows omit or hold a null in (Delta
      refuses the commit);
    - a type the stored column does not have (``_incompatible_append_column``);
    - a value a column stored without a recorded type cannot hold
      (``first_column_out_of_stored_range``).

    Args:
        delta_table: The loaded stored table.
        rows: The rows to append, with their own types.

    Returns:
        ``(column, reason)``, or ``None`` when the rows can be appended.
    """
    held = _held_columns(delta_table)
    for field in rows.schema:
        if field.name in held:
            change = _column_change(held[field.name], field)
            if change is not None:
                return field.name, change
    violation = _non_null_violation(held, rows)
    if violation is not None:
        name, omitted = violation
        return name, (
            f"{name} is stored as non-nullable, and the rows "
            f"{'omit it' if omitted else 'hold a null'}"
        )
    written = pa.schema([written_field(f) for f in rows.schema])
    mismatch = DeltaTableDatabase._incompatible_append_column(
        delta_table, rows.schema, written
    )
    if mismatch is not None:
        column, stored_type, incoming_type = mismatch
        return column, (
            f"it is stored as {stored_type} but the new rows have {incoming_type} "
            f"(input type {rows.schema.field(column).type})"
        )
    overflowing = first_column_out_of_stored_range(
        rows, [name for name, f in held.items() if recorded_type(f.metadata) is None]
    )
    if overflowing is not None:
        return overflowing, (
            "it is stored as a signed integer with no recorded Arrow type (a table "
            "written before ITL-928), and the new rows have "
            f"{rows.schema.field(overflowing).type} values it cannot hold; rebuild "
            "the database"
        )
    return None


def _with_nulls_resolved(incoming: Any, held: Any) -> Any:
    """``incoming`` with each ``null``-typed part replaced by the type ``held`` has at
    the same path, so the parts beside it are still compared (ITL-1334).

    Struct fields are matched by name; list items and map keys and items followed.
    A ``null`` part at a path ``held`` has no type for stays ``null`` (refused by
    ``add_records`` before this is reached).

    Args:
        incoming: A type reduced by ``_comparable_storage_type``.
        held: The type the table holds, reduced the same way, or ``None``.

    Returns:
        ``incoming``, its ``null`` parts resolved.
    """
    t = pa.types
    if held is None:
        return incoming
    if t.is_null(incoming):
        return held
    if t.is_struct(incoming) and t.is_struct(held):
        known = {f.name: f.type for f in held}
        return pa.struct(
            [
                f.with_type(_with_nulls_resolved(f.type, known.get(f.name)))
                for f in incoming
            ]
        )
    if t.is_map(incoming) and t.is_map(held):
        return pa.map_(  # pyright: ignore[reportCallIssue]  # pyarrow takes key/item fields
            incoming.key_field.with_type(
                _with_nulls_resolved(incoming.key_type, held.key_type)
            ),
            incoming.item_field.with_type(
                _with_nulls_resolved(incoming.item_type, held.item_type)
            ),
            keys_sorted=incoming.keys_sorted,
        )
    lists = (t.is_list, t.is_large_list, t.is_fixed_size_list)
    if any(k(incoming) for k in lists) and any(k(held) for k in lists):
        # cast: the type guards above narrow the types in the stubs' terms.
        list_type: Any = incoming
        item = list_type.value_field.with_type(
            _with_nulls_resolved(list_type.value_type, cast(Any, held).value_type)
        )
        if t.is_large_list(list_type):
            return pa.large_list(item)
        if t.is_list(list_type):
            return pa.list_(item)
        return pa.list_(item, list_type.list_size)
    return incoming


def _untyped_null_paths(incoming: Any, given: Any, path: str) -> list[str]:
    """Every path where ``incoming`` is ``null``-typed and ``given`` has no type.

    ``given`` is what the stored table (or a pending batch) has at ``path``, or ``None``
    when it has nothing there; a stored ``null`` type gives no type either. Struct
    fields are matched by name, and list items, map keys and items followed.

    Returns:
        The dotted paths (``x.b``, ``x[]``), in field order; empty when every ``null``
        part of ``incoming`` has a typed counterpart.
    """
    t = pa.types
    if given is not None and t.is_null(given):
        given = None
    if t.is_null(incoming):
        return [path] if given is None else []
    if t.is_struct(incoming):
        known = (
            {f.name: f.type for f in given}
            if given is not None and t.is_struct(given)
            else {}
        )
        return [
            found
            for field in incoming
            for found in _untyped_null_paths(
                field.type, known.get(field.name), f"{path}.{field.name}"
            )
        ]
    if t.is_map(incoming):
        is_map = given is not None and t.is_map(given)
        return _untyped_null_paths(
            incoming.key_type, given.key_type if is_map else None, f"{path}.key"
        ) + _untyped_null_paths(
            incoming.item_type, given.item_type if is_map else None, f"{path}.item"
        )
    if incoming.num_fields or t.is_fixed_size_list(incoming):
        is_list = given is not None and (
            t.is_list(given) or t.is_large_list(given) or t.is_fixed_size_list(given)
        )
        return _untyped_null_paths(
            incoming.value_type, given.value_type if is_list else None, f"{path}[]"
        )
    return []


def _holds_empty_struct(arrow_type: pa.DataType) -> bool:
    """Whether ``arrow_type`` is or nests a struct with no fields, which a Parquet file
    cannot store (it stores values in leaf columns only; ITL-1334)."""
    if isinstance(arrow_type, pa.BaseExtensionType):
        return _holds_empty_struct(arrow_type.storage_type)
    if pa.types.is_struct(arrow_type) and arrow_type.num_fields == 0:
        return True
    return any(
        _holds_empty_struct(arrow_type.field(i).type)
        for i in range(arrow_type.num_fields)
    )


def _holds_null_type(arrow_type: pa.DataType) -> bool:
    """Whether ``arrow_type`` is or nests a ``null`` type (Delta has none; ITL-1334)."""
    if pa.types.is_null(arrow_type):
        return True
    return any(
        _holds_null_type(arrow_type.field(i).type) for i in range(arrow_type.num_fields)
    )


def delta_unstorable_reason(field: "pa.Field[Any]") -> str | None:
    """Say why Delta cannot hold a column, even through a recorded type, or ``None``.

    The column as it is written (``written_field``) goes through the Delta writer's
    own conversion and type mapping (``_as_delta_writer_stores``), so the answer
    follows the installed ``deltalake``: a timestamp in seconds is converted, a
    ``decimal256`` or an interval is refused.

    A ``null``-typed part is left out, as the append check leaves it out: it fits a
    column already stored with a type, and a column with no stored type yet is
    refused by ``add_records``, which knows the stored table.

    Args:
        field: A column, as given to the database.

    Returns:
        Delta's reason (``Invalid data type for Delta Lake: Decimal256(40, 2)``), or
        ``None`` when Delta holds the column.
    """
    if arrow_utils.holds_renamed_map(field.type):
        # The recorded type goes through Arrow IPC, which renames them (ITL-1334).
        return "a map's key and item fields read back as 'key' and 'value'"
    if _holds_empty_struct(field.type):
        return "a struct with no fields has no leaf column for a Parquet file to store"
    written = written_field(field)
    reduced = _without_null_types(written.type)
    if reduced is None:
        return None
    try:
        _ = _as_delta_writer_stores(pa.schema([written.with_type(reduced)]))
    except Exception as exc:  # noqa: BLE001  # deltalake raises a bare Exception
        return str(exc).removeprefix("Schema error: ")
    return None


def _merge_source_in_stored_layout(
    delta_table: deltalake.DeltaTable, batch: pa.Table
) -> pa.Table:
    """Return ``batch`` with its list columns in the layout the table stores.

    Delta stores ``large_list<large_binary>`` as ``list<binary>``, and the MERGE
    cannot plan a ``CASE WHEN`` over the two (ITL-984). A list column, a logical
    list included, is therefore handed to the MERGE as its storage type cast to
    the stored type. The values are unchanged, and the field keeps its metadata (the
    recorded type, a logical column's extension name and metadata), which a MERGE
    that evolves the schema writes to the table; every other column, and a list
    column the table does not have yet, is passed as it is.

    Args:
        delta_table: The loaded stored table.
        batch: The rows about to be merged.

    Returns:
        ``batch``, with the cast list columns.
    """
    stored = _stored_arrow_schema(delta_table)
    for index, field in enumerate(batch.schema):
        column = batch.column(index)
        if isinstance(field.type, pa.ExtensionType):
            plain = column.combine_chunks().storage  # pyright: ignore[reportAttributeAccessIssue]  # an extension array
            is_list = pa.types.is_large_list(plain.type) or pa.types.is_list(plain.type)
        else:
            plain = column
            is_list = pa.types.is_large_list(field.type) or pa.types.is_list(field.type)
        if not is_list or field.name not in stored.names:
            continue
        target = stored.field(field.name).type
        if arrow_utils.normalize_to_large_types(
            plain.type
        ) != arrow_utils.normalize_to_large_types(target):
            # More than the layout differs (a timestamp unit, a width, an evolved
            # struct): the MERGE decides, and no cast may change a value.
            continue
        # The field keeps its metadata: a MERGE that evolves the schema takes each
        # field's metadata from the source, so a dropped record or extension name
        # would be dropped from the table (ITL-1334).
        metadata = dict(field.metadata or {})
        if isinstance(field.type, pa.ExtensionType):
            metadata[b"ARROW:extension:name"] = field.type.extension_name.encode()
            metadata[b"ARROW:extension:metadata"] = field.type.__arrow_ext_serialize__()
        batch = batch.set_column(
            index,
            pa.field(
                field.name,
                target,
                nullable=field.nullable,
                metadata=cast(Any, metadata) or None,
            ),
            pa.chunked_array([plain]).cast(target)
            if isinstance(plain, pa.Array)
            else plain.cast(target),
        )
    return batch


def _without_field_metadata(delta_type: Any) -> Any:
    """Return a Delta type with the metadata of every nested field dropped.

    Args:
        delta_type: A ``deltalake.schema`` type.

    Returns:
        The same type with no field metadata at any depth.
    """
    from deltalake.schema import ArrayType, Field, MapType, StructType

    if isinstance(delta_type, StructType):
        return StructType(
            [
                Field(f.name, _without_field_metadata(f.type), nullable=f.nullable)
                for f in delta_type.fields
            ]
        )
    if isinstance(delta_type, ArrayType):
        return ArrayType(
            _without_field_metadata(delta_type.element_type),
            contains_null=delta_type.contains_null,
        )
    if isinstance(delta_type, MapType):
        return MapType(
            _without_field_metadata(delta_type.key_type),
            _without_field_metadata(delta_type.value_type),
            value_contains_null=delta_type.value_contains_null,
        )
    return delta_type


def _stored_arrow_schema(delta_table: deltalake.DeltaTable) -> pa.Schema:
    """Return the Arrow schema of the table's stored Delta schema, without metadata.

    Delta keeps the Arrow field metadata it was written with, including the
    extension type name of a logical type. Reading that back into pyarrow would
    rebuild the registered extension type over Delta's own storage type (``list``
    where Orcapod wrote ``large_list``), which the extension type rejects. The
    append check compares storage types only, so the metadata is dropped first.

    Args:
        delta_table: The loaded stored table.

    Returns:
        The stored schema as plain Arrow types.
    """
    from deltalake.schema import Field, Schema

    stored = delta_table.schema()
    plain = Schema(
        [
            Field(f.name, _without_field_metadata(f.type), nullable=f.nullable)
            for f in stored.fields
        ]
    )
    return pa.schema(plain.to_arrow())  # pyright: ignore[reportArgumentType]  # arro3 schema; pyarrow reads it through the C data interface


def _types_conflict(stored: pa.DataType, incoming: pa.DataType) -> bool:
    """Return ``True`` if ``incoming`` changes a type that ``stored`` already has.

    Both types must already be reduced by ``_comparable_storage_type``. Struct
    fields are matched by name at every depth, through list and map types. A
    field present on only one side is not a conflict, just as a top-level column
    present on only one side is not: adding or omitting it is schema evolution,
    which Delta's merge accepts or rejects itself. Any other difference is a
    conflict.

    Args:
        stored: The type the stored table has.
        incoming: The type the new rows have.

    Returns:
        Whether a type the stored table already has would change.
    """
    if pa.types.is_struct(stored) and pa.types.is_struct(incoming):
        stored_fields = {f.name: f.type for f in stored}
        return any(
            _types_conflict(stored_fields[f.name], f.type)
            for f in incoming
            if f.name in stored_fields
        )
    if (pa.types.is_large_list(stored) and pa.types.is_large_list(incoming)) or (
        pa.types.is_list(stored) and pa.types.is_list(incoming)
    ):
        return _types_conflict(stored.value_type, incoming.value_type)
    if pa.types.is_fixed_size_list(stored) and pa.types.is_fixed_size_list(incoming):
        return stored.list_size != incoming.list_size or _types_conflict(
            stored.value_type, incoming.value_type
        )
    if pa.types.is_map(stored) and pa.types.is_map(incoming):
        return _types_conflict(stored.key_type, incoming.key_type) or _types_conflict(
            stored.item_type, incoming.item_type
        )
    return stored != incoming


def _database_error_to_raise(errors: list[Exception]) -> Exception | None:
    """Pick the database error a failed flush raises as itself.

    A ``OrcapodDatabaseReadError`` is preferred, because it is the error that fails a run
    whatever the orchestrator's ``error_policy``; failing that, an
    ``InputValidationError`` from a refused append.

    Args:
        errors: The error of every batch that failed, in flush order.

    Returns:
        The first ``OrcapodDatabaseReadError``, else the first ``InputValidationError``, or
        ``None`` when no batch failed with a database error.
    """
    for kind in (OrcapodDatabaseReadError, InputValidationError):
        chosen = next((e for e in errors if isinstance(e, kind)), None)
        if chosen is not None:
            return chosen
    return None


@dataclass
class _CommittedSnapshot:
    """What one version of a stored Delta table holds, kept in memory (ITL-919).

    Per-row lookups are answered from this snapshot instead of re-reading the table.
    Each part is read on first use and extended in place after this database's own
    commits, so its cost follows the rows written, not the rows stored.

    Attributes:
        version: The table version described.
        ids: The stored record ids (never null), or ``None`` until first needed.
        rows: Indexes over every stored row, read exactly as ``get_all_records``
            reads them, or ``None`` until first needed.
        files: The data files ``rows`` was read from (set together with ``rows``).
        dataset_schema: The schema of the dataset ``rows`` was read from.
        dataset_format: The file format of that dataset, which reads a data file
            the way the whole table is read (ITL-1173).
        dataset_filesystem: The filesystem the dataset's ``files`` are paths in.
    """

    version: int
    ids: set[bytes] | None = None
    rows: KeyIndex | None = None
    files: frozenset[str] = frozenset()
    dataset_schema: "pa.Schema | None" = None
    dataset_format: "pa_ds.FileFormat | None" = None
    dataset_filesystem: "pa_fs.FileSystem | None" = None


class DeltaTableDatabase:
    """
    A Delta table database with clear insert vs update semantics.

    - insert(): Never overwrites existing records by default. Can skip duplicates if requested.
                Can be batched for performance. Supports composite keys.

    - update(): Always overwrites existing records. Executes immediately.
                Requires pending batches to be flushed first (or use force_flush=True).

    Supports both single column and composite (multi-column) record IDs.
    """

    # Class constants for internal column names
    ROW_INDEX_COLUMN = "__row_index"
    RECORD_ID_COLUMN = "__record_id"

    def __init__(
        self,
        base_path: str | Path | UPath,
        storage_options: dict[str, str] | None = None,
        create_base_path: bool = True,
        batch_size: int = 1000,
        max_hierarchy_depth: int = 10,
        allow_schema_evolution: bool = True,
        _path_prefix: tuple[str, ...] = (),
        _root: "DeltaTableDatabase | None" = None,
        _scoped_path: tuple[str, ...] = (),
        _shared_pending_batches: "dict[str, pa.Table] | None" = None,
        _shared_pending_record_ids: "defaultdict[str, set[bytes]] | None" = None,
        _shared_delta_table_cache: "dict[str, deltalake.DeltaTable] | None" = None,
        _shared_snapshots: "dict[str, _CommittedSnapshot] | None" = None,
        _shared_pending_indexes: "dict[str, KeyIndex] | None" = None,
        _shared_absent_tables: "set[str] | None" = None,
    ):
        self._root_uri, self._storage_options = parse_base_path(
            base_path, storage_options
        )
        self._is_cloud: bool = is_cloud_uri(self._root_uri)
        self._path_prefix = _path_prefix
        self._root = _root
        self._scoped_path = _scoped_path
        self.batch_size = batch_size
        self.max_hierarchy_depth = max_hierarchy_depth
        self.allow_schema_evolution = allow_schema_evolution

        if not self._is_cloud:
            # _local_root is the absolute filesystem root (for list_sources, mkdir, etc.)
            # NOTE: do NOT access self._local_root on cloud instances.
            self._local_root = Path(self._root_uri)
            if create_base_path:
                self._local_root.mkdir(parents=True, exist_ok=True)
            elif not self._local_root.exists():
                raise ValueError(
                    f"Base path {self._local_root} does not exist and create_base_path=False"
                )
        # For cloud paths: create_base_path is silently ignored (no directory needed).

        # Shared state — when constructed via at(), these dicts are shared with the
        # root instance. Any handle's flush() writes every pending key of the family
        # and refreshes the loaded table and existing-ids entries for it, so these
        # caches must be family-wide too: a per-handle cache would keep serving the
        # snapshot a sibling loaded before the flush (ITL-864). All of them are
        # touched only from the thread that drives the pipeline (the async
        # orchestrator runs only user functions in worker threads).
        self._delta_table_cache: dict[str, deltalake.DeltaTable] = (
            _shared_delta_table_cache if _shared_delta_table_cache is not None else {}
        )
        if _shared_pending_batches is not None:
            self._pending_batches = _shared_pending_batches
        else:
            self._pending_batches: dict[str, pa.Table] = {}
        if _shared_pending_record_ids is not None:
            self._pending_record_ids = _shared_pending_record_ids
        else:
            self._pending_record_ids: dict[str, set[bytes]] = defaultdict(set)
        # The committed rows of each loaded table version (ITL-919), shared like the
        # loaded tables: a snapshot describes the cached table's version only.
        self._snapshots: dict[str, _CommittedSnapshot] = (
            _shared_snapshots if _shared_snapshots is not None else {}
        )
        # Indexes over each pending batch, valid while it is the key's batch.
        self._pending_indexes: dict[str, KeyIndex] = (
            _shared_pending_indexes if _shared_pending_indexes is not None else {}
        )
        # Record keys whose table was found absent (ITL-1173), so lookups do not try
        # to open it again on every row. Family-wide like the loaded tables, and
        # forgotten on any flush of the family and on a flush of the key: a table
        # another process creates is seen after this database's next flush, as rows
        # it appends to a loaded table are. ``table_exists`` always checks again.
        self._absent_tables: set[str] = (
            _shared_absent_tables if _shared_absent_tables is not None else set()
        )

    def _clear_pending(self):
        """Clear all pending state."""
        self._pending_batches = {}
        self._pending_record_ids = defaultdict(set)
        # Note: next_row_index continues incrementing

    def _get_record_key(self, record_path: tuple[str, ...]) -> str:
        """Generate cache key for source storage.

        The key includes the full path (prefix + record_path) so that scoped
        instances that share pending state with the root can be correctly flushed
        from the root.
        """
        return "/".join(self._path_prefix + record_path)

    @staticmethod
    def _sanitize_path_component(component: str) -> str:
        """Sanitize a path component for the current OS.

        On Windows, colons are not allowed in filenames (reserved for drive
        letters). Replace them with '!' so that URIs containing ':' can still
        be stored safely on all platforms.
        """
        import sys

        if sys.platform == "win32":
            return component.replace(":", "!")
        return component

    def _get_table_uri(
        self, record_path: tuple[str, ...], create_dir: bool = False
    ) -> str:
        """Get the URI for a given record path, incorporating base_path prefix.

        Args:
            record_path: Tuple of path components (relative to base_path).
            create_dir: If True, create the local directory (no-op for cloud paths).
        """
        full_path = self._path_prefix + record_path  # prefix applied once, here only
        if self._is_cloud:
            return self._root_uri.rstrip("/") + "/" + "/".join(full_path)
        else:
            path = self._local_root
            for subpath in full_path:
                path = path / self._sanitize_path_component(subpath)
            if create_dir:
                path.mkdir(parents=True, exist_ok=True)
            return str(path)

    def _validate_record_path(self, record_path: tuple[str, ...]) -> None:
        # TODO: consider removing this as path creation can be tried directly
        """
        Validate source path components.

        Args:
            record_path: Tuple of path components

        Raises:
            ValueError: If path is invalid
        """
        if not record_path:
            raise ValueError("Source path cannot be empty")

        if len(self._path_prefix) + len(record_path) > self.max_hierarchy_depth:
            raise ValueError(
                f"Source path depth {len(record_path)} exceeds maximum "
                f"{self.max_hierarchy_depth - len(self._path_prefix)} "
                f"(base_path uses {len(self._path_prefix)} components)"
            )

        # Validate path components
        for i, component in enumerate(record_path):
            if not component or not isinstance(component, str):
                raise ValueError(
                    f"Source path component {i} is invalid: {repr(component)}"
                )

            # Check for filesystem-unsafe characters.
            # Note: ':' is handled by _sanitize_path_component (replaced with '!' on Windows)
            # and is allowed here so that prefixed path components (e.g. "schema:<hex digest>")
            # can be stored cross-platform.
            unsafe_chars = ["/", "\\", "*", "?", '"', "<", ">", "|", "\0"]
            if any(char in component for char in unsafe_chars):
                raise ValueError(
                    f"Source path {record_path} component {component} contains invalid characters: {repr(component)}"
                )

    def _get_delta_table(
        self, record_path: tuple[str, ...]
    ) -> deltalake.DeltaTable | None:
        """
        Get an existing Delta table, either from cache or by loading it.

        Args:
            record_path: Tuple of path components

        Returns:
            deltalake.DeltaTable instance or None if table doesn't exist
        """
        record_key = self._get_record_key(record_path)
        table_uri = self._get_table_uri(record_path)

        # Check cache first
        if dt := self._delta_table_cache.get(record_key):
            return dt
        if record_key in self._absent_tables:
            return None

        try:
            # Try to load existing table
            delta_table = deltalake.DeltaTable(
                table_uri, storage_options=self._storage_options or None
            )
            self._delta_table_cache[record_key] = delta_table
            logger.debug(f"Loaded existing Delta table for {record_key}")
            return delta_table
        except deltalake.exceptions.TableNotFoundError:
            # Table doesn't exist: remembered until the next flush (ITL-1173).
            self._absent_tables.add(record_key)
            return None
        except Exception as e:
            # The table exists but could not be loaded: never report it as absent.
            self._delta_table_cache.pop(record_key, None)
            raise self._read_failed(record_path) from e

    def _read_failed(self, record_path: tuple[str, ...]) -> OrcapodDatabaseReadError:
        """Build the error for a table at ``record_path`` that exists but cannot be read.

        Args:
            record_path: Path components relative to this database view.

        Returns:
            A ``OrcapodDatabaseReadError`` naming the table; the caller raises it from the
            backend's exception.
        """
        return OrcapodDatabaseReadError(
            self._get_table_uri(record_path), self._path_prefix + record_path
        )

    def list_table_paths(self, prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
        """List the paths of the Delta tables stored under ``prefix``.

        Walks the directories below ``prefix``: a directory that holds a
        ``_delta_log`` directory is a table. The walk goes on below a table, because
        tables can be nested (Orcapod v0.3 stores a table one level below where
        v0.2 stored it), and never enters ``_delta_log``. No table is opened. Only
        flushed tables are listed, as with ``table_exists``.

        A local database is listed exactly. A cloud database is listed through ``fsspec``
        with one list request per directory: ``s3://`` and ``s3a://`` through
        ``s3fs`` with the credentials, endpoint and region of this database's
        storage options; ``gs://``, ``az://`` and ``abfs://`` only when the matching
        ``fsspec`` package is installed and reaches the database with its default
        credentials.

        Args:
            prefix: Leading path components, relative to ``self.base_path``.

        Returns:
            The table paths, relative to ``self.base_path``, sorted.

        Raises:
            TableListingNotSupportedError: If the database is on cloud storage and
                could not be listed; the reason is chained.
            OrcapodDatabaseReadError: If a local directory could not be listed.
        """
        full_prefix = self._path_prefix + prefix
        if self._is_cloud:
            list_directory = self._cloud_directory_lister()
            start = self._root_uri.split("://", 1)[1].rstrip("/")
            start = "/".join((start,) + full_prefix)
            join = _join_cloud_path
            component = str
        else:
            list_directory = _list_local_directory
            start = str(
                self._local_root.joinpath(
                    *(self._sanitize_path_component(part) for part in full_prefix)
                )
            )
            join = _join_local_path
            component = _stored_component

        found: list[tuple[str, ...]] = []
        # Directories still to visit, each with its path below ``prefix``.
        to_visit: list[tuple[str, tuple[str, ...]]] = [(start, ())]
        while to_visit:
            directory, below = to_visit.pop()
            try:
                children = list_directory(directory)
            except FileNotFoundError:
                continue
            except Exception as exc:
                if self._is_cloud:
                    raise TableListingNotSupportedError(
                        f"Could not list the tables of the Delta database at "
                        f"{self._root_uri!r}: {type(exc).__name__}: {exc}"
                    ) from exc
                raise OrcapodDatabaseReadError(directory, full_prefix + below) from exc
            for name in children:
                if name == _DELTA_LOG_DIRECTORY:
                    found.append(prefix + below)
                else:
                    to_visit.append((join(directory, name), below + (component(name),)))
        return sorted(found)

    def _cloud_directory_lister(self) -> "Callable[[str], list[str]]":
        """Return a function listing the subdirectories of a cloud directory.

        Returns:
            A function from a directory (bucket and key, no scheme) to the names of
            its subdirectories.

        Raises:
            TableListingNotSupportedError: If no ``fsspec`` file system is available
                for this database's scheme.
        """
        import fsspec

        scheme = self._root_uri.split("://", 1)[0].lower()
        protocol = "s3" if scheme == "s3a" else scheme
        options = _s3fs_options(self._storage_options) if protocol == "s3" else {}
        endpoint = str(options.get("client_kwargs", {}).get("endpoint_url", ""))
        if protocol == "s3" and not endpoint:
            # Without an endpoint in the storage options the file system takes it
            # from the environment: fsspec's setting first, then the AWS one.
            endpoint = os.environ.get("FSSPEC_S3_ENDPOINT_URL") or os.environ.get(
                "AWS_ENDPOINT_URL", ""
            )
        if endpoint.lower().startswith("http://") and not _allows_http(
            self._storage_options
        ):
            # deltalake refuses a plain-HTTP endpoint unless AWS_ALLOW_HTTP is set;
            # the listing follows the same rule and sends no credentials over it.
            raise TableListingNotSupportedError(
                f"The Delta database at {self._root_uri!r} cannot list its tables: its "
                "endpoint uses plain HTTP and AWS_ALLOW_HTTP is not set."
            )
        try:
            # No listing cache: tables written since the last call must be seen.
            file_system = fsspec.filesystem(
                protocol, use_listings_cache=False, skip_instance_cache=True, **options
            )
        except Exception as exc:
            raise TableListingNotSupportedError(
                f"The Delta database at {self._root_uri!r} cannot list its tables: no "
                f"usable fsspec file system for {scheme!r} "
                f"({type(exc).__name__}: {exc})."
            ) from exc

        def list_directory(directory: str) -> list[str]:
            """Return the names of the subdirectories of a cloud directory."""
            entries = file_system.ls(directory, detail=True)
            return [
                str(entry["name"]).rstrip("/").rsplit("/", 1)[-1]
                for entry in entries
                if entry["type"] == "directory"
            ]

        return list_directory

    def table_exists(self, record_path: tuple[str, ...]) -> bool:
        """Return ``True`` if a Delta table exists at ``record_path``.

        This is a cheap existence check that does NOT load any records.

        Args:
            record_path: Path components identifying the table, relative to
                ``self.base_path``.

        Returns:
            ``True`` if a table exists at the given path, ``False`` otherwise.
        """
        # Checks again even when the table was found absent before (ITL-1173).
        self._absent_tables.discard(self._get_record_key(record_path))
        return self._get_delta_table(record_path) is not None

    def _ensure_record_id_column(
        self, arrow_data: pa.Table, record_id: str | bytes
    ) -> pa.Table:
        """Ensure the table has an record id column."""
        record_id = coerce_record_id(record_id)
        if self.RECORD_ID_COLUMN not in arrow_data.column_names:
            # Add record_id column at the beginning
            key_array = pa.array([record_id] * len(arrow_data), type=pa.large_binary())
            arrow_data = arrow_data.add_column(0, self.RECORD_ID_COLUMN, key_array)
        return arrow_data

    def _remove_record_id_column(self, arrow_data: pa.Table) -> pa.Table:
        """Remove the record id column if it exists."""
        if self.RECORD_ID_COLUMN in arrow_data.column_names:
            arrow_data = arrow_data.drop([self.RECORD_ID_COLUMN])
        return arrow_data

    def _handle_record_id_column(
        self, arrow_data: pa.Table, record_id_column: str | None = None
    ) -> pa.Table:
        """
        Handle record_id column based on add_record_id_column parameter.

        Args:
            arrow_data: Arrow table with record id column
            record_id_column: Control entry ID column inclusion:

        """
        if not record_id_column:
            # Remove the record id column
            return self._remove_record_id_column(arrow_data)

        # Rename record id column
        if self.RECORD_ID_COLUMN in arrow_data.column_names:
            schema = arrow_data.schema
            new_names = [
                record_id_column if name == self.RECORD_ID_COLUMN else name
                for name in schema.names
            ]
            return arrow_data.rename_columns(new_names)
        else:
            raise ValueError(
                f"Record ID column '{self.RECORD_ID_COLUMN}' not found in the table and cannot be renamed."
            )

    def _snapshot(
        self, record_key: str, delta_table: deltalake.DeltaTable
    ) -> _CommittedSnapshot:
        """Return the snapshot of ``delta_table``'s version, starting an empty one if needed.

        Args:
            record_key: The table's absolute record key.
            delta_table: The loaded table the snapshot must describe.

        Returns:
            The snapshot for the table's version; its parts are read on first use.
        """
        version = delta_table.version()
        snapshot = self._snapshots.get(record_key)
        if snapshot is None or snapshot.version != version:
            snapshot = _CommittedSnapshot(version)
            self._snapshots[record_key] = snapshot
        return snapshot

    @classmethod
    def _scan_ids(cls, delta_table: deltalake.DeltaTable) -> set[bytes]:
        """Read the record ids stored in ``delta_table`` (nulls left out).

        Args:
            delta_table: The loaded table.

        Returns:
            The stored record ids.

        Raises:
            Exception: The backend's error when the table's data cannot be read.
        """
        stored = _stored_arrow_schema(delta_table)
        if cls.RECORD_ID_COLUMN not in stored.names:
            logger.warning(f"Delta table missing {cls.RECORD_ID_COLUMN} column")
            return set()
        # Read through a one-column schema: the other columns' types are never
        # converted, so a logical type stored in a list cannot break the id read.
        id_field = stored.field(cls.RECORD_ID_COLUMN)
        id_schema = pa.schema(
            [id_field.with_type(arrow_utils.normalize_to_large_types(id_field.type))]
        )
        dataset = delta_to_pyarrow_dataset(delta_table, schema=id_schema)
        ids = dataset.to_table()[cls.RECORD_ID_COLUMN]
        return {record_id for record_id in ids.to_pylist() if record_id is not None}

    @staticmethod
    def _scan_rows(
        delta_table: deltalake.DeltaTable,
    ) -> tuple[pa.Table, "pa_ds.FileSystemDataset"]:
        """Read every row of ``delta_table`` as ``get_all_records`` reads it.

        Args:
            delta_table: The loaded table.

        Returns:
            The rows (view types normalized: PyArrow has no comparison kernels for
            ``string_view``, ENG-601) and the dataset they were read from.

        Raises:
            Exception: The backend's error when the table's data cannot be read.
        """
        # deltalake builds a FileSystemDataset; its data files identify a version.
        dataset = cast(
            "pa_ds.FileSystemDataset",
            delta_to_pyarrow_dataset(delta_table, as_large_types=True),
        )
        table = from_delta_layout(
            arrow_utils.normalize_table_view_types(dataset.to_table())
        )
        return table.combine_chunks(), dataset

    @staticmethod
    def _describe_read(
        snapshot: _CommittedSnapshot, dataset: "pa_ds.FileSystemDataset"
    ) -> None:
        """Record in ``snapshot`` the dataset its rows were read from."""
        snapshot.files = frozenset(dataset.files)
        snapshot.dataset_schema = dataset.schema
        snapshot.dataset_format = dataset.format
        snapshot.dataset_filesystem = dataset.filesystem

    def _stored_ids(self, record_path: tuple[str, ...]) -> set[bytes]:
        """Return the record ids stored at ``record_path`` (in the loaded version).

        Raises:
            OrcapodDatabaseReadError: If the table exists but its ids could not be read.
        """
        delta_table = self._get_delta_table(record_path)
        if delta_table is None:
            return set()
        snapshot = self._snapshot(self._get_record_key(record_path), delta_table)
        if snapshot.ids is None:
            try:
                snapshot.ids = self._scan_ids(delta_table)
            except TypeIdentityError:
                # The table is intact; the registered class differs (ITL-881).
                raise
            except Exception as e:
                raise self._read_failed(record_path) from e
        return snapshot.ids

    def _stored_rows(self, record_path: tuple[str, ...]) -> KeyIndex | None:
        """Return the indexed rows stored at ``record_path``, or ``None`` if no table.

        Raises:
            OrcapodDatabaseReadError: If the table exists but could not be read.
        """
        delta_table = self._get_delta_table(record_path)
        if delta_table is None:
            return None
        snapshot = self._snapshot(self._get_record_key(record_path), delta_table)
        if snapshot.rows is None:
            try:
                table, dataset = self._scan_rows(delta_table)
            except TypeIdentityError:
                # The table is intact; the registered class differs (ITL-881).
                raise
            except Exception as e:
                raise self._read_failed(record_path) from e
            snapshot.rows = KeyIndex(table)
            self._describe_read(snapshot, dataset)
        return snapshot.rows

    def add_record(
        self,
        record_path: tuple[str, ...],
        record_id: str | bytes,
        record: pa.Table,
        skip_duplicates: bool = False,
        flush: bool = False,
        schema_handling: Literal["merge", "error", "coerce"] = "error",
    ) -> None:
        data_with_record_id = self._ensure_record_id_column(record, record_id)
        self.add_records(
            record_path=record_path,
            records=data_with_record_id,
            record_id_column=self.RECORD_ID_COLUMN,
            schema_handling=schema_handling,
            skip_duplicates=skip_duplicates,
            flush=flush,
        )

    def require_storable(self, schema: pa.Schema) -> None:
        """Refuse a table schema with a column Delta cannot hold (ITL-1334).

        Most types Delta has no column type for are kept through a recorded type
        (``delta_arrow_types``); the rest (``decimal256``, an interval) are refused,
        as Delta's own schema conversion decides. Called by ``add_records`` before
        anything of the table is buffered, and by a job node when the database is
        attached to it (``contexts/stored.require_storable``), before anything runs.

        Args:
            schema: The Arrow schema of a table this database is to hold.

        Raises:
            UnstorableColumnError: For the first column Delta cannot hold, naming the
                column, its Arrow type and Delta's reason.
        """
        for field in schema:
            reason = delta_unstorable_reason(field)
            if reason is not None:
                raise UnstorableColumnError(
                    database_kind=type(self).__name__,
                    column=field.name,
                    arrow_type=field.type,
                    reason=reason,
                )

    def _refuse_unmergeable_append(
        self,
        record_path: tuple[str, ...],
        records: pa.Table,
        pending: pa.Table | None = None,
    ) -> None:
        """Refuse, before anything is buffered, rows a flush would refuse (ITL-1334).

        Checked against a batch pending for the table (``_column_change``) and against
        the stored table (``_append_refusal``, which the flush repeats against the
        version it loads): what would not read back as written, would break the
        table, or the Delta writer would reject. A stored table that cannot be loaded
        is left to the flush, which reports it (``OrcapodDatabaseReadError``).

        Args:
            record_path: The table the rows are for.
            records: The rows about to be added.
            pending: The batch pending for the table as it would be queued with them
                (``schema_handling="merge"`` re-types it), or ``None`` for the one
                queued now.

        Raises:
            InputValidationError: For the first such column, naming it and the path.
        """
        if pending is None:
            pending = self._pending_batches.get(self._get_record_key(record_path))
        self._refuse_declared_nulls(record_path, records)
        refusal = None
        if pending is not None:
            held = {f.name: f for f in pending.schema}
            refusal = next(
                (
                    (f.name, change)
                    for f in records.schema
                    if f.name in held
                    and (change := _column_change(held[f.name], f)) is not None
                ),
                None,
            )
            # A pending column declared non-nullable fails the write of the batch
            # the rows join, whether or not a table is stored yet.
            violation = _non_null_violation(held, records) if refusal is None else None
            if violation is not None:
                name, omitted = violation
                refusal = (
                    name,
                    f"{name} is queued as non-nullable, and the rows "
                    f"{'omit it' if omitted else 'hold a null'}",
                )
        if refusal is None:
            try:
                delta_table = self._get_delta_table(record_path)
            except OrcapodDatabaseReadError:
                delta_table = None
            if delta_table is not None:
                refusal = _append_refusal(delta_table, records)
        if refusal is not None:
            column, reason = refusal
            raise InputValidationError(
                f"Cannot append to the stored table "
                f"{self._get_table_uri(record_path)!r}: column {column!r}: {reason}. "
                "The stored rows are unchanged and nothing was written."
            )

    def _refuse_declared_nulls(
        self, record_path: tuple[str, ...], rows: pa.Table
    ) -> None:
        """Refuse rows holding a null in a column they declare non-nullable (ITL-1334).

        Delta's writer refuses such a batch, a first one included, so every row
        queued with them would fail. ``add_records`` checks the rows as given, before
        any ``schema_handling`` casts them (a cast to a nullable pending column would
        hide the declaration), and again as they will be queued.

        Args:
            record_path: The table the rows are for.
            rows: The rows.

        Raises:
            InputValidationError: For the first such column, naming it.
        """
        violation = _non_null_violation({f.name: f for f in rows.schema}, rows)
        if violation is not None:
            name = violation[0]
            raise InputValidationError(
                f"Cannot append to the stored table "
                f"{self._get_table_uri(record_path)!r}: column {name!r}: {name} is "
                "declared non-nullable, and the rows hold a null. The stored rows are "
                "unchanged and nothing was written."
            )

    def _refuse_changed_attributes(
        self, record_path: tuple[str, ...], schema: pa.Schema
    ) -> None:
        """Refuse rows whose field attributes differ from the held columns', compared
        before any ``schema_handling`` casts them (ITL-1334).

        A ``"merge"`` or ``"coerce"`` casts the rows to the pending batch's schema,
        which would give them the pending columns' field metadata and nested
        attributes: the rows would then be stored, and read back, with another
        column's attributes. So the field tree of each column (``_column_change``) is
        compared as given, against the pending batch and the stored table, in every
        mode, as an append without schema handling is.

        Args:
            record_path: The table the rows are for.
            schema: The schema of the rows as given.

        Raises:
            InputValidationError: For the first such column, naming it and the path.
        """
        held: list[dict[str, pa.Field[Any]]] = []
        pending = self._pending_batches.get(self._get_record_key(record_path))
        if pending is not None:
            held.append({f.name: f for f in pending.schema})
        try:
            delta_table = self._get_delta_table(record_path)
        except OrcapodDatabaseReadError:
            delta_table = None
        if delta_table is not None:
            held.append(_held_columns(delta_table))
        for given in held:
            for field in schema:
                if field.name not in given:
                    continue
                change = _column_change(given[field.name], field)
                if change is not None:
                    raise InputValidationError(
                        f"Cannot append to the stored table "
                        f"{self._get_table_uri(record_path)!r}: column "
                        f"{field.name!r}: {change}. The stored rows are unchanged and "
                        "nothing was written."
                    )

    def _refuse_untyped_null_columns(
        self, record_path: tuple[str, ...], schema: pa.Schema
    ) -> None:
        """Refuse a ``null``-typed part that nothing stored gives a type (ITL-1334).

        Delta has no ``null`` type. A ``null``-typed value, at any depth, fits where
        the stored table (or a batch pending for it) already has a typed column or
        nested field at the same path, and the append check leaves it out. A ``null``
        part at a path nothing gives a type (a new column, a new struct field, the items
        of a new list) cannot be written, so it is refused here, before anything is
        buffered, instead of failing the flush without naming it.

        Args:
            record_path: The table the rows are for.
            schema: The schema of the rows about to be added.

        Raises:
            UnstorableColumnError: For the first such column, naming the path.
        """
        untyped = [f for f in schema if _holds_null_type(f.type)]
        if not untyped:
            return
        try:
            delta_table = self._get_delta_table(record_path)
        except OrcapodDatabaseReadError:
            # A stored table that cannot be loaded is left to the flush to report.
            return
        stored = _stored_arrow_schema(delta_table) if delta_table is not None else None
        pending = self._pending_batches.get(self._get_record_key(record_path))
        for field in untyped:
            known = [
                given.field(given.get_field_index(field.name)).type
                for given in (stored, pending.schema if pending is not None else None)
                if given is not None and given.get_field_index(field.name) >= 0
            ]
            # A ``null`` part is typed when the stored table or the pending batch
            # types its path; the flush writes it as typed nulls (``_conformed_array``).
            unresolved = _untyped_null_paths(field.type, None, field.name)
            for given_type in known:
                typed_here = set(unresolved) - set(
                    _untyped_null_paths(field.type, given_type, field.name)
                )
                unresolved = [p for p in unresolved if p not in typed_here]
            if unresolved:
                raise UnstorableColumnError(
                    database_kind=type(self).__name__,
                    column=field.name,
                    arrow_type=field.type,
                    reason=(
                        "Delta has no null type, and nothing stored gives "
                        f"{', '.join(repr(p) for p in unresolved)} a type"
                    ),
                )

    def add_records(
        self,
        record_path: tuple[str, ...],
        records: pa.Table,
        record_id_column: str | None = None,
        skip_duplicates: bool = False,
        flush: bool = False,
        schema_handling: Literal["merge", "error", "coerce"] = "error",
    ) -> None:
        """
        Insert new records. By default, never overwrites existing records.

        Args:
            schema_handling: How to handle schema differences
            skip_duplicates: If True, skip records with IDs that already exist.
                           If False, raise error on duplicates.
            flush: Whether to flush immediately after the insert

        Raises:
            ValueError: If any record IDs already exist and skip_duplicates=False
        """
        self._validate_record_path(record_path)

        if records.num_rows == 0:
            return

        # Convert view types up front so the whole write path (group_by dedup,
        # filters, storage) avoids string_view's missing kernels (ENG-601).
        records = arrow_utils.normalize_table_view_types(records)

        if record_id_column is None:
            record_id_column = records.column_names[0]

        # Step 1: Validate that record ID column exist
        if record_id_column not in records.column_names:
            raise ValueError(
                f"Specified record ID column {record_id_column} not found in input table {records.column_names}"
            )

        # rename record ID column to a standard name
        if record_id_column != self.RECORD_ID_COLUMN:
            rename_map = {record_id_column: self.RECORD_ID_COLUMN}
            total_name_map = {k: rename_map.get(k, k) for k in records.column_names}
            records = records.rename_columns(total_name_map)

        # A column Delta cannot hold is refused before anything is buffered (ITL-1334).
        self.require_storable(records.schema)
        self._refuse_untyped_null_columns(record_path, records.schema)

        # Step 2: Deduplicate within input table (keep last occurrence)
        deduplicated_table = self._deduplicate_within_table(records)

        # Step 3: Handle conflicts based on skip_duplicates setting
        if skip_duplicates:
            filtered_table = self._filter_existing_records(
                record_path, deduplicated_table
            )
            if filtered_table.num_rows == 0:
                logger.debug("All records were duplicates, nothing to insert")
                return None
        else:
            # Check for conflicts - insert never allows duplicates when skip_duplicates=False
            # self._check_all_conflicts(record_path, deduplicated_table)
            filtered_table = deduplicated_table

        # Step 4: the rows' field attributes, compared as given: a merge or a
        # coercion casts them to the pending batch's schema, which would give them
        # its field metadata and nested attributes instead (ITL-1334).
        self._refuse_changed_attributes(record_path, filtered_table.schema)
        self._refuse_declared_nulls(record_path, filtered_table)

        # Step 5: Handle schema compatibility (the pending batch is only staged)
        schema_compatible_table, staged_pending = self._handle_schema_compatibility(
            record_path, filtered_table, schema_handling
        )

        # Step 6: what the flush would refuse is refused now, on the rows that are
        # queued: after duplicates are dropped and any requested schema handling, the
        # rows already queued included when a merge re-types them. Nothing queued
        # changes unless both pass (ITL-1334).
        self._refuse_unmergeable_append(
            record_path, schema_compatible_table, staged_pending
        )
        if staged_pending is not None:
            self._refuse_unmergeable_append(record_path, staged_pending, staged_pending)
            self._pending_batches[self._get_record_key(record_path)] = staged_pending

        # Step 7: Add to pending batch (no overwrite logic needed)
        self._add_to_pending_batch(record_path, schema_compatible_table)

        # Step 8: Flush if asked to; otherwise commit this table alone once its
        # pending batch reaches batch_size. Only the full table: committing every
        # pending table here would commit another table's rows, such as pipeline
        # records whose results are still pending, out of their writer's order
        # (ITL-919).
        if flush:
            self.flush()
        elif self._should_auto_flush(record_path):
            self.flush_batch(record_path)

    def _deduplicate_within_table(self, table: pa.Table) -> pa.Table:
        # TODO: consider erroring out if duplicates are found
        """Remove duplicates within the input table, keeping the last occurrence."""
        if table.num_rows <= 1:
            return table

        # Create row indices
        indices = pa.array(range(table.num_rows))

        # Add row index column temporarily
        table_with_indices = table.add_column(0, self.ROW_INDEX_COLUMN, indices)

        # Group by RECORD_ID_COLUMN and get the maximum row index for each group
        # This gives us the last occurrence of each ID
        grouped = table_with_indices.group_by([self.RECORD_ID_COLUMN]).aggregate(
            [(self.ROW_INDEX_COLUMN, "max")]
        )

        # Get the row indices to keep - the aggregated column name has "_max" suffix
        max_indices_column = f"{self.ROW_INDEX_COLUMN}_max"
        indices_to_keep = grouped[max_indices_column].to_pylist()

        # Filter original table to keep only these rows
        mask = pc.is_in(indices, pa.array(indices_to_keep))
        return table.filter(mask)

    def _filter_existing_records(
        self, record_path: tuple[str, ...], table: pa.Table
    ) -> pa.Table:
        """Filter out records that already exist (for skip_duplicates=True)."""
        input_ids = set(table[self.RECORD_ID_COLUMN].to_pylist())
        record_key = self._get_record_key(record_path)

        # Get IDs that already exist in pending batch or Delta table
        existing_in_pending = input_ids.intersection(
            self._pending_record_ids[record_key]
        )
        existing_in_delta = input_ids.intersection(self._stored_ids(record_path))
        all_existing = existing_in_pending.union(existing_in_delta)

        if not all_existing:
            return table  # No duplicates found

        # Filter out existing records
        mask = pc.invert(
            pc.is_in(table[self.RECORD_ID_COLUMN], pa.array(list(all_existing)))
        )
        filtered = table.filter(mask)

        logger.debug(f"Skipped {len(all_existing)} duplicate records")
        return filtered

    def _check_all_conflicts(
        self, record_path: tuple[str, ...], table: pa.Table
    ) -> None:
        """Check for conflicts with both pending batch and Delta table."""
        input_ids = set(table[self.RECORD_ID_COLUMN].to_pylist())
        record_key = self._get_record_key(record_path)
        # Check conflicts with pending batch
        pending_conflicts = input_ids.intersection(self._pending_record_ids[record_key])
        if pending_conflicts:
            raise ValueError(
                f"Cannot insert records with IDs that already exist in pending batch: {pending_conflicts}. "
                f"Use skip_duplicates=True to skip existing records or update() method to overwrite."
            )

        # Check conflicts with Delta table
        existing_ids = self._stored_ids(record_path)
        delta_conflicts = input_ids.intersection(existing_ids)
        if delta_conflicts:
            raise ValueError(
                f"Cannot insert records with IDs that already exist in Delta table: {delta_conflicts}. "
                f"Use skip_duplicates=True to skip existing records or update() method to overwrite."
            )

    def _handle_schema_compatibility(
        self, record_path: tuple[str, ...], table: pa.Table, schema_handling: str
    ) -> tuple[pa.Table, pa.Table | None]:
        """Handle schema differences between input and pending batch.

        Nothing queued is changed here: a ``"merge"`` returns the pending batch cast to
        the unified schema, for the caller to queue once the rows are accepted.

        Returns:
            The input as it is to be queued, and the re-typed pending batch (``None``
            when the pending batch stays as it is).
        """
        record_key = self._get_record_key(record_path)
        pending_batch = self._pending_batches.get(record_key)
        if pending_batch is None:
            return table, None

        if pending_batch.schema.equals(table.schema):
            # TODO: perform more careful check
            return table, None

        if schema_handling == "error":
            raise ValueError(
                f"Schema mismatch between input {table.schema} and pending batch {pending_batch.schema}"
            )
        elif schema_handling == "merge":
            try:
                # Unify schemas and cast both input table and pending batch
                unified_schema = pa.unify_schemas([pending_batch.schema, table.schema])

                # The input and the pending batch, cast to the unified schema.
                return table.cast(unified_schema), pending_batch.cast(unified_schema)
            except Exception as e:
                # TODO: perform more careful error check
                raise ValueError(f"Cannot merge schemas: {e}")
        elif schema_handling == "coerce":
            try:
                # Coerce input table to match existing pending batch schema
                return table.cast(pending_batch.schema), None
            except Exception as e:
                raise ValueError(f"Cannot coerce schema: {e}")
        else:
            raise ValueError(f"Unknown schema handling: {schema_handling}")

    def _handle_delta_schema_compatibility(
        self, record_path: tuple[str, ...], table: pa.Table, schema_handling: str
    ) -> pa.Table:
        """Handle schema differences between input and Delta table for updates."""
        record_key = self._get_record_key(record_path)
        if self._delta_table_cache.get(record_key) is None:
            return table

        delta_table = self._delta_table_cache[record_key]

        try:
            # Get Delta table schema and convert from arro3 to pyarrow
            arro3_schema = delta_table.schema().to_arrow()
            delta_schema = pa.schema(arro3_schema)  # type: ignore
        except TypeIdentityError:
            # Stored and registered user-class shapes disagree: never report
            # this as missing data (ITL-881).
            raise
        except Exception as e:
            logger.warning(f"Could not get Delta table schema: {e}")
            return table

        if delta_schema.equals(table.schema):
            return table

        if schema_handling == "error":
            raise ValueError("Schema mismatch between input and Delta table")
        elif schema_handling == "merge":
            try:
                # Unify schemas - this might require adding null columns
                unified_schema = pa.unify_schemas([delta_schema, table.schema])
                return table.cast(unified_schema)
            except Exception as e:
                raise ValueError(f"Cannot merge schemas: {e}")
        elif schema_handling == "coerce":
            try:
                # Coerce input table to match Delta table schema
                return table.cast(delta_schema)
            except Exception as e:
                raise ValueError(f"Cannot coerce schema: {e}")
        else:
            raise ValueError(f"Unknown schema handling: {schema_handling}")

    def _add_to_pending_batch(self, record_path: tuple[str, ...], table: pa.Table):
        """Add table to pending batch."""
        # Add row index column for internal tracking
        record_key = self._get_record_key(record_path)
        pending_batch = self._pending_batches.get(record_key)
        if pending_batch is None:
            self._pending_batches[record_key] = table
        else:
            # Kept at O(log n) record batches, and its indexes extended in place, so a
            # lookup's cost does not grow with the rows pending (ITL-919).
            grown = compact_tail(pa.concat_tables([pending_batch, table]))
            self._pending_batches[record_key] = grown
            index = self._pending_indexes.get(record_key)
            if index is not None and index.table is pending_batch:
                index.append(table, grown)

        pending_ids = cast(list[bytes], table[self.RECORD_ID_COLUMN].to_pylist())
        self._pending_record_ids[record_key].update(pending_ids)

    def _should_auto_flush(self, record_path: tuple[str, ...]) -> bool:
        """Check if auto-flush should be triggered."""
        record_key = self._get_record_key(record_path)
        return (
            self._pending_batches.get(record_key) is not None
            and self._pending_batches[record_key].num_rows >= self.batch_size
        )

    def _pending_rows(self, record_path: tuple[str, ...]) -> KeyIndex | None:
        """Return the indexed pending rows at ``record_path``, or ``None`` if none.

        The indexes are built on first use and describe the current pending batch
        only (``_add_to_pending_batch`` extends them; a flush or schema change
        replaces the batch, and with it the indexes).
        """
        record_key = self._get_record_key(record_path)
        pending = self._pending_batches.get(record_key)
        if pending is None or pending.num_rows == 0:
            return None
        index = self._pending_indexes.get(record_key)
        if index is None or index.table is not pending:
            index = KeyIndex(pending)
            self._pending_indexes[record_key] = index
        return index

    def _without_stored_ids(
        self, record_path: tuple[str, ...], pending: pa.Table
    ) -> pa.Table | None:
        """Return the pending rows a flush would insert: those whose id is not stored.

        A flush inserts only rows whose record id is not stored yet (the merge is
        ``when_not_matched_insert_all``), so a pending row with a stored id is
        never returned by a read: the stored row is. The stored ids are read even
        when ``pending`` is empty, so a read with rows pending raises
        ``OrcapodDatabaseReadError`` whenever the stored ids cannot be read.

        Args:
            record_path: Path components relative to this database view.
            pending: Pending rows of the table at ``record_path``.

        Returns:
            The rows whose record id is not stored, or ``None`` if there are none.
        """
        stored_ids = self._stored_ids(record_path)
        if stored_ids and pending.num_rows > 0:
            # The mask is built from the pending ids against the cached set, so its
            # cost follows the pending rows, not the stored table. A null id is
            # never matched by the merge (``null = null`` is not true), so its row
            # is kept.
            keep = [
                record_id is None or record_id not in stored_ids
                for record_id in pending[self.RECORD_ID_COLUMN].to_pylist()
            ]
            if not all(keep):
                pending = pending.filter(pa.array(keep, type=pa.bool_()))
        return pending if pending.num_rows > 0 else None

    def _pending_not_stored(self, record_path: tuple[str, ...]) -> pa.Table | None:
        """Return every pending row a flush would insert at ``record_path``, or ``None``.

        Args:
            record_path: Path components relative to this database view.

        Returns:
            The pending rows whose record id is not stored, or ``None``.
        """
        pending = self._pending_rows(record_path)
        if pending is None:
            return None
        return self._without_stored_ids(record_path, pending.table)

    def _combine_stored_and_pending(
        self,
        stored: pa.Table | None,
        pending: pa.Table | None,
        record_id_column: str | None,
    ) -> pa.Table | None:
        """Combine stored rows with pending rows into one read result.

        Combined rows have the schema a flush would give them: both parts take
        their large layouts (a stored table reads back large), a column missing
        on one side is null-filled, a ``null``-typed column takes the other
        side's type, nullability is merged (nullable if either side is), and
        struct children are merged, as Delta's merge evolves the stored schema.
        A column whose types still differ raises ``pyarrow.ArrowTypeError``;
        nothing is widened. A single part is returned as it is.

        Args:
            stored: Rows read from the Delta table, or ``None``.
            pending: Pending rows that are not stored (see ``_pending_not_stored``),
                already filtered by the read's condition, or ``None``.
            record_id_column: Name to give the record id column, or ``None`` to
                drop it.

        Returns:
            The combined rows, or ``None`` if there are none.
        """
        parts = [t for t in (stored, pending) if t is not None and t.num_rows > 0]
        if not parts:
            return None
        if len(parts) == 1:
            table = parts[0]
        else:
            table = pa.concat_tables(
                [self._large_layout(t) for t in parts], promote_options="default"
            )
        return self._handle_record_id_column(table, record_id_column)

    @staticmethod
    def _large_layout(table: pa.Table) -> pa.Table:
        """Return ``table`` with every column in its large layout.

        Only layouts change (``string`` to ``large_string``, ``list`` to
        ``large_list``, recursively); ``null`` stays ``null``, and names,
        nullability and metadata are kept.

        Args:
            table: The table to normalize.

        Returns:
            The table with large-normalized column types.
        """
        schema = table.schema
        for index, field in enumerate(table.schema):
            schema = schema.set(
                index,
                field.with_type(
                    arrow_utils.normalize_to_large_types(field.type, keep_null=True)
                ),
            )
        return table if schema.equals(table.schema) else table.cast(schema)

    def get_all_records(
        self,
        record_path: tuple[str, ...],
        record_id_column: str | None = None,
        retrieve_pending: bool = True,
    ) -> pa.Table | None:
        """
        Get all records from both pending batch and Delta table.

        Every read method returns the rows a flush would leave: the stored rows
        plus the pending rows whose record id is not stored yet, as seen through
        any handle of this ``at()`` family.

        Returns:
            Combined Arrow table with all records, or None if no records exist

        Raises:
            OrcapodDatabaseReadError: If the Delta table exists but could not be read.
        """
        tables_to_combine = []

        # Add Delta table data
        if (delta_table := self._get_delta_table(record_path)) is not None:
            try:
                delta_table_data = delta_to_pyarrow_dataset(
                    delta_table, as_large_types=True
                ).to_table()
            except TypeIdentityError:
                # Stored and registered user-class shapes disagree: the table is
                # intact, so report the identity mismatch itself (ITL-881).
                raise
            except Exception as e:
                raise self._read_failed(record_path) from e
            delta_table_data = from_delta_layout(delta_table_data)
            if delta_table_data.num_rows > 0:
                tables_to_combine.append(delta_table_data)

        # Add the pending rows a flush would insert
        pending = self._pending_not_stored(record_path) if retrieve_pending else None
        stored = pa.concat_tables(tables_to_combine) if tables_to_combine else None
        return self._combine_stored_and_pending(stored, pending, record_id_column)

    def get_records_with_column_value(
        self,
        record_path: tuple[str, ...],
        column_values: Collection[tuple[str, Any]] | Mapping[str, Any],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        if flush:
            self.flush_batch(record_path)

        pair_list: list[tuple[str, Any]]
        if isinstance(column_values, Mapping):
            # Convert Mapping to list of tuples
            pair_list = list(cast(Mapping[str, Any], column_values).items())
        else:
            # Ensure it's a list of tuples
            pair_list = cast(list[tuple[str, Any]], list(column_values))

        expressions = [pc.field(c) == v for c, v in pair_list]
        combined_expression = expressions[0]
        for next_expression in expressions[1:]:
            combined_expression = combined_expression & next_expression

        pending = self._pending_rows(record_path)
        if pending is not None:
            # Rows are pending: the stored ids decide which of them a read returns.
            self._stored_ids(record_path)
        stored = self._stored_rows(record_path)

        # A side without a queried column holds null there once flushed, and
        # ``column == value`` never matches null, so it contributes no rows. A
        # column neither side has is unknown: the filter raises as before.
        queried = {c for c, _ in pair_list}
        stored_columns = set() if stored is None else set(stored.table.column_names)
        pending_columns = set() if pending is None else set(pending.table.column_names)
        if queried <= stored_columns | pending_columns:
            if not queried <= pending_columns:
                pending = None
            if not queried <= stored_columns:
                stored = None

        # Both sides are answered from indexes (ITL-919): no re-read, no scan.
        pending_matches = (
            None
            if pending is None
            else self._without_stored_ids(
                record_path, matching_rows(pending, pair_list, combined_expression)
            )
        )
        result = (
            None
            if stored is None
            else matching_rows(stored, pair_list, combined_expression)
        )
        return self._combine_stored_and_pending(
            result, pending_matches, record_id_column
        )

    def get_record_by_id(
        self,
        record_path: tuple[str, ...],
        record_id: str | bytes,
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        """
        Get a specific record by record_id with schema preservation.

        Args:
            record_path: Tuple of path components
            record_id: Unique identifier for the record

        Returns:
            Arrow table for the record or None if not found
        """

        if flush:
            self.flush_batch(record_path)

        record_id = coerce_record_id(record_id)
        # Serve a pending record only if its id is not stored: a flush keeps the
        # stored row and drops the pending one.
        record_key = self._get_record_key(record_path)
        if record_id in self._pending_record_ids[
            record_key
        ] and record_id not in self._stored_ids(record_path):
            # Return the pending record after removing the entry id column
            pending = self._pending_rows(record_path)
            assert pending is not None, "Pending batch should not be None"
            filtered_table = self._rows_with_ids(
                pending,
                [record_id],
                pending.table.schema.field(self.RECORD_ID_COLUMN).type,
            )
            if filtered_table.num_rows != 1:
                raise ValueError(
                    f"Expected exactly one record in pending batch with record ID {record_id}, but found {filtered_table.num_rows}"
                )
            return self._handle_record_id_column(filtered_table, record_id_column)

        # Now check the stored rows (the indexed snapshot, ITL-919)
        stored = self._stored_rows(record_path)
        if stored is None:
            return None
        result = self._rows_with_ids(stored, [record_id])
        if result.num_rows == 0:
            return None

        # Handle (remove/rename) the record id column before returning
        return self._handle_record_id_column(result, record_id_column)

    def get_records_by_ids(
        self,
        record_path: tuple[str, ...],
        record_ids: Collection[str | bytes] | pl.Series | pa.Array,
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> "pa.Table | None":
        """
        Retrieve records by entry IDs as a single table with schema preservation.

        Args:
            record_path: Tuple of path components
            record_ids: Entry IDs to retrieve

        Returns:
            Arrow table containing all found records with original schema, or None if no records found
        """
        _ = self._get_record_key(record_path)
        if flush:
            self.flush_batch(record_path)

        # Convert input to a list of bytes for consistency

        if isinstance(record_ids, pl.Series):
            record_ids_list = [coerce_record_id(r) for r in record_ids.to_list()]
        elif isinstance(record_ids, (pa.Array, pa.ChunkedArray)):
            record_ids_list = [coerce_record_id(r) for r in record_ids.to_pylist()]
        elif isinstance(record_ids, Collection):
            record_ids_list = [coerce_record_id(r) for r in record_ids]
        else:
            raise TypeError(
                f"record_ids must be Collection[str | bytes], pl.Series, or pa.Array, got {type(record_ids)}"
            )
        if len(record_ids) == 0:
            return None

        stored = self._stored_rows(record_path)
        pending = self._pending_rows(record_path)
        result = None
        if stored is not None:
            result = self._rows_with_ids(stored, record_ids_list)

        pending_matches = None
        if pending is not None:
            id_type = pending.table.schema.field(self.RECORD_ID_COLUMN).type
            # Reading the stored ids drops the pending rows a flush would not
            # insert; it raises OrcapodDatabaseReadError when the stored table cannot be read.
            pending_matches = self._without_stored_ids(
                record_path, self._rows_with_ids(pending, record_ids_list, id_type)
            )
        return self._combine_stored_and_pending(
            result, pending_matches, record_id_column
        )

    def to_config(self, db_registry: Any = None) -> dict[str, Any]:
        """Serialize database configuration to a JSON-compatible dict."""
        config: dict[str, Any] = {
            "type": "delta_table",
            "root_uri": self._root_uri,  # renamed from "base_path"
            "base_path": list(self._path_prefix),  # new: relative prefix tuple
            "batch_size": self.batch_size,
            "max_hierarchy_depth": self.max_hierarchy_depth,
            "allow_schema_evolution": self.allow_schema_evolution,
        }
        if self._storage_options:
            config["storage_options"] = self._storage_options
        return config

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "DeltaTableDatabase":
        """Reconstruct a DeltaTableDatabase from a config dict.

        Supports both the current format (``"root_uri"`` for the storage root,
        ``"base_path"`` as a list for the scoping prefix) and the legacy format
        produced before ENG-341 (``"base_path"`` as a URI string, no prefix).
        """
        if "root_uri" in config:
            # Current format (post-ENG-341)
            root_uri = config["root_uri"]
            base_path_value = config.get("base_path", [])
            _path_prefix = (
                tuple(base_path_value) if isinstance(base_path_value, list) else ()
            )
        else:
            # Legacy format (pre-ENG-341): "base_path" was the root URI string
            root_uri = config["base_path"]
            _path_prefix = ()
        return cls(
            base_path=root_uri,
            storage_options=config.get("storage_options"),
            create_base_path=True,
            batch_size=config.get("batch_size", 1000),
            max_hierarchy_depth=config.get("max_hierarchy_depth", 10),
            allow_schema_evolution=config.get("allow_schema_evolution", True),
            _path_prefix=_path_prefix,
        )

    @property
    def keeps_field_metadata(self) -> bool:
        """Whether this database keeps Arrow field metadata: it does, on every write
        path including schema evolution (ITL-1019)."""
        return True

    def flush(self) -> None:
        """Flush all pending batches.

        Every pending batch is attempted, even after one fails, and each failure is
        logged. What happens to a failed batch depends on why it failed:

        - Refused by its stored table (``InputValidationError``): the table, as
          another writer committed it after the rows were queued, cannot take them
          (``_append_refusal``). The batch is dropped from the queue, since every later
          flush would refuse it too; the error says how many rows were not written.
          Correct the rows and add them again (ITL-1334).
        - Any other failure (a table that cannot be loaded, a failed write): the batch
          stays queued, so a later flush tries it again.

        Raises:
            OrcapodDatabaseReadError: If a batch's existing table could not be loaded. It is
                raised as itself; every other batch that failed in this flush is
                added to it as a note.
            InputValidationError: If no batch failed with ``OrcapodDatabaseReadError`` but a
                batch was refused by its stored table (dropped from the queue, above).
                Other failed batches are added to it as notes.
            RuntimeError: If batches failed with other errors only, naming the
                failed batches, with the first failure as its ``__cause__``.
        """
        # A flush is where this database looks at the store again (ITL-1173).
        self._absent_tables.clear()
        errors: list[tuple[str, Exception]] = []
        for record_key in list(self._pending_batches.keys()):
            try:
                self._flush_by_key(record_key)
            except Exception as e:
                logger.error(f"Error flushing batch for {record_key}: {e}")
                errors.append((record_key, e))
        if errors:
            database_error = _database_error_to_raise([e for _, e in errors])
            if database_error is not None:
                # Keep every other failure visible on the error that is raised.
                for record_key, other in errors:
                    if other is not database_error:
                        database_error.add_note(
                            f"Another batch also failed to flush ({record_key}): "
                            f"{type(other).__name__}: {other}"
                            + "".join(
                                f" {note}" for note in getattr(other, "__notes__", [])
                            )
                        )
                raise database_error
            failed_keys = [k for k, _ in errors]
            raise RuntimeError(
                f"Failed to flush {len(errors)} batch(es): {failed_keys}"
            ) from errors[0][1]

    def flush_batch(self, record_path: tuple[str, ...]) -> None:
        """
        Flush pending batch for a specific source path.

        Args:
            record_path: Tuple of path components (relative to this instance's prefix)
        """
        record_key = self._get_record_key(record_path)
        self._flush_by_key(record_key)

    def _flush_by_key(self, record_key: str) -> None:
        """Flush a pending batch identified by its absolute record_key (prefix + path).

        This method is safe to call from any instance (root or scoped) since the
        key already encodes the full path. The URI is derived directly from the key
        without applying any instance-level prefix.
        """
        logger.debug("Flushing triggered!!")
        self._absent_tables.discard(record_key)

        if (
            record_key not in self._pending_batches
            or not self._pending_batches[record_key]
        ):
            return

        # Get all pending records
        pending_batch = self._pending_batches.pop(record_key)
        pending_ids = self._pending_record_ids.pop(record_key, set())

        # Derive the absolute table URI directly from the key (already includes prefix).
        abs_path_components = tuple(record_key.split("/"))
        if not self._is_cloud:
            path = self._local_root
            for component in abs_path_components:
                path = path / self._sanitize_path_component(component)
            table_uri = str(path)
        else:
            table_uri = self._root_uri.rstrip("/") + "/" + record_key

        try:
            # Combine all tables in the batch. Convert view types so they never
            # reach storage (ENG-601).
            original_table = arrow_utils.normalize_table_view_types(
                pending_batch.combine_chunks()
            )

            # Ensure parent directory exists (local only)
            if not self._is_cloud:
                os.makedirs(table_uri, exist_ok=True)

            # Every commit can fail because another writer committed first: a create
            # finds the table created, a MERGE finds rows added since it loaded the
            # table. Each attempt loads the table again and decides against what is
            # committed then, so an id another writer committed meanwhile is never
            # stored a second time (ITL-1377, ITL-1384).
            attempt = 1
            while True:
                try:
                    delta_table, base, inserted_ids = self._commit_once(
                        record_key, table_uri, abs_path_components, original_table
                    )
                    break
                except _LostCommitRace as lost:
                    # What this handle cached of the table describes a version
                    # another writer has moved past.
                    self._delta_table_cache.pop(record_key, None)
                    self._snapshots.pop(record_key, None)
                    if attempt >= _COMMIT_ATTEMPTS:
                        raise lost.error
                    logger.debug(
                        f"Commit to {record_key} lost to another writer "
                        f"(attempt {attempt}); retrying"
                    )
                    _sleep_before_retry(attempt)
                    attempt += 1

        except InputValidationError as e:
            # The stored table refuses these rows (``_append_refusal``,
            # ``_conformed_to_stored``): they would be refused by every later flush
            # too, so they leave the queue, as rows refused when added never enter
            # it (ITL-1334).
            # The refusal was decided against a version this handle may not have
            # cached: what it cached of the table is dropped, so its next read loads
            # the version another writer committed.
            self._delta_table_cache.pop(record_key, None)
            self._snapshots.pop(record_key, None)
            # The discard notice is part of the message, so it survives wherever the
            # error is reported: the log, and the note ``flush`` puts on another
            # batch's error when that one is raised instead.
            refused = InputValidationError(
                f"{e} The {pending_batch.num_rows} rows queued for table "
                f"{record_key!r} were not written and are no longer queued: correct "
                "them and add them again."
            )
            logger.error(f"Refused batch for {record_key}: {refused}")
            raise refused from e
        except Exception as e:
            logger.error(f"Error flushing batch for {record_key}: {e}")
            # Put the tables back in the pending queue
            self._pending_batches[record_key] = pending_batch
            self._pending_record_ids[record_key] = pending_ids
            raise

        # The rows are committed: nothing below may put them back in the queue.
        try:
            committed = (
                delta_table
                if delta_table is not None
                else deltalake.DeltaTable(
                    table_uri, storage_options=self._storage_options or None
                )
            )
            self._delta_table_cache[record_key] = committed
            self._snapshots[record_key] = self._extend_snapshot(
                base, committed, inserted_ids
            )
        except Exception as e:
            # The next read loads the table again and reports a failure itself.
            logger.warning(f"Could not reload {record_key} after committing: {e}")
            self._delta_table_cache.pop(record_key, None)
            self._snapshots.pop(record_key, None)

    def _commit_once(
        self,
        record_key: str,
        table_uri: str,
        abs_path_components: tuple[str, ...],
        original_table: pa.Table,
    ) -> tuple[deltalake.DeltaTable | None, _CommittedSnapshot, set[bytes]]:
        """Commit ``original_table`` to the table as it is now, in one attempt.

        A table that does not exist is created, and the create fails if another
        writer created it first. Rows for an existing table are committed by a
        MERGE that inserts only the ids the table does not hold: Delta refuses
        that commit when another writer added rows after the table was loaded,
        which a plain append never is (ITL-1377).

        Args:
            record_key: The table's absolute record key.
            table_uri: The table's location.
            abs_path_components: The table's absolute path, for errors.
            original_table: The pending rows, view types converted.

        Returns:
            The table the rows were merged through (moved to the version it
            committed), or ``None`` for a table this attempt created; the snapshot
            of the version the commit built on; and the non-null ids it inserted.

        Raises:
            _LostCommitRace: If another writer committed first; the caller loads
                the table again and retries.
            OrcapodDatabaseReadError: If the table exists but cannot be loaded, or
                its stored ids cannot be read.
            InputValidationError: If the stored table refuses the rows.
        """
        from deltalake.exceptions import CommitFailedError, TableNotFoundError

        # A column whose type Delta would not give back is written in its integer
        # form with its type recorded (ITL-928).
        combined_table = to_delta_layout(original_table)
        # Only TableNotFoundError means "absent": a table that exists but cannot be
        # loaded is never created over (ITL-884).
        try:
            delta_table = deltalake.DeltaTable(
                table_uri, storage_options=self._storage_options or None
            )
        except TableNotFoundError:
            delta_table = None
        except Exception as load_error:
            raise OrcapodDatabaseReadError(
                table_uri, abs_path_components
            ) from load_error

        if delta_table is None:
            try:
                # mode="error": a table another writer created after the load above
                # is never replaced; this attempt fails instead (ITL-1384).
                deltalake.write_deltalake(
                    table_uri,
                    combined_table,
                    mode="error",
                    storage_options=self._storage_options or None,
                    # Name the statistics columns once, at creation (ITL-1187).
                    configuration=statistics_configuration(combined_table.schema),
                )
            except Exception as create_error:
                if self._table_exists_at(table_uri):
                    raise _LostCommitRace(create_error) from create_error
                raise
            logger.debug(
                f"Created new Delta table for {record_key} with {len(combined_table)} records"
            )
            return (
                None,
                _CommittedSnapshot(version=-1, ids=set()),
                self._non_null_ids(combined_table),
            )

        # The add-time check, against the version just loaded: another writer may
        # have committed since the rows were buffered (ITL-1334).
        refusal = _append_refusal(delta_table, original_table)
        if refusal is not None:
            column, reason = refusal
            raise InputValidationError(
                f"Cannot append to the stored table {table_uri!r}: column "
                f"{column!r}: {reason}. The stored rows are unchanged and "
                "nothing was written."
            )
        # Delta's MERGE matches struct fields by position: the rows are given the
        # stored struct layout by name first, so an evolved struct (fields
        # reordered, omitted or added) keeps every value under its name; and a
        # recorded column's record describes the column after this append
        # (ITL-1334).
        combined_table = _with_merged_records(
            delta_table,
            original_table,
            _conformed_to_stored(delta_table, combined_table, table_uri),
        )
        base = self._snapshot_for_commit(
            record_key, delta_table, table_uri, abs_path_components
        )
        assert base.ids is not None
        inserted_ids = self._non_null_ids(combined_table) - base.ids
        try:
            # Insert only the ids the table does not hold. Every commit to an
            # existing table is this MERGE, also when every id is new: it is the
            # commit Delta checks against rows another writer added meanwhile
            # (ITL-1377). The table object moves to the version it committed, so
            # it is not loaded again (ITL-1173).
            delta_table.merge(
                source=_merge_source_in_stored_layout(delta_table, combined_table),
                predicate=f"target.{self.RECORD_ID_COLUMN} = source.{self.RECORD_ID_COLUMN}",
                source_alias="source",
                target_alias="target",
                merge_schema=self.allow_schema_evolution,
            ).when_not_matched_insert_all().execute()
        except CommitFailedError as conflict:
            raise _LostCommitRace(conflict) from conflict
        logger.debug(f"Merged batch of {len(combined_table)} records to {record_key}")
        return delta_table, base, inserted_ids

    def _table_exists_at(self, table_uri: str) -> bool:
        """Whether a Delta table can be loaded at ``table_uri`` now."""
        try:
            deltalake.DeltaTable(
                table_uri, storage_options=self._storage_options or None
            )
        except Exception:
            return False
        return True

    def _rows_with_ids(
        self,
        rows: KeyIndex,
        record_ids: list[bytes],
        value_type: pa.DataType | None = None,
    ) -> pa.Table:
        """Return the rows whose record id is one of ``record_ids``, in table order.

        Served from the record-id index (ITL-919); a table whose id column is not
        binary is filtered instead, with Arrow's comparison.

        Args:
            rows: The indexed rows (stored or pending).
            record_ids: The ids looked up.
            value_type: The Arrow type to build the looked-up ids with for the
                filter, or ``None`` to infer it from the ids.

        Returns:
            The matching rows (possibly none).
        """
        id_type = rows.table.schema.field(self.RECORD_ID_COLUMN).type
        if not all(is_indexed_lookup(id_type, r) for r in record_ids):
            return rows.table.filter(
                pc.is_in(
                    pc.field(self.RECORD_ID_COLUMN),
                    pa.array(record_ids, type=value_type),
                )
            )
        id_index = rows.column(self.RECORD_ID_COLUMN)
        positions = {p for r in set(record_ids) for p in id_index.get(r, ())}
        return rows_at(rows.table, sorted(positions))

    @classmethod
    def _non_null_ids(cls, table: pa.Table) -> set[bytes]:
        """Return the record ids of ``table``'s rows, nulls left out."""
        return {
            record_id
            for record_id in table[cls.RECORD_ID_COLUMN].to_pylist()
            if record_id is not None
        }

    def _snapshot_for_commit(
        self,
        record_key: str,
        delta_table: deltalake.DeltaTable,
        table_uri: str,
        abs_path_components: tuple[str, ...],
    ) -> _CommittedSnapshot:
        """Return the stored ids of the version a commit is about to build on.

        ``delta_table`` was loaded for this commit, so whether the pending ids are
        new is decided against the table as it is now, never against an older
        cached version. The cached snapshot is reused when it describes this
        version; otherwise the ids are read from it.

        Args:
            record_key: The table's absolute record key.
            delta_table: The table freshly loaded for the commit.
            table_uri: The table's location, for the error.
            abs_path_components: The table's absolute path, for the error.

        Returns:
            A snapshot of the loaded version with its ``ids`` read.

        Raises:
            OrcapodDatabaseReadError: If the stored ids could not be read.
        """
        cached = self._snapshots.get(record_key)
        if (
            cached is not None
            and cached.ids is not None
            and cached.version == delta_table.version()
        ):
            return cached
        try:
            ids = self._scan_ids(delta_table)
        except Exception as e:
            raise OrcapodDatabaseReadError(table_uri, abs_path_components) from e
        return _CommittedSnapshot(delta_table.version(), ids=ids)

    def _extend_snapshot(
        self,
        base: _CommittedSnapshot,
        committed: deltalake.DeltaTable,
        inserted_ids: set[bytes],
    ) -> _CommittedSnapshot:
        """Return the snapshot of ``committed``, built from ``base`` plus this commit.

        When ``committed`` is the version right after ``base`` (no other writer
        committed in between), its ids are ``base``'s plus the inserted ones, and
        its rows, if ``base`` has them, are ``base``'s plus the rows of the data
        files the commit added, read as ``get_all_records`` reads them. Only those
        files are read, with the format and filesystem of the dataset ``base``'s
        rows came from (ITL-1173); a partitioned table, whose partition values are
        not in its files, reads its dataset instead. Anything else (a skipped
        version, a removed file, a changed schema) leaves that part to be read
        again on first use.

        Args:
            base: The snapshot of the version the commit built on.
            committed: The table reloaded after the commit.
            inserted_ids: The non-null record ids the commit inserted.

        Returns:
            The new snapshot.
        """
        version = committed.version()
        if version != base.version + 1:
            return _CommittedSnapshot(version)
        assert base.ids is not None
        snapshot = _CommittedSnapshot(version, ids=base.ids | inserted_ids)
        if (
            base.rows is None
            or base.dataset_schema is None
            or base.dataset_format is None
            or base.dataset_filesystem is None
        ):
            return snapshot
        if committed.metadata().partition_columns:
            # A partition value is not in the data file: read the table's dataset.
            return self._extend_snapshot_from_dataset(base, snapshot, committed)
        # Read only the files this commit added, as the table's dataset reads them,
        # never building that dataset over every file (ITL-1173).
        paths = cast(
            list[str],
            # A record batch in deltalake 1.0.2, a table from 1.5.0: pa.table takes both.
            pa.table(committed.get_add_actions(flatten=False))
            .column("path")
            .to_pylist(),
        )
        files = frozenset(paths)
        # The schema deltalake gives the table's dataset (``to_pyarrow_dataset``).
        schema = pa.schema(
            committed.schema().to_arrow(as_large_types=True)  # pyright: ignore[reportArgumentType]  # an arro3 schema; pyarrow reads it through the Arrow C interface
        )
        if not base.files <= files or not schema.equals(base.dataset_schema):
            return snapshot
        added = [path for path in paths if path not in base.files]
        dataset = pa_ds.FileSystemDataset(
            [
                base.dataset_format.make_fragment(
                    path, filesystem=base.dataset_filesystem
                )
                for path in added
            ],
            base.dataset_schema,
            base.dataset_format,
            base.dataset_filesystem,
        )
        new_rows = from_delta_layout(
            arrow_utils.normalize_table_view_types(dataset.to_table())
        )
        return self._append_rows(base, snapshot, new_rows, files)

    def _extend_snapshot_from_dataset(
        self,
        base: _CommittedSnapshot,
        snapshot: _CommittedSnapshot,
        committed: deltalake.DeltaTable,
    ) -> _CommittedSnapshot:
        """Extend ``base``'s rows into ``snapshot`` by reading ``committed``'s dataset.

        Args:
            base: The snapshot of the version the commit built on, with its rows.
            snapshot: The new snapshot, with its ids.
            committed: The table reloaded after the commit.

        Returns:
            ``snapshot``, with the rows when they could be extended.
        """
        assert base.rows is not None
        dataset = cast(
            "pa_ds.FileSystemDataset",
            delta_to_pyarrow_dataset(committed, as_large_types=True),
        )
        files = frozenset(dataset.files)
        if (
            base.dataset_schema is None
            or not base.files <= files
            or not dataset.schema.equals(base.dataset_schema)
        ):
            return snapshot
        added = files - base.files
        new_rows = arrow_utils.normalize_table_view_types(
            pa.concat_tables(
                [
                    fragment.to_table(schema=dataset.schema)
                    for fragment in dataset.get_fragments()
                    if fragment.path in added
                ]
            )
            if added
            else dataset.schema.empty_table()
        )
        new_rows = from_delta_layout(new_rows)
        snapshot = self._append_rows(base, snapshot, new_rows, files)
        if snapshot.rows is not None:
            self._describe_read(snapshot, dataset)
        return snapshot

    @staticmethod
    def _append_rows(
        base: _CommittedSnapshot,
        snapshot: _CommittedSnapshot,
        new_rows: pa.Table,
        files: frozenset[str],
    ) -> _CommittedSnapshot:
        """Give ``snapshot`` ``base``'s rows followed by ``new_rows``, read from ``files``.

        Args:
            base: The snapshot of the version the commit built on, with its rows.
            snapshot: The new snapshot, with its ids.
            new_rows: The rows of the data files the commit added.
            files: Every data file of the new version.

        Returns:
            ``snapshot``, without rows when ``new_rows`` has another schema than
            ``base``'s rows.
        """
        assert base.rows is not None
        if not new_rows.schema.equals(base.rows.table.schema):
            return snapshot
        rows = base.rows
        rows.append(new_rows, compact_tail(pa.concat_tables([rows.table, new_rows])))
        snapshot.rows = rows
        snapshot.files = files
        snapshot.dataset_schema = base.dataset_schema
        snapshot.dataset_format = base.dataset_format
        snapshot.dataset_filesystem = base.dataset_filesystem
        return snapshot

    @staticmethod
    def _incompatible_append_column(
        delta_table: deltalake.DeltaTable, batch: pa.Schema, written: pa.Schema
    ) -> tuple[str, pa.DataType, pa.DataType] | None:
        """Find a column whose type in ``batch`` is not the type the table holds.

        One rule (ITL-1334): each column the stored table has is compared by its own
        type with the type the table holds for it, its recorded type or else its
        stored type (a column without a record holds a type Delta keeps exactly, so
        its stored type is what was written). A conversion the Delta writer would make
        is never taken as agreement: ``timestamp[ms]`` rows are refused by a
        ``timestamp[us]`` column, whose read would give them back as microseconds.
        Both sides are compared with extension types reduced to storage,
        large-normalized and nested nullability ignored; struct fields are matched
        by name, and a field on one side only is schema evolution
        (``_types_conflict``). A ``null``-typed part takes the held type at its path
        (``_with_nulls_resolved``), so its typed siblings, a map's keys beside
        ``null`` items included, are still compared.

        One exception keeps tables written before types were recorded (ITL-928)
        appendable: an unsigned integer column, dictionary-encoded or not, into a
        column without a record that holds its written form (the signed integer of
        the same width), its values then checked by
        ``first_column_out_of_stored_range``.

        The stored schema comes from the loaded log; no data is read.

        Args:
            delta_table: The loaded stored table.
            batch: The schema of the rows about to be merged into it, with their own
                types.
            written: ``batch`` as it is written (``written_field`` of each column).

        Returns:
            ``(column, held_type, incoming_type)`` for the first column whose
            compared types differ, or ``None`` when the batch can be appended.
        """
        stored = _stored_arrow_schema(delta_table)
        recorded = {
            f.name: recorded_type(f.metadata) for f in delta_table.schema().fields
        }

        def comparable(arrow_type: pa.DataType) -> pa.DataType:
            return _comparable_storage_type(
                arrow_utils.normalize_to_large_types(arrow_type, keep_null=True)
            )

        for field in batch:
            index = stored.get_field_index(field.name)
            if index < 0:
                continue  # a new column: schema evolution
            record = recorded.get(field.name)
            held = comparable(
                record if record is not None else stored.field(index).type
            )
            incoming = _with_nulls_resolved(comparable(field.type), held)
            if not _types_conflict(held, incoming):
                continue
            decoded = (
                field.type.value_type
                if pa.types.is_dictionary(field.type)
                else field.type
            )
            if record is None and pa.types.is_unsigned_integer(decoded):
                as_written = _with_nulls_resolved(
                    comparable(written_field(pa.field(field.name, decoded)).type),
                    held,
                )
                if not _types_conflict(held, as_written):
                    continue
            return field.name, held, incoming
        return None

    @property
    def base_path(self) -> tuple[str, ...]:
        """The current relative root of this database view (always () for root instances)."""
        return self._path_prefix

    def at(self, *path_components: str) -> "DeltaTableDatabase":
        """Return a new DeltaTableDatabase scoped to the given sub-path.

        The returned instance uses the same underlying filesystem root but
        all reads and writes are relative to the extended prefix.  The scoped
        instance shares the root's ``_pending_batches`` and
        ``_pending_record_ids`` dicts so that a ``flush()`` on the root is
        visible to — and can flush — all scoped instances.

        Raises:
            TypeError: If any component is not a str.
            ValueError: If any component is empty, is ``'.'`` or ``'..'``, or
                contains filesystem-unsafe characters (``/``, ``\\``, ``*``,
                ``?``, ``"``, ``<``, ``>``, ``|``, ``\\0``).
        """
        _unsafe_chars = ["/", "\\", "*", "?", '"', "<", ">", "|", "\0"]
        for i, component in enumerate(path_components):
            if not isinstance(component, str):
                raise TypeError(
                    f"at() path component {i} must be str, got {type(component)!r}"
                )
            if not component:
                raise ValueError(f"at() path component {i} must not be empty")
            if component in (".", ".."):
                raise ValueError(
                    f"at() path component {repr(component)}: '.' and '..' are not allowed"
                )
            if any(char in component for char in _unsafe_chars):
                raise ValueError(
                    f"at() path component {repr(component)} contains invalid characters"
                )
        new_root = self._root if self._root is not None else self
        new_scoped_path = self._scoped_path + path_components
        # Propagate shared pending state to the scoped instance so that
        # root.flush() is visible to — and can flush — all scoped instances.
        root_instance = new_root
        return DeltaTableDatabase(
            base_path=self._root_uri,
            storage_options=self._storage_options,
            batch_size=self.batch_size,
            max_hierarchy_depth=self.max_hierarchy_depth,
            allow_schema_evolution=self.allow_schema_evolution,
            _path_prefix=self._path_prefix + path_components,
            _root=new_root,
            _scoped_path=new_scoped_path,
            _shared_pending_batches=root_instance._pending_batches,
            _shared_pending_record_ids=root_instance._pending_record_ids,
            _shared_delta_table_cache=root_instance._delta_table_cache,
            _shared_snapshots=root_instance._snapshots,
            _shared_pending_indexes=root_instance._pending_indexes,
            _shared_absent_tables=root_instance._absent_tables,
        )

    def list_sources(self) -> list[tuple[str, ...]]:
        """
        List all record paths that contain a valid Delta table under base_path.

        Returns:
            List of record_path tuples (e.g. [("alpha",), ("beta", "sub")]).

        Raises:
            NotImplementedError: For cloud base paths (filesystem walk not supported).
        """
        if self._is_cloud:
            raise NotImplementedError(
                "list_sources() is not supported for cloud storage paths"
            )

        sources: list[tuple[str, ...]] = []

        def _scan(current_path: Path, path_components: tuple[str, ...]) -> None:
            if len(path_components) >= self.max_hierarchy_depth:
                return
            try:
                items = list(current_path.iterdir())
            except OSError as e:
                logger.warning(
                    "Could not list directory %s during list_sources scan: %s",
                    current_path,
                    e,
                )
                return
            for item in items:
                if not item.is_dir():
                    continue
                components = path_components + (item.name,)
                try:
                    deltalake.DeltaTable(
                        str(item), storage_options=self._storage_options or None
                    )
                    sources.append(components)
                except deltalake.exceptions.TableNotFoundError:
                    _scan(item, components)

        # Build the effective scoped root directory
        scoped_root = self._local_root
        for component in self._path_prefix:
            scoped_root = scoped_root / self._sanitize_path_component(component)

        _scan(scoped_root, ())
        return sources
