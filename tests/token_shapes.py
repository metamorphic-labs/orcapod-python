"""Helpers for tests that build a stream with hand-made source-info token columns.

A token column's list layers must be its data column's outer list layers (the
token-shape rule, ITL-1491). A test that replaces a token column with a list-typed
one gives its data column the same layers with these helpers.
"""

from __future__ import annotations

from typing import Any, cast

import pyarrow as pa


def data_type_for(
    token_type: pa.DataType, leaf: pa.DataType | None = None
) -> pa.DataType:
    """The data type with ``token_type``'s list layers over ``leaf`` (``int64``).

    Args:
        token_type: A token column's type: list layers over a string.
        leaf: The type under the layers.

    Returns:
        The same layers (large list, fixed-size list of the same size) over ``leaf``.
    """
    leaf = pa.int64() if leaf is None else leaf
    if pa.types.is_fixed_size_list(token_type):
        return pa.list_(
            data_type_for(token_type.value_type, leaf), token_type.list_size
        )
    if pa.types.is_large_list(token_type) or pa.types.is_list(token_type):
        return pa.large_list(data_type_for(token_type.value_type, leaf))
    return leaf


def _item_type(kind: pa.DataType) -> pa.DataType:
    """The type of a list type's items."""
    return cast("pa.DataType", cast("Any", kind).value_type)


def _is_list(kind: pa.DataType) -> bool:
    return (
        pa.types.is_large_list(kind)
        or pa.types.is_list(kind)
        or pa.types.is_fixed_size_list(kind)
    )


def nested_value(leaf: Any, data_type: pa.DataType) -> Any:
    """``leaf`` wrapped in one list per list layer of ``data_type``.

    A fixed-size layer holds its size of copies, any other layer one value.
    """
    if not _is_list(data_type):
        return leaf
    inner = nested_value(leaf, _item_type(data_type))
    count = (
        cast("Any", data_type).list_size
        if pa.types.is_fixed_size_list(data_type)
        else 1
    )
    return [inner] * count


def data_for(token: Any, leaf: Any, data_type: pa.DataType) -> Any:
    """A data value of ``data_type`` with the list structure of the token ``token``.

    Args:
        token: A token value: a string, ``None`` or (nested) lists of them.
        leaf: The value put at every token position.
        data_type: The data column's type (the token's layers over a leaf type).

    Returns:
        ``leaf`` at every token position, in the token's lists; where the token
        is null inside a list that the data type continues, a value of that type.
    """
    if isinstance(token, list):
        return [data_for(item, leaf, _item_type(data_type)) for item in token]
    return nested_value(leaf, data_type)


def with_token_column(
    table: pa.Table,
    name: str,
    tokens: list[Any],
    token_type: pa.DataType,
    *,
    nullable: bool = True,
) -> pa.Table:
    """Replace ``_source_<name>`` by ``tokens`` and give ``name`` a fitting shape.

    The data column ``name`` (a scalar column) becomes a column with the token
    type's list layers, each row's value repeated at every token position, so the
    stream built from the table satisfies the token-shape rule. A token type with
    no layer leaves the data column as it is.

    Args:
        table: A table holding ``name`` and ``_source_<name>``.
        name: The data column.
        tokens: The token column's values, one per row.
        token_type: The token column's Arrow type.
        nullable: Whether the token field is declared nullable.

    Returns:
        The table with both columns replaced, in their places.
    """
    token_name = f"_source_{name}"
    table = table.set_column(
        table.schema.get_field_index(token_name),
        pa.field(token_name, token_type, nullable=nullable),
        pa.array(tokens, token_type),
    )
    data_field = table.schema.field(name)
    data_type = data_type_for(token_type, data_field.type)
    if data_type == data_field.type:
        return table
    values = table.column(name).to_pylist()
    return table.set_column(
        table.schema.get_field_index(name),
        pa.field(name, data_type, nullable=data_field.nullable),
        pa.array(
            [data_for(t, v, data_type) for t, v in zip(tokens, values)], data_type
        ),
    )
