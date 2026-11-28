"""Which nested positions storage can keep a logical type at (ITL-1150).

A Python ``dict`` is stored as a list of key/value structs and a tuple as a struct (or a
fixed-size list), and Arrow cannot hold an extension type inside a struct field or a list
value field. A logical type or a native container (``set[int]``, ``list[set[int]]``) as a
dict key or value or a tuple field would therefore be stored as its plain storage and read
back as something else than declared (a set as a list). Until such positions are stored
faithfully, a declared type with one is refused where it is declared, so no value is stored
lossily. A field of a user class is not such a position: the class decodes each field by its
declared type.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa

_FOLLOW_UP = "ITL-1150"


class NestedTypeNotStorableError(ValueError):
    """A logical type is declared where storage cannot keep it (ITL-1150)."""


def require_storable_in_struct_field(arrow_type: pa.DataType) -> None:
    """Refuse a logical type or native container as a dict key or value or a tuple field.

    Args:
        arrow_type: The Arrow type the declared key, value or field type registers as.

    Raises:
        NestedTypeNotStorableError: If ``arrow_type`` is an extension type (a logical type
            or a native container such as ``set[int]``).
    """
    import pyarrow as pa

    if isinstance(arrow_type, pa.ExtensionType):
        raise NestedTypeNotStorableError(
            f"a value of Arrow extension type {arrow_type.extension_name!r} cannot be a "
            f"dict key or value or a tuple field yet: Arrow cannot hold an extension type "
            f"inside a struct field, so the value would not read back as declared (a set "
            f"would read as a list). Hold it in a dataclass field, where it keeps its type, "
            f"or declare the plain type (e.g. dict[str, list[int]] for "
            f"dict[str, set[int]]). Follow-up: {_FOLLOW_UP}."
        )


def dict_entry_fields(
    arrow_type: pa.DataType,
) -> tuple[pa.Field[Any], pa.Field[Any]] | None:
    """Return the key and value fields of ``arrow_type`` when it stores a dict.

    The one definition of the dict shape: a ``dict[K, V]`` is stored as a list (large,
    plain or fixed-size) of structs with exactly the two fields ``key`` and ``value``.
    The converter reads such a type as a dict (``arrow_type_to_python_type``,
    ``get_arrow_to_python_converter``), and the refusal of a narrow float key in a
    declared type (``narrow_float_keys``, ITL-1591) asks here too. An Arrow ``map`` is
    read as a dict as well and is not this shape: its key and item types are the
    type's own.

    Args:
        arrow_type: Any Arrow type.

    Returns:
        ``(key field, value field)``, or ``None`` when ``arrow_type`` is not a list of
        ``{key, value}`` structs.
    """
    import pyarrow as pa

    if not (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return None
    element_type = arrow_type.value_type
    if not pa.types.is_struct(element_type):
        return None
    if {field.name for field in element_type} != {"key", "value"}:
        return None
    return element_type.field("key"), element_type.field("value")
