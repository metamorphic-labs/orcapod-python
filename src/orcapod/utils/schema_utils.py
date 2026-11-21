# Library of functions for working with Schemas and for extracting Schemas from a function's signature

import inspect
import logging
import sys
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from types import UnionType
from typing import TYPE_CHECKING, Any, Union, get_args, get_origin, get_type_hints

from orcapod.errors import InputValidationError
from orcapod.types import ContentHash, Schema, SchemaLike

if TYPE_CHECKING:
    import pyarrow as pa
else:
    from orcapod.utils.lazy_module import LazyModule

    pa = LazyModule("pyarrow")

if TYPE_CHECKING:
    import pyarrow as pa

logger = logging.getLogger(__name__)


def verify_data_schema(data: dict, schema: SchemaLike) -> bool:
    """Verify that the dictionary's types match the expected types in the schema."""
    from beartype.door import is_bearable

    # verify that data contains no keys not in schema
    if set(data.keys()) - set(schema.keys()):
        logger.warning(
            f"DataProtocol contains keys not in schema: {set(data.keys()) - set(schema.keys())}. "
        )
        return False
    for key, type_info in schema.items():
        if key not in data:
            logger.warning(
                f"Key '{key}' not found in data. Assuming None but this behavior may change in the future"
            )
        if not is_bearable(data.get(key), type_info):
            logger.warning(
                f"Type mismatch for key '{key}': expected {type_info}, got {data.get(key)}."
            )
            return False
    return True


# TODO: is_subhint does not handle invariance properly
# so when working with mutable types, we have to make sure to perform deep copy
def check_schema_compatibility(
    incoming_types: SchemaLike, receiving_types: Schema
) -> bool:
    """Return whether ``incoming_types`` can be passed to ``receiving_types``.

    See ``schema_incompatibility`` for the rules and for the reason when the
    answer is no.

    Args:
        incoming_types: Schema of the data offered (e.g. a stream's data schema).
        receiving_types: Schema of the parameters that receive it.

    Returns:
        True if every incoming column is a parameter of a compatible type and
        every required parameter has a column.
    """
    return schema_incompatibility(incoming_types, receiving_types) is None


def _type_label(type_info: Any) -> str:
    """Return a short, readable label for a type in an error message."""
    if isinstance(type_info, type) and not get_args(type_info):
        return type_info.__name__
    return str(type_info)


def _union_members(hint: Any) -> tuple[Any, ...] | None:
    """Return the members of a union hint, or ``None`` if ``hint`` is not a union."""
    if get_origin(hint) in (Union, UnionType):
        return get_args(hint)
    return None


def _accepts(incoming: Any, receiving: Any) -> bool:
    """Return True if a value typed ``incoming`` can be passed where ``receiving`` is expected.

    ``incoming`` must be a subhint of ``receiving``, or the same type up to user-class
    identity (a class and its re-definition are one type). A receiving union accepts
    what one of its members accepts; an incoming union needs every member accepted.

    Args:
        incoming: The type hint of the value offered.
        receiving: The type hint of the parameter.

    Returns:
        Whether the value can be passed.
    """
    from beartype.door import is_subhint

    from orcapod.logical_types.user_class_identity import equivalent_hints

    if is_subhint(incoming, receiving) or equivalent_hints(incoming, receiving):
        return True
    incoming_members = _union_members(incoming)
    if incoming_members is not None:
        return all(_accepts(member, receiving) for member in incoming_members)
    receiving_members = _union_members(receiving)
    if receiving_members is not None:
        return any(_accepts(incoming, member) for member in receiving_members)
    return False


def schema_incompatibility(
    incoming_types: SchemaLike, receiving_types: Schema
) -> str | None:
    """Explain why ``incoming_types`` cannot be passed to ``receiving_types``.

    Every incoming column must be a parameter, its type must be a subhint of
    the parameter's type, and every parameter without a default must have a
    column. Nothing is logged: the caller decides how to report the reason.

    Args:
        incoming_types: Schema of the data offered (e.g. a stream's data schema).
        receiving_types: Schema of the parameters that receive it.

    Returns:
        ``None`` when the schemas are compatible, otherwise a one-line reason
        naming the first offending column or parameter.
    """
    for key, type_info in incoming_types.items():
        if key not in receiving_types:
            return f"column '{key}' is not a parameter of the function"
        if not _accepts(type_info, receiving_types[key]):
            return (
                f"column '{key}' has type {_type_label(type_info)}, "
                f"the function expects {_type_label(receiving_types[key])}"
            )

    # Every receiving key must be present in incoming OR be optional (has a default)
    for key in receiving_types:
        if key not in incoming_types and key not in receiving_types.optional_fields:
            return f"required parameter '{key}' has no matching column"

    return None


def _declares_none(annotation: object) -> bool:
    """Whether a return annotation, as read, is ``-> None``.

    ``typing.get_type_hints`` turns ``-> None`` into ``NoneType``. When it fails on
    another annotation, a postponed annotation (``from __future__ import annotations``)
    is still text: ``"None"``, or ``"'None'"`` when it was written as a string
    (``-> "None"``).

    Args:
        annotation: The return annotation as ``get_type_hints`` or the signature gives it.

    Returns:
        True when it is ``None`` written in one of those forms.
    """
    if annotation is type(None):
        return True
    return isinstance(annotation, str) and annotation.strip("'\"") == "None"


def extract_function_schemas(
    func: Callable,
    output_keys: Collection[str],
    input_typespec: SchemaLike | None = None,
    output_typespec: SchemaLike | Sequence[type] | None = None,
) -> tuple[Schema, Schema]:
    """
    Extract input and output data types from a function signature.

    This function analyzes a function's signature to determine the types of its parameters
    and return values. It combines information from type annotations, user-provided type
    specifications, and return key mappings to produce complete type specifications.

    Args:
        func: The function to analyze for type information.
        output_keys: Collection of string keys that will be used to map the function's
            return values. For functions returning a single value, provide a single key.
            For functions returning multiple values (tuple/list), provide keys matching
            the number of return items.
        input_types: Optional mapping of parameter names to their types. If provided,
            these types override any type annotations in the function signature for the
            specified parameters. If a parameter is not in this mapping and has no
            annotation, an error is raised.
        output_types: Optional type specification for return values. Can be either:
            - A dict mapping output keys to types (TypeSpec)
            - A sequence of types that will be mapped to output_keys in order
            These types override any inferred types from the function's return annotation.

    Returns:
        A tuple containing:
        - input_types_dict: Mapping of parameter names to their inferred/specified types
        - output_types_dict: Mapping of output keys to their inferred/specified types

    Raises:
        ValueError: In various scenarios:
            - Parameter has no type annotation and is not in input_types
            - Function has return annotation but no output_keys specified
            - Function has explicit None return but non-empty output_keys provided
            - Multiple output_keys specified but return annotation is not a sequence type
            - Return annotation is a sequence type but doesn't specify item types
            - Number of types in return annotation doesn't match number of output_keys
            - Output types sequence length doesn't match output_keys length
            - Output key not specified in output_types and has no type annotation

    Examples:
        >>> def add(x: int, y: int) -> int:
        ...     return x + y
        >>> input_types, output_types = extract_function_data_types(add, ['result'])
        >>> input_types
        {'x': <class 'int'>, 'y': <class 'int'>}
        >>> output_types
        {'result': <class 'int'>}

        >>> def process(data: str) -> tuple[int, str]:
        ...     return len(data), data.upper()
        >>> input_types, output_types = extract_function_data_types(
        ...     process, ['length', 'upper_data']
        ... )
        >>> input_types
        {'data': <class 'str'>}
        >>> output_types
        {'length': <class 'int'>, 'upper_data': <class 'str'>}

        >>> def legacy_func(x, y):  # No annotations
        ...     return x + y
        >>> input_types, output_types = extract_function_data_types(
        ...     legacy_func, ['sum'],
        ...     input_types={'x': int, 'y': int},
        ...     output_types={'sum': int}
        ... )
        >>> input_types
        {'x': <class 'int'>, 'y': <class 'int'>}
        >>> output_types
        {'sum': <class 'int'>}

        >>> def multi_return(data: list) -> tuple[int, float, str]:
        ...     return len(data), sum(data), str(data)
        >>> input_types, output_types = extract_function_data_types(
        ...     multi_return, ['count', 'total', 'repr'],
        ...     output_types=[int, float, str]  # Override with sequence
        ... )
        >>> output_types
        {'count': <class 'int'>, 'total': <class 'float'>, 'repr': <class 'str'>}
    """
    verified_output_types: Schema = {}
    if output_typespec is not None:
        if isinstance(output_typespec, dict):
            verified_output_types = output_typespec
        elif isinstance(output_typespec, Sequence):
            # If output_types is a collection, convert it to a dict with keys from return_keys
            if len(output_typespec) != len(output_keys):
                raise ValueError(
                    f"Output types collection length {len(output_typespec)} does not match return keys length {len(output_keys)}."
                )
            verified_output_types = {k: v for k, v in zip(output_keys, output_typespec)}

    # Use get_type_hints to resolve annotations that may be stored as strings
    # (e.g. when the defining module uses `from __future__ import annotations`).
    # Fall back to an empty dict if hints cannot be resolved (e.g. for built-ins).
    try:
        resolved_hints = get_type_hints(func)
    except Exception:
        resolved_hints = {}

    signature = inspect.signature(func)

    param_info: Schema = {}
    optional_params: set[str] = set()
    for name, param in signature.parameters.items():
        if input_typespec and name in input_typespec:
            param_info[name] = input_typespec[name]
        elif name in resolved_hints:
            param_info[name] = resolved_hints[name]
        elif param.annotation is not inspect.Parameter.empty:
            # annotation is already a live type (no __future__ postponement)
            param_info[name] = param.annotation
        else:
            raise ValueError(
                f"Parameter '{name}' has no type annotation and is not specified in input_types."
            )
        if param.default is not inspect.Parameter.empty:
            optional_params.add(name)

    # get_type_hints stores the return annotation under the key 'return'
    return_annot = resolved_hints.get("return", signature.return_annotation)
    # ``-> None`` declares no outputs (ITL-1257), which is not an output whose value is
    # None: the number of outputs is the number of output keys.
    if _declares_none(return_annot):
        return_annot = None
    inferred_output_types: Schema = {}
    if return_annot is not inspect.Signature.empty and return_annot is not None:
        output_item_types = []
        if len(output_keys) == 0:
            raise ValueError(
                "Function has a return type annotation, but no return keys were specified."
            )
        elif len(output_keys) == 1:
            # if only one return key, the entire annotation is inferred as the return type
            output_item_types = [return_annot]
        elif get_origin(return_annot) in (tuple, list) or (
            isinstance(get_origin(return_annot), type)
            and issubclass(get_origin(return_annot), Sequence)
        ):
            if get_origin(return_annot) is None:
                # right type was specified but did not specify the type of items
                raise ValueError(
                    f"Function return type annotation {return_annot} is a Sequence type but does not specify item types."
                )
            output_item_types = get_args(return_annot)
            if len(output_item_types) != len(output_keys):
                raise ValueError(
                    f"Function return type annotation {return_annot} has {len(output_item_types)} items, "
                    f"but output_keys has {len(output_keys)} items."
                )
        else:
            raise ValueError(
                f"Multiple return keys were specified but return type annotation {return_annot} is not a sequence type (list, tuple, Collection)."
            )
        for key, type_annot in zip(output_keys, output_item_types):
            inferred_output_types[key] = type_annot
    elif return_annot is None:
        if len(output_keys) != 0:
            raise ValueError(
                f"Function provides explicit return type annotation as None, but return keys of length {len(output_keys)} were specified."
            )
    else:
        inferred_output_types = {k: inspect.Signature.empty for k in output_keys}

    # TODO: simplify the handling here -- technically all keys should already be in return_types
    for key in output_keys:
        if key in verified_output_types:
            inferred_output_types[key] = verified_output_types[key]
        elif (
            key not in inferred_output_types
            or inferred_output_types[key] is inspect.Signature.empty
        ):
            raise ValueError(
                f"Type for return item '{key}' is not specified in output_types and has no type annotation in function signature."
            )
    # Reject bare container types (must have type parameters)
    _BARE_CONTAINER_TYPES = {dict, list, set, tuple}
    _BARE_CONTAINER_EXAMPLES = {
        dict: "dict[str, int]",
        list: "list[int]",
        set: "set[int]",
        tuple: "tuple[int, ...]",
    }
    for name, type_annot in {**param_info, **inferred_output_types}.items():
        if type_annot in _BARE_CONTAINER_TYPES:
            example = _BARE_CONTAINER_EXAMPLES[type_annot]
            raise ValueError(
                f"Type annotation for '{name}' is bare {type_annot.__name__} "
                f"without type parameters. Use e.g. {example} instead."
            )

    return Schema(param_info, optional_fields=optional_params), Schema(
        inferred_output_types
    )


def infer_schema_from_dict(
    data: Mapping, schema: SchemaLike | None = None, default=str
) -> Schema:
    """
    Returns a Schema for the given dictionary by inferring types from values.
    If schema is provided, it is used as a base when inferring types for the fields in dict.
    """
    if schema is None:
        schema = {}
    return Schema(
        {
            key: schema.get(key, type(value) if value is not None else default)
            for key, value in data.items()
        }
    )


# def get_compatible_type(type1: Any, type2: Any) -> Any:
#     if type1 is type2:
#         return type1
#     if issubclass(type1, type2):
#         return type2
#     if issubclass(type2, type1):
#         return type1
#     raise TypeError(f"Types {type1} and {type2} are not compatible")


def get_compatible_type(type1: Any, type2: Any) -> Any:
    # Handle identical or equal types (e.g., two separate `int | None` union objects)
    if type1 is type2 or type1 == type2:
        return type1

    # Handle None/NoneType
    if type1 is type(None) or type2 is type(None):
        # You might want to handle Optional types here
        if type1 is type(None):
            return type2
        return type1

    # Get origins for generic types (e.g., list from list[int])
    origin1 = get_origin(type1) or type1
    origin2 = get_origin(type2) or type2

    # If origins are different, check basic subclass relationship
    if origin1 != origin2:
        try:
            if issubclass(origin1, origin2):
                return type2
            if issubclass(origin2, origin1):
                return type1
        except TypeError:
            # issubclass fails on some special forms
            pass
        raise TypeError(f"Types {type1} and {type2} are not compatible")

    # Same origin - check type arguments
    args1 = get_args(type1)
    args2 = get_args(type2)

    # If no type arguments, return the origin
    if not args1 and not args2:
        return origin1

    # If only one has type arguments, prefer the more specific one
    if not args1:
        return type2
    if not args2:
        return type1

    # Both have type arguments - recursively check compatibility
    if len(args1) != len(args2):
        raise TypeError(f"Types {type1} and {type2} have incompatible argument counts")

    compatible_args = []
    for arg1, arg2 in zip(args1, args2):
        try:
            compatible_args.append(get_compatible_type(arg1, arg2))
        except TypeError:
            raise TypeError(
                f"Types {type1} and {type2} have incompatible type arguments"
            )

    # Reconstruct the generic type
    if sys.version_info >= (3, 9):
        return origin1[tuple(compatible_args)]
    else:
        # For Python < 3.9, you might need to use typing._GenericAlias
        from typing import _GenericAlias

        return _GenericAlias(origin1, tuple(compatible_args))


def union_schemas(*schemas: SchemaLike) -> Schema:
    """Merge multiple schemas, raising an error if type conflicts are found."""
    merged = dict(schemas[0])
    for schema in schemas[1:]:
        for key, right_type in schema.items():
            merged[key] = (
                get_compatible_type(merged[key], right_type)
                if key in merged
                else right_type
            )
    return Schema(merged)


def intersection_schemas(*schemas: SchemaLike) -> Schema:
    """
    Returns the intersection of all schemas, only returning keys that are present in all schemas.
    If a key is present in multiple schemas, the types must be compatible.
    """
    common_keys = set(schemas[0].keys())
    for schema in schemas[1:]:
        common_keys.intersection_update(schema.keys())

    intersection = {k: schemas[0][k] for k in common_keys}
    for schema in schemas[1:]:
        for key in common_keys:
            try:
                intersection[key] = get_compatible_type(intersection[key], schema[key])
            except TypeError:
                raise TypeError(
                    f"Type conflict for key '{key}': {intersection[key]} vs {schema[key]}"
                )
    return Schema(intersection)


def arrow_schema_identity(
    tag_schema: "pa.Schema", data_schema: "pa.Schema", data_context: Any
) -> tuple[ContentHash, ContentHash]:
    """Return the identity of a ``(tag, data)`` schema pair: the one rule (ITL-640).

    A schema enters an identity as its Arrow schema, hashed by the data context's
    ``arrow_hasher.hash_schema`` (labelled ``arrow_schema_v1``): two schemas whose
    Arrow types differ (width, unit, zone, precision, nullability at any depth, a
    logical type's identifier) are two identities, and column order, large and
    non-large variants, list item field names, dictionary encoding and field metadata
    are not part of it. The tag and data schemas are hashed apart, so a data-tag (a
    column in both) differs from a plain tag. The pair of ``ContentHash`` objects is
    what a source's pipeline and content identity embed; the structure hasher keeps
    each hash's label.

    Args:
        tag_schema: The Arrow schema of the tag columns.
        data_schema: The Arrow schema of the data columns (data-tags included).
        data_context: The data context whose ``arrow_hasher`` hashes the schemas.

    Returns:
        The labelled hashes of the tag schema and of the data schema.
    """
    return _hash_schema(tag_schema, data_context), _hash_schema(
        data_schema, data_context
    )


def _hash_schema(schema: "pa.Schema", data_context: Any) -> ContentHash:
    """``hash_schema`` of ``schema``; a logical type it cannot read names its column."""
    from orcapod.logical_types.identifier import LogicalTypeIdentifierError

    hasher = data_context.arrow_hasher
    try:
        return hasher.hash_schema(schema)
    except LogicalTypeIdentifierError as exc:
        for field in schema:
            try:
                hasher.hash_schema(pa.schema([field]))
            except LogicalTypeIdentifierError as column_exc:
                raise column_exc.for_column(field.name) from exc
        raise


def stored_type_mismatch(
    expected: "pa.Schema", stored: "pa.Schema", hasher: Any
) -> "tuple[pa.Field[Any], pa.Field[Any]] | None":
    """The first column whose stored Arrow type is not the expected one (ITL-640).

    The rule for every read of stored rows that must present a source's declared types
    (a ``DerivedSource``'s stored read, a ``CachedSource``'s cache read): a store that
    gives a column back as another type (Delta reads a fixed-size list back as a list,
    ITL-1275) would present rows of another schema under the source's identity. Types
    are compared as ``hash_schema`` compares them (one field each), so a large and a
    non-large variant agree. A column's top-level nullability is not compared: the
    stored rows say it. Columns missing from either side are not compared.

    Args:
        expected: The declared Arrow schema.
        stored: The Arrow schema of the stored rows.
        hasher: The data context's Arrow hasher.

    Returns:
        The expected and the stored field of the first column that differs, or
        ``None`` when every shared column agrees.
    """
    for field in expected:
        if field.name not in stored.names:
            continue
        given = stored.field(field.name)
        if hasher.hash_schema(pa.schema([given.with_nullable(True)])) != (
            hasher.hash_schema(pa.schema([field.with_nullable(True)]))
        ):
            return field, given
    return None


def declared_arrow_schema(schema: SchemaLike, converter: Any) -> "pa.Schema":
    """The Arrow schema a declared Python schema stands for: the converter's (ITL-640).

    Each declared Python type means exactly one Arrow type (``int`` is ``int64``,
    ``datetime`` is ``timestamp[us, tz=UTC]``); ``T | None`` is a nullable field. A
    class a factory covers (a dataclass, a pydantic model) that nothing has registered
    yet is registered first, as hashing it registers it.

    Args:
        schema: The declared Python schema.
        converter: The data context's type converter.

    Returns:
        The Arrow schema.
    """
    try:
        return converter.python_schema_to_arrow_schema(schema)
    except (TypeError, ValueError):
        converter.ensure_types_registered_for_schemas(Schema(dict(schema)))
        return converter.python_schema_to_arrow_schema(schema)


def compute_schema_hash(
    tag_schema: "pa.Schema",
    data_schema: "pa.Schema",
    data_context: Any,
    char_count: int | None,
) -> str:
    """Compute the schema hash used for system-tag column naming.

    This is the hash that ``SourceStreamBuilder`` embeds in system-tag column names
    when building an ``ArrowTableStream`` from a source table: the semantic hash of
    ``arrow_schema_identity`` of the pair, so a slot can predict the names from its
    Arrow schemas alone, without a live source. A hash inside a name is the bare hex
    digest.

    Args:
        tag_schema: The Arrow schema of the tag columns.
        data_schema: The Arrow schema of the data columns (data-tags included).
        data_context: The active ``DataContext``.
        char_count: Number of hex characters to include. ``None`` returns the full
            hex digest.

    Returns:
        Hex string: the full digest when ``char_count`` is ``None``, otherwise
        truncated to ``char_count`` characters.
    """
    identity = arrow_schema_identity(tag_schema, data_schema, data_context)
    return data_context.semantic_hasher.hash_object(identity).to_hex(
        char_count=char_count
    )


def compute_source_schema_hash(
    tag_schema: "pa.Schema",
    data_schema: "pa.Schema",
    data_context: Any,
    config: Any,
) -> str:
    """Compute the schema hash for system-tag column naming from a ``DataContext`` and config.

    This is the canonical single entry-point used by ``SourceStreamBuilder``,
    ``SourceNodeBase``, and ``PollingSource`` so the truncation by
    ``config.hashing.schema_n_char`` is not replicated at every call site.

    Args:
        tag_schema: The Arrow schema of the tag columns.
        data_schema: The Arrow schema of the data columns (data-tags included).
        data_context: The active ``DataContext``.
        config: Active ``OrcapodConfig`` — ``config.hashing.schema_n_char``
            controls hash truncation.

    Returns:
        Hex string schema hash, truncated to ``config.hashing.schema_n_char``
        characters.
    """
    return compute_schema_hash(
        tag_schema, data_schema, data_context, config.hashing.schema_n_char
    )


def column_names(columns: str | Iterable[str]) -> tuple[str, ...]:
    """Return the column names an argument gives, in the order given: the one rule.

    A bare string is one column name, never a collection of one-character names
    (ITL-1221); any other iterable gives its elements. Every operator and source
    argument that takes column names reads it through this function; the caller
    then applies its own rules (sorting, repeats, an empty collection).

    Args:
        columns: One column name, or an iterable of column names.

    Returns:
        The names, in the order given.

    Raises:
        TypeError: If ``columns`` is ``bytes`` or ``bytearray`` (empty or not),
            neither a string nor an iterable, or an element is not a string.
    """
    if isinstance(columns, str):
        return (columns,)
    if isinstance(columns, (bytes, bytearray)):
        # Iterable, but of integers; an empty one would pass as no names.
        raise TypeError(
            "column names must be a string or iterable of strings, "
            f"got {type(columns).__name__!r}"
        )
    try:
        names = tuple(columns)
    except TypeError:
        raise TypeError(
            "column names must be a string or iterable of strings, "
            f"got {type(columns).__name__!r}"
        ) from None
    bad = [name for name in names if not isinstance(name, str)]
    if bad:
        raise TypeError(
            f"column names must be strings; got {[type(x).__name__ for x in bad]!r}"
        )
    return names


def _normalize_column_list(value: Any) -> list[str]:
    """Normalize a ``tag_columns`` argument to a plain list of strings.

    The rule is ``column_names``: a bare string is one name.

    Args:
        value: A single column name (``str``) or an iterable of column names.

    Returns:
        A list of column name strings.

    Raises:
        TypeError: If ``value`` is not a ``str`` or iterable, or if any
            element of the iterable is not a ``str``.
    """
    try:
        return list(column_names(value))
    except TypeError as exc:
        raise TypeError(f"tag_columns: {exc}") from None


def require_output_keys_not_tags(
    tag_names: Collection[str], output_keys: Collection[str]
) -> None:
    """Raise if a function's output key is also a tag column of its input.

    A name is a tag or a data column, never both, and a function pod keeps its
    input's tags next to the data it produces.

    Args:
        tag_names: The tag column names of the pod's input.
        output_keys: The names of the function's output data columns.

    Raises:
        InputValidationError: If a name is in both. The message names the output
            key and the remedy.
    """
    for name in sorted(set(tag_names) & set(output_keys)):
        raise InputValidationError(
            f"function output key {name!r} is also a tag column of the input, and a "
            + "name cannot be both a tag and a data column. Rename the output "
            + "(the function's output key), or rename the tag with map_tags."
        )


def require_names_without_separator(names: Collection[str]) -> None:
    """Raise if a tag or data column name contains ``::`` or starts with ``:``.

    ``::`` separates the parts of a source-info token, ``<source id>::<record id>::<column>``,
    which splits at its first and last ``::`` only because neither a source id nor a
    column name holds one, a source id does not end with ``:`` and a column name does
    not start with one; the record id between them may hold anything (ITL-1154). A
    column ``:x`` would make ``s::row_0:::x`` also read as record id ``row_0:``.

    Args:
        names: Tag or data column names.

    Raises:
        InputValidationError: If a name contains ``::`` or starts with ``:``. The
            message names the column.
    """
    from orcapod.system_constants import constants

    for name in sorted(names):
        if constants.BLOCK_SEPARATOR in name or name.startswith(":"):
            raise InputValidationError(
                f"column name {name!r} contains {constants.BLOCK_SEPARATOR!r} or "
                "starts with ':', which would blur the separator of the parts of a "
                "source-info token; give the column another name."
            )


def require_names_not_meta(names: Collection[str]) -> None:
    """Raise if a tag or data column name starts with the reserved meta prefix.

    Names starting with the meta prefix (``__``) are meta columns: a datagram
    keeps them apart from its tags and data, and no operator carries them
    (ITL-1080). A tag or data column given such a name would silently stop being
    one on every path that rebuilds rows, so no stream, source or operator may
    produce one.

    Args:
        names: Tag or data column names.

    Raises:
        InputValidationError: If a name starts with the meta prefix. The message
            names the column.
    """
    from orcapod.system_constants import constants

    for name in sorted(names):
        if name.startswith(constants.META_PREFIX):
            raise InputValidationError(
                f"column name {name!r} starts with {constants.META_PREFIX!r}, which is "
                "reserved for meta columns; a tag or data column needs another name."
            )


DICTIONARY_COLUMNS_NOT_SUPPORTED = (
    "dictionary-encoded columns are not currently supported in Orcapod"
)
"""The start of the error for a dictionary-encoded column (ITL-1264, ITL-1277)."""


def dictionary_columns(
    arrow_schema: "pa.Schema",
) -> "list[tuple[str, pa.DataType, pa.DataType]]":
    """Return every column whose Arrow type is, or contains, a dictionary.

    Developer decisions, November 18 and 19, 2026 (ITL-1264, ITL-1277): dictionary-encoded
    columns are not currently supported in Orcapod, tags and data alike. A dictionary at
    the top level of a column's type or inside it (a list, list-view or fixed-size list
    item, a struct child, a map key or item, the values of a run-end-encoded type, an
    extension type's storage) counts. A library value in a cell (a pandas or polars data
    frame holding a categorical column) is an extension column stored as bytes, not a
    dictionary, and does not count.

    ``ArrowTableStream.__init__`` (every source and operator result becomes a stream
    there) and the source builder (first, before its key columns and its hash) refuse
    what this finds, with ``dictionary_columns_error``.

    Args:
        arrow_schema: The Arrow schema of a table about to become a stream.

    Returns:
        ``(name, type, decoded type)`` for each such column, in schema order.
    """
    from orcapod.utils.arrow_utils import decode_dictionaries

    found: list[tuple[str, pa.DataType, pa.DataType]] = []
    for field in arrow_schema:
        decoded = decode_dictionaries(field.type)
        if decoded != field.type:
            found.append((field.name, field.type, decoded))
    return found


def dictionary_columns_error(
    found: "Sequence[tuple[str, pa.DataType, pa.DataType]]",
    tag_columns: Collection[str],
    data_tags: Collection[str],
    system_tag_columns: Collection[str] = (),
) -> InputValidationError:
    """Return the error refusing the dictionary-encoded columns ``found``.

    Its job is the report, so it takes the roles it names. A user's table can hold a
    dictionary only in its own columns: Orcapod writes system tags, source info, the
    data context and meta columns as plain strings, so a dictionary there comes from a
    hand-built table, and is refused the same way.

    Args:
        found: The columns, as ``dictionary_columns`` returns them.
        tag_columns: The stream's tag columns, in tag order.
        data_tags: The tags that are data-tags.
        system_tag_columns: Names the stream will read as system tags beside those with
            the system-tag prefix.

    Returns:
        An ``InputValidationError`` naming each column (tags first, in tag order, then
        the others in table order) with its role, its type and the type it decodes to,
        and how to decode it.
    """
    tags = list(tag_columns)
    ordered = sorted(
        found,
        key=lambda item: (0, tags.index(item[0])) if item[0] in tags else (1, 0),
    )
    lines = [
        f"  - {_column_role(name, tag_columns, data_tags, system_tag_columns)} column "
        f"{name!r} has type {arrow_type} (decoded: {decoded})"
        for name, arrow_type, decoded in ordered
    ]
    return InputValidationError(
        f"{DICTIONARY_COLUMNS_NOT_SUPPORTED}:\n"
        + "\n".join(lines)
        + "\nIf keeping the dictionary encoding is not important, decode each such "
        "column before building the source or stream. With pyarrow, cast it to the "
        "decoded type shown and replace it keeping its field: "
        "table.set_column(i, field.with_type(decoded), column.cast(decoded)). Three "
        "kinds need a step first: a dictionary whose values hold a dictionary "
        "(chunk.dictionary_decode() on each chunk, then the cast), a list view (cast "
        "it to a large list of the decoded items) and a run-end-encoded column (cast "
        "its values, then expand its runs with pyarrow.compute.run_end_decode); the "
        "troubleshooting page gives each call. With polars, "
        "df = df.with_columns(pl.col(name).cast(pl.String)) decodes a Categorical or "
        "Enum column."
    )


def _column_role(
    name: str,
    tag_columns: Collection[str],
    data_tags: Collection[str],
    system_tag_columns: Collection[str],
) -> str:
    """The role a stream gives the column ``name``, for an error that names it."""
    from orcapod.system_constants import constants

    if name in data_tags:
        return "data-tag"
    if name in tag_columns:
        return "tag"
    if name in system_tag_columns or name.startswith(constants.SYSTEM_TAG_PREFIX):
        return "system-tag"
    if name.startswith(constants.SOURCE_PREFIX):
        return "source-info"
    if name == constants.CONTEXT_KEY:
        return "data context"
    if name.startswith(constants.META_PREFIX):
        return "meta"
    return "data"


UNION_OR_LIST_VIEW_COLUMNS_NOT_SUPPORTED = (
    "union and list-view columns are not supported in Orcapod"
)
"""The start of the error for a union or list-view column (ITL-640)."""


def _holds_union_or_list_view(arrow_type: "pa.DataType") -> bool:
    """Whether ``arrow_type`` is, or contains at any depth, a union or a list view."""
    if isinstance(arrow_type, pa.BaseExtensionType):
        return _holds_union_or_list_view(arrow_type.storage_type)
    if (
        pa.types.is_union(arrow_type)
        or pa.types.is_list_view(arrow_type)
        or pa.types.is_large_list_view(arrow_type)
    ):
        return True
    if pa.types.is_dictionary(arrow_type):
        return _holds_union_or_list_view(arrow_type.value_type)
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return _holds_union_or_list_view(arrow_type.value_type)
    if pa.types.is_run_end_encoded(arrow_type):
        return _holds_union_or_list_view(arrow_type.value_type)
    if pa.types.is_struct(arrow_type):
        return any(_holds_union_or_list_view(child.type) for child in arrow_type)
    if pa.types.is_map(arrow_type):
        return _holds_union_or_list_view(
            arrow_type.key_type
        ) or _holds_union_or_list_view(arrow_type.item_type)
    return False


def union_or_list_view_columns(
    arrow_schema: "pa.Schema",
) -> "list[tuple[str, pa.DataType]]":
    """Return every column whose Arrow type is, or contains, a union or a list view.

    Developer decision, November 21, 2026 (ITL-640): a stream's identity is its Arrow
    schema hash, and the Arrow schema hash (starfix) cannot hold a union (dense or
    sparse) or a list view (``list_view``, ``large_list_view``), so a stream with such
    a column could not have an identity. Such a type at the top of a column or inside
    it (a list, large list or fixed-size list item, a struct child, a map key or item,
    a dictionary's values, the values of a run-end-encoded type, an extension type's
    storage) counts.

    ``ArrowTableStream.__init__`` (every source and operator result becomes a stream
    there) and the source builder (first, before its key columns and its hash) refuse
    what this finds, with ``union_or_list_view_columns_error``, beside the dictionary
    refusal (``dictionary_columns``).

    Args:
        arrow_schema: The Arrow schema of a table about to become a stream.

    Returns:
        ``(name, type)`` for each such column, in schema order.
    """
    return [
        (field.name, field.type)
        for field in arrow_schema
        if _holds_union_or_list_view(field.type)
    ]


def union_or_list_view_columns_error(
    found: "Sequence[tuple[str, pa.DataType]]",
    tag_columns: Collection[str],
    data_tags: Collection[str],
    system_tag_columns: Collection[str] = (),
) -> InputValidationError:
    """Return the error refusing the union and list-view columns ``found``.

    Its job is the report, so it takes the roles it names.

    Args:
        found: The columns, as ``union_or_list_view_columns`` returns them.
        tag_columns: The stream's tag columns, in tag order.
        data_tags: The tags that are data-tags.
        system_tag_columns: Names the stream will read as system tags beside those with
            the system-tag prefix.

    Returns:
        An ``InputValidationError`` naming each column (tags first, in tag order, then
        the others in table order) with its role and its type, and what to do.
    """
    tags = list(tag_columns)
    ordered = sorted(
        found,
        key=lambda item: (0, tags.index(item[0])) if item[0] in tags else (1, 0),
    )
    lines = [
        f"  - {_column_role(name, tag_columns, data_tags, system_tag_columns)} column "
        f"{name!r} has type {arrow_type}"
        for name, arrow_type in ordered
    ]
    return InputValidationError(
        f"{UNION_OR_LIST_VIEW_COLUMNS_NOT_SUPPORTED}: a stream's identity is its Arrow "
        "schema hash, which cannot hold them:\n"
        + "\n".join(lines)
        + "\nCast a list view to the large list of the same items "
        "(column.cast(pa.large_list(item_type))); a union has no Orcapod type: "
        "split it into one column per member type, or drop it, before building the "
        "source or stream."
    )


def require_names_not_system(names: Collection[str]) -> None:
    """Raise if a tag or data column name is one a stream reads as a system column.

    A stream reads a column whose name starts with the system-tag prefix
    (``_tag_``) as a system tag, one that starts with the source-info prefix
    (``_source_``) as source info, and the context key column (``_context_key``)
    as the data context, so a tag or data column renamed to such a name stops
    being one (ITL-1160). The meta prefix has its own check,
    ``require_names_not_meta``.

    Args:
        names: Tag or data column names.

    Raises:
        InputValidationError: If a name starts with the system-tag or the
            source-info prefix, or is the context key column. The message names
            the column.
    """
    from orcapod.system_constants import constants

    reserved_prefixes = {
        constants.SYSTEM_TAG_PREFIX: "system tags",
        constants.SOURCE_PREFIX: "source info",
    }
    for name in sorted(names):
        for prefix, kind in reserved_prefixes.items():
            if name.startswith(prefix):
                raise InputValidationError(
                    f"column name {name!r} starts with {prefix!r}, which is reserved "
                    f"for {kind}; a tag or data column needs another name."
                )
        if name == constants.CONTEXT_KEY:
            raise InputValidationError(
                f"column name {name!r} is reserved for the data context; a tag or "
                "data column needs another name."
            )
