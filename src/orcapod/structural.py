"""``orcapod.dataclass``: dataclasses identified by name and fields (ITL-972).

An ordinary ``@dataclass`` is identified by its exact class: a pod annotated with it
accepts that class object and no other. A class made with ``@orcapod.dataclass`` is
**structural**: Orcapod identifies it by its name and its fields (names, types and
order). Two such classes with the same name and fields are one type: a pod annotated
with one accepts a column of the other, their instances compare equal when their field
values are equal, and stored values can be read back without the class being defined.

Orcapod never converts a value from one class to another. A function annotated with a
structural class receives the object that arrives, which may be an instance of another
class of the same type: a structural class promises its fields, not its methods.
``from_instance`` is the explicit conversion.

See ``docs/concepts/your-own-data-types.md``.
"""

from __future__ import annotations

import dataclasses
import types
import typing
from collections.abc import Callable
from typing import Any, TypeVar, overload

import orcapod.logical_types.user_class_identity as _uci

T = TypeVar("T")

__all__ = ["dataclass"]

_UNIONS = (typing.Union, types.UnionType)


@overload
def dataclass(cls: type[T], /) -> type[T]: ...


@overload
def dataclass(
    *, name: str | None = None, **options: Any
) -> Callable[[type[T]], type[T]]: ...


@typing.dataclass_transform()
def dataclass(
    cls: type[T] | None = None, /, *, name: str | None = None, **options: Any
) -> type[T] | Callable[[type[T]], type[T]]:
    """Define a dataclass that Orcapod identifies by its name and fields.

    Use it in place of ``dataclasses.dataclass``, bare or with keywords. The result is
    a real dataclass (``dataclasses.fields``, ``asdict`` and ``replace`` work) with two
    differences. ``==`` compares field values across classes of the same type: an
    instance equals an instance of another ``orcapod.dataclass`` with the same name
    and fields when their field values are equal (unless ``eq=False`` is passed or the
    class defines ``__eq__`` itself). And the class has ``from_instance``, which builds
    an instance from an instance of another dataclass with the same fields.

    Args:
        cls: The class, when used as a bare decorator.
        name: The name the type is identified by. Default: the class name. It must not
            be empty, contain ``@``, ``;`` or whitespace, or start with ``orcapod.``.
        **options: Keyword options of ``dataclasses.dataclass`` (``frozen``,
            ``slots``, ``kw_only``, ``order``, ...). ``init=False`` is not allowed.

    Returns:
        The dataclass, or a decorator that returns it.

    Raises:
        TypeError: If the class is already a dataclass or a pydantic model, or
            ``init=False`` is passed.
        ValueError: If the name is invalid.

    Example:
        >>> import orcapod as op
        >>> @op.dataclass
        ... class Point:
        ...     x: int
        ...     y: float
    """
    if options.get("init", True) is not True:
        raise TypeError(
            "orcapod.dataclass does not accept init=False: Orcapod builds instances "
            "through the generated __init__."
        )

    def decorate(target: type[T]) -> type[T]:
        """Make ``target`` a dataclass and mark it as a structural class."""
        if not isinstance(target, type):
            raise TypeError(f"orcapod.dataclass decorates a class, got {target!r}.")
        if "__dataclass_fields__" in vars(target):
            raise TypeError(
                f"{target.__qualname__} is already a dataclass. Use @orcapod.dataclass "
                f"in place of @dataclasses.dataclass, not on top of it."
            )
        if _uci.class_kind(target) == _uci.KIND_PYDANTIC:
            raise TypeError(
                f"{target.__qualname__} is a pydantic model. orcapod.dataclass makes "
                f"dataclasses only."
            )
        declared = _uci.validate_name(name if name is not None else target.__name__)
        own_eq = "__eq__" in vars(target)
        made = dataclasses.dataclass(target, **options)
        finish_structural_class(
            made, declared, structural_equality=options.get("eq", True) and not own_eq
        )
        return made

    if cls is None:
        return decorate
    return decorate(cls)


def finish_structural_class(
    cls: type, name: str, *, structural_equality: bool = True
) -> None:
    """Turn a dataclass that was created for this purpose into a structural class.

    Marks the class with its name and adds ``from_instance`` and, unless the class
    opted out of generated equality, the ``__eq__`` that compares across classes of
    one type. Used by ``orcapod.dataclass`` and for classes built from stored metadata.

    Args:
        cls: The dataclass just created.
        name: The validated name of the type.
        structural_equality: Whether to install ``structural_eq`` as ``__eq__``.
    """
    _uci.make_structural(cls, name)
    if structural_equality:
        setattr(cls, "__eq__", structural_eq)
    setattr(cls, "from_instance", classmethod(_from_instance))


# ---------------------------------------------------------------------------
# Equality across classes of one type
# ---------------------------------------------------------------------------

_MISSING = object()


def structural_eq(self: Any, other: Any) -> Any:
    """Compare two instances of structural classes of the same type by field values.

    Installed as ``__eq__`` by ``orcapod.dataclass``. Instances of the same class
    compare as dataclasses do. Instances of two structural classes with the same name
    and fields (a class defined again, a class built from stored metadata) are equal
    when their compared fields are equal. Anything else is not comparable.

    Args:
        self: An instance of a structural class.
        other: Any object.

    Returns:
        ``True`` or ``False`` for an instance of a class of the same type, else
        ``NotImplemented``.
    """
    mine, theirs = self.__class__, other.__class__
    if theirs is not mine and not (
        _uci.is_structural(theirs) and _uci.same_user_type(mine, theirs)
    ):
        return NotImplemented
    names = [field.name for field in dataclasses.fields(self) if field.compare]
    return tuple(getattr(self, name) for name in names) == tuple(
        getattr(other, name, _MISSING) for name in names
    )


# ---------------------------------------------------------------------------
# Conversion: an instance of the class from an instance of another dataclass
# ---------------------------------------------------------------------------


def _from_instance(cls: type, other: Any) -> Any:
    """Build an instance of this class from an instance of another dataclass.

    This is the only conversion between classes, and Orcapod never calls it for you.

    Args:
        other: An instance of an ordinary dataclass or an ``orcapod.dataclass`` with
            exactly the same field names and the same declared field types.

    Returns:
        A new instance of this class with ``other``'s field values. Fields typed by
        an ``orcapod.dataclass`` are converted the same way.

    Raises:
        TypeError: If ``other`` is not a dataclass instance, or its fields differ
            from this class's fields.
    """
    return cls(**converted_fields(cls, other))


def converted_fields(cls: type, other: Any) -> dict[str, Any]:
    """Return the keyword arguments that build a ``cls`` from ``other``.

    This is the acceptance rule of ``from_instance``: ``other`` is a dataclass instance whose stored fields have exactly the names of
    ``cls``'s fields, and whose declared field types agree with ``cls``'s.

    Args:
        cls: A structural class.
        other: The instance to convert.

    Returns:
        Field name to value, values of structural field types converted.

    Raises:
        TypeError: If ``other`` is not a dataclass instance, a field is missing or
            extra, or a field's declared type differs.
    """
    source = type(other)
    if not dataclasses.is_dataclass(other) or isinstance(other, type):
        raise TypeError(
            f"{cls.__qualname__} can be built from an instance of a dataclass, not "
            f"from {other!r}."
        )
    wanted = _uci.stored_field_names(cls)
    offered = _uci.stored_field_names(source)
    if set(wanted) != set(offered):
        missing = sorted(set(wanted) - set(offered))
        extra = sorted(set(offered) - set(wanted))
        raise TypeError(
            f"Cannot build {cls.__qualname__} from an instance of "
            f"{source.__qualname__}: the fields differ (missing: {missing}, not in "
            f"{cls.__qualname__}: {extra}). The two classes must have the same fields."
        )
    wanted_hints = _uci.type_hints(cls)
    offered_hints = _uci.type_hints(source)
    values: dict[str, Any] = {}
    for field in wanted:
        target = wanted_hints.get(field, Any)
        if not _convertible(offered_hints.get(field, Any), target):
            raise TypeError(
                f"Cannot build {cls.__qualname__} from an instance of "
                f"{source.__qualname__}: field {field!r} is declared "
                f"{offered_hints.get(field, Any)!r} there and {target!r} here."
            )
        values[field] = _converted_value(getattr(other, field), target)
    return values


def _convertible(offered: Any, wanted: Any) -> bool:
    """Return True if a field declared ``offered`` can fill a field declared ``wanted``.

    The declarations are equal, or differ only where ``wanted`` names a structural
    class and ``offered`` a dataclass (converted by ``from_instance``, which checks
    its fields in turn). Generics are compared by origin and arguments, union members
    in any order.
    """
    if offered == wanted:
        return True
    if isinstance(offered, type) and isinstance(wanted, type):
        return _uci.is_structural(wanted) and dataclasses.is_dataclass(offered)
    origin_offered, origin_wanted = (
        typing.get_origin(offered),
        typing.get_origin(wanted),
    )
    if origin_offered is None or origin_wanted is None:
        return False
    args_offered, args_wanted = typing.get_args(offered), typing.get_args(wanted)
    if origin_offered in _UNIONS and origin_wanted in _UNIONS:
        return all(
            any(_convertible(x, y) for y in args_wanted) for x in args_offered
        ) and all(any(_convertible(x, y) for x in args_offered) for y in args_wanted)
    if origin_offered != origin_wanted or len(args_offered) != len(args_wanted):
        return False
    return all(_convertible(x, y) for x, y in zip(args_offered, args_wanted))


def _mentions_structural(annotation: Any) -> bool:
    """Return True if an annotation names a structural class at any depth."""
    if isinstance(annotation, type) and typing.get_origin(annotation) is None:
        return _uci.is_structural(annotation)
    return any(_mentions_structural(arg) for arg in typing.get_args(annotation))


def _converted_value(value: Any, annotation: Any) -> Any:
    """Return a field value converted to the structural classes its annotation names.

    Used by ``from_instance`` for the fields of the instance it builds. Where the
    annotation names a structural class and the value is an instance of another
    dataclass, the value is replaced by ``from_instance`` of the annotated class.
    Lists, sets, tuples and dict values are rebuilt only when their annotation names a
    structural class; every other value is returned as it is.

    Args:
        value: The field value of the instance being converted.
        annotation: The annotation of the field receiving it.

    Returns:
        The value to store in the new instance.

    Raises:
        TypeError: If a value cannot be converted to the annotated structural class.
    """
    if value is None or not _mentions_structural(annotation):
        return value
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is None:
        if type(value) is annotation:
            return value
        return annotation.from_instance(value)
    if origin in _UNIONS:
        for member in args:
            if isinstance(member, type) and typing.get_origin(member) is None:
                if _uci.is_structural(member) and dataclasses.is_dataclass(value):
                    return _converted_value(value, member)
            elif typing.get_origin(member) is type(value):
                return _converted_value(value, member)
        return value
    if origin in (list, set, frozenset) and len(args) == 1:
        return origin(_converted_value(item, args[0]) for item in value)
    if origin is tuple:
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(_converted_value(item, args[0]) for item in value)
        if len(args) == len(value):
            return tuple(_converted_value(item, arg) for item, arg in zip(value, args))
        return value
    if origin is dict and len(args) == 2:
        return {key: _converted_value(item, args[1]) for key, item in value.items()}
    return value
