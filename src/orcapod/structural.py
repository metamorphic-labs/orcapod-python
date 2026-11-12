"""``orcapod.dataclass``: dataclasses identified by name and fields (ITL-972).

An ordinary ``@dataclass`` is identified by its exact class: a pod annotated with it
accepts that class object and no other. A class made with ``@orcapod.dataclass`` is
**structural**: Orcapod identifies it by its name and its fields (names, types and
order), so two such classes with the same name and fields are one type, stored values
can be read back without the class being defined, and a pod parameter annotated with
the class always receives an instance of that class.

See ``docs/concepts/your-own-data-types.md``.
"""

from __future__ import annotations

import dataclasses
import functools
import types
import typing
from collections.abc import Callable
from typing import Any, TypeVar, overload

import orcapod.logical_types.user_class_identity as _uci

T = TypeVar("T")

__all__ = ["dataclass"]

#: Decision point O3 (ITL-972): whether ``P(other)`` converts, beside
#: ``P.from_instance(other)``.
CONSTRUCTOR_CONVERTS = True

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
    a real dataclass (``dataclasses.fields``, ``asdict`` and ``replace`` work) with one
    addition: ``from_instance``, which builds an instance from an instance of another
    dataclass with the same fields.

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
        made = dataclasses.dataclass(target, **options)
        _uci.make_structural(made, declared)
        _add_conversion(made)
        return made

    if cls is None:
        return decorate
    return decorate(cls)


# ---------------------------------------------------------------------------
# Conversion: an instance of the class from an instance of another dataclass
# ---------------------------------------------------------------------------


def _add_conversion(cls: type) -> None:
    """Add ``from_instance`` and the converting constructor to a structural class.

    Called on a class that ``orcapod.dataclass`` has just created.
    """

    def from_instance(klass: type, other: Any) -> Any:
        """Build an instance of this class from an instance of another dataclass.

        Args:
            other: An instance of an ordinary dataclass or an ``orcapod.dataclass``
                with the same fields.

        Returns:
            A new instance of this class with ``other``'s field values. Fields typed
            by an ``orcapod.dataclass`` are converted the same way.

        Raises:
            TypeError: If ``other`` is not a dataclass instance, or its fields differ
                from this class's fields.
        """
        return klass(**converted_fields(klass, other))

    setattr(cls, "from_instance", classmethod(from_instance))

    if not CONSTRUCTOR_CONVERTS:
        return
    generated = cls.__init__

    @functools.wraps(generated)
    def __init__(self: Any, *args: Any, **kwargs: Any) -> None:
        if len(args) == 1 and not kwargs and _is_conversion_call(cls, args[0]):
            generated(self, **converted_fields(cls, args[0]))
            return
        generated(self, *args, **kwargs)

    setattr(cls, "__init__", __init__)


def _is_conversion_call(cls: type, argument: Any) -> bool:
    """Return True if ``cls(argument)`` means "convert ``argument``".

    It does when the argument is a dataclass instance and the class's first field
    could not itself hold a dataclass: the decision depends on the class definition
    and on the kind of the argument, never on values.
    """
    if not dataclasses.is_dataclass(argument) or isinstance(argument, type):
        return False
    names = _uci.stored_field_names(cls)
    if not names:
        return True
    return not _may_hold_dataclass(_uci.type_hints(cls).get(names[0], Any))


def _may_hold_dataclass(annotation: Any) -> bool:
    """Return True if a field with this annotation could hold a dataclass instance."""
    if annotation is Any or annotation is object:
        return True
    if isinstance(annotation, typing.TypeVar):
        return True
    if typing.get_origin(annotation) in _UNIONS:
        return any(_may_hold_dataclass(arg) for arg in typing.get_args(annotation))
    return isinstance(annotation, type) and dataclasses.is_dataclass(annotation)


def converted_fields(cls: type, other: Any) -> dict[str, Any]:
    """Return the keyword arguments that build a ``cls`` from ``other``.

    This is the acceptance rule of the conversion (decision point O4, ITL-972):
    ``other`` is a dataclass instance whose stored fields have exactly the names of
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
        values[field] = convert_argument(getattr(other, field), target)
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


def mentions_structural(annotation: Any) -> bool:
    """Return True if an annotation names a structural class at any depth.

    Args:
        annotation: A type annotation.

    Returns:
        Whether ``convert_argument`` can change a value of this annotation.
    """
    if isinstance(annotation, type) and typing.get_origin(annotation) is None:
        return _uci.is_structural(annotation)
    return any(mentions_structural(arg) for arg in typing.get_args(annotation))


def convert_argument(value: Any, annotation: Any) -> Any:
    """Return ``value`` with every structural class in ``annotation`` applied.

    Where the annotation names a structural class and the value is an instance of
    another dataclass, the value is replaced by an instance of the annotated class
    (``from_instance``). Lists, sets, tuples and dict values are rebuilt only when
    their annotation names a structural class; every other value is returned as it is.

    Args:
        value: The value to pass.
        annotation: The annotation of the parameter or field receiving it.

    Returns:
        The value to pass in its place.

    Raises:
        TypeError: If a value cannot be converted to the annotated structural class.
    """
    if value is None or not mentions_structural(annotation):
        return value
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is None:
        if type(value) is annotation:
            return value
        return annotation.from_instance(value)
    if origin in _UNIONS:
        return _convert_union_member(value, args)
    if origin in (list, set, frozenset) and len(args) == 1:
        return origin(convert_argument(item, args[0]) for item in value)
    if origin is tuple:
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(convert_argument(item, args[0]) for item in value)
        if len(args) == len(value):
            return tuple(convert_argument(item, arg) for item, arg in zip(value, args))
        return value
    if origin is dict and len(args) == 2:
        return {key: convert_argument(item, args[1]) for key, item in value.items()}
    return value


def _convert_union_member(value: Any, members: tuple[Any, ...]) -> Any:
    """Convert a value typed by a union to the member it belongs to.

    A dataclass instance goes to the structural member of the same type
    (``same_user_type``); a container goes to the first member with the same origin.
    A value no member claims is returned as it is.
    """
    cls = type(value)
    for member in members:
        if cls is member:
            return value
    for member in members:
        if isinstance(member, type) and typing.get_origin(member) is None:
            if (
                _uci.is_structural(member)
                and _uci.is_structural(cls)
                and _uci.same_user_type(cls, member)
            ):
                return member.from_instance(value)
        elif typing.get_origin(member) is cls:
            return convert_argument(value, member)
    return value
