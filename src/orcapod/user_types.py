"""Registering your own data types (dataclasses and pydantic models).

Orcapod identifies a dataclass or pydantic model by its **name**, its **kind**
(dataclass or pydantic) and its **fields** (names and types, in any order). Two classes
that agree on all three are the same type: results stored with one are found again with
the other. See ``docs/concepts/your-own-data-types.md``.

``register_type`` declares the name a class is identified by. ``set_type_naming`` sets
how classes that declare no name are named.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar, overload

import orcapod.logical_types.user_class_identity as _uci

T = TypeVar("T", bound=type)

__all__ = ["register_type", "set_type_naming"]


@overload
def register_type(cls: T, /, *, name: str | None = None) -> T: ...


@overload
def register_type(*, name: str | None = None) -> Callable[[T], T]: ...


def register_type(
    cls: T | None = None, /, *, name: str | None = None
) -> T | Callable[[T], T]:
    """Declare the name a dataclass or pydantic model is identified by.

    Use it as a decorator, with or without a name, or call it on a class you do not
    own. Without a name, the class is named by the naming setting (see
    ``set_type_naming``): a class defined in a notebook or script by its class name,
    a class defined in a module by its module path and class name.

    Declare the name before the class is first used in a source or pod. A class
    defined again with the same name and fields (a re-run notebook cell) is the same
    type; with different fields it is a different type.

    Args:
        cls: The class, when called directly.
        name: The declared name, e.g. ``"Point"`` or ``"geo.Point"``. It must not be
            empty, contain ``@`` or whitespace, or start with ``orcapod.``.

    Returns:
        The class unchanged, or a decorator that returns it unchanged.

    Raises:
        TypeError: If the class is not a dataclass or pydantic model.
        ValueError: If the name is invalid, the class is defined inside a function, or
            the class was already used under a different name.

    Example:
        >>> import dataclasses
        >>> import orcapod as op
        >>> @op.register_type(name="Point")
        ... @dataclasses.dataclass
        ... class Point:
        ...     x: int
        ...     y: int
    """

    def decorate(target: T) -> T:
        _uci.declare(target, name)
        return target

    if cls is None:
        return decorate
    return decorate(cls)


def set_type_naming(
    *, main: _uci.NamingStyle | None = None, module: _uci.NamingStyle | None = None
) -> None:
    """Set how dataclasses and pydantic models that declare no name are named.

    Applies to classes first used after the call, so call it before defining or using
    your classes.

    Args:
        main: Classes defined in a notebook or a script run directly (``__main__``):
            ``"name"`` (default) uses the class name (``"Point"``); ``"qualified"``
            uses ``"__main__.Point"``.
        module: Classes defined in an importable module: ``"qualified"`` (default)
            uses the module path and class name (``"pkg.models.Point"``); ``"name"``
            uses the class name only.

    Raises:
        ValueError: If a value is not ``"name"`` or ``"qualified"``.
    """
    _uci.set_naming(main=main, module=module)
