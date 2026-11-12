"""Registering your own data types (dataclasses and pydantic models).

Orcapod identifies a dataclass or pydantic model by its **name**, its **kind**
(dataclass or pydantic) and its **fields** (names and types, in declared order). Two classes
that agree on all three are the same type: results stored with one are found again with
the other. See ``docs/concepts/your-own-data-types.md``.

``register_type`` declares the name a class is identified by. Without it, a class
defined in a notebook or script (``__main__``) is named by its class name and a class
defined in a module by its module path and class name.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar, overload

import orcapod.logical_types.user_class_identity as _uci

T = TypeVar("T", bound=type)

__all__ = ["register_type"]


@overload
def register_type(
    cls: T, /, *, name: str | None = None, qualified_name: bool | None = None
) -> T: ...


@overload
def register_type(
    *, name: str | None = None, qualified_name: bool | None = None
) -> Callable[[T], T]: ...


def register_type(
    cls: T | None = None,
    /,
    *,
    name: str | None = None,
    qualified_name: bool | None = None,
) -> T | Callable[[T], T]:
    """Declare the name a dataclass or pydantic model is identified by.

    Use it as a decorator, with or without arguments, or call it on a class you do
    not own. The class is never modified: the name is recorded in Orcapod's registry
    and the same class object is returned.

    Without arguments the class keeps its default name: its class name if it is
    defined in a notebook or script, its module path and class name if it is defined
    in a module. ``qualified_name=False`` names a module class by its class name alone,
    without retyping it.

    Declare the name before the class is first used in a source or pod. Declaring the
    same class again with the same name does nothing; a different name raises.

    Args:
        cls: The class, when called directly.
        name: The declared name, e.g. ``"Point"`` or ``"geo.Point"``. It must not be
            empty, contain ``@`` or whitespace, or start with ``orcapod.``.
        qualified_name: Only without ``name``. ``False``: the class name (``"Point"``).
            ``True``: the module path and class name (``"pkg.models.Point"``).

    Returns:
        The class unchanged, or a decorator that returns it unchanged.

    Raises:
        TypeError: If the class is not a dataclass or pydantic model.
        ValueError: If both ``name`` and ``qualified_name`` are given, the name is invalid,
            the class is defined inside a function, or the class is already
            registered under a different name.

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
        """Declare ``target`` under the given name and return it unchanged."""
        _uci.declare(target, name, qualified_name=qualified_name)
        return target

    if cls is None:
        return decorate
    return decorate(cls)
