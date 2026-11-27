"""Whether an object is exactly its class: what it does is what the class defines."""

from __future__ import annotations

import functools
import inspect


def reads_as_its_class(obj: object, cls: type) -> bool:
    """Whether ``obj`` is exactly ``cls`` and reads only through ``cls``'s own members.

    Exact, never by inspection of what a subclass overrides: ``type(obj) is cls``, and
    ``obj`` holds no attribute of its own that shadows a method or a property of
    ``cls`` (an ``iter_data`` replaced on the instance). A ``functools.cached_property``
    keeps its value in the instance by design and is not a shadow. A subclass, however
    small, is never ``cls`` here (ITL-1347).

    Args:
        obj: The object to check.
        cls: The class whose reading is proven.

    Returns:
        Whether ``obj`` reads as ``cls`` does.
    """
    if type(obj) is not cls:
        return False
    own = getattr(obj, "__dict__", None)
    if not own:
        return True
    missing = object()
    for name in own:
        member = inspect.getattr_static(cls, name, missing)
        if member is missing or isinstance(member, functools.cached_property):
            continue
        if callable(member) or hasattr(type(member), "__get__"):
            return False
    return True
