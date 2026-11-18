"""Functions whose annotations are strings (``from __future__ import annotations``).

Used by ``test_none_return.py`` (ITL-1257): ``-> None`` read through
``typing.get_type_hints`` from a string annotation.
"""

from __future__ import annotations


def log_value(x: int) -> None:
    """Declares no outputs."""
    del x


def log_value_unannotated(x: int):  # no return annotation on purpose
    """Declares no outputs by having no return annotation."""
    del x


def maybe(x: int) -> int | None:
    """One output that may be null."""
    return None if x < 0 else x
