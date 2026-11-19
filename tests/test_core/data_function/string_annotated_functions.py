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


def unresolvable_input(x: Missing) -> None:  # noqa: F821  # pyright: ignore[reportUndefinedVariable]  # unresolvable on purpose
    """Declares no outputs; its parameter annotation cannot be resolved.

    ``typing.get_type_hints`` fails on it, so the return annotation stays the text
    ``"None"``; the input type is supplied with ``input_schema``.
    """
    del x


def unresolvable_input_quoted(x: Missing) -> "None":  # noqa: F821  # pyright: ignore[reportUndefinedVariable]  # unresolvable on purpose
    """As ``unresolvable_input``, with the return annotation written as a string.

    The postponed annotation is then the text ``"'None'"``.
    """
    del x
