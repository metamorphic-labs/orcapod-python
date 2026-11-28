"""Attribute an Orcapod warning to the user's code."""

from __future__ import annotations

import linecache
import sys
import warnings
from types import FrameType

WarningRegistry = dict[str | tuple[str, type[Warning], int], int]


def outside_orcapod_stacklevel() -> int:
    """Return the stacklevel of the first frame outside Orcapod, as seen from the caller.

    Level 1 is the caller of this function, the function that calls
    ``warnings.warn``. When every frame is Orcapod's (a worker thread), the caller
    of that function is used.

    Returns:
        The ``stacklevel`` to pass to ``warnings.warn``.
    """
    level = 1
    frame: FrameType | None = sys._getframe(1)  # pyright: ignore[reportPrivateUsage]  # as the warnings module does
    while frame is not None:
        module = str(frame.f_globals.get("__name__", ""))
        if module != "orcapod" and not module.startswith("orcapod."):
            return level
        frame = frame.f_back
        level += 1
    return 2


def error_filter_active(
    message: str, category: type[Warning], frame: FrameType
) -> bool:
    """Return whether the first warning filter matching this warning makes it an error."""
    return warning_filter_action(message, category, frame) == "error"


def warning_filter_action(
    message: str, category: type[Warning], frame: FrameType
) -> str | None:
    """Find the action of the first explicit filter matching a warning at a frame."""
    module = str(frame.f_globals.get("__name__", "<string>"))
    for action, text, filter_category, module_pattern, line in warnings.filters:
        if (
            issubclass(category, filter_category)
            and (text is None or text.match(message))
            and (module_pattern is None or module_pattern.match(module))
            and line in (0, frame.f_lineno)
        ):
            return action
    return None


def warn_at_frame(
    message: str,
    category: type[Warning],
    frame: FrameType,
    *,
    registry: WarningRegistry | None = None,
) -> None:
    """Issue a warning shown at ``frame``'s line through Python's warning filters.

    The one place Orcapod issues a warning at a frame it names (ITL-1344), for the
    warnings of a run, which are shown at the user's line that started it. With no
    site registry, Python's once-per-text-and-line rule cannot hide the same warning
    of the next run (a notebook cell run again).

    The frame's globals are never given to ``warnings.warn_explicit`` as
    ``module_globals``. Python uses them only to find the line's source text, and that
    lookup raises out of the call instead of warning (measured on Python 3.11.18,
    3.12.15, 3.13.18 and 3.14.10):

    - ``__spec__`` and ``__loader__`` both ``None``, as in an IPython or Jupyter
      namespace: ``ValueError`` on 3.12; also on 3.13 and 3.14 when the module is not
      named ``__main__`` (a module made with ``types.ModuleType``).
    - ``python -m package.module`` (named ``__main__``, loaded under another name):
      ``ImportError`` on 3.11 and 3.12.
    - A module whose file got shorter after it was imported: ``IndexError`` on 3.11
      and 3.12.
    - A loader whose ``get_source`` raises: that error, on 3.11 and 3.12; on 3.13 and
      3.14 unless it is an ``ImportError``.
    - A script (a loader and no spec) on 3.12, a spec without a loader beside a
      ``__loader__`` on 3.12 and later: a ``DeprecationWarning`` from ``importlib``,
      an error under ``-W error``.

    The source is registered with ``linecache.lazycache`` instead, as ``traceback``
    does for every frame it formats: it raises in none of these cases, and the line of
    a module with no file on disk (a zip import) is shown on every version.

    Args:
        message: The warning's text.
        category: The warning's class.
        frame: The frame whose file, line and module name the warning is shown at.
        registry: Optional caller-owned registry populated by Python's warning filters.
    """
    filename = frame.f_code.co_filename
    _ = linecache.lazycache(filename, frame.f_globals)
    warnings.warn_explicit(
        message,
        category,
        filename,
        frame.f_lineno,
        module=frame.f_globals.get("__name__"),
        registry=registry,
    )
