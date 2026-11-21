"""The warning that a feature is still in development, once per feature per process.

A feature in development calls ``warn_in_development`` where it is first used; this
module keeps the once-per-feature bookkeeping, which is process-wide, as
``logical_types/provisional.py`` does for ``ProvisionalHashWarning``.
"""

from __future__ import annotations

import threading
import warnings

from orcapod.errors import InDevelopmentWarning
from orcapod.utils.warning_utils import outside_orcapod_stacklevel

_lock = threading.RLock()
_issued: set[str] = set()


def warn_in_development(feature: str, message: str) -> None:
    """Issue ``InDevelopmentWarning`` for a feature, once per process.

    The feature is recorded only after ``warnings.warn`` returns, so under an error
    filter every use raises. The warning is attributed to the first frame outside
    Orcapod, the code that used the feature.

    Args:
        feature: The feature's key, for the once-per-process bookkeeping.
        message: The warning's text; the instruction to silence it is appended.
    """
    if feature in _issued:
        return
    with _lock:
        if feature in _issued:
            return
        warnings.warn(
            f"{message} To silence: warnings.filterwarnings('ignore', "
            "category=orcapod.errors.InDevelopmentWarning).",
            InDevelopmentWarning,
            stacklevel=outside_orcapod_stacklevel(),
        )
        _issued.add(feature)


def reset_in_development_warnings() -> None:
    """Forget which features have warned, so each warns again on its next use.

    A test hook: a test that checks the warning starts as a new process would.
    """
    with _lock:
        _issued.clear()
