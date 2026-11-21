"""The warning that a value type's hash is provisional (ITL-1068).

The library value types are declared provisional in the ``provisional`` list of
``orcapod/contexts/logical_types.json``; the logical type registry and the value
handler registry of a data context hold that list and call ``warn_provisional``
when they resolve a declared type. This module keeps the once-per-type bookkeeping, which
is process-wide, and notes every use by the type's extension name
(``utils/provisional.note_provisional_use``, ITL-1324), as a provisional operator does: the
golden hash tool refuses a frozen case that uses one without declaring it.
"""

from __future__ import annotations

import threading
import warnings

from orcapod.errors import ProvisionalHashWarning
from orcapod.utils.provisional import note_provisional_use
from orcapod.utils.warning_utils import outside_orcapod_stacklevel

_lock = threading.RLock()
_issued: set[str] = set()


def warn_provisional(extension_name: str) -> None:
    """Note a use of a provisional type and warn, once per type per process.

    The use is noted every time (``note_provisional_use``), by the extension name. The
    type is recorded as warned only after ``warnings.warn`` returns, so under an error
    filter every use raises. The warning is attributed to the first frame outside
    Orcapod, the code that used the type.

    Args:
        extension_name: The Arrow extension name of a type declared provisional.
    """
    note_provisional_use(extension_name)
    if extension_name in _issued:
        return
    with _lock:
        if extension_name in _issued:
            return
        warnings.warn(
            f"{extension_name} values are provisional: their value hash and stored "
            "identity may change in Orcapod v0.4, and results computed from them "
            "before then may be recomputed after the upgrade (nothing stored is lost "
            "or wrong). See 'Provisional value hashes' in docs/concepts/arrays.md. To "
            "silence: warnings.filterwarnings('ignore', "
            "category=orcapod.errors.ProvisionalHashWarning).",
            ProvisionalHashWarning,
            stacklevel=outside_orcapod_stacklevel(),
        )
        _issued.add(extension_name)


def reset_provisional_warnings() -> None:
    """Forget which types have warned, so each warns again on its next use.

    A test hook: a test that checks the warning starts as a new process would.
    """
    with _lock:
        _issued.clear()
