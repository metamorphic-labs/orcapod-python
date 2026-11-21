"""Let the ITL-1179 scripts run under starfix 0.5.0 on the current checkout.

Orcapod passes starfix's ``normalize_negative_zero`` and ``normalize_nan`` on every call
(``hashing/starfix_digest.py``); starfix 0.5.0 does not have them. Under 0.5.0,
``use_installed_starfix`` replaces the ``ArrowDigester`` that module calls with one that
drops the two keywords, which is what Orcapod did with 0.5.0 (commit 937a68f7 and before).
For an input without ``-0.0`` or a non-ordinary NaN (every input of these scripts) the
options change nothing, so the 0.5.0 run measures exactly what Orcapod computed with 0.5.0.
Under 0.6.0 or newer nothing is replaced.
"""

from __future__ import annotations

import importlib.metadata
from typing import Any

FLOAT_OPTIONS = ("normalize_negative_zero", "normalize_nan")


def use_installed_starfix() -> str:
    """Make Orcapod's starfix calls work with the installed starfix; return its version."""
    version = importlib.metadata.version("starfix")
    major, minor = (int(part) for part in version.split(".")[:2])
    if (major, minor) >= (0, 6):
        return version

    from orcapod.hashing import starfix_digest

    real = starfix_digest.ArrowDigester

    class _WithoutFloatOptions:
        """starfix 0.5.0's ``ArrowDigester``, accepting and dropping the float options."""

        @staticmethod
        def hash_table(table: Any, **options: Any) -> bytes:
            for name in FLOAT_OPTIONS:
                options.pop(name, None)
            return real.hash_table(table, **options)

        @staticmethod
        def hash_schema(schema: Any, **options: Any) -> bytes:
            for name in FLOAT_OPTIONS:
                options.pop(name, None)
            return real.hash_schema(schema, **options)

    starfix_digest.ArrowDigester = _WithoutFloatOptions  # pyright: ignore[reportAttributeAccessIssue]  # a script-only stand-in under 0.5.0
    return version
