"""Check the licenses of the documentation site's npm packages.

The Python dependencies are checked by ``.github/workflows/_license-check.yml``
(``pip-licenses``). The site under ``site/`` is built with npm packages; this script
checks them against the same allow list, plus the permissive licenses only npm packages
use (``BlueOak-1.0.0``, ``CC0-1.0``) and the font license of the site's bundled fonts
(``OFL-1.1``).

Usage (the ``docs-site`` workflow runs it):

    cd site && pnpm licenses list --json --prod | uv run python ../scripts/check_site_licenses.py

The script uses the standard library only.
"""

from __future__ import annotations

import json
import sys
from typing import cast

# The license identifiers allowed, as in _license-check.yml, plus BlueOak-1.0.0,
# CC0-1.0 and OFL-1.1.
ALLOWED = frozenset(
    {
        "MIT",
        "Apache-2.0",
        "BSD-2-Clause",
        "BSD-3-Clause",
        "ISC",
        "LGPL-3.0-only",
        "MPL-2.0",
        "Python-2.0",
        "PSF-2.0",
        "Unlicense",
        "BlueOak-1.0.0",
        "CC0-1.0",
        # SIL Open Font License 1.1 (OSI-approved): the brand fonts (Asap, Lexend Exa)
        # bundled with the documentation site only, never with the Python package.
        # Approved by the developer, November 17, 2026 (ITL-1170).
        "OFL-1.1",
    }
)


def disallowed(report: dict[str, list[dict[str, str | list[str]]]]) -> list[str]:
    """Return ``"<package>@<versions>: <license>"`` for every package not allowed.

    Args:
        report: The output of ``pnpm licenses list --json``: license identifier to
            the packages under it.
    """
    problems: list[str] = []
    for license_id, packages in report.items():
        if license_id in ALLOWED:
            continue
        for package in packages:
            versions = package.get("versions", [])
            listed = ",".join(versions) if isinstance(versions, list) else versions
            problems.append(f"{package['name']}@{listed}: {license_id}")
    return sorted(problems)


def main() -> int:
    report = cast(dict[str, list[dict[str, str | list[str]]]], json.load(sys.stdin))
    problems = disallowed(report)
    if problems:
        print("npm packages under a license that is not allowed:", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        return 1
    count = sum(len(packages) for packages in report.values())
    print(f"{count} npm packages, all under allowed licenses")
    return 0


if __name__ == "__main__":
    sys.exit(main())
