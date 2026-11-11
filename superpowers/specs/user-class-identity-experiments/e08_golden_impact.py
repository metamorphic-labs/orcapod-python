"""E08: which pinned golden values (PR #284, ITL-626) move if a class's identity string changes.

PR #284 pins hash values in ``tests/test_hashing/golden/data/*.json``. Its fixtures
``GoldenPoint`` (dataclass) and ``GoldenModel`` (pydantic) are registered under
``tests.test_hashing.golden.cases.GoldenPoint`` / ``...GoldenModel``.

This script copies the golden package from a git ref into a temp dir, recomputes every
group in a fresh process with the fixtures' identity changed, and lists the pinned
entries whose value differs, or which disappear because their key embeds a hash that
moved (per-row system-tag entries are keyed by the source's pipeline hash). Any design that changes the identity string of an existing
class (a different module, a module-less name, a structural key) moves exactly the
entries listed under the "module-less name" variant; the ``__main__`` variant shows what
an identical class defined in a notebook gets.

The ``unchanged`` variant must report no moved entries; otherwise the checkout and the
ref disagree for reasons unrelated to ITL-881 and those entries are listed separately.

Usage::

    git fetch origin eywalker/itl-626-guard-hash-stability-with-golden-regression
    uv run python superpowers/specs/user-class-identity-experiments/e08_golden_impact.py \
        [--ref <commit-or-branch>]

Default ref: the PR #284 head this analysis was written against.
"""

from __future__ import annotations

import argparse
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from _harness import run_python, write_files  # pyright: ignore[reportImplicitRelativeImport]  # run as a script beside _harness.py

DEFAULT_REF = "bc053f8917bff9c0b8d28965cc436755e52805df"
GOLDEN = "tests/test_hashing/golden"
FILES = [
    "__init__.py",
    "cases.py",
    "data/semantic.json",
    "data/schema.json",
    "data/datagram.json",
    "data/arrow.json",
    "data/pipeline.json",
]

CHILD = """
import json
from pathlib import Path
from tests.test_hashing.golden import cases
from _harness import report

for cls in (cases.GoldenPoint, cases.GoldenModel):
    {patch}

computed = cases.compute_all(allow_errors=True)
data = Path(cases.__file__).parent / "data"
moved = {{}}
for group, values in computed.items():
    pinned = json.loads((data / f"{{group}}.json").read_text())
    for key in sorted(pinned):
        if key not in values:
            moved[f"{{group}}: {{key}}"] = "entry gone (its key embeds a hash that moved)"
        elif pinned[key] != values[key]:
            moved[f"{{group}}: {{key}}"] = "value changed"
report(moved)
"""

VARIANTS = {
    "unchanged": "pass",
    'module = "__main__" (class defined in a notebook)': 'cls.__module__ = "__main__"',
    "module-less name (any change of the identity string)": (
        'cls.__module__ = "<no module>"'
    ),
}


def git_show(ref: str, path: str) -> str:
    """Return a file's content at ``ref``.

    Args:
        ref: Commit or branch.
        path: Repository-relative path.

    Returns:
        The file content.
    """
    return subprocess.run(
        ["git", "show", f"{ref}:{path}"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def main() -> None:
    """Extract the golden package and print the moved entries per variant."""
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument("--ref", default=DEFAULT_REF)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        files = {f"{GOLDEN}/{f}": git_show(args.ref, f"{GOLDEN}/{f}") for f in FILES}
        files["tests/__init__.py"] = ""
        files["tests/test_hashing/__init__.py"] = ""
        write_files(root, files)
        baseline_noise: set[str] = set()
        for name, patch in VARIANTS.items():
            res = run_python(["-c", CHILD.format(patch=patch)], cwd=root, timeout=600)
            moved: dict[str, Any] = res["result"] if res["ok"] else {}
            print(f"\n## {name}")
            if not res["ok"]:
                print(f"  FAILED: {res['error']}")
                continue
            if name == "unchanged":
                baseline_noise = set(moved)
                print(f"  moved entries: {len(moved)} (must be 0)")
                for key in moved:
                    print(f"    {key}")
                continue
            relevant = {k: v for k, v in moved.items() if k not in baseline_noise}
            print(f"  pinned entries that change or disappear: {len(relevant)}")
            for key, what in relevant.items():
                print(f"    {key:<100} {what}")


if __name__ == "__main__":
    main()
