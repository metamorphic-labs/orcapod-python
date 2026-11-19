"""Prove that ITL-1266 only moved golden entries between files, values unchanged.

Usage::

    uv run python docs/dev/scripts/itl_1266_moved_entries.py <base revision>

Reads every golden file of data context v1 (frozen and provisional) at the base revision
(git) and in the working tree, and compares: the same entry names, the same values, and
the list of entries whose file changed. Exits 1 on any other difference.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

FOLDER = "tests/test_hashing/golden/data/v1"


def _at(revision: str) -> dict[str, tuple[str, object]]:
    names = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", revision, FOLDER],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    out: dict[str, tuple[str, object]] = {}
    for name in names:
        text = subprocess.run(
            ["git", "show", f"{revision}:{name}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        for entry, value in json.loads(text).items():
            assert entry not in out, entry
            out[entry] = (name.removeprefix(FOLDER + "/"), value)
    return out


def _tree() -> dict[str, tuple[str, object]]:
    out: dict[str, tuple[str, object]] = {}
    for path in sorted(Path(FOLDER).rglob("*.json")):
        for entry, value in json.loads(path.read_text()).items():
            assert entry not in out, entry
            out[entry] = (path.relative_to(FOLDER).as_posix(), value)
    return out


def main() -> int:
    before, after = _at(sys.argv[1]), _tree()
    failures = 0
    if set(before) != set(after):
        print("entry names differ:", sorted(set(before) ^ set(after)))
        failures += 1
    changed_value = sorted(
        e for e in set(before) & set(after) if before[e][1] != after[e][1]
    )
    if changed_value:
        print("values differ:", changed_value)
        failures += 1
    moved: dict[tuple[str, str], list[str]] = {}
    for entry in sorted(set(before) & set(after)):
        if before[entry][0] != after[entry][0]:
            moved.setdefault((before[entry][0], after[entry][0]), []).append(entry)
    print(f"entries before: {len(before)}, after: {len(after)}")
    print(f"values changed: {len(changed_value)}")
    for (source, target), entries in sorted(moved.items()):
        cases = sorted({e.split(".", 1)[0] for e in entries})
        print(f"moved {len(entries)}: {source} -> {target}; cases {cases}")
    print(f"moved in all: {sum(len(e) for e in moved.values())}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
