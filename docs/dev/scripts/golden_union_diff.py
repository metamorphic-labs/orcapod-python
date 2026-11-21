"""Compare the union of golden entries at a git revision with the working tree.

Every golden file of the current data context (``tests/test_hashing/golden/data/<folder>/``)
is a flat JSON object of ``entry: value``. This script reads every file at ``BASE`` (a git
revision) and in the working tree, keys each entry by ``<group>:<entry>`` (the group is the
file name before ``-NN.json``), and prints the entries that changed, were added or were
removed. Used in the pull request of ITL-1246 / ITL-1227 to show that exactly the approved
entries changed and every other entry is byte-identical.

Usage::

    uv run python docs/dev/scripts/golden_union_diff.py [BASE]   # default: origin/main
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

GOLDEN = Path("tests/test_hashing/golden/data")


def _group(name: str) -> str:
    return name.rsplit("-", 1)[0]


def entries_at(revision: str) -> dict[str, str]:
    """The union of golden entries in the files at ``revision``."""
    listing = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", revision, str(GOLDEN)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    union: dict[str, str] = {}
    for path in listing:
        if not path.endswith(".json"):
            continue
        text = subprocess.run(
            ["git", "show", f"{revision}:{path}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        group = _group(Path(path).stem)
        for key, value in json.loads(text).items():
            union[f"{group}:{key}"] = json.dumps(value, sort_keys=True)
    return union


def entries_in_tree() -> dict[str, str]:
    """The union of golden entries in the working tree's files."""
    union: dict[str, str] = {}
    for path in sorted(GOLDEN.rglob("*.json")):
        group = _group(path.stem)
        for key, value in json.loads(path.read_text()).items():
            union[f"{group}:{key}"] = json.dumps(value, sort_keys=True)
    return union


def main() -> None:
    base = sys.argv[1] if len(sys.argv) > 1 else "origin/main"
    before = entries_at(base)
    after = entries_in_tree()
    changed = sorted(k for k in before.keys() & after.keys() if before[k] != after[k])
    added = sorted(after.keys() - before.keys())
    removed = sorted(before.keys() - after.keys())
    same = len(before.keys() & after.keys()) - len(changed)
    print(f"base {base}: {len(before)} entries; working tree: {len(after)} entries")
    print(f"unchanged (byte-identical values): {same}")
    print(f"changed: {len(changed)}")
    for key in changed:
        print(f"  {key}\n    before {before[key]}\n    after  {after[key]}")
    print(f"added: {len(added)}")
    for key in added:
        print(f"  {key}")
    print(f"removed: {len(removed)}")
    for key in removed:
        print(f"  {key}")


if __name__ == "__main__":
    main()
