"""Check the regenerated golden values against the approved moved list (ITL-1425).

The approved list is ``golden-moved-implementation.tsv`` (684 entries: the 659 approved
first, then the 25 of the provisional Filter cases that main added in PR 498, approved
the same day). The script compares every golden file of data context v1 at a base
revision (the golden values before the regeneration) with the files in the working tree:

* every approved entry is pinned at the base with its ``pinned`` value, and is pinned in
  the working tree with its ``live`` value;
* every other entry is pinned at the base and in the working tree with one value;
* no entry is pinned at one side only, and no entry is pinned twice.

Run from the repository root, giving the base revision:
``uv run python docs/dev/specs/itl-1425-source-identity/scripts/verify_regenerated_golden.py <rev>``.
"""

from __future__ import annotations

import ast
import csv
import json
import pathlib
import subprocess
import sys

GOLDEN = "tests/test_hashing/golden/data/v1"
TSV = pathlib.Path(
    "docs/dev/specs/itl-1425-source-identity/golden-moved-implementation.tsv"
)


def group_of(path: str) -> str:
    """The golden group a file holds: ``schema-01.json`` and ``provisional/schema.json``
    are both ``schema``."""
    return pathlib.PurePosixPath(path).stem.split("-")[0]


def pins_at(rev: str | None) -> dict[tuple[str, str], tuple[str, object]]:
    """Every pinned entry, keyed by (group, entry), with the file and value; ``None``
    reads the working tree."""
    if rev is None:
        paths = [str(p) for p in pathlib.Path(GOLDEN).rglob("*.json")]
    else:
        listing = subprocess.run(
            ["git", "ls-tree", "-r", "--name-only", rev, GOLDEN],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        paths = [p for p in listing.split() if p.endswith(".json")]
    pins: dict[tuple[str, str], tuple[str, object]] = {}
    for path in sorted(paths):
        if rev is None:
            text = pathlib.Path(path).read_text()
        else:
            text = subprocess.run(
                ["git", "show", f"{rev}:{path}"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        entries: dict[str, object] = json.loads(text)
        for entry, value in entries.items():
            key = (group_of(path), entry)
            if key in pins:
                raise SystemExit(f"pinned twice: {key} in {pins[key][0]} and {path}")
            pins[key] = (path, value)
    return pins


def tsv_value(text: str) -> object:
    """A TSV cell: a list is written as its Python repr, anything else as the text."""
    return ast.literal_eval(text) if text.startswith("[") else text


def main(rev: str) -> int:
    with TSV.open() as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    approved = {
        (row["group"], row["entry"]): (tsv_value(row["pinned"]), tsv_value(row["live"]))
        for row in rows
    }
    if len(approved) != len(rows):
        print(
            f"the TSV lists an entry twice ({len(rows)} rows, {len(approved)} entries)"
        )
        return 1
    before, after = pins_at(rev), pins_at(None)
    problems: list[str] = []
    for key in sorted(before.keys() ^ after.keys()):
        side = "base only" if key in before else "working tree only"
        problems.append(f"{key[0]}:{key[1]} pinned on the {side}")
    moved_files: dict[tuple[str, str], int] = {}
    matched = unchanged = 0
    for key in sorted(before.keys() & after.keys()):
        (old_path, old), (new_path, new) = before[key], after[key]
        if key in approved:
            pinned, live = approved[key]
            if old != pinned:
                problems.append(
                    f"{key[0]}:{key[1]}: base {old!r}, TSV pinned {pinned!r}"
                )
            if new != live:
                problems.append(
                    f"{key[0]}:{key[1]}: regenerated {new!r}, TSV live {live!r}"
                )
            if old == pinned and new == live:
                matched += 1
        elif old != new:
            problems.append(
                f"{key[0]}:{key[1]}: not approved, moved {old!r} -> {new!r}"
            )
        else:
            unchanged += 1
        if old_path != new_path:
            pair = (old_path, new_path)
            moved_files[pair] = moved_files.get(pair, 0) + 1
    for key in sorted(approved.keys() - before.keys()):
        problems.append(f"{key[0]}:{key[1]}: approved but not pinned at the base")
    print(f"base revision: {rev}")
    print(f"approved entries (TSV): {len(approved)}")
    print(f"approved entries regenerated with the TSV live value: {matched}")
    print(f"other entries, value unchanged: {unchanged}")
    print(
        f"entries pinned at the base: {len(before)}; in the working tree: {len(after)}"
    )
    for (old_path, new_path), count in sorted(moved_files.items()):
        print(f"entries now pinned in another file: {count} ({old_path} -> {new_path})")
    print(f"problems: {len(problems)}")
    for problem in problems:
        print(f"  {problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
