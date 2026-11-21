"""ITL-1279 and ITL-1324: prove a relocation moved entries between files and changed none.

usage: uv run --no-project python docs/dev/scripts/itl_1279/relocation_proof.py \\
    [--root ROOT] [--ref origin/main]

Reads every golden file (frozen and provisional) of data context v1 under ``ROOT``
(default: this repository) and the same folder at git ``--ref`` of this repository, and
compares the union of entries: names and values must be identical. It prints, per entry
that changed its file, the old and the new file, counted by (case, old file, new file).
Writes nothing.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[4]
FOLDER = Path("tests/test_hashing/golden/data/v1")


def entries_at(root: Path | None, ref: str | None) -> dict[str, tuple[str, Any]]:
    """group:entry -> (file, value), from the working files of ``root`` or a git ref."""
    if ref is not None:
        listing = subprocess.run(
            ["git", "ls-tree", "-r", "--name-only", ref, f"{FOLDER}/"],
            cwd=REPO,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split()
        files = {
            Path(f).relative_to(FOLDER).as_posix(): subprocess.run(
                ["git", "show", f"{ref}:{f}"],
                cwd=REPO,
                capture_output=True,
                text=True,
                check=True,
            ).stdout
            for f in listing
            if f.endswith(".json")
        }
    else:
        assert root is not None
        base = root / FOLDER
        files = {
            p.relative_to(base).as_posix(): p.read_text() for p in base.rglob("*.json")
        }
    out: dict[str, tuple[str, Any]] = {}
    for name, text in files.items():
        group = Path(name).stem.split("-")[0]
        for entry, value in json.loads(text).items():
            key = f"{group}:{entry}"
            assert key not in out, f"{key} pinned twice"
            out[key] = (name, value)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=REPO)
    parser.add_argument("--ref", default="origin/main")
    args = parser.parse_args()
    before = entries_at(None, args.ref)
    after = entries_at(args.root, None)
    removed = sorted(set(before) - set(after))
    added = sorted(set(after) - set(before))
    changed = sorted(k for k in set(before) & set(after) if before[k][1] != after[k][1])
    moved: Counter[tuple[str, str, str]] = Counter()
    for key in set(before) & set(after):
        if before[key][0] != after[key][0]:
            case = key.split(".", 1)[0]
            moved[(case, before[key][0], after[key][0])] += 1
    print(f"entries before: {len(before)}; after: {len(after)}")
    print(f"changed: {len(changed)}; removed: {len(removed)}; added: {len(added)}")
    print(f"moved between files: {sum(moved.values())}")
    for (case, old, new), count in sorted(moved.items()):
        print(f"  {count:3} {case}: {old} -> {new}")
    if changed or removed or added:
        raise SystemExit("a value or a name differs: the relocation is not a pure move")


if __name__ == "__main__":
    main()
