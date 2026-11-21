"""ITL-1179: compare every golden pin of a base revision with the working tree.

Reads the whole golden folder of data context v1 on both sides, the frozen files
(``<group>-NN.json``) and the provisional files (``provisional/<group>.json``) together,
through ``store.load_folder``, which refuses an entry pinned twice. The base side is taken
from git (``git archive``), so nothing in the checkout is touched. Every entry is reported
with the kind of file that pins it on each side.

The run passes when:

- every entry whose value differs is one of ``--approved`` (and every approved entry differs);
- no entry is removed;
- every added entry starts with one of ``--added-prefix`` and is pinned in a frozen file;
- no entry changed the kind of file that pins it.

Usage::

    uv run python docs/dev/scripts/itl_1179/golden_union_compare.py origin/main \\
        --approved 'pipeline:group_by_nan_keys.data_tag_key.row[{"k":NaN}].tag_hash' \\
        --approved 'pipeline:group_by_nan_keys.data_tag_key.row[{"k":NaN}].data_hash' \\
        --added-prefix arrow:float_rule.
"""

from __future__ import annotations

import argparse
import io
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))

from tests.test_hashing.golden import store  # noqa: E402

FOLDER = Path("tests/test_hashing/golden/data/v1")


def _pins(folder: Path) -> dict[str, tuple[Any, str]]:
    """Every pin of a golden folder: ``group:entry`` to (value, file kind)."""
    provisional = {
        f"{group}:{entry}"
        for group, entries in store.load_provisional(folder).items()
        for entry in entries
    }
    return {
        f"{group}:{entry}": (
            value,
            "provisional" if f"{group}:{entry}" in provisional else "frozen",
        )
        for group, entries in store.load_folder(folder).items()
        for entry, value in entries.items()
    }


def _base_folder(ref: str, into: Path) -> Path:
    """Extract the golden folder of ``ref`` under ``into`` and return its path."""
    archive = subprocess.run(
        ["git", "archive", "--format=tar", ref, FOLDER.as_posix()],
        cwd=ROOT,
        check=True,
        capture_output=True,
    ).stdout
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(into, filter="data")
    return into / FOLDER


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compare every golden pin of a revision with the working tree."
    )
    parser.add_argument("base", help="the git revision to compare against")
    parser.add_argument("--approved", action="append", default=[])
    parser.add_argument("--added-prefix", action="append", default=[])
    args = parser.parse_args(argv)

    with tempfile.TemporaryDirectory() as tmp:
        base = _pins(_base_folder(args.base, Path(tmp)))
    head = _pins(ROOT / FOLDER)

    changed = sorted(k for k in base.keys() & head.keys() if base[k][0] != head[k][0])
    removed = sorted(base.keys() - head.keys())
    added = sorted(head.keys() - base.keys())
    moved_kind = sorted(
        k for k in base.keys() & head.keys() if base[k][1] != head[k][1]
    )
    kinds = {
        kind: sum(1 for _, k in head.values() if k == kind)
        for kind in ("frozen", "provisional")
    }

    print(
        f"base {args.base}: {len(base)} pins; working tree: {len(head)} pins "
        f"({kinds['frozen']} frozen, {kinds['provisional']} provisional)"
    )
    print(f"changed: {len(changed)}")
    for key in changed:
        print(
            f"  {key} [{head[key][1]}]\n    base: {base[key][0]}\n    head: {head[key][0]}"
        )
    print(f"removed: {len(removed)}")
    for key in removed:
        print(f"  {key} [{base[key][1]}]")
    print(f"added: {len(added)}")
    for key in added:
        print(f"  {key} [{head[key][1]}]")
    print(f"pinned in another kind of file: {len(moved_kind)}")
    for key in moved_kind:
        print(f"  {key}: {base[key][1]} -> {head[key][1]}")

    problems: list[str] = []
    approved = set(args.approved)
    problems += [f"changed without approval: {k}" for k in changed if k not in approved]
    problems += [
        f"approved but unchanged: {k}" for k in sorted(approved - set(changed))
    ]
    problems += [f"removed: {k}" for k in removed]
    problems += [
        f"added outside the allowed prefixes: {k}"
        for k in added
        if not any(k.startswith(p) for p in args.added_prefix)
    ]
    problems += [
        f"added in a provisional file: {k}" for k in added if head[k][1] != "frozen"
    ]
    problems += [f"pinned in another kind of file: {k}" for k in moved_kind]
    print("PASS" if not problems else "FAIL")
    for problem in problems:
        print(f"  {problem}")
    return 0 if not problems else 1


if __name__ == "__main__":
    raise SystemExit(main())
