"""Compare two runs of ``starfix_differential_corpus.py`` (ITL-1179 stage 1, proof B).

Prints every entry whose outcome differs and classifies it:

* ``documented``: a ``known_defect.*`` case, which starfix 0.6.0 documents as hashing
  differently from 0.5.0;
* ``message``: both runs raise the same exception class and only the message differs;
* ``UNEXPECTED``: anything else (a digest moved, or a raise turned into a digest or back).

Exits non-zero when an unexpected difference is found.

Usage::

    uv run python docs/dev/scripts/itl_1179/compare_corpus_runs.py OLD.json NEW.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def _classify(name: str, old: str, new: str) -> str:
    if name.startswith("known_defect."):
        return "documented"
    if old.startswith("raise:") and new.startswith("raise:"):
        if old.split(":", 2)[1] == new.split(":", 2)[1]:
            return "message"
    return "UNEXPECTED"


def main() -> int:
    old_run = json.loads(Path(sys.argv[1]).read_text())
    new_run = json.loads(Path(sys.argv[2]).read_text())
    old, new = old_run["entries"], new_run["entries"]
    print(
        f"old: starfix {old_run['starfix_version']}, new: starfix {new_run['starfix_version']}, "
        f"pyarrow {old_run['pyarrow_version']} / {new_run['pyarrow_version']}"
    )
    if set(old) != set(new):
        print(f"UNEXPECTED: entry sets differ: {sorted(set(old) ^ set(new))}")
        return 1
    counts = {"same": 0, "documented": 0, "message": 0, "UNEXPECTED": 0}
    for name in sorted(old):
        if old[name] == new[name]:
            counts["same"] += 1
            continue
        kind = _classify(name.split("|")[0], old[name], new[name])
        counts[kind] += 1
        print(f"[{kind}] {name}\n    old: {old[name]}\n    new: {new[name]}")
    print(f"{len(old)} entries: {counts}")
    return 1 if counts["UNEXPECTED"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
