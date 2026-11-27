"""ITL-1518: every pinned golden entry against the values a tree computes.

    uv run python tests/test_hashing/golden/generate_golden_hashes.py --stdout > computed.json
    uv run python docs/dev/specs/itl-1518/compare_golden.py computed.json

Writes nothing. An entry pinned as ``null`` is not printed by ``--stdout`` and shows as
GONE; the generator's own ``--check`` is the authority on those.
"""

from __future__ import annotations

import glob
import json
import os
import sys

ROOT = "tests/test_hashing/golden/data/v1"


def main() -> None:
    with open(sys.argv[1]) as f:
        groups = json.load(f)
    pinned = {}
    for path in sorted(
        glob.glob(ROOT + "/*.json") + glob.glob(ROOT + "/provisional/*.json")
    ):
        group = os.path.basename(path).split("-")[0].split(".")[0]
        with open(path) as f:
            for key, value in json.load(f).items():
                pinned[f"{group}:{key}"] = (value, os.path.relpath(path, ROOT))
    computed = {f"{g}:{k}": v for g, d in groups.items() for k, v in d.items()}
    print(
        "computed",
        len(computed),
        {g: len(d) for g, d in groups.items()},
        "pinned",
        len(pinned),
    )
    differ = 0
    for key in sorted(set(pinned) | set(computed)):
        old, new = pinned.get(key), computed.get(key)
        if old is None:
            differ += 1
            print("NEW   ", key, "\n   ->", json.dumps(new))
        elif new is None:
            differ += 1
            print("GONE  ", old[1], key, "\n   was", json.dumps(old[0]))
        elif old[0] != new:
            differ += 1
            print("MOVED ", old[1], key)
            print("   was", json.dumps(old[0]), "\n   now", json.dumps(new))
    print("differ", differ)


if __name__ == "__main__":
    main()
