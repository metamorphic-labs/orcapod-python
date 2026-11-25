"""Golden entries that move under each rule (ITL-1466), by patching, writing nothing.

Run: uv run python docs/dev/specs/itl-1466/golden_moves.py
Each variant patches ``DerivedSource`` in this process and compares every golden case
with the pinned files.
"""

import json
import sys
import warnings
from pathlib import Path

warnings.simplefilter("ignore")
ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT), str(ROOT / "tests/test_hashing/golden")]

from orcapod.core.sources.base import RootSource  # noqa: E402
from orcapod.core.sources.derived_source import DerivedSource  # noqa: E402
from tests.test_hashing.golden import cases  # noqa: E402

DATA = ROOT / "tests/test_hashing/golden/data/v1"
pinned: dict[str, dict[str, object]] = {}
for f in [*DATA.glob("*.json"), *DATA.glob("provisional/*.json")]:
    if "torch" not in f.name:
        pinned.setdefault(f.stem.split("-")[0], {}).update(json.loads(f.read_text()))


def moved() -> list[str]:
    out = []
    for group, want in pinned.items():
        got = cases.compute_group(group)
        out += [f"{group}:{k}" for k in want if got.get(k) != want[k]]
    return sorted(out)


def report(label: str) -> None:
    m = moved()
    by: dict[str, int] = {}
    for k in m:
        by[k.split("[")[0].split(".row")[0]] = (
            by.get(k.split("[")[0].split(".row")[0], 0) + 1
        )
    print(f"{label}: {len(m)} move")
    for k, n in by.items():
        print(f"   {n:3} {k}")


report("as is")

original = DerivedSource.identity_structure


def root_identity(self):
    if self._tags_as_data is None:
        return RootSource.identity_structure(self)
    return original(self)


DerivedSource.identity_structure = root_identity
report("plain form takes the root identity (source id, schema, key columns)")


def with_row_id(self):
    s = root_identity(self)
    return (s[0], s[1], ("row_id",) if not s[2] else s[2], *s[3:])


DerivedSource.identity_structure = with_row_id
report("and the key policy of a row id differs from a position's, in both forms")
DerivedSource.identity_structure = root_identity
DerivedSource.SOURCE_VERSION = 2
report("kind version 2")
