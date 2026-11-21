"""Compare every golden entry with a base revision (ITL-1272).

Run from the repository root: ``uv run python docs/dev/scripts/itl_1272/golden_diff_against_main.py [REV]``
(default ``origin/main``). Entries are compared by ``<group>:<name>`` across all golden
files, so an entry that the generator wrote into another file of its group counts as the
same entry. It asserts the change the developer approved on 2026-11-21: exactly three
``ListSource`` content hashes move, nothing is removed, and every new entry belongs to the
``schema:hash_in_key_order`` case; every other entry is byte-identical.
"""

import json
import pathlib
import subprocess
import sys

ROOT = pathlib.Path("tests/test_hashing/golden/data/v1")
REV = sys.argv[1] if len(sys.argv) > 1 else "origin/main"
APPROVED_MOVES = {
    "schema:key_policy_identity.list.key.content_hash": (
        "semantic_v1:b868793e9bc08672bbef6b629818bb812baad7ed1d9701d7245617ea3df97486",
        "semantic_v1:6fe5579e74653616d575c5274fc0a72b4bccc1c46e56a837bdef0f709404df61",
    ),
    "schema:key_policy_identity.list.position.content_hash": (
        "semantic_v1:80f5d87ceac2469c2a4c411c3b36a3735885df1c036ffec49c60d0a5e18cb757",
        "semantic_v1:e2660b0b39a19ac9a65b45ab888a1864f5c510aa18f66e0a5a99db956e11218a",
    ),
    "pipeline:default_source_ids.list.content_hash": (
        "semantic_v1:dbaf6ac70a35ec6928af0f8b53e222de18b69337d8985324218c6c05c16abf29",
        "semantic_v1:9d892efaf950490f157554b14c2f32c8194dcb92e6fa21d29e6f86b0d3e64801",
    ),
}
NEW_CASE = "schema:hash_in_key_order."


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], capture_output=True, text=True, check=True
    ).stdout


def group(name: str) -> str:
    stem = pathlib.PurePosixPath(name).name.removesuffix(".json")
    return stem.split("-")[0]


def entries(rev: str | None) -> dict[str, object]:
    if rev is None:
        files = [p.relative_to(ROOT).as_posix() for p in ROOT.rglob("*.json")]
    else:
        files = [
            pathlib.PurePosixPath(f).relative_to(ROOT.as_posix()).as_posix()
            for f in git("ls-tree", "-r", "--name-only", rev, f"{ROOT}/").split()
        ]
    out: dict[str, object] = {}
    for f in sorted(files):
        text = (
            (ROOT / f).read_text() if rev is None else git("show", f"{rev}:{ROOT}/{f}")
        )
        for key, value in json.loads(text).items():
            name = f"{group(f)}:{key}"
            assert name not in out, f"{name} pinned twice"
            out[name] = value
    return out


old, new = entries(REV), entries(None)
removed = sorted(set(old) - set(new))
added = sorted(set(new) - set(old))
common = set(old) & set(new)
changed = sorted(k for k in common if old[k] != new[k])
print(f"base {REV} = {git('rev-parse', '--short', REV).strip()}")
print(
    f"entries on base: {len(old)}; on the branch: {len(new)}; changed: {len(changed)}; "
    f"removed: {len(removed)}; added: {len(added)}; byte-identical: {len(common) - len(changed)}"
)
for k in changed:
    print(f"  changed {k}: {old[k]} -> {new[k]}")
for k in added:
    print(f"  added {k}")
assert changed == sorted(APPROVED_MOVES), changed
assert all((old[k], new[k]) == APPROVED_MOVES[k] for k in changed)
assert removed == [], removed
assert all(k.startswith(NEW_CASE) for k in added), added
print(
    "OK: exactly the approved moves, no removal, additions only in schema:hash_in_key_order"
)
