"""Compare every golden entry with a base revision (ITL-1313, ITL-1314).

Run from the repository root:
``uv run python docs/dev/scripts/itl_1313/golden_diff_against_main.py [REV]`` (default
``origin/main``). Entries are compared by ``<group>:<name>`` across all golden files. It
asserts the change the developer approved on 2026-11-21: 16 entries removed (the
``tags_as_data`` default-id variant and the three PollingSource pins), five entries of the
keyed ``tags_as_data`` variant changed (its given name ``golden_derived_keyed``), nothing
added; every other entry byte-identical, PR 463's entries included.
"""

import json
import pathlib
import subprocess
import sys

ROOT = pathlib.Path("tests/test_hashing/golden/data/v1")
REV = sys.argv[1] if len(sys.argv) > 1 else "origin/main"
OLD_KEYED_ID = "tags_as_data:semantic_v1:d598019275aa77e05d5b5ee987b06ee00bdbdff3f8644f1b668d18c00e6f1359"
SYSTEM_TAG = (
    "_tag_source_id::1fc6bedf22327c3ac80d1b16aa8f8968f7d2397c0f0e73aa0217ec8f685b586f"
)
KEYED = "schema:derived_source_tags_as_data.record_id_column."
APPROVED_CHANGES = {
    f"{KEYED}source_id": (OLD_KEYED_ID, "golden_derived_keyed"),
    **{
        f'{KEYED}row[{{"subject":"{s}"}}].{SYSTEM_TAG}': (
            OLD_KEYED_ID,
            "golden_derived_keyed",
        )
        for s in ("s1", "s2", "s3")
    },
    f"{KEYED}content_hash": (
        "semantic_v1:ec0a6a598bdce8464c4ebfae8a5ff5ffe7b309c9784df44a48c86f298da18c2b",
        "semantic_v1:b363917eb697abd9de451a08da097c4e5549d05f1c3700aecbd19b9c7df8372d",
    ),
}
APPROVED_REMOVALS = {
    "schema:location_default_source_ids.polling_source.source_id",
    "schema:key_policy_identity.polling_source.subject.content_hash",
    "schema:key_policy_identity.polling_source.subject_count.content_hash",
}
REMOVED_VARIANT = "schema:derived_source_tags_as_data.default_source_id."
PR_463_CASE = "schema:hash_in_key_order."
PR_463_CHANGED = (
    "schema:key_policy_identity.list.key.content_hash",
    "schema:key_policy_identity.list.position.content_hash",
    "pipeline:default_source_ids.list.content_hash",
)


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], capture_output=True, text=True, check=True
    ).stdout


def group(name: str) -> str:
    return pathlib.PurePosixPath(name).name.removesuffix(".json").split("-")[0]


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
for k in removed:
    print(f"  removed {k}")
for k in added:
    print(f"  added {k}")
assert changed == sorted(APPROVED_CHANGES), changed
assert all((old[k], new[k]) == APPROVED_CHANGES[k] for k in changed)
variant = {k for k in removed if k.startswith(REMOVED_VARIANT)}
assert len(variant) == 13 and set(removed) == variant | APPROVED_REMOVALS, removed
assert added == [], added
pr463 = [k for k in old if k.startswith(PR_463_CASE)] + list(PR_463_CHANGED)
assert len(pr463) == 73 and all(new.get(k) == old[k] for k in pr463)
print(f"PR 463's {len(pr463)} entries (70 hash_in_key_order + 3 ListSource) unchanged")
print("OK: exactly the approved 16 removals and 5 changes, nothing added")
