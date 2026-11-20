"""Compare every golden entry with origin/main: only the approved MergeJoin entries differ."""

import json
import subprocess
import pathlib

root = pathlib.Path("tests/test_hashing/golden/data/v1")
files = sorted(p.relative_to(root).as_posix() for p in root.rglob("*.json"))
main_files = subprocess.run(
    ["git", "ls-tree", "-r", "--name-only", "origin/main", str(root) + "/"],
    capture_output=True,
    text=True,
).stdout.split()
assert files == sorted(pathlib.Path(f).relative_to(root).as_posix() for f in main_files)


def load(rev, f):
    if rev is None:
        return json.loads((root / f).read_text())
    return json.loads(
        subprocess.run(
            ["git", "show", f"{rev}:{root}/{f}"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    )


def group(f):
    return pathlib.Path(f).name.split("-")[0].removesuffix(".json")


old, new = {}, {}
for f in files:
    for k, v in load("origin/main", f).items():
        old[f"{group(f)}:{k}"] = v
    for k, v in load(None, f).items():
        new[f"{group(f)}:{k}"] = v


def approved(k):
    return k.startswith("pipeline:merge_join.") or k.startswith(
        "pipeline:join_key_nullability.merge_join."
    )


removed = sorted(set(old) - set(new))
added = sorted(set(new) - set(old))
common = set(old) & set(new)
changed = sorted(k for k in common if old[k] != new[k])
print(
    f"golden files: {len(files)} (same names as main); entries on main: {len(old)}; on the branch: {len(new)}"
)
print(
    f"value moved: {len(changed)}; name gone: {len(removed)}; name new: {len(added)}; byte-identical: {len(common) - len(changed)}"
)
assert all(approved(k) for k in changed + removed + added)
assert len(changed) + len(removed) + len(added) == 21
print(
    "all 21 differences are approved MergeJoin entries; every other entry is identical to main"
)
for k in changed:
    print("MOVED", k, json.dumps(old[k]), "->", json.dumps(new[k]))
for k in removed:
    print("GONE", k, json.dumps(old[k]))
for k in added:
    print("NEW", k, json.dumps(new[k]))
