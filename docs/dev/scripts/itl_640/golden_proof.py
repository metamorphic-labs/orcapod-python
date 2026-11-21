"""ITL-640 golden step proof: what the regenerated golden files change against a base.

Run from the repository root:

    uv run python docs/dev/scripts/itl_640/golden_proof.py [--base REF] [--write]

It reads the golden files of ``REF`` (default ``origin/main``) from git and the files in
the working tree, the frozen and the provisional files together, and checks:

1. every changed, removed or added entry is one of the developer's categories: A label
   only (``arrow_v1`` to ``arrow_schema_v1``, same digest), B value moved, C renamed (an
   entry whose name holds a system-tag column name: paired with its new name by the name
   with every digest blanked), or one of ``ADDITIONS``;
2. the A, B and C sets equal the sets measured before the golden step
   (``docs/dev/specs/itl-640/golden-moves-measured.json``);
3. the "must not move" entries are byte-identical: the semantic group, data and row
   hashes, row record-id and source-id values (a renamed entry keeps its value), source
   ids other than a ``derived:`` default built from a node's content hash, table hashes
   (the arrow ``table``, ``extension_table`` and ``dict_order`` cases, and every
   ``arrow_v1`` / ``arrow_ordered_v1`` value outside the arrow ``schema`` and
   ``extension_schema`` cases, whose schema hashes carried ``arrow_v1`` before ITL-640:
   those are category A) and operator identities;
4. every entry stays in its kind of file (frozen or provisional);
5. ``generate_golden_hashes.py --check`` matches.

``--write`` writes the full old/new list to ``docs/dev/specs/itl-640/golden-moves.json``.
Exit status 1 if any check fails.
"""

from __future__ import annotations

import argparse
import collections
import json
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[4]
GOLDEN = "tests/test_hashing/golden/data/v1"
MEASURED = ROOT / "docs/dev/specs/itl-640/golden-moves-measured.json"
OUTPUT = ROOT / "docs/dev/specs/itl-640/golden-moves.json"
HEX = re.compile(r"[0-9a-f]{64,70}")

_FORMS = ("live", "storage")
ADDITIONS = frozenset(
    {
        *(
            f"arrow:schema_storage_form.{label}.{form}"
            for label in ("File", "Directory", "list[File]", "struct_with_Directory")
            for form in _FORMS
        ),
        *(
            f"arrow:schema_storage_form_library.{label}.{form}"
            for label in (
                "numpy.ndarray",
                "pandas.DataFrame",
                "pandas.Series",
                "polars.DataFrame",
                "polars.Series",
            )
            for form in _FORMS
        ),
        *(f"torch:schema_storage_form.torch.Tensor.{form}" for form in _FORMS),
    }
)
"""The only entries the golden step adds: the canonical schema form, live and storage."""

DATA_KINDS = frozenset(
    {"data_hash", "input_data_hashes", "tag_hash", "row_hash", "key_bits"}
)
TABLE_CASES = frozenset({"table", "extension_table", "dict_order"})
TABLE_LABELS = ("arrow_v1:", "arrow_ordered_v1:")
# The arrow cases that pin schema hashes: before ITL-640 a schema hash carried the table
# hash's label, ``arrow_v1``; those 47 entries are category A (label only, same digest).
SCHEMA_HASH_CASES = frozenset({"schema", "extension_schema"})

Entry = tuple[str, str]  # (group, entry name)


def _group_and_place(path: str) -> tuple[str, str]:
    name = pathlib.PurePosixPath(path)
    if name.parent.name == "provisional":
        return name.stem, "provisional"
    return name.stem.split("-")[0], "frozen"


def _base_files(ref: str) -> dict[str, str]:
    listing = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", ref, GOLDEN],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    return {
        path: subprocess.run(
            ["git", "show", f"{ref}:{path}"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        for path in listing
        if path.endswith(".json")
    }


def _head_files() -> dict[str, str]:
    folder = ROOT / GOLDEN
    return {
        str(path.relative_to(ROOT)): path.read_text()
        for path in sorted(folder.rglob("*.json"))
    }


def _entries(files: dict[str, str]) -> tuple[dict[Entry, object], dict[Entry, str]]:
    values: dict[Entry, object] = {}
    places: dict[Entry, str] = {}
    for path, text in files.items():
        group, place = _group_and_place(path)
        for name, value in json.loads(text).items():
            key = (group, name)
            if key in values:
                raise SystemExit(f"{group}:{name} is pinned twice")
            values[key] = value
            places[key] = place
    return values, places


def blank(name: str) -> str:
    return HEX.sub("H", name)


def kind_of(name: str) -> str:
    last = re.sub(r"\[[^\]]*\]", "[]", blank(name)).split(".")[-1]
    if last.startswith("_tag_record_id"):
        return "row record id (value)"
    if last.startswith("_tag_source_id"):
        return "row source id (value)"
    return last


def label_only(old: object, new: object) -> bool:
    if not (isinstance(old, str) and isinstance(new, str)):
        return False
    old_label, _, old_digest = old.partition(":")
    new_label, _, new_digest = new.partition(":")
    return old_label != new_label and old_digest == new_digest != ""


def must_not_move(entry: Entry, old: object) -> str | None:
    """The reason ``entry`` must keep its value, or ``None`` if it may move."""
    group, name = entry
    kind = kind_of(name)
    if group == "semantic":
        return "semantic value"
    if kind in DATA_KINDS:
        return "data or row hash"
    if kind in ("row record id (value)", "row source id (value)"):
        return "row record-id or source-id value"
    if kind == "source_id" and not (
        isinstance(old, str) and old.startswith("derived:")
    ):
        return "given, table-hash or location source id"
    if group == "arrow" and name.split(".")[0] in TABLE_CASES:
        return "table hash"
    if (
        isinstance(old, str)
        and old.startswith(TABLE_LABELS)
        and not (group == "arrow" and name.split(".")[0] in SCHEMA_HASH_CASES)
    ):
        return "table hash"
    if any(part.startswith("operator") for part in name.split(".")):
        return "operator identity"
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="origin/main")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    base_commit = subprocess.run(
        ["git", "rev-parse", "--short=8", args.base],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    old, old_place = _entries(_base_files(args.base))
    new, new_place = _entries(_head_files())
    failures: list[str] = []

    common = old.keys() & new.keys()
    unchanged = {e for e in common if old[e] == new[e]}
    changed = common - unchanged
    removed = old.keys() - new.keys()
    added = new.keys() - old.keys()

    a_set = {e for e in changed if label_only(old[e], new[e])}
    b_set = changed - a_set
    by_blank: dict[tuple[str, str], list[Entry]] = collections.defaultdict(list)
    for e in added:
        by_blank[(e[0], blank(e[1]))].append(e)
    renamed: dict[Entry, Entry] = {}
    for e in sorted(removed):
        partners = by_blank.get((e[0], blank(e[1])), [])
        if len(partners) == 1:
            renamed[e] = partners[0]
        else:
            failures.append(f"removed with {len(partners)} partners: {e[0]}:{e[1]}")
    paired_new = set(renamed.values())
    extra = {f"{g}:{n}" for g, n in added - paired_new}
    c_moved = {e for e, to in renamed.items() if old[e] != new[to]}

    def text(entries: object) -> set[str]:
        return {f"{g}:{n}" for g, n in entries}  # pyright: ignore[reportGeneralTypeIssues]

    measured = json.loads(MEASURED.read_text())
    for label, got, key in (
        ("A", text(a_set), "A_label_only"),
        ("B", text(b_set), "B_value_moved"),
        ("C", text(renamed), "C_renamed"),
        ("C value moved", text(c_moved), "C_renamed_value_moved"),
    ):
        expected = set(measured[key])
        if got != expected:
            failures.append(
                f"{label}: {len(got - expected)} not measured, "
                f"{len(expected - got)} measured but not changed"
            )
    if extra != ADDITIONS:
        failures.append(
            f"additions: {sorted(extra - ADDITIONS)} not listed, "
            f"{sorted(ADDITIONS - extra)} listed but not added"
        )

    for e in sorted(old):
        reason = must_not_move(e, old[e])
        if reason is None:
            continue
        if e in renamed:
            if old[e] != new[renamed[e]]:
                failures.append(f"must not move ({reason}), renamed and moved: {e}")
        elif e not in unchanged:
            failures.append(f"must not move ({reason}): {e[0]}:{e[1]}")
    for e in changed:
        if old_place[e] != new_place[e]:
            failures.append(f"changed file kind: {e}")
    for e, to in renamed.items():
        if old_place[e] != new_place[to]:
            failures.append(f"renamed into another file kind: {e}")

    places = collections.Counter()
    for e in a_set:
        places[("A", old_place[e])] += 1
    for e in b_set:
        places[("B", old_place[e])] += 1
    for e in renamed:
        places[("C", old_place[e])] += 1
    for g, n in added - paired_new:
        places[("addition", new_place[(g, n)])] += 1
    guarded = collections.Counter(
        r for e in old if (r := must_not_move(e, old[e])) is not None
    )

    check = subprocess.run(
        [
            sys.executable,
            "tests/test_hashing/golden/generate_golden_hashes.py",
            "--check",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    check_line = (check.stdout.strip().splitlines() or [""])[-1]
    if check.returncode != 0:
        failures.append(f"--check failed: {check_line}")

    print(f"base {args.base} = {base_commit}; frozen and provisional files together")
    print(f"entries on the base: {len(old)}; after the golden step: {len(new)}")
    print(f"byte-identical: {len(unchanged)}")
    print(f"A label only: {len(a_set)}")
    print(f"B value moved: {len(b_set)}")
    print(f"C renamed: {len(renamed)} (value moved too: {len(c_moved)})")
    print(f"additions: {len(extra)} (the listed {len(ADDITIONS)})")
    print(
        "by file kind: "
        + ", ".join(f"{c} {p} {n}" for (c, p), n in sorted(places.items()))
    )
    print(
        "must not move, checked: "
        + ", ".join(f"{r} {n}" for r, n in sorted(guarded.items()))
    )
    print(f"--check: {check_line}")

    if args.write:
        rows = []
        for e in sorted(a_set | b_set):
            rows.append(
                {
                    "category": "A" if e in a_set else "B",
                    "group": e[0],
                    "file": old_place[e],
                    "entry": e[1],
                    "old": old[e],
                    "new": new[e],
                }
            )
        for e, to in sorted(renamed.items()):
            rows.append(
                {
                    "category": "C",
                    "group": e[0],
                    "file": old_place[e],
                    "entry": e[1],
                    "new entry": to[1],
                    "old": old[e],
                    "new": new[to],
                }
            )
        for g, n in sorted(added - paired_new):
            rows.append(
                {
                    "category": "addition",
                    "group": g,
                    "file": new_place[(g, n)],
                    "entry": n,
                    "new": new[(g, n)],
                }
            )
        OUTPUT.write_text(
            json.dumps({"base": base_commit, "changes": rows}, indent=1, sort_keys=True)
            + "\n"
        )
        print(f"wrote {OUTPUT.relative_to(ROOT)} ({len(rows)} rows)")

    for failure in failures:
        print("FAIL", failure)
    print("OK" if not failures else f"{len(failures)} failure(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
