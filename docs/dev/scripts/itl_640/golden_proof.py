"""ITL-640 golden step proof: what the regenerated golden files change against a base.

Run from the repository root:

    uv run python docs/dev/scripts/itl_640/golden_proof.py [--base REF] [--write]

It reads the golden files of ``REF`` (default ``origin/main``) from git and the files in
the working tree, the frozen and the provisional files together, and checks:

1. every changed, removed or added entry is one of the developer's categories: A label
   only (exactly ``arrow_v1`` to ``arrow_schema_v1``, same digest), B value moved, C
   renamed (an entry whose name holds a system-tag column name: paired with its new name
   by the name with every digest blanked, exactly one old and one new entry per blanked
   name), or one of ``ADDITIONS``;
2. the A, B and C sets equal the sets measured before the golden step
   (``docs/dev/specs/itl-640/golden-moves-measured.json``);
3. the "must not move" entries are byte-identical: the semantic group, data and row
   hashes, row record-id and source-id values (a renamed entry keeps its value), source
   ids other than a ``derived:`` default built from a node's content hash, table hashes
   (the arrow ``table``, ``extension_table`` and ``dict_order`` cases, and every
   ``arrow_v1`` / ``arrow_ordered_v1`` value outside the arrow ``schema`` and
   ``extension_schema`` cases, whose schema hashes carried ``arrow_v1`` before ITL-640:
   those are category A) and operator identities;
4. every entry stays in its kind of file (frozen or provisional), changed or not;
5. a renamed entry whose value moved is a function pipeline table's ``record_ids``, and
   its move is exactly the system-tag rename: each stored row's record-id preimage (its
   tags, system tags, input data hash and recomputation index) is rebuilt from a fresh
   run of the case, hashed (it must give the new pinned ids) and hashed again with every
   renamed digest in the system-tag column names mapped back (it must give the old
   pinned ids), so nothing but the column names moved those ids;
6. ``generate_golden_hashes.py --check`` matches.

The base is resolved once to a full commit, which every read of the base uses. With
``--write``, the full old/new list is written to
``docs/dev/specs/itl-640/golden-moves.json``, only when every check passes (a failed run
leaves the last valid report). Exit status 0 when every check passes, 1 otherwise.
"""

from __future__ import annotations

import argparse
import collections
import json
import pathlib
import re
import subprocess
import sys
import warnings
from collections.abc import Iterable

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
    """The group and the kind of file (frozen or provisional) of a golden file path."""
    name = pathlib.PurePosixPath(path)
    if name.parent.name == "provisional":
        return name.stem, "provisional"
    return name.stem.split("-")[0], "frozen"


def _base_files(ref: str) -> dict[str, str]:
    """Every golden file of the commit ``ref`` (a full commit id), read from git."""
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
    """Every golden file in the working tree."""
    folder = ROOT / GOLDEN
    return {
        str(path.relative_to(ROOT)): path.read_text()
        for path in sorted(folder.rglob("*.json"))
    }


def _entries(files: dict[str, str]) -> tuple[dict[Entry, object], dict[Entry, str]]:
    """The pinned values and the kind of file of every entry in ``files``."""
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
    """``name`` with every digest blanked: the key a renamed entry keeps."""
    return HEX.sub("H", name)


def kind_of(name: str) -> str:
    """The kind of value an entry pins (its last name component, digests blanked)."""
    last = re.sub(r"\[[^\]]*\]", "[]", blank(name)).split(".")[-1]
    if last.startswith("_tag_record_id"):
        return "row record id (value)"
    if last.startswith("_tag_source_id"):
        return "row source id (value)"
    return last


def label_only(old: object, new: object) -> bool:
    """Whether ``old`` became ``new`` by category A: ``arrow_v1`` to ``arrow_schema_v1``,
    the digest unchanged (no other label change counts)."""
    if not (isinstance(old, str) and isinstance(new, str)):
        return False
    old_label, _, old_digest = old.partition(":")
    new_label, _, new_digest = new.partition(":")
    return (
        old_label == "arrow_v1"
        and new_label == "arrow_schema_v1"
        and old_digest == new_digest != ""
    )


def _text(entries: Iterable[Entry]) -> set[str]:
    """Entries as ``group:name`` text."""
    return {f"{group}:{name}" for group, name in entries}


def _record_id_moves_unexplained(
    moved: set[Entry],
    renamed: dict[Entry, Entry],
    old: dict[Entry, object],
    new: dict[Entry, object],
) -> list[str]:
    """Check 5: the failures among the renamed entries whose value moved.

    Args:
        moved: The renamed entries whose value moved, by their old name.
        renamed: Every renamed entry, old name to new name.
        old: The base's pinned values.
        new: The pinned values after the step.

    Returns:
        One failure text per entry that is not a pipeline ``record_ids`` move exactly
        explained by the system-tag rename.
    """
    failures: list[str] = []
    record_ids = {
        e for e in moved if e[0] == "pipeline" and e[1].endswith(".record_ids")
    }
    failures += [
        f"renamed and moved, not a record_ids entry: {g}:{n}"
        for g, n in sorted(moved - record_ids)
    ]
    if not record_ids:
        return failures
    sys.path.insert(0, str(ROOT))
    warnings.simplefilter("ignore")
    import pyarrow as pa

    from orcapod.contexts import get_default_context
    from orcapod.databases import InMemoryArrowDatabase
    from tests.test_hashing.golden import cases

    back: dict[str, str] = {}  # new digest -> old digest, from every rename
    for (_, old_name), (_, new_name) in renamed.items():
        for before, after in zip(HEX.findall(old_name), HEX.findall(new_name)):
            if before != after:
                if back.get(after, before) != before:
                    failures.append(f"digest {after} renamed from two digests")
                back[after] = before
    hasher = get_default_context().arrow_hasher
    captured: list[InMemoryArrowDatabase] = []
    original_init = InMemoryArrowDatabase.__init__

    def capturing_init(
        self: InMemoryArrowDatabase, *args: object, **kwargs: object
    ) -> None:
        original_init(self, *args, **kwargs)  # pyright: ignore[reportArgumentType]  # passed through as given
        captured.append(self)

    case_functions = {name: fn for name, fn, _ in cases._CASES["pipeline"]}  # pyright: ignore[reportPrivateUsage]  # the registered cases, as the generator reads them
    InMemoryArrowDatabase.__init__ = capturing_init  # pyright: ignore[reportAttributeAccessIssue]  # restored below
    try:
        for entry in sorted(record_ids):
            case_name = entry[1].partition(".table[")[0]
            table_path = renamed[entry][1].partition(".table[")[2][
                : -len("].record_ids")
            ]
            captured.clear()
            _ = case_functions[case_name]()
            rebuilt_new: list[object] = []
            rebuilt_old: list[object] = []
            for database in captured:
                for path in database.list_sources():
                    if "/".join(path) != table_path:
                        continue
                    records = database.get_all_records(
                        path, record_id_column="__golden_record_id"
                    )
                    assert records is not None
                    names = records.column_names
                    system = [c for c in names if c.startswith("_tag_")]
                    user = names[1 : names.index(system[0])]
                    for row in range(records.num_rows):
                        preimage = records.slice(row, 1).select(
                            [
                                *user,
                                *system,
                                "__input_data_hash",
                                "__pipeline_recomputation_index",
                            ]
                        )
                        preimage = preimage.cast(
                            pa.schema(
                                [
                                    f.with_type(pa.int32())
                                    if f.name == "__pipeline_recomputation_index"
                                    else f
                                    for f in preimage.schema
                                ]
                            )
                        )
                        old_names = preimage.rename_columns(
                            [
                                HEX.sub(lambda m: back.get(m.group(0), m.group(0)), c)
                                for c in preimage.column_names
                            ]
                        )
                        rebuilt_new.append(
                            cases._jsonable(
                                hasher.hash_table(preimage).to_prefixed_digest()
                            )  # pyright: ignore[reportPrivateUsage]  # the generator's own value encoding
                        )
                        rebuilt_old.append(
                            cases._jsonable(
                                hasher.hash_table(old_names).to_prefixed_digest()
                            )  # pyright: ignore[reportPrivateUsage]  # as above
                        )
            if sorted(rebuilt_new, key=str) != new[renamed[entry]]:  # pyright: ignore[reportCallIssue, reportArgumentType]  # JSON values
                failures.append(
                    f"record ids not rebuilt from the stored rows: {entry[1]}"
                )
            elif sorted(rebuilt_old, key=str) != old[entry]:  # pyright: ignore[reportCallIssue, reportArgumentType]  # as above
                failures.append(f"record ids moved beyond the rename: {entry[1]}")
    finally:
        InMemoryArrowDatabase.__init__ = original_init  # pyright: ignore[reportAttributeAccessIssue]  # restoring
    return failures


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
    """Run every check, print the report; with ``--write`` write it when all pass.

    Returns:
        0 when every check passes, 1 otherwise.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="origin/main")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    # Resolved once: the listing and every blob read use this one commit.
    base_commit = subprocess.run(
        ["git", "rev-parse", "--verify", "--end-of-options", f"{args.base}^{{commit}}"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    old, old_place = _entries(_base_files(base_commit))
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
    removed_by_blank: dict[tuple[str, str], list[Entry]] = collections.defaultdict(list)
    for e in removed:
        removed_by_blank[(e[0], blank(e[1]))].append(e)
    renamed: dict[Entry, Entry] = {}
    for e in sorted(removed):
        key = (e[0], blank(e[1]))
        partners = by_blank.get(key, [])
        if len(partners) == 1 and len(removed_by_blank[key]) == 1:
            renamed[e] = partners[0]
        else:
            failures.append(
                f"removed with {len(partners)} new and {len(removed_by_blank[key])} "
                f"removed entries of one name: {e[0]}:{e[1]}"
            )
    paired_new = set(renamed.values())
    extra = {f"{g}:{n}" for g, n in added - paired_new}
    c_moved = {e for e, to in renamed.items() if old[e] != new[to]}

    text = _text
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
    for e in common:
        if old_place[e] != new_place[e]:
            failures.append(f"in another kind of file: {e[0]}:{e[1]}")
    record_id_failures = _record_id_moves_unexplained(c_moved, renamed, old, new)
    failures += record_id_failures
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

    print(
        f"base {args.base} = {base_commit[:8]}; frozen and provisional files together"
    )
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
    if c_moved:
        # The claim is printed only when check 5 found it true (CodeRabbit on PR 454):
        # a failed check is reported as failed, never beside a passing statement.
        if record_id_failures:
            print(
                f"record ids rebuilt: {len(c_moved)} entries; "
                f"{len(record_id_failures)} not explained by the system-tag names "
                "(see FAIL below)"
            )
        else:
            print(
                f"record ids rebuilt: {len(c_moved)} entries; with the system-tag "
                "names mapped back they are the old pinned ids"
            )

    if args.write and not failures:
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
            json.dumps(
                {"base": base_commit[:8], "changes": rows}, indent=1, sort_keys=True
            )
            + "\n"
        )
        print(f"wrote {OUTPUT.relative_to(ROOT)} ({len(rows)} rows)")

    for failure in failures:
        print("FAIL", failure)
    print("OK" if not failures else f"{len(failures)} failure(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
