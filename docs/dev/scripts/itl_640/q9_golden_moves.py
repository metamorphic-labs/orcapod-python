"""ITL-640 Q9: which golden entries move under the Arrow schema hash, by group, case
and kind of entry; which do not. Writes nothing to the golden folder.

Run: ``uv run python docs/dev/scripts/itl_640/q9_golden_moves.py [--list]``

An entry whose NAME holds a system-tag column name (``row[...]._tag_source_id::<hex>``)
is renamed when the schema hash moves; such a pair (pinned name gone, computed name new)
is matched by the name with every 64-hex digest blanked, and counted as ``renamed``
(value unchanged) or ``renamed+moved``. ``--list`` prints every changed entry.
"""

from __future__ import annotations

import collections
import json
import pathlib
import re
import sys
import warnings

ROOT = pathlib.Path(__file__).resolve().parents[4]
# The cases import as ``tests.test_hashing.golden.cases``, as the generator imports them:
# fixtures hash with their module path.
sys.path.insert(0, str(ROOT))
warnings.simplefilter("ignore")

from tests.test_hashing.golden import cases  # noqa: E402

GROUPS = ["semantic", "schema", "arrow", "datagram", "pipeline", "torch"]
HEX = re.compile(r"[0-9a-f]{64,70}")


FOLDER = ROOT / "tests" / "test_hashing" / "golden" / "data" / "v1"


def pinned_entries() -> dict[str, dict[str, object]]:
    """Every pinned entry by group: the frozen files and the provisional files (ITL-1266)."""
    out: dict[str, dict[str, object]] = collections.defaultdict(dict)
    for path in sorted(FOLDER.glob("*.json")):
        group = path.name.split("-")[0]
        out[group].update(json.loads(path.read_text()))
    for path in sorted((FOLDER / "provisional").glob("*.json")):
        out[path.stem].update(json.loads(path.read_text()))
    return out


def provisional_pins() -> set[tuple[str, str]]:
    """``(group, entry)`` of every entry pinned in a provisional file."""
    return {
        (path.stem, entry)
        for path in sorted((FOLDER / "provisional").glob("*.json"))
        for entry in json.loads(path.read_text())
    }


def blank(name: str) -> str:
    return HEX.sub("H", name)


def case_of(entry: str) -> str:
    return entry.split(".")[0]


def kind_of(entry: str) -> str:
    name = re.sub(r"\[[^\]]*\]", "[]", blank(entry))
    last = name.split(".")[-1]
    if last.startswith("_tag_record_id"):
        return "row record id (value)"
    if last.startswith("_tag_source_id"):
        return "row source id (value)"
    return last


def label_only(old: object, new: object) -> bool:
    """Whether only the label of a ``<label>:<digest>`` text changed."""
    if not (isinstance(old, str) and isinstance(new, str)):
        return False
    old_label, _, old_digest = old.partition(":")
    new_label, _, new_digest = new.partition(":")
    return old_label != new_label and old_digest == new_digest != ""


def main(show: bool) -> None:
    pinned = pinned_entries()
    in_provisional = provisional_pins()
    kind_count: collections.Counter[tuple[str, str]] = collections.Counter()
    moved: list[tuple[str, str, str, object, object]] = []
    renamed: list[tuple[str, str, str, bool]] = []
    relabelled: list[tuple[str, str]] = []
    unmatched: list[str] = []
    stayed: collections.Counter[tuple[str, str]] = collections.Counter()
    total = 0
    for group in GROUPS:
        live = cases.compute_group(group)
        pins = pinned.get(group, {})
        skipped = cases.unavailable_entries(group)
        gone = {e for e in pins if e not in live and e not in skipped}
        new = {e for e in live if e not in pins}
        by_blank_new = collections.defaultdict(list)
        for e in new:
            by_blank_new[blank(e)].append(e)
        for e in sorted(set(pins) & set(live)):
            total += 1
            if e in skipped:
                continue
            where = "provisional" if (group, e) in in_provisional else "frozen"
            if live[e] != pins[e]:
                if label_only(pins[e], live[e]):
                    relabelled.append((group, case_of(e)))
                    kind_count[("A label only", where)] += 1
                else:
                    moved.append((group, case_of(e), kind_of(e), pins[e], live[e]))
                    kind_count[("B moved", where)] += 1
            else:
                stayed[(group, kind_of(e))] += 1
        for e in sorted(gone):
            total += 1
            partners = by_blank_new.get(blank(e), [])
            if len(partners) == 1:
                value_moved = live[partners[0]] != pins[e]
                renamed.append((group, case_of(e), kind_of(e), value_moved))
                where = "provisional" if (group, e) in in_provisional else "frozen"
                kind_count[("C renamed", where)] += 1
                if value_moved:
                    kind_count[("C renamed, value moved", where)] += 1
            else:
                unmatched.append(f"{group}:{e} (partners {len(partners)})")
    print(f"pinned entries examined: {total}")
    print(f"label only (arrow_v1 -> arrow_schema_v1, same digest): {len(relabelled)}")
    for (g, c), n in sorted(collections.Counter(relabelled).items()):
        print(f"    {g}:{c} {n}")
    print(f"moved (same name, new value): {len(moved)}")
    print(f"renamed (a system-tag column name in the entry name): {len(renamed)}")
    print(f"  of which the value moved too: {sum(1 for r in renamed if r[3])}")
    print(f"unmatched (pinned, not produced, no renamed partner): {len(unmatched)}")
    for u in unmatched:
        print("   ", u)
    print()
    print("BY FILE KIND (frozen v1 files / provisional files):")
    for category in ("A label only", "B moved", "C renamed", "C renamed, value moved"):
        frozen = kind_count[(category, "frozen")]
        provisional = kind_count[(category, "provisional")]
        print(f"  {category:24} frozen {frozen:4}  provisional {provisional:4}")

    print()
    print("MOVED by group / kind:")
    for (g, k), n in sorted(collections.Counter((m[0], m[2]) for m in moved).items()):
        print(f"  {g:9} {k:40} {n}")
    print()
    print("MOVED by group / case:")
    for (g, c), n in sorted(collections.Counter((m[0], m[1]) for m in moved).items()):
        print(f"  {g:9} {c:50} {n}")
    print()
    print("RENAMED by group / kind (value moved?):")
    for (g, k, v), n in sorted(
        collections.Counter((r[0], r[2], r[3]) for r in renamed).items()
    ):
        print(f"  {g:9} {k:40} value moved={v}  {n}")
    print()
    print("RENAMED by group / case:")
    for (g, c), n in sorted(collections.Counter((r[0], r[1]) for r in renamed).items()):
        print(f"  {g:9} {c:50} {n}")
    print()
    print("UNCHANGED by group / kind (selected kinds):")
    for (g, k), n in sorted(stayed.items()):
        if k in (
            "data_hash",
            "row record id (value)",
            "row source id (value)",
            "source_id",
            "record_ids",
            "table_hash",
            "uri",
            "content_hash",
            "pipeline_hash",
            "system_tag_columns",
            "schema_hash",
        ):
            print(f"  {g:9} {k:40} {n}")
    if show:
        print()
        print("EVERY MOVED ENTRY:")
        for g, c, k, a, b in moved:
            print(f"  {g}:{c} [{k}] {str(a)[:70]} -> {str(b)[:70]}")


if __name__ == "__main__":
    main("--list" in sys.argv)
