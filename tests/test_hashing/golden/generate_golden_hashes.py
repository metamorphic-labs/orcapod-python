"""Add, check and release the golden hash files checked by ``test_golden_hashes.py``.

Usage::

    uv run python tests/test_hashing/golden/generate_golden_hashes.py            # add
    uv run python tests/test_hashing/golden/generate_golden_hashes.py --check    # report
    uv run python tests/test_hashing/golden/generate_golden_hashes.py --release 0.4.0
    uv run python tests/test_hashing/golden/generate_golden_hashes.py --remove <entry>

The golden values of the current data context live in
``tests/test_hashing/golden/data/<folder>/<group>-NN.json``, and the manifest of the data
context (``src/orcapod/contexts/data/<version>.json``) lists every file, with the sha256 of
its bytes, in its ``golden.history`` (see ``store.py`` and ``README.md`` in this directory).

READ THIS BEFORE REGENERATING
=============================

Every value in the golden files is a hash that Orcapod stores, uses as a cache or database
key, or embeds in a column name or a database table path. A changed value means data written
by an earlier release is no longer found, is keyed differently, or is read under another
column name. **A changed value is a breaking change.**

The rules (developer, November 15, 2026, ITL-946; reopened November 17, 2026, ITL-1255):

0. Data context v1 is open until the 0.4.0 release. 0.3.0 was withdrawn (2026-11-17) before
   anyone used it: its release revision is out of the history and its pins are kept under
   ``withdrawn`` in ``released_manifests.json``, so no file is released today. Until 0.4.0,
   a change to any v1 value needs the developer's approval of the exact list of entries
   that move, each explained; it is made with ``--remove`` then ``--add`` (rule 4). 0.4.0
   freezes v1 with ``--release 0.4.0``; so would any version published before it, a
   pre-release included (the Release workflow requires the release step for every version).
1. A golden file listed by a released revision never changes and is never removed. This
   script never rewrites one.
2. Adding values is allowed in any release. ``--add`` (the default) writes the entries the
   cases compute and no file pins yet: into the newest file of their group when no release
   lists that file yet, otherwise into a new ``<group>-NN.json`` with the next counter. It
   records the file in the open revision of the history (appending one after a release).
   Existing values are never rewritten, so check that the diff only adds.
3. A change to a released value has no exemption, not even a logical type's own identifier
   change: it needs a new data context version, with a new manifest and a new golden folder,
   shipped only in a minor (before 1.0) or major release. The old context's golden files and
   code move to ``orcapod-sanctuary``.
4. Before a release, a value no release lists may change only with the developer's approval
   and a written migration assessment: ``--remove <entry>`` takes the entry out of its
   unreleased file and records that file again in the open revision (it refuses an entry a
   release lists), then ``--add`` writes the new value.

Never edit a golden file to make a failing test pass. If a test fails and you did not mean to
change a hash, the code change is the bug.

Values must be identical on every supported Python version (3.11, 3.12, 3.13) and in every
process, whatever ``PYTHONHASHSEED`` is. A value that differs by interpreter is a defect to
fix, never something to pin per version.

Not pinned, because starfix rejects their data (ITL-848): ``null``-typed and map columns.
Since starfix 0.5 (hash format 2, label ``arrow_v1``, ITL-910), ``hash_schema`` on extension
columns, the canonical ``arrow.uuid`` and ``arrow.fixed_shape_tensor`` types, and
fixed-size-list values are hashable and pinned.

Releasing
---------

``--release X.Y.Z`` is the golden step of a release pull request: it appends the release
revision (``added`` empty), pins its digest under ``releases`` in
``src/orcapod/contexts/released_manifests.json``, and, at the data context's first release,
sets ``released_in`` and pins the digest of the manifest without ``golden``. It keeps the
``withdrawn`` records and refuses a withdrawn version. It computes no hash. Try it on a copy first with ``--root`` (``store.copy_tree`` makes one).

Comparing releases
------------------

To list the values an older checkout produces (for release notes), run this script with
that checkout's environment and this checkout's test package on the path::

    uv run --directory <old-checkout> python <this-checkout>/tests/test_hashing/golden/generate_golden_hashes.py \\
        --stdout --allow-errors > <scratch-file>

and compare ``<scratch-file>`` with the golden folder. ``--allow-errors`` records
``ERROR: ...`` for a case the old API cannot express instead of stopping.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import pathlib
import sys
from typing import Any

# Put the project root on sys.path so the cases import as ``tests.test_hashing.golden.cases``,
# the same module name pytest uses: function pods and dataclasses hash with their module path.
_PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from orcapod.contexts import get_default_context  # noqa: E402
from tests.test_hashing.golden import cases, store  # noqa: E402


def dump(values: dict[str, Any]) -> str:
    """Serialize one golden file's entries the way the golden files are stored.

    Args:
        values: Entries of one file.

    Returns:
        Indented JSON with sorted keys and a trailing newline.
    """
    return json.dumps(values, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _today() -> str:
    return datetime.date.today().isoformat()


def _require_sound(root: pathlib.Path, version: str) -> None:
    """Stop when the tree already breaks the golden rules; nothing is written then."""
    problems = store.tree_problems(root, version)
    if problems:
        raise SystemExit(
            "the golden history does not match the files; nothing written:\n  "
            + "\n  ".join(problems)
        )


def _provisional_listing(manifest: dict[str, Any]) -> dict[str, str]:
    """A copy of the manifest's ``golden.provisional`` listing."""
    return dict(manifest["golden"][store.PROVISIONAL])


def _set_provisional_listing(manifest: dict[str, Any], listing: dict[str, str]) -> None:
    manifest["golden"][store.PROVISIONAL] = dict(sorted(listing.items()))


def _open_frozen_file(
    folder: pathlib.Path,
    files: dict[str, list[pathlib.Path]],
    released: set[str],
    group: str,
) -> pathlib.Path:
    """The frozen file new entries of ``group`` go to.

    The newest file of the group when no release lists it, otherwise a new
    ``<group>-NN.json`` with the next counter.
    """
    existing = files.get(group, [])
    latest = existing[-1] if existing else None
    if latest is not None and latest.name not in released:
        return latest
    number = 1
    if latest is not None:
        match = store.FILE_NAME.match(latest.name)
        assert match is not None
        number = int(match["number"]) + 1
    if number > 99:
        raise SystemExit(f"{group} has no free file counter")
    return folder / f"{group}-{number:02d}.json"


class _Plan:
    """Files to write and the history and listing that record them; nothing written yet."""

    def __init__(self, folder: pathlib.Path, manifest: dict[str, Any]) -> None:
        self.folder = folder
        self.history: list[dict[str, Any]] = manifest["golden"]["history"]
        self.listing = _provisional_listing(manifest)
        self.contents: dict[pathlib.Path, dict[str, Any]] = {}

    def values(self, path: pathlib.Path) -> dict[str, Any]:
        """The planned entries of a file, read from disk on first use."""
        if path not in self.contents:
            self.contents[path] = dict(store.read_json(path)) if path.exists() else {}
        return self.contents[path]

    def write(self, manifest: dict[str, Any], manifest_path: pathlib.Path) -> None:
        """Write the planned files, record them, and write the manifest."""
        provisional = store.provisional_folder(self.folder)
        writes: list[tuple[pathlib.Path, bytes | None]] = []
        for path, values in self.contents.items():
            if path.parent == provisional:
                if values:
                    data = dump(values).encode("utf-8")
                    self.listing[path.name] = hashlib.sha256(data).hexdigest()
                    writes.append((path, data))
                else:
                    # An emptied provisional file is removed, with its listing.
                    self.listing.pop(path.name, None)
                    writes.append((path, None))
            else:
                data = dump(values).encode("utf-8")
                self.history = store.with_file(
                    self.history, path.name, hashlib.sha256(data).hexdigest(), _today()
                )
                writes.append((path, data))
        for path, data in writes:
            if data is None:
                path.unlink(missing_ok=True)
                print(f"removed {path}")
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                _ = path.write_bytes(data)
                print(f"wrote {path}")
        manifest["golden"]["history"] = self.history
        _set_provisional_listing(manifest, self.listing)
        store.write_json(manifest_path, manifest)


def add(root: pathlib.Path, version: str, groups: list[str]) -> list[str]:
    """Write the computed entries that no golden file pins yet.

    An entry of a case that declares a provisional feature goes to the provisional file of
    its group; any other to the group's open frozen file.

    Args:
        root: Project root.
        version: Data context version.
        groups: Groups to compute.

    Returns:
        Names of the files written, relative to the golden folder.
    """
    _require_sound(root, version)
    path = store.manifest_path(root, version)
    manifest = store.read_json(path)
    folder = store.golden_folder(root, manifest)
    pinned = store.load_folder(folder)
    owner = {entry: group for group, values in pinned.items() for entry in values}
    files = store.group_files(folder)
    released = store.released_files(manifest["golden"]["history"])
    plan = _Plan(folder, manifest)
    # Compute and plan every group before writing anything, so a failure leaves the files
    # and their history as they were.
    counts: dict[pathlib.Path, int] = {}
    for group in groups:
        live, provisional = cases.compute_group_kinds(group)
        clash = sorted(e for e in live if owner.get(e, group) != group)
        if clash:
            raise SystemExit(f"{group} entries pinned under another group: {clash}")
        new = {e: v for e, v in live.items() if e not in owner}
        if not new:
            continue
        # An entry name appears once in a folder: claim it before the next group plans.
        owner.update(dict.fromkeys(new, group))
        frozen_target = _open_frozen_file(folder, files, released, group)
        provisional_target = store.provisional_folder(folder) / f"{group}.json"
        for entry, value in new.items():
            target = provisional_target if entry in provisional else frozen_target
            plan.values(target)[entry] = value
            counts[target] = counts.get(target, 0) + 1
    if not plan.contents:
        print("nothing to add: every computed entry is pinned")
        return []
    for target, count in counts.items():
        print(f"adding {count} entries to {target}")
    plan.write(manifest, path)
    return [target.relative_to(folder).as_posix() for target in plan.contents]


def _misplaced(
    pinned_provisional: set[str], live: dict[str, Any], provisional: frozenset[str]
) -> tuple[list[str], list[str]]:
    """Entries pinned in the wrong kind of file: (to make provisional, to freeze)."""
    to_provisional = sorted(
        e for e in live if e in provisional and e not in pinned_provisional
    )
    to_frozen = sorted(
        e for e in pinned_provisional if e in live and e not in provisional
    )
    return to_provisional, to_frozen


def check(root: pathlib.Path, version: str, groups: list[str]) -> list[str]:
    """Compare the computed values with the golden folder; write nothing.

    Args:
        root: Project root.
        version: Data context version.
        groups: Groups to compute.

    Returns:
        One sentence per difference or rule problem, including an entry pinned in a
        frozen file whose case is provisional, or the other way round.
    """
    problems = store.tree_problems(root, version)
    manifest = store.read_json(store.manifest_path(root, version))
    folder = store.golden_folder(root, manifest)
    try:
        pinned = store.load_folder(folder)
        pinned_provisional = store.load_provisional(folder)
    except store.GoldenLayoutError as exc:
        return [*problems, str(exc)]
    for group in groups:
        live, provisional = cases.compute_group_kinds(group)
        pins = pinned.get(group, {})
        skipped = cases.unavailable_entries(group)
        for entry in sorted(set(live) | set(pins)):
            if entry in skipped:
                continue
            if entry not in pins:
                problems.append(f"{group}:{entry} is computed but not pinned")
            elif entry not in live:
                problems.append(f"{group}:{entry} is pinned but no longer produced")
            elif live[entry] != pins[entry]:
                problems.append(
                    f"{group}:{entry} moved: pinned {pins[entry]!r}, live {live[entry]!r}"
                )
        here = set(pinned_provisional.get(group, {}))
        to_provisional, to_frozen = _misplaced(here, live, provisional)
        problems.extend(
            f"{group}:{e} is pinned in a frozen file, but its case is provisional: "
            "run generate_golden_hashes.py --relocate"
            for e in to_provisional
            if e in pins
        )
        problems.extend(
            f"{group}:{e} is pinned in the provisional file, but its case is not "
            "provisional: run generate_golden_hashes.py --relocate"
            for e in to_frozen
        )
    return problems


def remove(root: pathlib.Path, version: str, entries: list[str]) -> list[str]:
    """Remove entries from golden files no release lists, recording the files again.

    The first step of an approved change to an unreleased value: remove the entry, then
    ``--add`` writes its new value. An entry in a file a release lists is refused, and
    nothing is written unless every entry can be removed. A provisional file is never
    listed by a release; one left without entries is removed.

    Args:
        root: Project root.
        version: Data context version.
        entries: Entry names to remove.

    Returns:
        Names of the files rewritten, relative to the golden folder.
    """
    _require_sound(root, version)
    path = store.manifest_path(root, version)
    manifest = store.read_json(path)
    folder = store.golden_folder(root, manifest)
    released = store.released_files(manifest["golden"]["history"])
    files = [
        *(file for paths in store.group_files(folder).values() for file in paths),
        *store.provisional_files(folder).values(),
    ]
    where = {entry: file for file in files for entry in store.read_json(file)}
    unknown = sorted(e for e in entries if e not in where)
    if unknown:
        raise SystemExit(f"no golden file pins {unknown}; nothing written")
    frozen = sorted(
        e for e in entries if where[e].parent == folder and where[e].name in released
    )
    if frozen:
        raise SystemExit(
            f"{frozen} are in files a release lists; a released value never changes "
            "(a change needs a new data context version); nothing written"
        )
    plan = _Plan(folder, manifest)
    for entry in dict.fromkeys(entries):
        _ = plan.values(where[entry]).pop(entry)
    plan.write(manifest, path)
    return [file.relative_to(folder).as_posix() for file in plan.contents]


def relocate(root: pathlib.Path, version: str, groups: list[str]) -> list[str]:
    """Move pinned entries to the kind of file their case declares; values unchanged.

    An entry of a provisional case pinned in a frozen file moves to the provisional file
    of its group: refused, with nothing written, for a file a release lists, since a
    frozen value never leaves. An entry pinned in a provisional file whose case no longer
    declares a provisional feature (its operator left the group) moves to the open frozen
    file of its group, to be frozen by the next release. Every value is copied as it is
    pinned; nothing is computed into a file.

    Args:
        root: Project root.
        version: Data context version.
        groups: Groups whose cases are computed to learn their kinds.

    Returns:
        Names of the files rewritten, relative to the golden folder.
    """
    _require_sound(root, version)
    path = store.manifest_path(root, version)
    manifest = store.read_json(path)
    folder = store.golden_folder(root, manifest)
    files = store.group_files(folder)
    released = store.released_files(manifest["golden"]["history"])
    pinned_provisional = store.load_provisional(folder)
    where = {
        entry: file
        for paths in files.values()
        for file in paths
        for entry in store.read_json(file)
    }
    plan = _Plan(folder, manifest)
    for group in groups:
        live, provisional = cases.compute_group_kinds(group)
        here = pinned_provisional.get(group, {})
        to_provisional, to_frozen = _misplaced(set(here), live, provisional)
        to_provisional = [e for e in to_provisional if e in where]
        stuck = sorted(e for e in to_provisional if where[e].name in released)
        if stuck:
            raise SystemExit(
                f"{stuck} are provisional but pinned in files a release lists; a "
                "released value never leaves its file; nothing written"
            )
        provisional_file = store.provisional_folder(folder) / f"{group}.json"
        for entry in to_provisional:
            value = plan.values(where[entry]).pop(entry)
            plan.values(provisional_file)[entry] = value
        if to_frozen:
            target = _open_frozen_file(folder, files, released, group)
            for entry in to_frozen:
                plan.values(target)[entry] = plan.values(provisional_file).pop(entry)
        if to_provisional or to_frozen:
            print(
                f"{group}: {len(to_provisional)} entries to the provisional file, "
                f"{len(to_frozen)} to a frozen file"
            )
    if not plan.contents:
        print(
            "nothing to relocate: every entry is in the kind of file its case declares"
        )
        return []
    plan.write(manifest, path)
    return [file.relative_to(folder).as_posix() for file in plan.contents]


def release(root: pathlib.Path, version: str, release_text: str) -> str:
    """Record a library release in the golden history and the pins.

    Args:
        root: Project root.
        version: Data context version.
        release_text: The library version, e.g. ``0.3.0``.

    Returns:
        The digest of the release revision.
    """
    try:
        name = store.normalize_release(release_text)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    _require_sound(root, version)
    path = store.manifest_path(root, version)
    manifest = store.read_json(path)
    pins = store.read_json(store.pins_path(root))
    pin = pins[version]
    if name in pin["releases"]:
        raise SystemExit(f"{name} is already released")
    if name in store.withdrawn_releases(pin):
        raise SystemExit(
            f"{name} was withdrawn and is never released again: a package index never "
            "takes a version twice; release the next version"
        )
    history = manifest["golden"]["history"]
    if not history:
        raise SystemExit("the golden history lists no file")
    history = store.with_release(history, name, _today())
    manifest["golden"]["history"] = history
    if manifest["released_in"] is None:
        manifest["released_in"] = name
    fixed = store.fixed_manifest_digest(manifest)
    pins[version] = {
        **pin,
        "manifest": fixed,
        "releases": {**pin["releases"], name: history[-1]["digest"]},
    }
    store.write_json(path, manifest)
    store.write_json(store.pins_path(root), pins)
    print(f"released {name}: revision {history[-1]['digest']}, manifest {fixed}")
    return history[-1]["digest"]


def main(argv: list[str] | None = None) -> None:
    """Add, check or release the golden files, or print the computed values.

    Args:
        argv: Command-line arguments; ``None`` reads ``sys.argv``.
    """
    parser = argparse.ArgumentParser(
        description="Add, check or release the golden hash files (ITL-626, ITL-946)."
    )
    mode = parser.add_mutually_exclusive_group()
    _ = mode.add_argument(
        "--add",
        action="store_true",
        help="write computed entries no golden file pins yet (the default)",
    )
    _ = mode.add_argument(
        "--check",
        action="store_true",
        help="report moved, unpinned and missing entries; write nothing",
    )
    _ = mode.add_argument(
        "--release",
        metavar="X.Y.Z",
        help="append the release revision and pin it; computes no hash",
    )
    _ = mode.add_argument(
        "--remove",
        metavar="ENTRY",
        action="append",
        help=(
            "remove an entry from a file no release lists (repeatable); with the "
            "developer's approval, the first step of changing an unreleased value"
        ),
    )
    _ = mode.add_argument(
        "--relocate",
        action="store_true",
        help=(
            "move pinned entries, values unchanged, between the frozen files and the "
            "provisional files, as their cases declare (ITL-1266)"
        ),
    )
    _ = mode.add_argument(
        "--stdout",
        action="store_true",
        help="print all computed groups as one JSON object; write nothing",
    )
    _ = parser.add_argument("--group", choices=cases.GROUPS, action="append")
    _ = parser.add_argument(
        "--allow-errors",
        action="store_true",
        help="with --stdout: record a failing case as ERROR instead of stopping",
    )
    _ = parser.add_argument(
        "--root",
        type=pathlib.Path,
        default=_PROJECT_ROOT,
        help="project root holding the manifest, the pins and the golden folder",
    )
    args = parser.parse_args(argv)
    if args.allow_errors and not args.stdout:
        parser.error("--allow-errors is only for --stdout: errors are never pinned")

    version = get_default_context().version
    if args.remove:
        _ = remove(args.root, version, args.remove)
        return
    if args.release is not None:
        _ = release(args.root, version, args.release)
        return

    unavailable = [g for g in args.group or [] if not cases.group_available(g)]
    if unavailable:
        parser.error(
            f"group(s) {unavailable} need an optional dependency that is not installed "
            f"({[cases.OPTIONAL_GROUPS[g] for g in unavailable]})"
        )
    groups = args.group or list(cases.available_groups())
    skipped = [
        g for g in cases.GROUPS if g not in groups and not cases.group_available(g)
    ]
    if skipped and not args.stdout:
        print(f"skipped {skipped}: optional dependency not installed")
    if args.stdout:
        computed = {
            g: cases.compute_group(g, allow_errors=args.allow_errors) for g in groups
        }
        _ = sys.stdout.write(dump(computed))
        return
    if args.relocate:
        _ = relocate(args.root, version, groups)
        return
    if args.check:
        problems = check(args.root, version, groups)
        if problems:
            raise SystemExit(
                f"{len(problems)} golden problem(s):\n  " + "\n  ".join(problems)
            )
        print(f"golden values of {', '.join(groups)} match")
        return
    _ = add(args.root, version, groups)


if __name__ == "__main__":
    main()
