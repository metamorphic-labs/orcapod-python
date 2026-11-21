"""The golden folder, its history and the generator's add, check and release (ITL-946).

The generator runs on a copy of the repository's golden tree (``--root``), never on the
repository itself.
"""

from __future__ import annotations

import functools
import json
import re
import warnings
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.errors import ProvisionalHashWarning
from orcapod.operators import Batch, MergeJoin
from tests.test_hashing.golden import cases, store
from tests.test_hashing.golden.generate_golden_hashes import dump, main

VERSION = get_default_context().version


# The trial trees copy the repository's golden folder, so the newest file of a group and the
# next counter come from that inventory, never from fixed names.
_REPOSITORY_FILES = store.group_files(
    store.golden_folder(
        store.PROJECT_ROOT,
        store.read_json(store.manifest_path(store.PROJECT_ROOT, VERSION)),
    )
)
NEWEST_SCHEMA = _REPOSITORY_FILES["schema"][-1].name
NEXT_SCHEMA = store.next_file_name(NEWEST_SCHEMA)


def _write(folder: Path, name: str, values: dict[str, Any]) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    _ = (folder / name).write_text(dump(values), encoding="utf-8")


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    """A copy of the repository's golden tree, reopened: one open revision, no release."""
    root = tmp_path / "tree"
    store.copy_tree(store.PROJECT_ROOT, root, VERSION)
    store.reopen_tree(root, VERSION, "2026-11-15")
    return root


def _manifest(root: Path) -> dict[str, Any]:
    return store.read_json(store.manifest_path(root, VERSION))


def _folder(root: Path) -> Path:
    return store.golden_folder(root, _manifest(root))


def _history(root: Path) -> list[dict[str, Any]]:
    return _manifest(root)["golden"]["history"]


def _pin(root: Path) -> dict[str, Any]:
    return store.read_json(store.pins_path(root))[VERSION]


def _record(root: Path, name: str, values: dict[str, Any]) -> None:
    """Write an unreleased golden file and record it, as an earlier ``--add`` would."""
    folder = _folder(root)
    _write(folder, name, values)
    manifest = _manifest(root)
    manifest["golden"]["history"] = store.with_file(
        manifest["golden"]["history"],
        name,
        store.file_digest(folder / name),
        "2026-11-15",
    )
    store.write_json(store.manifest_path(root, VERSION), manifest)


def _drop_entries(root: Path, name: str, count: int) -> dict[str, Any]:
    """Remove the last ``count`` entries of an unreleased file; return them."""
    values = store.read_json(_folder(root) / name)
    dropped = dict(list(values.items())[-count:])
    _record(root, name, {k: v for k, v in values.items() if k not in dropped})
    return dropped


def _snapshot(folder: Path) -> dict[str, bytes]:
    return {
        path.name: path.read_bytes()
        for path in sorted(folder.iterdir())
        if path.is_file()
    }


# ---------------------------------------------------------------------------
# The folder
# ---------------------------------------------------------------------------


def test_the_repository_tree_is_sound() -> None:
    """The repository's manifest, pins and golden folder agree."""
    assert store.tree_problems(store.PROJECT_ROOT, VERSION) == []


@pytest.mark.parametrize(
    ("first", "second"),
    [("arrow-01.json", "arrow-02.json"), ("arrow-01.json", "schema-01.json")],
)
def test_an_entry_pinned_in_two_files_is_an_error(
    tmp_path: Path, first: str, second: str
) -> None:
    """An entry name appears once in a folder, across the files of every group."""
    _write(tmp_path, first, {"a": 1, "b": 2})
    _write(tmp_path, second, {"b": 2})
    with pytest.raises(store.GoldenLayoutError, match="'b' is pinned twice"):
        _ = store.load_folder(tmp_path)


def test_entries_of_a_group_merge_over_its_files(tmp_path: Path) -> None:
    _write(tmp_path, "arrow-01.json", {"a": 1})
    _write(tmp_path, "arrow-02.json", {"b": 2})
    _write(tmp_path, "schema-01.json", {"c": 3})
    assert store.load_folder(tmp_path) == {
        "arrow": {"a": 1, "b": 2},
        "schema": {"c": 3},
    }


@pytest.mark.parametrize(
    "name", ["arrow.json", "arrow-1.json", "arrow-001.json", "notes.txt"]
)
def test_a_file_not_named_group_and_counter_is_an_error(
    tmp_path: Path, name: str
) -> None:
    _ = (tmp_path / name).write_text("{}")
    with pytest.raises(store.GoldenLayoutError, match="<group>-NN.json"):
        _ = store.load_folder(tmp_path)


def test_an_edited_listed_file_is_reported(tree: Path) -> None:
    """The problem says to restore the file, never to record the edit with the generator."""
    path = _folder(tree) / "arrow-01.json"
    _ = path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
    [problem] = [
        p
        for p in store.tree_problems(tree, VERSION)
        if "arrow-01.json does not have the sha256" in p
    ]
    assert "restore" in problem
    assert "--remove" in problem
    assert "to record it" not in problem


def test_an_unlisted_file_is_reported(tree: Path) -> None:
    name = store.next_file_name(_REPOSITORY_FILES["arrow"][-1].name)
    _write(_folder(tree), name, {"new": 1})
    assert f"{name} is in" in " ".join(store.tree_problems(tree, VERSION))


def test_a_recorded_file_repeating_an_entry_name_is_reported(tree: Path) -> None:
    """A duplicate entry name in correctly recorded files is a problem, and no release."""
    entry, value = next(iter(store.read_json(_folder(tree) / NEWEST_SCHEMA).items()))
    _record(tree, NEXT_SCHEMA, {entry: value})
    assert any("pinned twice" in p for p in store.tree_problems(tree, VERSION))
    with pytest.raises(SystemExit, match="pinned twice"):
        main(["--root", str(tree), "--release", "0.3.0"])


def test_a_recorded_file_of_an_unknown_group_is_reported(tree: Path) -> None:
    """A file whose group is not one of ``cases.GROUPS`` is a problem, even when recorded."""
    _record(tree, "obsolete-01.json", {"obsolete.entry": "x"})
    assert any(
        "obsolete-01.json" in p and "known group" in p
        for p in store.tree_problems(tree, VERSION)
    )


def test_a_key_repeated_inside_one_file_is_an_error(tmp_path: Path) -> None:
    """JSON keeps the last of two equal keys; the store refuses the file instead."""
    _ = (tmp_path / "arrow-01.json").write_text(
        '{"example.entry": "old", "example.entry": "current"}\n'
    )
    with pytest.raises(store.GoldenLayoutError, match="example.entry"):
        _ = store.load_folder(tmp_path)


def test_a_key_repeated_inside_a_recorded_file_is_reported(tree: Path) -> None:
    name = NEXT_SCHEMA
    folder = _folder(tree)
    _ = (folder / name).write_bytes(b'{"dup.entry": "a", "dup.entry": "b"}\n')
    manifest = _manifest(tree)
    manifest["golden"]["history"] = store.with_file(
        manifest["golden"]["history"],
        name,
        store.file_digest(folder / name),
        "2026-11-15",
    )
    store.write_json(store.manifest_path(tree, VERSION), manifest)
    assert any("dup.entry" in p for p in store.tree_problems(tree, VERSION))


@pytest.mark.parametrize(
    "name", ["../arrow-01.json", "sub/arrow-01.json", "obsolete-01.json", "arrow.json"]
)
def test_a_listed_name_that_is_not_a_golden_file_name_is_reported(
    tree: Path, name: str
) -> None:
    """A history entry names a ``<group>-NN.json`` file of a known group, in the folder."""
    history = _history(tree)
    history[0]["added"][name] = "0" * 64
    history = store.rechain(history)
    problems = store.folder_problems(history, _folder(tree))
    assert any(name in p and "not a golden file name" in p for p in problems)


def test_a_missing_listed_file_is_reported(tree: Path) -> None:
    (_folder(tree) / "torch-01.json").unlink()
    assert any(
        "torch-01.json" in p and "missing" in p
        for p in store.tree_problems(tree, VERSION)
    )


def test_a_file_listed_twice_is_reported(tree: Path) -> None:
    history = _history(tree)
    added = history[0]["added"]
    history = store.with_release(history, "0.3.0", "2026-11-16")
    history.append(
        {"date": "2026-11-17", "release": None, "added": dict(added), "previous": None}
    )
    history = store.rechain(history)
    assert store.history_problems(history) == []
    assert any(
        "listed by revisions 0 and 2" in p
        for p in store.folder_problems(history, _folder(tree))
    )


# ---------------------------------------------------------------------------
# The history
# ---------------------------------------------------------------------------


def _released_history(tree: Path) -> list[dict[str, Any]]:
    return store.with_release(_history(tree), "0.3.0", "2026-11-16")


def test_a_rechained_history_is_sound(tree: Path) -> None:
    assert store.history_problems(_released_history(tree)) == []


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        (
            lambda h: h[0].update(date="2026-11-14"),
            "revision 0: digest does not recompute",
        ),
        (lambda h: h[1].update(previous=None), "revision 1: previous is not"),
        (
            lambda h: h[1].update(added={"x-01.json": "00"}),
            "release revision and adds files",
        ),
        (lambda h: h[0].update(added={}), "adds no file and is not a release"),
        (lambda h: h[1].pop("digest"), "does not have exactly the fields"),
        (
            lambda h: h.append(dict(h[1], previous=h[1]["digest"])),
            "release 0.3.0 appears twice",
        ),
        (lambda h: h[0].update(date="15 November"), "no YYYY-MM-DD date"),
    ],
)
def test_a_broken_history_is_reported(tree: Path, change: Any, problem: str) -> None:
    history = _released_history(tree)
    change(history)
    assert problem in " ".join(store.history_problems(history))


def test_a_released_file_cannot_be_recorded_again(tree: Path) -> None:
    with pytest.raises(ValueError, match="never changes"):
        _ = store.with_file(
            _released_history(tree), "arrow-01.json", "00", "2026-11-17"
        )


def test_rechain_leaves_released_revisions_alone(tree: Path) -> None:
    history = _released_history(tree)
    tampered = [dict(history[0], date="2000-01-01"), *history[1:]]
    assert store.rechain(tampered)[0]["digest"] == history[0]["digest"]


# ---------------------------------------------------------------------------
# The pins
# ---------------------------------------------------------------------------


def test_a_final_release_needs_its_releases_line_and_a_pinned_manifest() -> None:
    pin = {"manifest": None, "releases": {}}
    problems = store.final_release_problems(pin, "0.3.0")
    assert "release 0.3.0 ships an open manifest" in problems
    assert any("no releases line" in p for p in problems)


@pytest.mark.parametrize("version", ["0.3.1.dev4+g1234abc", "0.3.0rc1", "0.3.0+local"])
def test_a_build_that_is_not_a_final_release_needs_no_releases_line(
    version: str,
) -> None:
    assert (
        store.final_release_problems({"manifest": None, "releases": {}}, version) == []
    )


def test_the_repository_pin_is_consistent() -> None:
    """The repository's pin, manifest and history agree, open or released."""
    pin = _pin(store.PROJECT_ROOT)
    manifest = _manifest(store.PROJECT_ROOT)
    assert store.pin_problems(manifest, pin, manifest["golden"]["history"]) == []


# ---------------------------------------------------------------------------
# The generator on a copy: add, check, release
# ---------------------------------------------------------------------------


def test_add_writes_new_entries_into_the_open_file_of_their_group(tree: Path) -> None:
    """A group's newest file that no release lists takes the new entries."""
    dropped = _drop_entries(tree, NEWEST_SCHEMA, 3)
    before = _snapshot(_folder(tree))
    main(["--root", str(tree), "--group", "schema"])
    after = _snapshot(_folder(tree))
    assert set(after) == set(before)
    schema = json.loads(after[NEWEST_SCHEMA])
    assert dropped.items() <= schema.items()
    assert {n: b for n, b in after.items() if n != NEWEST_SCHEMA} == {
        n: b for n, b in before.items() if n != NEWEST_SCHEMA
    }
    assert len(_history(tree)) == 1
    assert store.tree_problems(tree, VERSION) == []


def test_add_after_a_release_writes_a_new_file_and_a_new_revision(tree: Path) -> None:
    """A released file never changes: new entries go to the next ``<group>-NN.json``."""
    dropped = _drop_entries(tree, NEWEST_SCHEMA, 2)
    main(["--root", str(tree), "--release", "0.3.0"])
    before = _snapshot(_folder(tree))
    main(["--root", str(tree), "--group", "schema"])
    after = _snapshot(_folder(tree))
    assert {n: after[n] for n in before} == before
    assert set(after) - set(before) == {NEXT_SCHEMA}
    assert json.loads(after[NEXT_SCHEMA]) == dropped
    history = _history(tree)
    assert [r["release"] for r in history] == [None, "0.3.0", None]
    assert history[2]["previous"] == history[1]["digest"]
    assert list(history[2]["added"]) == [NEXT_SCHEMA]
    assert store.tree_problems(tree, VERSION) == []


def test_add_changes_nothing_when_a_later_group_fails(
    tree: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every group is computed before anything is written: files and history stay in step."""
    _ = _drop_entries(tree, NEWEST_SCHEMA, 1)
    before = _snapshot(_folder(tree)), store.manifest_path(tree, VERSION).read_bytes()
    compute = cases.compute_group_kinds

    def failing(group: str, **kwargs: Any) -> tuple[dict[str, Any], frozenset[str]]:
        if group == "semantic":
            raise RuntimeError("semantic failed")
        return compute(group, **kwargs)

    monkeypatch.setattr(cases, "compute_group_kinds", failing)
    with pytest.raises(RuntimeError, match="semantic failed"):
        main(["--root", str(tree), "--group", "schema", "--group", "semantic"])
    after = _snapshot(_folder(tree)), store.manifest_path(tree, VERSION).read_bytes()
    assert after == before
    assert store.tree_problems(tree, VERSION) == []


def test_add_refuses_one_new_entry_name_in_two_groups(
    tree: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two groups that compute the same new entry name in one run write nothing."""
    before = _snapshot(_folder(tree)), store.manifest_path(tree, VERSION).read_bytes()
    compute = cases.compute_group_kinds

    def with_shared_entry(
        group: str, **kwargs: Any
    ) -> tuple[dict[str, Any], frozenset[str]]:
        values, provisional = compute(group, **kwargs)
        return {**values, "shared.new_entry": group}, provisional

    monkeypatch.setattr(cases, "compute_group_kinds", with_shared_entry)
    with pytest.raises(SystemExit, match="shared.new_entry"):
        main(["--root", str(tree), "--group", "schema", "--group", "arrow"])
    after = _snapshot(_folder(tree)), store.manifest_path(tree, VERSION).read_bytes()
    assert after == before


def test_add_writes_nothing_when_every_entry_is_pinned(tree: Path) -> None:
    before = _snapshot(tree / store.CONTEXTS_DIR / "data"), _snapshot(_folder(tree))
    main(["--root", str(tree), "--group", "schema"])
    assert (
        _snapshot(tree / store.CONTEXTS_DIR / "data"),
        _snapshot(_folder(tree)),
    ) == before


def test_add_refuses_a_tree_that_breaks_the_rules(tree: Path) -> None:
    path = _folder(tree) / NEWEST_SCHEMA
    _ = path.write_text(path.read_text() + " ")
    manifest = store.manifest_path(tree, VERSION).read_bytes()
    with pytest.raises(SystemExit, match="nothing written"):
        main(["--root", str(tree), "--group", "schema"])
    assert store.manifest_path(tree, VERSION).read_bytes() == manifest


def test_remove_then_add_changes_an_unreleased_value(tree: Path) -> None:
    """The approved-change procedure: ``--remove`` the entry, then ``--add`` writes it."""
    name = NEWEST_SCHEMA
    values = store.read_json(_folder(tree) / name)
    entry = next(iter(values))
    _record(tree, name, {**values, entry: "old value"})
    main(["--root", str(tree), "--remove", entry])
    assert entry not in store.read_json(_folder(tree) / name)
    assert store.tree_problems(tree, VERSION) == []
    main(["--root", str(tree), "--group", "schema"])
    assert store.read_json(_folder(tree) / name) == values
    assert len(_history(tree)) == 1
    assert store.tree_problems(tree, VERSION) == []


def test_remove_refuses_an_entry_a_release_lists(tree: Path) -> None:
    main(["--root", str(tree), "--release", "0.3.0"])
    entry = next(iter(store.read_json(_folder(tree) / NEWEST_SCHEMA)))
    before = _snapshot(_folder(tree)), _snapshot(tree / store.CONTEXTS_DIR / "data")
    with pytest.raises(SystemExit, match="released"):
        main(["--root", str(tree), "--remove", entry])
    after = _snapshot(_folder(tree)), _snapshot(tree / store.CONTEXTS_DIR / "data")
    assert after == before


def test_remove_refuses_an_unknown_entry_and_writes_nothing(tree: Path) -> None:
    entry = next(iter(store.read_json(_folder(tree) / NEWEST_SCHEMA)))
    before = _snapshot(_folder(tree)), _snapshot(tree / store.CONTEXTS_DIR / "data")
    with pytest.raises(SystemExit, match="no.such.entry"):
        main(["--root", str(tree), "--remove", entry, "--remove", "no.such.entry"])
    after = _snapshot(_folder(tree)), _snapshot(tree / store.CONTEXTS_DIR / "data")
    assert after == before


def test_check_reports_a_moved_value_and_writes_nothing(tree: Path) -> None:
    name = NEWEST_SCHEMA
    values = store.read_json(_folder(tree) / name)
    entry = next(iter(values))
    _record(tree, name, {**values, entry: "moved"})
    before = _snapshot(_folder(tree)), store.manifest_path(tree, VERSION).read_bytes()
    with pytest.raises(SystemExit, match=re.escape(f"schema:{entry} moved")):
        main(["--root", str(tree), "--check", "--group", "schema"])
    after = _snapshot(_folder(tree)), store.manifest_path(tree, VERSION).read_bytes()
    assert after == before


def test_check_reports_an_unpinned_entry(tree: Path) -> None:
    dropped = _drop_entries(tree, NEWEST_SCHEMA, 1)
    with pytest.raises(
        SystemExit, match=re.escape(f"schema:{next(iter(dropped))} is computed")
    ):
        main(["--root", str(tree), "--check", "--group", "schema"])


def test_check_passes_on_the_copy(tree: Path) -> None:
    main(["--root", str(tree), "--check", "--group", "schema"])


def test_release_freezes_the_manifest_and_pins_the_release(tree: Path) -> None:
    """The freeze step, on a copy: release revision, pins and ``released_in``."""
    files = _snapshot(_folder(tree))
    main(["--root", str(tree), "--release", "v0.3.0"])
    manifest = _manifest(tree)
    history = manifest["golden"]["history"]
    pin = _pin(tree)
    assert history[-1]["release"] == "0.3.0"
    assert history[-1]["added"] == {}
    assert manifest["released_in"] == "0.3.0"
    assert pin == {
        "manifest": store.fixed_manifest_digest(manifest),
        "releases": {"0.3.0": history[-1]["digest"]},
    }
    assert _snapshot(_folder(tree)) == files
    assert store.tree_problems(tree, VERSION) == []
    assert store.final_release_problems(pin, "0.3.0") == []


def test_a_later_release_keeps_released_in_and_the_manifest_pin(tree: Path) -> None:
    main(["--root", str(tree), "--release", "0.3.0"])
    first = _pin(tree)
    main(["--root", str(tree), "--release", "0.3.1"])
    pin = _pin(tree)
    assert _manifest(tree)["released_in"] == "0.3.0"
    assert pin["manifest"] == first["manifest"]
    assert pin["releases"]["0.3.0"] == first["releases"]["0.3.0"]
    assert set(pin["releases"]) == {"0.3.0", "0.3.1"}
    assert store.tree_problems(tree, VERSION) == []


def test_a_released_manifest_edited_afterwards_is_reported(tree: Path) -> None:
    main(["--root", str(tree), "--release", "0.3.0"])
    manifest = _manifest(tree)
    manifest["description"] += " (edited)"
    store.write_json(store.manifest_path(tree, VERSION), manifest)
    assert any("differs from its pin" in p for p in store.tree_problems(tree, VERSION))


def test_a_released_golden_file_edited_afterwards_is_reported(tree: Path) -> None:
    main(["--root", str(tree), "--release", "0.3.0"])
    path = _folder(tree) / "semantic-01.json"
    _ = path.write_text(path.read_text().replace("semantic_v1", "semantic_v2", 1))
    assert any(
        "a released golden file never changes" in p
        for p in store.tree_problems(tree, VERSION)
    )


@pytest.mark.parametrize(
    "change",
    [
        lambda pin, h: pin["releases"].update({"0.3.1": h[-1]["digest"]}),
        lambda pin, h: pin["releases"].clear(),
        lambda pin, h: pin["releases"].update({"0.3.0": h[0]["digest"]}),
        lambda pin, h: pin.update(manifest=None),
    ],
)
def test_a_pin_that_disagrees_with_the_history_is_reported(
    tree: Path, change: Any
) -> None:
    main(["--root", str(tree), "--release", "0.3.0"])
    pins = store.read_json(store.pins_path(tree))
    change(pins[VERSION], _history(tree))
    store.write_json(store.pins_path(tree), pins)
    assert store.tree_problems(tree, VERSION)


@pytest.mark.parametrize(
    "version", ["0.3.0", "0.3.1+local", "0.3.1.dev1", "not-a-version"]
)
def test_release_refuses_a_version_it_cannot_record(tree: Path, version: str) -> None:
    main(["--root", str(tree), "--release", "0.3.0"])
    before = (
        _snapshot(tree / store.CONTEXTS_DIR),
        _snapshot(tree / store.CONTEXTS_DIR / "data"),
    )
    with pytest.raises(SystemExit):
        main(["--root", str(tree), "--release", version])
    after = (
        _snapshot(tree / store.CONTEXTS_DIR),
        _snapshot(tree / store.CONTEXTS_DIR / "data"),
    )
    assert after == before


def test_allow_errors_is_only_for_stdout(tree: Path) -> None:
    with pytest.raises(SystemExit):
        main(["--root", str(tree), "--allow-errors", "--group", "schema"])


# ---------------------------------------------------------------------------
# A withdrawn release (ITL-1255)
# ---------------------------------------------------------------------------

REASON = "Withdrawn before anyone used it."


def _released_and_withdrawn(tree: Path) -> Path:
    """Release 0.3.0 on the copy, add a file after it, then withdraw 0.3.0."""
    main(["--root", str(tree), "--release", "0.3.0"])
    _record(tree, NEXT_SCHEMA, {"later.entry": "x"})
    store.withdraw_release(tree, VERSION, "0.3.0", "2026-11-17", REASON)
    return tree


def test_a_withdrawn_release_freezes_nothing(tree: Path) -> None:
    """After the withdrawal the tree is sound and open, and the record keeps the pins."""
    main(["--root", str(tree), "--release", "0.3.0"])
    released = _pin(tree)
    files = _snapshot(_folder(tree))
    store.withdraw_release(tree, VERSION, "0.3.0", "2026-11-17", REASON)
    manifest = _manifest(tree)
    assert store.tree_problems(tree, VERSION) == []
    assert manifest["released_in"] is None
    assert store.released_files(manifest["golden"]["history"]) == set()
    assert [r["release"] for r in manifest["golden"]["history"]] == [None]
    assert _pin(tree) == {
        "manifest": None,
        "releases": {},
        "withdrawn": {
            "0.3.0": {
                "date": "2026-11-17",
                "reason": REASON,
                "manifest": released["manifest"],
                "revision": released["releases"]["0.3.0"],
            }
        },
    }
    assert _snapshot(_folder(tree)) == files


def test_withdrawing_rechains_the_revisions_after_the_release(tree: Path) -> None:
    _ = _released_and_withdrawn(tree)
    history = _history(tree)
    assert [r["release"] for r in history] == [None, None]
    assert NEXT_SCHEMA in history[1]["added"]
    assert history[1]["previous"] == history[0]["digest"]
    assert store.history_problems(history) == []


def test_withdrawing_only_takes_the_last_release(tree: Path) -> None:
    main(["--root", str(tree), "--release", "0.3.0"])
    _record(tree, NEXT_SCHEMA, {"later.entry": "x"})
    main(["--root", str(tree), "--release", "0.3.1"])
    with pytest.raises(ValueError, match="not the last release"):
        store.withdraw_release(tree, VERSION, "0.3.0", "2026-11-17", REASON)
    store.withdraw_release(tree, VERSION, "0.3.1", "2026-11-17", REASON)
    pin = _pin(tree)
    assert set(pin["releases"]) == {"0.3.0"}
    assert pin["manifest"] is not None
    assert _manifest(tree)["released_in"] == "0.3.0"
    assert store.tree_problems(tree, VERSION) == []


def test_a_value_a_withdrawn_release_listed_can_change(tree: Path) -> None:
    """The approved-change procedure works on a file the withdrawn release listed."""
    main(["--root", str(tree), "--release", "0.3.0"])
    name = NEWEST_SCHEMA
    entries = list(store.read_json(_folder(tree) / name))
    with pytest.raises(SystemExit, match="released"):
        main(["--root", str(tree), "--remove", entries[0]])
    store.withdraw_release(tree, VERSION, "0.3.0", "2026-11-17", REASON)
    main(["--root", str(tree), "--remove", entries[-1], "--remove", entries[0]])
    remaining = store.read_json(_folder(tree) / name)
    assert entries[0] not in remaining and entries[-1] not in remaining
    assert store.tree_problems(tree, VERSION) == []
    main(["--root", str(tree), "--group", "schema"])
    assert list(store.read_json(_folder(tree) / name)) == sorted(entries)
    assert store.tree_problems(tree, VERSION) == []


def test_release_refuses_a_withdrawn_version_and_writes_nothing(tree: Path) -> None:
    _ = _released_and_withdrawn(tree)
    before = (
        _snapshot(tree / store.CONTEXTS_DIR),
        _snapshot(tree / store.CONTEXTS_DIR / "data"),
    )
    for version in ("0.3.0", "v0.3.0"):
        with pytest.raises(SystemExit, match="withdrawn"):
            main(["--root", str(tree), "--release", version])
    after = (
        _snapshot(tree / store.CONTEXTS_DIR),
        _snapshot(tree / store.CONTEXTS_DIR / "data"),
    )
    assert after == before


def test_the_next_release_freezes_again_and_keeps_the_record(tree: Path) -> None:
    """The freeze at 0.4.0: the release step after a withdrawal."""
    _ = _released_and_withdrawn(tree)
    withdrawn = _pin(tree)["withdrawn"]
    main(["--root", str(tree), "--release", "0.4.0"])
    manifest = _manifest(tree)
    history = manifest["golden"]["history"]
    assert manifest["released_in"] == "0.4.0"
    assert [r["release"] for r in history] == [None, None, "0.4.0"]
    assert _pin(tree) == {
        "manifest": store.fixed_manifest_digest(manifest),
        "releases": {"0.4.0": history[-1]["digest"]},
        "withdrawn": withdrawn,
    }
    assert store.released_files(history) == set(_snapshot(_folder(tree)))
    assert store.tree_problems(tree, VERSION) == []
    assert store.final_release_problems(_pin(tree), "0.4.0") == []


def _set_record(field: str, value: Any) -> Any:
    def change(pin: dict[str, Any], history: list[dict[str, Any]]) -> None:
        pin["withdrawn"]["0.3.0"][field] = value

    return change


def _drop_record_field(pin: dict[str, Any], history: list[dict[str, Any]]) -> None:
    del pin["withdrawn"]["0.3.0"]["reason"]


def _rename_record(pin: dict[str, Any], history: list[dict[str, Any]]) -> None:
    pin["withdrawn"]["v0.3.0"] = pin["withdrawn"].pop("0.3.0")


def _pin_withdrawn_as_release(
    pin: dict[str, Any], history: list[dict[str, Any]]
) -> None:
    pin["releases"]["0.3.0"] = pin["withdrawn"]["0.3.0"]["revision"]


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        (_set_record("date", "17 November 2026"), "no YYYY-MM-DD date"),
        (_set_record("reason", " "), "gives no reason"),
        (_set_record("manifest", "not a digest"), "manifest is neither null"),
        (_set_record("revision", None), "revision is not a sha256 digest"),
        (_drop_record_field, "does not have exactly the fields"),
        (_rename_record, "not a normalized release version"),
        (lambda pin, h: pin.update(withdrawn=["0.3.0"]), "withdrawn is not a mapping"),
        (_pin_withdrawn_as_release, "withdrawn release 0.3.0 is pinned under releases"),
        (lambda pin, h: pin.update(extra={}), "the pin is not"),
    ],
)
def test_a_withdrawn_record_that_breaks_the_rules_is_reported(
    tree: Path, change: Any, problem: str
) -> None:
    _ = _released_and_withdrawn(tree)
    pins = store.read_json(store.pins_path(tree))
    change(pins[VERSION], _history(tree))
    store.write_json(store.pins_path(tree), pins)
    problems = store.tree_problems(tree, VERSION)
    assert any(problem in p for p in problems), problems


def test_a_withdrawn_release_left_in_the_history_is_reported(tree: Path) -> None:
    """A record does not withdraw a release whose revision is still in the history."""
    main(["--root", str(tree), "--release", "0.3.0"])
    pins = store.read_json(store.pins_path(tree))
    pin = pins[VERSION]
    pin["withdrawn"] = {
        "0.3.0": {
            "date": "2026-11-17",
            "reason": REASON,
            "manifest": pin["manifest"],
            "revision": pin["releases"].pop("0.3.0"),
        }
    }
    store.write_json(store.pins_path(tree), pins)
    problems = store.tree_problems(tree, VERSION)
    assert any("is a release revision of the history" in p for p in problems), problems


# ---------------------------------------------------------------------------
# Provisional files: outside the history, never frozen (ITL-1266)
# ---------------------------------------------------------------------------

NEWEST_SEMANTIC = _REPOSITORY_FILES["semantic"][-1].name
NEXT_SEMANTIC = store.next_file_name(NEWEST_SEMANTIC)


def _provisional(root: Path) -> Path:
    return store.provisional_folder(_folder(root))


def _listing(root: Path) -> dict[str, str]:
    return _manifest(root)["golden"][store.PROVISIONAL]


def _everything(root: Path) -> dict[str, bytes]:
    """Every file of the golden folder (provisional files included) and the manifest."""
    folder = _folder(root)
    files = {
        path.relative_to(folder).as_posix(): path.read_bytes()
        for path in sorted(folder.rglob("*"))
        if path.is_file()
    }
    files["manifest"] = store.manifest_path(root, VERSION).read_bytes()
    return files


def _set_listing(root: Path, listing: dict[str, str]) -> None:
    manifest = _manifest(root)
    manifest["golden"][store.PROVISIONAL] = listing
    store.write_json(store.manifest_path(root, VERSION), manifest)


def _freeze_provisional(root: Path, group: str, frozen_file: str) -> dict[str, Any]:
    """Put a group's provisional entries into a frozen file, as before ITL-1266."""
    path = _provisional(root) / f"{group}.json"
    entries = store.read_json(path)
    path.unlink()
    listing = dict(_listing(root))
    del listing[f"{group}.json"]
    _set_listing(root, listing)
    _record(
        root, frozen_file, {**store.read_json(_folder(root) / frozen_file), **entries}
    )
    return entries


def _without_declaration(
    group: str, feature: str = "PolarsFilter"
) -> list[tuple[str, Any, tuple[str, ...]]]:
    """The cases of a group as if ``feature`` had left the provisional group: no case
    declares it any more; every other declaration stays (ITL-1324: a group holds
    provisional library value types too)."""
    return [
        (name, fn, tuple(n for n in declared if n != feature))
        for name, fn, declared in cases._CASES[group]
    ]


def _declaring(group: str, feature: str) -> set[str]:
    """The case names of a group that declare ``feature`` and nothing else."""
    return {
        name
        for name, _, declared in cases._CASES[group]
        if tuple(declared) == (feature,)
    }


def test_the_repository_pins_provisional_entries_outside_the_history() -> None:
    """The provisional files are listed in golden.provisional, never in a revision."""
    manifest = store.read_json(store.manifest_path(store.PROJECT_ROOT, VERSION))
    listing = manifest["golden"][store.PROVISIONAL]
    assert listing
    revised = {name for r in manifest["golden"]["history"] for name in r["added"]}
    assert not revised & set(listing)


def test_relocate_moves_provisional_entries_out_of_a_frozen_file(tree: Path) -> None:
    """The ITL-1266 move on a copy: values unchanged, the frozen file only loses them."""
    original = _everything(tree)
    entries = _freeze_provisional(tree, "semantic", NEWEST_SEMANTIC)
    assert entries
    main(["--root", str(tree), "--relocate", "--group", "semantic"])
    after = _everything(tree)
    assert {k: v for k, v in after.items() if k != "manifest"} == {
        k: v for k, v in original.items() if k != "manifest"
    }
    assert (
        _listing(tree)
        == store.read_json(store.manifest_path(store.PROJECT_ROOT, VERSION))["golden"][
            store.PROVISIONAL
        ]
    )
    assert len(_history(tree)) == 1
    assert store.tree_problems(tree, VERSION) == []


def test_relocate_writes_nothing_when_every_entry_is_in_place(tree: Path) -> None:
    before = _everything(tree)
    main(["--root", str(tree), "--relocate", "--group", "semantic"])
    assert _everything(tree) == before


def test_relocate_refuses_a_provisional_entry_in_a_released_file(tree: Path) -> None:
    """A released value never leaves its file, not even to become provisional."""
    _ = _freeze_provisional(tree, "semantic", NEWEST_SEMANTIC)
    main(["--root", str(tree), "--release", "0.3.0"])
    before = _everything(tree)
    with pytest.raises(SystemExit, match="release lists"):
        main(["--root", str(tree), "--relocate", "--group", "semantic"])
    assert _everything(tree) == before


@pytest.mark.parametrize("released", [False, True])
def test_relocate_freezes_the_entries_of_an_operator_that_left_the_group(
    tree: Path, monkeypatch: pytest.MonkeyPatch, released: bool
) -> None:
    """Graduation: the entries of the feature that left go to the open frozen file; the
    provisional file keeps the entries of every other provisional feature."""
    graduating = _declaring("semantic", "PolarsFilter")
    provisional = store.read_json(_provisional(tree) / "semantic.json")
    entries = {k: v for k, v in provisional.items() if k.split(".")[0] in graduating}
    staying = {k: v for k, v in provisional.items() if k not in entries}
    assert entries and staying
    if released:
        main(["--root", str(tree), "--release", "0.3.0"])
    newest = store.read_json(_folder(tree) / NEWEST_SEMANTIC)
    monkeypatch.setitem(cases._CASES, "semantic", _without_declaration("semantic"))
    main(["--root", str(tree), "--relocate", "--group", "semantic"])
    assert store.read_json(_provisional(tree) / "semantic.json") == staying
    assert "semantic.json" in _listing(tree)
    if released:
        assert store.read_json(_folder(tree) / NEWEST_SEMANTIC) == newest
        assert store.read_json(_folder(tree) / NEXT_SEMANTIC) == entries
        assert list(_history(tree)[-1]["added"]) == [NEXT_SEMANTIC]
    else:
        assert store.read_json(_folder(tree) / NEWEST_SEMANTIC) == {
            **newest,
            **entries,
        }
    assert store.tree_problems(tree, VERSION) == []


def test_check_reports_a_provisional_entry_in_a_frozen_file(tree: Path) -> None:
    entries = _freeze_provisional(tree, "semantic", NEWEST_SEMANTIC)
    entry = sorted(entries)[0]
    with pytest.raises(
        SystemExit,
        match=re.escape(f"semantic:{entry} is pinned in a frozen file, but its case"),
    ):
        main(["--root", str(tree), "--check", "--group", "semantic"])


def test_check_reports_an_entry_in_the_provisional_file_whose_case_is_not(
    tree: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    graduating = _declaring("semantic", "PolarsFilter")
    entry = sorted(
        k
        for k in store.read_json(_provisional(tree) / "semantic.json")
        if k.split(".")[0] in graduating
    )[0]
    monkeypatch.setitem(cases._CASES, "semantic", _without_declaration("semantic"))
    with pytest.raises(
        SystemExit,
        match=re.escape(f"semantic:{entry} is pinned in the provisional file"),
    ):
        main(["--root", str(tree), "--check", "--group", "semantic"])


@pytest.mark.parametrize("order", [("semantic", "pipeline"), ("pipeline", "semantic")])
def test_add_writes_provisional_entries_to_their_provisional_files(
    tree: Path, order: tuple[str, str]
) -> None:
    """Two groups, in either order: each provisional file comes back byte for byte."""
    original = _everything(tree)
    for name in ("semantic.json", "pipeline.json"):
        (_provisional(tree) / name).unlink()
    _set_listing(
        tree,
        {
            k: v
            for k, v in _listing(tree).items()
            if k not in ("semantic.json", "pipeline.json")
        },
    )
    args = ["--root", str(tree)]
    for group in order:
        args += ["--group", group]
    main(args)
    assert _everything(tree) == original
    assert store.tree_problems(tree, VERSION) == []


def test_a_release_never_lists_a_provisional_file(tree: Path) -> None:
    """After a release a provisional value still changes by --remove and --add."""
    listing = dict(_listing(tree))
    main(["--root", str(tree), "--release", "0.3.0"])
    assert _history(tree)[-1]["added"] == {}
    assert _listing(tree) == listing
    released = store.released_files(_history(tree))
    assert not released & set(listing)
    path = _provisional(tree) / "semantic.json"
    values = store.read_json(path)
    entry = sorted(values)[0]
    main(["--root", str(tree), "--remove", entry])
    assert entry not in store.read_json(path)
    assert store.tree_problems(tree, VERSION) == []
    main(["--root", str(tree), "--group", "semantic"])
    assert store.read_json(path) == values
    assert _listing(tree) == listing
    assert store.tree_problems(tree, VERSION) == []


def test_removing_every_entry_of_a_provisional_file_removes_it(tree: Path) -> None:
    path = _provisional(tree) / "semantic.json"
    main(["--root", str(tree), *(f"--remove={e}" for e in store.read_json(path))])
    assert not path.exists()
    assert "semantic.json" not in _listing(tree)
    assert store.tree_problems(tree, VERSION) == []


def test_an_unlisted_provisional_file_is_reported(tree: Path) -> None:
    listing = dict(_listing(tree))
    listing.pop("schema.json", None)
    _set_listing(tree, listing)
    _write(_provisional(tree), "schema.json", {"x.y": "z"})
    assert any(
        "golden.provisional does not list it" in p
        for p in store.tree_problems(tree, VERSION)
    )


def test_an_edited_provisional_file_is_reported(tree: Path) -> None:
    path = _provisional(tree) / "semantic.json"
    _ = path.write_text(path.read_text() + " ")
    assert any(
        "does not have the sha256 golden.provisional lists" in p
        for p in store.tree_problems(tree, VERSION)
    )


def test_a_missing_listed_provisional_file_is_reported(tree: Path) -> None:
    (_provisional(tree) / "semantic.json").unlink()
    assert any(
        "provisional/semantic.json is missing" in p
        for p in store.tree_problems(tree, VERSION)
    )


@pytest.mark.parametrize("name", ["unknown.json", "semantic-01.json", "Semantic.json"])
def test_a_misnamed_provisional_listing_is_reported(tree: Path, name: str) -> None:
    _set_listing(tree, {**_listing(tree), name: "0" * 64})
    assert any(
        f"{name}, listed by golden.provisional, is not a provisional file name" in p
        for p in store.tree_problems(tree, VERSION)
    )


def test_a_misnamed_file_in_the_provisional_folder_is_an_error(tree: Path) -> None:
    _write(_provisional(tree), "semantic-01.json", {})
    with pytest.raises(store.GoldenLayoutError, match="<group>.json"):
        _ = store.load_folder(_folder(tree))


def test_an_entry_in_a_frozen_and_a_provisional_file_is_reported(tree: Path) -> None:
    entry, value = next(
        iter(store.read_json(_provisional(tree) / "semantic.json").items())
    )
    _record(
        tree,
        NEWEST_SEMANTIC,
        {**store.read_json(_folder(tree) / NEWEST_SEMANTIC), entry: value},
    )
    assert any("is pinned twice" in p for p in store.tree_problems(tree, VERSION))


def test_a_manifest_without_the_provisional_listing_is_reported(tree: Path) -> None:
    manifest = _manifest(tree)
    del manifest["golden"][store.PROVISIONAL]
    store.write_json(store.manifest_path(tree, VERSION), manifest)
    assert store.tree_problems(tree, VERSION) == [
        'the manifest golden field is not {"folder": ..., "history": [...], '
        '"provisional": {...}}'
    ]


# ---------------------------------------------------------------------------
# Case declarations: a case using a provisional operator must declare it (ITL-1266)
# ---------------------------------------------------------------------------

_A, _B = "ProvisionalA", "ProvisionalB"


def _using(*names: str) -> Any:
    def fn() -> dict[str, Any]:
        from orcapod.core.operators.provisional import warn_provisional_operator

        for name in names:
            warn_provisional_operator(name)
        return {"value": list(names)}

    return fn


@pytest.fixture
def two_provisional(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two provisional operators, so declarations can name several."""
    monkeypatch.setattr(
        cases, "declarable_provisional_names", lambda: frozenset({_A, _B})
    )


@pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")
@pytest.mark.parametrize("used", [(_A, _B), (_B, _A)])
@pytest.mark.parametrize("declared", [(_A, _B), (_B, _A)])
def test_a_case_declaring_every_provisional_operator_it_uses_is_provisional(
    monkeypatch: pytest.MonkeyPatch,
    two_provisional: None,
    used: tuple[str, ...],
    declared: tuple[str, ...],
) -> None:
    """Neither the order of use nor of declaration changes what is provisional."""
    monkeypatch.setitem(
        cases._CASES,
        "semantic",
        [("both", _using(*used), declared), ("plain", _using(), ())],
    )
    values, provisional = cases.compute_group_kinds("semantic")
    assert values == {"both.value": list(used), "plain.value": []}
    assert provisional == frozenset({"both.value"})


@pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")
@pytest.mark.parametrize(
    ("declared", "missing"),
    [((), [_A, _B]), ((_A,), [_B]), ((_B,), [_A])],
)
def test_a_case_using_an_undeclared_provisional_operator_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    two_provisional: None,
    declared: tuple[str, ...],
    missing: list[str],
) -> None:
    monkeypatch.setitem(cases._CASES, "semantic", [("both", _using(_B, _A), declared)])
    with pytest.raises(ValueError, match=re.escape(f"uses the provisional {missing}")):
        _ = cases.compute_group_kinds("semantic")


def _prebuilt_inputs() -> tuple[Any, Any]:
    """Two inputs with two tags and two data columns, colliding on ``age``."""
    from orcapod.sources import ArrowTableSource

    left = pa.table(
        {
            "subject": ["s1", "s2", "s3"],
            "session": [1, 2, 1],
            "age": [2, 4, 6],
            "weight": [20.5, 22.25, 19.0],
        }
    )
    right = pa.table(
        {
            "session": [2, 1],
            "subject": ["s2", "s1"],
            "height": [1.5, 1.25],
            "age": [8, 7],
        }
    )
    return (
        ArrowTableSource(
            left, tag_columns=["subject", "session"], tag_only_columns=True
        ),
        ArrowTableSource(
            right, tag_columns=["session", "subject"], tag_only_columns=True
        ),
    )


@functools.cache
def _prebuilt() -> dict[str, Any]:
    """Provisional operators and their computed, hashed streams, built once and before
    any case is recorded, as a module-level ``op = Batch(2)`` is: the construction
    warning (and its note) falls outside every recording, so only a note on use can
    reveal them (ITL-1279). Built on first call, not at import, so collecting this module
    uses up no once-per-process warning."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ProvisionalHashWarning)
        batch, merge_join = Batch(2), MergeJoin()
        batched, merged = batch(_prebuilt_inputs()[0]), merge_join(*_prebuilt_inputs())
        for stream in (batched, merged):
            _ = stream.as_table(), stream.content_hash(), stream.pipeline_hash()
    return {
        "batch": batch,
        "merge_join": merge_join,
        "batched": batched,
        "merged": merged,
    }


# Each use of a prebuilt provisional operator inside a case: one unary, one binary.
_PREBUILT_USES: dict[str, tuple[str, Any]] = {
    "unary content hash": (
        "Batch",
        lambda: {"h": _prebuilt()["batch"].content_hash().to_string()},
    ),
    "unary pipeline hash": (
        "Batch",
        lambda: {"h": _prebuilt()["batch"].pipeline_hash().to_string()},
    ),
    "unary applied": (
        "Batch",
        lambda: cases.stream_summary(_prebuilt()["batch"](_prebuilt_inputs()[0])),
    ),
    "unary kept stream read": (
        "Batch",
        lambda: {"rows": _prebuilt()["batched"].as_table().num_rows},
    ),
    "unary kept stream hash": (
        "Batch",
        lambda: {"h": _prebuilt()["batched"].content_hash().to_string()},
    ),
    "binary content hash": (
        "MergeJoin",
        lambda: {"h": _prebuilt()["merge_join"].content_hash().to_string()},
    ),
    "binary pipeline hash": (
        "MergeJoin",
        lambda: {"h": _prebuilt()["merge_join"].pipeline_hash().to_string()},
    ),
    "binary applied": (
        "MergeJoin",
        lambda: cases.stream_summary(_prebuilt()["merge_join"](*_prebuilt_inputs())),
    ),
    "binary applied in the other order": (
        "MergeJoin",
        lambda: cases.stream_summary(
            _prebuilt()["merge_join"](*reversed(_prebuilt_inputs()))
        ),
    ),
    "binary kept stream read": (
        "MergeJoin",
        lambda: {"rows": _prebuilt()["merged"].as_table().num_rows},
    ),
    "binary kept stream pipeline hash": (
        "MergeJoin",
        lambda: {"h": _prebuilt()["merged"].pipeline_hash().to_string()},
    ),
}


@pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")
@pytest.mark.parametrize("use", sorted(_PREBUILT_USES))
def test_a_case_using_a_prebuilt_provisional_operator_undeclared_is_refused(
    monkeypatch: pytest.MonkeyPatch, use: str
) -> None:
    """An operator built before the case (at module level) is noted where its identity
    is read and where it is applied, so an undeclared case is refused (Codex review of
    PR 469)."""
    name, fn = _PREBUILT_USES[use]
    _ = _prebuilt()
    monkeypatch.setitem(cases._CASES, "semantic", [("prebuilt", fn, ())])
    with pytest.raises(ValueError, match=re.escape(f"uses the provisional {[name]}")):
        _ = cases.compute_group_kinds("semantic")


@pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")
@pytest.mark.parametrize("use", sorted(_PREBUILT_USES))
def test_a_case_declaring_a_prebuilt_provisional_operator_is_provisional(
    monkeypatch: pytest.MonkeyPatch, use: str
) -> None:
    name, fn = _PREBUILT_USES[use]
    _ = _prebuilt()
    monkeypatch.setitem(cases._CASES, "semantic", [("prebuilt", fn, (name,))])
    values, provisional = cases.compute_group_kinds("semantic")
    assert values
    assert provisional == frozenset(values)


def test_a_case_declaring_an_unknown_provisional_name_is_refused(
    monkeypatch: pytest.MonkeyPatch, two_provisional: None
) -> None:
    monkeypatch.setitem(
        cases._CASES, "semantic", [("case", _using(), (_A, "PolarsFilter"))]
    )
    with pytest.raises(ValueError, match=re.escape("declares ['PolarsFilter']")):
        _ = cases.compute_group_kinds("semantic")


def test_the_repository_cases_declare_exactly_the_measured_cases() -> None:
    """The cases that depend on a provisional operator or library value type, measured
    for ITL-1266, ITL-1279 and ITL-1324 (docs/dev/scripts/itl_1279/measure_relocation.py),
    and no other. A split case is listed once: its provisional part."""
    declared = {
        (group, name): names
        for group, registered in cases._CASES.items()
        for name, _, names in registered
        if names
    }
    numpy_pandas = cases.NUMPY_PANDAS_TYPES
    polars = cases.POLARS_TYPES
    torch = (cases.TORCH_TENSOR,)
    assert declared == {
        ("semantic", "polars_expr"): ("PolarsFilter",),
        ("semantic", "value"): numpy_pandas,
        ("semantic", "type"): numpy_pandas,
        ("semantic", "polars_value"): polars,
        ("semantic", "polars_type"): polars,
        ("schema", "single_field"): numpy_pandas,
        ("schema", "polars_single_field"): polars,
        ("arrow", "extension_no_values"): numpy_pandas,
        ("arrow", "polars_extension_no_values"): polars,
        ("datagram", "ndarray"): (cases.NUMPY_NDARRAY,),
        ("datagram", "dataframe"): (cases.PANDAS_DATAFRAME,),
        ("datagram", "structural_with_array"): (cases.NUMPY_NDARRAY,),
        ("datagram", "polars_data_hash"): polars,
        ("datagram", "polars_dataframe"): (cases.POLARS_DATAFRAME,),
        ("datagram", "polars_series"): (cases.POLARS_SERIES,),
        ("pipeline", "polars_filter"): ("PolarsFilter",),
        ("pipeline", "polars_filter_expr"): ("PolarsFilter",),
        ("pipeline", "polars_filter_expr_job"): ("PolarsFilter",),
        ("pipeline", "merge_join"): ("MergeJoin",),
        ("pipeline", "join_key_nullability"): ("MergeJoin",),
        ("pipeline", "batch"): ("Batch",),
        ("pipeline", "group_by"): ("GroupBy",),
        ("pipeline", "index_pick"): ("Index", "Pick"),
        ("pipeline", "map_columns"): ("MapColumns",),
        ("pipeline", "add_tags"): ("AddTags",),
        ("pipeline", "group_by_arrangement"): ("GroupBy",),
        ("pipeline", "group_by_nan_keys"): ("GroupBy",),
        ("torch", "value"): torch,
        ("torch", "type"): torch,
        ("torch", "single_field"): torch,
        ("torch", "datagram"): torch,
    }


def test_the_provisional_library_value_types_are_declarable() -> None:
    """The names a case may declare include each provisional library value type
    (ITL-1324), by extension name, beside the provisional operators."""
    names = cases.declarable_provisional_names()
    assert {
        cases.NUMPY_NDARRAY,
        cases.PANDAS_DATAFRAME,
        cases.PANDAS_SERIES,
        cases.POLARS_DATAFRAME,
        cases.POLARS_SERIES,
        cases.TORCH_TENSOR,
    } <= names
    assert {"PolarsFilter", "GroupBy", "MergeJoin"} <= names


def _hashing(*values: Any) -> Any:
    def fn() -> dict[str, Any]:
        hasher = cases._ctx().semantic_hasher  # pyright: ignore[reportPrivateUsage]
        return {
            f"v{i}": hasher.hash_object(v).to_string() for i, v in enumerate(values)
        }

    return fn


@pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")
@pytest.mark.parametrize(
    ("values", "missing"),
    [
        ((np.arange(3), pd.Series([1.0])), [cases.NUMPY_NDARRAY, cases.PANDAS_SERIES]),
        ((pd.Series([1.0]), np.arange(3)), [cases.NUMPY_NDARRAY, cases.PANDAS_SERIES]),
        ((1, np.arange(3)), [cases.NUMPY_NDARRAY]),
    ],
)
def test_a_case_hashing_an_undeclared_library_value_type_is_refused(
    monkeypatch: pytest.MonkeyPatch, values: tuple[Any, ...], missing: list[str]
) -> None:
    """A direct use of a provisional library value type is recorded on every use, in
    any order, so a frozen case that hashes one without declaring it is refused."""
    monkeypatch.setitem(cases._CASES, "semantic", [("lib", _hashing(*values), ())])
    with pytest.raises(ValueError, match=re.escape(f"uses the provisional {missing}")):
        _ = cases.compute_group_kinds("semantic")


@pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")
def test_a_case_declaring_the_library_value_types_it_hashes_is_provisional(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(
        cases._CASES,
        "semantic",
        [
            ("lib", _hashing(pd.Series([1.0]), np.arange(3)), cases.NUMPY_PANDAS_TYPES),
            ("plain", _hashing(1, "a"), ()),
        ],
    )
    values, provisional = cases.compute_group_kinds("semantic")
    assert set(values) == {"lib.v0", "lib.v1", "plain.v0", "plain.v1"}
    assert provisional == frozenset({"lib.v0", "lib.v1"})
