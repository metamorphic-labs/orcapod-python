"""The release-tag check of the golden files (ITL-946): ``scripts/check_golden_tags.py``.

Every test runs the script as a command against a temporary git repository holding a copy of
the golden tree, released with ``generate_golden_hashes.py --release`` and tagged.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from orcapod.contexts import get_default_context
from tests.test_hashing.golden import store
from tests.test_hashing.golden.generate_golden_hashes import dump, main

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "check_golden_tags.py"
VERSION = get_default_context().version


def _env(*, ci: bool, ceiling: Path | None = None) -> dict[str, str]:
    env = {
        k: v for k, v in os.environ.items() if k != "CI" and not k.startswith("GIT_")
    }
    env.update(
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_CONFIG_NOSYSTEM="1",
        GIT_AUTHOR_NAME="test",
        GIT_AUTHOR_EMAIL="test@example.invalid",
        GIT_COMMITTER_NAME="test",
        GIT_COMMITTER_EMAIL="test@example.invalid",
    )
    if ci:
        env["CI"] = "true"
    if ceiling is not None:
        env["GIT_CEILING_DIRECTORIES"] = str(ceiling)
    return env


def _git(repo: Path, *args: str) -> None:
    _ = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        env=_env(ci=False),
    )


def _commit(repo: Path, message: str) -> None:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "--no-gpg-sign", "-m", message)


def _check(repo: Path, *, ci: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--repo", str(repo)],
        capture_output=True,
        text=True,
        check=False,
        env=_env(ci=ci, ceiling=repo.parent),
    )


def _folder(repo: Path) -> Path:
    return store.golden_folder(
        repo, store.read_json(store.manifest_path(repo, VERSION))
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A repository with an open commit tagged ``v0.2.0`` and a release tagged ``v0.3.0``."""
    root = tmp_path / "repo"
    store.copy_tree(store.PROJECT_ROOT, root, VERSION)
    store.reopen_tree(root, VERSION, "2026-11-15")
    _git(root.parent, "init", "-q", str(root))
    _commit(root, "open data context")
    _git(root, "tag", "v0.2.0")
    main(["--root", str(root), "--release", "0.3.0"])
    _commit(root, "release 0.3.0")
    _git(root, "tag", "v0.3.0")
    return root


def test_head_at_the_release_passes(repo: Path) -> None:
    result = _check(repo, ci=True)
    assert result.returncode == 0, result.stderr
    assert "v0.3.0" in result.stdout


def test_a_tag_that_released_nothing_is_not_checked(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    store.copy_tree(store.PROJECT_ROOT, root, VERSION)
    store.reopen_tree(root, VERSION, "2026-11-15")
    _git(root.parent, "init", "-q", str(root))
    _commit(root, "open data context")
    _git(root, "tag", "v0.2.0")
    result = _check(root, ci=True)
    assert result.returncode == 0, result.stderr
    assert "nothing to check" in result.stdout


def test_a_golden_file_added_after_the_release_passes(repo: Path) -> None:
    folder = _folder(repo)
    name = store.next_file_name(store.group_files(folder)["schema"][-1].name)
    _ = (folder / name).write_text(dump({"new.entry": "x"}))
    manifest = store.read_json(store.manifest_path(repo, VERSION))
    manifest["golden"]["history"] = store.with_file(
        manifest["golden"]["history"],
        name,
        store.file_digest(folder / name),
        "2026-11-20",
    )
    store.write_json(store.manifest_path(repo, VERSION), manifest)
    _commit(repo, f"add {name}")
    assert store.tree_problems(repo, VERSION) == []
    result = _check(repo, ci=True)
    assert result.returncode == 0, result.stderr


def _change_a_provisional_file(repo: Path) -> None:
    """Remove one entry of a provisional file with the tool, as an approved change would."""
    path = store.provisional_folder(_folder(repo)) / "semantic.json"
    entry = sorted(store.read_json(path))[0]
    main(["--root", str(repo), "--remove", entry])
    assert entry not in store.read_json(path)
    _commit(repo, f"remove {entry} from a provisional file")
    assert store.tree_problems(repo, VERSION) == []


def test_a_provisional_file_changed_after_the_release_passes(repo: Path) -> None:
    """A release never freezes a provisional file (ITL-1266): the tag check ignores it."""
    _change_a_provisional_file(repo)
    result = _check(repo, ci=True)
    assert result.returncode == 0, result.stderr


def test_the_gate_passes_a_provisional_change_after_the_release_step(
    repo: Path,
) -> None:
    """A provisional file changed after the release step adds no revision."""
    _change_a_provisional_file(repo)
    result = _gate(repo, "0.3.0")
    assert result.returncode == 0, result.stderr


def test_an_edited_released_file_fails(repo: Path) -> None:
    path = _folder(repo) / "semantic-01.json"
    _ = path.write_text(path.read_text().replace("semantic_v1", "semantic_v2", 1))
    _commit(repo, "edit a released file")
    result = _check(repo)
    assert result.returncode == 1
    assert "semantic-01.json differs from the released file" in result.stderr


def test_a_released_file_rewritten_with_its_history_and_pins_fails(repo: Path) -> None:
    """A rewrite of file, revision and pins inside the tree still differs from the tag."""
    folder = _folder(repo)
    path = folder / "semantic-01.json"
    _ = path.write_text(path.read_text().replace("semantic_v1", "semantic_v2", 1))
    manifest = store.read_json(store.manifest_path(repo, VERSION))
    history = manifest["golden"]["history"]
    history[0]["added"]["semantic-01.json"] = store.file_digest(path)
    for i, revision in enumerate(history):
        revision["previous"] = history[i - 1]["digest"] if i else None
        revision["digest"] = store.revision_digest(revision)
    store.write_json(store.manifest_path(repo, VERSION), manifest)
    pins = store.read_json(store.pins_path(repo))
    pins[VERSION]["releases"]["0.3.0"] = history[-1]["digest"]
    store.write_json(store.pins_path(repo), pins)
    assert store.tree_problems(repo, VERSION) == []
    _commit(repo, "rewrite the release")
    result = _check(repo)
    assert result.returncode == 1
    assert "semantic-01.json differs" in result.stderr
    assert "the releases line 0.3.0" in result.stderr
    assert "history up to release 0.3.0 changed" in result.stderr


def test_a_deleted_released_file_fails(repo: Path) -> None:
    (_folder(repo) / "torch-01.json").unlink()
    _commit(repo, "delete a released file")
    result = _check(repo)
    assert result.returncode == 1
    assert "torch-01.json is missing in HEAD" in result.stderr


def test_a_removed_releases_line_fails(repo: Path) -> None:
    pins = store.read_json(store.pins_path(repo))
    pins[VERSION]["releases"].clear()
    store.write_json(store.pins_path(repo), pins)
    _commit(repo, "drop the releases line")
    result = _check(repo)
    assert result.returncode == 1
    assert "the releases line 0.3.0" in result.stderr


def test_a_changed_manifest_pin_fails(repo: Path) -> None:
    pins = store.read_json(store.pins_path(repo))
    pins[VERSION]["manifest"] = "0" * 64
    store.write_json(store.pins_path(repo), pins)
    _commit(repo, "change the manifest pin")
    result = _check(repo)
    assert result.returncode == 1
    assert "manifest pin" in result.stderr


def test_outside_a_repository_it_skips_locally_and_fails_in_ci(tmp_path: Path) -> None:
    plain = tmp_path / "plain"
    store.copy_tree(store.PROJECT_ROOT, plain, VERSION)
    store.reopen_tree(plain, VERSION, "2026-11-15")
    local = _check(plain)
    assert local.returncode == 0
    assert "skipped" in local.stdout
    ci = _check(plain, ci=True)
    assert ci.returncode == 1
    assert "not a git work tree" in ci.stderr


def test_a_shallow_clone_fails_in_ci(repo: Path, tmp_path: Path) -> None:
    clone = tmp_path / "clone"
    _ = subprocess.run(
        ["git", "clone", "-q", "--depth", "1", f"file://{repo}", str(clone)],
        check=True,
        capture_output=True,
        env=_env(ci=False),
    )
    result = _check(clone, ci=True)
    assert result.returncode == 1
    assert "shallow" in result.stderr
    assert _check(clone).returncode == 0


def test_a_repository_without_tags_fails_in_ci(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    store.copy_tree(store.PROJECT_ROOT, root, VERSION)
    store.reopen_tree(root, VERSION, "2026-11-15")
    _git(root.parent, "init", "-q", str(root))
    _commit(root, "open data context")
    result = _check(root, ci=True)
    assert result.returncode == 1
    assert "no tags" in result.stderr


# ---------------------------------------------------------------------------
# --release-version: the Release workflow's gate before a tag is made
# ---------------------------------------------------------------------------


def _gate(repo: Path, version: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--repo",
            str(repo),
            "--release-version",
            version,
        ],
        capture_output=True,
        text=True,
        check=False,
        env=_env(ci=False, ceiling=repo.parent),
    )


def test_the_gate_passes_a_recorded_release(repo: Path) -> None:
    result = _gate(repo, "0.3.0")
    assert result.returncode == 0, result.stderr
    assert "0.3.0" in result.stdout


def test_the_gate_refuses_a_release_without_its_golden_step(repo: Path) -> None:
    result = _gate(repo, "0.3.1")
    assert result.returncode == 1
    assert "generate_golden_hashes.py --release 0.3.1" in result.stderr


def test_the_gate_refuses_an_open_data_context(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    store.copy_tree(store.PROJECT_ROOT, root, VERSION)
    store.reopen_tree(root, VERSION, "2026-11-15")
    _git(root.parent, "init", "-q", str(root))
    _commit(root, "open data context")
    result = _gate(root, "0.3.0")
    assert result.returncode == 1
    assert "0.3.0" in result.stderr


def test_the_gate_refuses_a_pin_that_is_not_the_release_revision(repo: Path) -> None:
    pins = store.read_json(store.pins_path(repo))
    pins[VERSION]["releases"]["0.3.0"] = "0" * 64
    store.write_json(store.pins_path(repo), pins)
    _commit(repo, "tamper with the pin")
    result = _gate(repo, "0.3.0")
    assert result.returncode == 1
    assert "release revision" in result.stderr


def test_the_gate_fails_outside_a_repository_even_locally(tmp_path: Path) -> None:
    plain = tmp_path / "plain"
    store.copy_tree(store.PROJECT_ROOT, plain, VERSION)
    result = _gate(plain, "0.3.0")
    assert result.returncode == 1


def test_the_gate_refuses_a_golden_addition_after_the_release_step(repo: Path) -> None:
    """The tagged commit's last revision is the release: no unreleased file ships with it."""
    folder = _folder(repo)
    name = store.next_file_name(store.group_files(folder)["schema"][-1].name)
    _ = (folder / name).write_text(dump({"new.entry": "x"}))
    manifest = store.read_json(store.manifest_path(repo, VERSION))
    manifest["golden"]["history"] = store.with_file(
        manifest["golden"]["history"],
        name,
        store.file_digest(folder / name),
        "2026-11-20",
    )
    store.write_json(store.manifest_path(repo, VERSION), manifest)
    _commit(repo, f"add {name} after the release step")
    result = _gate(repo, "0.3.0")
    assert result.returncode == 1
    assert "last revision" in result.stderr


def test_inherited_git_location_variables_do_not_redirect_the_check(
    repo: Path, tmp_path: Path
) -> None:
    """``--repo`` is the repository read, whatever ``GIT_DIR`` and friends point at."""
    other = tmp_path / "other"
    _ = subprocess.run(
        ["git", "clone", "-q", str(repo), str(other)],
        check=True,
        capture_output=True,
        env=_env(ci=False),
    )
    path = _folder(other) / "semantic-01.json"
    _ = path.write_text(path.read_text().replace("semantic_v1", "semantic_v2", 1))
    _commit(other, "edit a released file")
    _git(other, "fetch", "-q", "--tags")
    assert _check(other).returncode == 1
    env = _env(ci=True, ceiling=tmp_path)
    env.update(
        GIT_DIR=str(repo / ".git"),
        GIT_WORK_TREE=str(other),
        GIT_COMMON_DIR=str(repo / ".git"),
        GIT_OBJECT_DIRECTORY=str(repo / ".git" / "objects"),
    )
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--repo", str(other)],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert result.returncode == 1, result.stdout
    assert "semantic-01.json differs" in result.stderr


def test_tags_of_earlier_data_contexts_pass_with_nothing_to_check(
    tmp_path: Path,
) -> None:
    """The Release workflow's first release of a context: older tags only, none of it.

    As on the public repository before v0.3.0: tags whose commits have no
    ``released_manifests.json`` (v0.1.x) or the old ``{"v1": null}`` form (v0.2.0). In CI
    the check passes with nothing to check; it never fails for want of a release tag.
    """
    root = tmp_path / "repo"
    pins = root / store.PINS_FILE
    pins.parent.mkdir(parents=True)
    _ = (root / "README.md").write_text("old release\n")
    _git(root.parent, "init", "-q", str(root))
    _commit(root, "v0.1.x: no released_manifests.json")
    _git(root, "tag", "v0.1.4")
    _ = pins.write_text('{\n    "v1": null\n}\n')
    _commit(root, "v0.2.0: the old pin form")
    _git(root, "tag", "v0.2.0")
    store.copy_tree(store.PROJECT_ROOT, root, VERSION)
    store.reopen_tree(root, VERSION, "2026-11-15")
    _commit(root, "the open v1 data context")
    result = _check(root, ci=True)
    assert result.returncode == 0, result.stderr
    assert "nothing to check" in result.stdout


# The data function type snapshots (ITL-1259): a released one never changes.

SNAPSHOT = Path("src/orcapod/data_function_types/python.function/v1-rev1.json")
PINS = Path("src/orcapod/data_function_types/released.json")


def _release_a_snapshot(repo: Path) -> None:
    """Pin a snapshot as released in 9.9.9, commit and tag it ``v9.9.9``."""
    import hashlib
    import json

    snapshot = repo / SNAPSHOT
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    _ = snapshot.write_text('{"revision": 1}\n')
    digest = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    pins = {
        SNAPSHOT.relative_to(PINS.parent).as_posix(): {
            "released_in": "9.9.9",
            "sha256": digest,
        }
    }
    _ = (repo / PINS).write_text(json.dumps(pins, indent=2, sort_keys=True) + "\n")
    _commit(repo, "release a snapshot")
    _git(repo, "tag", "v9.9.9")


def test_a_released_type_snapshot_kept_passes(repo: Path) -> None:
    _release_a_snapshot(repo)
    result = _check(repo, ci=True)
    assert result.returncode == 0, result.stderr


def test_a_released_type_snapshot_rewritten_with_its_pin_fails(repo: Path) -> None:
    import hashlib
    import json

    _release_a_snapshot(repo)
    snapshot = repo / SNAPSHOT
    _ = snapshot.write_text('{"revision": 1, "changed": true}\n')
    pins = json.loads((repo / PINS).read_text())
    key = SNAPSHOT.relative_to(PINS.parent).as_posix()
    pins[key]["sha256"] = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    _ = (repo / PINS).write_text(json.dumps(pins, indent=2, sort_keys=True) + "\n")
    _commit(repo, "rewrite a released snapshot and its pin")
    result = _check(repo, ci=True)
    assert result.returncode == 1
    assert "python.function/v1-rev1.json" in result.stderr
    assert "never changes" in result.stderr


def test_a_removed_type_snapshot_pin_fails(repo: Path) -> None:
    _release_a_snapshot(repo)
    _ = (repo / PINS).write_text("{}\n")
    _commit(repo, "drop a released pin")
    result = _check(repo, ci=True)
    assert result.returncode == 1
    assert "pin of data function type snapshot" in result.stderr


def test_the_gate_refuses_a_type_snapshot_added_after_the_release_step(
    repo: Path,
) -> None:
    """An open (unpinned) snapshot is never tagged (Copilot, #448)."""
    import json

    snapshot = repo / SNAPSHOT
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    _ = snapshot.write_text('{"revision": 2}\n')
    key = SNAPSHOT.relative_to(PINS.parent).as_posix()
    _ = (repo / PINS).write_text(
        json.dumps({key: {"released_in": None, "sha256": None}}, indent=2) + "\n"
    )
    _commit(repo, "add a snapshot after the release step")
    result = _gate(repo, "0.3.0")
    assert result.returncode == 1
    assert "python.function/v1-rev1.json is not pinned" in result.stderr


def test_the_gate_passes_a_pinned_type_snapshot(repo: Path) -> None:
    import hashlib
    import json

    snapshot = repo / SNAPSHOT
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    _ = snapshot.write_text('{"revision": 1}\n')
    key = SNAPSHOT.relative_to(PINS.parent).as_posix()
    digest = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    _ = (repo / PINS).write_text(
        json.dumps({key: {"released_in": "0.3.0", "sha256": digest}}, indent=2) + "\n"
    )
    _commit(repo, "a pinned snapshot")
    result = _gate(repo, "0.3.0")
    assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# A withdrawn release (ITL-1255)
# ---------------------------------------------------------------------------


def _withdraw_and_change_a_released_file(repo: Path) -> None:
    """Withdraw 0.3.0 at HEAD and change a file 0.3.0 listed, as v1 now may."""
    store.withdraw_release(repo, VERSION, "0.3.0", "2026-11-17", "Withdrawn.")
    path = _folder(repo) / "semantic-01.json"
    _ = path.write_text(path.read_text().replace("semantic_v1", "semantic_v2", 1))
    manifest = store.read_json(store.manifest_path(repo, VERSION))
    manifest["golden"]["history"] = store.with_file(
        manifest["golden"]["history"], path.name, store.file_digest(path), "2026-11-18"
    )
    store.write_json(store.manifest_path(repo, VERSION), manifest)
    assert store.tree_problems(repo, VERSION) == []
    _commit(repo, "withdraw 0.3.0 and change a value it listed")


def test_a_withdrawn_release_passes_with_its_tag_and_after_it_is_deleted(
    repo: Path,
) -> None:
    """The order of the withdrawal and the tag deletion does not matter."""
    _withdraw_and_change_a_released_file(repo)
    with_tag = _check(repo, ci=True)
    assert with_tag.returncode == 0, with_tag.stderr
    assert "v0.3.0: withdrawn on 2026-11-17" in with_tag.stdout
    _git(repo, "tag", "-d", "v0.3.0")
    without_tag = _check(repo, ci=True)
    assert without_tag.returncode == 0, without_tag.stderr
    assert "v0.3.0" not in without_tag.stdout


@pytest.mark.parametrize(
    ("field", "problem"),
    [
        ("revision", "does not name the release revision the tag pinned"),
        ("manifest", "does not name the manifest pin the tag shipped"),
    ],
)
def test_a_withdrawn_record_that_does_not_match_its_tag_fails(
    repo: Path, field: str, problem: str
) -> None:
    _withdraw_and_change_a_released_file(repo)
    pins = store.read_json(store.pins_path(repo))
    pins[VERSION]["withdrawn"]["0.3.0"][field] = "0" * 64
    store.write_json(store.pins_path(repo), pins)
    _commit(repo, "tamper with the withdrawn record")
    result = _check(repo)
    assert result.returncode == 1
    assert f"v0.3.0: the withdrawn record of 0.3.0 {problem}" in result.stderr


def test_a_release_dropped_without_a_withdrawn_record_still_fails(repo: Path) -> None:
    _withdraw_and_change_a_released_file(repo)
    pins = store.read_json(store.pins_path(repo))
    del pins[VERSION]["withdrawn"]
    store.write_json(store.pins_path(repo), pins)
    _commit(repo, "drop the release without a record")
    result = _check(repo)
    assert result.returncode == 1
    assert "the releases line 0.3.0" in result.stderr
    assert "semantic-01.json differs from the released file" in result.stderr


def test_a_record_of_another_release_does_not_excuse_a_release(repo: Path) -> None:
    _withdraw_and_change_a_released_file(repo)
    pins = store.read_json(store.pins_path(repo))
    withdrawn = pins[VERSION]["withdrawn"]
    withdrawn["0.2.9"] = withdrawn.pop("0.3.0")
    store.write_json(store.pins_path(repo), pins)
    _commit(repo, "record the wrong release")
    result = _check(repo)
    assert result.returncode == 1
    assert "the releases line 0.3.0" in result.stderr


def test_the_release_after_a_withdrawn_one_is_checked(repo: Path) -> None:
    """The freeze at 0.4.0: its tag is compared with HEAD; the withdrawn tag is not."""
    _withdraw_and_change_a_released_file(repo)
    main(["--root", str(repo), "--release", "0.4.0"])
    _commit(repo, "release 0.4.0")
    _git(repo, "tag", "v0.4.0")
    assert _gate(repo, "0.4.0").returncode == 0
    passing = _check(repo, ci=True)
    assert passing.returncode == 0, passing.stderr
    assert "HEAD keeps what v0.4.0 released" in passing.stdout
    path = _folder(repo) / "semantic-01.json"
    _ = path.write_text(path.read_text().replace("semantic_v2", "semantic_v3", 1))
    _commit(repo, "edit a file 0.4.0 released")
    failing = _check(repo)
    assert failing.returncode == 1
    assert "v0.4.0: tests/test_hashing/golden/data" in failing.stderr


def test_the_gate_refuses_a_withdrawn_version(repo: Path) -> None:
    _withdraw_and_change_a_released_file(repo)
    result = _gate(repo, "0.3.0")
    assert result.returncode == 1
    assert "was withdrawn and is never released again" in result.stderr


def _gate_module() -> Any:
    """The script as a module, for the cases a command cannot reach."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("check_golden_tags_script", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_gate_reads_a_snapshot_name_holding_a_space(repo: Path) -> None:
    """Paths come NUL-separated from git: a space stays in its name (CodeRabbit, #448)."""
    import json

    snapshot = repo / "src/orcapod/data_function_types/python.function/v1 rev2.json"
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    _ = snapshot.write_text('{"revision": 2}\n')
    _ = (repo / PINS).write_text(json.dumps({}) + "\n")
    _commit(repo, "a snapshot named with a space")
    result = _gate(repo, "0.3.0")
    assert result.returncode == 1
    assert "python.function/v1 rev2.json is not pinned" in result.stderr


def test_the_gate_reports_a_snapshot_listing_git_cannot_read(tmp_path: Path) -> None:
    """A failing listing is a problem, never a pass (CodeRabbit, #448)."""
    module = _gate_module()
    problems = module.unpinned_type_snapshots(tmp_path)  # not a repository
    assert len(problems) == 1 and "git cannot list" in problems[0]


def test_the_gate_reports_a_snapshot_git_cannot_read(
    repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import hashlib
    import json

    snapshot = repo / SNAPSHOT
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    _ = snapshot.write_text('{"revision": 1}\n')
    key = SNAPSHOT.relative_to(PINS.parent).as_posix()
    digest = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    _ = (repo / PINS).write_text(
        json.dumps({key: {"released_in": "0.3.0", "sha256": digest}}) + "\n"
    )
    _commit(repo, "a pinned snapshot")
    module = _gate_module()
    real_show = module._show

    def show(repo_: Path, rev: str, path: Any) -> bytes | None:
        return (
            None if str(path).endswith("v1-rev1.json") else real_show(repo_, rev, path)
        )

    monkeypatch.setattr(module, "_show", show)
    problems = module.unpinned_type_snapshots(repo)
    assert problems == [f"git cannot read data function type snapshot {key}"]


# ---------------------------------------------------------------------------
# Pre-releases freeze nothing (ITL-1329)
# ---------------------------------------------------------------------------

# (text, normal form, pre-release) for every spelling the gate accepts.
VERSIONS = [
    ("0.4.0", "0.4.0", False),
    ("v0.4.0", "0.4.0", False),
    ("V0.4.0", "0.4.0", False),
    ("00.04.00", "0.4.0", False),
    ("0.4.0.post1", "0.4.0.post1", False),
    ("0.4.0-1", "0.4.0.post1", False),
    ("0.4.0.rev2", "0.4.0.post2", False),
    ("0.4.0rc1", "0.4.0rc1", True),
    ("v0.4.0rc1", "0.4.0rc1", True),
    ("0.4.0RC1", "0.4.0rc1", True),
    ("0.4.0-rc.1", "0.4.0rc1", True),
    ("0.4.0_rc_1", "0.4.0rc1", True),
    ("0.4.0.rc01", "0.4.0rc1", True),
    ("0.4.0c1", "0.4.0rc1", True),
    ("0.4.0pre2", "0.4.0rc2", True),
    ("0.4.0preview3", "0.4.0rc3", True),
    ("0.4.0rc", "0.4.0rc0", True),
    ("0.4.0a1", "0.4.0a1", True),
    ("0.4.0alpha1", "0.4.0a1", True),
    ("0.4.0b2", "0.4.0b2", True),
    ("0.4.0beta2", "0.4.0b2", True),
    ("0.4.0.dev3", "0.4.0.dev3", True),
    ("0.4.0dev3", "0.4.0.dev3", True),
    ("0.4.0-dev", "0.4.0.dev0", True),
    ("0.4.0rc1.dev2", "0.4.0rc1.dev2", True),
    ("0.4.0.post1.dev1", "0.4.0.post1.dev1", True),
]
# Texts the gate refuses: valid PEP 440 it never releases (an epoch, a local part, a
# release that is not three numbers), and texts that are no version at all.
NOT_RELEASE_VERSIONS = [
    "1!0.4.0",
    "0.4.0+local",
    "0.4",
    "0.4.0.1",
    "0.4.0rc1x",
    "latest",
    "",
    "v",
    "0.4.0-",
    "0.4.0 rc1",
]


@pytest.mark.parametrize(("text", "normal", "prerelease"), VERSIONS)
def test_release_version_tells_a_pre_release_from_a_final_one(
    text: str, normal: str, prerelease: bool
) -> None:
    """The one rule agrees with PEP 440 (``packaging``) on every spelling it accepts."""
    from packaging.version import Version

    module = _gate_module()
    assert module.release_version(text) == (normal, prerelease)
    reference = Version(text)
    assert (str(reference), reference.is_prerelease) == (normal, prerelease)


@pytest.mark.parametrize("text", NOT_RELEASE_VERSIONS)
def test_release_version_refuses_what_it_does_not_release(text: str) -> None:
    """A version the project never releases is refused, never guessed."""
    module = _gate_module()
    with pytest.raises(ValueError, match="is not a release version"):
        module.release_version(text)


def _classify(version: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--classify-version", version],
        capture_output=True,
        text=True,
        check=False,
        env=_env(ci=False),
    )


@pytest.mark.parametrize(
    ("version", "prerelease"),
    [
        ("0.4.0", "false"),
        ("0.4.0rc1", "true"),
        ("v0.4.0b2", "true"),
        ("0.4.0.dev3", "true"),
    ],
)
def test_classify_writes_the_step_outputs(version: str, prerelease: str) -> None:
    """The Release workflow's outputs, one ``name=value`` line each."""
    result = _classify(version)
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        f"version={version.removeprefix('v')}",
        f"prerelease={prerelease}",
    ]


@pytest.mark.parametrize(
    ("version", "normal"), [("0.4.0-rc.1", "0.4.0rc1"), ("0.4.0RC1", "0.4.0rc1")]
)
def test_classify_refuses_a_spelling_that_is_not_the_normal_form(
    version: str, normal: str
) -> None:
    """The tag and the package must name one version: the normal form is named."""
    result = _classify(version)
    assert result.returncode == 1
    assert result.stdout == ""
    assert f"give it as '{normal}'" in result.stderr


def test_classify_refuses_what_is_not_a_release_version() -> None:
    result = _classify("0.4.0+local")
    assert result.returncode == 1
    assert "is not a release version" in result.stderr


@pytest.mark.parametrize("candidate", ["0.4.0rc1", "0.4.0a1", "0.4.0b2", "0.4.0.dev3"])
def test_the_gate_passes_a_pre_release_with_no_record(
    repo: Path, candidate: str
) -> None:
    """A pre-release needs no release step: the gate passes when HEAD does not record
    it, and says that it freezes nothing."""
    result = _gate(repo, candidate)
    assert result.returncode == 0, result.stderr
    assert f"{candidate} is a pre-release: it freezes nothing" in result.stdout
    assert "unpinned" in result.stdout


@pytest.mark.parametrize("recorded", ["0.4.0rc1", "0.4.0-rc.1"])
def test_the_gate_refuses_a_pre_release_that_head_records(
    repo: Path, recorded: str
) -> None:
    """A pre-release recorded as a release (in any spelling) is refused: a pre-release
    never runs the release step."""
    pins = store.read_json(store.pins_path(repo))
    pins[VERSION]["releases"][recorded] = "1" * 64
    store.write_json(store.pins_path(repo), pins)
    _commit(repo, "record a pre-release as a release")
    result = _gate(repo, "0.4.0rc1")
    assert result.returncode == 1
    assert "pre-release 0.4.0rc1 cannot be tagged" in result.stderr
    assert "a pre-release never runs the golden release step" in result.stderr


def test_the_gate_refuses_a_withdrawn_pre_release(repo: Path) -> None:
    pins = store.read_json(store.pins_path(repo))
    pins[VERSION].setdefault("withdrawn", {})["0.4.0rc1"] = {
        "date": "2026-11-21",
        "reason": "test",
        "revision": "2" * 64,
        "manifest": "3" * 64,
    }
    store.write_json(store.pins_path(repo), pins)
    _commit(repo, "withdraw a pre-release")
    result = _gate(repo, "0.4.0rc1")
    assert result.returncode == 1
    assert "was withdrawn and is never released again" in result.stderr


def test_a_final_version_without_its_release_step_still_fails(repo: Path) -> None:
    """A final version behaves as before: the release step is required."""
    result = _gate(repo, "0.4.0")
    assert result.returncode == 1
    assert "generate_golden_hashes.py --release 0.4.0" in result.stderr
    post = _gate(repo, "0.3.0.post1")
    assert post.returncode == 1
    assert "generate_golden_hashes.py --release 0.3.0.post1" in post.stderr


def test_the_gate_refuses_a_version_it_does_not_release(repo: Path) -> None:
    result = _gate(repo, "0.4.0+local")
    assert result.returncode == 1
    assert "is not a release version" in result.stderr


def test_a_pre_release_allows_an_unpinned_type_snapshot(repo: Path) -> None:
    """Snapshots are pinned by the final release step; a pre-release may ship them open."""
    import json

    snapshot = repo / SNAPSHOT
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    _ = snapshot.write_text('{"revision": 2}\n')
    key = SNAPSHOT.relative_to(PINS.parent).as_posix()
    _ = (repo / PINS).write_text(
        json.dumps({key: {"released_in": None, "sha256": None}}, indent=2) + "\n"
    )
    _commit(repo, "add a snapshot after the release step")
    assert _gate(repo, "0.3.0").returncode == 1
    result = _gate(repo, "0.4.0rc1")
    assert result.returncode == 0, result.stderr


def test_a_pre_release_run_still_fails_on_a_changed_released_file(repo: Path) -> None:
    """The Release workflow runs the gate and then the tag check for every version: a
    pre-release passes the gate, and the tag check still catches HEAD changing a file
    that the final release 0.3.0 froze."""
    released = next(_folder(repo).glob("*.json"))
    _ = released.write_text(released.read_text().replace("{", "{ ", 1))
    _commit(repo, "edit a released golden file")
    assert _gate(repo, "0.4.0rc1").returncode == 0
    result = _check(repo, ci=True)
    assert result.returncode == 1
    assert "differs from the released file" in result.stderr


def test_a_pre_release_tag_is_not_a_release(repo: Path) -> None:
    """A candidate's tag pins no release, so the tag check skips it and still checks
    the final release before it."""
    (repo / "README.candidate").write_text("a commit after 0.3.0\n")
    _commit(repo, "work after the release")
    assert _gate(repo, "0.4.0rc1").returncode == 0
    _git(repo, "tag", "v0.4.0rc1")
    module = _gate_module()
    assert module.tag_release(repo, "v0.4.0rc1", VERSION) is None
    assert module.check_tag(repo, "v0.4.0rc1", VERSION) is None
    result = _check(repo, ci=True)
    assert result.returncode == 0, result.stderr
    assert "HEAD keeps what v0.3.0 released" in result.stdout
    assert "v0.4.0rc1: a pre-release, which freezes nothing" in result.stdout


def test_the_generator_refuses_to_release_a_pre_release(repo: Path) -> None:
    """``--release`` of a pre-release freezes nothing: it refuses and writes nothing."""
    pins_before = store.pins_path(repo).read_bytes()
    manifest = store.manifest_path(repo, VERSION)
    manifest_before = manifest.read_bytes()
    for candidate in ("0.4.0rc1", "0.4.0.dev1", "v0.4.0b1"):
        with pytest.raises(SystemExit, match="is a pre-release"):
            main(["--root", str(repo), "--release", candidate])
    assert store.pins_path(repo).read_bytes() == pins_before
    assert manifest.read_bytes() == manifest_before


# ---------------------------------------------------------------------------
# The Release workflow reads the rule (ITL-1329)
# ---------------------------------------------------------------------------

WORKFLOW = REPO_ROOT / ".github" / "workflows" / "release.yml"


def _classify_step(build: str) -> tuple[str, str]:
    """The text of the build job's classify step, and the text of the steps after it."""
    start = build.rindex("      - name:", 0, build.index("id: classify"))
    end = build.index("\n      - ", start + 1)
    return build[start:end], build[end:]


def _job(name: str) -> str:
    """The text of one job of the Release workflow (up to the next job)."""
    text = WORKFLOW.read_text()
    start = text.index(f"\n  {name}:\n")
    following = [
        text.find(f"\n  {other}:\n", start + 1)
        for other in (
            "test",
            "license-check",
            "build",
            "publish-testpypi",
            "publish-pypi",
            "linear-sync",
            "linear-complete",
        )
    ]
    end = min((i for i in following if i > start), default=len(text))
    return text[start:end]


def test_the_workflow_classifies_the_version_with_the_script() -> None:
    """The build job's ``prerelease`` output comes from the script, before the gate."""
    build = _job("build")
    assert "prerelease: ${{ steps.classify.outputs.prerelease }}" in build
    step, later = _classify_step(build)
    assert "--classify-version" in step
    assert "--release-version" in later
    # The tag check runs for every version, a pre-release included.
    assert "run: python3 scripts/check_golden_tags.py\n" in build


def test_the_workflow_marks_a_pre_release_and_skips_linear() -> None:
    publish = _job("publish-pypi")
    assert "prerelease: ${{ needs.build.outputs.prerelease == 'true' }}" in publish
    assert "freezes nothing" in publish
    for name in ("linear-sync", "linear-complete"):
        assert "needs.build.outputs.prerelease == 'false'" in _job(name), name


# ---------------------------------------------------------------------------
# No release record at HEAD names a pre-release (Codex review of PR 470)
# ---------------------------------------------------------------------------


def _pin_snapshot(repo: Path, released_in: str | None, content: str) -> str:
    """Write the snapshot ``content`` and pin it as released in ``released_in``."""
    import hashlib
    import json

    snapshot = repo / SNAPSHOT
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    _ = snapshot.write_text(content)
    key = SNAPSHOT.relative_to(PINS.parent).as_posix()
    digest = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    pin = {"released_in": released_in, "sha256": digest if released_in else None}
    _ = (repo / PINS).write_text(json.dumps({key: pin}, indent=2) + "\n")
    return key


@pytest.mark.parametrize("spelling", ["0.4.0rc1", "0.4.0-rc.1"])
def test_a_pre_release_in_the_golden_history_only_is_refused(
    repo: Path, spelling: str
) -> None:
    """A release revision of the candidate in the golden history, with no pin, is a
    release record: the candidate gate refuses it, so the workflow stops before the tag
    check and the tag."""
    path = store.manifest_path(repo, VERSION)
    manifest = store.read_json(path)
    manifest["golden"]["history"] = store.with_release(
        manifest["golden"]["history"], spelling, "2026-11-21"
    )
    store.write_json(path, manifest)
    _commit(repo, "a release revision of a candidate, without its pin")
    result = _gate(repo, "0.4.0rc1")
    assert result.returncode == 1
    assert f"has a release revision of pre-release {spelling}" in result.stderr


@pytest.mark.parametrize("spelling", ["0.4.0rc1", "0.4.0-rc.1", "0.3.1b1"])
def test_a_snapshot_pinned_in_a_pre_release_is_refused(
    repo: Path, spelling: str
) -> None:
    """A snapshot pin naming any pre-release is refused at the candidate gate: a
    pre-release pins nothing."""
    key = _pin_snapshot(repo, spelling, '{"value": 1}\n')
    _commit(repo, "a snapshot pinned by a candidate")
    result = _gate(repo, "0.4.0rc1")
    assert result.returncode == 1
    assert f"snapshot {key} is pinned as released in pre-release {spelling}" in (
        result.stderr
    )


def test_a_candidate_tag_never_freezes_a_snapshot(repo: Path) -> None:
    """The reviewer's sequence: a candidate that pinned a snapshot is refused at the
    gate; and were its tag to exist anyway, a later change of that snapshot before the
    final release is not blocked by it."""
    _ = _pin_snapshot(repo, "0.4.0rc1", '{"value": 1}\n')
    _commit(repo, "a candidate pins a snapshot")
    assert _gate(repo, "0.4.0rc1").returncode == 1
    _git(repo, "tag", "v0.4.0rc1")
    module = _gate_module()
    _ = _pin_snapshot(repo, None, '{"value": 2}\n')
    _commit(repo, "the snapshot changes before the final release")
    assert module.check_type_snapshots(repo, "v0.4.0rc1") == []
    result = _check(repo, ci=True)
    assert result.returncode == 0, result.stderr
    assert "v0.4.0rc1: a pre-release, which freezes nothing" in result.stdout


def test_a_pre_release_tag_is_never_a_release_even_with_a_record(repo: Path) -> None:
    """A candidate tag whose commit records it (a malformed state) still protects
    nothing: the tag check classifies the tag by its name."""
    pins = store.read_json(store.pins_path(repo))
    pins[VERSION]["releases"]["0.4.0rc1"] = "1" * 64
    store.write_json(store.pins_path(repo), pins)
    _commit(repo, "a candidate recorded as a release")
    _git(repo, "tag", "v0.4.0rc1")
    module = _gate_module()
    assert module.tag_release(repo, "v0.4.0rc1", VERSION) is None
    assert module.check_tag(repo, "v0.4.0rc1", VERSION) is None


def test_final_tags_keep_protecting_beside_a_later_candidate(repo: Path) -> None:
    """In a run for a later candidate, every final tag protects as before: a golden file
    0.3.0 froze and a snapshot 9.9.9 pinned are both caught; the candidate tag is
    skipped."""
    _release_a_snapshot(repo)  # final v9.9.9 pins a snapshot
    (repo / "README.candidate").write_text("after 9.9.9\n")
    _commit(repo, "work after the release")
    _git(repo, "tag", "v10.0.0rc1")
    assert _check(repo, ci=True).returncode == 0
    snapshot = repo / SNAPSHOT
    _ = snapshot.write_text('{"revision": 99}\n')
    released = _folder(repo) / "semantic-01.json"
    _ = released.write_text(
        released.read_text().replace("semantic_v1", "semantic_v2", 1)
    )
    _commit(repo, "change what final releases froze")
    assert _gate(repo, "10.0.0rc2").returncode == 0
    result = _check(repo, ci=True)
    assert result.returncode == 1
    assert "v9.9.9: data function type snapshot" in result.stderr
    assert "semantic-01.json differs from the released file" in result.stderr
    assert "v10.0.0rc1: a pre-release, which freezes nothing" in result.stdout


def test_the_workflow_normalizes_the_version_once() -> None:
    """The classify step is the one normalization: it reads the raw input, and every
    later use of the version is its output (a double ``v`` cannot reach a tag)."""
    build = _job("build")
    assert "steps.normalize" not in WORKFLOW.read_text()
    assert "id: normalize" not in build
    step, later = _classify_step(build)
    assert build.count("inputs.version") == 1
    assert "VERSION: ${{ inputs.version }}" in step
    uses = re.findall(r"\$\{\{ ([a-z_.]+)\.version \}\}", later)
    assert uses and set(uses) == {"steps.classify.outputs"}, uses
    assert "version: ${{ steps.classify.outputs.version }}" in build
    assert 'git tag "v${{ steps.classify.outputs.version }}"' in build
    assert 'git push origin "v${{ steps.classify.outputs.version }}"' in build
    for name in ("publish-pypi", "linear-sync", "linear-complete"):
        assert '"v${{ needs.build.outputs.version }}"' in _job(name), name


@pytest.mark.parametrize(
    "version", ["vv0.4.0rc1", "V0.4.0rc1", " 0.4.0rc1", "0.4.0rc1\n"]
)
def test_classify_refuses_a_raw_input_that_is_not_the_normal_form(version: str) -> None:
    """The workflow hands the raw input to the classifier: anything but the normal form
    with at most one leading lowercase ``v`` is refused."""
    result = _classify(version)
    assert result.returncode == 1
    assert result.stdout == ""


def _set_released_in(repo: Path, value: str | None) -> None:
    path = store.manifest_path(repo, VERSION)
    manifest = store.read_json(path)
    manifest["released_in"] = value
    store.write_json(path, manifest)


@pytest.mark.parametrize("spelling", ["0.4.0rc1", "0.4.0-rc.1"])
def test_a_pre_release_in_the_manifest_released_in_only_is_refused(
    repo: Path, spelling: str
) -> None:
    """The manifest's own ``released_in`` is a release record: a pre-release there,
    with no other record, is refused (CodeRabbit, #470)."""
    _set_released_in(repo, spelling)
    _commit(repo, "released_in names a candidate")
    result = _gate(repo, "0.4.0rc1")
    assert result.returncode == 1
    assert f"names pre-release {spelling} as the release that froze it" in result.stderr


@pytest.mark.parametrize("value", ["0.3.1.post1", None])
def test_a_final_or_null_manifest_released_in_passes(
    repo: Path, value: str | None
) -> None:
    """A final version or null in ``released_in`` is no pre-release record: the
    candidate gate passes (the fixture's own 0.3.0 passes in every other test)."""
    manifest = store.read_json(store.manifest_path(repo, VERSION))
    assert manifest["released_in"] == "0.3.0"
    assert _gate(repo, "0.4.0rc1").returncode == 0
    _set_released_in(repo, value)
    _commit(repo, "released_in final or null")
    result = _gate(repo, "0.4.0rc1")
    assert result.returncode == 0, result.stderr


def test_a_pre_release_in_another_data_context_s_releases_is_refused(
    repo: Path,
) -> None:
    """Every data context's ``releases`` pins are release records, not only the
    current one's."""
    pins = store.read_json(store.pins_path(repo))
    pins["v0"] = {"manifest": None, "releases": {"0.2.0b1": "4" * 64}}
    store.write_json(store.pins_path(repo), pins)
    _commit(repo, "another context records a candidate")
    result = _gate(repo, "0.4.0rc1")
    assert result.returncode == 1
    assert "pre-release 0.2.0b1 as a release of data context v0" in result.stderr
