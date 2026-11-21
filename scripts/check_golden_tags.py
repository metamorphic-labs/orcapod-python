"""Check HEAD against every release tag of the current data context (ITL-946).

A golden file that a release listed never changes, and the pins of a release never change.
The tests in the tree check the golden files against the manifest's history and the history
against ``released_manifests.json``; this check anchors all three to the release tags,
which no pull request can move. For each tag ``v<X.Y.Z>`` whose
``src/orcapod/contexts/released_manifests.json`` pins release ``X.Y.Z`` of the current data
context, it checks in HEAD:

- every golden file listed by the revisions up to that release exists with the bytes it has
  at the tag;
- the revisions up to that release are present unchanged in the golden history;
- the pins of the current data context at the tag (the manifest pin and every ``releases``
  line) are present unchanged.

A pre-release (``0.4.0rc1``, ``0.4.0a1``, ``0.4.0b2``, ``0.4.0.dev3``) freezes nothing
(developer, November 21, 2026, ITL-1329): it carries the hashes intended to be final, and
only a final release runs the golden release step. ``--release-version`` of a pre-release
therefore checks that HEAD does NOT record it as a release; its tag pins no release, so the
tag check skips it. ``release_version`` is the one rule that tells the two apart.

A withdrawn release is not compared with HEAD (ITL-1255): when HEAD's
``released_manifests.json`` records release ``X.Y.Z`` under ``withdrawn`` of the current data
context, the tag ``v<X.Y.Z>`` is checked against that record instead: the record's
``revision`` and ``manifest`` must be the release digest and the manifest pin the tag
shipped. So the check passes the same way whether the tag still exists or was deleted, and
a record never excuses a release it does not describe.

The provisional golden files (``golden.provisional``, ITL-1266) are not compared: no
revision lists them, so no release freezes them.

The current data context is the one manifest in ``src/orcapod/contexts/data/`` at HEAD.

Usage:

    uv run python scripts/check_golden_tags.py [--repo DIR]
    uv run python scripts/check_golden_tags.py --release-version X.Y.Z
    uv run python scripts/check_golden_tags.py --classify-version X.Y.Z

It exits 1 on a difference. Without git, history or tags (not a repository, a shallow
clone, no tags) it exits 1 in CI (``CI=true``) and 0 with a message elsewhere. The script
uses the standard library only.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path, PurePosixPath
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
CONTEXTS = PurePosixPath("src/orcapod/contexts")
PINS = CONTEXTS / "released_manifests.json"
GOLDEN_ROOT = PurePosixPath("tests/test_hashing/golden/data")


class GitUnavailableError(RuntimeError):
    """git, the history or the tags cannot be read."""


# A release version in the PEP 440 spellings this project uses: a release of exactly three
# numbers (``X.Y.Z``), an optional pre-release, post-release and development part, in any of
# the spellings PEP 440 normalizes (a leading ``v``, any case, ``-``/``_``/``.`` separators,
# ``alpha``/``beta``/``c``/``pre``/``preview``, an implicit number). No epoch, no local part:
# a version this project never publishes is refused, never guessed.
_RELEASE_VERSION = re.compile(
    r"""
    ^\s*v?
    (?P<release>[0-9]+\.[0-9]+\.[0-9]+)
    (?:[-_.]?(?P<pre_l>alpha|beta|preview|pre|rc|a|b|c)[-_.]?(?P<pre_n>[0-9]+)?)?
    (?:-(?P<post_n1>[0-9]+)|[-_.]?(?P<post_l>post|rev|r)[-_.]?(?P<post_n2>[0-9]+)?)?
    (?:[-_.]?(?P<dev_l>dev)[-_.]?(?P<dev_n>[0-9]+)?)?
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)
_PRE_RELEASE_LETTERS = {
    "a": "a",
    "alpha": "a",
    "b": "b",
    "beta": "b",
    "c": "rc",
    "rc": "rc",
    "pre": "rc",
    "preview": "rc",
}


def release_version(text: str) -> tuple[str, bool]:
    """The normalized form of a release version, and whether it is a pre-release.

    The one rule for telling a final release from a pre-release (ITL-1329). A final
    release is ``X.Y.Z`` or ``X.Y.Z.postN``; a pre-release has an alpha, beta or release
    candidate part (``0.4.0a1``, ``0.4.0b2``, ``0.4.0rc1``) or a development part
    (``0.4.0.dev3``), as PEP 440 defines one. The spellings PEP 440 normalizes are
    accepted (``v0.4.0``, ``0.4.0RC1``, ``0.4.0-rc.1``).

    Args:
        text: The version, optionally with a leading ``v``.

    Returns:
        The PEP 440 normalized version (``0.4.0rc1``) and ``True`` for a pre-release.

    Raises:
        ValueError: If ``text`` is not a version of the form this project releases (an
            epoch, a local part, or a release that is not three numbers): the gate never
            guesses.
    """
    match = _RELEASE_VERSION.match(text)
    if match is None:
        raise ValueError(
            f"{text!r} is not a release version: give X.Y.Z, optionally with a "
            "pre-release (aN, bN, rcN), post-release (.postN) or development (.devN) part"
        )
    parts = [".".join(str(int(n)) for n in match["release"].split("."))]
    if match["pre_l"]:
        letter = _PRE_RELEASE_LETTERS[match["pre_l"].lower()]
        parts.append(f"{letter}{int(match['pre_n'] or 0)}")
    if match["post_n1"] is not None or match["post_l"]:
        parts.append(f".post{int(match['post_n1'] or match['post_n2'] or 0)}")
    if match["dev_l"]:
        parts.append(f".dev{int(match['dev_n'] or 0)}")
    return "".join(parts), bool(match["pre_l"] or match["dev_l"])


# Variables that point git at another repository than ``--repo`` (git(1), "Environment
# Variables"); they are removed so that the repository read is the one named, and
# ``--no-replace-objects`` keeps replace refs from substituting the objects read.
_GIT_LOCATION_VARIABLES = frozenset(
    {
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_NAMESPACE",
        "GIT_GRAFT_FILE",
        "GIT_SHALLOW_FILE",
    }
)


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    env = {k: v for k, v in os.environ.items() if k not in _GIT_LOCATION_VARIABLES}
    try:
        return subprocess.run(
            ["git", "--no-replace-objects", "-C", str(repo), *args],
            capture_output=True,
            check=False,
            env=env,
        )
    except FileNotFoundError as exc:
        raise GitUnavailableError("git is not installed") from exc


def _show(repo: Path, rev: str, path: PurePosixPath) -> bytes | None:
    """The bytes of ``path`` at ``rev``, or ``None`` when it has no such file."""
    result = _git(repo, "cat-file", "blob", f"{rev}:{path}")
    return result.stdout if result.returncode == 0 else None


def _show_json(repo: Path, rev: str, path: PurePosixPath) -> Any:
    """The parsed JSON of ``path`` at ``rev``, or ``None`` when absent or not JSON."""
    data = _show(repo, rev, path)
    if data is None:
        return None
    try:
        return json.loads(data)
    except ValueError:
        return None


def _require_history(repo: Path) -> list[str]:
    """The repository's tags; raises when git, its history or its tags are missing."""
    inside = _git(repo, "rev-parse", "--is-inside-work-tree")
    if inside.returncode != 0 or inside.stdout.strip() != b"true":
        raise GitUnavailableError(f"{repo} is not a git work tree")
    shallow = _git(repo, "rev-parse", "--is-shallow-repository")
    if shallow.stdout.strip() != b"false":
        raise GitUnavailableError(
            "the clone is shallow; check out with the full history and tags "
            "(actions/checkout fetch-depth: 0)"
        )
    tags = _git(repo, "tag", "--list")
    names = tags.stdout.decode().split()
    if tags.returncode != 0 or not names:
        raise GitUnavailableError("the repository has no tags; fetch them")
    return names


def _current_version(repo: Path) -> str:
    """The version of the one data context manifest at HEAD."""
    listing = _git(repo, "ls-tree", "--name-only", "HEAD", f"{CONTEXTS / 'data'}/")
    names = [
        PurePosixPath(line).name
        for line in listing.stdout.decode().splitlines()
        if line.endswith(".json")
    ]
    if listing.returncode != 0 or len(names) != 1:
        raise SystemExit(
            f"cannot tell the current data context: manifests at HEAD are {names}"
        )
    return names[0].removesuffix(".json")


def _head_withdrawn(repo: Path, version: str) -> dict[str, Any]:
    """HEAD's records of the withdrawn releases of a data context, by version."""
    pins = _show_json(repo, "HEAD", PINS)
    pin = pins.get(version) if isinstance(pins, dict) else None
    withdrawn = pin.get("withdrawn") if isinstance(pin, dict) else None
    return withdrawn if isinstance(withdrawn, dict) else {}


def tag_release(repo: Path, tag: str, version: str) -> str | None:
    """The release of ``version`` a tag shipped, or ``None`` when it is not one.

    Args:
        repo: Repository root.
        tag: Tag name.
        version: Current data context version.

    Returns:
        ``X.Y.Z`` when the tag is ``v<X.Y.Z>`` and its ``released_manifests.json`` pins
        release ``X.Y.Z`` of ``version``; otherwise ``None``, always for the tag of a
        pre-release, which freezes nothing (ITL-1329).
    """
    if is_pre_release_tag(tag):
        return None
    release = tag.removeprefix("v")
    tag_pins = _show_json(repo, tag, PINS)
    pin = tag_pins.get(version) if isinstance(tag_pins, dict) else None
    if not isinstance(pin, dict) or not isinstance(pin.get("releases"), dict):
        return None
    return release if release in pin["releases"] else None


def check_withdrawn_tag(repo: Path, tag: str, version: str, release: str) -> list[str]:
    """Check HEAD's record of a withdrawn release against the tag of that release.

    Args:
        repo: Repository root.
        tag: Tag name.
        version: Current data context version.
        release: The release the tag shipped, recorded as withdrawn at HEAD.

    Returns:
        One sentence per difference between the record and what the tag pinned.
    """
    record = _head_withdrawn(repo, version).get(release)
    if not isinstance(record, dict):
        return [f"{tag}: the withdrawn record of {release} is not a record"]
    pin = _show_json(repo, tag, PINS)[version]
    problems: list[str] = []
    if record.get("revision") != pin["releases"][release]:
        problems.append(
            f"{tag}: the withdrawn record of {release} does not name the release "
            "revision the tag pinned"
        )
    if record.get("manifest") != pin.get("manifest"):
        problems.append(
            f"{tag}: the withdrawn record of {release} does not name the manifest pin "
            "the tag shipped"
        )
    return problems


def check_tag(repo: Path, tag: str, version: str) -> list[str] | None:
    """Check HEAD against one tag.

    A tag whose release HEAD records as withdrawn is checked by ``check_withdrawn_tag``
    instead.

    Args:
        repo: Repository root.
        tag: Tag name.
        version: Current data context version.

    Returns:
        ``None`` when the tag is not a release of ``version``; otherwise one sentence per
        difference (empty when HEAD keeps everything the tag released).
    """
    release = tag_release(repo, tag, version)
    if release is None:
        return None
    if release in _head_withdrawn(repo, version):
        return check_withdrawn_tag(repo, tag, version, release)
    pin = _show_json(repo, tag, PINS)[version]
    problems: list[str] = []
    head_pins = _show_json(repo, "HEAD", PINS)
    head_pin = head_pins.get(version) if isinstance(head_pins, dict) else None
    if not isinstance(head_pin, dict):
        return [f"{tag}: HEAD has no pin of data context {version}"]
    if head_pin.get("manifest") != pin.get("manifest"):
        problems.append(f"{tag}: the manifest pin of {version} changed")
    head_releases = head_pin.get("releases")
    head_releases = head_releases if isinstance(head_releases, dict) else {}
    for name, digest in pin["releases"].items():
        if head_releases.get(name) != digest:
            problems.append(f"{tag}: the releases line {name} of {version} changed")

    manifest_path = CONTEXTS / "data" / f"{version}.json"
    manifest = _show_json(repo, tag, manifest_path)
    golden = manifest.get("golden") if isinstance(manifest, dict) else None
    if not isinstance(golden, dict):
        return [*problems, f"{tag}: {manifest_path} has no golden section"]
    history = golden.get("history")
    if not isinstance(history, list):
        return [*problems, f"{tag}: {manifest_path} has no golden history"]
    end = next(
        (
            i
            for i, revision in enumerate(history)
            if isinstance(revision, dict)
            and revision.get("release") == release
            and revision.get("digest") == pin["releases"][release]
        ),
        None,
    )
    if end is None:
        return [*problems, f"{tag}: the history has no release revision {release}"]
    released = history[: end + 1]
    head_manifest = _show_json(repo, "HEAD", manifest_path)
    head_golden = (
        head_manifest.get("golden") if isinstance(head_manifest, dict) else None
    )
    head_history = head_golden.get("history") if isinstance(head_golden, dict) else None
    if not isinstance(head_history, list) or head_history[: end + 1] != released:
        problems.append(
            f"{tag}: the golden history up to release {release} changed in HEAD"
        )
    folder = GOLDEN_ROOT / str(golden.get("folder"))
    for revision in released:
        added = revision.get("added") if isinstance(revision, dict) else None
        if not isinstance(added, dict):
            problems.append(f"{tag}: a released revision lists no files")
            continue
        for name in added:
            path = folder / name
            at_tag = _show(repo, tag, path)
            at_head = _show(repo, "HEAD", path)
            if at_head is None:
                problems.append(f"{tag}: {path} is missing in HEAD")
            elif at_head != at_tag:
                problems.append(f"{tag}: {path} differs from the released file")
    return problems


TYPE_SNAPSHOTS = PurePosixPath("src/orcapod/data_function_types")
"""The data function type snapshots and their pins (``released.json``; ITL-1259)."""


def check_type_snapshots(repo: Path, tag: str) -> list[str]:
    """What HEAD changed of the data function type snapshots ``tag`` released.

    The tag of a pre-release released nothing (ITL-1329): it has no problem to report.

    Every snapshot the tag's ``released.json`` pins (a ``sha256`` that is not null)
    must be at HEAD with the same bytes, and pinned there with the same ``sha256`` and
    version: rewriting a released snapshot together with its pin is caught here, as a
    released golden file is. A tag without the folder released no snapshot.

    Args:
        repo: The repository.
        tag: The release tag.

    Returns:
        One problem per changed snapshot or pin; empty when HEAD keeps them.
    """
    if is_pre_release_tag(tag):
        return []
    pins_path = TYPE_SNAPSHOTS / "released.json"
    released = _show_json(repo, tag, pins_path)
    if not isinstance(released, dict):
        return []
    current = _show_json(repo, "HEAD", pins_path)
    current = current if isinstance(current, dict) else {}
    problems: list[str] = []
    for relative, pin in sorted(released.items()):
        if not isinstance(pin, dict) or pin.get("sha256") is None:
            continue
        if current.get(relative) != pin:
            problems.append(
                f"{tag}: the pin of data function type snapshot {relative} changed "
                f"({pin} at the tag, {current.get(relative)} at HEAD)"
            )
        path = TYPE_SNAPSHOTS / relative
        if _show(repo, "HEAD", path) != _show(repo, tag, path):
            problems.append(
                f"{tag}: data function type snapshot {relative} changed or is missing; "
                "a released snapshot never changes (give the type a new revision)"
            )
    return problems


def unpinned_type_snapshots(repo: Path) -> list[str]:
    """The data function type snapshots at HEAD that a release would ship unpinned.

    Every snapshot file under ``TYPE_SNAPSHOTS`` (other than ``released.json``) must be
    listed in HEAD's ``released.json`` with a ``sha256`` that is not null and equals the
    file's: a snapshot added or changed after the release step would otherwise ship open,
    and no later tag check would protect it (ITL-1259). A HEAD without the folder has no
    snapshot to pin; a listing or a file git cannot read is a problem, never a pass.

    Args:
        repo: The repository.

    Returns:
        One problem per open, missing or mismatched pin, or per path git cannot read;
        empty when every snapshot is pinned.
    """
    import hashlib

    listed = _git(
        repo, "ls-tree", "-r", "-z", "--name-only", "HEAD", str(TYPE_SNAPSHOTS)
    )
    if listed.returncode != 0:
        detail = listed.stderr.decode(errors="replace").strip()
        return [f"git cannot list the data function type snapshots at HEAD: {detail}"]
    pins = _show_json(repo, "HEAD", TYPE_SNAPSHOTS / "released.json")
    pins = pins if isinstance(pins, dict) else {}
    problems: list[str] = []
    names = [name for name in listed.stdout.decode().split("\0") if name]
    for name in sorted(names):
        path = PurePosixPath(name)
        relative = path.relative_to(TYPE_SNAPSHOTS).as_posix()
        if relative == "released.json" or path.suffix != ".json":
            continue
        pin = pins.get(relative)
        sha = pin.get("sha256") if isinstance(pin, dict) else None
        if sha is None:
            problems.append(
                f"data function type snapshot {relative} is not pinned: run "
                "generate_golden_hashes.py --release after adding it (a new version)"
            )
            continue
        data = _show(repo, "HEAD", path)
        if data is None:
            problems.append(f"git cannot read data function type snapshot {relative}")
        elif sha != hashlib.sha256(data).hexdigest():
            problems.append(
                f"data function type snapshot {relative} does not match its pin"
            )
    return problems


def check_release(repo: Path, release: str) -> list[str]:
    """Check that HEAD records a release of the current data context, before it is tagged.

    The Release workflow runs this before it creates the tag: HEAD's
    ``released_manifests.json`` pins ``release`` under the current context's
    ``releases``, its manifest pin is set, and the pinned digest is the digest of the
    release revision of ``release`` in HEAD's golden history, which is its last revision
    (no golden file added after the release step ships unreleased).

    A pre-release is checked by ``check_pre_release`` instead (ITL-1329).

    Args:
        repo: Repository root.
        release: The library version being released, optionally with a leading ``v``.

    Returns:
        One sentence per problem; empty when HEAD records the release (or, for a
        pre-release, does not).
    """
    try:
        release, prerelease = release_version(release)
    except ValueError as exc:
        return [str(exc)]
    if prerelease:
        return check_pre_release(repo, release)
    version = _current_version(repo)
    step = (
        f"run generate_golden_hashes.py --release {release} in the release pull request"
    )
    pins = _show_json(repo, "HEAD", PINS)
    pin = pins.get(version) if isinstance(pins, dict) else None
    releases = pin.get("releases") if isinstance(pin, dict) else None
    if not isinstance(pin, dict) or not isinstance(releases, dict):
        return [f"HEAD has no pin of data context {version}: {step}"]
    if release in _head_withdrawn(repo, version):
        return [
            f"release {release} of data context {version} was withdrawn and is never "
            "released again: a package index never takes a version twice"
        ]
    if release not in releases:
        return [
            f"HEAD does not record release {release} of data context {version}: {step}"
        ]
    problems: list[str] = []
    if pin.get("manifest") is None:
        problems.append(f"the manifest of data context {version} is not pinned: {step}")
    manifest = _show_json(repo, "HEAD", CONTEXTS / "data" / f"{version}.json")
    golden = manifest.get("golden") if isinstance(manifest, dict) else None
    history = golden.get("history") if isinstance(golden, dict) else None
    entries: list[Any] = history if isinstance(history, list) else []
    revisions = [
        revision
        for revision in entries
        if isinstance(revision, dict) and revision.get("release") == release
    ]
    if [revision.get("digest") for revision in revisions] != [releases[release]]:
        problems.append(
            f"the pinned digest of {release} is not the release revision of {release} "
            "in the golden history"
        )
    elif entries[-1] is not revisions[0]:
        problems.append(
            f"the release revision of {release} is not the last revision of the golden "
            "history: a golden file added after the release step would ship unreleased; "
            f"tag the commit of the release step, or run --release {release} again after "
            "the addition (a new version)"
        )
    problems.extend(unpinned_type_snapshots(repo))
    return problems


def check_pre_release(repo: Path, release: str) -> list[str]:
    """Check that HEAD records no pre-release as a release, before a pre-release is tagged.

    A pre-release carries the hashes intended to be final but freezes nothing: only a
    final release runs the golden release step (developer, November 21, 2026, ITL-1329).
    So no release record at HEAD names a pre-release, in any spelling: not the current
    data context's ``releases`` pins, not a release revision of its golden history, not
    a data function type snapshot pin (``released_in``). Nor may ``release`` be withdrawn
    (a package index never takes a version twice). Unpinned data function type snapshots
    are allowed: the final release step pins them. What earlier final releases froze is
    checked by the tag check, which the Release workflow runs for a pre-release too.

    Args:
        repo: Repository root.
        release: The normalized pre-release version (``release_version``).

    Returns:
        One sentence per problem; empty when HEAD does not record the pre-release.
    """
    version = _current_version(repo)
    pins = _show_json(repo, "HEAD", PINS)
    pin = pins.get(version) if isinstance(pins, dict) else None
    releases = pin.get("releases") if isinstance(pin, dict) else None
    recorded = sorted(releases) if isinstance(releases, dict) else []
    manifest = _show_json(repo, "HEAD", CONTEXTS / "data" / f"{version}.json")
    golden = manifest.get("golden") if isinstance(manifest, dict) else None
    history = golden.get("history") if isinstance(golden, dict) else None
    revisions = [
        str(revision["release"])
        for revision in (history if isinstance(history, list) else [])
        if isinstance(revision, dict) and revision.get("release") is not None
    ]
    snapshot_pins = _show_json(repo, "HEAD", TYPE_SNAPSHOTS / "released.json")
    snapshot_releases = {
        relative: str(snapshot_pin["released_in"])
        for relative, snapshot_pin in (
            snapshot_pins.items() if isinstance(snapshot_pins, dict) else []
        )
        if isinstance(snapshot_pin, dict)
        and snapshot_pin.get("released_in") is not None
    }
    step = (
        "a pre-release never runs the golden release step (only a final release "
        "freezes a data context); revert the release step's commit"
    )
    problems: list[str] = []
    for name in recorded:
        if _is_pre_release(name):
            problems.append(
                f"HEAD records pre-release {name} as a release of data context "
                f"{version}: {step}"
            )
    for name in revisions:
        if _is_pre_release(name):
            problems.append(
                f"the golden history of data context {version} at HEAD has a release "
                f"revision of pre-release {name}: {step}"
            )
    for relative, name in sorted(snapshot_releases.items()):
        if _is_pre_release(name):
            problems.append(
                f"data function type snapshot {relative} is pinned as released in "
                f"pre-release {name}: a pre-release pins no snapshot; revert the pin"
            )
    if any(_same_version(name, release) for name in _head_withdrawn(repo, version)):
        problems.append(
            f"release {release} of data context {version} was withdrawn and is never "
            "released again: a package index never takes a version twice"
        )
    return problems


def _is_pre_release(name: str) -> bool:
    """Whether a recorded version name is a pre-release, in any spelling."""
    try:
        return release_version(name)[1]
    except ValueError:
        return False


def is_pre_release_tag(tag: str) -> bool:
    """Whether ``tag`` names a pre-release (``v0.4.0rc1``): such a tag froze nothing.

    A tag that names no release version (not a version, or one this project does not
    release) is not a pre-release tag, and is checked as before.
    """
    return _is_pre_release(tag)


def _same_version(name: str, release: str) -> bool:
    """Whether a recorded version name is ``release``, in any spelling."""
    try:
        return release_version(name)[0] == release
    except ValueError:
        return False


def classify_version(text: str) -> tuple[str, bool]:
    """The version the Release workflow tags, and whether it is a pre-release.

    The workflow tags ``v<version>`` and ``hatch-vcs`` builds the package version from
    that tag, so the version must already be in its PEP 440 normal form: a spelling PEP
    440 would normalize (``0.4.0-rc.1``) is refused, naming the normal form, so that the
    tag, the package, the GitHub Release and the Linear release name the same version.

    Args:
        text: The workflow's version input, optionally with a leading ``v``.

    Returns:
        The version and ``True`` for a pre-release.

    Raises:
        ValueError: If ``text`` is not a release version, or not in its normal form.
    """
    normalized, prerelease = release_version(text)
    if text.removeprefix("v") != normalized:
        raise ValueError(
            f"{text!r} is not in its PEP 440 normal form: give it as {normalized!r}, so "
            "that the tag and the package name the same version"
        )
    return normalized, prerelease


def main(argv: Sequence[str] | None = None) -> int:
    """Run the check.

    Args:
        argv: Command-line arguments; ``None`` reads ``sys.argv``.

    Returns:
        The exit status.
    """
    parser = argparse.ArgumentParser(
        description="Check HEAD against every release tag of the current data context."
    )
    _ = parser.add_argument("--repo", type=Path, default=REPO_ROOT)
    _ = parser.add_argument(
        "--release-version",
        metavar="X.Y.Z",
        help=(
            "instead of comparing with the tags, check that HEAD records this release "
            "(the Release workflow's gate before it tags); fails without git"
        ),
    )
    _ = parser.add_argument(
        "--classify-version",
        metavar="X.Y.Z",
        help=(
            "print 'version=<version>' and 'prerelease=true|false' for the Release "
            "workflow's step outputs; fails on a version that is not a release version "
            "in its normal form"
        ),
    )
    args = parser.parse_args(argv)
    if args.classify_version is not None:
        try:
            normalized, prerelease = classify_version(args.classify_version)
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        print(f"version={normalized}")
        print(f"prerelease={'true' if prerelease else 'false'}")
        return 0
    if args.release_version is not None:
        try:
            _, prerelease = release_version(args.release_version)
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        try:
            problems = check_release(args.repo, args.release_version.removeprefix("v"))
        except GitUnavailableError as exc:
            print(f"error: cannot read HEAD: {exc}", file=sys.stderr)
            return 1
        if problems:
            heading = (
                f"pre-release {args.release_version} cannot be tagged:"
                if prerelease
                else f"release {args.release_version} is not recorded in the golden history:"
            )
            print(heading, file=sys.stderr)
            for problem in problems:
                print(f"  {problem}", file=sys.stderr)
            return 1
        if prerelease:
            print(
                f"{args.release_version} is a pre-release: it freezes nothing, so no golden "
                "release step is required and HEAD does not record it; the data function "
                "type snapshots may stay unpinned until the final release step pins them"
            )
        else:
            print(f"HEAD records release {args.release_version} in the golden history")
        return 0
    in_ci = os.environ.get("CI", "").lower() == "true"
    try:
        tags = _require_history(args.repo)
    except GitUnavailableError as exc:
        if in_ci:
            print(f"error: cannot check the release tags: {exc}", file=sys.stderr)
            return 1
        print(f"skipped: cannot check the release tags: {exc}")
        return 0
    version = _current_version(args.repo)
    withdrawn = _head_withdrawn(args.repo, version)
    checked: list[str] = []
    problems: list[str] = []
    for tag in sorted(tags):
        if is_pre_release_tag(tag):
            # A pre-release froze nothing (ITL-1329): its tag protects nothing.
            print(
                f"{tag}: a pre-release, which freezes nothing; not compared with HEAD"
            )
            continue
        # The data function type snapshots do not belong to a data context.
        problems.extend(check_type_snapshots(args.repo, tag))
        found = check_tag(args.repo, tag, version)
        if found is None:
            continue
        release = tag.removeprefix("v")
        if release in withdrawn and not found:
            record = withdrawn[release]
            print(
                f"{tag}: withdrawn on {record.get('date')}, not compared with HEAD; "
                "the record matches the tag"
            )
            continue
        checked.append(tag)
        problems.extend(found)
    if problems:
        print(
            "HEAD changes what a release of data context "
            f"{version} shipped; a released golden file, revision or pin never changes:",
            file=sys.stderr,
        )
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        return 1
    if checked:
        print(f"HEAD keeps what {', '.join(checked)} released (data context {version})")
    else:
        print(f"no release tag of data context {version}: nothing to check")
    return 0


if __name__ == "__main__":
    sys.exit(main())
