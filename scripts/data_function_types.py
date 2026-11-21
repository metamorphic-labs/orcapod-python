"""Write, check and pin the snapshots of the data function type declarations (ITL-1259).

A data function type's declaration (``PYTHON_FUNCTION`` in ``src/orcapod/core/data_function.py``,
classes in ``src/orcapod/core/data_function_types.py``) is the source of truth for what its result tables store. Each declaration is also written out
as a snapshot, ``src/orcapod/data_function_types/<type name>/v<major>-rev<revision>.json``,
and ``src/orcapod/data_function_types/released.json`` pins each snapshot file:

    {"python.function/v1-rev1.json": {"sha256": null, "released_in": null}}

While a snapshot is open (``sha256`` null, until the release that ships it), it must
equal what the code writes: ``--write`` rewrites it. Once released, its sha256 is pinned
and the file never changes: the code must still write it byte for byte, so changing what
a type stores needs a new revision (or a new major), with a new snapshot.

    uv run python scripts/data_function_types.py --check     # CI: tests/test_data_function_types.py
    uv run python scripts/data_function_types.py --write     # after changing a declaration
    uv run python scripts/data_function_types.py --release X.Y.Z   # the release step

``--release`` pins every open snapshot; ``generate_golden_hashes.py --release`` runs it. It
refuses a pre-release (ITL-1329): only a final release pins, so that a release candidate
freezes nothing. Which versions are pre-releases is the release gate's rule,
``release_version`` in ``check_golden_tags.py`` beside this script.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOTS = ROOT / "src" / "orcapod" / "data_function_types"
PINS = SNAPSHOTS / "released.json"


def _declarations() -> dict[str, str]:
    """The snapshot text of every declaration, by its path under ``SNAPSHOTS``."""
    sys.path.insert(0, str(ROOT / "src"))
    from orcapod.core.data_function import DECLARATIONS
    from orcapod.core.data_function_types import snapshot_path, snapshot_text

    return {
        snapshot_path(declaration): snapshot_text(declaration)
        for declaration in DECLARATIONS.values()
    }


def _read_pins(snapshots: Path) -> dict[str, dict[str, str | None]]:
    pins_path = snapshots / "released.json"
    if not pins_path.exists():
        return {}
    return json.loads(pins_path.read_text(encoding="utf-8"))


def _write_pins(snapshots: Path, pins: dict[str, dict[str, str | None]]) -> None:
    (snapshots / "released.json").write_text(
        json.dumps(pins, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(snapshots: Path) -> list[str]:
    """Write the snapshot of every declaration whose file is open or missing.

    Args:
        snapshots: The snapshot folder.

    Returns:
        The problems that stop the write: a pinned snapshot the code no longer writes
        byte for byte. Empty when everything was written.
    """
    pins = _read_pins(snapshots)
    problems: list[str] = []
    for relative, text in _declarations().items():
        path = snapshots / relative
        pin = pins.get(relative, {})
        if pin.get("sha256") is not None:
            if path.read_text(encoding="utf-8") != text:
                problems.append(
                    f"{relative} was released (pinned); the code now declares it "
                    "differently: give the type a new revision (or a new major) "
                    "instead of changing a released one"
                )
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        pins[relative] = {"sha256": None, "released_in": None}
    _write_pins(snapshots, pins)
    return problems


def check(snapshots: Path) -> list[str]:
    """Check the snapshots against the code and the pins.

    Args:
        snapshots: The snapshot folder.

    Returns:
        Every problem found; empty when the snapshots are sound.
    """
    pins = _read_pins(snapshots)
    declared = _declarations()
    problems: list[str] = []
    for relative, text in declared.items():
        path = snapshots / relative
        if not path.exists():
            problems.append(f"{relative} is missing: run --write")
        elif path.read_text(encoding="utf-8") != text:
            if pins.get(relative, {}).get("sha256") is None:
                problems.append(f"{relative} differs from the code: run --write")
            else:
                problems.append(
                    f"{relative} was released (pinned) and the code now declares it "
                    "differently: give the type a new revision (or a new major)"
                )
        if relative not in pins:
            problems.append(f"{relative} is not listed in released.json: run --write")
    for relative, pin in sorted(pins.items()):
        path = snapshots / relative
        sha = pin.get("sha256")
        if sha is None:
            if relative not in declared:
                problems.append(
                    f"{relative} is open but no declaration writes it: remove it"
                )
            continue
        if not path.exists():
            problems.append(f"{relative} was released and is missing")
        elif _sha256(path) != sha:
            problems.append(
                f"{relative} was released with sha256 {sha} and has changed "
                f"({_sha256(path)}): a released snapshot never changes"
            )
    for path in sorted(snapshots.rglob("*.json")):
        relative = path.relative_to(snapshots).as_posix()
        if relative != "released.json" and relative not in pins:
            problems.append(f"{relative} is not listed in released.json")
    return problems


def open_snapshots(snapshots: Path) -> list[str]:
    """The snapshots ``released.json`` lists as open (``sha256`` null), sorted.

    Args:
        snapshots: The snapshot folder.

    Returns:
        Their paths relative to ``snapshots``.
    """
    return sorted(
        relative
        for relative, pin in _read_pins(snapshots).items()
        if pin.get("sha256") is None
    )


def _refuse_pre_release(version: str) -> None:
    """Refuse to pin for a pre-release, or for a text that is not a release version.

    The rule is ``release_version`` of ``check_golden_tags.py`` beside this script (the
    release gate's, standard library only), loaded by path: ``scripts`` is not a package.

    Raises:
        SystemExit: If ``version`` is a pre-release or not a release version, or the gate
            script is missing (the rule is never guessed).
    """
    import importlib.util

    gate = Path(__file__).resolve().with_name("check_golden_tags.py")
    if not gate.is_file():
        raise SystemExit(
            f"{gate} is missing: it decides which versions are pre-releases; nothing "
            "was pinned"
        )
    spec = importlib.util.spec_from_file_location("check_golden_tags_script", gate)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    try:
        name, prerelease = module.release_version(version)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if prerelease:
        raise SystemExit(
            f"{name} is a pre-release: a pre-release pins no snapshot, only a final "
            "release does (ITL-1329); nothing was pinned"
        )


def release(snapshots: Path, version: str) -> list[str]:
    """Pin every open snapshot as released in ``version``, a final release.

    Args:
        snapshots: The snapshot folder.
        version: The library version being released (``0.4.0``).

    Returns:
        The relative paths pinned now.

    Raises:
        SystemExit: If ``version`` is a pre-release (ITL-1329), or a snapshot is unsound.
    """
    _refuse_pre_release(version)
    problems = check(snapshots)
    if problems:
        raise SystemExit("\n".join(problems))
    pins = _read_pins(snapshots)
    pinned: list[str] = []
    for relative, pin in sorted(pins.items()):
        if pin.get("sha256") is None:
            pins[relative] = {
                "sha256": _sha256(snapshots / relative),
                "released_in": version,
            }
            pinned.append(relative)
    _write_pins(snapshots, pins)
    return pinned


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command line."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    group = parser.add_mutually_exclusive_group(required=True)
    _ = group.add_argument("--check", action="store_true", help="check, change nothing")
    _ = group.add_argument("--write", action="store_true", help="write open snapshots")
    _ = group.add_argument("--release", metavar="X.Y.Z", help="pin the open snapshots")
    _ = parser.add_argument(
        "--snapshots", type=Path, default=SNAPSHOTS, help=argparse.SUPPRESS
    )
    args = parser.parse_args(argv)
    if args.check:
        problems = check(args.snapshots)
        for problem in problems:
            print(problem)
        if not problems:
            print("data function type snapshots OK")
        return 1 if problems else 0
    if args.write:
        problems = write(args.snapshots)
        for problem in problems:
            print(problem)
        return 1 if problems else 0
    for relative in release(args.snapshots, args.release):
        print(f"pinned {relative} for {args.release}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
