"""The data context of v0.3 is ``std:v1:default``, and released manifests are frozen (ITL-944).

A data context version names one generation of hashing. Its manifest file
(``src/orcapod/contexts/data/<version>.json``) must not change once released, so
``released_manifests.json`` pins the sha256 of every released file. The manifest of the
version under development is listed with ``null`` until the release-preparation commit
sets its digest, once.
"""

from __future__ import annotations

import hashlib
import json
from importlib import resources
from pathlib import Path

from packaging.version import Version

import orcapod
from orcapod import contexts

CONTEXTS_DIR = Path(str(resources.files("orcapod.contexts")))
MANIFEST_DIR = CONTEXTS_DIR / "data"


def _pins() -> dict[str, str | None]:
    return json.loads((CONTEXTS_DIR / "released_manifests.json").read_text())


def _manifest_files() -> dict[str, Path]:
    return {path.stem: path for path in sorted(MANIFEST_DIR.glob("*.json"))}


def test_default_data_context_is_v1() -> None:
    assert contexts.get_default_context_key() == "std:v1:default"
    assert contexts.get_default_context().version == "v1"


def test_v1_resolves_by_version_and_by_key() -> None:
    default = contexts.get_default_context()
    assert contexts.resolve_context("v1") is default
    assert contexts.resolve_context("std:v1:default") is default


def test_every_manifest_is_listed() -> None:
    assert set(_pins()) == set(_manifest_files())


def test_released_manifests_are_unchanged() -> None:
    files = _manifest_files()
    for version, pinned in _pins().items():
        if pinned is None:
            continue
        found = hashlib.sha256(files[version].read_bytes()).hexdigest()
        assert found == pinned, (
            f"The released data context manifest {files[version].name} was edited. "
            "A released manifest is frozen: a change to hashing needs a new data "
            "context version with its own manifest file."
        )


def test_only_the_default_manifest_is_open() -> None:
    open_versions = [version for version, pinned in _pins().items() if pinned is None]
    assert open_versions in ([], [contexts.get_default_context().version])


def test_a_final_release_ships_no_open_manifest() -> None:
    version = Version(orcapod.__version__)
    if version.is_devrelease or version.local is not None:
        return  # a development build: the default manifest may still be open
    assert None not in _pins().values(), (
        "A release must freeze its data context manifest: set its sha256 in "
        "src/orcapod/contexts/released_manifests.json."
    )
