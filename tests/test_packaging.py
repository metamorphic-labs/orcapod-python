"""Packaging guards for ``pyproject.toml`` and the built distributions."""

from __future__ import annotations

import shutil
import subprocess
import tarfile
import tomllib
import zipfile
from pathlib import Path
from typing import cast

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = REPO_ROOT / "pyproject.toml"
LICENSE_ID = "LicenseRef-Metamorphic-Proprietary"


def _pyproject_value(*keys: str) -> object:
    """Return the ``pyproject.toml`` value found by following ``keys``."""
    node: object = tomllib.loads(PYPROJECT.read_text())
    for key in keys:
        assert isinstance(node, dict)
        node = cast("dict[str, object]", node)[key]
    return node


def _pyproject_strings(*keys: str) -> list[str]:
    """Return the ``pyproject.toml`` list of strings found by following ``keys``."""
    value = _pyproject_value(*keys)
    assert isinstance(value, list)
    return [str(item) for item in cast("list[object]", value)]


def test_pygraphviz_is_not_a_dependency():
    """``pygraphviz`` needs Graphviz C headers to build and nothing imports it (ITL-134)."""
    project = tomllib.loads(PYPROJECT.read_text())["project"]
    declared = list(project["dependencies"])
    for extra in project.get("optional-dependencies", {}).values():
        declared.extend(extra)
    assert not [d for d in declared if d.lower().startswith("pygraphviz")]


def test_license_is_metamorphic_proprietary() -> None:
    """Orcapod is proprietary to Metamorphic Labs from v0.2 on (ITL-827)."""
    assert _pyproject_value("project", "license") == {"text": LICENSE_ID}
    # No license classifier at all: pip-licenses prefers classifiers over the License
    # field, so "License :: Other/Proprietary License" would fail the org license check.
    classifiers = _pyproject_strings("project", "classifiers")
    assert not [c for c in classifiers if c.startswith("License ::")]
    license_text = (REPO_ROOT / "LICENSE").read_text()
    assert license_text.startswith("Copyright (c) Metamorphic Labs, Inc.")


def test_basedpyright_is_a_dev_dependency_only() -> None:
    """A type checker is a development tool, not something users install (ITL-827)."""
    runtime = [d.lower() for d in _pyproject_strings("project", "dependencies")]
    dev = [d.lower() for d in _pyproject_strings("dependency-groups", "dev")]
    assert not [d for d in runtime if d.startswith("basedpyright")]
    assert [d for d in dev if d.startswith("basedpyright")]


@pytest.fixture(scope="module")
def built_dists(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    """Build the sdist, then the wheel from it, exactly as the release workflow does.

    Returns:
        The paths of the sdist and the wheel.
    """
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is required to build the distributions")
    out_dir = tmp_path_factory.mktemp("dist")
    _ = subprocess.run(
        [uv, "build", "--out-dir", str(out_dir), str(REPO_ROOT)],
        check=True,
        capture_output=True,
    )
    return next(out_dir.glob("*.tar.gz")), next(out_dir.glob("*.whl"))


def _sdist_members(sdist: Path) -> list[str]:
    """Return sdist member paths with the ``orcapod-<version>/`` prefix removed."""
    with tarfile.open(sdist) as tar:
        return [name.partition("/")[2] for name in tar.getnames() if "/" in name]


def test_sdist_contains_only_build_inputs(built_dists: tuple[Path, Path]) -> None:
    """The sdist ships the package source, build metadata, README and LICENSE (ITL-827)."""
    members = _sdist_members(built_dists[0])
    top_level = {m.split("/")[0] for m in members}
    # hatchling always adds .gitignore to an sdist; it cannot be excluded.
    build_inputs = {".gitignore", "PKG-INFO", "pyproject.toml", "src"}
    assert top_level == build_inputs | {"LICENSE", "README.md"}
    assert {m.split("/")[1] for m in members if m.startswith("src/")} == {"orcapod"}


def test_wheel_contains_only_the_package(built_dists: tuple[Path, Path]) -> None:
    """The wheel holds the ``orcapod`` package and its dist-info, nothing else."""
    wheel = built_dists[1]
    with zipfile.ZipFile(wheel) as zf:
        top_level = {name.split("/")[0] for name in zf.namelist()}
    version = wheel.name.split("-")[1]
    assert top_level == {"orcapod", f"orcapod-{version}.dist-info"}


def test_license_is_shipped_in_sdist_and_wheel(built_dists: tuple[Path, Path]) -> None:
    """Both distributions carry the repository LICENSE verbatim (ITL-827)."""
    sdist, wheel = built_dists
    expected = (REPO_ROOT / "LICENSE").read_bytes()
    with tarfile.open(sdist) as tar:
        member = tar.extractfile(f"{sdist.name.removesuffix('.tar.gz')}/LICENSE")
        assert member is not None
        assert member.read() == expected
    with zipfile.ZipFile(wheel) as zf:
        suffix = ".dist-info/licenses/LICENSE"
        licenses = [n for n in zf.namelist() if n.endswith(suffix)]
        assert len(licenses) == 1
        assert zf.read(licenses[0]) == expected
