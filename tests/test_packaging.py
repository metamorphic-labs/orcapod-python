"""Packaging guards for ``pyproject.toml`` and the built distributions."""

from __future__ import annotations

import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = REPO_ROOT / "pyproject.toml"
LICENSE_ID = "LicenseRef-Metamorphic-Proprietary"


def test_pygraphviz_is_not_a_dependency():
    """``pygraphviz`` needs Graphviz C headers to build and nothing imports it (ITL-134)."""
    project = tomllib.loads(PYPROJECT.read_text())["project"]
    declared = list(project["dependencies"])
    for extra in project.get("optional-dependencies", {}).values():
        declared.extend(extra)
    assert not [d for d in declared if d.lower().startswith("pygraphviz")]


def test_license_is_metamorphic_proprietary() -> None:
    """Orcapod is proprietary to Metamorphic Labs from v0.2 on (ITL-827)."""
    project = tomllib.loads(PYPROJECT.read_text())["project"]
    assert project["license"] == {"text": LICENSE_ID}
    # No license classifier at all: pip-licenses prefers classifiers over the License
    # field, so "License :: Other/Proprietary License" would fail the org license check.
    assert not [c for c in project.get("classifiers", []) if c.startswith("License ::")]
    license_text = (REPO_ROOT / "LICENSE").read_text()
    assert license_text.startswith("Copyright (c) Metamorphic Labs, Inc.")
