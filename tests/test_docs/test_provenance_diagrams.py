"""The provenance pictures in the documentation are what Orcapod draws today.

``docs/dev/scripts/provenance_diagrams.py`` draws them with ``job.diagram(highlight=...)``
from the tutorial's synthetic dataset; this test draws them again and compares them, byte
for byte, with the committed files. After a change to the diagram, rewrite them with
``uv run python docs/dev/scripts/provenance_diagrams.py``.
"""

from __future__ import annotations

import importlib.util
import re
from types import ModuleType
from pathlib import Path

import pytest

REPOSITORY = Path(__file__).resolve().parents[2]
GENERATOR = REPOSITORY / "docs" / "dev" / "scripts" / "provenance_diagrams.py"


def _generator() -> ModuleType:
    spec = importlib.util.spec_from_file_location("provenance_diagrams", GENERATOR)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def drawn() -> dict[str, str]:
    return _generator().render()


def test_every_picture_is_drawn(drawn: dict[str, str]) -> None:
    assert sorted(drawn) == sorted(_generator().PICTURES)


@pytest.mark.parametrize("name", _generator().PICTURES)
def test_the_committed_picture_is_current(drawn: dict[str, str], name: str) -> None:
    committed = (REPOSITORY / "docs" / "tutorial" / name).read_text(encoding="utf-8")
    assert committed == drawn[name], (
        f"docs/tutorial/{name} is stale: run "
        "uv run python docs/dev/scripts/provenance_diagrams.py"
    )


@pytest.mark.parametrize("name", _generator().PICTURES)
def test_a_picture_is_self_contained_and_on_its_own_background(name: str) -> None:
    """Readable on the site's light and dark pages (a white background of its own),
    and holding no local path."""
    svg = (REPOSITORY / "docs" / "tutorial" / name).read_text(encoding="utf-8")
    assert svg.startswith('<svg xmlns="http://www.w3.org/2000/svg"')
    assert re.search(r'<rect class="op-dag-bg"[^>]* fill="#ffffff"', svg)
    assert not re.search(r"/Users/|/home/|/private/|/tmp/|[A-Z]:\\\\", svg)


@pytest.mark.parametrize(
    "page",
    ["docs/tutorial/tracing-a-value.md", "docs/concepts/provenance.md"],
)
def test_the_pages_show_the_pictures(page: str) -> None:
    text = (REPOSITORY / page).read_text(encoding="utf-8")
    shown = set(
        re.findall(
            r"!\[[^\]]*\]\((?:\.\./tutorial/)?(provenance-diagram-[a-z-]+\.svg)\)", text
        )
    )
    assert shown and shown <= set(_generator().PICTURES)
