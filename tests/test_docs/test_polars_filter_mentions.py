"""The user documentation mentions ``PolarsFilter`` only with its warning (ITL-1296).

``PolarsFilter`` can change the values it passes through, so ``Filter`` is the filter the
documentation shows. Every paragraph, list item or table row of a published page (and of
the README) that names ``PolarsFilter`` or ``polars_filter`` must say it is not
recommended. Exempt: the ``PolarsFilter`` section of ``docs/concepts/operators.md``, which
opens with the warning itself; the API reference, generated from the docstrings; and code
samples.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS = REPO_ROOT / "docs"

_MENTION = re.compile(r"PolarsFilter|polars_filter")
_QUALIFIER = "not recommended"
_OWN_SECTION = ("docs/concepts/operators.md", "### PolarsFilter")


def _pages() -> list[Path]:
    pages = [REPO_ROOT / "README.md"]
    for page in sorted(DOCS.rglob("*.md")):
        relative = page.relative_to(DOCS).parts
        if relative[0] not in ("dev", "api"):
            pages.append(page)
    return pages


def _blocks(page: Path) -> list[tuple[int, str]]:
    """The page's text blocks outside code samples, each with its first line number.

    A block ends at a blank line, and a list item or a table row is a block of its own.
    The ``PolarsFilter`` section of the operators page (to the next heading of its level
    or above) is left out.
    """
    name = page.relative_to(REPO_ROOT).as_posix()
    blocks: list[tuple[int, str]] = []
    current: list[str] = []
    start = 0
    in_code = False
    in_own_section = False

    def close() -> None:
        if current:
            blocks.append((start, " ".join(current)))
            current.clear()

    for number, line in enumerate(page.read_text().splitlines(), start=1):
        stripped = line.strip()
        if stripped.startswith("```"):
            in_code = not in_code
            close()
            continue
        if in_code:
            continue
        if stripped.startswith("#"):
            close()
            level = len(stripped) - len(stripped.lstrip("#"))
            if name == _OWN_SECTION[0] and stripped == _OWN_SECTION[1]:
                in_own_section = True
            elif in_own_section and level <= 3:
                in_own_section = False
            continue
        if in_own_section:
            continue
        if not stripped:
            close()
            continue
        if stripped.startswith(("- ", "* ", "|")) or re.match(r"\d+\. ", stripped):
            close()
        if not current:
            start = number
        current.append(stripped)
    close()
    return blocks


def test_the_pages_checked_include_the_ones_that_mention_it() -> None:
    names = {page.relative_to(REPO_ROOT).as_posix() for page in _pages()}
    assert {
        "README.md",
        "docs/concepts/operators.md",
        "docs/concepts/sources.md",
        "docs/concepts/identity.md",
        "docs/reference/hashing.md",
        "docs/troubleshooting.md",
        "docs/tutorial/03-operators.md",
    } <= names


@pytest.mark.parametrize(
    "page", _pages(), ids=lambda page: page.relative_to(REPO_ROOT).as_posix()
)
def test_every_mention_says_not_recommended(page: Path) -> None:
    unqualified = [
        f"{page.relative_to(REPO_ROOT).as_posix()}:{line}: {text[:120]}"
        for line, text in _blocks(page)
        if _MENTION.search(text) and _QUALIFIER not in text.lower()
    ]
    assert unqualified == []


def test_the_own_section_opens_with_the_warning() -> None:
    """The exempt section says it itself, in its first block."""
    text = (DOCS / "concepts" / "operators.md").read_text()
    section = text[text.index("\n### PolarsFilter\n") :]
    first_block = section.split("\n\n")[1]
    assert first_block.startswith('!!! warning "Not recommended')
    assert "PolarsFilterWarning" in first_block and "decimal256" in first_block


def test_the_tutorial_does_not_use_it() -> None:
    for page in sorted((DOCS / "tutorial").glob("*.md")):
        assert not _MENTION.search(page.read_text()), page.name
