"""The term data-tag is never written as two words (developer, November 17, 2026; ITL-1023).

A data-tag is one column in both the tag and the data role. In prose it is always the
hyphenated "data-tag" (plural "data-tags"), in identifiers ``data_tag`` / ``data_tags``,
and in class names ``DataTag``: the two words would read as "the tags of the data". The
check is case-insensitive and also finds the two words split across a line break, a
comment marker or Markdown emphasis. Quoted history is exempt: the released
``CHANGELOG.md`` and the dated development plans and specifications under ``docs/dev/``.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

TEXT_SUFFIXES = {
    ".py",
    ".md",
    ".yml",
    ".yaml",
    ".toml",
    ".txt",
    ".json",
    ".ipynb",
    ".cfg",
}

EXEMPT = re.compile(r"^(CHANGELOG\.md|docs/dev/plans/|docs/dev/specs/)")

# "data" and "tag(s)" separated only by whitespace, comment markers or emphasis.
TWO_WORDS = re.compile(r"\bdata[\s#>*]+tags?\b", re.IGNORECASE)


def _tracked_text_files() -> list[str]:
    listed = subprocess.run(
        ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.split("\n")
    return [
        name
        for name in listed
        if name and Path(name).suffix in TEXT_SUFFIXES and not EXEMPT.match(name)
    ]


def _two_word_spellings(text: str) -> list[int]:
    """Return the line number of each two-word spelling in ``text``."""
    return [
        text.count("\n", 0, match.start()) + 1 for match in TWO_WORDS.finditer(text)
    ]


def test_the_check_finds_the_two_words() -> None:
    # Built from parts, so this file does not hold the spelling it forbids.
    word = "data"
    assert _two_word_spellings(f"a {word} tag") == [1]
    assert _two_word_spellings(f"the {word.title()}\n    # Tags of x") == [1]
    assert _two_word_spellings("a data-tag, the data_tags, metadata tags") == []


def test_no_file_writes_the_term_as_two_words() -> None:
    found = []
    for name in _tracked_text_files():
        path = ROOT / name
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        found.extend(f"{name}:{line}" for line in _two_word_spellings(text))
    assert not found, (
        "write the term as 'data-tag' (prose) or data_tag (identifiers), never as "
        f"two words: {found}"
    )
