"""The documentation site's content build (``scripts/docs_site_content.py``).

The Starlight site under ``site/`` is built from ``docs/`` by that script, which
converts every published page and generates the API reference from the source. It
fails on a broken link, a page that is neither published nor excluded, an object an API
page names that does not exist, or a docstring griffe reports a problem with; these
tests hold it to that, without Node. The site build itself (``pnpm build`` in
``site/``, which also validates every link and anchor) runs in the ``docs-site``
workflow.

Every test drives the script as a command, the way the site build runs it.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "docs_site_content.py"


def run_build(
    docs: Path, tmp_path: Path, base: str = "/"
) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
    out = tmp_path / "out"
    public = tmp_path / "public"
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--docs",
            str(docs),
            "--out",
            str(out),
            "--public",
            str(public),
            "--base",
            base,
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    return proc, out, public


def make_docs(tmp_path: Path, pages: dict[str, str]) -> Path:
    docs = tmp_path / "docs"
    for name in ("tutorial", "concepts", "reference", "api"):
        (docs / name).mkdir(parents=True)
    for path, text in pages.items():
        target = docs / path
        target.parent.mkdir(parents=True, exist_ok=True)
        _ = target.write_text(text, encoding="utf-8")
    return docs


def build_ok(tmp_path: Path, pages: dict[str, str], base: str = "/") -> Path:
    proc, out, _ = run_build(make_docs(tmp_path, pages), tmp_path, base)
    assert proc.returncode == 0, proc.stderr
    return out


def build_fails(tmp_path: Path, pages: dict[str, str]) -> str:
    proc, _, _ = run_build(make_docs(tmp_path, pages), tmp_path)
    assert proc.returncode != 0, "the build should have failed"
    return proc.stderr


# ---------------------------------------------------------------------------
# The real documentation
# ---------------------------------------------------------------------------


def test_the_documentation_builds(tmp_path: Path):
    """Every page converts, every link resolves, every API object and docstring renders."""
    proc, out, public = run_build(REPO_ROOT / "docs", tmp_path, "/orcapod-python/")
    assert proc.returncode == 0, proc.stderr
    assert (out / "index.md").is_file()
    assert (out / "api" / "operators.md").is_file()
    assert (public / "tutorial" / "workshop_data.py").is_file()
    assert not (out / "dev").exists()
    api = (out / "api" / "operators.md").read_text(encoding="utf-8")
    assert '<a id="orcapod.core.operators.Join"></a>' in api
    assert "\n::: " not in api


def test_the_build_is_deterministic(tmp_path: Path):
    first, out_a, _ = run_build(REPO_ROOT / "docs", tmp_path / "a")
    second, out_b, _ = run_build(REPO_ROOT / "docs", tmp_path / "b")
    assert first.returncode == 0 and second.returncode == 0
    files_a = sorted(p.relative_to(out_a) for p in out_a.rglob("*.md"))
    files_b = sorted(p.relative_to(out_b) for p in out_b.rglob("*.md"))
    assert files_a == files_b
    for rel in files_a:
        assert (out_a / rel).read_bytes() == (out_b / rel).read_bytes(), rel


# ---------------------------------------------------------------------------
# Page conversion
# ---------------------------------------------------------------------------


def test_title_line_becomes_front_matter(tmp_path: Path):
    out = build_ok(tmp_path, {"index.md": "# Identity & Hashing\n\nBody.\n"})
    assert (out / "index.md").read_text() == (
        '---\ntitle: "Identity & Hashing"\n---\n\nBody.\n'
    )


def test_a_page_without_a_title_fails(tmp_path: Path):
    assert "title" in build_fails(tmp_path, {"index.md": "Body.\n"})


def test_admonitions_become_asides(tmp_path: Path):
    page = (
        "# P\n\n"
        '!!! warning "Read `run()` first"\n'
        "    Text.\n"
        "\n"
        "    ```python\n"
        "    x = 1\n"
        "    ```\n"
        "\n"
        "After.\n"
        "\n"
        "!!! tip\n"
        "    Short.\n"
    )
    text = (build_ok(tmp_path, {"index.md": page}) / "index.md").read_text()
    assert ":::caution[Read `run()` first]\nText.\n\n```python\nx = 1\n```\n:::" in text
    assert ":::tip\nShort.\n:::" in text
    assert "\nAfter.\n" in text


def test_nested_admonition_gets_a_longer_fence(tmp_path: Path):
    page = '# P\n\n!!! note "Outer"\n    !!! tip\n        Inner.\n'
    text = (build_ok(tmp_path, {"index.md": page}) / "index.md").read_text()
    assert "::::note[Outer]\n:::tip\nInner.\n:::\n::::" in text


def test_collapsible_admonition_becomes_details(tmp_path: Path):
    page = '# P\n\n??? success "Solution"\n    Answer.\n'
    text = (build_ok(tmp_path, {"index.md": page}) / "index.md").read_text()
    assert "<details>\n<summary>Solution</summary>\n\nAnswer.\n\n</details>" in text


def test_admonition_syntax_inside_a_code_block_is_kept(tmp_path: Path):
    page = '# P\n\n```text\n!!! note "x"\n    y\n```\n'
    text = (build_ok(tmp_path, {"index.md": page}) / "index.md").read_text()
    assert '```text\n!!! note "x"\n    y\n```' in text


def test_unknown_admonition_type_fails(tmp_path: Path):
    page = "# P\n\n!!! abstract\n    x\n"
    assert "abstract" in build_fails(tmp_path, {"index.md": page})


def test_skip_markers_and_the_header_comment_are_removed(tmp_path: Path):
    page = (
        "# P\n<!--\nNote to authors.\n-->\n\n"
        "<!-- docs-test: skip -->\n```python\nx = 1\n```\n"
    )
    text = (build_ok(tmp_path, {"index.md": page}) / "index.md").read_text()
    assert "<!--" not in text
    assert "```python\nx = 1\n```" in text


# ---------------------------------------------------------------------------
# Links
# ---------------------------------------------------------------------------


def test_page_links_become_site_urls_with_the_base(tmp_path: Path):
    pages = {
        "index.md": "# Home\n\nSee [s](concepts/sources.md) and [t](tutorial/index.md).\n",
        "concepts/sources.md": "# Sources\n\nBack to [home](../index.md#top).\n",
        "tutorial/index.md": "# Tutorial\n\nText.\n",
    }
    out = build_ok(tmp_path, pages, base="/orcapod-python/")
    home = (out / "index.md").read_text()
    assert "[s](/orcapod-python/concepts/sources/)" in home
    assert "[t](/orcapod-python/tutorial/)" in home
    sources = (out / "concepts" / "sources.md").read_text()
    assert "[home](/orcapod-python/#top)" in sources


def test_mkdocs_anchors_become_starlight_anchors(tmp_path: Path):
    pages = {
        "index.md": (
            "# Home\n\n"
            "[a](concepts/function-pods.md#pipelinejob-database-backed-cached-execution)"
            " [b](#same-page-twice_1)\n\n"
            "## Same page twice\n\n## Same page twice\n"
        ),
        "concepts/function-pods.md": (
            "# FP\n\n## `PipelineJob` -- database-backed, cached execution\n"
        ),
    }
    home = (build_ok(tmp_path, pages) / "index.md").read_text()
    assert (
        "/concepts/function-pods/#pipelinejob----database-backed-cached-execution"
        in (home)
    )
    assert "[b](#same-page-twice-1)" in home


def test_links_in_code_are_not_rewritten(tmp_path: Path):
    page = "# P\n\n`[x](nope.md)`\n\n```markdown\n[y](nope.md)\n```\n"
    text = (build_ok(tmp_path, {"index.md": page}) / "index.md").read_text()
    assert "`[x](nope.md)`" in text
    assert "[y](nope.md)" in text


def test_external_links_are_kept(tmp_path: Path):
    page = "# P\n\n[x](https://example.org/a.md) [m](mailto:a@b.c)\n"
    text = (build_ok(tmp_path, {"index.md": page}) / "index.md").read_text()
    assert "[x](https://example.org/a.md) [m](mailto:a@b.c)" in text


def test_linked_file_is_copied(tmp_path: Path):
    pages = {
        "tutorial/index.md": "# T\n\nDownload [`data.py`](data.py).\n",
        "tutorial/data.py": "print('hi')\n",
    }
    docs = make_docs(tmp_path, pages)
    proc, out, public = run_build(docs, tmp_path, "/b/")
    assert proc.returncode == 0, proc.stderr
    assert (
        "[`data.py`](/b/tutorial/data.py)"
        in (out / "tutorial" / "index.md").read_text()
    )
    assert (public / "tutorial" / "data.py").read_text() == "print('hi')\n"


@pytest.mark.parametrize(
    ("link", "message"),
    [
        ("missing.md", "is not a published page"),
        ("../dev/notes.md", "is not a published page"),
        ("missing.py", "does not exist"),
        ("../../outside.md", "leaves docs/"),
    ],
)
def test_a_broken_link_fails(tmp_path: Path, link: str, message: str):
    pages = {"concepts/a.md": f"# A\n\n[x]({link})\n", "dev/notes.md": "# Notes\n"}
    stderr = build_fails(tmp_path, pages)
    assert "docs/concepts/a.md" in stderr
    assert message in stderr


# ---------------------------------------------------------------------------
# What is published
# ---------------------------------------------------------------------------


def test_excluded_folders_are_not_published(tmp_path: Path):
    out = build_ok(tmp_path, {"index.md": "# H\n", "dev/plan.md": "# Plan\n"})
    assert not (out / "dev").exists()


def test_an_unknown_top_level_entry_fails(tmp_path: Path):
    stderr = build_fails(tmp_path, {"index.md": "# H\n", "guides/a.md": "# A\n"})
    assert "guides" in stderr


def test_a_folder_inside_a_published_folder_fails(tmp_path: Path):
    stderr = build_fails(tmp_path, {"concepts/more/a.md": "# A\n"})
    assert "concepts/more" in stderr


# ---------------------------------------------------------------------------
# API pages
# ---------------------------------------------------------------------------


def test_api_page_is_generated_from_the_source(tmp_path: Path):
    pages = {"api/types.md": "# Types\n\nIntro.\n\n::: orcapod.types.Schema\n"}
    text = (build_ok(tmp_path, pages) / "api" / "types.md").read_text()
    assert '<a id="orcapod.types.Schema"></a>\n\n## `Schema`' in text
    assert '<a id="orcapod.types.Schema.merge"></a>' in text
    assert "**Raises:**" in text
    assert "Intro." in text


def test_api_page_naming_a_missing_object_fails(tmp_path: Path):
    pages = {"api/types.md": "# Types\n\n::: orcapod.types.NoSuchThing\n"}
    assert "orcapod.types.NoSuchThing" in build_fails(tmp_path, pages)


def test_a_removed_page_leaves_the_site(tmp_path: Path):
    docs = make_docs(tmp_path, {"index.md": "# H\n", "concepts/a.md": "# A\n"})
    proc, out, _ = run_build(docs, tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert (out / "concepts" / "a.md").is_file()
    (docs / "concepts" / "a.md").unlink()
    proc, out, _ = run_build(docs, tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert not (out / "concepts").exists()
    assert (out / "index.md").is_file()
