"""The documentation site's content build (``scripts/docs_site_content.py``).

The Starlight site under ``site/`` is built from ``docs/`` by that script, which
converts every published page and generates the API reference from the source. It
fails on a broken link, a page that is neither published nor excluded, an object an API
page names that does not exist, or a docstring griffe reports a problem with; these
tests hold it to that, and to the site's address (``--site-settings``), without Node. The site build itself (``pnpm build`` in
``site/``, which also validates every link and anchor) runs in the ``docs-site``
workflow.

Every test drives the script as a command, the way the site build runs it.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import cast

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "docs_site_content.py"


def run_build(
    docs: Path, tmp_path: Path, base: str = "/", hero: bool = False
) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
    """Run the content build; without ``hero``, the home page is built as a plain page."""
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
            *([] if hero else ["--no-hero"]),
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


def build_fails(tmp_path: Path, pages: dict[str, str], hero: bool = False) -> str:
    proc, _, _ = run_build(make_docs(tmp_path, pages), tmp_path, hero=hero)
    assert proc.returncode != 0, "the build should have failed"
    return proc.stderr


def front_matter(text: str) -> dict[str, object]:
    """The front matter of a generated page: each line ``key: <JSON>``."""
    lines = text.split("---\n")[1].splitlines()
    return {
        key: json.loads(value)
        for key, _, value in (line.partition(": ") for line in lines)
    }


# ---------------------------------------------------------------------------
# The real documentation
# ---------------------------------------------------------------------------


def test_the_documentation_builds(tmp_path: Path):
    """Every page converts, every link resolves, every API object and docstring renders."""
    proc, out, public = run_build(
        REPO_ROOT / "docs", tmp_path, "/orcapod-python/", hero=True
    )
    assert proc.returncode == 0, proc.stderr
    assert front_matter((out / "index.md").read_text())["template"] == "splash"
    assert (out / "api" / "operators.md").is_file()
    assert (public / "tutorial" / "workshop_data.py").is_file()
    assert not (out / "dev").exists()
    api = (out / "api" / "operators.md").read_text(encoding="utf-8")
    assert '<a id="orcapod.core.operators.Join"></a>' in api
    assert "\n::: " not in api


def test_the_build_is_deterministic(tmp_path: Path):
    first, out_a, _ = run_build(REPO_ROOT / "docs", tmp_path / "a", hero=True)
    second, out_b, _ = run_build(REPO_ROOT / "docs", tmp_path / "b", hero=True)
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


# ---------------------------------------------------------------------------
# The home page's hero
# ---------------------------------------------------------------------------

HERO_PAGES = {
    "index.md": "# Orcapod\n\nBody.\n",
    "getting-started.md": "# Getting Started\n",
    "tutorial/index.md": "# Tutorial\n",
}


def test_the_home_page_gets_the_hero(tmp_path: Path):
    """Splash template, the header's logo, buttons that resolve under the base."""
    docs = make_docs(tmp_path, HERO_PAGES)
    proc, out, _ = run_build(docs, tmp_path, "/orcapod-python/", hero=True)
    assert proc.returncode == 0, proc.stderr
    home = (out / "index.md").read_text()
    assert home.endswith("---\n\nBody.\n")
    meta = front_matter(home)
    assert meta["title"] == "Orcapod"
    assert meta["template"] == "splash"
    hero = cast(dict[str, object], meta["hero"])
    tagline = (
        "A Python library for building highly reproducible scientific data pipelines."
    )
    assert hero["tagline"] == tagline
    image = cast(dict[str, str], hero["image"])
    assert image["alt"] == ""
    logo = REPO_ROOT / "site" / "src" / "assets" / "orcapod-icon.png"
    assert (out / image["file"]).resolve() == logo
    assert hero["actions"] == [
        {
            "text": "Get started",
            "link": "/orcapod-python/getting-started/",
            "variant": "primary",
            "icon": "right-arrow",
        },
        {
            "text": "Tutorial",
            "link": "/orcapod-python/tutorial/",
            "variant": "secondary",
        },
        {
            "text": "GitHub",
            "link": "https://github.com/metamorphic-labs/orcapod-python",
            "variant": "minimal",
            "icon": "github",
        },
    ]
    # Only the home page has it.
    assert "hero" not in front_matter((out / "getting-started.md").read_text())


def test_the_tagline_is_the_home_page_s_own_wording():
    """The tagline shortens the first sentence of docs/index.md; it claims nothing new."""
    home = (REPO_ROOT / "docs" / "index.md").read_text(encoding="utf-8")
    first = " ".join(home.split())
    assert (
        "Python library for building highly reproducible scientific data pipelines."
        in first
    )


def test_a_hero_button_to_an_unpublished_page_fails(tmp_path: Path):
    pages = {"index.md": "# Orcapod\n", "tutorial/index.md": "# Tutorial\n"}
    stderr = build_fails(tmp_path, pages, hero=True)
    assert "docs/index.md" in stderr
    assert "getting-started.md is not a published page" in stderr


def test_the_hero_needs_a_home_page(tmp_path: Path):
    pages = {"getting-started.md": "# Getting Started\n", "tutorial/index.md": "# T\n"}
    assert "the hero needs a home page" in build_fails(tmp_path, pages, hero=True)


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


def test_a_repeated_heading_does_not_take_an_existing_anchor(tmp_path: Path):
    # github-slugger gives the third heading ``repeat-1-1``: ``repeat-1`` is taken.
    page = (
        "# P\n\n[b](#repeat_1) [c](#repeat-1)\n\n"
        "## Repeat\n\n## Repeat\n\n## Repeat-1\n"
    )
    text = (build_ok(tmp_path, {"index.md": page}) / "index.md").read_text()
    assert "[b](#repeat-1) [c](#repeat-1-1)" in text


def test_reference_definitions_are_rewritten(tmp_path: Path):
    pages = {
        "index.md": (
            "# Home\n\nSee [s][src] and [d][data].\n\n"
            '[src]: concepts/sources.md#top "Sources"\n'
            "  [data]: <tutorial/data.py>\n"
            "[ext]: https://example.org/a.md\n"
            "\n```markdown\n[x]: nope.md\n```\n"
        ),
        "concepts/sources.md": "# Sources\n\n## Top\n",
        "tutorial/data.py": "x = 1\n",
    }
    text = (build_ok(tmp_path, pages, base="/b/") / "index.md").read_text()
    assert '[src]: /b/concepts/sources/#top "Sources"\n' in text
    assert "  [data]: </b/tutorial/data.py>\n" in text
    assert "[ext]: https://example.org/a.md\n" in text
    assert "[x]: nope.md" in text


def test_a_broken_reference_definition_fails(tmp_path: Path):
    stderr = build_fails(tmp_path, {"index.md": "# H\n\n[x]: missing.md\n"})
    assert "is not a published page" in stderr


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


def test_the_favicon_is_written_with_the_linked_files(tmp_path: Path):
    """The site's favicon (orcapod.org's, in site/src/assets) is in public/ on every build."""
    docs = make_docs(tmp_path, {"index.md": "# Home\n"})
    proc, _, public = run_build(docs, tmp_path)
    assert proc.returncode == 0, proc.stderr
    favicon = REPO_ROOT / "site" / "src" / "assets" / "favicon.ico"
    assert (public / "favicon.ico").read_bytes() == favicon.read_bytes()


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


def test_a_linked_file_in_an_excluded_folder_fails(tmp_path: Path):
    pages = {
        "concepts/a.md": "# A\n\n[notes](../dev/notes.txt)\n",
        "dev/notes.txt": "developer notes\n",
    }
    docs = make_docs(tmp_path, pages)
    proc, _, public = run_build(docs, tmp_path)
    assert proc.returncode != 0, "the build should have failed"
    assert "docs/concepts/a.md" in proc.stderr
    assert "dev/notes.txt is not published" in proc.stderr
    assert not (public / "dev").exists()


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


def test_positional_only_separator_comes_before_keyword_only(tmp_path: Path):
    # ``map_tags(self, name_map=None, /, *, drop_unmapped=False, ...)``
    pages = {
        "api/streams.md": "# Streams\n\n::: orcapod.core.streams.base.StreamBase.map_tags\n"
    }
    text = (build_ok(tmp_path, pages) / "api" / "streams.md").read_text()
    assert "name_map: Mapping[str, str] | None = None,\n    /,\n    *,\n" in text
    assert "*,\n    /," not in text


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


# ---------------------------------------------------------------------------
# The site's address
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("base", "prefix"), [("/", ""), ("/other/", "/other")])
def test_links_and_anchors_follow_the_base(tmp_path: Path, base: str, prefix: str):
    pages = {
        "index.md": (
            "# Home\n\n"
            "[s](concepts/sources.md#derivedsource-tags_as_data) [h](#top)"
            " [d](tutorial/data.py)\n"
        ),
        "concepts/sources.md": (
            "# Sources\n\n[home](../index.md)\n\n## `DerivedSource` -- tags_as_data\n"
        ),
        "tutorial/data.py": "x = 1\n",
    }
    home = (build_ok(tmp_path, pages, base=base) / "index.md").read_text()
    assert f"[s]({prefix}/concepts/sources/#derivedsource----tags_as_data)" in home
    assert "[h](#top)" in home
    assert f"[d]({prefix}/tutorial/data.py)" in home
    sources = (tmp_path / "out" / "concepts" / "sources.md").read_text()
    assert f"[home]({prefix}/)" in sources


@pytest.mark.parametrize("given", ["other", "/other", "other/", "/other/"])
def test_the_base_is_normalized(tmp_path: Path, given: str):
    pages = {"index.md": "# H\n\n[s](concepts/a.md)\n", "concepts/a.md": "# A\n"}
    home = (build_ok(tmp_path, pages, base=given) / "index.md").read_text()
    assert "[s](/other/concepts/a/)" in home


def test_an_invalid_base_fails(tmp_path: Path):
    proc, _, _ = run_build(make_docs(tmp_path, {"index.md": "# H\n"}), tmp_path, "/../")
    assert proc.returncode != 0
    assert "--base" in proc.stderr


def site_settings(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    environ = {
        k: v
        for k, v in os.environ.items()
        if k not in ("DOCS_SITE_URL", "DOCS_SITE_BASE")
    }
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--site-settings"],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        env={**environ, **env},
    )


def settings_ok(env: dict[str, str]) -> dict[str, str]:
    proc = site_settings(env)
    assert proc.returncode == 0, proc.stderr
    return cast(dict[str, str], json.loads(proc.stdout))


@pytest.mark.parametrize(
    "env", [{}, {"DOCS_SITE_URL": "", "DOCS_SITE_BASE": ""}], ids=["unset", "empty"]
)
def test_the_default_address(env: dict[str, str]):
    assert settings_ok(env) == {
        "site": "https://metamorphic-labs.github.io",
        "base": "/orcapod-python/",
    }


def test_a_custom_domain():
    env = {"DOCS_SITE_URL": "https://docs.example.org", "DOCS_SITE_BASE": "/"}
    assert settings_ok(env) == {"site": "https://docs.example.org", "base": "/"}


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({"DOCS_SITE_URL": "https://docs.example.org/"}, "https://docs.example.org"),
        ({"DOCS_SITE_URL": "HTTP://localhost:8080"}, "http://localhost:8080"),
        ({"DOCS_SITE_BASE": "docs"}, "/docs/"),
        ({"DOCS_SITE_BASE": "/a/b"}, "/a/b/"),
    ],
)
def test_the_address_is_normalized(env: dict[str, str], expected: str):
    settings = settings_ok(env)
    assert expected in (settings["site"], settings["base"])


@pytest.mark.parametrize(
    "env",
    [
        {"DOCS_SITE_URL": "ftp://docs.example.org"},
        {"DOCS_SITE_URL": "docs.example.org"},
        {"DOCS_SITE_URL": "https://docs.example.org/orcapod/"},
        {"DOCS_SITE_URL": "https://docs.example.org?x=1"},
        {"DOCS_SITE_URL": "https://user@docs.example.org"},
        {"DOCS_SITE_URL": "https://docs.example.org:port"},
        {"DOCS_SITE_URL": " https://docs.example.org"},
        {"DOCS_SITE_BASE": "https://docs.example.org/"},
        {"DOCS_SITE_BASE": "/docs?x=1"},
        {"DOCS_SITE_BASE": "/docs#top"},
        {"DOCS_SITE_BASE": "/a/../b/"},
        {"DOCS_SITE_BASE": "/a//b/"},
        {"DOCS_SITE_BASE": "/my docs/"},
        {"DOCS_SITE_BASE": "/docs/\n"},
    ],
)
def test_an_invalid_address_is_refused(env: dict[str, str]):
    proc = site_settings(env)
    assert proc.returncode != 0
    assert proc.stdout == ""
    (variable,) = env
    assert variable in proc.stderr


def test_the_site_shows_code_comments_as_written():
    """A printed value that looks like a file name (`# 1.58`) stays in its code block.

    Expressive Code would otherwise take it out of the code as the block's title. The site
    build itself also fails on any titled code block.
    """
    config = (REPO_ROOT / "site" / "astro.config.mjs").read_text(encoding="utf-8")
    assert "frames: { extractFileNameFromCode: false }" in config
    page = REPO_ROOT / "docs" / "tutorial" / "02-function-pods.md"
    assert "print(to_mm(depth_um=1580))\n# 1.58\n```" in page.read_text(
        encoding="utf-8"
    )
