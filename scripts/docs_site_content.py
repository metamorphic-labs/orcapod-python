"""Build the content of the Starlight documentation site from ``docs/``.

``docs/`` is the single source of the documentation: the sample tests execute its
pages as they are. The Starlight site under ``site/`` never holds a copy in git. This
script writes the site's content collection (``site/src/content/docs/``) and its
static files (``site/public/``) from ``docs/`` on every build; both are generated and
ignored by git.

The conversion is mechanical and deterministic. For each published page:

- The first line, ``# <title>``, becomes the ``title`` front matter Starlight needs
  (Starlight renders the title as the page heading, so the line is removed).
- An admonition (``!!! note "Title"`` and its indented body) becomes a Starlight aside
  (``:::note[Title]``); ``warning`` maps to Starlight's ``caution``. A collapsible
  admonition (``??? success "Solution"``) becomes a ``<details>`` block.
- A link to another page (``../concepts/sources.md#derivedsource``) becomes the page's
  site URL (``/concepts/sources/#derivedsource``). A link to another file under
  ``docs/`` (``workshop_data.py``) copies the file to the site's static files.
- The ``<!-- docs-test: skip -->`` markers of the sample tests are removed.

A link to a page or file that does not exist, or to a page that is not published, is an
error: the build fails, as ``mkdocs build --strict`` did. The API pages
(``docs/api/*.md``) are converted the same way, after ``docs_site_api.py`` has replaced
each ``::: <object path>`` line with the reference generated from the source.

Usage:

    uv run python scripts/docs_site_content.py

The Astro build runs it (``site/astro.config.mjs``); there is no need to run it by hand.
"""

from __future__ import annotations

import argparse
import html
import json
import posixpath
import re
import sys
import unicodedata
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = REPO_ROOT / "docs"
SITE_DIR = REPO_ROOT / "site"

# What the site publishes from ``docs/``: these top-level pages, and every ``*.md``
# page directly inside these folders.
PUBLISHED_FILES = ("index.md", "getting-started.md", "troubleshooting.md")
PUBLISHED_FOLDERS = ("tutorial", "concepts", "reference", "api")
# What it never publishes: developer material. Every other top-level entry of ``docs/``
# is an error, so that publishing a new folder (or not) is a decision.
EXCLUDED = ("dev",)

# mkdocs admonition types and the Starlight aside each becomes.
ASIDE_TYPES = {
    "note": "note",
    "info": "note",
    "tip": "tip",
    "hint": "tip",
    "success": "tip",
    "warning": "caution",
    "caution": "caution",
    "attention": "caution",
    "danger": "danger",
    "error": "danger",
}

SKIP_MARKER = "<!-- docs-test: skip -->"
# A comment right below the title is a note to the page's authors (the API pages say
# that they are generator input); it is not shown.
_HEADER_COMMENT = re.compile(r"\A<!--.*?-->\n", re.DOTALL)

_ADMONITION = re.compile(
    r'^(?P<indent> *)(?P<marker>!!!|\?\?\?\+?) +(?P<kind>[a-z]+)(?: +"(?P<title>[^"]*)")? *$'
)
_FENCE = re.compile(r"^ *(?P<fence>`{3,}|~{3,})")
_DIRECTIVE_COLONS = re.compile(r"^ *(:{3,})")
_INLINE_LINK = re.compile(r"(\]\()(?P<target>[^)\s]+)(?P<rest>(?: +\"[^\"]*\")?\))")
_INLINE_CODE = re.compile(r"(`+)(?:.*?)\1")
_SCHEME = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")


_HEADING = re.compile(r"^#{1,6} +(?P<text>.+?) *#* *$")
_MARKUP = re.compile(r"[`*]|\[(?P<label>[^\]]*)\]\([^)]*\)")


def _heading_text(markdown: str) -> str:
    """The plain text of a heading: code spans and links kept as their text."""
    return _MARKUP.sub(lambda m: m.group("label") or "", markdown)


def mkdocs_slug(text: str) -> str:
    """The anchor mkdocs (Python-Markdown ``toc``) gives a heading with this text."""
    value = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    value = re.sub(r"[^\w\s-]", "", value).strip().lower()
    return re.sub(r"[-\s]+", "-", value)


def starlight_slug(text: str) -> str:
    """The anchor Starlight (``github-slugger``) gives a heading with this text."""
    return re.sub(r"[^\w\- ]", "", text.lower()).replace(" ", "-")


def heading_anchors(markdown: str) -> dict[str, str]:
    """Map each heading's mkdocs anchor to its Starlight anchor, for one page.

    Args:
        markdown: The page body, without its title line.

    Returns:
        ``{mkdocs anchor: Starlight anchor}`` for every heading outside code blocks,
        repeated headings numbered as each tool numbers them.
    """
    anchors: dict[str, str] = {}
    seen_mkdocs: set[str] = set()
    seen_starlight: dict[str, int] = {}
    fence: str | None = None
    for line in markdown.splitlines():
        if fence is not None:
            if _is_fence_close(line, fence):
                fence = None
            continue
        fence_match = _FENCE.match(line)
        if fence_match:
            fence = fence_match.group("fence")
            continue
        match = _HEADING.match(line)
        if match is None:
            continue
        text = _heading_text(match.group("text"))
        mk = mkdocs_slug(text)
        candidate, n = mk, 0
        while candidate in seen_mkdocs:
            n += 1
            candidate = f"{mk}_{n}"
        seen_mkdocs.add(candidate)
        sl = starlight_slug(text)
        count = seen_starlight.get(sl, 0)
        seen_starlight[sl] = count + 1
        anchors[candidate] = f"{sl}-{count}" if count else sl
    return anchors


class ContentError(Exception):
    """A documentation page cannot be converted (a broken link, an unknown admonition)."""


@dataclass(frozen=True)
class Page:
    """A published page: its path relative to ``docs/`` and its Markdown text."""

    path: str
    text: str


def page_url(path: str, base: str = "/") -> str:
    """Return the site URL of the page at ``path`` (relative to ``docs/``).

    Args:
        path: The page's path relative to ``docs/``, e.g. ``concepts/sources.md``.
        base: The site's base path (Astro ``base``).

    Returns:
        The URL, ending with a slash: ``/concepts/sources/``; an ``index.md`` is its
        directory's URL.
    """
    stem = path.removesuffix(".md")
    if stem == "index":
        stem = ""
    elif stem.endswith("/index"):
        stem = stem.removesuffix("/index")
    prefix = base.rstrip("/")
    return f"{prefix}/{stem}/" if stem else f"{prefix}/"


def published_paths(docs_dir: Path) -> list[str]:
    """Return the published pages under ``docs_dir``, sorted, relative to it.

    Raises:
        ContentError: If ``docs_dir`` holds a top-level entry that is neither published
            nor excluded, or a published folder holds a folder.
    """
    unknown = sorted(
        entry.name
        for entry in docs_dir.iterdir()
        if not entry.name.startswith(".")
        and entry.name not in (*PUBLISHED_FILES, *PUBLISHED_FOLDERS, *EXCLUDED)
    )
    nested = sorted(
        f"{folder}/{entry.name}"
        for folder in PUBLISHED_FOLDERS
        if (docs_dir / folder).is_dir()
        for entry in (docs_dir / folder).iterdir()
        if entry.is_dir() and not entry.name.startswith((".", "__"))
    )
    if unknown or nested:
        raise ContentError(
            "docs/ holds entries that are neither published nor excluded: "
            + ", ".join(unknown + nested)
            + " (add each to PUBLISHED_FILES, PUBLISHED_FOLDERS or EXCLUDED in"
            + " scripts/docs_site_content.py)"
        )
    found = [name for name in PUBLISHED_FILES if (docs_dir / name).is_file()]
    for folder in PUBLISHED_FOLDERS:
        found += [
            p.relative_to(docs_dir).as_posix() for p in (docs_dir / folder).glob("*.md")
        ]
    return sorted(found)


def split_title(text: str) -> tuple[str, str]:
    """Split a page into its title (the leading ``# `` line) and the rest.

    Raises:
        ContentError: If the page does not start with a level-1 heading.
    """
    first, _, rest = text.partition("\n")
    if not first.startswith("# "):
        raise ContentError("the page does not start with a '# <title>' line")
    return first.removeprefix("# ").strip(), rest.lstrip("\n")


def _is_fence_close(line: str, fence: str) -> bool:
    stripped = line.strip()
    return stripped.startswith(fence[0] * len(fence)) and set(stripped) == {fence[0]}


def _map_lines_outside_code(
    lines: Sequence[str], convert: Callable[[str], str]
) -> list[str]:
    """Apply ``convert`` to every line that is not inside a fenced code block."""
    out: list[str] = []
    fence: str | None = None
    for line in lines:
        if fence is None:
            match = _FENCE.match(line)
            if match:
                fence = match.group("fence")
                out.append(line)
            else:
                out.append(convert(line))
        else:
            if _is_fence_close(line, fence):
                fence = None
            out.append(line)
    return out


def _admonition_body(lines: Sequence[str], start: int, indent: int) -> int:
    """Return the index one past the last line of the body that starts at ``start``."""
    body_indent = " " * (indent + 4)
    end = start
    last_content = start
    while end < len(lines):
        line = lines[end]
        if line.strip() and not line.startswith(body_indent):
            break
        if line.strip():
            last_content = end + 1
        end += 1
    return last_content


def convert_admonitions(lines: Sequence[str]) -> list[str]:
    """Convert mkdocs admonitions to Starlight asides and ``<details>`` blocks.

    Raises:
        ContentError: For an admonition type with no Starlight equivalent.
    """
    out: list[str] = []
    i = 0
    fence: str | None = None
    while i < len(lines):
        line = lines[i]
        if fence is not None:
            if _is_fence_close(line, fence):
                fence = None
            out.append(line)
            i += 1
            continue
        fence_match = _FENCE.match(line)
        if fence_match:
            fence = fence_match.group("fence")
            out.append(line)
            i += 1
            continue
        match = _ADMONITION.match(line)
        if match is None:
            out.append(line)
            i += 1
            continue
        indent = len(match.group("indent"))
        kind = match.group("kind")
        title = match.group("title")
        end = _admonition_body(lines, i + 1, indent)
        body = [raw[indent + 4 :] if raw.strip() else "" for raw in lines[i + 1 : end]]
        body = convert_admonitions(body)
        pad = " " * indent
        if match.group("marker").startswith("???"):
            summary = html.escape(title if title else kind.capitalize())
            block = [
                "<details>",
                f"<summary>{summary}</summary>",
                "",
                *body,
                "",
                "</details>",
            ]
        else:
            if kind not in ASIDE_TYPES:
                raise ContentError(f"admonition type {kind!r} has no Starlight aside")
            inner = max(
                (len(m.group(1)) for m in map(_DIRECTIVE_COLONS.match, body) if m),
                default=2,
            )
            colons = ":" * max(3, inner + 1)
            label = f"[{title}]" if title else ""
            block = [f"{colons}{ASIDE_TYPES[kind]}{label}", *body, colons]
        out.extend(pad + b if b else "" for b in block)
        i = end
    return out


def _convert_link_targets(line: str, resolve: Callable[[str], str]) -> str:
    """Rewrite the targets of inline links on ``line``, outside inline code spans."""
    pieces: list[str] = []
    pos = 0
    for code in _INLINE_CODE.finditer(line):
        pieces.append(_rewrite_links(line[pos : code.start()], resolve))
        pieces.append(code.group(0))
        pos = code.end()
    pieces.append(_rewrite_links(line[pos:], resolve))
    return "".join(pieces)


def _rewrite_links(text: str, resolve: Callable[[str], str]) -> str:
    return _INLINE_LINK.sub(
        lambda m: m.group(1) + resolve(m.group("target")) + m.group("rest"), text
    )


class LinkResolver:
    """Turns a link written on a ``docs/`` page into a site URL.

    Args:
        docs_dir: The ``docs/`` directory.
        pages: The published pages, relative to ``docs_dir``.
        base: The site's base path.
    """

    def __init__(self, docs_dir: Path, pages: Iterable[Page], base: str) -> None:
        self._docs_dir: Path = docs_dir
        self._anchors: dict[str, dict[str, str]] = {
            p.path: heading_anchors(split_title(p.text)[1]) for p in pages
        }
        self._base: str = base
        self.assets: set[str] = set()

    def _fragment(self, page: str, fragment: str) -> str:
        """Translate an mkdocs anchor on ``page`` to its Starlight anchor."""
        return self._anchors[page].get(fragment, fragment)

    def resolve(self, page: str, target: str) -> str:
        """Return the site URL for ``target`` as written on ``page``.

        Raises:
            ContentError: If the target is a page that is not published or a file that
                does not exist.
        """
        if target.startswith("#"):
            return "#" + self._fragment(page, target[1:])
        if target.startswith("/") or _SCHEME.match(target):
            return target
        path, sep, fragment = target.partition("#")
        resolved = posixpath.normpath(posixpath.join(posixpath.dirname(page), path))
        if resolved.startswith("../"):
            raise ContentError(f"link {target!r} leaves docs/")
        if resolved.endswith(".md"):
            if resolved not in self._anchors:
                raise ContentError(
                    f"link {target!r}: {resolved} is not a published page"
                )
            if fragment:
                fragment = self._fragment(resolved, fragment)
            return page_url(resolved, self._base) + sep + fragment
        if not (self._docs_dir / resolved).is_file():
            raise ContentError(f"link {target!r}: docs/{resolved} does not exist")
        self.assets.add(resolved)
        return self._base.rstrip("/") + "/" + resolved + sep + fragment


def convert_page(page: Page, resolver: LinkResolver) -> str:
    """Return the Starlight form of ``page``.

    Raises:
        ContentError: If the page cannot be converted.
    """
    title, body = split_title(page.text)
    body = _HEADER_COMMENT.sub("", body).lstrip("\n")
    lines = [line for line in body.splitlines() if line.strip() != SKIP_MARKER]
    lines = convert_admonitions(lines)
    lines = _map_lines_outside_code(
        lines,
        lambda line: _convert_link_targets(
            line, lambda target: resolver.resolve(page.path, target)
        ),
    )
    front_matter = ["---", f"title: {json.dumps(title, ensure_ascii=False)}", "---", ""]
    return "\n".join(front_matter + lines).rstrip("\n") + "\n"


def build_content(
    pages: Sequence[Page], docs_dir: Path, out_dir: Path, public_dir: Path, base: str
) -> None:
    """Write the converted ``pages`` to ``out_dir`` and linked files to ``public_dir``.

    Each directory ends up holding exactly what this build writes, so a page removed
    from ``docs/`` leaves the site.

    Raises:
        ContentError: If a page cannot be converted; its path is in the message.
    """
    converted: dict[str, str] = {}
    try:
        resolver = LinkResolver(docs_dir, pages, base)
    except ContentError as exc:
        raise ContentError(f"a page has no title: {exc}") from exc
    for page in pages:
        try:
            converted[page.path] = convert_page(page, resolver)
        except ContentError as exc:
            raise ContentError(f"docs/{page.path}: {exc}") from exc
    _sync_tree(out_dir, {path: text.encode() for path, text in converted.items()})
    _sync_tree(
        public_dir,
        {asset: (docs_dir / asset).read_bytes() for asset in sorted(resolver.assets)},
    )


def _sync_tree(directory: Path, files: dict[str, bytes]) -> None:
    """Make ``directory`` hold exactly ``files``, writing only what changed.

    A file that is unchanged is not rewritten, so ``astro dev`` reloads only the
    pages an edit changed.
    """
    directory.mkdir(parents=True, exist_ok=True)
    for existing in sorted(directory.rglob("*"), reverse=True):
        rel = existing.relative_to(directory).as_posix()
        if existing.is_file() and rel not in files:
            existing.unlink()
        elif existing.is_dir() and not any(existing.iterdir()):
            existing.rmdir()
    for rel, data in files.items():
        target = directory / rel
        if target.is_file() and target.read_bytes() == data:
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        _ = target.write_bytes(data)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build the documentation site content."
    )
    _ = parser.add_argument("--docs", type=Path, default=DOCS_DIR)
    _ = parser.add_argument(
        "--out", type=Path, default=SITE_DIR / "src" / "content" / "docs"
    )
    _ = parser.add_argument("--public", type=Path, default=SITE_DIR / "public")
    _ = parser.add_argument("--base", default="/")
    args = parser.parse_args(argv)
    docs_dir = cast(Path, args.docs)
    out_dir = cast(Path, args.out)
    public_dir = cast(Path, args.public)
    base = cast(str, args.base)

    # Run as a script, scripts/ is on sys.path; the API generator imports griffe, so
    # it is imported only when the content is built.
    from docs_site_api import (  # pyright: ignore[reportImplicitRelativeImport]
        ApiPageError,
        expand_api_page,
    )

    pages: list[Page] = []
    try:
        for path in published_paths(docs_dir):
            text = (docs_dir / path).read_text(encoding="utf-8")
            if path.startswith("api/"):
                try:
                    text = expand_api_page(text, docs_dir / "api", base=base)
                except ApiPageError as exc:
                    raise ContentError(f"docs/{path}: {exc}") from exc
            pages.append(Page(path, text))
        build_content(pages, docs_dir, out_dir, public_dir, base)
    except ContentError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"wrote {len(pages)} pages to {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
