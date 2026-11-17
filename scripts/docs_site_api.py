"""Generate the API reference pages of the documentation site from the source code.

Each page under ``docs/api/`` names the objects it documents with a line
``::: <object path>`` (the mkdocstrings syntax the pages already used). This module
replaces each such line with the object's reference, generated from the source by
griffe (static analysis; the package is not imported): the signature, the Google-style
docstring with its sections, the base classes, and the public members that have a
docstring, in source order. Nothing on an API page is written by hand except the
introduction the page itself has.

Rules (as mkdocstrings applied them):

- A member is shown when its name does not start with ``_`` and it has a docstring;
  inherited members are not repeated.
- A class's signature is its ``__init__`` signature without ``self``.
- Every object gets an anchor equal to its object path as written on the page
  (``#orcapod.core.operators.Join``), so a link to it is stable whatever the headings
  say. A type in a signature or a parameter list links to the object's reference when
  that object is documented on one of the pages.
- A docstring that griffe reports a problem with (a parameter documented under
  ``Args:`` that the signature does not have, a section it cannot parse) is an error,
  as it was under ``mkdocs build --strict``.

``docs_site_content.py`` calls ``expand_api_page`` for every API page; the module has
no command of its own.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path

import griffe

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = REPO_ROOT / "src"
API_DIR = REPO_ROOT / "docs" / "api"
PACKAGE = "orcapod"

_DIRECTIVE = re.compile(r"^::: +(?P<path>[\w.]+) *$")
_SIGNATURE_WIDTH = 88

# Google docstring admonition titles and the Starlight aside each becomes.
_ASIDES = {
    "note": "note",
    "notes": "note",
    "tip": "tip",
    "warning": "caution",
    "warnings": "caution",
    "caution": "caution",
    "danger": "danger",
    "important": "caution",
}


class ApiPageError(Exception):
    """An API page names an object that does not exist, or a docstring is malformed."""


class _ProblemCollector(logging.Handler):
    """Collects the warnings griffe logs while parsing docstrings."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.problems: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.problems.append(record.getMessage())


@dataclass
class _Index:
    """Maps object paths (as written and canonical) to their site URL."""

    urls: dict[str, str] = field(default_factory=dict)

    def url(self, path: str | None) -> str | None:
        return self.urls.get(path) if path else None


@cache
def _package() -> griffe.Module:
    loaded = griffe.load(
        PACKAGE,
        search_paths=[str(SOURCE_DIR)],
        docstring_parser="google",
        resolve_aliases=True,
        resolve_external=False,
    )
    assert isinstance(loaded, griffe.Module)
    return loaded


def _directives(text: str) -> Iterator[str]:
    for line in text.splitlines():
        match = _DIRECTIVE.match(line)
        if match:
            yield match.group("path")


def _lookup(path: str) -> griffe.Object | griffe.Alias:
    package = _package()
    if not path.startswith(PACKAGE + "."):
        raise ApiPageError(f"::: {path}: not an object of the {PACKAGE} package")
    try:
        return package[path.removeprefix(PACKAGE + ".")]
    except KeyError:
        raise ApiPageError(f"::: {path}: no such object") from None


def _page_url(page: str, base: str) -> str:
    return f"{base.rstrip('/')}/api/{page}/" if page != "index" else f"{base.rstrip('/')}/api/"


@cache
def _index(base: str) -> _Index:
    """Index every object documented on any API page (and its documented members)."""
    index = _Index()
    for page_file in sorted(API_DIR.glob("*.md")):
        url = _page_url(page_file.stem, base)
        for path in _directives(page_file.read_text(encoding="utf-8")):
            obj = _lookup(path)
            for anchor, member in _anchored(path, obj):
                target = f"{url}#{anchor}"
                index.urls.setdefault(anchor, target)
                index.urls.setdefault(member.canonical_path, target)
    return index


def _members(obj: griffe.Object | griffe.Alias) -> list[griffe.Object | griffe.Alias]:
    """The members shown for ``obj``: public, documented, declared on it, in order."""
    if not obj.is_class:
        return []
    shown = []
    for name, member in obj.members.items():
        if name.startswith("_") or not member.docstring:
            continue
        if member.is_alias and member.inherited:
            continue
        shown.append(member)
    return sorted(shown, key=lambda m: m.lineno or 0)


def _anchored(
    path: str, obj: griffe.Object | griffe.Alias
) -> Iterator[tuple[str, griffe.Object | griffe.Alias]]:
    yield path, obj
    for member in _members(obj):
        yield f"{path}.{member.name}", member


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _annotation(expr: str | griffe.Expr | None, index: _Index) -> str:
    """Render a type annotation as inline code, linking documented names."""
    if expr is None:
        return ""
    if isinstance(expr, str):
        return _code(expr)
    parts: list[str] = []
    plain: list[str] = []
    for element in expr.iterate(flat=True):
        url = (
            index.url(element.canonical_path)
            if isinstance(element, griffe.ExprName)
            else None
        )
        if url is None:
            plain.append(str(element))
            continue
        if plain:
            parts.append(_code("".join(plain)))
            plain = []
        parts.append(f"[{_code(str(element))}]({url})")
    if plain:
        parts.append(_code("".join(plain)))
    return "".join(parts)


def _code(text: str) -> str:
    ticks = "``" if "`" in text else "`"
    pad = " " if text.startswith("`") or text.endswith("`") else ""
    return f"{ticks}{pad}{text}{pad}{ticks}"


def _parameters(obj: griffe.Object | griffe.Alias) -> list[griffe.Parameter]:
    if obj.is_class:
        init = obj.all_members.get("__init__")
        if init is None or not init.is_function:
            return []
        params = list(init.parameters)
    else:
        params = list(obj.parameters)
    if params and params[0].name in ("self", "cls"):
        params = params[1:]
    return params


def _signature(name: str, obj: griffe.Object | griffe.Alias) -> str:
    """Render the signature of a class or function as a Python code block."""
    prefix = ""
    if obj.is_class:
        prefix = "class "
    elif obj.is_function:
        prefix = "async def " if "async" in obj.labels else "def "
    params = _parameters(obj)
    rendered: list[str] = []
    seen_keyword_only = False
    previous_kind = None
    for param in params:
        kind = param.kind
        if (
            kind is griffe.ParameterKind.keyword_only
            and not seen_keyword_only
            and previous_kind is not griffe.ParameterKind.var_positional
        ):
            rendered.append("*")
        if kind is griffe.ParameterKind.keyword_only:
            seen_keyword_only = True
        if (
            previous_kind is griffe.ParameterKind.positional_only
            and kind is not griffe.ParameterKind.positional_only
        ):
            rendered.append("/")
        star = {
            griffe.ParameterKind.var_positional: "*",
            griffe.ParameterKind.var_keyword: "**",
        }.get(kind, "")
        text = f"{star}{param.name}"
        if param.annotation is not None:
            text += f": {param.annotation}"
        if param.default is not None:
            text += f" = {param.default}" if param.annotation is not None else f"={param.default}"
        rendered.append(text)
        previous_kind = kind
    if previous_kind is griffe.ParameterKind.positional_only:
        rendered.append("/")
    returns = ""
    if obj.is_function and obj.returns is not None:
        returns = f" -> {obj.returns}"
    one_line = f"{prefix}{name}({', '.join(rendered)}){returns}"
    if len(one_line) <= _SIGNATURE_WIDTH or not rendered:
        body = one_line
    else:
        lines = "".join(f"    {r},\n" for r in rendered)
        body = f"{prefix}{name}(\n{lines}){returns}"
    return f"```python\n{body}\n```"


def _indent_continuation(text: str) -> str:
    """Indent every line after the first by two spaces, for a list item."""
    lines = text.strip().splitlines()
    return "\n".join([lines[0], *("  " + ln if ln else "" for ln in lines[1:])]) if lines else ""


def _section(section: griffe.DocstringSection, index: _Index) -> str:
    kind = section.kind
    value = section.value
    if kind is griffe.DocstringSectionKind.text:
        return str(value).strip()
    if kind is griffe.DocstringSectionKind.admonition:
        admonition: griffe.DocstringAdmonition = value
        title = section.title or admonition.annotation or ""
        aside = _ASIDES.get(str(admonition.annotation or title).lower(), "note")
        return f":::{aside}[{title}]\n{admonition.description.strip()}\n:::"
    if kind is griffe.DocstringSectionKind.examples:
        out: list[str] = []
        for example_kind, text in value:
            if example_kind is griffe.DocstringSectionKind.examples or str(
                example_kind
            ).endswith("examples"):
                out.append(f"```pycon\n{text}\n```")
            else:
                out.append(str(text).strip())
        return "**Examples:**\n\n" + "\n\n".join(out)
    titles = {
        griffe.DocstringSectionKind.parameters: "Parameters",
        griffe.DocstringSectionKind.other_parameters: "Other parameters",
        griffe.DocstringSectionKind.attributes: "Attributes",
        griffe.DocstringSectionKind.returns: "Returns",
        griffe.DocstringSectionKind.yields: "Yields",
        griffe.DocstringSectionKind.receives: "Receives",
        griffe.DocstringSectionKind.raises: "Raises",
        griffe.DocstringSectionKind.warns: "Warns",
    }
    if kind in titles:
        items = []
        for element in value:
            name = getattr(element, "name", "") or ""
            annotation = _annotation(getattr(element, "annotation", None), index)
            head_parts = []
            if name and kind not in (
                griffe.DocstringSectionKind.raises,
                griffe.DocstringSectionKind.warns,
            ):
                head_parts.append(f"**{name}**")
            if annotation:
                head_parts.append(
                    f"({annotation})" if head_parts else annotation
                )
            default = getattr(element, "default", None)
            if default is not None and kind is griffe.DocstringSectionKind.parameters:
                head_parts.append(f"default {_code(str(default))}")
            head = " ".join(head_parts)
            description = _indent_continuation(element.description or "")
            items.append(f"- {head}: {description}" if head else f"- {description}")
        return f"**{titles[kind]}:**\n\n" + "\n".join(items)
    if kind is griffe.DocstringSectionKind.deprecated:
        return f":::caution[Deprecated {value.version or ''}]\n{value.description}\n:::"
    raise ApiPageError(f"docstring section {kind.value!r} is not rendered")


def _docstring(obj: griffe.Object | griffe.Alias, index: _Index) -> str:
    if not obj.docstring:
        return ""
    return "\n\n".join(_section(s, index) for s in obj.docstring.parse())


def _kind_label(obj: griffe.Object | griffe.Alias) -> str:
    labels = set(obj.labels)
    if obj.is_class:
        return "class"
    if "property" in labels:
        return "property"
    if obj.is_attribute:
        return "attribute"
    if "classmethod" in labels:
        return "classmethod"
    if "staticmethod" in labels:
        return "staticmethod"
    if obj.parent is not None and obj.parent.is_class:
        return "method"
    return "function"


def _render_object(
    path: str, obj: griffe.Object | griffe.Alias, level: int, index: _Index
) -> list[str]:
    name = obj.name
    out = [
        f'<a id="{path}"></a>',
        "",
        f"{'#' * level} {_code(name)}",
        "",
        f"<small>{_kind_label(obj)} · <code>{obj.canonical_path}</code></small>",
        "",
    ]
    if obj.is_class or obj.is_function:
        out += [_signature(name, obj), ""]
    elif obj.is_attribute and obj.annotation is not None:
        out += [f"Type: {_annotation(obj.annotation, index)}", ""]
    if obj.is_class and obj.bases:
        bases = ", ".join(_annotation(b, index) for b in obj.bases)
        out += [f"Bases: {bases}", ""]
    doc = _docstring(obj, index)
    if doc:
        out += [doc, ""]
    for member in _members(obj):
        out += _render_object(f"{path}.{member.name}", member, level + 1, index)
    return out


def expand_api_page(text: str, base: str = "/") -> str:
    """Replace every ``::: <object path>`` line of an API page with its reference.

    Args:
        text: The page as written under ``docs/api/``.
        base: The site's base path, for links between API pages.

    Returns:
        The page in Markdown, the ``:::`` lines replaced.

    Raises:
        ApiPageError: If an object does not exist or griffe reports a problem with a
            docstring it renders.
    """
    index = _index(base)
    collector = _ProblemCollector()
    logger = logging.getLogger("griffe")
    logger.addHandler(collector)
    try:
        out: list[str] = []
        for line in text.splitlines():
            match = _DIRECTIVE.match(line)
            if match is None:
                out.append(line)
                continue
            path = match.group("path")
            out += _render_object(path, _lookup(path), 2, index)
    finally:
        logger.removeHandler(collector)
    if collector.problems:
        raise ApiPageError("docstring problems:\n  " + "\n  ".join(collector.problems))
    return "\n".join(out).rstrip("\n") + "\n"


def documented_paths() -> Sequence[str]:
    """Every object path the API pages name, in page order (for tests)."""
    return [
        path
        for page in sorted(API_DIR.glob("*.md"))
        for path in _directives(page.read_text(encoding="utf-8"))
    ]
