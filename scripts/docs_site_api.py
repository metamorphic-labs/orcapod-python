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
from collections.abc import Iterator
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path

import griffe
from typing_extensions import override

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = REPO_ROOT / "src"
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

    @override
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


def _real(obj: griffe.Object | griffe.Alias) -> griffe.Object:
    """The object itself, for an alias (a re-export) the object it names."""
    return obj.final_target if isinstance(obj, griffe.Alias) else obj


def _lookup(path: str) -> griffe.Object:
    package = _package()
    if not path.startswith(PACKAGE + "."):
        raise ApiPageError(f"::: {path}: not an object of the {PACKAGE} package")
    try:
        found: griffe.Object | griffe.Alias = package[path.removeprefix(PACKAGE + ".")]
    except KeyError:
        raise ApiPageError(f"::: {path}: no such object") from None
    return _real(found)


def _page_url(page: str, base: str) -> str:
    return (
        f"{base.rstrip('/')}/api/{page}/"
        if page != "index"
        else f"{base.rstrip('/')}/api/"
    )


@cache
def _index(api_dir: Path, base: str) -> _Index:
    """Index every object documented on any API page (and its documented members)."""
    index = _Index()
    for page_file in sorted(api_dir.glob("*.md")):
        url = _page_url(page_file.stem, base)
        for path in _directives(page_file.read_text(encoding="utf-8")):
            obj = _lookup(path)
            for anchor, member in _anchored(path, obj):
                target = f"{url}#{anchor}"
                _ = index.urls.setdefault(anchor, target)
                _ = index.urls.setdefault(member.canonical_path, target)
    return index


def _members(obj: griffe.Object) -> list[griffe.Object]:
    """The members shown for ``obj``: public, documented, declared on it, in order."""
    if not isinstance(obj, griffe.Class):
        return []
    shown = [
        member
        for name, member in obj.members.items()
        if not name.startswith("_")
        and not isinstance(member, griffe.Alias)
        and member.docstring
    ]
    return sorted(shown, key=lambda m: m.lineno or 0)


def _anchored(path: str, obj: griffe.Object) -> Iterator[tuple[str, griffe.Object]]:
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


def _parameters(obj: griffe.Class | griffe.Function) -> list[griffe.Parameter]:
    if isinstance(obj, griffe.Class):
        init = obj.all_members.get("__init__")
        if init is None:
            return []
        init = _real(init)
        if not isinstance(init, griffe.Function):
            return []
        params = list(init.parameters)
    else:
        params = list(obj.parameters)
    if params and params[0].name in ("self", "cls"):
        params = params[1:]
    return params


_STARS = {
    griffe.ParameterKind.var_positional: "*",
    griffe.ParameterKind.var_keyword: "**",
}


def _signature(name: str, obj: griffe.Class | griffe.Function) -> str:
    """Render the signature of a class or function as a Python code block."""
    if isinstance(obj, griffe.Class):
        prefix = "class "
    else:
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
        star = _STARS.get(kind, "") if kind is not None else ""
        text = f"{star}{param.name}"
        if param.annotation is not None:
            text += f": {param.annotation}"
        if param.default is not None and not star:
            text += (
                f" = {param.default}"
                if param.annotation is not None
                else f"={param.default}"
            )
        rendered.append(text)
        previous_kind = kind
    if previous_kind is griffe.ParameterKind.positional_only:
        rendered.append("/")
    returns = ""
    if isinstance(obj, griffe.Function) and obj.returns is not None:
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
    return (
        "\n".join([lines[0], *("  " + ln if ln else "" for ln in lines[1:])])
        if lines
        else ""
    )


_ELEMENT_SECTIONS: tuple[tuple[type[griffe.DocstringSection], str, bool], ...] = (
    # (section class, title, whether its items have a name to show)
    (griffe.DocstringSectionParameters, "Parameters", True),
    (griffe.DocstringSectionOtherParameters, "Other parameters", True),
    (griffe.DocstringSectionAttributes, "Attributes", True),
    (griffe.DocstringSectionReturns, "Returns", True),
    (griffe.DocstringSectionYields, "Yields", True),
    (griffe.DocstringSectionReceives, "Receives", True),
    (griffe.DocstringSectionRaises, "Raises", False),
    (griffe.DocstringSectionWarns, "Warns", False),
)


def _element_item(
    element: griffe.DocstringElement, show_name: bool, index: _Index
) -> str:
    """One item of a parameters / returns / raises ... list."""
    head_parts: list[str] = []
    if show_name and isinstance(element, griffe.DocstringNamedElement) and element.name:
        head_parts.append(f"**{element.name}**")
    annotation = _annotation(element.annotation, index)
    if annotation:
        head_parts.append(f"({annotation})" if head_parts else annotation)
    if isinstance(element, griffe.DocstringParameter) and element.default is not None:
        head_parts.append(f"default {_code(str(element.default))}")
    head = " ".join(head_parts)
    description = _indent_continuation(element.description or "")
    return f"- {head}: {description}" if head else f"- {description}"


def _section(section: griffe.DocstringSection, index: _Index) -> str:
    """Render one parsed docstring section as Markdown.

    Raises:
        ApiPageError: For a section kind this generator does not render.
    """
    if isinstance(section, griffe.DocstringSectionText):
        return section.value.strip()
    if isinstance(section, griffe.DocstringSectionAdmonition):
        admonition: griffe.DocstringAdmonition = section.value
        kind = str(admonition.annotation or "")
        title = section.title or kind.capitalize()
        aside = _ASIDES.get(kind.lower(), "note")
        return f":::{aside}[{title}]\n{admonition.description.strip()}\n:::"
    if isinstance(section, griffe.DocstringSectionExamples):
        parts = [
            f"```python\n{text}\n```"
            if kind is griffe.DocstringSectionKind.examples
            else text.strip()
            for kind, text in section.value
        ]
        return "**Examples:**\n\n" + "\n\n".join(parts)
    if isinstance(section, griffe.DocstringSectionDeprecated):
        deprecated: griffe.DocstringDeprecated = section.value
        return (
            f":::caution[Deprecated {deprecated.version}]\n"
            f"{deprecated.description.strip()}\n:::"
        )
    for section_class, title, show_name in _ELEMENT_SECTIONS:
        if isinstance(section, section_class):
            elements: list[griffe.DocstringElement] = section.value  # pyright: ignore[reportAny] -- each listed class types value as a list of elements
            items = [_element_item(e, show_name, index) for e in elements]
            return f"**{title}:**\n\n" + "\n".join(items)
    raise ApiPageError(f"docstring section {section.kind.value!r} is not rendered")


def _docstring(obj: griffe.Object, index: _Index) -> str:
    if not obj.docstring:
        return ""
    return "\n\n".join(_section(s, index) for s in obj.docstring.parse())


def _kind_label(obj: griffe.Object) -> str:
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
    path: str, obj: griffe.Object, level: int, index: _Index
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
    if isinstance(obj, griffe.Class | griffe.Function):
        out += [_signature(name, obj), ""]
    elif isinstance(obj, griffe.Attribute) and obj.annotation is not None:
        out += [f"Type: {_annotation(obj.annotation, index)}", ""]
    if isinstance(obj, griffe.Class) and obj.bases:
        bases = ", ".join(_annotation(b, index) for b in obj.bases)
        out += [f"Bases: {bases}", ""]
    doc = _docstring(obj, index)
    if doc:
        out += [doc, ""]
    for member in _members(obj):
        out += _render_object(f"{path}.{member.name}", member, level + 1, index)
    return out


def expand_api_page(text: str, api_dir: Path, base: str = "/") -> str:
    """Replace every ``::: <object path>`` line of an API page with its reference.

    Args:
        text: The page as written under ``docs/api/``.
        api_dir: The ``docs/api/`` folder: the objects its pages name are the ones
            a type in a signature or parameter list can link to.
        base: The site's base path, for links between API pages.

    Returns:
        The page in Markdown, the ``:::`` lines replaced.

    Raises:
        ApiPageError: If an object does not exist or griffe reports a problem with a
            docstring it renders.
    """
    index = _index(api_dir, base)
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
