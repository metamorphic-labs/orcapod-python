"""How a source is named where a pipeline is shown (ITL-1454): by its assigned label.

Display only: a slot's name, a node's label, every hash, id and stored path are what they
are without it. Everything is read from what a source stores; no method or property of a
source is called, so showing a pipeline runs no user code.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from types import GetSetDescriptorType
from typing import Any


@dataclass(frozen=True)
class LabelledSource:
    """A source with an assigned label, as a display names it.

    Attributes:
        label: The assigned label.
        source_id: The full source id (``<kind>_v<N>:<name>``).
        parts: What tells it apart from a source of the same label, shortest first
            (a SpiralDB table's dataset, its project, both, then any source's name and
            id); ``""`` for a part its kind lacks.
    """

    label: str
    source_id: str
    parts: tuple[str, ...]


def _stored(source: object) -> Mapping[str, Any]:
    """Return an object's instance dictionary, read past any hook its class defines."""
    for cls in type(source).__mro__:
        found = cls.__dict__.get("__dict__")
        if isinstance(found, GetSetDescriptorType):
            return found.__get__(source)
    return {}


def _stored_text(source: object, name: str) -> str:
    """Return the text stored under ``name``, as a plain ``str``; ``""`` for none."""
    value = _stored(source).get(name)
    return str.__str__(value) if isinstance(value, str) else ""


def labelled_source(stream: Any) -> LabelledSource | None:
    """Return the source as a display names it, or ``None`` when it has no label.

    ``None`` for a stream that is no source, a source with no assigned label and a
    proxy of a source that could not be rebuilt. A ``CachedSource`` is named by its own
    label and the id of the source it wraps.

    Args:
        stream: The source, or any stream bound to a slot.
    """
    from orcapod.core.sources.base import RootSource
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.source_ids import build_source_id
    from orcapod.core.sources.source_proxy import SourceProxy

    if not issubclass(type(stream), RootSource):
        return None
    label = _stored_text(stream, "_label")
    if not label or issubclass(type(stream), SourceProxy):
        return None
    source = stream
    while type(source) is CachedSource:
        source = _stored(source).get("_source")
    # A cache accepts any source-like object; only a root source holds a name to show.
    if not issubclass(type(source), RootSource) or issubclass(
        type(source), SourceProxy
    ):
        return None
    name = _stored_text(source, "_source_name")
    if not name:
        return None
    kind, version = type(source).SOURCE_KIND, type(source).SOURCE_VERSION
    near, far = type(source).DISPLAY_PARTS
    dataset, project = _stored_text(source, near), _stored_text(source, far)
    both = f"{project}.{dataset}" if dataset and project else ""
    source_id = build_source_id(kind, version, name)
    return LabelledSource(label, source_id, (dataset, project, both, name, source_id))


def display_names(
    sources: Mapping[str, LabelledSource], names: Collection[str]
) -> dict[str, str]:
    """Return the text each labelled source is shown by: its label when that is enough.

    Sources sharing a label, and a source whose label is another shown name, are shown
    as ``label (part)`` with the shortest part that tells all of them apart. A source no
    part tells apart keeps its own name, so no two texts are ever equal. The order of
    the sources changes nothing.

    Args:
        sources: The labelled sources, by the name each is shown by without a label.
        names: Every name the display shows (the nodes' labels), these included.

    Returns:
        The text, by the same keys as ``sources``.
    """
    groups: dict[str, list[str]] = {}
    for name, source in sources.items():
        groups.setdefault(source.label, []).append(name)
    texts: dict[str, str] = {}
    for label, members in groups.items():
        taken = set(names) - set(members)
        if len(members) == 1 and label not in taken:
            texts[members[0]] = label
            continue
        taken.update(groups)
        for level in zip(*(sources[m].parts for m in members), strict=True):
            shown = [f"{label} ({part})" for part in level]
            if all(level) and len(set(shown)) == len(shown) and not taken & set(shown):
                texts.update(zip(members, shown))
                break
        else:
            texts.update({m: f"{label} ({m})" for m in members})
    used = list(texts.values())
    return {
        n: t if used.count(t) == 1 and t not in set(names) - {n} else n
        for n, t in texts.items()
    }


def node_display_names(nodes: Iterable[Any]) -> dict[str, tuple[str, str]]:
    """Return the text and full source id of each source node shown by its source's label.

    Args:
        nodes: The nodes of one pipeline or job.

    Returns:
        ``(text, source id)`` by node label, for the nodes whose bound source has an
        assigned label; a node given a label of its own, an unbound slot and every other
        node is not listed (it is shown by its own label).
    """
    all_nodes = list(nodes)
    found: dict[str, LabelledSource] = {}
    for node in all_nodes:
        if node.node_type != "source" or node.has_assigned_label:
            continue
        source = labelled_source(getattr(node, "bound_source", None))
        if source is not None:
            found[node.label] = source
    texts = display_names(found, {node.label for node in all_nodes})
    return {name: (text, found[name].source_id) for name, text in texts.items()}
