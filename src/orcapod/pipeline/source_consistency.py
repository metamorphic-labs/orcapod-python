"""Validation of sources sharing a canonical source ID within one graph."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from orcapod.errors import InconsistentSourceError
from orcapod.utils.schema_utils import arrow_schema_identity

if TYPE_CHECKING:
    from orcapod.core.sources.base import RootSource
    from orcapod.pipeline.pod_invocation import PodInvocation
    from orcapod.pipeline.restrictions import Rebinding
    from orcapod.protocols.core_protocols import StreamProtocol

SourceUse = tuple[str, "StreamProtocol"]


def invocation_source_uses(invocation: PodInvocation) -> Iterable[SourceUse]:
    """Name the input streams of a recorded step for diagnostics."""
    label = invocation.label or getattr(
        invocation.pod, "label", type(invocation.pod).__name__
    )
    for index, stream in enumerate(invocation.input_streams, 1):
        name = f"step {label!r}"
        if len(invocation.input_streams) > 1:
            name += f" input {index}"
        yield name, stream


def _root_uses(
    uses: Iterable[SourceUse],
    rebinding: Rebinding,
) -> Iterable[tuple[str, RootSource]]:
    """Walk to each source object, carrying its step or slot name."""
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.sources.base import RootSource

    replacements = {id(slot): source for slot, source in rebinding}
    visited: set[int] = set()
    pending = list(reversed(list(uses)))
    while pending:
        location, stream = pending.pop()
        if id(stream) in visited:
            continue
        visited.add(id(stream))
        if isinstance(stream, RootSource):
            yield location, stream
        elif isinstance(stream, SourceJobNode):
            bound = replacements.get(id(stream), stream.bound_source)
            if bound is not None:
                pending.append((f"slot {stream.name!r}", bound))
        else:
            # Loaded nodes can have inputs that could not be reconstructed.
            upstreams: tuple[StreamProtocol | None, ...] = stream.upstreams
            pending.extend(
                (location, upstream)
                for upstream in reversed(upstreams)
                if upstream is not None
            )


def require_consistent_sources(
    uses: Iterable[SourceUse],
    rebinding: Rebinding = (),
) -> None:
    """Refuse schema differences and conflicting fixed rows at a shared address.

    Compare distinct objects only. A live source has no fixed rows to compare,
    but does not prevent comparing two fixed sources that share its address.
    For a bind, use the prospective sources alongside every unchanged binding.

    Raises:
        InconsistentSourceError: Naming the sources and remedy for a conflict.
    """
    by_id: dict[str, list[tuple[str, RootSource]]] = {}
    for location, source in _root_uses(uses, rebinding):
        by_id.setdefault(source.source_id, []).append((location, source))

    for source_id, group in by_id.items():
        if len(group) < 2:
            continue
        first_location, first = group[0]
        schema = arrow_schema_identity(*first.arrow_schemas, first.data_context)
        for location, source in group[1:]:
            if (
                arrow_schema_identity(*source.arrow_schemas, source.data_context)
                != schema
            ):
                raise InconsistentSourceError(
                    f"Source {source_id!r} has different schemas in {first_location} "
                    f"and {location}. Give the sources different source_id values."
                )

        fixed = None
        fixed_location = first_location
        for location, source in group:
            content = source.fixed_content
            if content is None:
                continue
            if fixed is None:
                fixed, fixed_location = content, location
            elif fixed != content:
                raise InconsistentSourceError(
                    f"Source {source_id!r} has different rows in {fixed_location} "
                    f"and {location}. Give the sources different source_id values."
                )
