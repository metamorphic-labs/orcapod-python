"""Validate source object registrations within one graph."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, cast

from orcapod.errors import InconsistentSourceError
from orcapod.utils.schema_utils import arrow_schema_identity

if TYPE_CHECKING:
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.sources.base import RootSource
    from orcapod.pipeline.pod_invocation import PodInvocation
    from orcapod.pipeline.restrictions import Rebinding
    from orcapod.protocols.core_protocols import StreamProtocol
    from orcapod.types import ContentHash

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


def root_source_uses(
    uses: Iterable[SourceUse],
    rebinding: Rebinding = (),
    *,
    bound_leaves: Mapping[str, SourceJobNode] | None = None,
) -> Iterable[tuple[str, RootSource]]:
    """Walk to each source object, using live bindings for replaced graph leaves."""
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.sources.base import RootSource

    replacements = {id(slot): source for slot, source in rebinding}
    visited: set[tuple[int, bool]] = set()
    pending = [
        (location, stream, bool(bound_leaves))
        for location, stream in reversed(list(uses))
    ]
    while pending:
        location, stream, recorded = pending.pop()
        key = (id(stream), recorded)
        if key in visited:
            continue
        visited.add(key)
        if recorded and bound_leaves is not None:
            slot = bound_leaves.get(stream.content_hash().to_string())
            if slot is not None:
                pending.append((location, slot, False))
                continue
        if isinstance(stream, RootSource):
            yield location, stream
        elif isinstance(stream, SourceJobNode):
            bound = replacements.get(id(stream), stream.bound_source)
            if bound is not None:
                pending.append((f"slot {stream.name!r}", bound, False))
        else:
            # Loaded nodes can have inputs that could not be reconstructed.
            upstreams = cast("tuple[StreamProtocol | None, ...]", stream.upstreams)
            for index in reversed(range(len(upstreams))):
                upstream = upstreams[index]
                if upstream is not None:
                    upstream_location = location
                    if len(upstreams) > 1:
                        upstream_location += f", {stream.label!r} input {index + 1}"
                    pending.append((upstream_location, upstream, recorded))


def _comparison_schema(source: RootSource) -> tuple[ContentHash, ContentHash]:
    """Use the live proxy schema for the comparison, even after an unbound read."""
    from orcapod.core.sources.source_proxy import SourceProxy
    from orcapod.core.streams.base import typed_arrow_schemas

    if isinstance(source, SourceProxy) and source.is_bound:
        # The cached schema can describe the unbound proxy's Python types.
        schemas = typed_arrow_schemas(source, own=False)
    else:
        schemas = source.arrow_schemas
    return arrow_schema_identity(*schemas, source.data_context)


def require_consistent_sources(
    uses: Iterable[SourceUse],
    rebinding: Rebinding = (),
) -> None:
    """Require one object per content hash and one schema and key per source ID.

    Args:
        uses: The graph's streams and where they are used.
        rebinding: Prospective slot replacements to check before changing a binding.

    Raises:
        InconsistentSourceError: Naming both locations and how to resolve the conflict.
    """
    by_hash: dict[str, tuple[str, RootSource]] = {}
    by_id: dict[str, list[tuple[str, RootSource]]] = {}
    for location, source in root_source_uses(uses, rebinding):
        content_hash = source.content_hash().to_string()
        first_location, first = by_hash.setdefault(content_hash, (location, source))
        if first is not source:
            reuse = "Build the source once and reuse the object"
            if first.condition.equalities or source.condition.equalities:
                reuse = "Call where() once and reuse the resulting source object"
            raise InconsistentSourceError(
                f"Source {source.source_id!r} has distinct source objects with one "
                f"content hash in {first_location} and {location}. {reuse}, "
                "or give each source its own source_id."
            )
        by_id.setdefault(source.source_id, []).append((location, source))

    for source_id, group in by_id.items():
        if len(group) < 2:
            continue
        first_location, first = group[0]
        schema = _comparison_schema(first)
        for location, source in group[1:]:
            if _comparison_schema(source) != schema:
                raise InconsistentSourceError(
                    f"Source {source_id!r} has different schemas in {first_location} "
                    f"and {location}. Give the sources different source_id values."
                )

        known_keys = [
            (location, keys)
            for location, source in group
            if (keys := source._resolved_key_columns) is not None  # pyright: ignore[reportPrivateUsage]  # source consistency
        ]
        if known_keys:
            first_location, first_keys = known_keys[0]
            for location, keys in known_keys[1:]:
                if keys != first_keys:
                    raise InconsistentSourceError(
                        f"Source {source_id!r} has different key columns "
                        f"(record_id_columns) in {first_location} and {location}. "
                        "Give the sources different source_id values. ITL-1506 "
                        "tracks distinct readings in distinct named slots."
                    )
