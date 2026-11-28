"""Validate source object registrations within one graph."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from typing import TYPE_CHECKING, cast

from orcapod.errors import (
    InconsistentSourceError,
    SourceSpecMismatchError,
    live_input_cycle_error,
)
from orcapod.utils.schema_utils import arrow_schema_identity

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.sources.base import RootSource
    from orcapod.pipeline.pod_invocation import PodInvocation
    from orcapod.pipeline.restrictions import Rebinding
    from orcapod.protocols.core_protocols import StreamProtocol
    from orcapod.types import ContentHash

SourceUse = tuple[str, "StreamProtocol"]


def slot_dependency_error(
    name: str | None, dependency: SourceJobNode
) -> SourceSpecMismatchError:
    """Name the slots involved in an unsupported binding and how to replace it."""
    target = "a source slot" if name is None else f"source slot {name!r}"
    return SourceSpecMismatchError(
        f"Binding {target} to a source that depends on source slot "
        f"{dependency.name!r} is not supported yet (ITL-1559). "
        + "Pass the underlying source, or use a non-slot node's as_source() to read stored results."
    )


def _walk_live_inputs(
    streams: Iterable[StreamProtocol], *, stop_at_slots: bool = False
) -> Iterable[StreamProtocol]:
    """Walk live inputs by identity, checking slot dependencies and active-path cycles.

    A prospective binding stops at its first slot. A read or run follows each
    slot's binding and refuses a second slot anywhere in that binding. Track
    the binding walk separately so a path entering a slot from its own bound
    stream reports both slots, even if it then revisits the starting stream.
    """
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.pipeline.restrictions import streams_beneath

    pending: list[tuple[StreamProtocol, SourceJobNode | None, bool]] = [
        (stream, None, False) for stream in streams
    ]
    pending.reverse()
    active: set[tuple[int, bool]] = set()
    visited: set[tuple[int, bool]] = set()
    while pending:
        stream, binding, leaving = pending.pop()
        key = (id(stream), binding is not None)
        if leaving:
            active.remove(key)
            visited.add(key)
            continue
        if isinstance(stream, SourceJobNode) and binding is not None:
            raise slot_dependency_error(binding.name, stream)
        if key in active:
            raise live_input_cycle_error(stream.label)
        if key in visited:
            continue
        yield stream
        if isinstance(stream, SourceJobNode) and stop_at_slots:
            continue
        active.add(key)
        pending.append((stream, binding, True))
        next_binding = stream if isinstance(stream, SourceJobNode) else binding
        pending.extend(
            (upstream, next_binding, False)
            for upstream in reversed(streams_beneath(stream))
        )


def slot_dependency(source: StreamProtocol | None) -> SourceJobNode | None:
    """Find a bindable slot a source would read, if any."""
    from orcapod.core.nodes.source_node import SourceJobNode

    # TODO(ITL-1559): live slot references can create cycles or retain an old
    # slot after recompilation. Stored-result sources end the live-input walk.
    if source is not None:
        for stream in _walk_live_inputs((source,), stop_at_slots=True):
            if isinstance(stream, SourceJobNode):
                return stream
    return None


def require_live_source_bindings(streams: Iterable[StreamProtocol]) -> None:
    """Check live inputs before resolving schemas or hashes, reading rows or running."""
    for _ in _walk_live_inputs(streams):
        pass


def invocation_source_uses(invocations: Iterable[PodInvocation]) -> Iterable[SourceUse]:
    """Name each invocation's inputs, numbering occurrences of repeated labels."""
    labelled = [
        (
            invocation.label
            or getattr(invocation.pod, "label", type(invocation.pod).__name__),
            invocation,
        )
        for invocation in invocations
    ]
    counts = Counter(label for label, _ in labelled)
    occurrences: Counter[str] = Counter()
    for label, invocation in labelled:
        step = f"step {label!r}"
        if counts[label] > 1:
            occurrences[label] += 1
            step += f" (occurrence {occurrences[label]})"
        for index, stream in enumerate(invocation.input_streams, 1):
            name = step
            if len(invocation.input_streams) > 1:
                name += f" input {index}"
            yield name, stream


def root_source_uses(
    uses: Iterable[SourceUse],
    rebinding: Rebinding = (),
) -> Iterable[tuple[str, RootSource]]:
    """Walk to the source objects registered by the graph.

    A ``RootSource`` supplies the registered object and ends this walk even if it
    reads live inputs. Registering those inputs too would make a cache conflict
    with the source whose hash it preserves. Capture safety and restrictions
    instead follow live reads through sources.
    """
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.sources.base import RootSource

    replacements = {id(slot): source for slot, source in rebinding}
    visited: set[int] = set()
    pending = list(uses)
    pending.reverse()
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
                slot_use = f"slot {stream.name!r}"
                if location != f"node {stream.label!r}":
                    slot_use = f"{location} via {slot_use}"
                pending.append((slot_use, bound))
        else:
            # Loaded nodes can have inputs that could not be reconstructed.
            upstreams = cast("tuple[StreamProtocol | None, ...]", stream.upstreams)
            for index in reversed(range(len(upstreams))):
                upstream = upstreams[index]
                if upstream is not None:
                    upstream_location = location
                    if len(upstreams) > 1:
                        upstream_location += f", {stream.label!r} input {index + 1}"
                    pending.append((upstream_location, upstream))


def _comparison_schema(source: RootSource) -> tuple[ContentHash, ContentHash]:
    """Use a bound proxy's live Arrow types without changing source identity."""
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.derived_source import DerivedSource
    from orcapod.core.sources.source_proxy import SourceProxy
    from orcapod.core.streams.base import typed_arrow_schemas

    visited: set[int] = set()

    def live_schemas(
        current: StreamProtocol, from_proxy: bool = False
    ) -> tuple[pa.Schema, pa.Schema] | None:
        if id(current) in visited:
            return None
        visited.add(id(current))
        if isinstance(current, CachedSource):
            return live_schemas(current.cached_source, from_proxy=from_proxy)
        if (
            isinstance(current, SourceProxy)
            and (delegate := current.delegate) is not None
        ):
            return live_schemas(delegate, from_proxy=True)
        if isinstance(current, DerivedSource) and (
            inputs := current.live_source_inputs
        ):
            origin_schemas = live_schemas(inputs[0])
            if origin_schemas is not None:
                return current._arrow_schemas_from_origin(*origin_schemas)  # pyright: ignore[reportPrivateUsage]  # source consistency
        return typed_arrow_schemas(current) if from_proxy else None

    schemas = live_schemas(source) or source.arrow_schemas
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
            remedy = f"{reuse}."
            if first.source_id == source.source_id:
                remedy += (
                    " If the source ID contributes to the hash, give each source "
                    "its own source_id."
                )
            raise InconsistentSourceError(
                f"Source {source.source_id!r} has distinct source objects with one "
                f"content hash in {first_location} and {location}. {remedy}"
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
