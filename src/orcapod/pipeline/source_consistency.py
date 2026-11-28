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

    from orcapod.core.sources.base import RootSource
    from orcapod.pipeline.job import PipelineJob
    from orcapod.pipeline.pod_invocation import PodInvocation
    from orcapod.pipeline.restrictions import Rebinding
    from orcapod.protocols.core_protocols import StreamProtocol
    from orcapod.types import ContentHash

SourceUse = tuple[str, "StreamProtocol"]


def _binding_dependency_error(
    name: str | None, dependency: StreamProtocol
) -> SourceSpecMismatchError:
    """Name the dependency that makes a slot binding unsupported."""
    from orcapod.core.nodes.source_node import SourceJobNode

    target = "a source slot" if name is None else f"source slot {name!r}"
    if isinstance(dependency, SourceJobNode):
        detail = f"source slot {dependency.name!r}"
        remedy = "use a non-slot node's as_source() to read stored results"
    else:
        detail = f"node {dependency.label!r} of the same job"
        remedy = "use another job's node.as_source() to read stored results"
    return SourceSpecMismatchError(
        f"Binding {target} to a source that depends on {detail} "
        f"is not supported yet (ITL-1559). Pass the underlying source, or {remedy}."
    )


def require_no_owned_node_dependency(
    stream: StreamProtocol, name: str | None, owner: PipelineJob
) -> None:
    """Refuse a dependency on a node of the binding's own job."""
    from orcapod.core.nodes.staleness import RunHoldingNode
    from orcapod.core.sources.derived_source import DerivedSource

    dependency = (
        stream.stored_result_origin if isinstance(stream, DerivedSource) else stream
    )
    if isinstance(dependency, RunHoldingNode) and dependency._belongs_to_job(owner):  # pyright: ignore[reportPrivateUsage]  # binding checks use node provenance
        raise _binding_dependency_error(name, dependency)


def _require_live_inputs(
    streams: Iterable[StreamProtocol],
    binding: tuple[str | None, PipelineJob | None] | None = None,
) -> None:
    """Validate live inputs by identity, with a separate walk inside each binding.

    A binding refuses any bindable slot and any node of its owner, including a
    stored-result origin. Stored results from other jobs remain leaves.
    """
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.pipeline.restrictions import streams_beneath

    pending = [(stream, binding, False) for stream in streams]
    pending.reverse()
    active: set[tuple[int, bool, int]] = set()
    visited: set[tuple[int, bool, int]] = set()
    while pending:
        stream, current_binding, leaving = pending.pop()
        owner = current_binding[1] if current_binding is not None else None
        key = (id(stream), current_binding is not None, id(owner))
        if leaving:
            active.remove(key)
            visited.add(key)
            continue
        if key in visited:
            continue
        if current_binding is not None:
            name = current_binding[0]
            if isinstance(stream, SourceJobNode):
                raise _binding_dependency_error(name, stream)
            if owner is not None:
                require_no_owned_node_dependency(stream, name, owner)
        if key in active:
            raise live_input_cycle_error(stream.label)
        active.add(key)
        pending.append((stream, current_binding, True))
        next_binding = (
            (stream.name, stream._get_binding_owner())  # pyright: ignore[reportPrivateUsage]  # validate the active slot in its owning job
            if isinstance(stream, SourceJobNode)
            else current_binding
        )
        pending.extend(
            (upstream, next_binding, False)
            for upstream in reversed(streams_beneath(stream))
        )


def require_independent_source_binding(
    source: StreamProtocol | None, name: str | None, owner: PipelineJob | None = None
) -> None:
    """Refuse a prospective binding before reading its schema, name or rows."""
    if source is not None:
        _require_live_inputs((source,), (name, owner))


def require_live_source_bindings(streams: Iterable[StreamProtocol]) -> None:
    """Check live inputs before resolving schemas or hashes, reading rows or running."""
    _require_live_inputs(streams)


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
