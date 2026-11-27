"""Validation of sources sharing a canonical source ID within one graph."""

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
            pending.extend(
                (location, upstream, recorded)
                for upstream in reversed(upstreams)
                if upstream is not None
            )


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


def _key_columns_conflict(
    source_id: str, group: list[tuple[str, RootSource]]
) -> str | None:
    """Describe sources of one id and schema whose key columns differ, if any.

    Such sources are distinct readings of one dataset. They are refused everywhere for
    now (capture, bind, load, run), whatever slots they would take: allowing them in
    distinct named slots is ITL-1506, after the multi-slot binding work (ITL-1442).
    Call only after the schemas and fixed rows of ``group`` were found to agree. Only
    the sources whose key columns are known are compared: one whose key columns are
    unknown (an unbound proxy) neither causes nor suppresses a refusal.

    Args:
        source_id: The shared source id.
        group: Every distinct source with that id, with where it is used.

    Returns:
        The error message, or ``None`` when the known key columns agree.
    """
    known = [(where, s) for where, s in group if s.key_columns is not None]
    if len({source.key_columns for _, source in known}) < 2:
        return None
    where = ", ".join(location for location, _ in known)
    remedy = (
        "A later version will allow distinct readings of one dataset in distinct "
        "named slots (ITL-1506). For now, give each source its own name with "
        "source_id=... when it is built."
    )
    if all(source.holds_fixed_rows for _, source in known):
        return (
            f"Sources with source id {source_id!r} in {where} share one schema and "
            "the same rows, and differ only in key columns (record_id_columns), so "
            f"this is refused for now. {remedy}"
        )
    return (
        f"Sources with source id {source_id!r} in {where} share one schema and have "
        "different key columns (record_id_columns). The rows of a source read later "
        f"are not compared. This is refused for now. {remedy}"
    )


def _underlying(source: RootSource) -> RootSource:
    """The source whose rows ``source`` presents: a bound proxy's delegate, unwrapped.

    Args:
        source: A source in a consistency check.

    Returns:
        The innermost bound root source behind any proxies, or ``source`` itself.
    """
    from orcapod.core.sources.base import RootSource
    from orcapod.core.sources.source_proxy import SourceProxy

    while isinstance(source, SourceProxy) and isinstance(source.delegate, RootSource):
        source = source.delegate
    return source


def _differing_fixed_rows(
    group: list[tuple[str, RootSource]],
) -> tuple[str, str] | None:
    """Find two fixed sources of one id and schema that hold different rows.

    A source id names a dataset; key columns only name its rows and belong to the
    reading. Every pair of fixed sources is compared by the rule for that pair, so the
    answer never depends on the order of ``group``:

    - equal key columns (or unknown ones): tags, data and record ids
      (``fixed_content``), so positional row order counts as it always has;
    - different key columns: the record ids necessarily differ, so the rows are
      compared as a multiset of tags and data (``fixed_row_multiset``): every
      occurrence counts, order does not, and floats compare by the Arrow value rule
      (``-0.0`` equals ``0.0``, NaN equals NaN).

    Only the digests a pair needs are computed, each at most once per check for each
    underlying source: a bound proxy is digested as the source it stands in for
    (``_underlying``). Nothing is held across checks: an in-memory table can share
    memory with the caller's data, which the caller can change between checks.

    Args:
        group: Every distinct source with that id, with where it is used.

    Returns:
        The locations of the first such pair, or ``None`` when all fixed rows agree.
    """
    fixed = [(where, s) for where, s in group if s.holds_fixed_rows]
    exact: dict[int, bytes | None] = {}
    multisets: dict[int, bytes | None] = {}

    def digest(source: RootSource, *, record_ids: bool) -> bytes | None:
        source = _underlying(source)
        held = exact if record_ids else multisets
        if id(source) not in held:
            held[id(source)] = (
                source.fixed_content if record_ids else source.fixed_row_multiset
            )
        return held[id(source)]

    for index, (first_location, first) in enumerate(fixed):
        for location, source in fixed[index + 1 :]:
            first_keys, keys = first.key_columns, source.key_columns
            record_ids = first_keys is None or keys is None or first_keys == keys
            if digest(first, record_ids=record_ids) != digest(
                source, record_ids=record_ids
            ):
                return first_location, location
    return None


def require_consistent_sources(
    uses: Iterable[SourceUse],
    rebinding: Rebinding = (),
) -> None:
    """Refuse schema differences and conflicting fixed rows at a shared address.

    Compare distinct objects only. A live source has no fixed rows to compare,
    but does not prevent comparing two fixed sources that share its address.
    Every pair of fixed sources is compared by ``_differing_fixed_rows`` (record ids
    count only when their key columns are equal), so different rows are reported whatever
    the order of the uses. Sources whose schemas and fixed rows agree but whose known
    key columns differ are then refused too (``_key_columns_conflict``, ITL-1506).
    For a bind, use the prospective sources alongside every unchanged binding.

    Raises:
        InconsistentSourceError: Naming the sources and remedy for a conflict.
    """
    by_id: dict[str, list[tuple[str, RootSource]]] = {}
    for location, source in root_source_uses(uses, rebinding):
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

        differing = _differing_fixed_rows(group)
        if differing is not None:
            raise InconsistentSourceError(
                f"Source {source_id!r} has different rows in {differing[0]} "
                f"and {differing[1]}. Give the sources different source_id values."
            )

        message = _key_columns_conflict(source_id, group)
        if message is not None:
            raise InconsistentSourceError(message)
