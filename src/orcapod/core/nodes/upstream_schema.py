"""Schema validation for node upstream setters."""

from orcapod.core.streams.base import StreamBase, typed_arrow_schemas
from orcapod.logical_types.user_class_identity import equivalent_hints
from orcapod.protocols.core_protocols import StreamProtocol


def require_same_upstream_schemas(
    node: StreamBase, replacements: tuple[StreamProtocol, ...]
) -> None:
    """Require each replacement to preserve the schema of its input position.

    Args:
        node: The wired node.
        replacements: The proposed upstreams, in input order.

    Raises:
        ValueError: If the upstream count or a tag or data schema changes.
    """
    upstreams = node.upstreams
    if len(upstreams) != len(replacements):
        current = tuple(stream.output_schema() for stream in upstreams)
        proposed = tuple(stream.output_schema() for stream in replacements)
        raise ValueError(
            f"Cannot rewire node {node.label!r} with a different upstream count: "
            + f"current schemas {current!r}, replacement schemas {proposed!r}"
        )

    for index, (upstream, replacement) in enumerate(zip(upstreams, replacements)):
        if upstream is replacement:
            continue
        current = upstream.output_schema()
        proposed = replacement.output_schema()
        if any(
            old.keys() != new.keys()
            or any(not equivalent_hints(old[name], new[name]) for name in old)
            for old, new in zip(current, proposed)
        ):
            raise ValueError(
                f"Cannot rewire node {node.label!r}: upstream {index} has a different "
                + f"schema: current (tag={current[0]!r}, data={current[1]!r}), "
                + f"replacement (tag={proposed[0]!r}, data={proposed[1]!r})"
            )

        current_arrow = typed_arrow_schemas(upstream)
        proposed_arrow = typed_arrow_schemas(replacement)
        hasher = node.data_context.arrow_hasher
        if any(
            hasher.hash_schema(old) != hasher.hash_schema(new)
            for old, new in zip(current_arrow, proposed_arrow)
        ):
            raise ValueError(
                f"Cannot rewire node {node.label!r}: upstream {index} has a different "
                + f"Arrow schema: current (tag={current_arrow[0]}, data={current_arrow[1]}), "
                + f"replacement (tag={proposed_arrow[0]}, data={proposed_arrow[1]})"
            )
