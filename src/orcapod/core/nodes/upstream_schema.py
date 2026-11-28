"""Schema validation for node upstream setters."""

import pyarrow as pa

from orcapod.core.streams.base import (
    StreamBase,
    shared_zero_row_memo,
    typed_arrow_schemas,
    typed_zero_row_table,
)
from orcapod.logical_types.user_class_identity import equivalent_hints
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol
from orcapod.system_constants import constants
from orcapod.types import DataType, Schema
from orcapod.utils.schema_utils import declared_arrow_schema


def _user_schemas(stream: StreamProtocol) -> tuple[Schema, Schema]:
    """Return tag and data schemas without data-only meta fields."""
    tag, data = stream.output_schema()
    meta = [
        name
        for name in data
        if name.startswith(constants.META_PREFIX) and name not in tag
    ]
    return tag, data.drop(*meta) if meta else data


@shared_zero_row_memo()
def require_same_upstream_schemas(
    node: StreamBase, replacements: tuple[StreamProtocol, ...]
) -> None:
    """Require each replacement to preserve the schema of its input position.

    Args:
        node: The wired node.
        replacements: The proposed upstreams, in input order.

    Raises:
        ValueError: If the upstream count or a tag, data, or system-tag schema changes.
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
        current = _user_schemas(upstream)
        proposed = _user_schemas(replacement)
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

        current_arrow = typed_arrow_schemas(upstream, data_fields=tuple(current[1]))
        proposed_arrow = typed_arrow_schemas(
            replacement, data_fields=tuple(proposed[1])
        )
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

        columns = {"system_tags": True}
        current_system_tags = {
            name: hint
            for name, hint in upstream.output_schema(columns=columns)[0].items()
            if name not in current[0]
        }
        proposed_system_tags = {
            name: hint
            for name, hint in replacement.output_schema(columns=columns)[0].items()
            if name not in proposed[0]
        }
        if current_system_tags != proposed_system_tags:
            raise ValueError(
                f"Cannot rewire node {node.label!r}: upstream {index} has a different "
                + f"system-tag schema: current {current_system_tags!r}, "
                + f"replacement {proposed_system_tags!r}"
            )
        if current_system_tags:
            converter = node.data_context.type_converter
            old_system_arrow = _system_tag_arrow_schema(
                typed_zero_row_table(upstream), current_system_tags, converter
            )
            new_system_arrow = _system_tag_arrow_schema(
                typed_zero_row_table(replacement), proposed_system_tags, converter
            )
            if hasher.hash_schema(old_system_arrow) != hasher.hash_schema(
                new_system_arrow
            ):
                raise ValueError(
                    f"Cannot rewire node {node.label!r}: upstream {index} has a different "
                    + f"system-tag Arrow schema: current {old_system_arrow}, "
                    + f"replacement {new_system_arrow}"
                )


def _system_tag_arrow_schema(
    table: pa.Table | None,
    system_tags: dict[str, DataType],
    converter: TypeConverterProtocol,
) -> pa.Schema:
    """Use the system-tag Arrow fields when known, or their declared types."""
    if table is None:
        return declared_arrow_schema(system_tags, converter)
    schema = table.schema
    available = set(schema.names)
    missing = {
        name: hint for name, hint in system_tags.items() if name not in available
    }
    fields = [schema.field(name) for name in system_tags if name in available]
    if missing:
        fields.extend(declared_arrow_schema(missing, converter))
    return pa.schema(fields)
