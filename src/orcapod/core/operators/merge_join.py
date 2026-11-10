from typing import TYPE_CHECKING, Any

from orcapod.core.operators.base import BinaryOperator
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import arrow_utils, schema_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")


def _common_type(
    name: str, left_type: "pa.DataType", right_type: "pa.DataType"
) -> "pa.DataType":
    """Return the Arrow type a column present in both inputs gets in the output.

    Identical types are kept as they are. Types that differ only in width or
    layout (``int8`` and ``int64``, ``string`` and ``large_string``,
    ``timestamp[ms]`` and ``timestamp[us]``) are promoted to their common type
    with Arrow's permissive promotion, which does not depend on the input
    order and so keeps ``MergeJoin`` commutative.

    Args:
        name: Column name, for the error message.
        left_type: The column's Arrow type in the left input.
        right_type: The column's Arrow type in the right input.

    Returns:
        The common Arrow type.

    Raises:
        InputValidationError: If Arrow has no common type for the two (for
            example a timezone-aware and a naive timestamp).
    """
    if left_type == right_type:
        return left_type
    try:
        unified = pa.unify_schemas(
            [
                pa.schema([pa.field(name, left_type)]),
                pa.schema([pa.field(name, right_type)]),
            ],
            promote_options="permissive",
        )
    except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
        raise InputValidationError(
            f"Column '{name}' has Arrow types {left_type} and {right_type}, "
            f"which have no common type: {e}"
        ) from e
    return unified.field(name).type


class MergeJoin(BinaryOperator):
    """
    Binary operator that joins two streams, merging colliding data columns
    into sorted lists.

    For data columns that exist in both streams:
    - Values are combined into a list<T> and sorted independently per column.
    - Corresponding source columns are reordered to match the sort order of
      their data column.

    For non-colliding columns, values are kept as scalars (same as regular Join).

    Tag columns use inner join on shared tags, with union of tag schemas.

    Arrow types: every output column takes its Arrow type from the inputs,
    never from what Arrow infers for the Python values. A merged column is
    ``list<T>`` (built by ``arrow_utils.aggregated_list_type``, the same
    construction ``Batch`` and ``GroupBy`` use) where ``T`` is the input
    column's Arrow type, with or without rows; a logical (extension) ``T``
    yields the ``list[...]`` extension type. Columns that are not merged keep
    their input Arrow type. When a column is in both inputs with Arrow types
    that differ only in width or layout (``int8``/``int64``,
    ``string``/``large_string``, ``float``/``double``,
    ``timestamp[ms]``/``timestamp[us]``, ``list``/``large_list``), it gets
    their common type under Arrow's permissive promotion (``int64``,
    ``large_string``, ``double``, ``timestamp[us]``, ``large_list``), which
    does not depend on the argument order.

    MergeJoin is commutative: MergeJoin(A, B) produces the same result as
    MergeJoin(B, A), achieved by sorting merged values and system tag values.
    """

    @property
    def kernel_id(self) -> tuple[str, ...]:
        return (f"{self.__class__.__name__}",)

    def is_commutative(self) -> bool:
        return True

    def validate_binary_inputs(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> None:
        _, left_data_schema = left_stream.output_schema()
        _, right_data_schema = right_stream.output_schema()

        # Colliding data columns must have identical types since they are
        # merged into list[T] — both sides must contribute the same T.
        colliding_keys = set(left_data_schema.keys()) & set(
            right_data_schema.keys()
        )
        for key in colliding_keys:
            left_type = left_data_schema[key]
            right_type = right_data_schema[key]
            if left_type != right_type:
                raise InputValidationError(
                    f"Colliding data column '{key}' has incompatible types: "
                    f"{left_type} (left) vs {right_type} (right). "
                    f"MergeJoin requires colliding columns to have identical types."
                )

        try:
            self.binary_output_schema(left_stream, right_stream)
        except InputValidationError:
            raise
        except Exception as e:
            raise InputValidationError(
                f"Input streams are not compatible for merge join: {e}"
            ) from e

    def binary_output_schema(
        self,
        left_stream: StreamProtocol,
        right_stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)

        # Always get input schemas WITHOUT system tags for the base computation.
        # System tags are computed separately because the join renames them.
        left_tag_schema, left_data_schema = left_stream.output_schema()
        right_tag_schema, right_data_schema = right_stream.output_schema()

        # Tag schema: union of both tag schemas
        tag_schema = schema_utils.union_schemas(left_tag_schema, right_tag_schema)

        # Data schema: colliding columns become list[T], non-colliding stay scalar
        colliding_schema = schema_utils.intersection_schemas(
            left_data_schema, right_data_schema
        )

        merged_data_schema = {}
        all_data_keys = set(left_data_schema.keys()) | set(
            right_data_schema.keys()
        )
        for key in all_data_keys:
            if key in colliding_schema:
                merged_data_schema[key] = list[colliding_schema[key]]
            elif key in left_data_schema:
                merged_data_schema[key] = left_data_schema[key]
            else:
                merged_data_schema[key] = right_data_schema[key]

        # Add system tag columns if requested
        if columns_config.system_tags:
            system_tag_schema = self._predict_system_tag_schema(
                left_stream, right_stream
            )
            tag_schema = schema_utils.union_schemas(tag_schema, system_tag_schema)

        return tag_schema, Schema(merged_data_schema)

    def _canonical_order(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> list[tuple[StreamProtocol, int]]:
        """
        Determine canonical ordering of the two input streams by stable-sorting
        on pipeline_hash. Returns list of (stream, original_index) tuples in
        canonical order.
        """
        streams_with_idx = [(left_stream, 0), (right_stream, 1)]
        # Python's sorted is stable, so equal pipeline_hashes preserve input order
        return sorted(streams_with_idx, key=lambda s: s[0].pipeline_hash().to_hex())

    def _predict_system_tag_schema(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> Schema:
        """Predict the system tag columns that the join would produce.

        Each input stream's existing system tag columns get renamed by
        appending ::{pipeline_hash}:{canonical_position}. This method
        computes those output column names without performing the join.
        """
        n_char = self.orcapod_config.hashing.system_tag_n_char
        canonical = self._canonical_order(left_stream, right_stream)

        system_tag_fields: dict[str, type] = {}
        for stream, orig_idx in canonical:
            canon_pos = canonical.index((stream, orig_idx))
            stream_tag_schema, _ = stream.output_schema(columns={"system_tags": True})
            for col_name, col_type in stream_tag_schema.items():
                if col_name.startswith(constants.SYSTEM_TAG_PREFIX):
                    new_name = (
                        f"{col_name}{constants.BLOCK_SEPARATOR}"
                        f"{stream.pipeline_hash().to_hex(n_char)}:{canon_pos}"
                    )
                    system_tag_fields[new_name] = col_type
        return Schema(system_tag_fields)

    def binary_static_process(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> StreamProtocol:
        """Inner-join two streams on their shared tags, merging colliding data columns.

        A data column present in both streams becomes a sorted two-element
        list per row, with its source-info column reordered to match. System
        tag columns are renamed with each stream's pipeline hash and canonical
        position. A zero-row input yields a zero-row result with the same
        column types as a non-empty one.

        Args:
            left_stream: The left input stream.
            right_stream: The right input stream.

        Returns:
            The joined stream, with tag columns first.
        """
        n_char = self.orcapod_config.hashing.system_tag_n_char

        # Determine canonical ordering for system tag positions
        canonical = self._canonical_order(left_stream, right_stream)

        # Get tables with source + system_tags, append system tag blocks
        tables = {}
        for stream, orig_idx in canonical:
            canon_pos = canonical.index((stream, orig_idx))
            table = stream.as_table(columns={"source": True, "system_tags": True})
            table = arrow_utils.append_to_system_tags(
                table, f"{stream.pipeline_hash().to_hex(n_char)}:{canon_pos}"
            )
            tables[orig_idx] = table

        left_table = tables[0]
        right_table = tables[1]

        # Determine shared tag keys for inner join
        left_tag_keys, left_data_keys = left_stream.keys()
        right_tag_keys, right_data_keys = right_stream.keys()
        shared_tag_keys = set(left_tag_keys) & set(right_tag_keys)

        # Find colliding data columns
        colliding_keys = set(left_data_keys) & set(right_data_keys)

        # Snapshot the inputs' Arrow types BEFORE the Polars round-trip, which
        # stores strings, binaries and lists in their large layouts, drops
        # fixed_size_binary and may strip extension metadata. Every output
        # column is typed from these, never from what Arrow infers for the
        # Python values: a merged column becomes list<input type>, and every
        # other column gets its input type back.
        left_types = {f.name: f.type for f in left_table.schema}
        right_types = {f.name: f.type for f in right_table.schema}
        merged_names = set(colliding_keys) | {
            f"{constants.SOURCE_PREFIX}{col}" for col in colliding_keys
        }
        merged_element_types: dict[str, "pa.DataType"] = {}
        passthrough_types: dict[str, "pa.DataType"] = {}
        for name in left_types.keys() | right_types.keys():
            if name in left_types and name in right_types:
                column_type = _common_type(name, left_types[name], right_types[name])
            else:
                column_type = left_types.get(name) or right_types[name]
            if name in merged_names:
                merged_element_types[name] = column_type
            else:
                passthrough_types[name] = column_type

        # Capture nullable flags from input schemas BEFORE Polars conversion.
        # Polars' join discards nullable info (defaults all to True); we derive
        # the output schema from the inputs instead of from data null counts.
        # Only capture tag and data columns — system tag and source columns
        # are internally created with Arrow's all-nullable default; those fall
        # through to null-count inference below.
        captured_cols = set(left_tag_keys) | set(left_data_keys) | set(right_tag_keys) | set(right_data_keys)
        left_nullable = {f.name: f.nullable for f in left_table.schema if f.name in captured_cols}
        right_nullable = {f.name: f.nullable for f in right_table.schema if f.name in captured_cols}
        # Build expected output nullable map
        output_nullable: dict[str, bool] = {}
        output_nullable.update(left_nullable)
        for name, nullable in right_nullable.items():
            if name not in output_nullable:
                output_nullable[name] = nullable
        # Merged (colliding) data columns become 2-element lists — always non-null
        for col in colliding_keys:
            output_nullable[col] = False
            source_col = f"{constants.SOURCE_PREFIX}{col}"
            if source_col in output_nullable:
                output_nullable[source_col] = False

        # Perform inner join via Polars on shared tag keys
        # Use a common key trick to ensure cartesian product if no shared tags
        COMMON_JOIN_KEY = "_common"
        # COMMON_JOIN_KEY is a temporary column that gets dropped after the join
        output_nullable.pop(COMMON_JOIN_KEY, None)
        # Use int8 explicitly so a zero-row table doesn't produce a null-typed
        # column that Polars rejects as a join key (same as Join).
        left_table = left_table.add_column(
            0, COMMON_JOIN_KEY, pa.array([0] * len(left_table), type=pa.int8())
        )
        right_table = right_table.add_column(
            0, COMMON_JOIN_KEY, pa.array([0] * len(right_table), type=pa.int8())
        )

        join_keys = list(shared_tag_keys | {COMMON_JOIN_KEY})

        # Track which columns Polars will auto-suffix with _right
        # (right-table columns that collide with left, excluding join keys)
        left_col_set = set(left_table.column_names) - {COMMON_JOIN_KEY}
        right_col_set = set(right_table.column_names) - {COMMON_JOIN_KEY}
        join_key_set = set(join_keys) - {COMMON_JOIN_KEY}
        polars_suffixed_bases = (right_col_set & left_col_set) - join_key_set

        joined = (
            pl.DataFrame(left_table)
            .join(pl.DataFrame(right_table), on=join_keys, how="inner")
            .to_arrow()
        )
        joined = joined.drop(COMMON_JOIN_KEY)

        # Use the left stream's type converter — not the default context — so that a
        # MergeJoin over streams built with a non-default DataContext reconstructs the
        # merged column's extension type from the correct registry.
        tc = left_stream.data_context.type_converter

        # Process colliding data columns: merge into sorted lists
        for col in colliding_keys:
            left_col_name = col
            right_col_name = f"{col}_right"

            left_source_col = f"{constants.SOURCE_PREFIX}{col}"
            right_source_col = f"{left_source_col}_right"

            if right_col_name not in joined.column_names:
                continue

            left_vals = joined.column(left_col_name).to_pylist()
            right_vals = joined.column(right_col_name).to_pylist()

            # Also handle corresponding source columns
            has_source = (
                left_source_col in joined.column_names
                and right_source_col in joined.column_names
            )
            if has_source:
                left_sources = joined.column(left_source_col).to_pylist()
                right_sources = joined.column(right_source_col).to_pylist()

            merged_vals = []
            merged_sources = [] if has_source else None
            for i in range(len(left_vals)):
                lv, rv = left_vals[i], right_vals[i]
                if has_source:
                    ls, rs = left_sources[i], right_sources[i]
                    # Sort by data value, carry source along
                    pairs = sorted(zip([lv, rv], [ls, rs]), key=lambda p: p[0])
                    merged_vals.append([p[0] for p in pairs])
                    merged_sources.append([p[1] for p in pairs])
                else:
                    merged_vals.append(sorted([lv, rv]))

            # Replace the left column with merged list, drop right column
            col_idx = joined.column_names.index(left_col_name)
            joined = joined.drop(left_col_name)
            joined = joined.drop(right_col_name)

            merged_type = arrow_utils.aggregated_list_type(
                merged_element_types[col], tc
            )
            merged_array = arrow_utils.list_array(merged_vals, merged_type)
            joined = joined.add_column(col_idx, left_col_name, merged_array)

            if has_source:
                source_idx = joined.column_names.index(left_source_col)
                joined = joined.drop(left_source_col)
                joined = joined.drop(right_source_col)
                source_type = arrow_utils.aggregated_list_type(
                    merged_element_types[left_source_col], tc
                )
                source_array = arrow_utils.list_array(merged_sources or [], source_type)
                joined = joined.add_column(source_idx, left_source_col, source_array)

        # Handle remaining Polars-generated _right suffixed columns
        # (only from columns we know Polars auto-suffixed, not original names)
        for base_name in polars_suffixed_bases:
            suffixed_name = f"{base_name}_right"
            if suffixed_name not in joined.column_names:
                continue  # Already handled during colliding column processing
            if base_name not in joined.column_names:
                # Left version was removed, rename right to original
                idx = joined.column_names.index(suffixed_name)
                col_data = joined.column(suffixed_name)
                joined = joined.drop(suffixed_name)
                joined = joined.add_column(idx, base_name, col_data)
            else:
                # Both versions exist, drop the right one
                joined = joined.drop(suffixed_name)

        # Sort system tag values for same-pipeline-hash streams to ensure commutativity
        joined = arrow_utils.sort_system_tag_values(joined)

        # Reorder: tag columns first, then data columns
        all_tag_keys = set(left_tag_keys) | set(right_tag_keys)
        tag_cols = [c for c in joined.column_names if c in all_tag_keys]
        other_cols = [c for c in joined.column_names if c not in all_tag_keys]
        joined = joined.select(tag_cols + other_cols)

        # Reconstruct the schema: pass-through columns get their input Arrow
        # type back (undoing Polars' layout changes), merged columns keep the
        # list type built above, and nullability comes from the captured input
        # flags, falling back to null-count inference for unexpected columns.
        result_schema = pa.schema([
            pa.field(
                col_name,
                passthrough_types.get(col_name, joined.schema.field(col_name).type),
                nullable=output_nullable.get(col_name, joined.column(i).null_count > 0),
            )
            for i, col_name in enumerate(joined.column_names)
        ])
        joined = joined.cast(result_schema)
        return ArrowTableStream(
            joined,
            tag_columns=tuple(all_tag_keys),
        )

    def identity_structure(self) -> Any:
        return self.__class__.__name__

    def __repr__(self) -> str:
        return "MergeJoin()"
