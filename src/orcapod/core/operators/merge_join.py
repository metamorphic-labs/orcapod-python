from collections.abc import Sequence
from typing import Any, ClassVar, TYPE_CHECKING, cast

from orcapod.core.operators.base import (
    BinaryOperator,
    require_matchable_key,
    require_shared_arrow_type,
    require_tag_data_roles_agree,
)
from orcapod.core.operators.join import (
    _MERGED_SOURCE_PREFIX,  # pyright: ignore[reportPrivateUsage]  # one definition, shared with Join
    _inner_join,  # pyright: ignore[reportPrivateUsage]  # one definition, shared with Join
    Join,
)
from orcapod.core.operators.key_nullability import (
    key_nullable,
    reconcile_key_nullability,
)
from orcapod.core.data_tags import (
    check_namespaces,
    data_tag_names,
    stream_data_tags,
)
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import arrow_utils, schema_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import numpy as np
    import pyarrow as pa
else:
    np = LazyModule("numpy")
    pa = LazyModule("pyarrow")


def _output_types(
    left_types: "dict[str, pa.DataType]", right_types: "dict[str, pa.DataType]"
) -> "dict[str, pa.DataType]":
    """Return the Arrow type each input column takes in the output.

    Every type is the large-normalized form of the input type
    (``arrow_utils.normalize_to_large_types``); width, nullability and logical
    types are never changed. A column in both inputs goes through
    ``require_shared_arrow_type``, which either finds the shared normalized
    type or raises.

    Args:
        left_types: Column name to Arrow type for one input table.
        right_types: Column name to Arrow type for the other input table.

    Returns:
        Column name to its output Arrow type, for every column of either input.
    """
    resolved: dict[str, pa.DataType] = {}
    for name in left_types.keys() | right_types.keys():
        left_type, right_type = left_types.get(name), right_types.get(name)
        if left_type is not None and right_type is not None:
            resolved[name] = require_shared_arrow_type(
                "MergeJoin", name, left_type, right_type
            )
        else:
            known = left_type if left_type is not None else right_type
            assert known is not None
            resolved[name] = arrow_utils.normalize_to_large_types(known, keep_null=True)
    return resolved


def _merged_list(
    first: "pa.ChunkedArray[Any]",
    second: "pa.ChunkedArray[Any]",
    list_type: "pa.DataType",
) -> "pa.Array[Any]":
    """Return the two-element lists of one merged column, built from the inputs' arrays.

    Row ``i`` is ``[first[i], second[i]]``: the first argument's value, then the
    second's, compared with nothing (ITL-1281). The values are taken from the
    joined arrays (``take``) and the lists built from offsets
    (``arrow_utils.aggregated_list_array``), so every value is the input's, bit
    for bit, for every Arrow type, a struct and a null included.

    Args:
        first: The column of the first argument, one value per joined row.
        second: The same column of the second argument.
        list_type: The merged column's type, from ``aggregated_list_type``.

    Returns:
        The list array, one two-element list per joined row.
    """
    count = len(first)
    values = pa.chunked_array(
        [*first.chunks, *second.chunks], type=first.type
    ).combine_chunks()
    # Row i holds value i of the first column, then value i of the second.
    order = np.empty(2 * count, dtype=np.int64)
    order[0::2] = np.arange(count, dtype=np.int64)
    order[1::2] = np.arange(count, 2 * count, dtype=np.int64)
    offsets = pa.array(np.arange(0, 2 * count + 1, 2, dtype=np.int64), pa.int64())
    return arrow_utils.aggregated_list_array(
        values.take(pa.array(order, pa.int64())),
        cast("pa.Int64Array", offsets),
        list_type,
    )


class MergeJoin(BinaryOperator):
    """
    Binary operator that joins two streams, merging colliding data columns
    into two-element lists in argument order.

    MergeJoin is ordered, not commutative (developer, November 19, 2026,
    ITL-1281): ``MergeJoin()(a, b)`` and ``MergeJoin()(b, a)`` are two
    operations with two identities (``argument_symmetry`` is a tuple). For a
    data column that exists in both streams, each output row holds the list
    ``[value of the first argument, value of the second]``. Nothing is sorted
    and no values are compared, so every list of a row is parallel by origin
    row: element ``i`` of every merged column and of its source-info list comes
    from input ``i``, whose system-tag columns end in ``:{i}``. The lists are
    built from the inputs' Arrow arrays, so every value is the input's, bit for
    bit, whatever its type (a struct, a logical type and a null included). A
    column of the Arrow ``null`` type keeps that type in this table; read as rows
    (``Data``) or on the async path it is typed ``large_string`` (ITL-1319).

    For non-colliding columns, values are kept as scalars (same as regular Join).

    Tag columns use inner join on shared tags, with union of tag schemas.

    Arrow types: every output column takes the large-normalized form of its
    input Arrow type (``string`` becomes ``large_string``, ``list`` becomes
    ``large_list``, recursively; the Arrow hasher treats the two variants as
    identical), and nothing is inferred from Python values. Integer and float
    width, nullability and logical (extension) types are kept. A merged column
    is ``large_list<T>`` where ``T`` is the normalized input element type,
    built by ``arrow_utils.aggregated_list_type`` (the construction ``Batch``
    and ``GroupBy`` use), with or without rows; a logical ``T`` yields the
    logical list type of ``T``. Its source-info column is
    ``large_list<large_string>``.

    When the two inputs' Arrow types for the same column differ, both are
    normalized to their large variants; if they are then equal the join
    proceeds, otherwise it raises ``InputValidationError`` naming the column
    and both types (``require_shared_arrow_type``). Validation cannot detect
    such a disagreement, because streams expose only Python schemas; it is
    detected in ``binary_static_process`` before the join.

    Data-tags (ITL-1194) follow ``Join``, with ``Join``'s helpers. A tag
    ``x`` that is a data-tag in both inputs is a join key, so its two values are equal: the
    output has ONE scalar data-tag column ``x``, not a two-element list, and its
    source info is one token, the ``join_v1`` fold of the two inputs' tokens
    (sorted). A tag that is a data-tag in one input stays a data-tag, with its own token,
    whether the other input has it as a plain tag or not at all. A data-tag
    of one input named like a data-only column of the other is refused (the tag and
    data role check). A shared key that is ``T`` in one input and ``T | None`` in
    the other is ``T`` in both roles (ITL-829). Every other colliding data column is
    merged into a list in argument order, as for plain inputs.
    """

    # Provisional: its identity is not frozen with data context v1 (ITL-1279).
    PROVISIONAL: ClassVar[bool] = True

    # Data-tags (ITL-1023) are handled (ITL-1194).
    DATA_TAGS_ISSUE: ClassVar[str | None] = None

    @property
    def kernel_id(self) -> tuple[str, ...]:
        return (f"{self.__class__.__name__}",)

    def is_commutative(self) -> bool:
        """MergeJoin is ordered: its lists hold the first argument's value first."""
        return False

    def validate_binary_inputs(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> None:
        self._require_tag_data_roles_agree(left_stream, right_stream)
        left_data_schema, right_data_schema, merged = self._input_data_schemas(
            left_stream, right_stream
        )

        # Colliding data columns must have identical types since they are
        # merged into list[T] — both sides must contribute the same T. A merged
        # data-tag column is one value, checked in ``binary_output_schema``.
        colliding_keys = (
            set(left_data_schema.keys()) & set(right_data_schema.keys())
        ) - merged
        for key in sorted(colliding_keys):
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

        # Tag and data columns are listed by first appearance across the inputs
        # in argument order (MergeJoin is ordered). This is the single definition
        # of the order; ``binary_static_process`` reads it back from here.
        self._require_tag_data_roles_agree(left_stream, right_stream)

        # Always get input schemas WITHOUT system tags for the base computation.
        # System tags are computed separately because the join renames them.
        left_tag_schema, _ = left_stream.output_schema()
        right_tag_schema, _ = right_stream.output_schema()
        left_data_schema, right_data_schema, merged = self._input_data_schemas(
            left_stream, right_stream
        )

        # Tag schema: union of both tag schemas. A shared tag key that differs in
        # nullability only is non-nullable (ITL-829): a null key matches nothing.
        tag_schema = schema_utils.union_schemas(
            *reconcile_key_nullability(left_tag_schema, right_tag_schema)
        )

        # A data-tag of both inputs is one column, one value (ITL-1194).
        for name in sorted(merged):
            if left_data_schema[name] != right_data_schema[name]:
                raise InputValidationError(
                    f"MergeJoin: data-tag {name!r} has type "
                    f"{left_data_schema[name]} in one input and "
                    f"{right_data_schema[name]} in the other"
                )

        # Data schema: colliding columns become list[T], non-colliding stay scalar
        colliding_schema = schema_utils.intersection_schemas(
            {k: v for k, v in left_data_schema.items() if k not in merged},
            {k: v for k, v in right_data_schema.items() if k not in merged},
        )

        merged_data_schema = {}
        all_data_keys = [*left_data_schema] + [
            key for key in right_data_schema if key not in left_data_schema
        ]
        for key in all_data_keys:
            if key in colliding_schema:
                merged_data_schema[key] = list[colliding_schema[key]]
            elif key in left_data_schema:
                merged_data_schema[key] = left_data_schema[key]
            else:
                merged_data_schema[key] = right_data_schema[key]

        # A data-tag's column is laid out once, among the tags, and a
        # stream takes its data order from the layout: the data-tag columns
        # lead the data, in tag order. Without data-tags the order is as
        # before.
        data_tags = set(data_tag_names(tag_schema, merged_data_schema))
        check_namespaces(tag_schema, merged_data_schema, data_tags)
        merged_data_schema = {
            **{
                name: merged_data_schema[name]
                for name in tag_schema
                if name in data_tags
            },
            **{k: v for k, v in merged_data_schema.items() if k not in data_tags},
        }

        # Add system tag columns if requested
        if columns_config.system_tags:
            system_tag_schema = self._predict_system_tag_schema(
                left_stream, right_stream
            )
            tag_schema = schema_utils.union_schemas(tag_schema, system_tag_schema)

        return tag_schema, Schema(merged_data_schema)

    @staticmethod
    def _input_data_schemas(
        first_stream: StreamProtocol, second_stream: StreamProtocol
    ) -> tuple[Schema, Schema, set[str]]:
        """Return the inputs' data schemas as the join sees them, and the merged names.

        As in ``Join``: a data-tag key the join narrows to non-nullable (ITL-829)
        has that type in its data role too (``Join._narrow_data_tags``), and the
        merged columns are the data-tags of both inputs
        (``Join._merged_data_tags``).

        Args:
            first_stream: One input.
            second_stream: The other input.

        Returns:
            ``(first data schema, second data schema, merged names)``.
        """
        first_tags, first_data = first_stream.output_schema()
        second_tags, second_data = second_stream.output_schema()
        first_refs = set(data_tag_names(first_tags, first_data))
        second_refs = set(data_tag_names(second_tags, second_data))
        merged = Join._merged_data_tags(  # pyright: ignore[reportPrivateUsage]  # Join's one rule
            first_tags, first_refs, second_tags, second_refs
        )
        first_reconciled, second_reconciled = reconcile_key_nullability(
            first_tags, second_tags
        )
        return (
            Join._narrow_data_tags(  # pyright: ignore[reportPrivateUsage]  # Join's one rule
                first_data, first_tags, first_reconciled, first_refs
            ),
            Join._narrow_data_tags(  # pyright: ignore[reportPrivateUsage]  # Join's one rule
                second_data, second_tags, second_reconciled, second_refs
            ),
            merged,
        )

    @staticmethod
    def _require_tag_data_roles_agree(
        left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> None:
        """Raise if a name is a tag in one input and a data column in the other.

        Called by the schema prediction (so by input validation) and at the start
        of ``binary_static_process`` (the async path calls it directly).

        Raises:
            InputValidationError: Naming the column, the roles and the remedy.
        """
        try:
            require_tag_data_roles_agree(*left_stream.keys(), *right_stream.keys())
        except InputValidationError as exc:
            raise InputValidationError(f"MergeJoin: {exc}") from exc

    def _system_tag_suffix(self, stream: StreamProtocol, position: int) -> str:
        """Return what an input's system-tag column names gain: ``{pipeline_hash}:{position}``.

        ``position`` is the input's argument index (0 for the first, 1 for the
        second): MergeJoin is ordered, so its inputs are never reordered.
        """
        n_char = self.orcapod_config.hashing.system_tag_n_char
        return f"{stream.pipeline_hash().to_hex(n_char)}:{position}"

    def _predict_system_tag_schema(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> Schema:
        """Predict the system tag columns that the join would produce.

        Each input stream's existing system tag columns get renamed by
        appending ``::{pipeline_hash}:{argument index}``. This method computes
        those output column names without performing the join.
        """
        system_tag_fields: dict[str, type] = {}
        for position, stream in enumerate((left_stream, right_stream)):
            suffix = self._system_tag_suffix(stream, position)
            stream_tag_schema, _ = stream.output_schema(columns={"system_tags": True})
            for col_name, col_type in stream_tag_schema.items():
                if col_name.startswith(constants.SYSTEM_TAG_PREFIX):
                    new_name = f"{col_name}{constants.BLOCK_SEPARATOR}{suffix}"
                    system_tag_fields[new_name] = col_type
        return Schema(system_tag_fields)

    def binary_static_process(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> StreamProtocol:
        """Inner-join two streams on their shared tags, merging colliding data columns.

        A data column present in both streams becomes a two-element list per
        row, ``[left value, right value]``, and its source-info column the list
        of the two tokens in the same order. Polars only decides which rows
        match, from the prepared key columns and row numbers (``Join``'s
        ``_inner_join``, ITL-1284); every output value is taken from the input
        tables, and the lists are built from their arrays (ITL-1281). System tag
        columns are renamed with each stream's pipeline hash and argument
        index. A zero-row input yields a zero-row result with the same column
        types as a non-empty one.

        Args:
            left_stream: The left (first) input stream.
            right_stream: The right (second) input stream.

        Returns:
            The joined stream, with tag columns first.

        Raises:
            InputValidationError: If a column the two inputs share has two Arrow
                types, or a shared tag key has a type with no rule for matching.
        """
        self._require_tag_data_roles_agree(left_stream, right_stream)

        # Tables with source info and system tags, each input's system-tag
        # column names extended with its pipeline hash and argument index.
        left_table = arrow_utils.append_to_system_tags(
            left_stream.as_table(columns={"source": True, "system_tags": True}),
            self._system_tag_suffix(left_stream, 0),
        )
        right_table = arrow_utils.append_to_system_tags(
            right_stream.as_table(columns={"source": True, "system_tags": True}),
            self._system_tag_suffix(right_stream, 1),
        )

        # Determine shared tag keys for inner join
        left_tag_keys, left_data_keys = left_stream.keys()
        right_tag_keys, right_data_keys = right_stream.keys()
        shared_tag_keys = set(left_tag_keys) & set(right_tag_keys)

        # A data-tag of both inputs is a join key: the join keeps one column,
        # and its two source-info tokens become one (ITL-1194, as ``Join``). The
        # second input's token is set aside under a temporary name until then.
        merged = Join._merged_data_tags(  # pyright: ignore[reportPrivateUsage]  # Join's one rule
            left_tag_keys,
            stream_data_tags(left_stream),
            right_tag_keys,
            stream_data_tags(right_stream),
        )
        right_source = {
            f"{constants.SOURCE_PREFIX}{name}": f"{_MERGED_SOURCE_PREFIX}{name}"
            for name in sorted(merged)
            if f"{constants.SOURCE_PREFIX}{name}" in right_table.column_names
        }
        if right_source:
            right_table = right_table.rename_columns(
                [right_source.get(c, c) for c in right_table.column_names]
            )

        # Find colliding data columns (a merged data-tag column is not one)
        colliding_keys = (set(left_data_keys) & set(right_data_keys)) - merged

        # Every output column is typed from the inputs' Arrow types. Inputs
        # whose types differ beyond layout are rejected here, before the join
        # (require_shared_arrow_type), and so is a key Polars cannot be given.
        left_types = {f.name: f.type for f in left_table.schema}
        right_types = {f.name: f.type for f in right_table.schema}
        output_types = _output_types(left_types, right_types)
        for key in sorted(shared_tag_keys):
            require_matchable_key("MergeJoin", key, output_types[key])

        # The joined table is built from arrays with no field flags, so the
        # flags are taken from the inputs. Only tag and data columns are
        # captured: system tag and source columns are internally created with
        # Arrow's all-nullable default; those fall through to null-count
        # inference below.
        captured_cols = (
            set(left_tag_keys)
            | set(left_data_keys)
            | set(right_tag_keys)
            | set(right_data_keys)
        )
        left_nullable = {
            f.name: f.nullable for f in left_table.schema if f.name in captured_cols
        }
        right_nullable = {
            f.name: f.nullable for f in right_table.schema if f.name in captured_cols
        }
        # Build expected output nullable map
        output_nullable: dict[str, bool] = {}
        output_nullable.update(left_nullable)
        for name, nullable in right_nullable.items():
            if name not in output_nullable:
                output_nullable[name] = nullable
        # A shared tag key is nullable only when nullable in both inputs: a null
        # key matches nothing (ITL-829). Equal flags give the flag as before.
        for name in shared_tag_keys:
            output_nullable[name] = key_nullable(
                left_nullable[name], right_nullable[name]
            )
        # Merged (colliding) data columns become 2-element lists — always non-null
        for col in colliding_keys:
            output_nullable[col] = False
            source_col = f"{constants.SOURCE_PREFIX}{col}"
            if source_col in output_nullable:
                output_nullable[source_col] = False

        # Inner join on the shared tag keys. A constant key makes the join a
        # cartesian product when the inputs share no tag; int8 explicitly, so a
        # zero-row table doesn't produce a null-typed column (same as Join).
        COMMON_JOIN_KEY = "_common"
        output_nullable.pop(COMMON_JOIN_KEY, None)
        left_table = left_table.add_column(
            0, COMMON_JOIN_KEY, pa.array([0] * len(left_table), type=pa.int8())
        )
        right_table = right_table.add_column(
            0, COMMON_JOIN_KEY, pa.array([0] * len(right_table), type=pa.int8())
        )
        join_keys = sorted(shared_tag_keys | {COMMON_JOIN_KEY})

        # The right-table columns the join suffixes with ``_right``: those the
        # left table also has, other than the keys.
        left_col_set = set(left_table.column_names) - {COMMON_JOIN_KEY}
        right_col_set = set(right_table.column_names) - {COMMON_JOIN_KEY}
        suffixed_bases = (right_col_set & left_col_set) - set(join_keys)

        joined = _inner_join(left_table, right_table, join_keys)
        joined = joined.drop(COMMON_JOIN_KEY)
        for left_name, right_name in right_source.items():
            joined = Join._merge_source_columns(joined, left_name, right_name)  # pyright: ignore[reportPrivateUsage]  # Join's one rule
            joined = Join._fold_source_column(joined, left_name)  # pyright: ignore[reportPrivateUsage]  # Join's one rule

        # Use the left stream's type converter — not the default context — so that a
        # MergeJoin over streams built with a non-default DataContext reconstructs the
        # merged column's extension type from the correct registry.
        tc = left_stream.data_context.type_converter

        # Merge each colliding data column, and its source-info column, into
        # lists in argument order: [left value, right value].
        for col in sorted(colliding_keys):
            source_col = f"{constants.SOURCE_PREFIX}{col}"
            pairs = [(col, output_types[col])]
            if (
                source_col in joined.column_names
                and f"{source_col}_right" in joined.column_names
            ):
                pairs.append((source_col, output_types[source_col]))
            for name, element_type in pairs:
                list_type = arrow_utils.aggregated_list_type(
                    element_type, tc, column=name
                )
                merged_list = _merged_list(
                    joined.column(name), joined.column(f"{name}_right"), list_type
                )
                index = joined.column_names.index(name)
                joined = joined.drop_columns([name, f"{name}_right"])
                joined = joined.add_column(index, name, merged_list)
                output_types[name] = list_type

        # Any other right column the join suffixed: kept under its own name when
        # the left one is gone, dropped when the left one is still there.
        for base_name in suffixed_bases:
            suffixed_name = f"{base_name}_right"
            if suffixed_name not in joined.column_names:
                continue  # Already handled during colliding column processing
            if base_name not in joined.column_names:
                idx = joined.column_names.index(suffixed_name)
                col_data = joined.column(suffixed_name)
                joined = joined.drop_columns([suffixed_name])
                joined = joined.add_column(idx, base_name, col_data)
            else:
                joined = joined.drop_columns([suffixed_name])

        # Lay the columns out in the order ``binary_output_schema`` defines:
        # tags, then data, then the hidden columns. ``ArrowTableStream`` reads
        # its schema from the physical order and its keys from ``tag_columns``,
        # so both must follow it. No hash or record ID depends on column order.
        tag_schema, data_schema = self.binary_output_schema(left_stream, right_stream)
        tag_cols = list(tag_schema.keys())
        # A data-tag's column is laid out once, among the tags.
        data_cols = [name for name in data_schema if name not in tag_schema]
        listed = {*tag_cols, *data_cols}
        other_cols = [c for c in joined.column_names if c not in listed]
        joined = joined.select(tag_cols + data_cols + other_cols)

        # Reconstruct the schema: every column takes the type decided above
        # from the inputs (the large-normalized input type for pass-through
        # columns; the list type built above for merged columns), and
        # nullability comes from the captured input flags, falling back to
        # null-count inference for system tag and source-info columns. The
        # values are the inputs'.
        result_schema = pa.schema(
            [
                pa.field(
                    col_name,
                    output_types[col_name],
                    nullable=output_nullable.get(
                        col_name, joined.column(i).null_count > 0
                    ),
                )
                for i, col_name in enumerate(joined.column_names)
            ]
        )
        # Only a column whose type differs is cast (a layout change): Arrow's cast of
        # a ``large_list<null>`` column to its own type corrupts it.
        joined = pa.Table.from_arrays(
            [
                column if column.type == field.type else column.cast(field.type)
                for column, field in zip(joined.columns, result_schema)
            ],
            schema=result_schema,
        )
        return ArrowTableStream(
            joined,
            tag_columns=tuple(tag_cols),
            data_tags=data_tag_names(tag_schema, data_schema),
        )

    def rows_dropped_text(self, position: int, others: Sequence[str]) -> str | None:
        """What the join drops, for ``explain()``: rows without a match in the others."""
        names = ", ".join(repr(name) for name in others)
        return f"rows without a match in {names} are dropped"

    def identity_structure(self) -> Any:
        self._note_provisional_identity_use()  # a direct read is a use (ITL-1279)
        return self.__class__.__name__

    def __repr__(self) -> str:
        return "MergeJoin()"
