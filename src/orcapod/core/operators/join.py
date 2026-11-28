import asyncio
import functools
import itertools
import json
from collections.abc import Callable, Collection, Mapping, Sequence
from typing import Any, ClassVar, get_args

import polars as pl
import pyarrow as pa

from orcapod.types.explanation import plain_repr
from orcapod.core.operators.meta_columns import (
    WithoutMetaWriter,
    without_input_meta,
    without_meta_columns,
    without_meta_fields,
)
from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.match_keys import frozen_scalar
from orcapod.core.operators.arrow_keys import polars_key_frame
from orcapod.core.operators.base import (
    NonZeroInputOperator,
    require_matchable_key,
    require_shared_arrow_type,
    explain_nested_join_conflict,
    require_tag_data_roles_agree,
)
from orcapod.core.data_tags import (
    check_namespaces,
    data_tag_names,
    data_tags_first,
    merge_source_info,
    stream_data_tags,
)
from orcapod.core.operators.key_nullability import (
    key_nullable,
    reconcile_key_nullability,
)
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import (
    ArgumentGroup,
    DataProtocol,
    StreamProtocol,
    TagProtocol,
)
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, ContentHash, DataType, DataValue, Schema
from orcapod.utils import arrow_utils, schema_utils

# The row-number columns of the frames Polars joins (``_inner_join``); a tag, so a
# key, never starts with the meta prefix ``__``.
_LEFT_ROW = "__join_left_row"
_RIGHT_ROW = "__join_right_row"

# Temporary name of the right input's source info of a merged data-tag column
# during the join (``static_process``); never in an output.
_MERGED_SOURCE_PREFIX = "__join_merged_source__"

# A column that is a data-tag on several inputs gets ONE merged token per value, folded once
# over every input's token (ITL-1017), not pairwise: a pairwise fold would depend on
# the order of the join chain, which inputs tied on pipeline hash leave to the
# argument order. Between the pairwise steps of one join, each token position holds
# the members met so far, as this marker followed by their JSON array; the join folds
# them when it emits a row (``_fold_pending``). Never in an output.
_PENDING_MEMBERS = "\x00join-pending-members\x00"


# The constant key every join adds to both sides' keys (``_with_common_key``): with it,
# inputs that share no tag give every pair, and Polars applies its multi-key rule, under
# which a row whose key holds a top-level null matches nothing (a null list, struct or
# ``decimal256`` key too, which a single-key Polars join would match with another null).
_COMMON_JOIN_KEY = "_common"


def _with_common_key(table: "pa.Table") -> "pa.Table":
    """Return ``table`` with the constant join key ``_COMMON_JOIN_KEY`` as its first column.

    ``int8`` explicitly, so a zero-row table gets no ``null``-typed column, which Polars
    refuses in a join.
    """
    return table.add_column(
        0, _COMMON_JOIN_KEY, pa.array([0] * len(table), type=pa.int8())
    )


def _matched_pairs(
    left: "pl.DataFrame", right: "pl.DataFrame", keys: Sequence[str]
) -> "pl.DataFrame":
    """The pairs of rows a join matches: Polars' inner join of two key frames.

    The one statement of which rows a join pairs, for the sync join (``_inner_join``)
    and the streaming kernels. ``keys`` holds ``_COMMON_JOIN_KEY`` and both frames hold
    it (``_with_common_key``); the frames are ``arrow_keys.polars_key_frame``'s.

    Args:
        left: The left key frame, with its row-number column.
        right: The right key frame, with its row-number column.
        keys: The key columns, ``_COMMON_JOIN_KEY`` among them.

    Returns:
        One row per matched pair, holding both row-number columns.
    """
    return left.join(right, on=list(keys), how="inner")


def _inner_join(left: "pa.Table", right: "pa.Table", keys: list[str]) -> "pa.Table":
    """Inner-join two tables on their key columns, every value taken from the tables.

    Polars decides which rows match, from the prepared keys and the row numbers
    only (``arrow_keys``, ITL-1284); every output column is taken from ``left`` or
    ``right`` by those row numbers, so each value is the input's, bit for bit. The
    layout is the one Polars' join gives: the left columns (one of each key, the
    left row's value), then the right columns that are not keys, a name the left
    table also has taking the suffix ``_right``. The rows are in the order of
    Polars' join.

    Args:
        left: The left table.
        right: The right table.
        keys: The key columns, in both tables, each checked by
            ``require_matchable_key``.

    Returns:
        The joined table. Its fields carry no flags of the inputs (every field
        nullable); the caller restores them.
    """
    pairs = _matched_pairs(
        polars_key_frame(left, keys, _LEFT_ROW),
        polars_key_frame(right, keys, _RIGHT_ROW),
        keys,
    )
    left_rows = pairs.get_column(_LEFT_ROW).to_arrow()
    right_rows = pairs.get_column(_RIGHT_ROW).to_arrow()
    names: list[str] = []
    arrays: list[Any] = []
    for name in left.column_names:
        names.append(name)
        arrays.append(left.column(name).take(left_rows))
    for name in right.column_names:
        if name in keys:
            continue
        names.append(f"{name}_right" if name in left.column_names else name)
        arrays.append(right.column(name).take(right_rows))
    return pa.Table.from_arrays(arrays, names=names)


def _members(token: str | None) -> list[str | None]:
    """The member tokens a token position holds so far (itself, if not pending)."""
    if token is not None and token.startswith(_PENDING_MEMBERS):
        return json.loads(token.removeprefix(_PENDING_MEMBERS))
    return [token]


def _pending(tokens: Sequence[str | None]) -> str:
    """Hold the members of ``tokens`` at one token position until the join emits."""
    members = [member for token in tokens for member in _members(token)]
    return _PENDING_MEMBERS + json.dumps(members, ensure_ascii=False)


def _merge_pending(left: Any, right: Any) -> Any:
    """Merge two inputs' source info of one merged column, keeping its shape."""
    return merge_source_info([left, right], combine=_pending)


JOIN_FOLD_LABEL = "join_v1"
"""Label of the fold of ``Join``: the one source-info token of a column that is a data-tag on
several inputs is ``"join_v1:<hex>"`` (ITL-1017, ITL-1154). It names the operation;
the procedure is the members' tokens sorted (a missing token last), then
``arrow_utils.fold_system_tag_values``. The sort is part of it: the join meets the
members in the order of its chain, which is not canonical, and the provenance of N
equal values is a multiset. Change it only when this fold would give an existing
member list a different value.
"""


def _fold_members(tokens: Sequence[str | None]) -> str:
    """Fold the members' tokens of one merged value into its one token (``join_v1``)."""
    ordered = sorted(tokens, key=lambda token: (token is None, token or ""))
    return arrow_utils.fold_system_tag_values(ordered, label=JOIN_FOLD_LABEL)


def _fold_pending(value: Any) -> Any:
    """Replace every pending token position of ``value`` by its merged token."""
    if isinstance(value, list):
        return [_fold_pending(item) for item in value]
    if isinstance(value, str) and value.startswith(_PENDING_MEMBERS):
        return _fold_members(_members(value))
    return value


class Join(NonZeroInputOperator):
    # Data-tags (ITL-1023): handled.
    DATA_TAGS_ISSUE: ClassVar[str | None] = None

    @property
    def kernel_id(self) -> tuple[str, ...]:
        """
        Returns a unique identifier for the kernel.
        This is used to identify the kernel in the computational graph.
        """
        return (f"{self.__class__.__name__}",)

    def validate_nonzero_inputs(self, *streams: StreamProtocol) -> None:
        """Validate that input streams are compatible for joining."""
        # TODO: add more helpful validation
        try:
            self.output_schema(*streams)
        except Exception as e:
            if isinstance(e, TypeError):
                explain_nested_join_conflict("Join", *streams)
            raise InputValidationError(f"Input streams are not compatible: {e}") from e

    def order_input_streams(self, *streams: StreamProtocol) -> list[StreamProtocol]:
        # Canonically order by pipeline_hash for deterministic operation.
        # pipeline_hash is structure-only, so streams with the same schema+topology
        # get the same ordering regardless of data content.
        return sorted(streams, key=lambda s: s.pipeline_hash().to_hex())

    def argument_symmetry(self, streams: Collection) -> ArgumentGroup:
        return frozenset(streams)

    def output_schema(
        self,
        *streams: StreamProtocol,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)

        if len(streams) == 1:
            # if single stream, simply return the output schema of the single input stream
            tag_schema, data_schema = streams[0].output_schema(
                columns=columns, all_info=all_info
            )
            return tag_schema, data_tags_first(
                tag_schema, without_meta_fields(data_schema)
            )

        tag_schema, data_schema = self._joined_user_schemas(*streams)
        # Data-tags first among the data columns, in tag order (ITL-1205).
        data_schema = data_tags_first(tag_schema, data_schema)

        # Add system tag columns if requested
        if columns_config.system_tags:
            system_tag_schema = self._predict_system_tag_schema(*streams)
            tag_schema = schema_utils.union_schemas(tag_schema, system_tag_schema)

        return tag_schema, data_schema

    def _joined_user_schemas(self, *streams: StreamProtocol) -> tuple[Schema, Schema]:
        """Union the inputs' tag and data schemas in Join's column order.

        Join is commutative, so its column order must not depend on operand
        order: tag and data columns are listed by first appearance across the
        inputs in canonical ``pipeline_hash`` order (``order_input_streams``;
        inputs with equal pipeline hashes keep call order). This is the single
        definition of the order: ``output_schema`` returns it and
        ``static_process`` lays its table out by it.

        System tags are excluded: the join renames them, and
        ``_predict_system_tag_schema`` computes them separately. Meta
        (``__``-prefixed) columns are excluded too: Join drops them
        (``meta_columns.without_meta_columns``), including the ones a raw ``ArrowTableStream``
        lists as data columns in its ``output_schema``.

        Args:
            *streams: Two or more input streams.

        Data-tags (specification section 9): a shared tag ``x`` that is
        a data-tag in either input is a data-tag in the output. When it is
        a data-tag in both, their data columns ``x`` are merged into one: the join
        equated the tags, so the values are equal (section 9.3). Every other data
        column shared by two inputs is still an error, and so is a plain tag of one
        input named like a data column of another (section 4.3).

        Returns:
            The ``(tag_schema, data_schema)`` of the joined user columns.

        Raises:
            InputValidationError: If two inputs share a data column that is not the
                target of a data-tag both of them join on, a merged column's
                types differ, or a plain tag would share a name with a data column.
        """
        self._require_tag_data_roles_agree(streams)
        ordered_streams = self.order_input_streams(*streams)
        stream = ordered_streams[0]
        tag_schema, data_schema = stream.output_schema()
        data_schema = without_meta_fields(data_schema)
        data_tags = set(data_tag_names(tag_schema, data_schema))
        for other_stream in ordered_streams[1:]:
            other_tag_schema, other_data_schema = other_stream.output_schema()
            other_data_schema = without_meta_fields(other_data_schema)
            other_data_tags = set(data_tag_names(other_tag_schema, other_data_schema))
            merged = self._merged_data_tags(
                tag_schema, data_tags, other_tag_schema, other_data_tags
            )
            # A shared tag key that differs in nullability only is non-nullable
            # (ITL-829): a null key matches nothing. A data-tag key is one
            # column, so its data role takes the same type (ITL-1023).
            reconciled, other_reconciled = reconcile_key_nullability(
                tag_schema, other_tag_schema
            )
            data_schema = self._narrow_data_tags(
                data_schema, tag_schema, reconciled, data_tags
            )
            other_data_schema = self._narrow_data_tags(
                other_data_schema, other_tag_schema, other_reconciled, other_data_tags
            )
            tag_schema, other_tag_schema = reconciled, other_reconciled
            shared_data = set(data_schema) & set(other_data_schema)
            if unmerged := shared_data - merged:
                # A shared data column of two types is reported as a type conflict,
                # as for plain inputs; of one type, as an overlap.
                _ = schema_utils.intersection_schemas(
                    {name: data_schema[name] for name in unmerged},
                    {name: other_data_schema[name] for name in unmerged},
                )
                raise InputValidationError(
                    f"Datas should not have overlapping keys, but {sorted(shared_data - merged)} found in {stream} and {other_stream}."
                )
            for name in sorted(merged):
                if data_schema[name] != other_data_schema[name]:
                    raise InputValidationError(
                        f"{type(self).__name__}: data-tag {name!r} has type "
                        f"{data_schema[name]} in one input and "
                        f"{other_data_schema[name]} in another"
                    )
            tag_schema = schema_utils.union_schemas(tag_schema, other_tag_schema)
            data_schema = schema_utils.union_schemas(data_schema, other_data_schema)
            data_tags |= other_data_tags
            check_namespaces(tag_schema, data_schema, data_tags)
        return tag_schema, data_schema

    @staticmethod
    def _narrow_data_tags(
        data_schema: Schema,
        tag_schema: Schema,
        reconciled: Schema,
        data_tags: Collection[str],
    ) -> Schema:
        """Give a data-tag key narrowed by ``reconcile_key_nullability`` its data type.

        A data-tag is one column in two roles (ITL-1023): when the join makes
        the key non-nullable (ITL-829), its data role is that type too.

        Args:
            data_schema: One input's data schema.
            tag_schema: The input's tag schema.
            reconciled: The tag schema after ``reconcile_key_nullability``.
            data_tags: The input's data-tags.

        Returns:
            ``data_schema`` with each narrowed data-tag key given its tag type.
        """
        narrowed = [
            name
            for name in data_tags
            if name in data_schema and reconciled[name] != tag_schema[name]
        ]
        return Schema(
            {
                name: reconciled[name] if name in narrowed else data_type
                for name, data_type in data_schema.items()
            },
            optional_fields=data_schema.optional_fields,
        )

    @staticmethod
    def _merged_data_tags(
        tags: Collection[str],
        data_tags: Collection[str],
        other_tags: Collection[str],
        other_data_tags: Collection[str],
    ) -> set[str]:
        """Return the data columns two join inputs merge into one (section 9.5).

        A data column is merged when it is the target of a data-tag in both
        inputs and that tag is a join key (a tag of both inputs). ``Join`` joins on
        every shared tag, so the second condition holds for every shared data-tag.

        Args:
            tags: The first input's tag names.
            data_tags: The first input's data-tags.
            other_tags: The second input's tag names.
            other_data_tags: The second input's data-tags.

        Returns:
            The names of the merged data columns.
        """
        keys = set(tags) & set(other_tags)
        return keys & set(data_tags) & set(other_data_tags)

    def _require_tag_data_roles_agree(self, streams: Sequence[StreamProtocol]) -> None:
        """Raise if a name is a tag in one input and a data column in another.

        Every pair of inputs is checked, in call order, so the error does not
        depend on the canonical order.

        Args:
            streams: The input streams, in call order.

        Raises:
            InputValidationError: Naming the column, the two inputs (counting
                from 1 in call order) and the remedy.
        """
        keys = [stream.keys() for stream in streams]
        for i, j in itertools.combinations(range(len(streams)), 2):
            try:
                require_tag_data_roles_agree(*keys[i], *keys[j])
            except InputValidationError as exc:
                raise InputValidationError(
                    f"{type(self).__name__} of input {i + 1} (the first) and input "
                    + f"{j + 1} (the "
                    + f"second): {exc}"
                ) from exc

    def _predict_system_tag_schema(self, *streams: StreamProtocol) -> Schema:
        """Predict the system tag columns that the join would produce.

        Each input stream's existing system tag columns get renamed by
        appending ::{pipeline_hash}:{canonical_position}. This method
        computes those output column names without performing the join.
        """
        n_char = self.orcapod_config.hashing.system_tag_n_char
        ordered_streams = self.order_input_streams(*streams)

        system_tag_fields: dict[str, DataType] = {}
        for idx, stream in enumerate(ordered_streams):
            stream_tag_schema, _ = stream.output_schema(columns={"system_tags": True})
            for col_name, col_type in stream_tag_schema.items():
                if col_name.startswith(constants.SYSTEM_TAG_PREFIX):
                    new_name = (
                        f"{col_name}{constants.BLOCK_SEPARATOR}"
                        f"{stream.pipeline_hash().to_hex(n_char)}:{idx}"
                    )
                    system_tag_fields[new_name] = col_type
        nullable = arrow_utils.sorted_system_tag_nullability(
            {
                name: kind is type(None) or type(None) in get_args(kind)
                for name, kind in system_tag_fields.items()
            }
        )
        return Schema(
            {
                name: kind | None if nullable[name] else kind
                for name, kind in system_tag_fields.items()
            }
        )

    def static_process(self, *streams: StreamProtocol) -> StreamProtocol:
        """Join the streams on their shared tag columns.

        The result holds every input's tag and data columns, source info and
        system tags (renamed per input). It holds none of the inputs' meta
        (``__``-prefixed) columns, which Join drops (``meta_columns.without_meta_columns``).

        Raises:
            InputValidationError: If two inputs share a data column, or a
                shared tag key has different Arrow types in two inputs.
        """
        if len(streams) == 1:
            return without_input_meta(streams[0], streams)

        # Canonically order streams by pipeline_hash for deterministic
        # system tag column names regardless of input order (Join is commutative)
        ordered_streams = self.order_input_streams(*streams)
        # The output column order, and the check that no two inputs share a
        # data column (meta columns are dropped, so they never collide).
        tag_schema, data_schema = self._joined_user_schemas(*streams)

        n_char = self.orcapod_config.hashing.system_tag_n_char

        stream = ordered_streams[0]

        tag_keys, _ = [set(k) for k in stream.keys()]
        data_tags = set(stream_data_tags(stream))
        # Source-info columns of merged data-tag columns, folded once at the end.
        merged_source: set[str] = set()
        table = without_meta_columns(
            stream.as_table(columns={"source": True, "system_tags": True}), tag_keys
        )
        # trick to get cartesian product; use int8 explicitly so that zero-row
        # tables don't produce a null-typed column that Polars rejects on join
        table = _with_common_key(table)
        table = arrow_utils.append_to_system_tags(
            table,
            f"{stream.pipeline_hash().to_hex(n_char)}:0",
        )

        for idx, next_stream in enumerate(ordered_streams[1:], start=1):
            next_tag_keys, _ = next_stream.keys()
            next_table = without_meta_columns(
                next_stream.as_table(columns={"source": True, "system_tags": True}),
                next_tag_keys,
            )
            next_table = arrow_utils.append_to_system_tags(
                next_table,
                f"{next_stream.pipeline_hash().to_hex(n_char)}:{idx}",
            )
            # trick to ensure that there will always be at least one shared key
            # this ensure that no overlap in keys lead to full caretesian product
            next_table = _with_common_key(next_table)

            common_tag_keys = tag_keys.intersection(next_tag_keys)
            next_data_tags = set(stream_data_tags(next_stream))
            merged = self._merged_data_tags(
                tag_keys, data_tags, next_tag_keys, next_data_tags
            )
            # The merged columns are join keys, so the join keeps one of each. Their
            # source info is kept from both sides: the right side's is set aside
            # under a temporary name and merged after the join (section 9.4).
            right_source = {
                f"{constants.SOURCE_PREFIX}{name}": f"{_MERGED_SOURCE_PREFIX}{name}"
                for name in sorted(merged)
            }
            if right_source:
                next_table = next_table.rename_columns(
                    [right_source.get(c, c) for c in next_table.column_names]
                )
            # The joined table is built from arrays, with no field flags; capture
            # the schemas before the join.
            next_ref_schema = next_table.schema
            # A shared tag key must have one Arrow type in both inputs (up to
            # layout); otherwise Polars would join and keep one side's type. And
            # it must be a type Polars can be given (``arrow_keys``).
            for key in sorted(common_tag_keys):
                key_type = require_shared_arrow_type(
                    "Join",
                    key,
                    arrow_utils.column_arrow_type(table.schema, key),
                    arrow_utils.column_arrow_type(next_table.schema, key),
                )
                require_matchable_key("Join", key, key_type)
            common_tag_keys.add(_COMMON_JOIN_KEY)

            table_ref_schema = table.schema
            table = _inner_join(table, next_table, list(common_tag_keys))
            table = arrow_utils.restore_schema_nullability(
                table, table_ref_schema, next_ref_schema
            )
            table = self._shared_key_nullability(
                table, table_ref_schema, next_ref_schema, common_tag_keys
            )
            for left_name, right_name in right_source.items():
                table = self._merge_source_columns(table, left_name, right_name)
            merged_source |= set(right_source)

            tag_keys.update(next_tag_keys)
            data_tags |= next_data_tags

        # reorder columns to bring tag columns to the front
        # TODO: come up with a better algorithm
        table = table.drop(_COMMON_JOIN_KEY)
        for name in sorted(merged_source):
            table = self._fold_source_column(table, name)

        # Sort system tag values for same-pipeline-hash streams to ensure commutativity
        table = arrow_utils.sort_system_tag_values(table)

        # Lay the columns out in the order ``output_schema`` defines (see
        # ``_joined_user_schemas``): tags, then data, then the hidden columns.
        # ``ArrowTableStream`` reads its schema from the physical order and its
        # keys from ``tag_columns``, so both must follow it. No hash or record
        # ID depends on column order.
        tag_order = list(tag_schema.keys())
        # A data-tag's column is laid out once, among the tags.
        data_order = [name for name in data_schema if name not in tag_schema]
        listed = {*tag_order, *data_order}
        result_table = table.select(
            tag_order
            + data_order
            + [col for col in table.column_names if col not in listed]
        )
        return ArrowTableStream(
            result_table,
            tag_columns=tuple(tag_order),
            data_tags=data_tag_names(tag_schema, data_schema),
        )

    @staticmethod
    def _merge_source_columns(
        table: "pa.Table", left_name: str, right_name: str
    ) -> "pa.Table":
        """Replace two source-info columns of one merged data column by their merge.

        Args:
            table: The joined table.
            left_name: The left side's source-info column; replaced in place, with its
                type and field kept (the merge keeps the shape of the data column).
            right_name: The right side's source-info column; removed.

        Returns:
            The table with ``left_name`` holding both sides' members, pending until
            the join folds them (``_fold_source_column``).
        """
        field = table.schema.field(left_name)
        merged = pa.array(
            [
                _merge_pending(left, right)
                for left, right in zip(
                    table.column(left_name).to_pylist(),
                    table.column(right_name).to_pylist(),
                )
            ],
            type=field.type,
        )
        index = table.schema.get_field_index(left_name)
        table = table.set_column(index, field, merged)
        return table.drop_columns([right_name])

    @staticmethod
    def _fold_source_column(table: "pa.Table", name: str) -> "pa.Table":
        """Fold the pending members of a merged source-info column into merged tokens.

        Args:
            table: The joined table.
            name: The source-info column.

        Returns:
            The table with one merged token at every token position of ``name``.
        """
        field = table.schema.field(name)
        folded = pa.array(
            [_fold_pending(value) for value in table.column(name).to_pylist()],
            type=field.type,
        )
        return table.set_column(table.schema.get_field_index(name), field, folded)

    @staticmethod
    def _shared_key_nullability(
        table: "pa.Table",
        left_schema: "pa.Schema",
        right_schema: "pa.Schema",
        keys: Collection[str],
    ) -> "pa.Table":
        """Flag each shared tag key nullable only when both inputs have it nullable.

        A null key matches nothing, so a key non-nullable in either input holds
        no null after the join (ITL-829). The same arrays are re-wrapped under the
        new field flags: no value or type changes. Equal flags keep the flag.

        Args:
            table: The joined table.
            left_schema: The left input table's schema.
            right_schema: The right input table's schema.
            keys: The join keys; names absent from either schema are skipped.

        Returns:
            The table with each shared key's nullable flag decided by ``key_nullable``.
        """
        schema = table.schema
        changed = False
        for name in keys:
            left_index = left_schema.get_field_index(name)
            right_index = right_schema.get_field_index(name)
            index = schema.get_field_index(name)
            if left_index < 0 or right_index < 0 or index < 0:
                continue
            nullable = key_nullable(
                left_schema.field(left_index).nullable,
                right_schema.field(right_index).nullable,
            )
            if schema.field(index).nullable != nullable:
                changed = True
                schema = schema.set(  # pyright: ignore[reportUnknownMemberType]  # stubs leave Field unparametrized
                    index, schema.field(index).with_nullable(nullable)
                )
        if not changed:
            return table
        return pa.Table.from_arrays(table.columns, schema=schema)

    # ------------------------------------------------------------------
    # Async execution
    # ------------------------------------------------------------------

    def _compute_system_tag_suffixes(
        self,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> list[str]:
        """Compute per-input system-tag suffixes from pipeline hashes.

        Each suffix is ``{truncated_hash}:{canonical_position}`` where
        canonical position is determined by sorting the hashes (matching
        the deterministic ordering used by ``static_process``).

        Args:
            input_pipeline_hashes: Pipeline hash per input, positionally
                matching the input channels.

        Returns:
            List of suffix strings, one per input position.
        """
        n_char = self.orcapod_config.hashing.system_tag_n_char
        hex_strings = [h.to_hex() for h in input_pipeline_hashes]

        # Canonical order: sorted by full hex (same as order_input_streams).
        # Use the original index as a tiebreaker so that inputs with
        # identical pipeline hashes still receive distinct positions
        # (matching Python's stable sort used by order_input_streams).
        ranked = sorted(range(len(hex_strings)), key=lambda i: hex_strings[i])
        canon_position = [0] * len(hex_strings)
        for canon_idx, orig_idx in enumerate(ranked):
            canon_position[orig_idx] = canon_idx

        suffixes: list[str] = []
        for orig_idx in range(len(hex_strings)):
            truncated = input_pipeline_hashes[orig_idx].to_hex(n_char)
            suffixes.append(f"{truncated}:{canon_position[orig_idx]}")
        return suffixes

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Async streaming join with pairwise iterative semantics.

        Single input: streams through directly without any buffering.

        Two inputs: binary symmetric hash join — each arriving row is
        probed against the opposite side's buffer, emitting matches as
        soon as found.

        Three or more inputs: staggered pairwise binary joins in
        canonical order — ``join(join(x, y), z)`` — matching
        ``static_process``'s iterative accumulation.  Each binary join
        uses the per-pair intersection of tag keys, so partially
        overlapping tag schemas are handled correctly.

        Args:
            inputs: Readable channels, one per upstream.
            output: Writable channel for downstream.
            input_pipeline_hashes: Pipeline hash for each input,
                positionally matching ``inputs``. With 2+ inputs, each input's
                system-tag columns are renamed after its hash, as on the sync
                path.

        Raises:
            ValueError: If ``input_pipeline_hashes`` does not have one hash per input.
        """
        try:
            output = WithoutMetaWriter(output)
            if len(input_pipeline_hashes) != len(inputs):
                raise ValueError(
                    f"input_pipeline_hashes length ({len(input_pipeline_hashes)}) "
                    f"must match inputs length ({len(inputs)})"
                )
            if len(inputs) == 1:
                async for tag, data in inputs[0]:
                    await output.send((tag, data))
                return

            suffixes = self._compute_system_tag_suffixes(input_pipeline_hashes)
            await self._streaming_join(inputs, output, suffixes)
        finally:
            await output.close()

    async def _streaming_join(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        suffixes: list[str],
    ) -> None:
        """Dispatch between binary join (N=2) and staggered chain (N>=3).

        Args:
            inputs: Readable channels, one per upstream.
            output: Output channel for matched rows.
            suffixes: Per-input system-tag suffixes (positional).
        """
        n = len(inputs)
        block_sep = constants.BLOCK_SEPARATOR

        if n == 2:
            # Join in canonical order so the merged tag and data keys follow
            # the order ``output_schema`` defines, whatever the operand order.
            left_idx, right_idx = self._canonical_input_order(suffixes)
            pair_suffixes = [suffixes[left_idx], suffixes[right_idx]]

            def merge_fn(
                lt: TagProtocol,
                lp: DataProtocol,
                rt: TagProtocol,
                rp: DataProtocol,
            ) -> tuple[TagProtocol, DataProtocol]:
                return self._merge_pair_rename(lt, lp, rt, rp, pair_suffixes, block_sep)

            await self._binary_streaming_join(
                inputs[left_idx], inputs[right_idx], output, merge_fn
            )
            return

        # N >= 3: staggered pairwise joins matching static_process
        await self._staggered_join(inputs, output, suffixes)

    @staticmethod
    def _canonical_input_order(suffixes: Sequence[str]) -> list[int]:
        """Return the input indices in canonical order.

        The canonical position is encoded in each suffix: ``"hash:position"``
        when pipeline hashes are provided, or plain ``"0"``, ``"1"``, ...
        (call order) otherwise.

        Args:
            suffixes: Per-input system-tag suffixes (positional).

        Returns:
            The input indices sorted by canonical position.
        """

        def _canon_pos(i: int) -> int:
            parts = suffixes[i].rsplit(":", 1)
            return int(parts[1]) if len(parts) == 2 else int(parts[0])

        return sorted(range(len(suffixes)), key=_canon_pos)

    async def _staggered_join(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        suffixes: list[str],
    ) -> None:
        """Staggered pairwise binary joins: ``join(join(x, y), z)``.

        Matches ``static_process``'s iterative pairwise join semantics.
        Each input's system tags are pre-renamed, then binary joins are
        chained in canonical order.  Per-pair join keys are computed
        naturally by each binary join (intersection of its two inputs'
        tag keys), so partially overlapping tag schemas produce the
        same results as the sync path.

        Intermediate results flow through channels, so downstream joins
        can start work as soon as earlier joins emit matches — the
        pipeline is fully streaming end-to-end.

        Args:
            inputs: Readable channels, one per upstream.
            output: Output channel for matched rows.
            suffixes: Per-input system-tag suffixes (positional).
        """
        from orcapod.channels import Channel

        block_sep = constants.BLOCK_SEPARATOR
        sys_prefix = constants.SYSTEM_TAG_PREFIX

        canon_order = self._canonical_input_order(suffixes)

        async with asyncio.TaskGroup() as tg:
            # Pre-rename system tags for each input so binary joins
            # can pass them through without modification
            renamed_readers: list[
                ReadableChannel[tuple[TagProtocol, DataProtocol]]
            ] = []
            for orig_idx in canon_order:
                ch: Channel[tuple[TagProtocol, DataProtocol]] = Channel(buffer_size=64)
                tg.create_task(
                    self._rename_sys_tags(
                        inputs[orig_idx],
                        ch.writer,
                        suffixes[orig_idx],
                        block_sep,
                        sys_prefix,
                    )
                )
                renamed_readers.append(ch.reader)

            # Chain: renamed[0] ⋈ renamed[1] → intermediate ⋈ renamed[2] → … → output
            current_reader = renamed_readers[0]
            for i in range(1, len(renamed_readers)):
                is_last = i == len(renamed_readers) - 1
                if is_last:
                    target_writer = output
                else:
                    intermediate: Channel[tuple[TagProtocol, DataProtocol]] = Channel(
                        buffer_size=64
                    )
                    target_writer = intermediate.writer

                tg.create_task(
                    self._binary_streaming_join(
                        current_reader,
                        renamed_readers[i],
                        target_writer,
                        functools.partial(self._merge_pair_passthrough, final=is_last),
                    )
                )

                if not is_last:
                    current_reader = intermediate.reader

    async def _binary_streaming_join(
        self,
        left: ReadableChannel[tuple[TagProtocol, DataProtocol]],
        right: ReadableChannel[tuple[TagProtocol, DataProtocol]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        merge_fn: Callable[
            [TagProtocol, DataProtocol, TagProtocol, DataProtocol],
            tuple[TagProtocol, DataProtocol],
        ],
    ) -> None:
        """Binary symmetric hash join.

        Both sides are read concurrently via a merged bounded queue.
        Each arriving row is indexed and immediately probed against the
        opposite side.  Matched rows are emitted via ``merge_fn`` as
        soon as found, so downstream can begin work before either input
        is fully consumed.

        Args:
            left: Left input channel.
            right: Right input channel.
            output: Output channel for matched rows.
            merge_fn: Callable(left_tag, left_pkt, right_tag, right_pkt)
                that produces the merged (Tag, Data) pair.
        """
        _SENTINEL = object()
        queue: asyncio.Queue = asyncio.Queue(maxsize=64)

        async def _drain(
            ch: ReadableChannel[tuple[TagProtocol, DataProtocol]],
            side: int,
        ) -> None:
            async for item in ch:
                await queue.put((side, item))
            await queue.put((side, _SENTINEL))

        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(_drain(left, 0))
                tg.create_task(_drain(right, 1))

                buffers: list[list[tuple[TagProtocol, DataProtocol]]] = [
                    [],
                    [],
                ]
                indexes: list[dict[tuple, list[int]]] = [{}, {}]

                shared_keys: tuple[str, ...] | None = None
                needs_reindex = False
                closed_count = 0

                while closed_count < 2:
                    side, item = await queue.get()

                    if item is _SENTINEL:
                        closed_count += 1
                        continue

                    tag, pkt = item
                    other = 1 - side

                    # Determine shared tag keys once we have rows from both sides
                    if shared_keys is None:
                        if not buffers[other]:
                            buffers[side].append((tag, pkt))
                            continue
                        this_keys = set(tag.keys())
                        other_keys = set(buffers[other][0][0].keys())
                        shared_keys = tuple(sorted(this_keys & other_keys))
                        # Check the key types once, on the first row of each side,
                        # so the error does not depend on whether any values match.
                        self._check_shared_key_types(
                            tag, buffers[other][0][0], shared_keys
                        )
                        # The same, for a name that is a tag on one side and a data
                        # column on the other (the sync path checks the schemas).
                        try:
                            require_tag_data_roles_agree(
                                tag.keys(),
                                pkt.keys(),
                                buffers[other][0][0].keys(),
                                buffers[other][0][1].keys(),
                            )
                        except InputValidationError as exc:
                            raise InputValidationError(f"Join: {exc}") from exc
                        needs_reindex = True

                    # One-time re-index of rows buffered before shared_keys
                    if needs_reindex:
                        needs_reindex = False
                        for buf_side in (0, 1):
                            for j, (bt, _bp) in enumerate(buffers[buf_side]):
                                k = self._match_key(bt, shared_keys)
                                if k is not None:
                                    indexes[buf_side].setdefault(k, []).append(j)

                    # Index the new row
                    key = self._match_key(tag, shared_keys)
                    row_idx = len(buffers[side])
                    buffers[side].append((tag, pkt))
                    if key is None:
                        # A null key never matches, as in the sync join.
                        continue
                    indexes[side].setdefault(key, []).append(row_idx)

                    # Probe the opposite side for matches
                    for mi in indexes[other].get(key, []):
                        ot, op = buffers[other][mi]
                        if side == 0:
                            await output.send(merge_fn(tag, pkt, ot, op))
                        else:
                            await output.send(merge_fn(ot, op, tag, pkt))
        finally:
            await output.close()

    @staticmethod
    def _match_key(tag: TagProtocol, keys: Sequence[str]) -> tuple[object, ...] | None:
        """Return a row's hashable key over the shared tag columns.

        Values come from the tag's Arrow columns and are frozen, so list and
        struct keys are hashable and compare as the sync path's Polars join
        compares them (see ``match_keys``).

        Args:
            tag: The row's tag.
            keys: The shared tag columns; empty means every row matches.

        Returns:
            The key, or ``None`` when a key value is a null primitive and the
            row therefore matches nothing.
        """
        if not keys:
            return (0,)
        table = tag.as_table()
        values: list[object] = []
        for key in keys:
            value = frozen_scalar(table.column(key)[0])
            if value is None:
                return None
            values.append(value)
        return tuple(values)

    @staticmethod
    def _check_shared_key_types(
        first: TagProtocol, second: TagProtocol, keys: Sequence[str]
    ) -> None:
        """Raise if a shared tag key has disagreeing Arrow types in two inputs.

        The async counterpart of the checks ``static_process`` runs on the input
        tables before its join, applied to the first row of each side: one Arrow
        type in both, and one the sync join can match on.

        Args:
            first: A tag from one input.
            second: A tag from the other input.
            keys: The tag columns the two inputs share.

        Raises:
            InputValidationError: If a key's large-normalized Arrow types differ
                (``require_shared_arrow_type``), or its type has no rule for
                matching (``require_matchable_key``).
        """
        first_schema, second_schema = first.arrow_schema(), second.arrow_schema()
        for key in keys:
            key_type = require_shared_arrow_type(
                "Join",
                key,
                arrow_utils.column_arrow_type(first_schema, key),
                arrow_utils.column_arrow_type(second_schema, key),
            )
            require_matchable_key("Join", key, key_type)

    @staticmethod
    async def _rename_sys_tags(
        ch_in: ReadableChannel[tuple[TagProtocol, DataProtocol]],
        ch_out: WritableChannel[tuple[TagProtocol, DataProtocol]],
        suffix: str,
        block_sep: str,
        sys_prefix: str,
    ) -> None:
        """Read rows and rename system-tag keys by appending the per-input suffix.

        Used as a pre-processing step in ``_staggered_join`` so that
        downstream binary joins can pass system tags through without
        modification. The tag's user columns keep their Arrow table.
        """
        try:
            async for tag, pkt in ch_in:
                sys_tags = tag.system_tags()
                if sys_tags:
                    renamed: dict[str, DataValue] = {}
                    for k, v in sys_tags.items():
                        new_key = (
                            f"{k}{block_sep}{suffix}" if k.startswith(sys_prefix) else k
                        )
                        renamed[new_key] = v
                    tag = Join._build_tag(
                        Join._merge_tag_tables(tag, None), renamed, tag.data_context_key
                    )
                await ch_out.send((tag, pkt))
        finally:
            await ch_out.close()

    @staticmethod
    def _merge_tag_tables(
        left: TagProtocol, right: TagProtocol | None
    ) -> "pa.Table | None":
        """Merge two tags' user columns as Arrow, in Join's column order.

        The left tag's columns come first, then the right tag's columns the
        left lacks. The async path joins in canonical order, so this is the
        order ``_joined_user_schemas`` defines. A shared (join-key) column
        takes the right tag's field, as ``static_process`` does
        (``restore_schema_nullability`` lets the last reference win), nullable
        only when both tags have it nullable (``key_nullable``), and the
        left tag's values, as its Polars join does (they are equal because the
        rows matched on it, except ``0.0`` against ``-0.0``).

        Every column keeps the Arrow type it already has: nothing is
        re-inferred from Python values.

        Args:
            left: The canonically earlier tag.
            right: The later tag, or ``None`` to take ``left``'s columns alone.

        Returns:
            The merged one-row table, or ``None`` when neither tag has user
            columns (a tagless join).

        Raises:
            InputValidationError: If a shared column's large-normalized Arrow
                types differ between the two tags
                (``require_shared_arrow_type``).
        """
        tables = [left.as_table()]
        if right is not None:
            tables.append(right.as_table())
        fields: dict[str, pa.Field[Any]] = {}  # pyright: ignore[reportExplicitAny]  # Field is invariant; Any admits every Arrow type
        types: dict[str, pa.DataType] = {}
        arrays: dict[str, Any] = {}  # pyright: ignore[reportExplicitAny]  # a pa.ChunkedArray of any type
        for table in tables:
            # A tag without user columns is a zero-column, zero-row table.
            for index, name in enumerate(table.column_names):
                arrow_type = table.schema.types[index]
                field = table.schema.field(index)
                if name in types:
                    _ = require_shared_arrow_type("Join", name, types[name], arrow_type)
                    # A shared key is nullable only when both tags have it
                    # nullable: a null key matches nothing (ITL-829).
                    field = field.with_nullable(
                        key_nullable(fields[name].nullable, field.nullable)
                    )
                types[name] = arrow_type
                fields[name] = field
                # A shared column keeps the left tag's values (the sync join's
                # Polars join does): rows that match on ``0.0`` and ``-0.0``
                # keep the left spelling.
                arrays.setdefault(name, table.column(index))
        if not fields:
            return None
        columns = [
            arrays[name].cast(field.type)
            if arrays[name].type != field.type
            else arrays[name]
            for name, field in fields.items()
        ]
        return pa.Table.from_arrays(columns, schema=pa.schema(list(fields.values())))

    @staticmethod
    def _build_tag(
        table: "pa.Table | None",
        system_tags: Mapping[str, DataValue],
        data_context_key: str,
    ) -> TagProtocol:
        """Build a ``Tag`` from a merged Arrow table and its system tags.

        Args:
            table: The tag's user columns, or ``None`` for a tag without any.
            system_tags: The tag's system tags, already renamed and sorted.
            data_context_key: The data context key of the incoming datagrams.

        Returns:
            The new tag.
        """
        from orcapod.core.datagrams import Tag

        if table is None:
            return Tag({}, system_tags=system_tags, data_context=data_context_key)
        return Tag(table, system_tags=system_tags, data_context=data_context_key)

    @staticmethod
    def _merge_data(
        left_tag: TagProtocol,
        left: DataProtocol,
        right_tag: TagProtocol,
        right: DataProtocol,
        final: bool = True,
    ) -> DataProtocol:
        """Merge two data datagrams as Arrow, left columns first.

        Every column keeps the Arrow type it already has, and the source info
        of both sides is carried over. Neither side's meta columns are, as in
        ``static_process`` (``meta_columns.without_meta_columns``). A data column
        that is the target of a data-tag on both sides, joined on, is kept
        once, with the source info of both inputs, merged as ``static_process``
        merges it.

        Args:
            left_tag: The left row's tag.
            left: The canonically earlier data (in a staggered join, the
                result of the joins so far).
            right_tag: The right row's tag.
            right: The later data.
            final: Whether this row is emitted by the join (the last step of a
                staggered chain, or a two-input join): then every merged column's
                pending members are folded into merged tokens.

        Returns:
            The merged data, in the context of ``left``.

        Raises:
            InputValidationError: If both sides have a data column of the same
                name that is not such a data-tag.
        """
        from orcapod.core.datagrams import Data

        left_table, right_table = left.as_table(), right.as_table()
        merged = Join._merged_data_tags(
            left_tag.keys(),
            data_tag_names(left_tag.keys(), left_table.column_names),
            right_tag.keys(),
            data_tag_names(right_tag.keys(), right_table.column_names),
        )
        overlap = set(left_table.column_names) & set(right_table.column_names)
        if overlap - merged:
            raise InputValidationError(
                f"Join: inputs share data columns {sorted(overlap - merged)}."
            )
        source_info = {**left.source_info(), **right.source_info()}
        if merged:
            left_info, right_info = left.source_info(), right.source_info()
            for name in merged:
                source_info[name] = _merge_pending(
                    left_info.get(name), right_info.get(name)
                )
        if final:
            source_info = {
                name: _fold_pending(value) for name, value in source_info.items()
            }
        # Column order as ``static_process`` lays it out: the data-tag columns
        # first, in the merged tag order, then each side's other data columns.
        tag_order = [*left_tag.keys()]
        tag_order += [name for name in right_tag.keys() if name not in tag_order]
        with_data_tags = [
            name
            for name in tag_order
            if name in left_table.column_names or name in right_table.column_names
        ]
        if not with_data_tags:
            return Data(
                arrow_utils.hstack_tables(left_table, right_table),
                source_info=source_info,
                data_context=left.data_context_key,
            )
        left_rest = left_table.drop_columns(
            [c for c in with_data_tags if c in left_table.column_names]
        )
        right_rest = right_table.drop_columns(
            [c for c in with_data_tags if c in right_table.column_names]
        )
        owners = [
            left_table if name in left_table.column_names else right_table
            for name in with_data_tags
        ]
        # A data-tag join key is one column: its data role is nullable only
        # when the key is nullable in both tags, as the merged tag is (ITL-829).
        left_tags, right_tags = left_tag.as_table().schema, right_tag.as_table().schema
        fields = [
            owner.schema.field(name).with_nullable(
                key_nullable(
                    left_tags.field(name).nullable, right_tags.field(name).nullable
                )
            )
            if name in left_tags.names and name in right_tags.names
            else owner.schema.field(name)
            for owner, name in zip(owners, with_data_tags)
        ]
        data_tags = pa.Table.from_arrays(
            [owner.column(name) for owner, name in zip(owners, with_data_tags)],
            schema=pa.schema(fields),
        )
        return Data(
            arrow_utils.hstack_tables(data_tags, left_rest, right_rest),
            source_info=source_info,
            data_context=left.data_context_key,
        )

    @staticmethod
    def _merge_pair_rename(
        left_tag: TagProtocol,
        left_pkt: DataProtocol,
        right_tag: TagProtocol,
        right_pkt: DataProtocol,
        suffixes: list[str],
        block_sep: str,
    ) -> tuple[TagProtocol, DataProtocol]:
        """Merge a matched pair, renaming system tags with per-side suffixes.

        Used for direct 2-input joins where system tags are renamed
        during the merge (not pre-renamed). The merged row is built from the
        inputs' Arrow tables, in the left (canonically first) tag's context.
        """
        sys_prefix = constants.SYSTEM_TAG_PREFIX

        # Rename and merge system tags
        merged_sys: dict[str, DataValue] = {}
        for i, tag in enumerate((left_tag, right_tag)):
            for k, v in tag.system_tags().items():
                new_key = (
                    f"{k}{block_sep}{suffixes[i]}" if k.startswith(sys_prefix) else k
                )
                merged_sys[new_key] = v

        merged_sys = Join._sort_merged_system_tags(merged_sys)
        merged_tag = Join._build_tag(
            Join._merge_tag_tables(left_tag, right_tag),
            merged_sys,
            left_tag.data_context_key,
        )
        return merged_tag, Join._merge_data(left_tag, left_pkt, right_tag, right_pkt)

    @staticmethod
    def _merge_pair_passthrough(
        left_tag: TagProtocol,
        left_pkt: DataProtocol,
        right_tag: TagProtocol,
        right_pkt: DataProtocol,
        final: bool = True,
    ) -> tuple[TagProtocol, DataProtocol]:
        """Merge a matched pair, passing system tags through without renaming.

        Used in the staggered chain where system tags have already been
        pre-renamed by ``_rename_sys_tags``. The merged row is built from the
        inputs' Arrow tables, in the left (canonically first) tag's context.
        ``final`` is true only for the chain's last step (see ``_merge_data``).
        """
        # Combine system tags (already renamed)
        merged_sys: dict[str, DataValue] = {}
        merged_sys.update(left_tag.system_tags())
        merged_sys.update(right_tag.system_tags())

        # Sort within same-provenance-path groups for commutativity
        merged_sys = Join._sort_merged_system_tags(merged_sys)
        merged_tag = Join._build_tag(
            Join._merge_tag_tables(left_tag, right_tag),
            merged_sys,
            left_tag.data_context_key,
        )
        return merged_tag, Join._merge_data(
            left_tag, left_pkt, right_tag, right_pkt, final=final
        )

    @staticmethod
    def _sort_merged_system_tags(merged_sys: dict) -> dict:
        """Sort system tag values within same-provenance-path groups.

        When two joined inputs share a pipeline_hash, their system tag
        columns share a provenance path but occupy different canonical
        positions.  Sorting the paired (source_id, record_id) values
        across positions ensures commutativity — mirroring what
        ``sort_system_tag_values`` does on Arrow tables in
        ``static_process``.
        """
        sys_prefix = constants.SYSTEM_TAG_PREFIX
        block_sep = constants.BLOCK_SEPARATOR
        field_sep = constants.FIELD_SEPARATOR

        # Parse keys → groups[provenance_path][position][field_type] = key
        groups: dict[str, dict[str, dict[str, str]]] = {}
        for key in merged_sys:
            if not key.startswith(sys_prefix):
                continue
            base, sep, position = key.rpartition(field_sep)
            if not sep or not position.isdigit():
                continue
            after_prefix = base[len(sys_prefix) :]
            field_type, bsep, prov_path = after_prefix.partition(block_sep)
            if not bsep:
                continue
            groups.setdefault(prov_path, {}).setdefault(position, {})[field_type] = key

        sid_field = constants.SYSTEM_TAG_SOURCE_ID_PREFIX[len(sys_prefix) :]
        rid_field = constants.SYSTEM_TAG_RECORD_ID_PREFIX[len(sys_prefix) :]

        for _prov_path, positions in groups.items():
            if len(positions) <= 1:
                continue

            sorted_pos_keys = sorted(positions.keys(), key=int)

            # Collect (sort_key, {field_type: value}) per position
            entries: list[tuple[tuple, dict[str, object]]] = []
            for pos in sorted_pos_keys:
                fmap = positions[pos]
                sid_val = merged_sys.get(fmap.get(sid_field, ""))
                rid_val = merged_sys.get(fmap.get(rid_field, ""))
                vals = {ft: merged_sys[k] for ft, k in fmap.items()}
                entries.append(((sid_val or "", rid_val or b""), vals))

            entries.sort(key=lambda e: e[0])

            # Write sorted values back to the original position keys
            for pos, (_, sorted_vals) in zip(sorted_pos_keys, entries):
                fmap = positions[pos]
                for field_type, key in fmap.items():
                    if field_type in sorted_vals:
                        merged_sys[key] = sorted_vals[field_type]

        return merged_sys

    def rows_dropped_text(
        self,
        position: int,
        others: Sequence[str],
        inputs: Sequence[StreamProtocol],
    ) -> str | None:
        """What the join drops, for ``explain()``: rows without a match in the others."""
        if not others:
            # A join of one input passes every row through.
            return None
        names = ", ".join(plain_repr(name) for name in others)
        return f"rows without a match in {names} are dropped"

    def identity_structure(self) -> Any:
        return self.__class__.__name__

    def __repr__(self) -> str:
        return "Join()"
