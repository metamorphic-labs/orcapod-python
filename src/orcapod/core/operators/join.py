import asyncio
from collections.abc import Callable, Collection, Mapping, Sequence
from typing import Any, ClassVar, TYPE_CHECKING

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import (
    NonZeroInputOperator,
    require_shared_arrow_type,
)
from orcapod.core.referential_tags import (
    check_namespaces,
    merged_source_tokens,
    reference_names,
    stream_references,
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
from orcapod.types import ColumnConfig, ContentHash, DataValue, Schema
from orcapod.utils import arrow_utils, schema_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")


# Temporary name of the right input's source info of a merged referential column
# during the join (``static_process``); never in an output.
_MERGED_SOURCE_PREFIX = "__join_merged_source__"


class Join(NonZeroInputOperator):
    # Referential tags (ITL-1023): handled.
    REFERENTIAL_TAGS_ISSUE: ClassVar[str | None] = None

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
            return streams[0].output_schema(columns=columns, all_info=all_info)

        tag_schema, data_schema = self._joined_user_schemas(*streams)

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
        (``_without_meta``), including the ones a raw ``ArrowTableStream``
        lists as data columns in its ``output_schema``.

        Args:
            *streams: Two or more input streams.

        Referential tags (specification section 9): a shared tag ``x`` that is
        referential in either input is referential in the output. When it is
        referential in both, their data columns ``x`` are merged into one: the join
        equated the tags, so the values are equal (section 9.3). Every other data
        column shared by two inputs is still an error, and so is a plain tag of one
        input named like a data column of another (section 4.3).

        Returns:
            The ``(tag_schema, data_schema)`` of the joined user columns.

        Raises:
            InputValidationError: If two inputs share a data column that is not the
                target of a referential tag both of them join on, a merged column's
                types differ, or a plain tag would share a name with a data column.
        """
        ordered_streams = self.order_input_streams(*streams)
        stream = ordered_streams[0]
        tag_schema, data_schema = stream.output_schema()
        data_schema = self._without_meta_fields(data_schema)
        references = set(reference_names(tag_schema, data_schema))
        for other_stream in ordered_streams[1:]:
            other_tag_schema, other_data_schema = other_stream.output_schema()
            other_data_schema = self._without_meta_fields(other_data_schema)
            other_references = set(reference_names(other_tag_schema, other_data_schema))
            merged = self._merged_references(
                tag_schema, references, other_tag_schema, other_references
            )
            shared_data = set(data_schema) & set(other_data_schema)
            if shared_data - merged:
                raise InputValidationError(
                    f"Datas should not have overlapping keys, but {sorted(shared_data - merged)} found in {stream} and {other_stream}."
                )
            for name in sorted(merged):
                if data_schema[name] != other_data_schema[name]:
                    raise InputValidationError(
                        f"Join: referential tag {name!r} has type "
                        f"{data_schema[name]} in one input and "
                        f"{other_data_schema[name]} in another"
                    )
            tag_schema = schema_utils.union_schemas(tag_schema, other_tag_schema)
            data_schema = schema_utils.union_schemas(data_schema, other_data_schema)
            references |= other_references
            check_namespaces(tag_schema, data_schema, references)
        return tag_schema, data_schema

    @staticmethod
    def _merged_references(
        tags: Collection[str],
        references: Collection[str],
        other_tags: Collection[str],
        other_references: Collection[str],
    ) -> set[str]:
        """Return the data columns two join inputs merge into one (section 9.5).

        A data column is merged when it is the target of a referential tag in both
        inputs and that tag is a join key (a tag of both inputs). ``Join`` joins on
        every shared tag, so the second condition holds for every shared reference.

        Args:
            tags: The first input's tag names.
            references: The first input's referential tags.
            other_tags: The second input's tag names.
            other_references: The second input's referential tags.

        Returns:
            The names of the merged data columns.
        """
        keys = set(tags) & set(other_tags)
        return keys & set(references) & set(other_references)

    def _predict_system_tag_schema(self, *streams: StreamProtocol) -> Schema:
        """Predict the system tag columns that the join would produce.

        Each input stream's existing system tag columns get renamed by
        appending ::{pipeline_hash}:{canonical_position}. This method
        computes those output column names without performing the join.
        """
        n_char = self.orcapod_config.hashing.system_tag_n_char
        ordered_streams = self.order_input_streams(*streams)

        system_tag_fields: dict[str, type] = {}
        for idx, stream in enumerate(ordered_streams):
            stream_tag_schema, _ = stream.output_schema(columns={"system_tags": True})
            for col_name, col_type in stream_tag_schema.items():
                if col_name.startswith(constants.SYSTEM_TAG_PREFIX):
                    new_name = (
                        f"{col_name}{constants.BLOCK_SEPARATOR}"
                        f"{stream.pipeline_hash().to_hex(n_char)}:{idx}"
                    )
                    system_tag_fields[new_name] = col_type
        return Schema(system_tag_fields)

    def static_process(self, *streams: StreamProtocol) -> StreamProtocol:
        """Join the streams on their shared tag columns.

        The result holds every input's tag and data columns, source info and
        system tags (renamed per input). It holds none of the inputs' meta
        (``__``-prefixed) columns, which Join drops (``_without_meta``).

        Raises:
            InputValidationError: If two inputs share a data column, or a
                shared tag key has different Arrow types in two inputs.
        """
        if len(streams) == 1:
            return streams[0]

        # Canonically order streams by pipeline_hash for deterministic
        # system tag column names regardless of input order (Join is commutative)
        ordered_streams = self.order_input_streams(*streams)
        # The output column order, and the check that no two inputs share a
        # data column (meta columns are dropped, so they never collide).
        tag_schema, data_schema = self._joined_user_schemas(*streams)

        COMMON_JOIN_KEY = "_common"

        n_char = self.orcapod_config.hashing.system_tag_n_char

        stream = ordered_streams[0]

        tag_keys, _ = [set(k) for k in stream.keys()]
        references = set(stream_references(stream))
        table = self._without_meta(
            stream.as_table(columns={"source": True, "system_tags": True})
        )
        # trick to get cartesian product; use int8 explicitly so that zero-row
        # tables don't produce a null-typed column that Polars rejects on join
        table = table.add_column(
            0, COMMON_JOIN_KEY, pa.array([0] * len(table), type=pa.int8())
        )
        table = arrow_utils.append_to_system_tags(
            table,
            f"{stream.pipeline_hash().to_hex(n_char)}:0",
        )

        for idx, next_stream in enumerate(ordered_streams[1:], start=1):
            next_tag_keys, _ = next_stream.keys()
            next_table = self._without_meta(
                next_stream.as_table(columns={"source": True, "system_tags": True})
            )
            next_table = arrow_utils.append_to_system_tags(
                next_table,
                f"{next_stream.pipeline_hash().to_hex(n_char)}:{idx}",
            )
            # trick to ensure that there will always be at least one shared key
            # this ensure that no overlap in keys lead to full caretesian product
            next_table = next_table.add_column(
                0, COMMON_JOIN_KEY, pa.array([0] * len(next_table), type=pa.int8())
            )

            # Polars sets every field nullable; capture the schema before the join.
            next_ref_schema = next_table.schema

            common_tag_keys = tag_keys.intersection(next_tag_keys)
            next_references = set(stream_references(next_stream))
            merged = self._merged_references(
                tag_keys, references, next_tag_keys, next_references
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
            # A shared tag key must have one Arrow type in both inputs (up to
            # layout); otherwise Polars would join and keep one side's type.
            for key in sorted(common_tag_keys):
                _ = require_shared_arrow_type(
                    "Join",
                    key,
                    arrow_utils.column_arrow_type(table.schema, key),
                    arrow_utils.column_arrow_type(next_table.schema, key),
                )
            common_tag_keys.add(COMMON_JOIN_KEY)

            # Capture the left-side schema before the Polars join, which sets all
            # fields to nullable=True regardless of the original schema.
            table_ref_schema = table.schema
            table = (
                pl.DataFrame(table)
                .join(pl.DataFrame(next_table), on=list(common_tag_keys), how="inner")
                .to_arrow()
            )
            table = arrow_utils.restore_schema_nullability(
                table, table_ref_schema, next_ref_schema
            )
            for left_name, right_name in right_source.items():
                table = self._merge_source_columns(table, left_name, right_name)

            tag_keys.update(next_tag_keys)
            references |= next_references

        # reorder columns to bring tag columns to the front
        # TODO: come up with a better algorithm
        table = table.drop(COMMON_JOIN_KEY)

        # Sort system tag values for same-pipeline-hash streams to ensure commutativity
        table = arrow_utils.sort_system_tag_values(table)

        # Lay the columns out in the order ``output_schema`` defines (see
        # ``_joined_user_schemas``): tags, then data, then the hidden columns.
        # ``ArrowTableStream`` reads its schema from the physical order and its
        # keys from ``tag_columns``, so both must follow it. No hash or record
        # ID depends on column order.
        tag_order = list(tag_schema.keys())
        # A referential tag's column is laid out once, among the tags.
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
            reference_tags=reference_names(tag_schema, data_schema),
        )

    @staticmethod
    def _merge_source_columns(
        table: "pa.Table", left_name: str, right_name: str
    ) -> "pa.Table":
        """Replace two source-info columns of one merged data column by their merge.

        Args:
            table: The joined table.
            left_name: The left side's source-info column; replaced in place.
            right_name: The right side's source-info column; removed.

        Returns:
            The table with ``left_name`` holding both sides' tokens
            (``merged_source_tokens``) as ``large_list<large_string>``.
        """
        merged = pa.array(
            [
                merged_source_tokens(left, right)
                for left, right in zip(
                    table.column(left_name).to_pylist(),
                    table.column(right_name).to_pylist(),
                )
            ],
            type=pa.large_list(pa.large_string()),
        )
        index = table.schema.get_field_index(left_name)
        table = table.set_column(
            index, pa.field(left_name, merged.type, nullable=True), merged
        )
        return table.drop_columns([right_name])

    @staticmethod
    def _without_meta(table: "pa.Table") -> "pa.Table":
        """Drop the meta (``__``-prefixed) columns from an input table.

        Join keeps none of its inputs' meta columns. They describe how one
        input row was produced (a cached pod's ``__computed``, the result
        cache's ``__pf_*`` and ``__pod_ts``), nothing downstream of a Join
        reads them, and inputs routinely share their names. A raw
        ``ArrowTableStream`` returns its ``__`` columns from ``as_table``
        whatever the column configuration, together with a source-info
        column for each (``_source___x``), so both are dropped by name.

        Args:
            table: An input's table.

        Returns:
            The table without its meta columns and their source-info columns.
        """
        meta_source_prefix = f"{constants.SOURCE_PREFIX}{constants.META_PREFIX}"
        meta = [
            name
            for name in table.column_names
            if name.startswith((constants.META_PREFIX, meta_source_prefix))
        ]
        return table.drop_columns(meta) if meta else table

    @staticmethod
    def _without_meta_fields(schema: Schema) -> Schema:
        """Drop the meta (``__``-prefixed) fields from a data schema.

        The prediction counterpart of ``_without_meta``: a raw
        ``ArrowTableStream`` lists its ``__`` columns as data columns.

        Args:
            schema: An input's data schema.

        Returns:
            The schema without its meta fields.
        """
        if not any(name.startswith(constants.META_PREFIX) for name in schema):
            return schema
        return Schema(
            {
                name: data_type
                for name, data_type in schema.items()
                if not name.startswith(constants.META_PREFIX)
            }
        )

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
                        self._merge_pair_passthrough,
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
                        needs_reindex = True

                    # One-time re-index of rows buffered before shared_keys
                    if needs_reindex:
                        needs_reindex = False
                        for buf_side in (0, 1):
                            for j, (bt, _bp) in enumerate(buffers[buf_side]):
                                btd = bt.as_dict()
                                k = (
                                    tuple(btd[sk] for sk in shared_keys)
                                    if shared_keys
                                    else (0,)
                                )
                                indexes[buf_side].setdefault(k, []).append(j)

                    # Index the new row
                    td = tag.as_dict()
                    key = tuple(td[sk] for sk in shared_keys) if shared_keys else (0,)
                    row_idx = len(buffers[side])
                    buffers[side].append((tag, pkt))
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
    def _check_shared_key_types(
        first: TagProtocol, second: TagProtocol, keys: Sequence[str]
    ) -> None:
        """Raise if a shared tag key has disagreeing Arrow types in two inputs.

        The async counterpart of the check ``static_process`` runs on the input
        tables before its join, applied to the first row of each side.

        Args:
            first: A tag from one input.
            second: A tag from the other input.
            keys: The tag columns the two inputs share.

        Raises:
            InputValidationError: If a key's large-normalized Arrow types differ
                (``require_shared_arrow_type``).
        """
        first_schema, second_schema = first.arrow_schema(), second.arrow_schema()
        for key in keys:
            _ = require_shared_arrow_type(
                "Join",
                key,
                arrow_utils.column_arrow_type(first_schema, key),
                arrow_utils.column_arrow_type(second_schema, key),
            )

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
        (``restore_schema_nullability`` lets the last reference win); the
        values are equal because the rows matched on it.

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
                if name in types:
                    _ = require_shared_arrow_type("Join", name, types[name], arrow_type)
                types[name] = arrow_type
                fields[name] = table.schema.field(index)
                arrays[name] = table.column(index)
        if not fields:
            return None
        return pa.Table.from_arrays(
            list(arrays.values()), schema=pa.schema(list(fields.values()))
        )

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
    ) -> DataProtocol:
        """Merge two data datagrams as Arrow, left columns first.

        Every column keeps the Arrow type it already has, and the source info
        of both sides is carried over. Neither side's meta columns are, as in
        ``static_process`` (``_without_meta``). A data column that is the target
        of a referential tag on both sides, joined on, is kept once, with the
        source info of both (``merged_source_tokens``, as ``static_process``).

        Args:
            left_tag: The left row's tag.
            left: The canonically earlier data (in a staggered join, the
                result of the joins so far).
            right_tag: The right row's tag.
            right: The later data.

        Returns:
            The merged data, in the context of ``left``.

        Raises:
            InputValidationError: If both sides have a data column of the same
                name that is not such a reference.
        """
        from orcapod.core.datagrams import Data

        left_table, right_table = left.as_table(), right.as_table()
        merged = Join._merged_references(
            left_tag.keys(),
            reference_names(left_tag.keys(), left_table.column_names),
            right_tag.keys(),
            reference_names(right_tag.keys(), right_table.column_names),
        )
        overlap = set(left_table.column_names) & set(right_table.column_names)
        if overlap - merged:
            raise InputValidationError(
                f"Join: inputs share data columns {sorted(overlap - merged)}."
            )
        source_info = {**left.source_info(), **right.source_info()}
        if merged:
            right_table = right_table.drop_columns(sorted(merged))
            left_info, right_info = left.source_info(), right.source_info()
            for name in merged:
                source_info[name] = merged_source_tokens(
                    left_info.get(name), right_info.get(name)
                )
        return Data(
            arrow_utils.hstack_tables(left_table, right_table),
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
    ) -> tuple[TagProtocol, DataProtocol]:
        """Merge a matched pair, passing system tags through without renaming.

        Used in the staggered chain where system tags have already been
        pre-renamed by ``_rename_sys_tags``. The merged row is built from the
        inputs' Arrow tables, in the left (canonically first) tag's context.
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
        return merged_tag, Join._merge_data(left_tag, left_pkt, right_tag, right_pkt)

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

    def identity_structure(self) -> Any:
        return self.__class__.__name__

    def __repr__(self) -> str:
        return "Join()"
