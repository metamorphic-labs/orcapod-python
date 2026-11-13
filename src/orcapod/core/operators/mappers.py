from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.referential_tags import stream_references
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, ContentHash, Schema
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class MapData(UnaryOperator):
    """
    Operator that maps data in a stream using a user-defined function.
    The function is applied to each data in the stream, and the resulting data
    are returned as a new stream.

    Renaming data is always a data-only move (developer, November 12, 2026,
    ITL-1023): renaming the data column of a referential tag ``x`` gives the new
    data column and leaves ``x`` a plain tag with the same values.
    """

    # Referential tags (ITL-1023): handled.
    REFERENTIAL_TAGS_ISSUE: ClassVar[str | None] = None

    def __init__(
        self, name_map: Mapping[str, str], drop_unmapped: bool = False, **kwargs
    ):
        self.name_map = dict(name_map)
        self.drop_unmapped = drop_unmapped
        super().__init__(**kwargs)

    def to_config(self) -> dict[str, Any]:
        """Serialize this MapData operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``name_map`` and ``drop_unmapped``.
        """
        config = super().to_config()
        config["config"] = {
            "name_map": dict(self.name_map),
            "drop_unmapped": self.drop_unmapped,
        }
        return config

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        tag_columns, data_columns = stream.keys()
        unmapped_columns = set(data_columns) - set(self.name_map.keys())

        if not any(n in data_columns for n in self.name_map):
            # nothing to rename in the data, return stream as is
            return stream

        table = stream.as_table(
            columns={"source": True, "system_tags": True, "sort_by_tags": False}
        )

        # A referenced column keeps its tag under its own name, now plain; the
        # renamed data column is a copy beside it (a data-only move).
        references = stream_references(stream)
        moved = {c: self.name_map[c] for c in references if c in self.name_map}
        for old, new in moved.items():
            index = table.schema.get_field_index(old)
            table = table.add_column(
                index + 1, table.schema.field(old).with_name(new), table.column(old)
            )

        name_map = {
            tc: tc
            for tc in table.column_names
            if tc not in data_columns and not tc.startswith(constants.SOURCE_PREFIX)
        }  # no renaming on tag columns
        for c in data_columns:
            if c in self.name_map:
                name_map[c] = c if c in moved else self.name_map[c]
                name_map[f"{constants.SOURCE_PREFIX}{c}"] = (
                    f"{constants.SOURCE_PREFIX}{self.name_map[c]}"
                )
            else:
                name_map[c] = c

        renamed_table = table.rename_columns(name_map)

        if self.drop_unmapped and unmapped_columns:
            # An unmapped referential tag loses its data role and stays a plain tag.
            renamed_table = renamed_table.drop_columns(
                [c for c in unmapped_columns if c not in references]
                + [
                    f"{constants.SOURCE_PREFIX}{c}"
                    for c in unmapped_columns
                    if c in references
                ]
            )

        return ArrowTableStream(
            renamed_table,
            tag_columns=tag_columns,
            reference_tags=[
                c
                for c in references
                if c not in moved and not (self.drop_unmapped and c in unmapped_columns)
            ],
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        # verify that renamed value does NOT collide with other columns
        tag_columns, data_columns = stream.keys()
        relevant_source = []
        relevant_target = []
        for source, target in self.name_map.items():
            if source in data_columns:
                relevant_source.append(source)
                relevant_target.append(target)
        remaining_data_columns = set(data_columns) - set(relevant_source)
        overlapping_data_columns = remaining_data_columns.intersection(relevant_target)
        overlapping_tag_columns = set(tag_columns).intersection(relevant_target)

        if overlapping_data_columns or overlapping_tag_columns:
            message = f"Renaming {self.name_map} would cause collisions with existing columns: "
            if overlapping_data_columns:
                message += f"overlapping data columns: {overlapping_data_columns}, "
            if overlapping_tag_columns:
                message += f"overlapping tag columns: {overlapping_tag_columns}."
            raise InputValidationError(message)

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        tag_schema, data_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )

        # Create new data schema with renamed keys
        new_data_schema = {
            self.name_map.get(k, k): v
            for k, v in data_schema.items()
            if k in self.name_map or not self.drop_unmapped
        }

        return tag_schema, Schema(new_data_schema)

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Streaming: rename data columns per row without materializing.

        Args:
            inputs: The one readable input channel.
            output: Writable channel for downstream consumption.
            input_pipeline_hashes: Not used: this operator keeps system-tag
                names as they are, so no column is named after an input's
                pipeline hash.
        """
        try:
            rename_map: dict[str, str] | None = None
            unmapped: list[str] | None = None
            async for tag, data in inputs[0]:
                if rename_map is None:
                    pkt_keys = data.keys()
                    rename_map = {
                        k: self.name_map[k] for k in pkt_keys if k in self.name_map
                    }
                    if self.drop_unmapped:
                        unmapped = [k for k in pkt_keys if k not in self.name_map]
                if not rename_map:
                    await output.send((tag, data))
                else:
                    new_pkt = data.rename(rename_map)
                    if unmapped:
                        new_pkt = new_pkt.drop(*unmapped)
                    await output.send((tag, new_pkt))
        finally:
            await output.close()

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.name_map,
            self.drop_unmapped,
        )


class MapTags(UnaryOperator):
    """
    Operator that maps tags in a stream using a user-defined function.
    The function is applied to each tag in the stream, and the resulting tags
    are returned as a new stream.

    Referential tags (developer, November 12, 2026, ITL-1023): renaming a
    referential tag renames its one column in both roles, so the reference
    survives under the new name. With ``tag_only=True`` only the tag is renamed:
    the data column keeps its name and the renamed tag is a plain tag with the
    same values. A plain tag is renamed as before. A referential tag dropped as
    unmapped keeps its data column.

    Args:
        name_map: Old tag name to new tag name.
        drop_unmapped: Drop the tags ``name_map`` does not name.
        tag_only: Rename referential tags in the tag role only.
    """

    # Referential tags (ITL-1023): handled.
    REFERENTIAL_TAGS_ISSUE: ClassVar[str | None] = None

    def __init__(
        self,
        name_map: Mapping[str, str],
        drop_unmapped: bool = False,
        tag_only: bool = False,
        **kwargs,
    ):
        self.name_map = dict(name_map)
        self.drop_unmapped = drop_unmapped
        self.tag_only = tag_only
        super().__init__(**kwargs)

    def to_config(self) -> dict[str, Any]:
        """Serialize this MapTags operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``name_map`` and ``drop_unmapped``.
        """
        config = super().to_config()
        config["config"] = {
            "name_map": dict(self.name_map),
            "drop_unmapped": self.drop_unmapped,
        }
        if self.tag_only:
            config["config"]["tag_only"] = True
        return config

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        tag_columns, data_columns = stream.keys()
        missing_tags = set(tag_columns) - set(self.name_map.keys())
        references = stream_references(stream)

        if not any(n in tag_columns for n in self.name_map):
            # nothing to rename in the tags, return stream as is
            return stream

        table = stream.as_table(
            columns={"source": True, "system_tags": True, "sort_by_tags": False}
        )

        name_map = {
            tc: self.name_map.get(tc, tc)
            for tc in tag_columns
            if tc in self.name_map or not self.drop_unmapped
        }  # rename the tag as necessary
        new_tag_columns = list(name_map.values())
        renamed_refs = [c for c in references if c in self.name_map]
        if self.tag_only:
            # The renamed tag is a plain copy beside the data column, which keeps
            # its name and its source info.
            for old in renamed_refs:
                index = table.schema.get_field_index(old)
                new = self.name_map[old]
                table = table.add_column(
                    index + 1, table.schema.field(old).with_name(new), table.column(old)
                )
                name_map[new] = new
                name_map[old] = old
        else:
            # One column in both roles: its data source info follows the name.
            for old in renamed_refs:
                name_map[f"{constants.SOURCE_PREFIX}{old}"] = (
                    f"{constants.SOURCE_PREFIX}{self.name_map[old]}"
                )
        for c in data_columns:
            name_map.setdefault(c, c)  # no renaming on other data columns

        renamed_table = table.rename_columns(name_map)

        if missing_tags and self.drop_unmapped:
            # drop any tags that are not in the name map; a referential one keeps
            # its column as data.
            renamed_table = renamed_table.drop_columns(
                [c for c in missing_tags if c not in references]
            )

        if self.tag_only:
            new_references = [c for c in references if c not in self.name_map]
        else:
            new_references = [self.name_map.get(c, c) for c in references]
        if self.drop_unmapped:
            new_references = [c for c in new_references if c in new_tag_columns]
        return ArrowTableStream(
            renamed_table,
            tag_columns=new_tag_columns,
            reference_tags=new_references,
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # verify that renamed value does NOT collide with other columns
        tag_columns, data_columns = stream.keys()
        relevant_source = []
        relevant_target = []
        for source, target in self.name_map.items():
            if source in tag_columns:
                relevant_source.append(source)
                relevant_target.append(target)
        remaining_tag_columns = set(tag_columns) - set(relevant_source)
        overlapping_tag_columns = remaining_tag_columns.intersection(relevant_target)
        overlapping_data_columns = set(data_columns).intersection(relevant_target)

        if overlapping_tag_columns or overlapping_data_columns:
            message = f"Renaming {self.name_map} would cause collisions with existing columns: "
            if overlapping_tag_columns:
                message += f"overlapping tag columns: {overlapping_tag_columns}."
            if overlapping_data_columns:
                message += f"overlapping data columns: {overlapping_data_columns}."
            raise InputValidationError(message)

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        tag_schema, data_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )

        new_tag_schema = {
            self.name_map.get(k, k): v
            for k, v in tag_schema.items()
            if k in self.name_map or not self.drop_unmapped
        }
        if not self.tag_only:
            # A referential tag's data column is renamed with it (one column).
            references = set(stream_references(stream))
            data_schema = Schema(
                {
                    self.name_map.get(k, k) if k in references else k: v
                    for k, v in data_schema.items()
                }
            )

        return Schema(new_tag_schema), data_schema

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Streaming: rename tag columns per row without materializing.

        Args:
            inputs: The one readable input channel.
            output: Writable channel for downstream consumption.
            input_pipeline_hashes: Not used: this operator keeps system-tag
                names as they are, so no column is named after an input's
                pipeline hash.
        """
        try:
            rename_map: dict[str, str] | None = None
            unmapped: list[str] | None = None
            async for tag, data in inputs[0]:
                if rename_map is None:
                    tag_keys = tag.keys()
                    rename_map = {
                        k: self.name_map[k] for k in tag_keys if k in self.name_map
                    }
                    if self.drop_unmapped:
                        unmapped = [k for k in tag_keys if k not in self.name_map]
                if not rename_map:
                    await output.send((tag, data))
                else:
                    new_tag = tag.rename(rename_map)
                    if unmapped:
                        new_tag = new_tag.drop(*unmapped)
                    # A referential tag's data column is renamed with it, unless
                    # only the tag is renamed (ITL-1023).
                    data_keys = data.keys()
                    data_map = {k: v for k, v in rename_map.items() if k in data_keys}
                    if data_map and not self.tag_only:
                        data = data.rename(data_map)
                    await output.send((new_tag, data))
        finally:
            await output.close()

    def identity_structure(self) -> Any:
        # ``tag_only`` enters only when set, so a plain renaming keeps its identity.
        return (
            self.__class__.__name__,
            self.name_map,
            self.drop_unmapped,
        ) + ((("tag_only", True),) if self.tag_only else ())
