from collections.abc import Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from orcapod.core.operators.meta_columns import WithoutMetaWriter
from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.operators.column_selection import (
    is_meta_column,
    require_a_data_column,
)
from orcapod.core.data_tags import stream_data_tags
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, ContentHash, Schema
from orcapod.utils import schema_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


def _with_other_group_note(
    error: InputValidationError,
    names: Collection[str],
    other_columns: Collection[str],
    group: str,
    method: str,
) -> InputValidationError:
    """Add to a refusal which of the names are columns of the other group.

    A reporting helper: the caller, which knows the other group's columns,
    annotates the error the mapper raised from its own group alone.

    Args:
        error: The refusal of names that are not columns of the renamed group.
        names: The names the mapper was given.
        other_columns: The input's columns of the other group.
        group: The other group, ``"tag"`` or ``"data"``.
        method: The stream method that renames a column of the other group.

    Returns:
        The error with one ``"; 'x' is a data column: use map_data"`` clause per
        name that is a column of the other group.
    """
    note = "".join(
        f"; {n!r} is a {group} column: use {method}"
        for n in names
        if n in other_columns
    )
    return InputValidationError(f"{error}{note}")


def _plan_group(
    columns: Collection[str], name_map: Mapping[str, str], drop_unmapped: bool
) -> tuple[dict[str, str], list[str]]:
    """Return the renames and the dropped columns of one group's user columns.

    The one rule every mapper's schema prediction and result follow (ITL-1363):
    only a user column (a tag, or a data column that is not meta) is renamed or,
    with ``drop_unmapped``, dropped. A column outside ``columns`` (a system tag,
    source info, the context or a meta column) is never renamed and never
    dropped, whatever ``name_map`` holds, so a prediction must not filter a whole
    schema by the names of ``name_map``.

    Args:
        columns: The input's user columns of the group, in order.
        name_map: The mapper's renames.
        drop_unmapped: Whether the user columns not in ``name_map`` are dropped.

    Returns:
        The renames of the user columns ``name_map`` names, and the user columns
        dropped (none without ``drop_unmapped``), in input order.
    """
    renames = {c: name_map[c] for c in columns if c in name_map}
    dropped = [c for c in columns if c not in name_map] if drop_unmapped else []
    return renames, dropped


class MapData(UnaryOperator):
    """Rename data columns.

    Renaming data is always a data-only move (developer, November 12, 2026,
    ITL-1023): renaming the data column of a data-tag ``x`` gives the new
    data column and leaves ``x`` a plain tag with the same values.

    Args:
        name_map: Each data column to rename, mapped to its new name.
        drop_unmapped: When ``True``, the data columns not in ``name_map`` are
            dropped.
        strict: When ``True`` (the default), a key of ``name_map`` that is not a
            data column of the input raises ``InputValidationError``; when
            ``False`` it is ignored. Part of the operator's identity, as for
            ``SelectTagColumns`` and the other column operators.
    """

    # Data-tags (ITL-1023): handled.
    DATA_TAGS_ISSUE: ClassVar[str | None] = None

    def __init__(
        self,
        name_map: Mapping[str, str],
        drop_unmapped: bool = False,
        strict: bool = True,
        **kwargs,
    ):
        # A data column never gets a meta column's name (ITL-1080): refused here,
        # so no path (sync, async or the prediction) can produce one.
        schema_utils.require_names_not_meta(name_map.values())
        schema_utils.require_names_not_system(name_map.values())
        schema_utils.require_names_without_separator(name_map.values())
        self.name_map = dict(name_map)
        self.drop_unmapped = drop_unmapped
        self.strict = strict
        super().__init__(**kwargs)

    def _require_data_columns(self, data_columns: Collection[str]) -> None:
        """Refuse, when ``strict``, a name that is not a data column (ITL-1160).

        Args:
            data_columns: The input's data columns, without meta columns.

        Raises:
            InputValidationError: When ``strict`` and a key of ``name_map`` is
                not one of ``data_columns``.
        """
        missing = [n for n in self.name_map if n not in data_columns]
        if missing and self.strict:
            raise InputValidationError(
                f"MapData: {missing} not among the data columns "
                f"{list(data_columns)} (use strict=False to ignore names that are "
                f"not data columns)"
            )

    def _check_names(
        self, tag_columns: Collection[str], data_columns: Collection[str]
    ) -> None:
        """Run ``_require_data_columns`` and name the tag columns among the names.

        Args:
            tag_columns: The input's tag columns.
            data_columns: The input's data columns, without meta columns.

        Raises:
            InputValidationError: As ``_require_data_columns``, annotated.
        """
        try:
            self._require_data_columns(data_columns)
        except InputValidationError as error:
            raise _with_other_group_note(
                error, self.name_map, tag_columns, "tag", "map_tags"
            ) from error

    def to_config(self) -> dict[str, Any]:
        """Serialize this MapData operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``name_map``, ``drop_unmapped`` and
            ``strict``.
        """
        config = super().to_config()
        config["config"] = {
            "name_map": dict(self.name_map),
            "drop_unmapped": self.drop_unmapped,
            "strict": self.strict,
        }
        return config

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        tag_columns, listed_columns = stream.keys()
        # A raw stream lists its meta (``__``) columns as data; they are not data,
        # are never renamed, and no operator carries them (ITL-1080).
        data_columns = tuple(
            c for c in listed_columns if not c.startswith(constants.META_PREFIX)
        )
        unmapped_columns = set(data_columns) - set(self.name_map.keys())

        if not any(n in data_columns for n in self.name_map):
            # nothing to rename in the data, return stream as is
            return stream

        table = stream.as_table(
            columns={
                "source": True,
                "system_tags": True,
                "context": True,
                "sort_by_tags": False,
            }
        )

        # A data-tag column keeps its tag under its own name, now plain; the
        # renamed data column is a copy beside it (a data-only move), added after
        # the renames so that no rename reaches the copy.
        data_tags = stream_data_tags(stream)
        moved = {c: self.name_map[c] for c in data_tags if c in self.name_map}

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
        for old, new in moved.items():
            index = renamed_table.schema.get_field_index(old)
            renamed_table = renamed_table.add_column(
                index + 1,
                renamed_table.schema.field(old).with_name(new),
                renamed_table.column(old),
            )

        if self.drop_unmapped and unmapped_columns:
            # An unmapped data-tag loses its data role and stays a plain tag.
            renamed_table = renamed_table.drop_columns(
                [c for c in unmapped_columns if c not in data_tags]
                + [
                    f"{constants.SOURCE_PREFIX}{c}"
                    for c in unmapped_columns
                    if c in data_tags
                ]
            )

        return ArrowTableStream(
            renamed_table,
            tag_columns=tag_columns,
            data_tags=[
                c
                for c in data_tags
                if c not in moved and not (self.drop_unmapped and c in unmapped_columns)
            ],
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        tag_columns, data_columns = stream.keys()
        self._check_names(
            tag_columns, [c for c in data_columns if not is_meta_column(c)]
        )
        # verify that renamed value does NOT collide with other columns
        relevant_source = []
        relevant_target = []
        for source, target in self.name_map.items():
            # A meta (``__``) column a raw stream lists as data is never renamed.
            if source in data_columns and not source.startswith(constants.META_PREFIX):
                relevant_source.append(source)
                relevant_target.append(target)
        if self.drop_unmapped:
            # Only the renamed columns remain, and a stream needs a data column.
            require_a_data_column(relevant_target)
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

        # Only the user data columns are renamed or dropped (``_plan_group``).
        renames, dropped = _plan_group(
            [c for c in stream.keys()[1] if not is_meta_column(c)],
            self.name_map,
            self.drop_unmapped,
        )
        new_data_schema = {
            renames.get(k, k): v
            for k, v in data_schema.items()
            if not k.startswith(constants.META_PREFIX) and k not in dropped
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
            output = WithoutMetaWriter(output)
            rename_map: dict[str, str] | None = None
            unmapped: list[str] | None = None
            async for tag, data in inputs[0]:
                if rename_map is None:
                    pkt_keys = data.keys()
                    # The names are checked against the first row, as
                    # ``validate_unary_input`` checks them against the stream.
                    self._check_names(
                        tag.keys(), [k for k in pkt_keys if not is_meta_column(k)]
                    )
                    rename_map = {
                        k: self.name_map[k] for k in pkt_keys if k in self.name_map
                    }
                    if self.drop_unmapped:
                        require_a_data_column(list(rename_map.values()))
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
            self.strict,
        )


class MapTags(UnaryOperator):
    """Rename tag columns.

    Data-tags (developer, November 12, 2026, ITL-1023): renaming a
    data-tag renames its one column in both roles, so the data-tag
    survives under the new name. With ``tag_only=True`` only the tag is renamed:
    the data column keeps its name and the renamed tag is a plain tag with the
    same values. A plain tag is renamed as before. A data-tag dropped as
    unmapped keeps its data column.

    Args:
        name_map: Each tag column to rename, mapped to its new name.
        drop_unmapped: When ``True``, the tag columns not in ``name_map`` are
            dropped.
        strict: When ``True`` (the default), a key of ``name_map`` that is not a
            tag column of the input raises ``InputValidationError``; when
            ``False`` it is ignored. Part of the operator's identity, as for
            ``SelectTagColumns`` and the other column operators.
        tag_only: Rename data-tags in the tag role only.
    """

    # Provisional: its identity is not frozen with data context v1 (ITL-1279).
    PROVISIONAL: ClassVar[bool] = True

    # Data-tags (ITL-1023): handled.
    DATA_TAGS_ISSUE: ClassVar[str | None] = None

    def __init__(
        self,
        name_map: Mapping[str, str],
        drop_unmapped: bool = False,
        strict: bool = True,
        tag_only: bool = False,
        **kwargs,
    ):
        # A tag never gets a meta column's name (ITL-1080): refused here, so no
        # path (sync, async or the prediction) can produce one.
        schema_utils.require_names_not_meta(name_map.values())
        schema_utils.require_names_not_system(name_map.values())
        schema_utils.require_names_without_separator(name_map.values())
        self.name_map = dict(name_map)
        self.drop_unmapped = drop_unmapped
        self.strict = strict
        self.tag_only = tag_only
        super().__init__(**kwargs)

    def _require_tag_columns(self, tag_columns: Collection[str]) -> None:
        """Refuse, when ``strict``, a name that is not a tag column (ITL-1160).

        Args:
            tag_columns: The input's tag columns.

        Raises:
            InputValidationError: When ``strict`` and a key of ``name_map`` is
                not one of ``tag_columns``.
        """
        missing = [n for n in self.name_map if n not in tag_columns]
        if missing and self.strict:
            raise InputValidationError(
                f"MapTags: {missing} not among the tag columns {list(tag_columns)} "
                f"(use strict=False to ignore names that are not tag columns)"
            )

    def _check_names(
        self, tag_columns: Collection[str], data_columns: Collection[str]
    ) -> None:
        """Run ``_require_tag_columns`` and name the data columns among the names.

        Args:
            tag_columns: The input's tag columns.
            data_columns: The input's data columns, without meta columns.

        Raises:
            InputValidationError: As ``_require_tag_columns``, annotated.
        """
        try:
            self._require_tag_columns(tag_columns)
        except InputValidationError as error:
            raise _with_other_group_note(
                error, self.name_map, data_columns, "data", "map_data"
            ) from error

    def to_config(self) -> dict[str, Any]:
        """Serialize this MapTags operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``name_map``, ``drop_unmapped`` and
            ``strict``.
        """
        config = super().to_config()
        config["config"] = {
            "name_map": dict(self.name_map),
            "drop_unmapped": self.drop_unmapped,
            "strict": self.strict,
        }
        if self.tag_only:
            config["config"]["tag_only"] = True
        return config

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        tag_columns, data_columns = stream.keys()
        missing_tags = set(tag_columns) - set(self.name_map.keys())
        data_tags = stream_data_tags(stream)

        if not any(n in tag_columns for n in self.name_map) and not (
            self.drop_unmapped and missing_tags
        ):
            # nothing to rename or drop in the tags, return stream as is
            return stream

        table = stream.as_table(
            columns={
                "source": True,
                "system_tags": True,
                "context": True,
                "sort_by_tags": False,
            }
        )

        name_map = {
            tc: self.name_map.get(tc, tc)
            for tc in tag_columns
            if tc in self.name_map or not self.drop_unmapped
        }  # rename the tag as necessary
        new_tag_columns = list(name_map.values())
        renamed_refs = [c for c in data_tags if c in self.name_map]
        if self.tag_only:
            # The renamed tag is a plain copy beside the data column, which keeps
            # its name and its source info. The copy is added after the renames, so
            # that no rename of another column reaches it.
            for old in renamed_refs:
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
        if self.tag_only:
            for old in renamed_refs:
                index = renamed_table.schema.get_field_index(old)
                renamed_table = renamed_table.add_column(
                    index + 1,
                    renamed_table.schema.field(old).with_name(self.name_map[old]),
                    renamed_table.column(old),
                )

        if missing_tags and self.drop_unmapped:
            # drop any tags that are not in the name map; a data-tag keeps
            # its column as data.
            renamed_table = renamed_table.drop_columns(
                [c for c in missing_tags if c not in data_tags]
            )

        if self.tag_only:
            new_data_tags = [c for c in data_tags if c not in self.name_map]
        else:
            new_data_tags = [self.name_map.get(c, c) for c in data_tags]
        if self.drop_unmapped:
            new_data_tags = [c for c in new_data_tags if c in new_tag_columns]
        return ArrowTableStream(
            renamed_table,
            tag_columns=new_tag_columns,
            data_tags=new_data_tags,
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        tag_columns, data_columns = stream.keys()
        self._check_names(
            tag_columns, [c for c in data_columns if not is_meta_column(c)]
        )
        # verify that renamed value does NOT collide with other columns
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

        # Only the user tags are renamed or dropped: the system tags the
        # table keeps are kept here too (``_plan_group``, ITL-1363).
        renames, dropped = _plan_group(
            stream.keys()[0], self.name_map, self.drop_unmapped
        )
        new_tag_schema = {
            renames.get(k, k): v for k, v in tag_schema.items() if k not in dropped
        }
        if not self.tag_only:
            # A data-tag's data column is renamed with it (one column).
            data_tags = set(stream_data_tags(stream))
            data_schema = Schema(
                {
                    renames.get(k, k) if k in data_tags else k: v
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
            output = WithoutMetaWriter(output)
            rename_map: dict[str, str] | None = None
            unmapped: list[str] | None = None
            async for tag, data in inputs[0]:
                if rename_map is None:
                    tag_keys = tag.keys()
                    # The names are checked against the first row, as
                    # ``validate_unary_input`` checks them against the stream.
                    self._check_names(
                        tag_keys, [k for k in data.keys() if not is_meta_column(k)]
                    )
                    rename_map = {
                        k: self.name_map[k] for k in tag_keys if k in self.name_map
                    }
                    if self.drop_unmapped:
                        unmapped = [k for k in tag_keys if k not in self.name_map]
                if not rename_map and not unmapped:
                    await output.send((tag, data))
                else:
                    # With ``drop_unmapped`` the unmapped tags go even when no
                    # tag is renamed, as on the sync path and in the prediction.
                    new_tag = tag.rename(rename_map) if rename_map else tag
                    if unmapped:
                        new_tag = new_tag.drop(*unmapped)
                    # A data-tag's data column is renamed with it, unless
                    # only the tag is renamed (ITL-1023).
                    data_keys = data.keys()
                    data_map = {k: v for k, v in rename_map.items() if k in data_keys}
                    if data_map and not self.tag_only:
                        data = data.rename(data_map)
                    await output.send((new_tag, data))
        finally:
            await output.close()

    def identity_structure(self) -> Any:
        self._note_provisional_identity_use()  # a direct read is a use (ITL-1279)
        # ``tag_only`` enters only when set, so a plain renaming keeps its identity.
        return (
            self.__class__.__name__,
            self.name_map,
            self.drop_unmapped,
            self.strict,
        ) + ((("tag_only", True),) if self.tag_only else ())


class MapColumns(UnaryOperator):
    """Rename columns, whether each is a tag or a data column (ITL-1160).

    The counterpart of ``SelectColumns`` and ``DropColumns`` for renaming: one
    step renames tag and data columns alike. A renamed tag stays a tag and a
    renamed data column stays data; a renamed data column's source-info column
    is renamed with it. System tags pass through, the stream's column order is
    kept, and, as for ``SelectColumns``, no data meta (``__``) column of the
    input is carried.

    Args:
        name_map: Each column to rename, tag or data, mapped to its new name.
        drop_unmapped: When ``True``, every user tag and data column not in
            ``name_map`` is dropped (with the source-info column of a dropped
            data column); system tags are kept.
        strict: When ``True`` (the default), a key of ``name_map`` that is
            neither a tag nor a data column of the input raises
            ``InputValidationError``; when ``False`` it is ignored. Part of the
            operator's identity, as for ``SelectColumns``.

    Raises:
        InputValidationError: When called on a stream where a new name is taken
            by a column that is not renamed, two columns get the same new name,
            or (with ``drop_unmapped``) no data column would remain.
    """

    # Provisional: its identity is not frozen with data context v1 (ITL-1279).
    PROVISIONAL: ClassVar[bool] = True

    def __init__(
        self,
        name_map: Mapping[str, str],
        drop_unmapped: bool = False,
        strict: bool = True,
        **kwargs,
    ):
        # No column gets a meta column's name (ITL-1080): refused here, so no
        # path (sync, async or the prediction) can produce one.
        schema_utils.require_names_not_meta(name_map.values())
        schema_utils.require_names_not_system(name_map.values())
        schema_utils.require_names_without_separator(name_map.values())
        self.name_map = dict(name_map)
        self.drop_unmapped = drop_unmapped
        self.strict = strict
        super().__init__(**kwargs)

    def to_config(self) -> dict[str, Any]:
        """Serialize this MapColumns operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``name_map``, ``drop_unmapped`` and
            ``strict``.
        """
        config = super().to_config()
        config["config"] = {
            "name_map": dict(self.name_map),
            "drop_unmapped": self.drop_unmapped,
            "strict": self.strict,
        }
        return config

    def identity_structure(self) -> Any:
        self._note_provisional_identity_use()  # a direct read is a use (ITL-1279)
        return (
            self.__class__.__name__,
            self.name_map,
            self.drop_unmapped,
            self.strict,
        )

    @staticmethod
    def _user_columns(stream: StreamProtocol) -> tuple[tuple[str, ...], list[str]]:
        """The stream's tag columns and its data columns without meta columns.

        A raw ``ArrowTableStream`` lists its meta (``__``) columns as data columns.
        """
        tag_columns, data_columns = stream.keys()
        return tag_columns, [c for c in data_columns if not is_meta_column(c)]

    def _plan(
        self, tag_columns: Collection[str], data_columns: Collection[str]
    ) -> tuple[dict[str, str], dict[str, str], list[str], list[str]]:
        """Return the tag renames, data renames, dropped tags and dropped data.

        Args:
            tag_columns: The input's tag columns, in order.
            data_columns: The input's user data columns, in order (no meta column).
        """
        tag_renames, tags_dropped = _plan_group(
            tag_columns, self.name_map, self.drop_unmapped
        )
        data_renames, data_dropped = _plan_group(
            data_columns, self.name_map, self.drop_unmapped
        )
        return tag_renames, data_renames, tags_dropped, data_dropped

    def _validate_columns(
        self, tag_columns: Collection[str], data_columns: Collection[str]
    ) -> None:
        """Check the names against the input's columns and the result's columns.

        Args:
            tag_columns: The input's tag columns.
            data_columns: The input's user data columns (no meta column).

        Raises:
            InputValidationError: When ``strict`` and a name is neither a tag nor
                a data column; when a new name is taken by a column that is not
                renamed, or given to two columns; or when the result would have
                no data column.
        """
        missing = [
            n for n in self.name_map if n not in tag_columns and n not in data_columns
        ]
        if missing and self.strict:
            raise InputValidationError(
                f"MapColumns: {missing} not among the tag columns "
                f"{list(tag_columns)} or the data columns {list(data_columns)}; "
                f"use strict=False to ignore names that are not columns"
            )
        tag_renames, data_renames, _, data_dropped = self._plan(
            tag_columns, data_columns
        )
        renamed = {**tag_renames, **data_renames}
        targets = list(renamed.values())
        not_renamed = [c for c in (*tag_columns, *data_columns) if c not in renamed]
        taken = sorted(set(targets).intersection(not_renamed))
        doubled = sorted({t for t in targets if targets.count(t) > 1})
        if taken or doubled:
            message = f"MapColumns: renaming {renamed} would cause collisions:"
            if taken:
                message += f" columns that are not renamed already have {taken}."
            if doubled:
                message += f" two columns would both be named {doubled}."
            raise InputValidationError(message)
        try:
            require_a_data_column(
                [data_renames.get(c, c) for c in data_columns if c not in data_dropped]
            )
        except InputValidationError as error:
            raise InputValidationError(
                f"MapColumns({self.name_map}, drop_unmapped=True) would leave no "
                f"data column: {error}"
            ) from error

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """Validate the names against the input stream.

        Raises:
            InputValidationError: As ``_validate_columns`` describes.
        """
        self._validate_columns(*self._user_columns(stream))

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        tag_columns, data_columns = self._user_columns(stream)
        tag_renames, data_renames, tags_dropped, data_dropped = self._plan(
            tag_columns, data_columns
        )
        table = stream.as_table(
            columns={
                "source": True,
                "system_tags": True,
                "context": True,
                "sort_by_tags": False,
            }
        )
        dropped = {
            *tags_dropped,
            *data_dropped,
            *(f"{constants.SOURCE_PREFIX}{c}" for c in data_dropped),
        }
        table = table.drop_columns(
            [
                c
                for c in table.column_names
                # Only data meta goes; a tag column is never stripped as meta.
                if c in dropped or (is_meta_column(c) and c not in tag_columns)
            ]
        )
        renames = {
            **tag_renames,
            **data_renames,
            **{
                f"{constants.SOURCE_PREFIX}{c}": f"{constants.SOURCE_PREFIX}{t}"
                for c, t in data_renames.items()
            },
        }
        return ArrowTableStream(
            table.rename_columns([renames.get(c, c) for c in table.column_names]),
            tag_columns=[
                tag_renames.get(c, c) for c in tag_columns if c not in tags_dropped
            ],
            data_context=stream.data_context_key,
        )

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
        tag_renames, data_renames, tags_dropped, data_dropped = self._plan(
            *self._user_columns(stream)
        )
        # System tag columns are never renamed or dropped, so they are kept.
        return (
            Schema(
                {
                    tag_renames.get(k, k): v
                    for k, v in tag_schema.items()
                    if k not in tags_dropped
                }
            ),
            Schema(
                {
                    data_renames.get(k, k): v
                    for k, v in data_schema.items()
                    if k not in data_dropped and not is_meta_column(k)
                }
            ),
        )

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Streaming: rename (and drop) the columns per row without materializing.

        The names are validated against the first row, as ``validate_unary_input``
        validates them against the stream.

        Args:
            inputs: The one readable input channel.
            output: Writable channel for downstream consumption.
            input_pipeline_hashes: Not used: this operator keeps system-tag
                names as they are, so no column is named after an input's
                pipeline hash.
        """
        try:
            output = WithoutMetaWriter(output)
            plan: tuple[dict[str, str], dict[str, str], list[str], list[str]] | None
            plan = None
            async for tag, data in inputs[0]:
                if plan is None:
                    tag_keys = tag.keys()
                    data_keys = [k for k in data.keys() if not is_meta_column(k)]
                    self._validate_columns(tag_keys, data_keys)
                    plan = self._plan(tag_keys, data_keys)
                tag_renames, data_renames, tags_dropped, data_dropped = plan
                if tags_dropped:
                    tag = tag.drop(*tags_dropped)
                if data_dropped:
                    data = data.drop(*data_dropped)
                if tag_renames:
                    tag = tag.rename(tag_renames)
                if data_renames:
                    data = data.rename(data_renames)
                await output.send((tag, data))
        finally:
            await output.close()
