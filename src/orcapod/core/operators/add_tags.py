"""AddTags operator: add tags with constant values to every row of a stream."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.datagrams import Tag
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.operators.column_selection import is_meta_column
from orcapod.core.operators.meta_columns import WithoutMetaWriter
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.types import ColumnConfig, ContentHash, Schema
from orcapod.utils import schema_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol
else:
    pa = LazyModule("pyarrow")

TagConstant = bool | int | float | str | bytes
"""A value ``AddTags`` accepts: its exact Python type is the tag's declared type."""

_ACCEPTED_TYPES: dict[str, type] = {
    "bool": bool,
    "int": int,
    "float": float,
    "str": str,
    "bytes": bytes,
}
"""The accepted value types by their saved-config token."""

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def _checked_value(name: str, value: object) -> TagConstant:
    """Return ``value`` if it can be the constant value of tag ``name``.

    The value's exact type must be ``bool``, ``int``, ``float``, ``str`` or
    ``bytes``: that type is the tag's declared type, mapped to Arrow by the data
    context's type converter (``int64``, ``double``, ``bool``, ``large_string``,
    ``large_binary``), so no type is inferred from data. A subclass of one of
    them (other than ``bool``, which is its own type) is refused, as are
    ``None`` (no type) and every other type.

    Args:
        name: The tag's name.
        value: The tag's value.

    Returns:
        The value.

    Raises:
        TypeError: When the value's type is not accepted.
        ValueError: When an ``int`` is outside the ``int64`` range, or a
            ``float`` is NaN or infinite.
    """
    if type(value) not in _ACCEPTED_TYPES.values():
        raise TypeError(
            f"add_tags: tag {name!r} has a value of type {type(value).__name__!r}; "
            + "a constant tag value must be a bool, int, float, str or bytes"
        )
    if type(value) is int and not _INT64_MIN <= value <= _INT64_MAX:
        raise ValueError(
            f"add_tags: tag {name!r} has the int value {value}, outside the int64 range"
        )
    if type(value) is float and not math.isfinite(value):
        raise ValueError(
            f"add_tags: tag {name!r} has the float value {value}; "
            + "a constant tag value must be finite"
        )
    return value  # pyright: ignore[reportReturnType]  # checked above


class AddTags(UnaryOperator):
    """Add tags with constant values to every row of a stream.

    Each new tag is a plain tag column holding the same value on every row. A
    tag is not a data value, so an operator may add one (developer, November 16,
    2026, ITL-1196); data columns, their values and their source info pass
    through unchanged, and no data column is added. System tags keep their names
    and values (name-preserving), so record ids are unchanged.

    The new tags follow the input's tags, in the order given. A value's exact
    Python type is the tag's type, mapped to Arrow by the data context's type
    converter: ``bool`` to ``bool``, ``int`` to ``int64``, ``float`` to
    ``double``, ``str`` to ``large_string``, ``bytes`` to ``large_binary``. Every
    new tag is non-nullable.

    The operator's identity is its tags, names and values in order, so two
    different values are two operators with two stored paths.

    Args:
        tags: Each new tag's name mapped to its value.

    Raises:
        ValueError: When no tag is given, a name is empty, or a value is out of
            range (see ``_checked_value``).
        TypeError: When a name is not a string or a value's type is not
            accepted.
        InputValidationError: When a name is reserved (meta or system prefix,
            ``::`` separator).
    """

    def __init__(self, tags: Mapping[str, TagConstant], **kwargs: Any):
        if not tags:
            raise ValueError("add_tags: no tag given")
        not_strings = [n for n in tags if not isinstance(n, str)]
        if not_strings:
            raise TypeError(f"add_tags: tag names must be strings, got {not_strings!r}")
        if "" in tags:
            raise ValueError("add_tags: a tag name must not be empty")
        schema_utils.require_names_not_meta(list(tags))
        schema_utils.require_names_not_system(list(tags))
        schema_utils.require_names_without_separator(list(tags))
        self.tags: dict[str, TagConstant] = {
            name: _checked_value(name, value) for name, value in tags.items()
        }
        # The one-row table of the values, per type converter (the async path
        # needs it for every row); the converter is kept so its id stays valid.
        self._one_row: dict[int, tuple[TypeConverterProtocol, pa.Table]] = {}
        super().__init__(**kwargs)

    def identity_structure(self) -> Any:
        return (self.__class__.__name__, tuple(self.tags.items()))

    def to_config(self) -> dict[str, Any]:
        """Serialize this AddTags operator to a config dict.

        Each tag is saved with its type token and its value; a ``bytes`` value
        as hex text.

        Returns:
            A dict with ``class_name``, ``module_path`` and ``config`` keys, where
            ``config`` holds ``tags``, a list of ``{"name", "type", "value"}``.
        """
        config = super().to_config()
        config["config"] = {
            "tags": [
                {
                    "name": name,
                    "type": type(value).__name__,
                    "value": value.hex() if isinstance(value, bytes) else value,
                }
                for name, value in self.tags.items()
            ]
        }
        return config

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> AddTags:
        """Reconstruct an AddTags operator from a config dict.

        Args:
            config: Dict as returned by ``to_config()``.

        Returns:
            A new ``AddTags`` instance.

        Raises:
            ValueError: When a saved tag names an unknown type.
        """
        tags: dict[str, TagConstant] = {}
        for entry in config.get("config", {}).get("tags", []):
            token = entry["type"]
            if token not in _ACCEPTED_TYPES:
                raise ValueError(f"AddTags: unknown saved tag type {token!r}")
            value = entry["value"]
            tags[entry["name"]] = (
                bytes.fromhex(value)
                if token == "bytes"
                else _ACCEPTED_TYPES[token](value)
            )
        return cls(tags)

    def _constants(self, converter: TypeConverterProtocol, num_rows: int) -> pa.Table:
        """Return the new tag columns, ``num_rows`` rows of the constant values.

        Built by the type converter from the values' Python types, so each column
        has the declared Arrow type and is non-nullable.

        Args:
            converter: The data context's type converter.
            num_rows: The number of rows.

        Returns:
            A table of the new tag columns, in the order given.
        """
        cached = self._one_row.get(id(converter))
        if cached is None or cached[0] is not converter:
            one_row = converter.python_dicts_to_arrow_table(
                [dict(self.tags)],
                python_schema={name: type(value) for name, value in self.tags.items()},
            )
            cached = (converter, one_row)
            self._one_row[id(converter)] = cached
        return cached[1].take(pa.array([0] * num_rows, type=pa.int64()))

    def _with_constants(
        self, table: pa.Table, tag_keys: Sequence[str], converter: TypeConverterProtocol
    ) -> pa.Table:
        """Insert the new tag columns into ``table`` after its user tags.

        The one rule of both paths: the sync path applies it to the stream's
        table, the async path to each row's tag table.

        Args:
            table: A table holding the user tags ``tag_keys`` (and possibly system
                tags, data and source info).
            tag_keys: The input's user tag columns.
            converter: The data context's type converter.

        Returns:
            ``table`` with the new tag columns after the last user tag (first when
            there is none); every other column unchanged.
        """
        names = table.column_names
        position = max((names.index(k) for k in tag_keys), default=-1) + 1
        constants = self._constants(converter, table.num_rows)
        for offset, (field, column) in enumerate(
            zip(constants.schema, constants.columns, strict=True)
        ):
            table = table.add_column(position + offset, field, column)
        return table

    def _require_new_names(
        self, tag_keys: Sequence[str], data_keys: Sequence[str]
    ) -> None:
        """Refuse a new tag whose name is already a column of the input.

        Args:
            tag_keys: The input's tag columns.
            data_keys: The input's data columns, without meta columns.

        Raises:
            InputValidationError: When a new tag's name is a tag or data column.
        """
        taken = [
            f"{name!r} is a tag column"
            if name in tag_keys
            else f"{name!r} is a data column"
            for name in self.tags
            if name in tag_keys or name in data_keys
        ]
        if taken:
            raise InputValidationError(
                f"add_tags: cannot add a tag with an existing column's name: {'; '.join(taken)}"
            )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        tag_keys, data_keys = stream.keys()
        self._require_new_names(
            tag_keys, [c for c in data_keys if not is_meta_column(c)]
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
        new_tags = {name: type(value) for name, value in self.tags.items()}
        return Schema({**tag_schema, **new_tags}), data_schema

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        """Return the stream's rows with the new tags added.

        Args:
            stream: The upstream stream.

        Returns:
            A stream over the same rows, in order, with the new tag columns after
            the input's tags. A result with no rows has the same schema.
        """
        tag_keys, _ = stream.keys()
        data_context = stream.data_context  # pyright: ignore[reportAttributeAccessIssue]  # StreamProtocol omits data_context, which every concrete stream provides (ITL-844)
        table = stream.as_table(
            columns={"source": True, "system_tags": True, "sort_by_tags": False}
        )
        return ArrowTableStream(
            self._with_constants(table, tag_keys, data_context.type_converter),
            tag_columns=[*tag_keys, *self.tags],
            data_context=data_context,
        )

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Streaming: add the new tags to each row as it arrives.

        Each row's tag table goes through the rule of the sync path
        (``_with_constants``); its data passes through unchanged.

        Args:
            inputs: Single-element sequence of readable channels.
            output: Channel to send the rows to.
            input_pipeline_hashes: Not used: this operator keeps system-tag
                names as they are, so no column is named after an input's
                pipeline hash.
        """
        try:
            output = WithoutMetaWriter(output)
            checked = False
            async for tag, data in inputs[0]:
                tag_keys = tag.keys()
                if not checked:
                    # The names are checked against the first row, as
                    # ``validate_unary_input`` checks them against the stream.
                    self._require_new_names(
                        tag_keys, [k for k in data.keys() if not is_meta_column(k)]
                    )
                    checked = True
                tag_table = self._with_constants(
                    tag.as_table(columns={"system_tags": True}),
                    tag_keys,
                    tag.converter,  # pyright: ignore[reportAttributeAccessIssue]  # every concrete Tag provides its converter
                )
                await output.send(
                    (Tag(tag_table, data_context=tag.data_context_key), data)
                )
        finally:
            await output.close()
