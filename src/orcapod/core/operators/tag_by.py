"""TagBy operator: give existing data columns the tag role, as data-tags (ITL-1175)."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, ClassVar

from typing_extensions import override

from orcapod.core.operators.base import UnaryOperator
from orcapod.core.data_tags import require_taggable_type, stream_data_tags
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import schema_utils
from orcapod.utils.schema_utils import column_names


def _tag_by_names(columns: str | Iterable[str]) -> tuple[str, ...]:
    """Return the names ``TagBy`` marks, in name order.

    The order the names are given in is not kept (ITL-1226): it is part of neither
    the identity nor the output.

    Args:
        columns: One name, or any iterable of names (a list, a tuple, a set).

    Returns:
        The names, sorted.

    Raises:
        TypeError: If a name is not a string.
        ValueError: If no name is given, or a name is repeated.
    """
    try:
        names = column_names(columns)
    except TypeError as exc:
        raise TypeError(f"TagBy: {exc}") from None
    if not names:
        raise ValueError("TagBy: expected at least one column name")
    if repeated := sorted({name for name in names if names.count(name) > 1}):
        raise ValueError(f"TagBy: columns {repeated} are named more than once")
    return tuple(sorted(names))


class TagBy(UnaryOperator):
    """Give existing data columns of a stream the tag role, as data-tags.

    Each named data column becomes a data-tag (ITL-1023): one column in
    both roles, a tag and the data column it already was. Nothing else changes:
    the data columns keep their names, order, types, values and source info,
    the system tags keep their names and values (rule 1, name-preserving), and
    no row is added, removed or reordered. The new tags follow the input's user
    tags, in name order whatever the order they are given in (ITL-1226), and the
    column is stored once, where it was.

    The operator creates no value and moves none: every output data column is the
    input data column of that name, and no tag becomes data. A data column that a
    function computed may become a tag (developer, November 16, 2026), and then
    key a join or a group.

    Args:
        columns: One data column name, or any iterable of them (a list, a tuple,
            a set). Their order is not kept: the new tags are in name order, and
            the identity is the sorted names.

    Raises:
        TypeError: If ``columns`` holds a name that is not a string.
        ValueError: If no name is given, or a name is repeated.
        InputValidationError: If a name is reserved: the meta prefix, a system
            column name, or one holding ``::``.
    """

    # Provisional: its identity is not frozen with data context v1 (ITL-1279).
    PROVISIONAL: ClassVar[bool] = True

    # Data-tags (ITL-1023): handled; this operator makes them.
    DATA_TAGS_ISSUE: ClassVar[str | None] = None

    def __init__(self, columns: str | Iterable[str], **kwargs: Any) -> None:
        names = _tag_by_names(columns)
        schema_utils.require_names_not_meta(names)
        schema_utils.require_names_not_system(names)
        schema_utils.require_names_without_separator(names)
        self.columns: tuple[str, ...] = names
        super().__init__(**kwargs)

    @override
    def identity_structure(self) -> Any:
        """Return the operator's identity: its class name and the sorted names.

        Returns:
            ``("TagBy", [names in name order])``.
        """
        return (self.__class__.__name__, list(self.columns))

    @override
    def to_config(self) -> dict[str, Any]:
        """Serialize this operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path`` and ``config`` keys, where
            ``config`` holds ``columns``, the names in name order.
        """
        config = super().to_config()
        config["config"] = {"columns": list(self.columns)}
        return config

    @override
    def __repr__(self) -> str:
        return f"TagBy({list(self.columns)!r})"

    @override
    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """Check that every name is a data column of ``stream`` that can be a tag.

        Args:
            stream: The input stream.

        Raises:
            InputValidationError: Naming the column, if it is already a tag
                (a data-tag or plain), is not a data column of ``stream``, or has a
                type that cannot be a tag (``data_tags.require_taggable_type``).
        """
        tag_columns, data_columns = stream.keys()
        data_tags = stream_data_tags(stream)
        _, data_schema = stream.output_schema()
        for name in self.columns:
            if name in data_tags:
                raise InputValidationError(
                    f"TagBy: column {name!r} is already a data-tag"
                )
            if name in tag_columns:
                raise InputValidationError(
                    f"TagBy: column {name!r} is a plain tag, not a data column: only "
                    + "a data column can be given the tag role"
                )
            user_data = [
                c for c in data_columns if not c.startswith(constants.META_PREFIX)
            ]
            if name not in user_data:
                raise InputValidationError(
                    f"TagBy: column {name!r} is not a data column of the input; its "
                    + f"data columns are {user_data}"
                )
            try:
                require_taggable_type(data_schema[name])
            except InputValidationError as exc:
                raise InputValidationError(
                    f"TagBy: column {name!r} cannot be a tag: {exc}"
                ) from exc

    @override
    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return the input schemas with the named columns added to the tags.

        The tag schema is the input's user tags, then the named columns with their
        data types, then the system tags (when ``columns`` includes them); the data
        schema is the input's.

        Args:
            stream: The input stream.
            columns: Column inclusion config passed through to ``output_schema``.
            all_info: Whether to include all info columns.

        Returns:
            A ``(tag_schema, data_schema)`` tuple.
        """
        tag_schema, data_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )
        user_tags = {
            name: python_type
            for name, python_type in tag_schema.items()
            if not name.startswith(constants.SYSTEM_TAG_PREFIX)
        }
        system_tags = {
            name: python_type
            for name, python_type in tag_schema.items()
            if name.startswith(constants.SYSTEM_TAG_PREFIX)
        }
        new_tags = {name: data_schema[name] for name in self.columns}
        return Schema({**user_tags, **new_tags, **system_tags}), data_schema

    @override
    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        """Re-wrap the input's rows with the named columns among the tags.

        The input's table as it is laid out: no column moved, no cast, no value
        changed. The stream lists its data-tags first among the data columns, in
        tag order (ITL-1205), so the named columns join them there, as predicted;
        the tag order is the ``tag_columns`` given. The stream keeps the input's data
        context, and each row its own data context key (the context column).

        Args:
            stream: The input stream.

        Returns:
            The stream with the named columns as data-tags.
        """
        tag_columns, _ = stream.keys()
        # The context column too, so each row keeps its own data context key.
        table = stream.as_table(
            columns={
                "source": True,
                "system_tags": True,
                "context": True,
                "sort_by_tags": False,
            }
        )
        return ArrowTableStream(
            table,
            tag_columns=[*tag_columns, *self.columns],
            data_tags=[*stream_data_tags(stream), *self.columns],
            data_context=stream.data_context_key,
        )
