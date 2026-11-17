"""TagBy operator: give existing data columns the tag role, as referential tags (ITL-1175)."""

from __future__ import annotations

from collections.abc import Sequence, Set
from typing import Any, ClassVar

from typing_extensions import override

from orcapod.core.operators.base import UnaryOperator
from orcapod.core.referential_tags import require_taggable_type, stream_references
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import schema_utils


def _tag_by_names(columns: str | Sequence[str]) -> tuple[str, ...]:
    """Return the names ``TagBy`` marks, in the order given.

    Args:
        columns: One name, or a sequence of names.

    Returns:
        The names, in the order given.

    Raises:
        TypeError: If ``columns`` is a set (unordered: the order of the names is the
            order of the new tags) or holds a name that is not a string.
        ValueError: If no name is given, or a name is repeated.
    """
    if isinstance(columns, str):
        return (columns,)
    if isinstance(columns, Set):
        raise TypeError(
            "TagBy: give the column names as a list or a tuple, not a set: their "
            + "order is the order of the new tags"
        )
    names = tuple(columns)
    if not names:
        raise ValueError("TagBy: expected at least one column name")
    if not_strings := [name for name in names if not isinstance(name, str)]:
        raise TypeError(f"TagBy: column names must be strings, got {not_strings!r}")
    if repeated := sorted({name for name in names if names.count(name) > 1}):
        raise ValueError(f"TagBy: columns {repeated} are named more than once")
    return names


class TagBy(UnaryOperator):
    """Give existing data columns of a stream the tag role, as referential tags.

    Each named data column becomes a referential tag (ITL-1023): one column in
    both roles, a tag and the data column it already was. Nothing else changes:
    the data columns keep their names, order, types, values and source info,
    the system tags keep their names and values (rule 1, name-preserving), and
    no row is added, removed or reordered. The new tags follow the input's user
    tags, in the order given, and the column is stored once, where it was.

    The operator creates no value and moves none: every output data column is the
    input data column of that name, and no tag becomes data. A data column that a
    function computed may become a tag (developer, November 16, 2026), and then
    key a join or a group.

    Args:
        columns: One data column name, or a sequence of them. Their order is the
            order of the new tags, and part of the operator's identity.

    Raises:
        TypeError: If ``columns`` is a set or holds a name that is not a string.
        ValueError: If no name is given, or a name is repeated.
        InputValidationError: If a name is reserved: the meta prefix, a system
            column name, or one holding ``::``.
    """

    # Referential tags (ITL-1023): handled; this operator makes them.
    REFERENTIAL_TAGS_ISSUE: ClassVar[str | None] = None

    def __init__(self, columns: str | Sequence[str], **kwargs: Any) -> None:
        names = _tag_by_names(columns)
        schema_utils.require_names_not_meta(names)
        schema_utils.require_names_not_system(names)
        schema_utils.require_names_without_separator(names)
        self.columns: tuple[str, ...] = names
        super().__init__(**kwargs)

    @override
    def identity_structure(self) -> Any:
        """Return the operator's identity: its class name and the names, in order.

        Returns:
            ``("TagBy", [names in the order given])``.
        """
        return (self.__class__.__name__, list(self.columns))

    @override
    def to_config(self) -> dict[str, Any]:
        """Serialize this operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path`` and ``config`` keys, where
            ``config`` holds ``columns``, the names in order.
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
                (referential or plain), is not a data column of ``stream``, or has a
                type that cannot be a tag (``referential_tags.require_taggable_type``).
        """
        tag_columns, data_columns = stream.keys()
        references = stream_references(stream)
        _, data_schema = stream.output_schema()
        for name in self.columns:
            if name in references:
                raise InputValidationError(
                    f"TagBy: column {name!r} is already a referential tag"
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
        changed. The data columns are taken in table order, so leaving each named
        column where it is keeps the input's data order (the predicted one); the
        tag order is the ``tag_columns`` given. The rows keep the input's data
        context.

        Args:
            stream: The input stream.

        Returns:
            The stream with the named columns as referential tags.
        """
        tag_columns, _ = stream.keys()
        table = stream.as_table(
            columns={"source": True, "system_tags": True, "sort_by_tags": False}
        )
        return ArrowTableStream(
            table,
            tag_columns=[*tag_columns, *self.columns],
            reference_tags=[*stream_references(stream), *self.columns],
            data_context=stream.data_context_key,
        )
