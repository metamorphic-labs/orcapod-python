from __future__ import annotations

from collections.abc import Generator
from orcapod.core.sources.base import held_table_batches
from orcapod.core.streams.preview import PreviewRequest
from orcapod.protocols.core_protocols import StreamProtocol

from collections.abc import Callable, Collection
from typing import TYPE_CHECKING, Any, ClassVar

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import (
    SourceStreamBuilder,
    normalize_record_id_columns,
)
from orcapod.protocols.core_protocols import TagProtocol
from orcapod.types import SchemaLike
from orcapod.utils import arrow_utils
from orcapod.utils.tensor_conversions import warn_source_tensor_conversions

if TYPE_CHECKING:
    pass


class ListSource(RootSource):
    """A source backed by a Python list.

    Each element in the list becomes one (tag, data) pair. The element is
    stored as the data under ``name``; the tag is either the element's index
    (default) or the dict returned by ``tag_function(element, index)``.

    Its identity is that of every root source (``RootSource.identity_structure``:
    ``source_id``, output schema, key columns, no class name; developer, November 21,
    2026, ITL-1272; ITL-1425). The elements and the tags the tag function gave enter it only
    through the default source id, the hash of the built table, when no
    ``source_id`` is given, and through the output schema (their types); with a
    ``source_id``, the source is an address: other elements of the same types leave its
    content hash unchanged.
    """

    @staticmethod
    def _default_tag(element: Any, idx: int) -> dict[str, Any]:
        return {"element_index": idx}

    # The config type key; also the kind in the kind part of the source id.
    SOURCE_KIND: ClassVar[str] = "list"
    # The version of this kind's implementation, in the kind part (ITL-1425).
    SOURCE_VERSION: ClassVar[int] = 1
    TABLE_IS_ROWS: ClassVar[bool] = True

    def __init__(
        self,
        name: str,
        data: list[Any],
        tag_function: Callable[[Any, int], dict[str, Any] | TagProtocol] | None = None,
        expected_tag_keys: Collection[str] | None = None,
        data_schema: SchemaLike | None = None,
        source_id: str | None = None,
        record_id_columns: str | Collection[str] | None = None,
        *,
        hash_in_key_order: bool = False,
        **kwargs: Any,
    ) -> None:
        """Build the source from ``data``.

        Args:
            name: The data column each element is stored under.
            data: The elements, one row each.
            tag_function: Gives each row's tags from ``(element, index)``; by
                default ``{"element_index": index}``.
            expected_tag_keys: The tag column names, when they cannot be read from
                the first row (or there is no row).
            data_schema: The Python schema of the rows, when it must not be
                inferred from the values.
            source_id: The source's name; its id is ``list_v1:<name>``
                (ITL-1425). By default the name is ``<label>:<hex>`` of the built
                table's hash, so the id is ``list_v1:arrow_v1:<hex>``.
            record_id_columns: The key columns that name a row, in key order;
                ``None`` or ``()`` for the position.
            hash_in_key_order: Hash the table for the default source id with its
                rows sorted by the key columns (ITL-1272); needs key columns and no
                ``source_id``.
            **kwargs: Passed on to ``RootSource``.
        """
        super().__init__(source_id=source_id, **kwargs)

        self.name = name
        self._elements = list(data)

        if tag_function is None:
            tag_function = self.__class__._default_tag
            if expected_tag_keys is None:
                expected_tag_keys = ["element_index"]

        self._tag_function = tag_function
        self._expected_tag_keys = (
            tuple(expected_tag_keys) if expected_tag_keys is not None else None
        )

        # Build rows: each row is tag_fields merged with {name: element}.
        rows = []
        for idx, element in enumerate(self._elements):
            tag_fields = tag_function(element, idx)
            if hasattr(tag_fields, "as_dict"):
                tag_fields = tag_fields.as_dict()
            row = dict(tag_fields)
            row[name] = element
            rows.append(row)

        tag_columns = (
            list(self._expected_tag_keys)
            if self._expected_tag_keys is not None
            else [k for k in (rows[0].keys() if rows else []) if k != name]
        )

        arrow_table = self.data_context.type_converter.python_dicts_to_arrow_table(
            rows, python_schema=data_schema
        )
        if data_schema is None:
            # Canonical pattern for Python-origin sources: the type converter
            # defaults all fields to nullable=True. When no explicit schema is
            # given, infer the correct nullable flags from actual data so the
            # builder receives a reliable schema.
            arrow_table = arrow_table.cast(
                arrow_utils.infer_schema_nullable(arrow_table)
            )

        self._key_columns = normalize_record_id_columns(record_id_columns) or ()
        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            arrow_table,
            tag_columns=tag_columns,
            source_name=self._source_name,
            kind_part=self.kind_part(),
            record_id_columns=self._key_columns,
            hash_in_key_order=hash_in_key_order,
        )

        self._stream = result.stream
        if self._source_name is None:
            self._source_name = result.source_name
        warn_source_tensor_conversions(self.source_id, rows)

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize metadata-only config (data is not serializable)."""
        return {
            "source_type": self.SOURCE_KIND,
            "name": self.name,
            **self._source_id_config(),
            **self._identity_config(),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry=None) -> "ListSource":
        """Not supported — ListSource data cannot be reconstructed from config.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "ListSource cannot be reconstructed from config — "
            "original list data is not serializable."
        )

    def _preview_batches(
        self, request: PreviewRequest
    ) -> Generator[StreamProtocol, None, None]:
        """Slices of the table it holds, for exactly this class (``held_table_batches``)."""
        yield from held_table_batches(self, ListSource, request)
