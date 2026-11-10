"""Tests for the list-type and type-restoration helpers in ``arrow_utils`` (ITL-832).

An aggregating operator must build a list column from the Arrow type its input
column already has, never from the type Arrow infers for the Python values.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_type_converter
from orcapod.utils.arrow_utils import (
    aggregated_list_type,
    list_array,
    restore_column_types,
)

PRIMITIVE_ELEMENTS: list[tuple[pa.DataType, list[object]]] = [
    (pa.int8(), [1, 2]),
    (pa.float32(), [1.5, 2.5]),
    (pa.large_string(), ["a", "b"]),
    (pa.large_binary(), [b"a", b"b"]),
    (pa.list_(pa.int16()), [[1], [2, 3]]),
]


class TestAggregatedListType:
    """``aggregated_list_type`` wraps the element type it is given."""

    @pytest.mark.parametrize(
        "element_type", [t for t, _ in PRIMITIVE_ELEMENTS], ids=str
    )
    def test_primitive_element_is_wrapped_unchanged(
        self, element_type: pa.DataType
    ) -> None:
        """A plain Arrow element type becomes ``list<element>``, keeping its width."""
        converter = get_default_type_converter()
        assert aggregated_list_type(element_type, converter) == pa.list_(element_type)

    def test_extension_element_uses_list_logical_type(self) -> None:
        """An extension element gets the converter's ``list[...]`` extension type.

        Arrow cannot embed an extension type in a list value field, so the outer
        list must itself be the ``ListLogicalType`` extension.
        """
        converter = get_default_type_converter()
        element_type = converter.python_type_to_arrow_type(Path)
        list_type = aggregated_list_type(element_type, converter)
        assert list_type == converter.python_type_to_arrow_type(list[Path])
        assert isinstance(list_type, pa.ExtensionType)


class TestListArray:
    """``list_array`` builds an array of exactly the requested list type."""

    @pytest.mark.parametrize(
        ("element_type", "values"),
        PRIMITIVE_ELEMENTS,
        ids=[str(t) for t, _ in PRIMITIVE_ELEMENTS],
    )
    @pytest.mark.parametrize("rows", [0, 1])
    def test_type_is_independent_of_values(
        self, element_type: pa.DataType, values: list[object], rows: int
    ) -> None:
        """Empty and non-empty arrays carry the same, non-inferred type."""
        list_type = pa.list_(element_type)
        array = list_array([values] * rows, list_type)
        assert array.type == list_type
        assert len(array) == rows
        assert array.to_pylist() == [values] * rows

    @pytest.mark.parametrize("rows", [0, 1])
    def test_extension_list_type_wraps_storage(self, rows: int) -> None:
        """An extension list type is built over its storage and then wrapped."""
        converter = get_default_type_converter()
        list_type = converter.python_type_to_arrow_type(list[Path])
        assert isinstance(list_type, pa.ExtensionType)
        storage_values = [["/a", "/b"]] * rows
        array = list_array(storage_values, list_type)
        assert array.type == list_type
        assert len(array) == rows


class TestRestoreColumnTypes:
    """``restore_column_types`` undoes a layout change without touching nullability."""

    def test_restores_types_and_keeps_nullability(self) -> None:
        """Listed columns get their reference type back; flags, nulls and others stay."""
        table = pa.Table.from_arrays(
            [
                pa.array(["a", "b"], pa.large_string()),
                pa.array(["a", None], pa.large_string()),
                pa.array([[1], None], pa.large_list(pa.int8())),
                pa.array([1, 2], pa.int64()),
            ],
            schema=pa.schema(
                [
                    pa.field("s", pa.large_string(), nullable=False),
                    pa.field("n", pa.large_string(), nullable=True),
                    pa.field("l", pa.large_list(pa.int8()), nullable=True),
                    pa.field("other", pa.int64(), nullable=True),
                ]
            ),
        )
        restored = restore_column_types(
            table,
            {
                "s": pa.string(),
                "n": pa.string(),
                "l": pa.list_(pa.int8()),
                "absent": pa.int8(),
            },
        )
        assert restored.schema == pa.schema(
            [
                pa.field("s", pa.string(), nullable=False),
                pa.field("n", pa.string(), nullable=True),
                pa.field("l", pa.list_(pa.int8()), nullable=True),
                pa.field("other", pa.int64(), nullable=True),
            ]
        )
        assert restored.column("n").null_count == 1
        assert restored.column("l").null_count == 1
        assert restored.to_pylist() == table.to_pylist()
