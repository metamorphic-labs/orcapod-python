"""Float32 conversion warnings and their scope (ITL-924)."""

from __future__ import annotations

import warnings
from dataclasses import dataclass, replace
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.core.datagrams import Data, Datagram, Tag
from orcapod.errors import ValueConversionWarning
from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol
from orcapod.sources import DictSource, ListSource
from orcapod.types import DataValue, SchemaLike


class _CopyingConverter:
    def __init__(self, delegate: TypeConverterProtocol) -> None:
        self.delegate: TypeConverterProtocol = delegate

    def __getattr__(self, name: str) -> Any:
        return getattr(self.delegate, name)

    def python_dicts_to_arrow_table(
        self,
        python_dicts: list[dict[str, Any]],
        python_schema: SchemaLike | None = None,
        arrow_schema: pa.Schema | None = None,
    ) -> pa.Table:
        return self.delegate.python_dicts_to_arrow_table(
            [row.copy() for row in python_dicts], python_schema, arrow_schema
        )


def test_warns_for_python_float_and_stores_arrow_bits() -> None:
    values = [0.1, 1e-50, 1e300, 0.5, float("nan"), float("inf"), -0.0]
    with pytest.warns(ValueConversionWarning, match=r"numpy\.float32") as caught:
        source = ListSource(
            "x", values, data_schema={"element_index": int, "x": np.float32}
        )

    assert len(caught) == 1
    assert (
        source.as_table()["x"].combine_chunks().buffers()[1]
        == pa.array(values, type=pa.float32()).buffers()[1]
    )


@pytest.mark.parametrize("materialize", [False, True], ids=["lazy", "materialized"])
def test_list_source_warns_for_float64_tag(materialize: bool) -> None:
    tag = Tag({"key": 1, "tag": 0.1}, python_schema={"key": int, "tag": float})
    if materialize:
        assert tag.as_table()["tag"].type == pa.float64()

    with pytest.warns(ValueConversionWarning) as caught:
        source = ListSource(
            "x",
            [3],
            tag_function=lambda *_: tag,
            data_schema={"key": int, "tag": np.float32, "x": int},
        )

    assert len(caught) == 1
    assert source.as_table()["tag"].type == pa.float32()
    assert source.as_table()["tag"].to_pylist() == [float(np.float32(0.1))]


def test_rewrapped_float32_tag_warns_with_or_without_copying_rows() -> None:
    default_context = get_default_context()
    copying_context = replace(
        default_context,
        type_converter=_CopyingConverter(default_context.type_converter),
    )
    sources: list[ListSource] = []
    for context in (default_context, copying_context):
        table = pa.table({"tag": pa.array([0.1], type=pa.float32()), "key": [1]})
        tag = Tag(table, data_context=context)
        with pytest.warns(ValueConversionWarning) as caught:
            source = ListSource(
                "x",
                [3],
                tag_function=lambda *_: tag,
                data_schema={"tag": np.float32, "key": int, "x": int},
                data_context=context,
                record_id_columns=["key"],
            )
        assert len(caught) == 1
        assert source.as_table()["tag"].equals(table["tag"])
        sources.append(source)

    direct, copied = sources
    assert copied.as_table().equals(direct.as_table())
    assert copied.source_id == direct.source_id
    assert copied.content_hash() == direct.content_hash()


@dataclass
class _Reading:
    value: np.float32


def test_nested_only_float32_uses_arrow_conversion_without_a_warning() -> None:
    reading = cast(
        DataValue, cast(object, _Reading(cast(np.float32, cast(object, 0.1))))
    )
    values = [0.1] * 1024
    with warnings.catch_warnings():
        warnings.simplefilter("error", ValueConversionWarning)
        table = Datagram(
            {"items": values, "mapping": {"key": 0.1}, "reading": reading},
            python_schema={
                "items": list[np.float32],
                "mapping": dict[str, np.float32],
                "reading": _Reading,
            },
        ).as_table()

    narrowed = float(np.float32(0.1))
    assert table["items"].to_pylist() == [[narrowed] * len(values)]
    assert table["mapping"].to_pylist() == [[{"key": "key", "value": narrowed}]]
    assert table["reading"].to_pylist() == [{"value": narrowed}]


def test_numpy_float32_and_arrow_schema_stay_quiet() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", ValueConversionWarning)
        declared = ListSource(
            "x", [np.float32(0.1)], data_schema={"x": np.float32, "element_index": int}
        ).as_table()
        arrow_only = DictSource(
            [{"id": 1, "x": 0.1}],
            tag_columns=["id"],
            data_schema=pa.schema([("id", pa.int64()), ("x", pa.float32())]),
        ).as_table()
    assert arrow_only["x"].equals(declared["x"])


@pytest.mark.parametrize("row_type", [Datagram, Tag, Data])
def test_datagram_ingestion_warns_when_construction_or_update_supplies_a_float(
    row_type: type[Datagram],
) -> None:
    with pytest.warns(ValueConversionWarning):
        row = row_type({"x": 0.1}, python_schema={"x": np.float32})
    with pytest.warns(ValueConversionWarning):
        updated = row.update(x=0.2)
    with pytest.warns(ValueConversionWarning):
        expanded = updated.with_columns({"y": np.float32}, y=0.3)
    with warnings.catch_warnings():
        warnings.simplefilter("error", ValueConversionWarning)
        table = expanded.as_table()
    assert table["x"].to_pylist() == [float(np.float32(0.2))]
    assert table["y"].to_pylist() == [float(np.float32(0.3))]


@pytest.mark.parametrize("row_type", [Tag, Data])
def test_stored_float32_row_as_datagram_stays_quiet(row_type: type[Tag | Data]) -> None:
    table = pa.table({"x": pa.array([0.1], type=pa.float32())})
    with warnings.catch_warnings():
        warnings.simplefilter("error", ValueConversionWarning)
        rebuilt = row_type(table).as_datagram().as_table()
    assert rebuilt["x"].to_pylist() == table["x"].to_pylist()


def test_direct_warning_obeys_standard_filters() -> None:
    def convert() -> None:
        _ = DictSource(
            [{"id": 1, "x": 0.5}],
            tag_columns=["id"],
            data_schema={"id": int, "x": np.float32},
        )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("ignore", ValueConversionWarning)
        convert()
        assert not caught
        warnings.simplefilter("default", ValueConversionWarning)
        convert()
        convert()
        assert len(caught) == 1
        warnings.simplefilter("always", ValueConversionWarning)
        convert()
        convert()
        assert len(caught) == 3
        warnings.simplefilter("error", ValueConversionWarning)
        with pytest.raises(ValueConversionWarning):
            convert()
