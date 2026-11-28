"""No dict key and no set element is declared ``float32`` or ``float16`` (ITL-1591).

Developer decisions D16 and D25, November 28, 2026. A Python float stored under a
``float32`` or ``float16`` type is narrowed, and two distinct values can narrow to one
(``1e-50`` and ``2e-50`` both become ``0.0``): a dict stored two ``0.0`` keys and a set two
``0.0`` elements, so the container read back had lost an entry. A declared type whose
dict key or set element is ``float32`` or ``float16`` is therefore refused where it is
declared, in a Python schema or annotation and in an Arrow schema or table alike, at any
nesting depth, by a check of types that reads no value. The rule is
``semantic_types/narrow_float_keys.py``; the dict shape is the type converter's one
definition (``nested_storage.dict_entry_fields``), and an Arrow ``map`` is a dict too.

A ``float32`` or ``float16`` plain column, list element, struct field or dict VALUE is
accepted, and so is a ``float64`` key or element.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import numpy as np
import polars as pl
import pyarrow as pa
import pytest

from orcapod import contexts, function_pod
from orcapod.core.datagrams import Data, Datagram, Tag
from orcapod.core.sources import (
    ArrowTableSource,
    DataFrameSource,
    DeltaTableSource,
    DictSource,
    ListSource,
)
from orcapod.errors import InputValidationError
from orcapod.semantic_types.narrow_float_keys import (
    NARROW_FLOAT_KEYS_NOT_SUPPORTED,
    NarrowFloatKeyError,
    narrow_float_keys,
    require_no_narrow_float_keys,
    require_schema_without_narrow_float_keys,
)
from orcapod.semantic_types.nested_storage import dict_entry_fields

NOT_SUPPORTED = NARROW_FLOAT_KEYS_NOT_SUPPORTED


def _converter() -> Any:
    return contexts.resolve_context(None).type_converter


def _dict_of(key: pa.DataType, value: pa.DataType = pa.int64()) -> pa.DataType:
    """The converter's stored form of ``dict[key, value]``."""
    return pa.large_list(pa.struct([("key", key), ("value", value)]))


def _set_storage_field(element: pa.DataType) -> pa.Field[Any]:
    """A set in its storage form: the storage type, the identifier in field metadata."""
    live = _live_set(element)
    return pa.field(
        "item",
        live.storage_type,
        metadata={
            "ARROW:extension:name": live.extension_name,
            "ARROW:extension:metadata": live.__arrow_ext_serialize__(),
        },
    )


def _live_set(element: pa.DataType) -> Any:
    """The native ``set[T]`` container of a plain Arrow element, as the library builds it."""
    from orcapod.logical_types.list_logical_type_factory import (
        ArrowElement,
        ListLogicalType,
    )

    return ListLogicalType(
        ArrowElement(float, element), is_set=True
    ).get_arrow_extension_type()


NARROW: list[pa.DataType] = [pa.float32(), pa.float16()]
WIDE_OR_OTHER: list[pa.DataType] = [pa.float64(), pa.int64(), pa.large_string()]

# Every place a dict key can sit in a column's type: (name, builder of the column type
# from the key type).
DICT_PLACES: list[tuple[str, Callable[[pa.DataType], pa.DataType]]] = [
    ("top_level", _dict_of),
    (
        "non_large_list",
        lambda k: pa.list_(pa.struct([("key", k), ("value", pa.int64())])),
    ),
    (
        "value_first",
        lambda k: pa.large_list(pa.struct([("value", pa.int64()), ("key", k)])),
    ),
    ("in_a_list", lambda k: pa.large_list(_dict_of(k))),
    ("in_a_fixed_size_list", lambda k: pa.list_(_dict_of(k), 2)),
    ("as_a_dict_value", lambda k: _dict_of(pa.large_string(), _dict_of(k))),
    ("as_a_dict_key", lambda k: _dict_of(_dict_of(k))),
    ("in_a_struct", lambda k: pa.struct([("a", pa.int64()), ("d", _dict_of(k))])),
    (
        "deep",
        lambda k: pa.large_list(
            pa.struct([("s", pa.struct([("d", _dict_of(pa.int64(), _dict_of(k)))]))])
        ),
    ),
    ("map", lambda k: pa.map_(k, pa.int64())),
    ("map_in_a_list", lambda k: pa.large_list(pa.map_(k, pa.int64()))),
    ("as_a_map_value", lambda k: pa.map_(pa.large_string(), _dict_of(k))),
]

# Every place a set element can sit.
SET_PLACES: list[tuple[str, Callable[[pa.DataType], pa.DataType]]] = [
    ("live_set", _live_set),
    ("storage_form_in_a_list", lambda e: pa.large_list(_set_storage_field(e))),
    (
        "storage_form_in_a_struct",
        lambda e: pa.struct(
            [pa.field("a", pa.int64()), _set_storage_field(e).with_name("s")]
        ),
    ),
]


def _cases(
    places: list[tuple[str, Callable[[pa.DataType], pa.DataType]]],
    parts: list[pa.DataType],
) -> list[Any]:
    return [
        pytest.param(build, part, id=f"{name}-{part}")
        for name, build in places
        for part in parts
    ]


class TestTheRule:
    """A check of types, at every depth."""

    @pytest.mark.parametrize(("build", "key"), _cases(DICT_PLACES, NARROW))
    def test_refuses_a_narrow_float_dict_key_wherever_the_dict_sits(
        self, build: Callable[[pa.DataType], pa.DataType], key: pa.DataType
    ) -> None:
        column_type = build(key)
        assert narrow_float_keys(column_type) == [f"dict key type {key}"]
        with pytest.raises(NarrowFloatKeyError) as raised:
            require_schema_without_narrow_float_keys(
                pa.schema([("id", pa.int64()), ("k", column_type)])
            )
        message = str(raised.value)
        assert message.startswith(NOT_SUPPORTED)
        assert "'k'" in message and "'id'" not in message
        assert f"dict key type {key}" in message
        assert "float64" in message

    @pytest.mark.parametrize(("build", "element"), _cases(SET_PLACES, NARROW))
    def test_refuses_a_narrow_float_set_element_wherever_the_set_sits(
        self, build: Callable[[pa.DataType], pa.DataType], element: pa.DataType
    ) -> None:
        column_type = build(element)
        assert narrow_float_keys(column_type) == [f"set element type {element}"]
        with pytest.raises(NarrowFloatKeyError) as raised:
            require_schema_without_narrow_float_keys(
                pa.schema([("id", pa.int64()), ("s", column_type)])
            )
        message = str(raised.value)
        assert message.startswith(NOT_SUPPORTED)
        assert "'s'" in message and f"set element type {element}" in message

    @pytest.mark.parametrize("element", NARROW, ids=str)
    def test_refuses_a_top_level_set_in_its_storage_form(
        self, element: pa.DataType
    ) -> None:
        field = _set_storage_field(element).with_name("s")
        assert narrow_float_keys(field.type) == []  # a plain list without its metadata
        with pytest.raises(NarrowFloatKeyError, match="set element type"):
            require_schema_without_narrow_float_keys(pa.schema([field]))

    @pytest.mark.parametrize(
        ("build", "part"), _cases(DICT_PLACES + SET_PLACES, WIDE_OR_OTHER)
    )
    def test_accepts_every_other_key_and_element_type(
        self, build: Callable[[pa.DataType], pa.DataType], part: pa.DataType
    ) -> None:
        column_type = build(part)
        assert narrow_float_keys(column_type) == []
        require_no_narrow_float_keys(column_type)
        require_schema_without_narrow_float_keys(
            pa.schema([("id", pa.int64()), ("k", column_type)])
        )

    @pytest.mark.parametrize("narrow", NARROW, ids=str)
    def test_a_narrow_float_that_is_no_key_and_no_element_is_accepted(
        self, narrow: pa.DataType
    ) -> None:
        """A plain value, a list element, a struct field and a dict VALUE."""
        require_schema_without_narrow_float_keys(
            pa.schema(
                [
                    ("plain", narrow),
                    ("listed", pa.large_list(narrow)),
                    ("fixed", pa.list_(narrow, 2)),
                    ("field", pa.struct([("key", narrow), ("other", pa.int64())])),
                    ("lone_key", pa.large_list(pa.struct([("key", narrow)]))),
                    ("dict_value", _dict_of(pa.large_string(), narrow)),
                    ("map_value", pa.map_(pa.large_string(), narrow)),
                ]
            )
        )

    def test_names_every_such_column_whatever_the_column_order(self) -> None:
        fields = [
            pa.field("b", _dict_of(pa.float16())),
            pa.field("fine", _dict_of(pa.float64())),
            pa.field("a", pa.map_(pa.float32(), pa.int64())),
            pa.field("c", _live_set(pa.float32())),
            pa.field(
                "both", _dict_of(pa.float32(), pa.large_list(_dict_of(pa.float16())))
            ),
        ]
        messages = []
        for ordered in (fields, fields[::-1], fields[2:] + fields[:2]):
            with pytest.raises(NarrowFloatKeyError) as raised:
                require_schema_without_narrow_float_keys(pa.schema(ordered))
            messages.append(str(raised.value))
        message = messages[0]
        assert all(f"'{name}'" in message for name in ("a", "b", "c", "both"))
        assert "'fine'" not in message
        assert "(dict key type float, dict key type halffloat)" in message
        assert "set element type float" in message
        assert messages[0] == messages[1] == messages[2]

    def test_the_dict_shape_is_the_type_converters(self) -> None:
        """The rule and the converter read one definition of the dict shape."""
        converted = _converter().python_type_to_arrow_type(dict[float, int])
        fields = dict_entry_fields(converted)
        assert fields is not None
        assert (fields[0].type, fields[1].type) == (pa.float64(), pa.int64())
        assert dict_entry_fields(pa.large_list(pa.float32())) is None
        assert (
            dict_entry_fields(
                pa.large_list(pa.struct([("key", pa.float32()), ("v", pa.int64())]))
            )
            is None
        )
        assert dict_entry_fields(pa.map_(pa.float32(), pa.int64())) is None

    def test_the_error_is_an_input_validation_error(self) -> None:
        assert issubclass(NarrowFloatKeyError, InputValidationError)
        assert issubclass(NarrowFloatKeyError, ValueError)


D = dict[np.float32, int]
S = set[np.float32]

# Declared Python types that hold a float32 key or element, with a value of each.
PYTHON_TYPES: list[tuple[str, Any, Any]] = [
    ("dict", D, {np.float32(0.5): 1}),
    ("set", S, {np.float32(0.5)}),
    ("optional_dict", D | None, None),
    ("optional_set", S | None, None),
    ("dict_in_a_list", list[D], [{np.float32(0.5): 1}]),
    ("set_in_a_list", list[S], [{np.float32(0.5)}]),
    ("dict_as_a_dict_value", dict[str, D], {"a": {np.float32(0.5): 1}}),
    ("optional_key", dict[np.float32 | None, int], {np.float32(0.5): 1}),
]
PYTHON_CASES = [
    pytest.param(python_type, value, id=name)
    for name, python_type, value in PYTHON_TYPES
]

# Declared Python types that must keep working, with a value of each.
ACCEPTED_PYTHON: dict[str, tuple[Any, Any]] = {
    "plain": (np.float32, np.float32(0.5)),
    "listed": (list[np.float32], [np.float32(0.5), np.float32(1.5)]),
    "dict_value": (dict[str, np.float32], {"a": np.float32(0.5)}),
    "wide_key": (dict[float, int], {1e-50: 1, 2e-50: 2}),
    "wide_set": (set[float], {1e-50, 2e-50}),
}


@dataclasses.dataclass
class HoldsNarrowKey:
    """A user class with a float32-keyed dict field."""

    d: dict[np.float32, int]


class TestPythonDeclarations:
    """A type declared in Python is refused when it is declared."""

    @pytest.mark.parametrize(("python_type", "value"), PYTHON_CASES)
    def test_the_converter_refuses_the_type(self, python_type: Any, value: Any) -> None:
        with pytest.raises(NarrowFloatKeyError, match=NOT_SUPPORTED):
            _converter().python_type_to_arrow_type(python_type)

    @pytest.mark.parametrize(("python_type", "value"), PYTHON_CASES)
    @pytest.mark.parametrize("kind", [Datagram, Tag, Data])
    def test_a_datagram_refuses_it_when_built(
        self, kind: type[Datagram], python_type: Any, value: Any
    ) -> None:
        """At construction, before any table is asked for; in either column order."""
        for columns in (
            {"k": python_type, "x": int, "y": str},
            {"y": str, "x": int, "k": python_type},
        ):
            with pytest.raises(NarrowFloatKeyError) as raised:
                kind({"k": value, "x": 1, "y": "a"}, python_schema=columns)
            assert "Column 'k'" in str(raised.value)
            assert NOT_SUPPORTED in str(raised.value)

    @pytest.mark.parametrize(("python_type", "value"), PYTHON_CASES)
    def test_a_source_with_a_python_schema_refuses_it(
        self, python_type: Any, value: Any
    ) -> None:
        with pytest.raises(NarrowFloatKeyError) as raised:
            DictSource(
                [{"id": 1, "g": "a", "x": 1, "k": value}],
                tag_columns=["id", "g"],
                tag_only_columns=True,
                data_schema={"id": int, "g": str, "x": int, "k": python_type},
            )
        assert "'k'" in str(raised.value)

    @pytest.mark.parametrize("declared", [tuple[D, D], tuple[int, D]], ids=str)
    def test_a_tuple_field_is_refused(self, declared: Any) -> None:
        """A dict in a tuple: where a tuple type is converted (a datagram)."""
        with pytest.raises(NarrowFloatKeyError, match=NOT_SUPPORTED):
            _converter().python_type_to_arrow_type(declared)
        with pytest.raises(NarrowFloatKeyError, match="Column 'k'"):
            Datagram({"k": ({}, {}), "x": 1}, python_schema={"k": declared, "x": int})

    def test_a_list_source_refuses_it(self) -> None:
        with pytest.raises(NarrowFloatKeyError, match=NOT_SUPPORTED):
            ListSource("k", [{np.float32(0.5): 1}], data_schema={"k": D})

    def test_a_user_class_field_is_refused(self) -> None:
        with pytest.raises(NarrowFloatKeyError) as raised:
            DictSource(
                [{"id": 1, "c": cast(Any, HoldsNarrowKey({}))}],
                tag_columns=["id"],
                tag_only_columns=True,
                data_schema={"id": int, "c": HoldsNarrowKey},
            )
        assert "HoldsNarrowKey.d" in str(raised.value)

    @pytest.mark.parametrize("declared", [D, S, list[S], dict[str, D]], ids=str)
    def test_a_function_pod_refuses_a_declared_output_when_defined(
        self, declared: Any
    ) -> None:
        def produce(x: int, y: int):  # noqa: ANN202 - annotated below
            raise AssertionError("never called")

        produce.__annotations__ = {"x": int, "y": int, "return": declared}
        with pytest.raises(NarrowFloatKeyError, match=NOT_SUPPORTED):
            function_pod(output_keys="o")(produce)

    @pytest.mark.parametrize("declared", [D, S, list[S], dict[str, D]], ids=str)
    def test_a_function_pod_refuses_a_declared_input_when_defined(
        self, declared: Any
    ) -> None:
        """Whichever parameter declares it, of several."""
        for declared_types in (
            {"a": int, "b": declared, "c": str, "return": int},
            {"a": declared, "b": int, "c": str, "return": int},
        ):

            def consume(a, b, c):  # noqa: ANN001, ANN202 - annotated below
                raise AssertionError("never called")

            consume.__annotations__ = declared_types
            with pytest.raises(NarrowFloatKeyError, match=NOT_SUPPORTED):
                function_pod(output_keys="o")(consume)

    def test_a_function_pod_refuses_one_of_several_outputs(self) -> None:
        def produce(x: int) -> tuple[int, dict[np.float32, int], str]:
            raise AssertionError("never called")

        with pytest.raises(NarrowFloatKeyError) as raised:
            function_pod(output_keys=["n", "d", "s"])(produce)
        assert "'d'" in str(raised.value)

    def test_what_must_keep_working_keeps_working(self) -> None:
        """float32 as a plain value, list element and dict value; float64 keys."""
        values = {name: value for name, (_, value) in ACCEPTED_PYTHON.items()}
        schema = {
            name: python_type for name, (python_type, _) in ACCEPTED_PYTHON.items()
        }
        for order in (list(schema), list(schema)[::-1]):
            datagram = Datagram(
                {name: values[name] for name in order},
                python_schema={name: schema[name] for name in order},
            )
            read = Datagram(datagram.as_table()).as_dict()
            assert read["wide_key"] == {1e-50: 1, 2e-50: 2}
            assert read["wide_set"] == {1e-50, 2e-50}
            assert read["dict_value"] == {"a": 0.5}
            assert read["listed"] == [0.5, 1.5]
            assert read["plain"] == 0.5

        @function_pod(output_keys=["v", "w"])
        def keep(
            listed: list[float],
        ) -> tuple[dict[str, np.float32], dict[float, int]]:
            return {"a": np.float32(listed[0])}, {1e-50: 1, 2e-50: 2}

        source = DictSource(
            [{"id": 1, **values}],
            tag_columns=["id"],
            tag_only_columns=True,
            data_schema={"id": int, **schema},
        )
        ((_, data),) = list(keep.pod(source).iter_data())
        assert data.as_dict() == {"v": {"a": 0.5}, "w": {1e-50: 1, 2e-50: 2}}


def _table(column_type: pa.DataType) -> pa.Table:
    """One row, two tags, two plain data columns and a column ``k`` of the given type."""
    return pa.table(
        {
            "id": pa.array([1], pa.int64()),
            "g": pa.array(["a"], pa.large_string()),
            "x": pa.array([1], pa.int64()),
            "y": pa.array([0.5], pa.float32()),
            "k": pa.array([None], column_type),
        }
    )


ARROW_CASES = _cases(DICT_PLACES, NARROW) + _cases(SET_PLACES[:1], NARROW)


class TestArrowDeclarations:
    """A type declared in Arrow is refused where the declaration enters."""

    def test_the_dict_reproduction_no_longer_loses_an_entry(self) -> None:
        """``{1e-50: 1, 2e-50: 2}`` was stored with two ``0.0`` keys."""
        with pytest.raises(NarrowFloatKeyError) as raised:
            DictSource(
                [{"id": 1, "h": 0.1, "k": {1e-50: 1, 2e-50: 2}}],
                tag_columns=["id"],
                tag_only_columns=True,
                data_schema=pa.schema(
                    [
                        ("id", pa.int64()),
                        ("h", pa.float16()),
                        ("k", _dict_of(pa.float32())),
                    ]
                ),
            )
        message = str(raised.value)
        assert message.startswith(NOT_SUPPORTED)
        assert "'k'" in message and "'h'" not in message

    def test_the_set_reproduction_no_longer_loses_an_element(self) -> None:
        """``{1e-50, 2e-50}`` was stored as ``[0.0, 0.0]`` and read back ``{0.0}``."""
        with pytest.raises(NarrowFloatKeyError, match="set element type float"):
            DictSource(
                [{"id": 1, "k": {1e-50, 2e-50}}],
                tag_columns=["id"],
                tag_only_columns=True,
                data_schema=pa.schema(
                    [pa.field("id", pa.int64()), pa.field("k", _live_set(pa.float32()))]
                ),
            )

    def test_a_float64_key_and_element_from_an_arrow_schema_keep_every_entry(
        self,
    ) -> None:
        source = DictSource(
            [{"id": 1, "k": {1e-50: 1, 2e-50: 2}, "s": {1e-50, 2e-50}, "h": 0.5}],
            tag_columns=["id"],
            tag_only_columns=True,
            data_schema=pa.schema(
                [
                    pa.field("id", pa.int64()),
                    pa.field("k", _dict_of(pa.float64())),
                    pa.field("s", _live_set(pa.float64())),
                    pa.field("h", pa.float32()),
                ]
            ),
        )
        ((_, data),) = list(source.iter_data())
        assert data.as_dict() == {
            "k": {1e-50: 1, 2e-50: 2},
            "s": {1e-50, 2e-50},
            "h": 0.5,
        }

    @pytest.mark.parametrize(("build", "part"), ARROW_CASES)
    def test_an_arrow_table_source_refuses_it_in_every_role_and_tag_order(
        self, build: Callable[[pa.DataType], pa.DataType], part: pa.DataType
    ) -> None:
        table = _table(build(part))
        for tags, tag_only in (
            (["id", "g"], True),
            (["g", "id"], True),
            (["id", "g"], False),
            (["id", "k"], True),
            (["k", "id"], False),
        ):
            with pytest.raises(NarrowFloatKeyError) as raised:
                ArrowTableSource(table, tag_columns=tags, tag_only_columns=tag_only)
            assert str(raised.value).startswith(NOT_SUPPORTED)
            assert "'k'" in str(raised.value)

    def test_no_value_is_read(self) -> None:
        """A table with no rows is refused the same way: the types decide."""
        empty = _table(_dict_of(pa.float32())).slice(0, 0)
        with pytest.raises(NarrowFloatKeyError, match=NOT_SUPPORTED):
            ArrowTableSource(empty, tag_columns=["id", "g"])

    def test_a_data_frame_source_refuses_it(self) -> None:
        schema = pa.schema(
            [("id", pa.int64()), ("x", pa.int64()), ("k", _dict_of(pa.float32()))]
        )
        rows = {"id": [1], "x": [1], "k": [[{"key": 0.5, "value": 1}]]}
        with pytest.raises(NarrowFloatKeyError, match=NOT_SUPPORTED):
            DataFrameSource(
                rows, tag_columns=["id"], tag_only_columns=True, schema=schema
            )
        frame = pl.DataFrame(
            rows,
            schema={
                "id": pl.Int64,
                "x": pl.Int64,
                "k": pl.List(pl.Struct({"key": pl.Float32, "value": pl.Int64})),
            },
        )
        with pytest.raises(NarrowFloatKeyError, match=NOT_SUPPORTED):
            DataFrameSource(frame, tag_columns=["id"], tag_only_columns=True)

    def test_a_delta_table_source_refuses_it(self, tmp_path: Path) -> None:
        from deltalake import write_deltalake

        table = pa.table(
            {
                "id": pa.array([1], pa.int64()),
                "x": pa.array([1], pa.int64()),
                "k": pa.array(
                    [[{"key": 0.5, "value": 1}]],
                    pa.list_(pa.struct([("key", pa.float32()), ("value", pa.int64())])),
                ),
            }
        )
        path = str(tmp_path / "t")
        write_deltalake(path, table)
        with pytest.raises(NarrowFloatKeyError, match=NOT_SUPPORTED):
            DeltaTableSource(path, tag_columns=["id"], tag_only_columns=True)

    def test_a_narrow_float_that_is_no_key_passes_through_a_stream(self) -> None:
        """A plain float32 column, a list of them and a float32 dict value are kept."""
        table = pa.table(
            {
                "id": pa.array([1, 2], pa.int64()),
                "g": pa.array(["a", "b"], pa.large_string()),
                "plain": pa.array([0.5, 1.5], pa.float32()),
                "half": pa.array([0.5, 1.5], pa.float16()),
                "listed": pa.array([[0.5], [1.5]], pa.large_list(pa.float32())),
                "dict_value": pa.array(
                    [[{"key": "a", "value": 0.5}], []],
                    _dict_of(pa.large_string(), pa.float32()),
                ),
                "wide_key": pa.array(
                    [[{"key": 1e-50, "value": 1}, {"key": 2e-50, "value": 2}], []],
                    _dict_of(pa.float64()),
                ),
            }
        )
        for tags in (["id", "g"], ["g", "id"]):
            source = ArrowTableSource(table, tag_columns=tags, tag_only_columns=True)
            out = source.as_table()
            for name in ("plain", "half", "listed", "dict_value", "wide_key"):
                assert out.column(name).type == table.column(name).type
                assert out.column(name).to_pylist() == table.column(name).to_pylist()
            rows = [data.as_dict() for _, data in source.iter_data()]
            assert rows[0]["wide_key"] == {1e-50: 1, 2e-50: 2}
            assert rows[0]["dict_value"] == {"a": 0.5}

    def test_a_plain_arrow_declared_float32_value_is_still_narrowed(self) -> None:
        """Known limitation: a plain value (not a key or element) is narrowed as before."""
        source = DictSource(
            [{"id": 1, "h": 0.1}],
            tag_columns=["id"],
            tag_only_columns=True,
            data_schema=pa.schema([("id", pa.int64()), ("h", pa.float32())]),
        )
        assert source.as_table().column("h").to_pylist() == [float(np.float32(0.1))]


@pytest.mark.parametrize("cls", [Datagram, Tag, Data])
@pytest.mark.parametrize("declaration", ["python", "type", "field"])
def test_datagram_with_columns_refuses_declared_narrow_dict_key(cls, declaration):
    arrow_type = _dict_of(pa.float32())
    declared = {
        "python": dict[np.float32, int],
        "type": arrow_type,
        "field": pa.field("k", arrow_type),
    }[declaration]
    with pytest.raises(NarrowFloatKeyError, match="column 'k'|Column 'k'"):
        cls({"id": 1}).with_columns({"k": declared}, k={1e-50: 1, 2e-50: 2})


@pytest.mark.parametrize("arrow_type", NARROW)
@pytest.mark.parametrize("stored", [False, True])
@pytest.mark.parametrize("outer_set", [False, True])
def test_native_nested_set_refuses_narrow_element(
    arrow_type: pa.DataType, stored: bool, outer_set: bool
) -> None:
    from orcapod.logical_types.canonical_form import storage_form_schema
    from orcapod.logical_types.list_logical_type_factory import (
        ArrowElement,
        ListLogicalType,
    )

    inner = ListLogicalType(ArrowElement(float, arrow_type), is_set=True)
    outer = ListLogicalType(inner, is_set=outer_set).get_arrow_extension_type()
    storage = pa.array([[]], type=outer.storage_type)
    live = pa.ExtensionArray.from_storage(outer, storage)
    table = pa.table({"id": [1], "k": live})
    if stored:
        table = pa.Table.from_arrays(
            [table["id"], storage], schema=storage_form_schema(table.schema)
        )
    with pytest.raises(NarrowFloatKeyError, match="set element type"):
        ArrowTableSource(table, tag_columns="id", source_id="nested-set")


@pytest.mark.parametrize("operator", ["batch", "group_by", "merge_join"])
def test_struct_aggregation_keeps_narrow_float_as_data(operator: str) -> None:
    from orcapod.core.operators import Batch, GroupBy, MergeJoin

    pair = {"key": 0.5, "value": 1}
    pair_type = pa.struct([("key", pa.float32()), ("value", pa.int64())])
    table = pa.table({"id": [1], "pair": pa.array([pair], type=pair_type)})
    source = ArrowTableSource(
        table, tag_columns="id", tag_only_columns=True, source_id="pair"
    )
    if operator == "batch":
        result = Batch().static_process(source)
    elif operator == "group_by":
        result = GroupBy("id").static_process(source)
    else:
        result = MergeJoin().static_process(source, source)
    assert result.as_table()["pair"].to_pylist() == [
        [pair, pair] if operator == "merge_join" else [pair]
    ]
    assert next(result.iter_data())[1].as_table()["pair"].to_pylist() == (
        result.as_table()["pair"].to_pylist()
    )


def test_struct_aggregation_replays_from_delta(tmp_path: Path) -> None:
    from orcapod.core.nodes.operator_node import OperatorJobNode
    from orcapod.core.operators import Batch
    from orcapod.core.sources.derived_source import DerivedSource
    from orcapod.databases import DeltaTableDatabase
    from orcapod.pipeline import PipelineJob
    from orcapod.types import CacheMode

    pair = {"key": 0.5, "value": 1}
    pair_type = pa.struct([("key", pa.float32()), ("value", pa.int64())])
    table = pa.table({"id": [1], "pair": pa.array([pair], type=pair_type)})
    source = ArrowTableSource(
        table, tag_columns="id", tag_only_columns=True, source_id="pairs"
    )
    writer = OperatorJobNode(
        operator=Batch(),
        input_streams=(source,),
        pipeline_database=DeltaTableDatabase(tmp_path),
        cache_mode=CacheMode.LOG,
    )
    writer.run()
    replay = OperatorJobNode(
        operator=Batch(),
        input_streams=(source,),
        pipeline_database=DeltaTableDatabase(tmp_path),
        cache_mode=CacheMode.REPLAY,
    )
    replay.run()

    rows = replay.as_table()
    assert rows["pair"].type == pa.large_list(pair_type)
    assert rows["pair"].to_pylist() == [[pair]]
    assert next(replay.iter_data())[1].as_dict()["pair"] == {0.5: 1}
    derived = DerivedSource(replay, tags_as_data={"id": "copied_id"}, source_id="copy")
    assert derived.as_table()["pair"].to_pylist() == [[pair]]

    job = PipelineJob(database=DeltaTableDatabase(tmp_path / "job"))
    with job:
        derived.batch(label="batch")
    path = tmp_path / "job.json"
    job.save(path)
    assert PipelineJob.load(path).nodes["copy"].arrow_schemas[1].field("pair").type == (
        rows["pair"].type
    )


@pytest.mark.parametrize(
    "schema",
    [
        pa.schema([("id", pa.int64()), ("k", _dict_of(pa.float32()))]),
        {"id": int, "k": set[np.float16]},
    ],
)
def test_polling_source_refuses_declared_schema_without_fetch(
    schema: pa.Schema | dict[str, Any],
) -> None:
    from unittest.mock import Mock

    from orcapod.core.sources.polling_source import PollingSource
    from orcapod.protocols.core_protocols.sources import DynamicSourceProtocol

    impl = Mock(spec=DynamicSourceProtocol)
    impl.schema.return_value = schema
    with pytest.raises(NarrowFloatKeyError, match="column 'k'|Column 'k'"):
        PollingSource(impl, "id", source_id="poll", record_id_columns="id")
    impl.fetch.assert_not_called()


@pytest.mark.parametrize("reader", ["config", "node"])
@pytest.mark.parametrize("type_text", ["dict", "set"])
def test_saved_arrow_schema_refuses_narrow_keys(reader: str, type_text: str) -> None:
    from orcapod.pipeline.serialization import (
        arrow_type_texts,
        deserialize_arrow_schema,
        deserialize_schema,
    )

    arrow_type = (
        _dict_of(pa.float32()) if type_text == "dict" else _live_set(pa.float16())
    )
    columns = arrow_type_texts(pa.schema([("k", arrow_type)]), _converter())
    with pytest.raises(NarrowFloatKeyError, match="column 'k'"):
        if reader == "config":
            deserialize_arrow_schema({"columns": columns, "nullable": []})
        else:
            deserialize_schema(columns, _converter(), [])


def test_saved_job_rejects_prohibited_source_declaration(tmp_path: Path) -> None:
    from orcapod.databases import DeltaTableDatabase
    from orcapod.pipeline import PipelineJob

    table = _table(_dict_of(pa.float64()))
    source = ArrowTableSource(
        table, tag_columns="id", tag_only_columns=True, source_id="pairs"
    )
    job = PipelineJob(database=DeltaTableDatabase(tmp_path / "store"))
    with job:
        source.batch(label="batch")
    path = tmp_path / "job.json"
    job.save(path)
    saved = json.loads(path.read_text())
    for node in saved["pipeline"]["nodes"].values():
        if node["node_type"] == "source":
            node["source_config"]["data_schema"]["k"] = str(_dict_of(pa.float32()))
            node["output_schema"]["data"]["k"] = str(_dict_of(pa.float32()))
    path.write_text(json.dumps(saved))
    with pytest.raises(NarrowFloatKeyError, match="column 'k'"):
        PipelineJob.load(path)


@pytest.mark.parametrize("declared_in_arrow", [False, True])
def test_pipeline_input_slot_refuses_declared_narrow_type(
    declared_in_arrow: bool,
) -> None:
    from orcapod.core.nodes.source_node import SourceNode
    from orcapod.types import Schema

    with pytest.raises(NarrowFloatKeyError, match="column 'k'|Column 'k'"):
        if declared_in_arrow:
            SourceNode(
                "inputs", pa.schema([]), pa.schema([("k", _dict_of(pa.float32()))])
            )
        else:
            SourceNode("inputs", Schema({}), Schema({"k": dict[np.float32, int]}))


@pytest.mark.parametrize("cls", [Datagram, Tag, Data])
@pytest.mark.parametrize("kind", ["dataclass", "structural", "pydantic"])
def test_datagram_refuses_declared_user_class_field(cls, kind):
    from orcapod import dataclass as structural_dataclass
    from pydantic import ConfigDict, create_model

    annotation = dict[np.float32, int]
    if kind == "pydantic":
        model_config = ConfigDict(arbitrary_types_allowed=True)
        model = create_model(
            "NarrowKeyModel", __config__=model_config, bad=(annotation, ...)
        )
    else:
        model = type("NarrowKeyClass", (), {"__annotations__": {"bad": annotation}})
        model = (
            structural_dataclass(model)
            if kind == "structural"
            else dataclasses.dataclass(model)
        )
    with pytest.raises(NarrowFloatKeyError, match=r"k.*bad|bad"):
        cls({"k": model(bad={})}, python_schema={"k": model})


@pytest.mark.parametrize("is_source", [False, True])
def test_plain_list_with_unrelated_extension_metadata_is_allowed(is_source):
    field = pa.field(
        "k",
        pa.large_list(pa.float32()),
        metadata={b"ARROW:extension:metadata": b'{"category":"set"}'},
    )
    table = pa.Table.from_arrays(
        [pa.array([1]), pa.array([[0.5]], type=field.type)],
        schema=pa.schema([pa.field("id", pa.int64()), field]),
    )
    result = ArrowTableSource(table, tag_columns="id") if is_source else Datagram(table)
    assert result.as_table()["k"].to_pylist() == [[0.5]]


@pytest.mark.parametrize("container", ["union", "dictionary", "list_view", "run_end"])
def test_declared_mutation_refuses_map_through_other_arrow_containers(
    container: str,
) -> None:
    inner = pa.map_(pa.float32(), pa.int64())
    if container == "union":
        outer = pa.union([pa.field("map", inner)], mode="sparse")
    elif container == "dictionary":
        outer = pa.dictionary(pa.int8(), inner)  # pyright: ignore[reportArgumentType]  # Arrow accepts map values
    elif container == "list_view":
        outer = pa.large_list_view(inner)
    else:
        outer = pa.run_end_encoded(pa.int16(), inner)  # pyright: ignore[reportArgumentType]  # Arrow accepts map values
    with pytest.raises(NarrowFloatKeyError, match="dict key type"):
        Datagram({"id": 1}).with_columns({"k": outer}, k=[])


@pytest.mark.parametrize("source", ["dict", "frame", "csv"])
def test_narrow_key_diagnostic_precedes_source_value_conversion(
    tmp_path: Path, source: str
) -> None:
    from orcapod.core.sources.csv_source import CSVSource

    schema = pa.schema([("id", pa.int64()), ("k", _dict_of(pa.float32()))])
    with pytest.raises(NarrowFloatKeyError, match="float64"):
        if source == "dict":
            DictSource([{"id": 1, "k": "oops"}], "id", data_schema=schema)
        elif source == "frame":
            DataFrameSource({"id": [1], "k": ["oops"]}, "id", schema=schema)
        else:
            path = tmp_path / "narrow.csv"
            path.write_text("id,k\n1,oops\n")
            CSVSource(str(path), "id", schema=schema)


@pytest.mark.parametrize("annotation", [set[np.float16], dict[np.float16, int]])
def test_pod_refuses_python_float16_key_or_element(annotation):
    def run(k):
        return len(k)

    run.__annotations__ = {"k": annotation, "return": int}
    with pytest.raises(NarrowFloatKeyError, match="float64"):
        function_pod(output_keys="n")(run)


def test_duplicate_key_struct_is_not_dict_shape() -> None:
    pair = pa.struct(
        [("key", pa.float32()), ("key", pa.int64()), ("value", pa.int64())]
    )
    arrow_type = pa.large_list(pair)
    assert dict_entry_fields(arrow_type) is None
    table = pa.schema([("id", pa.int64()), ("k", arrow_type)]).empty_table()
    with pytest.raises(ValueError, match=r"column 'k'|Column 'k'"):
        ArrowTableSource(table, tag_columns="id")
    with pytest.raises(ValueError, match=r"column 'k'|Column 'k'"):
        DictSource([{"id": 1, "k": []}], "id", data_schema=table.schema)
