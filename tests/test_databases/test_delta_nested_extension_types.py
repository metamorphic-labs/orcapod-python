"""Delta accepts a logical type only as a whole top-level column (ITL-1372)."""

from __future__ import annotations

import re
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import pyarrow as pa
import pytest
from deltalake import DeltaTable
from pydantic import BaseModel

from orcapod import dataclass
from orcapod.contexts import get_default_context
from orcapod.databases import DeltaTableDatabase, ExtensionAwareDatabase
from orcapod.errors import UnstorableColumnError
from orcapod.logical_types.registry import LogicalTypeRegistry
from orcapod.semantic_types.universal_converter import UniversalTypeConverter

PATH = ("t",)
NAME = b"ARROW:extension:name"
META = b"ARROW:extension:metadata"


class _Unit(pa.ExtensionType):
    """A small Arrow extension with a caller-supplied storage type."""

    def __init__(self, name: str, storage: pa.DataType) -> None:
        super().__init__(storage, name)

    def __arrow_ext_serialize__(self) -> bytes:
        return self.extension_name.encode()

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> _Unit:
        return cls(serialized.decode(), storage_type)


@contextmanager
def _register(extension: _Unit, registered: bool) -> Iterator[None]:
    """Register the extension only when the test calls for a warm reader."""
    if registered:
        pa.register_extension_type(cast(Any, extension))
    try:
        yield
    finally:
        if registered:
            pa.unregister_extension_type(extension.extension_name)


def _annotation(extension: _Unit) -> dict[bytes | str, bytes | str]:
    """The storage form's extension identifier."""
    return {
        NAME: extension.extension_name.encode(),
        META: extension.__arrow_ext_serialize__(),
    }


def _nest(container: str, field: Any, array: Any) -> tuple[Any, Any]:
    """Wrap one row in a container, keeping any annotation on the nested field."""
    if container == "struct":
        array = pa.StructArray.from_arrays([array], fields=[field.with_name("v")])
    elif container == "list":
        array = pa.ListArray.from_arrays(
            pa.array([0, 1], pa.int32()), array, type=pa.list_(field.with_name("item"))
        )
    elif container == "large list":
        array = pa.LargeListArray.from_arrays(
            pa.array([0, 1], pa.int64()),
            array,
            type=pa.large_list(field.with_name("item")),
        )
    elif container == "fixed list":
        from_fixed_arrays: Any = pa.FixedSizeListArray.from_arrays
        array = from_fixed_arrays(array, type=pa.list_(field.with_name("item"), 1))
    elif container == "list view":
        from_arrays: Any = pa.ListViewArray.from_arrays
        array = from_arrays(
            pa.array([0], pa.int32()),
            pa.array([1], pa.int32()),
            array,
            type=pa.list_view(field.with_name("item")),
        )
    elif container == "dictionary":
        # Dictionary values have no Field of their own to carry an annotation.
        array = pa.DictionaryArray.from_arrays(pa.array([0], pa.int8()), array)
    else:
        assert container in ("map key", "map value")
        from_arrays: Any = pa.MapArray.from_arrays
        plain = pa.array(["k"], pa.large_string())
        plain_field = pa.field("key", plain.type, nullable=False)
        if container == "map key":
            key, value = array, plain
            key_field, value_field = (
                field.with_name("key").with_nullable(False),
                plain_field.with_name("value"),
            )
        else:
            key, value = plain, array
            key_field, value_field = plain_field, field.with_name("value")
        map_type = pa.map_(key_field, value_field)  # pyright: ignore[reportArgumentType]  # Arrow accepts Fields
        array = from_arrays(pa.array([0, 1], pa.int32()), key, value, type=map_type)
    return pa.field("measurement", array.type), array


@pytest.mark.parametrize(
    ("containers", "path"),
    [
        (("struct",), ".v"),
        (("struct", "struct"), ".v.v"),
        (("list",), "[]"),
        (("large list", "struct"), ".v[]"),
        (("fixed list", "struct"), ".v[]"),
        (("list view",), "[]"),
        (("map key",), ".key"),
        (("map value",), ".value"),
        (("struct", "map value"), ".value.v"),
        (("struct", "dictionary"), "<dictionary values>.v"),
    ],
)
@pytest.mark.parametrize("annotated", [False, True], ids=["actual", "annotated"])
@pytest.mark.parametrize(
    "registered", [False, True], ids=["unregistered", "registered"]
)
def test_delta_refuses_nested_logical_types(
    tmp_path: Path,
    containers: tuple[str, ...],
    path: str,
    annotated: bool,
    registered: bool,
) -> None:
    """The public write APIs refuse the same nested position before queuing."""
    extension = _Unit("itl1372.unit", pa.large_string())
    storage = pa.array(["a"], extension.storage_type)
    array: Any = storage if annotated else extension.wrap_array(storage)
    field: Any = pa.field(
        "measurement",
        array.type,
        metadata=_annotation(extension) if annotated else None,
    )
    for container in containers:
        field, array = _nest(container, field, array)
    table = pa.Table.from_arrays([array], schema=pa.schema([field]))
    records = table.append_column("id", pa.array([b"1"], pa.large_binary()))
    with _register(extension, registered):
        delta = DeltaTableDatabase(tmp_path)
        wrapped = ExtensionAwareDatabase(
            delta, UniversalTypeConverter(logical_type_registry=LogicalTypeRegistry())
        )
        with pytest.raises(
            UnstorableColumnError, match=re.escape("measurement" + path)
        ) as first:
            delta.add_records(PATH, records, record_id_column="id")
        delta.flush()
        assert delta.get_all_records(PATH) is None
        with pytest.raises(
            UnstorableColumnError, match="logical type nested in another type"
        ) as second:
            wrapped.add_record(PATH, b"2", table)
        assert str(first.value) == str(second.value)
        assert first.value.column == "measurement"
        assert "Orcapod container or class type" in str(first.value)
        delta.flush()
        assert not (tmp_path / "t" / "_delta_log").exists()


@pytest.mark.parametrize("inner_annotated", [False, True], ids=["actual", "annotated"])
@pytest.mark.parametrize("outer_annotated", [False, True], ids=["actual", "annotated"])
@pytest.mark.parametrize(
    "registered", [False, True], ids=["unregistered", "registered"]
)
def test_delta_refuses_nested_type_in_top_level_extension_storage(
    tmp_path: Path, inner_annotated: bool, outer_annotated: bool, registered: bool
) -> None:
    """A top-level extension cannot hide another extension in its storage."""
    inner = _Unit("itl1372.inner", pa.large_string())
    scalar = pa.array(["a"], inner.storage_type)
    item = scalar if inner_annotated else inner.wrap_array(scalar)
    child = pa.field(
        "value", item.type, metadata=_annotation(inner) if inner_annotated else None
    )
    _, storage = _nest("map value", child, item)
    outer = _Unit("itl1372.outer", storage.type)
    values = storage if outer_annotated else outer.wrap_array(storage)
    field = pa.field(
        "measurement",
        values.type,
        metadata=_annotation(outer) if outer_annotated else None,
    )
    table = pa.Table.from_arrays([values], schema=pa.schema([field]))
    with _register(inner, registered), _register(outer, registered):
        db = DeltaTableDatabase(tmp_path)
        for rid in (b"1", b"2"):
            with pytest.raises(
                UnstorableColumnError, match=r"measurement<storage>\.value"
            ):
                db.add_record(PATH, rid, table)
            db.flush()
            assert db.get_all_records(PATH) is None


@pytest.mark.parametrize("order", [("samples", "units"), ("units", "samples")])
def test_delta_names_both_unstorable_columns(
    tmp_path: Path, order: tuple[str, str]
) -> None:
    """An error names both columns and their paths in either schema order."""
    extension = _Unit("itl1372.multi", pa.large_string())
    values = extension.wrap_array(pa.array(["a"], extension.storage_type))
    field = pa.field("x", values.type)
    columns = {
        "samples": _nest("list", field, values)[1],
        "units": _nest("map value", field, values)[1],
    }
    table = pa.table(
        {
            "id": pa.array([b"1"], pa.large_binary()),
            **{key: columns[key] for key in order},
        }
    )
    db = DeltaTableDatabase(tmp_path)
    with pytest.raises(UnstorableColumnError) as error:
        db.add_records(PATH, table, record_id_column="id")
    assert "samples[]" in str(error.value)
    assert "units.value" in str(error.value)
    db.flush()
    assert db.get_all_records(PATH) is None


@pytest.mark.parametrize("annotated", [False, True], ids=["actual", "annotated"])
@pytest.mark.parametrize(
    "registered", [False, True], ids=["unregistered", "registered"]
)
@pytest.mark.parametrize(
    "metadata", [False, True], ids=["no metadata", "ordinary metadata"]
)
def test_delta_keeps_top_level_scalar_extension(
    tmp_path: Path, annotated: bool, registered: bool, metadata: bool
) -> None:
    """Both writes preserve the whole-column extension and its ordinary metadata."""
    extension = _Unit("itl1372.scalar", pa.large_string())
    storage = pa.array(["a"], extension.storage_type)
    actual = extension.wrap_array(storage)
    ordinary = {b"units": b"metres"} if metadata else {}

    def table(array: Any, is_annotated: bool) -> pa.Table:
        md = {**ordinary, **(_annotation(extension) if is_annotated else {})}
        return pa.Table.from_arrays(
            [array], schema=pa.schema([pa.field("x", array.type, metadata=md or None)])
        )

    given = table(storage, True) if annotated else table(actual, False)
    expected = table(actual, False) if registered else table(storage, True)
    with _register(extension, registered):
        for count in (1, 2):
            DeltaTableDatabase(tmp_path).add_record(
                PATH, str(count).encode(), given, flush=True
            )
            read = DeltaTableDatabase(tmp_path).get_all_records(PATH)
            assert read is not None
            assert read.select(["x"]).equals(
                pa.concat_tables([expected] * count), check_metadata=True
            )


@dataclass
class _WithPath:
    """A structural dataclass with a logical field."""

    value: Path


class _WithUUID(BaseModel):
    """A Pydantic model with a logical field."""

    value: UUID


@pytest.mark.parametrize(
    ("python_type", "value"),
    [
        (Path, Path("/a")),
        (list[Path], [Path("/a")]),
        (set[Path], {Path("/a")}),
        (_WithPath, _WithPath(Path("/a"))),
        (_WithUUID, _WithUUID(value=UUID(int=1))),
        (list[_WithPath], [_WithPath(Path("/a"))]),
    ],
    ids=["scalar", "list", "set", "dataclass", "pydantic", "list of dataclasses"],
)
def test_delta_keeps_orcapod_logical_columns(
    tmp_path: Path, python_type: Any, value: Any
) -> None:
    """The warm and cold reads recover the original logical types and values."""
    converter = get_default_context().type_converter
    table = converter.python_dicts_to_arrow_table(
        [{"x": value}], python_schema={"x": python_type}
    )
    field = table.schema.field("x")
    assert isinstance(field.type, pa.ExtensionType)
    assert field.type.extension_name
    field = field.with_metadata({b"units": b"metres"})
    table = pa.Table.from_arrays([table.column("x")], schema=pa.schema([field]))
    code = """
import sys

import pyarrow as pa

from orcapod.contexts import get_default_context
from orcapod.databases import DeltaTableDatabase, ExtensionAwareDatabase
from orcapod.databases.utils import identity_node
from tests.test_databases.test_delta_nested_extension_types import _WithPath, _WithUUID

db = ExtensionAwareDatabase(DeltaTableDatabase(sys.argv[1]), get_default_context().type_converter)
actual = db.get_all_records(("t",)).select(["x"])
with pa.ipc.open_file(sys.argv[2]) as reader:
    expected = reader.read_all()
given, recovered = expected.column("x").combine_chunks(), actual.column("x").combine_chunks()
assert given.type == recovered.type
assert given.storage.equals(recovered.storage)
assert actual.equals(expected)
actual_field, expected_field = actual.schema.field("x"), expected.schema.field("x")
assert identity_node(actual_field).identifier == identity_node(expected_field).identifier
ordinary = {k: v for k, v in actual_field.metadata.items() if not k.startswith(b"ARROW:extension:")}
assert ordinary == expected_field.metadata
"""
    for count in (1, 2):
        DeltaTableDatabase(tmp_path).add_record(
            PATH, str(count).encode(), table, flush=True
        )
        warm = DeltaTableDatabase(tmp_path).get_all_records(PATH)
        assert warm is not None
        expected = pa.concat_tables([table] * count)
        assert warm.select(["x"]).equals(expected, check_metadata=True)
        expected_file = tmp_path / "expected.arrow"
        with pa.ipc.new_file(str(expected_file), expected.schema) as writer:
            writer.write_table(expected)
        subprocess.run(
            [sys.executable, "-c", code, str(tmp_path), str(expected_file)], check=True
        )


def test_delta_refuses_annotation_in_later_chunk(tmp_path: Path) -> None:
    """A child annotation in a later chunk can differ from the table schema."""
    extension = _Unit("itl1372.later", pa.large_string())
    values = pa.array(["a"], extension.storage_type)
    field = pa.field("v", values.type)
    plain = pa.StructArray.from_arrays([values], fields=[field])
    annotated = pa.StructArray.from_arrays(
        [values], fields=[field.with_metadata(_annotation(extension))]
    )
    x = pa.chunked_array([plain, annotated])
    table = pa.Table.from_arrays(
        [pa.array([b"1", b"2"], pa.large_binary()), x], names=["id", "x"]
    )
    assert not table.schema.field("x").type.field("v").metadata
    assert x.chunk(1).type.field("v").metadata

    db = DeltaTableDatabase(tmp_path)
    with pytest.raises(UnstorableColumnError, match=r"x\.v"):
        db.add_records(PATH, table, record_id_column="id")
    db.flush()
    assert db.get_all_records(PATH) is None


def _struct_chunks_with_later_annotation() -> tuple[pa.StructArray, pa.StructArray]:
    """Return two struct chunks with a logical annotation only in the second."""
    inner = _Unit("itl1372.chunk_inner", pa.large_string())
    field = pa.field("path", inner.storage_type)
    plain = pa.StructArray.from_arrays(
        [pa.array(["ordinary"], field.type)], fields=[field]
    )
    annotated = pa.StructArray.from_arrays(
        [pa.array(["marked"], field.type)],
        fields=[field.with_metadata(_annotation(inner))],
    )
    return plain, annotated


@pytest.mark.parametrize("wrapped", [False, True], ids=["direct", "wrapped"])
@pytest.mark.parametrize(
    "registered", [False, True], ids=["unregistered", "registered"]
)
@pytest.mark.parametrize("append", [False, True], ids=["create", "append"])
def test_delta_refuses_later_chunk_annotation_in_extension_storage(
    tmp_path: Path, wrapped: bool, registered: bool, append: bool
) -> None:
    """Arrow type equality hides child metadata in a later extension chunk."""
    plain, annotated = _struct_chunks_with_later_annotation()
    outer = _Unit("itl1372.chunk_outer", plain.type)
    clean = outer.wrap_array(plain)
    marked = _Unit(outer.extension_name, annotated.type).wrap_array(annotated)
    chunks = pa.chunked_array([clean, marked])
    records = pa.Table.from_arrays(
        [pa.array([b"1", b"2"], pa.large_binary()), chunks], names=["id", "measurement"]
    )
    schema_type = records.schema.field("measurement").type
    later_type = records.column("measurement").chunk(1).type
    assert isinstance(schema_type, pa.BaseExtensionType)
    assert isinstance(later_type, pa.BaseExtensionType)
    assert schema_type.storage_type.field(0).metadata is None
    metadata = later_type.storage_type.field(0).metadata or {}
    assert metadata[NAME] == b"itl1372.chunk_inner"
    assert later_type.equals(schema_type, check_metadata=True)

    with _register(outer, registered):
        delta = DeltaTableDatabase(tmp_path)
        db = (
            ExtensionAwareDatabase(
                delta,
                UniversalTypeConverter(logical_type_registry=LogicalTypeRegistry()),
            )
            if wrapped
            else delta
        )
        table_dir = tmp_path / PATH[0]
        version = None
        if append:
            db.add_record(
                PATH, b"existing", pa.table({"measurement": clean}), flush=True
            )
            version = DeltaTable(str(table_dir)).version()
        with pytest.raises(UnstorableColumnError, match=r"measurement<storage>\.path"):
            db.add_records(PATH, records, record_id_column="id")
        db.flush()
        reopened = DeltaTableDatabase(tmp_path).get_all_records(PATH)
        if version is not None:
            assert DeltaTable(str(table_dir)).version() == version
            assert reopened is not None
            assert reopened.num_rows == 1
        else:
            assert not (table_dir / "_delta_log").exists()
            assert reopened is None


def test_delta_refuses_later_chunk_annotation_in_dictionary_values(
    tmp_path: Path,
) -> None:
    """The nested annotation is refused before Delta attempts to store the dictionary."""
    plain, annotated = _struct_chunks_with_later_annotation()
    indices = pa.array([0], pa.int8())
    chunks = pa.chunked_array(
        [pa.DictionaryArray.from_arrays(indices, chunk) for chunk in (plain, annotated)]
    )
    records = pa.Table.from_arrays(
        [pa.array([b"1", b"2"], pa.large_binary()), chunks], names=["id", "measurement"]
    )
    schema_type = records.schema.field("measurement").type
    later_type = records.column("measurement").chunk(1).type
    assert isinstance(schema_type, pa.DictionaryType)
    assert isinstance(later_type, pa.DictionaryType)
    assert schema_type.value_type.field(0).metadata is None
    metadata = later_type.value_type.field(0).metadata or {}
    assert metadata[NAME] == b"itl1372.chunk_inner"
    assert later_type.equals(schema_type, check_metadata=True)

    db = DeltaTableDatabase(tmp_path)
    with pytest.raises(
        UnstorableColumnError, match=r"measurement<dictionary values>\.path"
    ):
        db.add_records(PATH, records, record_id_column="id", flush=True)
    db.flush()
    assert not (tmp_path / PATH[0] / "_delta_log").exists()


@pytest.mark.parametrize("nested", [False, True], ids=["top-level", "nested"])
@pytest.mark.parametrize(
    "key", [b"ARROW:extension:review-marker", b"review-marker"], ids=["prefix", "plain"]
)
def test_delta_preserves_ordinary_field_metadata(
    tmp_path: Path, nested: bool, key: bytes
) -> None:
    """Creation and identical appends preserve ordinary field metadata verbatim."""
    values = pa.array(["text"], pa.large_string())
    field = pa.field("v", values.type, metadata={key: b"keep"})
    if nested:
        table = pa.table(
            {"measure": pa.StructArray.from_arrays([values], fields=[field])}
        )
    else:
        table = pa.Table.from_arrays(
            [values], schema=pa.schema([field.with_name("measure")])
        )

    for count in (1, 2):
        DeltaTableDatabase(tmp_path).add_record(
            PATH, str(count).encode(), table, flush=True
        )
        read = DeltaTableDatabase(tmp_path).get_all_records(PATH)
        assert read is not None
        assert read.select(["measure"]).equals(
            pa.concat_tables([table] * count), check_metadata=True
        )


def test_delta_refuses_nested_extension_metadata_without_a_name(tmp_path: Path) -> None:
    """The blanket rule covers incomplete as well as complete extension annotations."""
    values = pa.array(["a"], pa.large_string())
    field = pa.field("v", values.type, metadata={META: b"{}"})
    column = pa.StructArray.from_arrays([values], fields=[field])
    db = DeltaTableDatabase(tmp_path)
    with pytest.raises(UnstorableColumnError, match=r"x\.v"):
        db.add_record(PATH, b"1", pa.table({"x": column}))
    db.flush()
    assert db.get_all_records(PATH) is None
