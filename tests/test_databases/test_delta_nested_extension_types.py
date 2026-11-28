"""Delta keeps or refuses nested Arrow extension types before queuing (ITL-1372)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod.databases import (
    DeltaTableDatabase,
    ExtensionAwareDatabase,
    InMemoryArrowDatabase,
)
from orcapod.errors import UnstorableColumnError
from orcapod.logical_types.registry import LogicalTypeRegistry
from orcapod.semantic_types.universal_converter import UniversalTypeConverter

PATH = ("t",)


class _Unit(pa.ExtensionType):
    """A logical type over large strings."""

    def __init__(
        self, name: str = "itl1372.unit", storage_type: pa.DataType | None = None
    ) -> None:
        super().__init__(
            pa.large_string() if storage_type is None else storage_type, name
        )

    def __arrow_ext_serialize__(self) -> bytes:
        return b"{}"

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: bytes) -> _Unit:
        return cls()


@pytest.fixture
def unit() -> Any:
    """An Arrow extension type registered for the lifetime of one test."""
    extension = _Unit()
    pa.register_extension_type(cast(Any, extension))
    try:
        yield extension
    finally:
        pa.unregister_extension_type(extension.extension_name)


def _nested(kind: str, values: Any) -> Any:
    """Build one occurrence of `values` per row, nested in the given container."""
    offsets = range(len(values) + 1)
    if kind == "list":
        return pa.ListArray.from_arrays(pa.array(offsets, pa.int32()), values)
    if kind == "large list":
        return pa.LargeListArray.from_arrays(pa.array(offsets, pa.int64()), values)
    if kind == "fixed-size list":
        return pa.FixedSizeListArray.from_arrays(values, 1)
    if kind == "list of structs":
        return _nested("large list", _nested("struct", values))
    if kind == "struct of lists":
        return _nested("struct", _nested("large list", values))
    if kind == "struct of dictionaries":
        return _nested("struct", _nested("dictionary", values))
    if kind == "dictionary of structs":
        return _nested("dictionary", _nested("struct", values))
    if kind == "struct of structs":
        return _nested("struct", _nested("struct", values))
    if kind == "struct":
        return pa.StructArray.from_arrays([values], names=["v"])
    if kind == "dictionary":
        return pa.DictionaryArray.from_arrays(
            pa.array(range(len(values)), pa.int32()), values
        )
    ordinary = pa.array([f"k{i}" for i in range(len(values))], pa.large_string())
    from_arrays: Any = pa.MapArray.from_arrays
    if kind == "map key":
        return from_arrays(pa.array(offsets, pa.int32()), values, ordinary)
    assert kind == "map value"
    return from_arrays(pa.array(offsets, pa.int32()), ordinary, values)


REFUSED = [
    ("list", "list item"),
    ("large list", "list item"),
    ("fixed-size list", "list item"),
    ("map key", "map key"),
    ("map value", "map value"),
    ("dictionary", "dictionary values"),
    ("list of structs", "list item"),
    ("struct of lists", "list item"),
    ("struct of dictionaries", "dictionary values"),
    ("dictionary of structs", "dictionary values"),
]


@pytest.mark.parametrize(("kind", "why"), REFUSED)
@pytest.mark.parametrize("wrap", [False, True], ids=["direct", "wrapper"])
def test_nested_extension_is_refused_before_queueing(
    tmp_path: Path, unit: Any, kind: str, why: str, wrap: bool
) -> None:
    """Both write entry points reject the same column before it reaches the queue."""
    delta = DeltaTableDatabase(tmp_path)
    db = (
        ExtensionAwareDatabase(
            delta, UniversalTypeConverter(logical_type_registry=LogicalTypeRegistry())
        )
        if wrap
        else delta
    )
    values = unit.wrap_array(pa.array(["a"], pa.large_string()))
    table = pa.table({"measurement": _nested(kind, values)})
    rows = table.append_column("id", pa.array([b"1"], pa.large_binary()))
    with pytest.raises(UnstorableColumnError, match=why) as first:
        db.add_records(PATH, rows, record_id_column="id")
    db.flush()
    assert db.get_all_records(PATH) is None
    assert not (tmp_path / "t" / "_delta_log").exists()

    with pytest.raises(UnstorableColumnError, match=why) as second:
        db.add_record(PATH, b"2", table)
    assert str(first.value) == str(second.value)
    assert first.value.column == "measurement"
    assert first.value.arrow_type == table.schema.field("measurement").type
    message = str(first.value)
    assert "storage form" in message
    assert "InMemoryArrowDatabase" in message
    assert "docs/reference/database-types.md" in message
    db.flush()
    assert db.get_all_records(PATH) is None
    assert not (tmp_path / "t" / "_delta_log").exists()


@pytest.mark.parametrize("order", [("samples", "units"), ("units", "samples")])
def test_delta_names_both_unstorable_columns(
    tmp_path: Path, unit: Any, order: tuple[str, str]
) -> None:
    """All columns with unsupported extensions are named in either input order."""
    values = unit.wrap_array(pa.array(["a"], pa.large_string()))
    columns = {
        "samples": _nested("list", values),
        "units": _nested("map value", values),
    }
    table = pa.table({name: columns[name] for name in order})
    rows = table.append_column("id", pa.array([b"1"], pa.large_binary()))
    db = DeltaTableDatabase(tmp_path)
    with pytest.raises(UnstorableColumnError) as refused:
        db.add_records(PATH, rows, record_id_column="id")
    message = str(refused.value)
    assert "'samples'" in message and "list item" in message
    assert "'units'" in message and "map value" in message
    db.flush()
    assert db.get_all_records(PATH) is None


@pytest.mark.parametrize("kind", [*dict(REFUSED), "struct", "struct of structs"])
def test_memory_keeps_nested_extensions(unit: Any, kind: str) -> None:
    """In-memory preserves both writes, including the extension type."""
    values = unit.wrap_array(pa.array(["a"], pa.large_string()))
    table = pa.table({"x": _nested(kind, values)})
    db = InMemoryArrowDatabase()
    for rid in (b"1", b"2"):
        db.add_record(PATH, rid, table)
        db.flush()
    stored = db.get_all_records(PATH)
    assert stored is not None
    assert stored.select(["x"]).equals(
        pa.concat_tables([table, table]), check_metadata=True
    )


@pytest.mark.parametrize("kind", ["struct", "struct of structs"])
def test_delta_keeps_extension_in_struct(tmp_path: Path, unit: Any, kind: str) -> None:
    """Struct children keep their type and an identical second append succeeds."""
    values = unit.wrap_array(pa.array(["a"], pa.large_string()))
    table = pa.table({"x": _nested(kind, values)})
    db = DeltaTableDatabase(tmp_path)
    for rid in (b"1", b"2"):
        db.add_record(PATH, rid, table)
        db.flush()
        stored = DeltaTableDatabase(tmp_path).get_all_records(PATH)
        assert stored is not None
        expected = table if rid == b"1" else pa.concat_tables([table, table])
        assert stored.select(["x"]).equals(expected, check_metadata=True)

    code = """
import sys

import pyarrow as pa

from orcapod.databases import DeltaTableDatabase

table = DeltaTableDatabase(sys.argv[1]).get_all_records(("t",))
child = table.schema.field("x").type.field("v")
if sys.argv[2] == "struct of structs":
    child = child.type.field("v")
assert child.type == pa.large_string(), child
assert child.metadata == {
    b"ARROW:extension:name": b"itl1372.unit",
    b"ARROW:extension:metadata": b"{}",
}, child.metadata
"""
    subprocess.run([sys.executable, "-c", code, str(tmp_path), kind], check=True)


def test_delta_keeps_ordinary_struct_child_metadata_on_append(tmp_path: Path) -> None:
    """An identical second write preserves ordinary metadata on a struct child."""
    field = pa.field("v", pa.large_string(), metadata={b"unit": b"m"})
    array = pa.StructArray.from_arrays([pa.array(["a"], field.type)], fields=[field])
    table = pa.table({"x": array})
    db = DeltaTableDatabase(tmp_path)
    for rid in (b"1", b"2"):
        db.add_record(PATH, rid, table)
        db.flush()
    stored = DeltaTableDatabase(tmp_path).get_all_records(PATH)
    assert stored is not None
    assert stored.select(["x"]).equals(
        pa.concat_tables([table, table]), check_metadata=True
    )


@pytest.mark.parametrize(
    "registered", [False, True], ids=["unregistered", "registered"]
)
@pytest.mark.parametrize(
    "kind", ["list", "map value", "list of structs", "map of structs"]
)
def test_delta_refuses_annotated_storage_under_list_or_map(
    tmp_path: Path, unit: Any, kind: str, registered: bool
) -> None:
    """The refusal is independent of registry state and applies through a struct."""
    metadata = {
        b"ARROW:extension:name": b"itl1372.unit" if registered else b"itl1372.missing",
        b"ARROW:extension:metadata": b"{}",
    }
    field = pa.field("v", pa.large_string(), metadata=metadata)
    values: Any = pa.array(["a"], field.type)
    if kind.endswith("of structs"):
        values = pa.StructArray.from_arrays([values], fields=[field])
        field = pa.field("v", values.type)
    if kind.startswith("list"):
        array = pa.LargeListArray.from_arrays(
            pa.array([0, 1], pa.int64()),
            values,
            type=pa.large_list(field.with_name("item")),
        )
    else:
        from_arrays: Any = pa.MapArray.from_arrays
        map_type = pa.map_(pa.large_string(), field.with_name("value"))  # pyright: ignore[reportArgumentType]
        array = from_arrays(
            pa.array([0, 1], pa.int32()),
            pa.array(["key"], pa.large_string()),
            values,
            type=map_type,
        )
    db = DeltaTableDatabase(tmp_path)
    errors = []
    for rid in (b"1", b"2"):
        with pytest.raises(UnstorableColumnError, match="annotated storage") as refused:
            db.add_record(PATH, rid, pa.table({"measurement": array}))
        errors.append(str(refused.value))
        db.flush()
        assert db.get_all_records(PATH) is None
    assert errors[0] == errors[1]


@pytest.mark.parametrize("slot", ["map key", "map value"])
def test_delta_keeps_extension_in_struct_under_map(
    tmp_path: Path, unit: Any, slot: str
) -> None:
    """A map's struct child keeps its type and accepts identical appends."""
    values = unit.wrap_array(pa.array(["a"], pa.large_string()))
    table = pa.table({"x": _nested(slot, _nested("struct", values))})
    for rid in (b"1", b"2"):
        db = DeltaTableDatabase(tmp_path)
        db.add_record(PATH, rid, table)
        db.flush()
        stored = DeltaTableDatabase(tmp_path).get_all_records(PATH)
        assert stored is not None
        expected = table if rid == b"1" else pa.concat_tables([table, table])
        assert stored.select(["x"]).equals(expected, check_metadata=True)


@pytest.mark.parametrize("container", ["struct", "map key", "map value"])
@pytest.mark.parametrize("sibling", ["uint16", "high uint16", "list"])
def test_delta_refuses_extension_struct_with_lossy_sibling(
    tmp_path: Path, unit: Any, container: str, sibling: str
) -> None:
    """An extension disables type recording for its entire column."""
    extension = unit.wrap_array(pa.array(["a"], pa.large_string()))
    other = (
        pa.array([["s"]], pa.large_list(pa.large_string()))
        if sibling == "list"
        else pa.array([65535 if sibling == "high uint16" else 1], pa.uint16())
    )
    struct = pa.StructArray.from_arrays([extension, other], names=["v", "sibling"])
    values = struct if container == "struct" else _nested(container, struct)
    db = DeltaTableDatabase(tmp_path)
    table = pa.table({"measurement": values})
    errors = []
    for rid in (b"1", b"2"):
        with pytest.raises(UnstorableColumnError, match="type recording") as refused:
            db.add_record(PATH, rid, table)
        assert refused.value.column == "measurement"
        errors.append(str(refused.value))
        db.flush()
        assert DeltaTableDatabase(tmp_path).get_all_records(PATH) is None
        assert not (tmp_path / "t" / "_delta_log").exists()
    assert errors[0] == errors[1]


@pytest.mark.parametrize(
    "registered", [False, True], ids=["unregistered", "registered"]
)
@pytest.mark.parametrize("form", ["extension", "annotated"])
def test_delta_refuses_struct_extension_with_field_metadata(
    tmp_path: Path, unit: Any, registered: bool, form: str
) -> None:
    """Both writes refuse even if Arrow has no registered reader for the identifier."""
    if not registered:
        unit = _Unit("itl1372.unregistered")
    storage = pa.array(["a"], pa.large_string())
    if form == "annotated":
        metadata = {
            b"ARROW:extension:name": unit.extension_name.encode(),
            b"ARROW:extension:metadata": b"{}",
        }
        values = storage
    else:
        metadata = {b"units": b"metres"}
        values = unit.wrap_array(storage)
    field = pa.field("v", values.type, metadata=metadata)
    table = pa.table(
        {"measurement": pa.StructArray.from_arrays([values], fields=[field])}
    )
    db = DeltaTableDatabase(tmp_path)
    for rid in (b"1", b"2"):
        with pytest.raises(
            UnstorableColumnError, match="annotated storage|field metadata"
        ) as refused:
            db.add_record(PATH, rid, table)
        assert refused.value.column == "measurement"
        db.flush()
        assert DeltaTableDatabase(tmp_path).get_all_records(PATH) is None


@pytest.mark.parametrize(
    ("arrow_type", "value"),
    [
        (pa.bool_(), True),
        (pa.int8(), -8),
        (pa.int16(), -16),
        (pa.int32(), -32),
        (pa.int64(), -64),
        (pa.float32(), 1.25),
        (pa.float64(), -1.25),
        (pa.string(), "s"),
        (pa.large_string(), "s"),
        (pa.binary(), b"s"),
        (pa.large_binary(), b"s"),
        (pa.date32(), 1),
        (pa.struct([("n", pa.int64())]), {"n": 1}),
        (pa.map_(pa.large_string(), pa.int64()), [("n", 1)]),
    ],
    ids=str,
)
def test_delta_keeps_proven_sibling_layouts(
    tmp_path: Path, unit: Any, arrow_type: pa.DataType, value: Any
) -> None:
    """A supported sibling keeps its value and type through both writes."""
    from orcapod.utils.arrow_utils import normalize_to_large_types

    extension = unit.wrap_array(pa.array(["a"], pa.large_string()))
    original = pa.array([value], arrow_type)
    normalized = pa.array([value], normalize_to_large_types(arrow_type))
    tables = [
        pa.table(
            {
                "x": _nested(
                    "map value",
                    pa.StructArray.from_arrays(
                        [extension, sibling], names=["v", "sibling"]
                    ),
                )
            }
        )
        for sibling in (original, normalized)
    ]
    table, expected = tables
    for count in (1, 2):
        db = DeltaTableDatabase(tmp_path)
        db.add_record(PATH, str(count).encode(), table, flush=True)
        actual = DeltaTableDatabase(tmp_path).get_all_records(PATH)
        assert actual is not None
        assert actual.select(["x"]).equals(
            pa.concat_tables([expected] * count), check_metadata=True
        )


@pytest.mark.parametrize(
    ("storage_type", "value"), [(pa.string(), "a"), (pa.binary(), b"a")], ids=str
)
def test_delta_refuses_nested_extension_storage_that_would_be_widened(
    tmp_path: Path, storage_type: pa.DataType, value: Any
) -> None:
    """Delta normalizes small text and binary when comparing an identical append."""
    extension = _Unit("itl1372.small-storage", storage_type)
    values = extension.wrap_array(pa.array([value], storage_type))
    table = pa.table({"measurement": _nested("map value", _nested("struct", values))})
    db = DeltaTableDatabase(tmp_path)
    for rid in (b"1", b"2"):
        with pytest.raises(UnstorableColumnError, match="type recording") as refused:
            db.add_record(PATH, rid, table)
        assert refused.value.column == "measurement"
    db.flush()
    assert db.get_all_records(PATH) is None
