"""An append of another logical type identifier is refused (ITL-1371).

A table holds one logical type per column and per nested field. A column appended
with the same storage type but another logical type identifier (another extension
name, the same name with other extension metadata, or a logical type where the held
column has none, and the reverse) would read back as the held type, so every database
refuses it before anything is buffered, naming the column and both identifiers. The
identifier arrives in two forms: an Arrow extension type, or its storage type
annotated with ``ARROW:extension:name`` / ``ARROW:extension:metadata`` (the form a
stored column reads back in, and the one ``ExtensionAwareDatabase`` passes on).
"""

from __future__ import annotations

import contextlib
import itertools
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.databases import (
    DeltaTableDatabase,
    ExtensionAwareDatabase,
    InMemoryArrowDatabase,
)
from orcapod.databases.connector_arrow_database import ConnectorArrowDatabase
from orcapod.databases.spiraldb_connector import SpiralDBConnector
from orcapod.databases.utils import (
    field_logical_identifier,
    first_logical_identity_change,
    logical_identity_change,
)
from orcapod.errors import InputValidationError
from orcapod.logical_types.identifier import LogicalTypeIdentifier
from orcapod.logical_types.registry import LogicalTypeRegistry
from orcapod.semantic_types.universal_converter import UniversalTypeConverter
from tests.test_databases import fake_pyspiral

NAME = b"ARROW:extension:name"
META = b"ARROW:extension:metadata"
PATH = ("t",)


def _extension_class(name: str) -> Any:
    """An extension type named ``name`` over ``large_string``, any metadata bytes."""

    class _Ext(pa.ExtensionType):
        def __init__(self, meta: bytes) -> None:
            self._meta = meta
            super().__init__(pa.large_string(), name)

        def __arrow_ext_serialize__(self) -> bytes:
            return self._meta

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: bytes) -> Any:
            return cls(serialized)

    return _Ext


EXTENSIONS: dict[str, Any] = {
    name: _extension_class(name) for name in ("itl1371.a", "itl1371.b")
}

HELD = ("itl1371.a", b"m")
CHANGES: dict[str, tuple[str, bytes] | None] = {
    "other name": ("itl1371.b", b"m"),
    "other metadata": ("itl1371.a", b"n"),
    "no logical type": None,
}


def _text(identifier: tuple[str, bytes]) -> str:
    return LogicalTypeIdentifier(*identifier).text


def _leaf(
    identifier: tuple[str, bytes] | None, form: str, values: list[str]
) -> tuple["pa.Field[Any]", Any]:
    storage = pa.array(values, pa.large_string())
    if identifier is None:
        return pa.field("v", pa.large_string()), storage
    if form == "extension":
        ext = EXTENSIONS[identifier[0]](identifier[1])
        return pa.field("v", ext), pa.ExtensionArray.from_storage(ext, storage)
    metadata = {NAME: identifier[0].encode(), META: identifier[1]}
    return pa.field("v", pa.large_string(), metadata=metadata), storage


def _column(
    identifier: tuple[str, bytes] | None, form: str, depth: str, values: list[str]
) -> tuple["pa.Field[Any]", Any]:
    field, array = _leaf(identifier, form, values)
    n = len(values)
    if depth == "top":
        return field.with_name("x"), array
    if depth == "struct":
        struct = pa.StructArray.from_arrays([array], fields=[field])
        return pa.field("x", struct.type), struct
    if depth == "list":
        list_type = pa.large_list(field.with_name("item"))
        offsets = pa.array(range(n + 1), pa.int64())
        return pa.field("x", list_type), pa.LargeListArray.from_arrays(
            offsets, array, type=list_type
        )
    assert depth == "map value"
    # The stubs type pa.map_'s arguments as types and from_arrays' offsets as int64;
    # pyarrow takes key and item fields, and int32 offsets for a map.
    map_type: Any = pa.map_(
        pa.field("key", pa.large_string(), nullable=False),  # pyright: ignore[reportArgumentType]
        field.with_name("value"),  # pyright: ignore[reportArgumentType]
    )
    keys = pa.array([f"k{i}" for i in range(n)], pa.large_string())
    offsets: Any = pa.array(range(n + 1), pa.int32())
    from_arrays: Any = pa.MapArray.from_arrays
    return pa.field("x", map_type), from_arrays(offsets, keys, array, type=map_type)


def _rows(
    identifier: tuple[str, bytes] | None, form: str, depth: str, ids: list[bytes]
) -> pa.Table:
    """Rows ``ids`` with a data column ``a`` before and ``x`` after the record id."""
    field, array = _column(identifier, form, depth, [i.decode() for i in ids])
    return pa.Table.from_arrays(
        [
            pa.array([len(i) for i in ids], pa.int64()),
            pa.array(ids, pa.large_binary()),
            array,
        ],
        schema=pa.schema(
            [pa.field("a", pa.int64()), pa.field("id", pa.large_binary()), field]
        ),
    )


# ---------------------------------------------------------------------------
# Databases
# ---------------------------------------------------------------------------


class _Store:
    """One database of a kind over one root: ``db`` (the same instance, so a pending
    batch stays pending) until ``reopen`` opens it again over the same files."""

    def __init__(self, open_db: Any) -> None:
        self._open = open_db
        self.db = open_db()

    def reopen(self) -> None:
        self.db = self._open()

    def stored(self) -> pa.Table:
        """Flush and read every row back."""
        self.db.flush()
        table = self.db.get_all_records(PATH, record_id_column="id")
        assert table is not None
        return table


@contextlib.contextmanager
def _store(kind: str, root: Path) -> Iterator[_Store]:
    """A ``_Store`` of database ``kind`` over ``root``."""
    if kind == "in-memory":
        yield _Store(InMemoryArrowDatabase)
    elif kind == "Delta":
        yield _Store(lambda: DeltaTableDatabase(root / "delta"))
    else:
        assert kind == "SpiralDB"
        with fake_pyspiral.install(root / "spiral"):
            fake_pyspiral.FakeSpiral().project("p")
            yield _Store(lambda: ConnectorArrowDatabase(SpiralDBConnector("p")))


def _through(db: Any, via: str) -> Any:
    if via == "direct":
        return db
    converter = UniversalTypeConverter(logical_type_registry=LogicalTypeRegistry())
    return ExtensionAwareDatabase(db, converter)


def _supported(kind: str, form: str, depth: str) -> bool:
    """Whether ``kind`` stores the column at all (the type matrix): SpiralDB keeps an
    annotated top-level column only here; it refuses extension types, and the fake
    refuses these nested forms."""
    if kind == "SpiralDB":
        return form == "annotated" and depth == "top"
    return True


def _loses_nested_extension(kind: str, state: str, form: str, depth: str) -> bool:
    """Delta writes a list item or map value of an extension type as plain storage
    (ITL-1372): a committed table no longer holds the identifier, so only refusals
    are asserted there. (A struct child keeps its annotation in the Delta log.)"""
    return (
        kind == "Delta"
        and form == "extension"
        and depth in ("list", "map value")
        and state != "pending"
    )


KINDS = ("in-memory", "Delta", "SpiralDB")
STATES = ("pending", "committed", "reopened")
FORMS = ("extension", "annotated")
DEPTHS = ("top", "struct", "list", "map value")

BASE = [
    (kind, state, form, depth)
    for kind, state, form, depth in itertools.product(KINDS, STATES, FORMS, DEPTHS)
    if _supported(kind, form, depth)
    and not (kind == "in-memory" and state == "reopened")
]

REFUSED_CASES = [
    pytest.param(*case, change, id=f"{'-'.join(case)}-{change}")
    for case in BASE
    for change in CHANGES
    # A plain column over a Delta column that lost its nested type (ITL-1372) is not
    # another identifier: the held column is plain too.
    if not (_loses_nested_extension(*case) and CHANGES[change] is None)
]

SAME_CASES = [
    pytest.param(*case, id="-".join(case))
    for case in BASE
    if not _loses_nested_extension(*case)  # pinned by the ITL-1372 test below
]


def _write_held(store: _Store, via: str, state: str, form: str, depth: str) -> None:
    held = _rows(HELD, form, depth, [b"1", b"22"])
    _through(store.db, via).add_records(
        PATH, held, record_id_column="id", flush=state != "pending"
    )
    if state == "reopened":
        store.reopen()


def _assert_only_held_rows(store: _Store) -> None:
    """The two held rows, as written, and nothing of the refused append."""
    stored = store.stored().sort_by("id")
    assert stored.column("id").to_pylist() == [b"1", b"22"]
    assert stored.column("a").to_pylist() == [1, 2]
    x = stored.column("x")
    values = x.combine_chunks()
    if isinstance(values, pa.ExtensionArray):
        values = values.storage
    assert len(values) == 2


@pytest.mark.parametrize("via", ["direct", "wrapper"])
@pytest.mark.parametrize(("kind", "state", "form", "depth", "change"), REFUSED_CASES)
def test_another_identifier_is_refused(
    tmp_path: Path, kind: str, state: str, form: str, depth: str, change: str, via: str
) -> None:
    with _store(kind, tmp_path) as store:
        _write_held(store, via, state, form, depth)
        incoming = _rows(CHANGES[change], form, depth, [b"333", b"4444"])
        with pytest.raises(InputValidationError) as refused:
            _through(store.db, via).add_records(PATH, incoming, record_id_column="id")
        message = str(refused.value)
        assert "column 'x'" in message
        assert "othing was written" in message
        lost = _loses_nested_extension(kind, state, form, depth)
        assert (repr(_text(HELD)) if not lost else "no logical type") in message
        identifier = CHANGES[change]
        assert (
            repr(_text(identifier)) if identifier is not None else "no logical type"
        ) in message
        # Nothing was buffered, and the held rows are unchanged.
        _assert_only_held_rows(store)


@pytest.mark.parametrize("via", ["direct", "wrapper"])
@pytest.mark.parametrize(("kind", "state", "form", "depth"), SAME_CASES)
def test_same_identifier_is_accepted(
    tmp_path: Path, kind: str, state: str, form: str, depth: str, via: str
) -> None:
    with _store(kind, tmp_path) as store:
        _write_held(store, via, state, form, depth)
        same = _rows(HELD, form, depth, [b"333"])
        _through(store.db, via).add_records(PATH, same, record_id_column="id")
        assert store.stored().num_rows == 3


@pytest.mark.parametrize("via", ["direct", "wrapper"])
@pytest.mark.parametrize("form", FORMS)
@pytest.mark.parametrize("depth", DEPTHS)
@pytest.mark.parametrize("state", ["pending", "committed"])
@pytest.mark.parametrize("kind", ["in-memory", "Delta"])
def test_a_logical_type_over_a_plain_column_is_refused(
    tmp_path: Path, kind: str, state: str, form: str, depth: str, via: str
) -> None:
    with _store(kind, tmp_path) as store:
        _through(store.db, via).add_records(
            PATH,
            _rows(None, form, depth, [b"1"]),
            record_id_column="id",
            flush=state == "committed",
        )
        with pytest.raises(
            InputValidationError, match="the stored column's no logical"
        ):
            _through(store.db, via).add_records(
                PATH, _rows(HELD, form, depth, [b"22"]), record_id_column="id"
            )
        assert store.stored().column("id").to_pylist() == [b"1"]


@pytest.mark.parametrize("depth", ["list", "map value"])
def test_delta_nested_extension_second_append_is_refused(
    tmp_path: Path, depth: str
) -> None:
    """Pins a consequence of ITL-1372; revisit there.

    Delta writes a list item or map value of an extension type as plain storage on
    the first write: the first write is accepted and reads back plain. On main a
    second append of the same type to the committed table was accepted and stored as
    plain storage too (the map; the list was refused for its item's name); it is now
    refused, because the held column has no logical type. A same-type append to the
    pending batch is still accepted.
    """
    db = DeltaTableDatabase(tmp_path / "delta")
    db.add_records(PATH, _rows(HELD, "extension", depth, [b"1"]), record_id_column="id")
    db.add_records(
        PATH, _rows(HELD, "extension", depth, [b"22"]), record_id_column="id"
    )
    db.flush()
    stored = db.get_all_records(PATH, record_id_column="id")
    assert stored is not None and stored.num_rows == 2
    leaf = stored.schema.field("x").type
    leaf_field = leaf.value_field if depth == "list" else leaf.item_field
    assert field_logical_identifier(leaf_field) is None  # read back plain
    with pytest.raises(InputValidationError) as refused:
        db.add_records(
            PATH, _rows(HELD, "extension", depth, [b"333"]), record_id_column="id"
        )
    assert f"of logical type {_text(HELD)!r}, the stored column's no logical type" in (
        str(refused.value)
    )
    db.flush()
    after = db.get_all_records(PATH, record_id_column="id")
    assert after is not None and after.num_rows == 2


def test_the_reproducer_through_the_wrapper(tmp_path: Path) -> None:
    """ITL-1371 as reported: annotated, aligned columns, through the wrapper over Delta."""
    inner = DeltaTableDatabase(tmp_path / "delta")
    wrapper = ExtensionAwareDatabase(
        inner, UniversalTypeConverter(logical_type_registry=LogicalTypeRegistry())
    )
    wrapper.add_records(
        PATH, _rows(HELD, "annotated", "top", [b"1"]), record_id_column="id", flush=True
    )
    other = ("itl1371.b", b"m")
    with pytest.raises(InputValidationError) as refused:
        wrapper.add_records(
            PATH,
            _rows(other, "annotated", "top", [b"22"]),
            record_id_column="id",
            flush=True,
        )
    assert repr(_text(other)) in str(refused.value)
    assert repr(_text(HELD)) in str(refused.value)
    stored = inner.get_all_records(PATH, record_id_column="id")
    assert stored is not None and stored.column("id").to_pylist() == [b"1"]


# ---------------------------------------------------------------------------
# The rule itself
# ---------------------------------------------------------------------------


def test_identifier_is_read_from_either_form() -> None:
    ext = EXTENSIONS["itl1371.a"](b"m")
    annotated = pa.field(
        "x", pa.large_string(), metadata={NAME: b"itl1371.a", META: b"m"}
    )
    assert field_logical_identifier(pa.field("x", ext)) == LogicalTypeIdentifier(
        "itl1371.a", b"m"
    )
    assert field_logical_identifier(annotated) == LogicalTypeIdentifier(
        "itl1371.a", b"m"
    )
    assert field_logical_identifier(pa.field("x", pa.large_string())) is None
    # The two forms of one identifier are one identifier.
    assert logical_identity_change(annotated, pa.field("x", ext)) is None


def test_pyarrow_equality_does_not_decide() -> None:
    """pyarrow's equality of two Python extension types ignores their metadata."""
    m, n = EXTENSIONS["itl1371.a"](b"m"), EXTENSIONS["itl1371.a"](b"n")
    assert m == n  # what the rule must not rely on
    assert logical_identity_change(pa.field("x", m), pa.field("x", n)) is not None


@pytest.mark.parametrize(
    ("depth", "path"),
    [("top", "x"), ("struct", "x.v"), ("list", "x[]"), ("map value", "x.item")],
)
def test_reason_names_the_path(depth: str, path: str) -> None:
    held = _rows(HELD, "annotated", depth, [b"1"]).schema
    incoming = _rows(("itl1371.b", b"m"), "extension", depth, [b"1"]).schema
    found = first_logical_identity_change([held], incoming)
    assert found is not None
    assert found[0] == "x"
    assert found[1].startswith(f"{path} is of logical type")


def test_columns_are_matched_by_name_in_any_order() -> None:
    held = _rows(HELD, "annotated", "top", [b"1"]).schema
    incoming = _rows(("itl1371.b", b"m"), "annotated", "top", [b"1"]).schema
    reordered = pa.schema([incoming.field(n) for n in ("x", "id", "a")])
    assert first_logical_identity_change([held], incoming) == (
        first_logical_identity_change([held], reordered)
    )
    same = pa.schema([held.field(n) for n in ("x", "a", "id")])
    assert first_logical_identity_change([held], same) is None


def test_map_key_identifier_is_compared() -> None:
    def map_field(identifier: tuple[str, bytes]) -> "pa.Field[Any]":
        key = pa.field(
            "key",
            pa.large_string(),
            nullable=False,
            metadata={NAME: identifier[0].encode(), META: identifier[1]},
        )
        item = pa.field("value", pa.int64())
        return pa.field("x", pa.map_(key, item))  # pyright: ignore[reportArgumentType]  # pyarrow takes key/item fields

    assert logical_identity_change(map_field(HELD), map_field(HELD)) is None
    reason = logical_identity_change(map_field(HELD), map_field(("itl1371.a", b"n")))
    assert reason is not None and reason.startswith("x.key ")


def _struct_rows(form: str, unit: bytes, ids: list[bytes]) -> pa.Table:
    """``x: struct<v>``, ``v`` of logical type ``HELD`` with ``{unit: <unit>}`` beside
    its identifier."""
    field, array = _leaf(HELD, form, [i.decode() for i in ids])
    field = field.with_metadata({**dict(field.metadata or {}), b"unit": unit})
    struct = pa.StructArray.from_arrays([array], fields=[field])
    return pa.Table.from_arrays(
        [pa.array(ids, pa.large_binary()), struct],
        schema=pa.schema(
            [pa.field("id", pa.large_binary()), pa.field("x", struct.type)]
        ),
    )


@pytest.mark.parametrize(
    ("form", "state"),
    [
        ("annotated", "pending"),
        ("annotated", "committed"),
        ("annotated", "reopened"),
        # Committed, a struct child of an extension type with metadata of its own is
        # refused even for the same type, as on main: the held view drops the child's
        # own metadata (reported with ITL-1372).
        ("extension", "pending"),
    ],
)
def test_delta_other_nested_metadata_beside_an_identifier_is_still_refused(
    tmp_path: Path, form: str, state: str
) -> None:
    """Delta leaves the nested extension keys to the identity rule and compares the
    rest of a nested field's metadata as before (ITL-1334): a struct child of the
    same logical type with other non-extension metadata is refused."""
    with _store("Delta", tmp_path) as store:
        store.db.add_records(
            PATH,
            _struct_rows(form, b"m", [b"1"]),
            record_id_column="id",
            flush=state != "pending",
        )
        if state == "reopened":
            store.reopen()
        store.db.add_records(
            PATH, _struct_rows(form, b"m", [b"22"]), record_id_column="id"
        )
        with pytest.raises(InputValidationError, match="x.v has other field metadata"):
            store.db.add_records(
                PATH, _struct_rows(form, b"s", [b"333"]), record_id_column="id"
            )
        ids = store.stored().sort_by("id").column("id").to_pylist()
        assert ids == [b"1", b"22"]
