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
from typing import Any, cast

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
    first_logical_identity_change,
    metadata_bytes,
    identity_node,
    logical_identity_change,
)
from orcapod.errors import InputValidationError
from orcapod.logical_types.identifier import LogicalTypeIdentifier
from orcapod.logical_types.registry import make_arrow_extension_type
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


def _abstains(kind: str, state: str, depth: str) -> bool:
    """Whether the identity rule cannot know the held identifier, and so abstains:
    a Delta list item or map key or item, which the Delta log keeps no metadata for
    (ITL-1372). There the database's other checks decide, exactly as on main."""
    return kind == "Delta" and state != "pending" and depth in ("list", "map value")


def _main_accepts(kind: str, state: str, form: str, depth: str) -> bool:
    """Where the rule abstains, main accepts a map of extension values whatever the
    identifier (Delta wrote it as plain storage, ITL-1372; pinned below)."""
    return (
        _abstains(kind, state, depth) and form == "extension" and depth == "map value"
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
    if not _main_accepts(*case)
]

SAME_CASES = [
    pytest.param(*case, id="-".join(case))
    for case in BASE
    # Main refuses a second list of extension values for its item's name (ITL-1372).
    if not (_abstains(case[0], case[1], case[3]) and case[2:] == ("extension", "list"))
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
        if not _abstains(kind, state, depth):
            assert repr(_text(HELD)) in message
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


LOGICAL_OVER_PLAIN = [
    pytest.param(kind, state, form, depth, id=f"{kind}-{state}-{form}-{depth}")
    for kind, state, form, depth in itertools.product(
        ("in-memory", "Delta"), ("pending", "committed"), FORMS, DEPTHS
    )
    # The rule abstains at a stored Delta list or map item; main accepts extension
    # values there.
    if not (_abstains(kind, state, depth) and form == "extension")
]


@pytest.mark.parametrize("via", ["direct", "wrapper"])
@pytest.mark.parametrize(("kind", "state", "form", "depth"), LOGICAL_OVER_PLAIN)
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
        match = None if _abstains(kind, state, depth) else "stored column's no logical"
        with pytest.raises(InputValidationError, match=match):
            _through(store.db, via).add_records(
                PATH, _rows(HELD, form, depth, [b"22"]), record_id_column="id"
            )
        assert store.stored().column("id").to_pylist() == [b"1"]


@pytest.mark.parametrize("state", ["committed", "reopened"])
def test_delta_list_and_map_of_extension_values_decide_as_on_main(
    tmp_path: Path, state: str
) -> None:
    """Pins the behaviour of ITL-1372; revisit there.

    Delta writes a list item or map item of an extension type as plain storage, and
    its log keeps no metadata there, so the identity rule cannot know the stored
    identifier and abstains: main's checks decide. A second list is refused for its
    item's name (``item`` given, ``element`` stored); a second map is accepted with any
    identifier, and every row reads back as plain storage. A same-type append to the
    pending batch is accepted.
    """
    for depth in ("list", "map value"):
        root = tmp_path / depth.replace(" ", "_")
        with _store("Delta", root) as store:
            db = store.db
            db.add_records(
                PATH, _rows(HELD, "extension", depth, [b"1"]), record_id_column="id"
            )
            db.add_records(
                PATH, _rows(HELD, "extension", depth, [b"22"]), record_id_column="id"
            )
            db.flush()
            if state == "reopened":
                store.reopen()
            other = _rows(("itl1371.b", b"m"), "extension", depth, [b"333"])
            if depth == "list":
                with pytest.raises(InputValidationError, match="is named 'item'"):
                    store.db.add_records(PATH, other, record_id_column="id")
                assert store.stored().num_rows == 2
                continue
            store.db.add_records(PATH, other, record_id_column="id")
            stored = store.stored()
            assert stored.num_rows == 3
            item = stored.schema.field("x").type.item_field
            assert identity_node(item).identifier is None  # read back plain


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


def _change(held: "pa.Field[Any]", incoming: "pa.Field[Any]") -> str | None:
    return logical_identity_change(
        identity_node(held), identity_node(incoming), incoming.name
    )


def test_identifier_is_read_from_either_form() -> None:
    ext = EXTENSIONS["itl1371.a"](b"m")
    annotated = pa.field(
        "x", pa.large_string(), metadata={NAME: b"itl1371.a", META: b"m"}
    )
    assert identity_node(pa.field("x", ext)).identifier == (b"itl1371.a", b"m")
    assert identity_node(annotated).identifier == (b"itl1371.a", b"m")
    assert identity_node(pa.field("x", pa.large_string())).identifier is None
    # The two forms of one identifier are one identifier.
    assert _change(annotated, pa.field("x", ext)) is None
    # Arrow's own extension types too.
    assert identity_node(pa.field("x", pa.uuid())).identifier == (b"arrow.uuid", b"")


def test_pyarrow_equality_does_not_decide() -> None:
    """pyarrow's equality of two Python extension types ignores their metadata."""
    m, n = EXTENSIONS["itl1371.a"](b"m"), EXTENSIONS["itl1371.a"](b"n")
    assert m == n  # what the rule must not rely on
    assert _change(pa.field("x", m), pa.field("x", n)) is not None


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

    assert _change(map_field(HELD), map_field(HELD)) is None
    reason = _change(map_field(HELD), map_field(("itl1371.a", b"n")))
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
    """The identity rule finds the same identifier on the struct child; the ITL-1334
    comparison of nested field metadata, which is main's, still refuses the child's
    other non-extension metadata."""
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


# ---------------------------------------------------------------------------
# Review round 1 (PR 492)
# ---------------------------------------------------------------------------


def _struct_of(field: "pa.Field[Any]", array: Any, ids: list[bytes]) -> pa.Table:
    struct = pa.StructArray.from_arrays([array], fields=[field.with_name("v")])
    return pa.Table.from_arrays(
        [pa.array(ids, pa.large_binary()), struct],
        schema=pa.schema(
            [pa.field("id", pa.large_binary()), pa.field("x", struct.type)]
        ),
    )


@pytest.mark.parametrize("state", ["committed", "reopened"])
@pytest.mark.parametrize(
    ("first", "second"), [("extension", "annotated"), ("annotated", "extension")]
)
def test_delta_struct_child_representation_change_is_refused_as_on_main(
    tmp_path: Path, first: str, second: str, state: str
) -> None:
    """The same identifier in the other representation on a struct child is refused,
    as on main, and nothing about the stored rows changes: their identifier reads
    back, and no recorded type is written."""
    import deltalake

    with _store("Delta", tmp_path) as store:
        field, array = _leaf(HELD, first, ["1"])
        store.db.add_records(
            PATH, _struct_of(field, array, [b"1"]), record_id_column="id", flush=True
        )
        log_before = deltalake.DeltaTable(str(tmp_path / "delta" / "t")).schema()
        if state == "reopened":
            store.reopen()
        field, array = _leaf(HELD, second, ["22"])
        with pytest.raises(InputValidationError, match="x.v has other field metadata"):
            store.db.add_records(
                PATH, _struct_of(field, array, [b"22"]), record_id_column="id"
            )
        stored = store.stored()
        assert stored.column("id").to_pylist() == [b"1"]
        child = stored.schema.field("x").type.field("v")
        assert identity_node(child).identifier == (HELD[0].encode(), HELD[1])
        log_after = deltalake.DeltaTable(str(tmp_path / "delta" / "t")).schema()
        assert log_after.fields[1].metadata == log_before.fields[1].metadata


def _readable_type() -> Any:
    """A registered logical type that writes ``b"new"`` and also reads ``b"old"``."""
    ext_class: Any = make_arrow_extension_type(
        "itl1371.readable", pa.large_string(), b"new", also_reads=(b"old",)
    )
    try:
        pa.register_extension_type(ext_class())
    except pa.ArrowKeyError:
        pass  # registered by an earlier test in this process
    return ext_class


def _annotated_nested(meta: bytes, depth: str, ids: list[bytes]) -> pa.Table:
    field = pa.field(
        "v", pa.large_string(), metadata={NAME: b"itl1371.readable", META: meta}
    )
    array = pa.array([i.decode() for i in ids], pa.large_string())
    if depth == "struct":
        return _struct_of(field, array, ids)
    list_type = pa.large_list(field.with_name("item"))
    lists = pa.LargeListArray.from_arrays(
        pa.array(range(len(ids) + 1), pa.int64()), array, type=list_type
    )
    return pa.Table.from_arrays(
        [pa.array(ids, pa.large_binary()), lists],
        schema=pa.schema([pa.field("id", pa.large_binary()), pa.field("x", list_type)]),
    )


@pytest.mark.parametrize("depth", ["struct", "list"])
@pytest.mark.parametrize(
    ("held", "given"), [(b"old", b"new"), (b"old", b"old"), (b"new", b"new")]
)
def test_delta_registered_nested_type_decides_by_raw_bytes(
    tmp_path: Path, depth: str, held: bytes, given: bytes
) -> None:
    """A registered logical type nested in a Delta column with a recorded type.

    The identity rule decides by the raw stored bytes, which the Delta log keeps for
    a struct child: ``old`` then ``new`` is refused naming both. It cannot know them
    for a list item (the log keeps none, and the recorded type is read through the
    registry, which gives ``new`` for ``old``), so it abstains there. Every case is
    refused, as on main: main's field-metadata check compares the given annotation
    with the recorded type as the registry reads it, an extension type, so it refuses
    even an identical second append (pre-existing; reported with ITL-1372).
    """
    _readable_type()
    with _store("Delta", tmp_path) as store:
        store.db.add_records(
            PATH,
            _annotated_nested(held, depth, [b"1"]),
            record_id_column="id",
            flush=True,
        )
        store.reopen()
        with pytest.raises(InputValidationError) as refused:
            store.db.add_records(
                PATH, _annotated_nested(given, depth, [b"22"]), record_id_column="id"
            )
        message = str(refused.value)
        if depth == "struct" and held != given:
            assert f"x.v is of logical type {_raw_text(given)}" in message
            assert f"the stored column's {_raw_text(held)}" in message
        else:
            assert "has other field metadata" in message
        assert store.stored().column("id").to_pylist() == [b"1"]


def _raw_text(meta: bytes) -> str:
    return repr(f"itl1371.readable;{meta.hex()}")


def _wrapped(meta: bytes, values: list[str]) -> Any:
    ext = EXTENSIONS["itl1371.a"](meta)
    return ext.wrap_array(pa.array(values, pa.large_string()))


# The stubs know only some of these constructors' signatures.
_LIST_VIEW: Any = pa.ListViewArray
_LARGE_LIST_VIEW: Any = pa.LargeListViewArray

CONTAINERS: dict[str, Any] = {
    "list_view": lambda a: _LIST_VIEW.from_arrays(
        pa.array(range(len(a)), pa.int32()), pa.array([1] * len(a), pa.int32()), a
    ),
    "large_list_view": lambda a: _LARGE_LIST_VIEW.from_arrays(
        pa.array(range(len(a)), pa.int64()), pa.array([1] * len(a), pa.int64()), a
    ),
    "sparse union": lambda a: pa.UnionArray.from_sparse(
        pa.array([0] * len(a), pa.int8()), [a], ["v"]
    ),
    "dense union": lambda a: pa.UnionArray.from_dense(
        pa.array([0] * len(a), pa.int8()),
        pa.array(range(len(a)), pa.int32()),
        [a],
        ["v"],
    ),
    "dictionary": lambda a: pa.DictionaryArray.from_arrays(
        pa.array(range(len(a)), pa.int8()), a
    ),
    # Run-end encoded extension values cannot be combined by pyarrow; the values are
    # a struct whose child carries the identifier as an annotation.
    "run-end encoded": lambda a: pa.RunEndEncodedArray.from_arrays(
        pa.array(range(1, len(a) + 1), pa.int16()),
        pa.StructArray.from_arrays(
            [a.storage],
            fields=[
                pa.field(
                    "v",
                    pa.large_string(),
                    metadata={
                        NAME: a.type.extension_name.encode(),
                        META: a.type.__arrow_ext_serialize__(),
                    },
                )
            ],
        ),
    ),
}


def _outer_class() -> Any:
    """An extension type whose storage is directly another extension type."""

    class _Outer(pa.ExtensionType):
        def __init__(self, storage: Any) -> None:
            super().__init__(storage, "itl1371.outer")

        def __arrow_ext_serialize__(self) -> bytes:
            return b"outer"

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: bytes) -> Any:
            return cls(storage_type)

    return _Outer


_OUTER = _outer_class()
CONTAINERS["extension in extension"] = lambda a: _OUTER(a.type).wrap_array(a)


def _container_rows(container: str, meta: bytes, ids: list[bytes]) -> pa.Table:
    column = CONTAINERS[container](_wrapped(meta, [i.decode() for i in ids]))
    return pa.table({"id": pa.array(ids, pa.large_binary()), "x": column})


@pytest.mark.parametrize("state", ["pending", "committed"])
@pytest.mark.parametrize("container", list(CONTAINERS))
def test_every_container_is_walked(tmp_path: Path, container: str, state: str) -> None:
    """An identifier under any nested Arrow type is compared (in-memory, which keeps
    them all): the same identifier is appended and reads back, another is refused."""
    db = InMemoryArrowDatabase()
    db.add_records(
        PATH,
        _container_rows(container, b"m", [b"1"]),
        record_id_column="id",
        flush=state == "committed",
    )
    held_ids = [b"1"]
    if container != "dictionary":
        # (pyarrow cannot unify two dictionaries of extension values at the flush.)
        db.add_records(
            PATH, _container_rows(container, b"m", [b"22"]), record_id_column="id"
        )
        held_ids.append(b"22")
    with pytest.raises(InputValidationError, match="itl1371.a;6e"):
        db.add_records(
            PATH, _container_rows(container, b"n", [b"333"]), record_id_column="id"
        )
    db.flush()
    stored = db.get_all_records(PATH, record_id_column="id")
    assert stored is not None
    assert sorted(cast(list[bytes], stored.column("id").to_pylist())) == held_ids
    assert (
        stored.schema.field("x").type
        == _container_rows(container, b"m", [b"1"]).schema.field("x").type
    )


def test_delta_dictionary_of_extension_values(tmp_path: Path) -> None:
    """A dictionary is an encoding: its values' identifier stands where it stands. A
    pending batch is compared as given (another identifier refused). Delta decodes a
    dictionary column and stores its values as plain storage, with no identifier in
    the log (as on main), so the stored column holds no logical type and a logical
    append to it is refused, as over any plain column."""
    with _store("Delta", tmp_path) as store:
        store.db.add_records(
            PATH, _container_rows("dictionary", b"m", [b"1"]), record_id_column="id"
        )
        with pytest.raises(InputValidationError, match="x is of logical type"):
            store.db.add_records(
                PATH,
                _container_rows("dictionary", b"n", [b"22"]),
                record_id_column="id",
            )
        store.db.flush()
        with pytest.raises(InputValidationError, match="stored column's no logical"):
            store.db.add_records(
                PATH,
                _container_rows("dictionary", b"m", [b"333"]),
                record_id_column="id",
            )
        stored = store.stored()
        assert stored.column("id").to_pylist() == [b"1"]
        assert identity_node(stored.schema.field("x")).identifier is None


def _dictionary_layout(
    ids: list[bytes], meta: bytes, *, encoded: bool, nested: bool, form: str
) -> pa.Table:
    """``x``: an identifier ``itl1371.a;<meta>`` (extension or annotated), directly
    or as the child ``v`` of a struct, dictionary-encoded or not."""
    field: Any
    array: Any
    field, array = _leaf(("itl1371.a", meta), form, [i.decode() for i in ids])
    if nested:
        array = pa.StructArray.from_arrays([array], fields=[field])
        field = pa.field("x", array.type)
    if encoded:
        array = pa.DictionaryArray.from_arrays(
            pa.array(range(len(ids)), pa.int8()), array
        )
        field = field.with_type(array.type)
    return pa.Table.from_arrays(
        [pa.array(ids, pa.large_binary()), array],
        schema=pa.schema([pa.field("id", pa.large_binary()), field.with_name("x")]),
    )


@pytest.mark.parametrize("form", FORMS)
@pytest.mark.parametrize("state", ["committed", "reopened"])
def test_delta_dictionary_of_the_stored_type_is_accepted(
    tmp_path: Path, state: str, form: str
) -> None:
    """A committed extension column, then a dictionary of that exact type: the same
    identifier, accepted as on main, and every row reads back with it."""
    with _store("Delta", tmp_path) as store:
        store.db.add_records(
            PATH,
            _dictionary_layout(
                [b"1", b"2"], b"m", encoded=False, nested=False, form=form
            ),
            record_id_column="id",
            flush=True,
        )
        if state == "reopened":
            store.reopen()
        store.db.add_records(
            PATH,
            _dictionary_layout(
                [b"3", b"4"], b"m", encoded=True, nested=False, form=form
            ),
            record_id_column="id",
        )
        stored = store.stored()
        assert stored.num_rows == 4
        assert identity_node(stored.schema.field("x")).identifier == (
            b"itl1371.a",
            b"m",
        )


@pytest.mark.parametrize("state", ["committed", "reopened"])
def test_delta_dictionary_of_structs_with_another_child_identifier_is_refused(
    tmp_path: Path, state: str
) -> None:
    """A committed struct whose child is annotated ``m``, then a dictionary of structs
    whose child is annotated ``n``: the log's raw bytes and the given ones differ,
    refused; the stored rows keep ``m``. The same child identifier is accepted."""
    with _store("Delta", tmp_path) as store:
        store.db.add_records(
            PATH,
            _dictionary_layout(
                [b"1"], b"m", encoded=False, nested=True, form="annotated"
            ),
            record_id_column="id",
            flush=True,
        )
        if state == "reopened":
            store.reopen()
        with pytest.raises(InputValidationError, match="x.v is of logical type"):
            store.db.add_records(
                PATH,
                _dictionary_layout(
                    [b"2"], b"n", encoded=True, nested=True, form="annotated"
                ),
                record_id_column="id",
            )
        store.db.add_records(
            PATH,
            _dictionary_layout(
                [b"3"], b"m", encoded=True, nested=True, form="annotated"
            ),
            record_id_column="id",
        )
        stored = store.stored().sort_by("id")
        assert stored.column("id").to_pylist() == [b"1", b"3"]
        child = stored.schema.field("x").type.field("v")
        assert identity_node(child).identifier == (b"itl1371.a", b"m")


@pytest.mark.parametrize("state", ["pending", "committed", "reopened"])
@pytest.mark.parametrize(
    "metadata",
    [
        {b"orcapod:arrow_type": b"a"},  # an ordinary key with that name, nested
        {META: b"a"},  # extension metadata without a name: no identifier
    ],
    ids=["arrow_type key", "lone extension metadata"],
)
def test_other_nested_metadata_without_an_identifier_is_still_refused(
    tmp_path: Path, metadata: dict[bytes, bytes], state: str
) -> None:
    """Nested metadata other than a complete identifier stays with ITL-1334's
    comparison: a change is refused, as on main."""

    def rows(value: bytes, ids: list[bytes]) -> pa.Table:
        key = next(iter(metadata))
        field = pa.field("v", pa.large_string(), metadata={key: value})
        array = pa.array([i.decode() for i in ids], pa.large_string())
        return _struct_of(field, array, ids)

    with _store("Delta", tmp_path) as store:
        store.db.add_records(
            PATH, rows(b"a", [b"1"]), record_id_column="id", flush=state != "pending"
        )
        if state == "reopened":
            store.reopen()
        with pytest.raises(InputValidationError, match="x.v has other field metadata"):
            store.db.add_records(PATH, rows(b"b", [b"22"]), record_id_column="id")
        assert store.stored().column("id").to_pylist() == [b"1"]


@pytest.mark.parametrize("state", ["pending", "committed"])
def test_identifiers_are_compared_as_bytes(state: str) -> None:
    """An extension name that is not UTF-8: identical bytes are one identifier (the
    second append is accepted, as on main); another name or other metadata is
    refused, the error showing a name that is not text as its bytes in hex, so two
    such names read apart."""

    def rows(name: bytes, meta: bytes, ids: list[bytes]) -> pa.Table:
        field = pa.field("x", pa.large_string(), metadata={NAME: name, META: meta})
        return pa.Table.from_arrays(
            [
                pa.array(ids, pa.large_binary()),
                pa.array(["v"] * len(ids), pa.large_string()),
            ],
            schema=pa.schema([pa.field("id", pa.large_binary()), field]),
        )

    db = InMemoryArrowDatabase()
    db.add_records(
        PATH,
        rows(b"\xff", b"a", [b"1"]),
        record_id_column="id",
        flush=state == "committed",
    )
    db.add_records(PATH, rows(b"\xff", b"a", [b"22"]), record_id_column="id")
    with pytest.raises(InputValidationError) as refused:
        db.add_records(PATH, rows(b"\xfe", b"a", [b"333"]), record_id_column="id")
    assert "'0xfe;61', the stored column's '0xff;61'" in str(refused.value)
    with pytest.raises(InputValidationError, match="'0xff;62'"):
        db.add_records(PATH, rows(b"\xff", b"b", [b"4444"]), record_id_column="id")
    db.flush()
    stored = db.get_all_records(PATH, record_id_column="id")
    assert stored is not None and stored.num_rows == 2


@pytest.mark.parametrize("schema_handling", ["error", "merge", "coerce"])
@pytest.mark.parametrize("state", ["committed", "reopened"])
def test_delta_untyped_null_over_a_logical_column(
    tmp_path: Path, state: str, schema_handling: str
) -> None:
    """An untyped null column holds no value and claims no type: it is resolved to the
    held type and stored as a null, as on main, in every schema handling mode."""
    with _store("Delta", tmp_path) as store:
        store.db.add_records(
            PATH,
            _rows(HELD, "annotated", "top", [b"1"]),
            record_id_column="id",
            flush=True,
        )
        if state == "reopened":
            store.reopen()
        nulls = pa.table(
            {
                "a": pa.array([1], pa.int64()),
                "id": pa.array([b"22"], pa.large_binary()),
                "x": pa.nulls(1),
            }
        )
        store.db.add_records(
            PATH, nulls, record_id_column="id", schema_handling=schema_handling
        )
        stored = store.stored().sort_by("id")
        assert stored.column("x").to_pylist() == ["1", None]
        assert identity_node(stored.schema.field("x")).identifier == (
            HELD[0].encode(),
            HELD[1],
        )


@pytest.mark.parametrize("schema_handling", ["error", "merge", "coerce"])
@pytest.mark.parametrize("state", ["committed", "reopened"])
def test_delta_annotated_null_of_another_identifier_is_refused(
    tmp_path: Path, state: str, schema_handling: str
) -> None:
    """Only a bare null claims no type: a null column annotated with another
    identifier is a known different identifier, refused in every mode."""
    with _store("Delta", tmp_path) as store:
        store.db.add_records(
            PATH,
            _rows(HELD, "annotated", "top", [b"1"]),
            record_id_column="id",
            flush=True,
        )
        if state == "reopened":
            store.reopen()
        other = pa.field("x", pa.null(), metadata={NAME: b"itl1371.a", META: b"n"})
        nulls = pa.Table.from_arrays(
            [
                pa.array([1], pa.int64()),
                pa.array([b"22"], pa.large_binary()),
                pa.nulls(1),
            ],
            schema=pa.schema(
                [pa.field("a", pa.int64()), pa.field("id", pa.large_binary()), other]
            ),
        )
        with pytest.raises(InputValidationError, match="'itl1371.a;6e'"):
            store.db.add_records(
                PATH, nulls, record_id_column="id", schema_handling=schema_handling
            )
        assert store.stored().column("id").to_pylist() == [b"1"]


# The stubs type ``type_codes`` as an array; pyarrow takes a list.
_TYPE_CODES: Any = [7, 9]


def _duplicates(shape: str, first_meta: bytes, ids: list[bytes]) -> pa.Table:
    """Two places named ``dup`` of different identifiers (``first_meta`` and
    ``fixed``), as struct children, sparse or dense union members, or two top-level
    columns; only the first one's metadata varies."""
    n = len(ids)

    def ext(meta: bytes, values: list[str | None]) -> Any:
        return EXTENSIONS["itl1371.a"](meta).wrap_array(
            pa.array(values, pa.large_string())
        )

    idcol = pa.array(ids, pa.large_binary())
    if shape == "struct":
        x = pa.StructArray.from_arrays(
            [ext(first_meta, ["v"] * n), ext(b"fixed", ["w"] * n)], names=["dup", "dup"]
        )
        return pa.Table.from_arrays([idcol, x], names=["id", "x"])
    if shape == "columns":
        return pa.Table.from_arrays(
            [idcol, ext(first_meta, ["v"] * n), ext(b"fixed", ["w"] * n)],
            names=["id", "dup", "dup"],
        )
    codes = pa.array([7] * n, pa.int8())
    if shape == "sparse union":
        x = pa.UnionArray.from_sparse(
            codes,
            [ext(first_meta, ["v"] * n), ext(b"fixed", [None] * n)],
            ["dup", "dup"],
            type_codes=_TYPE_CODES,
        )
    else:
        x = pa.UnionArray.from_dense(
            codes,
            pa.array(range(n), pa.int32()),
            [ext(first_meta, ["v"] * n), ext(b"fixed", ["w"])],
            ["dup", "dup"],
            type_codes=_TYPE_CODES,
        )
    return pa.Table.from_arrays([idcol, x], names=["id", "x"])


@pytest.mark.parametrize("state", ["pending", "committed"])
@pytest.mark.parametrize("shape", ["struct", "sparse union", "dense union", "columns"])
def test_repeated_names_pair_by_occurrence(shape: str, state: str) -> None:
    """Arrow allows two struct children, union members or columns of one name. They
    pair by occurrence, never by name alone: a change to the FIRST of two places
    named ``dup`` is refused; the same identifiers are accepted."""
    db = InMemoryArrowDatabase()
    db.add_records(
        PATH,
        _duplicates(shape, b"a", [b"1"]),
        record_id_column="id",
        flush=state == "committed",
    )
    db.add_records(PATH, _duplicates(shape, b"a", [b"22"]), record_id_column="id")
    with pytest.raises(
        InputValidationError, match="'itl1371.a;62', the stored column's 'itl1371.a;61'"
    ):
        db.add_records(PATH, _duplicates(shape, b"b", [b"333"]), record_id_column="id")
    db.flush()
    stored = db.get_all_records(PATH, record_id_column="id")
    assert stored is not None and stored.num_rows == 2


@pytest.mark.parametrize("state", ["pending", "committed"])
def test_the_bytes_arrow_holds_decide_not_the_serializer(state: str) -> None:
    """A third-party extension type whose serializer's state changes after its array
    was built: Arrow holds the bytes taken when the type was made (its C data
    interface export), and those decide, as on main for the same bytes."""
    from arro3.core import Field as Arro3Field

    class Mutable(pa.ExtensionType):
        def __init__(self, payload: bytes) -> None:
            self.payload = payload
            super().__init__(pa.large_string(), "itl1371.mutable")

        def __arrow_ext_serialize__(self) -> bytes:
            return self.payload

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: bytes) -> Any:
            return cls(serialized)

    def rows(ext: Any, rid: bytes) -> pa.Table:
        x = ext.wrap_array(pa.array(["v"], pa.large_string()))
        return pa.table({"id": pa.array([rid], pa.large_binary()), "x": x})

    first, second = Mutable(b"original"), Mutable(b"original")
    held, given = rows(first, b"1"), rows(second, b"22")
    second.payload = b"changed"  # the callback now differs; Arrow's bytes do not
    exported = Arro3Field.from_arrow(given.schema.field("x")).metadata
    assert metadata_bytes(exported)[META] == b"original"
    db = InMemoryArrowDatabase()
    db.add_records(PATH, held, record_id_column="id", flush=state == "committed")
    db.add_records(PATH, given, record_id_column="id")

    third, fourth = Mutable(b"first"), Mutable(b"second")
    held, given = rows(third, b"1"), rows(fourth, b"22")
    fourth.payload = b"first"  # the callback now agrees; Arrow's bytes do not
    db = InMemoryArrowDatabase()
    db.add_records(PATH, held, record_id_column="id", flush=state == "committed")
    with pytest.raises(InputValidationError, match="'itl1371.mutable;7365636f6e64'"):
        db.add_records(PATH, given, record_id_column="id")
