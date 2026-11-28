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
import re
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
from orcapod.databases.utils import (
    IdentityNode,
    first_logical_identity_change,
    metadata_bytes,
    identity_node,
    logical_identity_change,
)
from orcapod.errors import (
    InputValidationError,
    OrcapodDatabaseReadError,
    UnstorableColumnError,
)
from orcapod.logical_types.identifier import LogicalTypeIdentifier
from orcapod.logical_types.registry import make_arrow_extension_type
from orcapod.logical_types.registry import LogicalTypeRegistry
from orcapod.semantic_types.universal_converter import UniversalTypeConverter

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
        raise AssertionError(kind)


def _through(db: Any, via: str) -> Any:
    if via == "direct":
        return db
    converter = UniversalTypeConverter(logical_type_registry=LogicalTypeRegistry())
    return ExtensionAwareDatabase(db, converter)


KINDS = ("in-memory", "Delta")
STATES = ("pending", "committed", "reopened")
FORMS = ("extension", "annotated")
DEPTHS = ("top", "struct", "list", "map value")

BASE = [
    (kind, state, form, depth)
    for kind, state, form, depth in itertools.product(KINDS, STATES, FORMS, DEPTHS)
    if (kind == "in-memory" and state != "reopened")
    or (kind == "Delta" and depth in ("top", "struct"))
]

REFUSED_CASES = [
    pytest.param(*case, change, id=f"{'-'.join(case)}-{change}")
    for case in BASE
    for change in CHANGES
]

SAME_CASES = [pytest.param(*case, id="-".join(case)) for case in BASE]


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
    # A stored Delta column with no identifier and no recorded type: the rule
    # abstains (test_delta_logical_type_over_a_plain_column).
    if kind == "in-memory" or (state == "pending" and depth in ("top", "struct"))
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
        with pytest.raises(InputValidationError, match="stored column's no logical"):
            _through(store.db, via).add_records(
                PATH, _rows(HELD, form, depth, [b"22"]), record_id_column="id"
            )
        assert store.stored().column("id").to_pylist() == [b"1"]


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


def _annotated_struct(meta: bytes, ids: list[bytes]) -> pa.Table:
    field = pa.field(
        "v", pa.large_string(), metadata={NAME: b"itl1371.readable", META: meta}
    )
    return _struct_of(
        field, pa.array([i.decode() for i in ids], pa.large_string()), ids
    )


@pytest.mark.parametrize(
    ("held", "given"), [(b"old", b"new"), (b"old", b"old"), (b"new", b"new")]
)
def test_delta_registered_struct_child_decides_by_raw_bytes(
    tmp_path: Path, held: bytes, given: bytes
) -> None:
    """Delta's log keeps the raw identifier bytes for an annotated struct child.

    A change from old to new is refused naming both identifiers. For identical
    writes, the ordinary field check compares the given annotation against the
    restored extension type and refuses the append.
    """
    _readable_type()
    with _store("Delta", tmp_path) as store:
        store.db.add_records(
            PATH,
            _annotated_struct(held, [b"1"]),
            record_id_column="id",
            flush=True,
        )
        store.reopen()
        with pytest.raises(InputValidationError) as refused:
            store.db.add_records(
                PATH, _annotated_struct(given, [b"22"]), record_id_column="id"
            )
        message = str(refused.value)
        if held != given:
            assert f"x.v is of logical type {_raw_text(given)}" in message
            assert f"the stored column's {_raw_text(held)}" in message
        else:
            assert "x.v has other field metadata" in message
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
    expected = _container_rows(container, b"m", [b"1"]).schema.field("x")
    # The layout, by Arrow type equality, and the identifiers, as raw bytes: type
    # equality alone would also hold for another payload.
    assert stored.schema.field("x").type == expected.type
    read_back = identity_node(stored.schema.field("x"))
    assert read_back == identity_node(expected)
    assert (b"itl1371.a", b"m") in _identifiers(read_back)


def _identifiers(node: IdentityNode) -> list[Any]:
    """Every identifier a node and its children carry, depth first."""
    found = [node.identifier] if node.identifier is not None else []
    for _, child in node.children:
        found.extend(_identifiers(child))
    return found


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


@pytest.mark.parametrize("state", ["committed", "reopened"])
def test_delta_annotated_dictionary_of_the_stored_type_is_accepted(
    tmp_path: Path, state: str
) -> None:
    """A dictionary of storage values with the held column's annotation keeps it."""
    form = "annotated"
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
    assert "b'\\xfe';61, the stored column's b'\\xff';61" in str(refused.value)
    with pytest.raises(InputValidationError, match=re.escape("b'\\xff';62")):
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


def test_an_unexportable_extension_level_is_not_compared() -> None:
    """Arrow cannot export an extension type whose storage is another extension type,
    so the bytes it holds for that outer level are out of reach: the outer level is
    not compared (its serializer, which may have changed, is not asked), and its
    storage still is."""

    class MutableOuter(pa.ExtensionType):
        def __init__(self, storage: Any, payload: bytes) -> None:
            self.payload = payload
            super().__init__(storage, "itl1371.mutable_outer")

        def __arrow_ext_serialize__(self) -> bytes:
            return self.payload

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: bytes) -> Any:
            return cls(storage_type, serialized)

    def rows(inner_meta: bytes, outer: bytes, rid: bytes) -> tuple[Any, pa.Table]:
        inner = _wrapped(inner_meta, ["v"])
        outer_type = MutableOuter(inner.type, outer)
        x = outer_type.wrap_array(inner)
        return outer_type, pa.table({"id": pa.array([rid], pa.large_binary()), "x": x})

    db = InMemoryArrowDatabase()
    _, held = rows(b"m", b"o", b"1")
    db.add_records(PATH, held, record_id_column="id", flush=True)
    # The outer serializer changed after its type was made: not asked, accepted.
    outer_type, same = rows(b"m", b"o", b"22")
    outer_type.payload = b"changed"
    db.add_records(PATH, same, record_id_column="id")
    # Another inner identifier is still refused.
    _, other_inner = rows(b"n", b"o", b"333")
    with pytest.raises(InputValidationError, match="x<storage> is of logical type"):
        db.add_records(PATH, other_inner, record_id_column="id")
    db.flush()
    stored = db.get_all_records(PATH, record_id_column="id")
    assert stored is not None and stored.num_rows == 2


@pytest.mark.parametrize("state", ["pending", "committed"])
def test_a_dictionary_over_a_plain_held_column_is_refused_outside_delta(
    state: str,
) -> None:
    """The dictionary exception is Delta's alone (its storage loses a dictionary's
    identifier): where the database holds the given schema, a dictionary of
    extension values over a plain column is a known different identifier."""
    db = InMemoryArrowDatabase()
    plain = pa.table(
        {
            "id": pa.array([b"1"], pa.large_binary()),
            "x": pa.array(["v"], pa.large_string()),
        }
    )
    db.add_records(PATH, plain, record_id_column="id", flush=state == "committed")
    with pytest.raises(InputValidationError, match="stored column's no logical"):
        db.add_records(
            PATH, _container_rows("dictionary", b"m", [b"22"]), record_id_column="id"
        )


def test_in_memory_duplicate_only_append_of_another_identifier_is_refused() -> None:
    """One rule in every database: a known different identifier is refused whatever
    ``skip_duplicates``, duplicate-only or not (on main, a duplicate-only append was a
    silent no-op; nothing was or is written)."""
    db = InMemoryArrowDatabase()
    db.add_records(
        PATH,
        _container_rows("list_view", b"m", [b"1"]),
        record_id_column="id",
        flush=True,
    )
    for ids in ([b"1"], [b"1", b"22"]):
        with pytest.raises(InputValidationError, match="itl1371.a;6e"):
            db.add_records(
                PATH,
                _container_rows("list_view", b"n", ids),
                record_id_column="id",
                skip_duplicates=True,
            )
    db.flush()
    stored = db.get_all_records(PATH, record_id_column="id")
    assert stored is not None
    assert stored.column("id").to_pylist() == [b"1"]
    assert (b"itl1371.a", b"m") in _identifiers(identity_node(stored.schema.field("x")))


def test_delta_identity_is_checked_before_recorded_types_are_decoded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The identity rule reads the Delta log only, and runs before the stored
    recorded types are decoded: a decode that would fail does not hide the
    identifier the error names."""
    import orcapod.databases.delta_lake_databases as delta_module

    with _store("Delta", tmp_path) as store:
        store.db.add_records(
            PATH,
            _rows(HELD, "annotated", "struct", [b"1"]),
            record_id_column="id",
            flush=True,
        )

        def undecodable(_: Any) -> Any:
            raise RuntimeError(
                "recorded types are not decoded before the identity rule"
            )

        monkeypatch.setattr(delta_module, "_held_columns", undecodable)
        with pytest.raises(
            InputValidationError, match="x.v is of logical type 'itl1371.b"
        ):
            store.db.add_records(
                PATH,
                _rows(("itl1371.b", b"m"), "annotated", "struct", [b"22"]),
                record_id_column="id",
            )


@pytest.mark.parametrize("state", ["pending", "committed"])
def test_extension_metadata_that_is_not_text(state: str) -> None:
    """An extension type's serialized metadata is bytes, read from Arrow's export as
    they are (none of it decoded): the same bytes are accepted, other bytes refused."""
    db = InMemoryArrowDatabase()
    db.add_records(
        PATH,
        _container_rows("list_view", b"\xff\xfe", [b"1"]),
        record_id_column="id",
        flush=state == "committed",
    )
    db.add_records(
        PATH, _container_rows("list_view", b"\xff\xfe", [b"22"]), record_id_column="id"
    )
    with pytest.raises(InputValidationError, match="'itl1371.a;fffd', the stored"):
        db.add_records(
            PATH,
            _container_rows("list_view", b"\xff\xfd", [b"333"]),
            record_id_column="id",
        )
    db.flush()
    stored = db.get_all_records(PATH, record_id_column="id")
    assert stored is not None and stored.num_rows == 2


RECORDED_STORAGES = {
    "uint16": (pa.uint16(), lambda n: list(range(n))),
    "fixed_size_binary": (pa.binary(4), lambda n: [b"%04d" % i for i in range(n)]),
}


@pytest.mark.parametrize("state", ["committed", "reopened"])
@pytest.mark.parametrize("encoded", [False, True], ids=["plain", "dictionary"])
@pytest.mark.parametrize("form", FORMS)
@pytest.mark.parametrize("storage", list(RECORDED_STORAGES))
def test_delta_logical_type_over_a_recorded_plain_column(
    tmp_path: Path, storage: str, form: str, encoded: bool, state: str
) -> None:
    """A stored Delta column whose log records its Arrow type (``uint16``, fixed-size
    binary) and carries no identifier is known to hold no logical type: another
    identifier is refused, as an extension type or an annotation, dictionary-encoded
    or not (round 3, finding 1)."""
    arrow_type, values_of = RECORDED_STORAGES[storage]

    def rows(ids: list[bytes], identifier: bool) -> pa.Table:
        values: Any = pa.array(values_of(len(ids)), arrow_type)
        field = pa.field("x", arrow_type)
        if identifier and form == "extension":
            values = _ext_over(arrow_type)(b"e").wrap_array(values)
            field = pa.field("x", values.type)
        elif identifier:
            field = pa.field(
                "x", arrow_type, metadata={NAME: b"itl1371.over", META: b"e"}
            )
        if encoded and identifier:
            values = pa.DictionaryArray.from_arrays(
                pa.array(range(len(ids)), pa.int8()), values
            )
            field = field.with_type(values.type)
        return pa.Table.from_arrays(
            [pa.array(ids, pa.large_binary()), values],
            schema=pa.schema([pa.field("id", pa.large_binary()), field]),
        )

    with _store("Delta", tmp_path) as store:
        store.db.add_records(
            PATH, rows([b"1"], False), record_id_column="id", flush=True
        )
        if state == "reopened":
            store.reopen()
        if encoded and form == "extension":
            with pytest.raises(UnstorableColumnError, match="dictionary values"):
                store.db.add_records(PATH, rows([b"22"], True), record_id_column="id")
        else:
            with pytest.raises(
                InputValidationError, match="stored column's no logical"
            ):
                store.db.add_records(PATH, rows([b"22"], True), record_id_column="id")
        assert store.stored().column("id").to_pylist() == [b"1"]


_EXT_OVER: dict[str, Any] = {}


def _ext_over(storage: Any) -> Any:
    """An extension class named ``itl1371.over`` over ``storage``, one per storage."""
    key = str(storage)
    if key not in _EXT_OVER:

        class _Over(pa.ExtensionType):
            def __init__(self, meta: bytes) -> None:
                self._meta = meta
                super().__init__(storage, "itl1371.over")

            def __arrow_ext_serialize__(self) -> bytes:
                return self._meta

            @classmethod
            def __arrow_ext_deserialize__(
                cls, storage_type: Any, serialized: bytes
            ) -> Any:
                return cls(serialized)

        _EXT_OVER[key] = _Over
    return _EXT_OVER[key]


@pytest.mark.parametrize("state", ["committed", "reopened"])
def test_delta_logical_type_over_a_plain_column(tmp_path: Path, state: str) -> None:
    """Without a recorded Arrow type (a plain text column, or one Delta wrote from a
    dictionary of extension values, losing its identifier: ITL-1372) a stored column
    with no identifier proves nothing and the rule abstains, so main's checks decide:
    an extension column is accepted, as on main (round 3, findings 1 and 5)."""
    with _store("Delta", tmp_path / "unrecorded") as store:
        store.db.add_records(
            PATH,
            _rows(None, "extension", "top", [b"1"]),
            record_id_column="id",
            flush=True,
        )
        if state == "reopened":
            store.reopen()
        store.db.add_records(
            PATH, _rows(HELD, "extension", "top", [b"22"]), record_id_column="id"
        )
        assert store.stored().num_rows == 2


@pytest.mark.parametrize("state", ["committed", "reopened"])
def test_delta_extension_after_a_legacy_dictionary_first_write(
    tmp_path: Path, state: str
) -> None:
    """A legacy dictionary of extension values was stored as plain text."""
    import deltalake

    root = tmp_path / "delta"
    # Model the old log, which had neither an extension identifier nor a recorded type.
    plain = pa.table(
        {
            "__record_id": pa.array([b"1"], pa.large_binary()),
            "x": pa.array(["1"], pa.large_string()),
        }
    )
    deltalake.write_deltalake(str(root / "t"), plain)
    with _store("Delta", tmp_path) as store:
        assert store.stored().column("x").to_pylist() == ["1"]
        if state == "reopened":
            store.reopen()
        extension = pa.table(
            {"id": pa.array([b"22"], pa.large_binary()), "x": _wrapped(b"m", ["22"])}
        )
        store.db.add_records(PATH, extension, record_id_column="id", flush=True)
        with pytest.raises(UnstorableColumnError, match="dictionary values"):
            store.db.add_records(
                PATH,
                _container_rows("dictionary", b"m", [b"333"]),
                record_id_column="id",
            )
        stored = store.stored().sort_by("id")
        assert stored.column("id").to_pylist() == [b"1", b"22"]


@pytest.mark.parametrize("skip_duplicates", [False, True])
def test_delta_identity_is_refused_before_an_undecodable_stored_type(
    tmp_path: Path, skip_duplicates: bool
) -> None:
    """A stored struct child annotated with an identifier the registered type cannot
    read. The order is the same with either duplicate setting: the identity rule
    reads the Delta log only and runs first, so another identifier is refused with
    the same ``InputValidationError`` naming both, before anything is decoded. Only
    when it passes does the duplicate scan run, and a read failure there propagates
    as it is (a read failure fails the run whatever the error policy): the same
    identifier with ``skip_duplicates`` raises ``OrcapodDatabaseReadError``. Without
    it nothing reads the stored ids or decodes the recorded types before the rows
    are queued but the field-metadata check (ITL-1334), which decodes the stored
    struct's recorded type: the registered reader's own ``ValueError`` is raised, as
    on main (that decode is not wrapped as a read failure; pre-existing)."""

    class Current(pa.ExtensionType):
        def __init__(self) -> None:
            super().__init__(pa.large_string(), "itl1371.current")

        def __arrow_ext_serialize__(self) -> bytes:
            return b"new"

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: bytes) -> Any:
            raise ValueError("itl1371.current cannot read this metadata")

    def rows(ids: list[bytes], name: bytes, meta: bytes) -> pa.Table:
        field = pa.field("v", pa.large_string(), metadata={NAME: name, META: meta})
        values = pa.array([i.decode() for i in ids], pa.large_string())
        return _struct_of(field, values, ids)

    db = DeltaTableDatabase(tmp_path / "delta")
    db.add_records(
        PATH,
        rows([b"1"], b"itl1371.current", b"old"),
        record_id_column="id",
        flush=True,
    )
    pa.register_extension_type(cast(Any, Current()))
    try:
        with pytest.raises(InputValidationError) as refused:
            DeltaTableDatabase(tmp_path / "delta").add_records(
                PATH,
                rows([b"22", b"333"], b"itl1371.other", b"new"),
                record_id_column="id",
                skip_duplicates=skip_duplicates,
            )
        message = str(refused.value)
        assert "x.v is of logical type 'itl1371.other;6e6577'" in message
        assert "the stored column's 'itl1371.current;6f6c64'" in message

        same = rows([b"22", b"333"], b"itl1371.current", b"old")
        db = DeltaTableDatabase(tmp_path / "delta")
        if skip_duplicates:
            with pytest.raises(OrcapodDatabaseReadError):
                db.add_records(PATH, same, record_id_column="id", skip_duplicates=True)
        else:
            with pytest.raises(ValueError, match="cannot read this metadata"):
                db.add_records(PATH, same, record_id_column="id")
    finally:
        pa.unregister_extension_type("itl1371.current")


@pytest.mark.parametrize("state", ["pending", "committed"])
@pytest.mark.parametrize(
    "ids", [[b"1", b"2"], [b"1", b"3"]], ids=["duplicates", "one new"]
)
def test_in_memory_ordinary_refusal_keeps_its_priority(
    state: str, ids: list[bytes]
) -> None:
    """With skip_duplicates, the duplicate test of the identity rule reads record ids
    only (no deduplication, no temporary column): an ordinary field-metadata change
    is refused as on main, even with a user column named ``__row_index``."""
    db = InMemoryArrowDatabase()

    def rows(row_ids: list[bytes], unit: bytes) -> pa.Table:
        field = pa.field("x", pa.large_string(), metadata={b"unit": unit})
        return pa.Table.from_arrays(
            [
                pa.array(row_ids, pa.large_binary()),
                pa.array(["v"] * len(row_ids), pa.large_string()),
            ],
            schema=pa.schema([pa.field("id", pa.large_binary()), field]),
        )

    db.add_records(
        PATH,
        rows([b"1", b"2"], b"a"),
        record_id_column="id",
        flush=state == "committed",
    )
    changed = rows(ids, b"b").append_column("__row_index", pa.array([0, 1]))
    with pytest.raises(InputValidationError, match="x has other field metadata"):
        db.add_records(PATH, changed, record_id_column="id", skip_duplicates=True)


def test_an_opaque_name_never_reads_as_a_text_name() -> None:
    """A name that is not UTF-8 is shown as a bytes literal, a form no text name
    takes: ``b'\\xff'`` and the text ``0xff`` read apart."""

    def rows(name: bytes, ids: list[bytes]) -> pa.Table:
        field = pa.field("x", pa.large_string(), metadata={NAME: name})
        return pa.Table.from_arrays(
            [
                pa.array(ids, pa.large_binary()),
                pa.array(["v"] * len(ids), pa.large_string()),
            ],
            schema=pa.schema([pa.field("id", pa.large_binary()), field]),
        )

    db = InMemoryArrowDatabase()
    db.add_records(PATH, rows(b"\xff", [b"1"]), record_id_column="id", flush=True)
    with pytest.raises(InputValidationError) as refused:
        db.add_records(PATH, rows(b"0xff", [b"22"]), record_id_column="id")
    assert "'0xff;', the stored column's b'\\xff';" in str(refused.value)
    with pytest.raises(InputValidationError) as refused:
        db.add_records(PATH, rows(b"b'\\xff'", [b"22"]), record_id_column="id")
    assert "\"b'\\\\xff';\", the stored column's b'\\xff';" in str(refused.value)


@pytest.mark.parametrize("skip_duplicates", [False, True])
@pytest.mark.parametrize("state", ["pending", "committed"])
@pytest.mark.parametrize("kind", ["in-memory", "Delta"])
def test_every_chunk_identifier_is_compared(
    tmp_path: Path, kind: str, state: str, skip_duplicates: bool
) -> None:
    """pyarrow lets the chunks of one column hold one Python extension class with
    other serialized metadata: each chunk's own identifier is compared, so a later
    chunk of another identifier is refused; chunks of the same bytes are accepted
    (round 3, finding 6)."""

    def rows(metas: list[bytes], ids: list[bytes]) -> pa.Table:
        chunks = [_wrapped(meta, [i.decode()]) for meta, i in zip(metas, ids)]
        return pa.Table.from_arrays(
            [pa.array(ids, pa.large_binary()), pa.chunked_array(chunks)],
            names=["id", "x"],
        )

    with _store(kind, tmp_path) as store:
        _check_chunk_identifiers(store, rows, state, skip_duplicates)


def _check_chunk_identifiers(
    store: _Store, rows: Any, state: str, skip_duplicates: bool
) -> None:
    db = store.db
    db.add_records(
        PATH, rows([b"m"], [b"1"]), record_id_column="id", flush=state == "committed"
    )
    db.add_records(
        PATH,
        rows([b"m", b"m"], [b"2", b"3"]),
        record_id_column="id",
        flush=state == "committed",
    )
    ids = [b"2", b"3"] if skip_duplicates else [b"4", b"5"]
    mixed = rows([b"m", b"n"], ids)
    assert [c.type.__arrow_ext_serialize__() for c in mixed.column("x").chunks] == [
        b"m",
        b"n",
    ]
    with pytest.raises(
        InputValidationError, match="'itl1371.a;6e', the stored column's 'itl1371.a;6d'"
    ):
        db.add_records(
            PATH, mixed, record_id_column="id", skip_duplicates=skip_duplicates
        )
    stored = store.stored()
    assert sorted(cast(list[bytes], stored.column("id").to_pylist())) == [
        b"1",
        b"2",
        b"3",
    ]


def test_a_null_record_id_is_never_taken_for_a_stored_one() -> None:
    """The duplicate test reads record ids as ``_filter_existing_records`` does: a
    null id is never a stored record, so an append of null ids with another
    identifier is refused even when a null id is committed."""

    def rows(meta: bytes, ids: list[bytes | None]) -> pa.Table:
        return pa.table(
            {
                "id": pa.array(ids, pa.large_binary()),
                "x": _wrapped(meta, ["v"] * len(ids)),
            }
        )

    db = InMemoryArrowDatabase()
    db.add_records(PATH, rows(b"m", [None]), record_id_column="id", flush=True)
    with pytest.raises(InputValidationError, match="itl1371.a;6e"):
        db.add_records(
            PATH, rows(b"n", [None]), record_id_column="id", skip_duplicates=True
        )


def test_a_map_and_a_struct_do_not_pair_their_places() -> None:
    """Places pair only within one role: a map's key never pairs with a struct field
    named ``key``, so a held map and a given struct of other key identifiers are not
    reported as a logical type change at ``x.key`` (their kinds differ; that is the
    database's other checks' to say)."""

    def annotated(meta: bytes, name: str = "key") -> "pa.Field[Any]":
        return pa.field(
            name,
            pa.large_string(),
            nullable=False,
            metadata={NAME: b"itl1371.a", META: meta},
        )

    item = pa.field("item", pa.int64())
    map_field = pa.field("x", pa.map_(annotated(b"m"), item))  # pyright: ignore[reportArgumentType]  # pyarrow takes key/item fields
    struct_field = pa.field("x", pa.struct([annotated(b"n"), item]))
    assert _change(map_field, struct_field) is None
    other_map = pa.field("x", pa.map_(annotated(b"n"), item))  # pyright: ignore[reportArgumentType]  # pyarrow takes key/item fields
    reason = _change(map_field, other_map)
    assert reason is not None and reason.startswith("x.key is of logical type")


def test_delta_merge_source_keeps_the_held_identifier_bytes(tmp_path: Path) -> None:
    """A list-typed extension column appended to a stored table goes to the MERGE (a
    batch with an id already stored) in the stored layout with its identifier: the bytes Arrow holds (the ones the
    identity rule compared), not its serializer asked again, which may answer
    otherwise. The stored identifier stays the one written."""
    import deltalake

    class ListExt(pa.ExtensionType):
        def __init__(self, payload: bytes) -> None:
            self.payload = payload
            super().__init__(pa.large_list(pa.large_string()), "itl1371.lists")

        def __arrow_ext_serialize__(self) -> bytes:
            return self.payload

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: bytes) -> Any:
            return cls(serialized)

    def rows(ext: Any, ids: list[bytes]) -> pa.Table:
        storage = pa.array([["v"]] * len(ids), pa.large_list(pa.large_string()))
        return pa.table(
            {"id": pa.array(ids, pa.large_binary()), "x": ext.wrap_array(storage)}
        )

    db = DeltaTableDatabase(tmp_path / "delta")
    db.add_records(PATH, rows(ListExt(b"m"), [b"1"]), record_id_column="id", flush=True)
    second = ListExt(b"m")
    given = rows(second, [b"1", b"22"])
    second.payload = b"changed"  # the serializer now answers otherwise
    db.add_records(PATH, given, record_id_column="id", flush=True)
    stored = deltalake.DeltaTable(str(tmp_path / "delta" / "t")).schema().fields[1]
    assert metadata_bytes(stored.metadata)[META] == b"m"
    stored_rows = db.get_all_records(PATH, record_id_column="id")
    assert stored_rows is not None and stored_rows.num_rows == 2


@pytest.mark.parametrize("state", ["pending", "committed"])
@pytest.mark.parametrize("kind", KINDS)
def test_duplicate_only_append_of_the_same_identifier_is_a_no_op(
    tmp_path: Path, kind: str, state: str
) -> None:
    """With ``skip_duplicates``, an append whose every row is already held, with the
    same identifier, writes nothing and raises nothing, as on main, in every
    database."""
    with _store(kind, tmp_path) as store:
        store.db.add_records(
            PATH,
            _rows(HELD, "annotated", "top", [b"1", b"22"]),
            record_id_column="id",
            flush=state == "committed",
        )
        store.db.add_records(
            PATH,
            _rows(HELD, "annotated", "top", [b"1", b"22"]),
            record_id_column="id",
            skip_duplicates=True,
        )
        stored = store.stored().sort_by("id")
        assert stored.column("id").to_pylist() == [b"1", b"22"]


# ---------------------------------------------------------------------------
# Review round 4 (PR 492)
# ---------------------------------------------------------------------------


def _map_of_structs(meta: bytes, ids: list[bytes]) -> pa.Table:
    """A map value whose struct child carries the given extension identifier."""
    field, leaf = _leaf(("itl1371.a", meta), "extension", [i.decode() for i in ids])
    struct = pa.StructArray.from_arrays([leaf], fields=[field])
    n = len(ids)
    keys = pa.array([f"k{i}" for i in range(n)], pa.large_string())
    offsets: Any = pa.array(range(n + 1), pa.int32())
    from_arrays: Any = pa.MapArray.from_arrays
    return pa.table(
        {
            "id": pa.array(ids, pa.large_binary()),
            "x": from_arrays(offsets, keys, struct),
        }
    )


@pytest.mark.parametrize("state", ["committed", "reopened"])
def test_delta_struct_children_beneath_maps_are_compared(
    tmp_path: Path, state: str
) -> None:
    """The map value keeps no metadata, but its struct child's identifier is kept."""
    with _store("Delta", tmp_path) as store:
        store.db.add_records(
            PATH,
            _map_of_structs(b"m", [b"1", b"22"]),
            record_id_column="id",
            flush=True,
        )
        if state == "reopened":
            store.reopen()
        for ids, skip in (([b"333"], False), ([b"1", b"22"], True)):
            with pytest.raises(
                InputValidationError, match=r"x\.item\.v is of logical type"
            ):
                store.db.add_records(
                    PATH,
                    _map_of_structs(b"n", ids),
                    record_id_column="id",
                    skip_duplicates=skip,
                )
        stored = store.stored()
        assert sorted(cast(list[bytes], stored.column("id").to_pylist())) == [
            b"1",
            b"22",
        ]


def _late_chunk_rows(metas: list[bytes], ids: list[bytes]) -> pa.Table:
    """``x: struct<v>``, one chunk per id, ``v`` annotated ``itl1371.a;<meta>`` in its
    own chunk: the column's field carries the first chunk's annotation only."""
    chunks = []
    for meta, i in zip(metas, ids):
        field = pa.field(
            "v", pa.large_string(), metadata={NAME: b"itl1371.a", META: meta}
        )
        chunks.append(
            pa.StructArray.from_arrays(
                [pa.array([i.decode()], pa.large_string())], fields=[field]
            )
        )
    column = pa.chunked_array(chunks)
    return pa.Table.from_arrays(
        [pa.array(ids, pa.large_binary()), column],
        schema=pa.schema(
            [pa.field("id", pa.large_binary()), pa.field("x", chunks[0].type)]
        ),
    )


@pytest.mark.parametrize("state", ["pending", "committed"])
@pytest.mark.parametrize("kind", KINDS)
def test_a_later_chunk_annotation_is_compared(
    tmp_path: Path, kind: str, state: str
) -> None:
    """Arrow keeps a nested field's annotation per chunk: a later chunk of another
    annotated identifier is refused, with fresh ids (either duplicate setting) and
    duplicate-only (every id already held, ``skip_duplicates``), in every database;
    chunks of the same identifier are accepted, and the same identifier
    duplicate-only is a no-op (round 4, finding 2; round 5, finding 3)."""
    with _store(kind, tmp_path) as store:
        store.db.add_records(
            PATH,
            _late_chunk_rows([b"m", b"m"], [b"1", b"22"]),
            record_id_column="id",
            flush=state == "committed",
        )
        fresh = _late_chunk_rows([b"m", b"n"], [b"333", b"4444"])
        duplicate_only = _late_chunk_rows([b"m", b"n"], [b"1", b"22"])
        assert [c.type.field("v").metadata[META] for c in fresh.column("x").chunks] == [
            b"m",
            b"n",
        ]
        for given, skip in ((fresh, False), (fresh, True), (duplicate_only, True)):
            with pytest.raises(InputValidationError, match="x.v is of logical type"):
                store.db.add_records(
                    PATH, given, record_id_column="id", skip_duplicates=skip
                )
        store.db.add_records(
            PATH,
            _late_chunk_rows([b"m", b"m"], [b"1", b"22"]),
            record_id_column="id",
            skip_duplicates=True,
        )
        store.db.add_records(
            PATH,
            _late_chunk_rows([b"m", b"m"], [b"333", b"4444"]),
            record_id_column="id",
        )
        stored = store.stored()
        assert sorted(cast(list[bytes], stored.column("id").to_pylist())) == [
            b"1",
            b"22",
            b"333",
            b"4444",
        ]


def _annotated_top(meta: bytes, ids: list[bytes]) -> pa.Table:
    field = pa.field("x", pa.large_string(), metadata={NAME: b"itl1371.a", META: meta})
    return pa.Table.from_arrays(
        [
            pa.array(ids, pa.large_binary()),
            pa.array(["v"] * len(ids), pa.large_string()),
        ],
        schema=pa.schema([pa.field("id", pa.large_binary()), field]),
    )


def test_delta_refusals_are_decided_on_the_table_stored_now(tmp_path: Path) -> None:
    """A handle kept open while the table is replaced (same ids, another identifier):
    every refusal is decided on the version stored now, so the held handle never
    refuses what a reopened one accepts. A fresh-id append the stale handle accepts is
    refused at flush, against the version then loaded, before anything is written.
    The one remaining difference writes nothing either way: a duplicate-only append
    of the identifier the stale version holds is a no-op on the held handle, where a
    reopened handle refuses it."""
    import deltalake

    root = tmp_path / "delta"
    held = DeltaTableDatabase(root)
    held.add_records(
        PATH, _annotated_top(b"m", [b"1", b"22"]), record_id_column="id", flush=True
    )
    replacement = _annotated_top(b"n", [b"1", b"22"]).rename_columns(
        ["__record_id", "x"]
    )
    deltalake.write_deltalake(
        str(root / "t"), replacement, mode="overwrite", schema_mode="overwrite"
    )
    reopened = DeltaTableDatabase(root)

    # The identifier stored now, duplicate-only: accepted (a no-op) by both handles;
    # the held handle's stale log differs, so it decides on the version stored now.
    for db in (held, reopened):
        db.add_records(
            PATH,
            _annotated_top(b"n", [b"1", b"22"]),
            record_id_column="id",
            skip_duplicates=True,
        )
    # The replaced identifier, duplicate-only, on a reopened handle: refused.
    with pytest.raises(
        InputValidationError, match="the stored column's 'itl1371.a;6e'"
    ):
        reopened.add_records(
            PATH,
            _annotated_top(b"m", [b"1", b"22"]),
            record_id_column="id",
            skip_duplicates=True,
        )

    # A fresh handle opened before a second replacement, back to ``m``.
    stale_handle = DeltaTableDatabase(root)
    stale_handle.get_all_records(PATH, record_id_column="id")  # loads the ``n`` version
    deltalake.write_deltalake(
        str(root / "t"),
        _annotated_top(b"m", [b"1", b"22"]).rename_columns(["__record_id", "x"]),
        mode="overwrite",
        schema_mode="overwrite",
    )
    # Duplicate-only ``n`` on the stale handle: a no-op (writes nothing); a reopened
    # handle refuses it.
    stale_handle.add_records(
        PATH,
        _annotated_top(b"n", [b"1", b"22"]),
        record_id_column="id",
        skip_duplicates=True,
    )
    with pytest.raises(InputValidationError):
        DeltaTableDatabase(root).add_records(
            PATH,
            _annotated_top(b"n", [b"1", b"22"]),
            record_id_column="id",
            skip_duplicates=True,
        )
    # Duplicate-only ``m`` (stored now) on the stale handle: its stale log says ``n``,
    # the refusal is decided on the version stored now, ``m``: accepted, a no-op.
    stale_handle.add_records(
        PATH,
        _annotated_top(b"m", [b"1", b"22"]),
        record_id_column="id",
        skip_duplicates=True,
    )
    # A fresh id of ``n`` on a stale handle that accepts it: refused at flush.
    other_stale = DeltaTableDatabase(root)
    other_stale.get_all_records(PATH, record_id_column="id")
    deltalake.write_deltalake(
        str(root / "t"),
        _annotated_top(b"n", [b"1", b"22"]).rename_columns(["__record_id", "x"]),
        mode="overwrite",
        schema_mode="overwrite",
    )
    other_stale.add_records(PATH, _annotated_top(b"m", [b"333"]), record_id_column="id")
    with pytest.raises(
        InputValidationError, match="the stored column's 'itl1371.a;6e'"
    ):
        other_stale.flush()
    final = DeltaTableDatabase(root).get_all_records(PATH, record_id_column="id")
    assert final is not None
    assert sorted(cast(list[bytes], final.column("id").to_pylist())) == [b"1", b"22"]


@pytest.mark.parametrize("kind", ["in-memory", "Delta"])
def test_the_identity_is_refused_before_the_reorder_calls_a_serializer(
    tmp_path: Path, kind: str
) -> None:
    """The rows are reordered by name (ITL-1367) after the identity rule, which reads
    the bytes Arrow holds: a permuted batch of another identifier whose serializer
    raises when asked again is refused naming both identifiers."""

    class Raising(pa.ExtensionType):
        def __init__(self, payload: bytes | None) -> None:
            self.payload = payload
            super().__init__(pa.large_string(), "itl1371.raising")

        def __arrow_ext_serialize__(self) -> bytes:
            if self.payload is None:
                raise RuntimeError("serializer asked again")
            return self.payload

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: bytes) -> Any:
            return cls(serialized)

    def rows(ext: Any, ids: list[bytes]) -> pa.Table:
        x = ext.wrap_array(pa.array(["v"] * len(ids), pa.large_string()))
        return pa.table(
            {
                "id": pa.array(ids, pa.large_binary()),
                "x": x,
                "z": pa.array(range(len(ids)), pa.int64()),
            }
        )

    with _store(kind, tmp_path) as store:
        store.db.add_records(
            PATH, rows(Raising(b"old"), [b"1", b"22"]), record_id_column="id"
        )
        incoming = Raising(b"new")
        permuted = rows(incoming, [b"1", b"22"]).select(["z", "id", "x"])
        incoming.payload = None
        with pytest.raises(InputValidationError) as refused:
            store.db.add_records(
                PATH, permuted, record_id_column="id", skip_duplicates=True
            )
        assert "'itl1371.raising;6e6577'" in str(refused.value)
        assert "'itl1371.raising;6f6c64'" in str(refused.value)


# ---------------------------------------------------------------------------
# Every review finding pinned (developer, 2026-11-22): round 4 limits and the
# declined CodeRabbit item
# ---------------------------------------------------------------------------


def _failing_reader(error: type[Exception], *, readable: bytes | None = None) -> Any:
    """A registered ``itl1371.a`` whose reader cannot read stored metadata."""

    class Failing(pa.ExtensionType):
        def __init__(self) -> None:
            super().__init__(pa.large_string(), "itl1371.a")

        def __arrow_ext_serialize__(self) -> bytes:
            return b"current" if readable is None else readable

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: bytes) -> Any:
            if serialized == readable:
                return cls()
            raise error("itl1371.a cannot read this metadata")

    return Failing


@pytest.mark.parametrize("skip_duplicates", [False, True])
def test_delta_identity_beneath_map_comes_before_an_undecodable_child(
    tmp_path: Path, skip_duplicates: bool
) -> None:
    """Delta compares a map's struct child's identifier before decoding it."""
    from orcapod.errors import TypeIdentityError

    root = tmp_path / "delta"
    DeltaTableDatabase(root).add_records(
        PATH, _map_of_structs(b"m", [b"1"]), record_id_column="id", flush=True
    )
    incoming = _map_of_structs(b"n", [b"1", b"22"])
    pa.register_extension_type(
        cast(Any, _failing_reader(TypeIdentityError, readable=b"n")())
    )
    try:
        with pytest.raises(
            InputValidationError, match=r"x\.item\.v is of logical type"
        ) as refused:
            DeltaTableDatabase(root).add_records(
                PATH, incoming, record_id_column="id", skip_duplicates=skip_duplicates
            )
        assert "the stored column's 'itl1371.a;6d'" in str(refused.value)
    finally:
        pa.unregister_extension_type("itl1371.a")


def test_delta_duplicate_scan_type_identity_error_propagates_as_itself(
    tmp_path: Path,
) -> None:
    """Pins main's ITL-881 carve-out (round 4, finding 6): the same identifier with
    ``skip_duplicates``, over a stored child the registered reader refuses with
    ``TypeIdentityError``: the duplicate scan's decode raises it as itself, not as
    ``OrcapodDatabaseReadError``, in every ``schema_handling`` mode."""
    from orcapod.errors import TypeIdentityError

    root = tmp_path / "delta"
    rows = _rows(HELD, "annotated", "struct", [b"1"])
    DeltaTableDatabase(root).add_records(PATH, rows, record_id_column="id", flush=True)
    pa.register_extension_type(cast(Any, _failing_reader(TypeIdentityError)()))
    try:
        for mode in ("error", "merge", "coerce"):
            with pytest.raises(TypeIdentityError):
                DeltaTableDatabase(root).add_records(
                    PATH,
                    rows,
                    record_id_column="id",
                    skip_duplicates=True,
                    schema_handling=cast(Any, mode),
                )
    finally:
        pa.unregister_extension_type("itl1371.a")


@pytest.mark.parametrize("schema_evolution", [False, True])
@pytest.mark.parametrize("serializer", ["changed", "raising"])
def test_delta_merge_source_with_a_changed_or_raising_serializer(
    tmp_path: Path, serializer: str, schema_evolution: bool
) -> None:
    """Pins the stated difference from main (round 4, finding 5): the MERGE source
    carries the identifier bytes Arrow holds. With a serializer that changed after
    its type was made, the stored identifier stays the held one (main rewrote it);
    with one that raises, the commit goes through under the held bytes (main failed
    it)."""
    import deltalake

    class ListExt(pa.ExtensionType):
        def __init__(self, payload: bytes | None) -> None:
            self.payload = payload
            super().__init__(pa.large_list(pa.large_string()), "itl1371.lists")

        def __arrow_ext_serialize__(self) -> bytes:
            if self.payload is None:
                raise RuntimeError("serializer asked again")
            return self.payload

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: bytes) -> Any:
            return cls(serialized)

    def rows(ext: Any, ids: list[bytes]) -> pa.Table:
        storage = pa.array([["v"]] * len(ids), pa.large_list(pa.large_string()))
        return pa.table(
            {"id": pa.array(ids, pa.large_binary()), "x": ext.wrap_array(storage)}
        )

    db = DeltaTableDatabase(tmp_path / "delta", allow_schema_evolution=schema_evolution)
    db.add_records(PATH, rows(ListExt(b"m"), [b"1"]), record_id_column="id", flush=True)
    second = ListExt(b"m")
    given = rows(second, [b"1", b"22"])
    second.payload = b"changed" if serializer == "changed" else None
    db.add_records(PATH, given, record_id_column="id", flush=True)
    stored = deltalake.DeltaTable(str(tmp_path / "delta" / "t")).schema().fields[1]
    assert metadata_bytes(stored.metadata)[META] == b"m"
    ids = deltalake.DeltaTable(str(tmp_path / "delta" / "t")).to_pyarrow_table()
    assert sorted(cast(list[bytes], ids["__record_id"].to_pylist())) == [b"1", b"22"]


@pytest.mark.parametrize("skip_duplicates", [False, True])
@pytest.mark.parametrize("kind", KINDS)
def test_a_zero_row_append_is_outside_the_rule(
    tmp_path: Path, kind: str, skip_duplicates: bool
) -> None:
    """Pins a stated limit (round 4, finding 7): a zero-row append writes nothing and
    returns before the rule, whatever identifier it carries, as on main."""
    with _store(kind, tmp_path) as store:
        store.db.add_records(
            PATH, _annotated_top(b"m", [b"1"]), record_id_column="id", flush=True
        )
        store.db.add_records(
            PATH,
            _annotated_top(b"n", []),
            record_id_column="id",
            skip_duplicates=skip_duplicates,
        )
        assert store.stored().column("id").to_pylist() == [b"1"]


def test_an_unwritable_encoding_is_refused_before_the_rule(tmp_path: Path) -> None:
    """Pins a stated limit (round 4, finding 7): a column the database cannot write is
    refused by its own check before the identity rule, as on main: Delta refuses
    metadata that is not UTF-8 as unstorable; SQLite refuses any field metadata."""
    from orcapod.databases.sqlite_connector import SQLiteConnector
    from orcapod.errors import UnstorableColumnError

    delta = DeltaTableDatabase(tmp_path / "delta")
    delta.add_records(
        PATH, _annotated_top(b"m", [b"1"]), record_id_column="id", flush=True
    )
    with pytest.raises(UnstorableColumnError):
        delta.add_records(PATH, _annotated_top(b"\xff", [b"22"]), record_id_column="id")

    sqlite = ConnectorArrowDatabase(SQLiteConnector(str(tmp_path / "store.db")))
    plain = pa.table(
        {
            "id": pa.array([b"1"], pa.large_binary()),
            "x": pa.array(["v"], pa.large_string()),
        }
    )
    sqlite.add_records(PATH, plain, record_id_column="id", flush=True)
    with pytest.raises(ValueError, match="does not preserve Arrow metadata"):
        sqlite.add_records(PATH, _annotated_top(b"n", [b"22"]), record_id_column="id")


def test_delta_flush_refuses_after_another_writer_changed_the_identifier(
    tmp_path: Path,
) -> None:
    """Pins the declined CodeRabbit item (flush-time recheck, 11052fab): rows queued
    with ``skip_duplicates``, then another writer stores the same ids under another
    identifier: the flush, which compares against the version it loads, refuses
    the queued rows (they were duplicates the MERGE would not insert; nothing is
    lost and the stored rows are the other writer's)."""
    import deltalake

    root = tmp_path / "delta"
    writer = DeltaTableDatabase(root)
    writer.add_records(
        PATH, _annotated_top(b"m", [b"1"]), record_id_column="id", skip_duplicates=True
    )
    deltalake.write_deltalake(
        str(root / "t"),
        _annotated_top(b"n", [b"1"]).rename_columns(["__record_id", "x"]),
        mode="overwrite",
        schema_mode="overwrite",
    )
    with pytest.raises(
        InputValidationError, match="the stored column's 'itl1371.a;6e'"
    ):
        writer.flush()
    stored = DeltaTableDatabase(root).get_all_records(PATH, record_id_column="id")
    assert stored is not None and stored.column("id").to_pylist() == [b"1"]
    assert identity_node(stored.schema.field("x")).identifier == (b"itl1371.a", b"n")


# ---------------------------------------------------------------------------
# Review round 5 (PR 492)
# ---------------------------------------------------------------------------


def test_delta_handle_after_delete_and_recreate_reads_the_new_tables_ids(
    tmp_path: Path,
) -> None:
    """Round 5, finding 2 (ITL-1413): a held handle whose table was deleted and
    recreated at the same path, at the same Delta version, no longer reuses the old
    table's cached ids, so an old id the new table lacks is stored once, and an id of
    the new table is not stored twice. Here the add of another identifier reopens the
    table, and a reopen drops the snapshot with the handle (review of PR 504, round
    eight); ITL-1377 also keys every snapshot by the Delta table's own id and commits
    every append to an existing table as a MERGE."""
    import shutil

    import deltalake

    root = tmp_path / "delta"
    held = DeltaTableDatabase(root)
    held.add_records(
        PATH, _annotated_top(b"m", [b"a", b"b"]), record_id_column="id", flush=True
    )
    held.add_records(  # caches the version-0 ids
        PATH, _annotated_top(b"m", [b"a"]), record_id_column="id", skip_duplicates=True
    )
    shutil.rmtree(root / "t")
    deltalake.write_deltalake(
        str(root / "t"),
        _annotated_top(b"n", [b"c", b"d"]).rename_columns(["__record_id", "x"]),
    )
    assert deltalake.DeltaTable(str(root / "t")).version() == 0
    held.add_records(
        PATH,
        _annotated_top(b"n", [b"a"]),
        record_id_column="id",
        skip_duplicates=True,
        flush=True,
    )
    held.add_records(
        PATH,
        _annotated_top(b"n", [b"c"]),
        record_id_column="id",
        skip_duplicates=True,
        flush=True,
    )
    stored = DeltaTableDatabase(root).get_all_records(PATH, record_id_column="id")
    assert stored is not None
    assert sorted(cast(list[bytes], stored.column("id").to_pylist())) == [
        b"a",
        b"c",
        b"d",
    ]
