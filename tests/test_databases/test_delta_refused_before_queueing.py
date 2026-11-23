"""What deltalake 1.6 refuses at commit, a ``DeltaTableDatabase`` refuses when rows are added.

A flush that fails and puts its rows back in the queue fails at every later flush too.
So each case deltalake 1.5+ refuses is refused when the rows are added, with nothing
queued (ITL-1403, review of PR 504). The cases:

- two struct fields differing only in case, one queued and one added;
- a recorded column whose record the append would change, in the small list form
  (``pa.list_``) as in the large one;
- a logical column whose identifier (extension name AND metadata) would change;
- a stored table written before deltalake 1.5.1 with two names differing only in case,
  which deltalake 1.6 cannot read;
- names equal only under Delta's case folding, which Python's ``str.lower`` does not
  fold alike: refused at the flush, once, and the rows leave the queue.

A bare ``null`` column appended to a stored logical column claims no type and is
stored.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import deltalake
import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import (
    InputValidationError,
    OrcapodDatabaseReadError,
    UnstorableColumnError,
)

PATH = ("t",)


def _rows(rid: bytes, **columns: Any) -> pa.Table:
    return pa.table({"rid": pa.array([rid], pa.large_binary()), **columns})


def _add(db: DeltaTableDatabase, table: pa.Table, **kwargs: Any) -> None:
    db.add_records(PATH, table, record_id_column="rid", **kwargs)


def _stored_ids(root: Path) -> list[bytes]:
    table = DeltaTableDatabase(root).get_all_records(PATH, record_id_column="rid")
    assert table is not None
    return sorted(cast(list[bytes], table.column("rid").to_pylist()))


def _struct(name: str, value: int) -> Any:
    return pa.array([{name: value}])


# (queued, added): two names differing only in case at each depth, in both orders.
NESTED_CASE_CLASHES: dict[str, tuple[Any, Any]] = {
    "top struct, lower first": (_struct("x", 1), _struct("X", 2)),
    "top struct, upper first": (_struct("X", 1), _struct("x", 2)),
    "struct in a struct": (
        pa.array([{"t": {"x": 1}}]),
        pa.array([{"t": {"X": 2}}]),
    ),
    "struct in a list": (
        pa.array([[{"x": 1}]], pa.large_list(pa.struct([("x", pa.int64())]))),
        pa.array([[{"X": 2}]], pa.large_list(pa.struct([("X", pa.int64())]))),
    ),
}


@pytest.mark.parametrize("case", sorted(NESTED_CASE_CLASHES))
def test_a_struct_field_differing_in_case_from_a_queued_one(
    tmp_path: Path, case: str
) -> None:
    """Review of PR 504, finding 1 (and CodeRabbit): checked on the merged type."""
    queued, added = NESTED_CASE_CLASHES[case]
    db = DeltaTableDatabase(tmp_path)
    _add(db, _rows(b"1", s=queued))
    with pytest.raises(InputValidationError, match="differ only in case"):
        _add(db, _rows(b"2", s=added), schema_handling="merge")
    db.flush()  # the first batch is intact and commits
    assert _stored_ids(tmp_path) == [b"1"]


def _list_of_structs(make_list: Any, *names: str) -> Any:
    return make_list(pa.struct([(n, pa.int64()) for n in names]))


# (stored, appended): an append that changes a recorded column's record.
RECORD_CHANGES: dict[str, tuple[Any, Any]] = {
    "list of structs gains a field": (
        pa.array([[{"a": 1}]], _list_of_structs(pa.list_, "a")),
        pa.array([[{"a": 2, "b": 3}]], _list_of_structs(pa.list_, "a", "b")),
    ),
    "large list of structs gains a field": (
        pa.array([[{"a": 1}]], _list_of_structs(pa.large_list, "a")),
        pa.array([[{"a": 2, "b": 3}]], _list_of_structs(pa.large_list, "a", "b")),
    ),
    "struct holding uint8 gains a field": (
        pa.array([{"a": 1}], pa.struct([("a", pa.uint8())])),
        pa.array([{"a": 2, "b": 3}], pa.struct([("a", pa.uint8()), ("b", pa.int64())])),
    ),
    "list items become nullable": (
        pa.array([[1]], pa.large_list(pa.field("item", pa.uint8(), nullable=False))),
        pa.array([[2]], pa.large_list(pa.field("item", pa.uint8(), nullable=True))),
    ),
    "struct child becomes nullable": (
        pa.array([{"a": 1}], pa.struct([pa.field("a", pa.uint8(), nullable=False)])),
        pa.array([{"a": 2}], pa.struct([pa.field("a", pa.uint8(), nullable=True)])),
    ),
}


@pytest.mark.parametrize("case", sorted(RECORD_CHANGES))
def test_a_recorded_column_whose_record_would_change(tmp_path: Path, case: str) -> None:
    """Review of PR 504, findings 2 and 6: what the commit would send is compared."""
    stored, appended = RECORD_CHANGES[case]
    db = DeltaTableDatabase(tmp_path)
    _add(db, _rows(b"1", x=stored), flush=True)
    with pytest.raises(InputValidationError, match="recorded Arrow type"):
        _add(db, _rows(b"2", x=appended))
    db.flush()  # nothing was queued
    assert _stored_ids(tmp_path) == [b"1"]


class _Unit(pa.ExtensionType):
    def __init__(self, serial: bytes = b"x") -> None:
        self.serial = serial
        super().__init__(pa.int64(), "itl1403.review.unit")

    def __arrow_ext_serialize__(self) -> bytes:
        return self.serial

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: bytes) -> "_Unit":
        return cls(serialized)


@pytest.fixture
def unit_type() -> Any:
    pa.register_extension_type(_Unit())  # pyright: ignore[reportArgumentType]  # pyarrow's stub names only PyExtensionType; any ExtensionType registers
    yield _Unit
    pa.unregister_extension_type("itl1403.review.unit")


@pytest.mark.parametrize(
    ("first", "second"),
    [(b"x", b"y"), (b"", b"y"), (b"x", b"")],
    ids=["other metadata", "metadata added", "metadata dropped"],
)
def test_a_logical_column_with_other_extension_metadata(
    tmp_path: Path, unit_type: Any, first: bytes, second: bytes
) -> None:
    """Review of PR 504, finding 3 (and CodeRabbit): the same extension name and
    storage with other metadata is another logical type."""

    def unit(value: int, serial: bytes) -> Any:
        return pa.ExtensionArray.from_storage(
            unit_type(serial), pa.array([value], pa.int64())
        )

    db = DeltaTableDatabase(tmp_path)
    _add(db, _rows(b"1", x=unit(1, first)), flush=True)
    with pytest.raises(
        InputValidationError, match="logical type 'itl1403.review.unit'"
    ):
        _add(db, _rows(b"2", x=unit(2, second)))
    _add(db, _rows(b"3", x=unit(3, first)), flush=True)  # the same identifier appends
    assert _stored_ids(tmp_path) == [b"1", b"3"]


def _path_column() -> Any:
    converter = get_default_context().type_converter
    table = converter.python_dicts_to_arrow_table(
        [{"x": Path("/a")}], python_schema={"x": Path | None}
    )
    return table.schema.field("x"), table.column("x"), "/a"


def _unit_column() -> Any:
    array = pa.ExtensionArray.from_storage(_Unit(b"x"), pa.array([7], pa.int64()))
    return pa.field("x", array.type), array, 7


@pytest.mark.parametrize(
    "make", [_path_column, _unit_column], ids=["orcapod.path", "extension"]
)
def test_a_bare_null_appended_to_a_logical_column_is_stored(
    tmp_path: Path, unit_type: Any, make: Any
) -> None:
    """Review of PR 504, finding 5 (and CodeRabbit): a bare null claims no type."""
    field, column, value = make()
    db = DeltaTableDatabase(tmp_path)
    _add(db, _rows(b"1").append_column(field, column), flush=True)
    _add(db, _rows(b"2", x=pa.nulls(1)), flush=True)
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="rid")
    assert back is not None
    assert back.schema.field("x").type == field.type
    assert back.sort_by("rid").column("x").to_pylist() == [value, None]


def _legacy_case_only_table(root: Path) -> None:
    """A table with ``a`` and ``A``, as deltalake before 1.5.1 wrote it."""
    deltalake.write_deltalake(
        str(root / "t"),
        pa.table(
            {"__record_id": pa.array([b"1"], pa.large_binary()), "a": [1], "b": [2]}
        ),
    )
    # Rename ``b`` to ``A`` in the log's schema (deltalake 1.6 cannot write it).
    log = root / "t" / "_delta_log" / "00000000000000000000.json"
    lines = [json.loads(line) for line in log.read_text().splitlines()]
    for action in lines:
        if "metaData" in action:
            schema = json.loads(action["metaData"]["schemaString"])
            schema["fields"][2]["name"] = "A"
            action["metaData"]["schemaString"] = json.dumps(schema)
    log.write_text("\n".join(json.dumps(action) for action in lines) + "\n")


def test_a_stored_table_with_case_only_names_cannot_be_read(tmp_path: Path) -> None:
    """Review of PR 504, finding 7, the stated limit: deltalake 1.6 reads no such
    table (the release note says so)."""
    _legacy_case_only_table(tmp_path)
    with pytest.raises(OrcapodDatabaseReadError):
        DeltaTableDatabase(tmp_path).get_all_records(PATH)


@pytest.mark.parametrize(
    "columns",
    [{"a": [3]}, {"b": [3]}, {"a": [3], "c": [4]}],
    ids=["a held name", "a new name", "a held and a new name"],
)
def test_a_stored_table_deltalake_can_no_longer_read_refuses_at_once(
    tmp_path: Path, columns: dict[str, list[int]]
) -> None:
    """Review of PR 504, finding 7: an append is refused when added, with nothing
    queued, never left to fail at every flush."""
    _legacy_case_only_table(tmp_path)
    db = DeltaTableDatabase(tmp_path)
    with pytest.raises(
        InputValidationError, match="deltalake 1.5.1 and later cannot read"
    ):
        _add(db, _rows(b"2", **{k: pa.array(v) for k, v in columns.items()}))
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
    db.flush()


# Pairs Delta's case folding (Rust ``to_lowercase``) makes one name and Python 3.12's
# ``str.lower`` does not (found by the review of PR 504, finding 4).
RUST_ONLY_FOLDS = [
    ("\u1c89", "\u1c8a"),
    ("\ua7cb", "\u0264"),
    ("\ua7cc", "\ua7cd"),
    ("\ua7da", "\ua7db"),
    ("\ua7ce", "\ua7cf"),
    ("\U00016ea0", "\U00016ebb"),
]


@pytest.mark.parametrize("nested", [False, True], ids=["columns", "struct fields"])
@pytest.mark.parametrize(
    "pair", RUST_ONLY_FOLDS, ids=[ascii(a) for a, _ in RUST_ONLY_FOLDS]
)
def test_names_folded_alike_only_by_delta_are_refused_once(
    tmp_path: Path, pair: tuple[str, str], nested: bool
) -> None:
    """Whichever check sees it first (``str.lower`` when added, Delta's error at the
    flush) refuses it with ``UnstorableColumnError``, and nothing stays queued."""
    first, second = pair
    if nested:
        rows = _rows(b"1", s=pa.array([{first: 1, second: 2}]))
    else:
        rows = _rows(b"1", **{first: pa.array([1]), second: pa.array([2])})
    db = DeltaTableDatabase(tmp_path)
    with pytest.raises(UnstorableColumnError):
        _add(db, rows, flush=True)
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
    db.flush()
    assert not (tmp_path / "t" / "_delta_log").exists()


def test_a_storage_typed_column_carrying_another_extension_name(tmp_path: Path) -> None:
    """A logical identity given as ``ARROW:extension:*`` field metadata on a storage
    type is compared as an extension type's is: another name is refused when added."""

    def rows(rid: bytes, name: str) -> pa.Table:
        field = pa.field(
            "x",
            pa.int64(),
            metadata={"ARROW:extension:name": name, "ARROW:extension:metadata": ""},
        )
        return pa.Table.from_arrays(
            [pa.array([rid], pa.large_binary()), pa.array([1])],
            schema=pa.schema([pa.field("rid", pa.large_binary()), field]),
        )

    db = DeltaTableDatabase(tmp_path)
    _add(db, rows(b"1", "itl1403.one"), flush=True)
    with pytest.raises(InputValidationError, match="'itl1403.two'"):
        _add(db, rows(b"2", "itl1403.two"))
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
    assert _stored_ids(tmp_path) == [b"1"]
