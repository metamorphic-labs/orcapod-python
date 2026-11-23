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


def _refused_once_at_flush(db: DeltaTableDatabase) -> InputValidationError:
    """The flush is the point of truth (ITL-1403): the rows were accepted when added;
    Delta refuses the commit, and the flush raises once, naming the table, Delta's
    reason and the rows not written; nothing stays queued; a second flush is a no-op."""
    assert db._pending_batches  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(InputValidationError, match="Delta refused the commit") as info:
        db.flush()
    assert "were not written and are no longer queued" in str(info.value)
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
    db.flush()  # a no-op
    return info.value


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
def test_a_recorded_column_whose_record_changes_is_refused_once_at_flush(
    tmp_path: Path, case: str
) -> None:
    """Review of PR 504, findings 2 and 6: Delta refuses the changed record; the flush
    raises once and the batch leaves the queue."""
    stored, appended = RECORD_CHANGES[case]
    db = DeltaTableDatabase(tmp_path)
    _add(db, _rows(b"1", x=stored), flush=True)
    _add(db, _rows(b"2", x=appended))  # accepted when added
    _refused_once_at_flush(db)
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
        InputValidationError, match="logical type 'itl1403.review.unit;"
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
    type (CodeRabbit on PR 504): another identifier, refused when added by ITL-1371's
    rule (PR 492), nothing queued."""

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
    with pytest.raises(InputValidationError, match="itl1403.two"):
        _add(db, rows(b"2", "itl1403.two"))  # ITL-1371: refused when added
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
    assert _stored_ids(tmp_path) == [b"1"]


# ── Second review of PR 504 (N2-N5): refuse only what the commit would refuse ──────

ROUTES = {
    "every id new": [b"2"],
    "a stored id among them": [b"1", b"2"],
}


def _with_record_rewritten(root: Path, column: str, rewrite: Any) -> None:
    """Rewrite one column's field metadata in the table's first log entry."""
    log = root / "t" / "_delta_log" / "00000000000000000000.json"
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    for action in actions:
        if "metaData" in action:
            schema = json.loads(action["metaData"]["schemaString"])
            for field in schema["fields"]:
                if field["name"] == column:
                    rewrite(field["metadata"])
            action["metaData"]["schemaString"] = json.dumps(schema)
    log.write_text("\n".join(json.dumps(action) for action in actions) + "\n")


@pytest.mark.parametrize(
    "evolution", [False, True], ids=["evolution off", "evolution on"]
)
@pytest.mark.parametrize("route", sorted(ROUTES))
def test_a_record_that_decodes_to_the_stored_type_is_not_a_change(
    tmp_path: Path, route: str, evolution: bool
) -> None:
    """N2: a stored record serialized otherwise (a trailing newline in its base64)
    decodes to the same ``uint8``: the append is accepted and sent as stored, on every
    commit route, as main stored it."""
    db = DeltaTableDatabase(tmp_path, allow_schema_evolution=evolution)
    _add(db, _rows(b"1", x=pa.array([1], pa.uint8())), flush=True)

    def add_newline(metadata: dict[str, Any]) -> None:
        metadata["orcapod:arrow_type"] += "\n"

    _with_record_rewritten(tmp_path, "x", add_newline)
    db = DeltaTableDatabase(tmp_path, allow_schema_evolution=evolution)
    ids = ROUTES[route]
    db.add_records(
        PATH,
        pa.table(
            {
                "rid": pa.array(ids, pa.large_binary()),
                "x": pa.array(list(range(1, len(ids) + 1)), pa.uint8()),
            }
        ),
        record_id_column="rid",
        skip_duplicates=len(ids) == 1,
        flush=True,
    )
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="rid")
    assert back is not None
    assert back.schema.field("x").type == pa.uint8()
    assert sorted(cast(list[bytes], back.column("rid").to_pylist())) == [b"1", b"2"]


@pytest.mark.parametrize(
    "evolution", [False, True], ids=["evolution off", "evolution on"]
)
@pytest.mark.parametrize("route", sorted(ROUTES))
def test_a_real_record_change_is_refused_at_flush_only_where_delta_refuses_it(
    tmp_path: Path, route: str, evolution: bool
) -> None:
    """N2: list items widening to nullable (non-null values). A MERGE that evolves the
    schema sends the new record, which deltalake refuses: refused when added. Without
    schema evolution the MERGE keeps the stored schema and commits, as main did."""
    stored = pa.large_list(pa.field("item", pa.uint8(), nullable=False))
    wider = pa.large_list(pa.field("item", pa.uint8(), nullable=True))
    db = DeltaTableDatabase(tmp_path, allow_schema_evolution=evolution)
    _add(db, _rows(b"1", x=pa.array([[1]], stored)), flush=True)
    ids = ROUTES[route]
    rows = pa.table(
        {
            "rid": pa.array(ids, pa.large_binary()),
            "x": pa.array([[n] for n in range(len(ids))], wider),
        }
    )

    db.add_records(
        PATH, rows, record_id_column="rid", skip_duplicates=len(ids) == 1
    )  # accepted when added, on every route
    if evolution:
        _refused_once_at_flush(db)
        assert _stored_ids(tmp_path) == [b"1"]
    else:
        db.flush()
        assert _stored_ids(tmp_path) == [b"1", b"2"]
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize(
    "evolution", [False, True], ids=["evolution off", "evolution on"]
)
@pytest.mark.parametrize("route", sorted(ROUTES))
@pytest.mark.parametrize(
    "variant",
    [
        "no metadata",
        "the stored identifier",
        "a readable older identifier",
        "another name",
    ],
)
def test_a_null_column_is_checked_like_any_column(
    tmp_path: Path, variant: str, route: str, evolution: bool
) -> None:
    """N3: a null column with no metadata takes the stored type; one carrying an
    identifier the stored logical type reads is written under the stored identifier;
    one carrying another type's identifier is refused when added wherever the commit
    would send it (a MERGE that evolves the schema), and stored where the MERGE keeps
    the stored schema. Nothing reaches a flush that Delta refuses."""
    field, column, _ = _path_column()
    path_type = field.type
    identifiers: dict[str, dict[str, Any] | None] = {
        "no metadata": None,
        "the stored identifier": {
            "ARROW:extension:name": path_type.extension_name,
            "ARROW:extension:metadata": path_type.__arrow_ext_serialize__(),
        },
        "a readable older identifier": {
            "ARROW:extension:name": path_type.extension_name,
            "ARROW:extension:metadata": b"",
        },
        "another name": {
            "ARROW:extension:name": "itl1403.not.a.path",
            "ARROW:extension:metadata": b"",
        },
    }
    db = DeltaTableDatabase(tmp_path, allow_schema_evolution=evolution)
    _add(db, _rows(b"1").append_column(field, column), flush=True)
    ids = ROUTES[route]
    null_field = pa.field("x", pa.null(), metadata=identifiers[variant])
    rows = pa.table({"rid": pa.array(ids, pa.large_binary())}).append_column(
        null_field, pa.nulls(len(ids))
    )

    if variant in ("a readable older identifier", "another name"):
        # Another identifier is ITL-1371's rule (on main, PR 492): refused when the
        # rows are added, on every route, whatever schema evolution says.
        with pytest.raises(InputValidationError, match="logical type"):
            db.add_records(
                PATH, rows, record_id_column="rid", skip_duplicates=len(ids) == 1
            )
        assert _stored_ids(tmp_path) == [b"1"]
    else:
        db.add_records(
            PATH, rows, record_id_column="rid", skip_duplicates=len(ids) == 1
        )
        db.flush()
        back = DeltaTableDatabase(tmp_path).get_all_records(
            PATH, record_id_column="rid"
        )
        assert back is not None
        assert back.schema.field("x").type == path_type
        assert back.sort_by("rid").column("x").to_pylist() == ["/a", None]
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("nested", [False, True], ids=["column", "struct field"])
def test_an_all_duplicate_add_is_skipped_not_refused(
    tmp_path: Path, nested: bool
) -> None:
    """N4: names are checked against the queued rows after duplicates are skipped: a
    row whose id is stored, skipped with ``skip_duplicates``, queues nothing, whatever
    its names."""
    db = DeltaTableDatabase(tmp_path)

    def add(name: str, rid: bytes, flush: bool = False) -> None:
        data = {"s": pa.array([{name: 1}])} if nested else {name: pa.array([1])}
        db.add_records(
            PATH,
            _rows(rid, **data),
            record_id_column="rid",
            schema_handling="merge",
            skip_duplicates=True,
            flush=flush,
        )

    add("a", b"stored", flush=True)
    add("a", b"pending")
    add("A", b"stored")  # a stored id: skipped, not refused
    db.flush()
    assert _stored_ids(tmp_path) == [b"pending", b"stored"]


@pytest.mark.parametrize(
    "suffix", ["'q", " space", '"dq', "(x)", "::y", "\nsecond line"]
)
def test_the_backstop_names_the_column_whole(tmp_path: Path, suffix: str) -> None:
    """N5: the column named by the flush's case-folding refusal is the batch's own
    field, with its type, also when its name holds a quote."""
    first, second = RUST_ONLY_FOLDS[0][0] + suffix, RUST_ONLY_FOLDS[0][1] + suffix
    db = DeltaTableDatabase(tmp_path)
    with pytest.raises(UnstorableColumnError) as info:
        _add(
            db, _rows(b"1", **{first: pa.array([1]), second: pa.array([2])}), flush=True
        )
    assert info.value.column in (first, second)
    assert "int64" in str(info.value)


# ── Third review of PR 504: the flush is the point of truth ────────────────────────


@pytest.mark.parametrize(
    "evolution", [False, True], ids=["evolution off", "evolution on"]
)
def test_a_null_with_a_foreign_identifier_after_a_queued_bare_null(
    tmp_path: Path, evolution: bool
) -> None:
    """Third review: a bare null queued, then a null carrying a foreign identifier.
    The foreign identifier is refused when added by ITL-1371's rule (PR 492, on main),
    which judges each add on its own; the queued bare null is untouched and stored."""
    field, column, _ = _path_column()
    db = DeltaTableDatabase(tmp_path, allow_schema_evolution=evolution)
    _add(db, _rows(b"1").append_column(field, column), flush=True)
    _add(db, _rows(b"2").append_column(pa.field("x", pa.null()), pa.nulls(1)))
    foreign = pa.field(
        "x",
        pa.null(),
        metadata={
            "ARROW:extension:name": "review.different",
            "ARROW:extension:metadata": b"",
        },
    )
    with pytest.raises(InputValidationError, match="review.different"):
        _add(db, _rows(b"3").append_column(foreign, pa.nulls(1)))
    db.flush()
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="rid")
    assert back is not None
    assert back.schema.field("x").type == field.type
    assert back.sort_by("rid").column("x").to_pylist() == ["/a", None]


def test_the_backstop_names_the_nested_field_beside_a_column_of_the_same_name(
    tmp_path: Path,
) -> None:
    """Third review: Delta's own schema check locates the collision, so the field named
    is the nested one, with its type, not a top-level column of the same name."""
    upper, lower = RUST_ONLY_FOLDS[0]
    db = DeltaTableDatabase(tmp_path)
    for rid, name in ((b"1", upper), (b"2", lower)):
        table = pa.table(
            {
                "rid": pa.array([rid], pa.large_binary()),
                lower: [1],
                "s": pa.array([{name: "value"}], pa.struct([(name, pa.string())])),
            }
        )
        db.add_records(PATH, table, record_id_column="rid", schema_handling="merge")
    with pytest.raises(UnstorableColumnError) as info:
        db.flush()
    assert info.value.column == f"s.{lower}"
    assert "string" in str(info.value)
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize(
    "change", ["another writer deletes a stored row", "schema evolution turned on"]
)
def test_what_changes_between_add_and_flush_is_decided_at_the_flush(
    tmp_path: Path, change: str
) -> None:
    """Third review: what holds when the rows are added may change before the flush.
    List items widening to nullable (non-null values) with schema evolution off: a
    MERGE that keeps the stored schema stores them. When schema evolution is turned on
    before the flush, the commit sends the changed record and Delta refuses it: the
    flush raises once, nothing stays queued. When another writer deletes a stored row,
    the commit is still decided at the flush, and the rows are stored."""
    stored = pa.large_list(pa.field("item", pa.uint8(), nullable=False))
    wider = pa.large_list(pa.field("item", pa.uint8(), nullable=True))
    db = DeltaTableDatabase(tmp_path, allow_schema_evolution=False)
    _add(db, _rows(b"1", x=pa.array([[1]], stored)), flush=True)
    rows = pa.table(
        {
            "rid": pa.array([b"1", b"2"], pa.large_binary()),
            "x": pa.array([[1], [2]], wider),
        }
    )
    db.add_records(PATH, rows, record_id_column="rid")  # accepted when added
    if change == "another writer deletes a stored row":
        deltalake.DeltaTable(str(tmp_path / "t")).delete("__record_id = X'31'")
        db.flush()
        assert _stored_ids(tmp_path) == [b"1", b"2"]
        assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
    else:
        db.allow_schema_evolution = True
        _refused_once_at_flush(db)


def test_the_backstop_names_the_struct_that_holds_the_pair(tmp_path: Path) -> None:
    """Review of PR 504, round four, finding 7: two structs under one column, the
    colliding pair in ``outer.right`` and an unrelated field of the same name in
    ``outer.left``: the field named is the one in the struct Delta refuses."""
    upper, lower = RUST_ONLY_FOLDS[0]
    db = DeltaTableDatabase(tmp_path)
    for rid, name in ((b"1", upper), (b"2", lower)):
        outer = pa.struct(
            [
                ("left", pa.struct([(lower, pa.int64())])),
                ("right", pa.struct([(name, pa.string())])),
            ]
        )
        table = pa.table(
            {
                "rid": pa.array([rid], pa.large_binary()),
                "outer": pa.array([{"left": {lower: 1}, "right": {name: "v"}}], outer),
            }
        )
        db.add_records(PATH, table, record_id_column="rid", schema_handling="merge")
    with pytest.raises(UnstorableColumnError) as info:
        db.flush()
    assert info.value.column == f"outer.right.{lower}"
    assert "string" in str(info.value)
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]


def test_the_backstop_names_the_column_colliding_with_a_stored_one(
    tmp_path: Path,
) -> None:
    """Review of PR 504, round five, finding 3: the pair forms at the commit, between a
    stored column and the batch's column (Delta's case folding only). The batch's own
    column is named, with its type."""
    upper, lower = RUST_ONLY_FOLDS[0]
    db = DeltaTableDatabase(tmp_path)
    _add(db, _rows(b"1", **{upper: pa.array([1])}), flush=True)
    with pytest.raises(UnstorableColumnError) as info:
        _add(db, _rows(b"2", **{lower: pa.array([2], pa.int32())}), flush=True)
    assert info.value.column == lower
    assert "int32" in str(info.value)
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]


# (stored, added, what another writer stores instead): a name the added rows hold that
# differs only in case from a stored one, as a column and as a struct field.
STALE_CLASHES: dict[str, tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = {
    "column": ({"a": pa.array([1])}, {"A": pa.array([2])}, {"b": pa.array([1])}),
    "struct field": (
        {"s": pa.array([{"a": 1, "c": 1}])},
        {"s": pa.array([{"A": 2, "c": 2}])},
        {"s": pa.array([{"b": 1, "c": 1}])},
    ),
}


@pytest.mark.parametrize("replaced", [True, False], ids=["replaced", "unchanged"])
@pytest.mark.parametrize("kind", sorted(STALE_CLASHES))
def test_a_clash_with_a_cached_version_is_checked_against_the_stored_one(
    tmp_path: Path, kind: str, replaced: bool
) -> None:
    """Review of PR 504, round seven: the database holds a cached handle of the table;
    another writer replaces the clashing name. The rows are checked again against the
    version stored now before they are refused: accepted and stored when the clash is
    gone, refused (nothing queued) when it is still there."""
    stored, added, other = STALE_CLASHES[kind]
    anchor = {"anchor": pa.array([1])}
    db = DeltaTableDatabase(tmp_path, allow_schema_evolution=True)
    _add(db, _rows(b"1", **anchor, **stored), flush=True)
    db.get_all_records(PATH, record_id_column="rid")  # the handle is cached
    assert db._delta_table_cache  # pyright: ignore[reportPrivateUsage]
    if replaced:
        current = deltalake.DeltaTable(str(tmp_path / "t")).to_pyarrow_table()
        names = [n for n in current.column_names if n not in stored]
        deltalake.write_deltalake(
            str(tmp_path / "t"),
            current.select(names).append_column(
                *next(iter((pa.field(k, v.type), v) for k, v in other.items()))
            ),
            mode="overwrite",
            schema_mode="overwrite",
        )
        _add(db, _rows(b"2", **anchor, **added), flush=True)
        assert _stored_ids(tmp_path) == [b"1", b"2"]
    else:
        with pytest.raises(InputValidationError, match="differ only in case"):
            _add(db, _rows(b"2", **anchor, **added))
        assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
        assert _stored_ids(tmp_path) == [b"1"]


@pytest.mark.parametrize(
    "names",
    [["a", "A", "A"], ["A", "a", "a"], ["a", "b", "A", "b"]],
    ids=lambda names: "".join(names),
)
def test_a_case_clash_beside_a_repeated_name_names_the_column(
    tmp_path: Path, names: list[str]
) -> None:
    """Review of PR 511: with a name repeated, the case clash is still refused with
    ``UnstorableColumnError``, naming the column and the type at its position."""
    types = [pa.int8(), pa.int16(), pa.int32(), pa.int64()][: len(names)]
    table = pa.Table.from_arrays(
        [pa.array([b"1"], pa.large_binary())] + [pa.array([1], t) for t in types],
        names=["rid", *names],
    )
    db = DeltaTableDatabase(tmp_path)
    with pytest.raises(UnstorableColumnError) as info:
        _add(db, table)
    position = next(
        i
        for i, n in enumerate(names)
        if any(m.lower() == n.lower() and m != n for m in names[:i])
    )
    assert info.value.column == names[position]
    assert str(types[position]) in str(info.value)
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
