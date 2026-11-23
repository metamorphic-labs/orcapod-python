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
from orcapod.errors import InputValidationError, UnstorableColumnError

PATH = ("t",)


def _rows(rid: bytes, **columns: Any) -> pa.Table:
    return pa.table({"rid": pa.array([rid], pa.large_binary()), **columns})


def _add(db: DeltaTableDatabase, table: pa.Table, **kwargs: Any) -> None:
    db.add_records(PATH, table, record_id_column="rid", **kwargs)


def _stored_ids(root: Path) -> list[bytes]:
    table = DeltaTableDatabase(root).get_all_records(PATH, record_id_column="rid")
    assert table is not None
    return sorted(cast(list[bytes], table.column("rid").to_pylist()))


def test_a_struct_field_differing_in_case_from_a_queued_one(tmp_path: Path) -> None:
    db = DeltaTableDatabase(tmp_path)
    _add(db, _rows(b"1", s=pa.array([{"x": 1}])))
    with pytest.raises(InputValidationError, match="'x' and 'X' differ only in case"):
        _add(db, _rows(b"2", s=pa.array([{"X": 2}])), schema_handling="merge")
    db.flush()  # the first batch is intact and commits
    assert _stored_ids(tmp_path) == [b"1"]


@pytest.mark.parametrize(
    "make_list", [pa.list_, pa.large_list], ids=["list", "large_list"]
)
def test_a_recorded_list_of_structs_cannot_evolve(
    tmp_path: Path, make_list: Any
) -> None:
    db = DeltaTableDatabase(tmp_path)
    _add(
        db,
        _rows(
            b"1", x=pa.array([[{"a": 1}]], make_list(pa.struct([("a", pa.int64())])))
        ),
        flush=True,
    )
    evolved = make_list(pa.struct([("a", pa.int64()), ("b", pa.int64())]))
    with pytest.raises(InputValidationError, match="recorded Arrow type"):
        _add(db, _rows(b"2", x=pa.array([[{"a": 2, "b": 3}]], evolved)))
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


def test_a_logical_column_with_other_extension_metadata(
    tmp_path: Path, unit_type: Any
) -> None:
    """The same extension name and storage, other metadata: another logical type."""

    def unit(value: int, serial: bytes) -> Any:
        return pa.ExtensionArray.from_storage(
            unit_type(serial), pa.array([value], pa.int64())
        )

    db = DeltaTableDatabase(tmp_path)
    _add(db, _rows(b"1", x=unit(1, b"x")), flush=True)
    with pytest.raises(
        InputValidationError, match="logical type 'itl1403.review.unit'"
    ):
        _add(db, _rows(b"2", x=unit(2, b"y")))
    _add(db, _rows(b"3", x=unit(3, b"x")), flush=True)  # the same identifier appends
    assert _stored_ids(tmp_path) == [b"1", b"3"]


def test_a_bare_null_appended_to_a_logical_column_is_stored(tmp_path: Path) -> None:
    converter = get_default_context().type_converter
    logical = converter.python_dicts_to_arrow_table(
        [{"x": Path("/a")}], python_schema={"x": Path | None}
    )
    first = _rows(b"1").append_column(logical.schema.field("x"), logical.column("x"))
    db = DeltaTableDatabase(tmp_path)
    _add(db, first, flush=True)
    _add(db, _rows(b"2", x=pa.nulls(1)), flush=True)
    back = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="rid")
    assert back is not None
    assert back.schema.field("x").type == logical.schema.field("x").type
    assert back.sort_by("rid").column("x").to_pylist() == ["/a", None]


def test_a_stored_table_deltalake_can_no_longer_read_refuses_at_once(
    tmp_path: Path,
) -> None:
    """A table with ``a`` and ``A``, as deltalake before 1.5.1 wrote it: an append of
    ``a`` alone is refused when added, with nothing queued, not at every flush."""
    deltalake.write_deltalake(
        str(tmp_path / "t"),
        pa.table(
            {"__record_id": pa.array([b"1"], pa.large_binary()), "a": [1], "b": [2]}
        ),
    )
    # Rename ``b`` to ``A`` in the log's schema: the table deltalake 1.0.2 would write.
    log = tmp_path / "t" / "_delta_log" / "00000000000000000000.json"
    lines = [json.loads(line) for line in log.read_text().splitlines()]
    for action in lines:
        if "metaData" in action:
            schema = json.loads(action["metaData"]["schemaString"])
            schema["fields"][2]["name"] = "A"
            action["metaData"]["schemaString"] = json.dumps(schema)
    log.write_text("\n".join(json.dumps(action) for action in lines) + "\n")

    db = DeltaTableDatabase(tmp_path)
    with pytest.raises(
        InputValidationError, match="deltalake 1.5.1 and later cannot read"
    ):
        _add(db, _rows(b"2", a=pa.array([3])))
    assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
    db.flush()


def test_names_folded_alike_only_by_delta_are_refused_once(tmp_path: Path) -> None:
    """U+1C89 and U+1C8A: Delta's folding (Rust ``to_lowercase``) makes them one name,
    Python's ``str.lower`` (3.12) does not. Whichever check sees it first refuses it
    with ``UnstorableColumnError``, and nothing stays queued."""
    first, second = "Ᲊ", "ᲊ"
    db = DeltaTableDatabase(tmp_path)
    with pytest.raises(UnstorableColumnError):
        _add(
            db, _rows(b"1", **{first: pa.array([1]), second: pa.array([2])}), flush=True
        )
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
