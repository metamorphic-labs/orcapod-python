"""Regression tests for ITL-884: a Delta table that cannot be read is never "no rows".

Covers:
- A table that exists but cannot be read raises ``StoreReadError`` on every read
  path, naming the table; a table that does not exist still reads as ``None``.
- A table whose log cannot be loaded is never overwritten by a flush.
- An append whose column types differ from the stored table is refused before
  anything is written, naming the column and both types; legitimate appends
  (same types in another layout, new or missing columns, unchanged extension
  storage) still succeed.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import deltalake
import pyarrow as pa
import pytest

from orcapod.databases import DeltaTableDatabase
from orcapod.errors import InputValidationError, StoreReadError

PATH = ("results", "t")


def rows(ids: list[bytes], x: pa.Array, **extra: pa.Array) -> pa.Table:
    """Build a table keyed by ``rid`` with an ``x`` column and optional extras."""
    return pa.table({"rid": pa.array(ids, pa.large_binary()), "x": x, **extra})


def table_dir(base: Path) -> Path:
    return base.joinpath(*PATH)


def snapshot(directory: Path) -> dict[str, bytes]:
    """Every file under ``directory`` with its bytes, keyed by relative path."""
    return {
        str(p.relative_to(directory)): p.read_bytes()
        for p in sorted(directory.rglob("*"))
        if p.is_file()
    }


class _Ext(pa.ExtensionType):
    """A minimal extension type standing in for a logical type's storage."""

    def __init__(self, storage: pa.DataType) -> None:
        super().__init__(storage, "orcapod.test.itl884")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> "_Ext":
        return cls(storage_type)


def ext_array(values: list, storage: pa.DataType) -> pa.ExtensionArray:
    return pa.ExtensionArray.from_storage(_Ext(storage), pa.array(values, storage))


@pytest.fixture
def base(tmp_path: Path) -> Path:
    return tmp_path / "store"


@pytest.fixture
def stored(base: Path) -> DeltaTableDatabase:
    """A store holding two rows (``a``, ``b``) of ``x: int64`` at ``PATH``."""
    db = DeltaTableDatabase(base_path=base)
    db.add_records(
        PATH,
        rows([b"a", b"b"], pa.array([1, 2], pa.int64())),
        record_id_column="rid",
        flush=True,
    )
    return db


@pytest.fixture
def unreadable(base: Path, stored: DeltaTableDatabase) -> DeltaTableDatabase:
    """A fresh view of a table whose log is intact but whose data files are gone."""
    data_files = list(table_dir(base).glob("*.parquet"))
    assert data_files
    for f in data_files:
        f.unlink()
    return DeltaTableDatabase(base_path=base)


def unsupported_reader_feature(base: Path) -> None:
    """Make the table's log require a reader feature deltalake does not know."""
    log = table_dir(base) / "_delta_log" / "00000000000000000000.json"
    actions = [json.loads(line) for line in log.read_text().splitlines()]
    for action in actions:
        if "protocol" in action:
            action["protocol"] = {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["orcapodMadeUpFeature"],
                "writerFeatures": ["orcapodMadeUpFeature"],
            }
    log.write_text("\n".join(json.dumps(a) for a in actions))


def assert_names_table(exc: StoreReadError, base: Path) -> None:
    assert Path(exc.table) == table_dir(base)
    assert exc.table in str(exc)
    assert exc.__cause__ is not None


# ---------------------------------------------------------------------------
# Reads: unreadable table raises, absent table is None
# ---------------------------------------------------------------------------

READS: dict[str, Callable[[DeltaTableDatabase], object]] = {
    "get_all_records": lambda db: db.get_all_records(PATH),
    "get_records_by_ids": lambda db: db.get_records_by_ids(PATH, [b"a"]),
    "get_record_by_id": lambda db: db.get_record_by_id(PATH, b"a"),
    "get_records_with_column_value": lambda db: db.get_records_with_column_value(
        PATH, {"x": 1}
    ),
}


class TestUnreadableTable:
    @pytest.mark.parametrize("read", READS.values(), ids=list(READS))
    def test_read_raises_store_read_error(self, base, unreadable, read):
        with pytest.raises(StoreReadError) as info:
            read(unreadable)
        assert_names_table(info.value, base)

    def test_skip_duplicates_append_raises_store_read_error(self, base, unreadable):
        # skip_duplicates reads the stored record ids before buffering the rows.
        with pytest.raises(StoreReadError) as info:
            unreadable.add_records(
                PATH,
                rows([b"c"], pa.array([3], pa.int64())),
                record_id_column="rid",
                skip_duplicates=True,
            )
        assert_names_table(info.value, base)

    def test_table_still_exists(self, unreadable):
        assert unreadable.table_exists(PATH)


class TestAbsentTable:
    @pytest.mark.parametrize("read", READS.values(), ids=list(READS))
    def test_read_returns_none(self, base, read):
        db = DeltaTableDatabase(base_path=base)
        assert read(db) is None
        assert not db.table_exists(PATH)


# ---------------------------------------------------------------------------
# Flush never overwrites a table it could not load
# ---------------------------------------------------------------------------


class TestUnloadableTableIsNeverOverwritten:
    def test_unsupported_reader_feature(self, base, stored):
        unsupported_reader_feature(base)
        before = snapshot(table_dir(base))
        db = DeltaTableDatabase(base_path=base)

        with pytest.raises(StoreReadError):
            db.get_all_records(PATH)
        with pytest.raises(RuntimeError) as info:
            db.add_records(
                PATH,
                rows([b"c"], pa.array([3], pa.int64())),
                record_id_column="rid",
                flush=True,
            )
        assert isinstance(info.value.__cause__, StoreReadError)
        assert snapshot(table_dir(base)) == before

    def test_protocol_error_on_load_is_not_treated_as_absent(
        self, base, stored, monkeypatch
    ):
        # deltalake 1.0.2 does not raise DeltaProtocolError for a local table we can
        # build here, so the load is replaced by one that raises it for this table.
        # Before ITL-884 the flush treated this error as "no table" and wrote the
        # batch with mode="overwrite", replacing the stored rows.
        real = deltalake.DeltaTable
        target = table_dir(base)

        def failing_load(table_uri, *args, **kwargs):
            if Path(str(table_uri)) == target:
                raise deltalake.exceptions.DeltaProtocolError("simulated")
            return real(table_uri, *args, **kwargs)

        before = snapshot(target)
        db = DeltaTableDatabase(base_path=base)
        monkeypatch.setattr(deltalake, "DeltaTable", failing_load)
        with pytest.raises(RuntimeError) as info:
            db.add_records(
                PATH,
                rows([b"c"], pa.array([3], pa.int64())),
                record_id_column="rid",
                flush=True,
            )
        monkeypatch.undo()

        assert isinstance(info.value.__cause__, StoreReadError)
        assert snapshot(target) == before
        stored_rows = DeltaTableDatabase(base_path=base).get_all_records(PATH)
        assert stored_rows is not None
        assert stored_rows.num_rows == 2


# ---------------------------------------------------------------------------
# Append refusal: a column's type differs from the stored table
# ---------------------------------------------------------------------------


def append(db: DeltaTableDatabase, table: pa.Table) -> None:
    db.add_records(PATH, table, record_id_column="rid", flush=True)


def refusal(info: pytest.ExceptionInfo[RuntimeError]) -> InputValidationError:
    """The ``InputValidationError`` behind a refused flush."""
    cause = info.value.__cause__
    assert isinstance(cause, InputValidationError), repr(info.value)
    return cause


class TestChangedColumnTypeIsRefused:
    @pytest.mark.parametrize(
        ("new_type", "value", "named"),
        [
            (pa.float64(), 2.0, "double"),
            (pa.int32(), 2, "int32"),
            (pa.large_string(), "2", "string"),
        ],
        ids=["int64->float64", "int64->int32", "int64->large_string"],
    )
    def test_plain_type_change(self, base, stored, new_type, value, named):
        with pytest.raises(RuntimeError) as info:
            append(stored, rows([b"c"], pa.array([value], new_type)))
        message = str(refusal(info))
        assert "'x'" in message
        assert "int64" in message
        assert named in message
        assert str(table_dir(base)) in message

        back = DeltaTableDatabase(base_path=base).get_all_records(PATH)
        assert back is not None
        assert back.num_rows == 2
        assert back.schema.field("x").type == pa.int64()

    def test_extension_storage_change(self, base):
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], ext_array([b"x"], pa.large_binary())))

        with pytest.raises(RuntimeError) as info:
            append(
                db,
                rows([b"b"], ext_array([[1.0]], pa.large_list(pa.float32()))),
            )
        message = str(refusal(info))
        assert "'x'" in message
        assert "binary" in message
        assert "list" in message

        back = DeltaTableDatabase(base_path=base).get_all_records(PATH)
        assert back is not None
        assert back.num_rows == 1


class TestLegitimateAppendsStillPass:
    def read_back(self, base: Path) -> pa.Table:
        back = DeltaTableDatabase(base_path=base).get_all_records(PATH)
        assert back is not None
        return back

    def test_identical_schema(self, base, stored):
        append(stored, rows([b"c"], pa.array([3], pa.int64())))
        assert self.read_back(base).num_rows == 3

    @pytest.mark.parametrize(
        ("first", "second"),
        [
            (pa.array(["a"], pa.string()), pa.array(["b"], pa.large_string())),
            (pa.array(["a"], pa.large_string()), pa.array(["b"], pa.string())),
            (
                pa.array([[1]], pa.list_(pa.int64())),
                pa.array([[2]], pa.large_list(pa.int64())),
            ),
            (pa.array([b"a"], pa.binary()), pa.array([b"b"], pa.large_binary())),
        ],
        ids=["string->large", "large->string", "list->large", "binary->large"],
    )
    def test_large_and_non_large_layouts(self, base, first, second):
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], first))
        append(db, rows([b"b"], second))
        assert self.read_back(base).num_rows == 2

    def test_new_column(self, base, stored):
        append(
            stored,
            rows([b"c"], pa.array([3], pa.int64()), y=pa.array(["new"])),
        )
        back = self.read_back(base)
        assert back.num_rows == 3
        assert "y" in back.column_names

    def test_missing_column(self, base):
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], pa.array([1], pa.int64()), y=pa.array(["old"])))
        append(db, rows([b"b"], pa.array([2], pa.int64())))
        assert self.read_back(base).num_rows == 2

    def test_extension_type_with_unchanged_storage(self, base):
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], ext_array([b"x"], pa.large_binary())))
        append(db, rows([b"b"], ext_array([b"y"], pa.large_binary())))
        assert self.read_back(base).num_rows == 2

    def test_nullability_only_difference_is_accepted(self, base):
        # Pins today's behaviour: the append check ignores nullability (ITL-884,
        # decision D3). Whether a nullability change should be refused belongs to
        # the nullability audit, ITL-842. The read-back assertion records what
        # Delta does on merge: the stored non-null child becomes nullable, so the
        # rows written first now read back with a different (looser) type.
        strict = pa.struct([pa.field("a", pa.int64(), nullable=False)])
        loose = pa.struct([pa.field("a", pa.int64(), nullable=True)])
        db = DeltaTableDatabase(base_path=base)
        append(db, rows([b"a"], pa.array([{"a": 1}], strict)))
        append(db, rows([b"b"], pa.array([{"a": 2}], loose)))

        back = self.read_back(base)
        assert back.num_rows == 2
        assert back.schema.field("x").type == loose
