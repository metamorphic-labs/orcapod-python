"""A Delta table created by ``DeltaTableDatabase`` names its statistics columns (ITL-1187).

delta-rs logs ``WARN deltalake_core::writer::stats ... Skipping column ... because it's a
binary field`` on every write for each binary column, unless the table names the columns
it keeps statistics for. These tests capture the Rust log (written to the process's
stderr) and fail on that warning, check that the list never makes a write fail, that file
skipping works on a listed column, and that a table created without the property is left
as it is.
"""

from __future__ import annotations

import datetime
import decimal
from pathlib import Path
from typing import Any, cast

import deltalake
import pyarrow as pa
import pytest

from orcapod.databases import DeltaTableDatabase
from orcapod.errors import InputValidationError, UnstorableColumnError
from orcapod.databases.delta_statistics import (
    NUM_INDEXED_COLUMNS_PROPERTY,
    STATS_COLUMNS_PROPERTY,
    statistics_columns,
    statistics_configuration,
)

STATS_WARNING = "deltalake_core::writer::stats"
PATH = ("t",)


class _StrExt(pa.ExtensionType):
    """An extension type stored as a string (as ``orcapod.path`` is)."""

    def __init__(self) -> None:
        super().__init__(pa.large_string(), "itl1187.test.str")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized) -> _StrExt:
        return cls()


class _BinExt(pa.ExtensionType):
    """An extension type stored as binary (as ``orcapod.numpy.ndarray`` is)."""

    def __init__(self) -> None:
        super().__init__(pa.large_binary(), "itl1187.test.bin")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized) -> _BinExt:
        return cls()


def rows(ids: range, extra: bool = False) -> pa.Table:
    """Rows keyed by a binary record id, with a binary hash and an int column.

    ``extra`` adds a binary and an int column, for schema evolution.
    """
    columns: dict[str, pa.Array[Any]] = {
        DeltaTableDatabase.RECORD_ID_COLUMN: pa.array(
            [f"id{i}".encode() for i in ids], pa.large_binary()
        ),
        "h": pa.array([f"h{i}".encode() for i in ids], pa.large_binary()),
        "v": pa.array(list(ids), pa.int64()),
        "__data_context": pa.array(["std:v1:default"] * len(ids), pa.large_string()),
    }
    if extra:
        columns["new_bin"] = pa.array([b"x"] * len(ids), pa.large_binary())
        columns["new_int"] = pa.array(list(ids), pa.int64())
    return pa.Table.from_arrays(list(columns.values()), names=list(columns))


def stats_warnings(capfd: pytest.CaptureFixture[str]) -> list[str]:
    """Return the delta-rs statistics warnings logged since the last call."""
    return [ln for ln in capfd.readouterr().err.splitlines() if STATS_WARNING in ln]


def configuration(table_path: Path) -> dict[str, str]:
    """Return the table properties of the Delta table at ``table_path``."""
    return dict(deltalake.DeltaTable(str(table_path)).metadata().configuration)


def columns_with_statistics(table_path: Path) -> list[set[str]]:
    """Return, per data file, the columns that file has a minimum for."""
    adds = pa.table(
        deltalake.DeltaTable(str(table_path)).get_add_actions(flatten=True)
    ).to_pylist()
    return [
        {
            k[len("min.") :]
            for k, v in a.items()
            if k.startswith("min.") and v is not None
        }
        for a in adds
    ]


def stored_ids(db: DeltaTableDatabase) -> list[bytes]:
    """Return the record ids the database reads back, sorted."""
    table = db.get_all_records(PATH, record_id_column="rid")
    assert table is not None
    return sorted(cast("list[bytes]", table["rid"].to_pylist()))


class TestStatisticsRule:
    """The rule that picks the statistics columns."""

    @pytest.mark.parametrize(
        "arrow_type",
        [
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.uint8(),
            pa.uint64(),
            pa.float32(),
            pa.float64(),
            pa.bool_(),
            pa.string(),
            pa.large_string(),
            pa.date32(),
            pa.timestamp("us"),
            pa.timestamp("us", tz="UTC"),
            pa.decimal128(10, 2),
            _StrExt(),
        ],
        ids=str,
    )
    def test_listed_types(self, arrow_type: pa.DataType) -> None:
        assert statistics_columns(pa.schema([("c", arrow_type)])) == ["c"]

    @pytest.mark.parametrize(
        "arrow_type",
        [
            pa.binary(),
            pa.large_binary(),
            pa.binary(4),
            pa.float16(),
            pa.null(),
            pa.large_list(pa.int64()),
            pa.map_(pa.large_string(), pa.int64()),
            pa.struct([("x", pa.int64())]),
            pa.struct([("x", pa.int64()), ("y", pa.large_binary())]),
            pa.dictionary(pa.int32(), pa.large_string()),
            _BinExt(),
        ],
        ids=str,
    )
    def test_left_out_types(self, arrow_type: pa.DataType) -> None:
        assert statistics_columns(pa.schema([("c", arrow_type)])) == []

    def test_name_with_a_comma_or_backtick_is_left_out(self) -> None:
        schema = pa.schema(
            [("a,b", pa.int64()), ("x`y", pa.int64()), ("c", pa.int64())]
        )
        assert statistics_columns(schema) == ["c"]

    @pytest.mark.parametrize(
        ("name", "other"),
        [
            ("v", pa.struct([("v", pa.large_binary())])),
            ("v", pa.struct([("v", pa.int64())])),
            ("v", pa.large_list(pa.struct([("v", pa.large_binary())]))),
            ("item", pa.large_list(pa.int64())),
            ("element", pa.large_list(pa.large_binary())),
            ("list", pa.large_list(pa.int64())),
            ("key", pa.map_(pa.large_string(), pa.int64())),
            ("value", pa.map_(pa.large_string(), pa.large_binary())),
            ("key_value", pa.map_(pa.large_string(), pa.int64())),
        ],
        ids=lambda x: str(x),
    )
    def test_name_of_a_nested_part_is_left_out(
        self, name: str, other: pa.DataType
    ) -> None:
        """delta-rs matches a listed name against nested leaf names too."""
        schema = pa.schema([(name, pa.int64()), ("rec", other), ("c", pa.int64())])
        assert statistics_columns(schema) == ["c"]

    def test_names_are_quoted(self) -> None:
        schema = pa.schema([("a.b", pa.int64()), ("s::t", pa.int64())])
        assert statistics_configuration(schema) == {
            STATS_COLUMNS_PROPERTY: "`a.b`,`s::t`"
        }

    def test_no_column_qualifies(self) -> None:
        schema = pa.schema([("h", pa.large_binary())])
        assert statistics_configuration(schema) == {NUM_INDEXED_COLUMNS_PROPERTY: "0"}


class TestNewTableWritePaths:
    """Every write path on a new table logs no statistics warning."""

    def test_create_append_merge_and_schema_evolution(
        self, tmp_path: Path, capfd: pytest.CaptureFixture[str]
    ) -> None:
        db = DeltaTableDatabase(tmp_path)
        capfd.readouterr()

        db.add_records(PATH, rows(range(0, 10)), flush=True)
        assert stats_warnings(capfd) == [], "create"
        assert configuration(tmp_path / "t") == {
            STATS_COLUMNS_PROPERTY: "`v`,`__data_context`"
        }

        db.add_records(PATH, rows(range(10, 20)), flush=True)
        assert stats_warnings(capfd) == [], "append"

        # One stored id among the new ones forces the MERGE path.
        db.add_records(PATH, rows(range(15, 30)), skip_duplicates=False, flush=True)
        assert stats_warnings(capfd) == [], "merge"

        # New columns (one binary, one int): schema evolution, through MERGE.
        db.add_records(PATH, rows(range(30, 40), extra=True), flush=True)
        assert stats_warnings(capfd) == [], "schema evolution"

        db.add_records(PATH, rows(range(40, 50), extra=True), flush=True)
        assert stats_warnings(capfd) == [], "append after schema evolution"

        # The property is never changed: a column added later has no statistics.
        assert configuration(tmp_path / "t") == {
            STATS_COLUMNS_PROPERTY: "`v`,`__data_context`"
        }
        assert all(
            stats == {"v", "__data_context"}
            for stats in columns_with_statistics(tmp_path / "t")
        )
        assert stored_ids(db) == sorted(f"id{i}".encode() for i in range(50))

    def test_job_writes_log_no_statistics_warning(
        self, tmp_path: Path, capfd: pytest.CaptureFixture[str]
    ) -> None:
        """A function node's pipeline and result tables, written by a job."""
        from orcapod import PipelineJob, function_pod
        from orcapod.core.sources import ArrowTableSource

        @function_pod(output_keys="d")
        def double(v: int | None) -> int:
            """Return twice ``v``."""
            return (v or 0) * 2

        source = ArrowTableSource(
            pa.table({"k": [1, 2, 3], "v": [1, 2, 3]}),
            tag_columns=["k"],
            tag_only_columns=True,
        )
        job = PipelineJob(name="stats", database=DeltaTableDatabase(tmp_path))
        with job:
            double.pod(source, label="double")
        capfd.readouterr()
        job.run()
        assert stats_warnings(capfd) == []

        tables = [log.parent for log in tmp_path.rglob("_delta_log")]
        assert len(tables) == 2  # the pipeline table and the result table
        for table_path in tables:
            listed = configuration(table_path)[STATS_COLUMNS_PROPERTY]
            assert "`__data_context`" in listed
            assert "`__record_id`" not in listed

    def test_table_with_no_listable_column(
        self, tmp_path: Path, capfd: pytest.CaptureFixture[str]
    ) -> None:
        db = DeltaTableDatabase(tmp_path)
        only_binary = rows(range(0, 5)).select(
            [DeltaTableDatabase.RECORD_ID_COLUMN, "h"]
        )
        capfd.readouterr()
        db.add_records(PATH, only_binary, flush=True)
        more = rows(range(5, 10)).select([DeltaTableDatabase.RECORD_ID_COLUMN, "h"])
        db.add_records(PATH, more, flush=True)
        assert stats_warnings(capfd) == []
        assert configuration(tmp_path / "t") == {NUM_INDEXED_COLUMNS_PROPERTY: "0"}
        assert len(stored_ids(db)) == 10


def _every_kind(ids: range) -> pa.Table:
    """Rows with a column of every kind the rule lists or leaves out."""
    n = len(ids)
    columns: dict[str, pa.Array[Any]] = {
        DeltaTableDatabase.RECORD_ID_COLUMN: pa.array(
            [f"id{i}".encode() for i in ids], pa.large_binary()
        ),
        # Listed.
        "i8": pa.array([1] * n, pa.int8()),
        "i32": pa.array([1] * n, pa.int32()),
        "u64": pa.array([2**63 + 1] * n, pa.uint64()),
        "f32": pa.array([1.5] * n, pa.float32()),
        "f64": pa.array([1.5] * n, pa.float64()),
        "flag": pa.array([True] * n, pa.bool_()),
        "s": pa.array(["a"] * n, pa.large_string()),
        "day": pa.array([datetime.date(2026, 11, 20)] * n, pa.date32()),
        "ts": pa.array([1] * n, pa.timestamp("us", tz="UTC")),
        "ts_ns": pa.array([1] * n, pa.timestamp("ns")),
        "amount": pa.array([decimal.Decimal("1.25")] * n, pa.decimal128(10, 2)),
        "ext_s": pa.ExtensionArray.from_storage(
            _StrExt(), pa.array(["p"] * n, pa.large_string())
        ),
        "a.dot": pa.array([1] * n, pa.int64()),
        "sp ace": pa.array([1] * n, pa.int64()),
        "_tag_source_id::ab12": pa.array(["src"] * n, pa.large_string()),
        "ünï": pa.array([1] * n, pa.int64()),
        # Left out.
        "b": pa.array([b"x"] * n, pa.large_binary()),
        "fsb": pa.array([b"abcd"] * n, pa.binary(4)),
        "ext_b": pa.ExtensionArray.from_storage(
            _BinExt(), pa.array([b"x"] * n, pa.large_binary())
        ),
        "ints": pa.array([[1, 2]] * n, pa.large_list(pa.int64())),
        "hashes": pa.array([[b"x"]] * n, pa.large_list(pa.large_binary())),
        "pairs": pa.array([[("k", 1)]] * n, pa.map_(pa.large_string(), pa.int64())),
        "rec": pa.array(
            [{"x": 1, "y": b"z"}] * n,
            pa.struct([("x", pa.int64()), ("y", pa.large_binary())]),
        ),
        "com,ma": pa.array([1] * n, pa.int64()),
    }
    return pa.Table.from_arrays(list(columns.values()), names=list(columns))


LISTED = {
    "i8",
    "i32",
    "u64",
    "f32",
    "f64",
    "flag",
    "s",
    "day",
    "ts",
    "ts_ns",
    "amount",
    "ext_s",
    "a.dot",
    "sp ace",
    "_tag_source_id::ab12",
    "ünï",
}


class TestEveryColumnKind:
    """A wrong list makes delta-rs refuse the write, so every kind is written for real."""

    def test_every_kind_is_written_without_warning(
        self, tmp_path: Path, capfd: pytest.CaptureFixture[str]
    ) -> None:
        db = DeltaTableDatabase(tmp_path)
        capfd.readouterr()
        db.add_records(PATH, _every_kind(range(0, 3)), flush=True)
        db.add_records(PATH, _every_kind(range(3, 6)), flush=True)
        db.add_records(
            PATH, _every_kind(range(5, 9)), skip_duplicates=False, flush=True
        )
        assert stats_warnings(capfd) == []

        assert set(statistics_columns(_delta_schema(tmp_path / "t"))) == LISTED
        assert all(stats == LISTED for stats in columns_with_statistics(tmp_path / "t"))
        assert len(stored_ids(db)) == 9
        # The stored values come back as written.
        fresh = DeltaTableDatabase(tmp_path).get_all_records(
            PATH, record_id_column="rid"
        )
        assert fresh is not None
        compared = sorted(LISTED - {"ext_s"})
        assert (
            fresh.sort_by("rid").select(compared).slice(0, 1).to_pylist()
            == _every_kind(range(0, 1)).select(compared).to_pylist()
        )

    def test_backtick_name_is_refused_before_anything_is_queued(
        self, tmp_path: Path
    ) -> None:
        """delta-rs cannot MERGE such a table, and every later commit is one (ITL-1375)."""
        db = DeltaTableDatabase(tmp_path)
        with pytest.raises(UnstorableColumnError) as info:
            # No flush: the refusal is when the rows are added (review of PR 511).
            db.add_records(
                PATH,
                rows(range(0, 3)).append_column("x`y", pa.array([0, 1, 2], pa.int64())),
            )
        assert info.value.column == "x`y"
        assert "backtick" in info.value.reason
        assert not db._pending_batches  # pyright: ignore[reportPrivateUsage]
        db.flush()
        assert not (tmp_path / "t" / "_delta_log").exists()
        # A backtick inside a nested field's name is merged as any other.
        nested = pa.array([{"x`y": i} for i in range(3)])
        db.add_records(PATH, rows(range(0, 3)).append_column("s", nested), flush=True)
        db.add_records(
            PATH,
            rows(range(3, 6)).append_column("s", pa.array([{"x`y": 3}] * 3)),
            flush=True,
        )
        assert len(stored_ids(db)) == 6

    def test_a_stored_table_with_a_backtick_name_takes_no_append(
        self, tmp_path: Path
    ) -> None:
        """A table written with such a column before ITL-1375 is refused, rows unchanged."""
        deltalake.write_deltalake(
            str(tmp_path / "t"),
            rows(range(0, 3)).append_column("x`y", pa.array([0, 1, 2], pa.int64())),
        )
        db = DeltaTableDatabase(tmp_path)
        # Rows holding the column are refused as new ones are (above); rows without
        # it are refused by the stored table.
        with pytest.raises(UnstorableColumnError):
            db.add_records(
                PATH,
                rows(range(3, 6)).append_column("x`y", pa.array([3, 4, 5], pa.int64())),
            )
        with pytest.raises(InputValidationError, match="backtick") as info:
            db.add_records(PATH, rows(range(3, 6)), flush=True)
        assert "'x`y'" in str(info.value)
        assert len(stored_ids(db)) == 3

    def test_names_of_nested_parts_are_left_out_and_written(
        self, tmp_path: Path, capfd: pytest.CaptureFixture[str]
    ) -> None:
        """A top-level name that is also a nested binary field's name logs nothing."""
        db = DeltaTableDatabase(tmp_path)

        def table(ids: range) -> pa.Table:
            n = len(ids)
            columns: dict[str, pa.Array[Any]] = {
                DeltaTableDatabase.RECORD_ID_COLUMN: pa.array(
                    [f"id{i}".encode() for i in ids], pa.large_binary()
                ),
                "v": pa.array(list(ids), pa.int64()),
                "rec": pa.array(
                    [{"v": b"x"}] * n, pa.struct([("v", pa.large_binary())])
                ),
                "element": pa.array([1] * n, pa.int64()),
                "hashes": pa.array([[b"x"]] * n, pa.large_list(pa.large_binary())),
                "value": pa.array([1] * n, pa.int64()),
                "pairs": pa.array(
                    [[("k", b"x")]] * n, pa.map_(pa.large_string(), pa.large_binary())
                ),
                "c": pa.array([1] * n, pa.int64()),
            }
            return pa.Table.from_arrays(list(columns.values()), names=list(columns))

        capfd.readouterr()
        db.add_records(PATH, table(range(0, 3)), flush=True)
        db.add_records(PATH, table(range(3, 6)), flush=True)
        db.add_records(PATH, table(range(5, 9)), skip_duplicates=False, flush=True)
        assert stats_warnings(capfd) == []
        assert configuration(tmp_path / "t") == {STATS_COLUMNS_PROPERTY: "`c`"}
        assert len(stored_ids(db)) == 9


def _delta_schema(table_path: Path) -> pa.Schema:
    """Return the Arrow schema delta-rs reports for the table at ``table_path``."""
    return pa.schema(deltalake.DeltaTable(str(table_path)).schema().to_arrow())  # pyright: ignore[reportArgumentType]  # an arro3 schema, read through the Arrow C interface


class TestFileSkipping:
    def test_a_listed_column_prunes_files(self, tmp_path: Path) -> None:
        """A filter on a listed column reads only the file that can hold the value."""
        db = DeltaTableDatabase(tmp_path)
        for lo in (0, 100, 200):
            db.add_records(PATH, rows(range(lo, lo + 5)), flush=True)

        table = deltalake.DeltaTable(str(tmp_path / "t"))
        # There are files to prune: three active files, with disjoint ``v`` ranges,
        # one of them holding 101 (review of PR 511).
        ranges = sorted(
            (add["min.v"], add["max.v"])
            for add in pa.table(table.get_add_actions(flatten=True)).to_pylist()
        )
        assert ranges == [(0, 4), (100, 104), (200, 204)]
        plan = pa.table(
            deltalake.QueryBuilder()
            .register("t", table)
            .execute("EXPLAIN ANALYZE SELECT * FROM t WHERE v = 101")
            .read_all()
        )
        text = "\n".join(str(row) for row in plan.to_pylist())
        # The scan is given only the file whose statistics can hold the value: the
        # other two are pruned before it (deltalake 1.6 lists the files it scans).
        assert text.count(".parquet") == 1, text
        assert "file_groups={1 group:" in text, text


class TestTablesCreatedElsewhere:
    def test_existing_table_without_the_property_is_unaltered(
        self, tmp_path: Path, capfd: pytest.CaptureFixture[str]
    ) -> None:
        """A table created before ITL-1187 keeps no property: delta-rs keeps statistics
        for every column it can, and none for a binary one."""
        deltalake.write_deltalake(str(tmp_path / "t"), rows(range(0, 10)))
        db = DeltaTableDatabase(tmp_path)
        capfd.readouterr()

        db.add_records(PATH, rows(range(10, 20)), flush=True)
        db.add_records(PATH, rows(range(15, 30)), skip_duplicates=False, flush=True)

        # deltalake 1.6 no longer logs the binary-column warning (1.0.2 did); what
        # the property was about stays: no min/max is kept for a binary column.
        assert configuration(tmp_path / "t") == {}
        # Every data file, those the two MERGEs added included, has a minimum for
        # ``v`` and none for the binary ``h`` (review of PR 511).
        per_file = columns_with_statistics(tmp_path / "t")
        assert len(per_file) >= 3
        assert all("v" in stats and "h" not in stats for stats in per_file)
        assert stored_ids(db) == sorted(f"id{i}".encode() for i in range(30))

    def test_plain_deltalake_appends_to_a_table_with_the_property(
        self, tmp_path: Path, capfd: pytest.CaptureFixture[str]
    ) -> None:
        """A writer that knows nothing of the property (main before ITL-1187) still appends."""
        DeltaTableDatabase(tmp_path).add_records(PATH, rows(range(0, 10)), flush=True)
        capfd.readouterr()

        deltalake.write_deltalake(
            str(tmp_path / "t"), rows(range(10, 20)), mode="append"
        )
        assert stats_warnings(capfd) == []

        assert stored_ids(DeltaTableDatabase(tmp_path)) == sorted(
            f"id{i}".encode() for i in range(20)
        )
        assert configuration(tmp_path / "t") == {
            STATS_COLUMNS_PROPERTY: "`v`,`__data_context`"
        }
