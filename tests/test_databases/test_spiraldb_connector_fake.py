"""SpiralDBConnector read path against the disk-backed pyspiral fake (no live SpiralDB).

These tests push real Arrow tables through the connector, unlike the ``MagicMock`` tests in
``test_spiraldb_connector.py``. See ``fake_pyspiral`` for the storage behaviour modelled.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.databases import (
    ConnectorArrowDatabase,
    SpiralDBConnector,
    read_nullability_record,
    spiraldb_connector,
    write_nullability_record,
)
from orcapod.errors import (
    NullabilityRecordError,
    NullabilityRecordMismatchError,
    NullInNonNullableColumnError,
    UnsupportedValueError,
)
from orcapod.utils import arrow_utils
from orcapod.sources import ArrowTableSource, SpiralDBTableSource
from tests.test_databases import fake_pyspiral

_NESTED = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("s", pa.string()),
        pa.field("list_str", pa.list_(pa.string())),
        pa.field("list_nonnull_item", pa.list_(pa.field("item", pa.string(), False))),
        pa.field(
            "st",
            pa.struct([pa.field("a", pa.string(), False), pa.field("b", pa.binary())]),
        ),
        pa.field("large_list_bin", pa.large_list(pa.binary())),
        pa.field(
            "tags",
            # key field "key" (non-null) and item field "value" (nullable)
            pa.map_(pa.string(), pa.list_(pa.string())),
        ),
    ]
)

# What the connector must hand out: the large variant at every depth, with every nested
# field's name and nullability unchanged.
_EXPECTED = {
    "id": pa.int64(),
    "s": pa.large_string(),
    "list_str": pa.large_list(pa.large_string()),
    "list_nonnull_item": pa.large_list(pa.field("item", pa.large_string(), False)),
    "st": pa.struct(
        [pa.field("a", pa.large_string(), False), pa.field("b", pa.large_binary())]
    ),
    "large_list_bin": pa.large_list(pa.large_binary()),
    "tags": pa.map_(pa.large_string(), pa.large_list(pa.large_string())),
}


def _nested_table() -> pa.Table:
    """Return two rows covering every nested type in ``_NESTED``."""
    return pa.table(
        {
            "id": [1, 2],
            "s": ["x", None],
            "list_str": [["a"], []],
            "list_nonnull_item": [["p", "q"], ["r"]],
            "st": [{"a": "u", "b": b"1"}, {"a": "v", "b": None}],
            "large_list_bin": [[b"z"], []],
            "tags": [[("k", ["a", "b"])], []],
        },
        schema=_NESTED,
    )


@pytest.fixture()
def spiral_root(tmp_path: Path):
    """Provide a fake SpiralDB root holding table ``default.nested`` in project ``p``."""
    with fake_pyspiral.install(tmp_path / "spiral"):
        project = fake_pyspiral.FakeSpiral().project("p")
        project.create_table("default.nested", key_schema=[("id", pa.int64())])
        connector = SpiralDBConnector("p")
        # Written through the connector, which records the non-nullable columns (ITL-936):
        # SpiralDB itself reports every column and struct field nullable.
        connector.upsert_records("nested", _nested_table(), id_column="id")
        # Sources hash their rows, and starfix 0.4.0 cannot hash map columns yet, so the
        # source-level tests read a copy without the map column.
        project.create_table("default.hashable", key_schema=[("id", pa.int64())])
        connector.upsert_records(
            "hashable", _nested_table().drop_columns(["tags"]), id_column="id"
        )
        yield tmp_path


class TestLargeTypeNormalization:
    """ITL-40: both read paths give the same, fully large-normalized types."""

    def test_iter_batches_normalizes_nested_types(self, spiral_root):
        connector = SpiralDBConnector("p")
        batches = list(connector.iter_batches('SELECT * FROM "nested"'))
        assert batches
        for batch in batches:
            assert {f.name: f.type for f in batch.schema} == _EXPECTED

    def test_get_column_info_matches_iter_batches(self, spiral_root):
        connector = SpiralDBConnector("p")
        info = {c.name: c.arrow_type for c in connector.get_column_info("nested")}
        assert info == _EXPECTED

    def test_source_exposes_normalized_types(self, spiral_root):
        source = SpiralDBTableSource("p", "hashable")
        schema = source.as_table().schema
        expected = {k: v for k, v in _EXPECTED.items() if k != "tags"}
        assert {name: schema.field(name).type for name in expected} == expected

    def test_normalization_leaves_hashes_unchanged(self, spiral_root):
        # Large and non-large layouts hash identically, so the Spiral source's per-row
        # content hashes equal those of the same rows given in their original layout.
        spiral = SpiralDBTableSource("p", "hashable", source_id="nested")
        # The nullability record restores the written nullability, so the rows as written
        # (non-large layouts) are the reference.
        direct = ArrowTableSource(
            _nested_table().drop_columns(["tags"]),
            tag_columns=["id"],
            source_id="nested",
        )

        def hashes(source) -> list[str]:
            return (
                source.as_table(columns={"content_hash": True})
                .column("_content_hash")
                .to_pylist()
            )

        assert hashes(spiral) == hashes(direct)


_ITEM = pa.field("item", pa.float32(), False)
_G = pa.struct([pa.field("x", pa.int32(), False), pa.field("y", pa.int32(), True)])
_RECORDED = pa.schema(
    [
        pa.field("k", pa.int64(), False),
        pa.field("v", pa.int32(), False),
        pa.field("w", pa.int32(), True),
        pa.field("g", _G, False),
        pa.field("l", pa.list_(_ITEM), False),
    ]
)
_TABLE = "p/default/t"


def _rows(schema: pa.Schema = _RECORDED, **override) -> pa.Table:
    """Return three rows of ``schema`` (``override`` replaces whole columns)."""
    columns = {
        "k": [1, 2, 3],
        "v": [10, 20, 30],
        "w": [None, 2, None],
        "g": [{"x": 1, "y": None}, {"x": 2, "y": 3}, {"x": 3, "y": None}],
        "l": [[1.0], [], [2.0, 3.0]],
        **override,
    }
    return pa.Table.from_pydict(columns, schema=schema)


def _with(schema: pa.Schema, name: str, nullable: bool) -> pa.Schema:
    i = schema.get_field_index(name)
    return schema.set(i, schema.field(name).with_nullable(nullable))


class TestNullabilityRecord:
    """ITL-936: the connector records non-nullable columns and enforces them on read."""

    @pytest.fixture()
    def fake(self, tmp_path: Path):
        """Provide the fake project ``p`` with an empty table ``default.t`` keyed on k."""
        with fake_pyspiral.install(tmp_path / "spiral"):
            project = fake_pyspiral.FakeSpiral().project("p")
            project.create_table("default.t", key_schema=[("k", pa.int64())])
            yield project

    def _batches_schema(self, connector: SpiralDBConnector) -> pa.Schema:
        batches = list(connector.iter_batches('SELECT * FROM "t"'))
        assert batches
        schemas = {b.schema for b in batches}
        assert len(schemas) == 1
        return schemas.pop()

    def test_spiral_itself_reports_every_column_nullable(self, fake):
        SpiralDBConnector("p").upsert_records("t", _rows(), id_column="k")
        stored = fake.table("default.t").schema().to_arrow()
        assert all(f.nullable for f in stored)
        assert stored.field("g").type.field("x").nullable

    def test_first_write_records_and_both_read_paths_restore_it(self, fake):
        connector = SpiralDBConnector("p")
        connector.upsert_records("t", _rows(), id_column="k")
        expected = pa.schema(
            [
                pa.field(
                    f.name, arrow_utils.normalize_to_large_types(f.type), f.nullable
                )
                for f in _RECORDED
            ]
        )
        assert self._batches_schema(connector) == expected
        info = connector.get_column_info("t")
        assert [(c.name, c.arrow_type, c.nullable) for c in info] == [
            (f.name, f.type, f.nullable) for f in expected
        ]

    def test_same_schema_writes_again(self, fake):
        connector = SpiralDBConnector("p")
        connector.upsert_records("t", _rows(), id_column="k")
        connector.upsert_records("t", _rows(k=[4, 5, 6]), id_column="k")
        assert sum(b.num_rows for b in connector.iter_batches('SELECT * FROM "t"')) == 6

    @pytest.mark.parametrize(
        ("name", "nullable", "attribute"),
        [("v", True, "only_in_record"), ("w", False, "only_in_write")],
    )
    def test_disagreeing_write_is_refused(self, fake, name, nullable, attribute):
        connector = SpiralDBConnector("p")
        connector.upsert_records("t", _rows(), id_column="k")
        other = _with(_RECORDED, name, nullable)
        rows = _rows(other, k=[4, 5, 6], w=[1, 2, 3])
        with pytest.raises(NullabilityRecordMismatchError) as info:
            connector.upsert_records("t", rows, id_column="k")
        assert getattr(info.value, attribute) == [name]
        assert info.value.table == _TABLE
        assert _TABLE in str(info.value)
        assert sum(b.num_rows for b in connector.iter_batches('SELECT * FROM "t"')) == 3

    @pytest.mark.parametrize(
        ("override", "column"),
        [
            ({"g": [{"x": 1, "y": 1}, {"x": None, "y": 2}, {"x": 3, "y": 3}]}, "g.x"),
            ({"l": [[1.0], [None], []]}, "l[]"),
            ({"v": [1, None, 3]}, "v"),
        ],
    )
    def test_null_in_non_nullable_column_is_refused_before_writing(
        self, fake, override, column
    ):
        # SpiralDB stores a null in a non-null struct field as 0 (live, ITL-936), so the
        # connector checks every non-nullable path itself before anything is written.
        connector = SpiralDBConnector("p")
        with pytest.raises(NullInNonNullableColumnError) as info:
            connector.upsert_records("t", _rows(**override), id_column="k")
        assert info.value.column == column
        assert info.value.table == _TABLE
        table = fake.table("default.t")
        assert table.schema().to_arrow().names == ["k"]  # no rows written
        assert read_nullability_record(table.get_metadata()) is None

    def test_record_is_written_before_the_rows(self, fake, monkeypatch):
        def failing_write(self, table, **kwargs):
            raise RuntimeError("network down")

        with monkeypatch.context() as m:
            m.setattr(fake_pyspiral.FakeTable, "write", failing_write)
            with pytest.raises(RuntimeError, match="network down"):
                SpiralDBConnector("p").upsert_records("t", _rows(), id_column="k")
        table = fake.table("default.t")
        assert read_nullability_record(table.get_metadata()) is not None
        SpiralDBConnector("p").upsert_records("t", _rows(), id_column="k")
        assert not SpiralDBConnector("p").get_column_info("t")[0].nullable

    def test_table_with_rows_and_no_record_gets_none(self, fake):
        fake.table("default.t").write(_rows())  # an earlier writer, no record
        connector = SpiralDBConnector("p")
        connector.upsert_records("t", _rows(k=[4, 5, 6]), id_column="k")
        assert read_nullability_record(fake.table("default.t").get_metadata()) is None
        assert all(c.nullable for c in connector.get_column_info("t"))
        schema = self._batches_schema(connector)
        assert all(f.nullable for f in schema)
        assert schema.field("g").type.field("x").nullable

    def test_unreadable_record_raises_naming_the_table(self, fake):
        connector = SpiralDBConnector("p")
        connector.upsert_records("t", _rows(), id_column="k")
        table = fake.table("default.t")
        header = json.loads(table.get_metadata()["__orcapod_nullability__"])
        header["version"] = 2
        table.set_metadata({"__orcapod_nullability__": json.dumps(header).encode()})
        for read in (
            lambda: connector.get_column_info("t"),
            lambda: list(connector.iter_batches('SELECT * FROM "t"')),
            lambda: connector.upsert_records("t", _rows(), id_column="k"),
        ):
            with pytest.raises(NullabilityRecordError, match="version 2") as info:
                read()
            assert info.value.table == _TABLE

    def test_recorded_column_missing_from_table_raises(self, fake):
        table = fake.table("default.t")
        table.write(_rows())
        write_nullability_record(
            table, _RECORDED.append(pa.field("gone", pa.int8(), False))
        )
        with pytest.raises(NullabilityRecordError, match="gone") as info:
            SpiralDBConnector("p").get_column_info("t")
        assert info.value.table == _TABLE

    def test_null_in_recorded_column_raises_on_read(self, fake):
        # A wrong record (w holds nulls) never hides a null.
        table = fake.table("default.t")
        table.write(_rows())
        write_nullability_record(table, _with(_RECORDED, "w", False))
        with pytest.raises(NullInNonNullableColumnError) as info:
            list(SpiralDBConnector("p").iter_batches('SELECT * FROM "t"'))
        assert info.value.column == "w"
        assert info.value.table == _TABLE
        assert "'w'" in str(info.value) and _TABLE in str(info.value)

    def test_only_recorded_tables_pay_the_per_batch_check(self, fake, monkeypatch):
        # Spies on the non-public per-batch step: the test is about when it runs.
        calls: list[int] = []
        real = spiraldb_connector.enforce_nullability_record

        def counting(batch, paths):
            calls.append(batch.num_rows)
            return real(batch, paths)

        monkeypatch.setattr(spiraldb_connector, "enforce_nullability_record", counting)
        fake.table("default.t").write(_rows())  # no record
        batches = list(SpiralDBConnector("p").iter_batches('SELECT * FROM "t"'))
        assert len(batches) > 1
        assert calls == []

        fake.create_table("default.r", key_schema=[("k", pa.int64())])
        SpiralDBConnector("p").upsert_records("r", _rows(), id_column="k")
        batches = list(SpiralDBConnector("p").iter_batches('SELECT * FROM "r"'))
        assert calls == [b.num_rows for b in batches]


# Orcapod's own results are large-typed (CLAUDE.md, Arrow types).
_RESULT = pa.schema(
    [
        pa.field("score", pa.float64(), False),
        pa.field("note", pa.large_string(), True),
        pa.field(
            "quality",
            pa.struct(
                [
                    pa.field("snr", pa.float32(), False),
                    pa.field("tag", pa.large_string()),
                ]
            ),
            False,
        ),
        pa.field("spikes", pa.large_list(pa.field("item", pa.float32(), False)), False),
    ]
)
_FLAT_RESULT = pa.schema([_RESULT.field("score"), _RESULT.field("note")])
_PATH = ("results", "fn")


def _result(i: int, schema: pa.Schema = _RESULT) -> pa.Table:
    """Return one result row of ``schema``."""
    row = {
        "score": [float(i)],
        "note": [None if i % 2 else f"n{i}"],
        "quality": [{"snr": float(i), "tag": None}],
        "spikes": [[float(i)] * i],
    }
    return pa.table({n: row[n] for n in schema.names}, schema=schema)


class TestResultStoreOnSpiral:
    """ConnectorArrowDatabase on SpiralDB keeps result nullability (ITL-936)."""

    @pytest.fixture()
    def db(self, tmp_path: Path):
        with fake_pyspiral.install(tmp_path / "spiral"):
            yield ConnectorArrowDatabase(SpiralDBConnector("p"))

    def test_results_read_back_with_the_written_schema(self, db):
        db.add_record(_PATH, "r1", _result(1))
        db.flush()
        db.add_record(_PATH, "r2", _result(2))
        db.flush()
        records = db.get_all_records(_PATH)
        assert records is not None
        assert records.num_rows == 2
        assert records.schema == _RESULT

    def test_result_table_written_before_the_record_keeps_todays_behaviour(
        self, db, monkeypatch
    ):
        # A result table written by an Orcapod without the record (simulated by
        # skipping it on the first flush) gets none later and reads back as SpiralDB
        # reports it: every column nullable.
        with monkeypatch.context() as m:
            m.setattr(
                spiraldb_connector, "write_nullability_record", lambda t, s: False
            )
            db.add_record(_PATH, "r1", _result(1, _FLAT_RESULT))
            db.flush()
        db.add_record(_PATH, "r2", _result(2, _FLAT_RESULT))
        db.flush()
        records = db.get_all_records(_PATH)
        assert records is not None
        assert records.num_rows == 2
        assert all(f.nullable for f in records.schema)

    def test_unrecorded_struct_result_table_cannot_take_a_second_flush(
        self, db, monkeypatch
    ):
        # Today's behaviour on a result table without the record: SpiralDB reports the
        # struct field nullable, so the flush-time type comparison fails.
        with monkeypatch.context() as m:
            m.setattr(
                spiraldb_connector, "write_nullability_record", lambda t, s: False
            )
            db.add_record(_PATH, "r1", _result(1))
            db.flush()
        db.add_record(_PATH, "r2", _result(2))
        with pytest.raises(ValueError, match="Schema mismatch"):
            db.flush()


_INNER = pa.struct([pa.field("a", pa.int32())])


class TestStructLevelNulls:
    """SpiralDB cannot store a null struct (ITL-936).

    Live, pyspiral 0.16.33: written with ``push_down_nulls=True``, a null struct reads back
    as a struct of null fields (value and hash change). The connector never passes
    ``push_down_nulls`` and refuses a null struct at any depth before writing anything.
    """

    @pytest.fixture()
    def fake(self, tmp_path: Path):
        with fake_pyspiral.install(tmp_path / "spiral"):
            project = fake_pyspiral.FakeSpiral().project("p")
            project.create_table("default.t", key_schema=[("k", pa.int64())])
            yield project

    @pytest.mark.parametrize(
        ("column", "values", "path"),
        [
            ("g", pa.array([{"a": 1}, None], _INNER), "g"),
            (
                "outer",
                pa.array(
                    [{"inner": {"a": 1}}, {"inner": None}],
                    pa.struct([pa.field("inner", _INNER)]),
                ),
                "outer.inner",
            ),
            ("items", pa.array([[{"a": 1}], [None]], pa.list_(_INNER)), "items[]"),
        ],
    )
    def test_null_struct_is_refused_before_writing(self, fake, column, values, path):
        rows = pa.table({"k": pa.array([1, 2], pa.int64()), column: values})
        with pytest.raises(UnsupportedValueError) as info:
            SpiralDBConnector("p").upsert_records("t", rows, id_column="k")
        message = str(info.value)
        assert repr(path) in message
        assert _TABLE in message
        table = fake.table("default.t")
        assert table.schema().to_arrow().names == ["k"]  # nothing written
        assert read_nullability_record(table.get_metadata()) is None

    def test_struct_with_null_fields_is_written(self, fake):
        rows = pa.table(
            {
                "k": pa.array([1, 2], pa.int64()),
                "g": pa.array([{"a": 1}, {"a": None}], _INNER),
                "items": pa.array([[{"a": 1}], None], pa.list_(_INNER)),
            }
        )
        connector = SpiralDBConnector("p")
        connector.upsert_records("t", rows, id_column="k")
        back = pa.Table.from_batches(connector.iter_batches('SELECT * FROM "t"'))
        assert back.column("g").to_pylist() == [{"a": 1}, {"a": None}]
        assert back.column("items").to_pylist() == [[{"a": 1}], None]

    def test_push_down_nulls_turns_a_null_struct_into_null_fields(self, fake):
        # What any writer passing push_down_nulls=True gets (live, ITL-936).
        table = fake.table("default.t")
        table.write(
            pa.table(
                {
                    "k": pa.array([1, 2], pa.int64()),
                    "g": pa.array([{"a": 1}, None], _INNER),
                }
            ),
            push_down_nulls=True,
        )
        stored = fake_pyspiral.FakeSpiral().scan(table.select()).to_table()
        assert stored.column("g").to_pylist() == [{"a": 1}, {"a": None}]

    def test_result_store_refuses_a_null_struct(self, tmp_path: Path):
        with fake_pyspiral.install(tmp_path / "spiral"):
            db = ConnectorArrowDatabase(SpiralDBConnector("p"))
            db.add_record(
                ("results", "fn"),
                "r1",
                pa.table({"g": pa.array([None], _INNER)}),
            )
            with pytest.raises(UnsupportedValueError, match="'g'"):
                db.flush()
