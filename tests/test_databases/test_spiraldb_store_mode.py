"""A SpiralDB connector in store mode, under ``ConnectorArrowDatabase`` (ITL-1498).

On the fake (``fake_spiral_sdk``). The live run of the same operations is in
``test_spiraldb_connector_integration.py``.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod.contexts.stored import tables_of_another_layout
from orcapod.databases import ConnectorArrowDatabase, SpiralDBConnector
from orcapod.databases import spiral_store as codec
from orcapod.databases import spiral_store_names as names
from orcapod.errors import (
    SpiralDBWriteNotSupportedError,
    SpiralStoreTableCollisionError,
)
from tests.test_databases import fake_spiral_sdk as fake

CONTEXT = "std:v1:default"
H = "0123456789abcdef" * 4
RESULT = ("my_fn", H, "v0", "python.function.v1", "rev1", "rdb_v1")
utc = dt.timezone.utc


@pytest.fixture
def root(tmp_path: Path) -> Iterator[Path]:
    with fake.install(tmp_path):
        yield tmp_path


def store(project: str = "p", dataset: str = "d") -> SpiralDBConnector:
    return SpiralDBConnector(project, dataset, _store=True)


def rows(ids: list[str], *, context: str = CONTEXT, offset: int = 0) -> pa.Table:
    """Rows of a stored table: names the codec escapes, an instant, a struct."""
    n = len(ids)
    return pa.table(
        {
            "__record_id": pa.array([i.encode() for i in ids], pa.large_binary()),
            "__data_context": pa.array([context] * n, pa.large_string()),
            "a.b": pa.array([offset + i for i in range(n)], pa.int64()),
            "__pod_ts": pa.array(
                [dt.datetime(2026, 1, 1, tzinfo=utc)] * n, pa.timestamp("us", tz="UTC")
            ),
            "s": pa.array([{"x": i} for i in range(n)], pa.struct([("x", pa.int8())])),
        }
    )


def by_id(table: pa.Table) -> pa.Table:
    return table.sort_by("__record_id")


def _client() -> Any:
    return fake.FakeClient()


def _table(name: str) -> Any:
    return _client().keyed_table("p", "d", name)


# ------------------------------------------------------------------ write and read


@pytest.mark.usefixtures("root")
def test_rows_round_trip() -> None:
    db = ConnectorArrowDatabase(store())
    written = rows(["r1", "r0", "r2"])
    db.add_records(RESULT, written, record_id_column="__record_id")
    assert db.table_exists(RESULT)  # pending
    db.flush()
    assert db.table_exists(RESULT)
    got = db.get_all_records(RESULT, record_id_column="__record_id")
    assert got is not None
    assert by_id(got).equals(by_id(written))
    one = db.get_record_by_id(RESULT, b"r2", record_id_column="__record_id")
    assert one is not None and one.column("a.b").to_pylist() == [2]
    schema = db.get_table_schema(RESULT)
    assert schema is not None and schema.names == written.schema.names


@pytest.mark.usefixtures("root")
def test_the_table_is_named_from_its_full_path_and_records_it() -> None:
    db = ConnectorArrowDatabase(store()).at("team", "store")
    db.add_records(RESULT, rows(["r0"]), record_id_column="__record_id")
    db.flush()
    full = ("team", "store", *RESULT)
    table = _table(names.store_table_name(full))
    metadata = table.get_metadata()
    record = names.read_store_table_metadata(metadata)
    assert record == names.StoreTableRecord(full, "rdb_v1", CONTEXT)
    manifest = codec.read_manifest(metadata, full)
    assert manifest is not None and "a.b" in manifest.schema.names
    # the stored column names are the codec's, not Orcapod's
    assert "a.b" not in table.schema().names


@pytest.mark.usefixtures("root")
def test_a_path_too_long_for_a_joined_name_is_stored() -> None:
    long_path = ("f" * 300, H, "v0", "python.function.v1", f"schema:{H}", "pdb_v1")
    db = ConnectorArrowDatabase(store())
    db.add_records(long_path, rows(["r0"]), record_id_column="__record_id")
    db.flush()
    assert len(names.store_table_name(long_path)) <= 164
    assert db.list_table_paths(("f" * 300,)) == [long_path]


@pytest.mark.usefixtures("root")
def test_later_writes_append_and_skip_existing_keeps_stored_rows() -> None:
    db = ConnectorArrowDatabase(store())
    db.add_records(RESULT, rows(["r0", "r1"]), record_id_column="__record_id")
    db.flush()
    db.add_records(RESULT, rows(["r2"]), record_id_column="__record_id")
    db.flush()
    again = rows(["r1", "r3"], offset=100)
    db.add_records(RESULT, again, record_id_column="__record_id", skip_duplicates=True)
    db.flush()
    got = db.get_all_records(RESULT, record_id_column="__record_id")
    assert got is not None
    values = dict(zip(got["__record_id"].to_pylist(), got["a.b"].to_pylist()))
    assert values == {b"r0": 0, b"r1": 1, b"r2": 0, b"r3": 101}


@pytest.mark.usefixtures("root")
def test_a_new_connector_reads_what_another_wrote() -> None:
    ConnectorArrowDatabase(store()).add_records(
        RESULT, rows(["r0"]), record_id_column="__record_id", flush=True
    )
    fresh = ConnectorArrowDatabase(store())
    assert fresh.table_exists(RESULT)
    got = fresh.get_all_records(RESULT, record_id_column="__record_id")
    assert got is not None and got.num_rows == 1


# ---------------------------------------------------------------- refusals


@pytest.mark.parametrize(
    "bad",
    [
        rows(["r0"]).drop_columns(["__data_context"]),
        pa.concat_tables([rows(["r0"]), rows(["r1"], context="std:v2:other")]),
        rows(["r0"]).set_column(
            4, "s", pa.array([None], pa.struct([("x", pa.int8())]))
        ),
    ],
    ids=["no data context", "two data contexts", "a null struct"],
)
@pytest.mark.usefixtures("root")
def test_rows_the_store_cannot_keep_are_refused_before_anything_is_written(
    bad: pa.Table,
) -> None:
    db = ConnectorArrowDatabase(store())
    with pytest.raises((ValueError, codec.UnstorableInSpiralError)):
        db.add_records(RESULT, bad, record_id_column="__record_id")
    assert fake.FakeClient()._store.tables() == []  # pyright: ignore[reportPrivateUsage]


@pytest.mark.usefixtures("root")
def test_another_schema_is_refused() -> None:
    db = ConnectorArrowDatabase(store())
    db.add_records(RESULT, rows(["r0"]), record_id_column="__record_id", flush=True)
    other = rows(["r1"]).append_column("more", pa.array([1]))
    fresh = ConnectorArrowDatabase(store())
    with pytest.raises(ValueError, match="(?i)schema"):
        fresh.add_records(RESULT, other, record_id_column="__record_id", flush=True)


def _make_table(name: str, metadata: dict[str, bytes], *, with_rows: bool) -> None:
    key = pa.schema([pa.field("__record_id", pa.large_binary(), nullable=False)])
    table = _client().project("p").create_keyed_table("d", name, key)
    if metadata:
        table.set_metadata(metadata)
    if with_rows:
        _ = table.append(
            pa.table(
                {
                    "__record_id": pa.array([b"x"], pa.large_binary()),
                    "v": pa.array([1]),
                }
            )
        )


@pytest.mark.usefixtures("root")
def test_a_table_of_another_path_under_the_name_is_refused() -> None:
    other = ("another", "rdb_v1")
    _make_table(
        names.store_table_name(RESULT),
        names.store_table_metadata(other, CONTEXT),
        with_rows=False,
    )
    db = ConnectorArrowDatabase(store())
    with pytest.raises(SpiralStoreTableCollisionError, match="another"):
        _ = db.table_exists(RESULT)
    with pytest.raises(SpiralStoreTableCollisionError):
        db.add_records(RESULT, rows(["r0"]), record_id_column="__record_id", flush=True)


@pytest.mark.usefixtures("root")
def test_a_table_with_rows_and_no_record_is_refused() -> None:
    _make_table(names.store_table_name(RESULT), {}, with_rows=True)
    with pytest.raises(SpiralStoreTableCollisionError, match="no store manifest"):
        _ = ConnectorArrowDatabase(store()).table_exists(RESULT)


@pytest.mark.usefixtures("root")
def test_an_empty_table_without_a_record_is_adopted() -> None:
    """A creation that stopped before the first write (decision 3)."""
    _make_table(names.store_table_name(RESULT), {}, with_rows=False)
    db = ConnectorArrowDatabase(store())
    assert not db.table_exists(RESULT)
    db.add_records(RESULT, rows(["r0"]), record_id_column="__record_id", flush=True)
    got = db.get_all_records(RESULT, record_id_column="__record_id")
    assert got is not None and got.num_rows == 1


@pytest.mark.usefixtures("root")
def test_an_empty_table_with_another_key_is_refused() -> None:
    key = pa.schema([pa.field("k", pa.large_string(), nullable=False)])
    _ = (
        _client()
        .project("p")
        .create_keyed_table("d", names.store_table_name(RESULT), key)
    )
    with pytest.raises(SpiralStoreTableCollisionError, match="key"):
        _ = ConnectorArrowDatabase(store()).table_exists(RESULT)


# ---------------------------------------------------------------- listing


PATHS = [
    RESULT,
    ("my_fn", H, "v0", "python.function.v1", "rev1", "rdb_v0"),
    ("my_fn", H, "v0", "python.function.v1", f"schema:{H}", "pdb_v1"),
    ("my_fnx", H, "rdb_v1"),
    ("side_effects", "Notify Pod", f"schema:{H}", "tdb_v1"),
]


@pytest.mark.usefixtures("root")
def test_tables_are_listed_by_their_recorded_paths() -> None:
    db = ConnectorArrowDatabase(store())
    for path in reversed(PATHS):
        db.add_records(path, rows(["r0"]), record_id_column="__record_id")
    db.flush()
    db.add_records(
        ("my_fn", "pending", "rdb_v1"), rows(["r0"]), record_id_column="__record_id"
    )
    _make_table("someone_elses", {}, with_rows=True)
    _make_table("o1_unmarked_" + "0" * 64, {}, with_rows=False)
    assert db.list_table_paths() == sorted([*PATHS, ("my_fn", "pending", "rdb_v1")])
    assert db.list_table_paths(("my_fn",)) == sorted(
        [p for p in PATHS if p[0] == "my_fn"] + [("my_fn", "pending", "rdb_v1")]
    )
    assert db.list_table_paths(("side_effects", "Notify Pod")) == [PATHS[-1]]
    assert db.at("my_fn").list_table_paths((H,)) == sorted(
        p[1:] for p in PATHS if p[:2] == ("my_fn", H)
    )


@pytest.mark.usefixtures("root")
def test_stale_layouts_are_found_by_their_recorded_paths() -> None:
    """ITL-954: by the recorded path, never by a computed hash."""
    db = ConnectorArrowDatabase(store())
    for path in PATHS:
        db.add_records(path, rows(["r0"]), record_id_column="__record_id")
    db.flush()
    # Under ("my_fn",): the current result and pipeline tables, and one of layout
    # rdb_v0; ("my_fnx", ...) shares the name's beginning, not the path.
    assert tables_of_another_layout(db, ("my_fn",), result_revision="rev1") == [
        PATHS[1]
    ]


@pytest.mark.usefixtures("root")
def test_a_listed_table_recording_a_path_of_another_name_is_refused() -> None:
    """A table copied or renamed under another name."""
    _make_table(
        "o1_my_fn_" + "0" * 64,
        names.store_table_metadata(RESULT, CONTEXT),
        with_rows=True,
    )
    with pytest.raises(SpiralStoreTableCollisionError, match="another name"):
        _ = ConnectorArrowDatabase(store()).list_table_paths(("my_fn",))


# ---------------------------------------------------------------- private, plain


@pytest.mark.usefixtures("root")
def test_a_store_mode_connector_is_not_saved() -> None:
    with pytest.raises(ValueError, match="not saved"):
        _ = store().to_config()


@pytest.mark.usefixtures("root")
def test_raw_reads_are_refused_in_store_mode() -> None:
    connector = store()
    for call in (
        lambda: connector.lookup_table("x"),
        lambda: list(connector.scan_batches("x")),
    ):
        with pytest.raises(ValueError, match="store-mode"):
            _ = call()


@pytest.mark.usefixtures("root")
def test_a_plain_connector_still_writes_nothing() -> None:
    connector = SpiralDBConnector("p", "d")
    assert connector.table_name(("a", "b")) == "a__b"
    with pytest.raises(SpiralDBWriteNotSupportedError):
        connector.validate_records(rows(["r0"]))


# ---------------------------------------- review of PR 595 (fe4145c7), round 1


def _tables() -> list[dict[str, Any]]:
    return fake.FakeClient()._store.tables()  # pyright: ignore[reportPrivateUsage]


@pytest.mark.usefixtures("root")
def test_an_empty_table_whose_key_is_of_another_type_is_refused_untouched() -> None:
    """P1: ``__record_id`` typed text is not the store's key; nothing is recorded on it."""
    key = pa.schema([pa.field("__record_id", pa.large_string(), nullable=False)])
    name = names.store_table_name(RESULT)
    _ = _client().project("p").create_keyed_table("d", name, key)
    db = ConnectorArrowDatabase(store())
    with pytest.raises(SpiralStoreTableCollisionError, match="key"):
        db.add_records(RESULT, rows(["r0"]), record_id_column="__record_id", flush=True)
    assert _table(name).get_metadata() == {}


@pytest.mark.usefixtures("root")
def test_a_path_too_long_to_record_is_refused_before_any_table_is_made() -> None:
    """P2: every check runs before the table is created."""
    path = ("x" * 9000, "rdb_v1")
    db = ConnectorArrowDatabase(store())
    with pytest.raises(ValueError, match="path"):
        db.add_records(path, rows(["r0"]), record_id_column="__record_id", flush=True)
    assert _tables() == []


@pytest.mark.usefixtures("root")
def test_two_data_contexts_buffered_for_one_table_are_refused_before_it_is_made() -> (
    None
):
    """P2: two batches, each of one data context, buffered for one first write."""
    db = ConnectorArrowDatabase(store())
    db.add_records(RESULT, rows(["r0"]), record_id_column="__record_id")
    db.add_records(
        RESULT, rows(["r1"], context="std:v2:other"), record_id_column="__record_id"
    )
    with pytest.raises(ValueError, match="data context"):
        db.flush()
    assert _tables() == []


@pytest.mark.usefixtures("root")
def test_a_recorded_table_with_no_rows_is_new_to_any_first_writer() -> None:
    """P2 (spec 2.5): a first write that failed after its metadata leaves a table any
    first writer may record again, with another schema too."""
    other = rows(["r0"]).drop_columns(["s"])
    manifest = codec.new_manifest(RESULT, "rdb_v1", other.schema)
    _make_table(
        names.store_table_name(RESULT),
        {
            **names.store_table_metadata(RESULT, CONTEXT),
            **codec.manifest_entries(manifest),
        },
        with_rows=False,
    )
    db = ConnectorArrowDatabase(store())
    assert not db.table_exists(RESULT)
    written = rows(["r0", "r1"])
    db.add_records(RESULT, written, record_id_column="__record_id", flush=True)
    got = ConnectorArrowDatabase(store()).get_all_records(
        RESULT, record_id_column="__record_id"
    )
    assert got is not None and by_id(got).equals(by_id(written))


@pytest.mark.usefixtures("root")
def test_a_table_listed_before_its_record_is_listed_after_it() -> None:
    """P2: a missing record is never cached."""
    _make_table(names.store_table_name(RESULT), {}, with_rows=False)
    db = ConnectorArrowDatabase(store())
    assert db.list_table_paths(("my_fn",)) == []
    db.add_records(RESULT, rows(["r0"]), record_id_column="__record_id", flush=True)
    assert db.list_table_paths(("my_fn",)) == [RESULT]


def _with_path_column(table: pa.Table, *, live: bool) -> pa.Table:
    from pathlib import Path as PathValue

    from orcapod.contexts import get_default_context
    from orcapod.logical_types.canonical_form import storage_form_schema

    converter = get_default_context().type_converter
    paths = converter.python_dicts_to_arrow_table(
        [{"p": PathValue(f"a/{i}")} for i in range(table.num_rows)],
        python_schema={"p": PathValue},
    )
    if not live:
        schema = storage_form_schema(paths.schema)
        live: Any = paths.column("p").combine_chunks()  # an extension array
        paths = pa.Table.from_arrays([live.storage], schema=schema)
    return table.append_column(paths.schema.field("p"), paths.column("p"))


@pytest.mark.usefixtures("root")
@pytest.mark.parametrize("first_live", [True, False])
def test_a_logical_type_live_or_in_storage_form_is_one_column(first_live: bool) -> None:
    """P2: compared by its identifier, whatever form it is given in."""
    db = ConnectorArrowDatabase(store())
    first = _with_path_column(rows(["r0"]), live=first_live)
    second = _with_path_column(rows(["r1"]), live=not first_live)
    db.add_records(RESULT, first, record_id_column="__record_id", flush=True)
    db.add_records(RESULT, second, record_id_column="__record_id", flush=True)
    got = ConnectorArrowDatabase(store()).get_all_records(
        RESULT, record_id_column="__record_id"
    )
    assert got is not None and got.num_rows == 2


# ------------------------------------ ITL-1502: keyed reads, one answer per run


class _Calls:
    """Counts what the store asks of SpiralDB: opens by name, listings, scans."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from orcapod.databases import spiraldb_connector

        self.opens = 0
        self.listings = 0
        self.scans: list[dict[str, Any]] = []
        open_table = spiraldb_connector._open_keyed_table  # pyright: ignore[reportPrivateUsage]
        list_tables = spiraldb_connector._list_keyed_tables  # pyright: ignore[reportPrivateUsage]
        scan = fake.FakeKeyedTable.scan

        def counted_open(*args: Any) -> Any:
            self.opens += 1
            return open_table(*args)

        def counted_list(*args: Any) -> Any:
            self.listings += 1
            return list_tables(*args)

        def counted_scan(table: Any, columns: Any = None, **kw: Any) -> Any:
            self.scans.append({"columns": columns, **kw})
            return scan(table, columns, **kw)

        monkeypatch.setattr(spiraldb_connector, "_open_keyed_table", counted_open)
        monkeypatch.setattr(spiraldb_connector, "_list_keyed_tables", counted_list)
        monkeypatch.setattr(fake.FakeKeyedTable, "scan", counted_scan)

    def whole_reads(self) -> list[dict[str, Any]]:
        """Scans that read every row: no key ranges, no filter, no limit."""
        return [
            s
            for s in self.scans
            if s.get("key_ranges") is None
            and s.get("filter") is None
            and s.get("limit") is None
        ]


def _stored_db(ids: list[str]) -> ConnectorArrowDatabase:
    db = ConnectorArrowDatabase(store())
    db.add_records(RESULT, rows(ids), record_id_column="__record_id", flush=True)
    return db


@pytest.mark.usefixtures("root")
def test_a_lookup_by_id_reads_only_its_key(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _stored_db(["r0", "r1", "r2"])
    calls = _Calls(monkeypatch)
    one = db.get_record_by_id(RESULT, b"r1", record_id_column="__record_id")
    some = db.get_records_by_ids(
        RESULT, [b"r2", b"r0", b"absent"], record_id_column="__record_id"
    )
    assert one is not None and one["__record_id"].to_pylist() == [b"r1"]
    assert some is not None and sorted(
        cast(list[bytes], some["__record_id"].to_pylist())
    ) == [
        b"r0",
        b"r2",
    ]
    assert calls.whole_reads() == []
    assert all(s.get("key_ranges") for s in calls.scans)


@pytest.mark.usefixtures("root")
def test_a_lookup_by_value_is_pushed_down(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _stored_db(["r0", "r1", "r2"])
    calls = _Calls(monkeypatch)
    got = db.get_records_with_column_value(
        RESULT, {"__data_context": CONTEXT, "a.b": 1}, record_id_column="__record_id"
    )
    assert got is not None and got["__record_id"].to_pylist() == [b"r1"]
    assert calls.whole_reads() == []
    assert calls.scans and all(s.get("filter") is not None for s in calls.scans)


@pytest.mark.usefixtures("root")
def test_skip_existing_reads_only_the_keys_it_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _stored_db(["r0", "r1", "r2"])
    calls = _Calls(monkeypatch)
    db.add_records(
        RESULT,
        rows(["r1", "r9"], offset=50),
        record_id_column="__record_id",
        skip_duplicates=True,
        flush=True,
    )
    assert calls.whole_reads() == []
    keyed = [s for s in calls.scans if s.get("key_ranges") is not None]
    assert keyed and all(len(s["key_ranges"]) <= 2 for s in keyed)
    got = db.get_all_records(RESULT, record_id_column="__record_id")
    assert got is not None
    assert dict(zip(got["__record_id"].to_pylist(), got["a.b"].to_pylist()))[b"r1"] == 1


@pytest.mark.usefixtures("root")
def test_one_open_by_name_per_absent_table_per_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = ConnectorArrowDatabase(store())
    calls = _Calls(monkeypatch)
    for _ in range(3):
        assert not db.table_exists(RESULT)
    assert calls.opens == 3  # outside a run, nothing is held (N42)
    calls.opens = 0
    with db.run_scope():
        for _ in range(3):
            assert not db.table_exists(RESULT)
        assert calls.opens == 1
        db.add_records(RESULT, rows(["r0"]), record_id_column="__record_id", flush=True)
        assert db.table_exists(RESULT)  # its own create made it present
    calls.opens = 0
    assert db.table_exists(RESULT)
    assert ConnectorArrowDatabase(store()).table_exists(RESULT)


@pytest.mark.usefixtures("root")
def test_one_listing_per_run(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _stored_db(["r0"])
    calls = _Calls(monkeypatch)
    for _ in range(3):
        assert db.list_table_paths(("my_fn",)) == [RESULT]
    assert calls.listings == 3
    calls.listings = 0
    other = ("my_fn", "other", "rdb_v1")
    with db.run_scope():
        for _ in range(3):
            assert db.list_table_paths(("my_fn",)) == [RESULT]
        db.add_records(other, rows(["r0"]), record_id_column="__record_id", flush=True)
        assert db.list_table_paths(("my_fn",)) == sorted([RESULT, other])
    assert calls.listings == 1


@pytest.mark.usefixtures("root")
def test_the_run_map_is_kept_by_the_outermost_scope_and_dropped_on_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = ConnectorArrowDatabase(store())
    view = db.at("anywhere")  # the same connector: one map
    calls = _Calls(monkeypatch)
    with pytest.raises(RuntimeError), db.run_scope():
        with view.run_scope():
            assert not db.table_exists(RESULT)
        assert not db.table_exists(RESULT)  # the inner exit kept the map
        assert calls.opens == 1
        raise RuntimeError("the run failed")
    assert not db.table_exists(RESULT)  # dropped on exit, also when the run raised
    assert calls.opens == 2


@pytest.mark.usefixtures("root")
def test_a_job_run_lists_the_dataset_once_and_opens_each_new_table_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from orcapod import PipelineJob, function_pod
    from orcapod.sources import ArrowTableSource

    @function_pod(output_keys="y")
    def double(x: int) -> int:
        return 2 * x

    source = ArrowTableSource(
        pa.table(
            {
                "k": pa.array(["a", "b"], pa.large_string()),
                "x": pa.array([1, 2], pa.int64()),
            }
        ).cast(
            pa.schema(
                [
                    pa.field("k", pa.large_string(), False),
                    pa.field("x", pa.int64(), False),
                ]
            )
        ),
        tag_columns=["k"],
        source_id="s",
        record_id_columns=["k"],
    )
    db = ConnectorArrowDatabase(store())
    calls = _Calls(monkeypatch)
    job = PipelineJob(name="calls", database=db)
    with job:
        double.pod(source, label="d")
    _ = job.run()
    assert calls.listings == 1
    tables = len(db.list_table_paths())
    assert calls.opens <= tables


_PUSHED: dict[str, Any] = {
    "s": pa.large_string(),
    "b": pa.large_binary(),
    "i": pa.int32(),
    "t": pa.bool_(),
    "f": pa.float64(),
    "ts": pa.timestamp("us", tz="UTC"),
}


@pytest.mark.usefixtures("root")
@pytest.mark.parametrize("seed", range(25))
def test_candidates_are_a_superset_of_the_matches(seed: int) -> None:
    """Whatever is pushed down, the candidates hold every row that matches exactly."""
    import random

    rng = random.Random(seed)
    values: dict[str, list[Any]] = {
        "s": ["x", "y", ""],
        "b": [b"x", b"", b"\x00"],
        "i": [0, 1, -1],
        "t": [True, False],
        "f": [0.0, -0.0, float("nan"), 1.5],
        "ts": [
            dt.datetime(2026, 1, 1, tzinfo=utc),
            dt.datetime(1970, 1, 1, tzinfo=utc),
        ],
    }
    n = 20
    columns: dict[str, Any] = {
        "__record_id": pa.array(
            [f"r{i}".encode() for i in range(n)], pa.large_binary()
        ),
        "__data_context": pa.array([CONTEXT] * n, pa.large_string()),
    }
    for name, arrow_type in _PUSHED.items():
        picks = [
            None if rng.random() < 0.2 else rng.choice(values[name]) for _ in range(n)
        ]
        columns[name] = pa.array(picks, arrow_type)
    table = pa.table(columns)
    connector = store()
    db = ConnectorArrowDatabase(connector)
    db.add_records(RESULT, table, record_id_column="__record_id", flush=True)
    name = connector.table_name(RESULT)
    for _ in range(20):
        asked = rng.sample(sorted(_PUSHED), rng.randint(1, 3))
        equal = {c: rng.choice([*values[c], None]) for c in asked}
        keys = (
            None
            if rng.random() < 0.5
            else [f"r{rng.randrange(n + 3)}".encode() for _ in range(rng.randint(1, 4))]
        )
        candidates = connector.read_candidates(name, keys=keys, equal=equal)
        assert candidates is not None
        whole = connector.read_candidates(name)
        assert whole is not None
        mask: Any = pa.array([True] * whole.num_rows)
        for column, value in equal.items():
            mask = pc.and_(
                mask,
                pc.fill_null(
                    pc.equal(whole[column], pa.scalar(value, whole[column].type)),
                    pa.scalar(False),
                ),
            )
        if keys is not None:
            mask = pc.and_(
                mask, pc.is_in(whole["__record_id"], pa.array(keys, pa.large_binary()))
            )
        matches = set(whole.filter(mask)["__record_id"].to_pylist())
        assert matches <= set(candidates["__record_id"].to_pylist())


# ------------------------------------- verification of PR 595 (0a4189c3)


def _interleaved(first_rows: pa.Table, second_rows: pa.Table) -> tuple[Any, Any]:
    """Writer 1 judges the table absent; writer 2 then creates and fills it; then
    writer 1 goes on. Returns the two databases."""
    first, second = store(), store()
    db1, db2 = ConnectorArrowDatabase(first), ConnectorArrowDatabase(second)
    store1 = first._store  # pyright: ignore[reportPrivateUsage]
    assert store1 is not None
    create_or_open = store1._create_or_open  # pyright: ignore[reportPrivateUsage]

    def the_other_writer_first(table_name: str) -> Any:
        db2.add_records(RESULT, second_rows, record_id_column="__record_id", flush=True)
        return create_or_open(table_name)

    store1._create_or_open = the_other_writer_first  # pyright: ignore[reportPrivateUsage]
    db1.add_records(RESULT, first_rows, record_id_column="__record_id")
    return db1, db2


@pytest.mark.usefixtures("root")
def test_a_writer_never_overwrites_another_writers_manifest() -> None:
    """P1 (N45): a manifest is written only where there is none, or the same one."""
    other_schema = rows(["b0"]).drop_columns(["s"])
    db1, _ = _interleaved(rows(["a0"]), other_schema)
    with pytest.raises(SpiralStoreTableCollisionError):
        db1.flush()
    got = ConnectorArrowDatabase(store()).get_all_records(
        RESULT, record_id_column="__record_id"
    )
    assert got is not None and by_id(got).equals(by_id(other_schema))


@pytest.mark.usefixtures("root")
def test_a_writer_of_the_same_manifest_appends_beside_another() -> None:
    db1, _ = _interleaved(rows(["a0"]), rows(["b0"]))
    db1.flush()
    got = ConnectorArrowDatabase(store()).get_all_records(
        RESULT, record_id_column="__record_id"
    )
    assert got is not None
    assert set(got["__record_id"].to_pylist()) == {b"a0", b"b0"}


@pytest.mark.usefixtures("root")
def test_a_storage_form_identifier_no_type_is_registered_for_is_kept_as_given() -> None:
    """P2: only a registered logical type is made live; any other identifier is kept in
    its storage form, as before."""
    field = pa.field(
        "u",
        pa.large_string(),
        metadata={
            b"ARROW:extension:name": b"test.unregistered.kind",
            b"ARROW:extension:metadata": b'{"version":1}',
        },
    )
    table = rows(["r0"]).append_column(field, pa.array(["x"], pa.large_string()))
    db = ConnectorArrowDatabase(store())
    db.add_records(RESULT, table, record_id_column="__record_id", flush=True)
    got = ConnectorArrowDatabase(store()).get_all_records(
        RESULT, record_id_column="__record_id"
    )
    assert got is not None
    assert got.schema.field("u").type == pa.large_string()
    assert got.schema.field("u").metadata == field.metadata
    assert got["u"].to_pylist() == ["x"]


@pytest.mark.usefixtures("root")
def test_a_table_is_opened_by_id_once_per_run_and_reads_its_own_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _stored_db(["r0"])
    opens: list[str] = []
    by_id = fake.FakeClient.keyed_table_by_id

    def counted(client: Any, table_id: str, **kw: Any) -> Any:
        opens.append(table_id)
        return by_id(client, table_id, **kw)

    monkeypatch.setattr(fake.FakeClient, "keyed_table_by_id", counted)
    with db.run_scope():
        for _ in range(3):
            assert db.get_record_by_id(RESULT, b"r0") is not None
        db.add_records(RESULT, rows(["r1"]), record_id_column="__record_id", flush=True)
        got = db.get_all_records(RESULT, record_id_column="__record_id")
        assert got is not None and set(got["__record_id"].to_pylist()) == {b"r0", b"r1"}
    assert len(opens) == 1


# ------------------------------------- verification r3 of PR 595 (2aef2bd3)


@pytest.mark.usefixtures("root")
def test_a_writer_appends_beside_an_equivalent_manifest_of_other_bytes() -> None:
    """(a) Equivalent schemas (metadata in another order) are one manifest: compared by
    identity, never by bytes."""
    first = rows(["a0"]).replace_schema_metadata({"x": "1", "y": "2"})
    second = rows(["b0"]).replace_schema_metadata({"y": "2", "x": "1"})
    db1, _ = _interleaved(first, second)
    db1.flush()
    got = ConnectorArrowDatabase(store()).get_all_records(
        RESULT, record_id_column="__record_id"
    )
    assert got is not None
    assert set(got["__record_id"].to_pylist()) == {b"a0", b"b0"}


@pytest.mark.usefixtures("root")
def test_a_storage_form_name_that_is_not_utf8_is_kept_as_given() -> None:
    """(b) As at fe4145c7: an extension name of any bytes is kept in storage form."""
    metadata = {b"ARROW:extension:name": b"test.\xff", b"ARROW:extension:metadata": b""}
    field = pa.field("u", pa.large_string(), metadata=metadata)
    table = rows(["r0"]).append_column(field, pa.array(["x"], pa.large_string()))
    db = ConnectorArrowDatabase(store())
    db.add_records(RESULT, table, record_id_column="__record_id", flush=True)
    got = ConnectorArrowDatabase(store()).get_all_records(
        RESULT, record_id_column="__record_id"
    )
    assert got is not None
    assert got.schema.field("u").metadata == metadata
    assert got["u"].to_pylist() == ["x"]


# ------------------------------------- review r4 of PR 595 (7a50c9d8)


def _reordered(table: pa.Table) -> pa.Table:
    """The same columns in another order (order never counts)."""
    return table.select(list(reversed(table.column_names)))


@pytest.mark.usefixtures("root")
def test_a_writer_appends_beside_a_manifest_of_reordered_columns() -> None:
    """At the re-open: the other writer's columns in another order are one manifest."""
    db1, _ = _interleaved(rows(["a0"]), _reordered(rows(["b0"])))
    db1.flush()
    got = ConnectorArrowDatabase(store()).get_all_records(
        RESULT, record_id_column="__record_id"
    )
    assert got is not None
    assert set(got["__record_id"].to_pylist()) == {b"a0", b"b0"}


@pytest.mark.usefixtures("root")
def test_a_table_judged_new_stays_new_beside_an_equivalent_record() -> None:
    """At the judged-empty check: a recorded table with no rows, whose manifest another
    writer records again with its columns in another order (other bytes, equivalent)
    before this write, is still new to this writer."""
    name = names.store_table_name(RESULT)

    def recorded(table: pa.Table) -> dict[str, bytes]:
        manifest = codec.new_manifest(RESULT, "rdb_v1", table.schema)
        return {
            **names.store_table_metadata(RESULT, CONTEXT),
            **codec.manifest_entries(manifest),
        }

    _make_table(name, recorded(rows(["x"])), with_rows=False)
    connector = store()
    db = ConnectorArrowDatabase(connector)
    db.add_records(RESULT, rows(["a0"]), record_id_column="__record_id")
    tables = connector._store  # pyright: ignore[reportPrivateUsage]
    assert tables is not None
    create_or_open = tables._create_or_open  # pyright: ignore[reportPrivateUsage]

    def rerecorded_first(table_name: str) -> Any:
        _table(table_name).set_metadata(recorded(_reordered(rows(["x"]))))
        return create_or_open(table_name)

    tables._create_or_open = rerecorded_first  # pyright: ignore[reportPrivateUsage]
    db.flush()
    got = ConnectorArrowDatabase(store()).get_all_records(
        RESULT, record_id_column="__record_id"
    )
    assert got is not None and got["__record_id"].to_pylist() == [b"a0"]
    # Recorded again by this writer: its own column order, not the other's.
    kept = codec.read_manifest(_table(name).get_metadata(), RESULT)
    assert kept is not None
    assert kept.schema.names == rows(["a0"]).schema.names
    assert kept.schema.names != _reordered(rows(["x"])).schema.names


# ------------------------------------- review r4 of PR 599 (a6a06638)


def _outcome(call: Any) -> Any:
    """What a lookup gives: its rows and schema, or the type of what it raised."""
    try:
        got = call()
    except Exception as exc:  # noqa: BLE001  # the error itself is the outcome compared
        return ("raised", type(exc).__name__)
    if got is None:
        return None
    return (got.schema, got.to_pylist())


def _as_with_whole_reads(
    monkeypatch: pytest.MonkeyPatch, connector: SpiralDBConnector, call: Any
) -> tuple[Any, Any]:
    """The lookup's outcome narrowed, and as PR 595 gives it (a whole-table read)."""
    narrowed = _outcome(call)
    whole_read = type(connector).read_candidates

    def whole(table_name: str, *, keys: Any = None, equal: Any = None) -> Any:
        return whole_read(connector, table_name)

    with monkeypatch.context() as patched:
        patched.setattr(connector, "read_candidates", whole)
        whole_outcome = _outcome(call)
    return narrowed, whole_outcome


def _store_with_rows(
    table: pa.Table,
) -> tuple[SpiralDBConnector, ConnectorArrowDatabase]:
    connector = store()
    db = ConnectorArrowDatabase(connector)
    db.add_records(RESULT, table, record_id_column="__record_id", flush=True)
    return connector, db


@pytest.mark.usefixtures("root")
def test_a_pending_row_is_merged_with_the_stored_schema_as_before(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pending row looked up alone keeps the stored rows' merged (nullable) schema."""
    connector, db = _store_with_rows(rows(["r0"]))
    pending = rows(["r9"])
    pending = pending.cast(
        pa.schema(
            [f.with_nullable(False) if f.name == "a.b" else f for f in pending.schema]
        )
    )
    db.add_records(RESULT, pending, record_id_column="__record_id")
    for call in (
        lambda: db.get_records_by_ids(RESULT, [b"r9"], record_id_column="__record_id"),
        lambda: db.get_record_by_id(RESULT, b"r9", record_id_column="__record_id"),
        lambda: db.get_records_with_column_value(
            RESULT, {"__record_id": b"r9"}, record_id_column="__record_id"
        ),
    ):
        narrowed, whole = _as_with_whole_reads(monkeypatch, connector, call)
        assert narrowed == whole


@pytest.mark.usefixtures("root")
@pytest.mark.parametrize(
    "constraints",
    [{"a.b": 99, "absent": 1}, {"absent": 1}, {"a.b": 99}, {"a.b": 0}],
    ids=str,
)
def test_a_lookup_by_value_gives_what_a_whole_read_gives_errors_included(
    monkeypatch: pytest.MonkeyPatch, constraints: dict[str, Any]
) -> None:
    connector, db = _store_with_rows(rows(["r0", "r1"]))
    narrowed, whole = _as_with_whole_reads(
        monkeypatch,
        connector,
        lambda: db.get_records_with_column_value(
            RESULT, constraints, record_id_column="__record_id"
        ),
    )
    assert narrowed == whole


@pytest.mark.usefixtures("root")
@pytest.mark.parametrize("value", [5, 999])
def test_a_time_column_stored_as_integers_is_not_pushed_down(
    monkeypatch: pytest.MonkeyPatch, value: int
) -> None:
    """A time is stored as an integer: equality on it is left to the exact filter."""
    table = rows(["r0"]).append_column(
        "t", pa.array([5], pa.int32()).view(pa.time32("s"))
    )
    connector, db = _store_with_rows(table)
    calls = _Calls(monkeypatch)
    narrowed, whole = _as_with_whole_reads(
        monkeypatch,
        connector,
        lambda: db.get_records_with_column_value(
            RESULT, {"t": value}, record_id_column="__record_id"
        ),
    )
    assert narrowed == whole
    assert all(s.get("filter") is None for s in calls.scans)


@pytest.mark.usefixtures("root")
def test_a_listing_and_a_lookup_open_a_table_by_id_once_per_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _stored_db(["r0"])
    opens: list[str] = []
    by_id = fake.FakeClient.keyed_table_by_id

    def counted(client: Any, table_id: str, **kw: Any) -> Any:
        opens.append(table_id)
        return by_id(client, table_id, **kw)

    monkeypatch.setattr(fake.FakeClient, "keyed_table_by_id", counted)
    with db.run_scope():
        assert db.list_table_paths(("my_fn",)) == [RESULT]
        assert db.get_record_by_id(RESULT, b"r0") is not None
    assert len(opens) == 1


@pytest.mark.usefixtures("root")
def test_an_existing_empty_table_is_opened_by_name_once_per_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _make_table(names.store_table_name(RESULT), {}, with_rows=False)
    db = ConnectorArrowDatabase(store())
    calls = _Calls(monkeypatch)
    with db.run_scope():
        for _ in range(3):
            assert not db.table_exists(RESULT)
    assert calls.opens == 1


# ------------------------------------- verification r5 of PR 595 (c047e640)


@pytest.mark.usefixtures("root")
def test_a_recorded_empty_table_with_a_corrupt_manifest_is_refused() -> None:
    """When in doubt, refuse: metadata that cannot be decoded never equals anything, so
    a table whose manifest chunk is corrupt (its header unchanged) is never recorded
    over."""
    manifest = codec.new_manifest(RESULT, "rdb_v1", rows(["x"]).schema)
    entries = codec.manifest_entries(manifest)
    corrupt = {
        **entries,
        f"{codec.MANIFEST_KEY}/0": entries[f"{codec.MANIFEST_KEY}/0"] + b" ",
    }
    name = names.store_table_name(RESULT)
    _make_table(
        name,
        {**names.store_table_metadata(RESULT, CONTEXT), **corrupt},
        with_rows=False,
    )
    db = ConnectorArrowDatabase(store())
    with pytest.raises(SpiralStoreTableCollisionError):
        db.add_records(RESULT, rows(["a0"]), record_id_column="__record_id", flush=True)
    assert (
        _table(name).get_metadata()[f"{codec.MANIFEST_KEY}/0"]
        == corrupt[f"{codec.MANIFEST_KEY}/0"]
    )


# ------------------------------------- verification r5 of PR 599 (a236e6c8)


@pytest.mark.usefixtures("root")
@pytest.mark.parametrize(
    "value", [2**64 - 1, 2**63 + 5, 5, 0], ids=["max", "absent-large", "small", "zero"]
)
def test_an_unsigned_column_is_not_pushed_down(
    monkeypatch: pytest.MonkeyPatch, value: int
) -> None:
    """Equality on an unsigned integer is left to the exact filter, so a value above the
    int64 range raises as it does over the whole table (ITL-1505 fixes that error)."""
    table = rows(["r0", "r1"]).append_column("u", pa.array([2**64 - 1, 5], pa.uint64()))
    connector, db = _store_with_rows(table)
    calls = _Calls(monkeypatch)
    narrowed, whole = _as_with_whole_reads(
        monkeypatch,
        connector,
        lambda: db.get_records_with_column_value(
            RESULT, {"u": value}, record_id_column="__record_id"
        ),
    )
    assert narrowed == whole
    assert all(s.get("filter") is None for s in calls.scans)


# ------------------------------------- CodeRabbit on PR 595 (49f1642d)


def _rename(table_name: str, new_name: str) -> None:
    """Rename a table in the fake, keeping its id (as the service does)."""
    found = fake._store().find("p", "d", table_name)  # pyright: ignore[reportPrivateUsage]
    assert found is not None
    found["name"] = new_name
    root = fake._store().root  # pyright: ignore[reportPrivateUsage]
    (root / found["id"] / "table.json").write_text(json.dumps(found))


@pytest.mark.usefixtures("root")
def test_a_listed_table_renamed_since_its_record_was_read_is_refused() -> None:
    """A rename keeps the table id: the name it is listed under now is checked against
    its record also when the record is cached, as a fresh connector checks it."""
    db = ConnectorArrowDatabase(store())
    db.add_records(RESULT, rows(["r0"]), record_id_column="__record_id", flush=True)
    assert db.list_table_paths(("my_fn",)) == [RESULT]
    other = (*RESULT[:-1], "rdb_v9")
    _rename(names.store_table_name(RESULT), names.store_table_name(other))
    with pytest.raises(SpiralStoreTableCollisionError, match="another name"):
        _ = db.list_table_paths(("my_fn",))
    with pytest.raises(SpiralStoreTableCollisionError, match="another name"):
        _ = ConnectorArrowDatabase(store()).list_table_paths(("my_fn",))


def _made_meanwhile(connector: SpiralDBConnector, make: Any) -> None:
    """Run ``make`` (another party's act) just before ``connector`` creates or opens a
    table for its first write."""
    tables = connector._store  # pyright: ignore[reportPrivateUsage]
    assert tables is not None
    create_or_open = tables._create_or_open  # pyright: ignore[reportPrivateUsage]

    def other_first(table_name: str) -> Any:
        make(table_name)
        return create_or_open(table_name)

    tables._create_or_open = other_first  # pyright: ignore[reportPrivateUsage]


@pytest.mark.usefixtures("root")
def test_an_identity_of_another_path_made_meanwhile_is_never_recorded_over() -> None:
    """A table judged absent that appears, empty, with another path's identity and no
    manifest, before the first write records it: refused, its identity kept."""
    other = names.store_table_metadata(("another", "rdb_v1"), CONTEXT)
    connector = store()
    db = ConnectorArrowDatabase(connector)
    db.add_records(RESULT, rows(["a0"]), record_id_column="__record_id")
    _made_meanwhile(connector, lambda name: _make_table(name, other, with_rows=False))
    with pytest.raises(SpiralStoreTableCollisionError, match="another"):
        db.flush()
    metadata = _table(names.store_table_name(RESULT)).get_metadata()
    assert names.read_store_table_metadata(metadata) == names.read_store_table_metadata(
        other
    )
    assert codec.MANIFEST_KEY not in metadata


@pytest.mark.usefixtures("root")
def test_skip_existing_holds_when_another_writer_filled_the_table_meanwhile() -> None:
    """At the first-write recheck, a table another writer recorded (the same manifest)
    and filled: ``skip_duplicates`` keeps that writer's rows, and writes the rest."""
    connector = store()
    db = ConnectorArrowDatabase(connector)
    db.add_records(
        RESULT,
        rows(["r1", "r9"], offset=100),
        record_id_column="__record_id",
        skip_duplicates=True,
    )

    def the_other_writer(_: str) -> None:
        ConnectorArrowDatabase(store()).add_records(
            RESULT, rows(["r1"]), record_id_column="__record_id", flush=True
        )

    _made_meanwhile(connector, the_other_writer)
    db.flush()
    got = ConnectorArrowDatabase(store()).get_all_records(
        RESULT, record_id_column="__record_id"
    )
    assert got is not None
    values = dict(zip(got["__record_id"].to_pylist(), got["a.b"].to_pylist()))
    assert values == {b"r1": 0, b"r9": 101}


@pytest.mark.usefixtures("root")
def test_skip_existing_writes_nothing_when_another_writer_holds_every_key() -> None:
    connector = store()
    db = ConnectorArrowDatabase(connector)
    db.add_records(
        RESULT,
        rows(["r1"], offset=100),
        record_id_column="__record_id",
        skip_duplicates=True,
    )

    def the_other_writer(_: str) -> None:
        ConnectorArrowDatabase(store()).add_records(
            RESULT, rows(["r1"]), record_id_column="__record_id", flush=True
        )

    _made_meanwhile(connector, the_other_writer)
    db.flush()
    got = ConnectorArrowDatabase(store()).get_all_records(
        RESULT, record_id_column="__record_id"
    )
    assert got is not None and got["a.b"].to_pylist() == [0]
    assert db.table_exists(RESULT)


@pytest.mark.usefixtures("root")
def test_a_writer_beside_an_equivalent_manifest_keeps_the_recorded_one() -> None:
    """The writer's own connector reads the columns as the table records them (the
    other writer's order), as a fresh connector does."""
    db1, _ = _interleaved(rows(["a0"]), _reordered(rows(["b0"])))
    db1.flush()
    mine = db1.get_table_schema(RESULT)
    fresh = ConnectorArrowDatabase(store()).get_table_schema(RESULT)
    assert mine is not None and fresh is not None
    assert mine.names == fresh.names == _reordered(rows(["b0"])).schema.names


@pytest.mark.usefixtures("root")
def test_an_existing_empty_table_is_read_as_no_rows() -> None:
    """``iter_batches`` yields nothing for an existing table with no rows (one a first
    write left before its rows); a missing table is still a ``KeyError``."""
    connector = store()
    name = connector.table_name(RESULT)
    with pytest.raises(KeyError):
        _ = list(connector.iter_batches(f'SELECT * FROM "{name}"'))
    _make_table(name, names.store_table_metadata(RESULT, CONTEXT), with_rows=False)
    fresh = store()
    assert fresh.table_name(RESULT) == name
    assert not fresh.has_table(name)
    assert list(fresh.iter_batches(f'SELECT * FROM "{name}"')) == []
    other = store()
    _ = other.table_name(RESULT)
    assert list(other.iter_batches(f'SELECT * FROM "{name}"')) == []


@pytest.mark.usefixtures("root")
def test_logical_types_are_read_with_the_data_context_the_table_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The converter of the table's recorded data context makes logical types live,
    and rows of another data context are refused for that table."""
    import orcapod.contexts as contexts

    db = ConnectorArrowDatabase(store())
    db.add_records(RESULT, rows(["r0"]), record_id_column="__record_id", flush=True)
    asked: list[object] = []
    real = contexts.resolve_context

    def resolve(context_info: Any = None) -> Any:
        asked.append(context_info)
        return real(context_info)

    monkeypatch.setattr(contexts, "resolve_context", resolve)
    fresh = ConnectorArrowDatabase(store())
    assert fresh.get_all_records(RESULT, record_id_column="__record_id") is not None
    assert CONTEXT in asked
    with pytest.raises(ValueError, match="data context"):
        fresh.add_records(
            RESULT,
            rows(["r1"], context="std:v9:other"),
            record_id_column="__record_id",
            flush=True,
        )


@pytest.mark.usefixtures("root")
def test_a_closed_store_connector_refuses_every_call_and_releases_its_tables() -> None:
    connector = store()
    name = connector.table_name(RESULT)
    shared = connector.share_client()
    connector.close()
    assert connector._store is None  # pyright: ignore[reportPrivateUsage]
    for call in (
        lambda: connector.get_pk_columns(name),
        lambda: connector.table_name(RESULT),
        lambda: connector.has_table(name),
        lambda: connector.get_table_names(),
        lambda: connector.validate_records(rows(["r0"])),
        lambda: connector.unstorable_column_reason("a", pa.int64(), primary_key=False),
    ):
        with pytest.raises(RuntimeError, match="closed"):
            _ = call()
    with pytest.raises(ValueError, match="store-mode"):
        _ = connector.to_config()
    # the connector sharing its client keeps its own reference
    assert shared.get_pk_columns(name) == ["__record_id"]
    assert not shared.has_table(name)
