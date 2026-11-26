"""A SpiralDB connector in store mode, under ``ConnectorArrowDatabase`` (ITL-1498).

On the fake (``fake_spiral_sdk``). The live run of the same operations is in
``test_spiraldb_connector_integration.py``.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
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
