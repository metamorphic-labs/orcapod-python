"""A SpiralDB connector in store mode, under ``ConnectorArrowDatabase`` (ITL-1498).

On the fake (``fake_spiral_sdk``). The live run of the same operations is in
``test_spiraldb_connector_integration.py``.
"""

from __future__ import annotations

import datetime as dt
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
