"""Conformance of reading: the same tests on the fake and, opt-in, on SpiralDB (ITL-1186).

The fake's runs are always on. The live runs need ``SPIRAL_INTEGRATION_TESTS=1``,
``SPIRAL_PROJECT_ID`` and a sign-in spiral-sdk finds in the environment (CI:
``SPIRAL_CLIENT_ID`` and ``SPIRAL_PRIVATE_KEY``, see ``RELEASING.md``); with the first two
set, a missing sign-in fails the tests rather than skipping them. ``SPIRAL_SERVER_URL``
names another address.

spiral-sdk cannot drop a keyed table, so a live run creates nothing anew: it uses fixed
tables in the dataset ``orcapod_conformance`` (``ORCAPOD_SPIRAL_CONFORMANCE_DATASET`` names
another), made on the first run and written the same rows at each. It writes through
spiral-sdk itself, never through Orcapod, which only reads.
"""

from __future__ import annotations

import datetime as dt
import decimal
import math
import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod.databases import SpiralDBConnector
from orcapod.databases import spiraldb_schema_table as st
from orcapod.databases.spiraldb_connector import SpiralDBTableNotFoundError
from orcapod.sources import SpiralDBTableSource
from tests.test_databases import fake_spiral_sdk as fake

_LIVE = os.environ.get("SPIRAL_INTEGRATION_TESTS") == "1"
_PROJECT = os.environ.get("SPIRAL_PROJECT_ID", "")
_URL = os.environ.get("SPIRAL_SERVER_URL", "https://api.spiraldb.dev")
_DATASET = os.environ.get("ORCAPOD_SPIRAL_CONFORMANCE_DATASET", "orcapod_conformance")

_INNER = pa.struct([pa.field("q", pa.int8()), pa.field("b", pa.large_string(), False)])
_UNITS = pa.schema(
    [
        pa.field("zz", pa.int64(), False, {"unit": "mV"}),
        pa.field(
            "g",
            pa.struct(
                [
                    pa.field("z", pa.int8(), False),
                    pa.field("a", _INNER, False),
                    pa.field("m", pa.large_string()),
                ]
            ),
            False,
        ),
        pa.field("k1", pa.large_string(), False),
        pa.field("aa", pa.float64()),
        pa.field(
            "ls",
            pa.large_list(pa.struct([("z", pa.int8()), ("a", pa.int8())])),
        ),
        pa.field("k2", pa.int32(), False),
        pa.field("fx", pa.list_(pa.field("item", pa.float32(), False), 2)),
    ],
    metadata={"who": "conformance"},
)
_ROWS = pa.Table.from_pylist(
    [
        {
            "zz": i,
            "g": {"z": i, "a": {"q": None, "b": "x"}, "m": None},
            "k1": key,
            "aa": None if i == 1 else i / 2,
            "ls": [{"z": 1, "a": 2}],
            "k2": i % 2,
            "fx": [1.0, 2.0],
        }
        for i, key in enumerate(["b", "a", "c"])
    ],
    schema=_UNITS,
)
_TYPED = pa.table(
    {
        "k": pa.array([1, 2, 3], pa.int64()),
        "h": pa.array([0.5, -0.0, float("nan")], pa.float64()).cast(pa.float16()),
        "f": pa.array([0.1, -0.0, float("nan")], pa.float32()),
        "d": pa.array([0.1, -0.0, float("nan")]),
        "s": pa.array(["x", "é", None], pa.large_string()),
        "b": pa.array([b"x", b"", None], pa.large_binary()),
        "u": pa.array([2**64 - 1, 0, 1], pa.uint64()),
        "sec": pa.array([1, 2, 3], pa.timestamp("s")),
        "utc": pa.array([1, 2, 3], pa.timestamp("us", tz="UTC")),
        "d32": pa.array([0, 1, None], pa.date32()),
    }
)


@dataclass(frozen=True)
class _Place:
    """Where the tables are."""

    project: str
    dataset: str
    overrides: dict[str, str] | None

    def connector(self) -> SpiralDBConnector:
        """Return a new connector."""
        return SpiralDBConnector(self.project, self.dataset, self.overrides)

    def source(self, name: str, **options: Any) -> SpiralDBTableSource:
        """Return a source on a table."""
        return SpiralDBTableSource(
            self.project,
            name,
            dataset=self.dataset,
            overrides=self.overrides,
            tag_only_columns=True,
            **options,
        )


def _seed(client: Any, place: _Place) -> None:
    """Write the tables and, once, their schema rows, through the SDK."""
    project = client.project(place.project)
    made = {}
    for name, keys, rows in (
        ("units", ["k2", "k1"], _ROWS),
        ("typed", ["k"], _TYPED),
    ):
        key_schema = pa.schema([rows.schema.field(k) for k in keys])
        table = project.create_keyed_table(
            place.dataset, name, key_schema, exist_ok=True
        )
        made[name] = (table.append(rows), rows.schema)
    schemas = project.create_keyed_table(
        place.dataset, st.SCHEMA_TABLE, st.KEY_SCHEMA, exist_ok=True
    )
    if st.require_schema_table(schemas):
        return
    rows = []
    for name, (table, written) in made.items():
        presented = pa.schema(
            [_in_order(f, written.field(f.name)) for f in table.schema()],
            metadata=written.metadata,
        )
        rows.append(
            st.SchemaRow(
                table.resource_id,
                name,
                tuple(table.key_columns),
                presented.serialize().to_pybytes(),
                st.FROM_RECORD,
                int(table.version_id),
            )
        )
    scope = st.Scope(place.project, place.dataset, "0" * 64)
    for values in (*(r.stored() for r in rows), st.marker_values(scope, rows)):
        schemas.append(
            pa.Table.from_pylist(
                [{"written_at": "", "written_by": "", **values}], schema=st.ROW_SCHEMA
            )
        )


def _in_order(found: Any, written: Any) -> Any:
    """Return the written field with its struct fields in SpiralDB's order."""
    if not pa.types.is_struct(written.type):
        return written
    return written.with_type(
        pa.struct([_in_order(f, written.type.field(f.name)) for f in found.type])
    )


@pytest.fixture(
    scope="module",
    params=[
        "fake",
        pytest.param(
            "live",
            marks=pytest.mark.skipif(
                not (_LIVE and _PROJECT),
                reason="Set SPIRAL_INTEGRATION_TESTS=1 and SPIRAL_PROJECT_ID",
            ),
        ),
    ],
)
def place(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[_Place]:
    """The seeded tables, on the fake or on SpiralDB."""
    if request.param == "fake":
        root: Path = tmp_path_factory.mktemp("spiral")
        with fake.install(root):
            made = _Place("p", _DATASET, None)
            _seed(fake.FakeClient(), made)
            yield made
        return
    made = _Place(_PROJECT, _DATASET, {"server.url": _URL})
    with made.connector() as connector:
        _seed(connector._client, made)  # pyright: ignore[reportPrivateUsage]  # the SDK's client, for the test's own writes
    yield made


def test_the_listing_holds_the_tables_and_not_the_schema_table(place: _Place) -> None:
    names = place.connector().get_table_names()
    assert {"units", "typed"} <= set(names)
    assert st.SCHEMA_TABLE not in names
    assert names == sorted(names)


def test_the_presented_schema(place: _Place) -> None:
    connector = place.connector()
    schema = connector.get_arrow_schema("units")
    assert connector.get_pk_columns("units") == ["k2", "k1"]
    assert schema.names == ["k2", "k1", "aa", "fx", "ls", "zz", "g"]
    group = schema.field("g").type
    assert [f.name for f in group] == ["m", "z", "a"]
    assert [f.name for f in group.field("a").type] == ["b", "q"]
    assert [f.name for f in schema.field("ls").type.value_type] == ["z", "a"]
    assert schema.metadata == {b"who": b"conformance"}
    assert schema.field("zz").metadata == {b"unit": b"mV"}
    assert [f.name for f in schema if not f.nullable] == ["k2", "k1", "zz", "g"]
    assert not group.field("z").nullable
    assert group.field("m").nullable
    assert not group.field("a").type.field("b").nullable
    for batch in connector.scan_batches("units"):
        assert batch.schema.equals(schema, check_metadata=True)


def test_rows_in_key_order_with_their_values(place: _Place) -> None:
    table = pa.Table.from_batches(place.connector().scan_batches("units"))
    keys = list(zip(table["k2"].to_pylist(), table["k1"].to_pylist()))
    assert keys == [(0, "b"), (0, "c"), (1, "a")]
    by_key = {(r["k2"], r["k1"]): r for r in _ROWS.to_pylist()}
    assert [by_key[k] for k in keys] == table.to_pylist()


def test_source_identity_and_rows(place: _Place) -> None:
    source = place.source("units")
    assert source.source_id == (
        f"spiraldb_table_v1:{place.project}.{place.dataset}.units@"
        f"{place.connector().get_spiral_table_id('units')}"
    )
    tags, data = source.output_schema()
    assert dict(tags) == {"k2": int, "k1": str}
    assert data["zz"] is int
    rows = [(t.as_dict(), d.as_dict()) for t, d in source.iter_data()]
    assert [t["k1"] for t, _ in rows] == ["b", "c", "a"]
    again = place.source("units")
    assert again.content_hash() == source.content_hash()
    assert again.pipeline_hash() == source.pipeline_hash()


@pytest.mark.parametrize("limit", [1, 2, 3, 9])
def test_limit_reads_the_first_keys(place: _Place, limit: int) -> None:
    table = pa.Table.from_batches(place.connector().scan_batches("units", limit=limit))
    assert table["k1"].to_pylist() == ["b", "c", "a"][:limit]


@pytest.mark.parametrize(
    ("where", "expected"),
    [
        ({"h": 0.5}, [1]),
        ({"h": -0.0}, [2]),
        ({"h": math.nan}, [3]),
        ({"f": 0.1}, []),
        ({"f": -0.0}, [2]),
        ({"f": math.nan}, [3]),
        ({"f": 1e300}, []),
        ({"d": 0.1}, [1]),
        ({"d": 0.0}, []),
        ({"s": "é"}, [2]),
        ({"b": b""}, [2]),
        ({"u": 2**64 - 1}, [1]),
        ({"u": 0}, [2]),
        ({"sec": dt.datetime(1970, 1, 1, 0, 0, 1)}, [1]),
        ({"utc": dt.datetime(1970, 1, 1, 0, 0, 0, 2, dt.timezone.utc)}, [2]),
        ({"d32": dt.date(1970, 1, 2)}, [2]),
        ({"s": "x", "k": 1}, [1]),
        ({"k": 1, "s": "é"}, []),
        ({"h": 0.5, "s": "x"}, [1]),
        ({"s": "x", "h": 0.5}, [1]),
        ({"k": 1, "s": "x"}, [1]),
        ({"s": "é", "k": 1}, []),
    ],
)
def test_where(place: _Place, where: dict[str, Any], expected: list[int]) -> None:
    batches = place.connector().scan_batches("typed", where=where)
    assert [k for b in batches for k in b.column("k").to_pylist()] == expected


def test_counts(place: _Place) -> None:
    table = place.connector().lookup_table("units")
    assert table.count_rows(at_most=2) == 2
    assert table.row_count_estimate() == 3


def test_versions(place: _Place) -> None:
    connector = place.connector()
    version = connector.get_table_version("units")
    assert version > 0
    pinned = pa.Table.from_batches(connector.scan_batches("units", asof=version))
    latest = pa.Table.from_batches(connector.scan_batches("units"))
    assert pinned.equals(latest)


def test_a_missing_table_and_dataset(place: _Place) -> None:
    with pytest.raises(SpiralDBTableNotFoundError):
        place.connector().lookup_table("absent")
    missing = SpiralDBConnector(place.project, "absent_dataset", place.overrides)
    with pytest.raises(SpiralDBTableNotFoundError):
        missing.lookup_table("absent")
    assert missing.get_table_names() == []


def test_an_early_close(place: _Place) -> None:
    batches: Any = place.connector().scan_batches("units")
    assert next(batches).num_rows > 0
    batches.close()


def _client(place: _Place) -> Any:
    """Return an SDK client on the place, for the test's own reads and writes."""
    if place.overrides is None:
        return fake.FakeClient()
    return place.connector()._client  # pyright: ignore[reportPrivateUsage]  # the SDK's client, for the test's own writes


def test_table_metadata(place: _Place) -> None:
    """Table metadata as measured with spiral-sdk 0.16.78 (the fake's ``table-metadata``).

    Fixed keys only, so a live table does not grow from run to run.
    """
    client = _client(place)
    key = pa.schema([pa.field("k", pa.large_string(), False)])
    table = client.project(place.project).create_keyed_table(
        place.dataset, "table_metadata", key, exist_ok=True
    )
    token = uuid.uuid4().hex.encode()
    table.set_metadata({"conformance.a": token, "conformance.b": b"before"})
    table.set_metadata({"conformance.b": token, "conformance.empty": b""})
    found = table.get_metadata()
    assert found["conformance.a"] == token  # an upsert keeps the other keys
    assert found["conformance.b"] == token
    assert found["conformance.empty"] == b""  # stored as empty, not deleted

    # Not versioned: a snapshot taken before a write sees metadata set after it, and a
    # metadata write makes no snapshot.
    before = client.keyed_table(place.project, place.dataset, "table_metadata")
    after = before.append(
        pa.table({"k": pa.array(["r"], pa.large_string()), "v": pa.array([token])})
    )
    after.set_metadata({"conformance.after": token})
    assert before.get_metadata()["conformance.after"] == token
    pinned = client.keyed_table_by_id(before.resource_id, asof=int(before.version_id))
    assert pinned.get_metadata()["conformance.after"] == token
    again = client.keyed_table(place.project, place.dataset, "table_metadata")
    assert again.version_id == after.version_id

    # Limits: a key up to 255 UTF-8 bytes, a key and its value together up to 8,192.
    table.set_metadata(
        {"m" * 255: b"x" * (8192 - 255), "conformance.max": b"x" * (8192 - 15)}
    )
    for refused in (
        {"m" * 256: token},
        {"conformance.big": b"x" * (8192 - 15 + 1)},
        {"m" * 255: b"x" * (8192 - 255 + 1)},
    ):
        with pytest.raises(Exception, match=r"status 400"):  # noqa: PT011  # the SDK's error type differs from the fake's
            table.set_metadata(refused)
    found = table.get_metadata()
    assert found["m" * 255] == b"x" * (8192 - 255)
    assert "m" * 256 not in found
    assert "conformance.big" not in found


def test_the_fake_keeps_concurrent_upserts_of_different_keys(tmp_path: Path) -> None:
    """Two clients upserting different keys at once keep both (measured live, 0.16.78).

    Review of PR 589, finding 1: each read waits for the other writer's read, so an
    upsert that is not atomic would lose one key.
    """
    import threading

    with fake.install(tmp_path):
        key = pa.schema([pa.field("k", pa.large_string(), False)])
        made = fake.FakeClient().project("p").create_keyed_table("d", "t", key)
        both_read = threading.Barrier(2, timeout=0.5)
        original = fake._Store.metadata  # pyright: ignore[reportPrivateUsage]  # the read an upsert starts from

        def metadata_after_the_other_read(self: Any, table_id: str) -> dict[str, bytes]:
            found = original(self, table_id)
            try:
                _ = both_read.wait()
            except threading.BrokenBarrierError:
                pass  # an atomic upsert holds the other writer back: no meeting
            return found

        def upsert(name: str) -> None:
            fake.FakeClient().keyed_table_by_id(made.resource_id).set_metadata(
                {name: b"1"}
            )

        with patch.object(fake._Store, "metadata", metadata_after_the_other_read):  # pyright: ignore[reportPrivateUsage]
            writers = [threading.Thread(target=upsert, args=(n,)) for n in "ab"]
            for w in writers:
                w.start()
            for w in writers:
                w.join()
        assert made.get_metadata() == {"a": b"1", "b": b"1"}


# ------------------------------------------------ the store codec (ITL-1497)


map_of: Any = pa.map_  # the stubs lack keys_sorted


def _codec_rows() -> pa.Table:
    """Rows of a store table: names the codec escapes, and every kept kind of value."""
    from orcapod.databases import spiral_store

    utc = dt.timezone.utc
    inner = pa.struct([pa.field("a.b", pa.int8(), False), pa.field("__c", pa.int8())])
    keys = [b"r0", b"r1", b"r2"]
    columns: dict[str, Any] = {  # pyarrow arrays of several types
        spiral_store.KEY_COLUMN: pa.array(keys, pa.large_binary()),
        "__data_context": pa.array(["std:v1:default"] * 3, pa.large_string()),
        "a.b": pa.array([1, 2, None], pa.int64()),
        "$x": pa.array([True, None, False]),
        "*": pa.array([b"", b"\x00", None], pa.large_binary()),
        "ok$*;": pa.array([0, 2**64 - 1, None], pa.uint64()),
        "ts_utc": pa.array(
            [
                dt.datetime.max.replace(tzinfo=utc),
                dt.datetime.min.replace(tzinfo=utc),
                None,
            ],
            pa.timestamp("us", tz="UTC"),
        ),
        "ts_ns": pa.array([2**63 - 1, -(2**63), 0], pa.int64()).view(
            pa.timestamp("ns", tz="-03:30")
        ),
        "time": pa.array([-1, 86_400_000_000, None], pa.int64()).view(pa.time64("us")),
        "f32": pa.array([0x7FC00001, 0x80000000, 0xFF800001], pa.uint32()).view(
            pa.float32()
        ),
        "d32": pa.array(
            [decimal.Decimal("1.23"), None, decimal.Decimal("-0.01")],
            pa.decimal32(5, 2),
        ),
        "d64": pa.array([86_400_000 + 1, 0, None], pa.int64()).view(pa.date64()),
        "s": pa.array([{"a.b": 1, "__c": None}] * 3, inner),
        "ls": pa.array(
            [[{"a.b": 1, "__c": 2}, {"a.b": 2, "__c": None}], None, []],
            pa.large_list(inner),
        ),
        "fl": pa.array(
            [[{"a.b": 1, "__c": 2}], None, [{"a.b": 3, "__c": None}]],
            pa.list_(inner, 1),
        ),
        "m": pa.array(
            [[("b", 1), ("a", 2)], None, []],
            map_of(pa.large_string(), pa.int64(), keys_sorted=True),
        ),
        "n": pa.array([None, None, None], pa.null()),
    }
    return pa.table(columns)


def test_the_store_codec_round_trips(place: _Place) -> None:
    """Encoded rows written through spiral-sdk and decoded back are the rows, exactly.

    A fixed table and fixed rows, so a live run writes the same rows each time.
    """
    from orcapod.databases import spiral_store

    rows = _codec_rows()
    manifest = spiral_store.new_manifest(("conformance", "rdb_v1"), "test", rows.schema)
    encoded = spiral_store.encode_rows(rows)
    key = pa.schema([pa.field(spiral_store.KEY_COLUMN, pa.large_binary(), False)])
    client = _client(place)
    table = client.project(place.project).create_keyed_table(
        place.dataset, "store_codec", key, exist_ok=True
    )
    table = table.append(encoded)
    read_back = client.keyed_table_by_id(table.resource_id).scan().to_table()
    decoded = spiral_store.decode_rows(read_back, manifest)
    order = pa.array(
        sorted(
            range(decoded.num_rows),
            key=lambda i: decoded[spiral_store.KEY_COLUMN][i].as_py(),
        ),
        pa.int64(),
    )
    got = decoded.take(order)
    want = rows.cast(spiral_store.logical_schema(rows.schema))
    assert got.schema.equals(want.schema, check_metadata=True)
    for name in want.column_names:
        a, b = got.column(name).combine_chunks(), want.column(name).combine_chunks()
        if pa.types.is_floating(a.type):
            assert a.view(pa.uint32()).equals(b.view(pa.uint32())), name
        else:
            assert a.equals(b), name


@pytest.mark.parametrize(
    "column",
    [
        pa.table({"*": pa.array([1])}),
        pa.table({"": pa.array([1])}),
        pa.table({"__x": pa.array([1])}),
        pa.table({"$x": pa.array([1])}),
        pa.table({"a.b": pa.array([1])}),
        pa.table({"s": pa.array([{"a.b": 1}], pa.struct([("a.b", pa.int8())]))}),
        pa.table({"t": pa.array([86_400], pa.int32()).view(pa.time32("s"))}),
        pa.table({"t": pa.array([-1], pa.int64()).view(pa.time64("ns"))}),
        pa.table({"b": pa.array([b"ab"], pa.binary(2))}),
        pa.table({"d": pa.array([1], pa.duration("s"))}),
        pa.table({"s": pa.array([{"a": 1}, None], pa.struct([("a", pa.int8())]))}),
        pa.table({"s": pa.array([{}], pa.struct([]))}),
    ],
    ids=lambda t: f"{t.schema.names[0]!r}: {t.schema.types[0]}",
)
def test_what_the_store_codec_avoids_is_refused(
    place: _Place, column: pa.Table
) -> None:
    """The service refuses each name and value the codec never writes; nothing is written."""
    client = _client(place)
    key = pa.schema([pa.field("k", pa.large_string(), False)])
    table = client.project(place.project).create_keyed_table(
        place.dataset, "store_codec_refusals", key, exist_ok=True
    )
    n = column.num_rows
    rows = column.add_column(
        0, "k", pa.array([f"r{i}" for i in range(n)], pa.large_string())
    )
    with pytest.raises(Exception):  # noqa: B017, PT011  # the SDK's error type differs from the fake's
        _ = table.append(rows)
    again = client.keyed_table_by_id(table.resource_id)
    assert again.version_id == table.version_id


def _written_and_read(place: _Place, name: str, column: pa.Table) -> pa.Table:
    """Write ``column`` under keys r0... to a fixed table and read those rows back."""
    client = _client(place)
    key = pa.schema([pa.field("k", pa.large_string(), False)])
    table = client.project(place.project).create_keyed_table(
        place.dataset, name, key, exist_ok=True
    )
    keys = pa.array([f"r{i}" for i in range(column.num_rows)], pa.large_string())
    table = table.append(column.add_column(0, "k", keys))
    read = client.keyed_table_by_id(table.resource_id).scan().to_table()
    return read.filter(pc.is_in(read.column("k"), keys)).sort_by("k")


def test_a_zero_width_fixed_size_list_of_strings_round_trips(place: _Place) -> None:
    """Review of PR 590, finding 3 (the fake's rebuild of a zero-width list)."""
    value = [[], None, []]
    column = pa.table({"z": pa.array(value, pa.list_(pa.large_string(), 0))})
    got = _written_and_read(place, "zero_width_lists", column)
    assert got.column("z").to_pylist() == value


def test_nul_is_cut_in_nested_field_names(place: _Place) -> None:
    """Review of PR 590, finding 4: the service cuts a name at NUL at every depth
    (measured live with spiral-sdk 0.16.78)."""
    inner = pa.struct([("x\0y", pa.int8()), ("z", pa.int8())])
    column = pa.table(
        {
            "s": pa.array([{"x\0y": 1, "z": 2}], inner),
            "l": pa.array([[{"x\0y": 3, "z": 4}]], pa.large_list(inner)),
        }
    )
    got = _written_and_read(place, "nul_in_field_names", column)
    assert got.column("s").to_pylist() == [{"x": 1, "z": 2}]
    assert got.column("l").to_pylist() == [[{"x": 3, "z": 4}]]


# ------------------------------------------------ store mode (ITL-1498)


def test_a_store_writes_reads_and_lists_its_table(place: _Place) -> None:
    """Store mode under ``ConnectorArrowDatabase``: a fixed path, so a fixed table; the
    same rows are written on every run (an upsert)."""
    from orcapod.databases import ConnectorArrowDatabase
    from orcapod.databases import spiral_store_names as names

    path = ("conformance_store", "0123456789abcdef" * 4, "rdb_v1")
    written = _codec_rows()
    store = SpiralDBConnector(
        place.project, place.dataset, place.overrides, _store=True
    )
    db = ConnectorArrowDatabase(store)
    db.add_records(path, written, record_id_column="__record_id", flush=True)
    fresh = ConnectorArrowDatabase(
        SpiralDBConnector(place.project, place.dataset, place.overrides, _store=True)
    )
    assert fresh.table_exists(path)
    got = fresh.get_all_records(path, record_id_column="__record_id")
    assert got is not None
    got = got.sort_by("__record_id")
    assert got.column_names == written.column_names
    for name in written.column_names:
        a, b = got.column(name).combine_chunks(), written.column(name).combine_chunks()
        if pa.types.is_floating(a.type):
            assert a.view(pa.uint32()).equals(b.view(pa.uint32())), name
        else:
            assert a.equals(b.cast(a.type)), name
    assert path in fresh.list_table_paths(("conformance_store",))
    client = _client(place)
    table = client.keyed_table(
        place.project, place.dataset, names.store_table_name(path)
    )
    record = names.read_store_table_metadata(table.get_metadata())
    assert record is not None and record.path == path


def test_the_fake_never_shows_a_half_written_metadata_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CodeRabbit on PR 589: a reader during a metadata write sees a whole mapping.

    Every file write is made to call a reader after it has truncated its file and before
    it has written anything.
    """
    with fake.install(tmp_path):
        key = pa.schema([pa.field("k", pa.large_string(), False)])
        table = fake.FakeClient().project("p").create_keyed_table("d", "t", key)
        table.set_metadata({"a": b"1"})
        seen: list[dict[str, bytes]] = []
        original = Path.write_text

        def write_text_with_a_reader(
            self: Path, data: str, *args: Any, **kw: Any
        ) -> int:
            with self.open("w") as handle:
                handle.flush()
                seen.append(
                    fake.FakeClient()
                    .keyed_table_by_id(table.resource_id)
                    .get_metadata()
                )
            return original(self, data, *args, **kw)

        monkeypatch.setattr(Path, "write_text", write_text_with_a_reader)
        table.set_metadata({"b": b"2"})
        monkeypatch.undo()
        assert seen == [{"a": b"1"}]
        assert table.get_metadata() == {"a": b"1", "b": b"2"}


@pytest.mark.parametrize("child", ["x", "x\0y"])
def test_a_null_under_a_non_null_column_is_refused_whatever_its_names(
    place: _Place, child: str
) -> None:
    """Review of PR 590 (round 2): cutting a nested name at NUL keeps the column's flag."""
    client = _client(place)
    key = pa.schema([pa.field("k", pa.large_string(), False)])
    table = client.project(place.project).create_keyed_table(
        place.dataset, "nul_names_non_null", key, exist_ok=True
    )
    item = pa.struct([(child, pa.int8())])
    schema = pa.schema(
        [
            pa.field("k", pa.large_string(), False),
            pa.field("l", pa.large_list(item), nullable=False),
        ]
    )
    rows = pa.Table.from_arrays(
        [
            pa.array(["r0", "r1"], pa.large_string()),
            pa.array([[{child: 1}], None], pa.large_list(item)),
        ],
        schema=schema,
    )
    with pytest.raises(Exception):  # noqa: B017, PT011  # the SDK's error type differs from the fake's
        _ = table.append(rows)


def _encoded(array: Any, how: str) -> Any:
    """``array`` (of two equal values) dictionary-encoded or run-end encoded, built
    from parts, as pyarrow encodes no struct itself."""
    one = array.slice(0, 1)
    if how == "dictionary":
        return pa.DictionaryArray.from_arrays(pa.array([0, 0], pa.int32()), one)
    return pa.RunEndEncodedArray.from_arrays(pa.array([2], pa.int32()), one)


@pytest.mark.parametrize("how", ["dictionary"])
def test_the_fake_refuses_an_encoded_time_outside_a_day(
    tmp_path: Path, how: str
) -> None:
    """CodeRabbit on PR 590: the time rule holds inside encoded arrays too. (pyarrow
    refuses to build a run-end-encoded array of such a time, so only a dictionary can
    carry one.)"""
    with fake.install(tmp_path):
        key = pa.schema([pa.field("k", pa.large_string(), False)])
        table = fake.FakeClient().project("p").create_keyed_table("d", "t", key)
        times = pa.array([-1, -1], pa.int64()).view(pa.time64("ns"))
        rows = pa.table(
            {"k": pa.array(["a", "b"], pa.large_string()), "t": _encoded(times, how)}
        )
        with pytest.raises(fake.SpiralError, match="time"):
            _ = table.append(rows)


@pytest.mark.parametrize("how", ["dictionary", "run_end"])
def test_the_fake_cuts_nul_in_names_inside_encoded_arrays(
    tmp_path: Path, how: str
) -> None:
    """CodeRabbit on PR 590: the NUL rule holds inside encoded arrays too."""
    with fake.install(tmp_path):
        key = pa.schema([pa.field("k", pa.large_string(), False)])
        table = fake.FakeClient().project("p").create_keyed_table("d", "t", key)
        values = pa.array([{"x\0y": 1}, {"x\0y": 1}], pa.struct([("x\0y", pa.int8())]))
        rows = pa.table(
            {"k": pa.array(["a", "b"], pa.large_string()), "s": _encoded(values, how)}
        )
        stored = table.append(rows).scan().to_table()
        assert stored.column("s").to_pylist() == [{"x": 1}, {"x": 1}]


def test_the_fake_cuts_nul_in_names_inside_nested_encoded_arrays(
    tmp_path: Path,
) -> None:
    """CodeRabbit on PR 590 (round 2): an encoded struct inside a list is cut too."""
    with fake.install(tmp_path):
        key = pa.schema([pa.field("k", pa.large_string(), False)])
        table = fake.FakeClient().project("p").create_keyed_table("d", "t", key)
        one = pa.array([{"x\0y": 1}], pa.struct([("x\0y", pa.int8())]))
        encoded = pa.DictionaryArray.from_arrays(pa.array([0, 0], pa.int32()), one)
        lists = pa.LargeListArray.from_arrays(pa.array([0, 1, 2], pa.int64()), encoded)
        rows = pa.table({"k": pa.array(["a", "b"], pa.large_string()), "l": lists})
        stored = table.append(rows).scan().to_table()
        assert stored.column("l").to_pylist() == [[{"x": 1}], [{"x": 1}]]


# ---------------------------------- verification r3 of PR 590 (0dc34c95)


class _ProbeExtension(pa.ExtensionType):
    """An extension type of the tests' own, over any storage."""

    def __init__(self, storage: Any) -> None:
        super().__init__(storage, "orcapod.test.probe")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: bytes) -> Any:
        return cls(storage_type)


def _struct_one(name: str) -> Any:
    return pa.array([{name: 1}], pa.struct([(name, pa.int8())]))


def _dictionary_of(values: Any, n: int) -> Any:
    return pa.DictionaryArray.from_arrays(pa.array([0] * n, pa.int32()), values)


def test_a_sliced_nullable_list_of_encoded_structs_round_trips(place: _Place) -> None:
    """A null row of a sliced list over dictionary-encoded structs is kept (measured
    live with spiral-sdk 0.16.78: such a list is stored as its decoded values)."""
    lists = pa.LargeListArray.from_arrays(
        pa.array([0, 1, 2, 3], pa.int64()),
        _dictionary_of(_struct_one("x"), 3),
        mask=pa.array([False, True, False]),
    )
    got = _written_and_read(
        place, "sliced_encoded_lists", pa.table({"l": lists.slice(1, 2)})
    )
    assert got.column("l").to_pylist() == [None, [{"x": 1}]]


@pytest.mark.parametrize("encoded", [False, True], ids=["plain", "dictionary"])
def test_nul_is_cut_in_names_inside_an_extension(place: _Place, encoded: bool) -> None:
    """An extension column is stored as its storage, its struct names cut at NUL (measured
    live with spiral-sdk 0.16.78 for a plain struct)."""
    storage = _struct_one("x\0y")
    if encoded:
        storage = _dictionary_of(storage, 1)
    column = pa.ExtensionArray.from_storage(_ProbeExtension(storage.type), storage)
    got = _written_and_read(
        place, f"nul_in_extension_{encoded}", pa.table({"e": column})
    )
    assert got.column("e").to_pylist() == [{"x": 1}]


def test_a_reserved_name_inside_an_encoded_struct_is_refused(place: _Place) -> None:
    """Measured live with spiral-sdk 0.16.78: a struct field named ``$a`` is refused, also
    when the struct is dictionary-encoded."""
    client = _client(place)
    key = pa.schema([pa.field("k", pa.large_string(), False)])
    table = client.project(place.project).create_keyed_table(
        place.dataset, "reserved_in_encoded", key, exist_ok=True
    )
    rows = pa.table(
        {
            "k": pa.array(["r0", "r1"], pa.large_string()),
            "s": _dictionary_of(_struct_one("$a"), 2),
        }
    )
    with pytest.raises(Exception, match="reserved"):  # noqa: B017, PT011  # the SDK's error type differs from the fake's
        _ = table.append(rows)


def test_store_lookups_read_by_key_and_by_value(place: _Place) -> None:
    """ITL-1502: lookups narrowed at the scan (key ranges, pushed-down equality) return
    exactly what an exact filter of the whole table returns."""
    from orcapod.databases import ConnectorArrowDatabase

    path = ("conformance_lookups", "0123456789abcdef" * 4, "rdb_v1")
    written = _codec_rows()
    store = SpiralDBConnector(
        place.project, place.dataset, place.overrides, _store=True
    )
    db = ConnectorArrowDatabase(store)
    db.add_records(path, written, record_id_column="__record_id", flush=True)
    with db.run_scope():
        one = db.get_record_by_id(path, b"r1", record_id_column="__record_id")
        assert one is not None and one["__record_id"].to_pylist() == [b"r1"]
        some = db.get_records_by_ids(
            path, [b"r2", b"absent", b"r0"], record_id_column="__record_id"
        )
        assert some is not None
        assert set(some["__record_id"].to_pylist()) == {b"r0", b"r2"}
        for constraints, expected in (
            ({"__data_context": "std:v1:default", "a.b": 2}, {b"r1"}),
            ({"$x": True, "*": b""}, {b"r0"}),
            ({"a.b": None}, set()),
        ):
            got = db.get_records_with_column_value(
                path, constraints, record_id_column="__record_id"
            )
            found = set() if got is None else set(got["__record_id"].to_pylist())
            assert found == expected, constraints
