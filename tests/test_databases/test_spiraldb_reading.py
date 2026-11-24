"""Reading SpiralDB through spiral-sdk, on the fake (ITL-1186).

What the connector and the table source read, what they refuse, and the regressions of the
review findings (B2, B3, B4, B9).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import math
import os
import signal
import sys
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import ConnectorArrowDatabase, SpiralDBConnector
from orcapod.databases import spiraldb_connector as connector_module
from orcapod.errors import (
    NullInNonNullableColumnError,
    SpiralDBWriteNotSupportedError,
    TableSchemaError,
    UnrecordedTableSchemaWarning,
)
from orcapod.nodes import FunctionJobNode
from orcapod.sources import DictSource, SpiralDBTableSource
from orcapod.types import ColumnInfo
from tests.test_databases import fake_spiral_sdk as fake

_ITEM = pa.field("item", pa.float32(), False)
_WRITTEN = pa.schema(
    [
        pa.field("k2", pa.int32(), False),
        pa.field("k1", pa.large_string(), False),
        pa.field("z", pa.float64(), False, {"unit": "mV"}),
        pa.field("a", pa.large_list(_ITEM)),
        pa.field(
            "g",
            pa.struct(
                [
                    pa.field("y", pa.int8(), False, {"m": "1"}),
                    pa.field("x", pa.large_string()),
                ]
            ),
            False,
        ),
    ],
    metadata={"who": "test"},
)


def _rows() -> pa.Table:
    values = [
        {"k2": 2, "k1": "a", "z": 1.0, "a": [1.0], "g": {"y": 1, "x": None}},
        {"k2": 1, "k1": "b", "z": 2.0, "a": [], "g": {"y": 2, "x": "q"}},
        {"k2": 1, "k1": "a", "z": 3.0, "a": None, "g": {"y": 3, "x": "r"}},
    ]
    return pa.Table.from_pylist(values, schema=_WRITTEN)


@pytest.fixture()
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """``units`` and ``swapped``: the same rows under the two key orders."""
    monkeypatch.setattr(connector_module, "_warned_unrecorded", set())
    with fake.install(tmp_path / "spiral"):
        made = fake.ExternalProject("p")
        for name, keys in (("units", ["k2", "k1"]), ("swapped", ["k1", "k2"])):
            made.create_table(
                f"default.{name}", key_schema=[_WRITTEN.field(k) for k in keys]
            ).write(_rows())
        yield made


def _keys(source: SpiralDBTableSource) -> list[tuple[Any, Any]]:
    return [(tag.as_dict()["k2"], tag.as_dict()["k1"]) for tag, _ in source.iter_data()]


def test_a_recorded_table_is_read_with_its_schema(project: Any) -> None:
    fake.migrate("p", {"units": _WRITTEN, "swapped": _WRITTEN})
    connector = SpiralDBConnector("p")
    schema = connector.get_arrow_schema("units")
    # SpiralDB's order: keys in key order, then by name, structs last, their fields too.
    assert schema.names == ["k2", "k1", "a", "z", "g"]
    assert [f.name for f in schema.field("g").type] == ["x", "y"]
    assert schema.metadata == {b"who": b"test"}
    assert schema.field("z").metadata == {b"unit": b"mV"}
    assert {f.name: f.nullable for f in schema} == {
        "k2": False,
        "k1": False,
        "a": True,
        "z": False,
        "g": False,
    }
    inner = schema.field("g").type.field("y")
    assert (inner.nullable, inner.metadata) == (False, {b"m": b"1"})
    assert schema.field("a").type == pa.large_list(_ITEM)
    batches = list(connector.scan_batches("units"))
    assert all(b.schema.equals(schema, check_metadata=True) for b in batches)
    assert connector.get_column_info("units") == [
        ColumnInfo(f.name, f.type, f.nullable) for f in schema
    ]


def test_rows_come_in_key_order_and_values_are_kept(project: Any) -> None:
    fake.migrate("p", {"units": _WRITTEN, "swapped": _WRITTEN})
    assert _keys(SpiralDBTableSource("p", "units")) == [(1, "a"), (1, "b"), (2, "a")]
    assert _keys(SpiralDBTableSource("p", "swapped")) == [(1, "a"), (2, "a"), (1, "b")]
    table = pa.Table.from_batches(SpiralDBConnector("p").scan_batches("units"))
    expected = {(r["k2"], r["k1"]): r for r in _rows().to_pylist()}
    assert {(r["k2"], r["k1"]): r for r in table.to_pylist()} == expected


def test_sync_and_async_read_the_same_rows(project: Any) -> None:
    fake.migrate("p", {"units": _WRITTEN})
    source = SpiralDBTableSource("p", "units")

    async def collect() -> list[Any]:
        return [(t.as_dict(), d.as_dict()) async for t, d in source.async_iter_data()]

    sync = [(t.as_dict(), d.as_dict()) for t, d in source.iter_data()]
    assert sorted(map(repr, asyncio.run(collect()))) == sorted(map(repr, sync))


def test_a_null_in_a_recorded_column_is_refused(project: Any) -> None:
    nulls = _rows().slice(0, 1)
    nulls = nulls.set_column(
        nulls.schema.get_field_index("z"),
        pa.field("z", pa.float64()),
        pa.array([None], pa.float64()),
    )
    project.table("default.units").write(nulls)
    fake.migrate("p", {"units": _WRITTEN})
    with pytest.raises(NullInNonNullableColumnError, match="units.*z|z.*units"):
        list(SpiralDBConnector("p").scan_batches("units"))


# ------------------------------------------------------------ a table that is not recorded


def _unrecorded(name: str = "units") -> str:
    return f"SpiralDB table p/default/{name} has no row"


@pytest.mark.parametrize("complete", [False, True], ids=["no table", "absent row"])
def test_an_unrecorded_table_warns_once_and_reads_nullable(
    project: Any, complete: bool
) -> None:
    if complete:
        fake.migrate("p", {"swapped": _WRITTEN})
        project.create_table("default.late", key_schema=[("k", pa.string())]).write(
            pa.table({"k": ["a"], "v": [1]})
        )
    name = "late" if complete else "units"
    with pytest.warns(UnrecordedTableSchemaWarning, match=_unrecorded(name)) as seen:
        source = SpiralDBTableSource("p", name)
        _ = source.as_table()
        _ = SpiralDBConnector("p").get_arrow_schema(name)
    assert len(seen) == 1
    assert "migrate_spiral_schemas.py" in str(seen[0].message)
    assert seen[0].filename == __file__
    schema = SpiralDBConnector("p").get_arrow_schema(name)
    assert all(f.nullable and not f.metadata for f in schema)


def test_the_warning_as_an_error_is_raised_every_time(project: Any) -> None:
    """Review finding B9."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", UnrecordedTableSchemaWarning)
        for _ in range(2):
            with pytest.raises(UnrecordedTableSchemaWarning):
                SpiralDBTableSource("p", "units")


def test_a_recorded_table_warns_nothing(project: Any) -> None:
    fake.migrate("p", {"units": _WRITTEN})
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        SpiralDBTableSource("p", "units").as_table()
        assert SpiralDBConnector("p").get_table_names() == ["swapped", "units"]


def test_a_recorded_table_that_is_gone_is_warned_about(project: Any) -> None:
    fake.migrate("p", {"units": _WRITTEN})
    project.drop_table("default.swapped")
    with pytest.warns(UnrecordedTableSchemaWarning, match="no longer holds.*swapped"):
        assert SpiralDBConnector("p").get_table_names() == ["units"]


# ------------------------------------------------------------------ a row that disagrees


def test_a_column_added_later_is_refused_naming_it(project: Any) -> None:
    fake.migrate("p", {"units": _WRITTEN})
    project.table("default.units").write(
        pa.table({"k2": pa.array([9], pa.int32()), "k1": ["z"], "new": [1]})
    )
    with pytest.raises(TableSchemaError, match="not recorded: .'new'"):
        SpiralDBTableSource("p", "units")


def test_an_older_version_with_other_columns_is_refused(project: Any) -> None:
    project.table("default.units").write(
        pa.table({"k2": pa.array([9], pa.int32()), "k1": ["z"], "new": [1]})
    )
    fake.migrate("p", {})
    connector = SpiralDBConnector("p")
    assert len(list(connector.scan_batches("units"))) > 0
    with pytest.raises(TableSchemaError, match="not in the table: .'new'"):
        list(connector.scan_batches("units", asof=1))


def test_an_older_version_with_the_same_columns_is_read(project: Any) -> None:
    fake.migrate("p", {"units": _WRITTEN})
    project.table("default.units").write(
        _rows().slice(0, 1).set_column(1, "k1", pa.array(["new"]))
    )
    assert len(_keys(SpiralDBTableSource("p", "units"))) == 4
    assert len(_keys(SpiralDBTableSource("p", "units", asof=1))) == 3


def test_a_recreated_table_is_not_read_with_the_old_row(project: Any) -> None:
    fake.migrate("p", {"units": _WRITTEN})
    project.drop_table("default.units")
    project.create_table("default.units", key_schema=[("k", pa.string())]).write(
        pa.table({"k": ["a"], "v": [1]})
    )
    with pytest.warns(UnrecordedTableSchemaWarning, match="units has no row"):
        schema = SpiralDBConnector("p").get_arrow_schema("units")
    assert schema.names == ["k", "v"]


@pytest.mark.parametrize("failure", ["status 401", "status 403", "transport failed"])
def test_a_schema_table_that_cannot_be_reached_is_not_an_absent_one(
    project: Any, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    original = fake.FakeClient.keyed_table

    def opened(self: Any, project_id: str, dataset: str, name: str, **given: Any):
        if name == "__orcapod_schemas__":
            raise fake.SpiralError(failure)
        return original(self, project_id, dataset, name, **given)

    monkeypatch.setattr(fake.FakeClient, "keyed_table", opened)
    with pytest.raises(fake.SpiralError, match=failure) as raised:
        SpiralDBTableSource("p", "units")
    assert "__orcapod_schemas__" in raised.value.__notes__[0]


def _batch(**columns: Any) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(columns)


def test_a_batch_is_mapped_by_name_and_never_by_position(project: Any) -> None:
    """Review finding B2."""
    found = pa.schema([("k", pa.int64()), ("v", pa.int64())])
    reading = connector_module._Reading(found, None, "p/default/t")  # pyright: ignore[reportPrivateUsage]  # the preparation is the subject
    out = reading.prepare(_batch(v=[9], k=[1]))
    assert out.to_pydict() == {"k": [1], "v": [9]}
    assert reading.prepare(_batch(k=[2], v=[3])).to_pydict() == {"k": [2], "v": [3]}
    for wrong in (
        _batch(k=[1]),
        _batch(k=[1], v=[1], w=[1]),
        _batch(k=[1], x=[1]),
        _batch(k=[1], v=["text"]),
    ):
        with pytest.raises(TableSchemaError, match="a batch's columns"):
            reading.prepare(wrong)


# -------------------------------------------------------------------------------- filters


_TYPED = pa.table(
    {
        "k": pa.array([1, 2, 3], pa.int64()),
        "h": pa.array([0.5, -0.0, float("nan")], pa.float64()).cast(pa.float16()),
        "f": pa.array([0.1, -0.0, float("nan")], pa.float32()),
        "d": pa.array([0.1, -0.0, float("nan")]),
        "n": pa.array([0.0, 1.0, None], pa.float32()),
        "s": pa.array(["x", "é", None], pa.large_string()),
        "b": pa.array([b"x", b"", None], pa.large_binary()),
        "u": pa.array([2**64 - 1, 0, 1], pa.uint64()),
        "i": pa.array([-1, 0, 1], pa.int8()),
        "t": pa.array([True, False, None]),
        "sec": pa.array([1, 2, 3], pa.timestamp("s")),
        "ns": pa.array([1, 1000, 3], pa.timestamp("ns")),
        "d64": pa.array([0, 86400001, None], pa.date64()),
        "d32": pa.array([0, 1, None], pa.date32()),
    }
)


@pytest.mark.parametrize(
    ("column", "value", "expected"),
    [
        # Measured on the service with pyspiral and through this connector: the same,
        # except the two uint64 cases, which pyspiral refused.
        ("h", 0.5, [1]),
        ("h", 0.0, []),
        ("h", -0.0, [2]),
        ("h", math.nan, [3]),
        ("h", 0.1, []),
        ("h", 1e300, []),
        ("f", 0.1, []),
        ("f", 0.0, []),
        ("f", -0.0, [2]),
        ("f", math.nan, [3]),
        ("f", 1e300, []),
        ("f", math.inf, []),
        ("n", 0.0, [1]),
        ("n", -0.0, []),
        ("n", 1.0, [2]),
        ("d", 0.1, [1]),
        ("d", math.nan, [3]),
        ("d", 0.0, []),
        ("d", -0.0, [2]),
        ("s", "x", [1]),
        ("s", "é", [2]),
        ("b", b"x", [1]),
        ("b", b"", [2]),
        ("u", 2**64 - 1, [1]),
        ("u", 0, [2]),
        ("i", -1, [1]),
        ("t", True, [1]),
        ("t", False, [2]),
        ("sec", dt.datetime(1970, 1, 1, 0, 0, 1), [1]),
        ("ns", dt.datetime(1970, 1, 1, 0, 0, 0, 1), [2]),
        ("d64", dt.date(1970, 1, 2), []),
        ("d64", dt.date(1970, 1, 1), [1]),
        ("d32", dt.date(1970, 1, 2), [2]),
    ],
)
def test_where_matches_as_the_service_does(
    project: Any, column: str, value: Any, expected: list[int]
) -> None:
    project.create_table("default.typed", key_schema=[("k", pa.int64())]).write(_TYPED)
    batches = SpiralDBConnector("p").scan_batches("typed", where={column: value})
    assert [k for b in batches for k in b.column("k").to_pylist()] == expected


@pytest.mark.parametrize("limit", [1, 2, 3])
def test_a_filter_applied_here_comes_before_the_limit(project: Any, limit: int) -> None:
    rows = pa.table(
        {
            "k": list(range(8)),
            "h": pa.array([1.0, 2.0] * 4, pa.float64()).cast(pa.float16()),
            "s": ["x", "x", "y", "x"] * 2,
        }
    )
    project.create_table("default.half", key_schema=[("k", pa.int64())]).write(rows)
    for where in ({"h": 2.0, "s": "x"}, {"s": "x", "h": 2.0}):
        batches = SpiralDBConnector("p").scan_batches("half", where=where, limit=limit)
        found = [k for b in batches for k in b.column("k").to_pylist()]
        assert found == [1, 3, 5, 7][:limit]


# --------------------------------------------------------------------------------- counts


def test_a_preview_counts_no_further_than_its_bound(
    project: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review finding B4: also when the preview shows the schema only."""
    fake.migrate("p", {"units": _WRITTEN})
    fake.count_log.clear()
    source = SpiralDBTableSource("p", "units", max_rows=1)
    monkeypatch.setattr(SpiralDBTableSource, "_preview_reads_rows", False)
    head = source.head(1)
    assert [c.limit for c in fake.count_log] == [11]
    assert head.total_rows == 3
    monkeypatch.setattr(SpiralDBTableSource, "_preview_reads_rows", True)
    fake.count_log.clear()
    head = SpiralDBTableSource("p", "units", max_rows=1).head(2)
    assert [c.limit for c in fake.count_log] == [3]
    assert (head.total_rows, "more than 2" in head.header()) == (None, True)


def test_a_count_is_remembered_per_version(project: Any) -> None:
    fake.migrate("p", {"units": _WRITTEN})
    fake.count_log.clear()
    connector = SpiralDBConnector("p")
    assert connector.lookup_table("units").count_rows(at_most=2) == 2
    assert connector.lookup_table("units").count_rows(at_most=1) == 1
    assert connector.lookup_table("units").count_rows() == 3
    assert connector.lookup_table("units").count_rows(at_most=9) == 3
    assert [c.limit for c in fake.count_log] == [2, None]
    project.table("default.units").write(
        _rows().slice(0, 1).set_column(1, "k1", pa.array(["new"]))
    )
    assert connector.get_row_count_estimate("units") == 4
    assert connector.get_row_count_estimate("units", asof=1) == 3


# ------------------------------------------------------------------------------- listing


@pytest.mark.parametrize("count", [0, 1, 3, 4, 10])
def test_the_listing_reads_every_page(tmp_path: Path, count: int) -> None:
    with fake.install(tmp_path / "spiral"):
        made = fake.ExternalProject("p q")
        for i in range(count):
            made.create_table(f"ds.t{i}", key_schema=[("k", pa.string())])
            made.create_table(f"other.t{i}", key_schema=[("k", pa.string())])
        names = SpiralDBConnector("p q", "ds").get_table_names()
        assert names == sorted(f"t{i}" for i in range(count))


def test_a_listing_that_does_not_advance_is_refused(project: Any) -> None:
    # A patch that ends inside the fake's own (monkeypatch would end after it).
    with patch.object(
        connector_module,
        "_http_get_json",
        lambda url, token: {"items": [], "nextPageToken": "same"},
    ):
        with pytest.raises(RuntimeError, match="does not advance"):
            SpiralDBConnector("p").get_table_names()


def test_the_token_is_never_sent_on_a_redirect() -> None:
    import http.server

    seen: list[str | None] = []

    class Redirecting(http.server.BaseHTTPRequestHandler):
        """Redirects every request to itself, under another path."""

        def do_GET(self) -> None:
            """Redirect."""
            seen.append(self.headers.get("Authorization"))
            self.send_response(302)
            self.send_header("Location", "/elsewhere")
            self.end_headers()

        def log_message(self, format: str, *args: Any) -> None:
            """Print nothing."""

    server = http.server.HTTPServer(("127.0.0.1", 0), Redirecting)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        with pytest.raises(OSError, match="302"):
            connector_module._http_get_json(  # pyright: ignore[reportPrivateUsage]  # the request is the subject
                f"http://127.0.0.1:{server.server_port}/", "secret"
            )
    finally:
        server.shutdown()
        server.server_close()
    assert seen == ["Bearer secret"]


# -------------------------------------------------------------------------------- writes


def test_every_write_is_refused_and_writes_nothing(project: Any) -> None:
    connector = SpiralDBConnector("p")
    columns = [ColumnInfo("k", pa.large_string(), False)]
    for write in (
        lambda: connector.create_table_if_not_exists("new", columns, "k"),
        lambda: connector.upsert_records("units", _rows(), "k1"),
        lambda: connector.upsert_records("units", _rows(), "k1", skip_existing=True),
        lambda: connector.delete_table("units"),
        lambda: connector.validate_records(_rows()),
        lambda: connector.unstorable_column_reason("v", pa.int64(), primary_key=False),
    ):
        with pytest.raises(SpiralDBWriteNotSupportedError, match="ITL-1186"):
            write()
    connector.delete_table("absent")  # nothing to delete
    assert connector.get_table_names() == ["swapped", "units"]
    assert connector.get_row_count_estimate("units") == 3


@pytest.mark.parametrize("flush", [False, True])
def test_a_database_refuses_rows_before_it_holds_them(
    project: Any, flush: bool
) -> None:
    """Review finding B3."""
    database = ConnectorArrowDatabase(SpiralDBConnector("p"))
    rows = pa.table({"id": pa.array([b"1"], pa.large_binary()), "v": [1]})
    with pytest.raises(SpiralDBWriteNotSupportedError):
        database.add_records(("t",), rows, record_id_column="id", flush=flush)
    assert database.get_all_records(("t",)) is None
    database.flush()
    assert database.list_table_paths() == [("swapped",), ("units",)]


def test_a_job_storing_there_is_refused_before_anything_runs(project: Any) -> None:
    calls: list[int] = []

    @function_pod(output_keys="w")
    def double(v: int) -> int:
        calls.append(v)
        return 2 * v

    source = DictSource([{"k": 1, "v": 1}], tag_columns=["k"], source_id="s")
    job = PipelineJob(name="j", database=ConnectorArrowDatabase(SpiralDBConnector("p")))
    with pytest.raises(SpiralDBWriteNotSupportedError):
        with job:
            double.pod(source, label="double")
        job.run()
    assert calls == []


def test_a_job_reads_a_source_and_stores_elsewhere(
    project: Any, tmp_path: Path
) -> None:
    from orcapod.databases import DeltaTableDatabase

    fake.migrate("p", {"units": _WRITTEN})

    @function_pod(output_keys="w")
    def double(z: float) -> float:
        return 2 * z

    job = PipelineJob(name="j", database=DeltaTableDatabase(tmp_path / "store"))
    with job:
        double.pod(SpiralDBTableSource("p", "units"), label="double")
    job.run()
    node = job.nodes["double"]
    assert isinstance(node, FunctionJobNode)
    assert sorted(cast("list[float]", node.as_table()["w"].to_pylist())) == [
        2.0,
        4.0,
        6.0,
    ]
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(tmp_path / "job.json")
    (source,) = [
        n.bound_source for n in loaded.nodes.values() if hasattr(n, "bound_source")
    ]
    assert isinstance(source, SpiralDBTableSource)
    assert source.source_id == SpiralDBTableSource("p", "units").source_id


# -------------------------------------------------------------------- client and lifetime


def test_the_one_override_is_the_address(project: Any) -> None:
    overrides = {"server.url": "https://api.example.invalid"}
    connector = SpiralDBConnector("p", overrides=overrides)
    assert connector._client.api_url == "https://api.example.invalid"  # pyright: ignore[reportPrivateUsage]  # what the client was given
    assert SpiralDBConnector.from_config(connector.to_config()).to_config() == {
        "connector_type": "spiraldb",
        "project_id": "p",
        "dataset": "default",
        "overrides": overrides,
    }
    with pytest.raises(ValueError, match="authn.token"):
        SpiralDBConnector("p", overrides={"authn.token": "x", **overrides})


def test_a_closed_connector_reads_nothing(project: Any) -> None:
    with SpiralDBConnector("p") as connector:
        shared = connector.share_client()
        handle = connector.lookup_table("units")
    connector.close()
    for use in (
        connector.get_table_names,
        lambda: connector.lookup_table("units"),
        lambda: list(connector.scan_batches("units")),
        handle.version,
        handle.arrow_schema,
        connector.to_config,
        connector.share_client,
    ):
        with pytest.raises(RuntimeError, match="closed"):
            use()
    assert shared.get_table_names() == ["swapped", "units"]


def test_an_abandoned_read_closes_its_reader(project: Any) -> None:
    fake.migrate("p", {"units": _WRITTEN})
    source = SpiralDBTableSource("p", "units")
    fake.scan_log.clear()
    rows: Any = source.iter_data()
    next(rows)
    rows.close()
    assert [scan.closed for scan in fake.scan_log] == [True]


def test_a_cancelled_async_read_closes_its_reader(
    project: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake.migrate("p", {"units": _WRITTEN})
    source = SpiralDBTableSource("p", "units")
    started, release = threading.Event(), threading.Event()
    original = fake.FakeReader.read_next_batch
    threads: set[int] = set()

    def slow(self: Any) -> pa.RecordBatch:
        threads.add(threading.get_ident())
        if self._record.identifier.endswith(".units"):
            started.set()
            release.wait(10)
        return original(self)

    async def read() -> None:
        async for _ in source.async_iter_data():
            pass

    async def cancel() -> None:
        # Before the read that blocks: what it looks up runs on the same thread.
        task = asyncio.create_task(read())
        while not started.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    _ = source.row_count_estimate  # counted before, so only the read is slowed
    fake.scan_log.clear()
    monkeypatch.setattr(fake.FakeReader, "read_next_batch", slow)
    asyncio.run(cancel())
    assert not any(scan.closed for scan in fake.scan_log)  # still being read
    release.set()
    deadline = threading.Event()
    for _ in range(200):
        if all(scan.closed for scan in fake.scan_log):
            break
        deadline.wait(0.01)
    assert [scan.closed for scan in fake.scan_log] == [True]
    assert len(threads) == 1  # one thread owned the reader
    assert threading.get_ident() not in threads


def test_many_threads_read_through_one_connector(project: Any) -> None:
    fake.migrate("p", {"units": _WRITTEN, "swapped": _WRITTEN})
    connector = SpiralDBConnector("p")

    def read(i: int) -> tuple[list[str], int]:
        name = ("units", "swapped")[i % 2]
        table = pa.Table.from_batches(connector.scan_batches(name))
        return connector.get_arrow_schema(name).names[:2], table.num_rows

    with ThreadPoolExecutor(8) as pool:
        results = list(pool.map(read, range(64)))
    assert results == [(["k2", "k1"], 3), (["k1", "k2"], 3)] * 32


@pytest.mark.skipif(sys.platform == "win32", reason="os.fork is POSIX")
def test_a_forked_child_is_refused_instead_of_hanging(
    project: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    connector = SpiralDBConnector("p")
    monkeypatch.setattr(os, "getpid", lambda: -1)
    with pytest.raises(RuntimeError, match="after a fork"):
        connector.lookup_table("units")


_AFTER_FORK = {
    "read": lambda source: source.as_table(),
    "head": lambda source: source.head(),
    "check": lambda source: source.check_for_new_data(),
    "where": lambda source: source.where(k1="a"),
    "close": lambda source: source.close(),
}


@pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork is POSIX")
@pytest.mark.parametrize("held", [False, True], ids=["lock free", "lock held"])
@pytest.mark.parametrize("use", sorted(_AFTER_FORK))
def test_a_source_in_a_forked_child_is_refused_and_never_waits(
    project: Any, use: str, held: bool
) -> None:
    """Review finding B7: the process is checked before the inherited lock, which a
    thread that does not exist in the child may hold for ever."""
    fake.migrate("p", {"units": _WRITTEN})
    source = SpiralDBTableSource("p", "units")
    lock = source._connection_lock  # pyright: ignore[reportPrivateUsage]  # the inherited lock is the subject
    taken, release = threading.Event(), threading.Event()

    def hold() -> None:
        with lock:
            taken.set()
            release.wait(30)

    holder = threading.Thread(target=hold)
    if held:
        holder.start()
        assert taken.wait(10)
    try:
        with warnings.catch_warnings():
            # Python warns of a fork with threads: that is what is tested.
            warnings.simplefilter("ignore")
            pid = os.fork()
        if pid == 0:  # the child: never returns to pytest
            code = 3
            try:
                _AFTER_FORK[use](source)
                code = 1
            except RuntimeError as exc:
                code = 0 if "after a fork" in str(exc) else 2
            finally:
                os._exit(code)
        status: int | None = None
        for _ in range(1000):
            done, found = os.waitpid(pid, os.WNOHANG)
            if done:
                status = os.waitstatus_to_exitcode(found)
                break
            time.sleep(0.01)
        if status is None:
            os.kill(pid, signal.SIGKILL)
            _ = os.waitpid(pid, 0)
        assert status == 0, "the child waited for the lock" if status is None else ""
    finally:
        release.set()
        if held:
            holder.join(10)
    # The parent is not affected.
    assert source.as_table().num_rows == 3


def test_a_missing_table_is_told_apart_from_other_failures(
    project: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    connector = SpiralDBConnector("p")
    with pytest.raises(connector_module.SpiralDBTableNotFoundError):
        connector.lookup_table("absent")

    def denied(*_given: Any, **_options: Any) -> None:
        raise fake.SpiralError("remote service returned an unknown error (status 404)")

    monkeypatch.setattr(fake.FakeClient, "keyed_table", denied)
    with pytest.raises(fake.SpiralError, match="404") as raised:
        connector.lookup_table("absent")
    assert raised.value.__notes__ == ["SpiralDB: could not open table p/default/absent"]


@pytest.mark.parametrize(
    "query", ['SELECT * FROM "units"', "select * from units", 'SELECT * FROM  "units"']
)
def test_a_query_names_its_table(project: Any, query: str) -> None:
    batches = list(SpiralDBConnector("p").iter_batches(query))
    assert sum(b.num_rows for b in batches) == 3


def test_a_query_without_a_table_is_refused(project: Any) -> None:
    with pytest.raises(ValueError, match="Cannot parse"):
        list(SpiralDBConnector("p").iter_batches("SELECT 1"))
