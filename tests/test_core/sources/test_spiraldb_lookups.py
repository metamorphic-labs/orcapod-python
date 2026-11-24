"""Count SDK calls for sources and jobs using the repository's spiral-sdk fake.

Construction and each freshness check get metadata from one lookup (ITL-1350).
Each read has its own table lookup, including reads that also count rows,
and the source reuses its SDK client (ITL-1431).
"""

from __future__ import annotations

import asyncio
import collections
import gc
import weakref
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.databases import (
    InMemoryArrowDatabase,
    SpiralDBConnector,
)
from orcapod.databases.spiraldb_schema_table import SCHEMA_TABLE
from orcapod.operators import SelectColumns
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.sources import SpiralDBTableSource
from tests.test_databases import fake_spiral_sdk

_PROJECT = "lookups"
# Two key columns (tags) and two data columns, in two arrangements: the key order of the
# table and the column layout of its rows (CodeRabbit on #479, the several-items rule).
ARRANGEMENTS: dict[str, tuple[list[str], list[str]]] = {
    # name: (key columns in key order, column layout)
    "keys_first": (["subject", "k"], ["subject", "k", "x", "y"]),
    "reversed": (["k", "subject"], ["y", "x", "k", "subject"]),
}
# The source's tag roles: every key a plain tag, or every key a data-tag (the default).
ROLES: dict[str, bool | tuple[str, ...]] = {"plain": True, "data_tags": ()}
_FIELDS = {
    "subject": pa.field("subject", pa.large_string(), nullable=False),
    "k": pa.field("k", pa.int64(), nullable=False),
    "x": pa.field("x", pa.int64(), nullable=False),
    "y": pa.field("y", pa.large_string(), nullable=True),
}
# What is counted: "client" built; "lookup" of the user's table by name; "open_schemas" and
# "read_schemas", the opening of the dataset's schema table and the reads of its rows;
# "schema" of the user's table; "scan" of its rows and "count" of its keys.


def _schema(layout: list[str]) -> pa.Schema:
    return pa.schema([_FIELDS[name] for name in layout])


def _keys(keys: list[str]) -> pa.Schema:
    return pa.schema([_FIELDS[name] for name in keys])


def _rows(layout: list[str], ks: list[int]) -> pa.Table:
    values = {
        "subject": [f"s{k % 2}" for k in ks],
        "k": ks,
        "x": [10 * k for k in ks],
        "y": [f"v{k}" for k in ks],
    }
    return pa.table({name: values[name] for name in layout}, schema=_schema(layout))


@pytest.fixture(params=sorted(ARRANGEMENTS))
def calls(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[collections.Counter[str]]:
    """A fake project with table ``default.t`` in one arrangement; counts the SDK
    calls made on it."""
    keys, layout = ARRANGEMENTS[request.param]
    counts: collections.Counter[str] = collections.Counter()

    def is_schemas(name: str) -> bool:
        return name == SCHEMA_TABLE

    def lookup_label(args: tuple[Any, ...], kwargs: Any) -> str:  # pyright: ignore[reportExplicitAny]  # the call's
        return "open_schemas" if is_schemas(args[3]) else "lookup"

    def handle_label(user: str) -> Callable[..., str | None]:
        def label(args: tuple[Any, ...], kwargs: Any) -> str | None:  # pyright: ignore[reportExplicitAny]  # the call's
            if is_schemas(args[0]._table["name"]):
                return "read_schemas" if user == "scan" else None
            columns = args[1] if len(args) > 1 else kwargs.get("columns")
            return "count" if user == "scan" and columns else user

        return label

    counted: dict[tuple[type, str], Callable[..., str | None]] = {
        (fake_spiral_sdk.FakeClient, "__init__"): lambda args, kwargs: "client",
        (fake_spiral_sdk.FakeClient, "keyed_table"): lookup_label,
        (fake_spiral_sdk.FakeKeyedTable, "schema"): handle_label("schema"),
        (fake_spiral_sdk.FakeKeyedTable, "scan"): handle_label("scan"),
    }
    for (cls, name), label_of in counted.items():
        original: Callable[..., Any] = getattr(cls, name)  # pyright: ignore[reportExplicitAny]  # the fake's method

        def wrapper(
            *args: Any,  # pyright: ignore[reportExplicitAny]  # passed through
            _original: Callable[..., Any] = original,  # pyright: ignore[reportExplicitAny]
            _label_of: Callable[..., str | None] = label_of,
            **kwargs: Any,  # pyright: ignore[reportExplicitAny]
        ) -> Any:  # pyright: ignore[reportExplicitAny]
            label = _label_of(args, kwargs)
            if label is not None:
                counts[label] += 1
            return _original(*args, **kwargs)

        monkeypatch.setattr(cls, name, wrapper)
    with fake_spiral_sdk.install(tmp_path / "spiral"):
        project = fake_spiral_sdk.ExternalProject(_PROJECT)
        table = project.create_table("default.t", key_schema=_keys(keys))
        table.write(_rows(layout, [0, 1, 2]))
        table.record(_schema(layout))
        _ARRANGED["keys"], _ARRANGED["layout"] = keys, layout
        counts.clear()
        yield counts


# The arrangement the current ``calls`` fixture built (for the recreation test).
_ARRANGED: dict[str, list[str]] = {}


def _source(role: str, **kwargs: Any) -> SpiralDBTableSource:  # pyright: ignore[reportExplicitAny]  # options
    return SpiralDBTableSource(
        project_id=_PROJECT, table_name="t", tag_only_columns=ROLES[role], **kwargs
    )


def _sync_and_async_rows(source: SpiralDBTableSource) -> tuple[list[Any], list[Any]]:  # pyright: ignore[reportExplicitAny]  # rows
    """Every row through the public sync and async reads, tags and data merged."""

    def merged(tag: Any, data: Any) -> tuple[tuple[str, Any], ...]:  # pyright: ignore[reportExplicitAny]
        return tuple(sorted({**tag.as_dict(), **data.as_dict()}.items()))

    async def collect() -> list[Any]:  # pyright: ignore[reportExplicitAny]
        return [merged(t, d) async for t, d in source.async_iter_data()]

    sync = [merged(t, d) for t, d in source.iter_data()]
    return sync, asyncio.run(collect())


def _expected_rows(ks: list[int]) -> list[Any]:  # pyright: ignore[reportExplicitAny]  # rows
    """The rows of ``_rows(..., ks)``, written out independently, tags and data merged."""
    return sorted(
        tuple(
            sorted({"subject": f"s{k % 2}", "k": k, "x": 10 * k, "y": f"v{k}"}.items())
        )
        for k in ks
    )


@pytest.mark.parametrize("role", sorted(ROLES))
def test_a_source_looks_its_table_up_once_to_build(
    calls: collections.Counter[str], role: str
) -> None:
    source = _source(role)
    # Before ITL-1350: 4 lookups (key, id, schema, version).
    assert calls == {
        "client": 1,
        "lookup": 1,
        "schema": 1,
        "open_schemas": 1,
        "read_schemas": 1,
    }
    # Its reads are those of every source: the key order, layout and role it has.
    tags, data = source.keys()
    assert set(tags) == {"subject", "k"}
    assert {"x", "y"} <= set(data)
    assert (set(tags) <= set(data)) == (role == "data_tags")
    sync, async_ = _sync_and_async_rows(source)
    assert sorted(sync) == sorted(async_) == _expected_rows([0, 1, 2])


@pytest.mark.parametrize("role", sorted(ROLES))
def test_a_source_with_a_given_version_reads_no_version(
    calls: collections.Counter[str], role: str
) -> None:
    _source(role, asof=1)
    # Before ITL-1350: 3 lookups (key, id, schema).
    assert calls == {
        "client": 1,
        "lookup": 1,
        "schema": 1,
        "open_schemas": 1,
        "read_schemas": 1,
    }


@pytest.mark.parametrize("role", sorted(ROLES))
def test_each_check_looks_the_table_up_once(
    calls: collections.Counter[str], role: str
) -> None:
    source = _source(role)
    for _ in range(2):
        calls.clear()
        assert source.check_for_new_data() is False
        # Before ITL-1350: 3 lookups (id, schema, version).
        assert calls == {"lookup": 1, "schema": 1, "open_schemas": 1}
    sync, async_ = _sync_and_async_rows(source)
    assert sorted(sync) == sorted(async_) == _expected_rows([0, 1, 2])


@pytest.mark.parametrize("role", sorted(ROLES))
def test_a_check_still_finds_a_table_created_again(
    calls: collections.Counter[str], role: str
) -> None:
    source = _source(role)
    keys, layout = _ARRANGED["keys"], _ARRANGED["layout"]
    project = fake_spiral_sdk.ExternalProject(_PROJECT)
    project.drop_table("default.t")
    again = project.create_table("default.t", key_schema=_keys(keys))
    again.write(_rows(layout, [0, 1, 2, 3]))
    again.record(_schema(layout))
    calls.clear()
    with pytest.raises(ValueError, match="dropped and created again"):
        source.check_for_new_data()
    # Refused on the id, before the new table's schema or version is read.
    assert calls == {"lookup": 1}


def test_missing_table_matches_source_diagnostic(
    calls: collections.Counter[str],
) -> None:
    with pytest.raises(ValueError) as exc:
        SpiralDBTableSource(project_id=_PROJECT, table_name="missing")
    assert type(exc.value) is ValueError
    assert str(exc.value) == (
        f"SpiralDB project {_PROJECT!r}, dataset 'default': "
        "Table 'missing' not found in database."
    )
    assert calls == {"lookup": 1, "client": 1}


def test_lookup_preserves_unrelated_sdk_errors(
    calls: collections.Counter[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    error = ValueError("Invalid SDK configuration")

    def fail(*_args: object, **_kwargs: object) -> None:
        raise error

    monkeypatch.setattr(fake_spiral_sdk.FakeClient, "keyed_table", fail)
    with pytest.raises(ValueError) as exc:
        _source("plain")
    assert exc.value is error
    assert calls == {"client": 1}


def test_restricted_copy_keeps_client_after_parent_closes(
    calls: collections.Counter[str],
) -> None:
    source = _source("plain")
    restricted = source.where(subject="s0")
    source.close()
    calls.clear()
    assert sorted(cast("list[int]", restricted.as_table()["k"].to_pylist())) == [0, 2]
    assert calls == {"lookup": 1, "open_schemas": 1, "schema": 1, "scan": 1}
    restricted.close()


def test_invalid_where_releases_client_with_exception_retained(
    calls: collections.Counter[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    clients: list[weakref.ReferenceType[fake_spiral_sdk.FakeClient]] = []
    original = fake_spiral_sdk.FakeClient.__init__

    def track(
        sdk: fake_spiral_sdk.FakeClient,
        *,
        api_url: str | None = None,
    ) -> None:
        original(sdk, api_url=api_url)
        clients.append(weakref.ref(sdk))

    monkeypatch.setattr(fake_spiral_sdk.FakeClient, "__init__", track)
    source = _source("plain")
    assert len(clients) == 1 and clients[0]() is not None
    with pytest.raises(ValueError, match="missing_column") as held:
        source.where(missing_column=1)

    source.close()
    gc.collect()
    assert held.value.__traceback__ is not None
    assert clients[0]() is None


@pytest.mark.parametrize("restricted", [False, True])
@pytest.mark.parametrize("async_read", [False, True])
def test_each_read_resolves_once_and_reuses_its_client(
    calls: collections.Counter[str], restricted: bool, async_read: bool
) -> None:
    source = _source("data_tags", where={"subject": "s0"} if restricted else None)
    expected = [0, 2] if restricted else [0, 1, 2]

    async def collect() -> list[int]:
        keys: list[int] = []
        async for tag, _ in source.async_iter_data():
            key = tag.as_dict()["k"]
            assert isinstance(key, int)
            keys.append(key)
        return keys

    for round_ in range(2):
        calls.clear()
        keys = (
            asyncio.run(collect())
            if async_read
            else cast("list[int]", source.as_table()["k"].to_pylist())
        )
        assert sorted(keys) == expected
        assert calls == {"lookup": 1, "open_schemas": 1, "schema": 1, "scan": 1} | (
            {} if restricted or round_ else {"count": 1}
        )

    calls.clear()
    head = source.head(1)
    assert head.table.num_rows == 1
    assert head.total_rows == 3
    # The count was made by the reads above (or is made here, after a restricted read).
    assert calls == {
        "lookup": 1,
        "open_schemas": 1,
        "schema": 1,
        "scan": 1,
    } | ({"count": 1} if restricted else {})


@pytest.mark.parametrize(
    ("dataset", "name"),
    [("default", ""), ("", "t"), ("default", "default.t"), ("default.extra", "t")],
)
def test_source_rejects_malformed_names_before_sdk_lookup(
    calls: collections.Counter[str], dataset: str, name: str
) -> None:
    """Empty or already qualified names keep the source's missing-table diagnostic."""
    with pytest.raises(ValueError) as exc:
        SpiralDBTableSource(
            _PROJECT, name, dataset=dataset, source_id="explicit-source"
        )
    assert type(exc.value) is ValueError
    assert str(exc.value) == (
        f"SpiralDB project {_PROJECT!r}, dataset {dataset!r}: "
        f"Table {name!r} not found in database."
    )
    assert calls["lookup"] == calls["open_schemas"] == calls["scan"] == 0


def _write_scope_table(root_layout: list[str], value: int) -> str:
    """Write the same-shaped table with a recognizable value in this fake endpoint."""
    project = fake_spiral_sdk.ExternalProject(_PROJECT)
    table = project.create_table("default.t", key_schema=_keys(["subject", "k"]))
    rows = _rows(root_layout, [0])
    rows = rows.set_column(
        rows.schema.get_field_index("x"),
        _FIELDS["x"],
        pa.array([value], type=pa.int64()),
    )
    table.write(rows)
    table.record(rows.schema)
    return table.table_id


async def _async_x(source: SpiralDBTableSource) -> list[object]:
    """Return the x values from a public asynchronous source read."""
    return [data.as_dict()["x"] async for _, data in source.async_iter_data()]


@pytest.mark.parametrize("async_read", [False, True])
def test_copy_keeps_its_original_endpoint(tmp_path: Path, async_read: bool) -> None:
    layout = ["subject", "k", "x", "y"]
    with fake_spiral_sdk.install(tmp_path / "endpoint-a"):
        original_id = _write_scope_table(layout, 14)
        source = _source("plain", asof=1)
        restricted = source.where(subject="s0")
        assert restricted.table_id == original_id

    with fake_spiral_sdk.install(tmp_path / "endpoint-b"):
        assert _write_scope_table(layout, 29) != original_id
        if async_read:
            assert asyncio.run(_async_x(restricted)) == [14]
        else:
            assert restricted.as_table()["x"].to_pylist() == [14]


@pytest.mark.parametrize("asof", [None, 1])
@pytest.mark.parametrize("mode", ["table", "iter", "async", "head", "count"])
def test_each_read_refuses_a_replacement_before_scanning(
    calls: collections.Counter[str], asof: int | None, mode: str
) -> None:
    source = _source("plain", asof=asof)
    project = fake_spiral_sdk.ExternalProject(_PROJECT)
    project.drop_table("default.t")
    keys, layout = _ARRANGED["keys"], _ARRANGED["layout"]
    replacement = project.create_table("default.t", key_schema=_keys(keys))
    replacement.write(_rows(layout, [0]))
    replacement.record(_schema(layout))
    reads: dict[str, Callable[[], object]] = {
        "table": source.as_table,
        "iter": lambda: list(source.iter_data()),
        "async": lambda: asyncio.run(_async_x(source)),
        "head": lambda: source.head(1),
        "count": lambda: source.row_count_estimate,
    }
    calls.clear()
    with pytest.raises(ValueError, match="dropped and created again"):
        reads[mode]()
    assert calls == {"lookup": 1}


def test_two_async_job_readers_use_the_restricted_sources_endpoint(
    tmp_path: Path,
) -> None:
    layout = ["subject", "k", "x", "y"]
    with fake_spiral_sdk.install(tmp_path / "endpoint-a"):
        _write_scope_table(layout, 14)
        restricted = _source("plain", asof=1).where(subject="s0")
    with fake_spiral_sdk.install(tmp_path / "endpoint-b"):
        _write_scope_table(layout, 29)
        job = PipelineJob(name="two-readers", database=InMemoryArrowDatabase())
        with job:
            SelectColumns(["subject", "x"])(restricted, label="left")
            SelectColumns(["k", "x"])(restricted, label="right")
        start = len(fake_spiral_sdk.scan_log)
        job.run(orchestrator=AsyncPipelineOrchestrator())
        assert job.nodes["left"].as_table()["x"].to_pylist() == [14]
        assert job.nodes["right"].as_table()["x"].to_pylist() == [14]
        # The async job fans one scan out to both readers of this source.
        assert len(fake_spiral_sdk.scan_log) - start == 1


@pytest.mark.parametrize("orchestrator", [None, AsyncPipelineOrchestrator])
def test_job_reuses_client_but_refreshes_table_each_run(
    calls: collections.Counter[str],
    orchestrator: type[AsyncPipelineOrchestrator] | None,
) -> None:
    source = _source("data_tags")
    job = PipelineJob(name="lookups", database=InMemoryArrowDatabase())
    with job:
        SelectColumns(["subject", "k", "x", "y"])(source, label="read")
    runner = orchestrator() if orchestrator is not None else None

    calls.clear()
    job.run(orchestrator=runner)
    assert job.nodes["read"].as_table().num_rows == 3
    assert calls == {
        "lookup": 2,
        "open_schemas": 2,
        "schema": 2,
        "count": 1,
        "scan": 1,
    }

    calls.clear()
    job.run(orchestrator=runner)
    assert calls == {"lookup": 1, "open_schemas": 1, "schema": 1}


def test_freshness_catches_metadata_schema_changes(
    calls: collections.Counter[str],
) -> None:
    source = _source("plain")
    layout = _ARRANGED["layout"]
    table = fake_spiral_sdk.ExternalProject(_PROJECT).table("default.t")
    changed = pa.schema(
        [
            _FIELDS[name].with_nullable(True) if name == "x" else _FIELDS[name]
            for name in layout
        ]
    )
    table.record(changed)
    calls.clear()
    with pytest.raises(ValueError, match="the table has column .x."):
        source.check_for_new_data()
    assert calls == {"lookup": 1, "open_schemas": 1, "read_schemas": 1, "schema": 1}


def test_freshness_pins_newer_version(calls: collections.Counter[str]) -> None:
    source = _source("plain")
    table = fake_spiral_sdk.ExternalProject(_PROJECT).table("default.t")
    table.write(_rows(_ARRANGED["layout"], [3]))
    calls.clear()
    assert source.check_for_new_data()
    assert calls == {"lookup": 1, "open_schemas": 1, "schema": 1}
    assert sorted(cast("list[int]", source.as_table()["k"].to_pylist())) == [0, 1, 2, 3]


def test_the_connector_getters_answer_as_the_handle_does(
    calls: collections.Counter[str],
) -> None:
    with SpiralDBConnector(project_id=_PROJECT) as connector:
        table = connector.lookup_table("t")
        # The key columns in the table's key order.
        assert connector.get_pk_columns("t") == table.key_columns == _ARRANGED["keys"]
        assert connector.get_spiral_table_id("t") == table.table_id
        assert connector.get_arrow_schema("t").equals(
            table.arrow_schema(), check_metadata=True
        )
        # SpiralDB stores a table's columns in its own order, whatever the order written:
        # the key columns in key order, then the value columns by name (ITL-1367; the
        # fake models it since main 4d192bfc).
        keys = _ARRANGED["keys"]
        values = sorted(name for name in _ARRANGED["layout"] if name not in keys)
        assert table.arrow_schema().names == [*keys, *values]
        assert connector.get_table_version("t") == table.version()


@pytest.mark.parametrize("role", sorted(ROLES))
def test_the_arrangements_give_the_same_rows_and_pipeline_hash(
    tmp_path: Path, role: str
) -> None:
    """Two tables holding the same rows in the two arrangements: the same rows and the
    same pipeline hash (a schema, in no column order); different content hashes, since
    they are two tables (two table ids) and the key order is part of a row's record id."""
    with fake_spiral_sdk.install(tmp_path / "spiral"):
        project = fake_spiral_sdk.ExternalProject(_PROJECT)
        sources = {}
        for name, (keys, layout) in ARRANGEMENTS.items():
            table = project.create_table(f"default.{name}", key_schema=_keys(keys))
            table.write(_rows(layout, [0, 1, 2]))
            table.record(_schema(layout))
            sources[name] = SpiralDBTableSource(
                project_id=_PROJECT, table_name=name, tag_only_columns=ROLES[role]
            )
        first, second = sources.values()
        for source in (first, second):
            sync, async_ = _sync_and_async_rows(source)
            assert sorted(sync) == sorted(async_) == _expected_rows([0, 1, 2])
        assert first.pipeline_hash() == second.pipeline_hash()
        assert first.content_hash() != second.content_hash()
