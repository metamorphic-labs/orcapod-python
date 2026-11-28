"""Source boundaries validate Arrow Null and explicitly nominated system tags."""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Collection
from pathlib import Path
from typing import Any, TypedDict
from uuid import UUID

import deltalake
import polars as pl
import pyarrow as pa
import pyarrow.csv as pa_csv
import pytest

from orcapod import PipelineJob
from orcapod.contexts.stored import with_data_context
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.row_ids import entry_id, instance_id, record_id
from orcapod.core.sources import (
    ArrowTableSource,
    CachedSource,
    CSVSource,
    DBTableSource,
    DataFrameSource,
    DeltaTableSource,
    DictSource,
    ListSource,
    PollingSource,
    PostgreSQLTableSource,
    SpiralDBTableSource,
    SQLiteTableSource,
)
from orcapod.core.sources.source_proxy import SourceProxy
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.operators import DropNulls
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.types import CacheMode, ColumnInfo, Cursor
from tests.test_core.sources.test_db_table_source import MockDBConnector
from tests.test_databases import fake_spiral_sdk

TAGS = ("g", "k")
SOURCE_KINDS = (
    "arrow",
    "dict",
    "dataframe",
    "csv",
    "delta",
    "db",
    "postgres",
    "spiral",
    "sqlite",
)


class _SourceOptions(TypedDict):
    """Keyword arguments supported by the sources in the validation matrix."""

    tag_columns: Collection[str]
    tag_only_columns: bool
    source_id: str
    system_tag_columns: Collection[str]


class _Connector(MockDBConnector):
    """Mock connector exposing the declared flags of an Arrow table."""

    def __init__(self, table: pa.Table) -> None:
        super().__init__({"rows": table}, {"rows": ["k"]})
        self.table = table

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        """Return the database's field declarations."""
        return [
            ColumnInfo(info.name, info.arrow_type, field.nullable)
            for info, field in zip(
                super().get_column_info(table_name),
                self.table.schema,
                strict=True,
            )
        ]

    def current_location(self) -> tuple[str, int, str, str]:
        """Return the connected server's canonical location."""
        return "localhost", 5432, "test", "public"

    def given_server(self) -> tuple[str, str]:
        """Return the server specified in the connection string."""
        return "localhost", ""


def _source(
    kind: str,
    table: pa.Table,
    path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    system_tag_columns: Collection[str] = (),
) -> StreamProtocol:
    """Build the named source over the same columns using its regular reader."""
    options: _SourceOptions = {
        "tag_columns": TAGS,
        "tag_only_columns": True,
        "source_id": "rows",
        "system_tag_columns": system_tag_columns,
    }
    if kind == "arrow":
        return ArrowTableSource(table, **options)
    if kind == "dict":
        return DictSource(table.to_pylist(), data_schema=table.schema, **options)
    if kind == "dataframe":
        return DataFrameSource(pl.from_arrow(table), schema=table.schema, **options)
    if kind == "csv":
        file = path / "rows.csv"
        pa_csv.write_csv(table, file)
        return CSVSource(str(file), schema=table.schema, **options)
    if kind == "delta":
        deltalake.write_deltalake(path / "delta", table)
        return DeltaTableSource(path / "delta", **options)
    if kind == "sqlite":
        if any(field.metadata for field in table.schema):
            # SQLite cannot store Arrow field metadata. Supply it at the reader boundary.
            monkeypatch.setattr(
                "orcapod.core.sources.sqlite_table_source.SQLiteConnector",
                lambda *_: _Connector(table),
            )
            return SQLiteTableSource(path / "rows.db", "rows", **options)
        file = path / "rows.db"
        types = {pa.string(): "TEXT", pa.int64(): "INTEGER"}
        cols = ", ".join(
            f"{f.name} {types[f.type]}" + ("" if f.nullable else " NOT NULL")
            for f in table.schema
        )
        with sqlite3.connect(file) as conn:
            conn.execute(f"CREATE TABLE rows ({cols}, PRIMARY KEY (k))")
            placeholders = ", ".join("?" for _ in table.schema)
            conn.executemany(
                f"INSERT INTO rows VALUES ({placeholders})",
                [tuple(row.values()) for row in table.to_pylist()],
            )
        return SQLiteTableSource(file, "rows", **options)
    if kind == "spiral":
        with fake_spiral_sdk.install(path / "spiral"):
            project = fake_spiral_sdk.ExternalProject("project")
            project.create_table(
                "default.rows", key_schema=pa.schema([table.schema.field("k")])
            ).write(table)
            project.table("default.rows").record(table.schema)
            return SpiralDBTableSource("project", "rows", **options)
    connector = _Connector(table)
    if kind == "postgres":
        monkeypatch.setattr(
            "orcapod.core.sources.postgresql_table_source.PostgreSQLConnector",
            lambda *args, **kwargs: connector,
        )
        return PostgreSQLTableSource("postgresql://localhost/test", "rows", **options)
    assert kind == "db"
    return DBTableSource(connector, "rows", **options)


def _table(field: pa.Field[Any], values: list[Any]) -> pa.Table:
    """Two tags, two ordinary data columns and two additional columns."""
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("g", pa.string(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
            pa.field("y", pa.int64(), nullable=False),
            pa.field("rev", pa.string(), nullable=False),
            field,
        ]
    )
    return pa.Table.from_pylist(
        [
            dict(k=i, g="G", x=2, y=3, rev="r", **{field.name: value})
            for i, value in enumerate(values)
        ],
        schema=schema,
    )


@pytest.mark.parametrize("kind", SOURCE_KINDS)
@pytest.mark.parametrize(
    ("arrow_type", "nullable", "values"),
    [
        (pa.int64(), False, [7, 8]),
        (pa.string(), True, ["a", "b"]),
        (pa.string(), True, ["a", None]),
    ],
)
def test_rejects_invalid_custom_system_tag_columns(
    kind: str,
    arrow_type: pa.DataType,
    nullable: bool,
    values: list[Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = _table(pa.field("extra", arrow_type, nullable=nullable), values)
    with pytest.raises(InputValidationError, match=r"'extra'.*non-null.*text"):
        _source(kind, table, tmp_path, monkeypatch, system_tag_columns=["rev", "extra"])


@pytest.mark.parametrize("kind", SOURCE_KINDS)
@pytest.mark.parametrize(
    "names",
    [("missing",), ("extra", "missing", "rev"), ("rev", "missing", "extra")],
)
def test_refuses_missing_custom_system_tag_columns(
    kind: str,
    names: tuple[str, ...],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = _table(pa.field("extra", pa.string(), nullable=False), ["a", "b"])
    with pytest.raises(ValueError, match=r"system_tag_columns not found.*'missing'"):
        _source(kind, table, tmp_path, monkeypatch, system_tag_columns=names)


@pytest.mark.parametrize("kind", SOURCE_KINDS)
@pytest.mark.parametrize(
    "metadata",
    [
        {b"owner": b"research"},
        {b"ARROW:extension:name": b"user.opaque", b"ARROW:extension:metadata": b""},
    ],
    ids=["ordinary", "extension"],
)
@pytest.mark.parametrize("names", [("extra",), ("extra", "rev"), ("rev", "extra")])
def test_refuses_field_metadata_on_custom_system_tags(
    kind: str,
    metadata: dict[bytes, bytes],
    names: tuple[str, ...],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = _table(
        pa.field("extra", pa.string(), nullable=False, metadata=metadata), ["a", "b"]
    )
    with pytest.raises(
        InputValidationError, match=r"'extra'.*[Rr]emove.*field metadata"
    ):
        _source(kind, table, tmp_path, monkeypatch, system_tag_columns=names)


def test_metadata_free_custom_system_tag_keeps_its_existing_row_ids() -> None:
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), False),
            pa.field("g", pa.large_string(), False),
            pa.field("x", pa.int64(), False),
            pa.field("y", pa.large_string(), False),
            pa.field("custom", pa.large_string(), False),
        ]
    )
    raw = pa.Table.from_pydict(
        {
            "k": [1, 2],
            "g": ["a", "b"],
            "x": [3, 4],
            "y": ["v", "w"],
            "custom": ["p", "q"],
        },
        schema=schema,
    )
    source = ArrowTableSource(
        raw,
        tag_columns=TAGS,
        tag_only_columns=True,
        system_tag_columns=["custom"],
        source_id="meta",
    )
    hasher = source.data_context.arrow_hasher
    actual = []
    for tag, data in source.iter_data():
        entry = entry_id(hasher, tag, data)
        instance = instance_id(hasher, tag, data)
        record = record_id(hasher, source.content_hash(), instance, UUID(int=40))
        actual.append((entry.to_hex(), instance.to_hex(), record.to_hex()))
    # The metadata-free source's hashes at 969ed093.
    assert actual == [
        (
            "00000266ff2e58dd5c1cb957ac9ff249ae13e17918a263847f8e2f65d756f746b3f561",
            "000002e9be964a36d499b5a1efe697c53d9b7e2e0103b79be7b0e2d489c1be8f7b8636",
            "00000279f295215ecb6c92fc49cf4ec41df41327a43c3b351f0baac467b4f45a0f33d2",
        ),
        (
            "0000029689a4a738cf93bbcfde75085dc43bac31570b0b368eff34b0f3d0fa427769c1",
            "000002b6b64a14a2bb8cc662759a15839f8e4e730c89a03230bb8cac2f951fbe8f2002",
            "000002fb3fc3affffe0e390dcfb5695db61b332eb2d38f83c2cfab5570c8a956371e18",
        ),
    ]


@pytest.mark.parametrize("kind", SOURCE_KINDS)
def test_accepts_two_non_nullable_string_system_tags(
    kind: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = _table(pa.field("extra", pa.string(), nullable=False), ["a", "b"])
    source = _source(
        kind, table, tmp_path, monkeypatch, system_tag_columns=["extra", "rev"]
    )
    tags, _ = source.output_schema(columns={"system_tags": True})
    assert {name: tags[name] for name in ("_tag_extra", "_tag_rev")} == {
        "_tag_extra": str,
        "_tag_rev": str,
    }


@pytest.mark.parametrize("kind", [kind for kind in SOURCE_KINDS if kind != "sqlite"])
def test_refuses_arrow_null_from_readers(
    kind: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    column = "unused" if kind == "spiral" else "__unused"
    table = _table(pa.field(column, pa.null()), [None, None])
    with pytest.raises(
        InputValidationError, match=rf"{column!r}.*declare a supported nullable type"
    ):
        _source(kind, table, tmp_path, monkeypatch)


def test_dataframe_schema_can_type_null_values() -> None:
    table = _table(pa.field("extra", pa.null()), [None, None])
    schema = pa.schema(
        [f.with_type(pa.string()) if f.name == "extra" else f for f in table.schema]
    )
    source = DataFrameSource(
        pl.from_arrow(table), schema=schema, tag_columns=TAGS, tag_only_columns=True
    )
    assert source.as_table().schema.field("extra") == pa.field(
        "extra", pa.large_string()
    )


@pytest.mark.parametrize(
    "kind", ["arrow", "dict", "dataframe", "csv", "db", "postgres"]
)
def test_rejects_null_value_even_under_a_non_nullable_system_tag_field(
    kind: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = _table(pa.field("extra", pa.string(), nullable=False), ["a", None])
    with pytest.raises(InputValidationError, match=r"'extra'.*contains null values"):
        _source(kind, table, tmp_path, monkeypatch, system_tag_columns=["extra", "rev"])


def test_sqlite_sql_null_is_a_value_in_a_typed_column(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = _table(pa.field("extra", pa.string()), [None, None])
    source = _source("sqlite", table, tmp_path, monkeypatch)
    assert source.as_table().schema.field("extra") == pa.field(
        "extra", pa.large_string()
    )
    assert source.as_table().column("extra").to_pylist() == [None, None]


def test_list_source_declared_any_does_not_turn_into_a_string() -> None:
    with pytest.raises(
        InputValidationError, match=r"'x'.*declare a supported nullable type"
    ):
        ListSource(
            "x", [None, None], data_schema={"element_index": int, "x": Any | None}
        )


def test_no_declared_type_for_python_none_still_infers_a_string() -> None:
    sources = [
        ListSource("x", [None, None]),
        DictSource([{"x": None, "y": 1}, {"x": None, "y": 2}]),
    ]
    for source in sources:
        assert source.as_table().schema.field("x") == pa.field("x", pa.large_string())


class _Poll:
    """Minimal impl for testing schema validation before the first fetch."""

    def __init__(self, schema: pa.Schema | None, batch: pl.DataFrame) -> None:
        self._schema = schema
        self._batch = batch

    def schema(self) -> pa.Schema | None:
        """Return the optional Arrow schema."""
        return self._schema

    def identity(self) -> str:
        """Return a stable identity for this poll."""
        return "test poll"

    def to_config(self) -> None:
        """This implementation holds a test batch in memory."""

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> _Poll:
        """Reject loading this in-memory implementation."""
        raise NotImplementedError

    async def fetch(
        self, cursor: Cursor[int] | None = None
    ) -> tuple[Cursor[int], pl.DataFrame]:
        """Return the prepared batch on the initial fetch."""
        return Cursor(value=0), self._batch

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        """There are no rows after the initial fetch."""
        return False

    async def close(self) -> None:
        """There is no connection to close."""


@pytest.mark.parametrize("values", [[], [[None], []]])
def test_polling_refuses_null_in_declaration_and_batch(values: list[Any]) -> None:
    table = _table(pa.field("__unused", pa.large_list(pa.null())), values)
    batch = pl.from_arrow(table)
    assert isinstance(batch, pl.DataFrame)
    with pytest.raises(
        InputValidationError, match=r"'__unused'.*declare a supported nullable type"
    ):
        PollingSource(
            _Poll(table.schema, batch), TAGS, source_id="poll", record_id_columns="k"
        )
    poll = PollingSource(
        _Poll(None, batch), TAGS, source_id="poll", record_id_columns="k"
    )
    with pytest.raises(
        InputValidationError, match=r"'__unused'.*declare a supported nullable type"
    ):
        poll.check_for_new_data()


def test_a_raw_arrow_table_stream_preserves_nested_null() -> None:
    field = pa.field("extra", pa.large_list(pa.null()))
    table = _table(field, [[None], []])
    assert (
        ArrowTableStream(table, tag_columns=TAGS).as_table().schema.field("extra")
        == field
    )


def test_cached_source_refuses_an_existing_stored_null_column() -> None:
    table = _table(pa.field("extra", pa.string(), nullable=False), ["a", "b"])
    source = ArrowTableSource(
        table, tag_columns=TAGS, tag_only_columns=True, source_id="rows"
    )
    tag_schema, data_schema = source.output_schema()
    proxy = SourceProxy(
        source_id=source.source_id,
        content_hash_str=source.content_hash().to_string(),
        pipeline_hash_str=source.pipeline_hash().to_string(),
        tag_schema=tag_schema,
        data_schema=data_schema,
    )
    database = InMemoryArrowDatabase()
    cached = CachedSource(proxy, cache_database=database)
    raw = source.as_table(columns={"source": True, "system_tags": True})
    raw = raw.append_column("__unused", pa.nulls(2))
    raw = raw.append_column(CachedSource.HASH_COLUMN_NAME, pa.array([b"a", b"b"]))
    database.add_records(
        cached.cache_path,
        with_data_context(raw, source.data_context_key),
        record_id_column=CachedSource.HASH_COLUMN_NAME,
        flush=True,
    )
    with pytest.raises(
        InputValidationError, match=r"'__unused'.*declare a supported nullable type"
    ):
        cached.as_table()


@pytest.mark.parametrize("route", ["table", "rows", "async_rows", "stored"])
def test_reopened_cache_refuses_legacy_system_tag_metadata(
    route: str,
    tmp_path: Path,
) -> None:
    source = ArrowTableSource(
        _table(pa.field("extra", pa.string(), False), ["a", "b"]),
        tag_columns=TAGS,
        tag_only_columns=True,
        system_tag_columns=["extra"],
        source_id="rows",
    )
    cache = source.cached(DeltaTableDatabase(tmp_path))
    columns = {"source": True, "system_tags": True}
    records = source.as_table(columns=columns)
    name = "_tag_extra"
    pos = records.schema.get_field_index(name)
    field = records.schema.field(pos).with_metadata(
        {b"ARROW:extension:name": b"user.opaque"}
    )
    records = records.set_column(pos, field, records.column(pos))
    records = records.append_column(
        CachedSource.HASH_COLUMN_NAME, pa.array([b"a", b"b"])
    )
    DeltaTableDatabase(tmp_path).add_records(
        cache.cache_path,
        with_data_context(records, source.data_context_key),
        record_id_column=CachedSource.HASH_COLUMN_NAME,
        flush=True,
    )
    reopened = CachedSource.from_config(cache.to_config())

    async def consume() -> None:
        _ = [row async for row in reopened.async_iter_data()]

    with pytest.raises(
        InputValidationError,
        match=r"'_tag_extra'.*cache.*written.*field metadata.*[Rr]ebuild",
    ):
        if route == "table":
            reopened.as_table(columns=columns)
        elif route == "rows":
            list(reopened.iter_data())
        elif route == "async_rows":
            asyncio.run(consume())
        else:
            reopened.get_all_records()


@pytest.mark.parametrize("as_batch", [False, True])
@pytest.mark.parametrize("declare_schema", [False, True])
def test_dataframe_source_validates_nominated_arrow_fields_before_polars(
    as_batch: bool,
    declare_schema: bool,
) -> None:
    table = _table(
        pa.field("extra", pa.string(), False, metadata={b"owner": b"research"}),
        ["a", "b"],
    )
    data = table.to_batches()[0] if as_batch else table
    schema = (
        table.schema.set(
            table.num_columns - 1, table.schema.field("extra").remove_metadata()
        )
        if declare_schema
        else None
    )
    with pytest.raises(
        InputValidationError, match=r"'extra'.*[Rr]emove.*field metadata"
    ):
        DataFrameSource(
            data,
            schema=schema,
            tag_columns=TAGS,
            system_tag_columns=["rev", "extra"],
        )


@pytest.mark.parametrize("route", ["table", "rows", "async_rows", "head"])
def test_bound_proxy_refuses_delegated_system_tag_metadata(
    route: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = ArrowTableSource(
        _table(pa.field("extra", pa.string(), False), ["a", "b"]),
        tag_columns=TAGS,
        tag_only_columns=True,
        system_tag_columns=["extra"],
        source_id="rows",
    )
    tags, data = source.output_schema()
    proxy = SourceProxy(
        source.source_id,
        source.content_hash().to_string(),
        source.pipeline_hash().to_string(),
        tags,
        data,
    )
    proxy.bind(source)
    columns = {"system_tags": True}
    table = source.as_table(columns=columns)
    pos = table.schema.get_field_index("_tag_extra")
    field = table.schema.field(pos).with_metadata({b"owner": b"research"})
    raw = ArrowTableStream(
        table.set_column(pos, field, table.column(pos)),
        tag_columns=TAGS,
    )
    monkeypatch.setattr(source, "as_table", raw.as_table)
    monkeypatch.setattr(source, "iter_data", raw.iter_data)
    monkeypatch.setattr(source, "head", raw.head)

    async def consume() -> None:
        _ = [row async for row in proxy.async_iter_data()]

    with pytest.raises(
        InputValidationError, match=r"'_tag_extra'.*[Rr]emove.*field metadata"
    ):
        if route == "table":
            proxy.as_table(columns=columns)
        elif route == "rows":
            list(proxy.iter_data())
        elif route == "async_rows":
            asyncio.run(consume())
        else:
            proxy.head(columns=columns)


@pytest.mark.parametrize("stored_flag", [False, True])
def test_cached_source_reads_older_token_field_flags_as_stored(
    stored_flag: bool,
) -> None:
    """A cache read uses the stored schema when its original source is unavailable."""
    source = ArrowTableSource(
        _table(pa.field("extra", pa.string(), False), ["a", "b"]),
        tag_columns=TAGS,
        tag_only_columns=True,
        source_id="rows",
    )
    tags, data = source.output_schema()
    proxy = SourceProxy(
        source_id=source.source_id,
        content_hash_str=source.content_hash().to_string(),
        pipeline_hash_str=source.pipeline_hash().to_string(),
        tag_schema=tags,
        data_schema=data,
    )
    database = InMemoryArrowDatabase()
    cache = CachedSource(proxy, cache_database=database)
    columns = {"source": True, "system_tags": True}
    table = source.as_table(columns=columns)
    token = "_source_extra"
    position = table.schema.get_field_index(token)
    table = table.set_column(
        position,
        table.schema.field(position).with_nullable(stored_flag),
        table.column(position),
    )
    table = table.append_column(CachedSource.HASH_COLUMN_NAME, pa.array([b"a", b"b"]))
    database.add_records(
        cache.cache_path,
        with_data_context(table, source.data_context_key),
        record_id_column=CachedSource.HASH_COLUMN_NAME,
        flush=True,
    )
    assert cache.as_table(columns=columns).schema.field(token).nullable is stored_flag
    assert all(
        data.source_info_table().schema.field(token).nullable
        for _, data in cache.iter_data()
    )


@pytest.mark.parametrize("row_count", [0, 2])
def test_derived_source_refuses_stored_null_in_its_direct_table_view(
    row_count: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = _table(pa.field("extra", pa.string(), nullable=False), ["a", "b"])
    source = ArrowTableSource(
        table, tag_columns=TAGS, tag_only_columns=True, source_id="rows"
    )
    job = PipelineJob(name="derived", database=InMemoryArrowDatabase())
    with job:
        DropNulls(["x", "y"])(source, label="origin")
    node = job.nodes["origin"]
    stored = (
        source.as_table()
        .slice(0, row_count)
        .append_column("__unused", pa.nulls(row_count))
    )
    monkeypatch.setattr(node, "get_all_records", lambda *args, **kwargs: stored)
    derived = node.as_source()
    with pytest.raises(
        InputValidationError, match=r"'__unused'.*declare a supported nullable type"
    ):
        derived.as_table()


@pytest.mark.parametrize("copy_tags", [False, True])
def test_derived_source_checks_reserved_columns_before_filtering_storage(
    copy_tags: bool,
) -> None:
    class NullMetadataStore(InMemoryArrowDatabase):
        """Inject a reserved Arrow Null column at the database read boundary."""

        def get_all_records(
            self, record_path: tuple[str, ...], *args: Any, **kwargs: Any
        ) -> pa.Table | None:
            """Add a reserved column to the stored operator table."""
            table = super().get_all_records(record_path, *args, **kwargs)
            if table is not None and record_path[-1] == "odb_v2":
                return table.append_column("__raw_missing", pa.nulls(table.num_rows))
            return table

    source = ArrowTableSource(
        _table(pa.field("extra", pa.string(), nullable=False), ["a", "b"]),
        tag_columns=TAGS,
        tag_only_columns=True,
        source_id="rows",
    )
    store = NullMetadataStore()
    job = PipelineJob(name="derived", database=store)
    with job:
        DropNulls(["x", "y"])(source, label="origin")
    node = job.nodes["origin"]
    assert isinstance(node, OperatorJobNode)
    node.attach_databases(pipeline_database=store, cache_mode=CacheMode.LOG)
    job.run()
    derived = (
        node.as_source(tags_as_data={"k": "copied"}, source_id="new")
        if copy_tags
        else node.as_source()
    )
    with pytest.raises(
        InputValidationError,
        match=r"'__raw_missing'.*declare a supported nullable type",
    ):
        derived.as_table()


@pytest.mark.parametrize("metadata", [None, {b"owner": b"research"}])
def test_spiral_validates_field_metadata_on_a_tag_copy_nominated_as_a_system_tag(
    tmp_path: Path,
    metadata: dict[bytes, bytes] | None,
) -> None:
    schema = pa.schema(
        [pa.field("k", pa.string(), False, metadata), pa.field("x", pa.int8(), False)]
    )
    table = pa.Table.from_pylist([{"k": "a", "x": 1}, {"k": "b", "x": 2}], schema)
    with fake_spiral_sdk.install(tmp_path):
        external = fake_spiral_sdk.ExternalProject("project").create_table(
            "default.rows", key_schema=pa.schema([schema.field("k")])
        )
        external.write(table)
        external.record(schema)

        def build() -> SpiralDBTableSource:
            return SpiralDBTableSource(
                "project",
                "rows",
                tag_columns=["k"],
                tag_only_columns=True,
                tags_as_data={"k": "copy"},
                system_tag_columns=["copy"],
            )

        if metadata is not None:
            with pytest.raises(
                InputValidationError, match=r"'copy'.*[Rr]emove.*field metadata"
            ):
                build()
            return
        source = build()
        columns = {"system_tags": True}
        assert source.as_table(columns=columns).column("_tag_copy").to_pylist() == [
            "a",
            "b",
        ]
        assert [tag.system_tags()["_tag_copy"] for tag, _ in source.iter_data()] == [
            "a",
            "b",
        ]


def test_spiral_validates_reserved_null_in_an_empty_scan_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = pa.schema(
        [pa.field("k", pa.string(), False), pa.field("x", pa.int8(), False)]
    )
    table = pa.Table.from_pylist([{"k": "a", "x": 1}], schema)
    with fake_spiral_sdk.install(tmp_path):
        external = fake_spiral_sdk.ExternalProject("project").create_table(
            "default.rows", key_schema=pa.schema([schema.field("k")])
        )
        external.write(table)
        external.record(schema)
        source = SpiralDBTableSource("project", "rows", tag_only_columns=True)
        handle = source._lookup_table()  # pyright: ignore[reportPrivateUsage]  # intercept the scan boundary
        invalid = schema.append(pa.field("__unused", pa.null()))
        batch = pa.RecordBatch.from_arrays(
            [pa.array([], f.type) for f in invalid], schema=invalid
        )
        monkeypatch.setattr(source, "_lookup_table", lambda: handle)
        monkeypatch.setattr(handle, "scan_batches", lambda **kwargs: iter([batch]))
        with pytest.raises(
            InputValidationError, match=r"'__unused'.*declare a supported nullable type"
        ):
            source.as_table()
