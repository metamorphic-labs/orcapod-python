"""``tag_only_columns`` on every table source (ITL-1159, ITL-1023).

Every source that declares tag columns accepts ``tag_only_columns``. On ``main`` (v0.3)
its one accepted value is ``True``; with referential tags (v0.4) every declared tag is
referential by default (also a data column of the same name), and ``tag_only_columns``
names the tags kept plain, ``True`` for all of them.

These tests build the same source with and without the option: ``True`` gives the v0.3
source (plain tags), the default a referential one with the same source id and record
ids.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from typing import TypedDict
from unittest.mock import patch

import polars as pl
import pyarrow as pa
import pytest
from deltalake import write_deltalake
from typing_extensions import Unpack

from orcapod.core.referential_tags import TagOnlyColumns
from orcapod.core.sources import (
    ArrowTableSource,
    CachedSource,
    CSVSource,
    DataFrameSource,
    DBTableSource,
    DeltaTableSource,
    DictSource,
    PostgreSQLTableSource,
    SpiralDBTableSource,
    SQLiteTableSource,
)
from orcapod.databases import InMemoryArrowDatabase, SQLiteConnector
from orcapod.protocols.core_protocols import SourceProtocol
from orcapod.types import ColumnConfig
from tests.test_core.sources.test_postgresql_table_source import (
    _PATCH as _PG_PATCH,
)
from tests.test_core.sources.test_postgresql_table_source import (
    DSN,
    _make_mock_connector,
)
from tests.test_databases import fake_pyspiral

# Builds one source over the rows ``id`` a/b, ``x`` 1/2 in ``tmp``; the keyword is either
# absent or ``tag_only_columns=...``.
Build = Callable[..., SourceProtocol]


class Option(TypedDict, total=False):
    """The option under test, absent or given."""

    tag_only_columns: TagOnlyColumns


def _arrow_table() -> pa.Table:
    return pa.table(
        {
            "id": pa.array(["a", "b"], pa.large_string()),
            "x": pa.array([1, 2], pa.int64()),
        }
    )


def _dict(_tmp: Path, **option: Unpack[Option]) -> SourceProtocol:
    return DictSource(
        [{"id": "a", "x": 1}, {"id": "b", "x": 2}], tag_columns=["id"], **option
    )


def _arrow(_tmp: Path, **option: Unpack[Option]) -> SourceProtocol:
    return ArrowTableSource(_arrow_table(), tag_columns=["id"], **option)


def _data_frame(_tmp: Path, **option: Unpack[Option]) -> SourceProtocol:
    return DataFrameSource(
        pl.DataFrame({"id": ["a", "b"], "x": [1, 2]}), tag_columns=["id"], **option
    )


def _csv(tmp: Path, **option: Unpack[Option]) -> CSVSource:
    path = tmp / "rows.csv"
    if not path.exists():
        _ = path.write_text("id,x\na,1\nb,2\n")
    return CSVSource(str(path), tag_columns=["id"], **option)


def _delta(tmp: Path, **option: Unpack[Option]) -> SourceProtocol:
    path = tmp / "delta"
    if not path.exists():
        write_deltalake(str(path), _arrow_table())
    return DeltaTableSource(path, tag_columns=["id"], **option)


def _sqlite_path(tmp: Path) -> Path:
    path = tmp / "rows.db"
    if not path.exists():
        with sqlite3.connect(path) as conn:
            _ = conn.execute("CREATE TABLE r (id TEXT PRIMARY KEY, x INTEGER)")
            _ = conn.executemany("INSERT INTO r VALUES (?, ?)", [("a", 1), ("b", 2)])
    return path


def _sqlite(tmp: Path, **option: Unpack[Option]) -> SourceProtocol:
    # No tag_columns: the primary key is the tag.
    return SQLiteTableSource(_sqlite_path(tmp), "r", **option)


def _db_table(tmp: Path, **option: Unpack[Option]) -> SourceProtocol:
    connector = SQLiteConnector(_sqlite_path(tmp))
    try:
        return DBTableSource(connector, "r", source_id="lab.db", **option)
    finally:
        connector.close()


def _postgresql(_tmp: Path, **option: Unpack[Option]) -> SourceProtocol:
    with patch(_PG_PATCH) as connector_class:
        connector_class.return_value = _make_mock_connector(
            pk_columns=["id"], batches=_arrow_table().to_batches()
        )
        return PostgreSQLTableSource(DSN, "measurements", **option)


@pytest.fixture()
def spiral_root(tmp_path: Path) -> Iterator[None]:
    """Provide a fake Spiral root holding table ``default.r`` keyed on ``id``."""
    with fake_pyspiral.install(tmp_path / "spiral"):
        project = fake_pyspiral.FakeSpiral().project("p")
        table = project.create_table(
            "default.r", key_schema=pa.schema([pa.field("id", pa.string())])
        )
        table.write(pa.table({"id": ["a", "b"], "x": pa.array([1, 2], pa.int64())}))
        yield


def _spiral(_tmp: Path, **option: Unpack[Option]) -> SourceProtocol:
    return SpiralDBTableSource("p", "r", **option)


FACTORIES: dict[str, Callable[..., SourceProtocol]] = {
    "dict": _dict,
    "arrow_table": _arrow,
    "data_frame": _data_frame,
    "csv": _csv,
    "delta_table": _delta,
    "sqlite": _sqlite,
    "db_table": _db_table,
    "postgresql": _postgresql,
    "spiraldb": _spiral,
}


@pytest.fixture(params=sorted(FACTORIES))
def build(request: pytest.FixtureRequest, tmp_path: Path) -> Build:
    """Return a builder of one kind of source, ``build(**option)``."""
    name: str = request.param
    if name == "spiraldb":
        _ = request.getfixturevalue("spiral_root")
    factory = FACTORIES[name]
    return lambda **option: factory(tmp_path, **option)


def _system_tag_values(source: SourceProtocol) -> Sequence[Sequence[object]]:
    table = source.as_table(columns=ColumnConfig(system_tags=True))
    return [
        table.column(name).to_pylist()
        for name in table.column_names
        if name.startswith("_tag")
    ]


class TestTrueKeepsEveryTagPlain:
    """``tag_only_columns=True`` gives the source v0.3 built: no tag is data."""

    def test_tags_stay_out_of_the_data(self, build: Build) -> None:
        tag_keys, data_keys = build(tag_only_columns=True).keys()
        assert tag_keys
        assert not set(tag_keys) & set(data_keys)

    def test_naming_every_tag_is_true(self, build: Build) -> None:
        explicit = build(tag_only_columns=True)
        tag_keys, _ = explicit.keys()
        named = build(tag_only_columns=list(tag_keys))
        assert named.keys() == explicit.keys()
        assert named.content_hash() == explicit.content_hash()
        assert named.pipeline_hash() == explicit.pipeline_hash()

    def test_plain_saved_config_states_true(self, build: Build) -> None:
        # Format 0.3.0 states every binding (ITL-1019): a plain source writes True.
        assert build(tag_only_columns=True).to_config()["tag_only_columns"] is True


class TestTheDefaultIsReferential:
    """Without the option every declared tag is also a data column (ITL-1023)."""

    def test_every_tag_is_data(self, build: Build) -> None:
        tag_keys, data_keys = build().keys()
        assert tag_keys
        assert set(tag_keys) <= set(data_keys)

    def test_an_empty_collection_or_false_is_the_default(self, build: Build) -> None:
        default = build()
        for value in (False, (), []):
            assert (
                build(tag_only_columns=value).content_hash() == default.content_hash()
            )

    def test_another_source_than_the_plain_one(self, build: Build) -> None:
        default = build()
        explicit = build(tag_only_columns=True)
        assert explicit.content_hash() != default.content_hash()
        assert explicit.pipeline_hash() != default.pipeline_hash()
        cache_db = InMemoryArrowDatabase()
        assert (
            CachedSource(explicit, cache_db).cache_path
            != CachedSource(default, cache_db).cache_path
        )

    def test_same_source_id_and_record_ids_as_the_plain_one(self, build: Build) -> None:
        default = build()
        explicit = build(tag_only_columns=True)
        assert explicit.source_id == default.source_id
        values = _system_tag_values(explicit)
        assert len(values) == 2  # the source id and the record id columns
        assert values == _system_tag_values(default)


class TestTheOption:
    def test_unknown_name_is_refused(self, build: Build) -> None:
        with pytest.raises(ValueError, match="not in tag_columns"):
            _ = build(tag_only_columns=["nope"])

    def test_keyword_only(self) -> None:
        with pytest.raises(TypeError):
            _ = ArrowTableSource(_arrow_table(), ["id"], (), None, False, True)  # pyright: ignore[reportCallIssue]  # the option is keyword-only


class TestFromConfig:
    """A config states its binding and reads back as saved; one without it is refused."""

    def test_config_reads_back_plain(self, tmp_path: Path) -> None:
        source = _csv(tmp_path, tag_only_columns=True)
        loaded = CSVSource.from_config(source.to_config())
        assert loaded.content_hash() == source.content_hash()
        assert loaded.keys() == source.keys()

    def test_referential_config_reads_back_referential(self, tmp_path: Path) -> None:
        source = _csv(tmp_path)
        loaded = CSVSource.from_config(source.to_config())
        assert loaded.content_hash() == source.content_hash()
        assert loaded.keys() == source.keys()

    def test_referential_config_states_no_plain_tag(self, tmp_path: Path) -> None:
        assert _csv(tmp_path).to_config()["tag_only_columns"] == []

    def test_config_without_the_key_is_refused(self, tmp_path: Path) -> None:
        # The saved format version decides plain tags, never a missing key (ITL-1159):
        # a 0.1.0 or 0.2.0 file is given True by the loader before a source reads it.
        config = {**_csv(tmp_path).to_config()}
        del config["tag_only_columns"]
        with pytest.raises(ValueError, match="tag_only_columns"):
            _ = CSVSource.from_config(config)
