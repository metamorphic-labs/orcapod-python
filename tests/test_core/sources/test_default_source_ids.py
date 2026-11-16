"""Every source's default source id names its kind; a given id is used as it is (ITL-1154).

Developer, November 15, 2026: "every source ought to have some default naming convention
for the source id that reflects the category, but we should make it totally overridable."
A default is ``<kind>:<rest>``, the kind being the class's ``SOURCE_KIND`` (its config type
key): ``<kind>:<label>:<hex>`` from a hash, ``<kind>:<location>`` from where the data lives.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from orcapod.contexts import get_default_context
from orcapod.core.sources import (
    ArrowTableSource,
    CachedSource,
    CSVSource,
    DataFrameSource,
    DeltaTableSource,
    DerivedSource,
    DictSource,
    ListSource,
    PollingSource,
    SQLiteTableSource,
)
from orcapod.core.sources.db_table_source import DBTableSource
from orcapod.core.sources.postgresql_table_source import PostgreSQLTableSource
from orcapod.core.sources.source_ids import hash_source_id, location_source_id
from orcapod.core.sources.spiraldb_table_source import SpiralDBTableSource
from orcapod.pipeline.serialization import SOURCE_REGISTRY
from tests.test_channels.test_polling_source import FakeDynamicSource

GIVEN = "a given id: kept as it is, even with :: and spaces"


# ---------------------------------------------------------------------------
# The one place that builds a location default
# ---------------------------------------------------------------------------


def test_location_source_id_is_kind_then_location() -> None:
    """``location_source_id`` joins the kind and the location with one ``:``."""
    assert location_source_id("csv", "data/x.csv") == "csv:data/x.csv"
    # A ':' in the location is kept: only '::' is refused.
    assert location_source_id("csv", "C:/x.csv") == "csv:C:/x.csv"


@pytest.mark.parametrize(
    ("kind", "location"),
    [("csv", "a::b.csv"), ("csv", ":b.csv"), ("csv", ""), ("", "x"), ("c:sv", "x")],
    ids=[
        "double-colon",
        "leading-colon",
        "empty-location",
        "empty-kind",
        "colon-in-kind",
    ],
)
def test_location_source_id_refuses(kind: str, location: str) -> None:
    """A location with ``::``, an empty location and a bad kind have no default."""
    with pytest.raises(ValueError):
        location_source_id(kind, location)


# ---------------------------------------------------------------------------
# One vocabulary: the kind is the config type key
# ---------------------------------------------------------------------------


ROOT_SOURCES: list[type[Any]] = [
    ArrowTableSource,
    CachedSource,
    CSVSource,
    DataFrameSource,
    DeltaTableSource,
    DerivedSource,
    DictSource,
    ListSource,
    PollingSource,
    SQLiteTableSource,
    PostgreSQLTableSource,
    SpiralDBTableSource,
    DBTableSource,
]


@pytest.mark.parametrize("cls", ROOT_SOURCES, ids=lambda c: c.__name__)
def test_every_root_source_has_a_kind(cls: type[Any]) -> None:
    """Every root source class declares a ``SOURCE_KIND`` with no ``:``."""
    kind = cls.__dict__.get("SOURCE_KIND")
    assert isinstance(kind, str) and kind and ":" not in kind


def test_registry_keys_are_the_kinds() -> None:
    """The source config registry is keyed by each class's ``SOURCE_KIND``."""
    for key, cls in SOURCE_REGISTRY.items():
        assert key == cls.SOURCE_KIND, (key, cls)


# ---------------------------------------------------------------------------
# Location defaults, and a given id kept as it is
# ---------------------------------------------------------------------------


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "rows.csv"
    path.write_text("k,v\na,1\nb,2\n")
    return path


@pytest.fixture
def delta_path(tmp_path: Path) -> Path:
    path = tmp_path / "events"
    write_deltalake(str(path), pa.table({"k": ["a", "b"], "v": [1, 2]}))
    return path


@pytest.fixture
def sqlite_path(tmp_path: Path) -> Path:
    path = tmp_path / "db.sqlite"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE trials (k TEXT PRIMARY KEY, v INTEGER)")
        conn.execute("INSERT INTO trials VALUES ('a', 1), ('b', 2)")
    return path


def test_csv_default_and_given(csv_path: Path) -> None:
    """``csv:<path as passed>``; a given id is used as it is."""
    assert CSVSource(str(csv_path), tag_columns="k").source_id == f"csv:{csv_path}"
    assert CSVSource(str(csv_path), tag_columns="k", source_id=GIVEN).source_id == GIVEN


def test_csv_path_with_double_colon_needs_a_source_id(tmp_path: Path) -> None:
    """A path holding ``::`` cannot be a default id: the source asks for a source_id."""
    path = tmp_path / "a::b.csv"
    path.write_text("k,v\na,1\n")
    with pytest.raises(ValueError, match="source_id"):
        CSVSource(str(path), tag_columns="k")
    assert CSVSource(str(path), tag_columns="k", source_id="ab").source_id == "ab"


def test_delta_default_and_given(delta_path: Path) -> None:
    """``delta_table:<directory name>``; a given id is used as it is."""
    assert DeltaTableSource(delta_path, tag_columns="k").source_id == (
        "delta_table:events"
    )
    given = DeltaTableSource(delta_path, tag_columns="k", source_id=GIVEN)
    assert given.source_id == GIVEN


def test_sqlite_default_and_given(sqlite_path: Path) -> None:
    """``sqlite_table:<table name>``; a given id is used as it is."""
    assert SQLiteTableSource(sqlite_path, "trials").source_id == "sqlite_table:trials"
    given = SQLiteTableSource(sqlite_path, "trials", source_id=GIVEN)
    assert given.source_id == GIVEN


def test_polling_default_is_the_hash_of_the_impl_identity() -> None:
    """``polling_source:<label>:<hex>`` of ``impl.identity()``, never its repr."""
    source = PollingSource(FakeDynamicSource(batches=[]), tag_columns="id")
    identity_hash = get_default_context().semantic_hasher.hash_object(
        "FakeDynamicSource"
    )
    assert source.source_id == hash_source_id("polling_source", identity_hash)
    given = PollingSource(
        FakeDynamicSource(batches=[]), tag_columns="id", source_id=GIVEN
    )
    assert given.source_id == GIVEN


@pytest.mark.parametrize(
    "make",
    [
        lambda: DictSource(
            data=[{"k": "a", "v": 1}], tag_columns=["k"], source_id=GIVEN
        ),
        lambda: ArrowTableSource(
            pa.table({"k": ["a"], "v": [1]}), tag_columns=["k"], source_id=GIVEN
        ),
        lambda: ListSource(name="item", data=[1, 2], source_id=GIVEN),
    ],
    ids=["dict", "arrow_table", "list"],
)
def test_in_memory_sources_keep_a_given_id(make: Any) -> None:
    """A given id is used as it is by the in-memory sources too."""
    assert make().source_id == GIVEN
