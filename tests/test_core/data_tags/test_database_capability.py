"""A database states whether it keeps Arrow field metadata (ITL-1019).

The developer's rule: a database that cannot keep field metadata is refused before
anything is written, and when in doubt, refuse. A database states the capability with
``keeps_field_metadata`` (``FieldMetadataKeepingDatabaseProtocol``); one without the member
is refused when a node would store a data-tag in it. Every database that states
``True`` keeps the marker on its write paths: in memory and Delta are covered by
``test_persistence.py``; SpiralDB is covered here against the pyspiral fake only (the live
service is unverified).
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.contexts import resolve_context
from orcapod.core.operators import Join
from orcapod.core.data_tags import mark_data_tags
from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    InMemoryArrowDatabase,
    NoOpArrowDatabase,
    SpiralDBConnector,
)
from orcapod.databases.extension_aware_database import ExtensionAwareDatabase
from orcapod.databases.sqlite_connector import SQLiteConnector
from orcapod.errors import InputValidationError
from orcapod.pipeline import PipelineJob
from orcapod.protocols.database_protocols import (
    FieldMetadataKeepingDatabaseProtocol,
    keeps_field_metadata,
)
from orcapod.types import CacheMode
from tests.test_databases import fake_spiral_sdk

CALLS: list[float] = []


@op.function_pod(output_keys=["y"])
def double(amp: float) -> float:
    """Double the amplitude; reads no tag, so ``unit`` falls through."""
    CALLS.append(amp)
    return 2 * amp


class ThirdPartyDatabase:
    """A database adapter that keeps field metadata but does not say so.

    It delegates every call to an in-memory database, except the capability, which it
    does not have: an unknown database.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def at(self, *path_components: str) -> ThirdPartyDatabase:
        return ThirdPartyDatabase(self._inner.at(*path_components))

    def __getattr__(self, name: str) -> Any:
        if name == "keeps_field_metadata":
            raise AttributeError(name)
        return getattr(self._inner, name)


class DeclaringThirdPartyDatabase(ThirdPartyDatabase):
    """The same adapter, stating that it keeps field metadata."""

    keeps_field_metadata = True

    def at(self, *path_components: str) -> DeclaringThirdPartyDatabase:
        return DeclaringThirdPartyDatabase(self._inner.at(*path_components))


def _extension_aware(database: Any) -> Any:
    """An ``ExtensionAwareDatabase`` over ``database``, typed loosely for the tests."""
    converter: Any = resolve_context(None).type_converter
    return ExtensionAwareDatabase(database, converter=converter)


def _adapter(cls: type[ThirdPartyDatabase]) -> Any:
    """An adapter over a fresh in-memory database; duck-typed, as a third party's is."""
    return cls(InMemoryArrowDatabase())


@pytest.fixture()
def spiral(tmp_path: Path) -> Iterator[None]:
    with fake_spiral_sdk.install(tmp_path / "spiral"):
        yield


def _source() -> op.sources.DictSource:
    return op.sources.DictSource(
        [{"unit": 1, "time": 10, "amp": 0.5}, {"unit": 2, "time": 20, "amp": 1.5}],
        tag_columns=["unit", "time"],
        tag_only_columns=["time"],
        label="spikes",
        source_id="spikes",
    )


# ---------------------------------------------------------------------------
# What each database states
# ---------------------------------------------------------------------------


def test_each_database_states_the_capability(tmp_path: Path, spiral: None) -> None:
    sqlite = ConnectorArrowDatabase(SQLiteConnector(":memory:"))
    spiraldb = ConnectorArrowDatabase(SpiralDBConnector("p"))
    stated = {
        "in memory": InMemoryArrowDatabase(),
        "delta": DeltaTableDatabase(tmp_path / "delta"),
        "spiraldb": spiraldb,
        "sqlite": sqlite,
        "noop": NoOpArrowDatabase(),
        "extension-aware over in memory": _extension_aware(InMemoryArrowDatabase()),
        "extension-aware over sqlite": _extension_aware(sqlite),
        "in memory, scoped": InMemoryArrowDatabase().at("a", "b"),
        "spiraldb, scoped": spiraldb.at("a"),
    }
    assert {name: keeps_field_metadata(db) for name, db in stated.items()} == {
        "in memory": True,
        "delta": True,
        "spiraldb": True,
        "sqlite": False,
        "noop": False,
        "extension-aware over in memory": True,
        "extension-aware over sqlite": False,
        "in memory, scoped": True,
        "spiraldb, scoped": True,
    }
    for db in stated.values():
        assert isinstance(db, FieldMetadataKeepingDatabaseProtocol)


def test_a_database_without_the_member_does_not_keep_field_metadata() -> None:
    adapter = ThirdPartyDatabase(InMemoryArrowDatabase())
    assert not keeps_field_metadata(adapter)
    assert not isinstance(adapter, FieldMetadataKeepingDatabaseProtocol)
    # A wrapper never states more than the database it wraps.
    assert not keeps_field_metadata(_extension_aware(adapter))


# ---------------------------------------------------------------------------
# An unknown database is refused before anything is written
# ---------------------------------------------------------------------------


def test_a_function_node_refuses_an_undeclared_database() -> None:
    inner = InMemoryArrowDatabase()
    adapter: Any = ThirdPartyDatabase(inner)
    job = PipelineJob(database=adapter)
    CALLS.clear()
    with pytest.raises(InputValidationError) as raised:
        with job:
            double.pod(_source(), label="double")
        job.run()
    message = str(raised.value)
    assert "ThirdPartyDatabase cannot store the data-tags ['unit']" in message
    assert "keeps_field_metadata" in message
    assert "keeps field metadata" in message and "tag_only_columns" in message
    assert CALLS == []
    assert inner.list_table_paths() == []


@pytest.mark.parametrize("mode", [CacheMode.LOG, CacheMode.REPLAY])
def test_an_operator_node_refuses_an_undeclared_database(mode: CacheMode) -> None:
    def source(name: str, column: str) -> op.sources.ArrowTableSource:
        table = pa.table({"id": [1, 2], column: [1.0, 2.0]})
        return op.sources.ArrowTableSource(
            table, tag_columns=["id"], label=name, source_id=name
        )

    job = PipelineJob(name="ops", database=InMemoryArrowDatabase())
    with job:
        _ = Join()(source("a", "x"), source("b", "z"), label="joined")
    inner = InMemoryArrowDatabase()
    with pytest.raises(InputValidationError, match="ThirdPartyDatabase cannot store"):
        job.nodes["joined"].attach_databases(
            pipeline_database=ThirdPartyDatabase(inner), cache_mode=mode
        )
    assert inner.list_table_paths() == []


def test_an_extension_aware_wrapper_cannot_pass_an_undeclared_database() -> None:
    job = PipelineJob(
        database=_extension_aware(ThirdPartyDatabase(InMemoryArrowDatabase()))
    )
    with pytest.raises(
        InputValidationError, match="ExtensionAwareDatabase cannot store"
    ):
        with job:
            double.pod(_source(), label="double")
        job.run()


def test_a_declaring_adapter_is_accepted() -> None:
    job = PipelineJob(database=_adapter(DeclaringThirdPartyDatabase))
    with job:
        double.pod(_source(), label="double")
    job.run()
    assert job.nodes["double"].keys() == (("unit", "time"), ("unit", "y"))


def test_plain_tags_ask_nothing_of_an_undeclared_database() -> None:
    job = PipelineJob(database=_adapter(ThirdPartyDatabase))
    plain = op.sources.DictSource(
        [{"unit": 1, "amp": 0.5}],
        tag_columns=["unit"],
        tag_only_columns=True,
        label="plain",
        source_id="plain",
    )
    with job:
        double.pod(plain, label="double")
    job.run()
    assert job.nodes["double"].keys() == (("unit",), ("y",))


def _rows(ids: list[int], extra: bool = False) -> pa.Table:
    columns: dict[str, pa.Array[Any]] = {
        "rid": pa.array([str(i).encode() for i in ids], pa.large_binary()),
        "unit": pa.array(ids, pa.int64()),
        "y": pa.array([float(i) for i in ids], pa.float64()),
    }
    if extra:
        columns["z"] = pa.array(ids, pa.int64())
    return mark_data_tags(pa.Table.from_pydict(columns), ["unit"])
