"""A saved connector database is supplied at load, never rebuilt (ITL-1391).

The connectors cannot hold a function node's tables (entry CA3, ITL-1162), so the jobs
here hold operators only. PostgreSQL cannot hold an operator node's tables either (a
table name is cut at 63 bytes), so a file naming it as the job's database is made by
putting its configuration into a saved file; as a result database it is saved directly.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    PostgreSQLConnector,
    SpiralDBConnector,
    SQLiteConnector,
)
from orcapod.sources import ArrowTableSource
from tests.test_databases import fake_pyspiral

KINDS = ("sqlite", "postgresql", "spiraldb")
# The kinds an operator-only job runs on.
RUNNABLE = ("sqlite", "spiraldb")
# Not a real credential.
PASSWORD = "synthetic-pass-word"


class Databases:
    """Connector databases of each kind, each over its own place per name."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def make(self, kind: str, name: str) -> ConnectorArrowDatabase:
        if kind == "sqlite":
            return ConnectorArrowDatabase(
                SQLiteConnector(str(self.root / f"{name}.sqlite"))
            )
        if kind == "postgresql":
            return ConnectorArrowDatabase(
                PostgreSQLConnector(f"postgresql://u:{PASSWORD}@h/{name}")
            )
        fake_pyspiral.FakeSpiral().project(name)
        return ConnectorArrowDatabase(SpiralDBConnector(name))


@pytest.fixture
def databases(tmp_path: Path) -> Iterator[Databases]:
    with (
        fake_pyspiral.install(tmp_path / "spiral"),
        patch("psycopg.connect") as connect,
    ):
        connect.return_value = MagicMock()
        yield Databases(tmp_path)


def _sources() -> dict[str, Any]:
    left = pa.table(
        {"a": [1, 2, 3], "b": ["x", "y", "z"], "p": [1.0, 2.0, 3.0], "q": [4, 5, 6]}
    )
    right = pa.table({"b": ["z", "x"], "a": [3, 1], "r": [30, 10], "s": ["u", "v"]})
    return {
        name: ArrowTableSource(
            table, tag_columns=["a", "b"], source_id=name, tag_only_columns=True
        )
        for name, table in (("left", left), ("right", right))
    }


def _save(
    path: Path, *, run_async: bool = False, **databases: Any
) -> tuple[PipelineJob, dict[str, Any]]:
    """An operator-only job, run and saved, and its file's bindings."""
    sources = _sources()
    job = PipelineJob(name="j", **databases)
    with job:
        joined = sources["left"].join(sources["right"], label="joined")
        joined.select_data_columns(["s", "p"], label="picked")
    if run_async:
        asyncio.run(job.run_async())
    else:
        job.run()
    job.save(path)
    return job, json.loads(path.read_text())["bindings"]


def _save_over(path: Path, databases: Databases, kind: str, **given: Any) -> None:
    """A saved job file whose database is of ``kind`` (see the module docstring)."""
    if kind in RUNNABLE:
        _save(path, database=databases.make(kind, "a"), **given)
        return
    _save(path, database=databases.make("sqlite", "a"), **given)
    data = json.loads(path.read_text())
    data["bindings"]["database"] = databases.make(kind, "a").to_config()
    path.write_text(json.dumps(data))


def _values(job: PipelineJob) -> list[dict[str, Any]]:
    rows = job.nodes["picked"].as_table().select(["a", "b", "s", "p"]).to_pylist()
    return sorted(rows, key=lambda row: row["a"])


EXPECTED = [
    {"a": 1, "b": "x", "s": "v", "p": 1.0},
    {"a": 3, "b": "z", "s": "u", "p": 3.0},
]


@pytest.mark.parametrize("kind", KINDS)
def test_load_without_the_database_names_the_slot_and_what_to_pass(
    databases: Databases, tmp_path: Path, kind: str
) -> None:
    path = tmp_path / "job.json"
    _save_over(path, databases, kind)
    with pytest.raises(ValueError) as refused:
        PipelineJob.load(path)
    message = str(refused.value)
    assert f"bindings.database is a connector database ({kind})" in message
    assert "pass database=ConnectorArrowDatabase(" in message
    assert "result_database" not in message
    assert "Unknown database type" not in message
    # The saved connection is never shown.
    assert PASSWORD not in message


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("result_kind", KINDS)
def test_every_missing_slot_is_named(
    databases: Databases, tmp_path: Path, kind: str, result_kind: str
) -> None:
    path = tmp_path / "job.json"
    results = databases.make(result_kind, "b")
    _save_over(path, databases, kind, result_database=results)
    # Any given database fills a slot: one the job's operators can be stored in.
    database = databases.make("sqlite", "a")
    wanted = {
        "database": f"bindings.database is a connector database ({kind})",
        "result_database": (
            f"bindings.result_database is a connector database ({result_kind})"
        ),
    }
    for given in ({}, {"database": database}, {"result_database": results}):
        with pytest.raises(ValueError) as refused:
            PipelineJob.load(path, **given)
        for slot, text in wanted.items():
            assert (text in str(refused.value)) is (slot not in given)
        assert PASSWORD not in str(refused.value)
    loaded = PipelineJob.load(path, result_database=results, database=database)
    assert loaded.database is database
    assert loaded.result_database is results


@pytest.mark.parametrize("kind", KINDS)
def test_a_connector_result_database_beside_a_delta_database(
    databases: Databases, tmp_path: Path, kind: str
) -> None:
    path = tmp_path / "job.json"
    results = databases.make(kind, "b")
    _save(
        path,
        database=DeltaTableDatabase(tmp_path / "delta"),
        result_database=results,
    )
    with pytest.raises(ValueError, match="pass result_database="):
        PipelineJob.load(path)
    loaded = PipelineJob.load(path, result_database=results)
    assert isinstance(loaded.database, DeltaTableDatabase)


@pytest.mark.parametrize("kind", RUNNABLE)
@pytest.mark.parametrize("saved_async", [False, True])
@pytest.mark.parametrize("loaded_async", [False, True])
def test_load_with_the_database_then_load_only(
    databases: Databases,
    tmp_path: Path,
    kind: str,
    saved_async: bool,
    loaded_async: bool,
) -> None:
    path = tmp_path / "job.json"
    job, bindings = _save(
        path, run_async=saved_async, database=databases.make(kind, "a")
    )
    assert _values(job) == EXPECTED
    hashes = {label: n.content_hash().to_string() for label, n in job.nodes.items()}

    # A new handle over the same place, as another process would make.
    loaded = PipelineJob.load(path, database=databases.make(kind, "a"))
    loaded.bind(sources=_sources())
    assert {
        label: n.content_hash().to_string() for label, n in loaded.nodes.items()
    } == hashes
    if loaded_async:
        asyncio.run(loaded.run_async(load_only=True))
    else:
        loaded.run(load_only=True)
    assert _values(loaded) == EXPECTED

    again = tmp_path / "again.json"
    loaded.save(again)
    assert (
        json.loads(again.read_text())["bindings"]["database"] == (bindings["database"])
    )
    with pytest.raises(ValueError, match="pass database="):
        PipelineJob.load(again)


@pytest.mark.parametrize("kind", KINDS)
def test_any_given_database_is_used_whatever_connector_was_saved(
    databases: Databases, tmp_path: Path, kind: str
) -> None:
    path = tmp_path / "job.json"
    _save_over(path, databases, kind)
    given = DeltaTableDatabase(tmp_path / "delta")
    assert PipelineJob.load(path, database=given).database is given


@pytest.mark.parametrize("connector", [None, {}, {"connector_type": 3}, "text"])
def test_a_connector_the_file_does_not_name_is_refused_too(
    databases: Databases, tmp_path: Path, connector: Any
) -> None:
    path = tmp_path / "job.json"
    _save(path, database=databases.make("sqlite", "a"))
    data = json.loads(path.read_text())
    data["bindings"]["database"]["connector"] = connector
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match=r"connector database \(unknown\)"):
        PipelineJob.load(path)


def test_saving_a_loaded_job_keeps_a_database_that_could_not_be_rebuilt(
    tmp_path: Path,
) -> None:
    path, again = tmp_path / "job.json", tmp_path / "again.json"
    _save(path, database=DeltaTableDatabase(tmp_path / "delta"))
    data = json.loads(path.read_text())
    saved = {"type": "no_such_database", "place": ["x", "y"]}
    data["bindings"]["database"] = saved
    path.write_text(json.dumps(data))

    loaded = PipelineJob.load(path)
    assert loaded.database is None
    loaded.save(again)
    assert json.loads(again.read_text())["bindings"]["database"] == saved

    # A bind of sources or a result database leaves it; a bound database replaces it.
    loaded.bind(sources=_sources())
    loaded.bind(result_database=DeltaTableDatabase(tmp_path / "results"))
    loaded.save(again)
    assert json.loads(again.read_text())["bindings"]["database"] == saved
    given = DeltaTableDatabase(tmp_path / "given")
    loaded.bind(database=given)
    loaded.save(again)
    assert json.loads(again.read_text())["bindings"]["database"] == given.to_config()
