"""A node whose newest record names a result that is gone (ITL-1525).

One instance has two pipeline records, and the result the newer record names is not
in the result store, while the older record's result is. The node serves the stored
result: an ordinary read, with no function call, no new record and no warning. The
log used to say "These inputs will be recomputed", which no run did.

The fixture is built through the public interface: one pipeline database, the job run
twice over two fresh result databases, then a fresh job over the same pipeline
database and the store that holds only the first run's results. The source has two
tags and two data columns, in two arrangements; the function has two outputs. Every
case runs on the sync and the async path, as a full and as a load-only run, over
in-memory and Delta databases.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, final

import pyarrow.compute as pc
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import RunSummary
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.sources import DictSource
from orcapod.system_constants import DATA_RECORD_ID
from orcapod.types import NodeConfig

KEY = "__key"
NODE_LOGGER = "orcapod.core.nodes.function_node"
# (tag order, data order): two arrangements of the same rows.
ARRANGEMENTS = {
    "as_declared": (("k", "s"), ("x", "w")),
    "reversed": (("s", "k"), ("w", "x")),
}
ROWS = [
    {"k": 1, "s": "a", "x": 0, "w": 5},
    {"k": 2, "s": "b", "x": 1, "w": 6},
]

BACKEND = pytest.mark.parametrize("backend", ["memory", "delta"])
USE_ASYNC = pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
LOAD_ONLY = pytest.mark.parametrize(
    "load_only", [False, True], ids=["full", "load_only"]
)
ARRANGEMENT = pytest.mark.parametrize("arrangement", list(ARRANGEMENTS))


@final
class Fixture:
    """Two records per instance; only the first run's results are still stored."""

    def __init__(
        self,
        backend: str,
        tmp_path: Path,
        arrangement: str,
        use_async: bool,
        *,
        ephemeral: bool,
        newer_persistent: bool = False,
    ) -> None:
        self.use_async = use_async
        self.ephemeral = ephemeral
        self.calls: list[int] = []
        self._added = 0
        self._backend = backend
        self._tmp_path = tmp_path
        self._databases = 0
        tags, data = ARRANGEMENTS[arrangement]
        self._tags = list(tags)
        self._rows = [{name: row[name] for name in (*tags, *data)} for row in ROWS]

        fixture = self

        @function_pod(output_keys=["result", "other"])
        def compute(x: int, w: int) -> tuple[int, int]:
            fixture.calls.append(x)
            return x + fixture._added, w + fixture._added

        self._compute = compute
        self.pipeline_db = self._database()
        self.older_results = self._database()
        self.newer_results = self._database()
        self._added = 10
        assert self.run(self.job(self.older_results)).ok
        self._added = 20
        assert self.run(
            self.job(self.newer_results, ephemeral=ephemeral and not newer_persistent)
        ).ok
        self._added = 30
        assert sorted(self.calls) == [0, 0, 1, 1]
        self.calls.clear()
        assert self.record_count() == 4

    def _database(self) -> ArrowDatabaseProtocol:
        if self._backend == "memory":
            return InMemoryArrowDatabase()
        self._databases += 1
        return DeltaTableDatabase(self._tmp_path / f"db{self._databases}")

    def job(
        self, results: ArrowDatabaseProtocol, *, ephemeral: bool | None = None
    ) -> PipelineJob:
        """A fresh job over the pipeline database and *results*."""
        if ephemeral is None:
            ephemeral = self.ephemeral
        job = PipelineJob(
            name="newest_result_missing",
            database=self.pipeline_db,
            result_database=InMemoryArrowDatabase() if ephemeral else results,
        )
        with job:
            source = DictSource(self._rows, tag_columns=self._tags, source_id="rows")
            self._compute.pod(source, label="compute")
        if ephemeral:
            job.apply_node_config(NodeConfig(is_result_ephemeral=True))
            job.set_ephemeral_database(results)
        return job

    def run(self, job: PipelineJob, *, load_only: bool = False) -> RunSummary:
        if self.use_async:
            return asyncio.run(job.run_async(load_only=load_only))
        return job.run(load_only=load_only)

    def record_count(self) -> int:
        (path,) = self.pipeline_db.list_table_paths()
        records = self.pipeline_db.get_all_records(path, record_id_column=KEY)
        assert records is not None
        return records.num_rows


def _rows(job: PipelineJob) -> list[tuple[Any, ...]]:
    return sorted(
        (
            tag.as_dict()["k"],
            tag.as_dict()["s"],
            data.as_dict()["result"],
            data.as_dict()["other"],
        )
        for tag, data in job.nodes["compute"].iter_data()
    )


def _node_log(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [record for record in caplog.records if record.name == NODE_LOGGER]


SERVED = [(1, "a", 10, 15), (2, "b", 11, 16)]


@BACKEND
@USE_ASYNC
@LOAD_ONLY
@ARRANGEMENT
def test_stored_result_is_served_and_nothing_claims_a_recompute(
    backend: str,
    use_async: bool,
    load_only: bool,
    arrangement: str,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fixture = Fixture(backend, tmp_path, arrangement, use_async, ephemeral=False)
    reader = fixture.job(fixture.older_results)

    # The seeding runs log too: only the run under test is examined.
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="orcapod"):
        summary = fixture.run(reader, load_only=load_only)

    assert summary.ok
    assert _rows(reader) == SERVED
    assert fixture.calls == []
    assert fixture.record_count() == 4
    node = summary.nodes["compute"]
    assert (node.computed, node.reused, node.not_stored) == (0, 2, 0)
    assert not any("recomputed" in r.getMessage() for r in caplog.records)
    # Serving a stored result is ordinary: no warning.
    assert [r for r in _node_log(caplog) if r.levelno >= logging.WARNING] == []


@BACKEND
@USE_ASYNC
@ARRANGEMENT
def test_ephemeral_full_run_serves_the_stored_result(
    backend: str,
    use_async: bool,
    arrangement: str,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fixture = Fixture(backend, tmp_path, arrangement, use_async, ephemeral=True)
    reader = fixture.job(fixture.older_results)

    # The seeding runs log too: only the run under test is examined.
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="orcapod"):
        summary = fixture.run(reader)

    assert summary.ok
    assert _rows(reader) == SERVED
    assert fixture.calls == []
    assert fixture.record_count() == 4
    node = summary.nodes["compute"]
    assert (node.computed, node.reused, node.not_stored) == (0, 2, 0)
    assert not any("recomputed" in r.getMessage() for r in caplog.records)
    assert [r for r in _node_log(caplog) if r.levelno >= logging.WARNING] == []


@USE_ASYNC
@LOAD_ONLY
@pytest.mark.parametrize(
    "keep_one_persistent_result", [False, True], ids=["absent", "partial"]
)
def test_ephemeral_result_is_served_silently_when_persistent_result_is_gone(
    use_async: bool,
    load_only: bool,
    keep_one_persistent_result: bool,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An older ephemeral match suppresses a warning about a newer persistent miss."""
    fixture = Fixture(
        "memory",
        tmp_path,
        "as_declared",
        use_async,
        ephemeral=True,
        newer_persistent=True,
    )
    reader = fixture.job(fixture.older_results)
    expected = SERVED
    if keep_one_persistent_result:
        (path,) = fixture.newer_results.list_table_paths()
        records = fixture.newer_results.get_all_records(
            path, record_id_column=DATA_RECORD_ID
        )
        assert records is not None
        persistent_results = reader.result_database
        assert persistent_results is not None
        persistent_results.add_records(
            path,
            records.filter(pc.equal(records["result"], 21)),
            record_id_column=DATA_RECORD_ID,
            flush=True,
        )
        expected = [SERVED[0], (2, "b", 21, 26)]

    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="orcapod"):
        summary = fixture.run(reader, load_only=load_only)

    assert summary.ok
    assert _rows(reader) == expected
    assert fixture.calls == []
    assert fixture.record_count() == 4
    node = summary.nodes["compute"]
    assert (node.computed, node.reused, node.not_stored) == (0, 2, 0)
    assert [r for r in _node_log(caplog) if r.levelno >= logging.WARNING] == []


@BACKEND
@USE_ASYNC
@ARRANGEMENT
def test_ephemeral_load_only_run_skips_and_counts(
    backend: str,
    use_async: bool,
    arrangement: str,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Ephemeral mode is unchanged: a load-only run skips these inputs."""
    fixture = Fixture(backend, tmp_path, arrangement, use_async, ephemeral=True)
    reader = fixture.job(fixture.older_results)

    # The seeding runs log too: only the run under test is examined.
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="orcapod"):
        summary = fixture.run(reader, load_only=True)

    assert summary.ok
    assert _rows(reader) == []
    assert fixture.calls == []
    assert fixture.record_count() == 4
    node = summary.nodes["compute"]
    assert (node.computed, node.reused, node.not_stored) == (0, 0, 2)
    assert not any("recomputed" in r.getMessage() for r in caplog.records)


@BACKEND
@USE_ASYNC
@LOAD_ONLY
@ARRANGEMENT
def test_no_stored_result_at_all_is_reported_without_a_recompute_claim(
    backend: str,
    use_async: bool,
    load_only: bool,
    arrangement: str,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """With every result gone, a full run computes and a load-only run skips."""
    fixture = Fixture(backend, tmp_path, arrangement, use_async, ephemeral=False)
    reader = fixture.job(fixture._database())

    # The seeding runs log too: only the run under test is examined.
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="orcapod"):
        summary = fixture.run(reader, load_only=load_only)

    assert summary.ok
    node = summary.nodes["compute"]
    if load_only:
        assert _rows(reader) == []
        assert fixture.calls == []
        assert fixture.record_count() == 4
        assert (node.computed, node.reused, node.not_stored) == (0, 0, 2)
    else:
        assert _rows(reader) == [(1, "a", 30, 35), (2, "b", 31, 36)]
        assert sorted(fixture.calls) == [0, 1]
        assert fixture.record_count() == 6
        assert (node.computed, node.reused, node.not_stored) == (2, 0, 0)
    assert not any("recomputed" in r.getMessage() for r in caplog.records)
    warnings = [r for r in _node_log(caplog) if r.levelno >= logging.WARNING]
    assert warnings
    assert all("no stored result" in r.getMessage() for r in warnings)
    assert all(r.getMessage().startswith("2 inputs") for r in warnings)
