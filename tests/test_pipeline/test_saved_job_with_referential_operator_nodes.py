"""Saved and loaded jobs with operator nodes and derived sources over referential tags.

ITL-1019 (gap audit S1, S2). The saved job is an independent point of verification: a
loaded operator node declares the referential tags of its saved output schema (a name in
both halves) and reads its stored table only when the table's marker agrees. A derived
source over a job node writes no new key: its saved schema states the binding.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import orcapod as op
from orcapod.core.operators import Join
from orcapod.core.referential_tags import stored_references, without_reference_marks
from orcapod.core.sources import DerivedSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.types import CacheMode

ORCHESTRATORS = pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
Orchestrator = type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator]
JOINED_KEYS = (("unit", "time"), ("unit", "amp", "gain"))


@op.function_pod(output_keys=["y"])
def double(amp: float) -> float:
    """Double the amplitude; reads no tag, so ``unit`` falls through."""
    return 2 * amp


def _csv(tmp_path: Path, name: str, column: str) -> op.sources.CSVSource:
    path = tmp_path / f"{name}.csv"
    path.write_text(f"unit,time,{column}\n1,10,0.5\n2,20,1.5\n")
    return op.sources.CSVSource(
        str(path),
        tag_columns=["unit", "time"],
        tag_only_columns=["time"],
        source_id=name,
    )


def _join_job(tmp_path: Path, mode: CacheMode = CacheMode.LOG) -> PipelineJob:
    """Two CSV sources joined; the join node stores (``LOG``) or replays."""
    store = DeltaTableDatabase(tmp_path / "store")
    job = PipelineJob(name="saved", database=store)
    with job:
        _ = Join()(
            _csv(tmp_path, "spikes", "amp"),
            _csv(tmp_path, "gains", "gain"),
            label="joined",
        )
    job.nodes["joined"].attach_databases(
        pipeline_database=store.at("saved"), cache_mode=mode
    )
    return job


def _hashes(job: PipelineJob) -> dict[str, tuple[str, str, Any]]:
    return {
        label: (
            node.content_hash().to_string(),
            node.pipeline_hash().to_string(),
            node.keys(),
        )
        for label, node in job.nodes.items()
    }


def _rows(table: Any) -> list[dict[str, Any]]:
    return sorted(table.to_pylist(), key=lambda row: str(row["unit"]))


def _saved_job(tmp_path: Path, orchestrator: Orchestrator) -> PipelineJob:
    """Run a ``LOG`` join and save the job."""
    job = _join_job(tmp_path)
    job.run(orchestrator=orchestrator())
    job.save(tmp_path / "job.json")
    return job


def _loaded_replaying(
    tmp_path: Path, database: ArrowDatabaseProtocol | None = None
) -> PipelineJob:
    """Load the saved job and let its join node replay what the run stored.

    A saved job does not record an operator node's cache mode (it loads as ``OFF``),
    so the mode is attached again after loading.
    """
    loaded = PipelineJob.load(tmp_path / "job.json", database=database)
    store = database or DeltaTableDatabase(tmp_path / "store")
    loaded.nodes["joined"].attach_databases(
        pipeline_database=store.at("saved"), cache_mode=CacheMode.REPLAY
    )
    return loaded


@ORCHESTRATORS
def test_a_loaded_operator_node_replays_its_stored_binding(
    tmp_path: Path, orchestrator: Orchestrator
) -> None:
    job = _saved_job(tmp_path, orchestrator)
    assert job.nodes["joined"].keys() == JOINED_KEYS
    expected = _rows(job.nodes["joined"].as_table())

    loaded = _loaded_replaying(tmp_path)
    assert _hashes(loaded) == _hashes(job)
    loaded.run(load_only=True, orchestrator=orchestrator())
    node = loaded.nodes["joined"]
    table = node.as_table(all_info=True)
    assert table.column_names.count("unit") == 1
    assert _rows(node.as_table()) == expected
    for tag, data in node.iter_data():
        assert tag["unit"] == data["unit"]


def test_the_file_states_the_operator_binding(tmp_path: Path) -> None:
    _saved_job(tmp_path, SyncPipelineOrchestrator)
    data = json.loads((tmp_path / "job.json").read_text())
    (descriptor,) = [
        d for d in data["pipeline"]["nodes"].values() if d.get("label") == "joined"
    ]
    schema = descriptor["output_schema"]
    assert list(schema["tag"]) == ["unit", "time"]
    assert "unit" in schema["data"] and "time" not in schema["data"]


def test_a_loaded_operator_node_gives_its_stored_rows_as_a_source(
    tmp_path: Path,
) -> None:
    job = _saved_job(tmp_path, SyncPipelineOrchestrator)
    loaded = PipelineJob.load(tmp_path / "job.json")
    node = loaded.nodes["joined"]
    records = node.get_all_records()
    assert records is not None and stored_references(records.schema) == ("unit",)
    source = node.as_source()
    assert source.keys() == JOINED_KEYS
    assert _rows(source.as_table()) == _rows(job.nodes["joined"].as_table())


def test_the_operator_table_is_checked_against_the_saved_job(tmp_path: Path) -> None:
    """The file and the store are independent: a loaded node refuses a disagreement."""
    _saved_job(tmp_path, SyncPipelineOrchestrator)
    store = DeltaTableDatabase(tmp_path / "store")
    tampered = InMemoryArrowDatabase()
    for path in store.list_table_paths():
        table = store.get_all_records(path, record_id_column="__copy_id")
        assert table is not None
        tampered.add_records(
            path, without_reference_marks(table), record_id_column="__copy_id"
        )
    tampered.flush()
    loaded = _loaded_replaying(tmp_path, database=tampered)
    with pytest.raises(InputValidationError, match=r"saved job declares \['unit'\]"):
        loaded.run(load_only=True)
    with pytest.raises(InputValidationError, match=r"saved job declares \['unit'\]"):
        _ = loaded.nodes["joined"].as_source().as_table()


# ---------------------------------------------------------------------------
# Derived sources in saved jobs
# ---------------------------------------------------------------------------


@ORCHESTRATORS
def test_a_job_over_a_derived_source_saves_and_loads_its_binding(
    tmp_path: Path, orchestrator: Orchestrator
) -> None:
    upstream = PipelineJob(name="up", database=InMemoryArrowDatabase())
    with upstream:
        double.pod(_csv(tmp_path, "spikes", "amp"), label="double")
    upstream.run(orchestrator=orchestrator())
    derived = upstream.nodes["double"].as_source()
    assert derived.keys() == (("unit", "time"), ("unit", "y"))

    job = PipelineJob(name="down", database=InMemoryArrowDatabase())
    with job:
        _ = derived.select_data_columns(["unit", "y"], label="kept")
    job.run(orchestrator=orchestrator())
    job.save(tmp_path / "down.json")

    data = json.loads((tmp_path / "down.json").read_text())
    (config,) = data["bindings"]["sources"].values()
    assert config["source_type"] == "derived"
    # No new key: the saved schema states the binding.
    assert "tag_only_columns" not in config
    assert "unit" in config["tag_schema"] and "unit" in config["data_schema"]

    loaded = PipelineJob.load(tmp_path / "down.json")
    for label, node in job.nodes.items():
        assert loaded.nodes[label].keys() == node.keys()
        assert loaded.nodes[label].pipeline_hash() == node.pipeline_hash()
    # A derived source over a job node is not rebuilt from its config, in plain mode
    # too, so only the node downstream of it keeps its run's content hash.
    assert loaded.nodes["kept"].content_hash() == job.nodes["kept"].content_hash()


def test_a_tags_as_data_source_over_a_referential_source_round_trips(
    tmp_path: Path,
) -> None:
    derived = DerivedSource(
        _csv(tmp_path, "spikes", "amp"), tags_as_data={"time": "t0"}
    )
    config = derived.to_config()
    # The origin's config states its plain tags under the 0.3.0 rule.
    assert config["origin"]["tag_only_columns"] == ["time"]
    rebuilt = DerivedSource.from_config(config)
    assert (
        rebuilt.keys()
        == derived.keys()
        == (
            ("unit", "time"),
            ("unit", "amp", "t0"),
        )
    )
    assert rebuilt.content_hash() == derived.content_hash()
    assert rebuilt.pipeline_hash() == derived.pipeline_hash()
    assert _rows(rebuilt.as_table()) == _rows(derived.as_table())


@ORCHESTRATORS
def test_a_job_over_a_tags_as_data_source_saves_and_loads(
    tmp_path: Path, orchestrator: Orchestrator
) -> None:
    derived = DerivedSource(
        _csv(tmp_path, "spikes", "amp"), tags_as_data={"time": "t0"}
    )
    job = PipelineJob(name="tad", database=DeltaTableDatabase(tmp_path / "store"))
    with job:
        double.pod(derived, label="double")
    job.run(orchestrator=orchestrator())
    assert job.nodes["double"].keys() == (("unit", "time"), ("unit", "y"))
    job.save(tmp_path / "tad.json")

    (config,) = json.loads((tmp_path / "tad.json").read_text())["bindings"][
        "sources"
    ].values()
    assert config["source_type"] == "derived"
    assert config["origin"]["tag_only_columns"] == ["time"]

    loaded = PipelineJob.load(tmp_path / "tad.json")
    assert _hashes(loaded) == _hashes(job)
    loaded.run(load_only=True, orchestrator=orchestrator())
    assert _rows(loaded.nodes["double"].as_table()) == _rows(
        job.nodes["double"].as_table()
    )
