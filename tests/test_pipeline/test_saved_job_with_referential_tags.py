"""Saving and loading a job or pipeline that holds referential tags (ITL-1019).

A file of format 0.3.0 states its bindings: every source config that takes the option
writes ``tag_only_columns``, and every function node writes ``fall_through_references``
(empty for a plain pod). Files at 0.1.0 and 0.2.0 hold only plain tags, decided by the
version, never by a missing key. The saved file is an independent point of verification:
a loaded node reads its stored table only when the table's marker agrees with the file.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import orcapod as op
from orcapod.core.function_pod import FunctionPod, ReferenceFallThroughPod
from orcapod.core.referential_tags import without_reference_marks
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    Pipeline,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.pipeline.serialization import (
    PLAIN_TAG_FORMAT_VERSIONS,
    source_config_at_version,
)

ORCHESTRATORS = pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)


@op.function_pod(output_keys=["y"])
def double(amp: float) -> float:
    """Double the amplitude; reads no tag, so ``unit`` falls through."""
    return 2 * amp


@op.function_pod(output_keys=["z"])
def shift(y: float) -> float:
    """A second step over the first one's output."""
    return y + 1


def _csv(tmp_path: Path) -> str:
    path = tmp_path / "spikes.csv"
    path.write_text("unit,time,amp\n1,10,0.5\n2,20,1.5\n")
    return str(path)


def _job(tmp_path: Path, tag_only: object = ("time",)) -> PipelineJob:
    """A CSV source (``unit`` referential unless ``tag_only`` says otherwise), two pods."""
    source = op.sources.CSVSource(
        _csv(tmp_path),
        tag_columns=["unit", "time"],
        tag_only_columns=tag_only,  # pyright: ignore[reportArgumentType]  # the test varies it
        source_id="spikes",
    )
    job = PipelineJob(database=DeltaTableDatabase(tmp_path / "store"))
    with job:
        doubled = double.pod(source, label="double")
        shift.pod(doubled, label="shift")
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


def _saved(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _function_descriptors(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        d["label"]: d
        for d in data["pipeline"]["nodes"].values()
        if d["node_type"] == "function"
    }


@ORCHESTRATORS
def test_a_referential_job_saves_and_loads_with_its_hashes_and_bindings(
    tmp_path: Path,
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
) -> None:
    job = _job(tmp_path)
    job.run(orchestrator=orchestrator())
    assert job.nodes["double"].keys() == (("unit", "time"), ("unit", "y"))
    rows = sorted(job.nodes["shift"].as_table().to_pylist(), key=lambda r: r["unit"])
    job.save(tmp_path / "job.json")

    loaded = PipelineJob.load(tmp_path / "job.json")
    assert _hashes(loaded) == _hashes(job)
    # A loaded job reads its stored rows; the binding comes back with them.
    loaded.run(load_only=True, orchestrator=orchestrator())
    table = loaded.nodes["shift"].as_table()
    assert table.column_names.count("unit") == 1
    assert sorted(table.to_pylist(), key=lambda r: r["unit"]) == rows
    records = loaded.nodes["double"].get_all_records()
    assert records is not None and sorted(records.column("y").to_pylist()) == [1.0, 3.0]


def test_the_file_states_every_binding(tmp_path: Path) -> None:
    job = _job(tmp_path)
    job.run()
    job.save(tmp_path / "job.json")
    data = _saved(tmp_path / "job.json")
    assert data["orcapod_pipeline_job_version"] == "0.3.0"
    assert data["bindings"]["sources"]["spikes"]["tag_only_columns"] == ["time"]
    descriptors = _function_descriptors(data)
    for label in ("double", "shift"):
        descriptor = descriptors[label]
        # Written for every function node, and the pod's config agrees.
        assert descriptor["fall_through_references"] == ["unit"]
        assert descriptor["function_config"]["fall_through_references"] == ["unit"]


def test_a_plain_job_states_its_plain_tags(tmp_path: Path) -> None:
    job = _job(tmp_path, tag_only=True)
    job.run()
    job.save(tmp_path / "job.json")
    data = _saved(tmp_path / "job.json")
    assert data["bindings"]["sources"]["spikes"]["tag_only_columns"] is True
    for descriptor in _function_descriptors(data).values():
        assert descriptor["fall_through_references"] == []
        assert "fall_through_references" not in descriptor["function_config"]
    loaded = PipelineJob.load(tmp_path / "job.json")
    assert _hashes(loaded) == _hashes(job)


def test_a_loaded_job_saved_again_keeps_its_bindings(tmp_path: Path) -> None:
    job = _job(tmp_path)
    job.run()
    job.save(tmp_path / "job.json")
    PipelineJob.load(tmp_path / "job.json").save(tmp_path / "again.json")
    for descriptor in _function_descriptors(_saved(tmp_path / "again.json")).values():
        assert descriptor["fall_through_references"] == ["unit"]
    assert _hashes(PipelineJob.load(tmp_path / "again.json")) == _hashes(job)


def test_a_pipeline_saves_and_loads_its_bindings(tmp_path: Path) -> None:
    job = _job(tmp_path)
    job.compiled_pipeline.save(tmp_path / "pipeline.json")
    loaded = Pipeline.load(tmp_path / "pipeline.json")
    for label in ("spikes", "double", "shift"):
        assert loaded.nodes[label].keys() == job.nodes[label].keys()
        assert (
            loaded.nodes[label].pipeline_hash().to_string()
            == job.nodes[label].pipeline_hash().to_string()
        )


def test_the_fall_through_pod_config_round_trips() -> None:
    plain = double.pod
    assert isinstance(plain, FunctionPod)
    pod = ReferenceFallThroughPod(plain, ["unit"])
    config = pod.to_config()
    assert config["fall_through_references"] == ["unit"]
    rebuilt = ReferenceFallThroughPod.from_config(config)
    assert rebuilt.references == ("unit",)
    assert rebuilt.pipeline_hash() == pod.pipeline_hash()
    del config["fall_through_references"]
    with pytest.raises(ValueError, match="fall_through_references"):
        ReferenceFallThroughPod.from_config(config)


# ---------------------------------------------------------------------------
# The format version decides; a missing or contradicting record is refused
# ---------------------------------------------------------------------------


def _edited(tmp_path: Path, edit: Any) -> Path:
    job = _job(tmp_path)
    job.run()
    job.save(tmp_path / "job.json")
    data = _saved(tmp_path / "job.json")
    edit(data)
    path = tmp_path / "edited.json"
    path.write_text(json.dumps(data))
    return path


def test_a_030_source_config_without_tag_only_columns_is_refused(
    tmp_path: Path,
) -> None:
    def edit(data: dict[str, Any]) -> None:
        del data["bindings"]["sources"]["spikes"]["tag_only_columns"]

    # The file is malformed, not a source that moved: the load fails.
    with pytest.raises(ValueError, match="source 'spikes'.*has no tag_only_columns"):
        PipelineJob.load(_edited(tmp_path, edit))


@pytest.mark.parametrize(
    "contradiction",
    [{"tag_only_columns": True}, {"tag_columns": [], "tag_only_columns": True}],
    ids=["all_plain", "no_tags"],
)
def test_a_source_config_that_contradicts_its_slot_is_refused(
    tmp_path: Path, contradiction: dict[str, Any]
) -> None:
    def edit(data: dict[str, Any]) -> None:
        # The slot declares ``unit`` referential; the config makes nothing referential.
        data["bindings"]["sources"]["spikes"].update(contradiction)

    path = _edited(tmp_path, edit)
    with pytest.raises(ValueError, match="source 'spikes'") as raised:
        PipelineJob.load(path)
    message = str(raised.value)
    assert str(path) in message
    assert "['unit']" in message and "[]" in message


def test_a_030_function_node_without_its_references_is_refused(tmp_path: Path) -> None:
    def edit(data: dict[str, Any]) -> None:
        for descriptor in _function_descriptors(data).values():
            del descriptor["fall_through_references"]

    path = _edited(tmp_path, edit)
    with pytest.raises(ValueError, match="has no fall_through_references") as raised:
        PipelineJob.load(path)
    # The error names the job file, not the temporary file of its embedded pipeline.
    assert str(raised.value).startswith(f"{path}: saved node")


def test_references_that_disagree_with_the_saved_schema_are_refused(
    tmp_path: Path,
) -> None:
    def edit(data: dict[str, Any]) -> None:
        _function_descriptors(data)["double"]["fall_through_references"] = []

    with pytest.raises(ValueError, match="saved output schema has the referential"):
        PipelineJob.load(_edited(tmp_path, edit))


@pytest.mark.parametrize("version", sorted(PLAIN_TAG_FORMAT_VERSIONS))
def test_an_older_file_holds_only_plain_tags(tmp_path: Path, version: str) -> None:
    def edit(data: dict[str, Any]) -> None:
        data["orcapod_pipeline_job_version"] = version
        data["pipeline"]["orcapod_pipeline_version"] = version

    # A referential schema in a file of a plain-tag version is not that version.
    with pytest.raises(ValueError, match=f"a file at format {version} holds only"):
        PipelineJob.load(_edited(tmp_path, edit))
    with pytest.raises(ValueError, match="names tag_only_columns"):
        source_config_at_version(
            {"source_type": "csv", "tag_only_columns": []}, version
        )
    assert source_config_at_version({"source_type": "csv"}, version) == {
        "source_type": "csv",
        "tag_only_columns": True,
    }


def test_the_table_is_checked_against_the_saved_job(tmp_path: Path) -> None:
    """The file and the store are independent: a loaded node refuses a disagreement."""
    job = _job(tmp_path)
    job.run()
    job.save(tmp_path / "job.json")
    store = DeltaTableDatabase(tmp_path / "store")
    # The same rows without the marker, as if another pipeline had written them.
    tampered = InMemoryArrowDatabase()
    for path in store.list_table_paths():
        table = store.get_all_records(path, record_id_column="__copy_id")
        assert table is not None
        tampered.add_records(
            path, without_reference_marks(table), record_id_column="__copy_id"
        )
    tampered.flush()
    loaded = PipelineJob.load(tmp_path / "job.json", database=tampered)
    with pytest.raises(InputValidationError, match="saved job declares \\['unit'\\]"):
        loaded.nodes["double"].get_all_records()


@pytest.mark.parametrize(
    ("tag_only", "saved_tag_only"),
    [((), []), (True, "unit")],
    ids=["referential", "plain"],
)
def test_a_source_config_with_a_bare_string_tag_column_loads(
    tmp_path: Path, tag_only: object, saved_tag_only: object
) -> None:
    # A source takes one tag name as a bare string; the binding check reads it as that
    # one name, never as its characters.
    source = op.sources.CSVSource(
        _csv(tmp_path),
        tag_columns=["unit"],
        tag_only_columns=tag_only,  # pyright: ignore[reportArgumentType]  # the test varies it
        source_id="spikes",
    )
    job = PipelineJob(database=DeltaTableDatabase(tmp_path / "store"))
    with job:
        double.pod(source, label="double")
    job.save(tmp_path / "job.json")
    data = _saved(tmp_path / "job.json")
    data["bindings"]["sources"]["spikes"].update(
        {"tag_columns": "unit", "tag_only_columns": saved_tag_only}
    )
    path = tmp_path / "edited.json"
    path.write_text(json.dumps(data))

    loaded = PipelineJob.load(path)
    assert _hashes(loaded) == _hashes(job)
    # The source was rebuilt from the edited config, with its bindings and rows.
    assert set(loaded.sources) == set(job.sources) == {"spikes"}
    rebuilt, original = loaded.sources["spikes"], job.sources["spikes"]
    assert rebuilt.keys() == original.keys()
    assert rebuilt.as_table().to_pylist() == original.as_table().to_pylist()


@pytest.mark.parametrize("version", sorted(PLAIN_TAG_FORMAT_VERSIONS))
def test_an_older_pipeline_with_a_referential_restored_source_schema_is_refused(
    tmp_path: Path, version: str
) -> None:
    # A source node is rebuilt from its source config's schemas, so those are what a
    # plain-tag version must hold plain, not only the descriptor's output schema.
    job = _job(tmp_path, tag_only=True)
    job.compiled_pipeline.save(tmp_path / "pipeline.json")
    data = _saved(tmp_path / "pipeline.json")
    data["orcapod_pipeline_version"] = version
    for descriptor in _function_descriptors({"pipeline": data}).values():
        del descriptor["fall_through_references"]
    plain_path = tmp_path / "plain.json"
    plain_path.write_text(json.dumps(data))
    Pipeline.load(plain_path)  # the untouched file loads

    for descriptor in data["nodes"].values():
        if descriptor["node_type"] == "source":
            descriptor["source_config"]["data_schema"]["unit"] = "int64"
    path = tmp_path / "edited.json"
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match=f"a file at format {version} holds only"):
        Pipeline.load(path)


@pytest.mark.parametrize("version", [*sorted(PLAIN_TAG_FORMAT_VERSIONS), "0.3.0"])
def test_a_mapping_inside_a_source_config_is_not_read_as_a_source(
    version: str,
) -> None:
    # A filter value may hold any key, ``source_type`` included; only the nested
    # source fields (a cached source's inner source, a derived source's origin) are
    # source configs.
    where = {"source_type": "delta_table"}
    tag_only = (
        {} if version in PLAIN_TAG_FORMAT_VERSIONS else {"tag_only_columns": True}
    )
    config = {"source_type": "csv", "where": dict(where), **tag_only}
    assert source_config_at_version(config, version)["where"] == where
    cached = {
        "source_type": "cached",
        "inner_source": {"source_type": "csv", **tag_only},
        "cache_database": {"source_type": "delta_table"},
    }
    upgraded = source_config_at_version(cached, version)
    assert upgraded["inner_source"]["tag_only_columns"] is True
    assert upgraded["cache_database"] == {"source_type": "delta_table"}
