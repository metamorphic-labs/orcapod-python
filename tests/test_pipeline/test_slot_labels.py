"""Public slot names are captured labels rather than dataset IDs."""

from pathlib import Path

import pytest

from orcapod import Pipeline, PipelineJob, function_pod
from orcapod.core.nodes.source_node import SourceNode
from orcapod.errors import InconsistentSourceError, SourceSlotNameCollisionError
from orcapod.sources import DictSource


@function_pod(output_keys="output")
def _increment(value: int) -> int:
    return value + 1


def _source(source_id: str, label: str | None = None) -> DictSource:
    return DictSource(
        [{"key": "row", "value": 1}],
        tag_columns=["key"],
        tag_only_columns=True,
        source_id=source_id,
        label=label,
    )


def test_a_raw_source_uses_its_label_at_capture() -> None:
    source = _source("data", "before")
    job = PipelineJob(name="job", auto_compile=False)
    with job:
        _increment.pod(source, label="output")
    source.label = "after"
    job.compile()
    assert job.sources == {"before": source}
    assert job.nodes["before"].name == job.nodes["before"].label == "before"


def test_an_unlabelled_source_uses_its_class_name() -> None:
    source = _source("data")
    job = PipelineJob(name="job")
    with job:
        _increment.pod(source, label="output")
    assert job.sources == {"DictSource": source}
    assert job.nodes["DictSource"].content_hash() == source.content_hash()


def test_a_declaration_can_supply_two_independent_jobs() -> None:
    source = _source("data", "source_label")
    declaration = source.as_slot()
    assert isinstance(declaration, SourceNode)
    assert declaration.label == "source_label"
    declaration.label = "input"
    first = PipelineJob(name="first")
    second = PipelineJob(name="second")
    for job in (first, second):
        with job:
            _increment.pod(declaration, label="output")
    assert first.nodes["input"] is not second.nodes["input"]
    assert first.sources["input"] is second.sources["input"] is source
    new_source = _source("replacement")
    first.bind(sources={"input": new_source})
    assert first.sources["input"] is new_source
    assert second.sources["input"] is source
    assert source.label == "source_label"
    assert not hasattr(declaration, "bound_source")


def test_a_blueprint_drops_the_declarations_initial_source() -> None:
    source = _source("data")
    pipeline = Pipeline(name="blueprint")
    with pipeline:
        _increment.pod(source.as_slot("input"), label="output")
    job = PipelineJob.from_pipeline(pipeline)
    assert job.unbound_sources == ["input"]
    assert job.sources == {}
    assert job.nodes["input"].pipeline_hash() == pipeline.nodes["input"].pipeline_hash()
    job.bind(sources={"input": source})
    assert job.sources["input"] is source


@pytest.mark.parametrize("reverse", [False, True])
def test_same_class_defaults_collide(reverse: bool) -> None:
    sources = [_source("first_data"), _source("second_data")]
    if reverse:
        sources.reverse()
    job = PipelineJob(name="job")
    with pytest.raises(SourceSlotNameCollisionError, match="as_slot") as caught:
        with job:
            _increment.pod(sources[0], label="first")
            _increment.pod(sources[1], label="second")
    assert "dict_v1:first_data" in str(caught.value)
    assert "dict_v1:second_data" in str(caught.value)


def test_two_roles_for_one_source_are_refused_during_capture() -> None:
    source = _source("data", "primary")
    job = PipelineJob(name="job")
    with job:
        _increment.pod(source, label="first")
        with pytest.raises(InconsistentSourceError, match="ITL-1559"):
            _increment.pod(source.as_slot("secondary"), label="second")
    assert set(job.nodes) == {"first", "primary"}


def test_bind_refuses_one_source_under_two_roles_atomically() -> None:
    one, two = _source("one", "left"), _source("two", "right")
    job = PipelineJob(name="job")
    with job:
        _increment.pod(one, label="first")
        _increment.pod(two, label="second")
    with pytest.raises(InconsistentSourceError, match="ITL-1559"):
        job.bind(sources={"right": one})
    assert job.sources == {"left": one, "right": two}


def test_graph_owned_slot_name_cannot_change() -> None:
    job = PipelineJob(name="job")
    with job:
        _increment.pod(_source("data", "input"), label="output")
    with pytest.raises(ValueError, match="ITL-1559"):
        job.nodes["input"].label = "changed"
    assert set(job.nodes) == {"input", "output"}


def test_a_slot_reserves_its_label_against_implicit_step_names() -> None:
    role = _increment.pod.label
    source = _source("data", role)
    job = PipelineJob(name="job")
    with job:
        _increment.pod(source)
    assert job.nodes[role].node_type == "source"
    assert job.nodes[f"{role}_1"].node_type == "function"


def test_an_explicit_step_label_cannot_take_the_slot_name() -> None:
    job = PipelineJob(name="job")
    with pytest.raises(SourceSlotNameCollisionError, match="explicit"):
        with job:
            _increment.pod(_source("data", "input"), label="input")


def test_an_empty_explicit_slot_name_is_refused() -> None:
    with pytest.raises(ValueError, match="empty"):
        _source("data").as_slot("")


def test_displays_show_the_role_and_dataset_separately() -> None:
    source = _source("data", "object_label")
    job = PipelineJob(name="job")
    with job:
        _increment.pod(source.as_slot("input"), label="output")
    slot = job.nodes["input"]
    diagram = next(
        node for node in job.diagram().to_dict()["nodes"] if node["label"] == "input"
    )
    assert diagram["facts"] == [["source id", "dict_v1:data"]]
    assert "object_label" not in str(job.diagram())
    assert slot.head(1).header().startswith("SourceJobNode[input (")
    assert "dict_v1:data" in slot.head(1).header()
    assert (
        "slot 'input' (DictSource, source id: 'dict_v1:data')"
        in job.nodes["output"].explain().text()
    )


def test_loaded_source_slot_keeps_its_saved_name(tmp_path: Path) -> None:
    source = _source("old_dataset", "different_current_label")
    job = PipelineJob(name="job")
    with job:
        _increment.pod(source.as_slot("old_dataset"), label="output")
    saved = tmp_path / "job.json"
    job.save(saved)
    reloaded = PipelineJob.load(saved)
    assert reloaded.unbound_sources == ["old_dataset"]
    assert reloaded.nodes["old_dataset"].label == "old_dataset"
    with pytest.raises(ValueError, match="ITL-1559"):
        reloaded.nodes["old_dataset"].label = "changed"
    reloaded.bind(sources={"old_dataset": source})
    assert reloaded.sources == {"old_dataset": source}


@pytest.mark.parametrize("field", ["label", "node_uri", "duplicate"])
def test_load_refuses_conflicting_source_descriptors(
    tmp_path: Path, field: str
) -> None:
    import copy
    import json

    job = PipelineJob(name="job")
    with job:
        _increment.pod(_source("data", "input"), label="output")
    saved = tmp_path / "job.json"
    job.save(saved)
    data = json.loads(saved.read_text())
    nodes = data["pipeline"]["nodes"]
    descriptor = next(node for node in nodes.values() if node["node_type"] == "source")
    if field == "duplicate":
        nodes["another hash"] = copy.deepcopy(descriptor)
    elif field == "node_uri":
        descriptor["node_uri"] = ["source_node", "other"]
    else:
        descriptor["label"] = "other"
    saved.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="source slot|Source slot"):
        PipelineJob.load(saved)


@pytest.mark.parametrize("pipeline_type", [Pipeline, PipelineJob])
def test_a_prebuilt_step_cannot_be_a_slot(
    pipeline_type: type[Pipeline] | type[PipelineJob],
) -> None:
    source = _source("data", "input")
    outside = source.select_data_columns(["value"])
    pipeline = pipeline_type(name="job")
    with pytest.raises(ValueError, match=r"node\.as_source\(\).*ITL-1559"):
        with pipeline:
            _increment.pod(outside, label="output")


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("mode", ["sync", "async"])
def test_several_slots_round_trip_in_operator_argument_order(
    tmp_path: Path, reverse: bool, mode: str
) -> None:
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.operators import MergeJoin

    def source(value: int, role: str) -> DictSource:
        return DictSource(
            [{"key": "row", "value": value}],
            tag_columns=["key"],
            tag_only_columns=True,
            source_id=f"dataset_{role}",
        )

    bindings = [("left", source(4, "left")), ("right", source(7, "right"))]
    if reverse:
        bindings.reverse()
    declarations = {role: stream.as_slot(role) for role, stream in bindings}
    blueprint = Pipeline(name="ordered")
    with blueprint:
        steps = {
            role: _increment.pod(slot, label=f"{role}_value")
            for role, slot in declarations.items()
        }
        MergeJoin()(steps["left"], steps["right"], label="paired")
    db = InMemoryArrowDatabase()
    job = PipelineJob.from_pipeline(blueprint, database=db)
    job.bind(sources=dict(bindings))
    if mode == "sync":
        job.run()
    else:
        import asyncio

        asyncio.run(job.run_async())
    assert job.nodes["paired"].as_table()["output"].to_pylist() == [[5, 8]]
    saved = tmp_path / "job.json"
    job.save(saved)
    loaded = PipelineJob.load(saved, database=db)
    assert set(loaded.unbound_sources) == {"left", "right"}
    upstreams = loaded.nodes["paired"].upstreams
    assert [step.label for step in upstreams] == ["left_value", "right_value"]
    assert [step.upstreams[0].label for step in upstreams] == ["left", "right"]
    loaded.bind(sources=dict(bindings))
    assert loaded.sources == dict(bindings)
