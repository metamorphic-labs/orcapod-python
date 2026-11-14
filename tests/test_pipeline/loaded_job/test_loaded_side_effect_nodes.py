"""A loaded job restores its side-effect nodes, read-only (ITL-1094).

A saved job file records each side-effect node (``node_type`` ``"side_effect"``) but no
side-effect function and no pod configuration. ``PipelineJob.load`` and ``Pipeline.load``
restore the node with its saved label, content hash, pipeline hash, output schema and
position in the graph. It never delivers: a read returns the rows of a run in this process
(none), and every run raises. The nodes below it keep their saved identity.
"""

from __future__ import annotations

import asyncio
import json
import pathlib
from collections.abc import Callable
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline import Pipeline
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.side_effects import SideEffectPod, SideEffectPodConfig
from orcapod.sources import DictSource
from orcapod.streams import ViewStatus

ROWS = 3
SIDE_EFFECT_NODES = ("delivered", "tail")


def _source() -> DictSource:
    """Three rows: tag ``k``, data ``x``."""
    return DictSource(
        [{"k": i, "x": i} for i in range(ROWS)], tag_columns=["k"], source_id="s"
    )


def _saved_job(root: pathlib.Path) -> tuple[PipelineJob, list[int], pathlib.Path]:
    """Run and save ``s -> delivered -> down -> tail``; return the job, deliveries, file.

    ``delivered`` (a side-effect node in the middle, ``track_completion`` and
    ``on_error="log"``) passes ``x`` on to the function node ``down`` (``y = x + 100``),
    and ``tail`` (a side-effect node at the end) receives ``y``.
    """
    sent: list[int] = []

    def deliver(ctx: Any, x: int) -> None:
        """Record ``x``."""
        sent.append(x)

    def notify(ctx: Any, y: int) -> None:
        """Record ``y``."""
        sent.append(y)

    @function_pod(output_keys="y")
    def plus(x: int) -> int:
        """``x + 100``."""
        return x + 100

    job = PipelineJob(name="se", database=DeltaTableDatabase(root / "store"))
    middle = SideEffectPod(
        deliver, config=SideEffectPodConfig(track_completion=True, on_error="log")
    )
    end = SideEffectPod(notify, config=SideEffectPodConfig(track_completion=False))
    with job:
        delivered = middle(_source(), label="delivered")
        down = plus.pod(delivered, label="down")
        _ = end(down, label="tail")
    _ = job.run()
    assert len(sent) == 2 * ROWS
    path = root / "job.json"
    job.save(path)
    return job, sent, path


@pytest.fixture
def saved(tmp_path: pathlib.Path) -> tuple[PipelineJob, list[int], pathlib.Path]:
    """A job with two side-effect nodes, run and saved."""
    return _saved_job(tmp_path)


def _descriptors(path: pathlib.Path) -> dict[str, dict[str, Any]]:
    """The node entries of the job file at ``path``, by label."""
    nodes = json.loads(path.read_text())["pipeline"]["nodes"]
    return {descriptor["label"]: descriptor for descriptor in nodes.values()}


def _reads(node: Any) -> dict[str, Callable[[], Any]]:
    """Every public read of ``node``, by name."""

    async def collect() -> list[Any]:
        return [row async for row in node.async_iter_data()]

    return {
        "head": lambda: node.head(100),
        "preview": node._repr_html_,
        "iter_data": lambda: list(node.iter_data()),
        "__iter__": lambda: list(node),
        "async_iter_data": lambda: asyncio.run(collect()),
        "as_table": node.as_table,
        "as_polars_df": node.as_polars_df,
        "as_df": node.as_df,
        "as_lazy_frame": lambda: node.as_lazy_frame().collect(),
        "as_pandas_df": node.as_pandas_df,
        "flow": node.flow,
    }


def test_a_loaded_job_lists_its_side_effect_nodes(
    saved: tuple[PipelineJob, list[int], pathlib.Path],
) -> None:
    """The node in the middle and the node at the end are both restored."""
    job, _, path = saved
    loaded = PipelineJob.load(path)
    assert set(loaded.nodes) == set(job.nodes) == {"s", "delivered", "down", "tail"}
    for label in SIDE_EFFECT_NODES:
        assert loaded.nodes[label].label == label


def test_a_restored_side_effect_node_has_its_saved_identity_and_position(
    saved: tuple[PipelineJob, list[int], pathlib.Path],
) -> None:
    """Saved content hash and pipeline hash; upstream is the restored node above it."""
    job, _, path = saved
    descriptors = _descriptors(path)
    loaded = PipelineJob.load(path)
    for label in SIDE_EFFECT_NODES:
        node = loaded.nodes[label]
        assert node.content_hash().to_string() == descriptors[label]["content_hash"]
        # The pipeline hash names the invocation log: the run's, unchanged.
        assert node.pipeline_hash() == job.nodes[label].pipeline_hash()
        assert node.output_schema() == job.nodes[label].output_schema()
        assert node.keys() == job.nodes[label].keys()
    assert loaded.nodes["delivered"].upstreams == (loaded.nodes["s"],)
    assert loaded.nodes["tail"].upstreams == (loaded.nodes["down"],)


def test_the_node_below_keeps_its_identity_and_its_upstream(
    saved: tuple[PipelineJob, list[int], pathlib.Path],
) -> None:
    """``down`` hangs below the restored ``delivered`` with the run's hashes."""
    job, _, path = saved
    loaded = PipelineJob.load(path)
    down = loaded.nodes["down"]
    assert down.upstreams == (loaded.nodes["delivered"],)
    assert down.content_hash() == job.nodes["down"].content_hash()
    assert down.pipeline_hash() == job.nodes["down"].pipeline_hash()


def test_no_read_or_run_of_a_loaded_job_delivers(
    saved: tuple[PipelineJob, list[int], pathlib.Path],
) -> None:
    """Reads return no rows, runs raise, and nothing is delivered."""
    _, sent, path = saved
    delivered_before = list(sent)
    loaded = PipelineJob.load(path)
    for label in SIDE_EFFECT_NODES:
        node = loaded.nodes[label]
        for name, read in _reads(node).items():
            _ = read()
            assert sent == delivered_before, f"{label}.{name} delivered"
        assert list(node.iter_data()) == []
        assert node.as_table().num_rows == 0
        assert node.head(100).status is ViewStatus.NOT_COMPUTED
        with pytest.raises(RuntimeError, match="cannot deliver"):
            _ = node.run()
    with pytest.raises(RuntimeError):
        _ = loaded.run()
    with pytest.raises(RuntimeError):
        _ = loaded.run(orchestrator=AsyncPipelineOrchestrator())
    assert sent == delivered_before


def test_saving_a_loaded_job_keeps_its_side_effect_nodes(
    saved: tuple[PipelineJob, list[int], pathlib.Path], tmp_path: pathlib.Path
) -> None:
    """Their entries and every edge are written back unchanged; loading again keeps them."""
    _, _, path = saved
    again = tmp_path / "again.json"
    PipelineJob.load(path).save(again)
    first = json.loads(path.read_text())["pipeline"]
    second = json.loads(again.read_text())["pipeline"]
    for key, descriptor in first["nodes"].items():
        if descriptor["node_type"] == "side_effect":
            assert second["nodes"][key] == descriptor
    assert set(second["nodes"]) == set(first["nodes"])
    assert sorted(map(tuple, second["edges"])) == sorted(map(tuple, first["edges"]))
    assert set(PipelineJob.load(again).nodes) == {"s", "delivered", "down", "tail"}


def test_pipeline_load_restores_side_effect_nodes(
    saved: tuple[PipelineJob, list[int], pathlib.Path], tmp_path: pathlib.Path
) -> None:
    """The pipeline blueprint lists them too, with the saved identity."""
    job, sent, _ = saved
    delivered_before = list(sent)
    blueprint = tmp_path / "pipeline.json"
    job.pipeline.save(blueprint)
    saved_nodes = json.loads(blueprint.read_text())["nodes"]
    by_label = {descriptor["label"]: descriptor for descriptor in saved_nodes.values()}
    loaded = Pipeline.load(blueprint)
    assert set(loaded.nodes) == {"s", "delivered", "down", "tail"}
    for label in SIDE_EFFECT_NODES:
        node = loaded.nodes[label]
        assert node.content_hash().to_string() == by_label[label]["content_hash"]
        assert node.pipeline_hash().to_string() == by_label[label]["pipeline_hash"]
        with pytest.raises(RuntimeError, match="cannot deliver"):
            _ = node.as_table()
    assert sent == delivered_before


def test_a_file_without_recorded_nullability_restores_side_effect_nodes(
    saved: tuple[PipelineJob, list[int], pathlib.Path], tmp_path: pathlib.Path
) -> None:
    """A job file of format 0.1.0 (v0.2: no ``nullable`` record) restores them too."""
    _, _, path = saved
    data = json.loads(path.read_text())
    data["orcapod_pipeline_job_version"] = "0.1.0"
    data["pipeline"]["orcapod_pipeline_version"] = "0.1.0"
    for descriptor in data["pipeline"]["nodes"].values():
        del descriptor["output_schema"]["nullable"]
        descriptor.get("source_config", {}).pop("nullable", None)
    old = tmp_path / "format_0_1_0.json"
    old.write_text(json.dumps(data))

    loaded = PipelineJob.load(old)
    assert set(loaded.nodes) == {"s", "delivered", "down", "tail"}
    assert loaded.nodes["delivered"].keys() == (("k",), ("x",))
    # Saving it again keeps the nullability unrecorded, as for every other node.
    again = tmp_path / "again.json"
    loaded.save(again)
    for descriptor in _descriptors(again).values():
        assert "nullable" not in descriptor["output_schema"]
