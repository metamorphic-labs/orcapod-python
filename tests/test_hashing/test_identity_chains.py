"""Three-chain identities are available alongside the legacy hashes."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from orcapod import Pipeline, PipelineJob, SourceNode, function_pod
from orcapod.core.identity_chains import NAME_CHAIN_HASH_LABEL, SOURCE_CHAIN_HASH_LABEL
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import UnboundSourceError
from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
from orcapod.operators import Index, Join, MergeJoin, Rejoin
from orcapod.sources import DictSource
from orcapod.types import ContentHash

if TYPE_CHECKING:
    from pathlib import Path

    from orcapod.protocols.hashing_protocols import IdentityChainProtocol


@function_pod(output_keys="total")
def add(x: int, y: int) -> int:
    """Add two data columns."""
    return x + y


@function_pod(output_keys="doubled")
def double(total: int) -> int:
    """Double the previous step's output."""
    return 2 * total


def _source(
    name: str, reverse: bool, *, shift: int = 0, right: bool = False
) -> DictSource:
    """Build a two-tag, two-data source in either column order."""
    x, y = ("u", "v") if right else ("x", "y")
    rows = [
        {"region": "north", "item": 1, x: 5 + shift, y: 7},
        {"region": "south", "item": 2, x: 11 + shift, y: 13},
    ]
    tags = ["region", "item"]
    if reverse:
        rows = [dict(reversed(tuple(row.items()))) for row in rows]
        tags.reverse()
    return DictSource(rows, tag_columns=tags, tag_only_columns=True, source_id=name)


def _hashes(element: IdentityChainProtocol) -> tuple[object, object]:
    """Read both combined hashes before the content hash switch."""
    return element._chain_content_hash(), element.node_hash()


def _run(job: PipelineJob, run_async: bool) -> None:
    """Execute the job on the requested path."""
    if run_async:
        asyncio.run(job.run_async())
    else:
        job.run()


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("run_async", [False, True])
def test_chain_agrees_across_plain_recorded_and_job_forms(
    reverse: bool, run_async: bool
) -> None:
    source = _source("reading", reverse)
    first = add.pod(source)
    plain = double.pod(first)
    blueprint = Pipeline(name="chain")
    with blueprint:
        double.pod(add.pod(source, label="first"), label="last")
    job = PipelineJob.from_pipeline(
        blueprint,
        database=InMemoryArrowDatabase(),
        sources={"reading": source},
    )
    _run(job, run_async)
    # to_invocations() returns the compiled blueprint with *unbound* slots.
    # Inspect the actual recording, whose upstreams are the original streams.
    recorded = next(
        inv for inv in blueprint._invocation_lut.values() if inv.label == "last"
    )
    node = job.nodes["last"]
    assert plain.pipeline_hash() == recorded.pipeline_hash() == node.pipeline_hash()
    assert _hashes(plain) == _hashes(recorded) == _hashes(node)
    assert plain.source_chain() == first.source_chain() == source.content_hash()
    assert plain.name_chain() == first.name_chain() == source.name_chain()
    assert node.source_chain() == job.nodes["first"].source_chain()
    assert node.name_chain() == job.nodes["first"].name_chain()
    assert plain.node_hash() != first.node_hash()
    assert plain._chain_content_hash() != first._chain_content_hash()
    assert _hashes(double.pod(first, label="another label")) == _hashes(plain)


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("run_async", [False, True])
def test_diamond_pools_both_occurrences(reverse: bool, run_async: bool) -> None:
    source = _source("diamond", reverse)
    x = source.select_data_columns(["x"])
    y = source.select_data_columns(["y"])
    diamond = Rejoin()(x, y)
    assert diamond.source_chain() != source.source_chain()
    assert diamond.name_chain() != source.name_chain()
    assert _hashes(diamond) == _hashes(Rejoin()(y, x))

    job = PipelineJob(name="diamond", database=InMemoryArrowDatabase())
    with job:
        left = source.select_data_columns(["x"], label="left")
        right = source.select_data_columns(["y"], label="right")
        Rejoin()(left, right, label="diamond_result")
    _run(job, run_async)
    assert _hashes(job.nodes["diamond_result"]) == _hashes(diamond)


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("run_async", [False, True])
def test_slots_keep_their_names_across_bindings(reverse: bool, run_async: bool) -> None:
    source = _source("reading-one", reverse)
    other = _source("reading-two", reverse, shift=100)
    x = SourceNode.from_stream(source, name="x_slot")
    y = SourceNode.from_stream(source, name="y_slot")
    blueprint = Pipeline(name="two_slots")
    with blueprint:
        double.pod(add.pod(x, label="x_first"), label="x_last")
        double.pod(add.pod(y, label="y_first"), label="y_last")

    invocations = blueprint.to_invocations().invocations
    assert {inv.label for inv in invocations} == {
        "x_first",
        "y_first",
        "x_last",
        "y_last",
    }
    for invocation in invocations:
        assert invocation.label is not None
        assert invocation.source_chain() is None
        assert invocation.node_hash() == blueprint.nodes[invocation.label].node_hash()

    job = PipelineJob.from_pipeline(
        blueprint,
        database=InMemoryArrowDatabase(),
        sources={"x_slot": source, "y_slot": other},
    )
    _run(job, run_async)
    x_node, y_node = job.nodes["x_last"], job.nodes["y_last"]
    structure = x_node.pipeline_hash()
    x_content, x_name = _hashes(x_node)
    y_content, y_name = _hashes(y_node)
    assert structure == y_node.pipeline_hash()
    assert x_content != y_content
    assert x_name != y_name
    assert x_name == blueprint.nodes["x_last"].node_hash()
    assert y_name == blueprint.nodes["y_last"].node_hash()

    # Step 1 still uses legacy keys. Compare the two chains on the extant
    # nodes after the bind, without relying on execution of content-equal nodes.
    job.bind(sources={"y_slot": source})
    assert x_node.pipeline_hash() == y_node.pipeline_hash() == structure
    assert _hashes(x_node) == (x_content, x_name)
    assert _hashes(y_node) == (x_content, y_name)
    assert (
        job.nodes["x_first"]._chain_content_hash()
        == job.nodes["y_first"]._chain_content_hash()
    )


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("run_async", [False, True])
def test_ordered_and_commutative_operators(reverse: bool, run_async: bool) -> None:
    left = _source("left", reverse)
    right = _source("right", reverse, right=True)
    assert left.pipeline_hash() != right.pipeline_hash()
    ordered = MergeJoin()(left, right)
    swapped = MergeJoin()(right, left)
    assert ordered.source_chain() != swapped.source_chain()
    assert ordered.name_chain() != swapped.name_chain()
    assert ordered._chain_content_hash() != swapped._chain_content_hash()
    assert ordered.node_hash() != swapped.node_hash()

    commutative = Join()(left, right)
    assert _hashes(commutative) == _hashes(Join()(right, left))
    assert commutative.source_chain() == Join()(right, left).source_chain()
    assert commutative.name_chain() == Join()(right, left).name_chain()

    job = PipelineJob(name="join_order", database=InMemoryArrowDatabase())
    with job:
        MergeJoin()(left, right, label="ordered")
        MergeJoin()(right, left, label="swapped")
        Join()(right, left, label="commutative")
    _run(job, run_async)
    assert _hashes(job.nodes["ordered"]) == _hashes(ordered)
    assert _hashes(job.nodes["swapped"]) == _hashes(swapped)
    assert _hashes(job.nodes["commutative"]) == _hashes(commutative)


def test_unbound_blueprint_names_all_upstream_slots() -> None:
    source = _source("reading", False)
    left = SourceNode.from_stream(source, name="left_slot")
    right = SourceNode.from_stream(source, name="right_slot")
    pipeline = Pipeline(name="unbound")
    with pipeline:
        MergeJoin()(left, right, label="result")
    invocation = pipeline.to_invocations().invocations[0]
    result = pipeline.nodes["result"]
    assert result.pipeline_hash() == invocation.pipeline_hash()
    assert result.node_hash() == invocation.node_hash()
    for element in (invocation, result):
        assert element.source_chain() is None
        with pytest.raises(UnboundSourceError, match="unbound.*left_slot.*right_slot"):
            element._chain_content_hash()


@pytest.mark.filterwarnings("ignore:Index is provisional")
def test_loaded_blueprint_computes_names_through_a_missing_pod(tmp_path: Path) -> None:
    source = _source("reading", False)
    lists = DictSource(
        [{"item": 1, "region": "north", "values": [1, 2], "y": 3}],
        tag_columns=["item", "region"],
        tag_only_columns=True,
        source_id="lists",
    )
    pipeline = Pipeline(name="saved")
    with pipeline:
        add.pod(source, label="first")
        Index(column="values", i=0)(lists, label="picked")
    expected = {label: node.node_hash() for label, node in pipeline.nodes.items()}
    path = tmp_path / "blueprint.json"
    pipeline.save(path)
    loaded = Pipeline.load(path)
    for label, slot in (("first", "reading"), ("picked", "lists")):
        node = loaded.nodes[label]
        assert node.producer is None
        assert node.node_hash() == expected[label]
        with pytest.raises(UnboundSourceError, match=f"unbound.*{slot}"):
            node._chain_content_hash()


def _data_tags(name: str, reverse: bool = False) -> DictSource:
    """A source on which two inputs with identical structure can be joined."""
    tags = ["region", "item", "x", "y"]
    rows = [
        {"region": "north", "item": 1, "x": 3, "y": 5},
        {"region": "south", "item": 2, "x": 7, "y": 11},
    ]
    if reverse:
        tags.reverse()
        rows = [dict(reversed(tuple(row.items()))) for row in rows]
    return DictSource(
        rows,
        tag_columns=tags,
        tag_only_columns=["region", "item"],
        source_id=name,
    )


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("run_async", [False, True])
def test_commutative_equal_structure_sorts_the_chains_independently(
    reverse: bool, run_async: bool
) -> None:
    left, right = _data_tags("a", reverse), _data_tags("b", reverse)
    assert left.pipeline_hash() == right.pipeline_hash()
    result = Join()(left, right)
    swapped = Join()(right, left)
    left_source, right_source = left.source_chain(), right.source_chain()
    assert left_source is not None and right_source is not None
    sources = tuple(sorted((left_source, right_source), key=ContentHash.to_string))
    names = tuple(
        sorted((left.name_chain(), right.name_chain()), key=ContentHash.to_string)
    )
    assert (
        result.source_chain()
        == swapped.source_chain()
        == SemanticAwarePythonHasher(hasher_id=SOURCE_CHAIN_HASH_LABEL).hash_object(
            sources
        )
    )
    assert (
        result.name_chain()
        == swapped.name_chain()
        == SemanticAwarePythonHasher(hasher_id=NAME_CHAIN_HASH_LABEL).hash_object(names)
    )
    assert _hashes(result) == _hashes(swapped)
    repeat = Join()(left, left)
    assert repeat.source_chain() != left.source_chain()
    assert repeat.name_chain() != left.name_chain()

    job = PipelineJob(name="tie", database=InMemoryArrowDatabase())
    with job:
        Join()(right, left, label="joined")
    _run(job, run_async)
    assert _hashes(job.nodes["joined"]) == _hashes(result)


@pytest.mark.xfail(strict=True, reason="ITL-1590: legacy P drops content-equal inputs")
def test_equal_structure_join_pipeline_hash_depends_only_on_structure() -> None:
    left, right = _data_tags("a"), _data_tags("b")
    assert left.pipeline_hash() == right.pipeline_hash()
    assert Join()(left, left).pipeline_hash() == Join()(left, right).pipeline_hash()
