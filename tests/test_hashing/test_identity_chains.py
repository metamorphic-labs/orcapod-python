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

    from collections.abc import Collection

    from orcapod.protocols.core_protocols import ArgumentGroup, StreamProtocol
    from orcapod.protocols.hashing_protocols import (
        IdentityChainProtocol,
        IdentityChains,
        IdentityWalkProtocol,
    )


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
    expected_sources = SemanticAwarePythonHasher(
        hasher_id=SOURCE_CHAIN_HASH_LABEL
    ).hash_object((left.source_chain(), left.source_chain()))
    expected_names = SemanticAwarePythonHasher(
        hasher_id=NAME_CHAIN_HASH_LABEL
    ).hash_object((left.name_chain(), left.name_chain()))
    assert repeat.source_chain() == expected_sources
    assert repeat.name_chain() == expected_names

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


@pytest.mark.parametrize("missing_builtin", [False, True])
def test_loaded_commutative_stub_keeps_chains(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, missing_builtin: bool
) -> None:
    """A missing producer must still pool its inputs in symmetric order."""
    from orcapod.pipeline.serialization import OPERATOR_REGISTRY
    from orcapod.sources import CSVSource

    class UnregisteredJoin(Join):
        """An operator whose config has no registered loader."""

    inputs = []
    for name, value in (("a", "x"), ("b", "y")):
        path = tmp_path / f"{name}.csv"
        path.write_text(f"k,{value}\n1,3\n")
        inputs.append(CSVSource(str(path), tag_columns=["k"], source_id=name))
    inputs.sort(key=lambda source: source.pipeline_hash().to_hex(), reverse=True)
    assert inputs[0].pipeline_hash() != inputs[1].pipeline_hash()
    job = PipelineJob(name="saved_symmetry", database=InMemoryArrowDatabase())
    with job:
        (Join() if missing_builtin else UnregisteredJoin())(*inputs, label="joined")
    job.run()
    expected = _hashes(job.nodes["joined"])
    path = tmp_path / "job.json"
    job.save(path)
    if missing_builtin:
        monkeypatch.delitem(OPERATOR_REGISTRY, "Join")
    loaded = PipelineJob.load(path, database=job.database)
    assert loaded.nodes["joined"].producer is None
    assert _hashes(loaded.nodes["joined"]) == expected
    loaded.save(path)
    assert (
        _hashes(PipelineJob.load(path, database=job.database).nodes["joined"])
        == expected
    )


@pytest.mark.parametrize("downstream", [False, True])
def test_raw_stream_label_never_enters_chains(downstream: bool) -> None:
    """Raw inputs and their projections ignore labels."""
    import pyarrow as pa

    from orcapod.core.streams.arrow_table_stream import ArrowTableStream

    table = pa.table({"k": [1], "x": [3]})
    before = ArrowTableStream(table, tag_columns=["k"], label="before")
    after = ArrowTableStream(table, tag_columns=["k"], label="after")
    left = before.select_data_columns(["x"]) if downstream else before
    right = after.select_data_columns(["x"]) if downstream else after
    assert left.name_chain() == right.name_chain()
    assert _hashes(left) == _hashes(right)


def test_chain_hashes_are_memoized_through_a_long_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reads reuse pooled hashes and rebind invalidates only source hashes."""
    from orcapod.core import identity_chains

    source = _source("reading", False)
    job = PipelineJob(name="long_chain", database=InMemoryArrowDatabase())
    with job:
        plain = source
        for index in range(50):
            plain = plain.select_data_columns(["x"], label=f"project_{index}")
    result = job.nodes["project_49"]
    hashes: list[str] = []
    visits: list[IdentityChainProtocol] = []
    original_hash = identity_chains._hash  # pyright: ignore[reportPrivateUsage]  # Count hash computations.
    original_visit = identity_chains.IdentityWalk.visit

    def count_hash(label: str, preimage: tuple[object, ...]) -> ContentHash:
        hashes.append(label)
        return original_hash(label, preimage)

    def count_visit(
        walk: identity_chains.IdentityWalk, element: IdentityChainProtocol
    ) -> identity_chains.IdentityChains:
        visits.append(element)
        return original_visit(walk, element)

    monkeypatch.setattr(identity_chains, "_hash", count_hash)
    monkeypatch.setattr(identity_chains.IdentityWalk, "visit", count_visit)
    before = _hashes(result)
    assert len({id(element) for element in visits}) == 51
    assert hashes
    hashes.clear()
    visits.clear()
    assert _hashes(result) == before
    assert hashes == []
    assert visits == [result, result]
    visits.clear()
    job.bind(sources={"reading": _source("rebound", False)})
    after = _hashes(result)
    assert after[0] != before[0]
    assert after[1] == before[1]
    assert hashes == [identity_chains.CONTENT_HASH_LABEL]
    assert len({id(element) for element in visits}) == 51
    hashes.clear()
    assert _hashes(result) == after
    assert hashes == []


@pytest.mark.parametrize("inner_job", [False, True])
def test_unbound_slot_binding_has_no_source_chain(inner_job: bool) -> None:
    """A binding cannot turn an unbound slot into a concrete source identity."""
    source = _source("reading", False)
    outer = PipelineJob(name="outer", database=InMemoryArrowDatabase())
    with outer:
        add.pod(source, label="result")
    inner = PipelineJob(name="inner", database=InMemoryArrowDatabase())
    if inner_job:
        with inner:
            add.pod(source, label="unused")
        from orcapod.core.nodes.source_node import SourceJobNode

        slot = inner.nodes["reading"]
        assert isinstance(slot, SourceJobNode)
        slot.bound_source = None
    else:
        slot = SourceNode.from_stream(source, name="inner_slot")
    outer.bind(sources={"reading": slot})
    result = outer.nodes["result"]
    assert result.source_chain() is None
    with pytest.raises(UnboundSourceError, match="unbound.*reading"):
        result._chain_content_hash()
    if inner_job:
        inner.bind(sources={"reading": source})
        assert result.source_chain() == source.content_hash()


@pytest.mark.parametrize("has_rows", [False, True])
def test_row_materialization_keeps_both_chains(has_rows: bool) -> None:
    """Both empty and nonempty row builds represent the original source."""
    from orcapod.core.operators.static_output_pod import RowsOnRead

    source = _source("reading", False)
    wrapped = RowsOnRead(list(source) if has_rows else [], source)
    assert _hashes(wrapped.built()) == _hashes(wrapped) == _hashes(source)
    wrapped.release_built()
    assert _hashes(wrapped.built()) == _hashes(source)


def test_async_batch_input_keeps_both_chains() -> None:
    """A barrier operator's row-built input reports the original job slot hashes."""
    from orcapod.operators import Batch

    seen: list[tuple[object, object]] = []

    class InspectBatch(Batch):
        """Read the rebuilt input identities when processing the rows."""

        def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
            seen.append(_hashes(stream))
            return super().unary_static_process(stream)

    source = _source("reading", False)
    job = PipelineJob(name="batch_input_identity", database=InMemoryArrowDatabase())
    with job:
        InspectBatch(2)(source)
    _run(job, run_async=True)
    assert seen == [_hashes(job.nodes["reading"])]


def test_copying_a_slot_under_a_new_name_recomputes_the_name_chain() -> None:
    """The renamed slot and its descendants use the new declaration's name."""
    from orcapod.core.nodes.source_node import SourceJobNode

    reading = SourceJobNode.from_stream(_source("reading", False))
    original = add.pod(reading)
    before = _hashes(original)
    renamed = SourceJobNode.from_stream(reading, name="renamed")
    result = add.pod(renamed)
    after = _hashes(result)
    assert after[0] == before[0]
    assert after[1] != before[1]
    assert _hashes(original) == before


def test_custom_operator_symmetry_receives_streams_on_save(tmp_path: Path) -> None:
    """A custom operator can require concrete stream objects in its callback."""
    from orcapod.core.nodes.source_node import SourceNodeBase

    class StreamCheckingJoin(Join):
        def argument_symmetry(
            self, streams: Collection[StreamProtocol]
        ) -> ArgumentGroup:
            assert all(
                isinstance(stream, (DictSource, SourceNodeBase)) for stream in streams
            )
            return super().argument_symmetry(streams)

    left = _source("left", False)
    right = _source("right", False, right=True)
    job = PipelineJob(name="custom_symmetry", database=InMemoryArrowDatabase())
    with job:
        StreamCheckingJoin()(left, right, label="joined")
    job.run()
    path = tmp_path / "job.json"
    job.save(path)
    assert path.is_file()
    assert job.nodes["joined"].node_hash()


def test_overlapping_identity_read_cannot_publish_stale_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cold reader cannot repopulate its memo with the old binding."""
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event, get_ident

    source = _source("reading", False)
    rebound = _source("rebound", False)
    job = PipelineJob(name="overlapping_read", database=InMemoryArrowDatabase())
    with job:
        source.select_data_columns(["x"], label="projection")
    result = job.nodes["projection"]
    computed, resume, binding = Event(), Event(), Event()
    main_thread = get_ident()
    original = result._compute_identity_chains  # pyright: ignore[reportPrivateUsage]  # Pause immediately before memo publication.

    def pause(walk: IdentityWalkProtocol) -> IdentityChains:
        chains = original(walk)
        if get_ident() != main_thread:
            computed.set()
            assert resume.wait(10)
        return chains

    def bind() -> None:
        binding.set()
        job.bind(sources={"reading": rebound})

    monkeypatch.setattr(result, "_compute_identity_chains", pause)
    with ThreadPoolExecutor(2) as executor:
        first = executor.submit(result.source_chain)
        try:
            assert computed.wait(5)
            rebinding = executor.submit(bind)
            assert binding.wait(5)
            # A synchronized bind is allowed to wait for the in-flight reader.
            try:
                rebinding.result(timeout=1)
            except TimeoutError:
                pass
            else:
                assert result.source_chain() == rebound.content_hash()
        finally:
            resume.set()
        first.result(timeout=5)
        rebinding.result(timeout=5)
    assert result.source_chain() == rebound.content_hash()


def test_identity_memo_is_invalidated_before_binding_is_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The new binding becomes visible after invalidation of dependent chains."""
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    from orcapod.core.nodes.source_node import SourceJobNode

    source = _source("reading", False)
    rebound = _source("rebound", False)
    job = PipelineJob(name="binding_visibility", database=InMemoryArrowDatabase())
    with job:
        source.select_data_columns(["x"], label="projection")
    result = job.nodes["projection"]
    slot = job.nodes["reading"]
    assert isinstance(slot, SourceJobNode)
    assert result.source_chain() == source.content_hash()
    original = slot._invalidate_identity_chains  # pyright: ignore[reportPrivateUsage]  # Inspect the binding at the invalidation boundary.
    invalidating, resume = Event(), Event()

    def pause() -> None:
        invalidating.set()
        assert resume.wait(5)
        original()

    monkeypatch.setattr(slot, "_invalidate_identity_chains", pause)
    with ThreadPoolExecutor(1) as executor:
        bind = executor.submit(job.bind, sources={"reading": rebound})
        try:
            assert invalidating.wait(5)
            assert slot.bound_source is source
        finally:
            resume.set()
        bind.result(timeout=5)
    assert slot.bound_source is rebound
    assert result.source_chain() == rebound.content_hash()


def test_identity_terms_accept_canonical_semantic_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Terms accepted by semantic hashing can include dictionaries and lists."""
    source = _source("reading", False)
    result = source.select_data_columns(["x"])
    source_term, name_term = {"mode": "x"}, ["variant"]
    monkeypatch.setattr(result, "_identity_terms", lambda: (source_term, name_term))
    for label, upstream, term, chain in (
        (
            SOURCE_CHAIN_HASH_LABEL,
            source.source_chain(),
            source_term,
            result.source_chain,
        ),
        (NAME_CHAIN_HASH_LABEL, source.name_chain(), name_term, result.name_chain),
    ):
        expected = SemanticAwarePythonHasher(hasher_id=label).hash_object(
            ("term", upstream, term)
        )
        assert chain() == expected
        assert chain() == expected
