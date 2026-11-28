"""A pipeline binds each source content hash to a single object (ITL-1469)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from orcapod import Pipeline, PipelineJob, function_pod, side_effect_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InconsistentSourceError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.side_effects import SideEffectPod
from orcapod.sources import CSVSource, DictSource

Orchestrator = type[AsyncPipelineOrchestrator] | type[SyncPipelineOrchestrator]
ORCHESTRATORS = [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
ROWS = [{"t1": "a", "t2": "b", "x": 1, "y": 2}, {"t1": "c", "t2": "d", "x": 3, "y": 4}]


@function_pod(output_keys="out")
def total(x: int, y: int) -> int:
    """Add the two data columns."""
    return x + y


@function_pod(output_keys="out")
def difference(x: int, y: int) -> int:
    """Subtract the second data column from the first."""
    return x - y


@function_pod(output_keys="out")
def product(x: int, y: int) -> int:
    """Multiply the two data columns."""
    return x * y


def _source(
    tmp_path: Path, kind: str, name: str, *, reverse: bool = False, change: bool = False
) -> DictSource | CSVSource:
    rows = [dict(row) for row in ROWS]
    if change:
        rows[-1]["y"] = 40
    columns = ["t1", "t2", "x", "y"]
    tags = ["t1", "t2"]
    if reverse:
        columns.reverse()
        tags.reverse()
    if kind == "csv":
        path = tmp_path / f"{name}-{reverse}.csv"
        path.write_text(
            ",".join(columns)
            + "\n"
            + "".join(",".join(str(row[col]) for col in columns) + "\n" for row in rows)
        )
        return CSVSource(
            str(path), source_id=name, tag_columns=tags, tag_only_columns=True
        )
    return DictSource(
        [{col: row[col] for col in columns} for row in rows],
        source_id=name,
        tag_columns=tags,
        tag_only_columns=True,
    )


def _shifted_source(name: str, offset: int) -> DictSource:
    """Build a fixture whose output values distinguish it from other slot bindings."""
    return DictSource(
        [{**row, "x": int(row["x"]) + offset} for row in ROWS],
        source_id=name,
        tag_columns=["t1", "t2"],
        tag_only_columns=True,
    )


def _pair(
    tmp_path: Path, kind: str, reverse: bool
) -> tuple[DictSource | CSVSource, DictSource | CSVSource]:
    first = _source(tmp_path, kind, "shared")
    second = _source(tmp_path, kind, "shared", reverse=True, change=kind != "equal")
    assert first is not second
    assert first.content_hash() == second.content_hash()
    return (second, first) if reverse else (first, second)


def _job(tmp_path: Path, slots: tuple[str, ...] = ("a", "b")) -> PipelineJob:
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        for slot, compute in zip(slots, (total, difference, product)):
            compute.pod(_source(tmp_path, "dict", slot), label=f"result_{slot}")
    return job


def _rebind(job: PipelineJob, sources: dict[str, StreamProtocol], setter: bool) -> None:
    if setter:
        for name, source in sources.items():
            job.nodes[name].bound_source = source
    else:
        job.bind(sources=sources)


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("kind", ["equal", "different", "csv"])
@pytest.mark.parametrize("graph_type", [Pipeline, PipelineJob])
def test_capture_refuses_two_objects_with_one_hash(
    tmp_path: Path,
    graph_type: type[Pipeline] | type[PipelineJob],
    kind: str,
    reverse: bool,
) -> None:
    first, second = _pair(tmp_path, kind, reverse)
    with graph_type("registration") as graph:
        total.pod(first, label="first")
        with pytest.raises(
            InconsistentSourceError, match="distinct source objects"
        ) as error:
            total.pod(second, label="second")
    for part in ("step 'first'", "step 'second'", "reuse", "source_id"):
        assert part in str(error.value)
    assert "second" not in graph.nodes


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("kind", ["equal", "different", "csv"])
def test_bind_refuses_two_objects_with_one_hash(
    tmp_path: Path, kind: str, reverse: bool
) -> None:
    first, second = _pair(tmp_path, kind, reverse)
    job = _job(tmp_path)
    before = job.sources.copy()
    with pytest.raises(
        InconsistentSourceError, match="distinct source objects"
    ) as error:
        job.bind(sources={"a": first, "b": second})
    assert "slot 'a'" in str(error.value) and "slot 'b'" in str(error.value)
    assert job.sources == before

    job.bind(sources={"a": first, "b": first})
    assert job.sources["a"] is job.sources["b"] is first
    job.bind(sources={"a": _source(tmp_path, kind, "replacement")})
    job.bind(sources={"b": second})
    assert job.sources["b"] is second


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("kind", ["equal", "different", "csv"])
@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_run_refuses_two_objects_with_one_hash(
    tmp_path: Path, kind: str, reverse: bool, orchestrator: Orchestrator
) -> None:
    first, second = _pair(tmp_path, kind, reverse)
    job = _job(tmp_path)
    # Inject invalid job state to exercise the run-time check independently.
    job.nodes["a"]._bind(first)
    job.nodes["b"]._bind(second)
    with pytest.raises(
        InconsistentSourceError, match="distinct source objects"
    ) as error:
        job.run(orchestrator=orchestrator())
    assert "slot 'a'" in str(error.value) and "slot 'b'" in str(error.value)
    assert job.last_run is None


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("kind", ["equal", "different", "csv"])
def test_load_refuses_different_configs_that_rebuild_to_one_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str, reverse: bool
) -> None:
    from orcapod.pipeline import serialization

    first, second = _pair(tmp_path, kind, reverse)
    job = _job(tmp_path)
    path = tmp_path / "job.json"
    job.save(path)
    saved = json.loads(path.read_text())
    saved["bindings"]["sources"] = {"a": first.to_config(), "b": second.to_config()}
    if kind != "csv":
        # In-memory sources have no persisted rows. Simulate their reconstruction
        # from distinct configs without changing the saved-format support for them.
        for name in ("a", "b"):
            saved["bindings"]["sources"][name]["fixture_slot"] = name
        restored = {"a": first, "b": second}
        monkeypatch.setattr(
            serialization,
            "resolve_source_from_config",
            lambda config: restored[config["fixture_slot"]],
        )
    path.write_text(json.dumps(saved))
    with pytest.raises(
        InconsistentSourceError, match="distinct source objects"
    ) as error:
        PipelineJob.load(path)
    assert "slot 'a'" in str(error.value) and "slot 'b'" in str(error.value)


@pytest.mark.parametrize("reverse", [False, True])
def test_load_refuses_one_source_id_with_different_key_columns(
    tmp_path: Path, reverse: bool
) -> None:
    csv = _source(tmp_path, "csv", "shared")
    job = _job(tmp_path)
    path = tmp_path / "job.json"
    job.save(path)
    saved = json.loads(path.read_text())
    first = {**csv.to_config(), "record_id_columns": ["t1", "x"]}
    second = {**csv.to_config(), "record_id_columns": ["t2", "y"]}
    configs = (second, first) if reverse else (first, second)
    saved["bindings"]["sources"] = dict(zip(("a", "b"), configs, strict=True))
    path.write_text(json.dumps(saved))

    with pytest.raises(InconsistentSourceError, match="different key columns") as error:
        PipelineJob.load(path)
    assert "slot 'a'" in str(error.value) and "slot 'b'" in str(error.value)
    assert "ITL-1506" in str(error.value)


@pytest.mark.parametrize("slots", [("a", "b"), ("c", "b", "a")])
@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_load_reuses_one_object_for_identical_saved_configs(
    tmp_path: Path, slots: tuple[str, ...], orchestrator: Orchestrator
) -> None:
    job = _job(tmp_path, slots)
    db = InMemoryArrowDatabase()
    source = _source(tmp_path, "csv", "shared")
    job.bind(database=db, sources=dict.fromkeys(slots, source))
    job.run(orchestrator=orchestrator())
    path = tmp_path / "job.json"
    job.save(path)
    saved = json.loads(path.read_text())
    configs = saved["bindings"]["sources"]
    assert all(configs[name] == configs[slots[0]] for name in slots)

    loaded = PipelineJob.load(path, database=db)
    shared = loaded.sources[slots[0]]
    for name in slots:
        assert loaded.sources[name] is shared
    copied = tmp_path / "copied.json"
    loaded.save(copied)
    assert copied.read_bytes() == path.read_bytes()

    def rows(graph: PipelineJob, name: str) -> list[dict[str, Any]]:
        return graph.nodes[name].as_table(columns={"sort_by_tags": True}).to_pylist()

    stored = {
        table_path: db.get_all_records(table_path)
        for table_path in db.list_table_paths()
    }
    loaded.run(orchestrator=orchestrator(), load_only=True)
    for name in slots:
        assert rows(loaded, name) == ROWS
        assert rows(loaded, f"result_{name}") == rows(job, f"result_{name}")
    assert {
        table_path: db.get_all_records(table_path)
        for table_path in db.list_table_paths()
    } == stored

    replacement = _source(tmp_path, "csv", "replacement")
    loaded.bind(sources={slots[0]: replacement})
    assert loaded.sources[slots[0]] is replacement
    for name in slots[1:]:
        assert loaded.sources[name] is shared


@pytest.mark.parametrize("reverse_keys", [False, True])
def test_load_shares_repeated_configs_written_by_main(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reverse_keys: bool
) -> None:
    # origin/main (f55ed6d7) wrote a separate, identical config for each of two
    # distinct CSVSource objects bound to different slots.
    fixture = Path(__file__).with_name("fixtures") / "itl_1469_main_shared_source.json"
    configs = json.loads(fixture.read_text())["bindings"]["sources"]
    assert configs["a"] == configs["b"]
    monkeypatch.chdir(tmp_path)
    Path("shared.csv").write_text("t1,t2,x,y\na,b,1,2\nc,d,3,4\n")
    path = fixture
    if reverse_keys:
        saved = json.loads(fixture.read_text())
        sources = saved["bindings"]["sources"]
        sources["b"] = dict(reversed(sources["b"].items()))
        path = tmp_path / "reordered.json"
        path.write_text(json.dumps(saved))

    job = PipelineJob.load(path)
    assert job.sources["a"] is job.sources["b"]
    assert job.nodes["a"].as_table().to_pylist() == ROWS
    assert job.nodes["b"].as_table().to_pylist() == ROWS


def test_itl_1554_loading_shared_slots_feeding_the_same_pod(tmp_path: Path) -> None:
    """Record the existing read-only DAG failure for this shared-source topology."""
    db = InMemoryArrowDatabase()
    with PipelineJob(database=db) as job:
        for slot in ("a", "b"):
            total.pod(_source(tmp_path, "dict", slot), label=f"total_{slot}")
    source = _source(tmp_path, "csv", "shared")
    job.bind(sources={"a": source, "b": source})
    job.run()
    path = tmp_path / "job.json"
    job.save(path)

    with pytest.raises(NotImplementedError, match="StreamBase.identity_structure"):
        PipelineJob.load(path, database=db)


@pytest.mark.parametrize("kind", ["equal", "different", "csv"])
def test_slot_setter_refuses_duplicates_and_preserves_bindings(
    tmp_path: Path, kind: str
) -> None:
    first, second = _pair(tmp_path, kind, False)
    job = _job(tmp_path)
    job.nodes["a"].bound_source = first
    original = job.nodes["b"].bound_source
    with pytest.raises(
        InconsistentSourceError, match="distinct source objects"
    ) as error:
        job.nodes["b"].bound_source = second
    assert "slot 'a'" in str(error.value) and "slot 'b'" in str(error.value)
    assert job.nodes["b"].bound_source is original
    assert job.sources["a"] is first
    assert job.sources["b"] is original
    job.compile()
    assert job.nodes["a"].bound_source is first
    assert job.nodes["b"].bound_source is original


def test_slot_setter_same_hash_replacement_blocks_capture_but_runs(
    tmp_path: Path,
) -> None:
    first = _source(tmp_path, "dict", "a")
    replacement = _source(tmp_path, "dict", "a", change=True)
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        total.pod(first, label="first")
    job.nodes["a"].bound_source = replacement
    with job:
        with pytest.raises(InconsistentSourceError, match="not as captured"):
            difference.pod(replacement, label="second")
    assert "second" not in job.nodes
    assert job.sources["a"] is replacement
    assert job.nodes["a"].bound_source is replacement
    job.run()
    assert sorted(job.first.as_table()["out"].to_pylist()) == [3, 43]


def test_slot_setter_can_unbind_and_recompile(tmp_path: Path) -> None:
    job = _job(tmp_path)
    job.nodes["a"].bound_source = None
    assert "a" not in job.sources
    job.compile()
    assert job.nodes["a"].bound_source is None
    assert job.unbound_sources == ["a"]


@pytest.mark.parametrize("binding", ["constructor", "from_stream", "setter"])
@pytest.mark.filterwarnings("ignore::orcapod.errors.SemiJoinIgnoredColumnWarning")
def test_source_slot_refuses_composite_with_conflicting_roots(
    tmp_path: Path, binding: str
) -> None:
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.operators import SemiJoin

    first, second = _pair(tmp_path, "different", False)
    composite = SemiJoin()(first, second, label="matched")
    slot = SourceJobNode.from_stream(first, name="standalone")
    with pytest.raises(
        InconsistentSourceError, match="distinct source objects"
    ) as error:
        if binding == "constructor":
            SourceJobNode("standalone", bound_source=composite)
        elif binding == "from_stream":
            SourceJobNode.from_stream(composite, name="standalone")
        else:
            slot.bound_source = composite
    for part in ("slot 'standalone'", "'matched' input 1", "'matched' input 2"):
        assert part in str(error.value)
    assert slot.bound_source is first


def test_coalesced_captures_release_duplicate_projections(tmp_path: Path) -> None:
    import gc
    from weakref import ref

    source = _source(tmp_path, "dict", "shared")
    graph = Pipeline("retention")

    def capture_projection():
        projection = source.select_data_columns(["x", "y"])
        with graph:
            _ = total.pod(projection, label="step")
        return ref(projection)

    projections = [capture_projection() for _ in range(8)]
    _ = gc.collect()

    assert "step" in graph.nodes
    assert sum(projection_ref() is not None for projection_ref in projections) <= 2


def test_capture_reports_both_uses_of_slots_with_the_same_name(tmp_path: Path) -> None:
    from orcapod.core.nodes.source_node import SourceJobNode

    first, second = _pair(tmp_path, "different", False)
    left = SourceJobNode.from_stream(first, name="same")
    right = SourceJobNode.from_stream(second, name="same")
    with Pipeline("diagnostics"):
        total.pod(left, label="first")
        with pytest.raises(
            InconsistentSourceError, match="distinct source objects"
        ) as error:
            difference.pod(right, label="second")
    message = str(error.value)
    assert "step 'first'" in message and "step 'second'" in message


def test_plain_derived_collision_only_suggests_reusing_the_object(
    tmp_path: Path,
) -> None:
    job = _job(tmp_path, ("a",))
    job.run()
    first = job.result_a.as_source(source_id="left")
    second = job.result_a.as_source(source_id="right")
    assert first.content_hash() == second.content_hash()
    assert first.source_id != second.source_id

    @function_pod(output_keys="answer")
    def consume(out: int) -> int:
        return out

    with Pipeline("diagnostics"):
        consume.pod(first, label="first")
        with pytest.raises(InconsistentSourceError, match="reuse the object") as error:
            consume.pod(second, label="second")
    assert "source_id" not in str(error.value)


def test_compile_refuses_schema_conflict_introduced_through_live_slot() -> None:
    import pyarrow as pa

    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.sources import ArrowTableSource

    def source(name: str, kind: pa.DataType) -> ArrowTableSource:
        return ArrowTableSource(
            pa.table({"x": pa.array([1], type=kind), "y": [2]}),
            source_id=name,
            infer_nullable=True,
        )

    first = SourceJobNode.from_stream(source("shared", pa.int32()), name="same")
    second = SourceJobNode.from_stream(source("other", pa.int64()), name="same")
    with Pipeline("compile", auto_compile=False) as pipeline:
        total.pod(first, label="first")
        difference.pod(second, label="second")
    second.bound_source = source("shared", pa.int64())
    with pytest.raises(InconsistentSourceError, match="different schemas") as error:
        pipeline.compile()
    assert "step 'first'" in str(error.value) and "step 'second'" in str(error.value)


@pytest.mark.filterwarnings("ignore::orcapod.errors.SemiJoinIgnoredColumnWarning")
def test_source_slot_run_checks_roots_changed_after_binding(tmp_path: Path) -> None:
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.operators import SemiJoin

    first, conflict = _pair(tmp_path, "different", False)
    right = SourceJobNode.from_stream(_source(tmp_path, "dict", "other"))
    composite = SemiJoin()(first, right, label="matched")
    slot = SourceJobNode.from_stream(composite, name="standalone")
    right.bound_source = conflict
    with pytest.raises(InconsistentSourceError, match="distinct source objects"):
        slot.run()


@pytest.mark.parametrize("setter", [False, True], ids=["bind", "setter"])
@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_binding_back_the_captured_object_restores_capture_and_rows(
    tmp_path: Path, setter: bool, orchestrator: Orchestrator
) -> None:
    original = _source(tmp_path, "dict", "a")
    another_object = _source(tmp_path, "dict", "a", reverse=True, change=True)
    replacement = _source(tmp_path, "dict", "b", change=True)
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        total.pod(original, label="before")

    # Bind inside an open capture context so the check must run when recording.
    with job:
        _rebind(job, {"a": replacement}, setter)
        for candidate in (replacement, original, another_object):
            with pytest.raises(
                InconsistentSourceError, match="not as captured"
            ) as error:
                difference.pod(candidate, label="refused")
            assert "'a'" in str(error.value)
            assert "ITL-1559" in str(error.value)
            assert "Bind" in str(error.value) and "new job" in str(error.value)
    job.run(orchestrator=orchestrator())
    assert sorted(job.before.as_table()["out"].to_pylist()) == [3, 43]

    _rebind(job, {"a": original}, setter)
    job.compile()
    with job:
        difference.pod(original, label="after")
    job.run(orchestrator=orchestrator())
    assert set(job.nodes) == {"a", "before", "after"}
    assert sorted(job.before.as_table()["out"].to_pylist()) == [3, 7]
    assert sorted(job.after.as_table()["out"].to_pylist()) == [-1, -1]


WRAPPER_PAIRS = [
    "source-cache",
    "two-caches",
    "nested-cache",
    "two-as-source",
    "two-tag-copies",
    "source-proxy",
    "cache-proxy",
]


@pytest.fixture(params=WRAPPER_PAIRS)
def wrapper_pair(tmp_path: Path, request: pytest.FixtureRequest):
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.source_proxy import SourceProxy
    from orcapod.databases import DeltaTableDatabase
    from orcapod.sources import DerivedSource

    source = _source(tmp_path, "dict", "shared")
    kind = request.param
    if kind == "two-as-source":
        job = _job(tmp_path, ("a",))
        job.run()
        first = job.result_a.as_source()
        return first, job.result_a.as_source(), first, "out"
    if kind == "two-tag-copies":
        first = DerivedSource(
            source, tags_as_data={"t1": "tag_value"}, source_id="copied"
        )
        second = DerivedSource(
            source, tags_as_data={"t1": "tag_value"}, source_id="copied"
        )
        return first, second, first, "x"
    if kind in ("source-proxy", "cache-proxy"):
        cache = source.cached(DeltaTableDatabase(tmp_path / "cache"))
        proxy = CachedSource.from_config(cache.to_config()).cached_source
        assert isinstance(proxy, SourceProxy)
        proxy.bind(source)
        return source if kind == "source-proxy" else cache, proxy, proxy, "x"

    cache = source.cached(InMemoryArrowDatabase())
    if kind == "source-cache":
        return source, cache, cache, "x"
    if kind == "two-caches":
        return cache, source.cached(InMemoryArrowDatabase()), cache, "x"
    return cache, cache.cached(InMemoryArrowDatabase()), cache, "x"


def test_wrapper_pairs_with_one_content_hash_are_refused(wrapper_pair) -> None:
    first, second, _, column = wrapper_pair
    assert first is not second
    assert first.content_hash() == second.content_hash()
    with Pipeline("wrapper_conflict") as graph:
        first.map_data({column: "a"}, label="first")
        with pytest.raises(InconsistentSourceError, match="distinct source objects"):
            second.map_data({column: "b"}, label="second")
    assert "first" in graph.nodes
    assert "second" not in graph.nodes


def test_one_wrapper_object_can_be_reused(wrapper_pair) -> None:
    _, _, shared, column = wrapper_pair
    with Pipeline("wrapper_reuse") as graph:
        shared.map_data({column: "a"}, label="first")
        shared.map_data({column: "b"}, label="second")
    assert {"first", "second"} <= graph.nodes.keys()


@function_pod(output_keys="out")
def increment(x: int) -> int:
    """Increment the selected input."""
    return x + 1


@function_pod(output_keys="doubled")
def double(out: int) -> int:
    """Double the earlier output."""
    return out * 2


@pytest.mark.parametrize("kind", ["select", "join", "function", "cache"])
@pytest.mark.parametrize("setter", [False, True], ids=["bind", "setter"])
@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
@pytest.mark.filterwarnings("ignore::orcapod.errors.SemiJoinIgnoredColumnWarning")
def test_rebound_job_refuses_prebuilt_streams_after_recompile(
    tmp_path: Path, kind: str, setter: bool, orchestrator: Orchestrator
) -> None:
    from orcapod.operators import SemiJoin

    original = _source(tmp_path, "dict", "a")
    replacement = _shifted_source("b", 10)
    peer = _source(tmp_path, "dict", "peer")
    original_cache = original.cached(InMemoryArrowDatabase())
    consumer = double if kind == "function" else increment

    def build(prebuilt: bool = False):
        if kind == "join":
            return SemiJoin()(original, peer, label="joined")
        if kind == "function":
            return increment.pod(original, label="prepared")
        source = (
            original
            if kind == "select"
            else (
                original.cached(InMemoryArrowDatabase()) if prebuilt else original_cache
            )
        )
        return source.select_data_columns(["x"], label="selected")

    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        recorded = build()
        consumer.pod(recorded, label="before")
    if setter:
        job.nodes["a"].bound_source = replacement
    else:
        job.bind(sources={"a": replacement})
    job.compile()

    prebuilt = build(prebuilt=True)
    assert (
        prebuilt is not recorded and prebuilt.content_hash() == recorded.content_hash()
    )
    with job:
        with pytest.raises(InconsistentSourceError, match="not as captured") as error:
            consumer.pod(prebuilt, label="refused")
        assert "'a'" in str(error.value)
    job.run(orchestrator=orchestrator())
    assert "refused" not in job.nodes
    column, expected = (
        ("doubled", [24, 28]) if kind == "function" else ("out", [12, 14])
    )
    assert sorted(job.before.as_table()[column].to_pylist()) == expected


@pytest.mark.parametrize(
    "names", [("a", "b"), ("b", "a"), ("a", "b", "c"), ("c", "b", "a")]
)
@pytest.mark.parametrize("setter", [False, True], ids=["bind", "setter"])
@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_swapping_or_rotating_slots_blocks_capture_but_preserves_rows(
    names: tuple[str, ...], setter: bool, orchestrator: Orchestrator
) -> None:
    sources = {
        name: _shifted_source(name, 10 * index) for index, name in enumerate(names)
    }
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        for name, source in sources.items():
            total.pod(source, label=f"original_{name}")
    rotated = names[1:] + names[:1]
    _rebind(
        job,
        {slot: sources[name] for slot, name in zip(names, rotated, strict=True)},
        setter,
    )
    job.compile()
    with job:
        for source in reversed(sources.values()):
            with pytest.raises(
                InconsistentSourceError, match="not as captured"
            ) as error:
                difference.pod(source, label="added")
            assert all(repr(name) in str(error.value) for name in names)
    job.run(orchestrator=orchestrator())

    assert "added" not in job.nodes
    assert set(job.sources) == set(names)
    for index, name in enumerate(names):
        before = job.nodes[f"original_{name}"]
        offset = 10 * ((index + 1) % len(names))
        assert sorted(before.as_table()["out"].to_pylist()) == [offset + 3, offset + 7]

    restored, *still_changed = names
    _rebind(job, {restored: sources[restored]}, setter)
    with job:
        with pytest.raises(InconsistentSourceError, match="not as captured") as error:
            difference.pod(sources[restored], label="added")
        assert repr(restored) not in str(error.value)
        assert all(repr(name) in str(error.value) for name in still_changed)

    _rebind(job, {name: sources[name] for name in still_changed}, setter)
    with job:
        difference.pod(sources[restored], label="added")
    job.run(orchestrator=orchestrator())
    assert sorted(job.added.as_table()["out"].to_pylist()) == [-1, -1]


def test_cached_derived_collision_only_suggests_reusing_the_object(
    tmp_path: Path,
) -> None:
    job = _job(tmp_path, ("a",))
    job.run()
    first = job.result_a.as_source(source_id="left").cached(InMemoryArrowDatabase())
    second = job.result_a.as_source(source_id="right").cached(InMemoryArrowDatabase())
    assert first.content_hash() == second.content_hash()
    assert first.source_id != second.source_id
    with Pipeline("diagnostics"):
        first.map_data({"out": "first_out"}, label="first")
        with pytest.raises(InconsistentSourceError, match="reuse the object") as error:
            second.map_data({"out": "second_out"}, label="second")
    assert "source_id" not in str(error.value)


@pytest.mark.parametrize("setter", [False, True])
def test_refusing_to_recompile_a_loaded_job_preserves_its_sources_mapping(
    tmp_path: Path, setter: bool
) -> None:
    original = _source(tmp_path, "csv", "a")
    replacement = _source(tmp_path, "csv", "b", change=True)
    db = InMemoryArrowDatabase()
    with PipelineJob(database=db) as job:
        total.pod(original, label="before")
    job.bind(sources={"a": replacement})
    job.run()
    path = tmp_path / "job.json"
    job.save(path)
    loaded = PipelineJob.load(path, database=db)
    if setter:
        loaded.nodes["a"].bound_source = original
    before = loaded.sources
    nodes = loaded.nodes

    with pytest.raises(RuntimeError, match="ITL-1560"):
        loaded.compile()

    assert set(loaded.sources) == set(before) == {"a"}
    assert loaded.sources["a"] is before["a"]
    assert loaded.nodes == nodes


@pytest.mark.parametrize("prebuilt", [False, True], ids=["same-pod", "prebuilt"])
@pytest.mark.parametrize("setter", [False, True], ids=["bind", "setter"])
@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_rebound_job_refuses_same_pod_or_prebuilt_recapture(
    tmp_path: Path, prebuilt: bool, setter: bool, orchestrator: Orchestrator
) -> None:
    first = _source(tmp_path, "dict", "a")
    second = _shifted_source("b", 10)
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        if prebuilt:
            increment.pod(first.select_data_columns(["x"]), label="original_a")
        else:
            total.pod(first, label="original_a")
        total.pod(second, label="original_b")
    if setter:
        job.nodes["a"].bound_source = second
        job.nodes["b"].bound_source = first
    else:
        job.bind(sources={"a": second, "b": first})
    job.compile()

    outside = first.select_data_columns(["x"]) if prebuilt else first
    with job:
        with pytest.raises(InconsistentSourceError, match="not as captured") as error:
            if prebuilt:
                outside.map_data({"x": "selected"}, label="refused")
            else:
                total.pod(outside, label="refused")
        assert "'a'" in str(error.value) and "'b'" in str(error.value)

    job.run(orchestrator=orchestrator())
    assert "refused" not in job.nodes
    assert sorted(job.original_a.as_table()["out"].to_pylist()) == (
        [12, 14] if prebuilt else [13, 17]
    )


@pytest.mark.parametrize("setter", [False, True], ids=["bind", "setter"])
@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_unbound_slot_blocks_prebuilt_capture_and_later_binding_still_runs(
    setter: bool, orchestrator: Orchestrator
) -> None:
    original, replacement, later = (
        _shifted_source(name, offset)
        for name, offset in (("a", 0), ("b", 10), ("c", 20))
    )
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        recorded = increment.pod(original, label="before")
    _rebind(job, {"a": replacement}, setter)
    job.nodes["a"].bound_source = None
    job.compile()

    prebuilt = increment.pod(original)
    assert prebuilt.content_hash() == recorded.content_hash()
    with job:
        with pytest.raises(InconsistentSourceError, match="not as captured") as error:
            double.pod(prebuilt, label="refused")
        assert "'a'" in str(error.value)
    _rebind(job, {"a": later}, setter)
    job.compile()
    job.run(orchestrator=orchestrator())
    assert "refused" not in job.nodes
    assert sorted(job.before.as_table()["out"].to_pylist()) == [22, 24]


@pytest.mark.parametrize("use_slot", [False, True], ids=["composite", "slot"])
@pytest.mark.parametrize("setter", [False, True], ids=["bind", "setter"])
@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_current_composite_binding_blocks_capture_but_preserves_rows(
    use_slot: bool, setter: bool, orchestrator: Orchestrator
) -> None:
    first, second, replacement = (
        _shifted_source(name, offset)
        for name, offset in (("a", 0), ("b", 10), ("c", 20))
    )
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        total.pod(first, label="first")
        difference.pod(second, label="second")
    composite = first.select_data_columns(["x", "y"])
    _rebind(job, {"a": replacement, "b": composite}, setter)
    job.compile()

    assert job.sources["b"] is composite
    with job:
        with pytest.raises(InconsistentSourceError, match="not as captured") as error:
            product.pod(job.nodes["b"] if use_slot else composite, label="refused")
        assert "'a'" in str(error.value) and "'b'" in str(error.value)
    job.run(orchestrator=orchestrator())
    assert "refused" not in job.nodes
    assert sorted(job.first.as_table()["out"].to_pylist()) == [23, 27]
    assert sorted(job.second.as_table()["out"].to_pylist()) == [-1, -1]


@pytest.mark.parametrize(
    "wrapped_slot", [False, True], ids=["prebuilt", "source-job-node"]
)
@pytest.mark.parametrize("setter", [False, True], ids=["bind", "setter"])
def test_capture_uses_the_binding_of_the_first_compiled_slot(
    wrapped_slot: bool, setter: bool
) -> None:
    from orcapod.core.nodes.source_node import SourceJobNode

    source = _shifted_source("a", 0)
    original = source.select_data_columns(["x", "y"])
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        if wrapped_slot:
            foreign = SourceJobNode.from_stream(original, name="slot")
            with pytest.raises(InconsistentSourceError, match="not supported yet"):
                total.pod(foreign, label="borrowed")
        total.pod(original, label="before")
    (name,) = job.sources
    assert job.sources[name] is original

    replacement = source.select_data_columns(["x", "y"])
    assert (
        replacement is not original
        and replacement.content_hash() == original.content_hash()
    )
    _rebind(job, {name: replacement}, setter)
    job.compile()
    with job:
        with pytest.raises(InconsistentSourceError, match="not as captured") as error:
            difference.pod(replacement, label="refused")
        assert repr(name) in str(error.value)

    _rebind(job, {name: original}, setter)
    with job:
        difference.pod(original, label="after")
    job.run()
    assert sorted(job.after.as_table()["out"].to_pylist()) == [-1, -1]


@pytest.mark.parametrize(
    "blocked_index,count", [(0, 2), (1, 2), (0, 3), (1, 3), (2, 3)]
)
@pytest.mark.parametrize("capture", ["function", "operator", "side_effect"])
def test_nested_capture_refusal_leaves_every_job_unchanged(
    blocked_index: int, count: int, capture: str
) -> None:
    from contextlib import ExitStack

    def deliver(x: int, ctx: Any) -> None:
        raise AssertionError("A refused side effect must never run")

    original = _shifted_source("a", 0)
    replacement = _shifted_source("b", 10)
    jobs = [PipelineJob(database=InMemoryArrowDatabase()) for _ in range(count)]
    for job in jobs:
        with job:
            total.pod(original, label="before")
    jobs[blocked_index].bind(sources={"a": replacement})

    with ExitStack() as contexts:
        for job in jobs:
            contexts.enter_context(job)
        with pytest.raises(InconsistentSourceError, match="not as captured"):
            if capture == "function":
                difference.pod(original, label="refused")
            elif capture == "operator":
                original.map_data({"x": "renamed"}, label="refused")
            else:
                tap = side_effect_pod(deliver)
                assert isinstance(tap, SideEffectPod)
                tap.process(original, label="refused")

    for job in jobs:
        assert "refused" not in job.nodes


@pytest.mark.parametrize("foreign_origin", ["standalone", "other_job"])
@pytest.mark.parametrize(
    "form",
    [
        "direct",
        "hashed",
        "select",
        "function",
        "join",
        "record",
        "invocations",
        "isolated",
    ],
)
@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_capture_refuses_foreign_source_slots(
    foreign_origin: str, form: str, orchestrator: Orchestrator
) -> None:
    from orcapod.core.function_pod import FunctionPod
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.operators import SelectDataColumns, SemiJoin
    from orcapod.pipeline.base import InvocationGraph
    from orcapod.pipeline.pod_invocation import FunctionInvocation

    source = _shifted_source("a", 0)
    if foreign_origin == "standalone":
        slot = SourceJobNode.from_stream(source, name="a")
    else:
        with PipelineJob(database=InMemoryArrowDatabase()) as upstream:
            total.pod(source, label="upstream")
        slot = upstream.nodes["a"]
        assert isinstance(slot, SourceJobNode)

    original = _shifted_source("kept", 0)
    with PipelineJob(database=InMemoryArrowDatabase()) as receiver:
        total.pod(original, label="kept_result")

    @function_pod(output_keys="scaled")
    def scale(out: int) -> int:
        return out * 10

    prebuilt = {
        "select": lambda: SelectDataColumns(["x", "y"])(slot),
        "function": lambda: total.pod(slot),
        "join": lambda: SemiJoin()(slot, _shifted_source("other", 0)),
    }
    pod = total.pod
    assert isinstance(pod, FunctionPod)
    if form == "record":
        with pytest.raises(InconsistentSourceError, match="not supported yet") as error:
            receiver.record_function_pod_invocation(pod, slot, label="borrowed")
    elif form in {"invocations", "isolated"}:
        invocations = (
            (FunctionInvocation(pod, (slot,), "borrowed"),)
            if form == "invocations"
            else ()
        )
        graph = InvocationGraph(
            invocations=invocations,
            source_streams={slot.content_hash().to_string(): slot},
        )
        with pytest.raises(InconsistentSourceError, match="not supported yet") as error:
            PipelineJob.from_invocations(graph)
    elif form in prebuilt:
        stream = prebuilt[form]()
        with (
            receiver,
            pytest.raises(InconsistentSourceError, match="not supported yet") as error,
        ):
            if form == "function":
                scale.pod(stream, label="borrowed")
            else:
                total.pod(stream, label="borrowed")
    else:
        with (
            receiver,
            pytest.raises(InconsistentSourceError, match="not supported yet") as error,
        ):
            mid = total.pod(slot, label="borrowed")
            if form == "hashed":
                _ = mid.content_hash()
            slot.bound_source = _shifted_source("changed", 10)
            scale.pod(mid, label="downstream")

    assert "'a'" in str(error.value)
    assert "Pass the source itself" in str(error.value)
    assert "as_source()" in str(error.value)
    assert "ITL-1559" in str(error.value)
    assert slot.bound_source is source
    assert set(receiver.nodes) == {"kept", "kept_result"}
    receiver.run(orchestrator=orchestrator())
    assert sorted(receiver.kept_result.as_table()["out"].to_pylist()) == [3, 7]


@pytest.mark.parametrize("owner_first", [True, False])
@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_foreign_slot_refusal_leaves_every_active_job_unchanged(
    owner_first: bool, orchestrator: Orchestrator
) -> None:
    source = _shifted_source("a", 0)
    with PipelineJob(database=InMemoryArrowDatabase()) as owner:
        total.pod(source, label="before")
    with PipelineJob(database=InMemoryArrowDatabase()) as receiver:
        total.pod(source, label="before")
    contexts = (owner, receiver) if owner_first else (receiver, owner)
    with contexts[0], contexts[1]:
        with pytest.raises(InconsistentSourceError, match="not supported yet"):
            product.pod(owner.nodes["a"], label="borrowed")
    for job in contexts:
        assert set(job.nodes) == {"a", "before"}
        job.run(orchestrator=orchestrator())
        assert sorted(job.before.as_table()["out"].to_pylist()) == [3, 7]


@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_capture_accepts_own_slots_and_another_jobs_stored_results(
    orchestrator: Orchestrator,
) -> None:
    source = _shifted_source("a", 0)
    with PipelineJob(database=InMemoryArrowDatabase()) as upstream:
        total.pod(source, label="before")
    with upstream:
        product.pod(upstream.nodes["a"], label="own_slot")
    upstream.run(orchestrator=orchestrator())
    assert sorted(upstream.own_slot.as_table()["out"].to_pylist()) == [2, 12]

    @function_pod(output_keys="scaled")
    def scale(out: int) -> int:
        return out * 10

    with PipelineJob(database=InMemoryArrowDatabase()) as receiver:
        with pytest.raises(InconsistentSourceError, match="not supported yet"):
            scale.pod(upstream.before, label="borrowed")
        scale.pod(upstream.before.as_source(), label="stored")
    assert "borrowed" not in receiver.nodes
    receiver.run(orchestrator=orchestrator())
    assert sorted(receiver.stored.as_table()["scaled"].to_pylist()) == [30, 70]


@pytest.mark.parametrize("origin", ["standalone", "pipeline", "job_blueprint"])
@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_schema_declaration_gives_each_job_its_own_binding(
    origin: str, orchestrator: Orchestrator
) -> None:
    from orcapod.core.nodes.source_node import SourceJobNode, SourceNode

    original = _shifted_source("a", 0)
    if origin == "pipeline":
        with Pipeline("blueprint") as blueprint:
            total.pod(original, label="result")
        declaration = blueprint.nodes["a"]
    elif origin == "job_blueprint":
        with PipelineJob(database=InMemoryArrowDatabase()) as upstream:
            total.pod(original, label="result")
        declaration = upstream.as_pipeline().nodes["a"]
        assert isinstance(upstream.nodes["a"], SourceJobNode)
        assert upstream.nodes["a"].bound_source is original
    else:
        declaration = SourceNode.from_stream(original, name="a")
    assert isinstance(declaration, SourceNode)
    assert not hasattr(declaration, "bound_source")

    with PipelineJob(database=InMemoryArrowDatabase()) as first:
        total.pod(declaration, label="result")
    first_slot = first.nodes["a"]
    assert isinstance(first_slot, SourceJobNode)
    assert first_slot is not declaration
    assert first_slot.bound_source is None
    first.bind(sources={"a": original})

    with PipelineJob(database=InMemoryArrowDatabase()) as second:
        total.pod(declaration, label="result")
        product.pod(declaration, label="reused")
    second_slot = second.nodes["a"]
    assert isinstance(second_slot, SourceJobNode)
    assert second_slot is not first_slot
    assert second_slot.bound_source is None
    second_slot.bound_source = _shifted_source("b", 10)

    first.run(orchestrator=orchestrator())
    second.run(orchestrator=orchestrator())
    assert first_slot.bound_source is original
    assert sorted(first.result.as_table()["out"].to_pylist()) == [3, 7]
    assert sorted(second.result.as_table()["out"].to_pylist()) == [13, 17]
    assert sorted(second.reused.as_table()["out"].to_pylist()) == [22, 52]


@pytest.mark.parametrize("wrapper", ["cache", "proxy"])
@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_live_derived_source_refuses_a_wrapped_foreign_slot(
    wrapper: str, orchestrator: Orchestrator
) -> None:
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.source_proxy import SourceProxy
    from orcapod.protocols.core_protocols import SourceProtocol
    from orcapod.protocols.database_protocols import DatabaseRegistryProtocol
    from orcapod.sources import DerivedSource
    from orcapod.types import Condition

    class SourceSlot(SourceJobNode):
        @property
        def source_id(self) -> str:
            source = self.bound_source
            assert isinstance(source, DictSource)
            return source.source_id

        @property
        def condition(self) -> Condition:
            return Condition()

        @property
        def row_limit(self) -> int | None:
            return None

        def to_config(
            self, db_registry: DatabaseRegistryProtocol | None = None
        ) -> dict[str, Any]:
            source = self.bound_source
            assert isinstance(source, DictSource)
            return source.to_config(db_registry)

        @classmethod
        def from_config(
            cls,
            config: dict[str, Any],
            db_registry: DatabaseRegistryProtocol | None = None,
        ) -> SourceProtocol:
            raise NotImplementedError

    source = _shifted_source("original", 0)
    foreign = SourceSlot.from_stream(source, name="foreign")
    assert isinstance(foreign, SourceProtocol)
    if wrapper == "cache":
        origin = CachedSource(foreign, InMemoryArrowDatabase())
    else:
        proxy = SourceProxy(
            foreign.source_id,
            foreign.content_hash().to_string(),
            foreign.pipeline_hash().to_string(),
            *foreign.output_schema(),
        )
        proxy.bind(foreign)
        origin = proxy
    derived = DerivedSource(origin, tags_as_data={"t1": "copied"}, source_id="tag_copy")

    with PipelineJob(database=InMemoryArrowDatabase()) as receiver:
        total.pod(source, label="kept")
    with receiver:
        with pytest.raises(InconsistentSourceError, match="foreign.*not supported yet"):
            total.pod(derived, label="borrowed")

    assert isinstance(foreign, SourceSlot)
    foreign.bound_source = _shifted_source("original", 10)
    receiver.run(orchestrator=orchestrator())
    assert sorted(receiver.kept.as_table()["out"].to_pylist()) == [3, 7]
    assert set(receiver.nodes) == {"original", "kept"}


@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_live_derived_source_and_its_source_are_both_capturable(
    orchestrator: Orchestrator,
) -> None:
    from orcapod.sources import DerivedSource

    original = _shifted_source("a", 0)
    derived = DerivedSource(
        original, tags_as_data={"t1": "copied"}, source_id="tag_copy"
    )
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        total.pod(original, label="original")
        product.pod(derived, label="derived")
    job.run(orchestrator=orchestrator())
    assert sorted(job.original.as_table()["out"].to_pylist()) == [3, 7]
    assert sorted(job.derived.as_table()["out"].to_pylist()) == [2, 12]
