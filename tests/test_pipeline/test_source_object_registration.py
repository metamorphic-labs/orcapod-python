"""A pipeline binds each source content hash to a single object (ITL-1469)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from orcapod import Pipeline, PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InconsistentSourceError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
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


def test_slot_setter_same_hash_replacement_survives_capture(tmp_path: Path) -> None:
    first = _source(tmp_path, "dict", "a")
    replacement = _source(tmp_path, "dict", "a", change=True)
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        total.pod(first, label="first")
    job.nodes["a"].bound_source = replacement
    with job:
        difference.pod(replacement, label="second")
    assert job.sources["a"] is replacement
    assert job.nodes["a"].bound_source is replacement
    job.run()
    assert sorted(job.first.as_table()["out"].to_pylist()) == [3, 43]
    assert sorted(job.second.as_table()["out"].to_pylist()) == [-37, -1]


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
@pytest.mark.parametrize("bound_first", [False, True])
@pytest.mark.parametrize("orchestrator", ORCHESTRATORS)
def test_capture_after_rebind_requires_the_current_source_object(
    tmp_path: Path, setter: bool, bound_first: bool, orchestrator: Orchestrator
) -> None:
    original = _source(tmp_path, "dict", "a")
    same_original_hash = _source(tmp_path, "dict", "a", reverse=True, change=True)
    replacement = _source(tmp_path, "dict", "b", change=True)
    with PipelineJob(database=InMemoryArrowDatabase()) as job:
        total.pod(original, label="before")
    if setter:
        job.nodes["a"].bound_source = replacement
    else:
        job.bind(sources={"a": replacement})
    job.compile()

    with job:
        if bound_first:
            difference.pod(replacement, label="after")
        for retired in (original, same_original_hash):
            with pytest.raises(InconsistentSourceError, match="slot 'a'") as error:
                product.pod(retired, label="refused")
            assert "Pass the source bound there" in str(error.value)
            assert "bind" in str(error.value)
        if not bound_first:
            difference.pod(replacement, label="after")

    assert set(job.sources) == {"a"}
    assert set(job.nodes) == {"a", "before", "after"}
    job.run(orchestrator=orchestrator())
    assert sorted(job.before.as_table()["out"].to_pylist()) == [3, 43]
    assert sorted(job.after.as_table()["out"].to_pylist()) == [-37, -1]

    # Both steps still read the same slot if it is rebound again.
    job.bind(sources={"a": original})
    job.compile()
    with job:
        with pytest.raises(InconsistentSourceError, match="slot 'a'"):
            product.pod(replacement, label="also_refused")
    job.run(orchestrator=orchestrator())
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
