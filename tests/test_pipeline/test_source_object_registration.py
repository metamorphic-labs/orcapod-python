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
    # Bypass bind to exercise the run-time check independently.
    job.nodes["a"].bound_source = first
    job.nodes["b"].bound_source = second
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
