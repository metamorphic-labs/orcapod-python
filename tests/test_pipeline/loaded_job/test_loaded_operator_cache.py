"""Saving and loading a job keeps every node's identity; a loaded operator reads its cache.

ITL-1043. The job joins a function node over one in-memory ``ArrowTableSource`` with a
second one and stores the join's output (``CacheMode.LOG``). Columns are declared nullable
or not. A job file records each column's nullability and the run content hash of every
operator node, so the loaded job reproduces each node's pipeline hash, content hash, table
path and output schema, and a loaded operator node attached in ``CacheMode.REPLAY`` reads
the rows its run stored. A file without those fields (``0.1.0``, or a ``0.2.0`` file written
before them) loads as before, lossy: the operator node still looks under its run's path,
and raises rather than show the stored rows as none.
"""

from __future__ import annotations

import json
import pathlib
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join
from orcapod.core.streams import HeadStatus
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import Pipeline
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.sources import ArrowTableSource
from orcapod.types import CacheMode

JOB_NAME = "loaded_operator_cache"
NULLABILITY = {
    "non_null": (False, False),
    "nullable_tag": (True, False),
    "nullable_data": (False, True),
    "nullable_tag_and_data": (True, True),
}
STORES = ["memory", "delta"]


@function_pod(output_keys="a1")
def inc(a: int) -> int:
    """Add one."""
    return a + 1


@function_pod(output_keys="a1")
def inc_nullable(a: int | None) -> int | None:
    """Add one to a value that may be missing."""
    return None if a is None else a + 1


def _source(column: str, tag_nullable: bool, data_nullable: bool) -> ArrowTableSource:
    """A two-row source with tag ``id`` and data ``column``, nulls allowed as declared."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=tag_nullable),
            pa.field(column, pa.int64(), nullable=data_nullable),
        ]
    )
    table = pa.table({"id": [0, 1], column: [1, 2]}, schema=schema)
    return ArrowTableSource(table, tag_columns=["id"], source_id=f"src_{column}")


def _sources(nullability: str) -> dict[str, StreamProtocol]:
    """The job's two sources, keyed by source node name."""
    tag_nullable, data_nullable = NULLABILITY[nullability]
    return {
        "src_a": _source("a", tag_nullable, data_nullable),
        "src_b": _source("b", tag_nullable, data_nullable),
    }


def _store(kind: str, tmp_path: pathlib.Path) -> ArrowDatabaseProtocol:
    """An empty store of the given kind."""
    if kind == "delta":
        return DeltaTableDatabase(base_path=tmp_path / "store")
    return InMemoryArrowDatabase()


def _run(
    store: ArrowDatabaseProtocol, nullability: str, with_function: bool = True
) -> PipelineJob:
    """Run the job with the join stored (``LOG``)."""
    _, data_nullable = NULLABILITY[nullability]
    sources = _sources(nullability)
    job = PipelineJob(name=JOB_NAME, store=store)
    with job:
        left: Any = sources["src_a"]
        if with_function:
            pod = inc_nullable if data_nullable else inc
            left = pod.pod(left, label="inc")
        _ = Join()(left, sources["src_b"], label="joined")
    # OperatorNode.attach_databases is the public way to turn on LOG / REPLAY mode.
    job.nodes["joined"].attach_databases(
        pipeline_database=store.at(JOB_NAME), cache_mode=CacheMode.LOG
    )
    _ = job.run()
    return job


def _save(
    job: PipelineJob,
    tmp_path: pathlib.Path,
    edit: Callable[[dict[str, Any]], None] | None = None,
) -> pathlib.Path:
    """Save ``job``, optionally rewriting the saved file with ``edit``."""
    saved = tmp_path / "job.json"
    job.save(saved)
    if edit is not None:
        data = json.loads(saved.read_text())
        edit(data)
        saved.write_text(json.dumps(data))
    return saved


def _load_replaying(saved: pathlib.Path, store: ArrowDatabaseProtocol) -> PipelineJob:
    """Load the job and attach its operator node in ``REPLAY`` mode."""
    loaded = PipelineJob.load(str(saved), store=store)
    loaded.nodes["joined"].attach_databases(
        pipeline_database=store.at(JOB_NAME), cache_mode=CacheMode.REPLAY
    )
    return loaded


def _sorted(table: pa.Table) -> pa.Table:
    """``table`` with its columns in name order and its rows in ``id`` order."""
    return table.select(sorted(table.column_names)).sort_by("id")


def _drop_saved_nullability(data: dict[str, Any]) -> None:
    """Remove the recorded nullability, as in a file written before it existed."""
    for descriptor in data["pipeline"]["nodes"].values():
        del descriptor["output_schema"]["nullable"]
        if "source_config" in descriptor:
            del descriptor["source_config"]["nullable"]


def _drop_saved_operator_content_hash(data: dict[str, Any]) -> None:
    """Remove the join's run content hash, as in a file written before it existed."""
    del data["run"]["node_job_content_hashes"]["joined"]


# ---------------------------------------------------------------------------
# A file with the fields: every node round-trips, the operator reads its rows
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("store_kind", STORES)
@pytest.mark.parametrize("nullability", list(NULLABILITY))
def test_every_node_keeps_its_identity_and_schema_through_save_and_load(
    store_kind: str, nullability: str, tmp_path: pathlib.Path
) -> None:
    """Pipeline hash, content hash, table path and output schema are the run's."""
    store = _store(store_kind, tmp_path)
    job = _run(store, nullability)
    loaded = PipelineJob.load(str(_save(job, tmp_path)), store=store)
    # In-memory sources are not saved: a source node's content hash is its bound
    # source's, so bind the same sources before comparing.
    loaded.bind(sources=_sources(nullability))
    assert set(loaded.nodes) == set(job.nodes)
    for label, run_node in job.nodes.items():
        loaded_node = loaded.nodes[label]
        assert loaded_node.pipeline_hash() == run_node.pipeline_hash(), label
        assert loaded_node.content_hash() == run_node.content_hash(), label
        assert loaded_node.output_schema() == run_node.output_schema(), label
        if label in ("inc", "joined"):
            assert loaded_node.node_identity_path == run_node.node_identity_path


@pytest.mark.parametrize("read", ["as_table", "iter_data", "head"])
@pytest.mark.parametrize("store_kind", STORES)
@pytest.mark.parametrize("nullability", list(NULLABILITY))
def test_loaded_replay_operator_reads_the_rows_its_run_stored(
    store_kind: str, nullability: str, read: str, tmp_path: pathlib.Path
) -> None:
    """A loaded operator node in REPLAY reads exactly the rows read before saving."""
    store = _store(store_kind, tmp_path)
    job = _run(store, nullability)
    expected = _sorted(job.nodes["joined"].as_table())
    node = _load_replaying(_save(job, tmp_path), store).nodes["joined"]
    if read == "as_table":
        assert _sorted(node.as_table()).equals(expected)
    elif read == "iter_data":
        assert len(list(node.iter_data())) == expected.num_rows
    else:
        assert _sorted(node.head().table).equals(expected)


@pytest.mark.parametrize("nullability", list(NULLABILITY))
def test_a_pipeline_loaded_and_bound_to_its_sources_reuses_the_operator_cache(
    nullability: str, tmp_path: pathlib.Path
) -> None:
    """``Pipeline.load`` + ``from_pipeline(sources=...)`` reads and writes the run's table."""
    store = InMemoryArrowDatabase()
    # Pipeline.load rebuilds operators but not function pods: an operator-only pipeline.
    job = _run(store, nullability, with_function=False)
    expected = _sorted(job.nodes["joined"].as_table())
    saved = tmp_path / "pipeline.json"
    job.pipeline.save(saved)
    tables_after_run = set(store.at(JOB_NAME).list_table_paths())

    def rebuilt(cache_mode: CacheMode) -> PipelineJob:
        """The saved pipeline bound to the same sources, its join in ``cache_mode``."""
        again = PipelineJob.from_pipeline(
            Pipeline.load(saved), store=store, sources=_sources(nullability)
        )
        again.nodes["joined"].attach_databases(
            pipeline_database=store.at(JOB_NAME), cache_mode=cache_mode
        )
        return again

    assert _sorted(rebuilt(CacheMode.REPLAY).nodes["joined"].as_table()).equals(
        expected
    )
    _ = rebuilt(CacheMode.LOG).run()
    assert set(store.at(JOB_NAME).list_table_paths()) == tables_after_run


# ---------------------------------------------------------------------------
# A file without the fields: lossy, but never stored rows shown as none
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("store_kind", STORES)
@pytest.mark.parametrize("nullability", list(NULLABILITY))
def test_without_saved_nullability_the_operator_still_reads_its_runs_table(
    store_kind: str, nullability: str, tmp_path: pathlib.Path
) -> None:
    """The saved pipeline hash, not the lossy schemas, names the operator's table."""
    store = _store(store_kind, tmp_path)
    job = _run(store, nullability)
    saved = _save(job, tmp_path, _drop_saved_nullability)
    node = _load_replaying(saved, store).nodes["joined"]
    assert node.pipeline_hash() == job.nodes["joined"].pipeline_hash()
    assert node.node_identity_path == job.nodes["joined"].node_identity_path
    assert _sorted(node.as_table()).equals(_sorted(job.nodes["joined"].as_table()))


def _drop_both(data: dict[str, Any]) -> None:
    """Remove every field this fix adds, as in a file written before them."""
    _drop_saved_nullability(data)
    _drop_saved_operator_content_hash(data)


@pytest.mark.parametrize("read", ["as_table", "iter_data"])
@pytest.mark.parametrize("store_kind", STORES)
@pytest.mark.parametrize("nullability", list(NULLABILITY))
def test_without_the_run_content_hash_the_loaded_operator_raises(
    store_kind: str, nullability: str, read: str, tmp_path: pathlib.Path
) -> None:
    """Stored rows the loaded node cannot identify as its run's never read as none."""
    store = _store(store_kind, tmp_path)
    job = _run(store, nullability)
    node = _load_replaying(_save(job, tmp_path, _drop_both), store).nodes["joined"]
    with pytest.raises(
        RuntimeError,
        match=r"'joined': its cache table \(.*\) holds 2 stored rows, none of them "
        r"under this node's content hash .* the job file does not record .*ITL-1048",
    ):
        result = getattr(node, read)()
        if read == "iter_data":
            _ = list(result)


@pytest.mark.parametrize("store_kind", STORES)
def test_without_the_run_content_hash_head_says_why_it_shows_no_rows(
    store_kind: str, tmp_path: pathlib.Path
) -> None:
    """``head()`` never raises on a loaded node: it shows no rows and names the cause."""
    store = _store(store_kind, tmp_path)
    job = _run(store, "nullable_tag")
    node = _load_replaying(_save(job, tmp_path, _drop_both), store).nodes["joined"]
    head = node.head()
    assert head.status is HeadStatus.UNAVAILABLE
    assert head.table.num_rows == 0
    assert "2 stored rows, none under its run's content hash" in head.header()
    assert "ITL-1048" in head.header()


def test_a_store_holding_only_other_runs_rows_is_named_as_the_cause(
    tmp_path: pathlib.Path,
) -> None:
    """With the run's content hash recorded, rows of other runs only mean another store."""
    saved_store = InMemoryArrowDatabase()
    saved = _save(_run(saved_store, "non_null"), tmp_path)
    # Another store whose table at the same path holds another run's rows.
    other_store = InMemoryArrowDatabase()
    other = PipelineJob(name=JOB_NAME, store=other_store)
    with other:
        other_a = ArrowTableSource(
            pa.table(
                {"id": [0], "a": [9]},
                schema=pa.schema(
                    [
                        pa.field("id", pa.int64(), nullable=False),
                        pa.field("a", pa.int64(), nullable=False),
                    ]
                ),
            ),
            tag_columns=["id"],
            source_id="other_a",
        )
        _ = Join()(
            inc.pod(other_a, label="inc"),
            _sources("non_null")["src_b"],
            label="joined",
        )
    other.nodes["joined"].attach_databases(
        pipeline_database=other_store.at(JOB_NAME), cache_mode=CacheMode.LOG
    )
    _ = other.run()
    node = _load_replaying(saved, other_store).nodes["joined"]
    with pytest.raises(RuntimeError, match=r"stored by other runs.*store"):
        _ = node.as_table()
    assert node.head().status is HeadStatus.UNAVAILABLE


def test_an_absent_cache_table_shows_no_rows_in_head_and_raises_elsewhere(
    tmp_path: pathlib.Path,
) -> None:
    """A store without the run's table: ``head()`` names it; ``as_table()`` raises."""
    store = InMemoryArrowDatabase()
    saved = _save(_run(store, "non_null"), tmp_path)
    node = _load_replaying(saved, InMemoryArrowDatabase()).nodes["joined"]
    head = node.head()
    assert head.status is HeadStatus.UNAVAILABLE
    assert "cache table is absent" in head.header()
    with pytest.raises(RuntimeError, match="'joined': the cache table is absent"):
        _ = node.as_table()


def test_an_empty_nullable_list_means_every_column_is_non_null(
    tmp_path: pathlib.Path,
) -> None:
    """A recorded empty list is a fact; only an absent field is unknown."""
    store = InMemoryArrowDatabase()
    job = _run(store, "non_null")
    data = json.loads(_save(job, tmp_path).read_text())
    joined = next(
        d for d in data["pipeline"]["nodes"].values() if d["label"] == "joined"
    )
    assert joined["output_schema"]["nullable"] == {"tag": [], "data": []}


# ---------------------------------------------------------------------------
# Saving a loaded job again: known stays known, unknown stays unknown
# ---------------------------------------------------------------------------


def _recorded_fields(saved: pathlib.Path) -> dict[str, Any]:
    """The nullability records and operator run hash a saved file holds."""
    data = json.loads(saved.read_text())
    records: dict[str, Any] = {}
    for descriptor in data["pipeline"]["nodes"].values():
        label = descriptor["label"]
        records[label] = descriptor["output_schema"].get("nullable")
        if "source_config" in descriptor:
            records[f"{label}.source_config"] = descriptor["source_config"].get(
                "nullable"
            )
    records["joined.run_content_hash"] = data["run"]["node_job_content_hashes"].get(
        "joined"
    )
    return records


@pytest.mark.parametrize("nullability", ["non_null", "nullable_tag_and_data"])
def test_saving_a_loaded_job_again_keeps_what_its_file_recorded(
    nullability: str, tmp_path: pathlib.Path
) -> None:
    """A re-saved file records the same nullability and operator run hash."""
    store = InMemoryArrowDatabase()
    saved = _save(_run(store, nullability), tmp_path)
    resaved = tmp_path / "resaved.json"
    PipelineJob.load(str(saved), store=store).save(resaved)
    assert _recorded_fields(resaved) == _recorded_fields(saved)


def test_saving_a_lossy_loaded_job_again_records_nothing_it_does_not_know(
    tmp_path: pathlib.Path,
) -> None:
    """Unknown nullability and run hash are not written as facts when re-saving."""
    store = InMemoryArrowDatabase()

    def drop_both(data: dict[str, Any]) -> None:
        """Remove every field this fix adds."""
        _drop_saved_nullability(data)
        _drop_saved_operator_content_hash(data)

    saved = _save(_run(store, "nullable_tag"), tmp_path, drop_both)
    resaved = tmp_path / "resaved.json"
    PipelineJob.load(str(saved), store=store).save(resaved)
    assert all(value is None for value in _recorded_fields(resaved).values())


def test_a_source_proxy_keeps_the_nullability_of_the_source_it_stands_for() -> None:
    """A source rebuilt as a ``SourceProxy`` from its config has the source's schema."""
    from orcapod.pipeline.serialization import resolve_source_from_config

    source = _source("a", tag_nullable=True, data_nullable=False)
    config = {**source.to_config(), "source_type": "no_such_source_type"}
    proxy = resolve_source_from_config(config, fallback_to_proxy=True)
    assert proxy.output_schema() == source.output_schema()
    assert proxy.pipeline_hash() == source.pipeline_hash()


# ---------------------------------------------------------------------------
# A real format 0.1.0 file (tests/test_pipeline/fixtures, written by ITL-1026)
# ---------------------------------------------------------------------------

FORMAT_0_1_0 = pathlib.Path(__file__).parents[1] / "fixtures" / "job_format_0_1_0.json"


def _add_values(value: int, score: int) -> int:
    """The fixture job's function."""
    return value + score


def _store_the_fixture_join(store: ArrowDatabaseProtocol) -> Any:
    """Run the fixture's job with today's code, its join stored; the run's join node.

    The fixture was saved before running, with in-memory sources, so no store holds
    its rows: this run stores them at the path the file's join node names.
    """
    from orcapod.core.data_function import PythonDataFunction
    from orcapod.core.function_pod import FunctionPod

    def source(column: str, values: list[int]) -> ArrowTableSource:
        """One of the fixture's two sources."""
        table = pa.table(
            {
                "key": pa.array(["a", "b"], type=pa.large_string()),
                column: pa.array(values, type=pa.int64()),
            }
        )
        return ArrowTableSource(
            table,
            tag_columns=["key"],
            infer_nullable=True,
            source_id=f"fixture_{column}",
        )

    name = json.loads(FORMAT_0_1_0.read_text())["pipeline"]["pipeline"]["name"]
    job = PipelineJob(name=tuple(name), store=store)
    with job:
        joined = Join()(source("value", [10, 20]), source("score", [100, 200]))
        function = PythonDataFunction(_add_values, output_keys="total")
        # As in the fixture's generator: a protocol mismatch shared with other tests.
        pod = FunctionPod(data_function=function)  # pyright: ignore[reportArgumentType]
        _ = pod(joined)
    run_join = job.nodes["Join"]
    # OperatorNode.attach_databases is the public way to turn on LOG / REPLAY mode.
    run_join.attach_databases(
        pipeline_database=store.at(*name), cache_mode=CacheMode.LOG
    )
    _ = job.run()
    return run_join


def test_a_format_0_1_0_file_finds_its_join_table_and_never_shows_its_rows_as_none() -> (
    None
):
    """The real 0.1.0 file: the run's path, ``head()`` says why, ``as_table()`` raises."""
    store = InMemoryArrowDatabase()
    run_join = _store_the_fixture_join(store)
    loaded = PipelineJob.load(str(FORMAT_0_1_0), store=store)
    node = loaded.nodes["Join"]
    name = json.loads(FORMAT_0_1_0.read_text())["pipeline"]["pipeline"]["name"]
    node.attach_databases(
        pipeline_database=store.at(*name), cache_mode=CacheMode.REPLAY
    )
    assert node.pipeline_hash() == run_join.pipeline_hash()
    assert node.node_identity_path == run_join.node_identity_path
    head = node.head()
    assert head.status is HeadStatus.UNAVAILABLE
    assert "ITL-1048" in head.header()
    with pytest.raises(
        RuntimeError, match=r"'Join': its cache table .* the job file does not record"
    ):
        _ = node.as_table()
