"""Saving and loading a job keeps every node's identity; a loaded operator reads its cache.

ITL-1043. The job joins a function node over one in-memory ``ArrowTableSource`` with a
second one and stores the join's output (``CacheMode.LOG``). Columns are declared nullable
or not. A job file records each column's nullability and the run content hash of every
operator node, so the loaded job reproduces each node's pipeline hash, content hash, table
path and output schema, and the stored view (``as_source()``) of a loaded operator node
attached in ``CacheMode.REPLAY`` reads the rows its run stored. A loaded node itself is
empty until it runs (ITL-1095). A file without those records is malformed and refused.
"""

from __future__ import annotations

import json
import warnings
import pathlib
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import OrcapodDatabaseContentWarning
from orcapod.pipeline import Pipeline, SyncPipelineOrchestrator
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
    return ArrowTableSource(
        table, tag_columns=["id"], source_id=f"src_{column}", tag_only_columns=True
    )


def _sources(nullability: str) -> dict[str, StreamProtocol]:
    """The job's two sources, keyed by source node name."""
    tag_nullable, data_nullable = NULLABILITY[nullability]
    return {
        "src_a": _source("a", tag_nullable, data_nullable),
        "src_b": _source("b", tag_nullable, data_nullable),
    }


def _store(kind: str, tmp_path: pathlib.Path) -> ArrowDatabaseProtocol:
    """An empty database of the given kind."""
    if kind == "delta":
        return DeltaTableDatabase(base_path=tmp_path / "store")
    return InMemoryArrowDatabase()


def _run(
    store: ArrowDatabaseProtocol, nullability: str, with_function: bool = True
) -> PipelineJob:
    """Run the job with the join stored (``LOG``)."""
    _, data_nullable = NULLABILITY[nullability]
    sources = _sources(nullability)
    job = PipelineJob(name=JOB_NAME, database=store)
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
    loaded = PipelineJob.load(str(saved), database=store)
    loaded.nodes["joined"].attach_databases(
        pipeline_database=store.at(JOB_NAME), cache_mode=CacheMode.REPLAY
    )
    return loaded


def _sorted(table: pa.Table) -> pa.Table:
    """``table`` with its columns in name order and its rows in ``id`` order."""
    return table.select(sorted(table.column_names)).sort_by("id")


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
    loaded = PipelineJob.load(str(_save(job, tmp_path)), database=store)
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
    """A loaded operator node's stored view reads exactly the rows read before saving;
    the node itself is empty until it runs (ITL-1095)."""
    store = _store(store_kind, tmp_path)
    job = _run(store, nullability)
    expected = _sorted(job.nodes["joined"].as_table())
    node = _load_replaying(_save(job, tmp_path), store).nodes["joined"]
    assert node.as_table().num_rows == 0
    stored = node.as_source()
    if read == "as_table":
        assert _sorted(stored.as_table()).equals(expected)
    elif read == "iter_data":
        assert len(list(stored.iter_data())) == expected.num_rows
    else:
        assert _sorted(stored.head().table).equals(expected)


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
            Pipeline.load(saved), database=store, sources=_sources(nullability)
        )
        again.nodes["joined"].attach_databases(
            pipeline_database=store.at(JOB_NAME), cache_mode=cache_mode
        )
        return again

    replaying = rebuilt(CacheMode.REPLAY)
    _ = replaying.run()  # a node shows rows only after a run (ITL-1095)
    assert _sorted(replaying.nodes["joined"].as_table()).equals(expected)
    _ = rebuilt(CacheMode.LOG).run()
    assert set(store.at(JOB_NAME).list_table_paths()) == tables_after_run


# ---------------------------------------------------------------------------
# A file without the fields: lossy, but never stored rows shown as none
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# A file without a record is malformed: refused, never loaded lossily
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "record"),
    [
        ("src_a", "output_schema"),
        ("src_a", "source_config"),
        ("src_b", "source_config"),
        ("inc", "output_schema"),
        ("joined", "output_schema"),
    ],
)
def test_a_file_without_a_nullability_record_is_refused(
    label: str, record: str, tmp_path: pathlib.Path
) -> None:
    """A job file, and the pipeline file it embeds, without one node's record (a source
    slot records it in its output schema and its source config)."""
    job = _run(InMemoryArrowDatabase(), "nullable_tag_and_data")

    def drop(pipeline: dict[str, Any]) -> None:
        (descriptor,) = [d for d in pipeline["nodes"].values() if d["label"] == label]
        del descriptor[record]["nullable"]

    saved = _save(job, tmp_path, lambda data: drop(data["pipeline"]))
    with pytest.raises(ValueError, match=f"saved node {label!r}") as caught:
        _ = PipelineJob.load(str(saved))
    assert f"no {record}.nullable record" in str(caught.value)
    pipeline_path = tmp_path / "pipeline.json"
    job.pipeline.save(pipeline_path)
    data = json.loads(pipeline_path.read_text())
    drop(data)
    pipeline_path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match=f"no {record}.nullable record"):
        _ = Pipeline.load(pipeline_path)


# What a node's entry in run.node_job_content_hashes is replaced with: removed, null, or
# a value that is not a hash text (CodeRabbit on PR 454).
RUN_HASH_DEFECTS: dict[str, Callable[[dict[str, Any], str], None]] = {
    "missing": lambda hashes, label: hashes.pop(label),
    "null": lambda hashes, label: hashes.__setitem__(label, None),
    "number": lambda hashes, label: hashes.__setitem__(label, 7),
}


@pytest.mark.parametrize("defect", list(RUN_HASH_DEFECTS))
@pytest.mark.parametrize("labels", [["joined"], ["inc"], ["inc", "joined"]])
def test_a_job_file_without_a_run_content_hash_is_refused(
    labels: list[str], defect: str, tmp_path: pathlib.Path
) -> None:
    """Every function and operator node records its run's content hash as text."""

    def drop(data: dict[str, Any]) -> None:
        for label in labels:
            RUN_HASH_DEFECTS[defect](data["run"]["node_job_content_hashes"], label)

    saved = _save(_run(InMemoryArrowDatabase(), "non_null"), tmp_path, drop)
    with pytest.raises(ValueError, match="have no run content hash") as caught:
        _ = PipelineJob.load(str(saved))
    assert f"saved nodes {sorted(labels)}" in str(caught.value)


# ---------------------------------------------------------------------------
# The database is the source of truth: a known key with no rows is an empty result
# ---------------------------------------------------------------------------


def _live_replaying(store: ArrowDatabaseProtocol, job: PipelineJob) -> Any:
    """The join of ``job``'s definition rebuilt from code, in REPLAY over ``store``."""
    rebuilt = PipelineJob.from_pipeline(job.pipeline, database=store)
    rebuilt.bind(sources=job.sources)
    node = rebuilt.nodes["joined"]
    node.attach_databases(
        pipeline_database=store.at(JOB_NAME), cache_mode=CacheMode.REPLAY
    )
    return node


def _assert_reads_as_live_with_no_rows(
    loaded_node: Any, live_node: Any, *, same_types: bool = True
) -> None:
    """The loaded node's stored view reads what a running node's reads from the same
    store: no rows.

    ``same_types=False`` where the loaded node's input slots are unbound: a running
    node types its zero-row table from its sources (ITL-912), while the loaded node has
    only their Python schema, so only the user columns are compared (ITL-1089).
    """
    user_columns = [*live_node.keys()[0], *live_node.keys()[1]]
    loaded_stored = loaded_node.as_source()
    live_stored = live_node.as_source()
    for config in ({}, {"all_info": True}):
        table = loaded_stored.as_table(**config)
        live = live_stored.as_table(**config)
        assert table.num_rows == 0
        if same_types:
            assert table.schema.equals(live.schema), (table.schema, live.schema)
        else:
            assert [n for n in table.column_names if n in user_columns] == [
                n for n in live.column_names if n in user_columns
            ]
    assert list(loaded_stored.iter_data()) == []
    head = loaded_stored.head()
    assert head.table.num_rows == 0
    assert head.status is live_stored.head().status


def test_a_store_holding_only_other_runs_rows_gives_an_empty_result(
    tmp_path: pathlib.Path,
) -> None:
    """With its key known, a database without the run's rows answers: no rows."""
    saved_store = InMemoryArrowDatabase()
    job = _run(saved_store, "non_null")
    saved = _save(job, tmp_path)
    # Another database whose table at the same path holds another run's rows.
    other_store = InMemoryArrowDatabase()
    other = PipelineJob(name=JOB_NAME, database=other_store)
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
            tag_only_columns=True,
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
    # The job file records the run's 2 rows: an advisory warning, not an error.
    with pytest.warns(OrcapodDatabaseContentWarning, match="records 2 rows .* holds 0"):
        _assert_reads_as_live_with_no_rows(node, _live_replaying(other_store, job))


@pytest.mark.parametrize("nullability", list(NULLABILITY))
def test_an_absent_cache_table_gives_an_empty_result(
    nullability: str, tmp_path: pathlib.Path
) -> None:
    """With its key known, a database without the table answers: no rows, typed."""
    job = _run(InMemoryArrowDatabase(), nullability)
    empty_store = InMemoryArrowDatabase()
    node = _load_replaying(_save(job, tmp_path), empty_store).nodes["joined"]
    # The job file records the run's 2 rows: an advisory warning, not an error.
    with pytest.warns(OrcapodDatabaseContentWarning, match="records 2 rows .* holds 0"):
        _assert_reads_as_live_with_no_rows(
            node, _live_replaying(empty_store, job), same_types=False
        )


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "ITL-1089: a loaded node over an unbound input slot has only a Python schema, "
        "so its zero-row table cannot have the types a running node's has (ITL-912)"
    ),
)
def test_an_absent_cache_table_gives_the_running_nodes_types(
    tmp_path: pathlib.Path,
) -> None:
    """The loaded node's zero-row table should have the running node's exact schema."""
    job = _run(InMemoryArrowDatabase(), "non_null")
    empty_store = InMemoryArrowDatabase()
    node = _load_replaying(_save(job, tmp_path), empty_store).nodes["joined"]
    live = _live_replaying(empty_store, job)
    # Neither node has run: both read their zero-row tables, and neither reads the
    # store (ITL-1095), so the job file's row count is not checked here.
    table = node.as_table(all_info=True)
    assert table.schema.equals(live.as_table(all_info=True).schema)


def _run_empty_join(store: ArrowDatabaseProtocol) -> PipelineJob:
    """Run the operator-only job over inputs with no common ``id``: an empty join."""

    def source(column: str, ids: list[int], source_id: str) -> ArrowTableSource:
        """A non-null source with tag ``id`` and data ``column``."""
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field(column, pa.int64(), nullable=False),
            ]
        )
        table = pa.table({"id": ids, column: [1] * len(ids)}, schema=schema)
        return ArrowTableSource(
            table, tag_columns=["id"], source_id=source_id, tag_only_columns=True
        )

    job = PipelineJob(name=JOB_NAME, database=store)
    with job:
        _ = Join()(
            source("a", [0, 1], "src_a"),
            source("b", [7, 8], "src_b_disjoint"),
            label="joined",
        )
    # OperatorNode.attach_databases is the public way to turn on LOG / REPLAY mode.
    job.nodes["joined"].attach_databases(
        pipeline_database=store.at(JOB_NAME), cache_mode=CacheMode.LOG
    )
    _ = job.run()
    return job


@pytest.mark.parametrize("other_runs_stored", [False, True])
def test_a_loaded_run_with_an_empty_result_reads_as_empty(
    other_runs_stored: bool, tmp_path: pathlib.Path
) -> None:
    """A run whose result was empty (it stores nothing) reads back as empty."""
    store = InMemoryArrowDatabase()
    if other_runs_stored:
        # Another run of the same pipeline stores rows in the shared table.
        _ = _run(store, "non_null", with_function=False)
    job = _run_empty_join(store)
    assert job.nodes["joined"].as_table().num_rows == 0
    node = _load_replaying(_save(job, tmp_path), store).nodes["joined"]
    # With other runs' rows the table exists and both read the stored types; without,
    # the loaded node's unbound slots leave it the Python schema (ITL-1089).
    _assert_reads_as_live_with_no_rows(
        node, _live_replaying(store, job), same_types=other_runs_stored
    )


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
    PipelineJob.load(str(saved), database=store).save(resaved)
    assert _recorded_fields(resaved) == _recorded_fields(saved)


def test_a_source_proxy_keeps_the_nullability_of_the_source_it_stands_for() -> None:
    """A source rebuilt as a ``SourceProxy`` from its config has the source's schema."""
    from orcapod.pipeline.serialization import resolve_source_from_config

    source = _source("a", tag_nullable=True, data_nullable=False)
    config = {**source.to_config(), "source_type": "no_such_source_type"}
    proxy = resolve_source_from_config(config, fallback_to_proxy=True)
    assert proxy.output_schema() == source.output_schema()
    assert proxy.pipeline_hash() == source.pipeline_hash()


# ---------------------------------------------------------------------------
# get_cached_output on a loaded node (CodeRabbit, PR #352)
# ---------------------------------------------------------------------------


def test_get_cached_output_of_a_loaded_node_reads_its_runs_rows(
    tmp_path: pathlib.Path,
) -> None:
    """``get_cached_output()`` on a loaded REPLAY node returns its run's rows."""
    store = InMemoryArrowDatabase()
    job = _run(store, "nullable_tag")
    node = _load_replaying(_save(job, tmp_path), store).nodes["joined"]
    cached = node.get_cached_output()
    assert cached is not None
    assert _sorted(cached.as_table()).equals(_sorted(job.nodes["joined"].as_table()))


# ---------------------------------------------------------------------------
# run.expected_rows: an optional, advisory count; the database stays the truth
# ---------------------------------------------------------------------------

READ_NODES = ["inc", "joined"]


def _read_all(job: PipelineJob, label: str) -> pa.Table:
    """Read a loaded node's stored view three ways and return its table (the node itself
    is empty until it runs, ITL-1095)."""
    stored = job.nodes[label].as_source()
    _ = list(stored.iter_data())
    _ = stored.head()
    return stored.as_table()


def _loaded(saved: pathlib.Path, store: ArrowDatabaseProtocol) -> PipelineJob:
    """Load the job; its join replays (the file does not record its cache mode)."""
    return _load_replaying(saved, store)


def test_the_job_file_records_the_rows_each_node_has_in_the_store(
    tmp_path: pathlib.Path,
) -> None:
    """``run.expected_rows``: the run's rows under each node's key, in its own section."""
    data = json.loads(
        _save(_run(InMemoryArrowDatabase(), "non_null"), tmp_path).read_text()
    )
    assert data["run"]["expected_rows"] == {"inc": 2, "joined": 2}


@pytest.mark.parametrize("label", READ_NODES)
@pytest.mark.parametrize("store_kind", STORES)
def test_fewer_rows_than_expected_warn_once_and_read_what_the_store_holds(
    store_kind: str, label: str, tmp_path: pathlib.Path
) -> None:
    """Another database with none of the run's rows: no rows, one warning per node."""
    saved = _save(_run(InMemoryArrowDatabase(), "non_null"), tmp_path)
    loaded = _loaded(saved, _store(store_kind, tmp_path))
    with pytest.warns(OrcapodDatabaseContentWarning) as record:
        table = _read_all(loaded, label)
        _ = _read_all(loaded, label)
    assert table.num_rows == 0
    messages = [
        str(w.message) for w in record if w.category is OrcapodDatabaseContentWarning
    ]
    assert len(messages) == 1
    assert f"Node {label!r}: the job file records 2 rows for its run" in messages[0]
    assert "holds 0 under the run's key" in messages[0]


@pytest.mark.parametrize("label", READ_NODES)
@pytest.mark.parametrize("store_kind", STORES)
def test_more_rows_than_expected_warn_and_read_them_all(
    store_kind: str, label: str, tmp_path: pathlib.Path
) -> None:
    """The database holds more than the file records: all of them, one warning."""
    store = _store(store_kind, tmp_path)
    job = _run(store, "non_null")

    def expect_one(data: dict[str, Any]) -> None:
        """Record one row where the run left two."""
        data["run"]["expected_rows"][label] = 1

    loaded = _loaded(_save(job, tmp_path, expect_one), store)
    with pytest.warns(
        OrcapodDatabaseContentWarning, match="records 1 row for its run.*holds 2"
    ):
        table = _read_all(loaded, label)
    assert _sorted(table).equals(_sorted(job.nodes[label].as_table()))


@pytest.mark.parametrize("store_kind", STORES)
def test_matching_counts_and_zero_expected_zero_found_do_not_warn(
    store_kind: str, tmp_path: pathlib.Path
) -> None:
    """No warning when the database holds what the file records, zero included."""
    store = _store(store_kind, tmp_path)
    job = _run(store, "non_null")
    empty = _run_empty_join(_store("memory", tmp_path))
    with warnings.catch_warnings():
        warnings.simplefilter("error", OrcapodDatabaseContentWarning)
        loaded = _loaded(_save(job, tmp_path), store)
        for label in READ_NODES:
            assert _read_all(loaded, label).num_rows == 2
        empty_saved = _save(empty, tmp_path / "empty")
        assert json.loads(empty_saved.read_text())["run"]["expected_rows"] == {
            "joined": 0
        }
        node = _load_replaying(empty_saved, InMemoryArrowDatabase()).nodes["joined"]
        assert node.as_source().as_table().num_rows == 0


def test_deleting_the_section_by_hand_loads_identically_without_a_check(
    tmp_path: pathlib.Path,
) -> None:
    """Without ``run.expected_rows`` every read gives the same rows and no warning."""
    store = InMemoryArrowDatabase()
    job = _run(store, "nullable_tag_and_data")

    def delete_section(data: dict[str, Any]) -> None:
        """Delete the advisory section."""
        del data["run"]["expected_rows"]

    with_section = _loaded(_save(job, tmp_path), store)
    without = _loaded(_save(job, tmp_path / "bare", delete_section), store)
    other = _loaded(
        _save(job, tmp_path / "bare2", delete_section), InMemoryArrowDatabase()
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error", OrcapodDatabaseContentWarning)
        for label in READ_NODES:
            assert _sorted(_read_all(without, label)).equals(
                _sorted(_read_all(with_section, label))
            )
            assert _read_all(other, label).num_rows == 0


def test_a_run_that_did_not_finish_records_no_counts(tmp_path: pathlib.Path) -> None:
    """Counts are written only when certain: not before a run, nor after a failed one."""
    store = InMemoryArrowDatabase()
    sources = _sources("non_null")
    job = PipelineJob(name=JOB_NAME, database=store)
    with job:
        _ = Join()(sources["src_a"], sources["src_b"], label="joined")
    pending = _save(job, tmp_path / "pending")
    assert "expected_rows" not in json.loads(pending.read_text())["run"]

    @function_pod(output_keys="a1")
    def fails(a: int) -> int:
        """Fail on every row."""
        raise ValueError(f"no {a}")

    failing = PipelineJob(name=JOB_NAME, database=store)
    with failing:
        _ = fails.pod(_sources("non_null")["src_a"], label="fails")
    with pytest.raises(ValueError, match="no 0|no 1"):
        _ = failing.run(orchestrator=SyncPipelineOrchestrator(error_policy="fail_fast"))
    failed = _save(failing, tmp_path / "failed")
    assert "expected_rows" not in json.loads(failed.read_text())["run"]


# ---------------------------------------------------------------------------
# CodeRabbit round 2 (PR #352)
# ---------------------------------------------------------------------------


def test_get_all_records_of_a_loaded_operator_runs_the_advisory_check(
    tmp_path: pathlib.Path,
) -> None:
    """``get_all_records()`` warns once, like the other reads, and returns no rows."""
    saved = _save(_run(InMemoryArrowDatabase(), "non_null"), tmp_path)
    node = _loaded(saved, InMemoryArrowDatabase()).nodes["joined"]
    with pytest.warns(OrcapodDatabaseContentWarning) as record:
        assert node.get_all_records() is None
        assert node.get_all_records() is None
    assert len([w for w in record if w.category is OrcapodDatabaseContentWarning]) == 1
