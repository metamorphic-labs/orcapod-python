"""A derived source carries the bindings of what it reads (ITL-1019, gap audit S1 and Q7).

``node.as_source()`` over a function or operator node whose stored records hold data-tags presents the bindings the stored table states (the marker of ``data_tags``),
checked against the node's keys, in the plain form and the ``tags_as_data`` form. Nothing is
added to the identity for the binding: it is in the origin's content hash and in the output
schema, where a data-tag is in both halves.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.core.operators import Join
from orcapod.core.data_tags import (
    TAG_KIND_METADATA_KEY,
    DataTagMaterializationWarning,
    mark_data_tags,
    without_tag_kind_marks,
)
from orcapod.core.sources import DerivedSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.types import CacheMode, NodeConfig
from orcapod.utils.schema_utils import arrow_schema_identity

JOB = "derived"
DATA_TAG_KEYS = (("unit", "time"), ("unit", "y"))


def _in_memory(_: Path) -> ArrowDatabaseProtocol:
    return InMemoryArrowDatabase()


def _delta(tmp_path: Path) -> ArrowDatabaseProtocol:
    return DeltaTableDatabase(tmp_path / "store")


STORES = pytest.mark.parametrize(
    "make_store", [_in_memory, _delta], ids=["memory", "delta"]
)
ORCHESTRATORS = pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
Orchestrator = type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator]


@op.function_pod(output_keys=["y"])
def double(amp: float) -> float:
    """Double the amplitude; reads no tag, so ``unit`` falls through."""
    return 2 * amp


@op.function_pod(output_keys=["w"])
def scale(unit: int, y: float) -> float:
    """Reads the data-tag ``unit`` as data."""
    return unit * y


def _source(tag_only: object = ("time",), rows: int = 2) -> op.sources.DictSource:
    """``unit`` a data-tag and ``time`` plain unless ``tag_only`` says otherwise."""
    return op.sources.DictSource(
        [{"unit": i, "time": 10 * i, "amp": i - 0.5} for i in range(1, rows + 1)],
        tag_columns=["unit", "time"],
        tag_only_columns=tag_only,  # pyright: ignore[reportArgumentType]  # the test varies it
        source_id="spikes",
    )


def _function_job(
    store: ArrowDatabaseProtocol,
    orchestrator: Orchestrator = SyncPipelineOrchestrator,
    tag_only: object = ("time",),
    run: bool = True,
) -> PipelineJob:
    job = PipelineJob(name=JOB, database=store)
    with job:
        double.pod(_source(tag_only), label="double")
    if run:
        job.run(orchestrator=orchestrator())
    return job


def _operator_job(
    store: ArrowDatabaseProtocol, orchestrator: Orchestrator = SyncPipelineOrchestrator
) -> PipelineJob:
    """A ``LOG`` join of the function node's output with itself filtered by ``unit``."""
    job = PipelineJob(name=JOB, database=store)
    with job:
        doubled = double.pod(_source(), label="double")
        _ = Join()(doubled, _source().select_data_columns(["unit"]), label="joined")
    job.nodes["joined"].attach_databases(
        pipeline_database=store.at(JOB), cache_mode=CacheMode.LOG
    )
    job.run(orchestrator=orchestrator())
    return job


def _marked(schema: pa.Schema) -> list[str]:
    return [f.name for f in schema if TAG_KIND_METADATA_KEY in (f.metadata or {})]


def _by_unit(table: pa.Table) -> list[dict[str, object]]:
    return sorted(table.to_pylist(), key=lambda row: str(row["unit"]))


# ---------------------------------------------------------------------------
# The plain form presents the bindings the stored table states
# ---------------------------------------------------------------------------


@STORES
@ORCHESTRATORS
def test_as_source_of_a_function_node_keeps_its_bindings(
    tmp_path: Path,
    make_store: Callable[[Path], ArrowDatabaseProtocol],
    orchestrator: Orchestrator,
) -> None:
    job = _function_job(make_store(tmp_path), orchestrator)
    node = job.nodes["double"]
    source = node.as_source()
    assert source.keys() == node.keys() == DATA_TAG_KEYS
    assert source.output_schema() == node.output_schema()
    table = source.as_table(all_info=True)
    assert table.column_names.count("unit") == 1
    assert _marked(table.schema) == []
    assert _by_unit(source.as_table()) == _by_unit(node.as_table())
    for tag, data in source.iter_data():
        assert tag["unit"] == data["unit"]
    head = source.head()
    assert head.table.column_names.count("unit") == 1


def test_as_source_keeps_the_bindings_when_results_are_ephemeral() -> None:
    """No persistent result row exists: the pipeline table's marker still holds."""
    store = InMemoryArrowDatabase()
    job = PipelineJob(name=JOB, database=store)
    with job:
        double.pod(_source(), label="double")
    node = job.nodes["double"]
    node.node_config = NodeConfig(is_result_ephemeral=True)
    job.set_ephemeral_database(InMemoryArrowDatabase())
    job.run()
    records = node.get_all_records()
    assert records is not None and records.num_rows == 2
    source = node.as_source()
    assert source.keys() == DATA_TAG_KEYS
    assert sorted(source.as_table().column("y").to_pylist()) == [1.0, 3.0]


@STORES
@ORCHESTRATORS
def test_as_source_of_an_operator_node_keeps_its_bindings(
    tmp_path: Path,
    make_store: Callable[[Path], ArrowDatabaseProtocol],
    orchestrator: Orchestrator,
) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DataTagMaterializationWarning)
        job = _operator_job(make_store(tmp_path), orchestrator)
    node = job.nodes["joined"]
    source = node.as_source()
    assert source.keys() == node.keys()
    assert "unit" in source.keys()[0] and "unit" in source.keys()[1]
    assert _by_unit(source.as_table()) == _by_unit(node.as_table())
    assert _marked(source.as_table(all_info=True).schema) == []


def test_a_derived_source_before_its_node_ran_has_the_binding_and_no_rows() -> None:
    job = _function_job(InMemoryArrowDatabase(), run=False)
    source = job.nodes["double"].as_source()
    assert source.keys() == DATA_TAG_KEYS
    assert source.as_table().num_rows == 0
    assert source.as_table().column_names.count("unit") == 1


def test_a_derived_source_refuses_records_that_disagree_with_the_node() -> None:
    written = InMemoryArrowDatabase()
    _function_job(written)
    tampered = InMemoryArrowDatabase()
    for path in written.list_table_paths():
        table = written.get_all_records(path, record_id_column="__copy_id")
        assert table is not None
        tampered.add_records(
            path,
            without_tag_kind_marks(table),
            record_id_column="__copy_id",
            flush=True,
        )
    source = _function_job(tampered, run=False).nodes["double"].as_source()
    # The node checks its own table first and names it.
    with pytest.raises(
        InputValidationError,
        match=r"pipeline table .* marks the data-tags \[\], but the node's pod "
        r"declares \['unit'\]",
    ):
        _ = source.as_table()


def test_a_derived_source_checks_the_records_it_reads_itself(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The derived source reads the binding from the records, not from the node alone."""
    node = _function_job(InMemoryArrowDatabase()).nodes["double"]
    records = node.get_all_records()
    assert records is not None
    # Records that mark one tag more than the node shares between tag and data.
    wrong = mark_data_tags(records, ["unit", "time"])
    monkeypatch.setattr(node, "get_all_records", lambda *_, **__: wrong)
    with pytest.raises(
        InputValidationError,
        match=r"the stored records mark the data-tags \['time', 'unit'\], but "
        r"the node declares \['unit'\]",
    ):
        _ = node.as_source().as_table()


# ---------------------------------------------------------------------------
# The tags_as_data form
# ---------------------------------------------------------------------------


@ORCHESTRATORS
def test_tags_as_data_over_a_data_tag_node(orchestrator: Orchestrator) -> None:
    job = _function_job(InMemoryArrowDatabase(), orchestrator)
    node = job.nodes["double"]
    source = node.as_source(
        tags_as_data={"time": "start", "unit": "unit_copy"},
        source_id="with_tags_as_data",
    )
    # The data-tag keeps its binding; each copy is a new plain data column.
    assert source.keys() == (("unit", "time"), ("unit", "y", "start", "unit_copy"))
    table = source.as_table(all_info=True)
    assert table.column_names.count("unit") == 1
    assert _marked(table.schema) == []
    # Fresh provenance: every data column, the data-tag included, gets a token.
    assert {"_source_unit", "_source_y", "_source_start", "_source_unit_copy"} <= set(
        table.column_names
    )
    for row in source.as_table().to_pylist():
        assert row["unit_copy"] == row["unit"] and row["start"] == 10 * row["unit"]


def test_tags_as_data_over_a_data_tag_source() -> None:
    source = DerivedSource(
        _source(), tags_as_data={"time": "start"}, source_id="with_tags_as_data"
    )
    assert source.keys() == (("unit", "time"), ("unit", "amp", "start"))
    assert source.as_table().column_names.count("unit") == 1


# ---------------------------------------------------------------------------
# Identity: the binding enters through what is already hashed
# ---------------------------------------------------------------------------


def test_the_binding_enters_the_content_and_pipeline_identity_through_the_schema() -> (
    None
):
    with_data_tags = (
        _function_job(InMemoryArrowDatabase())
        .nodes["double"]
        .as_source(source_id="stored")
    )
    plain = (
        _function_job(InMemoryArrowDatabase(), tag_only=True)
        .nodes["double"]
        .as_source(source_id="stored")
    )
    assert plain.keys() == (("unit", "time"), ("y",))
    tag_schema, data_schema = with_data_tags.arrow_schemas
    assert "unit" in tag_schema.names
    assert "unit" in data_schema.names
    schema_identity = arrow_schema_identity(
        tag_schema, data_schema, with_data_tags.data_context
    )
    assert with_data_tags.source_id == plain.source_id
    assert with_data_tags.identity_structure() == (
        with_data_tags.source_id,
        schema_identity,
        (),
    )
    assert with_data_tags.content_hash() != plain.content_hash()
    assert with_data_tags.pipeline_identity_structure() == schema_identity
    assert with_data_tags.pipeline_hash() != plain.pipeline_hash()


def test_equal_origins_give_equal_derived_sources() -> None:
    first = _function_job(InMemoryArrowDatabase()).nodes["double"].as_source()
    second = _function_job(InMemoryArrowDatabase()).nodes["double"].as_source()
    assert first.content_hash() == second.content_hash()
    assert first.pipeline_hash() == second.pipeline_hash()


# ---------------------------------------------------------------------------
# A pipeline over a derived source
# ---------------------------------------------------------------------------


@ORCHESTRATORS
def test_a_job_over_a_derived_source_reads_the_data_tag(
    orchestrator: Orchestrator,
) -> None:
    upstream = _function_job(InMemoryArrowDatabase(), orchestrator)
    derived = upstream.nodes["double"].as_source()
    job = PipelineJob(name="downstream", database=InMemoryArrowDatabase())
    with job:
        scale.pod(derived, label="scale")
    job.run(orchestrator=orchestrator())
    rows = _by_unit(job.nodes["scale"].as_table())
    assert [row["w"] for row in rows] == [1 * 1.0, 2 * 3.0]
