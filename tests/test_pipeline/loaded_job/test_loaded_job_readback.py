"""Reading the nodes of a job loaded with ``PipelineJob.load`` (ITL-904).

The main job is saved in a separate process (``save_job_script.py``) and loaded here, so
these tests see exactly what a user sees when loading a job in a new session: the function
pods and the in-memory sources of the saving process do not exist. Tests of one specific
shape (a datetime or ``Path`` output, ephemeral results) save their own small job in this
process; loading never touches the objects that built it.
"""

from __future__ import annotations

import datetime
import json
import logging
import pathlib
import subprocess
import sys
import uuid
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline.serialization import LoadStatus
from orcapod.sources import DictSource
from orcapod.types import NodeConfig

SAVE_SCRIPT = pathlib.Path(__file__).with_name("save_job_script.py")
FUNCTION_NODES = ("dbl", "inc", "half", "sq")
TAG_OF = {"dbl": "id", "inc": "id", "half": "id", "sq": "element_index"}


@pytest.fixture(scope="module")
def saved(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    """Directory holding a job saved by a separate Python process."""
    root = tmp_path_factory.mktemp("saved_job")
    _ = subprocess.run([sys.executable, str(SAVE_SCRIPT), str(root)], check=True)
    return root


def _load(saved: pathlib.Path, **kwargs: Any) -> PipelineJob:
    """Load the job saved in *saved*."""
    return PipelineJob.load(str(saved / "job.json"), **kwargs)


def _sorted(table: pa.Table, tag: str) -> pa.Table:
    """Return *table* sorted by the tag column *tag*."""
    return table.sort_by([(tag, "ascending")])


def _rewrite_job_file(
    saved: pathlib.Path,
    tmp_path: pathlib.Path,
    edit: Callable[[dict[str, Any]], None],
) -> str:
    """Write a copy of the saved job file, changed by *edit*, and return its path."""
    data: dict[str, Any] = json.loads((saved / "job.json").read_text())
    edit(data)
    path = tmp_path / "edited_job.json"
    _ = path.write_text(json.dumps(data))
    return str(path)


def _dict_source() -> DictSource:
    """Return a two-row in-memory source with tag ``id`` and data ``x``."""
    return DictSource(
        [{"id": 0, "x": 5}, {"id": 1, "x": 10}], tag_columns=["id"], source_id="dsrc"
    )


def _save_small_job(
    tmp_path: pathlib.Path,
    record: Callable[[], None],
    configure: Callable[[PipelineJob], None] | None = None,
) -> str:
    """Record, run and save a job on a Delta store in *tmp_path*; return its path.

    Args:
        tmp_path: Directory for the store and the saved file.
        record: Called inside the job's ``with`` block to record the pipeline.
        configure: Called after recording and before the run, to configure nodes.
    """
    job = PipelineJob(name="small", store=DeltaTableDatabase(str(tmp_path / "store")))
    with job:
        record()
    if configure is not None:
        configure(job)
    _ = job.run()
    path = tmp_path / "small_job.json"
    job.save(str(path))
    return str(path)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def test_load_does_not_crash_when_an_operator_consumes_a_function_node(
    saved: pathlib.Path,
) -> None:
    """A job whose Join consumes a function node loads."""
    job = _load(saved)
    assert {"dbl", "joined", "inc", "half", "sq"} <= set(job.nodes)


def test_loaded_function_node_output_schema_has_python_types(
    saved: pathlib.Path,
) -> None:
    """A loaded node reports Python types, not saved type strings."""
    tag_schema, data_schema = _load(saved).nodes["dbl"].output_schema()
    assert dict(tag_schema) == {"id": int}
    assert dict(data_schema) == {"dbl": int}


def test_datetime_output_feeding_an_operator_loads_with_its_type(
    tmp_path: pathlib.Path,
) -> None:
    """A ``datetime`` output, saved as ``timestamp[us, tz=UTC]``, loads as ``datetime``."""

    @function_pod(output_keys="t")
    def stamp(x: int) -> datetime.datetime:
        """Return a UTC timestamp derived from *x*."""
        return datetime.datetime(2026, 1, x % 28 + 1, tzinfo=datetime.timezone.utc)

    other = DictSource([{"id": 0, "y": 1}, {"id": 1, "y": 2}], tag_columns=["id"])

    def record() -> None:
        """Record the pipeline under test."""
        _ = Join()(stamp.pod(_dict_source(), label="stamp"), other, label="joined")

    path = _save_small_job(tmp_path, record)
    node = PipelineJob.load(path).nodes["stamp"]
    assert dict(node.output_schema()[1]) == {"t": datetime.datetime}
    assert node.as_table().num_rows == 2


def test_datetime_output_with_nothing_stored_returns_typed_zero_rows(
    tmp_path: pathlib.Path,
) -> None:
    """The zero-row table of a ``datetime`` output has a timestamp column."""

    @function_pod(output_keys="t")
    def stamp(x: int) -> datetime.datetime:
        """Return a UTC timestamp derived from *x*."""
        return datetime.datetime(2026, 1, x % 28 + 1, tzinfo=datetime.timezone.utc)

    def record() -> None:
        """Record the pipeline under test."""
        _ = stamp.pod(_dict_source(), label="stamp")

    path = _save_small_job(tmp_path, record)
    data = json.loads(pathlib.Path(path).read_text())
    data["run"]["node_has_stored_records"]["stamp"] = False
    _ = pathlib.Path(path).write_text(json.dumps(data))
    table = (
        PipelineJob.load(path, store=InMemoryArrowDatabase()).nodes["stamp"].as_table()
    )
    assert table.num_rows == 0
    assert pa.types.is_timestamp(table.schema.field("t").type)


def test_logical_type_output_schema_fails_naming_the_column_and_saved_type(
    tmp_path: pathlib.Path,
) -> None:
    """A ``Path`` output's schema cannot be restored yet: the error says which column."""

    @function_pod(output_keys="p")
    def to_path(x: int) -> pathlib.Path:
        """Return a path derived from *x*."""
        return pathlib.Path(f"/data/{x}")

    def record() -> None:
        """Record the pipeline under test."""
        _ = to_path.pod(_dict_source(), label="paths")

    node = PipelineJob.load(_save_small_job(tmp_path, record)).nodes["paths"]
    with pytest.raises(ValueError) as excinfo:
        _ = node.output_schema()
    message = str(excinfo.value)
    assert message.startswith(
        "Cannot restore the output schema of loaded node 'paths': Column 'p' has the "
        "saved type 'extension<orcapod.path"
    )
    assert message.endswith("which cannot be restored to a Python type.")
    # The stored rows are still readable, with their stored logical type.
    table = node.as_table()
    assert table.num_rows == 2
    assert isinstance(table.schema.field("p").type, pa.ExtensionType)


# ---------------------------------------------------------------------------
# Function nodes: stored rows
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("label", FUNCTION_NODES)
def test_loaded_function_node_as_table_returns_stored_rows(
    saved: pathlib.Path, label: str
) -> None:
    """A loaded node's table equals the live table: schema and values."""
    expected = pq.read_table(saved / "expected" / f"{label}.parquet")
    table = _load(saved).nodes[label].as_table()
    assert table.schema == expected.schema
    assert _sorted(table, TAG_OF[label]).equals(_sorted(expected, TAG_OF[label]))


@pytest.mark.parametrize("label", FUNCTION_NODES)
def test_loaded_function_node_as_table_all_info_has_stream_columns(
    saved: pathlib.Path, label: str
) -> None:
    """With ``all_info``, a loaded node's table has every live column and more."""
    # The loaded table follows the documented stream semantics: every column of the
    # live table, with the same values and types, plus the data context and the
    # source-info columns. (The live FunctionJobNode.as_table() drops those two
    # groups -- a separate, pre-existing bug the loaded node does not copy.)
    expected = pq.read_table(saved / "expected" / f"{label}.all_info.parquet")
    table = _load(saved).nodes[label].as_table(all_info=True)
    data_column = next(iter(_load(saved).nodes[label].output_schema()[1]))
    assert "_context_key" in table.column_names
    assert f"_source_{data_column}" in table.column_names
    for name in expected.column_names:
        assert table.schema.field(name).type == expected.schema.field(name).type
    common = _sorted(table, TAG_OF[label]).select(expected.column_names)
    assert common.to_pylist() == _sorted(expected, TAG_OF[label]).to_pylist()


def test_loaded_function_node_iter_data_yields_stored_rows(saved: pathlib.Path) -> None:
    """``iter_data()`` yields the stored (tag, data) pairs."""
    rows = sorted(
        (
            (tag.as_dict(), data.as_dict())
            for tag, data in _load(saved).nodes["dbl"].iter_data()
        ),
        key=lambda row: row[0]["id"],
    )
    assert rows == [({"id": 0}, {"dbl": 10}), ({"id": 1}, {"dbl": 20})]


def test_loaded_function_node_get_all_records_returns_stored_rows(
    saved: pathlib.Path,
) -> None:
    """``get_all_records()`` still returns the stored rows."""
    records = _load(saved).nodes["dbl"].get_all_records()
    assert records is not None
    assert records.num_rows == 2


def test_loaded_function_node_with_store_is_read_only(saved: pathlib.Path) -> None:
    """With a store attached, a loaded node is READ_ONLY."""
    assert _load(saved).nodes["dbl"].load_status == LoadStatus.READ_ONLY


def test_several_records_for_one_entry_are_read_once(tmp_path: pathlib.Path) -> None:
    """``as_table()`` keeps one row per entry, as ``iter_data()`` does."""

    @function_pod(output_keys="dbl")
    def double(x: int) -> int:
        """Double *x*."""
        return 2 * x

    def record() -> None:
        """Record the pipeline under test."""
        _ = double.pod(_dict_source(), label="dbl")

    path = _save_small_job(tmp_path, record)
    node = PipelineJob.load(path).nodes["dbl"]
    # Non-public on purpose: no public call writes a second pipeline record for an
    # entry in a test (a recomputation does), so append one to the stored table.
    entry_id_col = "__pipeline_entry_id"
    database = node._pipeline_database
    assert database is not None
    table_path = node._versioned_pipeline_path
    records = database.get_all_records(table_path, record_id_column=entry_id_col)
    assert records is not None
    extra = records.slice(0, 1)
    extra = extra.set_column(
        extra.schema.get_field_index(entry_id_col),
        extra.schema.field(entry_id_col),
        pa.array([uuid.uuid4().bytes], type=extra.schema.field(entry_id_col).type),
    )
    index_col = "__pipeline_recomputation_index"
    extra = extra.set_column(
        extra.schema.get_field_index(index_col),
        extra.schema.field(index_col),
        pa.array([1], type=extra.schema.field(index_col).type),
    )
    database.add_records(table_path, extra, record_id_column=entry_id_col, flush=True)

    reloaded = PipelineJob.load(path).nodes["dbl"]
    assert reloaded.as_table().num_rows == 2
    assert len(list(reloaded.iter_data())) == 2


def test_loaded_node_with_ephemeral_results_does_not_crash(
    tmp_path: pathlib.Path,
) -> None:
    """Ephemeral results come back as placeholders; the table says why it has none."""

    @function_pod(output_keys="dbl")
    def double(x: int) -> int:
        """Double *x*."""
        return 2 * x

    def record() -> None:
        """Record the pipeline under test."""
        _ = double.pod(_dict_source(), label="dbl")

    def make_ephemeral(job: PipelineJob) -> None:
        """Keep the node's results in an in-memory ephemeral store."""
        node = job.nodes["dbl"]
        node.node_config = NodeConfig(is_result_ephemeral=True)
        node.set_ephemeral_store(InMemoryArrowDatabase())

    path = _save_small_job(tmp_path, record, configure=make_ephemeral)
    node = PipelineJob.load(path).nodes["dbl"]
    assert [type(data).__name__ for _, data in node.iter_data()] == [
        "EmptyData",
        "EmptyData",
    ]
    with pytest.raises(RuntimeError) as excinfo:
        _ = node.as_table()
    assert str(excinfo.value) == (
        "FunctionJobNode 'dbl': the saved run kept 2 of its results in an ephemeral "
        "store, which is not saved with the job, so they cannot be read into a table. "
        "iter_data() yields them as EmptyData placeholders."
    )


# ---------------------------------------------------------------------------
# Function nodes: the store
# ---------------------------------------------------------------------------


def test_loaded_function_node_saved_with_records_raises_on_a_store_without_them(
    saved: pathlib.Path,
) -> None:
    """A store lacking the saved run's records raises, on every read method."""
    # The saved run stored this node's records, so a store without them is not the
    # store the job ran against: zero rows would make the results look lost.
    node = _load(saved, store=InMemoryArrowDatabase()).nodes["dbl"]
    reads = (node.as_table, lambda: list(node.iter_data()), node.get_all_records)
    for read in reads:
        with pytest.raises(RuntimeError) as excinfo:
            _ = read()
        message = str(excinfo.value)
        assert message.startswith(
            "FunctionJobNode 'dbl': the saved run stored results for this node, but "
            "the attached in_memory store has no table 'loaded_demo/double/"
        )
        assert message.endswith(
            "Load the job with the store it ran against: "
            "PipelineJob.load(path, store=...)."
        )


def test_mismatched_delta_store_error_names_the_store_location(
    saved: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """The mismatched-store error names a Delta store's location."""
    other = tmp_path / "other_store"
    node = _load(saved, store=DeltaTableDatabase(str(other))).nodes["dbl"]
    with pytest.raises(RuntimeError, match="the attached store at '") as excinfo:
        _ = node.as_table()
    assert str(other) in str(excinfo.value)


def test_saved_file_records_which_nodes_had_stored_records(
    saved: pathlib.Path,
) -> None:
    """``save()`` records, per function node, that its records were stored."""
    run = json.loads((saved / "job.json").read_text())["run"]
    assert run["node_has_stored_records"] == {
        "dbl": True,
        "inc": True,
        "half": True,
        "sq": True,
    }


def test_saving_a_loaded_job_keeps_the_stored_record_flags(
    saved: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """Re-saving a job loaded with the wrong store keeps the saved expectations."""
    resaved = tmp_path / "resaved.json"
    _load(saved, store=InMemoryArrowDatabase()).save(str(resaved))
    run = json.loads(resaved.read_text())["run"]
    assert run["node_has_stored_records"]["dbl"] is True
    node = PipelineJob.load(str(resaved), store=InMemoryArrowDatabase()).nodes["dbl"]
    with pytest.raises(RuntimeError, match="the saved run stored results"):
        _ = node.as_table()


def test_loaded_function_node_with_nothing_stored_returns_zero_rows(
    saved: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """A node saved with nothing stored reads zero rows with its columns."""

    def nothing_stored(data: dict[str, Any]) -> None:
        """Mark the node as saved with nothing stored."""
        data["run"]["node_has_stored_records"]["dbl"] = False

    path = _rewrite_job_file(saved, tmp_path, nothing_stored)
    node = PipelineJob.load(path, store=InMemoryArrowDatabase()).nodes["dbl"]
    table = node.as_table()
    assert table.num_rows == 0
    assert table.column_names == ["id", "dbl"]
    assert list(node.iter_data()) == []


def test_file_saved_without_the_stored_records_field_reads_zero_rows(
    saved: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """A file saved before the field existed loads as before."""

    # A store without the node's records gives zero rows.
    def drop_field(data: dict[str, Any]) -> None:
        """Remove the stored-records field, as in an older file."""
        del data["run"]["node_has_stored_records"]

    path = _rewrite_job_file(saved, tmp_path, drop_field)
    job = PipelineJob.load(path, store=InMemoryArrowDatabase())
    assert job.nodes["dbl"].as_table().num_rows == 0
    assert PipelineJob.load(path).nodes["dbl"].as_table().num_rows == 2


def test_loaded_function_node_without_store_says_how_to_attach_one(
    saved: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """Without a store, every read method says how to attach one."""

    def drop_store(data: dict[str, Any]) -> None:
        """Remove the saved store configuration."""
        data["bindings"]["store"] = None

    job = PipelineJob.load(_rewrite_job_file(saved, tmp_path, drop_store))
    node = job.nodes["dbl"]
    assert node.load_status == LoadStatus.UNAVAILABLE
    reads = (node.as_table, lambda: list(node.iter_data()), node.get_all_records)
    for read in reads:
        with pytest.raises(RuntimeError) as excinfo:
            _ = read()
        message = str(excinfo.value)
        assert "was loaded without a store" in message
        assert "PipelineJob.load(path, store=...)" in message
        assert "job.bind(store=...)" in message
        assert "no database attached" not in message


def test_binding_a_store_makes_a_storeless_loaded_node_readable(
    saved: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """``job.bind(store=...)`` makes a loaded node readable."""

    def drop_store(data: dict[str, Any]) -> None:
        """Remove the saved store configuration."""
        data["bindings"]["store"] = None

    job = PipelineJob.load(_rewrite_job_file(saved, tmp_path, drop_store))
    job.bind(store=DeltaTableDatabase(str(saved / "store")))
    assert job.nodes["dbl"].as_table().num_rows == 2


def test_loaded_job_still_cannot_run(saved: pathlib.Path) -> None:
    """A loaded job is read-only: ``run()`` raises."""
    with pytest.raises(RuntimeError, match="cannot execute a loaded job"):
        _ = _load(saved).run()


# ---------------------------------------------------------------------------
# Operator nodes
# ---------------------------------------------------------------------------


def test_loaded_operator_node_raises_instead_of_returning_empty(
    saved: pathlib.Path,
) -> None:
    """A loaded operator node, with nothing stored, raises and names its inputs."""
    node = _load(saved).nodes["joined"]
    expected = (
        "OperatorJobNode 'joined' has no stored results: a PipelineJob does not store "
        "operator outputs, so a loaded job cannot show them. Read the stored results of "
        "its input nodes ('dbl', 'asrc') instead."
    )
    for read in (node.as_table, lambda: list(node.iter_data())):
        with pytest.raises(RuntimeError) as excinfo:
            _ = read()
        assert str(excinfo.value) == expected


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------


def test_in_memory_sources_are_logged_at_info_with_the_remedy(
    saved: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """In-memory sources are logged at INFO with the consequence and remedy."""
    caplog.set_level(logging.DEBUG, logger="orcapod.pipeline.job")
    _ = _load(saved)
    job_records = [r for r in caplog.records if r.name == "orcapod.pipeline.job"]
    assert not [r for r in job_records if r.levelno >= logging.WARNING]
    messages = {r.getMessage() for r in job_records if r.levelno == logging.INFO}
    assert (
        "Source 'dsrc' (dict) keeps its data in memory and is not saved with the job; "
        "the loaded job has no data for it. Stored results of all nodes are still "
        "readable. To read source node 'dsrc' itself, bind a source: "
        "job.bind(sources={'dsrc': <source>})."
    ) in messages
    assert any(m.startswith("Source 'asrc' (arrow_table) keeps") for m in messages)
    assert any(m.startswith("Source 'lsrc' (list) keeps") for m in messages)


def test_unreconstructable_file_source_is_a_warning_with_the_reason(
    saved: pathlib.Path, tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A source that fails to rebuild is a WARNING carrying the reason."""
    missing = str(tmp_path / "gone.csv")

    def move_csv(data: dict[str, Any]) -> None:
        """Point the CSV source at a file that does not exist."""
        data["bindings"]["sources"]["csrc"]["file_path"] = missing

    caplog.set_level(logging.INFO, logger="orcapod.pipeline.job")
    job = PipelineJob.load(_rewrite_job_file(saved, tmp_path, move_csv))
    warnings = [
        r.getMessage()
        for r in caplog.records
        if r.name == "orcapod.pipeline.job" and r.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    assert warnings[0].startswith(
        "Could not reconstruct source 'csrc' (csv) from its saved config: "
    )
    assert "gone.csv" in warnings[0]
    assert warnings[0].endswith(
        "Stored results of all nodes are still readable. To read source node 'csrc' "
        "itself, bind a source: job.bind(sources={'csrc': <source>})."
    )
    # The stored results downstream of the missing source are still readable.
    assert job.nodes["inc"].as_table().num_rows == 2


def test_unreconstructable_store_is_a_warning_with_the_remedy(
    saved: pathlib.Path, tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A store that fails to rebuild is a WARNING with the remedy."""

    def break_store(data: dict[str, Any]) -> None:
        """Replace the store configuration with an unknown type."""
        data["bindings"]["store"] = {"type": "no_such_store_type"}

    caplog.set_level(logging.INFO, logger="orcapod.pipeline.job")
    _ = PipelineJob.load(_rewrite_job_file(saved, tmp_path, break_store))
    warnings = [
        r.getMessage()
        for r in caplog.records
        if r.name == "orcapod.pipeline.job" and r.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    assert warnings[0].startswith(
        "Could not reconstruct the store from its saved config: "
    )
    assert warnings[0].endswith(
        "Node results cannot be read until a store is given: "
        "PipelineJob.load(path, store=...) or job.bind(store=...)."
    )


def test_file_source_nodes_are_readable_after_load(saved: pathlib.Path) -> None:
    """CSV and Delta sources are rebuilt, so their nodes are readable."""
    job = _load(saved)
    assert job.nodes["csrc"].as_table().num_rows == 2
    assert job.nodes["tsrc"].as_table().num_rows == 2


def test_in_memory_source_node_is_readable_after_bind(saved: pathlib.Path) -> None:
    """An in-memory source node is readable once a source is bound to it."""
    job = _load(saved)
    job.bind(sources={"dsrc": _dict_source()})
    assert job.nodes["dsrc"].as_table().num_rows == 2
