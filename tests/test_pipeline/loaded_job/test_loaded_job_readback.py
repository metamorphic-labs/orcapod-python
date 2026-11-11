"""Reading the nodes of a job loaded with ``PipelineJob.load`` (ITL-904).

The job is saved in a separate process (``save_job_script.py``) and loaded here, so these
tests see exactly what a user sees when loading a job in a new session: the function pods
and the in-memory sources of the saving process do not exist.
"""

from __future__ import annotations

import json
import logging
import pathlib
import subprocess
import sys
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from orcapod import PipelineJob
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.serialization import LoadStatus
from orcapod.sources import DictSource

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
    return PipelineJob.load(str(saved / "job.json"), **kwargs)


def _sorted(table: pa.Table, tag: str) -> pa.Table:
    return table.sort_by([(tag, "ascending")])


def _rewrite_job_file(
    saved: pathlib.Path,
    tmp_path: pathlib.Path,
    edit: Callable[[dict[str, Any]], None],
) -> str:
    data: dict[str, Any] = json.loads((saved / "job.json").read_text())
    edit(data)
    path = tmp_path / "edited_job.json"
    _ = path.write_text(json.dumps(data))
    return str(path)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def test_load_does_not_crash_when_an_operator_consumes_a_function_node(
    saved: pathlib.Path,
) -> None:
    job = _load(saved)
    assert {"dbl", "joined", "inc", "half", "sq"} <= set(job.nodes)


def test_loaded_function_node_output_schema_has_python_types(
    saved: pathlib.Path,
) -> None:
    tag_schema, data_schema = _load(saved).nodes["dbl"].output_schema()
    assert dict(tag_schema) == {"id": int}
    assert dict(data_schema) == {"dbl": int}


# ---------------------------------------------------------------------------
# Function nodes: stored rows
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("label", FUNCTION_NODES)
def test_loaded_function_node_as_table_returns_stored_rows(
    saved: pathlib.Path, label: str
) -> None:
    expected = pq.read_table(saved / "expected" / f"{label}.parquet")
    table = _load(saved).nodes[label].as_table()
    assert table.schema == expected.schema
    assert _sorted(table, TAG_OF[label]).equals(_sorted(expected, TAG_OF[label]))


@pytest.mark.parametrize("label", FUNCTION_NODES)
def test_loaded_function_node_as_table_all_info_has_stream_columns(
    saved: pathlib.Path, label: str
) -> None:
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
    rows = sorted(
        (
            (tag.as_dict(), data.as_dict())
            for tag, data in _load(saved).nodes["dbl"].iter_data()
        ),
        key=lambda row: row[0]["id"],
    )
    assert rows == [({"id": 0}, {"dbl": 10}), ({"id": 1}, {"dbl": 20})]


def test_loaded_function_node_with_store_is_read_only(saved: pathlib.Path) -> None:
    assert _load(saved).nodes["dbl"].load_status == LoadStatus.READ_ONLY


def test_loaded_function_node_with_nothing_stored_returns_zero_rows(
    saved: pathlib.Path,
) -> None:
    node = _load(saved, store=InMemoryArrowDatabase()).nodes["dbl"]
    table = node.as_table()
    assert table.num_rows == 0
    assert table.column_names == ["id", "dbl"]
    assert list(node.iter_data()) == []


def test_loaded_function_node_without_store_says_how_to_attach_one(
    saved: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    def drop_store(data: dict[str, Any]) -> None:
        data["bindings"]["store"] = None

    job = PipelineJob.load(_rewrite_job_file(saved, tmp_path, drop_store))
    node = job.nodes["dbl"]
    assert node.load_status == LoadStatus.UNAVAILABLE
    for read in (node.as_table, lambda: list(node.iter_data())):
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
    from orcapod.databases import DeltaTableDatabase

    def drop_store(data: dict[str, Any]) -> None:
        data["bindings"]["store"] = None

    job = PipelineJob.load(_rewrite_job_file(saved, tmp_path, drop_store))
    job.bind(store=DeltaTableDatabase(str(saved / "store")))
    assert job.nodes["dbl"].as_table().num_rows == 2


def test_loaded_job_still_cannot_run(saved: pathlib.Path) -> None:
    with pytest.raises(RuntimeError, match="cannot execute a loaded job"):
        _load(saved).run()


# ---------------------------------------------------------------------------
# Operator nodes
# ---------------------------------------------------------------------------


def test_loaded_operator_node_raises_instead_of_returning_empty(
    saved: pathlib.Path,
) -> None:
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
    missing = str(tmp_path / "gone.csv")

    def move_csv(data: dict[str, Any]) -> None:
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
    def break_store(data: dict[str, Any]) -> None:
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
    job = _load(saved)
    assert job.nodes["csrc"].as_table().num_rows == 2
    assert job.nodes["tsrc"].as_table().num_rows == 2


def test_in_memory_source_node_is_readable_after_bind(saved: pathlib.Path) -> None:
    job = _load(saved)
    job.bind(
        sources={
            "dsrc": DictSource(
                [{"id": 0, "x": 5}, {"id": 1, "x": 10}],
                tag_columns=["id"],
                source_id="dsrc",
            )
        }
    )
    assert job.nodes["dsrc"].as_table().num_rows == 2
