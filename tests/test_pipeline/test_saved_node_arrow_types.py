"""A saved job writes each node's own Arrow types (ITL-640).

A node's saved ``output_schema`` text is its Arrow schema (from its typed zero-row
table), never types re-derived from Python ones, so an Arrow type with no Python
counterpart (``time64[ns]``, ``duration[ns]``, a decimal's precision, a fixed-size
binary width) is saved exactly. A loaded job's nodes then type themselves from that
text, and ``node.as_source()`` over a loaded node reads its stored rows with exactly
those types. Two tags and two data columns of each type per stream; sync and async.
"""

from __future__ import annotations

import decimal
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator

TYPES: dict[str, tuple[pa.DataType, list[Any], list[Any]]] = {
    "time64[ns]": (
        pa.time64("ns"),
        [1_000_000_001, 2_000_000_002],
        [3_000_000_003, 4_000_000_004],
    ),
    "duration[ns]": (pa.duration("ns"), [5, 6_000_000_007], [8, 9_000_000_001]),
    "decimal128(38,10)": (
        pa.decimal128(38, 10),
        [decimal.Decimal("1.0000000001"), decimal.Decimal("-2.5")],
        [decimal.Decimal("3.25"), decimal.Decimal("4.0000000004")],
    ),
    "decimal256(76,40)": (
        pa.decimal256(76, 40),
        [decimal.Decimal("1." + "0" * 39 + "1"), decimal.Decimal("2.5")],
        [decimal.Decimal("-3.5"), decimal.Decimal("4." + "0" * 39 + "4")],
    ),
    "fixed_size_binary(3)": (pa.binary(3), [b"abc", b"def"], [b"ghi", b"jkl"]),
}
ORCHESTRATORS: dict[str, Callable[[], Any]] = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}


@function_pod(output_keys="y")
def twice(x: int) -> int:
    """Double the integer data column."""
    return 2 * x


def _source(arrow_type: pa.DataType, first: list[Any], second: list[Any]) -> Any:
    """Tags ``k1``, ``k2`` and data ``d1``, ``d2`` of ``arrow_type``, and data ``x``."""
    columns = {
        "k1": pa.array(first, arrow_type),
        "k2": pa.array(second, arrow_type),
        "d1": pa.array(second, arrow_type),
        "d2": pa.array(first, arrow_type),
        "x": pa.array([1, 2], pa.int64()),
    }
    schema = pa.schema(
        pa.field(name, array.type, nullable=False) for name, array in columns.items()
    )
    return ArrowTableSource(
        pa.Table.from_arrays(list(columns.values()), schema=schema),
        tag_columns=["k1", "k2"],
        tag_only_columns=True,
        source_id="typed",
    )


def _job(database: Any, case: str) -> PipelineJob:
    """A function node (``twice``, the T tags) and an operator node (the T data)."""
    job = PipelineJob(name="arrow_types", database=database)
    with job:
        source = _source(*TYPES[case])
        twice.pod(source.select_data_columns(["x"]), label="twice")
        source.map_data({"d1": "e1", "d2": "e2"}, label="renamed")
    return job


def _types(table: pa.Table, names: list[str]) -> dict[str, pa.DataType]:
    return {name: table.schema.field(name).type for name in names}


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("case", list(TYPES))
def test_saved_text_is_the_arrow_type_and_a_loaded_node_reads_it(
    tmp_path: Path, case: str, orchestrator: str
) -> None:
    arrow_type = TYPES[case][0]
    database = InMemoryArrowDatabase()
    job = _job(database, case)
    job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    path = tmp_path / "job.json"
    job.save(path)

    saved = json.loads(path.read_text())["pipeline"]["nodes"]
    by_label = {node["label"]: node for node in saved.values()}
    text = str(arrow_type)
    assert by_label["twice"]["output_schema"]["tag"] == {"k1": text, "k2": text}
    assert by_label["renamed"]["output_schema"]["data"]["e1"] == text
    assert by_label["renamed"]["output_schema"]["data"]["e2"] == text

    loaded = PipelineJob.load(path, database=database)
    loaded.run(orchestrator=ORCHESTRATORS[orchestrator](), load_only=True)
    expected_twice = job.nodes["twice"].as_source().as_table()
    got_twice = loaded.nodes["twice"].as_source().as_table()
    assert _types(got_twice, ["k1", "k2"]) == {"k1": arrow_type, "k2": arrow_type}
    assert got_twice.sort_by("y").equals(expected_twice.sort_by("y"))

    got_renamed = loaded.nodes["renamed"].as_table()
    assert _types(got_renamed, ["k1", "k2", "e1", "e2"]) == dict.fromkeys(
        ["k1", "k2", "e1", "e2"], arrow_type
    )


def test_an_older_file_with_null_text_loads_and_refuses_only_the_stored_read(
    tmp_path: Path,
) -> None:
    """A file written from Python types holds 'null' for such a column (before ITL-640)."""
    database = InMemoryArrowDatabase()
    job = _job(database, "time64[ns]")
    job.run()
    path = tmp_path / "job.json"
    job.save(path)
    data = json.loads(path.read_text())
    for node in data["pipeline"]["nodes"].values():
        if node["label"] == "twice":
            node["output_schema"]["tag"]["k1"] = "null"
    path.write_text(json.dumps(data))

    loaded = PipelineJob.load(path, database=database)
    assert loaded.nodes["twice"].label == "twice"
    with pytest.raises(ValueError, match="'k1' has the saved type 'null'"):
        loaded.nodes["twice"].as_source().as_table()
