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
        label="typed",
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


# ---------------------------------------------------------------------------
# decimal32 and decimal64 read back at their own width; a type text that would not
# read back is refused before anything is written (CodeRabbit on PR 454)
# ---------------------------------------------------------------------------

# Two precisions and scales per width, each width as a tag and as data; the second
# arrangement swaps which precision is the tag.
_DECIMAL_ARRANGEMENTS: dict[str, dict[str, pa.DataType]] = {
    "narrow tags": {
        "t32": pa.decimal32(5, 2),
        "t64": pa.decimal64(18, 0),
        "d32": pa.decimal32(9, 4),
        "d64": pa.decimal64(12, 3),
    },
    "wide tags": {
        "t32": pa.decimal32(9, 4),
        "t64": pa.decimal64(12, 3),
        "d32": pa.decimal32(5, 2),
        "d64": pa.decimal64(18, 0),
    },
}


def _decimal_source(arrangement: str) -> ArrowTableSource:
    """A source with a decimal32 and a decimal64 tag and data column."""
    types = _DECIMAL_ARRANGEMENTS[arrangement]
    values = [decimal.Decimal(1), decimal.Decimal(2)]
    table = pa.Table.from_arrays(
        [pa.array(values, t) for t in types.values()], names=list(types)
    )
    return ArrowTableSource(
        table,
        tag_columns=["t32", "t64"],
        tag_only_columns=True,
        label="decimals",
        source_id="decimals",
    )


@pytest.mark.parametrize("arrangement", list(_DECIMAL_ARRANGEMENTS))
def test_decimal32_and_decimal64_save_and_load_at_their_own_width(
    tmp_path: Path, arrangement: str
) -> None:
    """The saved text of a decimal32 or decimal64 column reads back as that type: the
    loaded slot has the source's Arrow pair, the original source binds, and a
    load-only run reads the same rows."""
    from orcapod.core.operators import MapData

    source = _decimal_source(arrangement)
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="decimals", database=database)
    with job:
        MapData({"d32": "e32"})(source, label="renamed")
    job.run()
    path = tmp_path / "job.json"
    job.save(path)
    saved = json.loads(path.read_text())
    (slot,) = [
        d["source_config"]
        for d in saved["pipeline"]["nodes"].values()
        if d["node_type"] == "source"
    ]
    types = _DECIMAL_ARRANGEMENTS[arrangement]
    assert slot["tag_schema"] == {n: str(types[n]) for n in ("t32", "t64")}
    assert slot["data_schema"] == {n: str(types[n]) for n in ("d32", "d64")}
    loaded = PipelineJob.load(path, database=database)
    assert loaded.nodes["decimals"].arrow_schemas == source.arrow_schemas
    loaded.bind(sources={"decimals": _decimal_source(arrangement)})
    loaded.run(load_only=True)
    assert (
        loaded.nodes["renamed"]
        .as_table()
        .schema.equals(job.nodes["renamed"].as_table().schema)
    )


@pytest.mark.parametrize(
    "arrow_type",
    [pa.month_day_nano_interval(), pa.string_view(), pa.binary_view()],
    ids=["interval", "string_view", "binary_view"],
)
@pytest.mark.parametrize("save", ["pipeline", "job"])
def test_a_type_text_that_would_not_read_back_is_refused_before_writing(
    tmp_path: Path, arrow_type: pa.DataType, save: str
) -> None:
    """A slot declared in Arrow takes types no source holds (a source refuses to hash
    them); their text is not one the reader takes. Saving refuses, naming the node,
    the column and the text, and writes no file."""
    from orcapod import Pipeline
    from orcapod.core.nodes.source_node import SourceNode
    from orcapod.core.operators import MapData

    tags = pa.schema([pa.field("id", pa.int32(), nullable=False)])
    data = pa.schema([pa.field("odd", arrow_type), pa.field("n", pa.int16())])
    slot = SourceNode(name="declared", tag_schema=tags, data_schema=data)
    pipeline = Pipeline(name="unreadable")
    with pipeline:
        MapData({"n": "m"})(slot, label="renamed")
    path = tmp_path / "saved.json"
    with pytest.raises(ValueError, match="cannot be read back") as caught:
        if save == "pipeline":
            pipeline.save(path)
        else:
            PipelineJob.from_pipeline(pipeline, database=InMemoryArrowDatabase()).save(
                path
            )
    assert f"column 'odd': its saved type text {str(arrow_type)!r}" in str(caught.value)
    assert "Saving the pipeline's node 'declared'" in caught.value.__notes__
    assert not path.exists()
