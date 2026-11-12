"""The Delta store gives back the Arrow types it was given (ITL-928, part B).

Delta Lake has no unsigned integer, nanosecond, second or millisecond timestamp, or
zoned (other than UTC) timestamp type, and the Delta writer converts such columns. A
read must still return the column's written type (large-normalized) and values: a tag
read back with another type changes the record ids computed from it, so a re-run
writes its pipeline rows a second time.
"""

from __future__ import annotations

import datetime as dt
import decimal
import pathlib

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import SelectDataColumns
from orcapod.core.sources import CachedSource
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import ArrowTableSource
from orcapod.types import CacheMode
from orcapod.utils import arrow_utils

PART_B = "ITL-928 part B: the Delta store does not keep this Arrow type"

_T = [
    dt.datetime(2026, 3, 24, 17, 54, 17, 123456) + dt.timedelta(days=i)
    for i in range(3)
]

# Column name -> three values of that type, chosen within what the type holds.
COLUMNS: dict[str, pa.Array] = {
    "int8": pa.array([-1, 0, 1], pa.int8()),
    "int64": pa.array([-1, 0, 1], pa.int64()),
    "uint8": pa.array([0, 1, 2], pa.uint8()),
    "uint16": pa.array([0, 1, 2], pa.uint16()),
    "uint32": pa.array([0, 1, 2], pa.uint32()),
    "uint64": pa.array([0, 1, 2], pa.uint64()),
    "float32": pa.array([0.5, 1.5, 2.5], pa.float32()),
    "bool": pa.array([True, False, True], pa.bool_()),
    "timestamp[s]": pa.array([t.replace(microsecond=0) for t in _T], pa.timestamp("s")),
    "timestamp[ms]": pa.array(
        [t.replace(microsecond=123000) for t in _T], pa.timestamp("ms")
    ),
    "timestamp[us]": pa.array(_T, pa.timestamp("us")),
    "timestamp[us, UTC]": pa.array(_T, pa.timestamp("us", tz="UTC")),
    "timestamp[us, New_York]": pa.array(_T, pa.timestamp("us", tz="America/New_York")),
    "date32": pa.array([t.date() for t in _T], pa.date32()),
    "large_string": pa.array(["a", "b", "c"], pa.large_string()),
    "large_binary": pa.array([b"a", b"b", b"c"], pa.large_binary()),
    "decimal128(10, 2)": pa.array(
        [decimal.Decimal(v) for v in ("1.25", "2.50", "3.75")], pa.decimal128(10, 2)
    ),
    "large_list<uint32>": pa.array([[0], [1, 2], []], pa.large_list(pa.uint32())),
    "struct<u: uint16, t: timestamp[ms]>": pa.array(
        [{"u": i, "t": _T[i].replace(microsecond=0)} for i in range(3)],
        pa.struct([("u", pa.uint16()), ("t", pa.timestamp("ms"))]),
    ),
}
# Types the Delta writer converts on main: a read gives another type back.
CHANGED = {
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "timestamp[s]",
    "timestamp[ms]",
    "timestamp[us, New_York]",
    "large_list<uint32>",
    "struct<u: uint16, t: timestamp[ms]>",
}


def _written_and_read(tmp_path: pathlib.Path, column: pa.Array) -> pa.Table:
    """Write one three-row column to a Delta store and read it back from a new store object."""
    DeltaTableDatabase(tmp_path).add_records(
        ("t",),
        pa.table({"id": pa.array([b"a", b"b", b"c"], pa.large_binary()), "c": column}),
        record_id_column="id",
        flush=True,
    )
    read = DeltaTableDatabase(tmp_path).get_all_records(("t",), record_id_column="id")
    assert read is not None
    return read.sort_by("id")


@pytest.mark.parametrize("name", list(COLUMNS))
def test_a_read_returns_the_written_type_and_values(
    name: str, tmp_path: pathlib.Path, request: pytest.FixtureRequest
) -> None:
    if name in CHANGED:
        request.applymarker(pytest.mark.xfail(reason=PART_B, strict=True))
    column = COLUMNS[name]

    read = _written_and_read(tmp_path, column)

    assert read.schema.field("c").type == arrow_utils.normalize_to_large_types(
        column.type
    )
    assert read.column("c").to_pylist() == column.to_pylist()


@pytest.mark.xfail(reason=PART_B, strict=True)
@pytest.mark.parametrize(
    "column",
    [
        pa.array([0, 2**31, 2**32 - 1], pa.uint32()),
        pa.array([0, 2**63, 2**64 - 1], pa.uint64()),
    ],
    ids=["uint32", "uint64"],
)
def test_an_unsigned_value_above_the_signed_range_round_trips(
    column: pa.Array, tmp_path: pathlib.Path
) -> None:
    read = _written_and_read(tmp_path, column)

    assert read.schema.field("c").type == column.type
    assert read.column("c").to_pylist() == column.to_pylist()


@pytest.mark.xfail(reason=PART_B, strict=True)
def test_nanosecond_timestamps_keep_their_digits(tmp_path: pathlib.Path) -> None:
    column = pa.array(
        [1_700_000_000_123_456_789, 1, 1_700_000_000_000_000_001], pa.timestamp("ns")
    )

    read = _written_and_read(tmp_path, column)

    assert read.schema.field("c").type == pa.timestamp("ns")
    assert read.column("c").equals(column)


@pytest.mark.xfail(reason=PART_B, strict=True)
@pytest.mark.parametrize(
    "column",
    [
        pa.array([dt.date(2026, 1, i) for i in (1, 2, 3)], pa.date64()),
        pa.array([dt.time(i) for i in (1, 2, 3)], pa.time32("s")),
        pa.array([dt.time(i) for i in (1, 2, 3)], pa.time64("us")),
        pa.array([dt.timedelta(seconds=i) for i in (1, 2, 3)], pa.duration("us")),
    ],
    ids=["date64", "time32[s]", "time64[us]", "duration[us]"],
)
def test_types_delta_has_no_equivalent_for_are_stored(
    column: pa.Array, tmp_path: pathlib.Path
) -> None:
    read = _written_and_read(tmp_path, column)

    assert read.schema.field("c").type == column.type
    assert read.column("c").to_pylist() == column.to_pylist()


# ---------------------------------------------------------------------------
# Through a job: re-runs, operator replay, cached sources, a loaded job
# ---------------------------------------------------------------------------


@function_pod(output_keys="y")
def double(x: int) -> int:
    """Twice ``x``."""
    return 2 * x


@function_pod(output_keys="z")
def increment(y: int) -> int:
    """``y`` plus one."""
    return y + 1


def _source_table(k: pa.Array) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("k", k.type, nullable=False),
            pa.field("x", pa.int64(), nullable=False),
        ]
    )
    return pa.Table.from_arrays([k, pa.array([1, 2, 3], pa.int64())], schema=schema)


TAGS = {
    "int8": pa.array([1, 2, 3], pa.int8()),
    "uint32": pa.array([1, 2, 3], pa.uint32()),
    "timestamp[s]": pa.array(_T, pa.timestamp("s")),
    "timestamp[ns]": pa.array(_T, pa.timestamp("ns")),
    "timestamp[us, New_York]": pa.array(_T, pa.timestamp("us", tz="America/New_York")),
}


def _chained_job(root: pathlib.Path, table: pa.Table) -> PipelineJob:
    with PipelineJob(name="j", store=DeltaTableDatabase(root)) as job:
        src = ArrowTableSource(table, tag_columns=["k"], source_id="src")
        increment.pod(double.pod(src, label="double"), label="increment")
    return job


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("name", list(TAGS))
def test_a_rerun_on_the_same_store_writes_no_pipeline_rows(
    name: str, mode: str, tmp_path: pathlib.Path, request: pytest.FixtureRequest
) -> None:
    """The ITL-990 case: source -> pod -> pod, run twice; a fresh job reads 3 rows."""
    if name != "int8":
        request.applymarker(pytest.mark.xfail(reason=PART_B, strict=True))
    from orcapod.pipeline import AsyncPipelineOrchestrator

    table = _source_table(TAGS[name])
    for _ in range(2):
        job = _chained_job(tmp_path, table)
        if mode == "async":
            job.run(orchestrator=AsyncPipelineOrchestrator())
        else:
            job.run()

    fresh = _chained_job(tmp_path, table)
    for label in ("double", "increment"):
        node = fresh.nodes[label]
        assert node.as_table().num_rows == 3
        assert node.as_table().schema.field("k").type == TAGS[name].type
        records = node.get_all_records(all_info=True)
        assert records is not None
        assert records.num_rows == 3


@pytest.mark.parametrize("name", list(TAGS))
def test_a_fresh_job_reads_the_tag_type(
    name: str, tmp_path: pathlib.Path, request: pytest.FixtureRequest
) -> None:
    if name != "int8":
        request.applymarker(pytest.mark.xfail(reason=PART_B, strict=True))
    table = _source_table(TAGS[name])
    _chained_job(tmp_path, table).run()

    fresh = _chained_job(tmp_path, table).nodes["double"]

    assert fresh.as_table().schema.field("k").type == TAGS[name].type
    tag, _ = next(iter(fresh.iter_data()))
    assert tag.as_table().schema.field("k").type == TAGS[name].type
    records = fresh.get_all_records()
    assert records is not None
    assert records.schema.field("k").type == TAGS[name].type


@pytest.mark.parametrize("name", list(TAGS))
def test_a_loaded_job_reads_the_tag_type(
    name: str, tmp_path: pathlib.Path, request: pytest.FixtureRequest
) -> None:
    if name != "int8":
        request.applymarker(pytest.mark.xfail(reason=PART_B, strict=True))
    job = _chained_job(tmp_path / "store", _source_table(TAGS[name]))
    job.run()
    job.save(tmp_path / "job.json")

    loaded = PipelineJob.load(
        tmp_path / "job.json", store=DeltaTableDatabase(tmp_path / "store")
    )

    for label in ("double", "increment"):
        assert loaded.nodes[label].as_table().schema.field("k").type == TAGS[name].type


@pytest.mark.parametrize("name", list(TAGS))
def test_an_operator_replay_reads_the_tag_type(
    name: str, tmp_path: pathlib.Path, request: pytest.FixtureRequest
) -> None:
    if name != "int8":
        request.applymarker(pytest.mark.xfail(reason=PART_B, strict=True))
    table = pa.Table.from_arrays(
        [TAGS[name], pa.array([1, 2, 3], pa.int64()), pa.array([4, 5, 6], pa.int64())],
        schema=pa.schema(
            [
                pa.field("k", TAGS[name].type, nullable=False),
                pa.field("x", pa.int64(), nullable=False),
                pa.field("w", pa.int64(), nullable=False),
            ]
        ),
    )

    def build(mode: CacheMode) -> PipelineJob:
        store = DeltaTableDatabase(tmp_path)
        with PipelineJob(name="o", store=store) as job:
            src = ArrowTableSource(table, tag_columns=["k"], source_id="src")
            SelectDataColumns(["x"])(src, label="sel")
        # OperatorNode.attach_databases is the public way to turn on LOG / REPLAY mode.
        job.nodes["sel"].attach_databases(
            pipeline_database=store.at("o"), cache_mode=mode
        )
        return job

    build(CacheMode.LOG).run()

    replayed = build(CacheMode.REPLAY).nodes["sel"].as_table()

    assert replayed.schema.field("k").type == TAGS[name].type


@pytest.mark.parametrize("name", list(TAGS))
def test_a_cached_source_reads_the_tag_type(
    name: str, tmp_path: pathlib.Path, request: pytest.FixtureRequest
) -> None:
    if name != "int8":
        request.applymarker(pytest.mark.xfail(reason=PART_B, strict=True))
    table = _source_table(TAGS[name])

    def cached() -> CachedSource:
        return CachedSource(
            ArrowTableSource(table, tag_columns=["k"], source_id="src"),
            cache_database=DeltaTableDatabase(tmp_path),
        )

    cached().as_table()

    assert cached().as_table().schema.field("k").type == TAGS[name].type
