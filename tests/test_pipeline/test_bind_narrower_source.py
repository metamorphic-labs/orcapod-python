"""A slot declared ``T | None`` accepts a source of ``T``, presented as ``T | None`` (ITL-1112).

Developer decision, November 14, 2026: a source slot accepts a source that is stricter than
it declares (a column ``x: int`` for a slot declaring ``x: int | None``) and refuses the
reverse. The slot gives the rows of its declared schema: the bound source is treated as
``x: int | None``. Its rows are exactly those of a source with a nullable ``x``, the same
values and the same source id: Arrow fields, system-tag column names, row hashes and record
ids alike, so both write to one stored path and append to one table. Nothing downstream
moves: every pipeline hash and predicted schema is the declared one.
"""

from __future__ import annotations

import asyncio
import logging
import pathlib
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes.source_node import SourceNode
from orcapod.core.operators import Join, MapData
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import SourceSpecMismatchError, UnboundSourceError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource, CSVSource, DictSource
from orcapod.types import Schema


@function_pod(output_keys="z")
def inc(y: int | None) -> int:
    """Return ``y + 1``, or 0 for a missing ``y``."""
    return 0 if y is None else y + 1


def _arrow_source(
    xs: list[int | None], *, x_nullable: bool, ids: list[int] | None = None
) -> ArrowTableSource:
    """Return a source of tag ``id`` (non-null) and data ``x`` with the given flag."""
    schema = pa.schema(
        [pa.field("id", pa.int64(), False), pa.field("x", pa.int64(), x_nullable)]
    )
    ids = list(range(len(xs))) if ids is None else ids
    return ArrowTableSource(
        pa.table({"id": ids, "x": xs}, schema=schema),
        tag_columns=["id"],
        source_id="same",
    )


def _slot(x_type: Any) -> SourceNode:
    """Return slot ``src`` declaring tag ``id: int`` and data ``x`` of ``x_type``."""
    return SourceNode(
        name="src", tag_schema=Schema({"id": int}), data_schema=Schema({"x": x_type})
    )


def _job(tmp_path: pathlib.Path, x_type: Any = int | None) -> PipelineJob:
    """Return an unbound job: slot ``src`` -> MapData x->y ``renamed`` -> ``inc``."""
    job = PipelineJob(name="j", database=DeltaTableDatabase(str(tmp_path / "store")))
    with job:
        _ = inc.pod(MapData({"x": "y"})(_slot(x_type), label="renamed"), label="inc")
    return job


def _fresh(tmp_path: pathlib.Path) -> PipelineJob:
    """Return the job bound to a nullable source, as recorded."""
    job = _job(tmp_path)
    job.bind(sources={"src": _arrow_source([1, None], x_nullable=True)})
    return job


def _loaded(tmp_path: pathlib.Path) -> PipelineJob:
    """Return the job, run with a nullable source and saved, loaded again."""
    job = _fresh(tmp_path)
    _ = job.run()
    path = tmp_path / "job.json"
    job.save(str(path))
    return PipelineJob.load(str(path))


JOBS: dict[str, Callable[[pathlib.Path], PipelineJob]] = {
    "fresh": _fresh,
    "loaded": _loaded,
}


def _bind(job: PipelineJob, source: Any) -> None:
    """Bind *source* to slot ``src`` with ``PipelineJob.bind``."""
    job.bind(sources={"src": source})


def _assign(job: PipelineJob, source: Any) -> None:
    """Bind *source* to slot ``src`` by assigning the node's ``bound_source``."""
    job.nodes["src"].bound_source = source  # pyright: ignore[reportAttributeAccessIssue]  # "src" is a SourceJobNode


BINDS: dict[str, Callable[[PipelineJob, Any], None]] = {
    "bind": _bind,
    "bound_source": _assign,
}


def _pipeline_hashes(job: PipelineJob) -> dict[str, str]:
    """Return every node's pipeline hash, by node label."""
    return {name: node.pipeline_hash().to_string() for name, node in job.nodes.items()}


def _row_hashes(pairs: Any) -> list[tuple[str, str]]:
    """Return the tag and data content hashes of each ``(tag, data)`` pair."""
    return [
        (t.content_hash().to_string(), d.content_hash().to_string()) for t, d in pairs
    ]


async def _collect(node: Any) -> list[Any]:
    """Return every pair of ``node.async_iter_data()``."""
    return [pair async for pair in node.async_iter_data()]


# ---------------------------------------------------------------------------
# Accepted and refused
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("make_job", JOBS.values(), ids=JOBS.keys())
@pytest.mark.parametrize("bind", BINDS.values(), ids=BINDS.keys())
def test_a_narrower_source_is_accepted_and_moves_nothing(
    tmp_path: pathlib.Path,
    make_job: Callable[[pathlib.Path], PipelineJob],
    bind: Callable[[PipelineJob, Any], None],
) -> None:
    """``x: int`` binds to a slot of ``x: int | None``; schemas and pipeline hashes stay."""
    job = make_job(tmp_path)
    before = _pipeline_hashes(job)
    bind(job, DictSource([{"id": 0, "x": 1}], tag_columns=["id"], source_id="d"))
    assert _pipeline_hashes(job) == before
    assert dict(job.nodes["src"].output_schema()[1]) == {"x": int | None}
    assert dict(job.nodes["renamed"].output_schema()[1]) == {"y": int | None}
    assert job.nodes["src"].as_table().schema.field("x").nullable


@pytest.mark.parametrize("make_job", JOBS.values(), ids=JOBS.keys())
@pytest.mark.parametrize("bind", BINDS.values(), ids=BINDS.keys())
def test_a_nullable_source_is_refused_by_a_non_null_slot(
    tmp_path: pathlib.Path,
    make_job: Callable[[pathlib.Path], PipelineJob],
    bind: Callable[[PipelineJob, Any], None],
) -> None:
    """A source with ``x: int | None`` is refused by a slot declaring ``x: int``."""
    job = _job(tmp_path, x_type=int)
    job.bind(sources={"src": _arrow_source([1, 2], x_nullable=False)})
    if make_job is _loaded:
        _ = job.run()
        job.save(str(tmp_path / "job.json"))
        job = PipelineJob.load(str(tmp_path / "job.json"))
    with pytest.raises(
        SourceSpecMismatchError,
        match=r"'x' is declared int but the source has int \| None",
    ):
        bind(job, _arrow_source([1, None], x_nullable=True))


def test_a_narrower_tag_column_is_accepted(tmp_path: pathlib.Path) -> None:
    """A tag ``id: int`` binds to a slot declaring ``id: int | None`` and reads as one."""
    slot = SourceNode(
        name="src",
        tag_schema=Schema({"id": int | None}),
        data_schema=Schema({"x": int}),
    )
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(slot, label="renamed")
    job.bind(sources={"src": DictSource([{"id": 0, "x": 1}], tag_columns=["id"])})
    assert dict(job.nodes["src"].output_schema()[0]) == {"id": int | None}
    assert job.nodes["src"].as_table().schema.field("id").nullable


@pytest.mark.parametrize(
    "declared, given",
    [(int | None, str), (int | str | None, int), (int | None, float)],
    ids=["other-type", "other-union-member", "numeric"],
)
def test_only_the_none_member_may_be_missing(
    tmp_path: pathlib.Path, declared: Any, given: type
) -> None:
    """``T | None`` accepts ``T`` only: no other type, union member or numeric type."""
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(_slot(declared), label="renamed")
    value: Any = {str: "a", int: 1, float: 1.5}[given]
    with pytest.raises(SourceSpecMismatchError, match="'x'"):
        job.bind(
            sources={"src": DictSource([{"id": 0, "x": value}], tag_columns=["id"])}
        )


# ---------------------------------------------------------------------------
# The rows are those of the declared schema
# ---------------------------------------------------------------------------


def _bound_slot(tmp_path: pathlib.Path, source: ArrowTableSource) -> Any:
    """Return slot ``src`` of a fresh ``x: int | None`` job, bound to *source*."""
    job = _job(tmp_path)
    job.bind(sources={"src": source})
    return job.nodes["src"]


def test_rows_equal_those_of_a_nullable_source(tmp_path: pathlib.Path) -> None:
    """Narrower rows equal a nullable source's: fields, system tags, hashes, record ids."""
    narrower = _bound_slot(tmp_path / "a", _arrow_source([1, 2], x_nullable=False))
    nullable = _bound_slot(tmp_path / "b", _arrow_source([1, 2], x_nullable=True))
    assert narrower.as_table(all_info=True).equals(
        nullable.as_table(all_info=True), check_metadata=True
    )
    assert _row_hashes(narrower.iter_data()) == _row_hashes(nullable.iter_data())
    assert _row_hashes(asyncio.run(_collect(narrower))) == _row_hashes(
        nullable.iter_data()
    )
    assert narrower.keys(all_info=True) == nullable.keys(all_info=True)


def test_rows_of_each_read_carry_the_declared_flags(tmp_path: pathlib.Path) -> None:
    """``iter_data``, ``async_iter_data`` and ``head`` give nullable ``x``."""
    slot = _bound_slot(tmp_path, _arrow_source([1, 2], x_nullable=False))
    for _, data in slot.iter_data():
        assert data.as_table().schema.field("x").nullable
    for _, data in asyncio.run(_collect(slot)):
        assert data.as_table().schema.field("x").nullable
    view = slot.head(1, all_info=True)
    head = view.table
    assert head.schema.field("x").nullable
    assert view.tag_columns == slot.keys(columns={"system_tags": True})[0]
    assert head.equals(slot.as_table(all_info=True).slice(0, 1))


def test_zero_rows_have_the_types_of_rows(tmp_path: pathlib.Path) -> None:
    """A narrower source with no rows reads with the types it has with rows."""

    @function_pod(output_keys="x")
    def same(v: int) -> int:
        """Return ``v``."""
        return v

    origin = PipelineJob(name="origin", database=InMemoryArrowDatabase())
    with origin:
        _ = same.pod(
            DictSource([{"id": 0, "v": 1}], tag_columns=["id"], source_id="o"),
            label="same",
        )
    empty = origin.nodes["same"].as_source()  # pyright: ignore[reportAttributeAccessIssue]  # a function job node; as_source before a run has no rows
    job = _job(tmp_path)
    job.bind(sources={"src": empty})
    with_rows = _bound_slot(tmp_path / "rows", _arrow_source([1], x_nullable=False))
    table = job.nodes["src"].as_table()
    assert table.num_rows == 0
    assert table.schema == with_rows.as_table().schema


# ---------------------------------------------------------------------------
# One stored path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
def test_narrower_then_nullable_runs_append_to_one_path(
    tmp_path: pathlib.Path, orchestrator: type
) -> None:
    """A run of a narrower source, then of a nullable one, append under the same path."""
    job = _job(tmp_path)
    job.bind(sources={"src": _arrow_source([1, 2], x_nullable=False)})
    database = job.database
    assert database is not None
    first = job.run(orchestrator=orchestrator())
    assert first.ok
    paths = set(database.list_table_paths(()))
    job.bind(sources={"src": _arrow_source([None, 3], x_nullable=True, ids=[2, 3])})
    second = job.run(orchestrator=orchestrator())
    assert second.ok
    assert set(database.list_table_paths(())) == paths
    assert sorted(job.nodes["inc"].as_table().column("z").to_pylist()) == [0, 4]
    stored = [database.get_all_records(path) for path in paths if path[-1] == "pdb_v2"]
    assert [None if table is None else table.num_rows for table in stored] == [4]


# ---------------------------------------------------------------------------
# A loaded job's rebuilt file source
# ---------------------------------------------------------------------------


def _csv_job(tmp_path: pathlib.Path, text: str) -> str:
    """Save a job over a CSV source of *text*; return the job file's path."""
    csv = tmp_path / "data.csv"
    _ = csv.write_text(text)
    job = PipelineJob(name="c", database=DeltaTableDatabase(str(tmp_path / "store")))
    with job:
        _ = MapData({"x": "y"})(
            CSVSource(str(csv), tag_columns=["id"], source_id="csv"), label="renamed"
        )
    _ = job.run()
    path = tmp_path / "job.json"
    job.save(str(path))
    return str(path)


def _csv_slot(job: PipelineJob) -> Any:
    """Return the CSV source's slot of *job*."""
    return job.nodes["csv"]


def test_loaded_csv_of_another_type_is_left_unbound(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A CSV whose column changed type loads unbound, with a warning naming the slot."""
    path = _csv_job(tmp_path, "id,x\n0,1\n1,2\n")
    _ = (tmp_path / "data.csv").write_text("id,x\n0,a\n1,b\n")
    with caplog.at_level(logging.WARNING):
        job = PipelineJob.load(path)
    assert any(
        "'csv'" in r.getMessage() and "declared int" in r.getMessage()
        for r in caplog.records
    )
    assert job.unbound_sources == ["csv"]
    with pytest.raises(UnboundSourceError):
        _ = _csv_slot(job).as_table()
    with pytest.raises(RuntimeError, match="cannot execute a loaded job"):
        _ = job.run()
    _ = (tmp_path / "fixed.csv").write_text("id,x\n0,7\n")
    job.bind(
        sources={
            "csv": CSVSource(
                str(tmp_path / "fixed.csv"), tag_columns=["id"], source_id="csv"
            )
        }
    )
    assert _csv_slot(job).as_table().column("x").to_pylist() == [7]


def test_loaded_csv_now_narrower_is_bound_as_declared(tmp_path: pathlib.Path) -> None:
    """A CSV saved with an empty cell (``x: int | None``) and now without loads bound."""
    path = _csv_job(tmp_path, "id,x\n0,1\n1,\n")
    _ = (tmp_path / "data.csv").write_text("id,x\n0,1\n1,2\n")
    job = PipelineJob.load(path)
    slot = _csv_slot(job)
    assert dict(slot.output_schema()[1]) == {"x": int | None}
    assert slot.as_table().schema.field("x").nullable
    assert slot.as_table().column("x").to_pylist() == [1, 2]


# ---------------------------------------------------------------------------
# System-tag values are the source's own
# ---------------------------------------------------------------------------


def test_system_tag_values_and_source_info_are_the_sources_own(
    tmp_path: pathlib.Path,
) -> None:
    """Names follow the slot; source id, record ids and source-info tokens the source.

    The source is given no source id, so its default hash-based id is computed from its
    true table (``x: int``) and is carried unchanged.
    """
    schema = pa.schema(
        [pa.field("id", pa.int64(), False), pa.field("x", pa.int64(), False)]
    )
    source = ArrowTableSource(
        pa.table({"id": [0, 1], "x": [1, 2]}, schema=schema), tag_columns=["id"]
    )
    direct = source.as_table(all_info=True)
    slot = _bound_slot(tmp_path, source)
    bound = slot.as_table(all_info=True)

    def system_tag(table: pa.Table, prefix: str) -> list[Any]:
        """Return the values of the one column of *table* whose name starts with *prefix*."""
        (name,) = [c for c in table.column_names if c.startswith(prefix)]
        return table.column(name).to_pylist()

    for prefix in ("_tag_source_id::", "_tag_record_id::"):
        assert system_tag(bound, prefix) == system_tag(direct, prefix)
    assert system_tag(bound, "_tag_source_id::") == [source.source_id] * 2
    assert (
        bound.column("_source_x").to_pylist() == direct.column("_source_x").to_pylist()
    )
    # The names are the slot's, not the source's.
    names = [c for c in bound.column_names if c.startswith("_tag_")]
    assert names == list(slot.keys(columns={"system_tags": True})[0][1:])
    assert not set(names) & set(direct.column_names)


# ---------------------------------------------------------------------------
# Column order, transformed streams and meta columns (CodeRabbit round 1)
# ---------------------------------------------------------------------------


def _abc_job(a_type: Any) -> PipelineJob:
    """Return a job whose slot declares tag ``id`` and data ``a``, ``b``, ``c`` in order."""
    slot = SourceNode(
        name="src",
        tag_schema=Schema({"id": int}),
        data_schema=Schema({"a": a_type, "b": str, "c": float}),
    )
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"a": "a2"})(slot, label="renamed")
    return job


@pytest.mark.parametrize("a_type", [int, int | None], ids=["equal", "narrower"])
def test_columns_follow_the_declared_order(a_type: Any) -> None:
    """A source with its columns in another order gives them in the declared order."""
    job = _abc_job(a_type)
    job.bind(
        sources={
            "src": DictSource(
                [{"id": 0, "c": 1.5, "b": "s", "a": 1}], tag_columns=["id"]
            )
        }
    )
    node = job.nodes["src"]
    assert list(node.output_schema()[1]) == ["a", "b", "c"]
    table = node.as_table(columns={"source": True})
    assert table.column_names == [
        "id",
        "a",
        "b",
        "c",
        "_source_a",
        "_source_b",
        "_source_c",
    ]
    for _, data in node.iter_data():
        assert data.as_table().column_names == ["a", "b", "c"]
    for _, data in asyncio.run(_collect(node)):
        assert data.as_table().column_names == ["a", "b", "c"]
    assert node.head(1).table.column_names == ["id", "a", "b", "c"]
    assert job.nodes["renamed"].as_table().column_names == ["id", "a2", "b", "c"]


def _mapped_stream() -> Any:
    """Return a stream (not a root source): a DictSource with ``q`` renamed to ``x``."""
    source = DictSource([{"id": 0, "q": 1}], tag_columns=["id"], source_id="m")
    return MapData({"q": "x"})(source)


@pytest.mark.parametrize("x_type", [int, int | None], ids=["equal", "narrower"])
def test_a_transformed_stream_gets_the_slots_system_tag_names(
    tmp_path: pathlib.Path, x_type: Any
) -> None:
    """A stream's one system-tag pair takes the slot's names; its values stay its own."""
    stream = _mapped_stream()
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(_slot(x_type), label="renamed")
    job.bind(sources={"src": stream})
    node = job.nodes["src"]
    predicted = node.keys(columns={"system_tags": True})[0][1:]
    table = node.as_table(columns={"system_tags": True})
    names = [c for c in table.column_names if c.startswith("_tag_")]
    assert names == list(predicted)
    direct = stream.as_table(columns={"system_tags": True})
    direct_names = [c for c in direct.column_names if c.startswith("_tag_")]
    assert not set(direct_names) & set(names)
    for mine, theirs in zip(names, direct_names, strict=True):
        assert table.column(mine).to_pylist() == direct.column(theirs).to_pylist()


def _joined_stream(x_nullable: bool) -> Any:
    """Return a Join of two sources: two system-tag pairs, data ``x`` and ``w``."""
    left = _arrow_source([1, 2], x_nullable=x_nullable)
    right = DictSource([{"id": 0, "w": "p"}, {"id": 1, "w": "q"}], tag_columns=["id"])
    return Join()(left, right)


def _xw_job() -> PipelineJob:
    """Return a job whose slot declares tag ``id`` and data ``x: int | None``, ``w``."""
    slot = SourceNode(
        name="src",
        tag_schema=Schema({"id": int}),
        data_schema=Schema({"x": int | None, "w": str}),
    )
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(slot, label="renamed")
    return job


def test_a_joined_stream_of_the_declared_schema_passes_through() -> None:
    """An equal-schema stream with two system-tag pairs is bound as it is (known gap)."""
    stream = _joined_stream(x_nullable=True)
    job = _xw_job()
    job.bind(sources={"src": stream})
    assert job.nodes["src"].keys(all_info=True) == stream.keys(all_info=True)


@pytest.mark.parametrize("bind", BINDS.values(), ids=BINDS.keys())
def test_a_narrower_joined_stream_is_refused(
    bind: Callable[[PipelineJob, Any], None],
) -> None:
    """A narrower stream with two system-tag pairs cannot take the slot's one pair."""
    job = _xw_job()
    with pytest.raises(SourceSpecMismatchError, match="system-tag"):
        bind(job, _joined_stream(x_nullable=False))
    assert job.nodes["src"].bound_source is None  # pyright: ignore[reportAttributeAccessIssue]  # "src" is a SourceJobNode


def test_meta_columns_of_a_narrower_stream_are_kept() -> None:
    """The ``__`` columns a job node's rows carry reach the rows of a narrower bind."""

    @function_pod(output_keys="x")
    def same(v: int) -> int:
        """Return ``v``."""
        return v

    origin = PipelineJob(name="origin", database=InMemoryArrowDatabase())
    with origin:
        _ = same.pod(
            DictSource([{"id": 0, "v": 1}], tag_columns=["id"], source_id="o"),
            label="same",
        )
    _ = origin.run()
    stream = origin.nodes["same"]
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(_slot(int | None), label="renamed")
    job.bind(sources={"src": stream})
    node = job.nodes["src"]
    own = [
        sorted(data.as_table(columns={"meta": True}).column_names)
        for _, data in stream.iter_data()
    ]
    assert any("__computed" in names for names in own)
    for rows in (list(node.iter_data()), asyncio.run(_collect(node))):
        assert [
            sorted(data.as_table(columns={"meta": True}).column_names)
            for _, data in rows
        ] == own


@pytest.mark.parametrize("bind", BINDS.values(), ids=BINDS.keys())
def test_a_narrower_stream_without_system_tags_is_refused(
    bind: Callable[[PipelineJob, Any], None],
) -> None:
    """A narrower stream with no system-tag pair cannot be given the slot's pair."""
    schema = pa.schema(
        [pa.field("id", pa.int64(), False), pa.field("x", pa.int64(), False)]
    )
    stream = ArrowTableStream(
        pa.table({"id": [0], "x": [1]}, schema=schema), tag_columns=["id"]
    )
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(_slot(int | None), label="renamed")
    with pytest.raises(SourceSpecMismatchError, match="system-tag"):
        bind(job, stream)
    assert job.nodes["src"].bound_source is None  # pyright: ignore[reportAttributeAccessIssue]  # "src" is a SourceJobNode
