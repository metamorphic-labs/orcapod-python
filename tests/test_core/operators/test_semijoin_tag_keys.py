"""SemiJoin keys on the shared tag columns only (ITL-1088).

Developer decision, November 13, 2026: "Semijoin should key on shared tags only". The
match keys are exactly the names that are tag columns of both inputs. Every other shared
name (data on both sides, or a tag on one side and data on the other) is ignored, and the
right input's data columns are never read. For v0.3 such a name raises
``SemiJoinIgnoredColumnWarning`` once per application (provisional: it may become an
error). With no shared tag, every left row is returned. Before, every shared name, tag or
data, was a match key.
"""

from __future__ import annotations

import asyncio
import warnings
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.channels import Channel
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.operators import PolarsFilter, SemiJoin
from orcapod.core.sources import ArrowTableSource, CSVSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import InputValidationError, SemiJoinIgnoredColumnWarning
from orcapod.protocols.core_protocols import StreamProtocol
from tests.test_core.operators.test_async_sync_equivalence import (
    Row,
    _meta_columns,  # pyright: ignore[reportPrivateUsage]  # shared test helper
    _source,  # pyright: ignore[reportPrivateUsage]  # shared test helper
    _with_meta,  # pyright: ignore[reportPrivateUsage]  # shared test helper
    assert_equivalent,
    assert_same_provenance,
    sync_rows,
)


def left_source() -> ArrowTableSource:
    """Tag ``id`` 1..3, data ``y`` 1..3 and ``x``."""
    return _source(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "y": pa.array([1, 2, 3], pa.int64()),
            "x": pa.array([10, 20, 30], pa.int64()),
        },
        tags=("id",),
        source_id="left",
    )


def right_same_tag_other_y() -> ArrowTableSource:
    """Tag ``id`` 1..3 and data ``y`` that equals the left's only for ``id`` 1 and 3."""
    return _source(
        {"id": pa.array([1, 2, 3], pa.int64()), "y": pa.array([1, 9, 3], pa.int64())},
        tags=("id",),
        source_id="right_y",
    )


def right_tag_y() -> ArrowTableSource:
    """Tag ``y`` (the left's data column name) and data ``z``."""
    return _source(
        {"y": pa.array([1, 3], pa.int64()), "z": pa.array([0, 0], pa.int64())},
        tags=("y",),
        source_id="right_tag_y",
    )


def right_nothing_shared() -> ArrowTableSource:
    """Tag ``other`` and data ``w``: no name in common with the left."""
    return _source(
        {"other": pa.array([7], pa.int64()), "w": pa.array([0], pa.int64())},
        tags=("other",),
        source_id="right_nothing",
    )


def right_unequal_data_of_another_type() -> ArrowTableSource:
    """Tag ``id`` 1 and 3; data ``y`` and ``x`` of other types and values than the left's."""
    return _source(
        {
            "id": pa.array([3, 1], pa.int64()),
            "y": pa.array(["no", "match"], pa.large_string()),
            "x": pa.array([None, 0.5], pa.float64()),
        },
        tags=("id",),
        source_id="right_other_data",
        nullable=("x",),
    )


def right_tags_only() -> ArrowTableSource:
    """Tag ``id`` 1 and 3 and data ``z``: only the tag is shared."""
    return _source(
        {"id": pa.array([1, 3], pa.int64()), "z": pa.array([0, 0], pa.int64())},
        tags=("id",),
        source_id="right_tags_only",
    )


def right_data_id() -> ArrowTableSource:
    """Tag ``other`` and data ``id``: the left's tag name is data here."""
    return _source(
        {"other": pa.array([7], pa.int64()), "id": pa.array([1], pa.int64())},
        tags=("other",),
        source_id="right_data_id",
    )


def left_ids(stream: StreamProtocol) -> list[int | None]:
    """The ``id`` of each result row, in order."""
    return stream.as_table().column("id").to_pylist()


# (right input, ids kept, ignored (name, left role, right role))
CASES = [
    pytest.param(
        right_same_tag_other_y,
        [1, 2, 3],
        [("y", "data", "data")],
        id="data_on_both_sides",
    ),
    pytest.param(
        right_tag_y, [1, 2, 3], [("y", "data", "tag")], id="data_left_tag_right"
    ),
    pytest.param(
        right_data_id, [1, 2, 3], [("id", "tag", "data")], id="tag_left_data_right"
    ),
    pytest.param(
        right_unequal_data_of_another_type,
        [1, 3],
        [("y", "data", "data"), ("x", "data", "data")],
        id="right_data_never_read",
    ),
    pytest.param(right_tags_only, [1, 3], [], id="tags_only"),
    pytest.param(right_nothing_shared, [1, 2, 3], [], id="nothing_shared"),
]

Ignored = list[tuple[str, str, str]]


def assert_warned_once(record: list[warnings.WarningMessage], ignored: Ignored) -> None:
    """Exactly one ``SemiJoinIgnoredColumnWarning`` naming each column and its roles,
    or none when nothing is ignored."""
    found = [w for w in record if issubclass(w.category, SemiJoinIgnoredColumnWarning)]
    assert len(found) == (1 if ignored else 0)
    if ignored:
        message = str(found[0].message)
        for name, left_role, right_role in ignored:
            assert (
                f"'{name}' ({left_role} on the left, {right_role} on the right)"
                in message
            )
        assert "only on tag columns shared by both inputs" in message
        # Attributed to this test file, the user's call.
        assert found[0].filename == __file__


@pytest.mark.parametrize(("make_right", "kept", "ignored"), CASES)
def test_semijoin_keys_on_shared_tags_only(
    make_right: Callable[[], ArrowTableSource], kept: list[int], ignored: Ignored
) -> None:
    """Only shared tags decide; other shared names warn once and play no part."""
    left = left_source()
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        result = SemiJoin()(left, make_right())
        assert left_ids(result) == kept
    assert_warned_once(record, ignored)
    assert result.output_schema() == left.output_schema()
    assert result.as_table().schema == left.as_table().schema


async def _async_rows(
    op: SemiJoin, left: StreamProtocol, right: StreamProtocol
) -> list[Row]:
    channels: list[Channel[Row]] = [Channel(buffer_size=64) for _ in range(2)]
    output: Channel[Row] = Channel(buffer_size=64)
    for stream, channel in zip((left, right), channels):
        for row in stream.iter_data():
            await channel.writer.send(row)
        await channel.writer.close()
    await op.async_execute(
        [channel.reader for channel in channels],
        output.writer,
        input_pipeline_hashes=[left.pipeline_hash(), right.pipeline_hash()],
    )
    return await output.reader.collect()


@pytest.mark.parametrize(("make_right", "kept", "ignored"), CASES)
def test_async_rows_equal_sync_rows(
    make_right: Callable[[], ArrowTableSource], kept: list[int], ignored: Ignored
) -> None:
    """The async path keys on the same columns; called without validation it warns once."""
    left, right = left_source(), make_right()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", SemiJoinIgnoredColumnWarning)
        expected = sync_rows(SemiJoin(), left, right)
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        actual = asyncio.run(_async_rows(SemiJoin(), left, right))
    found = [w for w in record if issubclass(w.category, SemiJoinIgnoredColumnWarning)]
    assert len(found) == (1 if ignored else 0)
    assert len(expected) == len(kept)
    assert_equivalent(expected, actual)
    assert_same_provenance(expected, actual)


def test_validated_async_run_does_not_warn_again() -> None:
    """An operator applied (and warned) once does not warn again on its async run."""
    left, right = left_source(), right_same_tag_other_y()
    op = SemiJoin()
    with pytest.warns(SemiJoinIgnoredColumnWarning):
        _ = op(left, right)
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        rows = asyncio.run(_async_rows(op, left, right))
    assert len(rows) == 3
    assert not [
        w for w in record if issubclass(w.category, SemiJoinIgnoredColumnWarning)
    ]


@pytest.mark.parametrize("run_async", [False, True], ids=["sync", "async"])
def test_job_warns_once(run_async: bool) -> None:
    """A semi join in a job warns once, at the call, not again when the job runs."""
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        job = PipelineJob(name="semijoin_warns_once", database=InMemoryArrowDatabase())
        with job:
            _ = SemiJoin(label="sj")(left_source(), right_same_tag_other_y())
        if run_async:
            from orcapod.pipeline import AsyncPipelineOrchestrator

            job.run(orchestrator=AsyncPipelineOrchestrator())
        else:
            job.run()
    assert_warned_once(record, [("y", "data", "data")])


def test_warning_promoted_to_an_error_raises_again_on_retry() -> None:
    """Under an error filter the warning raises, and a retry raises again."""
    op = SemiJoin()
    with warnings.catch_warnings():
        warnings.simplefilter("error", SemiJoinIgnoredColumnWarning)
        for _ in range(2):
            with pytest.raises(SemiJoinIgnoredColumnWarning):
                _ = op(left_source(), right_same_tag_other_y())


def test_filter_a_stream_by_a_subset_of_itself() -> None:
    """The commonest use: every data name is shared (and warned about); tags decide."""
    source = left_source()
    subset = PolarsFilter(constraints={"id": 2})(left_source())
    with pytest.warns(SemiJoinIgnoredColumnWarning, match="'y'.*'x'"):
        result = SemiJoin()(source, subset)
    assert left_ids(result) == [2]


def test_filter_without_the_warning() -> None:
    """Reduce the right input to its tags plus one renamed data column: no warning."""
    source = left_source()
    subset = PolarsFilter(constraints={"id": 2})(left_source())
    keys_only = subset.select_data_columns(["x"]).map_data({"x": "_x"})
    with warnings.catch_warnings():
        warnings.simplefilter("error", SemiJoinIgnoredColumnWarning)
        result = SemiJoin()(source, keys_only)
    assert left_ids(result) == [2]


@pytest.mark.parametrize("path", ["sync", "async_validated", "async_unvalidated"])
def test_shared_meta_column_is_neither_a_key_nor_warned_about(path: str) -> None:
    """A meta (``__``) column of the same name on both inputs is not a user column
    (ITL-1080): no match key, no ``SemiJoinIgnoredColumnWarning``, not in the output."""
    # Same meta names on both sides, with different values: a key would drop rows.
    left = _with_meta(left_source(), _meta_columns("m", 3))
    right = _with_meta(right_tags_only(), _meta_columns("m", 2))
    op = SemiJoin()
    with warnings.catch_warnings():
        warnings.simplefilter("error", SemiJoinIgnoredColumnWarning)
        if path == "sync":
            result = op(left, right)
            ids = left_ids(result)
            meta = [n for n in result.as_table(all_info=True).column_names if "__" in n]
        else:
            if path == "async_validated":
                _ = op(left, right)
            rows = asyncio.run(_async_rows(op, left, right))
            ids = [tag.as_dict()["id"] for tag, _ in rows]
            meta = [
                n
                for _, data in rows
                for n in data.as_table(all_info=True).column_names
                if "__" in n
            ]
    assert ids == [1, 3]
    assert meta == []


def _shared_operator_inputs(
    data_tags: bool, reverse_tags: bool
) -> tuple[ArrowTableSource, ArrowTableSource, ArrowTableSource]:
    """Build two left inputs, exactly one of which shares both right tags."""

    def source(
        names: tuple[str, str],
        tags: tuple[str, str],
        data: tuple[str, str],
        values: list[int],
        source_id: str,
    ) -> ArrowTableSource:
        columns = {
            names[0]: [1, 2, 3],
            names[1]: values,
            data[0]: [4, 5, 6],
            data[1]: [7, 8, 9],
        }
        schema = pa.schema(
            [pa.field(name, pa.int64(), nullable=False) for name in columns]
        )
        return ArrowTableSource(
            pa.Table.from_pydict(columns, schema=schema),
            tag_columns=tags,
            tag_only_columns=not data_tags,
            source_id=source_id,
        )

    left_tags = ("b", "a") if reverse_tags else ("a", "b")
    return (
        source(("a", "b"), left_tags, ("x", "y"), [10, 20, 30], "left"),
        source(("c", "d"), ("c", "d"), ("m", "n"), [10, 20, 30], "other"),
        source(
            ("a", "b"), (left_tags[1], left_tags[0]), ("u", "v"), [10, 99, 30], "right"
        ),
    )


@pytest.mark.parametrize("data_tags", [False, True], ids=["plain-tags", "data-tags"])
@pytest.mark.parametrize("reverse_tags", [False, True])
@pytest.mark.parametrize("keyed_first", [True, False])
@pytest.mark.parametrize("empty", [True, False], ids=["empty-right", "nonempty-right"])
@pytest.mark.asyncio
async def test_job_semijoin_reused_across_different_keys(
    data_tags: bool, reverse_tags: bool, keyed_first: bool, empty: bool
) -> None:
    """Each job step uses its own key set when another step shares its operator."""

    def build_job() -> PipelineJob:
        left, other, right = _shared_operator_inputs(data_tags, reverse_tags)
        op = SemiJoin()
        job = PipelineJob(name="reused_semijoin", database=InMemoryArrowDatabase())
        with job:
            filter_input = right.polars_filter(u=0, label="empty") if empty else right
            steps = [("keyed", left), ("unkeyed", other)]
            for label, stream in steps if keyed_first else reversed(steps):
                _ = op(stream, filter_input, label=label)
        return job

    sync_job = build_job()
    async_job = build_job()
    _ = sync_job.run()
    _ = await async_job.run_async()
    assert [
        len(sync_job.nodes[label].as_table()) for label in ("keyed", "unkeyed")
    ] == [0 if empty else 2, 3]
    for label in ("keyed", "unkeyed"):
        expected = list(sync_job.nodes[label].iter_data())
        actual = list(async_job.nodes[label].iter_data())
        assert_equivalent(expected, actual)
        assert_same_provenance(expected, actual)


@pytest.mark.asyncio
async def test_direct_empty_right_uses_its_own_validation() -> None:
    """A keyed pair retains its keys after an unkeyed pair is validated."""
    left, other, right = _shared_operator_inputs(False, False)
    empty = right.polars_filter(u=0)
    op = SemiJoin()
    _ = op(left, empty)
    _ = op(other, empty)
    assert (
        await _async_rows(op, left, empty) == sync_rows(SemiJoin(), left, empty) == []
    )


@pytest.mark.parametrize("validate_other", [False, True])
@pytest.mark.asyncio
async def test_direct_empty_right_requires_validation_for_this_pair(
    validate_other: bool,
) -> None:
    """An empty row channel cannot tell a direct call which tags are shared."""
    left, other, right = _shared_operator_inputs(False, False)
    empty = right.polars_filter(u=0)
    op = SemiJoin()
    if validate_other:
        _ = op(other, empty)
    with pytest.raises(
        InputValidationError,
        match="SemiJoin cannot tell its match keys: the right input has no rows "
        "and the inputs were not validated",
    ):
        _ = await _async_rows(op, left, empty)


@pytest.mark.asyncio
async def test_empty_right_after_operator_node_is_rewired() -> None:
    """A node rewired to an unkeyed pair passes through its left rows."""
    left, other, right = _shared_operator_inputs(False, False)
    empty = right.polars_filter(u=0)
    job = PipelineJob(name="rewired_semijoin", database=InMemoryArrowDatabase())
    with job:
        _ = SemiJoin()(left, empty, label="step")
    node = job.nodes["step"]
    assert isinstance(node, OperatorJobNode)
    node.upstreams = (other, empty)

    left_channel: Channel[Row] = Channel(buffer_size=16)
    right_channel: Channel[Row] = Channel(buffer_size=16)
    output: Channel[Row] = Channel(buffer_size=16)
    for row in other.iter_data():
        await left_channel.writer.send(row)
    await left_channel.writer.close()
    await right_channel.writer.close()
    await node.async_execute([left_channel.reader, right_channel.reader], output.writer)
    actual = await output.reader.collect()
    expected = list(node.execute(other, empty).iter_data())
    assert len(expected) == 3
    assert_equivalent(expected, actual)
    assert_same_provenance(expected, actual)


@pytest.mark.parametrize("shared_tags", [True, False], ids=["keyed", "unkeyed"])
@pytest.mark.asyncio
async def test_empty_right_in_loaded_job(tmp_path: Path, shared_tags: bool) -> None:
    """A reconstructed SemiJoin has the right keys before any row arrives."""
    right_path = tmp_path / "right.csv"
    _ = right_path.write_text("a,b,u,v\n1,10,4,7\n2,20,5,8\n3,30,6,9\n")
    left_path = tmp_path / "left.csv"
    left_tags = ("a", "b") if shared_tags else ("c", "d")
    _ = left_path.write_text(
        f"{','.join(left_tags)},x,y\n1,10,4,7\n2,20,5,8\n3,30,6,9\n"
    )
    left = CSVSource(str(left_path), tag_columns=left_tags, tag_only_columns=True)
    right = CSVSource(str(right_path), tag_columns=("a", "b"), tag_only_columns=True)
    db_path = tmp_path / "db"
    job = PipelineJob(name="loaded_semijoin", database=DeltaTableDatabase(db_path))
    with job:
        empty = right.filter(u=0, label="empty")
        _ = SemiJoin()(left, empty, label="step")
    job_path = tmp_path / "job.json"
    job.save(job_path)

    sync_job = PipelineJob.load(job_path, database=DeltaTableDatabase(db_path))
    async_job = PipelineJob.load(job_path, database=DeltaTableDatabase(db_path))
    _ = sync_job.run(load_only=True)
    _ = await async_job.run_async(load_only=True)
    expected = list(sync_job.nodes["step"].iter_data())
    actual = list(async_job.nodes["step"].iter_data())
    assert len(expected) == (0 if shared_tags else 3)
    assert_equivalent(expected, actual)
    assert_same_provenance(expected, actual)
