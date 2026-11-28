"""The upstream setters preserve the schemas of a wired node (ITL-1125)."""

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import (
    FunctionJobNode,
    FunctionNode,
    OperatorJobNode,
    OperatorNode,
)
from orcapod.core.operators import Join, MapData
from orcapod.core.sources import DictSource
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.system_constants import OPERATOR_DB_SCHEMA_VERSION
from orcapod.types import CacheMode

_SYSTEM_TAGS = {"system_tags": True}


@function_pod(output_keys="total")
def _total(x: int | None, y: int | None) -> int:
    """Add the two data values."""
    return (x or 0) + (y or 0)


def _raw(
    x: int,
    reverse: bool,
    *,
    note: int | str | None = None,
    system_type: pa.DataType | None = None,
) -> ArrowTableStream:
    """Make a raw stream with two tags and two data columns."""
    items = [("id", 1), ("group", "a"), ("x", x), ("y", 5)]
    row = dict(reversed(items) if reverse else items)
    if note is not None:
        row["__note"] = note
    table = pa.Table.from_pylist([row])
    if system_type is not None:
        table = table.append_column("custom_id", pa.array([100], type=system_type))
    return ArrowTableStream(
        table,
        tag_columns=["group", "id"] if reverse else ["id", "group"],
        system_tag_columns=["custom_id"] if system_type is not None else [],
    )


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("kind", ["operator", "function"])
@pytest.mark.parametrize("blueprint", [False, True])
def test_rewiring_raw_stream_ignores_data_meta(
    kind: str, reverse: bool, blueprint: bool
) -> None:
    """A raw stream's data meta columns do not restrict a replacement."""
    before = _raw(4, reverse, note=8 if kind == "operator" else None)
    after = _raw(14, not reverse, note="replacement" if reverse else 18)
    if kind == "operator":
        node_class = OperatorNode if blueprint else OperatorJobNode
        node = node_class(MapData({"x": "xx", "y": "yy"}), (before,), label="step")
        expected = {"id": 1, "group": "a", "xx": 14, "yy": 5}
    else:
        pod = _total.pod
        assert isinstance(pod, FunctionPod)
        if blueprint:
            node = FunctionNode(pod, before, label="step")
        else:
            node = FunctionJobNode(
                pod, before, label="step", pipeline_database=InMemoryArrowDatabase()
            )
        expected = {"id": 1, "group": "a", "total": 19}

    node.upstreams = (after,)

    assert node.upstreams == (after,)
    tag, data = node.output_schema()
    assert set(tag) | set(data) == set(expected)
    if isinstance(node, (OperatorJobNode, FunctionJobNode)):
        node.run()
        assert node.as_table().to_pylist() == [expected]


@pytest.mark.parametrize("reverse", [False, True])
def test_rewiring_refuses_system_tag_arrow_width(reverse: bool) -> None:
    """Equal Python hints can conceal a change to a system-tag Arrow field."""
    before = _raw(4, reverse, system_type=pa.int32())
    after = _raw(14, not reverse, system_type=pa.int64())
    assert before.output_schema(columns=_SYSTEM_TAGS) == after.output_schema(
        columns=_SYSTEM_TAGS
    )
    node = OperatorJobNode(MapData({"x": "xx", "y": "yy"}), (before,), label="step")
    original = node.upstreams
    schema = node.output_schema(columns=_SYSTEM_TAGS)
    identity = node.node_identity_path

    with pytest.raises(ValueError, match=r"(?s)'step'.*system-tag.*int32.*int64"):
        node.upstreams = (after,)

    assert node.upstreams is original
    assert node.output_schema(columns=_SYSTEM_TAGS) is schema
    assert node.node_identity_path == identity
    assert (
        before.as_table(columns=_SYSTEM_TAGS).schema.field("_tag_custom_id").type
        == pa.int32()
    )
    node.run()
    assert node.as_table().column("xx").to_pylist() == [4]


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_refused_multi_input_rewire_preserves_job_output(
    orchestrator: type[SyncPipelineOrchestrator] | type[AsyncPipelineOrchestrator],
    reverse: bool,
) -> None:
    """A refusal leaves both real job paths able to write the original output."""
    tags = [("id", 1), ("group", "a")]
    rows = [[*tags, ("x", 4), ("y", 5)], [*tags, ("q", 8), ("z", 9)]]
    if reverse:
        rows = [list(reversed(row)) for row in reversed(rows)]
        tags.reverse()
    sources = [
        DictSource(
            [dict(row)],
            tag_columns=[name for name, _ in tags],
            tag_only_columns=["id"] if reverse else True,
            source_id=f"src_{index}",
        )
        for index, row in enumerate(rows)
    ]
    db = InMemoryArrowDatabase()
    job = PipelineJob(name="rewire", database=db)
    with job:
        joined = Join()(*sources, label="join")
        mapped_columns = [("x", "xx"), ("q", "qq")]
        if reverse:
            mapped_columns.reverse()
        MapData(dict(mapped_columns))(joined, label="mapped")
    node, downstream = job.nodes["join"], job.nodes["mapped"]
    assert isinstance(node, OperatorJobNode)
    assert isinstance(downstream, OperatorJobNode)
    downstream.attach_databases(db, CacheMode.LOG)
    upstreams = node.upstreams
    first_replacement = next(
        source
        for source in sources
        if source.output_schema() == upstreams[0].output_schema()
    )
    assert first_replacement is not upstreams[0]
    schema = downstream.output_schema(columns=_SYSTEM_TAGS)
    pipeline_hash = downstream.pipeline_hash()
    path = downstream.node_identity_path

    with pytest.raises(ValueError, match=r"'join'.*upstream 1"):
        node.upstreams = (first_replacement, upstreams[0])

    assert node.upstreams is upstreams
    assert downstream.output_schema(columns=_SYSTEM_TAGS) is schema
    assert downstream.pipeline_hash() == pipeline_hash
    assert downstream.node_identity_path == path
    assert job.run(orchestrator=orchestrator()).ok
    expected = {"id": 1, "group": "a", "xx": 4, "y": 5, "qq": 8, "z": 9}
    assert downstream.as_table().to_pylist() == [expected]
    stored = db.get_table_schema(path + (OPERATOR_DB_SCHEMA_VERSION,))
    assert stored is not None
    assert set(expected) <= set(stored.names)
    assert downstream.as_source().as_table().to_pylist() == [expected]


def test_rewiring_refuses_a_different_input_count() -> None:
    """Adding a Join input adds a system-tag pair even if its user schema matches."""
    sources = tuple(
        DictSource(
            [{"id": 1, "group": "a"}],
            tag_columns=["id", "group"],
            tag_only_columns=False,
            source_id=name,
        )
        for name in ("left", "right")
    )
    node = OperatorJobNode(Join(), sources[:1], label="step")
    original = node.upstreams

    with pytest.raises(ValueError, match="'step'.*different upstream count"):
        node.upstreams = sources

    assert node.upstreams is original
