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
from orcapod.core.sources import DictSource, PollingSource
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.system_constants import OPERATOR_DB_SCHEMA_VERSION
from orcapod.types import CacheMode, Cursor, Schema

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


class _PollingFeed:
    """A feed whose initial fetch may be empty and whose schema is optional."""

    def __init__(self, value: int | str | None, schema: Schema | None = None) -> None:
        self.value = value
        self._schema = schema
        self.polls = 0
        self.fetches = 0

    def identity(self) -> str:
        return "rewire-feed"

    def to_config(self) -> None:
        return None

    @classmethod
    def from_config(cls, config: dict[str, object]) -> "_PollingFeed":
        return cls(None)

    def schema(self) -> Schema | None:
        return self._schema

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        self.polls += 1
        return self.value is not None and (cursor is None or cursor.value == 0)

    async def fetch(
        self, cursor: Cursor[int] | None = None
    ) -> tuple[Cursor[int], list[dict[str, int | str]]]:
        self.fetches += 1
        if self.value is None:
            return Cursor(value=0), []
        return Cursor(value=1), [{"id": 1, "group": "a", "x": self.value, "y": 5}]

    async def close(self) -> None:
        pass


@pytest.mark.parametrize("initial_value", [None, 4, "four"])
@pytest.mark.parametrize("kind", ["operator", "function"])
def test_rewiring_to_unpolled_source_does_not_fetch(
    kind: str, initial_value: int | str | None
) -> None:
    """A first batch cannot determine whether assignment is accepted."""
    before = DictSource(
        [{"id": 1, "group": "a", "x": 2, "y": 5}],
        tag_columns=["id", "group"],
        tag_only_columns=True,
    )
    feed = _PollingFeed(initial_value)
    after = PollingSource(feed, ["id", "group"], record_id_columns="id")
    if kind == "operator":
        node = OperatorJobNode(MapData({"x": "xx"}), (before,), label="step")
    else:
        pod = _total.pod
        assert isinstance(pod, FunctionPod)
        node = FunctionNode(pod, before, label="step")

    node.upstreams = (after,)

    assert node.upstreams == (after,)
    assert (feed.polls, feed.fetches) == (0, 0)
    if isinstance(node, OperatorJobNode) and initial_value is None:
        feed.value = 4
        node.run()
        assert node.as_table().column("xx").to_pylist() == [4]


def test_rewiring_refuses_different_declared_polling_schema_without_fetch() -> None:
    """An unpolled source can still have a schema known at assignment time."""
    before = DictSource(
        [{"id": 1, "group": "a", "x": 2, "y": 5}],
        tag_columns=["id", "group"],
        tag_only_columns=True,
    )
    feed = _PollingFeed("four", Schema({"id": int, "group": str, "x": str, "y": int}))
    after = PollingSource(feed, ["id", "group"], record_id_columns="id")
    node = OperatorNode(MapData({"x": "xx"}), (before,), label="step")

    with pytest.raises(ValueError, match=r"'step'.*different schema"):
        node.upstreams = (after,)

    assert node.upstreams == (before,)
    assert (feed.polls, feed.fetches) == (0, 0)


@pytest.mark.parametrize("known_schema", [False, True])
def test_function_rewiring_arity_error_includes_node_and_schemas(
    known_schema: bool,
) -> None:
    """Report the current and proposed inputs without fetching an unknown schema."""
    before = _raw(2, False)
    feed = _PollingFeed(None)
    after = (
        ArrowTableStream(pa.table({"id": [1], "new_value": [5]}), tag_columns=["id"])
        if known_schema
        else PollingSource(feed, ["id", "group"], record_id_columns="id")
    )
    pod = _total.pod
    assert isinstance(pod, FunctionPod)
    node = FunctionNode(pod, before, label="named")

    with pytest.raises(
        ValueError,
        match=r"'named'.*different upstream count.*current schemas.*'x'.*replacement schemas",
    ) as error:
        node.upstreams = (before, after)

    replacement_schema = str(error.value).split("replacement schemas", 1)[1]
    assert ("new_value" if known_schema else "unavailable") in replacement_schema
    assert node.upstreams == (before,)
    assert (feed.polls, feed.fetches) == (0, 0)


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
