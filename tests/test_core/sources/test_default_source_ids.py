"""Default ids from a hash: ``<kind>_v<N>:<algorithm label>:<hex>`` (ITL-957, ITL-1425).

Developer decision of November 11, 2026: a source id is the source's name and must make
the source easy to identify. Every source id starts with its kind part,
``<SOURCE_KIND>_v<SOURCE_VERSION>`` (November 23, 2026, ITL-1425); a default name built
from a hash carries the hash's own label. The one string is used unchanged as the
``source_id`` system tag value and inside the ``_source_<col>`` values. A name given by
the user is never changed: the kind part is put in front of it.
"""

from __future__ import annotations

from collections.abc import Callable

import polars as pl
import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.config import HashingConfig, OrcapodConfig
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.operators import Join, MapData, SelectDataColumns
from orcapod.core.sources import (
    ArrowTableSource,
    DataFrameSource,
    DerivedSource,
    DictSource,
    ListSource,
    RootSource,
)
from orcapod.core.sources.source_ids import derived_source_name
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import RowIdentifierError
from orcapod.system_constants import constants
from orcapod.types import CacheMode

from ..conftest import double, make_int_stream

ROWS = [{"id": 1, "x": 10}, {"id": 2, "x": 20}, {"id": 3, "x": 30}]


def _arrow_table() -> pa.Table:
    return pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "x": pa.array([10, 20, 30], pa.int64()),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )


def _arrow_table_source(**kwargs) -> RootSource:
    return ArrowTableSource(
        _arrow_table(), tag_columns=["id"], tag_only_columns=True, **kwargs
    )


def _dict_source(**kwargs) -> RootSource:
    return DictSource(ROWS, tag_columns=["id"], tag_only_columns=True, **kwargs)


def _list_source(**kwargs) -> RootSource:
    return ListSource("x", [10, 20, 30], **kwargs)


def _data_frame_source(**kwargs) -> RootSource:
    return DataFrameSource(
        pl.DataFrame(ROWS), tag_columns=["id"], tag_only_columns=True, **kwargs
    )


# (source class, expected kind, factory, name of a data column)
InMemorySourceClass = (
    type[ArrowTableSource] | type[DictSource] | type[ListSource] | type[DataFrameSource]
)
IN_MEMORY_SOURCES: list[
    tuple[InMemorySourceClass, str, Callable[..., RootSource], str]
] = [
    (ArrowTableSource, "arrow_table", _arrow_table_source, "x"),
    (DictSource, "dict", _dict_source, "x"),
    (ListSource, "list", _list_source, "x"),
    (DataFrameSource, "data_frame", _data_frame_source, "x"),
]
IDS = [kind for _, kind, _, _ in IN_MEMORY_SOURCES]


def _arrow_label(source: RootSource) -> str:
    """The label the source's Arrow hasher puts on a table hash (never a literal)."""
    return source.data_context.arrow_hasher.hash_table(_arrow_table()).method


def _source_id_values(source: RootSource) -> set[str]:
    table = source.as_table(all_info=True)
    columns = [
        c
        for c in table.column_names
        if c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
    ]
    assert columns, "the source has no source_id system tag column"
    values: set[str] = set()
    for column in columns:
        for value in table.column(column).to_pylist():
            assert isinstance(value, str)
            values.add(value)
    return values


@pytest.mark.parametrize(
    ("cls", "kind", "make", "data_column"), IN_MEMORY_SOURCES, ids=IDS
)
class TestInMemoryDefaultSourceId:
    def test_default_is_kind_label_hex(self, cls, kind, make, data_column):
        source = make()
        parts = source.source_id.split(":")
        assert len(parts) == 3, source.source_id
        assert parts[0] == f"{kind}_v1"
        assert parts[1] == _arrow_label(source)
        assert len(parts[2]) >= 64
        int(parts[2], 16)  # raises unless the last part is hex

    def test_kind_is_the_config_type_key_defined_once_on_the_class(
        self, cls, kind, make, data_column
    ):
        source = make()
        assert cls.SOURCE_KIND == kind
        assert source.to_config()["source_type"] == cls.SOURCE_KIND
        assert source.source_id.startswith(f"{cls.SOURCE_KIND}_v{cls.SOURCE_VERSION}:")

    def test_same_string_in_system_tag_and_source_info(
        self, cls, kind, make, data_column
    ):
        source = make()
        assert _source_id_values(source) == {source.source_id}
        table = source.as_table(all_info=True)
        info = table.column(f"{constants.SOURCE_PREFIX}{data_column}").to_pylist()
        sep = constants.BLOCK_SEPARATOR
        assert info == [
            f"{source.source_id}{sep}row_{i}{sep}{data_column}" for i in range(3)
        ]

    def test_default_never_contains_the_block_separator(
        self, cls, kind, make, data_column
    ):
        assert constants.BLOCK_SEPARATOR not in make().source_id

    def test_default_is_deterministic(self, cls, kind, make, data_column):
        assert make().source_id == make().source_id

    @pytest.mark.parametrize("given", ["my_source", "a:b:c", "0123abcd", "arrow_table"])
    def test_given_name_is_never_changed(self, cls, kind, make, data_column, given):
        source = make(source_id=given)
        full = f"{kind}_v1:{given}"
        assert source.source_name == given
        assert source.source_id == full
        assert _source_id_values(source) == {full}
        table = source.as_table(all_info=True)
        info = table.column(f"{constants.SOURCE_PREFIX}{data_column}").to_pylist()
        sep = constants.BLOCK_SEPARATOR
        assert info == [f"{full}{sep}row_{i}{sep}{data_column}" for i in range(3)]

    def test_default_is_full_length_whatever_path_n_char(
        self, cls, kind, make, data_column
    ):
        """A default source id is a name, never shortened (developer, November 16, 2026)."""
        full = make().source_id
        config = OrcapodConfig(hashing=HashingConfig(path_n_char=12))
        assert make(config=config).source_id == full


class TestArrowTableDefaultIsTheLabelledTableHash:
    def test_default_is_kind_plus_the_table_hash_string(self):
        source = _arrow_table_source()
        table_hash = source.data_context.arrow_hasher.hash_table(_arrow_table())
        assert source.source_id == f"arrow_table_v1:{table_hash.to_string()}"


class TestKindsTellSourcesApart:
    def test_each_in_memory_class_has_its_own_kind(self):
        kinds = [cls.SOURCE_KIND for cls, _, _, _ in IN_MEMORY_SOURCES]
        assert len(set(kinds)) == len(kinds)

    def test_same_table_under_two_classes_gets_two_ids(self):
        # A DataFrameSource given an explicit schema builds the very table the
        # ArrowTableSource is given, so only the kind differs.
        arrow = _arrow_table_source()
        frame = DataFrameSource(
            pl.DataFrame(ROWS),
            tag_columns=["id"],
            schema=_arrow_table().schema,
            tag_only_columns=True,
        )
        assert arrow.source_id.split(":")[1:] == frame.source_id.split(":")[1:]
        assert arrow.source_id != frame.source_id


# ---------------------------------------------------------------------------
# node.as_source() and a plain node-backed DerivedSource
# ---------------------------------------------------------------------------


def _function_node() -> FunctionJobNode:
    return FunctionJobNode(
        function_pod=FunctionPod.from_fn(double, output_keys="result"),
        input_stream=make_int_stream(n=3),
        pipeline_database=InMemoryArrowDatabase(),
    )


def _operator_node() -> OperatorJobNode:
    return OperatorJobNode(
        operator=MapData({"x": "renamed_x"}),
        input_streams=(make_int_stream(n=3),),
        pipeline_database=InMemoryArrowDatabase(),
        cache_mode=CacheMode.LOG,
    )


NODES = [_function_node, _operator_node]
NODE_IDS = ["function_node", "operator_node"]


def _job(name: str | tuple[str, ...], database: InMemoryArrowDatabase) -> PipelineJob:
    """Compile a function and an operator over a named source."""
    job = PipelineJob(name=name, database=database)
    with job:
        doubled = FunctionPod.from_fn(double, output_keys="result")(
            _dict_source(source_id="inputs"), label="double"
        )
        MapData({"result": "renamed_result"})(doubled, label="rename")
    return job


@pytest.mark.parametrize("label", ["double", "rename"])
def test_default_names_the_job_and_node_content(label):
    first = _job(("project", "first"), InMemoryArrowDatabase().at("database_prefix"))
    second = _job(("project", "second"), InMemoryArrowDatabase())
    another_database = _job(("project", "first"), InMemoryArrowDatabase())
    a, b, c = (job.nodes[label] for job in (first, second, another_database))
    assert a.content_hash() == b.content_hash() == c.content_hash()
    labelled_hash = a.content_hash().to_string()
    derived = a.as_source()
    assert derived.source_id == f"derived_v1:project/first:{labelled_hash}"
    assert b.as_source().source_id == f"derived_v1:project/second:{labelled_hash}"
    assert derived.source_id == a.as_source().source_id == c.as_source().source_id
    assert derived.source_id == DerivedSource(a).source_id
    assert derived.content_hash() != b.as_source().content_hash()
    assert derived.content_hash() == c.as_source().content_hash()
    assert derived.pipeline_hash() == b.as_source().pipeline_hash()


@pytest.mark.parametrize("label", ["double", "rename"])
def test_plain_content_identity_uses_the_given_source_name(label):
    node = _job("first", InMemoryArrowDatabase()).nodes[label]
    first = node.as_source(source_id="my:first")
    second = node.as_source(source_id="my:second")
    assert first.source_id == "derived_v1:my:first"
    assert first.identity_structure() == RootSource.identity_structure(first)
    assert first.identity_structure()[-1] == ()
    assert first.content_hash() != second.content_hash()
    assert first.pipeline_hash() == second.pipeline_hash()


@pytest.mark.parametrize("label", ["double", "rename"])
def test_save_and_load_preserve_the_default_name(tmp_path, label):
    database = InMemoryArrowDatabase()
    job = _job(("project", "first"), database)
    job.run()
    saved = tmp_path / "job.json"
    job.save(saved)
    loaded = PipelineJob.load(saved, database=database)
    original = job.nodes[label].as_source()
    restored = loaded.nodes[label].as_source()
    assert restored.source_id == original.source_id
    assert restored.content_hash() == original.content_hash()


@pytest.mark.parametrize("name", [("project:part",), ()])
def test_unsafe_job_name_requires_an_explicit_source_id(name):
    node = _job(name, InMemoryArrowDatabase()).nodes["double"]
    with pytest.raises(ValueError, match="give the source a source_id"):
        node.as_source()
    assert node.as_source(source_id="my:name").source_id == "derived_v1:my:name"


@pytest.mark.parametrize("name", [("project/part",), ("",)])
def test_invalid_path_components_cannot_contribute_to_a_default_id(name):
    with pytest.raises(ValueError, match="give the source a source_id"):
        derived_source_name(name, _function_node().content_hash())


def test_latest_operator_history_keeps_earlier_other_inputs():
    database = InMemoryArrowDatabase()

    def run(rows):
        source = DictSource(
            data=rows,
            tag_columns=["id"],
            tag_only_columns=True,
            record_id_columns="id",
            source_id="inputs",
        )
        job = PipelineJob(name="stage", database=database)
        with job:
            MapData({"x": "renamed_x"})(source, label="rename")
        node = job.nodes["rename"]
        assert isinstance(node, OperatorJobNode)
        node.attach_databases(database.at(*job.name), cache_mode=CacheMode.LOG)
        job.run()
        return node

    first = run([{"id": 1, "x": 10}, {"id": 2, "x": 20}])
    earlier = first.as_source().as_table(all_info=True)
    second = run([{"id": 1, "x": 100}])
    records = second.get_all_records()
    assert records is not None and records.num_rows == 3
    derived = second.as_source()
    after = derived.as_table(all_info=True)
    assert {r["id"]: r["renamed_x"] for r in after.to_pylist()} == {1: 100, 2: 20}
    (record_key,) = [c for c in after.column_names if c.startswith("_tag_record_id")]
    assert earlier.sort_by("id")[record_key].equals(after.sort_by("id")[record_key])


def test_plain_source_refuses_rows_without_system_lineage():
    node = _function_node()
    node.run()
    with pytest.raises(RowIdentifierError, match="no complete.*pair"):
        node.as_source(source_id="stored").as_table()


def test_latest_function_history_breaks_timestamp_ties_by_result_uuid(monkeypatch):
    db = InMemoryArrowDatabase()
    job = _job("stage", db)
    job.run()
    node = job.nodes["double"]
    origin = node.get_all_records(all_info=True).slice(0, 1)
    greater = bytes.fromhex("01990000000170008000000000000001")
    lesser = bytes.fromhex("01990000000070008000000000000001")

    def with_result(result_id, value):
        table = origin.set_column(
            origin.schema.get_field_index("__data_id"),
            origin.schema.field("__data_id"),
            pa.array([result_id], type=origin["__data_id"].type),
        )
        return table.set_column(
            table.schema.get_field_index("result"),
            table.schema.field("result"),
            pa.array([value], type=table["result"].type),
        )

    stored = pa.concat_tables([with_result(greater, 100), with_result(lesser, 10)])
    monkeypatch.setattr(node, "get_all_records", lambda **_: stored)
    assert node.as_source().as_table()["result"].to_pylist() == [100]


def test_operator_lineage_tie_is_refused():
    raw = pa.table(
        {
            "id": [1, 1],
            "x": [10, 20],
            "_tag_source_id::lineage": ["dict_v1:inputs"] * 2,
            "_tag_record_id::lineage": ["row_0"] * 2,
        }
    )
    from orcapod.core.streams import ArrowTableStream

    node = OperatorJobNode(
        MapData({"x": "renamed_x"}),
        [ArrowTableStream(raw, tag_columns=["id"])],
        pipeline_database=InMemoryArrowDatabase(),
        cache_mode=CacheMode.LOG,
        label="replicated",
    )
    node.run()
    with pytest.raises(
        RowIdentifierError, match="replicated.*repeated the same system tags"
    ):
        node.as_source(source_id="stored").as_table()


def test_derived_source_over_join_binds_with_one_pair():
    left = _dict_source(source_id="left")
    right = DictSource(
        data=[{"id": 1, "y": 50}],
        tag_columns=["id"],
        tag_only_columns=True,
        source_id="right",
    )
    database = InMemoryArrowDatabase()
    origin = PipelineJob(name="stage1", database=database)
    with origin:
        Join()(left, right, label="joined")
    joined = origin.nodes["joined"]
    assert isinstance(joined, OperatorJobNode)
    joined.attach_databases(database.at(*origin.name), cache_mode=CacheMode.LOG)
    origin.run()
    assert len([n for n in joined.keys(all_info=True)[0] if n.startswith("_tag_")]) == 4
    derived = joined.as_source()

    consumer = PipelineJob(name="stage2", database=InMemoryArrowDatabase())
    with consumer:
        SelectDataColumns(["y"])(derived, label="selected")
    consumer.run()
    table = consumer.nodes["selected"].as_table(all_info=True)
    assert table["y"].to_pylist() == [50]
    assert len([n for n in table.column_names if n.startswith("_tag_")]) == 2


@pytest.mark.asyncio
async def test_plain_source_reads_stored_rows_sync_and_async():
    origin = DictSource(
        data=[{"id": 1, "group": "a", "x": 10, "y": 20}],
        tag_columns=["id", "group"],
        tag_only_columns=True,
        source_id="inputs",
    )
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="stage", database=database)
    with job:
        MapData({"x": "renamed_x"})(origin, label="rename")
    node = job.nodes["rename"]
    assert isinstance(node, OperatorJobNode)
    node.attach_databases(database.at(*job.name), cache_mode=CacheMode.LOG)
    job.run()
    source = node.as_source()
    expected = list(source.iter_data())
    assert [row async for row in source.async_iter_data()] == expected
    assert [dict(tag) for tag, _ in expected] == [{"id": 1, "group": "a"}]
    assert [dict(data) for _, data in expected] == [{"renamed_x": 10, "y": 20}]
    tag, data = expected[0]
    systems = tag.system_tags()
    assert len(systems) == 2
    assert set(systems) == {
        name
        for name in source.keys(columns={"system_tags": True})[0]
        if name.startswith("_tag_")
    }
    source_id = next(v for k, v in systems.items() if k.startswith("_tag_source_id"))
    record_id = next(v for k, v in systems.items() if k.startswith("_tag_record_id"))
    assert source_id == source.source_id
    assert record_id != "row_0"
    assert data.source_info() == {
        name: f"{source_id}::{record_id}::{name}" for name in ("renamed_x", "y")
    }
    assert source.head(all_info=True).table.equals(source.as_table(all_info=True))


@pytest.mark.parametrize("make_node", NODES, ids=NODE_IDS)
class TestAsSourceDefaultSourceId:
    def test_without_a_job_an_explicit_source_id_is_required(self, make_node):
        node = make_node()
        with pytest.raises(ValueError, match="give the source a source_id"):
            node.as_source()
        assert node.as_source(source_id="my:name").source_id == "derived_v1:my:name"

    def test_given_name_is_never_changed(self, make_node):
        source = DerivedSource(make_node(), source_id="my:name")
        assert source.source_name == "my:name"
        assert source.source_id == "derived_v1:my:name"
