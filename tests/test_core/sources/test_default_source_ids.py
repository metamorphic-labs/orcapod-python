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
from orcapod.core.operators import MapData
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
