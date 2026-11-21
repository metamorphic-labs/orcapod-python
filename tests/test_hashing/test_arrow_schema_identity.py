"""A schema enters every identity as its Arrow schema (ITL-640, ITL-949, ITL-1136).

Every case measured on November 18, 2026 (sources that differ only in an Arrow type
shared a pipeline hash, stored paths and system-tag names) is now two identities, with
separate tables and no failed write; the differences ``hash_schema`` ignores (column
order, large and non-large variants, list item names, dictionary encoding, field
metadata) still give one identity. Every source here has two tag columns and two data
columns, in more than one arrangement (the several-items rule, ITL-1236).
"""

from __future__ import annotations

import datetime as dt
import decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, SourceNode, function_pod
from orcapod.core.sources.cached_source import CachedSource
from orcapod.core.sources.polling_source import PollingSource
from orcapod.core.streams.base import typed_arrow_schemas
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import (
    InputValidationError,
    SchemaInconsistencyError,
    SourceSpecMismatchError,
)
from orcapod.sources import ArrowTableSource
from orcapod.core.operators import MapData
from orcapod.types import CacheMode, Cursor, Schema

D = decimal.Decimal
TS = [dt.datetime(2026, 1, 1, 0, 0, 1), dt.datetime(2026, 1, 2)]
TSZ = [t.replace(tzinfo=dt.timezone.utc) for t in TS]
DATES = [dt.date(2026, 1, 1), dt.date(2026, 1, 2)]


def _table(
    x: pa.Array[Any], x_field: pa.Field[Any] | None = None, order: str = "kgxw"
) -> pa.Table:
    """Two tags ``k``, ``g`` and two data columns ``x`` (under test), ``w``."""
    columns = {
        "k": (pa.field("k", pa.large_string(), nullable=False), pa.array(["r1", "r2"])),
        "g": (pa.field("g", pa.int64(), nullable=False), pa.array([1, 2])),
        "x": (x_field if x_field is not None else pa.field("x", x.type), x),
        "w": (pa.field("w", pa.float64(), nullable=False), pa.array([0.5, 1.5])),
    }
    picked = [columns[name] for name in order]
    return pa.Table.from_arrays(
        [array for _, array in picked], schema=pa.schema([f for f, _ in picked])
    )


def _source(table: pa.Table, source_id: str = "s", tags=("k", "g")) -> ArrowTableSource:
    return ArrowTableSource(
        table, tag_columns=list(tags), tag_only_columns=True, source_id=source_id
    )


def _system_tags(source: ArrowTableSource) -> list[str]:
    names = source.as_table(columns={"system_tags": True}).column_names
    return [name for name in names if name.startswith("_tag")]


# Pairs that differ only in the Arrow type of data column ``x``: two identities.
DIFFERENT = {
    "int32/int64": (pa.array([1, 2], pa.int32()), pa.array([1, 2], pa.int64())),
    "float32/float64": (
        pa.array([1.5, 2.5], pa.float32()),
        pa.array([1.5, 2.5], pa.float64()),
    ),
    "int8/uint8": (pa.array([1, 2], pa.int8()), pa.array([1, 2], pa.uint8())),
    "ts[us]/ts[ns]": (
        pa.array(TS, pa.timestamp("us")),
        pa.array(TS, pa.timestamp("ns")),
    ),
    "ts[us]/ts[us,UTC]": (
        pa.array(TS, pa.timestamp("us")),
        pa.array(TSZ, pa.timestamp("us", tz="UTC")),
    ),
    "decimal(10,2)/decimal(20,4)": (
        pa.array([D("1.25"), D("2.50")], pa.decimal128(10, 2)),
        pa.array([D("1.25"), D("2.50")], pa.decimal128(20, 4)),
    ),
    "date32/date64": (pa.array(DATES, pa.date32()), pa.array(DATES, pa.date64())),
    "list item nullable/non-null": (
        pa.array([[1], [2, 3]], pa.large_list(pa.field("item", pa.int64()))),
        pa.array(
            [[1], [2, 3]], pa.large_list(pa.field("item", pa.int64(), nullable=False))
        ),
    ),
    "fixed_size_list/large_list": (
        pa.array([[1, 2], [3, 4]], pa.list_(pa.int64(), 2)),
        pa.array([[1, 2], [3, 4]], pa.large_list(pa.int64())),
    ),
    "struct<a:int32>/struct<a:int64>": (
        pa.array([{"a": 1}, {"a": 2}], pa.struct([("a", pa.int32())])),
        pa.array([{"a": 1}, {"a": 2}], pa.struct([("a", pa.int64())])),
    ),
}

# Pairs that differ only in what ``hash_schema`` ignores: one identity.
SAME = {
    "string/large_string": (
        pa.array(["a", "b"], pa.string()),
        pa.array(["a", "b"], pa.large_string()),
    ),
    "list/large_list, item named item/element": (
        pa.array([[1], [2]], pa.list_(pa.field("item", pa.int64()))),
        pa.array([[1], [2]], pa.large_list(pa.field("element", pa.int64()))),
    ),
}


class TestSourceIdentity:
    @pytest.mark.parametrize("pair", list(DIFFERENT), ids=list(DIFFERENT))
    def test_an_arrow_type_difference_is_another_identity(self, pair: str) -> None:
        a, b = (_source(_table(x)) for x in DIFFERENT[pair])
        assert a.pipeline_hash() != b.pipeline_hash()
        # The same source id: the content hash still tells them apart.
        assert a.content_hash() != b.content_hash()
        assert _system_tags(a) != _system_tags(b)

    @pytest.mark.parametrize("pair", list(SAME), ids=list(SAME))
    def test_what_hash_schema_ignores_is_one_identity(self, pair: str) -> None:
        a, b = (_source(_table(x)) for x in SAME[pair])
        assert a.pipeline_hash() == b.pipeline_hash()
        assert a.content_hash() == b.content_hash()
        assert _system_tags(a) == _system_tags(b)

    def test_top_level_nullability_is_another_identity(self) -> None:
        x = pa.array([1, 2], pa.int64())
        a = _source(_table(x, pa.field("x", pa.int64(), nullable=True)))
        b = _source(_table(x, pa.field("x", pa.int64(), nullable=False)))
        assert a.pipeline_hash() != b.pipeline_hash()

    @pytest.mark.parametrize("tag_type", [pa.int32(), pa.uint64()])
    def test_a_tag_type_difference_is_another_identity(self, tag_type) -> None:
        def with_g(arrow_type: pa.DataType) -> ArrowTableSource:
            table = _table(pa.array([1, 2], pa.int64()))
            index = table.schema.get_field_index("g")
            table = table.set_column(
                index,
                pa.field("g", arrow_type, nullable=False),
                pa.array([1, 2], arrow_type),
            )
            return _source(table)

        assert with_g(tag_type).pipeline_hash() != with_g(pa.int64()).pipeline_hash()

    @pytest.mark.parametrize("order", ["xwgk", "wkxg", "gxkw"])
    def test_column_order_is_not_part_of_it(self, order: str) -> None:
        """Tags and data in other table layouts, and the tags declared in another order."""
        x = pa.array([1, 2], pa.int32())
        base = _source(_table(x))
        assert _source(_table(x, order=order)).pipeline_hash() == base.pipeline_hash()
        assert (
            _source(_table(x, order=order), tags=("g", "k")).pipeline_hash()
            == base.pipeline_hash()
        )

    def test_field_metadata_is_not_part_of_it(self) -> None:
        x = pa.array([1, 2], pa.int32())
        plain = _source(_table(x))
        tagged = _source(
            _table(x, pa.field("x", pa.int32(), metadata={"comment": "a note"}))
        )
        assert plain.pipeline_hash() == tagged.pipeline_hash()

    def test_a_data_tag_and_a_plain_tag_are_two_identities(self) -> None:
        table = _table(pa.array([1, 2], pa.int32()))
        plain = ArrowTableSource(
            table, tag_columns=["k", "g"], tag_only_columns=True, source_id="s"
        )
        data_tag = ArrowTableSource(
            table, tag_columns=["k", "g"], tag_only_columns=["k"], source_id="s"
        )
        assert plain.pipeline_hash() != data_tag.pipeline_hash()


def _run_function_job(database: Any, table: pa.Table, source_id: str) -> Any:
    job = PipelineJob(name="j", database=database)
    with job:
        _total.pod(_source(table, source_id), label="total")
    job.run()
    return job.nodes["total"]


@function_pod(output_keys="y")
def _total(x: int | None, w: float) -> float:
    return (x or 0) + w


def _databases(tmp_path: Path) -> dict[str, Any]:
    return {
        "memory": InMemoryArrowDatabase(),
        "delta": DeltaTableDatabase(tmp_path / "delta"),
    }


def _logged_map(database: Any, source: ArrowTableSource) -> Any:
    """Run ``x`` renamed to ``z`` in a job whose operator node stores its rows (LOG)."""
    job = PipelineJob(name="j", database=database)
    with job:
        source.map_data({"x": "z"}, label="op")
    node = job.nodes["op"]
    node.attach_databases(pipeline_database=database.at("j"), cache_mode=CacheMode.LOG)
    job.run()
    return node


@pytest.mark.parametrize("kind", ["memory", "delta"])
class TestStoredTables:
    def test_function_nodes_over_two_widths_store_apart(
        self, kind: str, tmp_path: Path
    ) -> None:
        database = _databases(tmp_path)[kind]
        a = _run_function_job(database, _table(pa.array([1, 2], pa.int32())), "a")
        b = _run_function_job(database, _table(pa.array([1, 2], pa.int64())), "b")
        assert a.node_identity_path != b.node_identity_path
        assert sorted(a.as_table().column("y").to_pylist()) == [1.5, 3.5]
        assert sorted(b.as_table().column("y").to_pylist()) == [1.5, 3.5]

    @pytest.mark.parametrize(
        "pair", ["int8/uint8", "list item nullable/non-null"], ids=["width", "item"]
    )
    def test_an_operator_cache_keeps_each_type(
        self, kind: str, pair: str, tmp_path: Path
    ) -> None:
        """The two silent Delta changes of November 18: no shared table, no cast."""
        database = _databases(tmp_path)[kind]
        nodes = []
        for index, x in enumerate(DIFFERENT[pair]):
            nodes.append(_logged_map(database, _source(_table(x), f"s{index}")))
        first, second = nodes
        assert first.node_identity_path != second.node_identity_path
        for node, x in zip(nodes, DIFFERENT[pair]):
            stored = node.as_source().as_table().schema.field("z").type
            # The stored type is the input's exactly: width and sign, and a list
            # item's nullability (CodeRabbit on PR 454: compared directly).
            assert stored == x.type
            assert stored == node.as_table().schema.field("z").type

    def test_as_source_keeps_its_identity_when_rows_arrive(
        self, kind: str, tmp_path: Path
    ) -> None:
        database = _databases(tmp_path)[kind]
        job = PipelineJob(name="j", database=database)
        with job:
            _total.pod(_source(_table(pa.array([1, 2], pa.int8()))), label="total")
        before = job.nodes["total"].as_source()
        assert before.as_table().num_rows == 0
        job.run()
        after = job.nodes["total"].as_source()
        assert after.as_table().num_rows == 2
        assert before.pipeline_hash() == after.pipeline_hash()
        assert before.as_table().schema.field("k").type == pa.large_string()
        assert after.as_table().schema.field("g").type == pa.int64()


def test_a_stored_read_of_another_type_is_refused(tmp_path: Path) -> None:
    """Delta reads a fixed_size_list back as large_list (ITL-1275): refused, named."""
    fixed, _ = DIFFERENT["fixed_size_list/large_list"]
    node = _logged_map(DeltaTableDatabase(tmp_path / "delta"), _source(_table(fixed)))
    with pytest.raises(InputValidationError, match=r"'z' reads back as large_list"):
        node.as_source().as_table()


class TestBind:
    def _job(self, slot: SourceNode) -> PipelineJob:
        job = PipelineJob(name="j", database=InMemoryArrowDatabase())
        with job:
            MapData({"x": "z"})(slot, label="op")
        return job

    def test_a_slot_from_a_source_refuses_another_width(self) -> None:
        source = _source(_table(pa.array([1, 2], pa.int32())))
        job = self._job(SourceNode.from_stream(source, name="src"))
        other = _source(_table(pa.array([1, 2], pa.int64())))
        with pytest.raises(
            SourceSpecMismatchError,
            match=r"'x' has the Arrow type int32 in the slot but int64 in the source",
        ):
            job.bind(sources={"src": other})
        job.bind(sources={"src": _source(_table(pa.array([3, 4], pa.int32())), "t")})

    def test_a_non_null_column_binds_to_a_nullable_slot_column(self) -> None:
        slot_table = _table(pa.array([1, 2], pa.int32()))
        job = self._job(SourceNode.from_stream(_source(slot_table), name="src"))
        narrower = _table(
            pa.array([1, 2], pa.int32()), pa.field("x", pa.int32(), nullable=False)
        )
        job.bind(sources={"src": _source(narrower, "t")})

    def test_nested_nullability_must_match(self) -> None:
        nullable_item, non_null_item = DIFFERENT["list item nullable/non-null"]
        job = self._job(
            SourceNode.from_stream(_source(_table(nullable_item)), name="src")
        )
        with pytest.raises(SourceSpecMismatchError, match="'x' has the Arrow type"):
            job.bind(sources={"src": _source(_table(non_null_item), "t")})

    def test_a_declared_python_type_means_one_arrow_type(self) -> None:
        slot = SourceNode(
            name="src",
            tag_schema=Schema({"k": str, "g": int}),
            data_schema=Schema({"x": dt.datetime | None, "w": float}),
        )
        job = self._job(slot)
        naive = _source(_table(pa.array(TS, pa.timestamp("ns"))))
        with pytest.raises(
            SourceSpecMismatchError,
            match=r"declared datetime.datetime \| None means timestamp\[us, tz=UTC\]; declare "
            "the slot from the source or with an Arrow schema",
        ):
            job.bind(sources={"src": naive})

    def test_a_slot_declared_in_arrow_takes_those_types(self) -> None:
        table = _table(pa.array(TS, pa.timestamp("ns")))
        slot = SourceNode(
            name="src",
            tag_schema=pa.schema([table.schema.field("k"), table.schema.field("g")]),
            data_schema=pa.schema([table.schema.field("x"), table.schema.field("w")]),
        )
        job = self._job(slot)
        job.bind(sources={"src": _source(table)})
        assert slot.pipeline_hash() == _source(table).pipeline_hash()

    def test_a_saved_job_keeps_the_slot_types(self, tmp_path: Path) -> None:
        source = _source(_table(pa.array([1, 2], pa.int8())))
        job = self._job(SourceNode.from_stream(source, name="src"))
        job.bind(sources={"src": source})
        job.save(tmp_path / "job.json")
        loaded = PipelineJob.load(tmp_path / "job.json")
        assert loaded.nodes["op"].pipeline_hash() == job.nodes["op"].pipeline_hash()
        assert loaded.nodes["src"].arrow_schemas[1].field("x").type == pa.int8()


class _Impl:
    """A polling implementation fetching one fixed table."""

    def __init__(self, table: pa.Table, schema: Any) -> None:
        self._table, self._schema = table, schema

    def identity(self) -> Any:
        return ("arrow-schema-identity", 1)

    def to_config(self) -> dict[str, Any] | None:
        return None

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "_Impl":
        raise NotImplementedError

    def schema(self) -> Any:
        return self._schema

    async def poll(self, cursor: Any = None) -> bool:
        return cursor is None

    async def fetch(self, cursor: Any = None) -> Any:
        return Cursor.now(1), self._table

    async def close(self) -> None:
        return None


class TestPollingSource:
    def test_a_batch_of_another_arrow_type_is_refused(self) -> None:
        table = _table(pa.array([1, 2], pa.int32()))
        declared = Schema({"k": str, "g": int, "x": int | None, "w": float})
        source = PollingSource(
            _Impl(table, declared), tag_columns=["k", "g"], record_id_columns="k"
        )
        before = source.pipeline_hash()
        with pytest.raises(
            SchemaInconsistencyError,
            match="fetched column 'x' has the Arrow type int32, but the source "
            "declares int64",
        ):
            source.as_table()
        assert source.pipeline_hash() == before

    def test_an_unpolled_preview_has_the_declared_arrow_types(self) -> None:
        """``head()`` before a poll shows the declared types, not the Python types'
        (CodeRabbit on PR 454), and fetches nothing."""
        table = _table(pa.array([1, 2], pa.int32()))
        declared = table.schema.set(
            table.schema.get_field_index("g"), pa.field("g", pa.int16(), nullable=False)
        )

        class _NeverFetched(_Impl):
            async def fetch(self, cursor: Any = None) -> Any:
                raise AssertionError("fetched")

        source = PollingSource(
            _NeverFetched(table, declared),
            tag_columns=["k", "g"],
            record_id_columns="k",
        )
        for columns in (None, {"system_tags": True}):
            shown = source.head(columns=columns).as_table().schema
            assert shown.field("g").type == pa.int16()
            assert shown.field("x").type == pa.int32()
            assert shown.field("w").type == pa.float64()

    @pytest.mark.parametrize("form", ["python", "arrow"])
    def test_a_batch_with_undeclared_columns_is_refused(self, form: str) -> None:
        """A column the declaration does not name would move the system-tag names."""
        table = _table(pa.array([1, 2], pa.int64()))
        extra = table.append_column("y", pa.array([3, 4], pa.int64())).append_column(
            "z", pa.array(["a", "b"], pa.large_string())
        )
        declared: Any = (
            Schema({"k": str, "g": int, "x": int | None, "w": float})
            if form == "python"
            else table.schema
        )
        source = PollingSource(
            _Impl(extra, declared), tag_columns=["k", "g"], record_id_columns="k"
        )
        before = source.pipeline_hash()
        with pytest.raises(
            SchemaInconsistencyError,
            match=r"the fetched columns \['y', 'z'\] are not in the declared schema",
        ):
            source.as_table()
        assert source.pipeline_hash() == before
        matching = PollingSource(
            _Impl(table, declared), tag_columns=["k", "g"], record_id_columns="k"
        )
        assert matching.as_table().num_rows == 2

    def test_an_arrow_declaration_is_the_identity_exactly(self) -> None:
        table = _table(pa.array([1, 2], pa.int32()))
        source = PollingSource(
            _Impl(table, table.schema), tag_columns=["k", "g"], record_id_columns="k"
        )
        assert source.as_table().schema.field("x").type == pa.int32()
        assert source.pipeline_hash() == _source(table).pipeline_hash()


@pytest.mark.parametrize("kind", ["memory", "delta"])
@pytest.mark.parametrize(
    "pair", ["int8/uint8", "ts[us]/ts[ns]"], ids=["width", "timestamp-unit"]
)
def test_the_async_run_stores_where_and_as_the_sync_run_does(
    kind: str, pair: str, tmp_path: Path
) -> None:
    """Both paths give each Arrow type its own table and keep the type (ITL-640)."""
    import asyncio

    results: dict[str, list[tuple[tuple[str, ...], pa.DataType]]] = {}
    for path in ("sync", "async"):
        database = _databases(tmp_path / path)[kind]
        seen = []
        for index, x in enumerate(DIFFERENT[pair]):
            job = PipelineJob(name="j", database=database)
            with job:
                _source(_table(x), f"s{index}").map_data({"x": "z"}, label="op")
            node = job.nodes["op"]
            node.attach_databases(
                pipeline_database=database.at("j"), cache_mode=CacheMode.LOG
            )
            if path == "sync":
                job.run()
            else:
                asyncio.run(job.run_async())
            stored = node.as_source().as_table().schema.field("z").type
            seen.append((tuple(node.node_identity_path), stored))
        results[path] = seen
    assert results["sync"] == results["async"]
    (first_path, first_type), (second_path, second_type) = results["sync"]
    assert first_path != second_path
    assert (first_type, second_type) == tuple(x.type for x in DIFFERENT[pair])


# ITL-1280: ``node.as_source()`` rebuilt its Arrow schema from Python types before a run,
# widening narrow ints and floats, giving a timestamp UTC microseconds and raising on a
# decimal. Its types now are its origin's, before and after rows are stored.
AS_SOURCE_TYPES: dict[str, pa.Array[Any]] = {
    "int8": pa.array([1, 2], pa.int8()),
    "int32": pa.array([1, 2], pa.int32()),
    "uint16": pa.array([1, 2], pa.uint16()),
    "float16": pa.array([1.5, 2.5], pa.float32()).cast(pa.float16()),
    "float32": pa.array([1.5, 2.5], pa.float32()),
    "ts[s] naive": pa.array(TS, pa.timestamp("s")),
    "ts[us] naive": pa.array(TS, pa.timestamp("us")),
    "ts[ms, Europe/Berlin]": pa.array(TS, pa.timestamp("ms", tz="Europe/Berlin")),
    "decimal128(10,2)": pa.array([D("1.25"), D("2.50")], pa.decimal128(10, 2)),
}


@function_pod(output_keys="y")
def _plus_one(w: float) -> float:
    return w + 1


def _as_source_cases() -> list[Any]:
    cases = []
    for type_name in AS_SOURCE_TYPES:
        for role, node_kind in (
            ("tag", "operator"),
            ("tag", "function"),
            ("data", "operator"),
        ):
            for kind in ("memory", "delta"):
                for path in ("sync", "async"):
                    if type_name == "float16" and kind == "delta":
                        # Delta Lake has no float16: the write is refused loudly.
                        continue
                    cases.append(
                        pytest.param(
                            type_name,
                            role,
                            node_kind,
                            kind,
                            path,
                            id=f"{type_name}-{role}-{node_kind}-{kind}-{path}",
                        )
                    )
    return cases


@pytest.mark.parametrize(
    ("type_name", "role", "node_kind", "kind", "path"), _as_source_cases()
)
def test_as_source_has_its_origins_types_before_and_after_a_run(
    type_name: str, role: str, node_kind: str, kind: str, path: str, tmp_path: Path
) -> None:
    """Tag ``g`` or data ``x`` of the type, beside a second tag and data column."""
    import asyncio

    column = AS_SOURCE_TYPES[type_name]
    g = column if role == "tag" else pa.array([1, 2], pa.int64())
    x = column if role == "data" else pa.array([1, 2], pa.int64())
    table = pa.Table.from_arrays(
        [pa.array(["r1", "r2"], pa.large_string()), g, x, pa.array([0.5, 1.5])],
        schema=pa.schema(
            [
                pa.field("k", pa.large_string(), nullable=False),
                pa.field("g", g.type),
                pa.field("x", x.type),
                pa.field("w", pa.float64(), nullable=False),
            ]
        ),
    )
    database = _databases(tmp_path)[kind]
    job = PipelineJob(name="j", database=database)
    with job:
        if node_kind == "operator":
            _source(table).map_data({"x": "z"}, label="node")
        else:
            _plus_one.pod(_source(table), label="node")
    node = job.nodes["node"]
    if node_kind == "operator":
        node.attach_databases(
            pipeline_database=database.at("j"), cache_mode=CacheMode.LOG
        )
    name = "g" if role == "tag" else "z"
    before = node.as_source()
    before_type = before.as_table().schema.field(name).type
    if path == "sync":
        job.run()
    else:
        asyncio.run(job.run_async())
    after = node.as_source()
    after_table = after.as_table()
    assert after_table.num_rows == 2
    assert before_type == after_table.schema.field(name).type == column.type
    assert node.as_table().schema.field(name).type == column.type
    assert before.pipeline_hash() == after.pipeline_hash()
    assert before.content_hash() == after.content_hash()


# CodeRabbit on PR 454: a CachedSource presents its source's declared Arrow types; a
# cache store that gives a column back as another type is refused, never converted, so
# an empty and a filled cache can never differ.
FIXED_SIZE = pa.array([[1, 2], [3, 4]], pa.list_(pa.int8(), 2))


def _cached(table: pa.Table, database: Any) -> CachedSource:
    return CachedSource(_source(table), cache_database=database)


@pytest.mark.parametrize("kind", ["memory", "delta"])
def test_a_cached_source_reads_its_declared_types(kind: str, tmp_path: Path) -> None:
    table = _table(pa.array([1, 2], pa.int8()), order="gxkw")
    database = _databases(tmp_path)[kind]
    first = _cached(table, database)
    rows = first.as_table()
    again = _cached(table, database).as_table()
    for read in (rows, again):
        assert read.schema.field("x").type == pa.int8()
    assert first.arrow_schemas == typed_arrow_schemas(_source(table))


def test_a_cached_read_of_another_type_is_refused(tmp_path: Path) -> None:
    """Delta reads a fixed-size list back as a list (ITL-1275): refused, naming it."""
    table = _table(FIXED_SIZE)
    database = _databases(tmp_path)["delta"]
    with pytest.raises(
        InputValidationError,
        match=r"the cached column 'x' reads back as large_list<.*>, but the source's "
        r"rows have fixed_size_list<.*>\[2\]",
    ):
        _cached(table, database).as_table()


def test_a_cached_fixed_size_list_in_memory_is_exact(tmp_path: Path) -> None:
    table = _table(FIXED_SIZE)
    database = _databases(tmp_path)["memory"]
    for _ in range(2):
        read = _cached(table, database).as_table()
        assert read.schema.field("x").type == pa.list_(pa.int8(), 2)


_CACHED_REFUSAL = (
    r"the cached column 'x' reads back as large_list<.*>, but the source's "
    r"rows have fixed_size_list<.*>\[2\]"
)


def test_get_all_records_refuses_a_cached_read_of_another_type(tmp_path: Path) -> None:
    """Every read of the cache's rows is checked (fourth Codex review of PR 454):
    ``get_all_records()`` after an ``as_table()`` refusal, and on a fresh wrapper over
    the filled cache."""
    table = _table(FIXED_SIZE)
    database = _databases(tmp_path)["delta"]
    first = _cached(table, database)
    with pytest.raises(InputValidationError, match=_CACHED_REFUSAL):
        first.as_table()
    with pytest.raises(InputValidationError, match=_CACHED_REFUSAL):
        first.get_all_records()
    fresh = _cached(table, database)
    with pytest.raises(InputValidationError, match=_CACHED_REFUSAL):
        fresh.get_all_records()
    with pytest.raises(InputValidationError, match=_CACHED_REFUSAL):
        fresh.as_table()


def test_get_all_records_of_a_memory_cache_keeps_the_type(tmp_path: Path) -> None:
    table = _table(FIXED_SIZE)
    database = _databases(tmp_path)["memory"]
    _ = _cached(table, database).as_table()
    for source in (_cached(table, database), _cached(table, database)):
        records = source.get_all_records()
        assert records is not None
        assert records.schema.field("x").type == pa.list_(pa.int8(), 2)


# CodeRabbit on PR 454: a slot that declares one role and takes the other from its
# source keeps its declared role's Arrow types.
@pytest.mark.parametrize("declared_role", ["tag", "data"])
def test_a_slot_declaring_one_role_keeps_that_roles_types(declared_role: str) -> None:
    from orcapod.core.nodes.source_node import SourceJobNode

    table = _table(
        pa.array([1, 2], pa.int64()), pa.field("x", pa.int64(), nullable=False)
    )
    source = _source(table)
    tags = Schema({"k": str | None, "g": int | None})
    data = Schema({"x": int | None, "w": float | None})
    partial = (
        SourceJobNode("slot", tag_schema=tags, bound_source=source)
        if declared_role == "tag"
        else SourceJobNode("slot", data_schema=data, bound_source=source)
    )
    source_tags, source_data = typed_arrow_schemas(source)
    full = SourceJobNode(
        "slot",
        tag_schema=tags if declared_role == "tag" else partial.output_schema()[0],
        data_schema=data if declared_role == "data" else partial.output_schema()[1],
        bound_source=source,
    )
    tag_arrow, data_arrow = partial.arrow_schemas
    if declared_role == "tag":
        assert all(field.nullable for field in tag_arrow)
        assert data_arrow == source_data
    else:
        assert all(field.nullable for field in data_arrow)
        assert tag_arrow == source_tags
    assert partial.output_schema() == full.output_schema()
