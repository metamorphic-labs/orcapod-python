"""Source-info tokens carried as a recipe until their text is needed (ITL-1464).

The oracle is the same operation with every source's tokens built as text at once, as
sources did before: values, types, nullability, row order and hashes are equal, and no
derived column reaches a user, a user's operator, a stored table, a hash or a pickle.
"""

from __future__ import annotations

import pickle
from collections.abc import Callable
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from orcapod import contexts
from orcapod.core.data_tags import stream_data_tags
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.streams import ArrowTableStream
from orcapod.core.streams.base import ZERO_ROW_INPUT_COLUMNS
from orcapod.databases import InMemoryArrowDatabase
from orcapod.operators import (
    AddTags,
    DropColumns,
    DropNulls,
    DropTagColumns,
    MapColumns,
    MapData,
    MapTags,
    PolarsFilter,
    SelectDataColumns,
    TagBy,
)
from orcapod.sources import ArrowTableSource, CachedSource
from orcapod.system_constants import constants
from orcapod.utils import arrow_utils, source_tokens
from tests.test_core.operators.test_async_sync_equivalence import (
    AFTER_OPERATOR_CASES,
    CASES,
    Case,
)
from tests.test_core.operators.test_row_built_form import assert_same_values

CARRIED = {"source": True, "system_tags": True, "context": True}


@pytest.fixture
def text_sources(monkeypatch: pytest.MonkeyPatch) -> Callable[[], None]:
    """Make every source built from now on hold its tokens as text."""
    add = arrow_utils.add_system_tag_columns

    def switch() -> None:
        monkeypatch.setattr(
            arrow_utils,
            "add_system_tag_columns",
            lambda *args: source_tokens.materialize(add(*args)),
        )

    return switch


def _table() -> pa.Table:
    """Chunked, two tags (one a data-tag), two data columns, nulls."""
    whole = pa.table(
        {
            "k": pa.array(range(10), pa.int32()),
            "j": pa.array([f"j{i % 3}" for i in range(10)]),
            "x": pa.array([None if i % 4 == 0 else float(i) for i in range(10)]),
            "y": pa.array([f"y{i}" for i in range(10)]),
        }
    )
    return pa.concat_tables([whole.slice(0, 3), whole.slice(3, 4), whole.slice(7)])


def _source(**kwargs: Any) -> ArrowTableSource:
    return ArrowTableSource(
        _table(),
        tag_columns=["k", "j"],
        tag_only_columns=["j"],
        record_id_columns=["k"],
        **kwargs,
    )


def _plain_source() -> ArrowTableSource:
    return ArrowTableSource(
        _table(), tag_columns=["k", "j"], tag_only_columns=True, record_id_columns=["k"]
    )


def _derived(table: pa.Table) -> tuple[str, ...]:
    return source_tokens.derived_columns(table.schema)


ROW_WISE: dict[str, Callable[[], Any]] = {
    "drop_nulls": lambda: DropNulls(["x"])(_source()),
    "drop_nulls_all": lambda: DropNulls()(_source()),
    "polars_filter": lambda: PolarsFilter([pl.col("x") > 2, pl.col("j") == "j1"])(
        _source()
    ),
    "polars_constraint": lambda: PolarsFilter([], {"j": "j1", "y": "y4"})(_source()),
    "filter": lambda: _source().filter({"j": "j1", "y": "y4"}),
    "map_data": lambda: MapData({"x": "y", "y": "x"})(_source()),
    "map_data_tag": lambda: MapData({"k": "kk", "x": "z"})(_source()),
    "map_tags": lambda: MapTags({"k": "kk", "j": "jj"})(_source()),
    "map_tags_only": lambda: MapTags({"k": "kk", "j": "jj"}, tag_only=True)(_source()),
    "map_columns": lambda: MapColumns({"j": "jj", "x": "z", "k": "kk"})(
        _plain_source()
    ),
    "select_data": lambda: SelectDataColumns(["y"])(_source()),
    "drop_tags": lambda: DropTagColumns(["k"])(_source()),
    "drop_columns": lambda: DropColumns(["k", "x"])(_source()),
    "add_tags": lambda: AddTags({"b": 1, "a": "q"})(_source()),
    "tag_by": lambda: TagBy(["x"])(DropNulls(["x"])(_plain_source())),
    "chain": lambda: (
        _source()
        .drop_nulls(["x"])
        .map_data({"x": "y", "y": "x"})
        .polars_filter(pl.col("y") > 2)
        .add_tags(c=1)
    ),
}


@pytest.mark.parametrize("name", ROW_WISE)
def test_a_row_wise_operator_carries_the_tokens_and_gives_the_same_rows(
    name: str, text_sources: Callable[[], None]
) -> None:
    result = ROW_WISE[name]()
    carried = result.as_carried_table(columns=CARRIED)
    tokens = [c for c in carried.column_names if c.startswith(constants.SOURCE_PREFIX)]
    assert tokens and set(_derived(carried)) == set(tokens)
    table = result.as_table(all_info=True)
    rows = [(t.as_dict(all_info=True), d.as_dict(all_info=True)) for t, d in result]
    digest = result.content_hash()
    text_sources()
    oracle = ROW_WISE[name]()
    expected = oracle.as_table(all_info=True)
    assert not _derived(oracle.as_carried_table(columns=CARRIED))
    assert table.schema.equals(expected.schema, check_metadata=True)
    assert_same_values(table, expected)
    assert rows == [
        (t.as_dict(all_info=True), d.as_dict(all_info=True)) for t, d in oracle
    ]
    assert digest == oracle.content_hash()


@pytest.mark.parametrize(
    "case", [*[pytest.param(c, id=c.label) for c in CASES], *AFTER_OPERATOR_CASES]
)
def test_every_operator_gives_what_text_sources_give(
    case: Case, text_sources: Callable[[], None]
) -> None:
    """Every operator of the equivalence corpus: the whole table, its carried form, and
    F (the table a job node hands on) of the carried form."""
    result = case.make_op()(*case.make_sources())
    table = result.as_table(all_info=True)
    carried = result.as_carried_table(columns=ZERO_ROW_INPUT_COLUMNS)
    plain = result.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
    built = source_tokens.materialize(carried)
    assert built.schema.equals(plain.schema, check_metadata=True)
    assert_same_values(built, plain)
    if plain.num_rows:
        formed = StaticOutputOperatorPod._row_built_form(carried, result.keys())
        from_text = StaticOutputOperatorPod._row_built_form(plain, result.keys())
        assert _derived(formed) or not _derived(carried)
        formed = source_tokens.materialize(formed)
        assert formed.schema.equals(from_text.schema, check_metadata=True)
        assert_same_values(formed, from_text)
    text_sources()
    expected = case.make_op()(*case.make_sources()).as_table(all_info=True)
    assert table.schema.equals(expected.schema, check_metadata=True)
    assert_same_values(table, expected)


def _column_configs() -> list[dict[str, bool]]:
    groups = ("source", "system_tags", "context", "content_hash", "sort_by_tags")
    return [
        {g: bool(bits >> i & 1) for i, g in enumerate(groups)}
        for bits in range(2 ** len(groups))
    ]


@pytest.mark.parametrize("make", [_source, ROW_WISE["chain"]], ids=["source", "chain"])
def test_no_public_read_gives_a_derived_column(make: Callable[[], Any]) -> None:
    stream = make()
    tables: list[pa.Table] = [stream.as_table(columns=c) for c in _column_configs()]
    tables += [stream.head(3, columns=c).table for c in _column_configs()]
    tables += [stream.as_table(all_info=True), stream.head(3, all_info=True).table]
    tables += [stream.run().table]
    for tag, data in stream.flow():
        tables += [tag.as_table(all_info=True), data.as_table(all_info=True)]
        assert all(isinstance(t, str) for t in data.source_info().values())
    tables.append(stream.as_polars_df(columns=CARRIED).to_arrow())
    tables.append(pa.Table.from_pandas(stream.as_pandas_df(columns=CARRIED)))
    assert not stream.as_lazy_frame(columns=CARRIED).collect().is_empty()
    for table in tables:
        assert not _derived(table)
        assert pa.struct([]) not in table.schema.types
        _ = table.to_pylist()
    for schema in (stream.arrow_schema(), *stream.output_schema(all_info=True)):
        assert "derived" not in str(schema)
    assert "derived_token" not in stream.run()._repr_html_() + repr(stream.run())


def test_a_table_without_its_system_tags_holds_text() -> None:
    """The recipe reads the system tags, so a table without them is never carried."""
    for config in ({"source": True}, {"source": True, "context": True}):
        table = _source().as_carried_table(columns=config)
        assert not _derived(table)
        assert table.column("_source_x").type == pa.large_string()


class _ReadsEverything(UnaryOperator):
    """A user's operator: reads every column of its input table."""

    DATA_TAGS_ISSUE = None
    seen: list[pa.Table] = []

    def validate_unary_input(self, stream: Any) -> None:
        pass

    def unary_output_schema(self, stream: Any, **kwargs: Any) -> Any:
        return stream.output_schema(**kwargs)

    def identity_structure(self) -> Any:
        return "ReadsEverything"

    def unary_static_process(self, stream: Any) -> Any:
        table = stream.as_table(columns={"source": True, "system_tags": True})
        self.seen.append(table)
        assert all(len(v) == len(table) for v in table.to_pydict().values())
        return ArrowTableStream(
            table, tag_columns=stream.keys()[0], data_tags=stream_data_tags(stream)
        )


def test_a_users_operator_reads_text() -> None:
    _ReadsEverything.seen = []
    result = _ReadsEverything()(_source())
    after = result.drop_nulls(["x"])
    table = after.as_table(columns=CARRIED)
    (seen,) = _ReadsEverything.seen
    assert not _derived(seen)
    assert str(seen.column("_source_x")[1]).endswith('::{"k":1}::x')
    assert table.column("_source_x").to_pylist() == [
        t for i, t in enumerate(seen.column("_source_x").to_pylist()) if i % 4
    ]


@pytest.mark.parametrize(
    "predicate",
    [
        pl.col("_source_x").str.ends_with("::x"),
        pl.any_horizontal(pl.all().is_not_null()),
        pl.any_horizontal(pl.col("^_source_.*$").str.contains("::")),
        pl.concat_str(["_source_x", "_source_y"]).str.contains("::x"),
        "x_is_big",
        [True] * 5 + [False] * 5,
    ],
    ids=["named", "all", "regex", "concat", "name", "mask"],
)
def test_a_predicate_that_may_read_tokens_reads_text(
    predicate: Any, text_sources: Callable[[], None]
) -> None:
    def make() -> Any:
        table = _table().append_column("x_is_big", pa.array([True, False] * 5))
        return PolarsFilter([predicate])(ArrowTableSource(table, tag_columns=["k"]))

    table = make().as_table(all_info=True)
    text_sources()
    expected = make().as_table(all_info=True)
    assert table.num_rows > 0
    assert table.equals(expected)


def test_an_unknown_column_is_reported_as_with_text(
    text_sources: Callable[[], None],
) -> None:
    def message() -> str:
        with pytest.raises(pl.exceptions.ColumnNotFoundError) as error:
            _ = PolarsFilter([pl.col("nope") > 1])(_source()).as_table()
        return str(error.value)

    got = message()
    text_sources()
    assert got == message()


def test_a_constraint_expression_may_read_tokens() -> None:
    """``filter(x=<expression>)``: the value is looked into as a predicate is."""
    kept = PolarsFilter([], {"y": pl.col("_source_y").str.slice(0, 2)})(_source())
    assert kept.as_table().num_rows == 0


def test_stored_rows_hold_text(text_sources: Callable[[], None]) -> None:
    """A cached source stores every column, tokens and record hashes included."""

    def stored() -> pa.Table:
        database = InMemoryArrowDatabase()
        cached = CachedSource(_source(), cache_database=database)
        _ = cached.as_table()
        table = cached.get_all_records()
        assert table is not None
        return table.drop_columns(
            [c for c in table.column_names if "time" in c or c.endswith("_ts")]
        )

    table = stored()
    assert not _derived(table)
    text_sources()
    expected = stored()
    assert table.schema.equals(expected.schema, check_metadata=True)
    assert table.equals(expected)


def test_a_derived_column_that_leaks_is_an_error() -> None:
    carried = _source().as_carried_table(columns=CARRIED)
    tokens = carried.column("_source_x")
    with pytest.raises(TypeError, match="no value"):
        _ = tokens.to_pylist()
    with pytest.raises(pa.ArrowNotImplementedError):
        _ = tokens.cast(pa.large_string())
    with pytest.raises(TypeError, match="not pickled"):
        _ = pickle.dumps(carried)
    with pytest.raises(pa.ArrowNotImplementedError):
        pq.write_table(carried, pa.BufferOutputStream())
    with pytest.raises(Exception, match="no value|derived"):
        _ = contexts.get_default_context().arrow_hasher.hash_table(carried)


def test_a_spilled_table_keeps_its_recipes() -> None:
    """As the row spool writes it: an extension column's storage, wrapped when read."""
    carried: Any = _source().as_carried_table(columns=CARRIED).combine_chunks()
    arrow: Any = pa
    batch: Any = carried.to_batches()[0]
    storage = arrow.RecordBatch.from_arrays(
        [getattr(c, "storage", c) for c in batch.columns], names=batch.schema.names
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(sink, storage.schema) as writer:
        writer.write_batch(storage)
    read = pa.ipc.open_file(sink.getvalue()).get_batch(0)
    wrapped = arrow.Table.from_arrays(
        [
            arrow.ExtensionArray.from_storage(f.type, c)
            if isinstance(f.type, pa.BaseExtensionType)
            else c
            for f, c in zip(carried.schema, read.columns, strict=True)
        ],
        schema=carried.schema,
    )
    order = pa.array([7, 2, 2, 9])
    assert _derived(wrapped) == _derived(carried)
    assert source_tokens.materialize(wrapped.take(order)).equals(
        source_tokens.materialize(carried).take(order)
    )


def test_a_carried_column_has_no_bytes_and_is_not_null() -> None:
    carried = _source().as_carried_table(columns=CARRIED)
    taken = carried.take(pa.array([1, 5])).filter(pc.field("k") > 1)
    for table in (carried, taken, pa.concat_tables([carried, taken]).combine_chunks()):
        for name in _derived(table):
            assert table.column(name).nbytes == 0
            assert table.column(name).null_count == 0
            assert not table.schema.field(name).nullable


def test_recipes_tell_types_apart() -> None:
    one, two = (
        source_tokens.derived_array(source_tokens.TokenRecipe("s", "r", c), 2)
        for c in ("a", "b")
    )
    assert one.type != two.type
    assert pa.schema([("c", one.type)]) != pa.schema([("c", two.type)])
    with pytest.raises(pa.ArrowInvalid):
        _ = pa.concat_tables([pa.table({"c": one}), pa.table({"c": two})])


def test_extended_system_tag_names_are_followed_and_sorting_builds_the_text() -> None:
    """A join's system-tag rename keeps the tokens derived; sorting the values of two
    inputs with one pipeline hash builds them first, so each names its own row."""
    left, right = (
        _source(source_id=name).map_data(
            {"x": f"x{name}", "y": f"y{name}", "k": f"k{name}"}
        )
        for name in ("b", "a")
    )
    tables = [
        arrow_utils.append_to_system_tags(
            s.as_carried_table(columns={"source": True, "system_tags": True}), f"h:{i}"
        )
        for i, s in enumerate((left, right))
    ]
    plain = [source_tokens.materialize(t) for t in tables]
    assert plain[0].column("_source_xb")[1].as_py().startswith("arrow_table_v1:b::")
    joined = arrow_utils.hstack_tables(tables[0], tables[1].drop_columns(["k", "j"]))
    assert _derived(joined)
    done = arrow_utils.sort_system_tag_values(joined)
    assert not _derived(done)
    for table in plain:
        for name in table.column_names:
            if name.startswith(constants.SOURCE_PREFIX):
                assert done.column(name).equals(table.column(name))


def test_rows_are_built_a_part_at_a_time(monkeypatch: pytest.MonkeyPatch) -> None:
    """Iteration builds text per part of the rows, whatever their chunks."""
    from orcapod.core.streams import arrow_table_stream

    expected = _source().as_table(columns=CARRIED)
    sizes: list[int] = []
    build = source_tokens.materialize

    def record(table: pa.Table, names: Any = None) -> pa.Table:
        sizes.append(table.num_rows)
        return build(table, names)

    monkeypatch.setattr(arrow_table_stream, "ROWS_PER_PART", 4)
    monkeypatch.setattr(source_tokens, "materialize", record)
    rows = list(_source())
    assert sizes == [4, 4, 2]
    assert [d.source_info()["x"] for _, d in rows] == expected.column(
        "_source_x"
    ).to_pylist()


def test_no_rows_hold_text_types() -> None:
    table = _source().polars_filter(pl.col("k") > 100).as_table(all_info=True)
    assert table.num_rows == 0
    assert table.schema.field("_source_x").type == pa.large_string()
    assert table.schema.equals(_source().as_table(all_info=True).schema)


def test_a_source_with_its_own_read_gives_its_own_table() -> None:
    """A subclass, or an object, with its own ``as_table`` is read through it."""

    class Own(ArrowTableSource):
        def as_table(self, **kwargs: Any) -> pa.Table:
            return super().as_table(**kwargs).slice(0, 2)

    own = Own(_table(), tag_columns=["k"])
    assert own.drop_columns(["x"]).as_table().num_rows == 2
