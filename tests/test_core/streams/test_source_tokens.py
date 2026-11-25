"""Source-info tokens carried as a recipe until their text is needed (ITL-1464).

The oracle is the same operation with every source's tokens built as text at once, as
sources did before: values, types, nullability, row order and hashes are equal, and no
derived column reaches a user, a user's operator, a stored table, a hash or a pickle.
"""

from __future__ import annotations

import hashlib
import json
import pickle
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.feather as feather
import pyarrow.parquet as pq
import pytest

from orcapod import contexts
from orcapod.core.data_tags import stream_data_tags
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.operators.join import JOIN_FOLD_LABEL
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.row_spool import RowSpool
from orcapod.core.streams import ArrowTableStream
from orcapod.core.streams.carried import carried_table
from orcapod.core.streams.base import ZERO_ROW_INPUT_COLUMNS
from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    InMemoryArrowDatabase,
    SQLiteConnector,
)
from orcapod.operators import (
    AddTags,
    Batch,
    DropColumns,
    DropNulls,
    DropTagColumns,
    GroupBy,
    Join,
    MapColumns,
    MapData,
    MapTags,
    MergeJoin,
    PolarsFilter,
    Rejoin,
    SelectDataColumns,
    TagBy,
)
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.sources import ArrowTableSource, CachedSource, SpiralDBTableSource
from orcapod.system_constants import constants
from orcapod.utils import arrow_utils, source_tokens
from tests.test_core.operators.test_async_sync_equivalence import (
    AFTER_OPERATOR_CASES,
    CASES,
    Case,
)
from tests.test_core.operators.test_row_built_form import assert_same_values
from tests.test_databases import fake_spiral_sdk as fake

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
    "select_columns": lambda: _source().select_columns(["k", "y"]),
    "select_tags": lambda: _source().select_tag_columns(["k"]),
    "drop_data": lambda: _source().drop_data_columns(["y"]),
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
    carried = carried_table(result, CARRIED)
    tokens = [c for c in carried.column_names if c.startswith(constants.SOURCE_PREFIX)]
    assert tokens and set(_derived(carried)) == set(tokens)
    table = result.as_table(all_info=True)
    rows = [(t.as_dict(all_info=True), d.as_dict(all_info=True)) for t, d in result]
    digest = result.content_hash()
    text_sources()
    oracle = ROW_WISE[name]()
    expected = oracle.as_table(all_info=True)
    assert not _derived(carried_table(oracle, CARRIED))
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
    carried = carried_table(result, ZERO_ROW_INPUT_COLUMNS)
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


def _other(name: str, **kwargs: Any) -> ArrowTableSource:
    """A source sharing the data-tags ``k`` and ``j``, of its own schema (``name``)."""
    table = pa.table(
        {
            f"{name}1": pa.array([float(i) for i in range(12)]),
            "j": pa.array([f"j{i % 3}" for i in range(12)]),
            "k": pa.array(reversed(range(12)), pa.int32()),
            f"{name}2": pa.array([f"v{i}" for i in range(12)]),
        }
    )
    return ArrowTableSource(table, tag_columns=["j", "k"], **kwargs)


def _text_other(name: str) -> ArrowTableStream:
    """``_other`` as a raw stream whose tokens are text already."""
    source = _other(name)
    return ArrowTableStream(
        source.as_table(columns={"source": True, "system_tags": True}),
        tag_columns=["j", "k"],
        data_tags=["j", "k"],
    )


MATCHES: dict[str, tuple[Callable[[], Any], set[str]]] = {
    # The columns whose tokens may be text after it, by name; every other is derived.
    "join_2": (lambda: _other("a").join(_other("b")), set()),
    "join_2_swapped": (lambda: _other("b").join(_other("a")), set()),
    "join_3": (lambda: Join()(_other("c"), _other("a"), _other("b")), set()),
    "join_chain": (lambda: _other("a").join(_other("b")).join(_other("c")), set()),
    "join_then_row_wise": (
        lambda: (
            _other("a")
            .join(_other("b"))
            .map_data({"a1": "x", "b2": "y"})
            .drop_nulls()
            .polars_filter(pl.col("x") > 3)
        ),
        set(),
    ),
    "join_tag_only": (
        lambda: _source().join(_other("a", tag_only_columns=["j"])),
        set(),
    ),
    "join_text_member": (
        lambda: Join()(_other("a"), _text_other("b"), _other("c")),
        {"j", "k", "b1", "b2"},
    ),
    "join_one_schema": (
        lambda: (
            _other("a", source_id="s2")
            .map_data({"a1": "p", "a2": "q"})
            .join(_other("a", source_id="s1").map_data({"a1": "p2", "a2": "q2"}))
        ),
        set(),
    ),
    "semi_join": (lambda: _source().semi_join(_plain_source()), set()),
    "merge_join": (
        lambda: MergeJoin()(
            _other("a").map_data({"a2": "same"}), _other("b").map_data({"b2": "same"})
        ),
        {"same"},
    ),
    "rejoin": (
        lambda: Rejoin()(
            _other("a").select_data_columns(["a1", "j", "k"]),
            _other("a").select_data_columns(["a2", "j", "k"]),
        ),
        set(),
    ),
    "rejoin_filtered": (
        lambda: Rejoin()(
            _other("a").select_data_columns(["a1", "j", "k"]),
            _other("a").polars_filter(pl.col("a1") > 3).drop_data_columns(["a1"]),
        ),
        set(),
    ),
}


@pytest.mark.parametrize("name", MATCHES)
def test_a_match_carries_the_tokens_and_gives_the_same_rows(
    name: str, text_sources: Callable[[], None]
) -> None:
    make, text = MATCHES[name]
    result = make()
    carried = carried_table(result, {"source": True, "system_tags": True})
    tokens = {
        c.removeprefix("_source_")
        for c in carried.column_names
        if c.startswith(constants.SOURCE_PREFIX)
    }
    derived = {c.removeprefix("_source_") for c in _derived(carried)}
    assert tokens - derived == text & tokens
    table = result.as_table(all_info=True)
    rows = [(t.as_dict(all_info=True), d.as_dict(all_info=True)) for t, d in result]
    assert table.num_rows > 0
    text_sources()
    oracle = make()
    expected = oracle.as_table(all_info=True)
    assert table.schema.equals(expected.schema, check_metadata=True)
    assert_same_values(table, expected)
    assert rows == [
        (t.as_dict(all_info=True), d.as_dict(all_info=True)) for t, d in oracle
    ]


@pytest.mark.parametrize("order", [("s1", "s2"), ("s2", "s1")])
def test_inputs_of_one_pipeline_hash_keep_their_own_tokens(
    order: tuple[str, str], text_sources: Callable[[], None]
) -> None:
    """Two sources of one schema, every column a data-tag: the join sorts their
    system-tag values per row, so the tokens that read them are text first."""

    def make() -> Any:
        streams = [
            ArrowTableSource(
                _table().select(["k", "j"]).slice(i),
                tag_columns=["k", "j"],
                source_id=s,
            )
            for i, s in enumerate(order)
        ]
        assert streams[0].pipeline_hash() == streams[1].pipeline_hash()
        return streams[0].join(streams[1])

    result = make()
    assert not _derived(carried_table(result, CARRIED))
    table = result.as_table(all_info=True)
    text_sources()
    expected = make().as_table(all_info=True)
    assert table.num_rows == 9
    assert table.schema.equals(expected.schema, check_metadata=True)
    assert_same_values(table, expected)


def test_a_join_fold_is_the_fold_of_its_members() -> None:
    """The merged token is ``join_v1`` of the inputs' own tokens, whatever the order."""
    a, b = _other("a"), _other("b")
    table = a.join(b).as_table(columns={"source": True})
    own = [
        dict(zip(t["k"].to_pylist(), t["_source_k"].to_pylist(), strict=True))
        for t in (s.as_table(columns={"source": True}) for s in (a, b))
    ]
    for key, token in zip(
        table["k"].to_pylist(), table["_source_k"].to_pylist(), strict=True
    ):
        assert token == arrow_utils.fold_system_tag_values(
            sorted(tokens[key] for tokens in own), label=JOIN_FOLD_LABEL
        )


def test_a_recipe_survives_its_json() -> None:
    leaf = source_tokens.TokenRecipe("s", "r", "c")
    fold = source_tokens.FoldRecipe(
        JOIN_FOLD_LABEL, (leaf, source_tokens.FoldRecipe(JOIN_FOLD_LABEL, (leaf, leaf)))
    )
    for recipe in (leaf, fold):
        kind = source_tokens.DerivedTokenType(recipe)
        again = source_tokens.DerivedTokenType.__arrow_ext_deserialize__(
            kind.storage_type, kind.__arrow_ext_serialize__()
        )
        assert again == kind and again.recipe == recipe


def _keyed(rows: int, groups: int, **kwargs: Any) -> ArrowTableSource:
    """Rows in a shuffled order, a key with null and NaN, two tags, two data columns."""
    order = [(i * 7919) % rows for i in range(rows)]
    table = pa.table(
        {
            "t": pa.array(order, pa.int64()),
            "g": pa.array(
                [None if i % groups == 1 else f"g{i % groups}" for i in order]
            ),
            "f": pa.array([float("nan") if i % 2 else -0.0 for i in order]),
            "x": pa.array([float(i) for i in order]),
            "y": pa.array([f"y{i}" for i in order]),
        }
    )
    return ArrowTableSource(table, tag_columns=["t", "g", "f"], **kwargs)


AGGREGATES: dict[str, tuple[Callable[[], Any], bool]] = {
    # The operation, and whether every list of tokens is derived after it.
    "group_by": (lambda: _keyed(50, 4).group_by(["g"]), True),
    "group_by_float": (lambda: _keyed(50, 4).group_by(["f", "g"]), True),
    "group_by_one_row_groups": (lambda: _keyed(9, 3).group_by(["t"]), True),
    "group_by_plain": (
        lambda: _keyed(50, 4, tag_only_columns=True).group_by(["g"]),
        True,
    ),
    "group_by_order": (
        lambda: GroupBy(by=["g"], member_order=["f", "t"])(_keyed(50, 4)),
        True,
    ),
    "group_by_chain": (
        lambda: (
            _keyed(50, 4)
            .polars_filter(pl.col("x") > 5)
            .group_by(["g"])
            .map_data({"x": "z", "t": "tt"})
            .add_tags(a=1)
            .polars_filter(pl.col("a") == 1)
        ),
        True,
    ),
    "group_by_then_filter": (
        lambda: _keyed(50, 4).group_by(["g"]).polars_filter(pl.col("g") == "g2"),
        True,
    ),
    "group_by_kept_by_drop_nulls": (
        lambda: _keyed(50, 4).group_by(["g"]).drop_nulls(),
        True,
    ),
    "regrouped": (lambda: _keyed(50, 4).group_by(["f", "g"]).group_by(["f"]), False),
    "join_grouped": (lambda: _other("a").join(_other("b")).group_by(["j"]), True),
    "merge_join_grouped": (
        lambda: MergeJoin()(
            _other("a").map_data({"a2": "same"}), _other("b").map_data({"b2": "same"})
        ).group_by(["j"]),
        False,
    ),
    "grouped_joined": (
        lambda: Join()(
            _other("a").group_by(["j"]),
            _other("b").group_by(["j"]).map_data({"k": "k2"}),
        ),
        True,
    ),
    "grouped_semi_join": (
        lambda: _other("a").group_by(["j"]).semi_join(_other("b")),
        True,
    ),
    "grouped_rejoin": (
        lambda: Rejoin()(
            _keyed(50, 4).group_by(["g"]).select_data_columns(["x", "g"]),
            _keyed(50, 4).group_by(["g"]).select_data_columns(["y", "g"]),
        ),
        True,
    ),
    "grouped_merge_join": (
        lambda: MergeJoin()(
            _other("a").group_by(["j"]).map_data({"a2": "same"}),
            _other("b").group_by(["j"]).map_data({"b2": "same"}),
        ),
        False,
    ),
    "batch": (lambda: Batch(batch_size=3)(_keyed(10, 4)), True),
    "batch_dropped": (
        lambda: Batch(batch_size=3, drop_partial_batch=True)(_keyed(10, 4)),
        True,
    ),
    "batch_all": (lambda: Batch()(_keyed(10, 4)), True),
    "batch_of_groups": (
        lambda: Batch(batch_size=2)(_keyed(50, 5).group_by(["g"])),
        False,
    ),
    "several_chunks": (
        lambda: ArrowTableSource(
            pa.concat_tables([_table(), _table().slice(0, 4)]).append_column(
                "t", pa.array(range(14))
            ),
            tag_columns=["j", "t"],
        ).group_by(["j"]),
        True,
    ),
}


@pytest.mark.parametrize("name", AGGREGATES)
@pytest.mark.parametrize("chunk", [3, source_tokens.ROWS_PER_CHUNK])
def test_an_aggregation_carries_the_tokens_and_gives_the_same_rows(
    name: str,
    chunk: int,
    text_sources: Callable[[], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(source_tokens, "ROWS_PER_CHUNK", chunk)
    monkeypatch.setattr(arrow_utils, "_FOLD_PART", chunk)
    make, derived = AGGREGATES[name]
    result = make()
    carried = carried_table(result, ZERO_ROW_INPUT_COLUMNS)
    lists = [
        f
        for f in carried.schema
        if f.name.startswith(constants.SOURCE_PREFIX)
        and f.type != pa.large_string()
        and source_tokens.derived_recipe(f.type) is None
    ]
    assert lists
    assert all(source_tokens.is_derived(f.type) for f in lists) is derived
    formed = StaticOutputOperatorPod._row_built_form(carried, result.keys())
    plain = result.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
    from_text = StaticOutputOperatorPod._row_built_form(plain, result.keys())
    formed = source_tokens.materialize(formed)
    assert formed.schema.equals(from_text.schema, check_metadata=True)
    assert_same_values(formed, from_text)
    table = result.as_table(all_info=True)
    rows = [(t.as_dict(columns=CARRIED), d.as_dict(columns=CARRIED)) for t, d in result]
    assert not _derived(table)
    text_sources()
    oracle = make()
    expected = oracle.as_table(all_info=True)
    assert table.schema.equals(expected.schema, check_metadata=True)
    assert_same_values(table, expected)
    assert repr(rows) == repr(
        [(t.as_dict(columns=CARRIED), d.as_dict(columns=CARRIED)) for t, d in oracle]
    )


def test_an_aggregation_of_no_rows_gives_text_types(
    text_sources: Callable[[], None],
) -> None:
    def make() -> list[pa.Table]:
        none = _keyed(10, 4).polars_filter(pl.col("x") > 100)
        return [
            s.as_table(all_info=True) for s in (none.group_by(["g"]), Batch()(none))
        ]

    tables = make()
    text_sources()
    for table, expected in zip(tables, make(), strict=True):
        assert table.num_rows == 0
        assert table.schema.equals(expected.schema, check_metadata=True)


def _ipc_writes(table: pa.Table) -> dict[str, Callable[[], Any]]:
    def new(make: Any) -> None:
        with make(pa.BufferOutputStream(), table.schema) as writer:
            writer.write_table(table)

    return {
        "file": lambda: new(pa.ipc.new_file),
        "stream": lambda: new(pa.ipc.new_stream),
        "batch": lambda: table.combine_chunks().to_batches()[0].serialize(),
        "feather": lambda: feather.write_feather(
            table, cast(Any, pa.BufferOutputStream())
        ),
        "c_stream": lambda: pa.RecordBatchReader.from_stream(cast(Any, table)),
    }


@pytest.mark.parametrize("write", ["file", "stream", "batch", "feather", "c_stream"])
@pytest.mark.parametrize("grouped", [False, True])
def test_arrow_ipc_refuses_a_derived_column(write: str, grouped: bool) -> None:
    """No path out of the process, or into another library, gives a recipe's storage."""
    stream: Any = _keyed(20, 3).group_by(["g"]) if grouped else _keyed(20, 3)
    carried = carried_table(stream, {"source": True, "system_tags": True})
    with pytest.raises(pa.ArrowInvalid, match="[Rr]ecursion"):
        _ipc_writes(carried)[write]()
    with pytest.raises(TypeError, match="no pandas form"):
        _ = carried.to_pandas()
    _ipc_writes(source_tokens.materialize(carried))[write]()


def test_a_grouped_stream_refuses_to_leak() -> None:
    carried = carried_table(
        _keyed(10, 4).group_by(["g"]), {"source": True, "system_tags": True}
    )
    tokens = carried.column("_source_x")
    with pytest.raises(TypeError, match="no value"):
        _ = tokens.to_pylist()
    with pytest.raises(pa.ArrowNotImplementedError):
        _ = tokens.cast(pa.large_list(pa.large_string()))
    with pytest.raises(TypeError, match="not pickled"):
        _ = pickle.dumps(carried)
    with pytest.raises(pa.ArrowNotImplementedError):
        pq.write_table(carried, pa.BufferOutputStream())
    kind = tokens.type
    assert (
        source_tokens.MemberTokenType.__arrow_ext_deserialize__(
            kind.storage_type, kind.__arrow_ext_serialize__()
        )
        == kind
    )


def test_the_lists_of_one_aggregation_share_the_members_ids() -> None:
    """One array of ids for every token column; a spill writes it once per column, with
    the ids' text once (a dictionary) per column too."""
    carried: Any = carried_table(
        _keyed(50, 4).group_by(["g"]), {"source": True, "system_tags": True}
    )
    addresses = {
        tuple(
            buffer.address
            for buffer in carried.column(name)
            .chunk(0)
            .storage.values.field(0)
            .buffers()
            if buffer is not None
        )
        for name in ("_source_x", "_source_y", "_source_t")
    }
    assert len(addresses) == 1


@pytest.mark.parametrize("members", [0, 1, 4095, 4096, 4097, 9000])
def test_a_fold_is_the_digest_of_the_canonical_list(members: int) -> None:
    values = [None if i % 5 == 0 else f'r"{i}\u00e9,' for i in range(members)]
    text = json.dumps(values, separators=(",", ":"), ensure_ascii=False)
    expected = "l_v1:" + hashlib.sha256(text.encode()).hexdigest()
    assert arrow_utils.fold_system_tag_values(values, label="l_v1") == expected
    assert arrow_utils.fold_system_tag_values(iter(values), label="l_v1") == expected


def test_grouped_rows_are_built_a_bounded_part_at_a_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from orcapod.core.streams import arrow_table_stream

    monkeypatch.setattr(arrow_table_stream, "ROWS_PER_PART", 2)
    stream = _keyed(50, 5).group_by(["g"])
    whole = stream.as_table(columns={"source": True})
    sizes: list[int] = []
    build = source_tokens.materialize

    def record(table: pa.Table, names: Any = None) -> pa.Table:
        sizes.append(table.num_rows)
        return build(table, names)

    monkeypatch.setattr(source_tokens, "materialize", record)
    rows = list(stream)
    assert sizes == [1] * 5  # 10 members a row: one row passes 16 with the next
    assert [d.source_info()["x"] for _, d in rows] == whole["_source_x"].to_pylist()


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
        table = carried_table(_source(), config)
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
    carried = carried_table(_source(), CARRIED)
    tokens = carried.column("_source_x")
    with pytest.raises(TypeError, match="no value"):
        _ = tokens.to_pylist()
    with pytest.raises(pa.ArrowNotImplementedError):
        _ = tokens.cast(pa.large_string())
    with pytest.raises(TypeError, match="not pickled"):
        _ = pickle.dumps(carried)
    with pytest.raises(pa.ArrowNotImplementedError):
        pq.write_table(carried, pa.BufferOutputStream())
    hasher = contexts.get_default_context().arrow_hasher
    for hash_it in (
        lambda: hasher.hash_table(carried),
        lambda: hasher.hash_table(carried.slice(0, 0)),
        lambda: hasher.hash_schema(carried.schema),
        lambda: hasher.hash_table_strict(carried),
        lambda: hasher.hash_table_ordered(carried),
    ):
        with pytest.raises(TypeError, match="derived"):
            _ = hash_it()


@pytest.mark.parametrize("grouped", [False, True])
@pytest.mark.parametrize("rows", [0, 3])
@pytest.mark.parametrize("kind", ["memory", "delta", "connector"])
def test_a_store_refuses_a_derived_column(
    kind: str, rows: int, grouped: bool, tmp_path: Path
) -> None:
    stream: Any = _keyed(9, 3).group_by(["g"]) if grouped else _source()
    carried = carried_table(stream, CARRIED).slice(0, rows)
    records = carried.append_column(
        "rid", pa.array([str(i).encode() for i in range(rows)], pa.large_binary())
    )
    databases: dict[str, Callable[[], Any]] = {
        "memory": InMemoryArrowDatabase,
        "delta": lambda: DeltaTableDatabase(str(tmp_path / "delta")),
        "connector": lambda: ConnectorArrowDatabase(
            SQLiteConnector(str(tmp_path / "store.db"))
        ),
    }
    database = databases[kind]()
    with pytest.raises(TypeError, match="derived"):
        database.add_records(("t",), records, record_id_column="rid", flush=True)


def test_the_carried_read_is_no_part_of_a_stream() -> None:
    """Review of PR 563: no public name gives a derived column."""
    for stream in (_source(), ROW_WISE["chain"]()):
        assert not [n for n in dir(stream) if "carried" in n and not n.startswith("_")]
    assert "carried" not in " ".join(dir(StreamProtocol))


SPILLED: dict[str, Callable[[], Any]] = {
    "source": lambda: _keyed(500, 12),
    "grouped": lambda: _keyed(500, 12).group_by(["g"]),
    "one_row_groups": lambda: _keyed(50, 3).group_by(["t"]),
    "batch": lambda: Batch(batch_size=7)(_keyed(500, 12)),
    # Two inputs, so two kinds of ids; and lists beside tokens of one a row.
    "joined": lambda: Join()(
        _other("a").group_by(["j"]), _other("b").group_by(["j"]).map_data({"k": "k2"})
    ),
    # Lists of one source read by two groupings: equal names, other members.
    "two_groupings": lambda: Join()(
        _keyed(60, 4).group_by(["g"]).select_data_columns(["x", "g"]),
        _keyed(60, 4)
        .polars_filter(pl.col("x") > 30)
        .group_by(["g"])
        .select_data_columns(["y", "g"]),
    ),
}


def _own_chunks(table: pa.Table, sizes: list[int]) -> list[pa.Table]:
    """``table`` cut into slices of ``sizes`` rows, in turn."""
    parts: list[pa.Table] = []
    start = 0
    while start < table.num_rows:
        size = sizes[len(parts) % len(sizes)]
        parts.append(table.slice(start, size))
        start += size
    return parts


@pytest.mark.parametrize("limit", [0, 4096, 1 << 30])
@pytest.mark.parametrize("sizes", [[1], [5, 2, 9], [1000]])
@pytest.mark.parametrize("name", SPILLED)
def test_a_spooled_table_keeps_its_recipes(
    name: str, sizes: list[int], limit: int, tmp_path: Path
) -> None:
    """The row spool: every row back, by number and whole, lists and tokens derived."""
    carried = carried_table(SPILLED[name](), CARRIED)
    text = source_tokens.materialize(carried)
    assert _derived(carried)
    with RowSpool(memory_bytes=limit, root=str(tmp_path)) as spool:
        for part in _own_chunks(carried, sizes):
            spool.append(part)
        whole = spool.table()
        rows = np.arange(carried.num_rows, dtype=np.int64)
        for order in (rows, rows[::-1], rows[::3], rows[-1:]):
            taken = spool.take(order)
            assert taken.schema.equals(carried.schema, check_metadata=True)
            assert_same_values(
                source_tokens.materialize(taken), text.take(cast(Any, pa.array(order)))
            )
        part = spool.slice(1, 3)
        assert_same_values(source_tokens.materialize(part), text.slice(1, 3))
    # Also once the files are gone.
    assert whole.schema.equals(carried.schema, check_metadata=True)
    assert_same_values(source_tokens.materialize(whole), text)


def test_a_table_of_derived_columns_only_is_spooled(tmp_path: Path) -> None:
    carried = carried_table(_keyed(20, 4), CARRIED)
    tokens = carried.select(list(_derived(carried)))
    with RowSpool(memory_bytes=0, root=str(tmp_path)) as spool:
        spool.append(tokens)
        taken = spool.take(np.array([3, 1], np.int64))
    assert taken.num_rows == 2 and taken.schema.equals(tokens.schema)


def test_a_spool_writes_a_groups_own_ids_once(tmp_path: Path) -> None:
    """Two of many groups write their members' ids, not every group's, and once for all
    the token columns."""
    wide = pa.table(
        {
            "t": pa.array(range(6000)),
            "g": pa.array([f"g{i % 100}" for i in range(6000)]),
            **{f"v{n}": pa.array([1.0] * 6000) for n in range(12)},
        }
    )
    stream = ArrowTableSource(
        wide, tag_columns=["t", "g"], record_id_columns=["t"], source_id="s" * 40
    ).group_by(["g"])
    carried = carried_table(stream, {"source": True, "system_tags": True})
    lists = [n for n in _derived(carried) if n != "_source_g"]
    assert len(lists) == 13
    ids = 120 * (len('{"t":1234}') + 8 + 8)  # 120 members: text, two numbers
    with RowSpool(memory_bytes=0, root=str(tmp_path)) as spool:
        spool.append(carried.select(lists).slice(0, 2))
        (written,) = tmp_path.rglob("*.arrow")
        assert written.stat().st_size < 4 * ids
    with RowSpool(root=str(tmp_path)) as held:
        held.append(carried.select(lists).slice(0, 2))
        assert held.memory_bytes < 8 * ids


def test_a_spilled_list_pins_nothing_of_the_tables_ids() -> None:
    """Rows kept in memory hold their own ids, not the dictionary of every row."""
    carried = carried_table(
        _keyed(5000, 100).group_by(["g"]), {"source": True, "system_tags": True}
    )
    with RowSpool() as spool:
        spool.append(carried.slice(0, 1))
        chunk: Any = spool.table().column("_source_x").chunk(0)
        members = chunk.storage.flatten()
        assert len(members.field(0).dictionary) == len(members) < 200


def test_grouped_tables_of_two_reads_are_put_together() -> None:
    """Chunks whose ids have dictionaries of their own: concatenated, combined, taken."""
    tables: list[Any] = [
        carried_table(
            _keyed(n, 4, source_id="s").group_by(["g"]),
            {"source": True, "system_tags": True},
        )
        for n in (50, 23)
    ]
    texts = pa.concat_tables([source_tokens.materialize(t) for t in tables])
    whole = pa.concat_tables(tables)
    order = pa.array([6, 0, 7, 0])
    assert_same_values(source_tokens.materialize(whole), texts)
    assert_same_values(source_tokens.materialize(whole.combine_chunks()), texts)
    assert_same_values(source_tokens.materialize(whole.take(order)), texts.take(order))


def test_a_carried_column_has_no_bytes_and_is_not_null() -> None:
    carried = carried_table(_source(), CARRIED)
    taken = carried.take(pa.array([1, 5])).filter(pc.field("k") > 1)
    for table in (carried, taken, pa.concat_tables([carried, taken]).combine_chunks()):
        for name in _derived(table):
            # At most a validity bitmap.
            assert table.column(name).nbytes <= table.num_rows // 8 + 1
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
            carried_table(s, {"source": True, "system_tags": True}), f"h:{i}"
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


class _OwnSource(ArrowTableSource):
    def as_table(self, **kwargs: Any) -> pa.Table:
        return super().as_table(**kwargs).slice(0, 2)


class _OwnStream(ArrowTableStream):
    def as_table(self, **kwargs: Any) -> pa.Table:
        return super().as_table(**kwargs).slice(0, 2)


class _OwnSpiral(SpiralDBTableSource):
    def as_table(self, **kwargs: Any) -> pa.Table:
        return super().as_table(**kwargs).slice(0, 2)


class _Forwarding:
    """A stream of the read interface as it is, forwarding to another."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self.inner, name)


def _raw(kind: type[ArrowTableStream]) -> ArrowTableStream:
    return kind(
        carried_table(_source(), {"source": True, "system_tags": True}),
        tag_columns=["k", "j"],
        data_tags=["k"],
    )


def _replaced() -> Any:
    source = _source()
    read = source.as_table
    setattr(source, "as_table", lambda **kwargs: read(**kwargs).slice(0, 2))  # noqa: B010  # the point
    return source


@pytest.mark.parametrize("kind", ["source", "stream", "spiral", "replaced"])
def test_a_stream_with_its_own_read_is_read_through_it(
    kind: str, tmp_path: Path
) -> None:
    """Review of PRs 563 and 564: a subclass, or an object, with its own ``as_table``."""
    with fake.install(tmp_path):
        if kind == "spiral":
            table = fake.ExternalProject("example").create_table(
                "default.demo", key_schema=pa.schema([("k", pa.uint64())])
            )
            table.write(
                pa.table({"k": pa.array([1, 2, 3], pa.uint64()), "x": [3.0, 4.0, 5.0]})
            )
        own: Any = {
            "source": lambda: _OwnSource(_table(), tag_columns=["k"]),
            "stream": lambda: _raw(_OwnStream),
            "spiral": lambda: _OwnSpiral("example", "demo"),
            "replaced": _replaced,
        }[kind]()
        assert not _derived(carried_table(own, CARRIED))
        assert own.map_data({"x": "z"}).as_table().num_rows == 2
        assert own.polars_filter(pl.col("k") > -1).as_table().num_rows == 2


def test_a_stream_without_the_internal_read_is_read_as_before() -> None:
    forwarding: Any = _Forwarding(_source())
    assert not _derived(carried_table(forwarding, CARRIED))
    result = DropNulls(["x"]).static_process(forwarding)
    assert result.as_table(all_info=True).equals(
        DropNulls(["x"]).static_process(_source()).as_table(all_info=True)
    )


class _OwnDropNulls(DropNulls):
    """A user's operator over a library one."""

    seen: list[pa.Table] = []

    def unary_static_process(self, stream: Any) -> Any:
        self.seen.append(self._input_table(stream, CARRIED))
        return super().unary_static_process(stream)


def test_a_subclass_of_a_library_operator_reads_text() -> None:
    _OwnDropNulls.seen = []
    result = _OwnDropNulls(["x"])(_source())
    assert not _derived(carried_table(result, CARRIED))
    (seen,) = _OwnDropNulls.seen
    assert not _derived(seen)


def test_every_library_source_is_read_without_text() -> None:
    """Each kind declares the read itself; the ones with rows of their own do not."""
    from orcapod import sources

    declared = {
        name
        for name in dir(sources)
        if isinstance(cls := getattr(sources, name), type)
        and "_carried_table" in vars(cls)
    }
    kinds = {
        name
        for name in dir(sources)
        if isinstance(cls := getattr(sources, name), type)
        and vars(cls).get("TABLE_IS_ROWS") is True
    }
    assert kinds - declared == {"CachedSource"}
    assert declared <= kinds


@pytest.mark.parametrize("kind", ["join", "merge_join", "semi_join", "rejoin"])
def test_a_match_reads_a_stream_with_its_own_read_through_it(kind: str) -> None:
    """Review of PR 564: a subclass that shows fewer rows is matched on those."""

    def matched(left: Any) -> Any:
        return {
            "join": lambda: left.join(_other("a")),
            "merge_join": lambda: MergeJoin()(left, _other("a")),
            "semi_join": lambda: left.semi_join(_other("a")),
            "rejoin": lambda: Rejoin()(left, _source().select_data_columns(["k"])),
        }[kind]()

    # Of the stream's ten rows its own read shows k = 0 and 1; the matches of the others
    # are left out. (``_other`` holds j of another k, so k = 1, 4 and 7 match it.)
    own = matched(_raw(_OwnStream)).as_table()["k"].to_pylist()
    whole = matched(_raw(ArrowTableStream)).as_table()["k"].to_pylist()
    expected = list(range(10)) if kind == "rejoin" else [1, 4, 7]
    assert sorted(whole) == expected
    assert sorted(own) == [k for k in expected if k < 2]


class _OwnJoin(Join):
    """A user's operator over a library one."""

    seen: list[pa.Table] = []

    def static_process(self, *streams: Any) -> Any:
        self.seen.extend(self._input_table(s, CARRIED) for s in streams)
        return super().static_process(*streams)


def test_a_subclass_of_a_join_reads_text() -> None:
    _OwnJoin.seen = []
    result = _OwnJoin()(_other("a"), _other("b"))
    assert not _derived(carried_table(result, CARRIED))
    assert [_derived(t) for t in _OwnJoin.seen] == [(), ()]
    assert result.as_table().num_rows == 12
