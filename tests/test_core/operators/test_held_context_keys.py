"""Audit whether operators carry the row context keys held by Arrow tables."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import replace
from typing import cast

import polars as pl
import pyarrow as pa
import pytest

from orcapod import contexts
from orcapod.channels import Channel
from orcapod.core.operators import (
    AddTags,
    Batch,
    DropColumns,
    DropDataColumns,
    DropNulls,
    DropTagColumns,
    GroupBy,
    Index,
    Join,
    MapColumns,
    MapData,
    MapTags,
    MergeJoin,
    Pick,
    PolarsFilter,
    Rejoin,
    SelectColumns,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
    TagBy,
)
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.streams import ArrowTableStream
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig

from .several_items import (
    DATA_FIRST_REVERSED,
    MAIN,
    MIXED,
    PLAIN,
    TAGS_FIRST,
    Arrangement,
    Row,
    async_rows,
    make_source,
)

ALPHA = "std:v1:test-held-alpha"
BETA = "std:v1:test-held-beta"
HELD_KEYS = (ALPHA, ALPHA, BETA, BETA, ALPHA, ALPHA)
ALL_IDS = (10, 20, 30, 40, 50, 60)
KEPT_IDS = (10, 30, 40, 50)
TABLE_COLUMNS = ColumnConfig(context=True, source=True, system_tags=True)
NAMED = ("t1", "d2", "t2", "d3")
ARRANGEMENTS = (
    Arrangement(MAIN.tags, TAGS_FIRST, PLAIN),
    Arrangement(tuple(reversed(MAIN.tags)), DATA_FIRST_REVERSED, PLAIN),
)


@pytest.fixture
def held_contexts(monkeypatch: pytest.MonkeyPatch) -> dict[str, contexts.DataContext]:
    """Register two copies so a datagram or Rejoin can resolve either key."""
    default = contexts.get_default_context()
    registered = {key: replace(default, context_key=key) for key in (ALPHA, BETA)}
    registry = contexts._get_registry()  # pyright: ignore[reportPrivateUsage]  # register test contexts
    original = registry.get_context

    def get_context(key: str | None = None) -> contexts.DataContext:
        if key is not None and key in registered:
            return registered[key]
        return original(key)

    monkeypatch.setattr(registry, "get_context", get_context)
    return registered


def _stream(arrangement: Arrangement) -> ArrowTableStream:
    """Build the shared six-row stream with held keys and list and dictionary data."""
    source = make_source(MAIN, arrangement)
    table = source.as_table(columns=TABLE_COLUMNS)
    table = table.set_column(
        table.schema.get_field_index(constants.CONTEXT_KEY),
        pa.field(constants.CONTEXT_KEY, pa.large_string(), nullable=False),
        pa.array(HELD_KEYS, type=pa.large_string()),
    )
    list_type = pa.large_list(pa.int64())
    dict_type = pa.large_list(
        pa.struct(
            [
                pa.field("key", pa.large_string(), nullable=False),
                pa.field("value", pa.int64()),
            ]
        )
    )
    table = table.append_column(
        pa.field("items", list_type, nullable=False),
        pa.array([[value] for value in ALL_IDS], type=list_type),
    ).append_column(
        pa.field("fields", dict_type, nullable=False),
        pa.array(
            [[{"key": "value", "value": value}] for value in ALL_IDS], type=dict_type
        ),
    )
    tag_columns, data_columns = source.keys()
    return ArrowTableStream(
        table,
        tag_columns=tag_columns,
        data_tags=[name for name in tag_columns if name in data_columns],
    )


def _right(stream: ArrowTableStream, ids: tuple[int, ...]) -> ArrowTableStream:
    """Make a partner with matching origins and tags and distinct data names."""
    table = stream.as_table(columns=TABLE_COLUMNS)
    positions = [ALL_IDS.index(row_id) for row_id in ids]
    _, data_columns = stream.keys()
    renames = {
        **{name: f"right_{name}" for name in data_columns},
        **{
            f"{constants.SOURCE_PREFIX}{name}": f"{constants.SOURCE_PREFIX}right_{name}"
            for name in data_columns
        },
    }
    return ArrowTableStream(
        table.take(positions).rename_columns(
            [renames.get(name, name) for name in table.column_names]
        ),
        tag_columns=stream.keys()[0],
    )


def _table_contexts(table: pa.Table) -> dict[int, str]:
    """Pair each original row, or the first member of a group, with its held key."""
    values = cast(list[int | list[int]], table.column("d1").to_pylist())
    keys = cast(list[str], table.column(constants.CONTEXT_KEY).to_pylist())
    return {
        value[0] if isinstance(value, list) else value: key
        for value, key in zip(values, keys, strict=True)
    }


def _row_contexts(rows: list[Row]) -> dict[int, str]:
    """Pair each original row, or the first member of a group, with its reported key."""
    keys: dict[int, str] = {}
    for _, data in rows:
        value = cast(int | list[int], data["d1"])
        keys[value[0] if isinstance(value, list) else value] = data.data_context_key
    return keys


def _case(
    name: str,
    make_op: Callable[[], StaticOutputOperatorPod],
    *,
    ids: tuple[int, ...] = ALL_IDS,
    binary: bool = False,
    sync_gap: str = "",
) -> list[object]:
    """Exercise both paths and pin any known sync gap at the key comparison."""
    return [
        pytest.param(
            make_op,
            ids,
            binary,
            False,
            id=f"{name}-sync",
            marks=pytest.mark.xfail(strict=True, raises=AssertionError, reason=sync_gap)
            if sync_gap
            else (),
        ),
        pytest.param(make_op, ids, binary, True, id=f"{name}-async"),
    ]


CASES = [
    *_case("AddTags", lambda: AddTags({"a": 1, "b": "tag"})),
    *_case(
        "Batch",
        lambda: Batch(2),
        ids=(10, 30, 50),
        sync_gap="Batch builds new rows and needs a rule for assigning context across members",
    ),
    *_case("DropColumns", lambda: DropColumns(["t1", "d3"])),
    *_case("DropDataColumns", lambda: DropDataColumns(["d2", "d3"])),
    *_case("DropNulls-named", lambda: DropNulls(NAMED), ids=KEPT_IDS),
    *_case(
        "DropNulls-reversed", lambda: DropNulls(tuple(reversed(NAMED))), ids=KEPT_IDS
    ),
    *_case("DropNulls-all", DropNulls, ids=KEPT_IDS),
    *_case("DropTagColumns", lambda: DropTagColumns(["t1", "t2"])),
    *_case(
        "GroupBy",
        lambda: GroupBy(MAIN.tags),
        sync_gap="GroupBy builds new rows and needs a rule for assigning context across members",
    ),
    *_case(
        "Index",
        lambda: Index("items", 0, out="selected"),
        sync_gap="Index reads its sync input through iter_data (ITL-1359)",
    ),
    *_case("Join-single", Join),
    *_case(
        "Join",
        Join,
        binary=True,
        sync_gap="Join needs a rule for assigning context across inputs",
    ),
    *_case("MapColumns", lambda: MapColumns({"t1": "u1", "d3": "e3"})),
    *_case("MapData", lambda: MapData({"d2": "e2", "d3": "e3"})),
    *_case("MapTags", lambda: MapTags({"t1": "u1", "t2": "u2"})),
    *_case(
        "MergeJoin",
        MergeJoin,
        binary=True,
        sync_gap="MergeJoin needs a rule for assigning context across inputs",
    ),
    *_case(
        "Pick",
        lambda: Pick("fields", "value", out="selected"),
        sync_gap="Pick reads its sync input through iter_data (ITL-1359)",
    ),
    *_case(
        "PolarsFilter",
        lambda: PolarsFilter(predicates=[pl.col("d1") != 20, pl.col("d1") != 60]),
        ids=KEPT_IDS,
        sync_gap="PolarsFilter needs to carry the keys without exposing them to wildcard expressions",
    ),
    *_case("Rejoin", Rejoin, binary=True),
    *_case("SelectColumns", lambda: SelectColumns(["t1", "t2", "d1", "d2"])),
    *_case("SelectDataColumns", lambda: SelectDataColumns(["d1", "d2"])),
    *_case("SelectTagColumns", lambda: SelectTagColumns(["t1", "t2"])),
    *_case("SemiJoin", SemiJoin, binary=True, ids=KEPT_IDS),
    *_case("TagBy", lambda: TagBy(["d1", "d3"])),
]


@pytest.mark.parametrize("arrangement", ARRANGEMENTS, ids=lambda value: value.id)
@pytest.mark.parametrize(("make_op", "ids", "binary", "use_async"), CASES)
@pytest.mark.usefixtures("held_contexts")
def test_operator_context_keys(
    arrangement: Arrangement,
    make_op: Callable[[], StaticOutputOperatorPod],
    ids: tuple[int, ...],
    binary: bool,
    use_async: bool,
) -> None:
    """Compare held table keys, or channel keys already flattened by ITL-1359."""
    stream = _stream(arrangement)
    inputs = (stream, _right(stream, ids)) if binary else (stream,)
    op = make_op()
    if use_async:
        reference = _row_contexts(async_rows(SelectDataColumns(["d1", "d2"]), stream))
        assert reference == dict.fromkeys(ALL_IDS, stream.data_context_key)
        actual = _row_contexts(async_rows(op, *inputs))
    else:
        projection = stream.select_data_columns(["d1", "d2"])
        reference = _table_contexts(projection.as_table(columns=TABLE_COLUMNS))
        assert reference == dict(zip(ALL_IDS, HELD_KEYS, strict=True))
        actual = _table_contexts(op(*inputs).as_table(columns=TABLE_COLUMNS))
    if actual.keys() != set(ids):
        pytest.fail(f"Expected original row IDs {ids}, got {tuple(actual)}")
    assert actual == {row_id: reference[row_id] for row_id in ids}


@pytest.mark.parametrize("names", [NAMED, tuple(reversed(NAMED)), None])
@pytest.mark.parametrize("role", [PLAIN, MIXED])
def test_drop_nulls_keeps_context_and_provenance(
    held_contexts: dict[str, contexts.DataContext],
    names: tuple[str, ...] | None,
    role: str,
) -> None:
    """Keep the matching Arrow rows and preserve each context supplied to a channel."""
    stream = _stream(Arrangement(tuple(reversed(MAIN.tags)), DATA_FIRST_REVERSED, role))
    table = stream.as_table(columns=TABLE_COLUMNS)
    hidden = [
        name
        for name in table.column_names
        if name == constants.CONTEXT_KEY
        or name.startswith((constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX))
    ]
    result = stream.drop_nulls() if names is None else stream.drop_nulls(names)
    assert (
        result.as_table(columns=TABLE_COLUMNS)
        .select(hidden)
        .equals(table.take([0, 2, 3, 4]).select(hidden))
    )

    async def execute() -> list[Row]:
        incoming: Channel[Row] = Channel()
        outgoing: Channel[Row] = Channel()
        for index, key in enumerate(HELD_KEYS):
            # Form each channel row in its own registered stream context.
            singleton = ArrowTableStream(
                table.slice(index, 1),
                tag_columns=stream.keys()[0],
                data_tags=[
                    name for name in stream.keys()[0] if name in stream.keys()[1]
                ],
                data_context=held_contexts[key],
            )
            await incoming.writer.send(next(singleton.iter_data()))
        await incoming.writer.close()
        op = DropNulls() if names is None else DropNulls(names)
        await op.async_execute(
            [incoming.reader],
            outgoing.writer,
            input_pipeline_hashes=[stream.pipeline_hash()],
        )
        return await outgoing.reader.collect()

    rows = asyncio.run(execute())
    assert _row_contexts(rows) == {
        row_id: HELD_KEYS[ALL_IDS.index(row_id)] for row_id in KEPT_IDS
    }
    expected = [
        row
        for row_id, row in zip(ALL_IDS, stream.iter_data(), strict=True)
        if row_id in KEPT_IDS
    ]
    assert [(tag.system_tags(), data.source_info()) for tag, data in rows] == [
        (tag.system_tags(), data.source_info()) for tag, data in expected
    ]
    assert [tag.data_context_key for tag, _ in rows] == [
        data.data_context_key for _, data in rows
    ]
