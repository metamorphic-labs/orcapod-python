"""A source-info token's shape must match its data's shape (ITL-1491).

Developer, November 27, 2026. A token column's type is some list layers on top of
``large_string``. Those layers must match exactly the outer list layers of its data
column's type: a ``large_list`` layer needs a ``large_list`` layer, a fixed-size list
a fixed-size list of the same size. The data may be deeper than its token, and a
plain string token fits any data. A token with a layer is never null (an element
inside it may be); a plain string token may be null. The rule is checked once, where
a stream is built from a table, from the columns' types and validity only.

Every stream here has two tags and two data columns, in two column layouts, and the
checked column in each role it can have (a data column, a data-tag).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.operators import Batch, GroupBy, MergeJoin, SelectDataColumns
from orcapod.core.sources import ArrowTableSource, DictSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.utils import schema_utils

STR = pa.large_string()
INT = pa.int64()


def large(kind: pa.DataType) -> pa.DataType:
    return pa.large_list(kind)


def fixed(kind: pa.DataType, size: int = 2) -> pa.DataType:
    return pa.list_(kind, size)


ROLES = pytest.mark.parametrize("role", ["data", "data_tag"])
LAYOUTS = pytest.mark.parametrize("layout", ["tags_first", "data_first"])


def _stream(
    token_type: pa.DataType,
    tokens: list[Any],
    data_type: pa.DataType,
    data: list[Any],
    role: str = "data",
    layout: str = "tags_first",
) -> ArrowTableStream:
    """Two tags (``g``, ``k``) and two data columns; ``x`` is the checked column.

    In the ``data_tag`` role ``x`` is a tag that is a data-tag too.
    """
    rows = len(data)
    columns: dict[str, tuple[pa.DataType, list[Any]]] = {
        "g": (STR, [f"g{i}" for i in range(rows)]),
        "k": (INT, list(range(rows))),
        "x": (data_type, data),
        "w": (INT, [10 * i for i in range(rows)]),
        "_source_x": (token_type, tokens),
        "_source_w": (STR, [f"s::row_{i}::w" for i in range(rows)]),
    }
    names = (
        ["g", "k", "x", "w", "_source_x", "_source_w"]
        if layout == "tags_first"
        else ["_source_w", "w", "_source_x", "x", "k", "g"]
    )
    table = pa.Table.from_arrays(
        [pa.array(columns[n][1], columns[n][0]) for n in names],
        schema=pa.schema(
            [
                pa.field(n, columns[n][0], nullable=None in columns[n][1])
                if n == "x"
                else pa.field(n, columns[n][0])
                for n in names
            ]
        ),
    )
    tags = ["g", "k", "x"] if role == "data_tag" else ["g", "k"]
    return ArrowTableStream(
        table, tag_columns=tags, data_tags=["x"] if role == "data_tag" else []
    )


# (token type, tokens, data type, data): two rows each.
VALID: dict[str, tuple[pa.DataType, list[Any], pa.DataType, list[Any]]] = {
    # A plain string token has no layer: it fits any data, and may be null.
    "string_over_int": (STR, ["a", "b"], INT, [1, 2]),
    "null_string_over_int": (STR, [None, "b"], INT, [1, 2]),
    "string_over_list": (STR, ["a", None], large(INT), [[1], [2, 3]]),
    "string_over_fixed": (STR, ["a", "b"], fixed(INT), [[1, 2], [3, 4]]),
    "string_over_struct": (
        STR,
        ["a", "b"],
        pa.struct([("p", INT)]),
        [{"p": 1}, {"p": 2}],
    ),
    # One layer.
    "list_over_list": (large(STR), [["a"], ["b", "c"]], large(INT), [[1], [2, 3]]),
    "non_large_spellings": (
        pa.list_(pa.string()),
        [["a"], ["b", "c"]],
        pa.list_(INT),
        [[1], [2, 3]],
    ),
    "fixed_over_fixed": (
        fixed(STR),
        [["a", "b"], ["c", "d"]],
        fixed(INT),
        [[1, 2], [3, 4]],
    ),
    # The data may be deeper than its token.
    "list_over_list_of_lists": (
        large(STR),
        [["a"], ["b", "c"]],
        large(large(INT)),
        [[[1]], [[2], [3, 4]]],
    ),
    "list_over_list_of_fixed": (
        large(STR),
        [["a"], ["b", "c"]],
        large(fixed(INT)),
        [[[1, 2]], [[3, 4], [5, 6]]],
    ),
    "fixed_over_fixed_of_lists": (
        fixed(STR),
        [["a", "b"], ["c", "d"]],
        fixed(large(INT)),
        [[[1], [2]], [[3, 4], []]],
    ),
    # Two layers, of either kind at either level.
    "two_levels": (
        large(large(STR)),
        [[["a"]], [["b"], ["c", "d"]]],
        large(large(INT)),
        [[[1]], [[2], [3, 4]]],
    ),
    "two_levels_over_three": (
        large(large(STR)),
        [[["a"]], [["b"], ["c"]]],
        large(large(large(INT))),
        [[[[1]]], [[[2]], [[3, 4]]]],
    ),
    "list_of_fixed": (
        large(fixed(STR)),
        [[["a", "b"]], [["c", "d"], ["e", "f"]]],
        large(fixed(INT)),
        [[[1, 2]], [[3, 4], [5, 6]]],
    ),
    "fixed_of_lists": (
        fixed(large(STR)),
        [[["a"], []], [["b", "c"], ["d"]]],
        fixed(large(INT)),
        [[[1], []], [[2, 3], [4]]],
    ),
    # A list token is never null, but an element inside it may be.
    "null_element": (large(STR), [["a", None], [None]], large(INT), [[1, 2], [3]]),
    "empty_list": (large(STR), [[], ["a"]], large(INT), [[], [1]]),
    "null_inner_list": (
        large(large(STR)),
        [[None, ["a"]], [[None]]],
        large(large(INT)),
        [[[1], [2]], [[3]]],
    ),
    "null_element_of_fixed": (
        fixed(STR),
        [["a", None], [None, None]],
        fixed(INT),
        [[1, 2], [3, 4]],
    ),
    # A layer's item field (name, nullability, metadata) is not compared.
    "other_item_field": (
        pa.large_list(pa.field("member", STR, nullable=False, metadata={"m": "v"})),
        [["a"], ["b", "c"]],
        large(INT),
        [[1], [2, 3]],
    ),
    "other_data_item_field": (
        large(STR),
        [["a"], ["b", "c"]],
        pa.large_list(pa.field("element", INT, nullable=False)),
        [[1], [2, 3]],
    ),
}


@LAYOUTS
@ROLES
@pytest.mark.parametrize("case", list(VALID))
def test_a_token_whose_layers_are_its_datas_outer_layers_is_accepted(
    case: str, role: str, layout: str
) -> None:
    """The stream is built, and its token column keeps the declared layers."""
    token_type, tokens, data_type, data = VALID[case]
    stream = _stream(token_type, tokens, data_type, data, role, layout)
    table = stream.as_table(columns={"source": True})
    assert table.column("_source_x").to_pylist() == tokens
    assert table.column("x").to_pylist() == data
    assert schema_utils.token_layers_fit(
        table.schema.field("_source_x").type, table.schema.field("x").type
    )
    # A stream with no rows is checked on its types alone.
    empty = _stream(token_type, [], data_type, [], role, layout)
    assert empty.as_table().num_rows == 0


# (token type, tokens, data type, data): the token's layers are not the data's.
WRONG_SHAPE: dict[str, tuple[pa.DataType, list[Any], pa.DataType, list[Any]]] = {
    "list_over_int": (large(STR), [["a"], ["b"]], INT, [1, 2]),
    "list_over_string": (large(STR), [["a"], ["b"]], STR, ["p", "q"]),
    "list_over_struct": (
        large(STR),
        [["a"], ["b"]],
        pa.struct([("p", large(INT))]),
        [{"p": [1]}, {"p": [2]}],
    ),
    "fixed_over_int": (fixed(STR), [["a", "b"], ["c", "d"]], INT, [1, 2]),
    "list_over_fixed": (
        large(STR),
        [["a", "b"], ["c", "d"]],
        fixed(INT),
        [[1, 2], [3, 4]],
    ),
    "fixed_over_list": (
        fixed(STR),
        [["a", "b"], ["c", "d"]],
        large(INT),
        [[1, 2], [3, 4]],
    ),
    "fixed_of_another_size": (
        fixed(STR, 2),
        [["a", "b"], ["c", "d"]],
        fixed(INT, 3),
        [[1, 2, 3], [4, 5, 6]],
    ),
    "two_levels_over_one": (
        large(large(STR)),
        [[["a"]], [["b"]]],
        large(INT),
        [[1], [2]],
    ),
    "inner_kind_differs": (
        large(fixed(STR)),
        [[["a", "b"]], [["c", "d"]]],
        large(large(INT)),
        [[[1, 2]], [[3, 4]]],
    ),
    "inner_size_differs": (
        large(fixed(STR, 2)),
        [[["a", "b"]], [["c", "d"]]],
        large(fixed(INT, 3)),
        [[[1, 2, 3]], [[4, 5, 6]]],
    ),
    # Under its layers a token is a string: any other leaf is refused, also where
    # the layers fit (second review of PR #601, finding 1).
    "integer_token": (INT, [1, 2], INT, [1, 2]),
    "binary_token": (pa.large_binary(), [b"t", b"u"], INT, [1, 2]),
    "list_of_integers_token": (large(INT), [[1], [2]], large(INT), [[3], [4]]),
    "list_of_binary_token": (
        large(pa.large_binary()),
        [[b"t"], [b"u"]],
        large(INT),
        [[3], [4]],
    ),
    "nested_integers_token": (
        large(large(INT)),
        [[[1]], [[2]]],
        large(large(INT)),
        [[[3]], [[4]]],
    ),
    "fixed_of_integers_token": (
        fixed(INT),
        [[1, 2], [3, 4]],
        fixed(INT),
        [[5, 6], [7, 8]],
    ),
    # A token layer of a kind other than a list is refused, whatever the data.
    "struct_token": (
        pa.struct([("t", STR)]),
        [{"t": "a"}, {"t": "b"}],
        pa.struct([("t", INT)]),
        [{"t": 1}, {"t": 2}],
    ),
    "list_of_struct_token": (
        large(pa.struct([("t", STR)])),
        [[{"t": "a"}], [{"t": "b"}]],
        large(pa.struct([("t", INT)])),
        [[{"t": 1}], [{"t": 2}]],
    ),
}


def _refusal(build: Any) -> str:
    """The message of the ``InputValidationError`` ``build`` raises."""
    with pytest.raises(InputValidationError) as refused:
        build()
    return str(refused.value)


@LAYOUTS
@ROLES
@pytest.mark.parametrize("case", list(WRONG_SHAPE))
def test_a_token_whose_layers_are_not_its_datas_is_refused(
    case: str, role: str, layout: str
) -> None:
    """The stream is refused when it is built, with rows and with none, naming the
    token column, the data column and both types."""
    token_type, tokens, data_type, data = WRONG_SHAPE[case]
    assert not schema_utils.token_layers_fit(token_type, data_type)
    for token_values, data_values in ((tokens, data), ([], [])):
        message = _refusal(
            lambda: _stream(
                token_type, token_values, data_type, data_values, role, layout
            )
        )
        assert "'_source_x'" in message and "'x'" in message
        assert str(token_type) in message and str(data_type) in message
        assert "'_source_w'" not in message


# A token with a layer that is null on some row; the shapes fit.
NULL_TOKEN: dict[str, tuple[pa.DataType, list[Any], pa.DataType, list[Any]]] = {
    "null_list": (large(STR), [["a"], None], large(INT), [[1], [2]]),
    "all_null_lists": (large(STR), [None, None], large(INT), [[1], [2]]),
    "null_fixed": (fixed(STR), [None, ["a", "b"]], fixed(INT), [[1, 2], [3, 4]]),
    "null_two_levels": (
        large(large(STR)),
        [None, [["a"]]],
        large(large(INT)),
        [[[1]], [[2]]],
    ),
    "null_over_null_data": (large(STR), [None, ["a"]], large(INT), [None, [1]]),
}


@LAYOUTS
@ROLES
@pytest.mark.parametrize("case", list(NULL_TOKEN))
def test_a_null_list_token_is_refused(case: str, role: str, layout: str) -> None:
    """A token that has a layer is never null: the stream is refused when built."""
    token_type, tokens, data_type, data = NULL_TOKEN[case]
    assert schema_utils.token_layers_fit(token_type, data_type)
    message = _refusal(
        lambda: _stream(token_type, tokens, data_type, data, role, layout)
    )
    assert "'_source_x'" in message and "'x'" in message
    assert str(token_type) in message and "null" in message
    # With no rows there is no null: the same types are accepted.
    assert _stream(token_type, [], data_type, [], role, layout).as_table().num_rows == 0


def test_every_violating_column_is_named_once() -> None:
    """Two violating columns, one of each kind, are both named in one refusal."""
    table = pa.table(
        {
            "k": pa.array([1, 2], INT),
            "x": pa.array([1, 2], INT),
            "y": pa.array([[1], [2]], large(INT)),
            "z": pa.array([5, 6], INT),
            "_source_x": pa.array([["a"], ["b"]], large(STR)),
            "_source_y": pa.array([None, ["c"]], large(STR)),
            "_source_z": pa.array(["s::0::z", None], STR),
        }
    )
    message = _refusal(lambda: ArrowTableStream(table, tag_columns=["k"]))
    assert message.count("'_source_x'") == 1 and message.count("'_source_y'") == 1
    assert "'_source_z'" not in message


# ---------------------------------------------------------------------------
# What the library builds satisfies the rule
# ---------------------------------------------------------------------------


def _paths(source_id: str) -> DictSource:
    return DictSource(
        [
            {"g": "m", "k": 1, "p": Path("/a"), "n": 1},
            {"g": "m", "k": 2, "p": Path("/b"), "n": 2},
            {"g": "n", "k": 1, "p": Path("/c"), "n": 3},
        ],
        tag_columns=["g", "k"],
        tag_only_columns=True,
        source_id=source_id,
    )


AGGREGATIONS = {
    "batch": lambda: Batch(batch_size=2)(_paths("a")),
    "group_by": lambda: GroupBy(by=["g"])(_paths("a")),
    "merge_join": lambda: MergeJoin()(_paths("a"), _paths("b")),
    "group_by_then_batch": lambda: Batch(batch_size=2)(GroupBy(by=["g"])(_paths("a"))),
}


@pytest.mark.parametrize("aggregation", list(AGGREGATIONS))
def test_a_logical_list_counts_as_one_list_layer(aggregation: str) -> None:
    """``Batch``, ``GroupBy`` and ``MergeJoin`` over a logical element (a ``Path``).

    The data column is the logical list type built for a list of ``Path`` (an
    extension type stored as a list); for the rule it is one ``large_list`` layer
    per list level, and its token is a list of strings of as many levels.
    """
    table = AGGREGATIONS[aggregation]().as_table(columns={"source": True})
    levels = 2 if aggregation == "group_by_then_batch" else 1
    token = STR
    for _ in range(levels):
        token = large(token)
    for name in ("p", "n"):
        assert table.schema.field(f"_source_{name}").type == token
        assert schema_utils.token_layers_fit(token, table.schema.field(name).type), (
            table.schema.field(name).type
        )
    assert isinstance(table.schema.field("p").type, pa.ExtensionType)
    # The same table given back as a raw stream is accepted.
    system = AGGREGATIONS[aggregation]().as_table(
        columns={"source": True, "system_tags": True}
    )
    tags = [n for n in ("g", "k") if n in system.column_names]
    assert ArrowTableStream(system, tag_columns=tags).as_table().num_rows > 0


def test_an_operator_over_a_valid_stream_keeps_it_valid() -> None:
    """A stream built by hand with fitting layers passes through operators."""
    token_type, tokens, data_type, data = VALID["list_of_fixed"]
    stream = _stream(token_type, tokens, data_type, data)
    selected = SelectDataColumns(["x"])(stream).as_table(columns={"source": True})
    assert selected.schema.field("_source_x").type == token_type
    batched = Batch(batch_size=2)(stream).as_table(columns={"source": True})
    assert batched.schema.field("_source_x").type == large(token_type)
    assert schema_utils.token_layers_fit(
        batched.schema.field("_source_x").type, batched.schema.field("x").type
    )


def test_a_source_builds_plain_string_tokens_over_any_data() -> None:
    """A source's tokens are plain strings, also over list and fixed-size data."""
    source = ArrowTableSource(
        pa.table(
            {
                "k": pa.array([1, 2], INT),
                "g": pa.array(["m", "n"], STR),
                "x": pa.array([[1], [2, 3]], large(INT)),
                "w": pa.array([[1, 2], [3, 4]], fixed(INT)),
            }
        ),
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="lists",
    )
    table = source.as_table(columns={"source": True})
    assert table.schema.field("_source_x").type == STR
    assert table.schema.field("_source_w").type == STR


# ---------------------------------------------------------------------------
# Index: the element's token has the declared type with one layer removed
# ---------------------------------------------------------------------------


def _index_rows(
    stream: ArrowTableStream, i: int, route: str, out: str | None
) -> pa.Table:
    """``Index`` on ``x`` over ``stream``, on the sync or the async route."""
    import asyncio

    from orcapod.channels import Channel
    from orcapod.core.operators import Index

    operator = Index("x", i, out=out)
    if route == "sync":
        return operator(stream).as_table(columns={"source": True})

    async def run() -> list[Any]:
        incoming: Channel[Any] = Channel(buffer_size=16)
        outgoing: Channel[Any] = Channel(buffer_size=16)
        for row in stream.iter_data():
            await incoming.writer.send(row)
        await incoming.writer.close()
        await operator.async_execute(
            [incoming.reader],
            outgoing.writer,
            input_pipeline_hashes=[stream.pipeline_hash()],
        )
        return await outgoing.reader.collect()

    rows = asyncio.run(run())
    return pa.concat_tables(
        [data.as_table(columns={"source": True}) for _, data in rows]
    )


ROUTES = pytest.mark.parametrize("route", ["sync", "async"])
MEMBER = pa.field("member", STR, nullable=False)
# (token type, tokens, data type, data, the element's token type)
INDEXED: dict[
    str, tuple[pa.DataType, list[Any], pa.DataType, list[Any], pa.DataType]
] = {
    "list_of_strings": (
        large(STR),
        [["a", "b"], ["c", "d"]],
        large(INT),
        [[1, 2], [3, 4]],
        STR,
    ),
    "list_of_fixed": (
        large(fixed(STR)),
        [[["a", "b"], ["c", "d"]], [["e", "f"], ["g", "h"]]],
        large(fixed(INT)),
        [[[1, 2], [3, 4]], [[5, 6], [7, 8]]],
        fixed(STR),
    ),
    "list_of_named_item_lists": (
        large(pa.large_list(MEMBER)),
        [[["a"], ["b", "c"]], [["d"], []]],
        large(large(INT)),
        [[[1], [2, 3]], [[4], []]],
        pa.large_list(MEMBER),
    ),
    "three_levels": (
        large(large(large(STR))),
        [[[[]], [["a"]]], [[["b"]], [[], ["c"]]]],
        large(large(large(INT))),
        [[[[]], [[1]]], [[[2]], [[], [3]]]],
        large(large(STR)),
    ),
}


@ROUTES
@ROLES
@pytest.mark.parametrize("out", [None, "picked"])
@pytest.mark.parametrize("i", [0, 1])
@pytest.mark.parametrize("case", list(INDEXED))
def test_index_gives_the_element_the_declared_token_type_less_one_layer(
    case: str, i: int, out: str | None, role: str, route: str
) -> None:
    """The element's token is the token list's element, typed as that list's items.

    ``Index`` typed the element's token again from its value, so a fixed-size
    list became a ``large_list`` (which no longer fits fixed-size data), a list's
    item field lost its name and nullability, and an empty list lost its inner
    levels (ITL-1491). Both routes give the declared type with one layer removed.
    """
    token_type, tokens, data_type, data, element_type = INDEXED[case]
    if role == "data_tag" and out is None:
        pytest.skip("Index replaces a data column in place, not a tag")
    stream = _stream(token_type, tokens, data_type, data, role)
    table = _index_rows(stream, i, route, out)
    name = f"_source_{out or 'x'}"
    assert table.schema.field(name).type.equals(element_type, check_metadata=True), (
        table.schema.field(name).type
    )
    assert table.column(name).to_pylist() == [token[i] for token in tokens]
    assert table.column(out or "x").to_pylist() == [value[i] for value in data]
    if out is not None:
        assert table.schema.field("_source_x").type.equals(
            token_type, check_metadata=True
        )


@ROUTES
@pytest.mark.parametrize("out", [None, "picked"])
def test_index_refuses_a_null_element_where_a_list_token_remains(
    out: str | None, route: str
) -> None:
    """A null inner list is a valid element, but not a valid token of its own.

    The stream is valid: a token list may hold a null element. ``Index`` on that
    element would give a row a null token whose type is a list, which the
    token-shape rule forbids; both routes refuse, naming the column. The element
    that is a list is taken as before.
    """
    stream = _stream(
        large(large(STR)),
        [[None, ["a"]], [["b"], ["c"]]],
        large(large(INT)),
        [[[1], [2]], [[3], [4]]],
    )
    with pytest.raises((InputValidationError, ExceptionGroup)) as refused:
        _ = _index_rows(stream, 0, route, out)
    error: BaseException = refused.value
    while isinstance(error, ExceptionGroup):
        (error,) = error.exceptions
    assert isinstance(error, InputValidationError)
    assert f"'_source_{out or 'x'}'" in str(error) and "Index" in str(error)
    taken = _index_rows(stream, 1, route, out)
    assert taken.column(f"_source_{out or 'x'}").to_pylist() == [["a"], ["c"]]


@ROUTES
def test_index_keeps_a_null_string_element_as_a_null_token(route: str) -> None:
    """A null element of a list of string tokens is a null string token: valid."""
    stream = _stream(
        large(STR), [[None, "a"], ["b", None]], large(INT), [[1, 2], [3, 4]]
    )
    table = _index_rows(stream, 0, route, None)
    assert table.schema.field("_source_x").type == STR
    assert table.column("_source_x").to_pylist() == [None, "b"]
