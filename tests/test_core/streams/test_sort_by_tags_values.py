"""A read sorted by tags keeps every value and orders lists and structs (ITL-1295).

When a tag is a list (or a struct holding one), Arrow cannot sort the table, and the
sort every presentation uses (``as_table(columns={"sort_by_tags": True})``, ``head().sort_by_tags()``,
the notebook preview) orders it with Polars. It used to carry the whole table through
Polars: a ``date64`` came back at midnight (tags and data alike) and a ``decimal256``
made Polars panic. Now Polars sees only the prepared tag columns and the row numbers,
and the rows are taken from the table: every value is the table's own, bit for bit.

The order: ascending by the tags in tag order, nulls last at every depth: a null list
element or struct field after the values beside it (a list element by element, a
shorter list first when it is a prefix; a struct field by field). Arrow sorts a struct
tag of scalars itself, in that order, and the fallback gives a struct the same order.

Each table has three tags (``t``, the list or struct under test, ``n: int64``,
``s: large_string``) in three tag orders and in three roles (plain tags, data-tags, one
data-tag among plain tags), and three data columns (``x``, the row's name;
``d: date64`` off midnight; ``e: decimal256``).
"""

from __future__ import annotations

import decimal
import re
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.semantic_types.nested_none import NestedNoneNotRestorableError

D = decimal.Decimal
DAY = 86_400_000

# Seven rows; every row differs in its tags, so no tie reaches the record ids.
N = [1, 1, 0, 1, 1, 0, 1]
S = ["b", "a", "a", "b", "a", "b", "a"]
X = [f"r{i}" for i in range(7)]
DATA_D = pa.array([DAY * i + 7 for i in range(7)], pa.date64())
DATA_E = pa.array(
    [D(f"1e{30 + i}") + D("0.01") for i in range(7)], pa.decimal256(40, 2)
)


def _date64(values: list[int | None]) -> "pa.Array[Any]":
    return pa.array(values, pa.date64())


def _decimal256(values: list[Any]) -> "pa.Array[Any]":
    return pa.array(values, pa.decimal256(40, 2))


# The elements under test: a < b < c; a list of them has a null inside it and two
# lists share a first element, so the second decides (and a null inside goes last).
ELEMENTS = {
    "date64": (_date64, [DAY + 1, DAY + 2, 2 * DAY + 5]),
    "decimal256": (_decimal256, [D("-1e35"), D("0.01"), D("1e36")]),
}


def _list_tag(make: Any, values: list[Any]) -> "pa.Array[Any]":
    a, b, _ = values
    # [b, None], [a], [b, a], None, [None], [], [b, a]
    return pa.LargeListArray.from_arrays(
        pa.array([0, 2, 3, 5, 5, 6, 6, 8], pa.int64()),
        make([b, None, a, b, a, None, b, a]),
        mask=pa.array([False, False, False, True, False, False, False]),
    )


def _struct_tag(make: Any, values: list[Any]) -> "pa.Array[Any]":
    a, b, c = values
    return pa.StructArray.from_arrays(
        [
            make([c, a, None, b, a, b, a]),
            pa.array([1, 2, 3, 1, 3, None, 2], pa.int64()),
        ],
        names=["v", "i"],
        mask=pa.array([False, False, False, False, False, False, True]),
    )


def _list_of_struct_tag(make: Any, values: list[Any]) -> "pa.Array[Any]":
    a, b, _ = values
    # [(b, 1), None], [(a, 2)], [(b, 1), (a, None)], None, [None], [], [(b, 1), (a, 2)]
    elements = pa.StructArray.from_arrays(
        [
            make([b, a, a, b, a, a, b, a]),
            pa.array([1, 0, 2, 1, None, 0, 1, 2], pa.int64()),
        ],
        names=["v", "i"],
        mask=pa.array([False, True, False, False, False, True, False, False]),
    )
    return pa.LargeListArray.from_arrays(
        pa.array([0, 2, 3, 5, 5, 6, 6, 8], pa.int64()),
        elements,
        mask=pa.array([False, False, False, True, False, False, False]),
    )


def _list_of_struct_tag_without_null_element(
    make: Any, values: list[Any]
) -> "pa.Array[Any]":
    a, b, _ = values
    # [(b, 1)], [(a, 2)], [(b, 1), (a, 2)], None, [(a, 0)], [], [(b, 1), (a, 1)]: no
    # null struct element (a null list and an empty list only), so the list can be a
    # data-tag a function reads (a null element cannot be read back yet, ITL-1144).
    elements = pa.StructArray.from_arrays(
        [
            make([b, a, b, a, a, b, a]),
            pa.array([1, 2, 1, 2, 0, 1, 1], pa.int64()),
        ],
        names=["v", "i"],
    )
    return pa.LargeListArray.from_arrays(
        pa.array([0, 1, 2, 4, 4, 5, 5, 7], pa.int64()),
        elements,
        mask=pa.array([False, False, False, True, False, False, False]),
    )


KINDS = {
    "large_list": _list_tag,
    "struct": _struct_tag,
    "large_list<struct>": _list_of_struct_tag,
    "large_list<struct without null element>": _list_of_struct_tag_without_null_element,
}
TAGS = {
    f"{kind}<{name}>": (build, make, values)
    for name, (make, values) in ELEMENTS.items()
    for kind, build in KINDS.items()
}
TAG_ORDERS = [("t", "n", "s"), ("n", "s", "t"), ("s", "t", "n")]
# How the tags are given: plain tags, every tag also a data column (data-tags), or the
# last tag in tag order a data-tag among plain tags.
PLAIN, DATA_TAGS, MIXED = "plain", "data_tags", "mixed"
ROLES = (PLAIN, DATA_TAGS, MIXED)


def _exact(array: "pa.Array[Any]") -> list[Any]:
    """Values, temporal ones as integers, so a date64 off midnight shows."""
    if pa.types.is_date64(array.type):
        return array.view(pa.int64()).to_pylist()
    if isinstance(array, pa.LargeListArray):
        values = _exact(array.values)
        offsets = [int(o) for o in array.offsets.to_numpy()]
        nulls = array.is_null().to_pylist()
        return [
            None if nulls[i] else values[offsets[i] : offsets[i + 1]]
            for i in range(len(array))
        ]
    if isinstance(array, pa.StructArray):
        fields = [_exact(array.field(i)) for i in range(array.type.num_fields)]
        nulls = array.is_null().to_pylist()
        return [
            None if nulls[i] else tuple(f[i] for f in fields) for i in range(len(array))
        ]
    return array.to_pylist()


def _key(value: Any) -> Any:
    """The order stated in the module docstring: nulls last at every depth."""
    if value is None:
        return (1,)
    if isinstance(value, list):
        return (0, [_key(v) for v in value])
    if isinstance(value, tuple):
        return (0, tuple(_key(v) for v in value))
    return (0, value)


def _table(tag_name: str, tag_order: tuple[str, ...]) -> pa.Table:
    build, make, values = TAGS[tag_name]
    columns = {
        "t": build(make, values),
        "n": pa.array(N, pa.int64()),
        "s": pa.array(S, pa.large_string()),
    }
    arrays = {
        **{name: columns[name] for name in tag_order},
        "x": pa.array(X, pa.large_string()),
        "d": DATA_D,
        "e": DATA_E,
    }
    return pa.Table.from_arrays(
        list(arrays.values()),
        schema=pa.schema(
            pa.field(name, array.type, nullable=name != "x")
            for name, array in arrays.items()
        ),
    )


def _expected(table: pa.Table, tag_order: tuple[str, ...]) -> list[int]:
    values = {name: _exact(table.column(name).combine_chunks()) for name in tag_order}
    return sorted(
        range(table.num_rows),
        key=lambda row: tuple(_key(values[name][row]) for name in tag_order),
    )


def _source(
    table: pa.Table, tag_order: tuple[str, ...], role: str = PLAIN
) -> ArrowTableSource:
    tag_only: bool | list[str]
    if role == PLAIN:
        tag_only = True
    elif role == DATA_TAGS:
        tag_only = []
    else:
        tag_only = list(tag_order[:-1])
    return ArrowTableSource(
        table,
        tag_columns=list(tag_order),
        tag_only_columns=tag_only,
        source_id="sorted",
    )


def _assert_rows(result: pa.Table, table: pa.Table, order: list[int]) -> None:
    """``result`` holds ``table``'s rows in ``order``, every value exactly."""
    for name in ("t", "n", "s", "x", "d", "e"):
        got = result.column(name).combine_chunks()
        want = table.column(name).combine_chunks().take(pa.array(order))
        assert got.type == want.type, name
        assert _exact(got) == _exact(want), name


READS = {
    "as_table": lambda stream: stream.as_table(columns={"sort_by_tags": True}),
    "head().sort_by_tags()": lambda stream: stream.head(10).sort_by_tags().as_table(),
}


@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("read", list(READS))
@pytest.mark.parametrize("tag_order", TAG_ORDERS, ids=lambda o: "".join(o))
@pytest.mark.parametrize("tag_name", list(TAGS))
def test_a_sorted_read_orders_list_and_struct_tags_and_keeps_every_value(
    tag_name: str, tag_order: tuple[str, ...], read: str, role: str
) -> None:
    table = _table(tag_name, tag_order)
    result = READS[read](_source(table, tag_order, role))
    _assert_rows(result, table, _expected(table, tag_order))


# The tags whose decimal256 values the preview shows as they are (it summarises a list
# of structs as text).
PREVIEW_SHOWS_DECIMAL256 = ("large_list<decimal256>", "struct<decimal256>")
PREVIEW_PANICS = pytest.mark.xfail(
    strict=True,
    reason="The notebook preview renders its table through Polars "
    + "(StreamView._display_frame), which panics on a decimal256 column whatever the "
    + "sort does: ITL-1302, not this sort.",
)


@pytest.mark.parametrize("tag_order", TAG_ORDERS, ids=lambda o: "".join(o))
@pytest.mark.parametrize(
    "tag_name",
    [
        pytest.param(name, marks=PREVIEW_PANICS)
        if name in PREVIEW_SHOWS_DECIMAL256
        else name
        for name in TAGS
    ],
)
def test_the_preview_shows_list_and_struct_tags_in_tag_order(
    tag_name: str, tag_order: tuple[str, ...]
) -> None:
    """The preview's rows (its ``x`` cells); the ``decimal256`` data column is left out."""
    table = _table(tag_name, tag_order).drop_columns(["e"])
    source = _source(table, tag_order)

    def preview() -> str:
        try:
            return source._repr_html_()
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as error:  # a Polars panic is a BaseException
            raise AssertionError(f"{type(error).__name__}: {error}") from None

    html = preview()
    assert "sorted by tags" in html
    shown = re.findall(r"<td[^>]*>&quot;(r\d)&quot;</td>", html)
    assert shown == [X[row] for row in _expected(table, tag_order)]


@pytest.mark.parametrize("tag_order", TAG_ORDERS, ids=lambda o: "".join(o))
@pytest.mark.parametrize("element", list(ELEMENTS))
def test_a_struct_tag_orders_the_same_beside_a_list_tag(
    element: str, tag_order: tuple[str, ...]
) -> None:
    """Alone, Arrow sorts a struct tag; beside a list tag, the fallback does: one order."""
    table = _table(f"struct<{element}>", tag_order)
    alone = READS["as_table"](_source(table, tag_order))
    with_list = table.append_column(
        "l", pa.array([[1]] * table.num_rows, pa.large_list(pa.int64()))
    )
    beside = READS["as_table"](_source(with_list, (*tag_order, "l")))
    assert beside.column("x").to_pylist() == alone.column("x").to_pylist()
    assert alone.column("x").to_pylist() == [
        X[row] for row in _expected(table, tag_order)
    ]


def test_nulls_inside_a_list_order_last() -> None:
    """``[b, a]`` before ``[b, None]``; ``[None]`` after every list starting with a value."""
    make, (a, b, _) = ELEMENTS["date64"]
    table = _table("large_list<date64>", ("t", "n", "s"))
    result = READS["as_table"](_source(table, ("t", "n", "s")))
    assert _exact(result.column("t").combine_chunks()) == [
        [],
        [a],
        [b, a],
        [b, a],
        [b, None],
        [None],
        None,
    ]


@function_pod(output_keys=["y", "z"])
def marked(x: str, w: str) -> tuple[str, str]:
    """Return ``x`` and ``w`` marked."""
    return x + "+", w + "-"


TAGS_FIRST, DATA_FIRST = "tags_first", "data_first"


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
@pytest.mark.parametrize("layout", [TAGS_FIRST, DATA_FIRST])
@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("tag_order", TAG_ORDERS, ids=lambda o: "".join(o))
@pytest.mark.parametrize("tag_name", list(TAGS))
def test_a_job_node_sorted_read_keeps_every_value(
    tag_name: str,
    tag_order: tuple[str, ...],
    role: str,
    layout: str,
    orchestrator: str,
    request: pytest.FixtureRequest,
) -> None:
    """A function node with two data columns in and two out, read sorted: through the
    node and through its stored records, for every tag order, role and table layout,
    on the sync and the async orchestrator."""
    t_is_data_tag = role == DATA_TAGS or (role == MIXED and tag_order[-1] == "t")
    if tag_name.startswith("large_list<struct><") and t_is_data_tag:
        # The function reads a data-tag as data; a None struct element inside the
        # list cannot be read back yet: the sync run records those two rows as failed
        # (the read misses them), the async run raises.
        request.applymarker(
            pytest.mark.xfail(
                strict=True,
                raises=AssertionError
                if orchestrator == "sync"
                else NestedNoneNotRestorableError,
                reason="ITL-1144: NestedNoneNotRestorableError: a None inside a "
                + "container cannot be read back yet, so the rows whose list-of-struct "
                + "data-tag holds a null element fail in the function; ITL-1308: the "
                + "sync run records those rows as failed and continues, the async run "
                + "raises",
            )
        )
    full = _table(tag_name, tag_order)
    data = {"x": full.column("x"), "w": pa.array([f"w{i}" for i in range(7)])}
    tags = {name: full.column(name) for name in tag_order}
    columns = {**tags, **data} if layout == TAGS_FIRST else {**data, **tags}
    # The tags keep their flags; both data columns hold no null.
    table = pa.Table.from_arrays(
        list(columns.values()),
        schema=pa.schema(
            full.schema.field(name) if name in tags else pa.field(name, col.type, False)
            for name, col in columns.items()
        ),
    )
    job = PipelineJob(name="sorted", database=InMemoryArrowDatabase())
    with job:
        marked.pod(_source(table, tag_order, role), label="node")
    job.run(
        orchestrator=SyncPipelineOrchestrator()
        if orchestrator == "sync"
        else AsyncPipelineOrchestrator()
    )
    order = _expected(full, tag_order)
    for result in (
        job.nodes["node"].as_table(columns={"sort_by_tags": True}),
        job.nodes["node"].as_source().as_table(columns={"sort_by_tags": True}),
    ):
        for name in tag_order:
            want = full.column(name).combine_chunks().take(pa.array(order))
            assert _exact(result.column(name).combine_chunks()) == _exact(want), name
        assert result.column("y").to_pylist() == [X[row] + "+" for row in order]
        assert result.column("z").to_pylist() == [f"w{row}-" for row in order]


@pytest.mark.parametrize(
    "tag",
    [
        pa.array([[("a", 1)], [("b", 2)]], pa.map_(pa.large_string(), pa.int64())),
        pa.array([(1, 2, 3), (0, 1, 2)], pa.month_day_nano_interval()),
        pa.array([[2], [1]], pa.list_view(pa.int64())),
    ],
    ids=["map", "interval", "list_view"],
)
def test_a_tag_with_no_order_is_refused_naming_it(tag: "pa.Array[Any]") -> None:
    """A raw stream may hold one (a source refuses it); Polars never sees it."""
    stream = ArrowTableStream(
        pa.table(
            {
                "l": pa.array([[1], [0]], pa.large_list(pa.int64())),
                "bad": tag,
                "x": [1, 2],
            }
        ),
        tag_columns=["l", "bad"],
    )
    with pytest.raises(TypeError, match="cannot order rows by the tag 'bad'"):
        stream.as_table(columns={"sort_by_tags": True})
