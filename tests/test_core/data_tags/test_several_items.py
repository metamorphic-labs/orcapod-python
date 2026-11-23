"""Data-tags among several named columns: DropNulls and Index (ITL-1236).

* ``DropNulls`` naming several columns, plain tags, data-tags holding nulls and data,
  in several orders: one row set, one schema, the same on the async path; each named
  column, in both roles for a data-tag, becomes non-nullable.
* ``Index`` with ``out=`` reads a list-valued data-tag (after ``Batch``) as it reads a
  data column, on both paths.

Each test runs on its input in its declared tag order with the tags first, and in the
reversed tag order with the data first: the result is the same except for the column
order, which follows the input.
"""

from __future__ import annotations

import itertools

import pyarrow as pa
import pytest

from orcapod.core.operators import Batch, DropNulls, Index
from orcapod.core.sources import ArrowTableSource

from ..operators.several_items import (
    DATA_FIRST_REVERSED,
    DATA_TAGS,
    MAIN,
    MIXED,
    TAGS_FIRST,
    Arrangement,
    async_rows,
    fingerprint,
    make_source,
    presentation,
    rows_fingerprint,
)

# Tags t1 and t2 and data d1, each null in one row: only row 0 has no null.
NULLS = pa.table(
    {
        "t1": pa.array(["a", None, "b", "c"], pa.large_string()),
        "t2": pa.array([1, 2, None, 4], pa.int64()),
        "d1": pa.array([1, 2, 3, None], pa.int64()),
        "d2": pa.array([5, 6, 7, 8], pa.int64()),
    }
)


# (tag order, table column order) of ``NULLS``: declared and tags first, reversed and
# data first.
NULLS_ARRANGEMENTS = {
    "t1-t2/tags_first": (("t1", "t2"), ("t1", "t2", "d1", "d2")),
    "t2-t1/data_first_reversed": (("t2", "t1"), ("d2", "d1", "t2", "t1")),
}
CONTROL = "t1-t2/tags_first"


def _nullable_source(
    tag_only: list[str], arrangement: str = CONTROL
) -> ArrowTableSource:
    tag_order, layout = NULLS_ARRANGEMENTS[arrangement]
    return ArrowTableSource(
        NULLS.select(list(layout)),
        tag_columns=list(tag_order),
        source_id="n",
        tag_only_columns=tag_only,
    )


@pytest.mark.parametrize("arrangement", list(NULLS_ARRANGEMENTS))
@pytest.mark.parametrize("tag_only", [[], ["t2"]], ids=["data_tags", "mixed"])
def test_drop_nulls_of_several_columns_with_nullable_data_tags(
    tag_only: list[str], arrangement: str
) -> None:
    source = _nullable_source(tag_only, arrangement)
    results = {}
    for names in itertools.permutations(["t1", "t2", "d1"]):
        op = DropNulls(list(names))
        kept = op(source)
        tag_schema, data_schema = kept.output_schema()
        assert tag_schema["t1"] is str and tag_schema["t2"] is int
        assert data_schema["d1"] is int
        # A data-tag is one column: narrowed in both roles, each data-tag.
        for name, python_type in (("t1", str), ("t2", int)):
            if name not in tag_only:
                assert data_schema[name] is python_type
            else:
                assert name not in data_schema
        # The prediction equals the computed stream's schema (a prediction is
        # verified against the static result, CLAUDE.md), with system tags and
        # source info too.
        for config in (
            {},
            {"system_tags": True},
            {"source": True, "system_tags": True},
        ):
            computed = op.static_process(source).output_schema(columns=config)
            assert kept.output_schema(columns=config) == computed
        # The computed rows hold no null in a named column, in either role.
        table = kept.as_table()
        assert all(table.column(name).null_count == 0 for name in names)
        assert not any(table.schema.field(name).nullable for name in names)
        rows = rows_fingerprint(list(kept.iter_data()))
        assert rows_fingerprint(async_rows(DropNulls(list(names)), source)) == rows
        results[names] = {**fingerprint(kept), "presentation": presentation(kept)}
        assert [data["d2"] for _, data in kept.iter_data()] == [5]
    first = next(iter(results.values()))
    assert all(result == first for result in results.values())
    # The tags follow the input's order; nothing else depends on the arrangement.
    assert first["presentation"][0] == NULLS_ARRANGEMENTS[arrangement][0]
    control = DropNulls(["t1", "t2", "d1"])(_nullable_source(tag_only))
    assert {k: v for k, v in first.items() if k != "presentation"} == fingerprint(
        control
    )


@pytest.mark.parametrize("arrangement", list(NULLS_ARRANGEMENTS))
@pytest.mark.parametrize("tag_only", [[], ["t2"]], ids=["data_tags", "mixed"])
def test_drop_nulls_of_one_data_tag_keeps_the_rows_where_it_has_a_value(
    tag_only: list[str], arrangement: str
) -> None:
    kept = DropNulls(["t1"])(_nullable_source(tag_only, arrangement))
    assert sorted(data["d2"] for _, data in kept.iter_data()) == [5, 7, 8]  # pyright: ignore[reportArgumentType]


INDEX_ARRANGEMENTS = {
    "t1-t2-t3/tags_first": (MAIN.tags, TAGS_FIRST),
    "t3-t2-t1/data_first_reversed": (tuple(reversed(MAIN.tags)), DATA_FIRST_REVERSED),
}


@pytest.mark.parametrize("arrangement", list(INDEX_ARRANGEMENTS))
@pytest.mark.parametrize("role", [DATA_TAGS, MIXED])
@pytest.mark.parametrize("column", ["t1", "t3", "d1"])
def test_index_reads_a_list_valued_data_tag_as_a_data_column(
    role: str, column: str, arrangement: str
) -> None:
    """After ``Batch``, t1 and t3 are list-valued data-tags (t2 is plain in MIXED)."""
    tag_order, layout = INDEX_ARRANGEMENTS[arrangement]
    batched = Batch(batch_size=3)(
        make_source(MAIN, Arrangement(tag_order, layout, role))
    )
    op = Index(column, 0, out=f"first_{column}")
    indexed = op(batched)
    first = [data[f"first_{column}"] for _, data in indexed.iter_data()]
    assert first == {"t1": ["b", "b"], "t3": [1, 2], "d1": [10, 40]}[column]
    # The data-tag itself is unchanged: still a tag and a data column, with its lists;
    # Index keeps its input's tag order as its input presents it (Batch presents
    # the source's tag order, below), and nothing else depends on the
    # arrangement.
    assert presentation(indexed)[0] == presentation(batched)[0]
    control = Index(column, 0, out=f"first_{column}")(
        Batch(batch_size=3)(make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, role)))
    )
    assert fingerprint(indexed) == fingerprint(control)
    assert column in presentation(indexed)[1]
    lists = {"t1": [["b", "a", "a"], ["b", "a", "b"]], "t3": [[1, 2, 1], [2, 1, 2]]}
    lists["d1"] = [[10, 20, 30], [40, 50, 60]]
    assert [data[column] for _, data in indexed.iter_data()] == lists[column]
    if column != "d1":
        assert [tag[column] for tag, _ in indexed.iter_data()] == lists[column]
    assert rows_fingerprint(
        async_rows(Index(column, 0, out=f"first_{column}"), batched)
    ) == rows_fingerprint(list(indexed.iter_data()))


# Resolved by ITL-1365: a stream's tag schema follows its tag order.
@pytest.mark.parametrize("role", [DATA_TAGS, MIXED])
def test_batch_presents_its_input_streams_tag_order(role: str) -> None:
    """The source's tags are (t3, t2, t1); its table holds them as t1, t2, t3."""
    tag_order, layout = INDEX_ARRANGEMENTS["t3-t2-t1/data_first_reversed"]
    source = make_source(MAIN, Arrangement(tag_order, layout, role))
    assert presentation(source)[0] == tag_order
    assert presentation(Batch(batch_size=3)(source))[0] == tag_order
