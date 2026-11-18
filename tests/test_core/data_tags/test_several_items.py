"""Data-tags among several named columns: DropNulls and Index (ITL-1236).

* ``DropNulls`` naming several columns, plain tags, data-tags holding nulls and data,
  in several orders: one row set, one schema, the same on the async path; each named
  column, in both roles for a data-tag, becomes non-nullable.
* ``Index`` with ``out=`` reads a list-valued data-tag (after ``Batch``) as it reads a
  data column, on both paths.
"""

from __future__ import annotations

import itertools

import pyarrow as pa
import pytest

from orcapod.core.operators import Batch, DropNulls, Index
from orcapod.core.sources import ArrowTableSource

from ..operators.several_items import (
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


def _nullable_source(tag_only: list[str]) -> ArrowTableSource:
    return ArrowTableSource(
        NULLS, tag_columns=["t1", "t2"], source_id="n", tag_only_columns=tag_only
    )


@pytest.mark.parametrize("tag_only", [[], ["t2"]], ids=["data_tags", "mixed"])
def test_drop_nulls_of_several_columns_with_nullable_data_tags(
    tag_only: list[str],
) -> None:
    source = _nullable_source(tag_only)
    results = {}
    for names in itertools.permutations(["t1", "t2", "d1"]):
        op = DropNulls(list(names))
        kept = op(source)
        tag_schema, data_schema = kept.output_schema()
        assert tag_schema["t1"] is str and tag_schema["t2"] is int
        assert data_schema["d1"] is int
        if "t1" not in tag_only:
            # A data-tag is one column: narrowed in both roles.
            assert data_schema["t1"] is str
        rows = rows_fingerprint(list(kept.iter_data()))
        assert rows_fingerprint(async_rows(DropNulls(list(names)), source)) == rows
        results[names] = {**fingerprint(kept), "presentation": presentation(kept)}
        assert [data["d2"] for _, data in kept.iter_data()] == [5]
    first = next(iter(results.values()))
    assert all(result == first for result in results.values())


@pytest.mark.parametrize("tag_only", [[], ["t2"]], ids=["data_tags", "mixed"])
def test_drop_nulls_of_one_data_tag_keeps_the_rows_where_it_has_a_value(
    tag_only: list[str],
) -> None:
    kept = DropNulls(["t1"])(_nullable_source(tag_only))
    assert sorted(data["d2"] for _, data in kept.iter_data()) == [5, 7, 8]  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize("role", [DATA_TAGS, MIXED])
@pytest.mark.parametrize("column", ["t1", "t3", "d1"])
def test_index_reads_a_list_valued_data_tag_as_a_data_column(
    role: str, column: str
) -> None:
    """After ``Batch``, t1 and t3 are list-valued data-tags (t2 is plain in MIXED)."""
    batched = Batch(batch_size=3)(
        make_source(MAIN, Arrangement(MAIN.tags, TAGS_FIRST, role))
    )
    op = Index(column, 0, out=f"first_{column}")
    indexed = op(batched)
    first = [data[f"first_{column}"] for _, data in indexed.iter_data()]
    assert first == {"t1": ["b", "b"], "t3": [1, 2], "d1": [10, 40]}[column]
    # The data-tag itself is unchanged: still a tag and a data column.
    assert presentation(indexed)[0] == ("t1", "t2", "t3")
    assert rows_fingerprint(
        async_rows(Index(column, 0, out=f"first_{column}"), batched)
    ) == rows_fingerprint(list(indexed.iter_data()))
