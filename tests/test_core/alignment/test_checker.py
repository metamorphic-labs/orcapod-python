"""The row-alignment checker accepts aligned tables and rejects every kind of misalignment.

A checker that passes everything proves nothing. Each test here takes a table the checker
accepts and moves the cells of one column to other rows (a rotation, the shape of ITL-1029),
and the checker must name that column.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.operators import Batch, Join
from orcapod.system_constants import constants

from .checker import (
    Layout,
    assert_members_cover,
    assert_requested_columns,
    assert_all_columns_checked,
    assert_rows_aligned,
    assert_same_rows_by_identity,
)
from .expectations import pairs, rows
from .fixtures import TAG_COLUMNS, make_sources
from .gate import new_gated_scale_pod
from .jobs import derive_y, make_run, stream_function_tokens
from .records import assert_records_aligned
from .oracle import Oracle


def rotated(table: pa.Table, column: str) -> pa.Table:
    """``table`` with the cells of ``column`` each moved one row down (the last to the top)."""
    index = table.column_names.index(column)
    values = table.column(column)
    order = [table.num_rows - 1, *range(table.num_rows - 1)]
    return table.set_column(index, table.field(column), values.take(order))


def source_table() -> tuple[pa.Table, Layout, Oracle, Any]:
    """Source A of the unique scenario with every column group, its layout and oracle."""
    sources = make_sources("unique")
    table = sources.a.as_table(all_info=True)
    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=sources.a.keys()[1])
    return table, layout, sources.oracle, sources.a.data_context


def test_an_aligned_source_table_passes_and_every_column_is_compared() -> None:
    table, layout, oracle, context = source_table()
    checked = assert_rows_aligned(table, layout, oracle, context)
    assert_all_columns_checked(table, checked)


def _misalignable_columns() -> list[str]:
    """Every column of source A's full table whose rotation changes the table."""
    table, *_ = source_table()
    # A column with one value in every row (the context key, a single source's id) is
    # unchanged by a rotation; it is checked by test_a_wrong_context_key_is_named and the
    # join test instead.
    return [c for c in table.column_names if len(set(table.column(c).to_pylist())) > 1]


@pytest.mark.parametrize("column", _misalignable_columns())
def test_a_rotated_column_is_named(column: str) -> None:
    table, layout, oracle, context = source_table()
    with pytest.raises(AssertionError, match=rf"column '{column}'"):
        assert_rows_aligned(rotated(table, column), layout, oracle, context)


def test_a_wrong_context_key_is_named() -> None:
    table, layout, oracle, context = source_table()
    index = table.column_names.index(constants.CONTEXT_KEY)
    wrong = table.set_column(
        index,
        table.field(constants.CONTEXT_KEY),
        pa.array(["other"] * table.num_rows, type=pa.large_string()),
    )
    with pytest.raises(AssertionError, match=constants.CONTEXT_KEY):
        assert_rows_aligned(wrong, layout, oracle, context)


def test_a_column_no_rule_covers_fails() -> None:
    table, layout, oracle, context = source_table()
    extra = table.append_column("__unexplained", pa.array(range(table.num_rows)))
    with pytest.raises(AssertionError, match="__unexplained"):
        assert_rows_aligned(extra, layout, oracle, context)


def test_rotated_join_record_ids_are_named() -> None:
    sources = make_sources("unique")
    joined = Join()(sources.a, sources.b)
    table = joined.as_table(all_info=True)
    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=joined.keys()[1])
    assert_rows_aligned(table, layout, sources.oracle, joined.data_context)
    record_ids = [
        c
        for c in table.column_names
        if c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
    ]
    assert len(record_ids) == 2
    for column in record_ids:
        with pytest.raises(AssertionError, match=f"column '{column}'"):
            assert_rows_aligned(
                rotated(table, column), layout, sources.oracle, joined.data_context
            )


def test_a_rotated_folded_record_id_is_named() -> None:
    sources = make_sources("unique")
    batched = Batch(2)(sources.a)
    table = batched.as_table(all_info=True)
    tag_keys, data_keys = batched.keys()
    layout = Layout(
        tag_keys=tag_keys, data_keys=data_keys, members=frozenset(tag_keys + data_keys)
    )
    checked = assert_rows_aligned(table, layout, sources.oracle, batched.data_context)
    assert_all_columns_checked(table, checked)
    (record_id,) = [
        c
        for c in table.column_names
        if c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
    ]
    with pytest.raises(AssertionError, match="folded record id"):
        assert_rows_aligned(
            rotated(table, record_id), layout, sources.oracle, batched.data_context
        )


def test_identity_comparison_ignores_row_order_but_not_cell_moves() -> None:
    table, *_ = source_table()
    reversed_rows = table.take(list(reversed(range(table.num_rows))))
    assert_same_rows_by_identity(table, reversed_rows)
    with pytest.raises(AssertionError):
        assert_same_rows_by_identity(table, rotated(table, "x"))


def test_a_null_token_beside_a_value_is_named() -> None:
    table, layout, oracle, context = source_table()
    index = table.column_names.index("_source_x")
    tokens = table.column("_source_x").to_pylist()
    tokens[0] = None
    nulled = table.set_column(
        index, table.field("_source_x"), pa.array(tokens, type=pa.large_string())
    )
    with pytest.raises(AssertionError, match="column '_source_x': null token"):
        assert_rows_aligned(nulled, layout, oracle, context)


def test_a_system_tag_that_is_no_source_id_or_record_id_fails() -> None:
    table, layout, oracle, context = source_table()
    extra = table.append_column(
        f"{constants.SYSTEM_TAG_PREFIX}unexplained", pa.array(range(table.num_rows))
    )
    with pytest.raises(AssertionError, match="no alignment rule"):
        assert_rows_aligned(extra, layout, oracle, context)


def test_rotated_function_tokens_are_named_against_the_same_run() -> None:
    """A function output's tokens are matched by record id with the run's own rows."""
    sources = make_sources("unique")
    derive_y(sources)
    stream = new_gated_scale_pod()(sources.a)
    table = stream.as_table(columns={"source": True, "system_tags": True})
    tags, data = stream.keys()
    layout = Layout(tag_keys=tuple(tags), data_keys=tuple(data))
    tokens = stream_function_tokens(stream)
    assert_rows_aligned(
        table, layout, sources.oracle, stream.data_context, function_tokens=tokens
    )
    with pytest.raises(AssertionError, match="column '_source_y': token"):
        assert_rows_aligned(
            rotated(table, "_source_y"),
            layout,
            sources.oracle,
            stream.data_context,
            function_tokens=tokens,
        )


def test_stored_records_with_a_swapped_output_id_are_named(tmp_path: Path) -> None:
    """A record whose (``__data_id``, ``_source_y``) pair is another row's is caught."""
    run = make_run("unique", "shuffled", "sync", None, "in_memory", tmp_path)
    records = run.job.nodes["scaled"].get_all_records(all_info=True)
    assert records is not None
    assert_records_aligned(run.job, run.sources, records)
    swapped = records
    for column in ("__data_id", "_source_y"):
        index = swapped.column_names.index(column)
        values = swapped.column(column).to_pylist()
        values[0], values[1] = values[1], values[0]
        swapped = swapped.set_column(
            index,
            swapped.field(column),
            pa.array(values, type=swapped.field(column).type),
        )
    with pytest.raises(AssertionError, match="column '__data_id'"):
        assert_records_aligned(run.job, run.sources, swapped)


def test_a_table_without_the_requested_provenance_fails() -> None:
    table, layout, *_ = source_table()
    with pytest.raises(AssertionError, match="no source-info columns"):
        assert_requested_columns(
            table.drop_columns(["_source_x"]), {"columns": {"source": True}}, layout
        )
    system_tags = [
        c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
    ]
    with pytest.raises(AssertionError, match="no system tag"):
        assert_requested_columns(
            table.drop_columns(system_tags), {"columns": {"system_tags": True}}, layout
        )


def test_a_table_asked_sorted_but_not_in_tag_order_fails() -> None:
    table, layout, *_ = source_table()
    ordered = table.sort_by([("g", "ascending")])
    assert_requested_columns(ordered, {"columns": {"sort_by_tags": True}}, layout)
    unsorted = ordered.take(list(reversed(range(ordered.num_rows))))
    with pytest.raises(AssertionError, match="not in tag order"):
        assert_requested_columns(unsorted, {"columns": {"sort_by_tags": True}}, layout)


def test_a_row_replaced_by_a_copy_of_another_fails_the_expected_rows() -> None:
    table, layout, oracle, context = source_table()
    expected = rows(oracle, "A")
    assert_rows_aligned(table, layout, oracle, context, expected_rows=expected)
    duplicated = table.take([0, *range(table.num_rows - 1)])
    with pytest.raises(AssertionError, match="not the expected rows"):
        assert_rows_aligned(duplicated, layout, oracle, context, expected_rows=expected)


def test_an_empty_join_fails_its_expected_rows() -> None:
    sources = make_sources("unique")
    joined = Join()(sources.a, sources.b)
    table = joined.as_table(all_info=True)
    layout = Layout(tag_keys=TAG_COLUMNS, data_keys=joined.keys()[1])
    expected = pairs(sources.oracle, "A", "B")
    assert_rows_aligned(
        table, layout, sources.oracle, joined.data_context, expected_rows=expected
    )
    with pytest.raises(AssertionError, match="no rows"):
        assert_rows_aligned(
            table.slice(0, 0),
            layout,
            sources.oracle,
            joined.data_context,
            expected_rows=expected,
        )


def test_a_batch_table_repeating_one_batch_instead_of_another_is_named() -> None:
    """Each batch row is aligned by itself; the members must still cover the input."""
    sources = make_sources("unique")
    batched = Batch(2)(sources.a)
    table = batched.as_table(columns={"source": True, "system_tags": True})
    xs = [o.values["x"] for o in sources.oracle.origins if o.source_id == "A"]
    assert_members_cover(table, "x", xs)
    repeated = table.take([0, 0, 2])
    tag_keys, data_keys = batched.keys()
    layout = Layout(
        tag_keys=tag_keys, data_keys=data_keys, members=frozenset(tag_keys + data_keys)
    )
    assert_rows_aligned(repeated, layout, sources.oracle, batched.data_context)
    with pytest.raises(AssertionError, match="are not the input rows"):
        assert_members_cover(repeated, "x", xs)
