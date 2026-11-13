"""The row-alignment checker accepts aligned tables and rejects every kind of misalignment.

A checker that passes everything proves nothing. Each test here takes a table the checker
accepts and moves the cells of one column to other rows (a rotation, the shape of ITL-1029),
and the checker must name that column.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.operators import Batch, Join
from orcapod.system_constants import constants

from .checker import (
    Layout,
    assert_all_columns_checked,
    assert_rows_aligned,
    assert_same_rows_by_identity,
)
from .fixtures import TAG_COLUMNS, make_sources
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
