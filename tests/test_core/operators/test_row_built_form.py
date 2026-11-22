"""F, the row-built form of a table, equals the table built from its rows (ITL-1347, L0).

``StaticOutputOperatorPod._row_built_form`` (F in the spec,
``docs/dev/specs/itl-1347-job-table-transport.md``) gives a node's output table as
``_rows_to_table`` (R) builds it from the node's rows, without making the rows. Every
operator, column role and arrangement of the async-sync matrix is checked: the column
names and their order, every field (type, nullability, field metadata), the schema
metadata, and every value (floats by their bits).

Two properties each case covers by construction:

- A data-tag's source info comes first among the source info, in tag order, whatever
  its column's place in the table (``source_mid``, ``tag_by``): a row lists its source
  info in the order of its data keys.
- A system-tag field is nullable only when a value is null: a join over inputs tied on
  pipeline hash (``ref_join_3_tied``) declares its system tags nullable, a table built
  from rows does not.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod.core.operators import DropNulls, SelectDataColumns
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.streams import ArrowTableStream
from orcapod.core.streams.base import ZERO_ROW_INPUT_COLUMNS, row_built_columns
from orcapod.system_constants import constants
from tests.test_core.operators.test_async_sync_equivalence import (
    AFTER_OPERATOR_CASES,
    CASES,
    FUNCTION_POD_CASES,
    Case,
)


_FLOAT_BITS = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}


def _as_bits(array: Any) -> Any:  # pyright: ignore[reportExplicitAny]  # any Arrow array kind
    """``array``'s values, with every float, at any depth, as its bit pattern.

    Arrow's ``equals`` holds NaN unequal to itself, so floats are compared as unsigned
    integers of their width. Lists and structs are taken apart into their validity, their
    lengths and their children's values, so the physical layout (a list's first offset,
    the bytes behind a null) does not count.
    """
    kind = array.type
    if isinstance(array, pa.ExtensionArray):
        return ("extension", _as_bits(array.storage))
    if pa.types.is_floating(kind):
        filled = pc.if_else(pc.is_null(array), pa.scalar(0, kind), array)
        return ("float", array.is_null(), filled.view(_FLOAT_BITS[kind.bit_width]))
    if pa.types.is_map(kind):
        return (
            "map",
            array.is_null(),
            pc.list_value_length(array),
            _as_bits(array.keys),
            _as_bits(array.items),
        )
    if (
        pa.types.is_list(kind)
        or pa.types.is_large_list(kind)
        or pa.types.is_fixed_size_list(kind)
    ):
        return (
            "list",
            array.is_null(),
            pc.list_value_length(array),
            _as_bits(array.flatten()),
        )
    if pa.types.is_struct(kind):
        return (
            "struct",
            array.is_null(),
            tuple(_as_bits(child) for child in array.flatten()),
        )
    return ("value", array)


def assert_same_values(formed: pa.Table, built: pa.Table) -> None:
    """Every column of ``formed`` holds the values of ``built``, floats by their bits.

    The physical layout (a list's first offset, the bytes behind a null) is not a
    value and may differ.
    """
    for name in built.column_names:
        expected, got = built.column(name), formed.column(name)
        assert got.type == expected.type, name
        assert _as_bits(got.combine_chunks()) == _as_bits(expected.combine_chunks()), (
            name
        )


def _all_cases() -> list[Any]:
    cases: list[Any] = [pytest.param(c, id=c.label) for c in CASES]
    cases += list(AFTER_OPERATOR_CASES)
    cases += [pytest.param(c, id=f"fp-{c.label}") for c in FUNCTION_POD_CASES]
    return cases


@pytest.mark.parametrize("case", _all_cases())
def test_row_built_form_equals_the_table_built_from_rows(case: Case) -> None:
    """F of an operator's result table equals R of its rows, field and bit for bit."""
    result = case.make_op()(*case.make_sources())
    rows = list(result.iter_data())
    if not rows:
        pytest.skip("no rows: F is defined on a table with rows")
    built, _, _ = StaticOutputOperatorPod._rows_to_table(rows)  # pyright: ignore[reportPrivateUsage]  # R, the oracle
    formed = StaticOutputOperatorPod._row_built_form(  # pyright: ignore[reportPrivateUsage]  # F, under test
        result.as_table(columns=ZERO_ROW_INPUT_COLUMNS), result.keys()
    )
    assert formed.column_names == built.column_names
    assert formed.schema.equals(built.schema, check_metadata=True)
    assert_same_values(formed, built)


def test_row_built_columns_is_the_same_on_every_slice() -> None:
    """The row-by-row steps of F give the same columns on a slice as on the whole."""
    case = next(c for c in CASES if c.label == "ref_join_3_tied")
    result = case.make_op()(*case.make_sources())
    table = result.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
    keys = result.keys()
    whole = row_built_columns(table, keys)
    for start in range(table.num_rows):
        part = row_built_columns(table.slice(start, 1), keys)
        assert part.schema.equals(whole.schema, check_metadata=True)
        assert part.equals(whole.slice(start, 1))


def _raw_stream() -> ArrowTableStream:
    """A raw stream with provenance fields no source writes (Codex on PR #483).

    A system-tag field with field metadata, a list-valued source-info column whose item
    field is named ``member`` and non-nullable, and a meta (``__``) data column with
    its own source info.
    """
    system_tag = f"{constants.SYSTEM_TAG_PREFIX}source_id::abc"
    member = pa.field("member", pa.large_string(), nullable=False)
    table = pa.table(
        {
            "k": pa.array([1, 2], pa.int64()),
            "j": pa.array(["a", "b"], pa.large_string()),
            system_tag: pa.array(["s1", "s2"], pa.large_string()),
            "x": pa.array([1.0, float("nan")]),
            "z": pa.array([3, None], pa.int8()),
            "__note": pa.array(["a", "b"]),
            "_source_x": pa.array([["a", "b"], ["c"]], pa.large_list(member)),
            "_source_z": pa.array(["t1", "t2"], pa.large_string()),
            "_source___note": pa.array(["n1", "n2"], pa.large_string()),
        }
    )
    position = table.schema.get_field_index(system_tag)
    schema = table.schema.set(
        position, table.schema.field(position).with_metadata({b"m": b"v"})
    )
    return ArrowTableStream(
        pa.Table.from_arrays(table.columns, schema=schema), tag_columns=["k", "j"]
    )


@pytest.mark.parametrize(
    "make",
    [
        pytest.param(_raw_stream, id="raw"),
        pytest.param(
            lambda: SelectDataColumns(["z", "x"])(_raw_stream()), id="select-z-x"
        ),
        pytest.param(lambda: DropNulls(["j", "k"])(_raw_stream()), id="drop-nulls"),
    ],
)
def test_row_built_form_of_a_raw_stream(make: Any) -> None:
    """F equals R on a raw stream: provenance fields as rows build them, no meta.

    R takes a system tag and a source-info token from its values, so the system-tag
    field has no metadata and a list token's item field is nullable ``item``; a meta
    column is not data, so neither it nor its source info is passed on.
    """
    stream = make()
    built, _, _ = StaticOutputOperatorPod._rows_to_table(list(stream.iter_data()))  # pyright: ignore[reportPrivateUsage]  # R, the oracle
    formed = StaticOutputOperatorPod._row_built_form(  # pyright: ignore[reportPrivateUsage]  # F, under test
        stream.as_table(columns=ZERO_ROW_INPUT_COLUMNS), stream.keys()
    )
    assert "__note" not in formed.column_names
    assert "_source___note" not in formed.column_names
    assert formed.column_names == built.column_names
    assert formed.schema.equals(built.schema, check_metadata=True)
    assert_same_values(formed, built)
