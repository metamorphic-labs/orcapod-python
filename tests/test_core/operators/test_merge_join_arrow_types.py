"""MergeJoin keeps the Arrow types its inputs already have (ITL-832).

The rule: whatever Arrow types the inputs have are respected; an Arrow type is
inferred from Python values only when none is known. For ``MergeJoin`` this
means a merged column is ``list<T>`` where ``T`` is the input column's Arrow
type (not what ``pa.array`` infers from the Python values), for empty and
non-empty results alike, and every column that passes through the Polars join
unchanged keeps its input type.

Predictions are compared against ``static_process(...)``, never against
``process(...)``, whose ``output_schema`` delegates back to the operator.
"""

from __future__ import annotations

import datetime
import decimal
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pytest

from orcapod.contexts import get_default_type_converter
from orcapod.core.operators import MergeJoin, PolarsFilter
from orcapod.core.sources import CSVSource, DictSource
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.protocols.core_protocols import StreamProtocol

SCHEMA_CONFIGS: tuple[dict[str, bool], ...] = ({}, {"system_tags": True})

# (Arrow type, two distinct values in ascending order)
ELEMENT_CASES: list[tuple[pa.DataType, list[Any]]] = [
    (pa.int8(), [1, 2]),
    (pa.int16(), [1, 2]),
    (pa.int32(), [1, 2]),
    (pa.int64(), [1, 2]),
    (pa.uint8(), [1, 2]),
    (pa.float32(), [1.5, 2.5]),
    (pa.float64(), [1.5, 2.5]),
    (pa.string(), ["a", "b"]),
    (pa.large_string(), ["a", "b"]),
    (pa.binary(), [b"a", b"b"]),
    (pa.large_binary(), [b"a", b"b"]),
    (pa.bool_(), [False, True]),
    (
        pa.timestamp("ms"),
        [datetime.datetime(2020, 1, 1), datetime.datetime(2021, 1, 1)],
    ),
    (pa.date32(), [datetime.date(2020, 1, 1), datetime.date(2021, 1, 1)]),
    (pa.list_(pa.int8()), [[1], [2]]),
    (pa.large_list(pa.int32()), [[1], [2]]),
    (
        pa.decimal128(5, 2),
        [decimal.Decimal("1.00"), decimal.Decimal("2.50")],
    ),
]

# Pairs whose Arrow types disagree while their Python types agree. What
# MergeJoin should do with them is pending (ITL-835, _resolve_disagreeing_types);
# until then the tests below pin the pre-ITL-832 behaviour, so the decision
# shows up as a deliberate test change. Each entry: left type, right type,
# values, and the expected outcome keyed by (rows, argument order) for a
# colliding data column ("x" type) and for a shared tag key ("k" type, or the
# name of the exception the join raises).
_TS = [datetime.datetime(2020, 1, 1), datetime.datetime(2021, 1, 1)]
_DEC = [decimal.Decimal("1.00"), decimal.Decimal("2.50")]
DISAGREEING_PAIRS: list[
    tuple[
        pa.DataType, pa.DataType, list[Any], dict[tuple[int, str], str], dict[str, str]
    ]
] = [
    (
        pa.int8(),
        pa.int64(),
        [1, 2],
        {
            (2, "LR"): "list<item: int64>",
            (2, "RL"): "list<item: int64>",
            (0, "LR"): "list<item: int64>",
            (0, "RL"): "list<item: int64>",
        },
        {"LR": "int8", "RL": "int64"},
    ),
    (
        pa.float32(),
        pa.float64(),
        [1.5, 2.5],
        {
            (2, "LR"): "list<item: double>",
            (2, "RL"): "list<item: double>",
            (0, "LR"): "list<item: double>",
            (0, "RL"): "list<item: double>",
        },
        {"LR": "float", "RL": "double"},
    ),
    (
        pa.string(),
        pa.large_string(),
        ["a", "b"],
        {
            (2, "LR"): "list<item: string>",
            (2, "RL"): "list<item: string>",
            (0, "LR"): "list<item: string>",
            (0, "RL"): "list<item: string>",
        },
        {"LR": "large_string", "RL": "large_string"},
    ),
    (
        pa.binary(),
        pa.large_binary(),
        [b"a", b"b"],
        {
            (2, "LR"): "list<item: binary>",
            (2, "RL"): "list<item: binary>",
            (0, "LR"): "list<item: binary>",
            (0, "RL"): "list<item: binary>",
        },
        {"LR": "large_binary", "RL": "large_binary"},
    ),
    (
        pa.list_(pa.int64()),
        pa.large_list(pa.int64()),
        [[1], [2]],
        {
            (2, "LR"): "list<item: list<item: int64>>",
            (2, "RL"): "list<item: list<item: int64>>",
            (0, "LR"): "list<item: list<item: int64>>",
            (0, "RL"): "list<item: list<item: int64>>",
        },
        {"LR": "large_list<item: int64>", "RL": "large_list<item: int64>"},
    ),
    (
        pa.timestamp("ms"),
        pa.timestamp("us"),
        _TS,
        {
            (2, "LR"): "list<item: timestamp[us]>",
            (2, "RL"): "list<item: timestamp[us]>",
            (0, "LR"): "list<item: timestamp[ms]>",
            (0, "RL"): "list<item: timestamp[us]>",
        },
        {"LR": "SchemaError", "RL": "SchemaError"},
    ),
    (
        pa.decimal128(5, 2),
        pa.decimal128(7, 2),
        _DEC,
        {
            (2, "LR"): "list<item: decimal128(3, 2)>",
            (2, "RL"): "list<item: decimal128(3, 2)>",
            (0, "LR"): "list<item: decimal128(5, 2)>",
            (0, "RL"): "list<item: decimal128(7, 2)>",
        },
        {"LR": "SchemaError", "RL": "SchemaError"},
    ),
]


def _table(columns: dict[str, tuple[pa.DataType, list[Any]]]) -> pa.Table:
    """Build a table of non-nullable fields from ``{name: (type, values)}``."""
    return pa.Table.from_arrays(
        [pa.array(values, type=arrow_type) for arrow_type, values in columns.values()],
        schema=pa.schema(
            [
                pa.field(name, arrow_type, nullable=False)
                for name, (arrow_type, _) in columns.items()
            ]
        ),
    )


def _stream(
    columns: dict[str, tuple[pa.DataType, list[Any]]], tags: list[str]
) -> ArrowTableStream:
    """Build a stream (no source info, no system tags) from ``{name: (type, values)}``."""
    return ArrowTableStream(_table(columns), tag_columns=tags)


def _pair(
    left_type: pa.DataType,
    right_type: pa.DataType,
    values: list[Any],
    rows: int,
) -> tuple[ArrowTableStream, ArrowTableStream]:
    """Build two streams sharing tag ``probe`` and colliding data column ``x``.

    Each row of the left stream holds ``values[0]`` and each row of the right
    stream ``values[1]``, so every merged row is ``values`` in sorted order.
    """
    left = _stream(
        {
            "probe": (pa.int64(), list(range(rows))),
            "x": (left_type, [values[0]] * rows),
        },
        ["probe"],
    )
    right = _stream(
        {
            "probe": (pa.int64(), list(range(rows))),
            "x": (right_type, [values[1]] * rows),
        },
        ["probe"],
    )
    return left, right


def _assert_matches_prediction(
    op: MergeJoin | PolarsFilter,
    result: StreamProtocol,
    *inputs: StreamProtocol,
) -> None:
    """Assert the materialized schema equals the operator's prediction."""
    for config in SCHEMA_CONFIGS:
        assert result.output_schema(columns=config) == op.output_schema(
            *inputs, columns=config
        ), config


def _ids(value: object) -> str:
    """Name a parametrized case by its Arrow types, leaving values out."""
    return str(value) if isinstance(value, pa.DataType) else ""


class TestMergedColumnElementType:
    """A merged column is ``list<input type>`` with or without rows."""

    @pytest.mark.parametrize(("arrow_type", "values"), ELEMENT_CASES, ids=_ids)
    @pytest.mark.parametrize("rows", [0, 2])
    def test_element_type_is_the_input_type(
        self, arrow_type: pa.DataType, values: list[Any], rows: int
    ) -> None:
        """The merged list keeps the input's width and layout, and the prediction."""
        left, right = _pair(arrow_type, arrow_type, values, rows)
        op = MergeJoin()
        result = op.static_process(left, right)
        table = result.as_table()

        assert table.schema.field("x").type == pa.list_(arrow_type)
        assert table.column("x").to_pylist() == [values] * rows
        _assert_matches_prediction(op, result, left, right)

    @pytest.mark.parametrize("rows", [0, 2])
    def test_logical_element_type_uses_list_logical_type(self, rows: int) -> None:
        """A logical (extension) element yields the converter's ``list[...]`` type."""
        converter = get_default_type_converter()
        path_type = converter.python_type_to_arrow_type(Path)
        assert isinstance(path_type, pa.ExtensionType)

        def make(value: str) -> ArrowTableStream:
            """Build a stream with a ``Path``-typed column ``x`` holding ``value``."""
            storage = pa.array([value] * rows, type=path_type.storage_type)
            table = pa.Table.from_arrays(
                [
                    pa.array(list(range(rows)), type=pa.int64()),
                    pa.ExtensionArray.from_storage(path_type, storage),
                ],
                schema=pa.schema(
                    [
                        pa.field("probe", pa.int64(), nullable=False),
                        pa.field("x", path_type, nullable=False),
                    ]
                ),
            )
            return ArrowTableStream(table, tag_columns=["probe"])

        left, right = make("/b"), make("/a")
        op = MergeJoin()
        result = op.static_process(left, right)
        table = result.as_table()

        assert table.schema.field("x").type == converter.python_type_to_arrow_type(
            list[Path]
        )
        assert len(table) == rows
        _assert_matches_prediction(op, result, left, right)

    @pytest.mark.parametrize("rows", [0, 2])
    def test_empty_and_non_empty_results_concatenate(self, rows: int) -> None:
        """Empty and non-empty results share one schema, so they concatenate."""
        tables = [
            MergeJoin()
            .static_process(*_pair(pa.int8(), pa.int8(), [1, 2], n))
            .as_table()
            for n in (0, rows)
        ]
        assert len(pa.concat_tables(tables)) == rows


def _ordered(
    left: StreamProtocol, right: StreamProtocol, order: str
) -> tuple[StreamProtocol, StreamProtocol]:
    """Return the two streams in argument order ``"LR"`` or ``"RL"``."""
    return (left, right) if order == "LR" else (right, left)


class TestDisagreeingArrowTypes:
    """Pin MergeJoin's behaviour when the inputs' Arrow types disagree.

    The decision is pending (ITL-835): these pin what MergeJoin did before
    ITL-832, so changing ``_resolve_disagreeing_types`` must change them too.
    Validation passes for every pair, because the Python types agree.
    """

    @pytest.mark.parametrize(
        ("left_type", "right_type", "values", "data_expected", "tag_expected"),
        DISAGREEING_PAIRS,
        ids=_ids,
    )
    @pytest.mark.parametrize("rows", [0, 2])
    @pytest.mark.parametrize("order", ["LR", "RL"])
    def test_colliding_data_column(
        self,
        left_type: pa.DataType,
        right_type: pa.DataType,
        values: list[Any],
        data_expected: dict[tuple[int, str], str],
        tag_expected: dict[str, str],
        rows: int,
        order: str,
    ) -> None:
        """A colliding column's merged type is inferred, as before ITL-832."""
        del tag_expected
        first, second = _ordered(*_pair(left_type, right_type, values, rows), order)
        op = MergeJoin()
        op.validate_inputs(first, second)
        table = op.static_process(first, second).as_table()
        assert str(table.schema.field("x").type) == data_expected[(rows, order)]

    @pytest.mark.parametrize(
        ("left_type", "right_type", "values", "data_expected", "tag_expected"),
        DISAGREEING_PAIRS,
        ids=_ids,
    )
    @pytest.mark.parametrize("rows", [0, 2])
    @pytest.mark.parametrize("order", ["LR", "RL"])
    def test_shared_tag_key(
        self,
        left_type: pa.DataType,
        right_type: pa.DataType,
        values: list[Any],
        data_expected: dict[tuple[int, str], str],
        tag_expected: dict[str, str],
        rows: int,
        order: str,
    ) -> None:
        """A shared tag key keeps the joined type, or the Polars join raises."""
        del data_expected
        left = _stream(
            {"k": (left_type, [values[0]] * rows), "p": (pa.int64(), [0] * rows)},
            ["k"],
        )
        right = _stream(
            {"k": (right_type, [values[0]] * rows), "q": (pa.int64(), [0] * rows)},
            ["k"],
        )
        first, second = _ordered(left, right, order)
        op = MergeJoin()
        op.validate_inputs(first, second)
        expected = tag_expected[order]
        if expected == "SchemaError":
            with pytest.raises(pl.exceptions.SchemaError):
                op.static_process(first, second).as_table()
        else:
            table = op.static_process(first, second).as_table()
            assert str(table.schema.field("k").type) == expected
            assert len(table) == rows * rows

    def test_csv_and_dict_sources_on_a_string_column(self, tmp_path: Path) -> None:
        """A CSV ``string`` column colliding with a dict ``large_string`` one merges.

        This is the common real-world disagreement: today the merged column is
        inferred as ``list<string>`` and the join succeeds.
        """
        csv_path = tmp_path / "labels.csv"
        csv_path.write_text("id,label\n1,a\n2,b\n")
        csv_source = CSVSource(str(csv_path), tag_columns=["id"])
        dict_source = DictSource(
            [{"id": 1, "label": "x"}, {"id": 2, "label": "y"}], tag_columns=["id"]
        )
        assert csv_source.as_table().schema.field("label").type == pa.string()
        assert dict_source.as_table().schema.field("label").type == pa.large_string()

        table = MergeJoin()(csv_source, dict_source).as_table()
        assert table.schema.field("label").type == pa.list_(pa.string())
        labels = table.sort_by("id").column("label").to_pylist()
        assert labels == [["a", "x"], ["b", "y"]]


def _sources(rows: int) -> tuple[StreamProtocol, StreamProtocol]:
    """Build two sources with source info and system tags; ``rows=0`` filters all.

    The left source has a ``string`` tag ``probe``, a colliding ``int8``
    column ``x`` and pass-through columns of several layouts; the right
    source has the same tag as ``large_string``.
    """
    struct_type = pa.struct([("a", pa.int8()), ("b", pa.string())])
    left = ArrowTableSource(
        _table(
            {
                "probe": (pa.string(), ["p0", "p1"]),
                "x": (pa.int8(), [1, 2]),
                "s": (pa.string(), ["a", "b"]),
                "b": (pa.binary(), [b"a", b"b"]),
                "l": (pa.list_(pa.int8()), [[1], [2]]),
                "st": (struct_type, [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]),
            }
        ),
        tag_columns=["probe"],
    )
    right = ArrowTableSource(
        _table(
            {
                "probe": (pa.large_string(), ["p0", "p1"]),
                "x": (pa.int8(), [3, 0]),
                "f": (pa.float32(), [1.0, 2.0]),
            }
        ),
        tag_columns=["probe"],
    )
    if rows:
        return left, right
    drop_all = PolarsFilter(predicates=[pl.lit(False)])
    return drop_all.static_process(left), drop_all.static_process(right)


class TestPassThroughColumns:
    """Columns the join does not merge keep their input Arrow types."""

    @pytest.mark.parametrize("rows", [0, 2])
    def test_merge_join_keeps_pass_through_types(self, rows: int) -> None:
        """Data, source-info and system-tag columns keep their types; tags promote."""
        left, right = _sources(rows)
        op = MergeJoin()
        result = op.static_process(left, right)
        table = result.as_table(all_info=True)
        left_schema = left.as_table(all_info=True).schema
        right_schema = right.as_table(all_info=True).schema

        for name in ("s", "b", "l", "st", "_source_s", "_source_b"):
            assert table.schema.field(name).type == left_schema.field(name).type, name
        assert table.schema.field("f").type == pa.float32()
        assert (
            table.schema.field("_source_f").type == right_schema.field("_source_f").type
        )
        assert table.schema.field("probe").type == pa.large_string()
        assert table.schema.field("x").type == pa.list_(pa.int8())

        input_system_tag_types = {
            f.type for f in (*left_schema, *right_schema) if f.name.startswith("_tag")
        }
        output_system_tag_types = {
            f.type for f in table.schema if f.name.startswith("_tag")
        }
        assert output_system_tag_types == input_system_tag_types
        assert len(table) == rows
        if rows:
            # The empty inputs come from PolarsFilter, which drops non-null
            # flags, so their prediction differs in nullability only (ITL-820).
            _assert_matches_prediction(op, result, left, right)

    def test_merge_join_merged_values_and_sources_stay_sorted(self) -> None:
        """Merged values are sorted per row and their source tokens follow them."""
        left, right = _sources(2)
        table = MergeJoin().static_process(left, right).as_table(all_info=True)
        rows = table.sort_by("probe").to_pylist()
        assert [row["x"] for row in rows] == [[1, 3], [0, 2]]
        left_tokens = left.as_table(all_info=True).column("_source_x").to_pylist()
        right_tokens = right.as_table(all_info=True).column("_source_x").to_pylist()
        assert [row["_source_x"] for row in rows] == [
            [left_tokens[0], right_tokens[0]],
            [right_tokens[1], left_tokens[1]],
        ]

    def test_shared_tag_type_is_commutative(self) -> None:
        """A shared tag the inputs disagree on keeps the joined type in both orders.

        No promotion and no rejection for tag keys: the type is what the join
        produced before ITL-832 (``large_string``), whichever input comes first.
        """
        left, right = _sources(2)
        op = MergeJoin()
        forward = op.static_process(left, right).as_table(all_info=True)
        backward = op.static_process(right, left).as_table(all_info=True)
        assert forward.schema.field("probe").type == pa.large_string()
        # Column order depends on the argument order; types must not.
        assert {f.name: f.type for f in forward.schema} == {
            f.name: f.type for f in backward.schema
        }

    def test_polars_filter_keeps_input_types(self) -> None:
        """Filtering keeps every column's Arrow type, system tags included."""
        source, _ = _sources(2)
        op = PolarsFilter(predicates=[pl.col("x") > 1])
        result = op.static_process(source)
        input_schema = source.as_table(all_info=True).schema
        output_schema = result.as_table(all_info=True).schema

        assert len(result.as_table()) == 1
        for field in output_schema:
            assert field.type == input_schema.field(field.name).type, field.name


def _row_hashes(stream: StreamProtocol) -> list[tuple[str, str]]:
    """Return each row's (tag content hash, data content hash)."""
    return [
        (tag.content_hash().to_hex(), data.content_hash().to_hex())
        for tag, data in stream.iter_data()
    ]


def _in_pre_fix_layout(stream: StreamProtocol) -> ArrowTableStream:
    """Rebuild ``stream`` with the column types the code before ITL-832 emitted.

    Before the fix, ``MergeJoin`` and ``PolarsFilter`` kept the Arrow types
    Polars produced (large layouts) while setting nullability exactly as they
    still do. So this swaps in the types of a Polars round-trip and keeps every
    field's nullability, isolating the type change the fix makes.
    """
    table = stream.as_table(columns={"source": True, "system_tags": True})
    polars_schema = pl.DataFrame(table).to_arrow().schema
    pre_fix_schema = pa.schema(
        [
            field.with_type(polars_schema.field(field.name).type)
            for field in table.schema
        ]
    )
    tag_columns, _ = stream.keys()
    return ArrowTableStream(table.cast(pre_fix_schema), tag_columns=tag_columns)


def _string_like_types(stream: StreamProtocol) -> dict[str, pa.DataType]:
    """Return the Arrow types of the pass-through columns ``s``, ``b``, ``l``, ``st``."""
    schema = stream.as_table().schema
    return {name: schema.field(name).type for name in ("s", "b", "l", "st")}


class TestPassThroughContentHashes:
    """Restoring pass-through layouts leaves every row's content hash unchanged.

    The developer approved the pass-through fix on the condition that content
    hashes do not change. The Arrow hasher ignores layout (``string`` versus
    ``large_string``, ``list`` versus ``large_list``), so the rows must hash the
    same in the restored layout and in the pre-fix (Polars) layout.
    """

    def test_polars_filter_row_hashes_unchanged(self) -> None:
        """Filter output rows hash exactly as they did before the fix."""
        source, _ = _sources(2)
        fixed = PolarsFilter(predicates=[pl.lit(True)]).static_process(source)
        pre_fix = _in_pre_fix_layout(fixed)

        assert _string_like_types(fixed) != _string_like_types(pre_fix)
        assert _row_hashes(fixed) == _row_hashes(pre_fix)

    def test_merge_join_row_hashes_unchanged(self) -> None:
        """MergeJoin output with pass-through columns hashes as before the fix."""
        left, right = _sources(2)
        fixed = MergeJoin().static_process(left, right)
        pre_fix = _in_pre_fix_layout(fixed)

        assert _string_like_types(fixed) != _string_like_types(pre_fix)
        assert _row_hashes(fixed) == _row_hashes(pre_fix)
