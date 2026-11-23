"""The mask kernel of ``Filter`` (ITL-1378): every term kind on Arrow tables."""

from __future__ import annotations

import struct
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import pytest

from orcapod.core.operators.filter_mask import (
    AllOf,
    AnyOf,
    ColumnRef,
    FieldRef,
    NotOf,
    Term,
    check_against_schema,
    condition_mask,
    filter_table,
)
from orcapod.errors import InputValidationError

NAN = float("nan")


def c(name: str) -> ColumnRef:
    return ColumnRef(name)


def mask(table: pa.Table, node) -> list[bool | None]:
    return condition_mask(table, node).to_pylist()


@pytest.fixture
def units() -> pa.Table:
    return pa.table(
        {
            "session": pa.array(
                ["s1", "s1", "s1", "s2", "s2", "s3"], pa.large_string()
            ),
            "unit": pa.array([3, 7, 9, 3, 7, 3], pa.int64()),
            "t": pa.array([0.5, 1.2, 2.0, 0.7, NAN, 3.1], pa.float64()),
            "rate": pa.array([4.0, None, 2.5, 6.1, 0.0, None], pa.float64()),
            "probe": pa.array(
                ["imec0", "imec1", "imec0", "imec1", "imec0", "imec1"],
                pa.large_string(),
            ),
        }
    )


def kept(table: pa.Table, node) -> list[tuple[str, int]]:
    out = filter_table(table, node)
    return list(zip(out.column("session").to_pylist(), out.column("unit").to_pylist()))


class TestSpecExamples:
    """The table of section 1 of the design."""

    def test_equality(self, units):
        assert kept(units, Term(c("session"), "==", "s1")) == [
            ("s1", 3),
            ("s1", 7),
            ("s1", 9),
        ]

    def test_several_equalities(self, units):
        node = AllOf((Term(c("session"), "==", "s1"), Term(c("probe"), "==", "imec0")))
        assert kept(units, node) == [("s1", 3), ("s1", 9)]

    def test_is_in(self, units):
        assert kept(units, Term(c("unit"), "is_in", (3, 7))) == [
            ("s1", 3),
            ("s1", 7),
            ("s2", 3),
            ("s2", 7),
            ("s3", 3),
        ]

    def test_comparison_keeps_nan(self, units):
        assert kept(units, Term(c("t"), ">=", 1.0)) == [
            ("s1", 7),
            ("s1", 9),
            ("s2", 7),
            ("s3", 3),
        ]

    def test_between(self, units):
        node = AllOf((Term(c("t"), ">=", 0.5), Term(c("t"), "<=", 2.0)))
        assert kept(units, node) == [("s1", 3), ("s1", 7), ("s1", 9), ("s2", 3)]

    def test_null_tests(self, units):
        assert kept(units, Term(c("rate"), "is_null", None)) == [("s1", 7), ("s3", 3)]
        assert len(kept(units, Term(c("rate"), "is_not_null", None))) == 4

    def test_negation(self, units):
        assert kept(units, NotOf(Term(c("unit"), "is_in", (3, 7)))) == [("s1", 9)]

    def test_or(self, units):
        node = AnyOf((Term(c("session"), "==", "s3"), Term(c("t"), "<", 0.6)))
        assert kept(units, node) == [("s1", 3), ("s3", 3)]

    def test_mixed(self, units):
        node = AllOf((Term(c("t"), ">=", 1.0), Term(c("session"), "==", "s1")))
        assert kept(units, node) == [("s1", 7), ("s1", 9)]


class TestKleene:
    @pytest.fixture
    def t(self) -> pa.Table:
        return pa.table({"x": pa.array([1, None, 3], pa.int64())})

    def test_comparison_with_null_is_unknown(self, t):
        assert mask(t, Term(c("x"), "==", 1)) == [True, None, False]
        assert mask(t, Term(c("x"), "!=", 1)) == [False, None, True]
        assert mask(t, Term(c("x"), "is_in", (1,))) == [True, None, False]

    def test_not_of_unknown_is_unknown(self, t):
        assert mask(t, NotOf(Term(c("x"), ">", 1))) == [True, None, False]

    def test_and_or(self, t):
        unknown_or_false = Term(c("x"), "==", 1)
        false_everywhere = Term(c("x"), "==", 5)
        true_where_valid = Term(c("x"), ">=", 0)
        assert mask(t, AllOf((unknown_or_false, false_everywhere))) == [
            False,
            None,
            False,
        ]
        assert mask(t, AllOf((unknown_or_false, Term(c("x"), "is_null", None)))) == [
            False,
            None,
            False,
        ]
        assert mask(
            t, AllOf((Term(c("x"), "is_not_null", None), unknown_or_false))
        ) == [
            True,
            False,
            False,
        ]
        assert mask(t, AnyOf((unknown_or_false, true_where_valid))) == [
            True,
            None,
            True,
        ]
        assert mask(t, AnyOf((Term(c("x"), "is_null", None), true_where_valid))) == [
            True,
            True,
            True,
        ]

    def test_null_dropped_and_kept_by_saying_so(self):
        t = pa.table({"rate": pa.array([4.0, None, 0.5])})
        assert filter_table(t, Term(c("rate"), "!=", 4.0)).column(0).to_pylist() == [
            0.5
        ]
        node = AnyOf((Term(c("rate"), "<=", 1.0), Term(c("rate"), "is_null", None)))
        assert filter_table(t, node).column(0).to_pylist() == [None, 0.5]


class TestFloatRule:
    @pytest.fixture
    def t(self) -> pa.Table:
        return pa.table(
            {"x": pa.array([1.0, NAN, -0.0, 0.0, float("inf"), None], pa.float64())}
        )

    def test_equality(self, t):
        assert mask(t, Term(c("x"), "==", NAN)) == [
            False,
            True,
            False,
            False,
            False,
            None,
        ]
        assert mask(t, Term(c("x"), "==", 0.0)) == [
            False,
            False,
            True,
            True,
            False,
            None,
        ]
        assert mask(t, Term(c("x"), "==", -0.0)) == mask(t, Term(c("x"), "==", 0.0))
        assert mask(t, Term(c("x"), "!=", NAN)) == [
            True,
            False,
            True,
            True,
            True,
            None,
        ]

    def test_is_in(self, t):
        assert mask(t, Term(c("x"), "is_in", (NAN, -0.0))) == [
            False,
            True,
            True,
            True,
            False,
            None,
        ]

    @pytest.mark.parametrize(
        "op,value,expected",
        [
            (">=", 1.0, [True, True, False, False, True, None]),
            (">", 1.0, [False, True, False, False, True, None]),
            ("<", 1.0, [False, False, True, True, False, None]),
            ("<=", 1.0, [True, False, True, True, False, None]),
            ("<", NAN, [True, False, True, True, True, None]),
            ("<=", NAN, [True, True, True, True, True, None]),
            (">", NAN, [False, False, False, False, False, None]),
            (">=", NAN, [False, True, False, False, False, None]),
            (">", float("inf"), [False, True, False, False, False, None]),
            ("<", 0.0, [False, False, False, False, False, None]),
        ],
    )
    def test_order_nan_greatest(self, t, op, value, expected):
        assert mask(t, Term(c("x"), op, value)) == expected

    def test_float32_rounds_literal(self):
        stored = struct.unpack("f", struct.pack("f", 0.1))[0]
        t = pa.table({"x": pa.array([stored, 0.2], pa.float32())})
        assert mask(t, Term(c("x"), "<=", 0.1)) == [True, False]
        assert mask(t, Term(c("x"), "==", 0.1)) == [True, False]

    def test_float32_out_of_range_refused(self):
        t = pa.table({"x": pa.array([1.0], pa.float32())})
        with pytest.raises(InputValidationError, match="beyond its range"):
            condition_mask(t, Term(c("x"), "<", 1e300))
        assert mask(t, Term(c("x"), "<", float("inf"))) == [True]

    def test_float16(self):
        t = pa.table({"x": pa.array([1.5, 2.0], pa.float32()).cast(pa.float16())})
        assert mask(t, Term(c("x"), ">", 1.6)) == [False, True]
        with pytest.raises(InputValidationError, match="beyond its range"):
            condition_mask(t, Term(c("x"), "<", 1e6))


class TestLiterals:
    @pytest.mark.parametrize("arrow_type", [pa.uint8(), pa.uint32(), pa.int64()])
    def test_int_on_every_width(self, arrow_type):
        t = pa.table({"x": pa.array([7, 8], arrow_type)})
        assert mask(t, Term(c("x"), "==", 7)) == [True, False]
        assert mask(t, Term(c("x"), "is_in", (7, 9))) == [True, False]

    def test_out_of_range_refused(self):
        with pytest.raises(InputValidationError, match="outside its range"):
            condition_mask(
                pa.table({"x": pa.array([1], pa.uint32())}), Term(c("x"), "==", -1)
            )
        with pytest.raises(InputValidationError, match="outside its range"):
            condition_mask(
                pa.table({"x": pa.array([1], pa.int8())}), Term(c("x"), "==", 300)
            )
        with pytest.raises(InputValidationError, match="outside its range"):
            condition_mask(
                pa.table({"x": pa.array([1], pa.int8())}),
                Term(c("x"), "is_in", (1, 300)),
            )

    @pytest.mark.parametrize(
        "arrow_type,value",
        [
            (pa.float64(), 7),
            (pa.int64(), True),
            (pa.int64(), "7"),
            (pa.int64(), 7.0),
            (pa.date32(), "2026-01-01"),
            (pa.date32(), datetime(2026, 1, 1)),
            (pa.bool_(), 1),
        ],
    )
    def test_kind_mismatch_refused(self, arrow_type, value):
        t = pa.table({"x": pa.array([], arrow_type)})
        with pytest.raises(InputValidationError, match="converts no value"):
            condition_mask(t, Term(c("x"), "==", value))

    def test_strings_and_bytes_order(self):
        t = pa.table(
            {
                "s": pa.array(["a", "B", "b"], pa.string()),
                "b": pa.array([b"\x00", b"\xff", b"a"], pa.binary()),
            }
        )
        assert mask(t, Term(c("s"), "<", "a")) == [False, True, False]
        assert mask(t, Term(c("b"), ">", b"a")) == [False, True, False]
        assert mask(t, Term(c("s"), "is_in", ("b", "a"))) == [True, False, True]

    def test_dates(self):
        t = pa.table({"d": pa.array([date(2026, 1, 1), date(2026, 2, 1)], pa.date32())})
        assert mask(t, Term(c("d"), ">=", date(2026, 1, 15))) == [False, True]
        t64 = t.cast(pa.schema([("d", pa.date64())]))
        assert mask(t64, Term(c("d"), "==", date(2026, 2, 1))) == [False, True]

    def test_aware_timestamps_compare_instants(self):
        utc = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
        t = pa.table(
            {"ts": pa.array([utc, utc + timedelta(hours=1)], pa.timestamp("us", "UTC"))}
        )
        plus_two = timezone(timedelta(hours=2))
        assert mask(
            t, Term(c("ts"), "==", datetime(2026, 1, 1, 14, tzinfo=plus_two))
        ) == [
            True,
            False,
        ]
        with pytest.raises(InputValidationError, match="naive"):
            condition_mask(t, Term(c("ts"), "==", datetime(2026, 1, 1, 12)))

    def test_naive_timestamps(self):
        t = pa.table({"ts": pa.array([datetime(2026, 1, 1, 12)], pa.timestamp("ns"))})
        assert mask(t, Term(c("ts"), "==", datetime(2026, 1, 1, 12))) == [True]
        with pytest.raises(InputValidationError, match="aware"):
            condition_mask(
                t, Term(c("ts"), "==", datetime(2026, 1, 1, 12, tzinfo=timezone.utc))
            )

    def test_sub_unit_literal_refused(self):
        t = pa.table({"ts": pa.array([datetime(2026, 1, 1)], pa.timestamp("s"))})
        assert mask(t, Term(c("ts"), "==", datetime(2026, 1, 1))) == [True]
        with pytest.raises(InputValidationError, match="below the column's unit"):
            condition_mask(t, Term(c("ts"), "<", datetime(2026, 1, 1, 0, 0, 0, 1)))

    def test_bool_equality_but_no_order(self):
        t = pa.table({"b": pa.array([True, False, None])})
        assert mask(t, Term(c("b"), "==", True)) == [True, False, None]
        with pytest.raises(InputValidationError, match="no order"):
            condition_mask(t, Term(c("b"), "<", True))

    def test_unsupported_types_take_null_tests_only(self):
        t = pa.table(
            {
                "l": pa.array([[1], None], pa.large_list(pa.int64())),
                "d": pa.array([1, 2], pa.decimal128(5, 0)),
            }
        )
        assert mask(t, Term(c("l"), "is_null", None)) == [False, True]
        for name in ("l", "d"):
            with pytest.raises(InputValidationError, match="only is_null"):
                condition_mask(t, Term(c(name), "==", 1))

    def test_missing_column(self):
        with pytest.raises(InputValidationError, match="no column 'y'"):
            condition_mask(pa.table({"x": [1]}), Term(c("y"), "==", 1))


class TestStructFields:
    @pytest.fixture
    def t(self) -> pa.Table:
        info = pa.struct(
            [
                ("kind", pa.large_string()),
                ("depth", pa.struct([("um", pa.float64())])),
            ]
        )
        return pa.table(
            {
                "info": pa.array(
                    [
                        {"kind": "good", "depth": {"um": 10.0}},
                        {"kind": "noise", "depth": {"um": 20.0}},
                        None,
                        {"kind": None, "depth": None},
                    ],
                    info,
                )
            }
        )

    def test_field(self, t):
        kind = FieldRef(c("info"), "kind")
        assert mask(t, Term(kind, "!=", "noise")) == [True, False, None, None]
        assert mask(t, Term(kind, "is_null", None)) == [False, False, True, True]

    def test_field_of_field(self, t):
        um = FieldRef(FieldRef(c("info"), "depth"), "um")
        assert mask(t, Term(um, ">", 15.0)) == [False, True, None, None]

    def test_missing_field_lists_fields(self, t):
        with pytest.raises(InputValidationError, match="kind: large_string"):
            condition_mask(t, Term(FieldRef(c("info"), "kinds"), "==", "x"))

    def test_field_of_non_struct(self):
        with pytest.raises(InputValidationError, match="is not a struct"):
            condition_mask(pa.table({"x": [1]}), Term(FieldRef(c("x"), "f"), "==", 1))


class TestColumnAgainstColumn:
    def test_same_type(self):
        t = pa.table(
            {
                "a": pa.array([1.0, NAN, 2.0, None, NAN, -0.0]),
                "b": pa.array([2.0, 1.0, 2.0, 1.0, NAN, 0.0]),
            }
        )
        assert mask(t, Term(c("a"), "<", c("b"))) == [
            True,
            False,
            False,
            None,
            False,
            False,
        ]
        assert mask(t, Term(c("a"), "<=", c("b"))) == [
            True,
            False,
            True,
            None,
            True,
            True,
        ]
        assert mask(t, Term(c("a"), "==", c("b"))) == [
            False,
            False,
            True,
            None,
            True,
            True,
        ]
        assert mask(t, Term(c("b"), ">", c("a"))) == mask(t, Term(c("a"), "<", c("b")))

    def test_null_never_greater_than_nan(self):
        t = pa.table({"a": pa.array([None], pa.float64()), "b": pa.array([NAN])})
        assert mask(t, Term(c("a"), "<=", c("b"))) == [None]

    def test_layouts_of_one_type(self):
        t = pa.table(
            {
                "a": pa.array(["x", "y"], pa.string()),
                "b": pa.array(["x", "z"], pa.large_string()),
            }
        )
        assert mask(t, Term(c("a"), "==", c("b"))) == [True, False]

    def test_different_types_refused(self):
        t = pa.table({"a": pa.array([1], pa.int32()), "b": pa.array([1], pa.int64())})
        with pytest.raises(InputValidationError, match="one type"):
            condition_mask(t, Term(c("a"), "==", c("b")))


def test_check_against_schema_reads_no_rows():
    schema = pa.schema([("x", pa.int8())])
    check_against_schema(schema, Term(c("x"), "==", 3))
    with pytest.raises(InputValidationError):
        check_against_schema(schema, Term(c("x"), "==", 300))


def test_values_pass_through_bit_for_bit():
    # A float32 signalling NaN, built from its bits (a Python float would quiet it).
    values = pa.Array.from_buffers(
        pa.float32(),
        2,
        [None, pa.py_buffer(bytes.fromhex("0100807f") + struct.pack("<f", 1.0))],
    )
    t = pa.table(
        {
            "k": pa.array([1, 2], pa.int64()),
            "v": values,
            "time": pa.array([1, 2], pa.time64("ns")),
        }
    )
    out = filter_table(t, Term(c("k"), "is_in", (1, 2)))
    assert out.schema == t.schema
    assert out.column("time").equals(t.column("time"))
    assert out.column("v").chunk(0).buffers()[1].to_pybytes()[:4] == bytes.fromhex(
        "0100807f"
    )
