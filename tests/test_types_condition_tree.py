"""The ``Condition`` tree built with ``col`` (ITL-1374): terms, checks, canonical form.

The canonical form is the identity a ``Filter`` hashes: typed values, no order counting.
"""

from __future__ import annotations

import datetime as dt
import json
import math
from collections.abc import Callable
from zoneinfo import ZoneInfo

import numpy as np
import pyarrow as pa
import pytest

import orcapod
from orcapod.types import ColumnRef, Condition, col
from orcapod.types.condition import (
    AllOf,
    AnyOf,
    ColumnOperand,
    FieldPath,
    Negation,
    Term,
    check_condition,
    check_condition_columns,
    check_equality_conditions,
    condition_value_type,
    field_arrow_type,
)

_NEW_YORK = ZoneInfo("America/New_York")
_AWARE = dt.datetime(2026, 11, 20, 14, 30, tzinfo=dt.timezone.utc)
_NAIVE = dt.datetime(2026, 11, 20, 9, 30)
_DAY = dt.date(2026, 11, 20)


def _canonical(condition: Condition) -> str:
    """The canonical form as JSON text, to compare forms exactly."""
    return json.dumps(condition.canonical_form())


class TestBuilding:
    def test_col_is_exported(self):
        assert orcapod.col is col
        assert isinstance(col("t"), ColumnRef)

    @pytest.mark.parametrize(
        "condition, term",
        [
            (col("t") == 1.0, Term(FieldPath("t"), "==", 1.0)),
            (col("t") != 1.0, Term(FieldPath("t"), "!=", 1.0)),
            (col("t") < 1.0, Term(FieldPath("t"), "<", 1.0)),
            (col("t") <= 1.0, Term(FieldPath("t"), "<=", 1.0)),
            (col("t") > 1.0, Term(FieldPath("t"), ">", 1.0)),
            (col("t") >= 1.0, Term(FieldPath("t"), ">=", 1.0)),
            (col("u").is_in([3, 7, 3]), Term(FieldPath("u"), "is_in", (3, 7))),
            (col("r").is_null(), Term(FieldPath("r"), "is_null", None)),
            (col("r").is_not_null(), Term(FieldPath("r"), "is_not_null", None)),
            (
                col("info").field("kind") == "a",
                Term(FieldPath("info", ("kind",)), "==", "a"),
            ),
            (
                col("a") < col("b"),
                Term(FieldPath("a"), "<", ColumnOperand(FieldPath("b"))),
            ),
        ],
    )
    def test_each_operator_builds_one_term(self, condition, term):
        assert condition.root == AllOf((term,))

    def test_a_reversed_comparison_is_its_mirror(self):
        """``1.0 < col("t")`` is Python's reflected ``col("t") > 1.0``."""
        assert (1.0 < col("t")) == (col("t") > 1.0)

    def test_between_is_two_terms(self):
        condition = col("t").between(0.5, 2.0)
        assert condition == (col("t") >= 0.5) & (col("t") <= 2.0)
        assert len(condition.root.children) == 2

    def test_a_field_of_a_field(self):
        ref = col("info").field("probe").field("name")
        assert ref.path == FieldPath("info", ("probe", "name"))
        assert repr(ref) == "col('info').field('probe').field('name')"

    def test_or_and_not(self):
        either = (col("s") == "a") | (col("t") < 1.0)
        assert isinstance(either.root.children[0], AnyOf)
        negated = ~col("u").is_in([3])
        assert isinstance(negated.root.children[0], Negation)

    def test_or_flattens(self):
        a, b, c = col("a") == 1, col("b") == 2, col("c") == 3
        assert ((a | b) | c).root == (a | (b | c)).root
        (node,) = ((a | b) | c).root.children
        assert isinstance(node, AnyOf) and len(node.children) == 3

    def test_keywords_mappings_and_conditions_together(self):
        """``of`` takes conditions, mappings and keywords; all must hold."""
        combined = Condition.of(col("t") >= 1.0, {"s": "a"}, u=3)
        assert combined == (col("t") >= 1.0) & Condition.of(s="a", u=3)
        assert combined.columns == frozenset({"t", "s", "u"})
        assert not combined.only_equalities
        assert dict(combined.equalities) == {"s": "a", "u": 3}

    def test_columns_names_both_sides_and_the_struct_column(self):
        condition = (col("a") < col("b")) & (col("info").field("kind") == "x")
        assert condition.columns == frozenset({"a", "b", "info"})

    def test_an_equality_on_a_column_is_an_equality(self):
        assert (col("s") == "a") == Condition.of(s="a")
        assert (col("s") == "a").only_equalities
        assert not (col("info").field("k") == "a").only_equalities
        assert not (col("a") == col("b")).only_equalities


class TestRefusedWhenBuilt:
    @pytest.mark.parametrize(
        "build, error, match",
        [
            (lambda: col("t") == None, ValueError, r"t.*None.*is_null"),  # noqa: E711  # the refusal under test
            (lambda: col("t") >= np.float64(1.0), TypeError, "convert"),
            (lambda: col("t") >= "1", None, None),
            (lambda: Condition.of(u=[3, 7]), TypeError, r"is_in"),
            (lambda: col("u").is_in([]), ValueError, "at least one"),
            (lambda: col("u").is_in([3, None]), ValueError, "None"),
            (lambda: col("u").is_in([3, 7.0]), TypeError, "one type"),
            (lambda: col("u").is_in("ab"), TypeError, "collection"),
            (lambda: col("u").is_in(3), TypeError, "collection"),  # pyright: ignore[reportArgumentType]  # the refusal under test
            (lambda: col("t").between(1, 2.0), TypeError, "one type"),
            (lambda: col("t").between(2.0, 1.0), ValueError, "low end"),
            (lambda: col("t").between(math.nan, 1.0), ValueError, "NaN"),
            (lambda: col("t").between(_AWARE, _NAIVE), TypeError, "between"),
            (lambda: Condition.of([True, False]), TypeError, "mask by position"),  # pyright: ignore[reportArgumentType]  # the refusal under test
            (lambda: Condition.of(np.array([True])), TypeError, "mask by position"),  # pyright: ignore[reportArgumentType]  # the refusal under test
            (lambda: col(""), ValueError, "not empty"),
            (lambda: col(3), TypeError, "string"),  # pyright: ignore[reportArgumentType]
            (lambda: col("a").field(""), ValueError, "not empty"),
        ],
    )
    def test_refused(self, build, error, match):
        if error is None:
            build()  # a string is a value; its column decides (check_condition)
            return
        with pytest.raises(error, match=match):
            build()

    def test_a_column_reference_has_no_truth_value(self):
        with pytest.raises(TypeError, match="&"):
            bool(col("t"))
        with pytest.raises(TypeError, match="&"):
            _ = col("t") and col("u")

    def test_a_column_reference_is_not_hashable(self):
        with pytest.raises(TypeError):
            hash(col("t"))

    def test_and_with_something_else_is_refused(self):
        with pytest.raises(TypeError):
            _ = (col("t") > 1.0) & True  # pyright: ignore[reportOperatorIssue]
        with pytest.raises(TypeError):
            _ = (col("t") > 1.0) | col("u")  # pyright: ignore[reportOperatorIssue]

    def test_or_and_not_of_the_empty_condition_are_refused(self):
        with pytest.raises(ValueError, match="every row"):
            _ = Condition() | (col("t") > 1.0)
        with pytest.raises(ValueError, match="every row"):
            _ = ~Condition()

    def test_equal_to_two_values_is_refused(self):
        with pytest.raises(ValueError, match="cannot also equal"):
            _ = (col("u") == 3) & (col("u") == 7)


# Each pair: two ways of writing one condition. Equal, equal hashes, one canonical form.
_SAME: list[tuple[Callable[[], Condition], Callable[[], Condition]]] = [
    (lambda: Condition.of(s="a", u=3), lambda: Condition.of(u=3, s="a")),
    (
        lambda: (col("t") >= 1.0) & (col("u") == 3),
        lambda: (col("u") == 3) & (col("t") >= 1.0),
    ),
    (
        lambda: Condition.of(col("t") >= 1.0, s="a"),
        lambda: Condition.of({"s": "a"}, col("t") >= 1.0),
    ),
    (lambda: col("u").is_in([3, 7, 9]), lambda: col("u").is_in({9, 3, 7, 3})),
    (
        lambda: (col("s") == "a") | (col("t") < 0.5),
        lambda: (col("t") < 0.5) | (col("s") == "a"),
    ),
    (
        lambda: (col("a") == 1) | ((col("b") == 2) | (col("c") == 3)),
        lambda: ((col("c") == 3) | (col("a") == 1)) | (col("b") == 2),
    ),
    (lambda: col("x") == 0.0, lambda: col("x") == -0.0),
    (lambda: col("x") == math.nan, lambda: col("x") == float("nan")),
    (lambda: col("x").is_in([0.0, 1.0]), lambda: col("x").is_in([1.0, -0.0])),
    (lambda: col("at") == _AWARE, lambda: col("at") == _AWARE.astimezone(_NEW_YORK)),
    (lambda: col("at") == _NAIVE, lambda: col("at") == _NAIVE.replace(fold=1)),
    (lambda: col("t").between(1.0, 2.0), lambda: (col("t") <= 2.0) & (col("t") >= 1.0)),
    (
        lambda: ~((col("a") == 1) & (col("b") == 2)),
        lambda: ~((col("b") == 2) & (col("a") == 1)),
    ),
]

# Each pair: two different conditions. Not equal, different canonical forms.
_DIFFERENT: list[tuple[Callable[[], Condition], Callable[[], Condition]]] = [
    (lambda: col("x") == 1, lambda: col("x") == 1.0),
    (lambda: col("x") == 1, lambda: col("x") == True),  # noqa: E712  # the value True under test
    (lambda: col("x") == 1.0, lambda: col("x") == True),  # noqa: E712  # the value True under test
    (lambda: col("x") == "1", lambda: col("x") == b"1"),
    (lambda: col("x") == _DAY, lambda: col("x") == dt.datetime(2026, 11, 20)),
    (lambda: col("x") == _AWARE, lambda: col("x") == _AWARE.replace(tzinfo=None)),
    (lambda: col("x") < 1, lambda: col("x") <= 1),
    (lambda: col("x") != 1, lambda: ~(col("x") == 1)),
    (lambda: col("a") < col("b"), lambda: col("b") > col("a")),
    (lambda: col("a") == col("b"), lambda: col("a") == "b"),
    (lambda: col("info").field("k") == 1, lambda: col("info") == 1),
    (lambda: col("info").field("k") == 1, lambda: col("info.k") == 1),
    (lambda: col("x").is_null(), lambda: col("x").is_not_null()),
    (lambda: col("x").is_in([1]), lambda: col("x") == 1),
    (
        lambda: (col("a") == 1) & (col("b") == 2),
        lambda: (col("a") == 1) | (col("b") == 2),
    ),
]


class TestEqualityAndCanonicalForm:
    @pytest.mark.parametrize("first, second", _SAME)
    def test_one_condition_written_two_ways(self, first, second):
        a, b = first(), second()
        assert a == b
        assert hash(a) == hash(b)
        assert _canonical(a) == _canonical(b)

    @pytest.mark.parametrize("first, second", _DIFFERENT)
    def test_different_conditions(self, first, second):
        a, b = first(), second()
        assert a != b
        assert _canonical(a) != _canonical(b)

    @pytest.mark.parametrize(
        "condition, expected",
        [
            (col("x") == 5, ("term", ("col", "x"), "==", ("int", "5"))),
            (
                col("x") == 1.5,
                ("term", ("col", "x"), "==", ("float", "0x1.8000000000000p+0")),
            ),
            (col("x") == -0.0, ("term", ("col", "x"), "==", ("float", "0x0.0p+0"))),
            (col("x") == math.nan, ("term", ("col", "x"), "==", ("float", "nan"))),
            (col("x") == math.inf, ("term", ("col", "x"), "==", ("float", "inf"))),
            (col("x") == True, ("term", ("col", "x"), "==", ("bool", "true"))),  # noqa: E712  # the value True under test
            (col("x") == "a", ("term", ("col", "x"), "==", ("str", "a"))),
            (col("x") == b"\x01", ("term", ("col", "x"), "==", ("bytes", "01"))),
            (col("x") == _DAY, ("term", ("col", "x"), "==", ("date", "2026-11-20"))),
            (
                col("x") == _NAIVE,
                ("term", ("col", "x"), "==", ("datetime_naive", "2026-11-20T09:30:00")),
            ),
            (
                col("x") == dt.datetime(1, 1, 1, tzinfo=dt.timezone.utc),
                ("term", ("col", "x"), "==", ("datetime_aware", "0")),
            ),
            (
                col("x").is_in([7, 3]),
                ("term", ("col", "x"), "is_in", (("int", "3"), ("int", "7"))),
            ),
            (col("x").is_null(), ("term", ("col", "x"), "is_null")),
            (
                col("a").field("b").field("c") > 1,
                (
                    "term",
                    ("field", ("field", ("col", "a"), "b"), "c"),
                    ">",
                    ("int", "1"),
                ),
            ),
            (
                col("a") < col("b"),
                ("term", ("col", "a"), "<", ("ref", ("col", "b"))),
            ),
            (
                ~(col("x") == 1),
                ("not", ("term", ("col", "x"), "==", ("int", "1"))),
            ),
            (
                (col("y") == 1) | (col("x") == 1),
                (
                    "any",
                    (
                        ("term", ("col", "x"), "==", ("int", "1")),
                        ("term", ("col", "y"), "==", ("int", "1")),
                    ),
                ),
            ),
        ],
    )
    def test_the_form_of_each_kind(self, condition, expected):
        """One term per condition: the form is ``("all", (<term form>,))``."""
        assert condition.canonical_form() == ("all", (expected,))

    def test_the_form_holds_only_strings_and_tuples(self):
        condition = Condition.of(
            (col("a") >= 1.0) | ~col("b").is_in([b"x", b"y"]),
            col("c").field("d").is_not_null(),
            col("e") < col("f"),
            at=_AWARE,
            day=_DAY,
        )

        def check(item: object) -> None:
            if isinstance(item, tuple):
                for inner in item:
                    check(inner)
            else:
                assert type(item) is str

        check(condition.canonical_form())

    def test_nested_and_is_flattened(self):
        a, b, c = col("a") == 1, col("b") == 2, col("c") == 3
        nested = Condition._of_children(
            (AllOf((a.root.children[0], AllOf(b.root.children + c.root.children))),)
        )  # pyright: ignore[reportPrivateUsage]  # a nesting no public call builds
        assert _canonical(nested) == _canonical(a & b & c)


_TREES: list[Callable[[], Condition]] = [
    lambda: (col("t") >= 1.0) & (col("u") == 3),
    lambda: col("u").is_in([3, 7]),
    lambda: col("x").is_in([math.nan, 1.5, -math.inf]),
    lambda: col("r").is_null() | col("r").is_not_null(),
    lambda: ~((col("s") == "a") | (col("t") < 0.5)),
    lambda: col("info").field("kind") != "noise",
    lambda: col("a") <= col("b").field("c"),
    lambda: Condition.of(
        col("at") > _AWARE, col("day").between(_DAY, _DAY), raw=b"ab", naive=_NAIVE
    ),
]


class TestTextConfigAndRepr:
    @pytest.mark.parametrize("build", _TREES)
    def test_round_trip_through_strict_json(self, build):
        condition = build()
        saved = json.loads(json.dumps(condition.to_config(), allow_nan=False))
        loaded = Condition.from_config(saved)
        assert loaded == condition
        assert _canonical(loaded) == _canonical(condition)

    @pytest.mark.parametrize("build", _TREES)
    def test_repr_rebuilds(self, build):
        condition = build()
        assert eval(repr(condition), {"Condition": Condition}) == condition  # noqa: S307  # the test's own repr

    def test_only_equalities_keep_the_mapping_form(self):
        assert (col("s") == "a").to_config() == {"s": "a"}

    def test_the_tree_key_cannot_be_a_column(self):
        assert list((col("t") > 1.0).to_config()) == [":condition"]

    def test_text(self):
        condition = Condition.of(
            (col("s") == "a") | (col("t") < 0.5),
            ~col("u").is_in([3, 7]),
            col("info").field("kind").is_null(),
            col("a") >= col("b"),
            v=1,
        )
        assert condition.text() == (
            "(s='a' or t < 0.5), not (u in [3, 7]), info.kind is null, a >= b, v=1"
        )

    @pytest.mark.parametrize(
        "saved",
        [
            {":condition": {"any": []}},
            {":condition": {"all": [{"column": "x", "fields": [], "op": "~"}]}},
            {
                ":condition": {
                    "all": [{"column": "x", "fields": [], "op": "is_in", "values": []}]
                }
            },
            {
                ":condition": {
                    "all": [{"column": "x", "fields": [], "op": "==", "value": None}]
                }
            },
            {":condition": {"all": []}, "x": 1},
        ],
    )
    def test_a_malformed_saved_tree_is_refused(self, saved):
        with pytest.raises(ValueError):
            Condition.from_config(saved)


_SCHEMA = pa.schema(
    [
        pa.field("u8", pa.uint8()),
        pa.field("u32", pa.uint32()),
        pa.field("i64", pa.int64()),
        pa.field("f32", pa.float32()),
        pa.field("f64", pa.float64()),
        pa.field("f64b", pa.float64()),
        pa.field("s", pa.large_string()),
        pa.field("s_small", pa.string()),
        pa.field("b", pa.large_binary()),
        pa.field("flag", pa.bool_()),
        pa.field("day", pa.date32()),
        pa.field("ts", pa.timestamp("us")),
        pa.field("ts_s", pa.timestamp("s")),
        pa.field("tz", pa.timestamp("us", tz="UTC")),
        pa.field("dec", pa.decimal128(5, 2)),
        pa.field("xs", pa.large_list(pa.int64())),
        pa.field(
            "info",
            pa.struct(
                [
                    pa.field("kind", pa.large_string()),
                    pa.field("inner", pa.struct([pa.field("n", pa.int32())])),
                ]
            ),
        ),
    ]
)


class TestCheckAgainstArrow:
    @pytest.mark.parametrize(
        "condition",
        [
            col("u32") == 7,
            col("u32").is_in([3, 7]),
            col("u8") <= 255,
            col("i64").between(-5, 5),
            col("f32") > 0.1,
            col("f64") != math.nan,
            col("s") >= "imec",
            col("b") < b"z",
            col("flag") == True,  # noqa: E712  # the value True under test
            col("day") > _DAY,
            col("ts") <= _NAIVE,
            col("tz") >= _AWARE,
            col("info").field("kind") == "a",
            col("info").field("inner").field("n") > 3,
            col("info").is_null(),
            col("dec").is_not_null(),
            col("xs").is_null(),
            col("f64") < col("f64b"),
            col("s") == col("s_small"),
        ],
    )
    def test_accepted(self, condition):
        check_condition(condition, _SCHEMA)

    @pytest.mark.parametrize(
        "condition, error, match",
        [
            (col("nope") == 1, ValueError, r"no column 'nope'.*u8"),
            (
                col("info").field("nope") == 1,
                ValueError,
                r"no field 'nope'.*kind: large_string",
            ),
            (col("s").field("x") == 1, ValueError, "not a struct"),
            (col("u32") == -1, TypeError, r"u32.*cannot hold"),
            (col("u8").is_in([1, 300]), TypeError, r"u8.*cannot hold"),
            (col("f64") == 1, TypeError, r"f64.*convert"),
            (col("i64") == True, TypeError, r"i64.*convert"),  # noqa: E712  # the value True under test
            (col("i64") == 1.0, TypeError, r"i64.*convert"),
            (col("day") == "2026-11-20", TypeError, r"day.*convert"),
            (col("day") == _NAIVE, TypeError, r"day.*convert"),
            (col("tz") == _NAIVE, TypeError, "aware"),
            (col("ts") == _AWARE, TypeError, "naive"),
            (col("ts_s") == _NAIVE.replace(microsecond=1), TypeError, "exactly"),
            (col("f32") == 1e300, TypeError, "range"),
            (col("flag") < True, TypeError, r"flag.*no order"),
            (col("dec") == 1.5, TypeError, "does not support"),
            (col("xs") == 1, TypeError, "does not support"),
            (col("info") > 1, TypeError, "no order"),
            (col("i64") < col("u32"), TypeError, "different types"),
            (col("f64") == col("i64"), TypeError, "different types"),
            (col("i64") < col("nope"), ValueError, "nope"),
        ],
    )
    def test_refused(self, condition, error, match):
        with pytest.raises(error, match=match):
            check_condition(condition, _SCHEMA)

    def test_the_field_type(self):
        assert (
            field_arrow_type(FieldPath("info", ("inner", "n")), _SCHEMA) == pa.int32()
        )

    def test_value_types(self):
        assert condition_value_type(pa.uint32()) is int
        assert condition_value_type(pa.float16()) is float
        assert condition_value_type(pa.date64()) is dt.date
        assert condition_value_type(pa.time64("us")) is None

    def test_the_equality_check_where_uses(self):
        assert check_equality_conditions({"u32": 7, "s": "a"}, _SCHEMA) == {
            "u32": 7,
            "s": "a",
        }
        with pytest.raises(ValueError, match="nope"):
            check_equality_conditions({"nope": 1}, _SCHEMA)
        with pytest.raises(TypeError, match=r"u8.*cannot hold"):
            check_equality_conditions({"u8": 256}, _SCHEMA)


class TestCheckAgainstPython:
    _PY = {"u": int, "t": float, "r": float | None, "s": str, "info": object}

    def test_accepted(self):
        check_condition_columns(
            Condition.of(
                col("u").is_in([3]),
                col("r") > 1.0,
                col("info").field("x") == 1,
                col("t") < col("r"),
                s="a",
            ),
            self._PY,
        )

    @pytest.mark.parametrize(
        "condition, error, match",
        [
            (col("nope") == 1, ValueError, r"no column 'nope'.*'u'"),
            (col("u") < col("nope"), ValueError, "nope"),
            (col("t") == 1, TypeError, r"t.*float.*int"),
            (col("r") >= 1, TypeError, r"r.*float.*int"),
            (col("u").is_in([True]), TypeError, r"u.*int.*bool"),
        ],
    )
    def test_refused(self, condition, error, match):
        with pytest.raises(error, match=match):
            check_condition_columns(condition, self._PY)
