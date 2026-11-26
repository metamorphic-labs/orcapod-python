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

    def test_equal_to_two_values_is_kept(self):
        """A contradiction is kept, never refused (developer, 2026-11-22)."""
        condition = (col("u") == 3) & (col("u") == 7)
        assert len(condition.root.children) == 2


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


# --- Datetimes are frozen when a condition is built (ITL-1374, review of PR 497) -----


class _MutableZone(dt.tzinfo):
    """A tzinfo whose offset can change after the fact and that counts its calls."""

    def __init__(self, hours: int) -> None:
        self.hours = hours
        self.calls = 0

    def utcoffset(self, _value: dt.datetime | None) -> dt.timedelta:
        self.calls += 1
        return dt.timedelta(hours=self.hours)

    def dst(self, _value: dt.datetime | None) -> dt.timedelta:
        self.calls += 1
        return dt.timedelta(0)

    def tzname(self, _value: dt.datetime | None) -> str:
        self.calls += 1
        return f"MUTABLE{self.hours:+d}"


_BUILDERS_OF_TWO: list[Callable[[dt.datetime, dt.datetime], Condition]] = [
    lambda a, b: Condition.of(at=a),
    lambda a, b: Condition.of({"at": a, "other": b}),
    lambda a, b: (col("at") == a) & (col("other") >= b),
    lambda a, b: col("at").is_in([a, b]),
    lambda a, b: col("at").between(a, b),
    lambda a, b: (col("at") < a) | ~(col("other") == b),
]


class TestDatetimesAreFrozen:
    @pytest.mark.parametrize(
        "build, literals", zip(_BUILDERS_OF_TWO, [1, 2, 2, 2, 2, 2], strict=True)
    )
    def test_a_mutable_tzinfo_is_read_once(self, build, literals):
        """After the condition is built nothing calls the value's tzinfo, and changing
        its offset changes neither the condition, its identity nor what it checks."""
        zone = _MutableZone(0)
        first = dt.datetime(2026, 11, 25, 9, 0, tzinfo=zone)
        second = dt.datetime(2026, 11, 25, 10, 0, tzinfo=zone)
        condition = build(first, second)
        form, text, config = (
            _canonical(condition),
            condition.text(),
            json.dumps(condition.to_config()),
        )
        # Building read each datetime's offset once: one call per literal it holds.
        assert zone.calls == literals
        zone.calls, zone.hours = 0, 1  # the offset moves after the build

        schema = pa.schema(
            [
                pa.field("at", pa.timestamp("us", tz="UTC")),
                pa.field("other", pa.timestamp("us", tz="UTC")),
            ]
        )
        check_condition(condition, schema)
        assert _canonical(condition) == form
        assert condition.text() == text
        assert json.dumps(condition.to_config()) == config
        _ = hash(condition), condition == condition
        assert zone.calls == 0  # building again (below) reads the tzinfo, as it should
        rebuilt = build(
            dt.datetime(2026, 11, 25, 9, 0, tzinfo=dt.timezone.utc),
            dt.datetime(2026, 11, 25, 10, 0, tzinfo=dt.timezone.utc),
        )
        assert condition == rebuilt
        assert _canonical(condition) == _canonical(rebuilt)

    @pytest.mark.parametrize(
        "spelling",
        [
            lambda v: Condition.of(at=v),
            lambda v: Condition.of({"at": v}),
            lambda v: col("at") == v,
        ],
        ids=["keyword", "mapping", "col"],
    )
    def test_an_aware_value_is_held_as_its_utc_instant(self, spelling):
        value = dt.datetime(2026, 11, 1, 1, 30, fold=1, tzinfo=_NEW_YORK)
        (term,) = spelling(value).root.children
        assert isinstance(term, Term)
        assert term.value == value.astimezone(dt.timezone.utc)
        assert term.value.tzinfo is dt.timezone.utc  # pyright: ignore[reportAttributeAccessIssue]

    def test_a_naive_value_keeps_its_wall_time_without_tzinfo(self):
        class NoOffset(dt.tzinfo):
            def utcoffset(self, _value: dt.datetime | None) -> None:
                return None

        value = dt.datetime(2026, 11, 25, 9, 0, fold=1, tzinfo=NoOffset())
        (term,) = Condition.of(at=value).root.children
        assert isinstance(term, Term)
        assert term.value == dt.datetime(2026, 11, 25, 9, 0)
        assert term.value.tzinfo is None  # pyright: ignore[reportAttributeAccessIssue]


_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
# The epoch's wall time at UTC-00:00:00.000001 names one microsecond after the epoch.
_FRACTIONAL = dt.datetime(1970, 1, 1, tzinfo=dt.timezone(-dt.timedelta(microseconds=1)))
_ONE_AFTER_EPOCH = _EPOCH + dt.timedelta(microseconds=1)
_FIRST_0100 = dt.datetime(2025, 11, 2, 1, 0, tzinfo=_NEW_YORK)  # 05:00 UTC
_SECOND_0100 = _FIRST_0100.replace(fold=1)  # 06:00 UTC


class TestOneInstantOneIdentity:
    @pytest.mark.parametrize(
        "first, second",
        [
            (_FRACTIONAL, _ONE_AFTER_EPOCH),
            (_SECOND_0100, _SECOND_0100.astimezone(dt.timezone.utc)),
            (_FIRST_0100, _FIRST_0100.astimezone(dt.timezone(dt.timedelta(hours=-4)))),
        ],
        ids=["fractional offset", "second 01:00 of an overlap", "first 01:00"],
    )
    @pytest.mark.parametrize(
        "spelling",
        [
            lambda v: Condition.of(at=v, unit=3),
            lambda v: Condition.of({"unit": 3}, col("at") >= v),
            lambda v: col("at").is_in([v, _EPOCH]),
        ],
        ids=["keyword", "col", "is_in"],
    )
    def test_two_spellings_of_an_instant(self, first, second, spelling):
        """One identity, both accepted by the check, and the saved form reads back as
        an equal condition with the same canonical form."""
        a, b = spelling(first), spelling(second)
        assert a == b and hash(a) == hash(b)
        assert _canonical(a) == _canonical(b)
        for zone in ("UTC", "America/New_York"):
            schema = pa.schema(
                [
                    pa.field("at", pa.timestamp("us", tz=zone)),
                    pa.field("unit", pa.int64()),
                ]
            )
            check_condition(a, schema)
            check_condition(b, schema)
        for condition in (a, b):
            saved = json.loads(json.dumps(condition.to_config()))
            loaded = Condition.from_config(saved)
            assert loaded == condition
            assert _canonical(loaded) == _canonical(condition)

    def test_the_two_times_of_an_overlap_stay_two(self):
        assert Condition.of(at=_FIRST_0100) != Condition.of(at=_SECOND_0100)
        assert _canonical(Condition.of(at=_FIRST_0100)) != _canonical(
            Condition.of(at=_SECOND_0100)
        )

    def test_a_check_still_refuses_a_part_below_the_unit(self):
        schema = pa.schema([pa.field("at", pa.timestamp("s", tz="UTC"))])
        with pytest.raises(TypeError, match="exactly"):
            check_condition(col("at") == _FRACTIONAL, schema)


class TestBetweenComparesInstants:
    def test_a_range_ending_before_it_starts_is_refused(self):
        """01:00 fold=1 (06:00 UTC) .. 01:30 fold=0 (05:30 UTC): Python's wall-time
        comparison within one tzinfo would accept it."""
        low = _SECOND_0100
        high = dt.datetime(2025, 11, 2, 1, 30, tzinfo=_NEW_YORK)
        assert not low > high  # Python's own comparison: wall time, fold ignored
        with pytest.raises(ValueError, match="low end above"):
            col("at").between(low, high)

    def test_a_range_across_the_overlap_is_taken(self):
        low = dt.datetime(2025, 11, 2, 1, 30, tzinfo=_NEW_YORK)  # 05:30 UTC
        condition = col("at").between(low, _SECOND_0100)  # .. 06:00 UTC
        assert condition == (col("at") >= low.astimezone(dt.timezone.utc)) & (
            col("at") <= _SECOND_0100.astimezone(dt.timezone.utc)
        )


class TestSavedTreesMeetTheBuildChecks:
    @pytest.mark.parametrize(
        "children",
        [
            [
                {"column": "k", "fields": [], "op": "==", "value": 1},
                {"column": "k", "fields": [], "op": "==", "value": 2},
            ],
            [
                {"column": "k", "fields": [], "op": "==", "value": 2},
                {"column": "x", "fields": [], "op": ">", "value": 1.0},
                {"column": "k", "fields": [], "op": "==", "value": 1},
            ],
            [
                {
                    "any": [
                        {
                            "all": [
                                {"column": "k", "fields": [], "op": "==", "value": 1},
                                {"column": "k", "fields": [], "op": "==", "value": 2},
                            ]
                        },
                        {"column": "x", "fields": [], "op": "is_null"},
                    ]
                }
            ],
        ],
        ids=["two values", "two values around another term", "inside an or"],
    )
    # (was: refused, before the developer's decision of 2026-11-22)
    def test_two_values_for_one_column_load_as_saved(self, children):
        """A saved contradiction loads as the same contradiction, never refused, and
        saves again to the same form."""
        saved = {":condition": {"all": children}}
        loaded = Condition.from_config(saved)
        assert not loaded.only_equalities
        again = Condition.from_config(json.loads(json.dumps(loaded.to_config())))
        assert again == loaded and _canonical(again) == _canonical(loaded)

    @pytest.mark.parametrize(
        "saved, error",
        [
            (
                {
                    "column": "k",
                    "fields": [],
                    "op": "is_in",
                    "values": [1, {"float": "nan"}],
                },
                TypeError,
            ),
            ({"column": "k", "fields": [], "op": "<", "value": [1]}, TypeError),
            ({"column": "", "fields": [], "op": "==", "value": 1}, ValueError),
            ({"any": []}, ValueError),
        ],
        ids=["is_in of two types", "a list value", "an empty name", "an empty or"],
    )
    def test_a_term_no_builder_makes_is_refused(self, saved, error):
        with pytest.raises(error):
            Condition.from_config({":condition": {"all": [saved]}})

    def test_a_repeated_requirement_is_kept_once(self):
        term = {"column": "k", "fields": [], "op": "==", "value": 1}
        other = {"column": "x", "fields": [], "op": ">", "value": 1.0}
        loaded = Condition.from_config({":condition": {"all": [term, other, term]}})
        assert loaded == Condition.of(col("x") > 1.0, k=1)
        assert len(loaded.root.children) == 2

    def test_where_never_sees_two_equalities_on_one_column_as_equalities(self):
        """A tree holding two equalities of one column (which no builder makes) is not
        'equalities only', so a source's where refuses it rather than keep one."""
        one, two = (col("k") == 1).root.children, (col("k") == 2).root.children
        condition = Condition._of_children((*one, *two))  # pyright: ignore[reportPrivateUsage]  # the state no public call reaches
        assert not condition.only_equalities


# --- CodeRabbit review of PR 493 -------------------------------------------------------


class _ShiftingZone(dt.tzinfo):
    """A tzinfo whose offset moves by one hour on every call after the first."""

    def __init__(self, hours: int) -> None:
        self.hours = hours

    def utcoffset(self, _value: dt.datetime | None) -> dt.timedelta:
        offset = dt.timedelta(hours=self.hours)
        self.hours += 1
        return offset

    def dst(self, _value: dt.datetime | None) -> dt.timedelta:
        return dt.timedelta(0)


class TestReviewFindings:
    @pytest.mark.parametrize(
        "wall, hours",
        [(dt.datetime(1, 1, 1, 0, 0), 1), (dt.datetime(2026, 11, 25, 9, 0), 0)],
        ids=["no UTC datetime", "in range"],
    )
    def test_the_offset_is_read_once(self, wall, hours):
        """Every step of the freeze uses the first offset the tzinfo gave."""
        (term,) = (col("at") == wall.replace(tzinfo=_ShiftingZone(hours))).root.children
        assert isinstance(term, Term)
        expected = wall.replace(tzinfo=dt.timezone(dt.timedelta(hours=hours)))
        assert term.value == expected

    @pytest.mark.parametrize(
        "build",
        [
            lambda a, b: Condition.of({"at": a}, {"at": b}),
            lambda a, b: Condition.of({"at": a}, at=b),
        ],
        ids=["two mappings", "mapping and keyword"],
    )
    def test_the_value_compared_is_the_value_kept(self, build):
        """Values are frozen before a repeated column is compared, and that frozen
        value is the one kept."""
        first = dt.datetime(2026, 11, 25, 9, 0, tzinfo=dt.timezone.utc)
        second = dt.datetime(2026, 11, 25, 10, 0, tzinfo=_ShiftingZone(1))
        condition = build(first, second)
        assert dict(condition.equalities) == {"at": first}

    @pytest.mark.parametrize(
        "build",
        [
            lambda: col("x") == 1,
            lambda: (col("x") == 1) & (col("y") > 2.0),
            lambda: col("x").is_in([1, 2]),
        ],
    )
    def test_or_with_itself_is_itself(self, build):
        condition = build() | build()
        assert condition == build()
        assert _canonical(condition) == _canonical(build())
        loaded = Condition.from_config(json.loads(json.dumps(condition.to_config())))
        assert loaded == condition and _canonical(loaded) == _canonical(condition)

    @pytest.mark.parametrize(
        "value", [1, "x", {"date": "2026-11-25"}, {"float": "nan"}], ids=repr
    )
    def test_an_equality_on_a_column_named_like_the_tree_key(self, value):
        saved = {":condition": value}
        assert Condition.from_config(saved).to_config() == saved

    @pytest.mark.parametrize(
        "first, second",
        [
            (col("x").is_in([1, 2, 3]), col("x").is_in([3, 1, 2])),
            (
                (col("a") == 1) | (col("b") == 2) | col("c").is_null(),
                col("c").is_null() | (col("b") == 2) | (col("a") == 1),
            ),
            (
                Condition.of(col("t") > 1.0, ~col("u").is_in([2, 1])),
                Condition.of(~col("u").is_in([1, 2]), col("t") > 1.0),
            ),
        ],
        ids=["is_in values", "or alternatives", "all children"],
    )
    def test_the_saved_form_follows_no_given_order(self, first, second):
        assert json.dumps(first.to_config()) == json.dumps(second.to_config())

    @pytest.mark.parametrize(
        "build",
        [
            lambda: Condition.of({"": 1}),
            lambda: Condition.of(**{"": 1}),
            lambda: col(""),
        ],
        ids=["mapping", "keyword", "col"],
    )
    def test_an_empty_column_name_is_refused_everywhere(self, build):
        with pytest.raises(ValueError, match="not empty"):
            build()

    def test_membership_of_many_values_keeps_the_first_of_each(self):
        values = [*range(5000), *range(5000), -0.0][:-1]
        (term,) = col("x").is_in(values).root.children
        assert isinstance(term, Term) and term.value == tuple(range(5000))
        (floats,) = col("x").is_in([0.0, -0.0, math.nan, float("nan")]).root.children
        assert isinstance(floats, Term) and len(floats.value) == 2  # pyright: ignore[reportArgumentType]


# --- Contradictions are kept, never refused (developer, 2026-11-22) -------------------


_CONTRADICTIONS: list[tuple[Callable[[], Condition], Callable[[], Condition]]] = [
    (
        lambda: (col("k") == 1) & (col("k") == 5),
        lambda: (col("k") == 5) & (col("k") == 1),
    ),
    (
        lambda: Condition.of(col("k") == 1, col("k") == 5),
        lambda: Condition.of(col("k") == 5, col("k") == 1),
    ),
    (
        lambda: Condition.of(col("k") == 5, k=1),
        lambda: Condition.of(col("k") == 1, k=5),
    ),
    (
        lambda: (col("k") == 1) & (col("k") > 5),
        lambda: (col("k") > 5) & (col("k") == 1),
    ),
    (
        lambda: col("k").is_in([1, 2]) & col("k").is_in([3, 4]),
        lambda: col("k").is_in([4, 3]) & col("k").is_in([2, 1]),
    ),
]


class TestContradictionsAreKept:
    @pytest.mark.parametrize("first, second", _CONTRADICTIONS)
    def test_built_order_free_saved_and_reloaded(self, first, second):
        a, b = first(), second()
        assert a == b and hash(a) == hash(b)
        assert _canonical(a) == _canonical(b)
        terms = a.canonical_form()[1]
        assert len(terms) == 2  # both requirements are in the identity
        loaded = Condition.from_config(json.loads(json.dumps(a.to_config())))
        assert loaded == a and _canonical(loaded) == _canonical(a)
        assert not (a.only_equalities and len(a.equalities) < len(terms))

    def test_equal_duplicates_still_collapse(self):
        a = col("k") == 1
        assert a & a == a
        assert len((a & a).root.children) == 1
        assert Condition.of(a, k=1) == a

    @pytest.mark.parametrize(
        "build",
        [
            lambda: Condition.of({"k": 1}, k=5),
            lambda: Condition.of({"k": 1}, {"k": 5}),
        ],
        ids=["mapping and keyword", "two mappings"],
    )
    def test_one_column_named_twice_in_one_call_is_refused(self, build):
        with pytest.raises(ValueError, match=r"'k' is named twice in one call"):
            build()


class TestCanonicalText:
    """``canonical_text()`` reads the same whatever the order given (ITL-1402 explain)."""

    @pytest.mark.parametrize(
        "first, second",
        [
            (
                lambda: (col("t") >= 1.0) & (col("u") == 3),
                lambda: (col("u") == 3) & (col("t") >= 1.0),
            ),
            (lambda: col("u").is_in([3, 7]), lambda: col("u").is_in([7, 3])),
            (
                lambda: Condition.of(col("t") > 1.0, s="a", u=3),
                lambda: Condition.of({"u": 3}, col("t") > 1.0, s="a"),
            ),
            (
                lambda: ~((col("a") == 1) | col("b").is_in([2, 1])),
                lambda: ~(col("b").is_in([1, 2]) | (col("a") == 1)),
            ),
            (
                lambda: (col("k") == 1) & (col("k") == 5),
                lambda: (col("k") == 5) & (col("k") == 1),
            ),
        ],
        ids=[
            "and",
            "is_in values",
            "keywords and arguments",
            "nested or and not",
            "contradiction",
        ],
    )
    def test_two_orders_read_the_same(self, first, second):
        assert first().canonical_text() == second().canonical_text()

    def test_the_words_are_those_of_text(self):
        condition = col("u").is_in([3, 7]) & (col("t") >= 1.0)
        assert condition.canonical_text() == "t >= 1.0, u in [3, 7]"
        assert condition.text() == "u in [3, 7], t >= 1.0"  # text keeps the order given
        assert Condition().canonical_text() == "no condition"


class TestSecondReviewOfPR493:
    @pytest.mark.parametrize(
        "offset",
        [
            dt.timedelta(microseconds=1),
            dt.timedelta(seconds=1, microseconds=1),
            dt.timedelta(hours=1, microseconds=1),
        ],
        ids=["zero whole seconds", "one second", "one hour"],
    )
    def test_a_value_kept_under_its_offset_reloads_exactly(self, offset):
        """A year-1 value at a positive offset has no UTC datetime and keeps its
        offset; the saved form restores that offset, fraction included."""
        value = dt.datetime(1, 1, 1, tzinfo=dt.timezone(offset))
        condition = col("at") == value
        loaded = Condition.from_config(json.loads(json.dumps(condition.to_config())))
        (term,) = loaded.root.children
        assert isinstance(term, Term)
        assert term.value.utcoffset() == offset  # pyright: ignore[reportAttributeAccessIssue]
        assert loaded == condition and _canonical(loaded) == _canonical(condition)

    @pytest.mark.parametrize(
        "first, second",
        [
            (
                lambda: col("x").is_in([0.0, -0.0, 1.0]),
                lambda: col("x").is_in([-0.0, 0.0, 1.0]),
            ),
            (lambda: col("x") == 0.0, lambda: col("x") == -0.0),
            (lambda: col("x") >= -0.0, lambda: col("x") >= 0.0),
        ],
        ids=["is_in", "equality", "comparison"],
    )
    def test_canonical_text_shows_one_zero(self, first, second):
        assert first().canonical_text() == second().canonical_text()
        assert "-0.0" not in first().canonical_text() + second().canonical_text()

    def test_a_saved_comparison_with_both_operands_is_refused(self):
        saved = {
            ":condition": {
                "all": [
                    {
                        "column": "x",
                        "fields": [],
                        "op": "==",
                        "value": 1,
                        "other": {"column": "y", "fields": []},
                    }
                ]
            }
        }
        with pytest.raises(ValueError, match="one operand"):
            Condition.from_config(saved)
        for condition in (col("x") == col("y"), col("x") == 1):
            assert Condition.from_config(condition.to_config()) == condition


class TestMembershipZeroAndScale:
    def test_one_zero_is_kept_and_saved_whatever_the_order(self):
        first, second = (
            col("x").is_in([0.0, -0.0, 1.0]),
            col("x").is_in([-0.0, 0.0, 1.0]),
        )
        assert json.dumps(first.to_config()) == json.dumps(second.to_config())
        assert first.text() == second.text() == "x in [0.0, 1.0]"

    def test_large_memberships_compare_as_sets(self):
        values = list(range(10_000))
        assert col("x").is_in(values) == col("x").is_in(list(reversed(values)))
        assert col("x").is_in(values) != col("x").is_in([*values[:-1], 10_000])


# --- Review findings as classes (developer, 2026-11-22: every finding gets a test) ------

_LONDON = ZoneInfo("Europe/London")
# The repeated hour of three fall-back days: (zone, date, hour), its 30th minute after the
# first occurrence's start.
_OVERLAPS = [
    (_NEW_YORK, dt.date(2025, 11, 2), 1),
    (_NEW_YORK, dt.date(2026, 11, 1), 1),
    (_LONDON, dt.date(2026, 10, 25), 1),
]


@pytest.mark.parametrize(
    "zone, day, hour", _OVERLAPS, ids=["NY 2025", "NY 2026", "London 2026"]
)
class TestBetweenAcrossEveryOverlap:
    """Codex review of PR 497, finding 4, as a class: ``between`` compares instants in
    every daylight-saving overlap, never wall times within one tzinfo."""

    def test_a_reversed_range_is_refused(self, zone, day, hour):
        second_start = dt.datetime.combine(day, dt.time(hour, 0), tzinfo=zone).replace(
            fold=1
        )
        first_half = dt.datetime.combine(day, dt.time(hour, 30), tzinfo=zone)
        assert second_start.astimezone(dt.timezone.utc) > first_half.astimezone(
            dt.timezone.utc
        )
        with pytest.raises(ValueError, match="low end above"):
            col("at").between(second_start, first_half)

    def test_an_ordered_range_across_the_overlap_is_taken(self, zone, day, hour):
        first_half = dt.datetime.combine(day, dt.time(hour, 30), tzinfo=zone)
        second_start = first_half.replace(minute=0, fold=1)
        condition = col("at").between(first_half, second_start)
        assert condition == (col("at") >= first_half.astimezone(dt.timezone.utc)) & (
            col("at") <= second_start.astimezone(dt.timezone.utc)
        )


@pytest.mark.parametrize(
    "first, second",
    [
        (lambda: col("t") > 1.0, lambda: col("t") < 2.0),
        (lambda: Condition.of(s="a"), lambda: col("u").is_in([3])),
        (lambda: col("r").is_null(), lambda: (col("a") == 1) | (col("b") == 2)),
    ],
    ids=["comparisons", "equality and membership", "null test and or"],
)
def test_python_and_between_conditions_raises(
    first: Callable[[], object], second: Callable[[], object]
) -> None:
    """Every condition kind refuses a Python truth test."""
    a, b = first(), second()
    with pytest.raises(TypeError, match="Condition has no truth value"):
        _ = a and b


@pytest.mark.parametrize("operator", ["==", "!=", "<", "<=", ">", ">="])
@pytest.mark.parametrize(
    "value", [1, {"date": "2026-11-25"}, {"float": "nan"}], ids=["int", "date", "nan"]
)
def test_a_saved_comparison_holds_one_operand(operator, value):
    """CodeRabbit on PR 493 (both operands silently resolved to one), as a class: every
    comparison operator and several saved value forms."""
    saved = {
        ":condition": {
            "all": [
                {
                    "column": "x",
                    "fields": [],
                    "op": operator,
                    "value": value,
                    "other": {"column": "y", "fields": []},
                }
            ]
        }
    }
    with pytest.raises(ValueError, match="one operand"):
        Condition.from_config(saved)


def _same_everywhere(a: Condition, b: Condition) -> None:
    """Equal conditions agree in every form: equality, hash, canonical form and text,
    the saved config (as JSON), and the condition the saved config loads as."""
    assert a == b
    assert hash(a) == hash(b)
    assert a.canonical_form() == b.canonical_form()
    assert a.canonical_text() == b.canonical_text()
    saved_a = json.dumps(a.to_config(), sort_keys=True)
    assert saved_a == json.dumps(b.to_config(), sort_keys=True)
    reloaded = Condition.from_config(json.loads(saved_a))
    assert reloaded == a
    assert json.dumps(reloaded.to_config(), sort_keys=True) == saved_a


class TestCodeRabbitBodiesOfPr497:
    """CodeRabbit's review bodies on PR 497 (a72f9187): items outside the diff."""

    @pytest.mark.parametrize(
        "build",
        [
            lambda z: col("x") == z,
            lambda z: col("x") >= z,
            lambda z: col("x") < z,
            lambda z: col("x") != z,
            lambda z: col("x").is_in([z, 1.0]),
            lambda z: (col("x") == z) & (col("y") > 1),
            lambda z: ~(col("x") <= z),
            lambda z: Condition.of(x=z),
        ],
        ids=["eq", "ge", "lt", "ne", "is_in", "tree_eq", "not", "mapping"],
    )
    def test_signed_zero_is_saved_as_zero(self, build):
        _same_everywhere(build(-0.0), build(0.0))

    @pytest.mark.parametrize(
        "build",
        [
            lambda z: (col("x") >= z) & (col("x") >= -z),
            lambda z: (col("x") == z) | (col("x") == -z),
        ],
        ids=["and", "or"],
    )
    def test_signed_zero_in_either_operand_order(self, build):
        _same_everywhere(build(-0.0), build(0.0))

    @pytest.mark.parametrize(
        ("first", "second"),
        [
            (
                dt.datetime(1, 1, 1, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=1))),
                dt.datetime(1, 1, 1, 1, 0, tzinfo=dt.timezone(dt.timedelta(hours=2))),
            ),
            (
                dt.datetime(1, 1, 1, 0, 30, tzinfo=dt.timezone(dt.timedelta(hours=5))),
                dt.datetime(
                    1,
                    1,
                    1,
                    0,
                    0,
                    0,
                    1,
                    tzinfo=dt.timezone(
                        dt.timedelta(hours=4, minutes=30, microseconds=1)
                    ),
                ),
            ),
            (
                dt.datetime(
                    9999, 12, 31, 23, 0, tzinfo=dt.timezone(-dt.timedelta(hours=1))
                ),
                dt.datetime(
                    9999, 12, 31, 22, 0, tzinfo=dt.timezone(-dt.timedelta(hours=2))
                ),
            ),
        ],
        ids=["year_1_whole_hours", "year_1_fractional_offset", "year_9999"],
    )
    @pytest.mark.parametrize(
        "build",
        [
            lambda a, b: (col("t") == a, col("t") == b),
            lambda a, b: (col("t").is_in([a, b]), col("t").is_in([b, a])),
            lambda a, b: (col("t") >= a, col("t") >= b),
        ],
        ids=["equal", "is_in_orders", "order"],
    )
    def test_an_instant_outside_utc_range_has_one_representation(
        self, first, second, build
    ):
        _same_everywhere(*build(first, second))

    def test_combining_keeps_the_first_occurrence_and_contradictions(self):
        wide = Condition.of({f"c{i}": i for i in range(2000)})
        again = wide & Condition.of({f"c{i}": i for i in range(1000)})
        assert again == wide
        assert [t.path.column for t in again.terms()][:3] == ["c0", "c1", "c2"]
        contradiction = (col("k") == 1) & (col("k") == 2) & (col("k") == 1)
        assert sorted(repr(t.value) for t in contradiction.terms()) == ["1", "2"]

    def test_a_contradiction_is_not_read_as_equalities(self):
        contradiction = (col("k") == 1) & (col("k") == 2)
        assert not contradiction.only_equalities
        assert "k" in json.dumps(contradiction.to_config())
        assert ":condition" in contradiction.to_config()

    def test_two_binary_layouts_compare_as_one_type(self):
        schema = pa.schema([("a", pa.binary()), ("b", pa.large_binary())])
        check_condition(col("a") == col("b"), schema)
