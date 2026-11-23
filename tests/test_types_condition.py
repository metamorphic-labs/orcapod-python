"""``orcapod.types.Condition`` on its own: the factory, combining, equality, text, saved form.

A condition knows nothing of a source or a limit (ITL-1218); the source-side behaviour is in
``tests/test_core/sources/test_spiraldb_table_source_where.py``.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import zoneinfo

import numpy as np
import pytest

from orcapod.types import Condition

_DAY = dt.date(2026, 11, 20)
_START = dt.datetime(2026, 11, 20, 9, 30)
_AWARE = _START.replace(tzinfo=dt.timezone.utc)
_NEW_YORK = zoneinfo.ZoneInfo("America/New_York")
# The two 01:30 of the daylight-saving overlap in New York, November 1, 2026: Python's
# ``==`` calls them equal (one tzinfo, wall time); they are two instants an hour apart.
_FIRST_0130 = dt.datetime(2026, 11, 1, 1, 30, tzinfo=_NEW_YORK)
_SECOND_0130 = _FIRST_0130.replace(fold=1)


class TestFactory:
    """``Condition.of`` takes what ``where()`` takes."""

    @pytest.mark.parametrize(
        "build",
        [
            lambda: Condition.of(unit=106, session=2, label="a"),
            lambda: Condition.of(label="a", session=2, unit=106),
            lambda: Condition.of({"unit": 106, "session": 2, "label": "a"}),
            lambda: Condition.of({"label": "a"}, unit=106, session=2),
            lambda: Condition.of(Condition.of(session=2), unit=106, label="a"),
            lambda: Condition.of(unit=106).combined(Condition.of(label="a", session=2)),
        ],
        ids=[
            "keywords",
            "keywords in another order",
            "mapping",
            "mapping and keywords",
            "condition and keywords",
            "combined",
        ],
    )
    def test_every_form_of_three_columns_is_one_condition(self, build):
        """Keywords, a mapping, both, a condition, combined, in any order: one value."""
        condition = build()

        assert condition == Condition.of(unit=106, session=2, label="a")
        assert hash(condition) == hash(Condition.of(unit=106, session=2, label="a"))
        assert dict(condition.equalities) == {"unit": 106, "session": 2, "label": "a"}

    def test_a_condition_is_returned_unchanged(self):
        """Given a condition and no keyword, ``of`` returns that object."""
        condition = Condition.of(unit=106, session=2)

        assert Condition.of(condition) is condition

    def test_a_column_named_mapping_is_a_keyword(self):
        """The mapping is positional-only, so any column name can be a keyword."""
        assert dict(Condition.of(mapping="b").equalities) == {"mapping": "b"}

    def test_the_order_given_is_kept_for_display(self):
        """``equalities`` and the text keep the order given."""
        condition = Condition.of(session=2, unit=106)

        assert list(condition.equalities) == ["session", "unit"]
        assert condition.text() == "session=2, unit=106"

    def test_no_column_is_refused(self):
        """``of()`` with nothing, an empty mapping or the empty condition is refused."""
        for arguments in [(), ({},), (Condition(),)]:
            with pytest.raises(ValueError, match="at least one"):
                Condition.of(*arguments)

    def test_mapping_and_keyword_must_agree(self):
        """A column in the mapping and as a keyword with another value is refused."""
        with pytest.raises(ValueError, match=r"unit.*106.*107"):
            Condition.of({"unit": 106, "session": 2}, unit=107)
        assert Condition.of({"unit": 106}, unit=106) == Condition.of(unit=106)

    def test_none_is_refused(self):
        """A null equals nothing, so ``None`` is refused, naming the column."""
        with pytest.raises(ValueError, match=r"note.*None"):
            Condition.of(unit=106, note=None)

    @pytest.mark.parametrize(
        "value, type_name",
        [
            (np.int64(1), "int64"),
            ([1.0], "list"),
            ({"a": 1}, "dict"),
            (dt.time(9, 30), "time"),
        ],
    )
    def test_an_unsupported_value_type_is_refused(self, value, type_name):
        """Only exact bool, int, float, str, bytes, datetime and date are taken."""
        with pytest.raises(TypeError, match=rf"channel.*{type_name}.*convert"):
            Condition.of(unit=106, channel=value)

    def test_a_column_name_is_a_string(self):
        """A non-string key is refused."""
        with pytest.raises(TypeError, match="column name"):
            Condition.of({1: "a"})  # pyright: ignore[reportArgumentType]

    @pytest.mark.parametrize("value", [True, 1, 1.5, "a", b"a", _DAY, _START, _AWARE])
    def test_each_supported_type_is_taken(self, value):
        """Each supported type is taken as it is, never converted."""
        condition = Condition.of(column=value)

        assert type(condition.equalities["column"]) is type(value)
        assert condition.equalities["column"] == value


class TestCombined:
    """``combined`` requires both; a contradiction is refused."""

    def test_columns_of_both_are_required(self):
        """The result requires every column of both, the first's first."""
        combined = Condition.of(unit=106, session=2).combined(
            Condition.of(label="a", flag=True)
        )

        assert list(combined.equalities) == ["unit", "session", "label", "flag"]

    def test_a_repeated_column_is_kept_once(self):
        """The same column with an equal value is one requirement."""
        combined = Condition.of(unit=106, x=0.0).combined(
            Condition.of(x=-0.0, unit=106, session=2)
        )

        assert combined == Condition.of(unit=106, session=2, x=0.0)
        assert list(combined.equalities) == ["unit", "x", "session"]

    @pytest.mark.parametrize(
        "first, second",
        [(106, 107), (1, True), (1, 1.0), ("m1", "m2"), (_START, _AWARE)],
    )
    def test_a_conflicting_value_is_refused(self, first, second):
        """Another value (another type counts) for one column is refused, naming it."""
        with pytest.raises(ValueError, match=r"unit.*already required"):
            Condition.of(session=2, unit=first).combined(Condition.of(unit=second))

    def test_combining_leaves_both_unchanged(self):
        """Conditions are immutable values."""
        first, second = Condition.of(unit=106), Condition.of(session=2)

        first.combined(second)

        assert dict(first.equalities) == {"unit": 106}
        assert dict(second.equalities) == {"session": 2}


class TestEquality:
    """Equality is by value and order-free; the hash agrees with it."""

    @pytest.mark.parametrize(
        "first, second",
        [
            (Condition.of(unit=106, session=2), Condition.of(session=2, unit=106)),
            (Condition.of(x=0.0), Condition.of(x=-0.0)),
            (Condition.of(x=math.nan), Condition.of(x=float("nan"))),
            (
                Condition.of(x=math.nan, unit=106),
                Condition.of(unit=106, x=math.nan),
            ),
            (Condition.of(day=_DAY), Condition.of(day=dt.date(2026, 11, 20))),
            (Condition(), Condition()),
            (Condition.of(at=_AWARE), Condition.of(at=_AWARE.astimezone(_NEW_YORK))),
            (
                Condition.of(at=_SECOND_0130, unit=106),
                Condition.of(unit=106, at=_SECOND_0130.astimezone(dt.timezone.utc)),
            ),
            (
                Condition.of(at=_SECOND_0130),
                Condition.of(at=dt.datetime.fromisoformat(_SECOND_0130.isoformat())),
            ),
        ],
        ids=[
            "order",
            "0.0 and -0.0",
            "NaN",
            "NaN and order",
            "date",
            "empty",
            "one instant in two zones",
            "an overlap instant in UTC, in another order",
            "an overlap instant reloaded with a fixed offset",
        ],
    )
    def test_equal_conditions_and_their_hashes(self, first, second):
        """Equal conditions have equal hashes."""
        assert first == second
        assert hash(first) == hash(second)
        assert len({first, second}) == 1

    @pytest.mark.parametrize(
        "first, second",
        [
            (Condition.of(x=1), Condition.of(x=True)),
            (Condition.of(x=1), Condition.of(x=1.0)),
            (Condition.of(x=True), Condition.of(x=1.0)),
            (Condition.of(x=_START), Condition.of(x=_AWARE)),
            (Condition.of(x="1"), Condition.of(x=b"1")),
            (Condition.of(x=1), Condition.of(x=1, y=2)),
            (Condition.of(x=1), Condition.of(y=1)),
            (Condition.of(x=1), Condition()),
        ],
        ids=[
            "1 and True",
            "1 and 1.0",
            "True and 1.0",
            "naive and aware",
            "str and bytes",
            "a column more",
            "another column",
            "empty",
        ],
    )
    def test_different_conditions(self, first, second):
        """Values of different types, or other columns, are different conditions."""
        assert first != second
        assert len({first, second}) == 2

    def test_not_equal_to_a_mapping(self):
        """A condition is not its mapping."""
        assert Condition.of(x=1) != {"x": 1}

    def test_immutable(self):
        """No attribute can be set, and ``equalities`` is read-only."""
        condition = Condition.of(x=1)

        with pytest.raises(AttributeError):
            condition.x = 2  # pyright: ignore[reportAttributeAccessIssue]
        with pytest.raises(TypeError):
            condition.equalities["x"] = 2  # pyright: ignore[reportIndexIssue]


class TestText:
    """The text form is a method and ``str``."""

    def test_text_names_each_column_and_value(self):
        """``text()`` and ``str`` read ``a='x', b=1``, in the order given."""
        condition = Condition.of(label="a", unit=106, day=_DAY)

        expected = "label='a', unit=106, day=datetime.date(2026, 11, 20)"
        assert condition.text() == expected
        assert str(condition) == expected

    def test_empty_text_and_truth(self):
        """The empty condition reads ``no condition`` and is false."""
        assert Condition().text() == "no condition"
        assert not Condition()
        assert Condition.of(x=1)

    def test_repr_rebuilds(self):
        """``repr`` is a call that builds an equal condition."""
        condition = Condition.of(label="a", unit=106)

        assert eval(repr(condition), {"Condition": Condition}) == condition  # noqa: S307  # the test's own repr


class TestSavedForm:
    """``to_config`` / ``from_config``: the saved ``where`` mapping, unchanged on disk."""

    def test_the_saved_form_is_the_mapping(self):
        """Dates, datetimes and bytes are tagged; other values are as they are."""
        condition = Condition.of(
            label="a", unit=106, x=1.5, flag=True, day=_DAY, at=_AWARE, raw=b"\x01"
        )

        assert condition.to_config() == {
            "label": "a",
            "unit": 106,
            "x": 1.5,
            "flag": True,
            "day": {"date": "2026-11-20"},
            "at": {"datetime": "2026-11-20T09:30:00+00:00"},
            "raw": {"bytes": "01"},
        }

    @pytest.mark.parametrize(
        "condition",
        [
            Condition.of(label="a", unit=106, x=-0.0, flag=False),
            Condition.of(day=_DAY, at=_AWARE, naive=_START, raw=b"ab"),
        ],
        ids=["scalars", "dates, times and bytes"],
    )
    def test_round_trip_through_json(self, condition):
        """A condition written and read back through JSON is equal, types included."""
        saved = json.loads(json.dumps(condition.to_config()))

        loaded = Condition.from_config(saved)

        assert loaded == condition
        assert [type(v) for v in loaded.equalities.values()] == [
            type(v) for v in condition.equalities.values()
        ]

    def test_an_empty_mapping_reads_as_the_empty_condition(self):
        """A source saved without ``where`` reads as no condition."""
        assert Condition.from_config({}) == Condition()


class TestInstants:
    """An aware datetime is its instant (CodeRabbit, PR 435): fold counts, zones do not."""

    def test_the_two_times_of_an_overlap_are_two_values(self):
        assert _FIRST_0130 == _SECOND_0130  # Python's wall-time comparison
        first, second = Condition.of(at=_FIRST_0130), Condition.of(at=_SECOND_0130)
        assert first != second
        assert len({first, second}) == 2

    @pytest.mark.parametrize(
        "first, second",
        [(_FIRST_0130, _SECOND_0130), (_SECOND_0130, _FIRST_0130)],
        ids=["first then second", "second then first"],
    )
    def test_combining_the_two_times_of_an_overlap_is_refused(self, first, second):
        with pytest.raises(ValueError, match="cannot also equal"):
            Condition.of(at=first, unit=106).combined(Condition.of(unit=106, at=second))
        with pytest.raises(ValueError, match="as a keyword"):
            Condition.of({"at": first}, at=second)

    @pytest.mark.parametrize(
        "other",
        [
            _SECOND_0130.astimezone(dt.timezone.utc),
            dt.datetime.fromisoformat(_SECOND_0130.isoformat()),
        ],
        ids=["UTC", "fixed offset"],
    )
    def test_one_instant_in_another_zone_is_kept_once(self, other):
        combined = Condition.of(at=_SECOND_0130, unit=106).combined(
            Condition.of(unit=106, at=other)
        )
        assert dict(combined.equalities) == {"at": _SECOND_0130, "unit": 106}
        assert Condition.of({"at": _SECOND_0130}, at=other) == Condition.of(
            at=_SECOND_0130
        )

    def test_the_saved_form_reads_back_as_the_same_instant(self):
        condition = Condition.of(at=_SECOND_0130, unit=106)
        loaded = Condition.from_config(json.loads(json.dumps(condition.to_config())))
        assert loaded == condition
        assert hash(loaded) == hash(condition)

    @pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
    @pytest.mark.parametrize(
        "value", [_FIRST_0130, _SECOND_0130], ids=["fold 0", "fold 1"]
    )
    def test_a_value_handed_back_by_arrow_has_the_same_instant(self, unit, value):
        """Arrow hands back a nanosecond timestamp as a ``pandas.Timestamp``, which
        cannot reach year 1: the instant is computed from calendar fields."""
        import pyarrow as pa

        from orcapod.utils.datetime_utils import instant_microseconds

        stored = pa.scalar(
            value, type=pa.timestamp(unit, tz="America/New_York")
        ).as_py()
        assert isinstance(stored, dt.datetime)
        assert instant_microseconds(stored) == instant_microseconds(value)
        assert instant_microseconds(value) == (
            instant_microseconds(value.astimezone(dt.timezone.utc))
        )

    @pytest.mark.parametrize(
        "value, expected",
        [
            (dt.datetime(1, 1, 1, tzinfo=dt.timezone.utc), 0),
            (
                dt.datetime(1, 1, 1, 1, tzinfo=dt.timezone(dt.timedelta(hours=1))),
                0,
            ),
            (
                dt.datetime(9999, 12, 31, 23, 59, 59, 999_999, tzinfo=dt.timezone.utc),
                (dt.date(9999, 12, 31).toordinal() * 86_400) * 1_000_000 - 1,
            ),
        ],
        ids=["year 1", "year 1 at +01:00", "year 9999"],
    )
    def test_the_ends_of_the_range(self, value, expected):
        from orcapod.utils.datetime_utils import instant_microseconds

        assert instant_microseconds(value) == expected

    def test_a_naive_time_is_not_an_instant(self):
        assert Condition.of(at=_START) != Condition.of(at=_AWARE)
        assert Condition.of(at=_START) == Condition.of(at=_START.replace(fold=1))


_NON_FINITE = [math.nan, math.inf, -math.inf]


class TestNonFiniteFloats:
    """A NaN or infinite value is accepted and saved as strict JSON (developer, 2026-11-19).

    Which rows it selects is the table system's equality, not Orcapod's (documented);
    Condition equality, which decides whether two restrictions are one, keeps NaN equal
    to NaN and 0.0 equal to -0.0.
    """

    @pytest.mark.parametrize("value", _NON_FINITE, ids=["NaN", "inf", "-inf"])
    @pytest.mark.parametrize(
        "build",
        [
            lambda v: Condition.of(x=v, unit=106, label="a"),
            lambda v: Condition.of(unit=106, label="a", x=v),
            lambda v: Condition.of({"unit": 106, "x": v}),
            lambda v: Condition.of({"x": v}, unit=106),
            lambda v: Condition.of(unit=106).combined(Condition.of(x=v)),
        ],
        ids=["first keyword", "last keyword", "mapping", "mapping first", "combined"],
    )
    def test_round_trips_through_strict_json(self, build, value):
        condition = build(value)
        text = json.dumps(condition.to_config(), allow_nan=False)
        loaded = Condition.from_config(json.loads(text))
        assert loaded == condition
        assert hash(loaded) == hash(condition)
        assert condition.to_config()["x"] == {
            "float": "nan" if value != value else ("inf" if value > 0 else "-inf")
        }

    def test_a_file_python_wrote_with_bare_nan_still_reads(self):
        """``json.dumps`` without ``allow_nan=False`` writes ``NaN`` / ``Infinity``."""
        saved = json.loads('{"x": NaN, "y": Infinity, "z": -Infinity, "unit": 106}')
        assert Condition.from_config(saved) == Condition.of(
            x=math.nan, y=math.inf, z=-math.inf, unit=106
        )

    def test_finite_values_keep_their_saved_form(self):
        condition = Condition.of(x=1.5, y=-0.0, z=1e300)
        assert condition.to_config() == {"x": 1.5, "y": -0.0, "z": 1e300}
        assert json.loads(json.dumps(condition.to_config(), allow_nan=False)) == (
            condition.to_config()
        )

    @pytest.mark.parametrize(
        "first, second",
        [(math.inf, -math.inf), (math.nan, math.inf), (math.nan, 0.0)],
        ids=["inf and -inf", "NaN and inf", "NaN and 0.0"],
    )
    def test_different_values_conflict(self, first, second):
        with pytest.raises(ValueError, match="cannot also equal"):
            Condition.of(x=first).combined(Condition.of(x=second))


class TestRangeEndsAndAwareness:
    """Instants at the ends of the datetime range; awareness by ``utcoffset``."""

    @pytest.mark.parametrize(
        "value",
        [
            dt.datetime(1, 1, 1, tzinfo=dt.timezone(dt.timedelta(hours=1))),
            dt.datetime(
                9999,
                12,
                31,
                23,
                59,
                59,
                999999,
                tzinfo=dt.timezone(dt.timedelta(hours=-1)),
            ),
        ],
        ids=["year 1, +01:00", "year 9999, -01:00"],
    )
    def test_a_time_whose_utc_instant_leaves_the_range_compares_and_hashes(self, value):
        condition = Condition.of(at=value, unit=106)
        assert condition == Condition.of(unit=106, at=value)
        assert hash(condition) == hash(Condition.of(at=value, unit=106))
        assert condition.combined(Condition.of(at=value)) == condition
        shifted = value.replace(tzinfo=dt.timezone(dt.timedelta(0)))
        assert Condition.of(at=value) != Condition.of(at=shifted)

    def test_a_tzinfo_with_no_offset_is_naive(self):
        class NoOffset(dt.tzinfo):
            def utcoffset(self, _dt):
                return None

            def dst(self, _dt):
                return None

        naive_ish = _START.replace(tzinfo=NoOffset())
        assert Condition.of(at=naive_ish) == Condition.of(at=_START)
        assert Condition.of(at=naive_ish) != Condition.of(at=_AWARE)


class TestImmutable:
    def test_init_runs_once(self):
        condition = Condition.of(unit=106, session=2)
        with pytest.raises(TypeError, match="immutable"):
            condition.__init__()
        assert dict(condition.equalities) == {"unit": 106, "session": 2}
