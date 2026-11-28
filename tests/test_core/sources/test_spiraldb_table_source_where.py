"""``SpiralDBTableSource.where()`` restricts which rows flow and changes nothing else (ITL-1218).

A restriction is not an operator: it is attached to the source, chooses which of its rows
are read, and leaves the source's identity, schema, system tags and every row's record id
as they are. Runs against the disk-backed pyspiral fake.
"""

from __future__ import annotations

import asyncio
import copy
import datetime as dt
import json
import math
from decimal import Decimal
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import InconsistentSourceError, RestrictedAggregationWarning
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.restrictions import restrictions_in_force
from orcapod.core.sources.base import RootSource
from orcapod.core.sources.cached_source import CachedSource
from orcapod.sources import SpiralDBTableSource
from orcapod.core.operators import MapData
from orcapod.types import CacheMode, Condition, col
from orcapod.types.condition import check_equality_conditions
from tests.test_databases import fake_spiral_sdk

_PROJECT = "workshop-sample"
_KEYS = pa.schema(
    [pa.field("subject_id", pa.string()), pa.field("timestamp", pa.uint64())]
)
_DAY = dt.date(2026, 11, 20)
_START = dt.datetime(2026, 11, 20, 9, 30)


def _rows(subjects: list[str], timestamps: list[int]) -> pa.Table:
    """Return ``event`` rows: two key columns and data of several types."""
    return pa.table(
        {
            "subject_id": subjects,
            "timestamp": pa.array(timestamps, pa.uint64()),
            "amplitude": pa.array([t + 0.5 for t in timestamps], pa.float32()),
            "channel": pa.array([t % 2 for t in timestamps], pa.int16()),
            "note": [f"n{t}" for t in timestamps],
            "day": pa.array([_DAY] * len(timestamps), pa.date32()),
            "started": pa.array([_START] * len(timestamps), pa.timestamp("us")),
            "flag": [t > 2 for t in timestamps],
            "trace": pa.array([[float(t)] for t in timestamps], pa.list_(pa.float64())),
        }
    )


@pytest.fixture()
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Provide a fake project holding a 5-row ``event`` table read 2 rows per batch."""
    monkeypatch.setattr(fake_spiral_sdk, "scan_batch_rows", 2)
    with fake_spiral_sdk.install(tmp_path / "spiral"):
        project = fake_spiral_sdk.ExternalProject(_PROJECT)
        project.create_table("default.event", key_schema=_KEYS).write(
            _rows(["m1", "m1", "m1", "m2", "m2"], [1, 2, 3, 4, 5])
        )
        fake_spiral_sdk.scan_log.clear()
        yield project
        fake_spiral_sdk.scan_log.clear()


def _source(**kwargs: Any) -> SpiralDBTableSource:
    """Return a source over ``event`` with ``kwargs``."""
    return SpiralDBTableSource(_PROJECT, "event", **kwargs)


def _timestamps(source: RootSource) -> list[int]:
    """Return the ``timestamp`` key of every row ``source`` reads, in order."""
    return cast("list[int]", source.as_table().column("timestamp").to_pylist())


def _rows_by_key(source: SpiralDBTableSource) -> dict[int, dict[str, Any]]:
    """Return every column of every row (system tags, source info), by timestamp."""
    return {row["timestamp"]: row for row in source.as_table(all_info=True).to_pylist()}


class TestSurface:
    """``where`` returns a new source; the original is unchanged."""

    def test_returns_a_new_source_and_leaves_the_original(self, project):
        """The new source reads the matching rows; the original still reads all."""
        source = _source()

        restricted = source.where(subject_id="m2")

        assert isinstance(restricted, SpiralDBTableSource)
        assert restricted is not source
        assert _timestamps(restricted) == [4, 5]
        assert _timestamps(source) == [1, 2, 3, 4, 5]
        assert source.to_config()["where"] == {}

    def test_condition_reaches_the_scan(self, project):
        """The condition is applied by SpiralDB in the scan, as ``where=`` is."""
        _source().where(subject_id="m2").as_table()

        assert (
            repr(fake_spiral_sdk.scan_log[-1].kwargs["where"]) == "subject_id == 'm2'"
        )

    def test_mapping_and_keywords_combine(self, project):
        """A mapping and keywords are one set of conditions, all of which must hold."""
        restricted = _source().where({"subject_id": "m1"}, timestamp=2)

        assert _timestamps(restricted) == [2]

    def test_a_column_named_mapping_can_be_given_as_a_keyword(self, project):
        """The mapping is positional-only, so any column name is a keyword."""
        project.create_table(
            "default.odd", key_schema=pa.schema([pa.field("mapping", pa.string())])
        ).write(pa.table({"mapping": ["a", "b"], "n": [1, 2]}))
        source = SpiralDBTableSource(_PROJECT, "odd")

        assert source.where(mapping="b").as_table().column("n").to_pylist() == [2]

    def test_chained_calls_combine(self, project):
        """A second ``where`` adds its conditions to the first's."""
        restricted = _source().where(subject_id="m1").where(flag=True)

        assert _timestamps(restricted) == [3]

    def test_adds_to_the_constructor_where(self, project):
        """``where()`` adds to the conditions given to the constructor: one mechanism."""
        restricted = _source(where={"subject_id": "m1"}).where(channel=0)

        assert _timestamps(restricted) == [2]
        assert restricted.to_config()["where"] == {"subject_id": "m1", "channel": 0}

    def test_repeating_a_condition_is_allowed(self, project):
        """A column given again with the same value is the same condition."""
        restricted = _source(where={"subject_id": "m1"}).where(subject_id="m1")

        assert _timestamps(restricted) == [1, 2, 3]

    def test_limit_reads_the_first_matching_rows(self, project):
        """The constructor's ``limit`` reads the first rows that match, in key order."""
        restricted = _source(limit=2).where(subject_id="m2")

        assert _timestamps(restricted) == [4, 5]
        assert _timestamps(_source(limit=1).where(subject_id="m2")) == [4]

    @pytest.mark.parametrize(
        "column, value, expected",
        [
            ("amplitude", 3.5, [3]),
            ("note", "n4", [4]),
            ("day", _DAY, [1, 2, 3, 4, 5]),
            ("started", _START, [1, 2, 3, 4, 5]),
            ("flag", False, [1, 2]),
        ],
    )
    def test_each_scalar_type_can_be_matched(self, project, column, value, expected):
        """A value of the column's own Python type matches."""
        assert _timestamps(_source().where({column: value})) == expected


_Restrict = Callable[[SpiralDBTableSource], SpiralDBTableSource]

# One restriction (subject_id "m1" and flag True), given in every form ``where`` takes.
_FORMS_OF_ONE_RESTRICTION: dict[str, _Restrict] = {
    "keywords": lambda s: s.where(subject_id="m1", flag=True),
    "keywords in the other order": lambda s: s.where(flag=True, subject_id="m1"),
    "mapping": lambda s: s.where({"subject_id": "m1", "flag": True}),
    "mapping and keyword": lambda s: s.where({"flag": True}, subject_id="m1"),
    "chained": lambda s: s.where(subject_id="m1").where(flag=True),
    "chained in the other order": lambda s: s.where(flag=True).where(subject_id="m1"),
    "a restriction": lambda s: s.where(Condition.of(subject_id="m1", flag=True)),
    "a restriction in the other order": lambda s: s.where(
        Condition.of({"flag": True, "subject_id": "m1"})
    ),
    "a restriction and a keyword": lambda s: s.where(
        Condition.of(flag=True), subject_id="m1"
    ),
    "restrictions chained": lambda s: s.where(Condition.of(flag=True)).where(
        Condition.of(subject_id="m1")
    ),
}

# The same restriction with a condition of a several-keyword call given twice.
_REPEATED_CONDITIONS: dict[str, _Restrict] = {
    "keywords then chained": lambda s: s.where(subject_id="m1", flag=True).where(
        subject_id="m1"
    ),
    "chained then keywords": lambda s: s.where(subject_id="m1").where(
        subject_id="m1", flag=True
    ),
    "mapping repeats": lambda s: s.where(flag=True).where(
        {"subject_id": "m1", "flag": True}
    ),
}


class TestSeveralConditionsInOneCall:
    """``where(a=x, b=y)``: every condition of one call must hold.

    Rows of ``event``: ``subject_id="m1"`` alone lets 1, 2, 3 through and ``flag=True``
    alone 3, 4, 5, so only row 3 holds both: a dropped condition lets more rows through.
    """

    def test_two_keywords_read_only_the_rows_where_both_hold(self, project):
        """Each condition alone lets more rows through than both together."""
        assert _timestamps(_source().where(subject_id="m1")) == [1, 2, 3]
        assert _timestamps(_source().where(flag=True)) == [3, 4, 5]

        assert _timestamps(_source().where(subject_id="m1", flag=True)) == [3]

    def test_three_keywords_read_only_the_rows_where_all_hold(self, project):
        """With three conditions, leaving out any one lets more rows through."""
        project.create_table(
            "default.grid", key_schema=pa.schema([pa.field("id", pa.string())])
        ).write(
            pa.table(
                {
                    "id": [
                        f"r{x}{y}{z}" for x in (0, 1) for y in (0, 1) for z in (0, 1)
                    ],
                    "x": [x for x in (0, 1) for _ in (0, 1) for _ in (0, 1)],
                    "y": [y for _ in (0, 1) for y in (0, 1) for _ in (0, 1)],
                    "z": [z for _ in (0, 1) for _ in (0, 1) for z in (0, 1)],
                }
            )
        )
        grid = SpiralDBTableSource(_PROJECT, "grid")

        def ids(source: SpiralDBTableSource) -> list[str]:
            """Return the ``id`` of every row ``source`` reads, in order."""
            return cast("list[str]", source.as_table().column("id").to_pylist())

        assert ids(grid.where(x=1, y=0, z=1)) == ["r101"]
        assert ids(grid.where(y=0, z=1)) == ["r001", "r101"]
        assert ids(grid.where(x=1, z=1)) == ["r101", "r111"]
        assert ids(grid.where(x=1, y=0)) == ["r100", "r101"]

    @pytest.mark.parametrize(
        "conditions, expression",
        [
            (
                {"subject_id": "m1", "flag": True},
                "subject_id == 'm1' & flag == True",
            ),
            (
                {"flag": True, "subject_id": "m1"},
                "flag == True & subject_id == 'm1'",
            ),
        ],
    )
    def test_two_keywords_reach_the_scan_in_the_order_given(
        self, project, conditions, expression
    ):
        """SpiralDB receives one expression joining both equalities, in call order."""
        _source().where(**conditions).as_table()

        assert repr(fake_spiral_sdk.scan_log[-1].kwargs["where"]) == expression

    @pytest.mark.parametrize("form", list(_FORMS_OF_ONE_RESTRICTION))
    def test_every_form_is_the_same_restriction(self, project, form):
        """Keywords, a mapping, both, or a chain: the same rows, config and identity."""
        reference = _source().where(subject_id="m1", flag=True)

        restricted = _FORMS_OF_ONE_RESTRICTION[form](_source())

        assert _rows_by_key(restricted) == _rows_by_key(reference)
        assert restricted.to_config() == reference.to_config()
        assert restricted.content_hash() == reference.content_hash()
        assert restricted.pipeline_hash() == reference.pipeline_hash()

    def test_two_keywords_are_saved_in_the_config(self, project):
        """``to_config()["where"]`` holds both conditions, each as given."""
        restricted = _source().where(subject_id="m1", day=_DAY)

        assert restricted.to_config()["where"] == {
            "subject_id": "m1",
            "day": {"date": _DAY.isoformat()},
        }

    @pytest.mark.parametrize(
        "conditions, column",
        [
            ({"subject_id": "m1", "channel": 1.0}, "channel"),
            ({"subject_id": b"m1", "channel": 1}, "subject_id"),
            ({"flag": True, "subject_id": "m1", "amplitude": True}, "amplitude"),
        ],
    )
    def test_a_value_of_another_type_among_several_is_refused(
        self, project, conditions, column
    ):
        """One wrong value refuses the whole call, naming its column; nothing is read."""
        source = _source()

        with pytest.raises(TypeError, match=rf"{column}.*convert"):
            source.where(**conditions)
        assert fake_spiral_sdk.scan_log == []
        assert source.to_config()["where"] == {}

    @pytest.mark.parametrize("form", list(_REPEATED_CONDITIONS))
    def test_repeating_a_condition_across_calls_is_allowed(self, project, form):
        """A condition of a several-keyword call given again with its value is kept once."""
        restricted = _REPEATED_CONDITIONS[form](_source())

        assert _timestamps(restricted) == [3]
        assert restricted.to_config()["where"] == {"subject_id": "m1", "flag": True}

    def test_a_conflict_with_a_several_keyword_call_is_refused(self, project):
        """A later call giving one of the columns another value is refused, naming it."""
        restricted = _source().where(subject_id="m1", flag=True)

        with pytest.raises(ValueError, match=r"flag.*True.*False"):
            restricted.where(flag=False)


class TestRefusals:
    """What ``where`` refuses, before anything is read."""

    def test_no_condition_is_refused(self, project):
        """``where()`` with no condition is refused."""
        with pytest.raises(ValueError, match="at least one"):
            _source().where()

    def test_a_conflicting_value_is_refused(self, project):
        """A column already restricted to another value is refused, naming both."""
        with pytest.raises(ValueError, match=r"subject_id.*'m1'.*'m2'"):
            _source().where(subject_id="m1").where(subject_id="m2")

    def test_a_conflict_with_the_constructor_where_is_refused(self, project):
        """The constructor's conditions count as already given."""
        with pytest.raises(ValueError, match="subject_id"):
            _source(where={"subject_id": "m1"}).where(subject_id="m2")

    def test_a_conflict_between_mapping_and_keyword_is_refused(self, project):
        """The same column in the mapping and as a keyword must agree."""
        with pytest.raises(ValueError, match="subject_id"):
            _source().where({"subject_id": "m1"}, subject_id="m2")

    def test_an_unknown_column_is_refused(self, project):
        """A column the table does not have is refused, naming it and the table."""
        with pytest.raises(ValueError, match=r"event.*nope"):
            _source().where(nope=1)

    @pytest.mark.parametrize(
        "column, value, value_type",
        [
            ("channel", True, "bool"),
            ("channel", 1.0, "float"),
            ("channel", np.int64(1), "int64"),
            ("amplitude", True, "bool"),
            ("note", b"n1", "bytes"),
            ("day", _START, "datetime"),
            ("started", "2026-11-20T09:30:00", "str"),
            ("flag", 1, "int"),
        ],
    )
    def test_a_value_of_another_type_is_refused(
        self, project, column, value, value_type
    ):
        """No implicit conversion: the error names the column, its type and the value's."""
        with pytest.raises(TypeError, match=rf"{column}.*{value_type}.*convert"):
            _source().where({column: value})

    def test_the_constructor_refuses_a_value_of_another_type(self, project):
        """The constructor's ``where`` is checked by the same rule."""
        with pytest.raises(TypeError, match="channel"):
            _source(where={"channel": "1"})

    @pytest.mark.parametrize("value", [1e300, 0.1])
    def test_an_inexact_float_is_refused_by_the_equality_check(self, value):
        """The equality check refuses both overflow and lost precision."""
        schema = pa.schema([pa.field("x", pa.float32())])

        with pytest.raises(TypeError, match="cannot hold .* exactly"):
            check_equality_conditions({"x": value}, schema)

    @pytest.mark.parametrize("value", [1e300, -1e300])
    def test_a_float_overflow_is_refused_by_where_and_the_constructor(
        self, project, value
    ):
        """``amplitude`` is float32: a value beyond its range is refused either way."""
        with pytest.raises(TypeError, match="amplitude"):
            _source().where(amplitude=value)
        with pytest.raises(TypeError, match="amplitude"):
            _source(where={"amplitude": value})

    def test_an_out_of_range_integer_is_refused(self, project):
        """An integer the column's Arrow type cannot hold is refused."""
        with pytest.raises(TypeError, match="channel"):
            _source().where(channel=70_000)

    def test_a_naive_time_for_a_zoned_column_is_refused(self, project):
        """A plain timestamp column takes a naive datetime only."""
        with pytest.raises(TypeError, match="naive"):
            _source().where(started=_START.replace(tzinfo=dt.timezone.utc))

    def test_a_zoned_column_takes_an_aware_time_only(self):
        """A time zone column takes an aware datetime only (the rule itself)."""
        schema = pa.schema([pa.field("at", pa.timestamp("us", tz="UTC"))])
        aware = _START.replace(tzinfo=dt.timezone.utc)

        # About the condition check itself: the pyspiral fake keeps no time zone.
        assert check_equality_conditions({"at": aware}, schema) == {"at": aware}
        with pytest.raises(TypeError, match="aware"):
            check_equality_conditions({"at": _START}, schema)

    @pytest.mark.parametrize("fold", [0, 1])
    @pytest.mark.parametrize("zone", ["America/New_York", "UTC"])
    def test_a_time_in_a_daylight_saving_overlap_is_accepted(self, fold, zone):
        """Either 01:30 of an overlap is a time a zoned column holds (compared by
        instant: the column hands it back in its own zone), beside another column."""
        import zoneinfo

        schema = pa.schema(
            [
                pa.field("at", pa.timestamp("us", tz=zone)),
                pa.field("unit", pa.int64()),
            ]
        )
        local = dt.datetime(
            2026, 11, 1, 1, 30, fold=fold, tzinfo=zoneinfo.ZoneInfo("America/New_York")
        )

        # About the condition check itself: the pyspiral fake keeps no time zone.
        for conditions in ({"at": local, "unit": 1}, {"unit": 1, "at": local}):
            assert check_equality_conditions(conditions, schema) == conditions

    @pytest.mark.parametrize("value", [math.inf, -math.inf, 0.0])
    @pytest.mark.parametrize(
        "restrict",
        [
            lambda v: _source(where={"amplitude": v, "subject_id": "m1"}),
            lambda v: _source(where={"subject_id": "m1", "amplitude": v}),
            lambda v: _source().where(subject_id="m1", amplitude=v),
            lambda v: _source().where(amplitude=v, subject_id="m1"),
            lambda v: _source().where(subject_id="m1").where(amplitude=v),
            lambda v: _source().where(
                Condition.of({"subject_id": "m1", "amplitude": v})
            ),
        ],
        ids=[
            "where= first",
            "where= last",
            "where() last",
            "where() first",
            "where() chained",
            "Condition.of mapping",
        ],
    )
    def test_an_infinite_float_or_zero_is_pushed_down_as_given(
        self, project, restrict, value
    ):
        """Accepted, and sent to the table system as ``column == value``: which rows
        it selects is that system's equality (developer decision, 2026-11-19). ``-0.0``
        and NaN are refused (ITL-1480; ``test_spiraldb_table_source_float_equalities``)."""
        source = restrict(value)
        source.as_table()

        pushed = dict(fake_spiral_sdk.scan_log[-1].kwargs["where"].equalities)
        assert set(pushed) == {"amplitude", "subject_id"}
        sent = pushed["amplitude"]
        assert (
            sent != sent
            if value != value
            else (sent == value and math.copysign(1, sent) == math.copysign(1, value))
        )
        saved = json.dumps(source.to_config(), allow_nan=False)
        assert SpiralDBTableSource.from_config(json.loads(saved)).to_config() == (
            source.to_config()
        )

    def test_a_time_finer_than_the_column_is_refused(self):
        """A value the column would truncate is refused, not cut."""
        schema = pa.schema([pa.field("at", pa.timestamp("s"))])

        with pytest.raises(TypeError, match="exactly"):
            check_equality_conditions({"at": _START.replace(microsecond=5)}, schema)

    def test_none_is_refused(self, project):
        """``None`` matches no row (a null equals nothing), so it is refused."""
        with pytest.raises(ValueError, match="None"):
            _source().where(note=None)

    def test_a_column_of_an_unsupported_type_is_refused(self, project):
        """A list column cannot be matched by equality here."""
        with pytest.raises(TypeError, match="trace"):
            _source().where(trace=[1.0])

    def test_a_tags_as_data_copy_is_not_a_table_column(self, project):
        """A copy made by ``tags_as_data`` is not a column of the table."""
        source = _source(tags_as_data={"timestamp": "time"})

        with pytest.raises(ValueError, match="time"):
            source.where(time=1)

    def test_nothing_is_read_when_refusing(self, project):
        """A refused ``where`` scans nothing."""
        with pytest.raises(TypeError):
            _source().where(channel="0")

        assert fake_spiral_sdk.scan_log == []


_UNITS: list[Any] = ["s", "ms", "us", "ns"]  # pyarrow types the unit as a literal
_ZONE = "America/New_York"
# 01:30 on 2026-11-01 happens twice in New York: fold 0 (EDT, 05:30 UTC) and fold 1
# (EST, 06:30 UTC). A naive column holds one 01:30 for both.
_WALL = dt.datetime(2026, 11, 1, 1, 30)


def _local(fold: int) -> dt.datetime:
    import zoneinfo

    return _WALL.replace(fold=fold, tzinfo=zoneinfo.ZoneInfo(_ZONE))


def _typed_rows() -> pa.Table:
    """Key ``k`` 0, 1, 2: every timestamp unit zoned and zone-free, and the other
    column types a condition can match."""
    import zoneinfo

    zone = zoneinfo.ZoneInfo(_ZONE)
    later = _WALL.replace(hour=3)
    columns: dict[str, Any] = {"k": pa.array([0, 1, 2], pa.int64())}
    for unit in _UNITS:
        columns[f"naive_{unit}"] = pa.array([_WALL, _WALL, later], pa.timestamp(unit))
        columns[f"zoned_{unit}"] = pa.array(
            [_local(0), _local(1), later.replace(tzinfo=zone)],
            pa.timestamp(unit, tz=_ZONE),
        )
    one, two = dt.date(2026, 1, 1), dt.date(2026, 1, 2)
    columns.update(
        {
            "d64": pa.array([one, two, one], pa.date64()),
            "bin": pa.array([b"a", b"b", b"a"], pa.binary()),
            "i8": pa.array([-1, 2, -1], pa.int8()),
            "u8": pa.array([255, 2, 255], pa.uint8()),
            "i32": pa.array([1, 2, 1], pa.int32()),
            "u32": pa.array([1, 2, 1], pa.uint32()),
            "i64": pa.array([1, 2, 1], pa.int64()),
            "f64": pa.array([1.5, 2.5, 1.5], pa.float64()),
            "t32": pa.array([dt.time(1), dt.time(2), dt.time(1)], pa.time32("s")),
            "t64": pa.array([dt.time(1), dt.time(2), dt.time(1)], pa.time64("us")),
            "dec": pa.array([Decimal("1.50")] * 3, pa.decimal128(5, 2)),
        }
    )
    return pa.table(columns)


@pytest.fixture()
def typed(tmp_path: Path):
    """Provide a ``typed`` table in a fake that keeps time zones."""
    root = tmp_path / "typed"
    with fake_spiral_sdk.install(root):
        project = fake_spiral_sdk.ExternalProject(_PROJECT)
        project.create_table(
            "default.typed", key_schema=pa.schema([pa.field("k", pa.int64())])
        ).write(_typed_rows())
        yield SpiralDBTableSource(_PROJECT, "typed", tag_only_columns=True)


def _keys(source: SpiralDBTableSource) -> list[int]:
    return cast("list[int]", source.as_table().column("k").to_pylist())


@pytest.mark.filterwarnings("ignore:arrow_type_to_python_type")
class TestColumnTypes:
    """``where`` on every timestamp unit, zoned and zone-free, and the other column
    types (CodeRabbit, PR 435: Arrow hands back a nanosecond timestamp as a
    ``pandas.Timestamp``, which no instant computation may need to reach year 1)."""

    @pytest.mark.parametrize("unit", _UNITS)
    @pytest.mark.parametrize("fold", [0, 1])
    def test_a_zoned_column_matches_the_instant_of_either_fold(self, typed, unit, fold):
        assert _keys(typed.where({f"zoned_{unit}": _local(fold)})) == [fold]

    @pytest.mark.parametrize("unit", _UNITS)
    @pytest.mark.parametrize("fold", [0, 1])
    def test_an_aware_value_in_another_zone_matches_by_instant(self, typed, unit, fold):
        in_utc = _local(fold).astimezone(dt.timezone.utc)
        assert _keys(typed.where({f"zoned_{unit}": in_utc})) == [fold]

    @pytest.mark.parametrize("unit", _UNITS)
    @pytest.mark.parametrize("fold", [0, 1])
    def test_a_naive_column_holds_one_wall_time_for_both_folds(self, typed, unit, fold):
        assert _keys(typed.where({f"naive_{unit}": _WALL.replace(fold=fold)})) == [
            0,
            1,
        ]

    @pytest.mark.parametrize("unit", _UNITS)
    def test_the_wrong_kind_of_time_is_refused_for_every_unit(self, typed, unit):
        with pytest.raises(TypeError, match="aware"):
            typed.where({f"zoned_{unit}": _WALL})
        with pytest.raises(TypeError, match="naive"):
            typed.where({f"naive_{unit}": _local(0)})

    @pytest.mark.parametrize(
        "restrict",
        [
            lambda s, z, n: s.where(**{z: _local(1), n: _WALL}),
            lambda s, z, n: s.where(**{n: _WALL, z: _local(1)}),
            lambda s, z, n: s.where({n: _WALL}, **{z: _local(1)}),
            lambda s, z, n: s.where({z: _local(1)}).where({n: _WALL}),
            lambda s, z, n: s.where(Condition.of({n: _WALL, z: _local(1)})),
        ],
        ids=[
            "zoned first",
            "naive first",
            "mapping and keyword",
            "chained",
            "Condition",
        ],
    )
    @pytest.mark.parametrize(
        "zoned, naive", [("zoned_ns", "naive_s"), ("zoned_s", "naive_ns")]
    )
    def test_several_time_columns_in_every_order(self, typed, restrict, zoned, naive):
        assert _keys(restrict(typed, zoned, naive)) == [1]

    def test_two_zoned_units_and_a_key_together(self, typed):
        both = {"zoned_ms": _local(0), "zoned_ns": _local(0), "k": 0}
        assert _keys(typed.where(both)) == [0]
        assert _keys(typed.where(dict(reversed(both.items())))) == [0]
        assert _keys(typed.where(zoned_ms=_local(0), zoned_ns=_local(1))) == []

    @pytest.mark.parametrize(
        "column, value, expected",
        [
            ("d64", dt.date(2026, 1, 1), [0, 2]),
            ("bin", b"a", [0, 2]),
            ("i8", -1, [0, 2]),
            ("u8", 255, [0, 2]),
            ("i32", 2, [1]),
            ("u32", 1, [0, 2]),
            ("i64", 2, [1]),
            ("f64", 1.5, [0, 2]),
        ],
    )
    def test_each_other_type_can_be_matched(self, typed, column, value, expected):
        assert _keys(typed.where({column: value})) == expected
        assert _keys(typed.where({column: value, "k": expected[-1]})) == [expected[-1]]

    @pytest.mark.parametrize(
        "column, value, match",
        [
            ("i8", 128, "cannot hold"),
            ("u8", -1, "cannot hold"),
            ("u32", 2**32, "cannot hold"),
            ("t32", 1, "does not support"),
            ("t64", 1, "does not support"),
            ("dec", 1.5, "does not support"),
        ],
    )
    def test_a_value_the_column_cannot_take_is_refused(
        self, typed, column, value, match
    ):
        """Time of day, decimal and duration columns take no condition: a condition
        holds only a bool, int, float, str, bytes, datetime or date."""
        with pytest.raises(TypeError, match=match):
            typed.where({column: value})

    @pytest.mark.parametrize(
        "value", [dt.time(1), Decimal("1.50"), dt.timedelta(1)], ids=repr
    )
    def test_no_condition_holds_a_time_of_day_decimal_or_duration(self, value):
        with pytest.raises(TypeError, match="exactly a bool"):
            Condition.of(x=value)

    def test_a_float16_column_takes_a_float(self):
        """The check accepts it; which rows match is the table system's equality.
        (About the check itself: pyarrow, and so the fake, has no float16 equality.)"""
        schema = pa.schema([pa.field("h", pa.float16()), pa.field("k", pa.int64())])
        for conditions in ({"h": 1.5, "k": 1}, {"k": 1, "h": 1.5}):
            assert check_equality_conditions(conditions, schema) == conditions


# Two layouts of one table: three key columns (two timestamps and an integer), three
# data columns (two timestamps and a float), in different column and key orders.
_LAYOUTS: dict[str, tuple[list[str], list[str]]] = {
    "keys first": (["at", "wall", "k", "seen", "noted", "x"], ["at", "wall", "k"]),
    "keys last, reordered": (
        ["x", "noted", "seen", "k", "wall", "at"],
        ["k", "wall", "at"],
    ),
}


def _role_rows() -> dict[str, Any]:
    import zoneinfo

    later = _WALL.replace(hour=3)
    zoned_later = later.replace(tzinfo=zoneinfo.ZoneInfo(_ZONE))
    return {
        "at": pa.array(
            [_local(0), _local(1), zoned_later], pa.timestamp("ns", tz=_ZONE)
        ),
        "wall": pa.array([_WALL, _WALL, later], pa.timestamp("ms")),
        "k": pa.array([0, 1, 2], pa.int64()),
        "seen": pa.array(
            [_local(0), _local(1), zoned_later], pa.timestamp("s", tz=_ZONE)
        ),
        "noted": pa.array([_WALL, _WALL, later], pa.timestamp("us")),
        "x": pa.array([0.5, 1.5, 2.5]),
    }


@pytest.fixture(params=list(_LAYOUTS))
def roles(request, tmp_path: Path):
    """Provide a ``roles`` table (one layout per parameter) in a fake keeping zones."""
    columns, keys = _LAYOUTS[request.param]
    rows = _role_rows()
    table = pa.table({name: rows[name] for name in columns})
    root = tmp_path / "roles"
    with fake_spiral_sdk.install(root):
        fake_spiral_sdk.ExternalProject(_PROJECT).create_table(
            "default.roles", key_schema=pa.schema([table.schema.field(k) for k in keys])
        ).write(table)
        yield


def _sync_and_async_keys(source: SpiralDBTableSource) -> tuple[list[int], list[int]]:
    """The ``k`` of every row read, sync and async (sorted), after checking that the
    two paths yield the same rows."""

    def merged(tag: Any, data: Any) -> dict[str, Any]:
        return {**tag.as_dict(all_info=True), **data.as_dict(all_info=True)}

    async def collect() -> list[dict[str, Any]]:
        return [merged(t, d) async for t, d in source.async_iter_data()]

    sync = [merged(t, d) for t, d in source.iter_data()]
    async_rows = asyncio.run(asyncio.wait_for(collect(), timeout=60))
    assert async_rows == sync
    return (
        sorted(cast("list[int]", source.as_table().column("k").to_pylist())),
        sorted(row["k"] for row in async_rows),
    )


class TestTimestampColumnsInEveryRole:
    """Timestamp columns as tags, as data-tags and as data, restricted together, in
    two table layouts, read sync and async (several-items rule, CodeRabbit PR 435)."""

    @pytest.mark.parametrize(
        "tag_only", [True, False], ids=["key columns tags", "key columns data-tags"]
    )
    @pytest.mark.parametrize(
        "restrict, expected",
        [
            (lambda s: s.where(at=_local(1), seen=_local(1)), [1]),
            (lambda s: s.where(seen=_local(1), at=_local(1)), [1]),
            (lambda s: s.where({"wall": _WALL}, noted=_WALL), [0, 1]),
            (lambda s: s.where(noted=_WALL).where(wall=_WALL, k=0), [0]),
            (
                lambda s: s.where(
                    Condition.of({"seen": _local(0), "wall": _WALL, "at": _local(0)})
                ),
                [0],
            ),
            (lambda s: s.where(at=_local(0), seen=_local(1)), []),
            (lambda s: s.where(wall=_WALL, x=1.5, noted=_WALL), [1]),
        ],
        ids=[
            "zoned tag and zoned data",
            "zoned data and zoned tag",
            "naive tag (mapping) and naive data",
            "chained, with the integer tag",
            "Condition over three columns",
            "the two folds disagree",
            "naive tag, float data, naive data",
        ],
    )
    def test_matching_keys_on_both_paths(self, roles, tag_only, restrict, expected):
        source = SpiralDBTableSource(_PROJECT, "roles", tag_only_columns=tag_only)
        tags, data = source.output_schema()
        assert {"at", "wall", "k"} <= set(tags)
        assert ({"at", "wall", "k"} <= set(data)) is not tag_only

        assert _sync_and_async_keys(restrict(source)) == (expected, expected)


class TestIdentity:
    """A restricted source has the unrestricted source's identity and rows."""

    def test_hashes_source_id_and_schema_are_unchanged(self, project):
        """Content hash, pipeline hash, source id and schema equal the unrestricted ones."""
        source = _source()
        restricted = source.where(subject_id="m2")

        assert restricted.content_hash() == source.content_hash()
        assert restricted.pipeline_hash() == source.pipeline_hash()
        assert restricted.source_id == source.source_id
        assert restricted.output_schema(all_info=True) == source.output_schema(
            all_info=True
        )
        assert restricted.keys(all_info=True) == source.keys(all_info=True)

    def test_a_given_source_id_is_kept(self, project):
        """A source id the user gave is kept."""
        restricted = _source(source_id="events").where(subject_id="m2")

        assert restricted.source_id == "spiraldb_table_v1:events"

    def test_rows_are_the_matching_rows_unchanged(self, project):
        """Every column of a matching row, system tags and source info included, is equal."""
        whole = _rows_by_key(_source())
        restricted = _rows_by_key(_source().where(subject_id="m2"))

        assert restricted == {t: whole[t] for t in (4, 5)}
        record_ids = [
            name
            for name in _source().as_table(all_info=True).column_names
            if name.startswith("_tag_record_id::")
        ]
        assert record_ids, "the record-id system tag is present"

    def test_data_tags_behave_as_for_the_unrestricted_source(self, project):
        """With data-tags (the default), the rows are the matching rows unchanged."""
        whole = _source()
        restricted = whole.where(subject_id="m1")

        assert restricted.keys() == whole.keys()
        assert "subject_id" in restricted.keys()[1]
        assert _rows_by_key(restricted) == {
            t: _rows_by_key(whole)[t] for t in (1, 2, 3)
        }

    def test_rows_of_a_tags_as_data_source_are_unchanged(self, project):
        """``tags_as_data`` copies are carried over and hold the same values."""
        whole = _source(tags_as_data={"timestamp": "time"}, tag_only_columns=True)
        restricted = whole.where(subject_id="m2")

        assert restricted.content_hash() == whole.content_hash()
        assert _rows_by_key(restricted) == {t: _rows_by_key(whole)[t] for t in (4, 5)}


class TestCopyThenApply:
    """``where`` is a copy of the source with the restriction applied to the copy."""

    def test_where_equals_copy_then_apply(self, project):
        """The two private steps give the rows and identity ``where`` gives."""
        source = _source(limit=4)

        # About the private steps themselves: the future copy() and apply_restriction.
        composed = source._copy()  # pyright: ignore[reportPrivateUsage]
        composed._apply_restriction(  # pyright: ignore[reportPrivateUsage]
            Condition.of(subject_id="m1")
        )
        restricted = source.where(subject_id="m1")

        assert composed.to_config() == restricted.to_config()
        assert composed.content_hash() == restricted.content_hash()
        assert composed.pipeline_hash() == restricted.pipeline_hash()
        assert composed.as_table(all_info=True).equals(
            restricted.as_table(all_info=True)
        )
        assert _timestamps(source) == [1, 2, 3, 4]
        assert source.to_config()["where"] == {}


class TestCarriedOver:
    """Everything of the source but the conditions is carried over unchanged."""

    def test_arguments_are_carried_over(self, project):
        """Every saved argument is the original's; only ``where`` differs."""
        source = _source(
            dataset="default",
            limit=4,
            max_rows=50,
            batch_rows=3,
            tag_columns=["subject_id", "timestamp"],
            tag_only_columns=["timestamp"],
            tags_as_data={"timestamp": "time"},
            record_id_columns=["subject_id", "timestamp"],
            source_id="events",
            label="events",
        )

        restricted = source.where(subject_id="m1")

        expected = {**source.to_config(), "where": {"subject_id": "m1"}}
        assert restricted.to_config() == expected
        assert restricted.label == "events"

    def test_the_pinned_version_is_carried_over(self, project):
        """The new source scans the version the original pinned, not a later one."""
        version = project.table("default.event").last_modified_at()
        source = _source()
        project.table("default.event").write(_rows(["m2"], [6]))

        source.where(subject_id="m2").as_table()

        assert fake_spiral_sdk.scan_log[-1].kwargs["asof"] == version
        assert source.where(subject_id="m2").to_config()["asof"] is None

    def test_a_given_asof_is_carried_over(self, project):
        """An ``asof`` the user gave stays the given one."""
        version = project.table("default.event").last_modified_at()
        restricted = _source(asof=version).where(subject_id="m2")

        assert restricted.to_config()["asof"] == version

    def test_checking_one_does_not_move_the_other(self, project):
        """A new version taken in by one source is not taken in by the other."""
        old = project.table("default.event").last_modified_at()
        source = _source()
        restricted = source.where(subject_id="m2")
        project.table("default.event").write(_rows(["m2"], [6]))

        assert restricted.check_for_new_data() is True
        restricted.as_table()
        source.as_table()

        new, kept = (scan.kwargs["asof"] for scan in fake_spiral_sdk.scan_log[-2:])
        assert (new, kept) == (old + 1, old)

    def test_no_mutable_state_is_shared(self, project):
        """The two objects share no mutable container."""
        source = _source(
            tags_as_data={"timestamp": "time"}, overrides={"server.url": "b"}
        )
        source.content_hash()
        restricted = source.where(subject_id="m2")

        for name, value in vars(source).items():
            if isinstance(value, (dict, list, set)):
                assert getattr(restricted, name) is not value, name

    def test_reading_one_does_not_populate_the_other(self, project):
        """Rows read through one source are not held by the other."""
        source = _source()
        restricted = source.where(subject_id="m2")
        before = copy.copy(vars(source))

        restricted.as_table()
        restricted.content_hash()

        assert vars(source).keys() == before.keys()
        for name, value in vars(source).items():
            assert value is before[name] or value == before[name], name

    def test_the_new_source_has_its_own_modified_time(self, project):
        """The restricted source is modified when it is made: other rows flow."""
        source = _source()
        restricted = source.where(subject_id="m2")

        assert source.last_modified is not None
        assert restricted.last_modified is not None
        assert restricted.last_modified >= source.last_modified


class TestReads:
    """Every read path applies the restriction."""

    def test_async_yields_the_rows_of_the_sync_read(self, project):
        """``async_iter_data`` yields exactly the rows ``iter_data`` yields."""
        restricted = _source().where(subject_id="m1")

        async def collect() -> list[dict[str, Any]]:
            """Return every row ``async_iter_data`` yields, tags and data merged."""
            return [
                {**tag.as_dict(all_info=True), **data.as_dict(all_info=True)}
                async for tag, data in restricted.async_iter_data()
            ]

        sync = [
            {**tag.as_dict(all_info=True), **data.as_dict(all_info=True)}
            for tag, data in restricted.iter_data()
        ]
        assert asyncio.run(collect()) == sync
        assert [row["timestamp"] for row in sync] == [1, 2, 3]

    def test_head_reads_only_matching_rows(self, project):
        """``head`` reads matching rows in one bounded scan and names the condition."""
        view = _source().where(subject_id="m1").head(2)

        assert view.as_table().column("timestamp").to_pylist() == [1, 2]
        assert len(fake_spiral_sdk.scan_log) == 1
        assert fake_spiral_sdk.scan_log[0].kwargs["limit"] == 2
        assert view.description is not None
        assert "where subject_id='m1'" in view.description


class TestRunsAndSavedJobs:
    """A job over a restricted source reports, saves and loads its restriction."""

    @staticmethod
    def _job(store: Path, source: SpiralDBTableSource, calls: list[int]) -> PipelineJob:
        """Return a job doubling ``amplitude`` over ``source``, counting calls."""

        @function_pod(output_keys="doubled")
        def double(amplitude: float | None) -> float | None:
            """Return twice ``amplitude``; counts the call."""
            calls.append(1)
            return None if amplitude is None else 2 * amplitude

        job = PipelineJob(name="events", database=DeltaTableDatabase(store))
        with job:
            double.pod(source, label="double")
        return job

    @pytest.mark.parametrize("orchestrator", [None, AsyncPipelineOrchestrator])
    def test_run_summary_reports_the_restriction(self, project, tmp_path, orchestrator):
        """The summary maps the source's slot to its source id and conditions."""
        source = _source(source_id="events", limit=3, tag_only_columns=True)
        job = self._job(tmp_path / "store", source.where(subject_id="m1"), [])

        summary = job.run(orchestrator=orchestrator() if orchestrator else None)

        assert summary.conditions == {"events": Condition.of(subject_id="m1")}
        assert summary.limits == {"events": 3}
        assert summary.restricted_source_ids == {"events": source.source_id}
        assert "events (spiraldb_table_v1:events): where subject_id='m1', limit 3" in (
            repr(summary)
        )
        assert "subject_id=&#x27;m1&#x27;" in summary._repr_html_()

    @pytest.mark.parametrize("form", list(_FORMS_OF_ONE_RESTRICTION))
    def test_the_run_summary_records_both_conditions(self, project, tmp_path, form):
        """``RunSummary.conditions`` is the same for every form of the restriction."""
        source = _source(source_id="events", tag_only_columns=True)
        restricted = _FORMS_OF_ONE_RESTRICTION[form](source)
        job = self._job(tmp_path / "store", restricted, [])

        summary = job.run()

        assert summary.conditions == {
            "events": Condition.of(subject_id="m1", flag=True)
        }
        assert summary.restricted_source_ids == {"events": "spiraldb_table_v1:events"}
        rows = job.nodes["double"].as_table().column("timestamp").to_pylist()
        assert rows == [3]

    def test_the_key_is_the_slot_and_the_value_names_the_source(
        self, project, tmp_path
    ):
        """A source bound to a slot is reported under the slot, with its own id."""
        job = self._job(tmp_path / "store", _source(source_id="events"), [])
        bound = _source().where(subject_id="m1")
        job.bind(sources={"events": bound})

        summary = job.run()

        assert summary.conditions == {"events": Condition.of(subject_id="m1")}
        assert summary.restricted_source_ids == {"events": bound.source_id}

    def test_a_slot_bound_to_a_stream_over_the_source_keeps_its_name(
        self, project, tmp_path
    ):
        """A slot bound to an operator's stream over a restricted source names it."""
        job = self._job(tmp_path / "store", _source(source_id="events"), [])
        restricted = _source(source_id="events").where(subject_id="m1")
        data_columns = list(restricted.keys()[1])
        job.bind(sources={"events": restricted.select_data_columns(data_columns)})

        summary = job.run()

        assert summary.conditions == {"events": Condition.of(subject_id="m1")}
        assert summary.restricted_source_ids == {"events": "spiraldb_table_v1:events"}

    def test_two_unbound_sources_of_one_label_are_both_reported(self, project):
        """Sources read without a slot are keyed by label, numbered when they meet."""
        first = _source(label="events").where(subject_id="m1")
        second = _source(label="events").where(subject_id="m2")

        # About the summary's collector itself: no public run reaches two unbound
        # sources of one label without a join of one table with itself.
        found = restrictions_in_force([first, second])

        assert found == {
            "events": (first.source_id, Condition.of(subject_id="m1"), None),
            "events (2)": (second.source_id, Condition.of(subject_id="m2"), None),
        }

    def test_one_table_restricted_two_ways_in_one_job_is_refused(
        self, project, tmp_path
    ):
        """Two restrictions of one table in one graph would be one source: refused."""

        @function_pod(output_keys="a")
        def first(amplitude: float | None) -> float | None:
            """Return ``amplitude``."""
            return amplitude

        @function_pod(output_keys="b")
        def second(channel: int | None) -> int | None:
            """Return ``channel``."""
            return channel

        events = _source(source_id="events", tag_only_columns=True)
        job = PipelineJob(name="two", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            first.pod(events.where(subject_id="m1"), label="first")
            with pytest.raises(
                InconsistentSourceError, match="distinct source objects"
            ):
                second.pod(events.where(subject_id="m2"), label="second")
        assert {"first", "second"} & job.nodes.keys() == {"first"}

    def test_streams_built_before_the_capture_are_checked_too(self, project, tmp_path):
        """Streams over differently restricted sources, built outside, are refused."""

        @function_pod(output_keys="a")
        def first(amplitude: float | None) -> float | None:
            """Return ``amplitude``."""
            return amplitude

        @function_pod(output_keys="b")
        def second(amplitude: float | None) -> float | None:
            """Return ``amplitude``."""
            return amplitude

        events = _source(source_id="events", tag_only_columns=True)
        m1 = events.where(subject_id="m1").select_data_columns(["amplitude"])
        m2 = events.where(subject_id="m2").select_data_columns(["amplitude"])
        job = PipelineJob(name="two", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            first.pod(m1, label="first")
            with pytest.raises(
                InconsistentSourceError, match="distinct source objects"
            ):
                second.pod(m2, label="second")
        assert {"first", "second"} & job.nodes.keys() == {"first"}

    @pytest.mark.parametrize(
        "captured, prebuilt",
        [
            ({"subject_id": "m1"}, {"subject_id": "m2"}),
            ({"subject_id": "m1", "flag": True}, {"flag": False, "subject_id": "m1"}),
            ({"flag": True, "subject_id": "m1"}, {"subject_id": "m2"}),
        ],
        ids=["one condition", "several conditions", "several against one"],
    )
    @pytest.mark.parametrize(
        "prebuilt_first", [False, True], ids=["captured first", "prebuilt first"]
    )
    def test_a_stream_with_the_hash_of_a_captured_output_is_checked(
        self, project, tmp_path, captured, prebuilt, prebuilt_first
    ):
        """A stream built before the capture with the hash of a step captured inside
        it, over another restriction, is refused in either order: compilation would
        resolve it to the captured step and read that step's rows."""

        @function_pod(output_keys="a")
        def first(amplitude: float | None) -> float | None:
            """Return ``amplitude``."""
            return amplitude

        events = _source(source_id="events", tag_only_columns=True)
        outside = events.where(**prebuilt).select_data_columns(["amplitude"])
        job = PipelineJob(name="two", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            if prebuilt_first:
                first.pod(outside, label="first")
                with pytest.raises(
                    InconsistentSourceError, match="distinct source objects"
                ):
                    events.where(**captured).select_data_columns(
                        ["amplitude"], label="selected"
                    )
            else:
                events.where(**captured).select_data_columns(
                    ["amplitude"], label="selected"
                )
                with pytest.raises(
                    InconsistentSourceError, match="distinct source objects"
                ):
                    first.pod(outside, label="first")
        assert {"first", "selected"} & job.nodes.keys() == {
            "first" if prebuilt_first else "selected"
        }

    @pytest.mark.parametrize(
        "prebuilt_first", [False, True], ids=["captured first", "prebuilt first"]
    )
    def test_a_stream_with_the_hash_of_an_equal_captured_output_is_accepted(
        self, project, tmp_path, prebuilt_first
    ):
        """Prebuilt and captured projections can reuse one restricted source."""

        @function_pod(output_keys="a")
        def first(amplitude: float | None) -> float | None:
            """Return ``amplitude``."""
            return amplitude

        events = _source(source_id="events", tag_only_columns=True).where(
            flag=True, subject_id="m1"
        )
        outside = events.select_data_columns(["amplitude"])
        job = PipelineJob(name="two", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            if prebuilt_first:
                first.pod(outside, label="first")
            events.select_data_columns(["amplitude"])
            if not prebuilt_first:
                first.pod(outside, label="first")

        job.run()

        rows = job.nodes["first"].as_table().column("timestamp").to_pylist()
        assert rows == [3]

    def test_a_refused_invocation_records_nothing(self, project, tmp_path):
        """After the refusal the graph holds only what was captured before it."""

        @function_pod(output_keys="a")
        def first(amplitude: float | None) -> float | None:
            """Return ``amplitude``."""
            return amplitude

        @function_pod(output_keys="b")
        def second(channel: int | None) -> int | None:
            """Return ``channel``."""
            return channel

        events = _source(source_id="events", tag_only_columns=True)
        job = PipelineJob(name="two", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            first.pod(events.where(subject_id="m1"), label="first")
            with pytest.raises(ValueError):
                second.pod(events.where(subject_id="m2"), label="second")
        only_first = PipelineJob(name="one")
        with only_first:
            first.pod(events.where(subject_id="m1"), label="first")

        assert sorted(job.nodes) == sorted(only_first.nodes)
        job.run()
        rows = job.nodes["first"].as_table().column("timestamp").to_pylist()
        assert sorted(rows) == [1, 2, 3]

    def test_two_inputs_of_one_invocation_are_checked(self, project):
        """One operator over two differently restricted sources of one table."""
        events = _source(source_id="events", tag_only_columns=True)
        job = PipelineJob(name="two")
        with job:
            with pytest.raises(
                InconsistentSourceError, match="distinct source objects"
            ):
                events.where(subject_id="m1").semi_join(
                    events.where(subject_id="m2"), label="both"
                )
        assert not job.nodes

    @pytest.mark.parametrize("restrict", [False, True])
    def test_one_restriction_twice_in_one_job_is_accepted(
        self, project, tmp_path, restrict
    ):
        """Two branches can reuse one source with or without a restriction."""

        @function_pod(output_keys="a")
        def first(amplitude: float | None) -> float | None:
            """Return ``amplitude``."""
            return amplitude

        @function_pod(output_keys="b")
        def second(channel: int | None) -> int | None:
            """Return ``channel``."""
            return channel

        events = _source(source_id="events", tag_only_columns=True)
        if restrict:
            events = events.where(subject_id="m1", flag=True)
        job = PipelineJob(name="two", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            first.pod(events, label="first")
            second.pod(events, label="second")

        job.run()
        expected = [3] if restrict else [1, 2, 3, 4, 5]
        for label in ("first", "second"):
            rows = job.nodes[label].as_table().column("timestamp").to_pylist()
            assert sorted(rows) == expected

    def test_an_unrestricted_source_is_not_reported(self, project, tmp_path):
        """A source with no condition and no limit has no entry."""
        job = self._job(tmp_path / "store", _source(source_id="events"), [])

        assert job.run().conditions == {}

    def test_a_limit_alone_is_reported_without_a_condition(self, project, tmp_path):
        """A slot read with a limit only is in ``limits`` and the source ids, not in
        ``conditions``."""
        job = self._job(tmp_path / "store", _source(source_id="events", limit=2), [])

        summary = job.run()

        assert summary.conditions == {}
        assert summary.limits == {"events": 2}
        assert summary.restricted_source_ids == {"events": "spiraldb_table_v1:events"}
        assert "events (spiraldb_table_v1:events): limit 2" in repr(summary)

    def test_one_table_with_two_limits_in_one_job_is_refused(self, project, tmp_path):
        """The same condition with different limits reads different rows: refused."""

        @function_pod(output_keys="a")
        def first(amplitude: float | None) -> float | None:
            """Return ``amplitude``."""
            return amplitude

        @function_pod(output_keys="b")
        def second(channel: int | None) -> int | None:
            """Return ``channel``."""
            return channel

        job = PipelineJob(name="two", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            first.pod(_source(source_id="events", limit=1), label="first")
            with pytest.raises(
                InconsistentSourceError, match="distinct source objects"
            ):
                second.pod(_source(source_id="events", limit=2), label="second")
        assert {"first", "second"} & job.nodes.keys() == {"first"}

    def test_node_run_reports_the_restriction(self, project, tmp_path):
        """A node's own ``run()`` reports the restriction in the same shape."""
        source = _source(source_id="events").where(subject_id="m2")
        job = self._job(tmp_path / "store", source, [])

        node = job.nodes["double"]
        node.run()

        assert node.last_run is not None
        assert node.last_run.conditions == {"events": Condition.of(subject_id="m2")}
        assert node.last_run.restricted_source_ids == {
            "events": "spiraldb_table_v1:events"
        }

    def test_bound_restricted_source_runs_only_matching_rows(self, project, tmp_path):
        """Binding a restricted source runs its rows; nothing stored is removed."""
        store = tmp_path / "store"
        calls: list[int] = []
        source = _source(source_id="events", tag_only_columns=True)
        job = self._job(store, source, calls)
        job.run()
        assert len(calls) == 5

        calls.clear()
        job.bind(sources={"events": source.where(subject_id="m2")})
        job.run()

        assert calls == []
        node = job.nodes["double"]
        assert sorted(node.as_table().column("timestamp").to_pylist()) == [4, 5]
        assert node.as_source().as_table().num_rows == 5

    def test_a_saved_job_keeps_the_restriction(self, project, tmp_path):
        """A saved job records the conditions and a loaded job reads with them."""
        store = tmp_path / "store"
        source = _source(source_id="events", tag_only_columns=True).where(
            subject_id="m2", day=_DAY
        )
        job = self._job(store, source, [])
        job.run()
        path = tmp_path / "job.json"
        job.save(path)

        saved = json.loads(path.read_text())
        assert saved["bindings"]["sources"]["events"]["where"] == {
            "subject_id": "m2",
            "day": {"date": _DAY.isoformat()},
        }
        loaded = PipelineJob.load(path, database=DeltaTableDatabase(store))
        loaded_source = loaded.nodes["events"].bound_source
        assert isinstance(loaded_source, SpiralDBTableSource)
        assert loaded_source.content_hash() == source.content_hash()
        assert _timestamps(loaded_source) == [4, 5]
        summary = loaded.run(load_only=True)
        assert summary.conditions == {"events": Condition.of(day=_DAY, subject_id="m2")}


class TestAggregationWarning:
    """A restricted source feeding an aggregating step is said once per run."""

    @staticmethod
    def _job(store: Path, source: SpiralDBTableSource) -> PipelineJob:
        """Return a job grouping ``source`` by subject, then summing ``channel``."""

        @function_pod(output_keys="total")
        def total(channel: list[int | None]) -> int:
            """Return the sum of the group's channels."""
            return sum(c or 0 for c in channel)

        job = PipelineJob(name="grouped", database=DeltaTableDatabase(store))
        with job:
            grouped = source.select_data_columns(["channel"]).group_by(
                ["subject_id"], label="by_subject"
            )
            total.pod(grouped, label="total")
        return job

    @pytest.mark.parametrize("orchestrator", [None, AsyncPipelineOrchestrator])
    def test_warns_once_naming_slot_and_step(self, project, tmp_path, orchestrator):
        """One warning names the source slot and the ``GroupBy`` node."""
        source = _source(source_id="events", tag_only_columns=True)
        job = self._job(tmp_path / "store", source.where(day=_DAY))

        with pytest.warns(RestrictedAggregationWarning) as caught:
            job.run(orchestrator=orchestrator() if orchestrator else None)

        messages = [
            str(w.message)
            for w in caught
            if issubclass(w.category, RestrictedAggregationWarning)
        ]
        assert len(messages) == 1
        assert "'events' feeds GroupBy 'by_subject'" in messages[0]
        totals = job.nodes["total"].as_table().column("total").to_pylist()
        assert sorted(totals) == [1, 2]

    def test_no_warning_without_a_restriction(self, project, tmp_path, recwarn):
        """An unrestricted source under an aggregation issues no such warning."""
        job = self._job(
            tmp_path / "store", _source(source_id="events", tag_only_columns=True)
        )

        job.run()

        assert not [
            w for w in recwarn if issubclass(w.category, RestrictedAggregationWarning)
        ]


class TestConditionInstances:
    """``where`` and ``where=`` take a ``Condition`` built beforehand (``orcapod.types``)."""

    @pytest.mark.parametrize("tag_only_columns", [(), True])
    @pytest.mark.parametrize(
        "conditions",
        [
            {"subject_id": "m1", "flag": True, "channel": 1},
            {"channel": 1, "subject_id": "m1", "flag": True},
        ],
        ids=["tag first", "data first"],
    )
    def test_tag_data_and_data_tag_columns_in_one_condition(
        self, project, tag_only_columns, conditions
    ):
        """A key column (a data-tag by default, a plain tag with ``tag_only_columns``)
        and two data columns, in either order: the rows and identity of the keywords."""
        source = _source(tag_only_columns=tag_only_columns)

        restricted = source.where(Condition.of(conditions))

        reference = source.where(**conditions)
        assert _timestamps(restricted) == [3]
        assert _rows_by_key(restricted) == _rows_by_key(reference)
        assert restricted.keys() == source.keys()
        assert restricted.content_hash() == source.content_hash()
        assert restricted.pipeline_hash() == source.pipeline_hash()
        assert restricted.source_id == source.source_id
        assert restricted.to_config() == reference.to_config()

    def test_the_constructor_takes_a_condition(self, project):
        """``where=Condition`` is the constructor's ``where=`` mapping."""
        condition = Condition.of(subject_id="m1", flag=True)

        built = _source(where=condition)

        assert (
            built.to_config() == _source(where=dict(condition.equalities)).to_config()
        )
        assert _timestamps(built) == [3]

    def test_the_limit_stays_the_source_own(self, project):
        """``where(Condition)`` adds the condition and keeps the source's limit."""
        restricted = _source(limit=1).where(Condition.of(subject_id="m2"))

        assert _timestamps(restricted) == [4]
        assert restricted.to_config()["limit"] == 1

    def test_the_empty_condition_is_refused(self, project):
        """``where(Condition())`` requires nothing: refused like ``where()``."""
        with pytest.raises(ValueError, match="at least one"):
            _source().where(Condition())

    def test_a_condition_is_checked_against_the_table(self, project):
        """The source refuses a condition its table does not fit."""
        with pytest.raises(ValueError, match="nope"):
            _source().where(Condition.of(nope=1))
        with pytest.raises(TypeError, match="channel"):
            _source().where(Condition.of(channel=1.0))

    def test_a_saved_config_of_the_earlier_form_loads(self, project):
        """The saved form is unchanged: a config written before the type existed
        (``where`` with tagged date values, ``limit``) loads to the same restriction."""
        source = _source(where={"subject_id": "m2", "day": _DAY}, limit=1)
        saved = {
            **source.to_config(),
            "where": {"subject_id": "m2", "day": {"date": "2026-11-20"}},
            "limit": 1,
        }

        assert source.to_config() == saved
        loaded = SpiralDBTableSource.from_config(saved)
        assert loaded._condition == Condition.of(  # pyright: ignore[reportPrivateUsage]
            day=_DAY, subject_id="m2"
        )
        assert _timestamps(loaded) == [4]

    @pytest.mark.parametrize(
        "values",
        [(0.0, -0.0), (-0.0, 0.0), (math.nan, math.nan)],
        ids=["0.0 then -0.0", "-0.0 then 0.0", "NaN twice"],
    )
    def test_equal_float_conditions_of_two_reads_are_refused(self, project, values):
        """``0.0`` and ``-0.0``, or NaN twice, are one condition to Orcapod but not to
        SpiralDB, so ``where`` refuses the ``-0.0`` or NaN read (ITL-1480) before a
        pipeline could hold both."""
        events = _source(source_id="events", tag_only_columns=True)
        with pytest.raises(TypeError, match="ITL-1480"):
            for value in values:
                events.where(amplitude=value)

    @pytest.mark.parametrize("values", [(0.0, 0.0), (0.5, 0.5)])
    def test_equal_float_conditions_still_require_one_source_object(
        self, project, tmp_path, values
    ):
        """Equal conditions can belong to distinct source objects."""

        @function_pod(output_keys="a")
        def first(amplitude: float | None) -> float | None:
            """Return ``amplitude``."""
            return amplitude

        @function_pod(output_keys="b")
        def second(channel: int | None) -> int | None:
            """Return ``channel``."""
            return channel

        events = _source(source_id="events", tag_only_columns=True)
        first_read = events.where(amplitude=values[0])
        second_read = events.where(amplitude=values[1])
        assert first_read.condition == second_read.condition
        assert first_read.content_hash() == second_read.content_hash()
        job = PipelineJob(name="two", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            first.pod(first_read, label="first")
            with pytest.raises(InconsistentSourceError, match=r"Call where\(\) once"):
                second.pod(second_read, label="second")
            second.pod(first_read, label="second")

        summary = job.run()

        assert summary.conditions == {"events": Condition.of(amplitude=values[0])}


_RESTRICTED_FORMS: dict[str, tuple[Callable[[], SpiralDBTableSource], list[str]]] = {
    "a condition": (lambda: _source().where(subject_id="m2"), ["subject_id='m2'"]),
    "a limit": (lambda: _source(limit=2), ["limit 2"]),
    "a condition and a limit": (
        lambda: _source(limit=2).where(subject_id="m2"),
        ["subject_id='m2'", "limit 2"],
    ),
    "several conditions in one call": (
        lambda: _source().where(flag=True, subject_id="m1"),
        ["flag=True", "subject_id='m1'"],
    ),
    "chained conditions": (
        lambda: _source().where(subject_id="m1").where(channel=1),
        ["subject_id='m1'", "channel=1"],
    ),
    "the constructor's where=": (
        lambda: _source(where={"channel": 0, "subject_id": "m1"}),
        ["channel=0", "subject_id='m1'"],
    ),
    "a range and an equality": (
        lambda: _source().where(col("timestamp") >= 2, col("timestamp") < 4, flag=True),
        ["timestamp >= 2", "timestamp < 4", "flag=True"],
    ),
}


class TestCachedSourceOverARestriction:
    """A whole-table cache over a restricted source is refused at construction.

    A cache is kept under its source's identity, which a restriction is not part of:
    a restricted read would be stored as the table's rows and read back by every cache
    of the table (and two caches in one job would be one input).
    """

    @pytest.mark.parametrize("form", list(_RESTRICTED_FORMS))
    @pytest.mark.parametrize("via", ["CachedSource", "cached()"])
    def test_a_restricted_source_is_refused(self, project, tmp_path, form, via):
        """Every form of a restriction is refused, naming it, by both entry points."""
        build, named = _RESTRICTED_FORMS[form]
        restricted = build()
        cache = DeltaTableDatabase(tmp_path / "cache")

        with pytest.raises(ValueError, match="cannot hold a restricted read") as caught:
            if via == "CachedSource":
                CachedSource(restricted, cache_database=cache)
            else:
                restricted.cached(cache_database=cache)

        for text in named:
            assert text in str(caught.value)
        assert fake_spiral_sdk.scan_log == []

    def test_an_unrestricted_cache_still_works(self, project, tmp_path):
        """A cache over the whole table reads every row."""
        cached = _source().cached(cache_database=DeltaTableDatabase(tmp_path / "c"))

        assert _timestamps(cached) == [1, 2, 3, 4, 5]

    def test_a_restricted_read_after_a_cache_reads_its_own_rows(
        self, project, tmp_path
    ):
        """A plain restricted read beside a cache of the whole table is not affected."""
        events = _source(source_id="events", tag_only_columns=True)
        cached = events.cached(cache_database=DeltaTableDatabase(tmp_path / "c"))
        cached.as_table()

        assert _timestamps(events.where(subject_id="m2", flag=True)) == [4, 5]
        assert _timestamps(events.where(flag=True, subject_id="m1")) == [3]

    def test_a_second_cache_in_one_job_is_refused_when_restricted(
        self, project, tmp_path
    ):
        """In a job over a cache of the table, a restricted cache of it is refused
        where it is built, before it could become a second branch's input."""

        @function_pod(output_keys="a")
        def first(amplitude: float | None) -> float | None:
            """Return ``amplitude``."""
            return amplitude

        events = _source(source_id="events", tag_only_columns=True)
        cache = DeltaTableDatabase(tmp_path / "cache")
        job = PipelineJob(name="two", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            first.pod(events.cached(cache_database=cache), label="first")
            with pytest.raises(ValueError, match="cannot hold a restricted read"):
                events.where(subject_id="m2", flag=True).cached(cache_database=cache)

        job.run()

        rows = job.nodes["first"].as_table().column("timestamp").to_pylist()
        assert sorted(rows) == [1, 2, 3, 4, 5]


# The restrictions of one source under which a caching operator is refused: a condition,
# several conditions, a limit, both.
_RESTRICTIONS: dict[str, _Restrict] = {
    "a condition": lambda s: s.where(subject_id="m1"),
    "several conditions": lambda s: s.where(subject_id="m1", flag=True),
    "a limit": lambda s: SpiralDBTableSource(
        _PROJECT, "event", source_id="events", tag_only_columns=True, limit=3
    ),
    "a condition and a limit": lambda s: SpiralDBTableSource(
        _PROJECT, "event", source_id="events", tag_only_columns=True, limit=3
    ).where(subject_id="m1"),
    "a range": lambda s: s.where(col("timestamp") >= 2, col("timestamp") < 4),
}
_CACHING = [CacheMode.LOG, CacheMode.REPLAY]


class TestOperatorCacheOverARestriction:
    """An operator node that caches (LOG or REPLAY) is refused over a restricted source.

    Developer, November 19, 2026 ("B is the proper behavior for now"): the cache is
    kept under the node's identity, which a restriction is not part of, so a log
    written from a restricted read would be replayed for another restriction or none.
    Refused at the earliest point both are known: setting the cache mode over a
    restricted source, binding a restricted source under a caching node, and, for
    any other order, the start of every run, before anything is read or written.
    """

    @staticmethod
    def _events() -> SpiralDBTableSource:
        return _source(source_id="events", tag_only_columns=True)

    @staticmethod
    def _job(
        store: DeltaTableDatabase, source: Any, *, chain: bool = False
    ) -> PipelineJob:
        """A job renaming ``amplitude`` (op ``rename``), optionally after a select."""
        job = PipelineJob(name="events", database=store)
        with job:
            upstream = source
            if chain:
                upstream = upstream.select_data_columns(
                    ["amplitude", "channel"], label="select"
                ).map_data({"channel": "lane"}, label="lane")
            upstream.map_data({"amplitude": "a"}, label="rename")
        return job

    @staticmethod
    def _cache(
        job: PipelineJob, node: str, mode: CacheMode, store: DeltaTableDatabase
    ) -> None:
        """Give ``node`` the cache mode, over the job's own database."""
        job.nodes[node].attach_databases(
            pipeline_database=store.at("events"), cache_mode=mode
        )

    @pytest.mark.parametrize("mode", _CACHING, ids=lambda m: m.name)
    @pytest.mark.parametrize("restriction", list(_RESTRICTIONS))
    @pytest.mark.parametrize(
        "chain", [False, True], ids=["direct", "through-operators"]
    )
    def test_setting_the_cache_mode_over_a_restricted_source_is_refused(
        self, project, tmp_path, mode, restriction, chain
    ):
        """Restriction first, cache mode second: refused when the mode is set."""
        source = _RESTRICTIONS[restriction](self._events())
        store = DeltaTableDatabase(tmp_path / "store")
        job = self._job(store, source, chain=chain)

        with pytest.raises(ValueError, match="cannot cache a restricted read") as info:
            self._cache(job, "rename", mode, store)

        assert f"CacheMode.{mode.name}" in str(info.value)
        assert "'rename'" in str(info.value)
        assert "events: " in str(info.value)
        assert job.nodes["rename"].cache_mode is CacheMode.OFF  # nothing attached

    @pytest.mark.parametrize("mode", _CACHING, ids=lambda m: m.name)
    @pytest.mark.parametrize("restriction", list(_RESTRICTIONS))
    def test_binding_a_restricted_source_under_a_cache_is_refused(
        self, project, tmp_path, mode, restriction
    ):
        """Cache mode first, restriction second: refused at the bind, nothing bound."""
        events = self._events()
        store = DeltaTableDatabase(tmp_path / "store")
        job = self._job(store, events, chain=True)
        self._cache(job, "rename", mode, store)
        slot = job.nodes["events"]

        with pytest.raises(ValueError, match="cannot cache a restricted read"):
            job.bind(sources={"events": _RESTRICTIONS[restriction](events)})
        assert job.nodes["rename"].cache_mode is mode  # the bind changed nothing

        assert slot.bound_source is events

    @pytest.mark.parametrize("mode", _CACHING, ids=lambda m: m.name)
    @pytest.mark.parametrize("run", ["job.run", "job.run_async", "node.run"])
    def test_a_slot_rebound_directly_is_refused_when_the_run_starts(
        self, project, tmp_path, mode, run
    ):
        """Invalid job state is refused at run time before any reads or writes."""
        events = self._events()
        folder = tmp_path / "store"
        store = DeltaTableDatabase(folder)
        job = self._job(store, events)
        self._cache(job, "rename", mode, store)
        job.nodes["events"]._bind(events.where(subject_id="m1"))
        scans = len(fake_spiral_sdk.scan_log)

        with pytest.raises(ValueError, match="cannot cache a restricted read"):
            if run == "job.run":
                job.run()
            elif run == "job.run_async":
                asyncio.run(asyncio.wait_for(job.run_async(), timeout=120))
            else:
                job.nodes["rename"].run()

        assert len(fake_spiral_sdk.scan_log) == scans
        assert not folder.exists() or not any(folder.rglob("*.parquet"))

    @pytest.mark.parametrize("mode", _CACHING, ids=lambda m: m.name)
    @pytest.mark.parametrize(
        "restricted", [("events",), ("units",), ("events", "units")]
    )
    def test_two_sources_feeding_one_operator(
        self, project, tmp_path, mode, restricted
    ):
        """A join of two tables caches only when neither source is restricted."""
        project.create_table("default.unit", key_schema=_KEYS).write(
            pa.table(
                {
                    "subject_id": ["m1", "m1", "m2"],
                    "timestamp": pa.array([1, 2, 4], pa.uint64()),
                    "depth": pa.array([10.0, 20.0, 40.0]),
                }
            )
        )
        events = self._events()
        units = SpiralDBTableSource(
            _PROJECT, "unit", source_id="units", tag_only_columns=True
        )
        sources = {"events": events, "units": units}
        for name in restricted:
            sources[name] = sources[name].where(subject_id="m1")
        store = DeltaTableDatabase(tmp_path / "s")
        job = PipelineJob(name="events", database=store)
        with job:
            sources["events"].join(sources["units"], label="joined")

        with pytest.raises(ValueError, match="cannot cache a restricted read") as info:
            self._cache(job, "joined", mode, store)
        for name in restricted:
            assert f"{name}: where subject_id='m1'" in str(info.value)

    @pytest.mark.parametrize("mode", _CACHING, ids=lambda m: m.name)
    @pytest.mark.parametrize("path", ["sync", "async"])
    def test_an_unrestricted_pipeline_still_caches(self, project, tmp_path, mode, path):
        """Without a restriction, a LOG run stores and a REPLAY run replays."""
        store = DeltaTableDatabase(tmp_path / "store")
        job = self._job(store, self._events(), chain=True)
        self._cache(job, "rename", CacheMode.LOG, store)
        job.run() if path == "sync" else asyncio.run(
            asyncio.wait_for(job.run_async(), timeout=120)
        )
        logged = job.nodes["rename"].as_table().column("timestamp").to_pylist()

        self._cache(job, "rename", mode, store)
        job.run() if path == "sync" else asyncio.run(
            asyncio.wait_for(job.run_async(), timeout=120)
        )

        assert sorted(logged) == [1, 2, 3, 4, 5]
        rows = job.nodes["rename"].as_table().column("timestamp").to_pylist()
        assert sorted(rows) == [1, 2, 3, 4, 5]

    @pytest.mark.parametrize("restriction", list(_RESTRICTIONS))
    @pytest.mark.parametrize("path", ["sync", "async"])
    def test_cache_mode_off_over_a_restriction_still_runs(
        self, project, tmp_path, restriction, path
    ):
        """``CacheMode.OFF`` caches nothing, so a restricted source is accepted."""
        store = DeltaTableDatabase(tmp_path / "store")
        source = _RESTRICTIONS[restriction](self._events())
        job = self._job(store, source, chain=True)
        self._cache(job, "rename", CacheMode.OFF, store)

        job.run() if path == "sync" else asyncio.run(
            asyncio.wait_for(job.run_async(), timeout=120)
        )

        expected = _timestamps(source)
        rows = job.nodes["rename"].as_table().column("timestamp").to_pylist()
        assert sorted(rows) == sorted(expected)
        assert len(expected) < 5

    def test_a_function_node_over_a_restriction_is_not_concerned(
        self, project, tmp_path
    ):
        """A function node stores per row, so it runs over a restricted source."""

        @function_pod(output_keys="doubled")
        def double(amplitude: float | None) -> float | None:
            """Return twice ``amplitude``."""
            return None if amplitude is None else 2 * amplitude

        job = PipelineJob(name="events", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            double.pod(self._events().where(subject_id="m1"), label="double")
        job.run()

        rows = job.nodes["double"].as_table().column("timestamp").to_pylist()
        assert sorted(rows) == [1, 2, 3]


class TestOperatorCacheEntryPoints:
    """Every entry point that runs a caching operator node refuses a restricted read.

    CodeRabbit (PR 435): a LOG node attached over an unrestricted source could still
    log restricted rows through ``node.upstreams = ...`` and ``node.execute(...)`` /
    ``node.async_execute(...)``, which pass neither ``attach_databases`` nor ``run()``.
    """

    @staticmethod
    def _logged_node(store: DeltaTableDatabase, mode: CacheMode) -> Any:
        events = _source(source_id="events", tag_only_columns=True)
        job = PipelineJob(name="events", database=store)
        with job:
            events.map_data({"amplitude": "a"}, label="rename")
        node = job.nodes["rename"]
        node.attach_databases(pipeline_database=store.at("events"), cache_mode=mode)
        return node

    @staticmethod
    def _stored_files(folder: Path) -> list[Path]:
        return sorted(folder.rglob("*.parquet")) if folder.exists() else []

    @pytest.mark.parametrize("mode", _CACHING, ids=lambda m: m.name)
    @pytest.mark.parametrize("restriction", list(_RESTRICTIONS))
    def test_assigning_restricted_upstreams_is_refused(
        self, project, tmp_path, mode, restriction
    ):
        node = self._logged_node(DeltaTableDatabase(tmp_path / "s"), mode)
        before = node.upstreams
        restricted = _RESTRICTIONS[restriction](
            _source(source_id="events", tag_only_columns=True)
        )

        with pytest.raises(ValueError, match="cannot cache a restricted read"):
            node.upstreams = (restricted,)

        assert node.upstreams is before

    @pytest.mark.parametrize("mode", _CACHING, ids=lambda m: m.name)
    @pytest.mark.parametrize("restriction", list(_RESTRICTIONS))
    def test_execute_over_restricted_streams_is_refused(
        self, project, tmp_path, mode, restriction
    ):
        folder = tmp_path / "s"
        node = self._logged_node(DeltaTableDatabase(folder), mode)
        restricted = _RESTRICTIONS[restriction](
            _source(source_id="events", tag_only_columns=True)
        )
        scans = len(fake_spiral_sdk.scan_log)

        with pytest.raises(ValueError, match="cannot cache a restricted read"):
            node.execute(restricted.select_data_columns(["amplitude", "channel"]))

        assert len(fake_spiral_sdk.scan_log) == scans
        assert self._stored_files(folder) == []

    @pytest.mark.parametrize("mode", _CACHING, ids=lambda m: m.name)
    def test_async_execute_over_a_restricted_slot_is_refused(
        self, project, tmp_path, mode
    ):
        """The slot's source set directly (no job check); async_execute itself refuses
        and closes its output."""
        from orcapod.channels import Channel

        folder = tmp_path / "s"
        node = self._logged_node(DeltaTableDatabase(folder), mode)
        (slot,) = node.upstreams
        slot._bind(
            _source(source_id="events", tag_only_columns=True).where(subject_id="m1")
        )
        inputs, output = Channel(buffer_size=8), Channel(buffer_size=8)

        async def run() -> list[Any]:
            await inputs.writer.close()
            with pytest.raises(ValueError, match="cannot cache a restricted read"):
                await asyncio.wait_for(
                    node.async_execute([inputs.reader], output.writer), timeout=30
                )
            # Bounded: a regression that leaves the output open fails, not hangs.
            return await asyncio.wait_for(output.reader.collect(), timeout=30)

        assert asyncio.run(run()) == []
        assert self._stored_files(folder) == []

    @staticmethod
    def _keyed(rows: Any) -> list[tuple[tuple[str, int], float, int]]:
        """Both tags and two data columns of each row, in key order."""
        return sorted(
            ((tag["subject_id"], tag["timestamp"]), data["a"], data["channel"])
            for tag, data in rows
        )

    @pytest.mark.parametrize("mode", _CACHING, ids=lambda m: m.name)
    def test_execute_over_unrestricted_streams_still_caches(
        self, project, tmp_path, mode
    ):
        """LOG stores what ``execute`` returns; REPLAY returns that log, not a recompute.

        The REPLAY node is handed a stream over another table (other rows, same
        columns): a recompute would return its rows and scan it.
        """
        store = DeltaTableDatabase(tmp_path / "s")
        node = self._logged_node(store, CacheMode.LOG)
        expected = [
            (("m1", 1), 1.5, 1),
            (("m1", 2), 2.5, 0),
            (("m1", 3), 3.5, 1),
            (("m2", 4), 4.5, 0),
            (("m2", 5), 5.5, 1),
        ]

        logged = list(
            node.execute(_source(source_id="events", tag_only_columns=True)).iter_data()
        )

        assert self._keyed(logged) == expected
        stored = node.as_source().as_table().to_pylist()
        assert self._keyed(((row, row) for row in stored)) == expected

        if mode is CacheMode.REPLAY:
            project.create_table("default.other", key_schema=_KEYS).write(
                _rows(["m3", "m3"], [6, 7])
            )
            other = SpiralDBTableSource(
                _PROJECT, "other", source_id="events", tag_only_columns=True
            )
            assert sorted(_timestamps(other)) == [6, 7]
            node.attach_databases(
                pipeline_database=store.at("events"), cache_mode=CacheMode.REPLAY
            )
            scans = len(fake_spiral_sdk.scan_log)

            replayed = list(node.execute(other).iter_data())

            assert self._keyed(replayed) == expected
            assert len(fake_spiral_sdk.scan_log) == scans


def _proxy_for(source: SpiralDBTableSource) -> Any:
    """A ``SourceProxy`` standing in for ``source``'s identity, unbound."""
    from orcapod.core.sources.source_proxy import SourceProxy

    # With the system-tag columns, as a slot needs them (a loaded job's proxy has its
    # node's schema).
    tag_schema, data_schema = source.output_schema(columns={"system_tags": True})
    return SourceProxy(
        source_id=source.source_id,
        content_hash_str=source.content_hash().to_string(),
        pipeline_hash_str=source.pipeline_hash().to_string(),
        tag_schema=tag_schema,
        data_schema=data_schema,
        expected_class_name="SpiralDBTableSource",
    )


def _events() -> SpiralDBTableSource:
    return _source(source_id="events", tag_only_columns=True)


class TestRestrictionThroughWrappers:
    """A restriction is seen through a bound ``SourceProxy`` and a ``CachedSource``.

    CodeRabbit (PR 435): a proxy bound to a restricted source has no condition of its
    own and binds by identity, which a restriction leaves unchanged, so the walks must
    look through it. Wrappers checked: ``SourceJobNode`` (bound source),
    ``SourceProxy`` (delegate), ``CachedSource`` (cached source), and
    ``DerivedSource`` when it reads a source live.
    """

    @staticmethod
    def _bound_proxy(restriction: str) -> Any:
        restricted = _RESTRICTIONS[restriction](_events())
        proxy = _proxy_for(_events())
        proxy.bind(restricted)
        return proxy

    def test_live_derived_sources_expose_their_origins_restrictions(
        self, project, tmp_path
    ):
        from orcapod.sources import DerivedSource

        def derive(subject: str) -> DerivedSource:
            return DerivedSource(
                _events().where(subject_id=subject),
                tags_as_data={"subject_id": "copied"},
                source_id=f"copy_{subject}",
            )

        first, second = derive("m1"), derive("m2")
        with PipelineJob(database=DeltaTableDatabase(tmp_path / "store")) as job:
            first.map_data({"amplitude": "renamed"}, label="first")
            with pytest.raises(ValueError, match="two different restrictions"):
                second.map_data({"amplitude": "renamed"}, label="second")

        assert job.run().conditions == {"copy_m1": Condition.of(subject_id="m1")}

    @staticmethod
    def _cached_job(store: DeltaTableDatabase, mode: CacheMode) -> PipelineJob:
        job = PipelineJob(name="events", database=store)
        with job:
            _events().map_data({"amplitude": "a"}, label="rename")
        job.nodes["rename"].attach_databases(
            pipeline_database=store.at("events"), cache_mode=mode
        )
        return job

    @pytest.mark.parametrize("mode", _CACHING, ids=lambda m: m.name)
    @pytest.mark.parametrize("restriction", list(_RESTRICTIONS))
    @pytest.mark.parametrize("entry", ["upstreams", "execute"])
    def test_a_proxy_bound_to_a_restricted_source_is_refused_under_a_cache(
        self, project, tmp_path, mode, restriction, entry
    ):
        """The entries a proxy can reach: a slot refuses a proxy itself (it carries
        no system-tag columns), so a proxy never feeds a job through a slot."""
        store = DeltaTableDatabase(tmp_path / "s")
        proxy = self._bound_proxy(restriction)
        node = self._cached_job(store, mode).nodes["rename"]
        with pytest.raises(ValueError, match="cannot cache a restricted read"):
            if entry == "upstreams":
                node.upstreams = (proxy,)
            else:
                node.execute(proxy)

    def test_a_proxy_does_not_fit_a_slot(self, project, tmp_path):
        from orcapod.errors import SourceSpecMismatchError

        job = self._cached_job(DeltaTableDatabase(tmp_path / "s"), CacheMode.OFF)
        with pytest.raises(SourceSpecMismatchError, match="system-tag"):
            job.bind(sources={"events": self._bound_proxy("a condition")})

    def test_the_summary_and_the_walk_see_through_a_proxy(self, project):
        proxy = self._bound_proxy("several conditions")
        found = restrictions_in_force([proxy.select_data_columns(["amplitude"])])
        assert list(found.values()) == [
            (
                "spiraldb_table_v1:events",
                Condition.of(subject_id="m1", flag=True),
                None,
            )
        ]

    @pytest.mark.parametrize("order", ["proxy first", "source first"])
    def test_a_proxy_and_a_source_restricted_two_ways_are_refused_at_capture(
        self, project, tmp_path, order
    ):
        proxy = self._bound_proxy("a condition")  # subject_id='m1'
        other = _events().where(subject_id="m2")
        first, second = (proxy, other) if order == "proxy first" else (other, proxy)
        job = PipelineJob(
            name="two", database=DeltaTableDatabase(tmp_path / "s"), auto_compile=False
        )
        with job:
            first.select_data_columns(["amplitude"], label="a")
            with pytest.raises(
                InconsistentSourceError, match="distinct source objects"
            ):
                second.select_data_columns(["channel"], label="b")
        assert {inv.label for inv in job._invocation_lut.values()} == {"a"}

    def test_a_cache_over_a_proxy_bound_later_refuses_to_read(self, project, tmp_path):
        proxy = _proxy_for(_events())
        cached = proxy.cached(cache_database=DeltaTableDatabase(tmp_path / "c"))
        proxy.bind(_events().where(subject_id="m1"))
        scans = len(fake_spiral_sdk.scan_log)

        with pytest.raises(ValueError, match="cannot hold a restricted read"):
            cached.as_table()

        assert len(fake_spiral_sdk.scan_log) == scans
        assert not (tmp_path / "c").exists() or not any(
            (tmp_path / "c").rglob("*.parquet")
        )

    def test_a_cache_over_a_proxy_bound_to_a_restricted_source_is_refused(
        self, project, tmp_path
    ):
        proxy = self._bound_proxy("a limit")
        with pytest.raises(ValueError, match="cannot hold a restricted read"):
            proxy.cached(cache_database=DeltaTableDatabase(tmp_path / "c"))


class TestCachedSourceFromConfig:
    """A saved restricted source never produces a cache (CodeRabbit, PR 435)."""

    @staticmethod
    def _cached_config(tmp_path: Path, inner: dict[str, Any]) -> dict[str, Any]:
        config = (
            _events()
            .cached(cache_database=DeltaTableDatabase(tmp_path / "c"))
            .to_config()
        )
        return {**config, "inner_source": inner}

    @pytest.mark.parametrize("restriction", list(_RESTRICTIONS))
    @pytest.mark.parametrize("table", ["available", "unavailable"])
    def test_a_restricted_inner_config_is_refused(
        self, project, tmp_path, restriction, table
    ):
        inner = _RESTRICTIONS[restriction](_events()).to_config()
        if table == "unavailable":
            # The source cannot be rebuilt, so a SourceProxy would stand in for it.
            inner = {**inner, "table_name": "no_such_table"}
        config = self._cached_config(tmp_path, inner)

        with pytest.raises(ValueError, match="cannot hold a restricted read"):
            CachedSource.from_config(config)

    def test_an_unrestricted_inner_config_still_loads(self, project, tmp_path):
        config = self._cached_config(tmp_path, _events().to_config())
        loaded = CachedSource.from_config(config)
        assert sorted(_timestamps(loaded)) == [1, 2, 3, 4, 5]


class TestBindOfADatabaseOverARestrictedSlot:
    """``bind(database=...)`` refuses before it changes anything (CodeRabbit, PR 435)."""

    @pytest.mark.parametrize("mode", _CACHING, ids=lambda m: m.name)
    def test_nothing_changes_and_a_second_bind_behaves_alike(
        self, project, tmp_path, mode
    ):
        store = DeltaTableDatabase(tmp_path / "s")
        job = TestRestrictionThroughWrappers._cached_job(store, mode)
        node = job.nodes["rename"]
        job.nodes["events"]._bind(_events().where(subject_id="m1"))
        before = (job.database, node._pipeline_database)  # pyright: ignore[reportPrivateUsage]

        other = DeltaTableDatabase(tmp_path / "other")
        for _ in range(2):
            with pytest.raises(ValueError, match="cannot cache a restricted read"):
                job.bind(database=other)
            after = (job.database, node._pipeline_database)  # pyright: ignore[reportPrivateUsage]
            assert after[0] is before[0] and after[1] is before[1]

    @pytest.mark.parametrize("mode", _CACHING, ids=lambda m: m.name)
    @pytest.mark.parametrize("with_database", [True, False], ids=["db", "no-db"])
    def test_binding_a_restricted_source_is_refused_before_anything_changes(
        self, project, tmp_path, mode, with_database
    ):
        """Refuse a cache over a prospective restricted binding (PR 435)."""
        store = DeltaTableDatabase(tmp_path / "s")
        job = TestRestrictionThroughWrappers._cached_job(store, mode)
        node, slot = job.nodes["rename"], job.nodes["events"]
        restricted = _events().where(subject_id="m1")
        before = (job.database, node._pipeline_database, slot.bound_source)  # pyright: ignore[reportPrivateUsage]
        other = DeltaTableDatabase(tmp_path / "other")

        for _ in range(2):
            with pytest.raises(ValueError, match="cannot cache a restricted read"):
                if with_database:
                    job.bind(sources={"events": restricted}, database=other)
                else:
                    job.bind(sources={"events": restricted})
            after = (job.database, node._pipeline_database, slot.bound_source)  # pyright: ignore[reportPrivateUsage]
            assert all(a is b for a, b in zip(after, before, strict=True))


class TestOneRestrictionPerSource:
    """The one-restriction rule is per source identity, not per stream hash.

    CodeRabbit (PR 435): two different projections (or operator chains) over two
    restrictions of one source have different stream hashes.
    """

    @staticmethod
    def _chains() -> dict[str, Callable[[Any], Any]]:
        return {
            "select": lambda s: s.select_data_columns(["amplitude"]),
            "drop": lambda s: s.drop_data_columns(["note"]),
            "map then select": lambda s: s.map_data(
                {"channel": "lane"}
            ).select_data_columns(["lane"]),
        }

    @pytest.mark.parametrize("second_chain", ["select", "drop", "map then select"])
    @pytest.mark.parametrize("first_chain", ["select", "drop", "map then select"])
    @pytest.mark.parametrize(
        "second",
        [
            lambda: _events().where(subject_id="m2"),
            lambda: _events(),
            lambda: SpiralDBTableSource(
                _PROJECT, "event", source_id="events", tag_only_columns=True, limit=2
            ).where(subject_id="m1"),
        ],
        ids=["another condition", "no restriction", "the condition and a limit"],
    )
    def test_two_restrictions_of_one_source_are_refused_whatever_the_chains(
        self, project, tmp_path, first_chain, second_chain, second
    ):
        chains = self._chains()

        @function_pod(output_keys="x")
        def keep(amplitude: float | None) -> float | None:
            """Return ``amplitude``."""
            return amplitude

        job = PipelineJob(name="two", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            chains[first_chain](_events().where(subject_id="m1"))
            with pytest.raises(
                InconsistentSourceError, match="distinct source objects"
            ):
                chains[second_chain](second())
        assert "events" in job.nodes
        assert len(job.nodes) == (3 if first_chain == "map then select" else 2)

    @pytest.mark.parametrize("order", ["m1 first", "m2 first"])
    @pytest.mark.parametrize(
        "chains",
        [
            ("select", "drop"),
            ("map then select", "select"),
            ("drop", "map then select"),
        ],
        ids=["select and drop", "map-select and select", "drop and map-select"],
    )
    def test_chains_built_before_the_job_over_two_restrictions_are_refused(
        self, project, tmp_path, chains, order
    ):
        """Streams built outside the job: their steps are never recorded, so only the
        sources they read can show the conflict."""
        built = self._chains()
        m1 = built[chains[0]](_events().where(subject_id="m1"))
        m2 = built[chains[1]](_events().where(subject_id="m2"))
        first, second = (m1, m2) if order == "m1 first" else (m2, m1)
        job = PipelineJob(name="two", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            with pytest.raises(
                InconsistentSourceError, match="distinct source objects"
            ):
                first.semi_join(second, label="both")
        assert not job.nodes

    @pytest.mark.parametrize("order", ["select first", "drop first"])
    def test_one_restriction_through_two_chains_is_accepted(
        self, project, tmp_path, order
    ):
        chains = self._chains()
        names = ["select", "drop"] if order == "select first" else ["drop", "select"]
        job = PipelineJob(name="one", database=DeltaTableDatabase(tmp_path / "s"))
        events = _events().where(flag=True, subject_id="m1")
        with job:
            for name in names:
                chains[name](events)
        summary = job.run()
        assert summary.conditions == {
            "events": Condition.of(subject_id="m1", flag=True)
        }


class TestForeignRestrictedSlots:
    """A job refuses foreign slots even when their sources are restricted."""

    @staticmethod
    def _slot(source: Any) -> Any:
        from orcapod.core.nodes.source_node import SourceJobNode

        return SourceJobNode.from_stream(source)

    @pytest.mark.parametrize("step", ["select", "drop"])
    def test_a_foreign_slot_is_refused_before_its_restriction_changes(
        self, project, tmp_path, step
    ):
        m1 = _events().where(subject_id="m1")
        slot = self._slot(m1)
        later = {
            "select": lambda s: s.select_data_columns(["amplitude"]),
            "drop": lambda s: s.drop_data_columns(["note"]),
        }[step]
        job = PipelineJob(name="live", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            with pytest.raises(InconsistentSourceError, match="not supported yet"):
                MapData({"channel": "lane"})(slot)
            slot.bound_source = _events().where(subject_id="m2")
            later(m1)
        assert job.sources["events"] is m1
        assert job.run().conditions == {"events": Condition.of(subject_id="m1")}

    def test_a_foreign_slot_does_not_replace_a_captured_restricted_source(
        self, project, tmp_path
    ):
        m1 = _events().where(subject_id="m1")
        slot = self._slot(m1)
        job = PipelineJob(name="live", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            m1.select_data_columns(["amplitude"])
            with pytest.raises(InconsistentSourceError, match="not supported yet"):
                MapData({"channel": "lane"})(slot)
            slot.bound_source = _events().where(subject_id="m2")
        assert job.sources["events"] is m1
        assert job.run().conditions == {"events": Condition.of(subject_id="m1")}

    def test_a_foreign_slot_with_an_equal_restriction_is_refused(
        self, project, tmp_path
    ):
        m1 = _events().where(subject_id="m1")
        slot = self._slot(m1)
        job = PipelineJob(name="live", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            replacement = _events().where(subject_id="m1")
            slot.bound_source = replacement
            with pytest.raises(InconsistentSourceError, match="not supported yet"):
                MapData({"channel": "lane"})(slot)
            m1.select_data_columns(["amplitude"])
        assert job.sources["events"] is m1
        assert job.run().conditions == {"events": Condition.of(subject_id="m1")}


@pytest.mark.filterwarnings("ignore::orcapod.errors.SemiJoinIgnoredColumnWarning")
class TestASlotOverTwoTables:
    """A slot bound to a stream over two restricted tables (CodeRabbit, PR 435).

    A ``semi_join`` keeps its left input's system tags, so it fits a slot; every
    check then reads both tables. One source read under two restrictions across
    slots is refused by a bind before it binds, and by a run before anything is
    read, as at capture. A job node's ``run()`` checks what that run reads.
    """

    @staticmethod
    def _others() -> SpiralDBTableSource:
        return SpiralDBTableSource(
            _PROJECT, "other", source_id="others", tag_only_columns=True
        )

    @pytest.fixture()
    def other_table(self, project):
        project.create_table("default.other", key_schema=_KEYS).write(
            _rows(["m1", "m2", "m1", "m2", "m2"], [1, 2, 3, 4, 5])
        )
        fake_spiral_sdk.scan_log.clear()

    def _job(self, tmp_path: Path, others: SpiralDBTableSource) -> PipelineJob:
        """Slot ``events`` (unrestricted) and slot ``others`` bound to ``others``;
        node ``both`` reads the two slots."""
        job = PipelineJob(name="two", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            events = _events()
            events.map_data({"amplitude": "a"}, label="rename")
            others.map_data({"amplitude": "b"}, label="other")
            events.semi_join(others, label="both")
        return job

    @pytest.mark.parametrize("form", ["semi_join", "nested"])
    @pytest.mark.parametrize(
        "bound_other",
        [lambda s: s.where(subject_id="m2"), lambda s: s],
        ids=["another condition", "no restriction"],
    )
    def test_a_bind_reading_a_source_two_ways_is_refused_before_anything_changes(
        self, project, other_table, tmp_path, form, bound_other
    ):
        job = self._job(tmp_path, self._others().where(subject_id="m1"))
        slot = job.nodes["events"]
        before = (job.database, slot.bound_source)
        other_read = bound_other(self._others())
        events = _events().where(subject_id="m1")
        stream = (
            events.semi_join(other_read)
            if form == "semi_join"
            else events.semi_join(other_read.semi_join(events))
        )
        other_db = DeltaTableDatabase(tmp_path / "other")

        for _ in range(2):
            with pytest.raises(
                InconsistentSourceError, match="distinct source objects"
            ):
                job.bind(sources={"events": stream}, database=other_db)
            assert job.database is before[0]
            assert slot.bound_source is before[1]

    @pytest.mark.parametrize("run", ["job.run", "job.run_async", "node.run"])
    def test_a_slot_rebound_directly_is_refused_when_the_run_starts(
        self, project, other_table, tmp_path, run
    ):
        job = self._job(tmp_path, self._others().where(subject_id="m1"))
        job.nodes["events"]._bind(
            _events().semi_join(self._others().where(subject_id="m2"))
        )
        scans = len(fake_spiral_sdk.scan_log)

        with pytest.raises(InconsistentSourceError, match="distinct source objects"):
            if run == "job.run":
                job.run()
            elif run == "job.run_async":
                asyncio.run(asyncio.wait_for(job.run_async(), timeout=120))
            else:
                job.nodes["both"].run()
        assert len(fake_spiral_sdk.scan_log) == scans

    @pytest.mark.parametrize("order", ["events first", "others first"])
    def test_two_tables_restricted_once_each_are_accepted(
        self, project, other_table, tmp_path, order
    ):
        """Two sources, one restriction each: no conflict; the run reads only the
        rows both restrictions keep."""
        others = self._others().where(subject_id="m2")
        job = self._job(tmp_path, others)
        events = _events().where(subject_id="m2")
        stream = (
            events.semi_join(others)
            if order == "events first"
            else events.semi_join(others.semi_join(events))
        )
        job.bind(sources={"events": stream})
        job.run()
        rows = job.nodes["rename"].as_table()
        assert sorted(rows.column("timestamp").to_pylist()) == [4, 5]

    @pytest.mark.parametrize("mode", _CACHING, ids=lambda m: m.name)
    @pytest.mark.parametrize("restricted", ["events", "others", "both"])
    def test_a_cache_over_the_slot_is_refused_naming_each_restriction(
        self, project, other_table, tmp_path, mode, restricted
    ):
        store = DeltaTableDatabase(tmp_path / "s")
        job = TestRestrictionThroughWrappers._cached_job(store, mode)
        events, others = _events(), self._others()
        if restricted in ("events", "both"):
            events = events.where(subject_id="m1")
        if restricted in ("others", "both"):
            others = others.where(subject_id="m2")

        with pytest.raises(ValueError, match="cannot cache a restricted read") as info:
            job.bind(sources={"events": events.semi_join(others)})
        if restricted in ("events", "both"):
            assert "events: where subject_id='m1'" in str(info.value)
        if restricted in ("others", "both"):
            assert "others: where subject_id='m2'" in str(info.value)


class TestBindingTwoSlotsOverOneSource:
    """One bind call over two slots that read one source (CodeRabbit, PR 435).

    Slot ``events`` reads the table; slot ``pick`` reads a column selection of it.
    Equivalent restrictions in one bind are accepted in either mapping order; a
    conflicting one, or one slot changed away from the other's, is refused and both
    slots keep their bindings.
    """

    @staticmethod
    def _pick(source: Any) -> Any:
        return source.select_data_columns(["amplitude", "flag"])

    def _job(self, tmp_path: Path) -> PipelineJob:
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode

        m1 = _events().where(subject_id="m1")
        selected = self._pick(m1)
        borrowed = SourceJobNode.from_stream(selected, name="pick")
        pick = SourceNode.from_stream(selected, name="pick")
        job = PipelineJob(name="two", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            m1.map_data({"amplitude": "a"}, label="rename")
            with pytest.raises(InconsistentSourceError, match="not supported yet"):
                MapData({"amplitude": "b"})(borrowed)
            MapData({"amplitude": "b"})(pick)
        job.bind(sources={"pick": selected})
        return job

    @staticmethod
    def _bound(job: PipelineJob) -> tuple[Any, Any]:
        return job.nodes["events"].bound_source, job.nodes["pick"].bound_source

    @pytest.mark.parametrize("order", ["events first", "pick first"])
    def test_one_restricted_object_for_both_slots_is_accepted(
        self, project, tmp_path, order
    ):
        job = self._job(tmp_path)
        events = _events().where(subject_id="m2", flag=True)
        pick = self._pick(events)
        sources: dict[str, Any] = {"events": events, "pick": pick}
        if order == "pick first":
            sources = dict(reversed(sources.items()))

        job.bind(sources=sources)

        assert self._bound(job) == (events, pick)
        summary = job.run()
        assert summary.conditions == {
            "events": Condition.of(subject_id="m2", flag=True),
            "pick": Condition.of(subject_id="m2", flag=True),
        }
        assert sorted(
            job.nodes["rename"].as_table().column("timestamp").to_pylist()
        ) == [4, 5]

    @pytest.mark.parametrize("order", ["events first", "pick first"])
    @pytest.mark.parametrize(
        "pick_read",
        [
            lambda: _events().where(subject_id="m1"),
            lambda: _events(),
            lambda: _events().where(subject_id="m2", flag=False),
        ],
        ids=["the old condition", "no restriction", "another condition"],
    )
    def test_conflicting_restrictions_are_refused_and_nothing_changes(
        self, project, tmp_path, order, pick_read
    ):
        job = self._job(tmp_path)
        before = (job.database, *self._bound(job))
        sources: dict[str, Any] = {
            "events": _events().where(subject_id="m2"),
            "pick": self._pick(pick_read()),
        }
        if order == "pick first":
            sources = dict(reversed(sources.items()))
        other = DeltaTableDatabase(tmp_path / "other")

        for _ in range(2):
            with pytest.raises(
                InconsistentSourceError, match="distinct source objects"
            ):
                job.bind(sources=sources, database=other)
            after = (job.database, *self._bound(job))
            assert all(a is b for a, b in zip(after, before, strict=True))

    @pytest.mark.parametrize("slot", ["events", "pick"])
    def test_one_slot_changed_away_from_the_other_is_refused(
        self, project, tmp_path, slot
    ):
        job = self._job(tmp_path)
        before = self._bound(job)
        m2 = _events().where(subject_id="m2")
        new = m2 if slot == "events" else self._pick(m2)

        with pytest.raises(InconsistentSourceError, match="distinct source objects"):
            job.bind(sources={slot: new})
        after = self._bound(job)
        assert after[0] is before[0] and after[1] is before[1]

    @pytest.mark.parametrize("slot", ["events", "pick"])
    def test_one_slot_rebound_to_the_same_restricted_source_is_accepted(
        self, project, tmp_path, slot
    ):
        job = self._job(tmp_path)
        m1, _ = self._bound(job)
        new = m1 if slot == "events" else self._pick(m1)

        job.bind(sources={slot: new})

        assert job.nodes[slot].bound_source is new
        assert job.run().conditions == {
            "events": Condition.of(subject_id="m1"),
            "pick": Condition.of(subject_id="m1"),
        }


class TestSummaryKeys:
    """A slot keeps its name; a direct source is numbered only when its label collides."""

    @pytest.mark.parametrize("order", ["slot first", "direct first"])
    def test_a_slot_keeps_its_name_whatever_the_visit_order(
        self, project, tmp_path, order
    ):
        from orcapod.core.nodes.source_node import SourceJobNode

        bound = _events().where(subject_id="m1")
        slot = SourceJobNode.from_stream(bound)
        direct = _source(label=slot.name).where(subject_id="m2")
        streams = [slot, direct] if order == "slot first" else [direct, slot]

        found = restrictions_in_force(streams)

        assert found[slot.name] == (
            bound.source_id,
            Condition.of(subject_id="m1"),
            None,
        )
        assert found[f"{slot.name} (2)"] == (
            direct.source_id,
            Condition.of(subject_id="m2"),
            None,
        )

    def test_a_direct_label_with_no_collision_is_not_numbered(self, project):
        first = _source(label="a").where(subject_id="m1")
        second = _source(label="b").where(subject_id="m2")
        assert set(restrictions_in_force([second, first])) == {"a", "b"}


class TestWhereTakesEqualitiesAndRanges:
    """``where`` refuses every condition but equalities and ranges, pointing to
    ``filter`` (ITL-1374, ITL-998)."""

    @pytest.mark.parametrize(
        "build",
        [
            lambda: col("subject_id").is_in(["m1", "m2"]),
            lambda: col("note").is_null(),
            lambda: (col("subject_id") == "m1") | (col("subject_id") == "m2"),
            lambda: ~(col("subject_id") == "m1"),
            lambda: col("subject_id") != "m1",
            lambda: col("timestamp") < col("channel"),
            lambda: (col("timestamp") < 2) | (col("timestamp") > 4),
            lambda: ~(col("timestamp") >= 3),
            lambda: (col("timestamp") >= 3) & ~(col("timestamp") < 5),
        ],
        ids=[
            "is_in",
            "null test",
            "or",
            "not",
            "not equal",
            "two columns",
            "or of ranges",
            "not a range",
            "range and not",
        ],
    )
    def test_any_other_condition_is_refused(self, project, build):
        """The whole condition is refused, pointing to ``filter``; nothing is read."""
        source = _source()
        fake_spiral_sdk.scan_log.clear()

        with pytest.raises(TypeError, match=r"equalities .* and ranges .*filter"):
            source.where(build())
        assert fake_spiral_sdk.scan_log == []

    def test_two_values_of_one_column_are_refused(self, project):
        """A column equals one value: two different ones are refused."""
        both = (col("subject_id") == "m1") & (col("subject_id") == "m2")

        with pytest.raises(ValueError, match="already restricted to 'm1'"):
            _source().where(both)

    def test_an_equality_built_with_col_is_taken(self, project):
        """``col(c) == v`` is the equality ``where`` takes, with the keyword's rows."""
        restricted = _source().where(col("subject_id") == "m2")

        assert _timestamps(restricted) == [4, 5]
        assert restricted.condition == Condition.of(subject_id="m2")

    def test_the_constructor_refuses_it_too(self, project):
        with pytest.raises(TypeError, match=r"equalities .* and ranges"):
            _source(where=col("timestamp").is_in([3]))
