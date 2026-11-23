"""Every finding of the independent review of PR 497 (Filter), as a regression test.

The review read head ``2bc65cc4``. Each class below names its finding; a fixed finding has
a test that failed before the fix, parametrized over the class of inputs the finding names;
a finding documented as a limit has a test that pins the documented behaviour. Findings 2,
3, 4 and 7 were fixed in ``Condition`` (piece A, ITL-1374), whose own tests are in
``tests/test_types_condition_tree.py``; the tests here check them through ``Filter``.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Literal, TypeAlias

import pyarrow as pa
import pytest

from orcapod import PipelineJob, col
from orcapod.core.operators import Filter, SelectDataColumns
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.types import Condition

from .several_items import async_rows

pytestmark = pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")

TimeUnit: TypeAlias = Literal["s", "ms", "us", "ns"]
UNITS: tuple[TimeUnit, ...] = ("s", "ms", "us", "ns")
EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


def _instants(unit: TimeUnit) -> ArrowTableStream:
    """A stream with a zoned timestamp column at ``unit``: the epoch and one second on."""
    values = pa.array([0, 1], pa.int64()).cast(pa.timestamp("s", "UTC"))
    return ArrowTableStream(
        pa.table({"id": [1, 2], "x": values.cast(pa.timestamp(unit, "UTC"))}),
        tag_columns=["id"],
    )


def _ids(stream) -> list[int]:
    return stream.as_table()["id"].to_pylist()


class TestFinding1ContextPassesThrough:
    """Sync filtering replaced each kept row's data context with the default."""

    @pytest.mark.parametrize(
        ("condition", "kept"),
        [
            (col("k") >= 2, ["held-b", "held-c"]),
            (col("k") == 1, ["held-a"]),
            (col("k").is_in([1, 3]), ["held-a", "held-c"]),
            (col("k") > 9, []),
        ],
        ids=["tail", "head", "gaps", "none"],
    )
    def test_each_kept_rows_context_is_its_own(self, condition, kept):
        table = pa.table(
            {
                "k": pa.array([1, 2, 3], pa.int64()),
                "v": pa.array([10, 20, 30], pa.int64()),
                "_context_key": pa.array(
                    ["held-a", "held-b", "held-c"], pa.large_string()
                ),
            }
        )
        stream = ArrowTableStream(table, tag_columns=["k"])
        out = Filter(condition)(stream).as_table(columns={"context": True})
        assert out["_context_key"].to_pylist() == kept
        # As every operator that passes rows through keeps it.
        selected = SelectDataColumns(["v"])(stream).as_table(columns={"context": True})
        assert selected["_context_key"].to_pylist() == ["held-a", "held-b", "held-c"]


class _CountingZone(dt.tzinfo):
    """A mutable zone that counts the times its offset is read."""

    def __init__(self) -> None:
        self.hours = 0
        self.calls = 0

    def utcoffset(self, value: dt.datetime | None) -> dt.timedelta:
        self.calls += 1
        return dt.timedelta(hours=self.hours)

    def dst(self, value: dt.datetime | None) -> dt.timedelta:
        return dt.timedelta(0)

    def tzname(self, value: dt.datetime | None) -> str:
        return "counting"


class TestFinding2DatetimesAreFrozenWhenBuilt:
    """A datetime with a mutable tzinfo stayed live: its callback ran in head() and
    a changed offset chose other rows under the same hash."""

    @pytest.mark.parametrize("unit", UNITS)
    @pytest.mark.parametrize(
        "build",
        [
            lambda v: col("x") == v,
            lambda v: col("x") >= v,
            lambda v: col("x").is_in([v]),
            lambda v: col("x").between(v, v),
        ],
        ids=["equal", "order", "is_in", "between"],
    )
    def test_no_callback_after_build_and_the_same_rows(self, unit, build):
        zone = _CountingZone()
        stream = Filter(build(dt.datetime(1970, 1, 1, tzinfo=zone)))(_instants(unit))
        before = stream.pipeline_hash()
        calls = zone.calls
        head = _ids(stream.head())
        zone.hours = 1  # would move the instant by an hour if still read
        assert _ids(stream) == head
        assert zone.calls == calls
        assert stream.pipeline_hash() == before


class TestFinding3OneInstantOneIdentity:
    """One instant spelled with a sub-second offset was refused, and its saved form
    changed its identity."""

    @pytest.mark.parametrize("unit", ("us", "ns"))
    @pytest.mark.parametrize(
        "spelling",
        [
            EPOCH + dt.timedelta(seconds=1),
            dt.datetime(1970, 1, 1, tzinfo=dt.timezone(-dt.timedelta(seconds=1))),
            dt.datetime(
                1970,
                1,
                1,
                5,
                30,
                1,
                tzinfo=dt.timezone(dt.timedelta(hours=5, minutes=30)),
            ),
            dt.datetime(
                1970,
                1,
                1,
                0,
                0,
                0,
                999_999,
                tzinfo=dt.timezone(-dt.timedelta(microseconds=1)),
            ),
        ],
        ids=["utc", "whole_second_offset", "half_hour_offset", "microsecond_offset"],
    )
    def test_every_spelling_selects_the_row_and_keeps_its_identity(
        self, unit, spelling
    ):
        condition = col("x") == spelling
        reference = col("x") == EPOCH + dt.timedelta(seconds=1)
        assert condition.canonical_form() == reference.canonical_form()
        assert _ids(Filter(condition)(_instants(unit))) == [2]
        saved = json.loads(json.dumps(Filter(condition).to_config()))
        rebuilt = Filter.from_config(saved)
        assert rebuilt.condition.canonical_form() == reference.canonical_form()


class TestFinding4BetweenComparesInstants:
    """``between`` compared wall times, so a range across a daylight-saving overlap that
    ends before it starts was accepted."""

    def test_a_reversed_range_across_the_overlap_is_refused(self):
        zone = pytest.importorskip("zoneinfo").ZoneInfo("America/New_York")
        start = dt.datetime(2025, 11, 2, 1, 0, tzinfo=zone, fold=1)
        end = dt.datetime(2025, 11, 2, 1, 30, tzinfo=zone, fold=0)
        assert start.timestamp() > end.timestamp()
        with pytest.raises(ValueError, match="low end above its high end"):
            _instants("us").filter(col("x").between(start, end))


class TestFinding5DirectChannelLimits:
    """The direct async channel cannot check what it does not receive. Documented in
    ``Filter.async_execute``; pinned here, beside the job path, which refuses."""

    @staticmethod
    def _narrow() -> ArrowTableSource:
        table = pa.table(
            {"k": pa.array([1], pa.int8()), "v": pa.array([1], pa.int64())}
        )
        return ArrowTableSource(
            table, tag_columns=["k"], tag_only_columns=True, source_id="narrow"
        )

    def test_no_rows_on_the_channel_are_not_checked(self):
        empty = self._narrow().filter(v=999)
        with pytest.raises(InputValidationError):
            Filter(Condition.of(k=300))(empty).as_table()  # sync refuses
        assert async_rows(Filter(Condition.of(k=300)), empty) == []  # the limit

    def test_a_raw_null_column_arrives_typed_as_text(self):
        raw = ArrowTableStream(
            pa.table({"k": [1, 2], "v": pa.nulls(2)}), tag_columns=["k"]
        )
        with pytest.raises(InputValidationError, match="null"):
            Filter(Condition.of(v="x"))(raw).as_table()  # sync refuses
        assert async_rows(Filter(Condition.of(v="x")), raw) == []  # the limit

    @pytest.mark.parametrize(
        "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
    )
    def test_a_job_refuses_with_no_rows(self, orchestrator):
        job = PipelineJob(name="narrow", database=InMemoryArrowDatabase())
        with job:
            empty = self._narrow().filter(v=999, label="none")
            Filter(Condition.of(k=300))(empty, label="kept")
        with pytest.raises(InputValidationError, match="int8"):
            job.run(orchestrator=orchestrator())


class TestFinding7SavedTreesLoadAsBuilt:
    """A saved tree with two equalities on one column loaded and changed meaning when
    saved again. Contradictions are now kept as written (developer, 2026-11-22): the
    saved tree loads as built, selects no row, and saves to the same form."""

    @pytest.mark.parametrize("values", [(1, 2), (2, 1), (1, 2, 3)])
    def test_a_contradiction_round_trips_with_no_row(self, values):
        source = ArrowTableSource(
            pa.table({"k": pa.array([1, 2, 3], pa.int64()), "v": [4, 5, 6]}),
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="keys",
        )
        condition = col("k") == values[0]
        for value in values[1:]:
            condition = condition & (col("k") == value)
        op = Filter(condition)
        saved = json.loads(json.dumps(op.to_config()))
        rebuilt = Filter.from_config(saved)
        assert rebuilt.condition == condition
        assert rebuilt.to_config() == saved
        assert rebuilt(source).as_table().num_rows == 0
        assert rebuilt.content_hash() == op.content_hash()


class TestChangelogClaimOfPr497:
    """CodeRabbit (PR 497 review body): a row is dropped when its WHOLE condition is
    false or unknown, so ``is_null() | ...`` keeps the null rows the changelog names."""

    @pytest.mark.parametrize(
        ("condition", "kept"),
        [
            (col("rate").is_null() | (col("rate") > 1.0), [1, 2, 3]),
            (col("rate") > 1.0, [1, 3]),
            (~(col("rate") > 1.0), [4]),
            ((col("rate") > 1.0) | (col("k") == 2), [1, 2, 3]),
        ],
        ids=["null_or_value", "value_alone", "not_value", "value_or_other_column"],
    )
    def test_a_row_is_kept_when_its_whole_condition_holds(self, condition, kept):
        stream = ArrowTableStream(
            pa.table(
                {
                    "k": [1, 2, 3, 4],
                    "rate": pa.array([4.0, None, 2.5, 0.5], pa.float64()),
                }
            ),
            tag_columns=["k"],
        )
        assert sorted(_ids_by(Filter(condition)(stream), "k")) == kept


def _ids_by(stream, name: str) -> list[int]:
    return stream.as_table()[name].to_pylist()
