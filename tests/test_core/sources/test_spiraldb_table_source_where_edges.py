"""Edges of a SpiralDB ``where`` found in the round-2 review of 77d75ed0 (ITL-998).

1. Every column name works as a keyword, also ``self``, ``conditions``, ``columns``
   and, for ``Condition.of``, ``cls``.
2. A timestamp bound is taken when the column's type holds it exactly, also when its
   instant is not a Python ``datetime`` in the column's zone (year 1 east of UTC, year
   9999 west of it).
3. An equality to ``-0.0`` or a NaN is refused with the ``filter`` guidance before the
   arguments are merged, also beside a different value for the same column.

Runs on the fake only.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import math
import zoneinfo
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod.sources import SpiralDBTableSource
from orcapod.types import Condition, col
from orcapod.types.condition import check_restriction
from tests.test_databases import fake_spiral_sdk

_PROJECT = "workshop-sample"
_UTC = dt.timezone.utc
_EAST = dt.timezone(dt.timedelta(hours=1))
_WEST = dt.timezone(dt.timedelta(hours=-5))
_KEYS = pa.schema([pa.field("id", pa.int64()), pa.field("lab", pa.large_string())])
_STAMPS = [
    dt.datetime(1, 1, 1, tzinfo=_UTC),
    dt.datetime(1, 1, 1, 1, tzinfo=_UTC),
    dt.datetime(2026, 11, 1, tzinfo=_UTC),
    dt.datetime(9999, 12, 31, 22, tzinfo=_UTC),
    dt.datetime(9999, 12, 31, 22, 30, tzinfo=_UTC),
]


def _table() -> pa.Table:
    """Return rows with columns named like ``where``'s parameters, and extreme times."""
    n = len(_STAMPS)
    return pa.table(
        {
            "id": pa.array(range(n), pa.int64()),
            "lab": pa.array(["a", "b", "a", "b", "a"], pa.large_string()),
            "self": pa.array([1, 2, 1, 2, 1], pa.int64()),
            "conditions": pa.array(["x", "x", "y", "y", "x"], pa.large_string()),
            "columns": pa.array([7, 8, 7, 8, 9], pa.int32()),
            "cls": pa.array([0, 0, 1, 1, 1], pa.int64()),
            "t_us": pa.array(_STAMPS, pa.timestamp("us", tz="UTC")),
            "t_s": pa.array(_STAMPS, pa.timestamp("s", tz="UTC")),
            "t_ms_paris": pa.array(_STAMPS, pa.timestamp("ms", tz="Europe/Paris")),
            "v": pa.array([-0.0, 0.0, math.nan, 1.5, 2.5], pa.float64()),
        }
    )


_LOW = dt.datetime(1677, 9, 21, 0, 12, 43, 145225, tzinfo=_UTC)
_HIGH = dt.datetime(2262, 4, 11, 23, 47, 16, 854775, tzinfo=_UTC)
_NS_ZONES = ("America/New_York", "Europe/Paris")


def _ns_count(instant: dt.datetime) -> int:
    """Return the nanoseconds since the epoch of a UTC ``instant``."""
    since = instant - dt.datetime(1970, 1, 1, tzinfo=_UTC)
    return (since // dt.timedelta(microseconds=1)) * 1000


def _nanos() -> pa.Table:
    """Return nanosecond timestamps at both ends of the 64-bit count, in two zones."""
    counts = [-(2**63), _ns_count(_LOW), 0, _ns_count(_HIGH), 2**63 - 1]
    columns: dict[str, Any] = {
        "id": pa.array(range(len(counts)), pa.int64()),
        "lab": pa.array(["a"] * len(counts), pa.large_string()),
    }
    for number, zone in enumerate(_NS_ZONES):
        columns[f"ns{number}"] = pa.array(counts, pa.int64()).view(
            pa.timestamp("ns", tz=zone)
        )
    return pa.table(columns)


@pytest.fixture()
def project(tmp_path: Path):
    """Provide a fake project holding the ``edges`` table."""
    with fake_spiral_sdk.install(tmp_path / "spiral"):
        project = fake_spiral_sdk.ExternalProject(_PROJECT)
        project.create_table("default.edges", key_schema=_KEYS).write(_table())
        project.create_table("default.nanos", key_schema=_KEYS).write(_nanos())
        fake_spiral_sdk.scan_log.clear()
        yield project
        fake_spiral_sdk.scan_log.clear()


def _source(**kwargs: Any) -> SpiralDBTableSource:
    """Return a source over ``edges``, tags kept tag-only."""
    return SpiralDBTableSource(_PROJECT, "edges", tag_only_columns=True, **kwargs)


def _ids(source: SpiralDBTableSource) -> list[Any]:
    """Return the ``id`` of every row ``source`` reads (sync)."""
    return source.as_table()["id"].to_pylist()


def _async_ids(source: SpiralDBTableSource) -> list[Any]:
    """Return the ``id`` of every row ``source`` reads (async)."""

    async def collect() -> list[Any]:
        return [tag.as_dict()["id"] async for tag, _ in source.async_iter_data()]

    return asyncio.run(asyncio.wait_for(collect(), timeout=60))


def _expected(mask: Any) -> list[Any]:
    """Return the ids of the rows where the pyarrow ``mask`` holds."""
    return _table().filter(pc.fill_null(mask, False))["id"].to_pylist()


class TestEveryColumnNameIsAKeyword:
    @pytest.mark.parametrize(
        ("keywords", "expected"),
        [
            ({"self": 1}, [0, 2, 4]),
            ({"conditions": "x"}, [0, 1, 4]),
            ({"columns": 8}, [1, 3]),
            ({"self": 1, "conditions": "x", "columns": 7}, [0]),
            ({"columns": 7, "conditions": "x", "self": 1}, [0]),
        ],
        ids=["self", "conditions", "columns", "all three", "all three reversed"],
    )
    def test_where_takes_a_column_named_like_its_parameters(
        self, project, keywords, expected
    ):
        """Keywords, a mapping, and keywords beside a range: one set of rows."""
        for source, condition in (
            (_source().where(**keywords), Condition.of(keywords)),
            (_source().where(keywords), Condition.of(keywords)),
            (
                _source().where(col("id") >= 0, **keywords),
                Condition.of(col("id") >= 0, keywords),
            ),
        ):
            assert _ids(source) == expected
            assert _async_ids(source) == expected
            assert source.condition == condition

    def test_condition_of_takes_a_column_named_cls(self, project):
        condition = Condition.of(cls=1, self=1)

        assert condition == Condition.of({"self": 1, "cls": 1})
        assert _ids(_source().where(condition)) == [2, 4]


_BOUNDS = [
    dt.datetime(1, 1, 1, tzinfo=_EAST),
    dt.datetime(1, 1, 1, 1, 30, tzinfo=_EAST),
    dt.datetime(9999, 12, 31, 23, tzinfo=_WEST),
    dt.datetime(9999, 12, 31, 18, 30, tzinfo=_WEST),
]


class TestTimestampBoundsAtTheEndsOfTheCalendar:
    @pytest.mark.parametrize("column", ["t_us", "t_s", "t_ms_paris"])
    @pytest.mark.parametrize(
        "bound",
        _BOUNDS,
        ids=["year 1 +01:00", "year 1 01:30+01:00", "year 9999 -05:00", "18:30-05:00"],
    )
    @pytest.mark.parametrize("op", ["<", "<=", ">", ">="])
    def test_a_bound_the_column_holds_exactly_is_taken(
        self, project, column, bound, op
    ):
        """The instant converts exactly to the column's type, though it is not a
        Python ``datetime`` in UTC: taken, and the rows are pyarrow's."""
        build = {
            "<": lambda c: c < bound,
            "<=": lambda c: c <= bound,
            ">": lambda c: c > bound,
            ">=": lambda c: c >= bound,
        }[op]
        compare = {"<": pc.less, "<=": pc.less_equal, ">": pc.greater}.get(
            op, pc.greater_equal
        )
        table = _table()
        expected = _expected(
            compare(
                table[column], pa.scalar(bound, type=table.schema.field(column).type)
            )
        )

        source = _source().where(build(col(column)))

        assert _ids(source) == expected
        assert _async_ids(source) == expected

    def test_a_bound_finer_than_the_column_is_still_refused(self, project):
        bound = dt.datetime(1, 1, 1, 0, 0, 0, 5, tzinfo=_EAST)

        with pytest.raises(TypeError, match="cannot hold .* exactly"):
            _source().where(col("t_s") >= bound)
        assert fake_spiral_sdk.scan_log == []

    @pytest.mark.parametrize(
        "bound",
        [dt.datetime(1, 1, 1, tzinfo=_UTC), dt.datetime(9999, 12, 31, tzinfo=_UTC)],
        ids=["year 1", "year 9999"],
    )
    def test_a_bound_beyond_a_nanosecond_column_is_refused(self, project, bound):
        """``timestamp[ns]`` reaches 1677 to 2262 only."""
        condition = Condition.of(col("t") >= bound)
        schema = pa.schema([pa.field("t", pa.timestamp("ns", "UTC"))])

        with pytest.raises(TypeError, match="beyond the range of a 64-bit count of ns"):
            check_restriction(condition, schema)


class TestNanosecondBoundsAtTheEndsOfTheCount:
    @pytest.mark.parametrize("number", range(len(_NS_ZONES)), ids=list(_NS_ZONES))
    @pytest.mark.parametrize(
        ("instant", "op"),
        [(_LOW, ">="), (_LOW, ">"), (_HIGH, "<="), (_HIGH, "<")],
        ids=["low >=", "low >", "high <=", "high <"],
    )
    def test_a_zoned_bound_at_the_edge_reads_pyarrows_rows(
        self, project, number, instant, op
    ):
        """Bounds a ``timestamp[ns]`` column holds at the ends of its 64-bit count,
        given in the column's zone, read the rows pyarrow selects (sync and async)."""
        column = f"ns{number}"
        bound = instant.astimezone(zoneinfo.ZoneInfo(_NS_ZONES[number]))
        table = _nanos()
        compare = {
            ">=": pc.greater_equal,
            ">": pc.greater,
            "<=": pc.less_equal,
            "<": pc.less,
        }[op]
        # pyarrow drops the seconds of an offset (New York's 1677 offset is -4:56:02),
        # so the expected rows compare the integer counts.
        count = _ns_count(instant)
        expected = table.filter(
            compare(table[column].cast(pa.int64()), pa.scalar(count, pa.int64()))
        )["id"].to_pylist()
        build = {
            ">=": lambda c: c >= bound,
            ">": lambda c: c > bound,
            "<=": lambda c: c <= bound,
            "<": lambda c: c < bound,
        }[op]

        source = SpiralDBTableSource(_PROJECT, "nanos", tag_only_columns=True).where(
            build(col(column))
        )

        assert expected == {">=": [1, 2, 3, 4], ">": [2, 3, 4], "<=": [0, 1, 2, 3]}.get(
            op, [0, 1, 2]
        )
        assert _ids(source) == expected
        assert _async_ids(source) == expected


class TestSpecialFloatsBeforeMerging:
    @pytest.mark.parametrize("refused", [-0.0, math.nan], ids=["negative zero", "NaN"])
    @pytest.mark.parametrize(
        "where",
        [
            lambda r: _source().where(col("id") >= 0, {"v": r}, v=1.5),
            lambda r: _source().where(col("id") >= 0, {"v": 1.5}, v=r),
            lambda r: _source().where({"v": r}, {"v": 1.5}),
            lambda r: _source().where({"v": 1.5}, {"v": r}),
            lambda r: _source().where(col("v") == 1.5, {"v": r}),
        ],
        ids=[
            "mapping refused, keyword other",
            "mapping other, keyword refused",
            "two mappings, refused first",
            "two mappings, refused last",
            "col other, mapping refused",
        ],
    )
    def test_refused_with_the_filter_guidance_beside_another_value(
        self, project, refused, where
    ):
        """Not the generic two-values error: the specified ``TypeError``."""
        with pytest.raises(TypeError, match=r"ITL-1480.*filter"):
            where(refused)
        assert fake_spiral_sdk.scan_log == []
