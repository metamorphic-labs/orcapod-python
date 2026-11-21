"""``datetime`` in both directions: what a function receives, what ``-> datetime`` stores
(ITL-1268).

Every Arrow timestamp type (``s``, ``ms``, ``us``, ``ns``, with or without a time zone)
reads as the Python type ``datetime``, and a source stores it as given. A function
receives a naive ``datetime`` for a column without a zone, an aware one for a zoned
column, and a ``pandas.Timestamp`` for an ``ns`` column. ``-> datetime`` declares an
instant, ``timestamp[us, tz=UTC]`` (developer, November 21, 2026: the mapping does not
change). An aware value of any zone is stored as its instant; a naive value is refused,
with a message that names the time zone and how to attach one; a value with a part
below one microsecond is refused, never truncated. One rule decides it for every path
that stores a Python ``datetime``: sources and function outputs alike.

The streams here have two timestamp columns (``a``, ``b``) and two tags, in two table
layouts.
"""

from __future__ import annotations

import pickle
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.sources import ArrowTableSource, DictSource, ListSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import (
    NaiveDatetimeError,
    OutputTypeError,
    RowFailuresWarning,
    SubMicrosecondDatetimeError,
    UnstorableDatetimeError,
)
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.semantic_types.universal_converter import UniversalTypeConverter

Unit = Literal["s", "ms", "us", "ns"]
UNITS: tuple[Unit, ...] = ("s", "ms", "us", "ns")
ZONES = (None, "UTC", "America/Los_Angeles")
LAYOUTS = {
    "tags_first": ("k", "run", "a", "b"),
    "data_first": ("b", "a", "run", "k"),
}
ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}
STORED = pa.timestamp("us", tz="UTC")
# 2023-11-14 22:13:20 UTC and 2020-09-13 12:26:40 UTC, in seconds.
SECONDS = {"a": [1_700_000_000, 1_600_000_000], "b": [1_600_000_000, 1_700_000_000]}
# The same instants with a part below one microsecond (``ns`` only), and with none.
NANOS_SUB_MICRO = 123_456_789
NANOS_WHOLE_MICRO = 123_456_000

UTC_HINT = "value.replace(tzinfo=timezone.utc)"


def _timestamp(unit: Unit, zone: str | None) -> pa.DataType:
    """The Arrow type ``timestamp[unit]`` or ``timestamp[unit, tz=zone]``."""
    return pa.timestamp(unit) if zone is None else pa.timestamp(unit, tz=zone)


def _column(
    seconds: list[int], unit: Unit, zone: str | None, nanos: int = 0
) -> pa.Array[Any]:
    """Timestamps of ``seconds`` (UTC) plus ``nanos``, as ``timestamp[unit, zone]``.

    A column without a zone holds the UTC wall time.
    """
    per_second = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}[unit]
    extra = nanos // (1_000_000_000 // per_second)
    raw = pa.array([s * per_second + extra for s in seconds], pa.int64())
    return raw.cast(_timestamp(unit, zone))


def _source(
    unit: Unit, zone: str | None, layout: str = "tags_first", nanos: int = 0
) -> ArrowTableSource:
    """Two rows with tags ``k`` and ``run`` and two timestamp columns ``a`` and ``b``."""
    columns = {
        "k": pa.array([1, 2], pa.int64()),
        "run": pa.array(["r1", "r2"], pa.large_string()),
        "a": _column(SECONDS["a"], unit, zone, nanos),
        "b": _column(SECONDS["b"], unit, zone, nanos),
    }
    names = LAYOUTS[layout]
    table = pa.table(
        [columns[name] for name in names],
        schema=pa.schema(
            [pa.field(name, columns[name].type, nullable=False) for name in names]
        ),
    )
    return ArrowTableSource(
        table, tag_columns=["k", "run"], tag_only_columns=True, source_id="times"
    )


def _instants(table: pa.Table, column: str) -> list[pd.Timestamp]:
    """The instants of a column, in UTC, ordered by ``k`` (exact to the nanosecond)."""
    instants: list[pd.Timestamp] = []
    for value in table.sort_by("k").column(column).to_pylist():
        assert value is not None
        stamp = pd.Timestamp(value)
        assert isinstance(stamp, pd.Timestamp)  # never NaT: the column has no null
        zoned = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp
        instants.append(zoned.tz_convert("UTC"))
    return instants


def _expected(column: str, nanos: int = 0, shift: timedelta = timedelta()) -> list[Any]:
    """The instants ``_source`` holds in ``column``, ordered by ``k``."""
    return [
        pd.Timestamp(s, unit="s", tz="UTC") + pd.Timedelta(nanos, "ns") + shift
        for s in SECONDS[column]
    ]


# ---------------------------------------------------------------------------
# Reading: every timestamp type reads as datetime; what a function receives
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("layout", list(LAYOUTS))
@pytest.mark.parametrize("zone", ZONES, ids=str)
@pytest.mark.parametrize("unit", UNITS)
def test_every_timestamp_type_reads_as_datetime_and_is_stored_as_given(
    unit: Unit, zone: str | None, layout: str
) -> None:
    """A source over two columns of one timestamp type: both read as ``datetime``,
    and both are stored with their own unit and zone."""
    source = _source(unit, zone, layout)
    data_schema = source.output_schema()[1]
    assert data_schema["a"] is datetime and data_schema["b"] is datetime
    table = source.as_table()
    for name in ("a", "b"):
        assert table.column(name).type == _timestamp(unit, zone)


@pytest.mark.parametrize("zone", ZONES, ids=str)
@pytest.mark.parametrize("unit", UNITS)
def test_what_a_function_receives(unit: Unit, zone: str | None) -> None:
    """A naive ``datetime`` for a column without a zone, an aware one for a zoned
    column; a ``pandas.Timestamp`` for an ``ns`` column."""
    seen: list[Any] = []

    @function_pod(output_keys="n")
    def look(a: datetime, b: datetime) -> int:
        seen.extend([a, b])
        return 0

    _ = look.pod(_source(unit, zone)).as_table()
    assert len(seen) == 4
    for value in seen:
        assert isinstance(value, datetime)
        assert isinstance(value, pd.Timestamp) == (unit == "ns")
        assert (value.tzinfo is None) == (zone is None)


# ---------------------------------------------------------------------------
# -> datetime: what is stored, sync and async, plain pod and job node
# ---------------------------------------------------------------------------


def _identity() -> Any:
    @function_pod(output_keys=["a_out", "b_out"])
    def same(a: datetime, b: datetime) -> tuple[datetime, datetime]:
        return a, b

    return same


def _shift() -> Any:
    @function_pod(output_keys=["a_out", "b_out"])
    def shift(a: datetime, b: datetime) -> tuple[datetime, datetime]:
        return a + timedelta(hours=1), b + timedelta(hours=1)

    return shift


def _attach_utc() -> Any:
    @function_pod(output_keys=["a_out", "b_out"])
    def attach(a: datetime, b: datetime) -> tuple[datetime, datetime]:
        return a.replace(tzinfo=timezone.utc), b.replace(tzinfo=timezone.utc)

    return attach


def _async_identity() -> Any:
    @function_pod(output_keys=["a_out", "b_out"])
    async def same(a: datetime, b: datetime) -> tuple[datetime, datetime]:
        return a, b

    return same


FUNCTIONS: dict[str, tuple[Callable[[], Any], timedelta]] = {
    "identity": (_identity, timedelta()),
    "shift": (_shift, timedelta(hours=1)),
    "async_identity": (_async_identity, timedelta()),
}


@pytest.mark.parametrize("layout", list(LAYOUTS))
@pytest.mark.parametrize("zone", ["UTC", "America/Los_Angeles"])
@pytest.mark.parametrize("unit", UNITS)
@pytest.mark.parametrize("function", list(FUNCTIONS))
def test_an_aware_value_is_stored_as_its_instant(
    function: str, unit: Unit, zone: str, layout: str
) -> None:
    """Over zoned columns of every unit, an identity, a shift and an async function
    store each instant exactly, as ``timestamp[us, tz=UTC]``."""
    make, shift = FUNCTIONS[function]
    table = make().pod(_source(unit, zone, layout)).as_table()
    for out, column in (("a_out", "a"), ("b_out", "b")):
        assert table.column(out).type == STORED
        assert _instants(table, out) == _expected(column, shift=shift)


@pytest.mark.parametrize("layout", list(LAYOUTS))
@pytest.mark.parametrize("unit", UNITS)
def test_attaching_a_zone_stores_a_naive_column(unit: Unit, layout: str) -> None:
    """Over columns without a zone, a function that attaches one stores the instant."""
    table = _attach_utc().pod(_source(unit, None, layout)).as_table()
    for out, column in (("a_out", "a"), ("b_out", "b")):
        assert table.column(out).type == STORED
        assert _instants(table, out) == _expected(column)


@pytest.mark.parametrize(
    "zone", [ZoneInfo("America/Los_Angeles"), timezone(timedelta(hours=2))], ids=str
)
def test_a_value_in_another_zone_is_stored_as_the_same_instant(zone: Any) -> None:
    """An aware value in a zone other than UTC is accepted: the stored instant is the
    same, in UTC."""
    wall = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=zone)

    @function_pod(output_keys=["first", "second"])
    def give(a: datetime, b: datetime) -> tuple[datetime, datetime]:
        return wall, wall + timedelta(days=1)

    table = give.pod(_source("us", "UTC")).as_table()
    assert table.column("first").type == STORED
    stored = [value for value in table.column("first").to_pylist() if value is not None]
    assert stored == [wall, wall]
    assert all(value.utcoffset() == timedelta(0) for value in stored)
    assert table.column("second").to_pylist() == [wall + timedelta(days=1)] * 2


# ---------------------------------------------------------------------------
# Refusals: a naive value; a part below one microsecond
# ---------------------------------------------------------------------------


def _assert_naive_message(message: str) -> None:
    """The message names the time zone and how to attach one, and no value."""
    assert "no timezone info" in message
    assert "instant" in message
    assert UTC_HINT in message
    assert "astimezone" in message


@pytest.mark.parametrize("layout", list(LAYOUTS))
@pytest.mark.parametrize("unit", UNITS)
@pytest.mark.parametrize("function", ["identity", "shift", "async_identity"])
def test_a_naive_value_is_refused_with_a_message_that_says_why(
    function: str, unit: Unit, layout: str
) -> None:
    """Over columns without a zone, returning the naive value (as it is, shifted or
    from an async function) is refused, naming the time zone and the conversion."""
    make, _ = FUNCTIONS[function]
    with pytest.raises(OutputTypeError) as caught:
        _ = make().pod(_source(unit, None, layout)).as_table()
    message = str(caught.value)
    _assert_naive_message(message)
    assert "output 'a_out'" in message
    # No returned value in the message, nor through chaining.
    assert "2023" not in message and "2020" not in message
    assert caught.value.__suppress_context__


@pytest.mark.parametrize("layout", list(LAYOUTS))
@pytest.mark.parametrize("zone", ["UTC", "America/Los_Angeles"])
def test_a_part_below_one_microsecond_is_refused(zone: str, layout: str) -> None:
    """Over ``ns`` columns with nanoseconds, the identity is refused, not truncated;
    the message names the output and what would be lost."""
    source = _source("ns", zone, layout, nanos=NANOS_SUB_MICRO)
    with pytest.raises(OutputTypeError) as caught:
        _ = _identity().pod(source).as_table()
    message = str(caught.value)
    assert "output 'a_out'" in message
    assert "pandas._libs.tslibs.timestamps.Timestamp" in message
    assert "below one microsecond" in message
    assert 'value.floor("us")' in message
    assert "123456789" not in message


@pytest.mark.parametrize("layout", list(LAYOUTS))
@pytest.mark.parametrize("zone", ["UTC", "America/Los_Angeles"])
def test_nanoseconds_of_whole_microseconds_are_accepted(zone: str, layout: str) -> None:
    """An ``ns`` value whose part below one microsecond is zero is stored exactly."""
    source = _source("ns", zone, layout, nanos=NANOS_WHOLE_MICRO)
    table = _identity().pod(source).as_table()
    assert _instants(table, "a_out") == _expected("a", nanos=NANOS_WHOLE_MICRO)
    assert _instants(table, "b_out") == _expected("b", nanos=NANOS_WHOLE_MICRO)


def test_a_floored_value_is_accepted() -> None:
    """The conversion the message suggests stores the value to the microsecond."""

    @function_pod(output_keys=["a_out", "b_out"])
    def floor(a: datetime, b: datetime) -> tuple[datetime, datetime]:
        # An ns column gives a pandas.Timestamp; floor it to whole microseconds.
        assert isinstance(a, pd.Timestamp) and isinstance(b, pd.Timestamp)
        return a.floor("us"), b.floor("us")

    source = _source("ns", "UTC", nanos=NANOS_SUB_MICRO)
    table = floor.pod(source).as_table()
    assert _instants(table, "a_out") == _expected("a", nanos=NANOS_WHOLE_MICRO)


def test_nested_values_follow_the_same_rule() -> None:
    """A ``datetime`` inside a list or a dict output is checked by the same rule."""

    @function_pod(output_keys=["many", "named"])
    def nested(a: datetime, b: datetime) -> tuple[list[datetime], dict[str, datetime]]:
        return [a, b], {"a": a, "b": b}

    for zone, refused in ((None, NaiveDatetimeError), ("UTC", None)):
        stream = nested.pod(_source("us", zone))
        if refused is None:
            table = stream.as_table()
            assert len(table.column("many")[0].as_py()) == 2
            continue
        with pytest.raises(OutputTypeError, match="no timezone info"):
            _ = stream.as_table()
    with pytest.raises(OutputTypeError, match="below one microsecond"):
        _ = nested.pod(_source("ns", "UTC", nanos=NANOS_SUB_MICRO)).as_table()


# ---------------------------------------------------------------------------
# In a job: a refused row fails as a row; stored results read back
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("function", ["identity", "async_identity"])
def test_a_job_stores_instants_and_reads_them_back(
    function: str, orchestrator: str
) -> None:
    """A job node over zoned ``ns`` columns stores the instants; reading the stored
    results back gives them again."""
    make, _ = FUNCTIONS[function]
    job = PipelineJob(name="itl1268", database=InMemoryArrowDatabase())
    with job:
        _ = make().pod(
            _source("ns", "America/Los_Angeles", nanos=NANOS_WHOLE_MICRO), label="same"
        )
    summary = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert "all rows succeeded" in str(summary)
    node = job.nodes["same"]
    for table in (node.as_table(), node.as_source().as_table()):
        assert table.column("a_out").type == STORED
        assert _instants(table, "a_out") == _expected("a", nanos=NANOS_WHOLE_MICRO)
        assert _instants(table, "b_out") == _expected("b", nanos=NANOS_WHOLE_MICRO)


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize(
    ("zone", "nanos", "words"),
    [(None, 0, "no timezone info"), ("UTC", NANOS_SUB_MICRO, "below one microsecond")],
    ids=["naive", "sub_microsecond"],
)
def test_a_refused_value_fails_its_row_in_a_job(
    zone: str | None, nanos: int, words: str, orchestrator: str
) -> None:
    """In a job a refused value fails its row (the error policy), and nothing is
    stored for it; the error is the same as outside a job."""
    job = PipelineJob(name="itl1268", database=InMemoryArrowDatabase())
    with job:
        _ = _identity().pod(_source("ns", zone, nanos=nanos), label="same")
    with pytest.warns(RowFailuresWarning, match=words):
        summary = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert "2 rows failed" in str(summary)
    assert job.nodes["same"].as_source().as_table().num_rows == 0


# ---------------------------------------------------------------------------
# Sources over Python values: the same rule
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "build",
    [
        lambda v: DictSource(
            [{"k": 1, "a": v, "b": v}], tag_columns=["k"], tag_only_columns=True
        ),
        lambda v: DictSource(
            [{"k": 1, "a": v, "b": v}],
            tag_columns=["k"],
            tag_only_columns=True,
            data_schema={"k": int, "a": datetime, "b": datetime},
        ),
        lambda v: ListSource("a", [v, v]),
    ],
    ids=["dict_inferred", "dict_declared", "list"],
)
def test_a_source_refuses_a_naive_value_by_the_same_rule(build: Any) -> None:
    """A source over Python values raises the same error, with the value."""
    with pytest.raises(NaiveDatetimeError) as caught:
        _ = build(datetime(2026, 1, 2, 3, 4, 5)).as_table()
    _assert_naive_message(str(caught.value))
    assert "Got: datetime.datetime(2026, 1, 2, 3, 4, 5)" in str(caught.value)
    assert isinstance(caught.value, ValueError)


def test_a_source_refuses_a_part_below_one_microsecond() -> None:
    """A source declared ``datetime`` refuses a ``pandas.Timestamp`` with
    nanoseconds and stores one with whole microseconds."""
    schema = {"k": int, "a": datetime, "b": datetime}

    def build(value: Any) -> DictSource:
        return DictSource(
            [{"k": 1, "a": value, "b": value}],
            tag_columns=["k"],
            tag_only_columns=True,
            data_schema=schema,
        )

    with pytest.raises(SubMicrosecondDatetimeError, match="below one microsecond"):
        _ = build(pd.Timestamp("2023-11-14 22:13:20.123456789", tz="UTC")).as_table()
    whole = pd.Timestamp("2023-11-14 22:13:20.123456", tz="UTC")
    table = build(whole).as_table()
    assert table.column("a").to_pylist() == [whole] and table.column("b").type == STORED


# ---------------------------------------------------------------------------
# The rule itself
# ---------------------------------------------------------------------------


def test_coerce_utc_reads_a_naive_value_as_utc_and_still_refuses_nanoseconds() -> None:
    """A converter built with ``coerce_utc`` attaches UTC to a naive value; a value
    below one microsecond is refused under either policy."""
    converter = UniversalTypeConverter(datetime_timezone="coerce_utc")
    convert = converter.get_python_to_arrow_converter(datetime)
    assert convert(datetime(2026, 1, 2)) == datetime(2026, 1, 2, tzinfo=timezone.utc)
    with pytest.raises(SubMicrosecondDatetimeError):
        convert(pd.Timestamp("2023-11-14 22:13:20.123456789"))
    strict = UniversalTypeConverter().get_python_to_arrow_converter(datetime)
    with pytest.raises(NaiveDatetimeError):
        strict(datetime(2026, 1, 2))


@pytest.mark.parametrize(
    "error",
    [
        NaiveDatetimeError(datetime(2026, 1, 2)),
        SubMicrosecondDatetimeError(pd.Timestamp("2023-11-14 22:13:20.123456789")),
    ],
)
def test_the_errors_pickle_and_keep_reason_and_hint(
    error: UnstorableDatetimeError,
) -> None:
    """Executors send errors across processes: the error pickles with its value, and
    its reason and hint name no value."""
    copy = pickle.loads(pickle.dumps(error))
    assert type(copy) is type(error)
    assert str(copy) == str(error)
    assert "2026" not in error.reason + error.hint
    assert "2023" not in error.reason + error.hint
