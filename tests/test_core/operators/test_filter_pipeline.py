"""``Filter`` as a step of a pipeline (ITL-1382, the section 8 matrix of ITL-1374).

What ``test_filter.py`` and ``test_filter_mask.py`` do not cover: a saved job holding a
filter of every condition kind, the restricted-aggregation warning, an identity that does
not follow the input's column width, and ``where`` against ``filter`` on one table.
"""

from __future__ import annotations

import datetime as dt
import warnings
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, col, function_pod
from orcapod.core.operators import Filter
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import RestrictedAggregationWarning
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.sources import SpiralDBTableSource
from orcapod.types import CacheMode, Condition
from tests.test_databases import fake_spiral_sdk

pytestmark = pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")

_AT = dt.datetime(2026, 11, 25, 9, 0, tzinfo=dt.timezone.utc)
_DAY = dt.date(2026, 11, 25)


def _recordings() -> ArrowTableSource:
    """Two tags, data of several types, a struct column and a NaN."""
    table = pa.table(
        {
            "session": pa.array(["s1", "s1", "s2", "s2", "s3"], pa.large_string()),
            "unit": pa.array([3, 7, 3, 7, 3], pa.uint32()),
            "t": pa.array([-0.0, 1.2, float("nan"), 0.5, 3.1], pa.float64()),
            "rate": pa.array([4.0, None, 2.5, 6.1, None], pa.float64()),
            "at": pa.array([_AT] * 5, pa.timestamp("us", tz="UTC")),
            "day": pa.array([_DAY] * 5, pa.date32()),
            "info": pa.array(
                [
                    {"kind": "good", "score": 1},
                    {"kind": "noise", "score": 2},
                    None,
                    {"kind": "good", "score": 3},
                    {"kind": "mua", "score": 4},
                ],
                pa.struct(
                    [pa.field("kind", pa.large_string()), pa.field("score", pa.int64())]
                ),
            ),
        }
    )
    return ArrowTableSource(
        table,
        tag_columns=["session", "unit"],
        tag_only_columns=True,
        source_id="recordings",
    )


# One condition of every kind, together.
_EVERY_KIND = Condition.of(
    (col("t") >= 1.0) | (col("t") == 0.0) | col("rate").is_null(),
    ~col("info").field("kind").is_in(["noise", "mua"]),
    col("unit").between(3, 7),
    col("at") <= _AT,
    col("day") == _DAY,
    col("rate") != col("t"),
    session="s1",
)


def _kept(stream: Any) -> list[tuple[str, int]]:
    return sorted(
        (row["session"], row["unit"]) for row in stream.as_table().to_pylist()
    )


class TestSavedJob:
    def test_a_saved_job_reloads_the_same_filter(self, tmp_path: Path):
        """Every condition kind survives ``save`` and ``load``: same hashes, same rows."""
        store = tmp_path / "store"
        database = DeltaTableDatabase(store)
        job = PipelineJob(name="filtered", database=database)
        with job:
            _recordings().filter(_EVERY_KIND, label="kept")
        # The job's own database object: job.run() flushes it, so the rows reach disk.
        job.nodes["kept"].attach_databases(
            pipeline_database=database.at("filtered"), cache_mode=CacheMode.LOG
        )
        job.run()
        expected = _kept(job.nodes["kept"])
        assert expected == [("s1", 3)]

        path = tmp_path / "job.json"
        job.save(path)
        assert '":condition"' in path.read_text()
        loaded = PipelineJob.load(path, database=DeltaTableDatabase(store))

        kept, reloaded = job.nodes["kept"], loaded.nodes["kept"]
        assert reloaded.pipeline_hash() == kept.pipeline_hash()
        assert reloaded.content_hash() == kept.content_hash()
        operator = reloaded.producer
        assert isinstance(operator, Filter)
        assert operator.condition == _EVERY_KIND
        assert operator.identity_structure() == kept.producer.identity_structure()
        # Everything stored for the node reads back (its run's own view is empty
        # before a run in this process, by design).
        assert _kept(reloaded.as_source()) == expected


class TestRestrictedAggregationWarning:
    """A filter is in every downstream identity, so an aggregate after it never warns
    (design section 5, F12)."""

    @pytest.mark.parametrize("orchestrator", [None, AsyncPipelineOrchestrator])
    def test_a_filter_feeding_group_by_does_not_warn(self, tmp_path, orchestrator):
        @function_pod(output_keys="n")
        def count(t: list[float | None]) -> int:
            return len(t)

        job = PipelineJob(name="grouped", database=DeltaTableDatabase(tmp_path))
        with job:
            grouped = (
                _recordings()
                .filter(col("rate").is_not_null(), col("unit").is_in([3, 7]))
                .select_data_columns(["t"])
                .group_by(["session"], label="by_session")
            )
            count.pod(grouped, label="count")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            job.run(orchestrator=orchestrator() if orchestrator else None)
        assert not [
            w for w in caught if issubclass(w.category, RestrictedAggregationWarning)
        ]


class TestIdentityDoesNotFollowTheInput:
    def test_one_literal_on_columns_of_two_widths(self):
        """``col("unit") == 7`` is one operator on a ``uint32`` and an ``int64`` column
        (the literal enters the identity, not the column's type); the rows agree."""

        def source(arrow_type: pa.DataType) -> ArrowTableSource:
            table = pa.table(
                {
                    "unit": pa.array([3, 7, 9], arrow_type),
                    "x": pa.array([1.0, 2.0, 3.0]),
                }
            )
            return ArrowTableSource(
                table, tag_columns=["unit"], tag_only_columns=True, source_id="u"
            )

        narrow, wide = source(pa.uint32()), source(pa.int64())
        operator = Filter(Condition.of(unit=7))
        assert (
            operator.identity_structure()
            == Filter(col("unit") == 7).identity_structure()
        )
        assert operator(narrow).as_table()["x"].to_pylist() == [2.0]
        assert operator(wide).as_table()["x"].to_pylist() == [2.0]
        # The streams differ by their input's schema only.
        assert operator(narrow).pipeline_hash() != operator(wide).pipeline_hash()


# --- where and filter on one table -------------------------------------------------

_PROJECT = "workshop-sample"
_KEYS = pa.schema(
    [pa.field("subject_id", pa.string()), pa.field("timestamp", pa.uint64())]
)


@pytest.fixture()
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A fake SpiralDB project holding a 6-row ``event`` table."""
    monkeypatch.setattr(fake_spiral_sdk, "scan_batch_rows", 2)
    with fake_spiral_sdk.install(tmp_path / "spiral"):
        project = fake_spiral_sdk.ExternalProject(_PROJECT)
        project.create_table("default.event", key_schema=_KEYS).write(
            pa.table(
                {
                    "subject_id": ["m1", "m1", "m1", "m2", "m2", "m3"],
                    "timestamp": pa.array([1, 2, 3, 4, 5, 6], pa.uint64()),
                    "channel": pa.array([0, 1, 0, 1, 0, 1], pa.int16()),
                    "note": ["a", "b", "a", "b", "a", "b"],
                    "reading": pa.array(
                        [float("nan"), 1.0, float("nan"), -0.0, 0.0, 2.0], pa.float64()
                    ),
                }
            )
        )
        yield project


def _events() -> SpiralDBTableSource:
    return SpiralDBTableSource(
        _PROJECT, "event", source_id="events", tag_only_columns=True
    )


def _timestamps(stream: Any) -> list[int]:
    return sorted(stream.as_table()["timestamp"].to_pylist())


class TestWhereAgainstFilter:
    @pytest.mark.parametrize(
        "equalities",
        [
            {"subject_id": "m1"},
            {"subject_id": "m1", "channel": 0},
            {"channel": 1, "note": "b", "subject_id": "m2"},
            {"note": "b", "subject_id": "m2", "channel": 1},
        ],
        ids=["one", "two", "three", "three_reordered"],
    )
    def test_the_same_equalities_keep_the_same_rows(self, project, equalities):
        """``where(**e)`` and ``filter(**e)`` keep the same rows; only ``filter`` is a
        step: ``where`` leaves the source's hashes, ``filter`` makes new ones."""
        events = _events()
        restricted = events.where(**equalities)
        filtered = events.filter(**equalities)

        assert _timestamps(restricted) == _timestamps(filtered)
        assert restricted.content_hash() == events.content_hash()
        assert filtered.content_hash() != events.content_hash()
        assert filtered.pipeline_hash() != events.pipeline_hash()

    def test_where_then_the_same_filter(self, project):
        """The large-table advice: ``where`` for speed, the same ``filter`` for identity."""
        events = _events()
        both = events.where(subject_id="m2").filter(subject_id="m2")
        assert _timestamps(both) == _timestamps(events.filter(subject_id="m2"))
        assert both.pipeline_hash() == events.filter(subject_id="m2").pipeline_hash()


# --- Review findings, pinned (developer, 2026-11-22: every finding gets a test) ---------


def _limited(limit: int | None = None) -> SpiralDBTableSource:
    return SpiralDBTableSource(
        _PROJECT, "event", source_id="events", limit=limit, tag_only_columns=True
    )


@pytest.mark.parametrize(
    "column, value, alone, together",
    [
        ("note", "b", [2], [2, 4]),
        ("channel", 1, [2], [2, 4]),
        ("subject_id", "m2", [], [4, 5]),
    ],
    ids=["string data", "int16 data", "key column"],
)
def test_a_limit_counts_the_rows_where_matches(project, column, value, alone, together):
    """CodeRabbit on PR 498 (sources.md:449, Major) and on PR 493 (spec 465-466): over a
    source with ``limit=2``, ``where`` plus the same ``filter`` keeps the first two
    matching rows, the ``filter`` alone only the matches among the first two rows read."""
    assert _timestamps(_limited(2).filter(**{column: value})) == alone
    assert _timestamps(
        _limited(2).where(**{column: value}).filter(**{column: value})
    ) == (together)
    assert _timestamps(_limited().filter(**{column: value})) == _timestamps(
        _limited().where(**{column: value}).filter(**{column: value})
    )


@pytest.mark.parametrize(
    "restriction, condition, expected",
    [
        ({"subject_id": "m1"}, Condition.of(channel=0), [1, 3]),
        ({"note": "b"}, Condition.of(col("timestamp") >= 4), [4, 6]),
        ({"subject_id": "m2", "channel": 0}, Condition.of(note="a"), [5]),
    ],
    ids=["key restriction", "data restriction", "two restrictions"],
)
def test_a_filter_reads_what_the_source_selects(
    project, restriction, condition, expected
):
    """CodeRabbit on PR 498 (sources.md:446): a filter over a restricted source sees only
    the rows the source's ``where`` selects, and its condition is not pushed into it."""
    source = _events().where(**restriction)
    filtered = source.filter(condition)
    assert _timestamps(filtered) == expected
    assert source.condition == Condition.of(restriction)  # nothing was pushed


@pytest.mark.parametrize(
    "value, alone, with_where",
    [
        (float("nan"), [1, 3], [1, 3]),
        (-0.0, [4, 5], [4]),
        (2.0, [6], [6]),
    ],
    ids=["NaN", "signed zero", "ordinary float"],
)
def test_where_and_filter_on_a_float_column(project, value, alone, with_where):
    """CodeRabbit on PR 493 (spec 483, Major) and PR 498 (sources.md:447, operators.md:861):
    ``filter`` matches floats by Orcapod's rule (NaN equals NaN); ``where`` by the table
    system's equality (SpiralDB's, as measured on the service: NaN equals NaN and 0.0 does
    not equal -0.0), so ``where`` plus ``filter`` can keep fewer rows than the ``filter``
    alone, and only for a zero."""
    assert _timestamps(_events().filter(reading=value)) == alone
    assert (
        _timestamps(_events().where(reading=value).filter(reading=value)) == with_where
    )


@pytest.mark.parametrize(
    "condition",
    [
        (col("subject_id") == "m1") & (col("subject_id") == "m2"),
        Condition.of(col("channel") == 1, channel=0),
        Condition.of(col("note").is_in(["a"]), col("note").is_in(["b"])),
    ],
    ids=["two key values", "two data values", "disjoint memberships"],
)
@pytest.mark.parametrize("restricted", [False, True], ids=["plain", "where-restricted"])
def test_a_contradiction_over_a_spiral_source_never_raises(
    project, condition, restricted
):
    """CodeRabbit on PR 493 (spec 491, pushdown): a valid filter whose condition holds two
    values for one column keeps zero rows over a SpiralDB source, restricted or not,
    and never raises (version 1 pushes nothing)."""
    source = _events().where(subject_id="m1") if restricted else _events()
    assert _timestamps(source.filter(condition)) == []


@pytest.mark.parametrize("orchestrator", [None, AsyncPipelineOrchestrator])
def test_a_filter_after_a_where_still_warns_before_an_aggregate(
    project, tmp_path, orchestrator
):
    """CodeRabbit on PR 493 (spec 440): a ``Filter`` does not clear the
    ``RestrictedAggregationWarning`` of a ``where``-restricted source upstream of it."""

    @function_pod(output_keys="n")
    def count(channel: list[int | None]) -> int:
        return len(channel)

    job = PipelineJob(
        name="restricted", database=DeltaTableDatabase(tmp_path / "store")
    )
    with job:
        grouped = (
            SpiralDBTableSource(
                _PROJECT, "event", source_id="events", tag_only_columns=True
            )
            .where(note="b")
            .filter(col("channel") >= 0)
            .select_data_columns(["channel"])
            .group_by(["subject_id"], label="by_subject")
        )
        count.pod(grouped, label="count")
    with pytest.warns(RestrictedAggregationWarning):
        job.run(orchestrator=orchestrator() if orchestrator else None)


def _narrow(arrow_type: pa.DataType, values: list[object]) -> ArrowTableSource:
    return ArrowTableSource(
        pa.table(
            {"k": pa.array([1, 2], pa.int64()), "x": pa.array(values, arrow_type)}
        ),
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="narrow",
    )


_UNFIT = [
    (pa.int8(), [1, 2], 300),
    (pa.uint32(), [1, 2], -1),
    (
        pa.timestamp("us", tz="UTC"),
        [dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)] * 2,
        dt.datetime(2026, 1, 1),
    ),
    (
        pa.timestamp("s"),
        [dt.datetime(2026, 1, 1)] * 2,
        dt.datetime(2026, 1, 1, 0, 0, 0, 1),
    ),
]
_UNFIT_IDS = ["int8 range", "uint32 sign", "naive on zoned", "below the unit"]


@pytest.mark.parametrize("arrow_type, values, literal", _UNFIT, ids=_UNFIT_IDS)
class TestArrowChecksRunWhenApplied:
    """CodeRabbit on PR 493 (spec 259): the Arrow-level checks (range, sign, zone, unit)
    run when the filter meets its input, the same in a job and outside one."""

    def test_outside_a_job(self, arrow_type, values, literal):
        from orcapod.errors import InputValidationError

        filtered = _narrow(arrow_type, values).filter(x=literal)  # builds
        with pytest.raises(InputValidationError, match="'x'"):
            filtered.as_table()

    def test_in_a_job(self, tmp_path, arrow_type, values, literal):
        from orcapod.errors import InputValidationError

        job = PipelineJob(name="unfit", database=DeltaTableDatabase(tmp_path / "store"))
        with job:
            _narrow(arrow_type, values).filter(x=literal, label="unfit")  # builds
        with pytest.raises(InputValidationError, match="'x'"):
            job.run()


@pytest.mark.parametrize("value", ["x", "y"])
def test_a_column_named_label(value):
    """CodeRabbit on PR 498 (sources.md:425): ``label=`` is the step's label, so a column
    named ``label`` is filtered through a mapping or ``col``."""
    source = ArrowTableSource(
        pa.table({"k": [1, 2], "label": ["x", "y"], "v": [1.0, 2.0]}),
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="labelled",
    )
    by_mapping = source.filter({"label": value})
    by_col = source.filter(col("label") == value)
    assert by_mapping.as_table()["label"].to_pylist() == [value]
    assert by_mapping.pipeline_hash() == by_col.pipeline_hash()
    with pytest.raises(ValueError, match="at least one"):
        source.filter(label=value)
