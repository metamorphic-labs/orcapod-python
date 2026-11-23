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
from tests.test_databases import fake_pyspiral

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
        job = PipelineJob(name="filtered", database=DeltaTableDatabase(store))
        with job:
            _recordings().filter(_EVERY_KIND, label="kept")
        job.nodes["kept"].attach_databases(
            pipeline_database=DeltaTableDatabase(store).at("filtered"),
            cache_mode=CacheMode.LOG,
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
    monkeypatch.setattr(fake_pyspiral, "scan_batch_rows", 2)
    with fake_pyspiral.install(tmp_path / "spiral"):
        project = fake_pyspiral.FakeSpiral().project(_PROJECT)
        project.create_table("default.event", key_schema=_KEYS).write(
            pa.table(
                {
                    "subject_id": ["m1", "m1", "m1", "m2", "m2", "m3"],
                    "timestamp": pa.array([1, 2, 3, 4, 5, 6], pa.uint64()),
                    "channel": pa.array([0, 1, 0, 1, 0, 1], pa.int16()),
                    "note": ["a", "b", "a", "b", "a", "b"],
                }
            )
        )
        yield project


def _events() -> SpiralDBTableSource:
    return SpiralDBTableSource(_PROJECT, "event", source_id="events")


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
