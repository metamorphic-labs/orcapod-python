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
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import RestrictedAggregationWarning
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.node_run import restrictions_in_force
from orcapod.core.sources.spiraldb_table_source import _checked_conditions
from orcapod.sources import SpiralDBTableSource
from orcapod.types import Condition
from tests.test_databases import fake_pyspiral

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
    monkeypatch.setattr(fake_pyspiral, "scan_batch_rows", 2)
    with fake_pyspiral.install(tmp_path / "spiral"):
        project = fake_pyspiral.FakeSpiral().project(_PROJECT)
        project.create_table("default.event", key_schema=_KEYS).write(
            _rows(["m1", "m1", "m1", "m2", "m2"], [1, 2, 3, 4, 5])
        )
        fake_pyspiral.scan_log.clear()
        yield project
        fake_pyspiral.scan_log.clear()


def _source(**kwargs: Any) -> SpiralDBTableSource:
    """Return a source over ``event`` with ``kwargs``."""
    return SpiralDBTableSource(_PROJECT, "event", **kwargs)


def _timestamps(source: SpiralDBTableSource) -> list[int]:
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

        assert repr(fake_pyspiral.scan_log[-1].kwargs["where"]) == "subject_id == 'm2'"

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

        assert repr(fake_pyspiral.scan_log[-1].kwargs["where"]) == expression

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
            ({"flag": True, "subject_id": "m1", "amplitude": 3}, "amplitude"),
        ],
    )
    def test_a_value_of_another_type_among_several_is_refused(
        self, project, conditions, column
    ):
        """One wrong value refuses the whole call, naming its column; nothing is read."""
        source = _source()

        with pytest.raises(TypeError, match=rf"{column}.*convert"):
            source.where(**conditions)
        assert fake_pyspiral.scan_log == []
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
            ("amplitude", 3, "int"),
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

    def test_a_float_beyond_the_column_range_is_refused(self):
        """A finite float a float32 column cannot reach is refused; rounding is not."""
        schema = pa.schema([pa.field("x", pa.float32())])

        # About the condition check itself: the fake has no float32 data to match.
        with pytest.raises(TypeError, match="range"):
            _checked_conditions({"x": 1e300}, schema)
        assert _checked_conditions({"x": 0.1}, schema) == {"x": 0.1}

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
        assert _checked_conditions({"at": aware}, schema) == {"at": aware}
        with pytest.raises(TypeError, match="aware"):
            _checked_conditions({"at": _START}, schema)

    def test_a_time_finer_than_the_column_is_refused(self):
        """A value the column would truncate is refused, not cut."""
        schema = pa.schema([pa.field("at", pa.timestamp("s"))])

        with pytest.raises(TypeError, match="exactly"):
            _checked_conditions({"at": _START.replace(microsecond=5)}, schema)

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

        assert fake_pyspiral.scan_log == []


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

        assert restricted.source_id == "events"

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

        assert fake_pyspiral.scan_log[-1].kwargs["asof"] == version
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

        new, kept = (scan.kwargs["asof"] for scan in fake_pyspiral.scan_log[-2:])
        assert (new, kept) == (old + 1, old)

    def test_no_mutable_state_is_shared(self, project):
        """The two objects share no mutable container."""
        source = _source(tags_as_data={"timestamp": "time"}, overrides={"a": "b"})
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
        assert len(fake_pyspiral.scan_log) == 1
        assert fake_pyspiral.scan_log[0].kwargs["limit"] == 2
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

        assert summary.restrictions == {"events": Condition.of(subject_id="m1")}
        assert summary.limits == {"events": 3}
        assert summary.restricted_source_ids == {"events": source.source_id}
        assert "events (events): where subject_id='m1', limit 3" in repr(summary)
        assert "subject_id=&#x27;m1&#x27;" in summary._repr_html_()

    @pytest.mark.parametrize("form", list(_FORMS_OF_ONE_RESTRICTION))
    def test_the_run_summary_records_both_conditions(self, project, tmp_path, form):
        """``RunSummary.restrictions`` is the same for every form of the restriction."""
        source = _source(source_id="events", tag_only_columns=True)
        restricted = _FORMS_OF_ONE_RESTRICTION[form](source)
        job = self._job(tmp_path / "store", restricted, [])

        summary = job.run()

        assert summary.restrictions == {
            "events": Condition.of(subject_id="m1", flag=True)
        }
        assert summary.restricted_source_ids == {"events": "events"}
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

        assert summary.restrictions == {"events": Condition.of(subject_id="m1")}
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

        assert summary.restrictions == {"events": Condition.of(subject_id="m1")}
        assert summary.restricted_source_ids == {"events": "events"}

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
        with pytest.raises(ValueError, match="different rows in one pipeline"):
            with job:
                first.pod(events.where(subject_id="m1"), label="first")
                second.pod(events.where(subject_id="m2"), label="second")

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
        with pytest.raises(ValueError, match="different rows in one pipeline"):
            with job:
                first.pod(m1, label="first")
                second.pod(m2, label="second")

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
        with pytest.raises(ValueError, match="different rows in one pipeline"):
            with job:
                if prebuilt_first:
                    first.pod(outside, label="first")
                events.where(**captured).select_data_columns(["amplitude"])
                if not prebuilt_first:
                    first.pod(outside, label="first")

    @pytest.mark.parametrize(
        "prebuilt_first", [False, True], ids=["captured first", "prebuilt first"]
    )
    def test_a_stream_with_the_hash_of_an_equal_captured_output_is_accepted(
        self, project, tmp_path, prebuilt_first
    ):
        """The same restriction, its conditions in another order, is one stream."""

        @function_pod(output_keys="a")
        def first(amplitude: float | None) -> float | None:
            """Return ``amplitude``."""
            return amplitude

        events = _source(source_id="events", tag_only_columns=True)
        outside = events.where(flag=True, subject_id="m1").select_data_columns(
            ["amplitude"]
        )
        job = PipelineJob(name="two", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            if prebuilt_first:
                first.pod(outside, label="first")
            events.where(subject_id="m1", flag=True).select_data_columns(["amplitude"])
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
        with pytest.raises(ValueError, match="different rows in one pipeline"):
            with job:
                events.where(subject_id="m1").semi_join(
                    events.where(subject_id="m2"), label="both"
                )

    @pytest.mark.parametrize("restrict", [False, True])
    def test_one_restriction_twice_in_one_job_is_accepted(
        self, project, tmp_path, restrict
    ):
        """Two source objects with the same restriction (or none) are one source."""

        @function_pod(output_keys="a")
        def first(amplitude: float | None) -> float | None:
            """Return ``amplitude``."""
            return amplitude

        @function_pod(output_keys="b")
        def second(channel: int | None) -> int | None:
            """Return ``channel``."""
            return channel

        def events(*conditions: tuple[str, object]) -> SpiralDBTableSource:
            """Return the events source, with ``conditions`` in the order given."""
            source = _source(source_id="events", tag_only_columns=True)
            return source.where(dict(conditions)) if restrict else source

        job = PipelineJob(name="two", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            # The same conditions given in another order are the same restriction.
            first.pod(events(("subject_id", "m1"), ("flag", True)), label="first")
            second.pod(events(("flag", True), ("subject_id", "m1")), label="second")

        job.run()
        expected = [3] if restrict else [1, 2, 3, 4, 5]
        for label in ("first", "second"):
            rows = job.nodes[label].as_table().column("timestamp").to_pylist()
            assert sorted(rows) == expected

    def test_an_unrestricted_source_is_not_reported(self, project, tmp_path):
        """A source with no condition and no limit has no entry."""
        job = self._job(tmp_path / "store", _source(source_id="events"), [])

        assert job.run().restrictions == {}

    def test_a_limit_alone_is_reported_without_a_condition(self, project, tmp_path):
        """A slot read with a limit only is in ``limits`` and the source ids, not in
        ``restrictions``."""
        job = self._job(tmp_path / "store", _source(source_id="events", limit=2), [])

        summary = job.run()

        assert summary.restrictions == {}
        assert summary.limits == {"events": 2}
        assert summary.restricted_source_ids == {"events": "events"}
        assert "events (events): limit 2" in repr(summary)

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
        with pytest.raises(ValueError, match="different rows in one pipeline"):
            with job:
                first.pod(_source(source_id="events", limit=1), label="first")
                second.pod(_source(source_id="events", limit=2), label="second")

    def test_node_run_reports_the_restriction(self, project, tmp_path):
        """A node's own ``run()`` reports the restriction in the same shape."""
        source = _source(source_id="events").where(subject_id="m2")
        job = self._job(tmp_path / "store", source, [])

        node = job.nodes["double"]
        node.run()

        assert node.last_run is not None
        assert node.last_run.restrictions == {"events": Condition.of(subject_id="m2")}
        assert node.last_run.restricted_source_ids == {"events": "events"}

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
        assert summary.restrictions == {
            "events": Condition.of(day=_DAY, subject_id="m2")
        }


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
    def test_equal_float_conditions_are_one_restriction_in_a_pipeline(
        self, project, tmp_path, values
    ):
        """``0.0`` and ``-0.0``, or NaN twice, are one restriction (as Polars matches
        them), so one table read with both in one pipeline is accepted."""

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
            first.pod(events.where(amplitude=values[0]), label="first")
            second.pod(events.where(amplitude=values[1]), label="second")

        summary = job.run()

        assert summary.restrictions == {"events": Condition.of(amplitude=values[0])}
