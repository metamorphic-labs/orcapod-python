"""SpiralDBTableSource reads no rows until asked and then reads them in batches (ITL-186).

Runs against the disk-backed pyspiral fake, whose ``scan_log`` records every scan a source
opens and how many batches were taken from it.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import SourceTooLargeError
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.serialization import resolve_source_from_config
from orcapod.sources import SpiralDBTableSource
from tests.test_databases import fake_pyspiral

_PROJECT = "edi-sample"
_KEYS = pa.schema(
    [pa.field("subject_id", pa.string()), pa.field("timestamp", pa.uint64())]
)
_ROWS = 5


def _rows(timestamps: list[int], subject: str = "m1") -> pa.Table:
    """Return all-nullable ``event`` rows for ``timestamps``."""
    table = pa.table(
        {
            "subject_id": [subject] * len(timestamps),
            "timestamp": pa.array(timestamps, pa.uint64()),
            "amplitude": pa.array([t + 0.5 for t in timestamps], pa.float32()),
        }
    )
    return table.cast(pa.schema([pa.field(f.name, f.type, True) for f in table.schema]))


@pytest.fixture()
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Provide a fake project holding a 5-row ``event`` table read 2 rows per batch."""
    monkeypatch.setattr(fake_pyspiral, "scan_batch_rows", 2)
    with fake_pyspiral.install(tmp_path / "spiral"):
        project = fake_pyspiral.FakeSpiral().project(_PROJECT)
        project.create_table("default.event", key_schema=_KEYS).write(
            _rows([1, 2, 3, 4, 5])
        )
        fake_pyspiral.scan_log.clear()
        yield project
        fake_pyspiral.scan_log.clear()


def _timestamps(source: SpiralDBTableSource) -> list[int]:
    return source.as_table().column("timestamp").to_pylist()


def _record_ids(source: SpiralDBTableSource) -> dict[int, bytes]:
    """Return each row's record ID by its ``timestamp`` key."""
    table = source.as_table(all_info=True)
    (column,) = [n for n in table.column_names if n.startswith("_tag_record_id::")]
    return dict(
        zip(table.column("timestamp").to_pylist(), table.column(column).to_pylist())
    )


class TestNoRowIsReadUntilAsked:
    def test_construction_and_identity_read_no_rows(self, project):
        source = SpiralDBTableSource(_PROJECT, "event")
        source.output_schema(all_info=True)
        source.keys(all_info=True)
        source.content_hash()
        source.pipeline_hash()
        source.to_config()
        assert source.row_count_estimate == _ROWS

        assert fake_pyspiral.scan_log == []

    def test_iter_data_reads_batches_only_as_rows_are_consumed(self, project):
        source = SpiralDBTableSource(_PROJECT, "event")
        rows = iter(source.iter_data())

        next(rows)
        assert [scan.batches_read for scan in fake_pyspiral.scan_log] == [1]

        assert sum(1 for _ in rows) == _ROWS - 1
        assert [scan.batches_read for scan in fake_pyspiral.scan_log] == [3]

    def test_batches_handed_on_hold_at_most_batch_rows(self, project, monkeypatch):
        monkeypatch.setattr(fake_pyspiral, "scan_batch_rows", 4)
        source = SpiralDBTableSource(_PROJECT, "event", batch_rows=3)

        # About the non-public batching: no public method shows the batch sizes.
        sizes = [s.as_table().num_rows for s in source._batch_streams()]  # pyright: ignore[reportPrivateUsage]

        assert sizes == [3, 1, 1]

    def test_a_read_that_matches_nothing_has_the_schema_of_one_that_does(self, project):
        nothing = SpiralDBTableSource(_PROJECT, "event", where={"subject_id": "nobody"})
        rows = SpiralDBTableSource(_PROJECT, "event")

        assert list(nothing.iter_data()) == []
        empty = nothing.as_table(all_info=True)
        assert empty.num_rows == 0
        assert empty.schema == rows.as_table(all_info=True).schema

    def test_a_table_with_key_columns_only_is_refused(self, project):
        project.create_table("default.never_written", key_schema=_KEYS)

        with pytest.raises(ValueError, match="never_written.*data column"):
            SpiralDBTableSource(_PROJECT, "never_written")

    def test_schema_is_the_tables(self, project):
        source = SpiralDBTableSource(_PROJECT, "event")

        assert source.keys() == (("subject_id", "timestamp"), ("amplitude",))
        tag_schema, data_schema = source.output_schema()
        assert dict(tag_schema) == {"subject_id": str | None, "timestamp": int | None}
        assert dict(data_schema) == {"amplitude": float | None}
        assert source.as_table().schema.names == [
            "subject_id",
            "timestamp",
            "amplitude",
        ]


class TestRows:
    def test_streamed_rows_equal_as_table_rows(self, project):
        source = SpiralDBTableSource(_PROJECT, "event")

        streamed = [
            {**tag.as_dict(all_info=True), **data.as_dict(all_info=True)}
            for tag, data in source.iter_data()
        ]
        table_rows = source.as_table(all_info=True).to_pylist()

        assert len(streamed) == _ROWS
        assert len(table_rows) == _ROWS
        for streamed_row, table_row in zip(streamed, table_rows, strict=True):
            shared = streamed_row.keys() & table_row.keys()
            assert {"subject_id", "timestamp", "amplitude"} <= shared
            assert any(name.startswith("_tag_record_id::") for name in shared)
            assert any(name.startswith("_source_") for name in shared)
            assert {k: streamed_row[k] for k in shared} == {
                k: table_row[k] for k in shared
            }

    def test_async_iteration_yields_the_same_rows(self, project):
        source = SpiralDBTableSource(_PROJECT, "event")

        async def collect() -> list[dict]:
            return [
                {**tag.as_dict(all_info=True), **data.as_dict(all_info=True)}
                async for tag, data in source.async_iter_data()
            ]

        expected = [
            {**tag.as_dict(all_info=True), **data.as_dict(all_info=True)}
            for tag, data in source.iter_data()
        ]
        assert asyncio.run(collect()) == expected

    def test_dataframe_methods_return_the_rows(self, project):
        source = SpiralDBTableSource(_PROJECT, "event", limit=2)

        assert source.as_df().get_column("timestamp").to_list() == [1, 2]
        assert source.as_lazy_frame().collect().height == 2
        assert list(source.as_pandas_df()["timestamp"]) == [1, 2]


class TestRestrictions:
    def test_limit_reads_the_first_rows_in_key_order(self, project):
        source = SpiralDBTableSource(_PROJECT, "event", limit=2)

        assert _timestamps(source) == [1, 2]
        assert fake_pyspiral.scan_log[-1].kwargs["limit"] == 2

    def test_where_reads_only_matching_rows(self, project):
        project.table("default.event").write(_rows([7, 8], subject="m2"))
        source = SpiralDBTableSource(_PROJECT, "event", where={"subject_id": "m2"})

        assert _timestamps(source) == [7, 8]

    def test_where_and_limit_together(self, project):
        project.table("default.event").write(_rows([7, 8], subject="m2"))
        source = SpiralDBTableSource(
            _PROJECT, "event", where={"subject_id": "m2"}, limit=1
        )

        assert _timestamps(source) == [7]

    def test_where_on_an_unknown_column_is_refused(self, project):
        with pytest.raises(ValueError, match="nope"):
            SpiralDBTableSource(_PROJECT, "event", where={"nope": 1})

    @pytest.mark.parametrize("name", ["limit", "max_rows", "batch_rows"])
    def test_sizes_must_be_positive(self, project, name):
        with pytest.raises(ValueError, match=name):
            SpiralDBTableSource(_PROJECT, "event", **{name: 0})

    def test_every_read_of_one_source_uses_the_version_it_was_built_at(self, project):
        table = project.table("default.event")
        version = table.last_modified_at()
        source = SpiralDBTableSource(_PROJECT, "event")
        table.write(_rows([9]))

        source.as_table()
        list(source.iter_data())

        assert [scan.kwargs["asof"] for scan in fake_pyspiral.scan_log] == [version] * 2

    def test_a_given_version_is_used(self, project):
        source = SpiralDBTableSource(_PROJECT, "event", asof=1)

        source.as_table()

        assert fake_pyspiral.scan_log[-1].kwargs["asof"] == 1
        assert source.to_config()["asof"] == 1


class TestRowCap:
    def test_a_read_over_the_cap_is_refused_before_scanning(self, project):
        source = SpiralDBTableSource(_PROJECT, "event", max_rows=3)

        with pytest.raises(SourceTooLargeError) as raised:
            source.as_table()

        assert fake_pyspiral.scan_log == []
        message = str(raised.value)
        assert "5 rows" in message and "max_rows=3" in message
        assert "limit=" in message and "where=" in message
        with pytest.raises(SourceTooLargeError):
            list(source.iter_data())

    def test_a_limit_within_the_cap_is_read(self, project):
        source = SpiralDBTableSource(_PROJECT, "event", max_rows=3, limit=3)

        assert _timestamps(source) == [1, 2, 3]

    def test_a_limit_over_the_cap_is_refused(self, project):
        source = SpiralDBTableSource(_PROJECT, "event", max_rows=3, limit=4)

        with pytest.raises(SourceTooLargeError, match="4 rows"):
            source.as_table()

    def test_a_filtered_read_over_the_cap_stops_while_reading(self, project):
        source = SpiralDBTableSource(
            _PROJECT, "event", max_rows=3, where={"subject_id": "m1"}
        )

        with pytest.raises(SourceTooLargeError, match="max_rows=3"):
            list(source.iter_data())

    def test_a_filtered_read_within_the_cap_is_read(self, project):
        project.table("default.event").write(_rows([7, 8], subject="m2"))
        source = SpiralDBTableSource(
            _PROJECT, "event", max_rows=3, where={"subject_id": "m2"}
        )

        assert _timestamps(source) == [7, 8]

    def test_no_cap(self, project):
        source = SpiralDBTableSource(_PROJECT, "event", max_rows=None)

        assert len(_timestamps(source)) == _ROWS


class TestIdentity:
    def test_default_source_id_names_the_table_and_carries_its_spiral_id(self, project):
        source = SpiralDBTableSource(_PROJECT, "event")
        table_id = project.table("default.event").table_id

        assert source.table_id == table_id
        assert source.source_id == f"{_PROJECT}/default.event@{table_id}"

    def test_a_given_source_id_is_used_as_it_is(self, project):
        assert SpiralDBTableSource(_PROJECT, "event", source_id="mine").source_id == (
            "mine"
        )

    def test_a_recreated_table_is_a_different_source(self, project):
        before = SpiralDBTableSource(_PROJECT, "event")
        project.drop_table("default.event")
        project.create_table("default.event", key_schema=_KEYS).write(
            _rows([1, 2, 3, 4, 5])
        )
        after = SpiralDBTableSource(_PROJECT, "event")

        assert after.source_id != before.source_id
        assert after.content_hash() != before.content_hash()
        assert after.pipeline_hash() == before.pipeline_hash()
        assert _record_ids(after)[1] != _record_ids(before)[1]

    def test_restrictions_do_not_change_identity(self, project):
        whole = SpiralDBTableSource(_PROJECT, "event")
        part = SpiralDBTableSource(
            _PROJECT, "event", where={"subject_id": "m1"}, limit=2, asof=1, max_rows=9
        )

        assert part.source_id == whole.source_id
        assert part.content_hash() == whole.content_hash()
        assert part.pipeline_hash() == whole.pipeline_hash()


class TestRecordIdentity:
    def test_the_row_token_is_the_labelled_hash_of_the_primary_key(self, project):
        source = SpiralDBTableSource(_PROJECT, "event", limit=1)
        key = pa.table(
            {
                "subject_id": pa.array(["m1"], pa.large_string()),
                "timestamp": pa.array([1], pa.uint64()),
            },
            schema=pa.schema(
                [
                    pa.field("subject_id", pa.large_string(), nullable=False),
                    pa.field("timestamp", pa.uint64(), nullable=False),
                ]
            ),
        )
        token = source.data_context.arrow_hasher.hash_table(key).to_string()

        (info,) = source.as_table(columns={"source": True}).column("_source_amplitude")

        assert info.as_py() == f"{source.source_id}::{token}::amplitude"
        assert "::" not in token

    def test_a_row_has_the_same_record_id_in_every_read(self, project):
        whole = _record_ids(SpiralDBTableSource(_PROJECT, "event"))
        limited = _record_ids(SpiralDBTableSource(_PROJECT, "event", limit=2))
        filtered = _record_ids(
            SpiralDBTableSource(_PROJECT, "event", where={"timestamp": 4})
        )

        assert len(set(whole.values())) == _ROWS
        assert limited == {t: whole[t] for t in (1, 2)}
        assert filtered == {4: whole[4]}

    def test_a_row_keeps_its_record_id_when_an_earlier_key_is_added(self, project):
        before = _record_ids(SpiralDBTableSource(_PROJECT, "event"))
        project.table("default.event").write(_rows([0]))

        after = _record_ids(SpiralDBTableSource(_PROJECT, "event", max_rows=None))

        assert list(after) == [0, 1, 2, 3, 4, 5]
        assert {t: after[t] for t in before} == before

    def test_record_ids_do_not_depend_on_tag_columns(self, project):
        by_key = _record_ids(SpiralDBTableSource(_PROJECT, "event"))
        by_subject = _record_ids(
            SpiralDBTableSource(_PROJECT, "event", tag_columns="subject_id")
        )

        assert by_subject == by_key


class TestSaveAndLoad:
    def test_config_round_trip_reads_no_rows(self, project):
        source = SpiralDBTableSource(
            _PROJECT,
            "event",
            where={"subject_id": "m1", "timestamp": 2},
            limit=3,
            max_rows=50,
            batch_rows=7,
        )
        config = source.to_config()

        loaded = resolve_source_from_config(config)

        assert fake_pyspiral.scan_log == []
        assert isinstance(loaded, SpiralDBTableSource)
        assert loaded.to_config() == config
        assert loaded.content_hash() == source.content_hash()
        assert config["table_id"] == source.table_id
        assert _timestamps(loaded) == [2]

    def test_a_date_and_time_in_where_survive_the_round_trip(self, project):
        keys = pa.schema([pa.field("start", pa.timestamp("us"))])
        start = dt.datetime(2026, 3, 24, 17, 54, 17)
        project.create_table("default.session", key_schema=keys).write(
            pa.table({"start": pa.array([start], pa.timestamp("us")), "n": [1]})
        )
        source = SpiralDBTableSource(_PROJECT, "session", where={"start": start})

        loaded = SpiralDBTableSource.from_config(source.to_config())

        assert loaded.as_table().num_rows == 1

    def test_loading_refuses_a_name_that_now_leads_to_another_table(self, project):
        config = SpiralDBTableSource(_PROJECT, "event").to_config()
        project.drop_table("default.event")
        project.create_table("default.event", key_schema=_KEYS).write(_rows([1]))
        new_id = project.table("default.event").table_id

        with pytest.raises(ValueError) as raised:
            SpiralDBTableSource.from_config(config)

        assert config["table_id"] in str(raised.value)
        assert new_id in str(raised.value)


class TestInAJob:
    def _job(self, store: Path, calls: list[float], **source_kwargs) -> PipelineJob:
        @function_pod(output_keys="doubled")
        def double(amplitude: float | None) -> float | None:
            calls.append(amplitude)
            return None if amplitude is None else 2 * amplitude

        job = PipelineJob(name="events", store=DeltaTableDatabase(store))
        with job:
            double.pod(
                SpiralDBTableSource(_PROJECT, "event", **source_kwargs), label="double"
            )
        return job

    @pytest.mark.parametrize("orchestrator", [None, AsyncPipelineOrchestrator])
    def test_run_rerun_and_an_appended_row(self, project, tmp_path, orchestrator):
        def run(calls: list[float]) -> PipelineJob:
            job = self._job(tmp_path / "store", calls)
            job.run(orchestrator=orchestrator() if orchestrator else None)
            return job

        calls: list[float] = []
        first = run(calls)
        assert sorted(calls) == [1.5, 2.5, 3.5, 4.5, 5.5]
        assert first.nodes["double"].as_table().num_rows == _ROWS

        calls.clear()
        run(calls)
        assert calls == []

        # A new row, with a key before every other, is the only one computed.
        project.table("default.event").write(_rows([0]))
        third = run(calls)
        assert calls == [0.5]
        assert third.nodes["double"].get_all_records().num_rows == _ROWS + 1

    def test_a_job_over_the_cap_fails_before_computing(self, project, tmp_path):
        calls: list[float] = []
        job = self._job(tmp_path / "store", calls, max_rows=3)

        with pytest.raises(SourceTooLargeError):
            job.run()

        assert calls == []
