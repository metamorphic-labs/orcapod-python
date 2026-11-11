"""SpiralDBTableSource end to end against the disk-backed pyspiral fake (no live SpiralDB).

Covers the workshop path (ITL-926) through the public interface: EDI-shaped tables (every
column nullable, composite keys with string, timestamp and uint32 columns) -> Join of two
Spiral sources -> function pod -> PipelineJob with a Delta store -> re-run -> save and load.
See ``tests/test_databases/fake_pyspiral.py`` for the storage behaviour modelled.
"""

from __future__ import annotations

import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.operators import Join, SelectDataColumns
from orcapod.sources import SpiralDBTableSource
from tests.test_databases import fake_pyspiral

_PROJECT = "edi-sample"
_TS = pa.timestamp("us")
_SESSION_KEYS = pa.schema(
    [pa.field("subject_id", pa.string()), pa.field("session_start_datetime", _TS)]
)
_EXPERIMENT_KEYS = pa.schema(
    [*_SESSION_KEYS, pa.field("experiment_index", pa.uint32())]
)
_STARTS = [dt.datetime(2026, 1, 1, 9), dt.datetime(2026, 1, 2, 9)]


def _all_nullable(table: pa.Table) -> pa.Table:
    """Mark every column nullable, as the EDI sample tables are."""
    return table.cast(pa.schema([pa.field(f.name, f.type, True) for f in table.schema]))


@pytest.fixture()
def spiral_root(tmp_path: Path):
    root = tmp_path / "spiral"
    with fake_pyspiral.install(root):
        project = fake_pyspiral.FakeSpiral().project(_PROJECT)
        session = project.create_table("default.session", key_schema=_SESSION_KEYS)
        session.write(
            _all_nullable(
                pa.table(
                    {
                        "subject_id": ["m1", "m2"],
                        "session_start_datetime": pa.array(_STARTS, _TS),
                        "booth": ["b1", None],
                        "date_of_record": pa.array([dt.date(2026, 1, 1), None]),
                    }
                )
            )
        )
        experiment = project.create_table(
            "default.experiment", key_schema=_EXPERIMENT_KEYS
        )
        experiment.write(
            _all_nullable(
                pa.table(
                    {
                        "subject_id": ["m1", "m1", "m2"],
                        "session_start_datetime": pa.array(
                            [_STARTS[0], _STARTS[0], _STARTS[1]], _TS
                        ),
                        "experiment_index": pa.array([0, 1, 0], pa.uint32()),
                        "correct_trials": pa.array([5, 6, 7], pa.int64()),
                        "total_trials": pa.array([10, 10, None], pa.int64()),
                        "stop_timestamp": pa.array([2**63 + 1, 2, 3], pa.uint64()),
                        "eye_params": pa.array(
                            [[1.0], [], [2.0, 3.0]], pa.list_(pa.float32())
                        ),
                    }
                )
            )
        )
        yield root


class TestSchema:
    def test_nullable_spiral_columns_are_optional(self, spiral_root):
        tag, data = SpiralDBTableSource(_PROJECT, "session").output_schema()
        assert dict(tag) == {
            "subject_id": str | None,
            "session_start_datetime": dt.datetime | None,
        }
        assert dict(data) == {"booth": str | None, "date_of_record": dt.date | None}

    def test_arrow_types_are_kept(self, spiral_root):
        table = SpiralDBTableSource(_PROJECT, "experiment").as_table()
        assert {f.name: f.type for f in table.schema} == {
            "subject_id": pa.large_string(),
            "session_start_datetime": _TS,
            "experiment_index": pa.uint32(),
            "correct_trials": pa.int64(),
            "total_trials": pa.int64(),
            "stop_timestamp": pa.uint64(),
            "eye_params": pa.large_list(pa.float32()),
        }

    def test_dataframe_view_has_tags_and_data_only(self, spiral_root):
        frame = SpiralDBTableSource(_PROJECT, "session").as_df()
        assert frame.columns == [
            "subject_id",
            "session_start_datetime",
            "booth",
            "date_of_record",
        ]

    def test_plain_typed_pod_is_refused(self, spiral_root):
        @function_pod(output_keys="n")
        def plain(total_trials: int) -> int:
            return total_trials

        source = SpiralDBTableSource(_PROJECT, "experiment")
        selected = SelectDataColumns(["total_trials"])(source)
        with pytest.raises(ValueError, match="int \\| None"):
            plain.pod(selected)


class TestErrors:
    def test_missing_table_names_project_and_dataset(self, spiral_root):
        with pytest.raises(ValueError) as info:
            SpiralDBTableSource(_PROJECT, "no_such_table", dataset="default")
        message = str(info.value)
        assert "no_such_table" in message
        assert _PROJECT in message
        assert "default" in message


class TestWorkshopPath:
    def _job(self, store: Path, calls: list[int]) -> PipelineJob:
        @function_pod(output_keys="accuracy")
        def counted(
            correct_trials: int | None, total_trials: int | None
        ) -> float | None:
            calls.append(1)
            if correct_trials is None or not total_trials:
                return None
            return correct_trials / total_trials

        job = PipelineJob(name="workshop", store=DeltaTableDatabase(store))
        with job:
            joined = Join()(
                SpiralDBTableSource(_PROJECT, "session"),
                SpiralDBTableSource(_PROJECT, "experiment"),
                label="joined",
            )
            trials = SelectDataColumns(["correct_trials", "total_trials"])(
                joined, label="trials"
            )
            counted.pod(trials, label="accuracy")
        return job

    def test_run_rerun_save_load(self, spiral_root, tmp_path):
        store = tmp_path / "store"
        calls: list[int] = []
        job = self._job(store, calls)
        job.run()
        rows = job.nodes["accuracy"].as_table().to_pylist()
        assert sorted((r["subject_id"], r["experiment_index"]) for r in rows) == [
            ("m1", 0),
            ("m1", 1),
            ("m2", 0),
        ]
        assert sorted((r["accuracy"] for r in rows), key=lambda v: (v is None, v)) == [
            0.5,
            0.6,
            None,
        ]
        assert len(calls) == 3

        calls.clear()
        rerun = self._job(store, calls)
        rerun.run()
        assert calls == []

        path = tmp_path / "job.json"
        rerun.save(path)
        loaded = PipelineJob.load(path, store=DeltaTableDatabase(store))
        records = loaded.nodes["accuracy"].get_all_records()
        assert records is not None
        assert records.num_rows == 3


def test_hashes_are_stable_across_processes(spiral_root):
    source = SpiralDBTableSource(_PROJECT, "experiment")
    script = (
        "import json\n"
        "from tests.test_databases import fake_pyspiral\n"
        "from orcapod.sources import SpiralDBTableSource\n"
        f"with fake_pyspiral.install({str(spiral_root)!r}):\n"
        f"    s = SpiralDBTableSource({_PROJECT!r}, 'experiment')\n"
        "print(json.dumps([s.content_hash().to_string(), s.pipeline_hash().to_string()]))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
        cwd=Path(__file__).resolve().parents[3],
    )
    other = json.loads(result.stdout.strip().splitlines()[-1])
    assert other == [
        source.content_hash().to_string(),
        source.pipeline_hash().to_string(),
    ]
