"""Pinned source snapshots and execution caches cooperate across orchestrators."""

from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import GroupBy
from orcapod.databases import InMemoryArrowDatabase
from orcapod.hashing.arrow_hashers import StarfixArrowHasher
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import SpiralDBTableSource
from tests.test_databases import fake_pyspiral


@function_pod(output_keys="total")
def total(x: list[int | None]) -> int:
    """Reduce one group from the nullable fake table."""
    return sum(value for value in x if value is not None)


@pytest.mark.parametrize("first", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator])
@pytest.mark.parametrize(
    "second", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_pinned_grouped_job_reuses_scan_rows_and_hashes(
    tmp_path: Path, first, second
) -> None:
    """A warm run neither scans nor groups nor hashes, even with channel backpressure."""
    with fake_pyspiral.install(tmp_path / "spiral"):
        project = fake_pyspiral.FakeSpiral().project("reuse-integration")
        data = pa.table(
            {
                "g": [i // 20 for i in range(100)],
                "k": list(range(100)),
                "x": list(range(100)),
            }
        )
        project.create_table(
            "default.events",
            key_schema=pa.schema([data.schema.field("g"), data.schema.field("k")]),
        ).write(data)
        fake_pyspiral.scan_log.clear()
        source = SpiralDBTableSource("reuse-integration", "events", batch_rows=13)
        with PipelineJob(name="spiral_reuse", store=InMemoryArrowDatabase()) as job:
            grouped = source.group_by(["g"]).drop_tag_columns(["k"])
            total.pod(grouped, label="total")
        assert fake_pyspiral.scan_log == []
        assert job.run(orchestrator=first()).ok
        before = job.nodes["total"].as_table(all_info=True)
        assert len(fake_pyspiral.scan_log) == 1
        with (
            patch.object(
                GroupBy, "unary_static_process", side_effect=AssertionError("regrouped")
            ),
            patch.object(
                StarfixArrowHasher, "hash_table", side_effect=AssertionError("rehashed")
            ),
        ):
            summary = job.run(orchestrator=second())
        assert summary.ok
        assert summary.nodes["total"].computed == 0
        assert summary.nodes["total"].reused == 5
        assert len(fake_pyspiral.scan_log) == 1
        assert job.nodes["total"].as_table(all_info=True).equals(before)
