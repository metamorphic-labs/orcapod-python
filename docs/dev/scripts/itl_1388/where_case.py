"""Case (ii) of ITL-1388: a source restricted with ``where``, saved and loaded.

Usage (from the repository root, so ``tests`` is importable):
``uv run python docs/dev/scripts/itl_1388/where_case.py <workdir>``

Runs against the disk-backed SpiralDB double of the test suite
(``tests/test_databases/fake_pyspiral.py``), never a live service. A job ``events``
over an ``event`` table (5 rows: m1 x3, m2 x2) runs once unrestricted, then the same job,
same database, with the source restricted ``where(subject_id="m2")``; the restricted job
is saved. A fresh process (this script with ``--read``) loads it and compares each read
with the live restricted job (2 rows).
"""

from __future__ import annotations

import json
import subprocess
import sys
import warnings
from pathlib import Path
from typing import Any

import pyarrow as pa

warnings.simplefilter("ignore")

from orcapod import PipelineJob, function_pod  # noqa: E402
from orcapod.databases import DeltaTableDatabase  # noqa: E402
from orcapod.sources import SpiralDBTableSource  # noqa: E402
from tests.test_databases import fake_pyspiral  # noqa: E402

PROJECT = "audit-sample"


def rows(table: Any) -> list[tuple[Any, ...]]:
    t = table.select(["subject_id", "timestamp", "doubled"]).to_pylist()
    return sorted((r["subject_id"], r["timestamp"], r["doubled"]) for r in t)


def write(work: Path) -> dict[str, Any]:
    project = fake_pyspiral.FakeSpiral().project(PROJECT)
    keys = pa.schema(
        [pa.field("subject_id", pa.string()), pa.field("timestamp", pa.uint64())]
    )
    project.create_table("default.event", key_schema=keys).write(
        pa.table(
            {
                "subject_id": ["m1", "m1", "m1", "m2", "m2"],
                "timestamp": pa.array([1, 2, 3, 4, 5], pa.uint64()),
                "amplitude": pa.array([1.5, 2.5, 3.5, 4.5, 5.5], pa.float64()),
            }
        )
    )

    @function_pod(output_keys="doubled")
    def double(amplitude: float | None) -> float | None:
        return None if amplitude is None else 2 * amplitude

    def job_over(source: SpiralDBTableSource) -> PipelineJob:
        job = PipelineJob(name="events", database=DeltaTableDatabase(work / "store"))
        with job:
            double.pod(source, label="double")
        return job

    plain = SpiralDBTableSource(
        PROJECT, "event", source_id="events", tag_only_columns=True
    )
    unrestricted = job_over(plain)
    unrestricted.run()
    restricted = job_over(plain.where(subject_id="m2"))
    restricted.run()
    restricted.save(work / "where_job.json")
    saved = json.loads((work / "where_job.json").read_text())
    return {
        "same_content_hash": plain.where(subject_id="m2").content_hash()
        == plain.content_hash(),
        "saved_where": saved["bindings"]["sources"]["events"].get("where"),
        "live_unrestricted": rows(unrestricted.nodes["double"].as_table()),
        "live_restricted": rows(restricted.nodes["double"].as_table()),
        "live_restricted_as_source": rows(
            restricted.nodes["double"].as_source().as_table()
        ),
    }


def read(work: Path) -> dict[str, Any]:
    job = PipelineJob.load(work / "where_job.json")
    out: dict[str, Any] = {
        "loaded_source_condition": str(job.nodes["events"].bound_source.condition),  # pyright: ignore[reportAttributeAccessIssue, reportOptionalMemberAccess]  # a source node of a loaded job
        "loaded_as_source": rows(job.nodes["double"].as_source().as_table()),
    }
    summary = job.run(load_only=True)
    out["load_only_conditions"] = {k: str(v) for k, v in summary.conditions.items()}
    out["load_only_as_table"] = rows(job.nodes["double"].as_table())
    return out


if __name__ == "__main__":
    work = Path(sys.argv[1]).resolve()
    with fake_pyspiral.install(work / "spiral"):
        if "--read" in sys.argv:
            print(json.dumps(read(work), default=str))
        else:
            work.mkdir(parents=True, exist_ok=True)
            result = write(work)
            child = subprocess.run(
                [sys.executable, __file__, str(work), "--read"],
                capture_output=True,
                text=True,
            )
            result["fresh_process"] = (
                json.loads(child.stdout.strip().splitlines()[-1])
                if child.returncode == 0
                else child.stderr[-1500:]
            )
            print(json.dumps(result, default=str))
