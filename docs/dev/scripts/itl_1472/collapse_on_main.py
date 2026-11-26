"""ITL-1472 evidence: restricted uses of one table collapse into one node on main.

Run from the repository root (offline, the disk-backed SpiralDB fake of the tests):

    uv run python docs/dev/scripts/itl_1472/collapse_on_main.py

What it shows, each under its own heading:

1. ``f(A.where(m1))`` and ``f(A.where(m2))`` in one job are refused.
2. Why: the two sources, and the two invocations of ``f``, have one content hash, and
   a pipeline keys an invocation and a leaf by that hash.
3. With only the refusal switched off, the two uses become ONE slot and ONE node, and
   the rows that flow are those of whichever use was registered first.
4. Two separate ``DictSource`` objects with equal rows become one slot and one node.
5. Two node objects of one identity are equal and hash alike, so a ``dict`` or the DAG
   keyed by node objects holds one of them.
"""

from __future__ import annotations

import sys
import tempfile
import warnings
from pathlib import Path

import pyarrow as pa

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))  # the SpiralDB fake lives in the test package

from orcapod import function_pod  # noqa: E402  # after the path insert
from orcapod.core.nodes import FunctionJobNode  # noqa: E402
from orcapod.core.sources import DictSource  # noqa: E402
from orcapod.core.sources.spiraldb_table_source import SpiralDBTableSource  # noqa: E402
from orcapod.databases import InMemoryArrowDatabase  # noqa: E402
from orcapod.pipeline import PipelineJob  # noqa: E402
from orcapod.pipeline import job as job_module  # noqa: E402
from orcapod.pipeline import restrictions  # noqa: E402
from orcapod.pipeline.pod_invocation import FunctionInvocation  # noqa: E402
from tests.test_databases import fake_spiral_sdk  # noqa: E402

warnings.simplefilter("ignore")

PROJECT = "workshop-sample"
KEYS = pa.schema([pa.field("subject_id", pa.string()), pa.field("t", pa.uint64())])


@function_pod(output_keys="doubled")
def double(amplitude: float | None) -> float | None:
    """Return twice ``amplitude``."""
    return None if amplitude is None else 2 * amplitude


def heading(text: str) -> None:
    print(f"\n== {text}")


def events() -> SpiralDBTableSource:
    return SpiralDBTableSource(
        PROJECT, "event", source_id="events", tag_only_columns=True
    )


def job_over(*sources: object, labels: tuple[str, ...]) -> PipelineJob:
    job = PipelineJob(name="demo", database=InMemoryArrowDatabase())
    with job:
        for source, label in zip(sources, labels, strict=True):
            double.pod(source, label=label)
    return job


def describe(job: PipelineJob) -> None:
    kinds = {name: node.node_type for name, node in sorted(job.nodes.items())}
    print("  job.nodes:", kinds)
    job.run()
    for name, node in sorted(job.nodes.items()):
        if isinstance(node, FunctionJobNode):
            rows = node.as_table().sort_by("t").to_pydict()
            print(f"  rows of {name!r}: t = {rows['t']}")


def main() -> None:
    with (
        tempfile.TemporaryDirectory() as root,
        fake_spiral_sdk.install(Path(root) / "spiral"),
    ):
        project = fake_spiral_sdk.ExternalProject(PROJECT)
        project.create_table("default.event", key_schema=KEYS).write(
            pa.table(
                {
                    "subject_id": ["m1", "m1", "m1", "m2", "m2"],
                    "t": pa.array([1, 2, 3, 4, 5], pa.uint64()),
                    "amplitude": pa.array([1.5, 2.5, 3.5, 4.5, 5.5], pa.float64()),
                }
            )
        )
        table = events()
        m1, m2 = table.where(subject_id="m1"), table.where(subject_id="m2")

        heading("1. f(A.where(m1)) and f(A.where(m2)) in one job: refused")
        try:
            job_over(m1, m2, labels=("first", "second"))
        except ValueError as exc:
            print("  ValueError:", str(exc)[:118], "...")

        heading("2. One identity: the sources and the invocations hash alike")
        print("  source content hashes equal:", m1.content_hash() == m2.content_hash())
        print(
            "  source pipeline hashes equal:", m1.pipeline_hash() == m2.pipeline_hash()
        )
        inv1 = FunctionInvocation(double.pod, (m1,))
        inv2 = FunctionInvocation(double.pod, (m2,))
        print("  invocation keys equal:", inv1.content_hash() == inv2.content_hash())

        heading("3. Refusal switched off (demonstration only): one slot, one node")
        original = restrictions.require_one_restriction
        restrictions.require_one_restriction = lambda *a, **k: None
        saved_job_refusal = job_module.refuse_restriction_conflicts
        job_module.refuse_restriction_conflicts = lambda *a, **k: None
        try:
            for order, pair in (("m1 then m2", (m1, m2)), ("m2 then m1", (m2, m1))):
                print(f"  registered {order}:")
                describe(job_over(*pair, labels=("first", "second")))
        finally:
            restrictions.require_one_restriction = original
            job_module.refuse_restriction_conflicts = saved_job_refusal

    heading("4. Two separate DictSource objects with equal rows: one slot, one node")
    rows = [{"t": 1, "amplitude": 1.5}, {"t": 2, "amplitude": 2.5}]
    a = DictSource(rows, tag_columns=["t"], tag_only_columns=True)
    b = DictSource(rows, tag_columns=["t"], tag_only_columns=True)
    print(
        "  a is b:",
        a is b,
        "| content hashes equal:",
        a.content_hash() == b.content_hash(),
    )
    describe(job_over(a, b, labels=("first", "second")))

    heading("5. Two node objects of one identity are one dict key")
    job_a = job_over(a, labels=("f",))
    job_b = job_over(b, labels=("f",))
    node_a, node_b = job_a.nodes["f"], job_b.nodes["f"]
    print("  node_a is node_b:", node_a is node_b)
    print("  node_a == node_b:", node_a == node_b)
    print("  len({node_a: 1, node_b: 2}):", len({node_a: 1, node_b: 2}))


if __name__ == "__main__":
    main()
