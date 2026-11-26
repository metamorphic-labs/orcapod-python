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
5. One node feeding one operator twice: ``Join`` and ``Rejoin`` refuse it themselves;
   ``MergeJoin`` and ``SemiJoin`` build a node with two inputs but one graph edge, and
   the run fails.
6. A job reads a source once per run, however many nodes it feeds.
7. A stream built before the ``with`` block is a leaf (a slot), not its steps.
8. Where a saved job keeps a restriction and its edges today.
"""

from __future__ import annotations

import json
import sys
import tempfile
import warnings
from pathlib import Path

import pyarrow as pa

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))  # the SpiralDB fake lives in the test package

from orcapod import function_pod  # noqa: E402  # after the path insert
from orcapod.core.nodes import FunctionJobNode  # noqa: E402
from orcapod.core.operators import Join, MergeJoin, Rejoin, SemiJoin  # noqa: E402
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


@function_pod(output_keys="halved")
def halve(amplitude: float | None) -> float | None:
    """Return half of ``amplitude``."""
    return None if amplitude is None else amplitude / 2


def more_on_main(table: SpiralDBTableSource, root: Path) -> None:
    heading("5. One node feeding one operator twice")
    for operator in (Join, Rejoin, MergeJoin, SemiJoin):
        name = operator.__name__
        try:
            job = PipelineJob(name="twice", database=InMemoryArrowDatabase())
            with job:
                operator()(table, table, label="op")
            node = job.nodes["op"]
            print(
                f"  {name}(A, A): built, {len(node.upstreams)} inputs,",
                f"{len(job._graph_edges)} graph edge(s)",  # pyright: ignore[reportPrivateUsage]
            )
            job.run()
            print("    ran:", node.as_table().num_rows, "rows")
        except Exception as exc:  # noqa: BLE001  # the demonstration prints what main raises
            print(f"  {name}(A, A): {type(exc).__name__}: {str(exc)[:88]}")

    heading("6. Scans of one run: a source feeding two functions")
    job = PipelineJob(name="scans", database=InMemoryArrowDatabase())
    with job:
        double.pod(table, label="f")
        halve.pod(table, label="g")
    fake_spiral_sdk.scan_log.clear()
    job.run()
    print(
        "  scans:",
        [f"where={r.filter!r}, limit={r.limit}" for r in fake_spiral_sdk.scan_log],
    )

    heading("7. A stream built before the with block")
    renamed = table.where(subject_id="m1").map_data({"amplitude": "a"})
    job = PipelineJob(name="outside", database=InMemoryArrowDatabase())
    with job:
        renamed.map_data({"a": "amplitude"}, label="back")
    print(
        "  job.nodes:",
        {n[:12]: node.node_type for n, node in sorted(job.nodes.items())},
    )

    heading("8. A saved job: the restriction and the edges")
    job = PipelineJob(name="saved", database=InMemoryArrowDatabase())
    with job:
        double.pod(table.where(subject_id="m1"), label="f")
    path = root / "job.json"
    job.save(path)
    saved = json.loads(path.read_text())
    print(
        "  bindings.sources.events.where:",
        saved["bindings"]["sources"]["events"]["where"],
    )
    edges = saved["pipeline"]["edges"]
    print(
        f"  pipeline.edges: {len(edges)} pairs of node keys, such as",
        [k[:14] for k in edges[0]],
    )
    print("  pipeline.nodes keys:", sorted(k[:14] for k in saved["pipeline"]["nodes"]))


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

        more_on_main(table, Path(root))

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


if __name__ == "__main__":
    main()
