"""ITL-892 through Orcapod: does a process that used a Delta store exit?

Modes:
    pipeline-sync    ``PipelineJob`` on ``DeltaTableDatabase``, sync orchestrator, then
                     ``get_all_records`` on the function node.
    pipeline-async   the same with the async orchestrator.
    plain-read       ``DeltaTableDatabase`` write, flush, ``get_all_records``.
    source-read      ``DeltaTableSource`` over a table written by ``write_deltalake``.
    rerun            ``pipeline-sync`` twice on the same store (second run is all cache hits).

Prints ``done`` and returns normally so the interpreter runs its shutdown.
"""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path


def pipeline(store_dir: Path, orchestrator_name: str, runs: int = 1) -> None:
    import orcapod as op
    from orcapod.databases import DeltaTableDatabase
    from orcapod.pipeline import AsyncPipelineOrchestrator, PipelineJob
    from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

    orch = {
        "sync": SyncPipelineOrchestrator,
        "async": AsyncPipelineOrchestrator,
    }[orchestrator_name]

    @op.function_pod(output_keys=["result"])
    def double(x: int) -> int:
        return x * 2

    for _ in range(runs):
        source = op.sources.DictSource(
            [{"id": i, "x": 10 * i} for i in range(3)], tag_columns=["id"]
        )
        job = PipelineJob(store=DeltaTableDatabase(store_dir))
        with job:
            double.pod(source, label="double")
        job.run(orchestrator=orch())
        # Row counts are not checked: main still has the ITL-864 partial read.
        job.nodes["double"].get_all_records()


def plain_read(store_dir: Path) -> None:
    import pyarrow as pa

    from orcapod.databases import DeltaTableDatabase

    db = DeltaTableDatabase(store_dir)
    table = pa.table(
        {
            "__record_id": pa.array(["a", "b", "c"], pa.large_string()),
            "v": pa.array([1, 2, 3], pa.int64()),
        }
    )
    db.add_records(("t",), table, record_id_column="__record_id")
    db.flush()
    fresh = DeltaTableDatabase(store_dir)
    fresh.get_all_records(("t",), record_id_column="__record_id")


def source_read(store_dir: Path) -> None:
    import deltalake
    import pyarrow as pa

    import orcapod as op

    path = store_dir / "src"
    deltalake.write_deltalake(
        str(path),
        pa.table({"id": pa.array([1, 2, 3]), "x": pa.array([1.0, 2.0, 3.0])}),
    )
    src = op.sources.DeltaTableSource(path, tag_columns=["id"])
    src.as_table()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "mode",
        choices=[
            "pipeline-sync",
            "pipeline-async",
            "plain-read",
            "source-read",
            "rerun",
        ],
    )
    args = ap.parse_args()
    store = Path(tempfile.mkdtemp())
    if args.mode == "pipeline-sync":
        pipeline(store, "sync")
    elif args.mode == "pipeline-async":
        pipeline(store, "async")
    elif args.mode == "rerun":
        pipeline(store, "sync", runs=2)
    elif args.mode == "plain-read":
        plain_read(store)
    else:
        source_read(store)
    print("done", flush=True)


if __name__ == "__main__":
    main()
