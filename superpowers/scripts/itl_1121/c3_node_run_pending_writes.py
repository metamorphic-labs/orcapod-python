"""Comment 3: node.run() starts without the pre-run flush that job.run() makes (ITL-919).

The hazard ITL-919 guards against: a table-committing store reaches batch_size on a
table mid-run and commits pipeline records whose results are still pending. Check, on a
Delta store with a small batch_size and rows left pending before the run, that every
committed pipeline-record version only references committed results after node.run().
"""

import tempfile
from pathlib import Path

import deltalake
import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.databases.storage_utils import delta_to_pyarrow_dataset
from orcapod.sources import DictSource


@function_pod(output_keys="y")
def double(x: int) -> int:
    return 2 * x


def col(table: deltalake.DeltaTable, name: str) -> set:
    return set(
        delta_to_pyarrow_dataset(table).to_table(columns=[name])[name].to_pylist()
    )


def check(root: Path) -> None:
    tables = {
        ("result" if "_result" in t.relative_to(root).parts else "pipeline"): t
        for t in {log.parent for log in root.rglob("_delta_log")}
        if t.name != "leftover"
    }
    result_history = deltalake.DeltaTable(str(tables["result"])).history()
    pipeline_history = deltalake.DeltaTable(str(tables["pipeline"])).history()
    print(
        f"  pipeline commits: {len(pipeline_history)}, result commits: {len(result_history)}"
    )
    bad = 0
    for entry in pipeline_history:
        records = deltalake.DeltaTable(
            str(tables["pipeline"]), version=entry["version"]
        )
        versions = [
            r["version"] for r in result_history if r["timestamp"] <= entry["timestamp"]
        ]
        committed = (
            col(
                deltalake.DeltaTable(str(tables["result"]), version=max(versions)),
                "__record_id",
            )
            if versions
            else set()
        )
        if not col(records, "__data_id") <= committed:
            bad += 1
    print(f"  pipeline versions referencing uncommitted results: {bad}")


for batch_size, n in [(50, 120), (5, 40)]:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        store = DeltaTableDatabase(root, batch_size=batch_size)
        for i in range(batch_size - 1):  # pending, never flushed
            store.add_record(
                ("leftover",),
                f"left-{i}".encode(),
                pa.table({"v": pa.array([i], pa.int64())}),
            )
        job = PipelineJob(name="j", database=store)
        with job:
            double.pod(
                DictSource(
                    [{"k": i, "x": i} for i in range(n)],
                    tag_columns=["k"],
                    source_id="s",
                ),
                label="dbl",
            )
        print(f"batch_size={batch_size}, rows={n}: node.run()")
        job.nodes["dbl"].run()
        check(root)
