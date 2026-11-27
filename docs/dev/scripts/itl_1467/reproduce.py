"""Reproduce both ITL-1467 symptoms on origin/main at 5eb327ad."""

from contextlib import chdir
from pathlib import Path
from tempfile import TemporaryDirectory

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes import FunctionJobNode
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.sources import DictSource


@function_pod(output_keys="total")
def total(x: int, y: int) -> int:
    """Add the two data values."""
    return x + y


def make_job(database: InMemoryArrowDatabase | DeltaTableDatabase) -> None:
    """Run and save a job over a source whose data is not saved."""
    source = DictSource(
        [{"id": 1, "group": "a", "x": 2, "y": 3}],
        tag_columns=["id", "group"],
        tag_only_columns=True,
        source_id="memory",
    )
    job = PipelineJob(name="small", database=database)
    with job:
        _ = total.pod(source, label="total")
    _ = job.run()
    job.save("job.json")


def main() -> None:
    """Show the load-only summary and the creation of a missing Delta root."""
    with TemporaryDirectory() as directory, chdir(directory):
        for database in (InMemoryArrowDatabase(), DeltaTableDatabase("store")):
            make_job(database)
            loaded = PipelineJob.load("job.json", database=database)
            print(type(database).__name__, loaded.unbound_sources)
            print(loaded.run(load_only=True))
            node = loaded.nodes["total"]
            assert isinstance(node, FunctionJobNode)
            print("function rows:", node.as_table().num_rows)

        _ = Path("store").rename("moved")
        print("store exists before load:", Path("store").exists())
        loaded = PipelineJob.load("job.json")
        print("store exists after load:", Path("store").exists())
        node = loaded.nodes["total"]
        assert isinstance(node, FunctionJobNode)
        print("stored rows:", node.as_source().as_table().num_rows)


if __name__ == "__main__":
    main()
