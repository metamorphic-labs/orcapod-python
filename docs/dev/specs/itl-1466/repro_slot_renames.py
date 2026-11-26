"""A stream built before a job becomes a slot that renames its system-tag columns.

Run on the library as it is: uv run python docs/dev/specs/itl-1466/repro_slot_renames.py
"""

import warnings

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource

warnings.simplefilter("ignore")


@function_pod(output_keys=["r"])
def add(x: float, y: float) -> float:
    return x + y


units = DictSource(
    [
        {"s": "m1", "u": "a", "x": 1.0, "y": 2.0},
        {"s": "m1", "u": "b", "x": 1.0, "y": 2.0},
    ],
    tag_columns=["s", "u"],
    tag_only_columns=True,
    source_id="units",
)
selected = units.select_tag_columns(["s"])  # built before the job

job = PipelineJob(name="p", database=InMemoryArrowDatabase())
with job:
    add.pod(selected, label="add")
job.run()


def system_tags(stream):
    return [c for c in stream.as_table(columns={"system_tags": True}).column_names
            if c.startswith("_tag")]  # fmt: skip


plain = add.pod(selected)
print("job nodes:", sorted(job.nodes))
print("plain stream:", system_tags(plain))
print("job node:    ", system_tags(job.nodes["add"]))
print("content hash equal:", plain.content_hash() == job.nodes["add"].content_hash())

inside = PipelineJob(name="p", database=InMemoryArrowDatabase())
with inside:
    add.pod(units.select_tag_columns(["s"]), label="add")
inside.run()
print("built inside the job:", system_tags(inside.nodes["add"]))
print("content hash equal:", plain.content_hash() == inside.nodes["add"].content_hash())
