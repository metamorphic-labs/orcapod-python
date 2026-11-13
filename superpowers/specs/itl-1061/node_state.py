"""Show each job node's in-memory state and staleness after one and two runs (ITL-1061).

    uv run python superpowers/specs/itl-1061/node_state.py

For each node: its kind, ``last_modified``, ``is_stale``, its upstreams (the objects its
staleness is computed against), and the in-memory result it keeps (operator
``_cached_output_stream`` rows, function ``_cached_output_datas`` entries). Also shows what a
re-bound source does to the source node's modified time. Private attributes are read here
only because what they hold is the subject of the investigation.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import rerun_counts  # noqa: E402  # pyright: ignore[reportImplicitRelativeImport]  # a sibling script, run from this folder's path

from orcapod import PipelineJob  # noqa: E402
from orcapod.databases import InMemoryArrowDatabase  # noqa: E402


def describe(job: PipelineJob, title: str) -> None:
    print(f"### {title}")
    for label in (
        "sessions",
        "experiments",
        "drop_nulls",
        "group_by",
        "map_tags",
        "join",
        "accuracy",
        "grade",
    ):
        node = job.nodes[label]
        kept = ""
        if hasattr(node, "_cached_output_stream"):
            s = node._cached_output_stream
            kept = f"_cached_output_stream={'None' if s is None else type(s).__name__}"
        if hasattr(node, "_cached_output_datas"):
            kept = f"_cached_output_datas={len(node._cached_output_datas)}"
        if hasattr(node, "bound_source"):
            kept = f"bound_source={type(node.bound_source).__name__} (src last_modified {node.bound_source.last_modified:%H:%M:%S.%f})"
        ups = ",".join(type(u).__name__ for u in node.upstreams)
        lm = node.last_modified
        print(
            f"{label:<12} {type(node).__name__:<16} last_modified="
            f"{'None' if lm is None else format(lm, '%H:%M:%S.%f')} is_stale={getattr(node, 'is_stale', 'n/a')!s:<5} "
            f"upstreams=[{ups}] {kept}"
        )
    print()


sources = rerun_counts.arrow_sources()
job = PipelineJob(name="ws", store=InMemoryArrowDatabase())
with job:
    rerun_counts.build(sources[0], sources[1], rerun_counts.make_pods())
describe(job, "after compile, before any run")
job.run()
describe(job, "after job.run() #1")
job.run()
describe(job, "after job.run() #2")
# A source whose data really changed: experiments with different ``correct`` values.
import pyarrow as pa  # noqa: E402
import pyarrow.compute as pc  # noqa: E402

from orcapod.sources import ArrowTableSource  # noqa: E402

_, experiments = rerun_counts.tables()
changed = experiments.set_column(
    experiments.schema.get_field_index("correct"),
    "correct",
    pc.subtract(pa.scalar(10, pa.int64()), experiments["correct"]),
)
before = job.nodes["accuracy"].as_table()["accuracy"].to_pylist()
job.bind(
    sources={
        "experiments": ArrowTableSource(
            changed,
            tag_columns=["subject_id", "session", "experiment_index"],
            source_id="experiments",
        )
    }
)
describe(job, "after job.bind(sources={'experiments': same id, CHANGED rows})")
would_skip = [
    label
    for label in ("drop_nulls", "group_by", "map_tags", "join", "accuracy", "grade")
    if not job.nodes[label].is_stale
]
print("a skip on 'not is_stale and holds a result' would skip:", would_skip)
job.run()
after = job.nodes["accuracy"].as_table()["accuracy"].to_pylist()
print("today's run #3 picked up the change:", before != after)
