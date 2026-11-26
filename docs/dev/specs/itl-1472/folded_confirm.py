"""ITL-1472, O34: what the folded design does, on the prototype.

Run on a tree with ``folded-prototype.patch`` applied (it is not on any pushed branch):

    O34_MARK=label uv run python docs/dev/specs/itl-1472/folded_confirm.py

``O34_MARK=label`` is the developer's variant: the node of a pod called in the default
mode on a stream with data columns its function does not take has one label appended
to its identity tuple; every other node has today's identity.
"""

from __future__ import annotations

import warnings

from orcapod import PipelineJob, function_pod
from orcapod.core.sources import DictSource
from orcapod.databases import InMemoryArrowDatabase

warnings.simplefilter("ignore")
CALLS: list[float] = []


@function_pod(output_keys="doubled")
def double(amplitude: float) -> float:
    """Return twice ``amplitude``; record the call."""
    CALLS.append(amplitude)
    return 2 * amplitude


@function_pod(output_keys="halved")
def halve(amplitude: float) -> float:
    """Return half of ``amplitude``."""
    return amplitude / 2


def source(columns: tuple[str, ...]) -> DictSource:
    values = {"amplitude": 1.5, "noise": 0.25, "gain": 3.0}
    rows = [
        {"subject": s, "t": t, **{c: values[c] + t for c in columns}}
        for s, t in (("m1", 1), ("m1", 2), ("m2", 3))
    ]
    return DictSource(
        rows, tag_columns=["subject", "t"], source_id="events", tag_only_columns=True
    )


def job_of(database: InMemoryArrowDatabase, build) -> PipelineJob:  # noqa: ANN001
    job = PipelineJob(name="confirm", database=database)
    with job:
        build()
    return job


def paths(database: InMemoryArrowDatabase) -> list[str]:
    return sorted("/".join(p)[:110] for p in database.list_sources())


def main() -> None:
    wide = source(("amplitude", "noise", "gain"))
    narrow = source(("amplitude",))

    print("== 1. No select node: the pod's node is fed by the source")
    db = InMemoryArrowDatabase()
    job = job_of(db, lambda: (double.pod(wide, label="f"), halve.pod(wide, label="h")))
    print("  nodes:", {n: node.node_type for n, node in sorted(job.nodes.items())})
    job.run()
    f = job.nodes["f"]
    print("  rows of f:", sorted(f.as_table().column("doubled").to_pylist()))
    print("  calls:", len(CALLS))

    print("== 2. Results are shared with the hand-written select")
    CALLS.clear()
    by_hand = job_of(
        db,
        lambda: double.pod(wide.select_data_columns(["amplitude"]), label="f"),
    )
    by_hand.run()
    g = by_hand.nodes["f"]
    print("  nodes:", sorted(by_hand.nodes))
    print("  calls made by the hand-written form on the same store:", len(CALLS))
    print("  pipeline hash equal:", f.pipeline_hash() == g.pipeline_hash())
    print("  content hash equal:", f.content_hash() == g.content_hash())
    for path in paths(db):
        print("   ", path)

    print("== 3. The modes: default, strict, process")
    plain = job_of(InMemoryArrowDatabase(), lambda: double.pod(narrow, label="f"))
    strict = job_of(
        InMemoryArrowDatabase(), lambda: double.pod(narrow, strict=True, label="f")
    )
    process = job_of(
        InMemoryArrowDatabase(), lambda: double.pod.process(narrow, label="f")
    )
    a, b, c = (j.nodes["f"] for j in (plain, strict, process))
    print(
        "  no extra column: default, strict and process share the pipeline hash:",
        a.pipeline_hash() == b.pipeline_hash() == c.pipeline_hash(),
        "| content hash:",
        a.content_hash() == b.content_hash() == c.content_hash(),
    )
    for name, call in (
        ("strict=True", lambda: double.pod(wide, strict=True)),
        ("process", lambda: double.pod.process(wide)),
    ):
        try:
            call()
            print(f"  extra columns, {name}: accepted")
        except Exception as exc:  # noqa: BLE001  # the demonstration prints the refusal
            print(f"  extra columns, {name}: {type(exc).__name__}: {str(exc)[:70]}")
    print(
        "  extra columns, default: pipeline hash differs from the no-extra pod's:",
        f.pipeline_hash() != a.pipeline_hash(),
    )

    print("== 4. The input's column order never counts")
    other = job_of(
        InMemoryArrowDatabase(),
        lambda: double.pod(source(("gain", "noise", "amplitude")), label="f"),
    )
    print(
        "  columns given in another order: pipeline hash equal:",
        other.nodes["f"].pipeline_hash() == f.pipeline_hash(),
        "| content hash equal:",
        other.nodes["f"].content_hash() == f.content_hash(),
    )
    print("  the node's identity tuple, last element:", f.identity_structure()[-1])
    print(
        "  elements in the tuple:",
        len(f.identity_structure()),
        "| without extra columns:",
        len(a.identity_structure()),
    )


if __name__ == "__main__":
    main()
