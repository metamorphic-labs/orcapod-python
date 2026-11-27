"""ITL-1518: what one job finds in an existing store, before and after the change.

Run it on the base code, then again on a tree with ``prototype.patch`` applied, on the
same store directory and with the same ``CALLS_FILE``:

    CALLS_FILE=<file> uv run python docs/dev/specs/itl-1518/store_run.py <store dir>
    O34_MARK=label CALLS_FILE=<file> uv run python docs/dev/specs/itl-1518/store_run.py <store dir>

It prints the function calls made by this run, every node with its hashes, and every
table of the store with its row count.
"""

from __future__ import annotations

import collections
import os
import sys
import warnings

import deltalake

sys.path.insert(0, os.path.dirname(__file__))

import store_functions as fns  # noqa: E402  # beside this script

from orcapod import PipelineJob  # noqa: E402
from orcapod.core.sources import DictSource  # noqa: E402
from orcapod.databases import DeltaTableDatabase  # noqa: E402

warnings.simplefilter("ignore")


def src(name: str, columns: tuple[str, ...], tag_only: bool = True) -> DictSource:
    """A three-row source with tags ``subject`` and ``t`` and the given data columns."""
    rows = [
        {"subject": s, "t": t, **{c: i + 1.5 + t for i, c in enumerate(columns)}}
        for s, t in (("m1", 1), ("m1", 2), ("m2", 3))
    ]
    kw = {"tag_only_columns": True} if tag_only else {}
    return DictSource(rows, tag_columns=["subject", "t"], source_id=name, **kw)


def calls() -> collections.Counter[str]:
    """The calls recorded so far, per function."""
    path = os.environ["CALLS_FILE"]
    if not os.path.exists(path):
        return collections.Counter()
    with open(path) as f:
        return collections.Counter(f.read().split())


def main() -> None:
    store = sys.argv[1]
    job = PipelineJob(name="store", database=DeltaTableDatabase(store))
    with job:
        wide = src("wide", ("amplitude", "noise", "gain"))
        doubled = fns.double.pod(wide, label="double_wide")  # extra columns
        fns.triple.pod(doubled, label="triple_down")  # downstream of it
        fns.halve.pod(src("narrow", ("amplitude",)), label="halve_narrow")  # none
        fns.add.pod(  # two inputs, each with an extra column
            src("a", ("x", "e1")), src("b", ("y", "e2")), label="add_two"
        )
        fns.scale.pod(  # data-tags fall through
            src("dt", ("amplitude", "noise"), tag_only=False), label="scale_datatags"
        )
    before = calls()
    job.run()
    print("calls in this run:", dict(calls() - before))
    for name, node in sorted(job.nodes.items()):
        print(
            f"  node {name:28s} {type(node).__name__:18s} "
            f"c={node.content_hash().to_hex()[:10]} p={node.pipeline_hash().to_hex()[:10]}"
        )
    for name in (
        "double_wide",
        "triple_down",
        "halve_narrow",
        "add_two",
        "scale_datatags",
    ):
        table = job.nodes[name].as_table()
        print("  rows", name, table.num_rows, sorted(table.column_names))
    print("tables in the store:")
    for root, dirs, _ in os.walk(store):
        if "_delta_log" in dirs:
            n = deltalake.DeltaTable(root).to_pyarrow_table().num_rows
            print(f"  {n:3d} rows  {os.path.relpath(root, store)}")
            dirs[:] = []
    sys.stdout.flush()
    os._exit(0)  # the process otherwise stays alive after the run


if __name__ == "__main__":
    main()
