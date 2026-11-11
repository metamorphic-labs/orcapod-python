"""Child process for ``a3_cached_rerun.py`` (ITL-874). Not meant to be run on its own.

Usage: ``uv run python a3_cached_rerun_child.py <store_dir> <counter_file> <sync|async> <array|int>``

``int`` runs the same job over plain integers, as a control.

Builds the job ``source -> double -> GroupBy -> DropDataColumns -> first`` on a
``DeltaTableDatabase`` at ``<store_dir>``, runs it, and prints one JSON object: how many times
each pod body ran in this process, a digest of every array read back, and the content hashes
of the source, of each data row and of the job's nodes.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

import numpy as np

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.operators import DropDataColumns, GroupBy
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.sources import DictSource

os.environ.setdefault("RUST_LOG", "error")

STORE, COUNTER, MODE, VALUES = (
    Path(sys.argv[1]),
    Path(sys.argv[2]),
    sys.argv[3],
    sys.argv[4],
)

X = {
    "a": np.arange(6, dtype=np.float32).reshape(2, 3),
    "b": np.asfortranarray(np.arange(6, dtype=np.float64).reshape(3, 2)),
    "c": np.array([1, -2, 3], dtype=np.int8),
    "d": np.array(7.5, dtype=np.float32),
}
GROUP = {"a": "g1", "b": "g1", "c": "g2", "d": "g2"}


def bump(name: str) -> None:
    with COUNTER.open("a") as f:
        f.write(name + "\n")


@function_pod(output_keys="y")
def double(x: np.ndarray) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    bump("double")
    return np.asarray(x * 2)


@function_pod(output_keys="first")
def first(y: list[np.ndarray]) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    bump("first")
    return y[0]


@function_pod(output_keys="y")
def double_int(x: int) -> int:
    bump("double")
    return x * 2


@function_pod(output_keys="first")
def first_int(y: list[int]) -> int:
    bump("first")
    return y[0]


def digest(value: object) -> str:
    """Digest dtype, shape, memory order and values. Lists digest each member."""
    if isinstance(value, list):
        return "[" + ",".join(digest(v) for v in value) + "]"
    if not isinstance(value, np.ndarray):
        return repr(value)
    h = hashlib.sha256()
    order = "F" if value.flags["F_CONTIGUOUS"] and value.ndim > 1 else "C"
    h.update(f"{value.dtype.str}|{value.shape}|{order}|".encode())
    h.update(np.ascontiguousarray(value).tobytes())
    return h.hexdigest()[:16]


def main() -> None:
    before = COUNTER.read_text().splitlines() if COUNTER.exists() else []
    values = X if VALUES == "array" else {k: i for i, k in enumerate(X)}
    pod_a, pod_b = (double, first) if VALUES == "array" else (double_int, first_int)
    src = DictSource(
        data=[{"k": k, "g": GROUP[k], "x": v} for k, v in values.items()],
        tag_columns=["k", "g"],
        source_id=VALUES,
    )
    job = PipelineJob(name="cached_rerun", store=DeltaTableDatabase(base_path=STORE))
    with job:
        doubled = pod_a.pod(src, label="double")
        grouped = GroupBy(by=["g"])(doubled, label="grouped")
        pod_b.pod(DropDataColumns(["k"])(grouped), label="first")
    orch = SyncPipelineOrchestrator() if MODE == "sync" else AsyncPipelineOrchestrator()
    job.run(orchestrator=orch)
    after = COUNTER.read_text().splitlines() if COUNTER.exists() else []
    new_calls = after[len(before) :]

    out: dict[str, object] = {
        "calls": {n: new_calls.count(n) for n in ("double", "first")},
        "source_content_hash": str(src.content_hash()),
        "source_row_hashes": {
            t.as_dict()["k"]: str(d.content_hash()) for t, d in src.iter_data()
        },
    }
    for label, column, key in (("double", "y", "k"), ("first", "first", "g")):
        node = job.nodes[label]
        out[f"{label}_arrays"] = {
            t.as_dict()[key]: digest(d.as_dict()[column]) for t, d in node.iter_data()
        }
        out[f"{label}_row_hashes"] = {
            t.as_dict()[key]: str(d.content_hash()) for t, d in node.iter_data()
        }
        out[f"{label}_node_content_hash"] = str(node.content_hash())
    print(json.dumps(out, sort_keys=True))


if __name__ == "__main__":
    main()
