"""A3: NumPy arrays through ``PipelineJob`` on every local store and both orchestrators (ITL-874).

Claims tested:

* A job ``source -> pod(-> ndarray) -> GroupBy -> DropDataColumns -> pod(list[ndarray] ->
  ndarray)`` runs on ``InMemoryArrowDatabase``, ``DeltaTableDatabase`` and
  ``ConnectorArrowDatabase(SQLiteConnector)``, with the sync and the async orchestrator.
* After the run, every node read through ``job.nodes[label].iter_data()`` returns arrays with
  the original dtype, shape and values. ``as_table()`` shows the Arrow type stored.
* The same job with plain ``int`` values, as a control. It shows which failures are specific
  to arrays.

A row that fails is logged at WARNING by default, not raised; those messages are
captured and printed.

Run: ``uv run python superpowers/specs/array-tensor-storage-experiments/a3_pipeline_stores.py``
"""

from __future__ import annotations

import logging
import os
import tempfile
from collections.abc import Callable
from pathlib import Path

import numpy as np

from a3_common import describe, report, same_array  # pyright: ignore[reportImplicitRelativeImport]  # run as a script: sys.path[0] is this directory
from orcapod import PipelineJob, function_pod
from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    InMemoryArrowDatabase,
    SQLiteConnector,
)
from orcapod.operators import DropDataColumns, GroupBy
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.sources import DictSource

os.environ.setdefault("RUST_LOG", "error")  # silence deltalake's binary-stats warnings

X = {
    "a": np.arange(6, dtype=np.float32).reshape(2, 3),
    "b": np.asfortranarray(np.arange(6, dtype=np.float64).reshape(3, 2)),
    "c": np.array([1, -2, 3], dtype=np.int8),
}
GROUP = {"a": "g1", "b": "g1", "c": "g2"}


class _Collect(logging.Handler):
    """Collect WARNING+ log records: a row that fails is logged, not raised."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(" ".join(record.getMessage().split())[:200])


@function_pod(output_keys="y")
def double(x: np.ndarray) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    return np.asarray(x * 2)


@function_pod(output_keys="first")
def first(y: list[np.ndarray]) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    return y[0]


@function_pod(output_keys="y")
def double_int(x: int) -> int:
    return x * 2


@function_pod(output_keys="first")
def first_int(y: list[int]) -> int:
    return y[0]


def build(job: PipelineJob, arrays: bool) -> None:
    values = X if arrays else {k: i for i, k in enumerate(X)}
    src = DictSource(
        data=[{"k": k, "g": GROUP[k], "x": v} for k, v in values.items()],
        tag_columns=["k", "g"],
        source_id="arrays" if arrays else "ints",
    )
    pod_a, pod_b = (double, first) if arrays else (double_int, first_int)
    with job:
        doubled = pod_a.pod(src, label="double")
        grouped = GroupBy(by=["g"])(doubled, label="grouped")
        pod_b.pod(DropDataColumns(["k"])(grouped), label="first")


def check_arrays(job: PipelineJob) -> tuple[str, str]:
    doubled = {
        t.as_dict()["k"]: d.as_dict()["y"] for t, d in job.nodes["double"].iter_data()
    }
    firsts = {
        t.as_dict()["g"]: d.as_dict()["first"]
        for t, d in job.nodes["first"].iter_data()
    }
    grouped = list(job.nodes["grouped"].iter_data())
    problems = [
        f"double[{k}]={describe(doubled.get(k))}"
        for k, v in X.items()
        if not same_array(doubled.get(k), v * 2)
    ]
    for g, got in firsts.items():
        if not any(same_array(got, X[k] * 2) for k in X if GROUP[k] == g):
            problems.append(f"first[{g}]={describe(got)}")
    counts = f"rows double={len(doubled)} grouped={len(grouped)} first={len(firsts)}"
    arrow = job.nodes["double"].as_table().schema.field("y").type
    if problems or len(doubled) != 3 or len(firsts) != 2 or len(grouped) != 2:
        return "DEFECT", f"{counts}; stored y as {arrow}; problems={problems}"
    return "OK", f"{counts}; stored y as {arrow}; arrays equal"


def check_ints(job: PipelineJob) -> tuple[str, str]:
    counts = {
        label: len(list(job.nodes[label].iter_data()))
        for label in ("double", "grouped", "first")
    }
    status = "OK" if counts == {"double": 3, "grouped": 2, "first": 2} else "DEFECT"
    return status, f"rows {counts}"


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="itl874_a3_stores_"))
    stores: dict[str, Callable[[str], ArrowDatabaseProtocol]] = {
        "InMemoryArrowDatabase": lambda _: InMemoryArrowDatabase(),
        "DeltaTableDatabase": lambda tag: DeltaTableDatabase(base_path=tmp / tag),
        "SQLite": lambda tag: ConnectorArrowDatabase(
            SQLiteConnector(tmp / f"{tag}.db")
        ),
    }
    orchestrators = {
        "sync": SyncPipelineOrchestrator,
        "async": AsyncPipelineOrchestrator,
    }
    collector = _Collect()
    logging.getLogger().addHandler(collector)
    for arrays in (True, False):
        kind = "ndarray" if arrays else "int control"
        for store_name, make_store in stores.items():
            for orch_name, orch in orchestrators.items():
                check = f"{kind} / {store_name} / {orch_name}"
                tag = f"{'arr' if arrays else 'int'}_{store_name}_{orch_name}"
                collector.messages.clear()
                try:
                    job = PipelineJob(name=tag, store=make_store(tag))
                    build(job, arrays)
                    job.run(orchestrator=orch())
                    status, observed = (check_arrays if arrays else check_ints)(job)
                except Exception as exc:  # noqa: BLE001 - every failure is reported
                    inner = exc
                    while isinstance(inner, BaseExceptionGroup):
                        inner = inner.exceptions[0]
                    status = "DEFECT"
                    observed = (
                        f"run raised {type(exc).__name__} "
                        f"({type(inner).__name__}: {' '.join(str(inner).split())[:200]})"
                    )
                logged = sorted(set(collector.messages))
                if logged:
                    observed += f"; logged: {logged[:2]}"
                report(status, check, observed)
    logging.getLogger().removeHandler(collector)


if __name__ == "__main__":
    main()
