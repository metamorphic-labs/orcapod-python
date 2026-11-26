"""End-to-end evaluation of the SpiralDB store as of ITL-1498 (no lease, one process).

One job (a function pod, a join, a filter, a group by, a second function, a side effect)
through every scenario of ITL-1446's goals, on Delta and on a SpiralDB store (the fake, or
live), compared value for value; every SpiralDB call is counted per table.

    uv run python docs/dev/scripts/itl_1446_store_evaluation.py fake
    SPIRAL_PROJECT_ID=... uv run python docs/dev/scripts/itl_1446_store_evaluation.py live

Live runs need a sign-in spiral-sdk finds (``SPIRAL_CLIENT_ID``, ``SPIRAL_PRIVATE_KEY``) and
write fixed-name tables of a fresh job name into ``ORCAPOD_SPIRAL_CONFORMANCE_DATASET``
(default ``orcapod_conformance``). Prints a JSON report; numbers measured live are not
kept in the repository.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import tempfile
import time
import uuid
import warnings
from collections import Counter, defaultdict
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    SpiralDBConnector,
)
from orcapod.side_effects import SideEffectPod
from orcapod.sources import ArrowTableSource

warnings.filterwarnings("ignore")
REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))

CALLS: Counter[str] = Counter()
SENT: list[float] = []


def make_rate(version: str) -> Any:
    @function_pod(output_keys="rate_hz", version=version)
    def firing_rate(n_spikes: int, duration_s: float) -> float:
        CALLS["firing_rate"] += 1
        return n_spikes / duration_s

    return firing_rate


@function_pod(output_keys="mean_rate_hz")
def mean_rate(rate_hz: list[float]) -> float:
    CALLS["mean_rate"] += 1
    return sum(rate_hz) / len(rate_hz)


def notify(ctx: Any, mean_rate_hz: float) -> None:
    SENT.append(mean_rate_hz)


def units_table(extra: bool = False) -> pa.Table:
    rows = [
        (s, u, 100 + 7 * i, 10.0 + (i % 3))
        for i, (s, u) in enumerate(
            (s, u) for s in ("m1", "m2", "m3", "m4") for u in ("u1", "u2", "u3")
        )
    ]
    if extra:
        rows += [("m1", "u4", 999, 9.0), ("m2", "u4", 555, 11.0)]
    schema = pa.schema(
        [
            pa.field("subject_id", pa.large_string(), nullable=False),
            pa.field("unit", pa.large_string(), nullable=False),
            pa.field("n_spikes", pa.int64(), nullable=False),
            pa.field("duration_s", pa.float64(), nullable=False),
        ]
    )
    return pa.Table.from_pylist(
        [dict(zip(schema.names, r)) for r in rows], schema=schema
    )


SUBJECTS = pa.Table.from_pylist(
    [
        {"subject_id": s, "genotype": g}
        for s, g in (("m1", "ko"), ("m2", "ko"), ("m3", "wt"), ("m4", "ko"))
    ],
    schema=pa.schema(
        [
            pa.field("subject_id", pa.large_string(), nullable=False),
            pa.field("genotype", pa.large_string(), nullable=False),
        ]
    ),
)


def sources(extra: bool = False) -> dict[str, Any]:
    return {
        "units": ArrowTableSource(
            units_table(extra),
            tag_columns=["subject_id", "unit"],
            tag_only_columns=True,
            source_id="units",
            record_id_columns=["subject_id", "unit"],
        ),
        "subjects": ArrowTableSource(
            SUBJECTS,
            tag_columns=["subject_id"],
            source_id="subjects",
            record_id_columns=["subject_id"],
        ),
    }


def build(job: PipelineJob, *, extra: bool = False, version: str = "v1.0") -> None:
    given = sources(extra)
    units, subjects = given["units"], given["subjects"]
    with job:
        rates = make_rate(version).pod(units, label="rates")
        kept = rates.join(subjects).filter(genotype="ko")
        grouped = kept.group_by(["subject_id"]).select_data_columns(["rate_hz"])
        means = mean_rate.pod(grouped, label="means")
        SideEffectPod(notify)(means, label="notify")


# ----------------------------------------------------------------- what is compared


def node_rows(job: PipelineJob) -> dict[str, pa.Table]:
    columns = {"system_tags": True, "source": True, "content_hash": True}
    return {n: job.nodes[n].as_table(columns=columns) for n in sorted(job.nodes)}


def node_hashes(job: PipelineJob) -> dict[str, tuple[str, str]]:
    out = {}
    for name in sorted(job.nodes):
        node = job.nodes[name]
        out[name] = (node.content_hash().to_string(), node.pipeline_hash().to_string())
    return out


def stored(db: Any) -> dict[tuple[str, ...], pa.Table]:
    out = {}
    for path in db.list_table_paths():
        table = db.get_all_records(path, record_id_column="__record_id")
        if table is not None:
            out[path] = table.sort_by("__record_id")
    return out


def as_set(table: pa.Table) -> list[str]:
    return sorted(json.dumps(r, sort_keys=True, default=str) for r in table.to_pylist())


def compare_tables(a: pa.Table, b: pa.Table, *, ordered: bool) -> str | None:
    if sorted(a.column_names) != sorted(b.column_names):
        return f"columns {sorted(a.column_names)} vs {sorted(b.column_names)}"
    b = b.select(a.column_names)
    if ordered:
        return None if a.to_pylist() == b.to_pylist() else "rows differ (ordered)"
    return None if as_set(a) == as_set(b) else "rows differ (as sets)"


GENERATED = {"__pod_ts", "__recorded_at", "executed_at", "__run_id", "__executor_info"}


def compare_stores(
    delta: dict[tuple[str, ...], pa.Table], spiral: dict[tuple[str, ...], pa.Table]
) -> list[str]:
    problems = []
    if set(delta) != set(spiral):
        problems.append(
            f"paths only in Delta {sorted(set(delta) - set(spiral))}, only in the store "
            f"{sorted(set(spiral) - set(delta))}"
        )
    for path in sorted(set(delta) & set(spiral)):
        a, b = delta[path], spiral[path]
        ids_a = a["__record_id"].to_pylist()
        ids_b = b["__record_id"].to_pylist()
        if ids_a != ids_b:
            problems.append(f"{'/'.join(path)}: record ids differ")
            continue
        for name in a.column_names:
            if name in GENERATED or name not in b.column_names:
                continue
            if a[name].to_pylist() != b[name].to_pylist():
                problems.append(f"{'/'.join(path)}: column {name!r} differs")
        extra = sorted(set(a.column_names) ^ set(b.column_names))
        if extra:
            problems.append(f"{'/'.join(path)}: columns in one store only {extra}")
    return problems


# ------------------------------------------------------------------- SpiralDB calls


class Calls:
    """Counts the SDK calls of a store-mode connector, per table name."""

    def __init__(self, connector: SpiralDBConnector) -> None:
        self.counts: Counter[tuple[str, str]] = Counter()
        store = connector._store  # pyright: ignore[reportPrivateUsage]
        assert store is not None
        self.store = store
        open_table, list_tables = store._open_table, store._list_tables  # pyright: ignore[reportPrivateUsage]
        names_by_id: dict[str, str] = {}

        def counted_open(name: str) -> Any:
            self.counts[("open by name", name)] += 1
            table = open_table(name)
            if table is None:
                return None
            names_by_id[str(table.resource_id)] = name
            return _Table(table, name, self.counts)

        def counted_list() -> dict[str, str]:
            self.counts[("list dataset", "*")] += 1
            listed = list_tables()
            names_by_id.update(listed)
            return listed

        client = store._client  # pyright: ignore[reportPrivateUsage]

        store._open_table = counted_open  # pyright: ignore[reportPrivateUsage]
        store._list_tables = counted_list  # pyright: ignore[reportPrivateUsage]
        store._client = _Client(client, self.counts, names_by_id)  # pyright: ignore[reportPrivateUsage]

    def by_path(self) -> dict[str, dict[str, int]]:
        out: dict[str, dict[str, int]] = defaultdict(dict)
        paths = {n: "/".join(p) for n, p in self.store._paths.items()}  # pyright: ignore[reportPrivateUsage]
        for (what, name), n in sorted(self.counts.items()):
            out[paths.get(name, name)][what] = n
        return dict(out)

    def total(self) -> int:
        return sum(self.counts.values())


class _Client:
    def __init__(
        self, client: Any, counts: Counter[tuple[str, str]], names_by_id: dict[str, str]
    ) -> None:
        self._client, self._c, self._names = client, counts, names_by_id

    def project(self, id: str) -> Any:
        return _Project(self._client.project(id), self._c)

    def keyed_table_by_id(self, table_id: str, **kw: Any) -> Any:
        name = self._names.get(table_id, table_id)
        self._c[("open by id", name)] += 1
        return _Table(self._client.keyed_table_by_id(table_id, **kw), name, self._c)


class _Project:
    def __init__(self, project: Any, counts: Counter[tuple[str, str]]) -> None:
        self._p, self._c = project, counts

    def create_keyed_table(self, dataset: str, name: str, key: Any, **kw: Any) -> Any:
        self._c[("create", name)] += 1
        return _Table(
            self._p.create_keyed_table(dataset, name, key, **kw), name, self._c
        )


class _Table:
    def __init__(self, table: Any, name: str, counts: Counter[tuple[str, str]]) -> None:
        self._t, self._n, self._c = table, name, counts

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._t, attr)

    def get_metadata(self) -> Any:
        self._c[("metadata read", self._n)] += 1
        return self._t.get_metadata()

    def set_metadata(self, metadata: Any) -> None:
        self._c[("metadata write", self._n)] += 1
        self._t.set_metadata(metadata)

    def append(self, data: Any) -> Any:
        self._c[("commit", self._n)] += 1
        return _Table(self._t.append(data), self._n, self._c)

    def scan(self, columns: Any = None, **kw: Any) -> Any:
        narrowed = [
            what
            for what in ("key_ranges", "filter", "limit")
            if kw.get(what) is not None
        ]
        if columns is None and not narrowed:
            kind = "whole-table scan"
        else:
            kind = "scan of " + (
                "all columns" if columns is None else f"{list(columns)}"
            )
            if narrowed:
                kind += " by " + ", ".join(narrowed)
        self._c[(kind, self._n)] += 1
        return self._t.scan(columns, **kw)


# ------------------------------------------------------------------------ scenarios


def open_backend(kind: str, root: Path, resources: ExitStack, run: str) -> Any:
    """Return (database, result database, calls) of one backend."""
    if kind == "control":
        db = DeltaTableDatabase(root / "control")
        return db, db.at("results"), None
    if kind == "delta":
        db = DeltaTableDatabase(root / "delta")
        return db, db.at("results"), None
    if kind == "fake":
        from tests.test_databases import fake_spiral_sdk as fake

        resources.enter_context(fake.install(root / "spiral"))
        project, dataset, overrides = "p", "d", None
    else:
        project = os.environ["SPIRAL_PROJECT_ID"]
        dataset = os.environ.get(
            "ORCAPOD_SPIRAL_CONFORMANCE_DATASET", "orcapod_conformance"
        )
        overrides = {
            "server.url": os.environ.get(
                "SPIRAL_SERVER_URL", "https://api.spiraldb.dev"
            )
        }
    connector = SpiralDBConnector(project, dataset, overrides, _store=True)
    calls = Calls(connector)
    db = ConnectorArrowDatabase(connector)
    return db, db.at("results"), calls


def evaluate(kind: str) -> dict[str, Any]:
    run = uuid.uuid4().hex[:8]
    report: dict[str, Any] = {"backend": kind, "steps": {}, "failures": []}
    with tempfile.TemporaryDirectory() as tmp, ExitStack() as resources:
        root = Path(tmp)
        backends = {
            "delta": open_backend("delta", root, resources, run),
            "control": open_backend("control", root, resources, run),
            "spiral": open_backend(kind, root, resources, run),
        }
        name = f"eval_{run}"
        saved = root / "job.json"
        jobs: dict[str, dict[str, PipelineJob]] = defaultdict(dict)

        def step(label: str, fn: Any) -> None:
            for backend, (db, rdb, calls) in backends.items():
                CALLS.clear()
                SENT.clear()
                before = calls.total() if calls else 0
                kinds_before = (
                    Counter(k for (k, _), n in calls.counts.items() for _ in range(n))
                    if calls
                    else Counter()
                )
                start = time.perf_counter()
                try:
                    job = fn(backend, db, rdb)
                    error = None
                except Exception as exc:  # noqa: BLE001  # every failure is reported
                    job, error = None, f"{type(exc).__name__}: {exc}"
                took = time.perf_counter() - start
                entry: dict[str, Any] = {
                    "seconds": round(took, 3),
                    "function_calls": dict(CALLS),
                    "side_effects_sent": len(SENT),
                }
                if calls is not None:
                    entry["spiral_calls"] = calls.total() - before
                    kinds = Counter(
                        k for (k, _), n in calls.counts.items() for _ in range(n)
                    )
                    entry["spiral_calls_by_kind"] = dict(kinds - kinds_before)
                if error:
                    entry["error"] = error
                    report["failures"].append(f"{label} on {backend}: {error}")
                report["steps"].setdefault(label, {})[backend] = entry
                if job is not None:
                    jobs[label][backend] = job
            pair = jobs.get(label, {})
            try:
                compare(label, pair)
            except Exception as exc:  # noqa: BLE001  # every failure is reported
                report["failures"].append(
                    f"{label}: comparing: {type(exc).__name__}: {exc}"
                )

        def differences(
            label: str, d: PipelineJob, other: PipelineJob, db: Any
        ) -> set[str]:
            found: set[str] = set()
            if node_hashes(d) != node_hashes(other):
                found.add("node hashes differ")
            ordered = "async" not in label
            rd, ro = node_rows(d), node_rows(other)
            for node in rd:
                problem = compare_tables(rd[node], ro[node], ordered=ordered)
                if problem:
                    found.add(f"node {node}: {problem}")
            for problem in compare_stores(stored(backends["delta"][0]), stored(db)):
                found.add(f"store: {problem}")
            return found

        def compare(label: str, pair: dict[str, PipelineJob]) -> None:
            if len(pair) != 3:
                return
            # Two Delta stores differ in generated values (result ids and the tokens
            # holding them, run ids, timestamps): only what the control does not show
            # is a difference of the SpiralDB store.
            control = differences(
                label, pair["delta"], pair["control"], backends["control"][0]
            )
            spiral = differences(
                label, pair["delta"], pair["spiral"], backends["spiral"][0]
            )
            report.setdefault("generated_differences", {})[label] = sorted(control)
            for problem in sorted(spiral - control):
                report["failures"].append(f"{label}: {problem}")

        def first(backend: str, db: Any, rdb: Any) -> PipelineJob:
            job = PipelineJob(name=name, database=db)
            build(job)
            job.run()
            if backend == "delta":
                job.save(saved)
            return job

        def again(backend: str, db: Any, rdb: Any, **options: Any) -> PipelineJob:
            job = PipelineJob(name=name, database=db)
            build(job, **options)
            job.run()
            return job

        step("first run", first)
        step("unchanged rerun", again)
        step("added rows", lambda b, db, rdb: again(b, db, rdb, extra=True))
        step(
            "changed function version",
            lambda b, db, rdb: again(b, db, rdb, extra=True, version="v2.0"),
        )

        def run_async(backend: str, db: Any, rdb: Any) -> PipelineJob:
            job = PipelineJob(name=name + "_async", database=db)
            build(job)
            asyncio.run(job.run_async())
            return job

        step("run_async (first run, compared as sets)", run_async)

        def load_only(backend: str, db: Any, rdb: Any) -> PipelineJob:
            job = PipelineJob.load(saved, database=db)
            job.bind(sources=sources())
            job.run(load_only=True)
            return job

        step("save, load, run(load_only=True)", load_only)

        def cleared(backend: str, db: Any, rdb: Any) -> PipelineJob:
            job = PipelineJob(name=name, database=db)
            build(job)
            job.run()
            for node in job.nodes.values():
                clear = getattr(node, "clear_cache", None)
                if clear is not None:
                    clear()
            job.run()
            return job

        step("clear_cache, then run", cleared)

        def separate(backend: str, db: Any, rdb: Any) -> PipelineJob:
            job = PipelineJob(name=name + "_sep", database=db, result_database=rdb)
            build(job)
            job.run()
            return job

        step("separate result database", separate)

        traces: dict[str, str] = {}
        for backend in ("delta", "control", "spiral"):
            job = jobs.get("first run", {}).get(backend)
            if job is None:
                continue
            try:
                trace = job.nodes["means"].trace({"subject_id": "m1"}, "mean_rate_hz")
                # Times and run ids are generated: masked.
                text = re.sub(
                    r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC", "<time>", str(trace)
                )
                traces[backend] = re.sub(r"run [0-9a-f]+", "run <id>", text)
            except Exception as exc:  # noqa: BLE001
                report["failures"].append(
                    f"trace on {backend}: {type(exc).__name__}: {exc}"
                )
        report["traces"] = traces
        if (
            len(traces) == 3
            and traces["delta"] == traces["control"] != traces["spiral"]
        ):
            report["failures"].append("trace: the store's trace differs from Delta's")
        report["trace_equal"] = len(traces) == 3 and traces["delta"] == traces["spiral"]
        report["trace_equal_between_two_deltas"] = (
            len(traces) == 3 and traces["delta"] == traces["control"]
        )

        calls = backends["spiral"][2]
        if calls is not None:
            report["spiral_calls_by_table"] = calls.by_path()
    return report


if __name__ == "__main__":
    kind = sys.argv[1] if len(sys.argv) > 1 else "fake"
    print(json.dumps(evaluate(kind), indent=1, default=str))
