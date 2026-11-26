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
from typing import Any, cast

import pyarrow as pa

from orcapod.utils.arrow_utils import normalize_to_large_types

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
    """The firing-rate function pod, at a version."""

    @function_pod(output_keys="rate_hz", version=version)
    def firing_rate(n_spikes: int, duration_s: float) -> float:
        CALLS["firing_rate"] += 1
        return n_spikes / duration_s

    return firing_rate


@function_pod(output_keys="mean_rate_hz")
def mean_rate(rate_hz: list[float]) -> float:
    """The mean of a subject's rates."""
    CALLS["mean_rate"] += 1
    return sum(rate_hz) / len(rate_hz)


def notify(ctx: Any, mean_rate_hz: float) -> None:
    """The side effect: records each delivered payload in ``SENT``."""
    SENT.append(mean_rate_hz)


def units_table(extra: bool = False) -> pa.Table:
    """The units source's rows; ``extra`` adds two."""
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
    """The job's two sources, by name."""
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
    """Build the evaluated pipeline in ``job``."""
    given = sources(extra)
    units, subjects = given["units"], given["subjects"]
    with job:
        rates = make_rate(version).pod(units, label="rates")
        kept = rates.join(subjects).filter(genotype="ko")
        grouped = kept.group_by(["subject_id"]).select_data_columns(["rate_hz"])
        means = mean_rate.pod(grouped, label="means")
        SideEffectPod(notify)(means, label="notify")


# ----------------------------------------------------------------- what is compared
#
# Two runs on Delta differ only in generated values: the run ids and times (the
# ``GENERATED`` columns, masked), and the function result ids, random per result, which
# also appear in the result tables' ``__record_id``, the pipeline tables' ``__data_id``
# and inside the source-info tokens. Result ids are matched by the result's input hash
# (``__input_data_hash``) and replaced by the reference run's, at every place they
# appear (``IdMap``). Everything else is compared exactly: the schema (names, large
# types, nullability) apart from the rows, and the rows, in order where the run's
# order is deterministic (sync), as sets otherwise (async). No difference is ever
# dropped because the control shows one of the same kind: the control (a second Delta
# run) is itself compared, and a difference it shows means the normalization is
# incomplete, which fails the evaluation too.


GENERATED = {
    "__pod_ts",
    "__recorded_at",
    "executed_at",
    "__run_id",
    "__pipeline_run_id",
    "pipeline_run_id",
    "__executor_info",
}
"""Columns whose values a run generates (times, run ids): masked before comparing."""


def node_rows(job: PipelineJob) -> dict[str, pa.Table]:
    """Every node's rows, with system tags, source info and content hashes."""
    columns = {"system_tags": True, "source": True, "content_hash": True}
    return {n: job.nodes[n].as_table(columns=columns) for n in sorted(job.nodes)}


def node_hashes(job: PipelineJob) -> dict[str, tuple[str, str]]:
    """Every node's content hash and pipeline hash."""
    out = {}
    for name in sorted(job.nodes):
        node = job.nodes[name]
        out[name] = (node.content_hash().to_string(), node.pipeline_hash().to_string())
    return out


def stored(db: Any, job_name: str) -> dict[tuple[str, ...], pa.Table]:
    """Every stored table this evaluation wrote, by path.

    Only paths under this evaluation's jobs (``job_name`` and its suffixed forms) are
    kept, and the separate result database (``results``): a live dataset also holds
    the tables of earlier evaluations. Result tables are not named by job, so in a
    live dataset ``results`` can still hold earlier evaluations' results.
    """
    out = {}
    for path in db.list_table_paths():
        if not path or not (path[0].startswith(job_name) or path[0] == "results"):
            continue
        table = db.get_all_records(path, record_id_column="__record_id")
        if table is not None:
            out[path] = table.sort_by("__record_id")
    return out


def _binary(table: pa.Table, column: str) -> list[bytes]:
    """A binary column's values (never null in a result table)."""
    return cast("list[bytes]", table[column].to_pylist())


class IdMap:
    """Replaces another run's function result ids by the reference run's.

    Built from the result tables of the two stores: a result is the same result when
    its input hash is the same.
    """

    def __init__(
        self,
        reference: dict[tuple[str, ...], pa.Table],
        other: dict[tuple[str, ...], pa.Table],
    ) -> None:
        self.ids: dict[bytes, bytes] = {}
        for path in set(reference) & set(other):
            if not path[-1].startswith("rdb_"):
                continue  # not a function result table
            ref, oth = reference[path], other[path]
            by_input: dict[bytes, bytes] = dict(
                zip(_binary(ref, "__input_data_hash"), _binary(ref, "__record_id"))
            )
            for key, rid in zip(
                _binary(oth, "__input_data_hash"), _binary(oth, "__record_id")
            ):
                if key in by_input:
                    self.ids[rid] = by_input[key]
        self.text = {k.hex(): v.hex() for k, v in self.ids.items()}
        self._pattern = (
            re.compile("|".join(sorted(self.text, key=len, reverse=True)))
            if self.text
            else None
        )

    def value(self, value: Any) -> Any:
        """A value with every other-run id replaced, at any depth."""
        if isinstance(value, bytes):
            return self.ids.get(value, value)
        if isinstance(value, str) and self._pattern is not None:
            return self._pattern.sub(lambda m: self.text[m.group(0)], value)
        if isinstance(value, list):
            return [self.value(v) for v in cast("list[Any]", value)]
        if isinstance(value, dict):
            return {k: self.value(v) for k, v in cast("dict[str, Any]", value).items()}
        return value


def normalized_rows(table: pa.Table, ids: IdMap | None) -> list[dict[str, Any]]:
    """The rows with generated columns masked and other-run ids replaced."""
    rows = table.to_pylist()
    out = []
    for row in rows:
        clean = {
            name: (None if name in GENERATED else value) for name, value in row.items()
        }
        out.append(ids.value(clean) if ids is not None else clean)
    return out


def schema_by_name(table: pa.Table) -> dict[str, tuple[str, bool]]:
    """A table's columns by name: large-normalized type and nullability."""
    return {
        f.name: (str(normalize_to_large_types(f.type, keep_null=True)), f.nullable)
        for f in table.schema
    }


def as_set(rows: list[dict[str, Any]]) -> list[str]:
    """Rows as a sorted list of JSON texts: compared as a multiset."""
    return sorted(json.dumps(r, sort_keys=True, default=repr) for r in rows)


def compare_tables(
    a: pa.Table, b: pa.Table, *, ordered: bool, ids: IdMap | None = None
) -> list[str]:
    """Differences between a reference table ``a`` and ``b``.

    Compares the schemas by name (large types and nullability), then the rows, with
    generated columns masked and ``b``'s result ids replaced by ``a``'s (``ids``):
    in order when ``ordered``, as a multiset otherwise.
    """
    problems = []
    sa, sb = schema_by_name(a), schema_by_name(b)
    if sa != sb:
        problems.append(
            "schema differs: "
            + ", ".join(
                f"{n}: {sa.get(n)} vs {sb.get(n)}"
                for n in sorted(set(sa) | set(sb))
                if sa.get(n) != sb.get(n)
            )
        )
    common = [n for n in a.column_names if n in b.column_names]
    ra = normalized_rows(a.select(common), None)
    rb = normalized_rows(b.select(common), ids)
    if ordered:
        if ra != rb:
            problems.append("rows differ (ordered)")
    elif as_set(ra) != as_set(rb):
        problems.append("rows differ (as sets)")
    return problems


def compare_stores(
    delta: dict[tuple[str, ...], pa.Table],
    other: dict[tuple[str, ...], pa.Table],
    ids: IdMap,
) -> list[str]:
    """Differences between the reference Delta store and another: the paths, then each
    table's schema and rows (as sets: a store keeps no row order)."""
    problems = []
    if set(delta) != set(other):
        problems.append(
            f"paths only in Delta {sorted(set(delta) - set(other))}, only in the other "
            f"store {sorted(set(other) - set(delta))}"
        )
    for path in sorted(set(delta) & set(other)):
        for problem in compare_tables(delta[path], other[path], ordered=False, ids=ids):
            problems.append(f"{'/'.join(path)}: {problem}")
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
            """Open a table by name, counted."""
            self.counts[("open by name", name)] += 1
            table = open_table(name)
            if table is None:
                return None
            names_by_id[str(table.resource_id)] = name
            return _Table(table, name, self.counts)

        def counted_list() -> dict[str, str]:
            """List the dataset, counted."""
            self.counts[("list dataset", "*")] += 1
            listed = list_tables()
            names_by_id.update(listed)
            return listed

        client = store._client  # pyright: ignore[reportPrivateUsage]

        store._open_table = counted_open  # pyright: ignore[reportPrivateUsage]
        store._list_tables = counted_list  # pyright: ignore[reportPrivateUsage]
        store._client = _Client(client, self.counts, names_by_id)  # pyright: ignore[reportPrivateUsage]

    def by_path(self) -> dict[str, dict[str, int]]:
        """The counts, per table path and kind of call."""
        out: dict[str, dict[str, int]] = defaultdict(dict)
        paths = {n: "/".join(p) for n, p in self.store._paths.items()}  # pyright: ignore[reportPrivateUsage]
        for (what, name), n in sorted(self.counts.items()):
            out[paths.get(name, name)][what] = n
        return dict(out)

    def total(self) -> int:
        """The number of calls counted."""
        return sum(self.counts.values())


class _Client:
    """A client whose table openings are counted."""

    def __init__(
        self, client: Any, counts: Counter[tuple[str, str]], names_by_id: dict[str, str]
    ) -> None:
        self._client, self._c, self._names = client, counts, names_by_id

    def project(self, id: str) -> Any:
        """A counting project."""
        return _Project(self._client.project(id), self._c)

    def keyed_table_by_id(self, table_id: str, **kw: Any) -> Any:
        """Open a table by id, counted."""
        name = self._names.get(table_id, table_id)
        self._c[("open by id", name)] += 1
        return _Table(self._client.keyed_table_by_id(table_id, **kw), name, self._c)


class _Project:
    """A project whose creates are counted."""

    def __init__(self, project: Any, counts: Counter[tuple[str, str]]) -> None:
        self._p, self._c = project, counts

    def create_keyed_table(self, dataset: str, name: str, key: Any, **kw: Any) -> Any:
        """Create a table, counted."""
        self._c[("create", name)] += 1
        return _Table(
            self._p.create_keyed_table(dataset, name, key, **kw), name, self._c
        )


class _Table:
    """A keyed table whose calls are counted."""

    def __init__(self, table: Any, name: str, counts: Counter[tuple[str, str]]) -> None:
        self._t, self._n, self._c = table, name, counts

    def __getattr__(self, attr: str) -> Any:
        return getattr(self._t, attr)

    def get_metadata(self) -> Any:
        """Read the metadata, counted."""
        self._c[("metadata read", self._n)] += 1
        return self._t.get_metadata()

    def set_metadata(self, metadata: Any) -> None:
        """Write the metadata, counted."""
        self._c[("metadata write", self._n)] += 1
        self._t.set_metadata(metadata)

    def append(self, data: Any) -> Any:
        """Commit rows, counted."""
        self._c[("commit", self._n)] += 1
        return _Table(self._t.append(data), self._n, self._c)

    def scan(self, columns: Any = None, **kw: Any) -> Any:
        """Scan, counted by kind."""
        kind = "whole-table scan" if columns is None else f"scan of {list(columns)}"
        if "limit" in kw:
            kind += f" limit {kw['limit']}"
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
    """Run every scenario on Delta, a second Delta (the control) and the SpiralDB store
    of ``kind`` (``fake`` or ``live``); return the report."""
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
        payloads: dict[str, dict[str, list[float]]] = defaultdict(dict)

        def step(label: str, fn: Any) -> None:
            """Run one scenario on every backend, then compare."""
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
                payloads[label][backend] = list(SENT)
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
                compare_payloads(label, payloads[label])
            except Exception as exc:  # noqa: BLE001  # every failure is reported
                report["failures"].append(
                    f"{label}: comparing: {type(exc).__name__}: {exc}"
                )

        def differences(
            label: str, d: PipelineJob, other: PipelineJob, db: Any
        ) -> list[str]:
            """Every difference of ``other``'s job and store from Delta's, generated
            values normalized (see ``compare_tables``)."""
            found: list[str] = []
            if node_hashes(d) != node_hashes(other):
                found.append("node hashes differ")
            reference = stored(backends["delta"][0], name)
            theirs = stored(db, name)
            ids = IdMap(reference, theirs)
            ordered = "async" not in label
            rd, ro = node_rows(d), node_rows(other)
            for node in sorted(set(rd) | set(ro)):
                if node not in rd or node not in ro:
                    found.append(f"node {node}: in one job only")
                    continue
                for problem in compare_tables(
                    rd[node], ro[node], ordered=ordered, ids=ids
                ):
                    found.append(f"node {node}: {problem}")
            for problem in compare_stores(reference, theirs, ids):
                found.append(f"store: {problem}")
            return found

        def compare(label: str, pair: dict[str, PipelineJob]) -> None:
            """Compare the store, and the control, with Delta: every difference is a
            failure (the control's means the normalization missed a generated value)."""
            if len(pair) != 3:
                return
            for backend in ("control", "spiral"):
                for problem in differences(
                    label, pair["delta"], pair[backend], backends[backend][0]
                ):
                    report["failures"].append(f"{label}: {backend}: {problem}")

        def compare_payloads(label: str, sent: dict[str, list[float]]) -> None:
            """The side effect's delivered payloads equal Delta's: in order, except
            in the async scenario, whose delivery order is not defined."""
            reference = sent.get("delta")
            if reference is None:
                return
            for backend in ("control", "spiral"):
                got = sent.get(backend)
                if got is None:
                    continue
                if "async" in label:
                    same = sorted(got) == sorted(reference)
                else:
                    same = got == reference
                if not same:
                    report["failures"].append(
                        f"{label}: {backend}: side effect payloads {got} vs {reference}"
                    )

        def first(backend: str, db: Any, rdb: Any) -> PipelineJob:
            """The first run (Delta also saves the job)."""
            job = PipelineJob(name=name, database=db)
            build(job)
            job.run()
            if backend == "delta":
                job.save(saved)
            return job

        def again(backend: str, db: Any, rdb: Any, **options: Any) -> PipelineJob:
            """A new job of the same name, run."""
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
            """A new job, run asynchronously."""
            job = PipelineJob(name=name + "_async", database=db)
            build(job)
            asyncio.run(job.run_async())
            return job

        step("run_async (first run, compared as sets)", run_async)

        def load_only(backend: str, db: Any, rdb: Any) -> PipelineJob:
            """The saved job, loaded and run load-only."""
            job = PipelineJob.load(saved, database=db)
            job.bind(sources=sources())
            job.run(load_only=True)
            return job

        step("save, load, run(load_only=True)", load_only)

        def cleared(backend: str, db: Any, rdb: Any) -> PipelineJob:
            """A run, every node cleared, then another run."""
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
            """A job with a separate result database."""
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
