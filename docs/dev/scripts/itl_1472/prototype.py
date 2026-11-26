"""ITL-1472 prototype: register an invocation by its input edges. Evidence only.

Run from the repository root (offline, the disk-backed SpiralDB fake of the tests):

    uv run python docs/dev/scripts/itl_1472/prototype.py

No library file is changed: ``install()`` replaces, in this process only,

- ``AbstractPipelineBase._record_invocation``: an invocation and a leaf are keyed by a
  USE KEY in place of the content hash. A leaf's use key is its content hash and an
  ordinal per Python object; a pod's is its content hash over its inputs' use keys.
- ``AbstractPipelineBase.compile``: afterwards, a slot whose label was numbered takes
  that label as its slot name (the ``bind`` key).
- ``__eq__`` / ``__hash__`` of the four job node classes: by object, not by content
  (two nodes of one identity are two keys of the DAG).
- the one-restriction refusals: off.

It is NOT the design's implementation: the restriction stays on the bound source here
(the design moves it to the edge), and node keys are ad-hoc strings. It answers what two
nodes sharing one identity and one store do today, with no other change. The numbered
headings are cited from the spec as ``[proto N]``.
"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))  # the SpiralDB fake lives in the test package

from orcapod import function_pod  # noqa: E402  # after the path insert
from orcapod.core.nodes import (  # noqa: E402
    FunctionJobNode,
    OperatorJobNode,
    SourceJobNode,
)
from orcapod.core.sources import DictSource  # noqa: E402
from orcapod.core.sources.spiraldb_table_source import SpiralDBTableSource  # noqa: E402
from orcapod.databases import DeltaTableDatabase  # noqa: E402
from orcapod.pipeline import PipelineJob  # noqa: E402
from orcapod.pipeline import job as job_module  # noqa: E402
from orcapod.pipeline import restrictions  # noqa: E402
from orcapod.pipeline.base import AbstractPipelineBase  # noqa: E402
from orcapod.pipeline.pod_invocation import PodInvocation  # noqa: E402
from orcapod.side_effects import (  # noqa: E402
    SideEffectJobNode,
    SideEffectPodConfig,
    side_effect_pod,
)  # noqa: E402
from orcapod.types import CacheMode  # noqa: E402
from tests.test_databases import fake_spiral_sdk  # noqa: E402

warnings.simplefilter("ignore")

PROJECT = "workshop-sample"
KEYS = pa.schema([pa.field("subject_id", pa.string()), pa.field("t", pa.uint64())])


# --------------------------------------------------------------------------------------
# The prototype
# --------------------------------------------------------------------------------------


class _Key:
    """Stands where compile asks an input for ``content_hash().to_string()``."""

    def __init__(self, key: str) -> None:
        self._key = key

    def content_hash(self) -> "_Key":
        return self

    def to_string(self) -> str:
        return self._key


class _UseKeys:
    """Use keys of streams while a pipeline is built (object identity is the handle)."""

    def __init__(self) -> None:
        self._ordinals: dict[str, list[int]] = {}
        self._kept: list[Any] = []  # keeps ids from being reused
        self._memo: dict[int, str] = {}

    def of(self, stream: Any) -> str:
        # Once per object: a walk that asked a shared upstream once per path would
        # be exponential in stacked diamonds (ITL-1111).
        known = self._memo.get(id(stream))
        if known is not None:
            return known
        self._kept.append(stream)
        producer = getattr(stream, "producer", None)
        content = stream.content_hash().to_string()
        if producer is None:
            seen = self._ordinals.setdefault(content, [])
            seen.append(id(stream))
            key = f"{content}#{len(seen) - 1}"
        else:
            key = self.of_invocation(content, producer, stream.upstreams)
        self._memo[id(stream)] = key
        return key

    def of_invocation(self, content: str, pod: Any, inputs: tuple[Any, ...]) -> str:
        keys = [self.of(upstream) for upstream in inputs]
        if isinstance(pod.argument_symmetry(inputs), frozenset):
            keys.sort()
        return f"{content}({','.join(keys)})"


def install() -> None:
    """Apply the prototype to this process."""

    def record(self: AbstractPipelineBase[Any], invocation: PodInvocation) -> None:
        uses: _UseKeys = self.__dict__.setdefault("_use_keys", _UseKeys())
        inputs = tuple(invocation.input_streams)
        key = uses.of_invocation(
            invocation.content_hash().to_string(), invocation.pod, inputs
        )
        input_keys = [uses.of(upstream) for upstream in inputs]
        shadow = type(invocation)(
            pod=invocation.pod, input_streams=inputs, label=invocation.label
        )
        shadow._input_streams = tuple(_Key(k) for k in input_keys)  # pyright: ignore[reportPrivateUsage]
        self._remember_invocation(key, shadow)  # pyright: ignore[reportPrivateUsage]
        self._hash_graph.add_node(key)  # pyright: ignore[reportPrivateUsage]
        for upstream, upstream_key in zip(inputs, input_keys, strict=True):
            self._hash_graph.add_edge(upstream_key, key)  # pyright: ignore[reportPrivateUsage]
            self._source_streams.setdefault(upstream_key, upstream)  # pyright: ignore[reportPrivateUsage]

    original_compile = AbstractPipelineBase.compile

    def compile_(self: AbstractPipelineBase[Any]) -> None:
        original_compile(self)
        for node in self._persistent_node_map.values():  # pyright: ignore[reportPrivateUsage]
            if isinstance(node, SourceJobNode) and node.label != node.name:
                node._name = node.label  # pyright: ignore[reportPrivateUsage]

    AbstractPipelineBase._record_invocation = record  # pyright: ignore[reportPrivateUsage]
    AbstractPipelineBase.compile = compile_
    AbstractPipelineBase._held_streams = lambda self: []  # pyright: ignore[reportPrivateUsage]
    import orcapod.pipeline.base as base_module

    base_module._refuse_sources_sharing_a_slot_name = lambda *a, **k: None  # pyright: ignore[reportPrivateUsage]
    restrictions.require_one_restriction = lambda *a, **k: None
    job_module.refuse_restriction_conflicts = lambda *a, **k: None
    restrictions.refuse_restricted_operator_caches = lambda *a, **k: None
    restrictions.refuse_restricted_operator_cache = lambda *a, **k: None
    import orcapod.core.nodes.operator_node as operator_node_module

    for name in ("refuse_restricted_operator_cache", "refuse_restriction_conflicts"):
        if hasattr(operator_node_module, name):
            setattr(operator_node_module, name, lambda *a, **k: None)
    for cls in (FunctionJobNode, OperatorJobNode, SourceJobNode, SideEffectJobNode):
        cls.__eq__ = lambda self, other: self is other  # pyright: ignore[reportAttributeAccessIssue]
        cls.__hash__ = lambda self: id(self)  # pyright: ignore[reportAttributeAccessIssue]


# --------------------------------------------------------------------------------------
# The probes
# --------------------------------------------------------------------------------------

CALLS: list[int] = []
DELIVERED: list[int] = []


@function_pod(output_keys="doubled")
def double(t: int | None, amplitude: float | None) -> float | None:
    """Return twice ``amplitude``; record the call."""
    CALLS.append(t)
    return None if amplitude is None else 2 * amplitude


@side_effect_pod
def tell(ctx: Any, parity: int | None, amplitude: float | None) -> None:
    """Record a delivery."""
    DELIVERED.append(int(amplitude or 0))  # 1.5 is the row t=1, and so on


@side_effect_pod(config=SideEffectPodConfig(track_completion=False))
def tell_untracked(ctx: Any, parity: int | None, amplitude: float | None) -> None:
    """Record a delivery, with no invocation log."""
    DELIVERED.append(int(amplitude or 0))  # 1.5 is the row t=1, and so on


def heading(number: int, text: str) -> None:
    print(f"\n== [proto {number}] {text}")


def ts(node: Any) -> list[int]:
    return sorted(node.as_table().column("t").to_pylist())


def events() -> SpiralDBTableSource:
    return SpiralDBTableSource(PROJECT, "event", source_id="events")


def build(store: Path, record: Callable[[], None], name: str = "demo") -> PipelineJob:
    job = PipelineJob(name=name, database=DeltaTableDatabase(store))
    with job:
        record()
    return job


def names(job: PipelineJob) -> dict[str, str]:
    return {name: node.node_type for name, node in sorted(job.nodes.items())}


def run(job: PipelineJob, mode: str) -> Any:
    if mode == "sync":
        return job.run()
    return asyncio.run(job.run_async())


def probe_function_nodes(root: Path) -> None:
    table = events()
    m1, odd = table.where(subject_id="m1"), table.where(parity=1)

    heading(1, "f over two restrictions: two slots, two nodes, one identity, one store")
    reference = build(root / "ref", lambda: double.pod(table, label="double"))
    ref_node = reference.nodes["double"]
    for mode in ("sync", "async"):
        CALLS.clear()
        job = build(
            root / f"s1-{mode}", lambda: (double.pod(m1), double.pod(odd)), "demo"
        )
        print(f"  {mode}: job.nodes = {names(job)}")
        f1, f2 = job.nodes["double_1"], job.nodes["double_2"]
        print(
            "    content hash equal:",
            f1.content_hash() == f2.content_hash() == ref_node.content_hash(),
            "| pipeline hash equal:",
            f1.pipeline_hash() == f2.pipeline_hash() == ref_node.pipeline_hash(),
            "| stored path equal:",
            f1.node_uri == f2.node_uri == ref_node.node_uri,
        )
        run(job, mode)
        print(f"    rows: double_1 t={ts(f1)}, double_2 t={ts(f2)}")
        print(
            f"    function calls: {sorted(CALLS)} ({len(CALLS)} calls, 4 distinct rows)"
        )
        stored = f1.as_source().as_table().column("t").to_pylist()
        print(f"    double_1.as_source() serves t={sorted(stored)}")
        print(
            "    pipeline table rows:",
            f1.get_all_records().num_rows,
            "| as seen from double_2:",
            f2.get_all_records().num_rows,
        )
        CALLS.clear()
        again = build(
            root / f"s1-{mode}", lambda: (double.pod(m1), double.pod(odd)), "demo"
        )
        run(again, mode)
        print(f"    a new job on the same store: {len(CALLS)} calls")

    heading(2, "state-driven run(): one node runs, its twin stays empty")
    CALLS.clear()
    job = build(root / "s2", lambda: (double.pod(m1), double.pod(odd)))
    f1, f2 = job.nodes["double_1"], job.nodes["double_2"]
    f1.run()
    print(f"  after double_1.run(): double_1 t={ts(f1)}, double_2 t={ts(f2)}")
    print(f"  calls: {sorted(CALLS)}; double_2 stale: {f2.is_stale}")
    summary = job.run()
    held = sorted(n for n, s in summary.nodes.items() if getattr(s, "held", False))
    print(f"  then job.run(): held={held}; calls now {sorted(CALLS)}")
    summary = job.run(load_only=True)
    print(f"  load_only: double_1 t={ts(f1)}, double_2 t={ts(f2)}")

    heading(3, "registration order: names and rows in both orders")
    for order, pair in (("m1, odd", (m1, odd)), ("odd, m1", (odd, m1))):
        job = build(root / f"s3-{order[0]}", lambda p=pair: [double.pod(s) for s in p])
        job.run()
        slots = {
            name: restrictions.restriction_text(
                node.bound_source.condition, node.bound_source.row_limit
            )
            for name, node in sorted(job.nodes.items())
            if isinstance(node, SourceJobNode)
        }
        rows = {n: ts(job.nodes[n]) for n in ("double_1", "double_2")}
        print(f"  registered {order}: slots={slots} rows={rows}")


def probe_side_effects(root: Path) -> None:
    table = events()
    m1, odd = table.where(subject_id="m1"), table.where(parity=1)
    for number, pod, text in (
        (4, tell, "tracked side effect over two edges: once per entry"),
        (5, tell_untracked, "untracked side effect over two edges: once per crossing"),
    ):
        heading(number, text)
        for mode in ("sync", "async"):
            DELIVERED.clear()
            job = build(root / f"s{number}-{mode}", lambda p=pod: (p(m1), p(odd)))
            run(job, mode)
            print(
                f"  {mode}: nodes={sorted(names(job))} delivered t={sorted(DELIVERED)}"
            )


def probe_operators(root: Path) -> None:
    table = events()
    m1, odd = table.where(subject_id="m1"), table.where(parity=1)
    heading(6, "a cached operator over two restrictions, by cache mode")
    for cache_mode in (CacheMode.OFF, CacheMode.LOG, CacheMode.REPLAY):
        store = root / "s6"
        job = build(
            store,
            lambda: (m1.group_by("subject_id"), odd.group_by("subject_id")),
            "ops",
        )
        ops = {n: o for n, o in sorted(job.nodes.items()) if o.node_type == "operator"}
        views = job._database_views(job._database, job._result_database)  # pyright: ignore[reportPrivateUsage]
        for node in ops.values():
            node.attach_databases(pipeline_database=views[0], cache_mode=cache_mode)
        job.run()
        shown = {
            name: sorted(
                zip(
                    node.as_table().column("subject_id").to_pylist(),
                    node.as_table().column("amplitude").to_pylist(),
                    strict=True,
                )
            )
            for name, node in ops.items()
        }
        print(f"  {cache_mode.name}:")
        for name, rows in shown.items():
            print(f"    {name}: {rows}")


def probe_twins_and_save(root: Path) -> None:
    heading(7, "two equal DictSource objects: two slots, two nodes")
    rows = [{"t": 1, "amplitude": 1.5}, {"t": 2, "amplitude": 2.5}]
    a = DictSource(rows, tag_columns=["t"])
    b = DictSource(rows, tag_columns=["t"])
    job = build(root / "s7", lambda: (double.pod(a), double.pod(b), double.pod(a)))
    print(f"  f(a), f(b), f(a): job.nodes = {sorted(names(job))}")

    heading(8, "save and load with main's format")
    table = events()
    m1, odd = table.where(subject_id="m1"), table.where(parity=1)
    job = build(root / "s8", lambda: (double.pod(m1), double.pod(odd)))
    job.run()
    path = root / "job.json"
    try:
        job.save(path)
    except Exception as exc:  # noqa: BLE001  # the probe reports what happens
        print(f"  save: {type(exc).__name__}: {str(exc)[:150]}")
        return
    saved = json.loads(path.read_text())
    print(f"  saved nodes: {len(saved['pipeline']['nodes'])}")
    print(f"  saved sources: {sorted(saved['bindings']['sources'])}")
    for name, config in sorted(saved["bindings"]["sources"].items()):
        print(f"    {name}: where={config.get('where')}")
    try:
        loaded = PipelineJob.load(path, database=DeltaTableDatabase(root / "s8"))
        print(f"  loaded job.nodes = {names(loaded)}")
        loaded.run(load_only=True)
        for name in ("double_1", "double_2"):
            print(f"    {name}: t={ts(loaded.nodes[name])}")
    except Exception as exc:  # noqa: BLE001  # the probe reports what happens
        print(f"  load: {type(exc).__name__}: {str(exc)[:200]}")


def main() -> None:
    install()
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        with fake_spiral_sdk.install(root / "spiral"):
            project = fake_spiral_sdk.ExternalProject(PROJECT)
            project.create_table("default.event", key_schema=KEYS).write(
                pa.table(
                    {
                        "subject_id": ["m1", "m1", "m1", "m2", "m2"],
                        "t": pa.array([1, 2, 3, 4, 5], pa.uint64()),
                        "parity": pa.array([1, 0, 1, 0, 1], pa.int64()),
                        "amplitude": pa.array([1.5, 2.5, 3.5, 4.5, 5.5], pa.float64()),
                    }
                )
            )
            probes = (
                probe_function_nodes,
                probe_side_effects,
                probe_operators,
                probe_twins_and_save,
            )
            for probe in probes:
                try:
                    probe(root)
                except Exception as exc:  # noqa: BLE001  # the probe reports what happens
                    import traceback

                    traceback.print_exc(limit=-6)
                    print(f"  PROBE FAILED: {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    main()
