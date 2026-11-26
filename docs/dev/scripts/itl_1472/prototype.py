"""ITL-1472 evidence: a restriction held on the edge, prototyped on ``main``.

Run from the repository root (offline, the disk-backed SpiralDB fake of the tests):

    uv run python docs/dev/scripts/itl_1472/prototype.py

This replaces the twin-node prototype of the first round (superseded: see the git
history of this file). It is NOT the design's code: it changes ``main`` in this process
only, at three places (``install``), to show what the model of the design note does.

1. Recording: a restriction a source states at a pod's input is taken off the source
   and kept on the edge ``(upstream key, node key, position)``. Two different
   restrictions on one edge are refused there. The pipeline's leaf is the source
   without its restriction, so the graph is the one ``main`` builds from identity.
2. The sync orchestrator hands a node the rows of each input edge: for an edge out of
   a source node with a restriction, a pushed-down read of the bound source under it
   (one read per source node and distinct restriction in a run), and the source's
   full read only when a node takes an edge without a restriction; positions come
   from the node's ``upstreams``, so one node may feed two positions.
3. ``main``'s whole-pipeline rule (one restriction per table) is switched off.

The async path, ``bind``, save and load, and the run summary are not prototyped.
"""

from __future__ import annotations

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
from orcapod.core.nodes.source_node import SourceJobNode  # noqa: E402
from orcapod.core.operators import MergeJoin, SemiJoin  # noqa: E402
from orcapod.core.operators.static_output_pod import at_edge  # noqa: E402
from orcapod.core.sources.spiraldb_table_source import SpiralDBTableSource  # noqa: E402
from orcapod.databases import DeltaTableDatabase  # noqa: E402
from orcapod.pipeline import PipelineJob  # noqa: E402
from orcapod.pipeline import base as pipeline_base  # noqa: E402
from orcapod.pipeline import restrictions  # noqa: E402
from orcapod.pipeline.base import AbstractPipelineBase  # noqa: E402
from orcapod.pipeline.pod_invocation import PodInvocation  # noqa: E402
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator  # noqa: E402
from orcapod.types import CacheMode, Condition  # noqa: E402
from tests.test_databases import fake_spiral_sdk  # noqa: E402

PROJECT = "workshop-sample"
KEYS = pa.schema([pa.field("subject_id", pa.string()), pa.field("t", pa.uint64())])

EdgeKey = tuple[str, str, "int | None"]
Restriction = tuple[Condition, "int | None"]


class EdgeRestrictionConflict(ValueError):
    """Two different restrictions were written for one edge."""


# A pipeline keeps its edges on itself (``_proto_edges``): edge key to restriction
# (``None`` for an edge written without one). Its compiled nodes point to them.
# The restricted reads of a run: one per source node and distinct restriction.
READS: dict[int, dict[tuple[int, str, int | None], Any]] = {}


def text(restriction: Restriction | None) -> str:
    if restriction is None:
        return "no restriction"
    return restrictions.restriction_text(*restriction)


def unrestricted(source: SpiralDBTableSource) -> SpiralDBTableSource:
    """The design's ``unrestricted()``: the same table, reading every row."""
    plain = source._copy()  # pyright: ignore[reportPrivateUsage]
    plain._condition = Condition()  # pyright: ignore[reportPrivateUsage]
    plain._limit = None  # pyright: ignore[reportPrivateUsage]
    return plain


def restricted(source: Any, restriction: Restriction) -> Any:
    """The design's ``restricted(condition, limit)``: a pushed-down read of ``source``."""
    condition, limit = restriction
    reader = source.where(condition) if condition else source._copy()
    reader._limit = limit
    return reader


def positions(pod: Any, inputs: tuple[Any, ...]) -> list[int | None]:
    """The edge position of each input: its index, or ``None`` for a commutative pod."""
    symmetry = getattr(pod, "argument_symmetry", None)
    ordered = symmetry is None or isinstance(symmetry(inputs), tuple)
    return [index if ordered else None for index in range(len(inputs))]


class _Deferred:
    """A source node's full read, made only when a node takes an unrestricted edge."""

    def __init__(self, node: Any, execute: Callable[..., Any]) -> None:
        self._node, self._execute, self._stream = node, execute, None

    def read(self) -> Any:
        if self._stream is None:
            self._stream = self._execute(self._node)
        return self._stream

    def __getattr__(self, name: str) -> Any:  # anything else that reads it
        return getattr(self.read(), name)


def install() -> None:
    """Apply the prototype to this process."""
    original_record = AbstractPipelineBase._record_invocation  # pyright: ignore[reportPrivateUsage]

    def record(self: AbstractPipelineBase[Any], invocation: PodInvocation) -> None:
        key = invocation.content_hash().to_string()
        inputs = tuple(invocation.input_streams)
        edges = self.__dict__.setdefault("_proto_edges", {})
        written: dict[EdgeKey, Restriction | None] = {}
        leaves: dict[str, SpiralDBTableSource] = {}
        for stream, position in zip(
            inputs, positions(invocation.pod, inputs), strict=True
        ):
            upstream = stream.content_hash().to_string()
            restriction = restrictions.restriction_of(stream)
            edge = (upstream, key, position)
            for held in (edges, written):
                if edge in held and held[edge] != restriction:
                    pair = sorted([text(held[edge]), text(restriction)])
                    raise EdgeRestrictionConflict(
                        f"The edge from {stream.label!r} to {invocation.pod.label!r} "
                        f"(position {position}) is written with two different "
                        f"restrictions ({pair[0]}; {pair[1]}). An edge carries one "
                        "restriction. To use both, make each a step of the pipeline: "
                        "source.filter(...)."
                    )
            written[edge] = restriction
            if restriction is not None and isinstance(stream, SpiralDBTableSource):
                leaves[upstream] = stream
        # Nothing is kept from a refused invocation. The leaf is the source without
        # its restriction, one object per identity.
        for upstream, stream in leaves.items():
            if upstream not in self._source_streams:  # pyright: ignore[reportPrivateUsage]
                self._source_streams[upstream] = unrestricted(stream)  # pyright: ignore[reportPrivateUsage]
        edges.update(written)
        original_record(self, invocation)

    AbstractPipelineBase._record_invocation = record  # pyright: ignore[reportPrivateUsage]

    original_compile = AbstractPipelineBase.compile

    def compile_(self: AbstractPipelineBase[Any]) -> None:
        original_compile(self)
        for node in (self._persistent_node_map or {}).values():  # pyright: ignore[reportPrivateUsage]
            node.__dict__["_proto_edges"] = self.__dict__.get("_proto_edges", {})

    AbstractPipelineBase.compile = compile_

    def nothing(*args: Any, **kwargs: Any) -> None:
        return None

    # The whole-pipeline rule is replaced by the edge rule above. The operator-cache
    # refusal is left on: it never fires, since no bound source is restricted.
    restrictions.require_one_restriction = nothing
    pipeline_base.require_one_restriction = nothing  # pyright: ignore[reportAttributeAccessIssue]

    def read_edge(output: Any) -> Any:
        return at_edge(output.read() if isinstance(output, _Deferred) else output)

    def edge_inputs(node: Any, outputs: dict[Any, Any]) -> list[Any]:
        """A node's inputs by position: each edge's rows."""
        node_key = node.content_hash().to_string()
        edges: dict[EdgeKey, Restriction | None] = {
            edge: restriction
            for edge, restriction in node.__dict__.get("_proto_edges", {}).items()
            if edge[1] == node_key
        }
        ordered = any(edge[2] is not None for edge in edges)
        streams: list[Any] = []
        for index, upstream in enumerate(node.upstreams):
            edge = (
                upstream.content_hash().to_string(),
                node_key,
                index if ordered else None,
            )
            restriction = edges.get(edge)
            if restriction is None or not isinstance(upstream, SourceJobNode):
                streams.append(read_edge(outputs[upstream]))
                continue
            reads = READS.setdefault(id(outputs), {})
            read_key = (id(upstream), restriction[0].text(), restriction[1])
            if read_key not in reads:
                bound = upstream._bound_source  # pyright: ignore[reportPrivateUsage]
                upstream._bound_source = restricted(bound, restriction)  # pyright: ignore[reportPrivateUsage]
                try:
                    reads[read_key] = at_edge(original_execute(upstream))
                finally:
                    upstream._bound_source = bound  # pyright: ignore[reportPrivateUsage]
            streams.append(reads[read_key])
        return streams

    def single_input(node: Any, graph: Any, outputs: dict[Any, Any]) -> Any:
        return edge_inputs(node, outputs)[0]

    def ordered_inputs(node: Any, graph: Any, outputs: dict[Any, Any]) -> list[Any]:
        return edge_inputs(node, outputs)

    SyncPipelineOrchestrator._single_input = staticmethod(single_input)  # pyright: ignore[reportPrivateUsage]
    SyncPipelineOrchestrator._ordered_inputs = staticmethod(ordered_inputs)  # pyright: ignore[reportPrivateUsage]

    original_execute = SourceJobNode.execute

    def execute(self: Any, *, observer: Any = None) -> Any:
        return _Deferred(self, original_execute)

    SourceJobNode.execute = execute  # pyright: ignore[reportAttributeAccessIssue]


# ---------------------------------------------------------------------------
# The demonstrations
# ---------------------------------------------------------------------------


@function_pod(output_keys="doubled")
def double(amplitude: float | None) -> float | None:
    """Return twice ``amplitude``."""
    return None if amplitude is None else 2 * amplitude


@function_pod(output_keys="tripled")
def triple(parity: int | None) -> int | None:
    """Return three times ``parity``: it reads another column than ``double``."""
    return None if parity is None else 3 * parity


@function_pod(output_keys="halved")
def halve(amplitude: float | None) -> float | None:
    """Return half of ``amplitude``: it reads the column ``double`` reads."""
    return None if amplitude is None else amplitude / 2


def heading(number: int, title: str) -> None:
    print(f"\n== [proto {number}] {title}")


def ts(node: Any) -> list[Any]:
    return sorted(node.as_table().column("t").to_pylist())


def events() -> SpiralDBTableSource:
    return SpiralDBTableSource(
        PROJECT, "event", source_id="events", tag_only_columns=True
    )


def build(root: Path, record: Callable[[], Any], name: str = "demo") -> PipelineJob:
    job = PipelineJob(name=name, database=DeltaTableDatabase(root / name))
    with job:
        record()
    return job


def kinds(job: PipelineJob) -> dict[str, str]:
    return {name: node.node_type for name, node in sorted(job.nodes.items())}


def scans() -> list[str]:
    """The scans made since the last call: each one's pushed-down filter and limit."""
    made = [
        f"where={record.filter!r}, limit={record.limit}"
        for record in fake_spiral_sdk.scan_log
    ]
    fake_spiral_sdk.scan_log.clear()
    return made


def refused(record: Callable[[], Any], root: Path, name: str) -> str:
    try:
        build(root, record, name)
    except EdgeRestrictionConflict as exc:
        return str(exc)
    return "accepted"


def main() -> None:
    warnings.simplefilter("ignore")
    install()
    with (
        tempfile.TemporaryDirectory() as tmp,
        fake_spiral_sdk.install(Path(tmp) / "spiral"),
    ):
        root = Path(tmp)
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
        table = events()
        m1, m2 = table.where(subject_id="m1"), table.where(subject_id="m2")
        odd = table.where(parity=1)

        heading(
            1, "f(A.where(m1)) and g(A.where(m2)): three nodes, two pushed-down reads"
        )
        plain = build(root, lambda: double.pod(table, label="f"), "plain")
        job = plain
        for order, pair in (
            ("f then g", ((double, m1, "f"), (triple, m2, "g"))),
            ("g then f", ((triple, m2, "g"), (double, m1, "f"))),
        ):
            job = build(
                root,
                lambda pair=pair: [pod.pod(src, label=lb) for pod, src, lb in pair],
                "two-" + order[0],
            )
            scans()
            job.run()
            print(f"  recorded {order}: nodes={kinds(job)}")
            print(
                f"    rows of f: t={ts(job.nodes['f'])}; of g: t={ts(job.nodes['g'])}"
            )
            print(f"    scans in the run: {scans()}")
        f, f_plain = job.nodes["f"], plain.nodes["f"]
        print(
            "  f equals f of the unrestricted job: content hash",
            f.content_hash() == f_plain.content_hash(),
            "| pipeline hash",
            f.pipeline_hash() == f_plain.pipeline_hash(),
            "| stored path",
            f.node_uri == f_plain.node_uri,
        )
        print(f"  the source node read by hand: t={ts(job.nodes['events'])}")
        print(f"    scans of that read: {scans()}")

        heading(2, "two different restrictions on one edge: refused, in either order")
        cases: dict[str, Callable[[], Any]] = {
            "f(m1), f(m2)": lambda: (double.pod(m1), double.pod(m2)),
            "f(m2), f(m1)": lambda: (double.pod(m2), double.pod(m1)),
            "f(m1), f(A)": lambda: (double.pod(m1), double.pod(table)),
            "f(A), f(m1)": lambda: (double.pod(table), double.pod(m1)),
            "f(m1), f(m1 written again)": lambda: (
                double.pod(m1),
                double.pod(events().where(subject_id="m1")),
            ),
            "f(m1), h(m2), both reading only 'amplitude'": lambda: (
                double.pod(m1),
                halve.pod(m2),
            ),
            "f(op(m1)), g(op(m2))": lambda: (
                double.pod(
                    m1.map_data({"amplitude": "a"}).map_data({"a": "amplitude"})
                ),
                halve.pod(m2.map_data({"amplitude": "a"}).map_data({"a": "amplitude"})),
            ),
        }
        for number, (name, record) in enumerate(cases.items()):
            print(f"  {name}:\n    {refused(record, root, f'c{number}')[:175]}")

        heading(3, "the remedy: the condition as a step (filter)")
        job = build(
            root,
            lambda: (
                double.pod(table.filter(subject_id="m1"), label="f1"),
                double.pod(table.filter(subject_id="m2"), label="f2"),
            ),
            "filters",
        )
        scans()
        job.run()
        print(f"  nodes={kinds(job)}")
        print(f"  f1: t={ts(job.nodes['f1'])}; f2: t={ts(job.nodes['f2'])}")
        print(f"  scans: {scans()}")

        heading(4, "an ordered operator: each position is its own edge")
        for name, op, inputs in (
            ("MergeJoin(m1, odd)", MergeJoin, (m1, odd)),
            ("MergeJoin(odd, m1)", MergeJoin, (odd, m1)),
            ("MergeJoin(A, A)", MergeJoin, (table, table)),
            ("SemiJoin(A, m1)", SemiJoin, (table, m1)),
            ("SemiJoin(m1, A)", SemiJoin, (m1, table)),
        ):
            job = build(
                root, lambda op=op, inputs=inputs: op()(*inputs, label="op"), name
            )
            scans()
            job.run()
            rows = job.nodes["op"].as_table().sort_by("t").to_pydict()
            print(f"  {name}: t={rows['t']} amplitude={rows['amplitude']}")
            print(f"    scans: {scans()}")

        heading(5, "one restriction on two edges out of a source: one read")
        job = build(
            root,
            lambda: (double.pod(m1, label="f"), triple.pod(m1, label="g")),
            "shared",
        )
        scans()
        job.run()
        print(f"  rows of f: t={ts(job.nodes['f'])}; of g: t={ts(job.nodes['g'])}")
        print(f"  scans: {scans()}")

        heading(6, "a cached operator below a restricted edge, by cache mode")
        for cache_mode in (CacheMode.OFF, CacheMode.LOG, CacheMode.REPLAY):
            shown: list[str] = []
            for source in (m1, odd):
                job = build(root, lambda s=source: s.group_by("subject_id"), "ops")
                (node,) = [n for n in job.nodes.values() if n.node_type == "operator"]
                views = job._database_views(job._database, job._result_database)  # pyright: ignore[reportPrivateUsage]
                node.attach_databases(pipeline_database=views[0], cache_mode=cache_mode)
                job.run()
                rows = node.as_table()
                shown.append(
                    str(
                        sorted(
                            zip(
                                rows.column("subject_id").to_pylist(),
                                rows.column("amplitude").to_pylist(),
                                strict=True,
                            )
                        )
                    )
                )
            print(f"  {cache_mode.name}: a job under m1 shows {shown[0]}")
            print(f"    then a job under parity=1 shows {shown[1]}")

        heading(7, "the edge view: the upstream node's stream under the restriction")
        job = build(root, lambda: double.pod(m1, label="f"), "view")
        scans()
        view = restricted(job.nodes["events"].bound_source, (m1.condition, 2))
        print(f"  events -> f with a limit of 2, before any run: t={ts(view)}")
        print(f"    scans: {scans()}")
        print(f"    f holds {job.nodes['f'].as_table().num_rows} rows: nothing ran")


if __name__ == "__main__":
    main()
