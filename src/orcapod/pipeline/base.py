"""AbstractPipelineBase — shared recording mechanism for Pipeline and PipelineJob."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Hashable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, cast

from orcapod.core.nodes.function_node import FunctionNodeBase
from orcapod.core.tracker import AutoRegisteringContextBasedTracker
from orcapod.pipeline.dag import OrcaDAG
from orcapod.pipeline.pod_invocation import (
    FunctionInvocation,
    OperatorInvocation,
    PodInvocation,
    SideEffectInvocation,
)
from orcapod.side_effects import SideEffectNode
from orcapod.protocols import core_protocols as cp
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import networkx as nx
    from orcapod.pipeline.diagram import PipelineDiagram
    from orcapod.side_effects import SideEffectPodStream
    from orcapod.types.provenance import Highlight
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
else:
    nx = LazyModule("networkx")

logger = logging.getLogger(__name__)

NodeT = TypeVar("NodeT", bound=Hashable)


def _refuse_sources_sharing_a_slot_name(
    slot_name: str, streams: list[cp.StreamProtocol]
) -> None:
    """Refuse two different sources that would take one slot name (ITL-1425).

    A slot is named by its source's name, without the kind part, so two different
    sources whose names are equal (a SQLite table ``data.db:main.records`` and a CSV
    file at that path; Arrow and data frame sources over one table with default ids)
    would share one ``job.sources`` and ``bind`` key, and one would replace the other.
    Only sources are refused here: a slot of another stream is named by its label and
    content hash.

    Raises:
        SourceSlotNameCollisionError: Naming every full source id and the remedy.
    """
    from orcapod.core.sources.base import RootSource

    sources = [s for s in streams if isinstance(s, RootSource)]
    if len(sources) < 2:
        return
    from orcapod.errors import SourceSlotNameCollisionError

    ids = sorted({s.source_id for s in sources})
    described = (
        ", ".join(repr(i) for i in ids)
        if len(ids) > 1
        else f"{ids[0]!r} ({len(sources)} sources with other key columns or rows)"
    )
    raise SourceSlotNameCollisionError(
        f"{len(sources)} different sources would take the slot name {slot_name!r}: "
        f"{described}. A slot is named by the source's name without its kind part; "
        "give each source its own name with source_id=... when it is built."
    )


@dataclass(frozen=True)
class InvocationGraph:
    """Interchange value object between Pipeline and PipelineJob.

    Carries a topologically ordered tuple of ``PodInvocation`` objects and a
    mapping from content-hash string to the corresponding source stream.
    Both classes exchange representations by converting to this neutral form
    via ``to_invocations()`` and reconstructing via ``from_invocations()``.

    Args:
        invocations: Topologically ordered sequence of pod invocations
            (sources excluded).
        source_streams: Mapping of content-hash string → source stream
            (nodes whose invocation was not recorded in the pipeline).
    """

    invocations: tuple[PodInvocation, ...]
    source_streams: dict[str, Any]  # hash → StreamProtocol-compatible node


class AbstractPipelineBase(Generic[NodeT], AutoRegisteringContextBasedTracker, ABC):
    """Shared recording mechanism and graph state for Pipeline and PipelineJob.

    Manages the ``with``-block recording phase: accumulating invocations into
    ``_invocation_lut``, capturing raw source streams into ``_source_streams``,
    and building the topology in ``_hash_graph``.  On context exit, ``compile()``
    materialises the accumulated state into a frozen DAG of node objects.

    ``_invocation_lut`` and ``_source_streams`` are **additive** — they persist
    across multiple ``with`` blocks so that repeated recording sessions extend
    the same graph rather than replacing it.

    Args:
        name: Pipeline name (string or tuple). Used to scope database paths.
        tracker_manager: Optional tracker manager override.
    """

    def __init__(
        self,
        name: str | tuple[str, ...] = "pipeline",
        tracker_manager: cp.TrackerManagerProtocol | None = None,
    ) -> None:
        """Initialize shared pipeline state.

        Args:
            name: Pipeline name (string or tuple). Used to scope database paths.
                Stored internally as a tuple.
            tracker_manager: Optional tracker manager override. Uses the default
                tracker manager if ``None``.
        """
        super().__init__(tracker_manager=tracker_manager)
        self._name: tuple[str, ...] = (name,) if isinstance(name, str) else tuple(name)

        # --- Additive recording state (never cleared) -----------------
        # Maps content-hash-string → PodInvocation for each recorded invocation.
        self._invocation_lut: dict[str, PodInvocation] = {}
        # Maps content-hash-string → raw stream for each unregistered upstream.
        self._source_streams: dict[str, Any] = {}
        # Topology graph — vertices and edges are content-hash strings.
        # Additive: persists and grows across multiple with-blocks.
        self._hash_graph: "nx.DiGraph" = nx.DiGraph()

        # --- Compiled state (populated / replaced by compile()) --------
        self._persistent_node_map: dict[str, NodeT] = {}
        self._nodes: dict[str, NodeT] = {}
        self._node_graph: OrcaDAG[NodeT] | None = None
        self._compiled: bool = False
        # Each recorded step's inputs' keys, as they were when it was recorded
        # (ITL-1387): a later rebind of another job's slot does not move them.
        self._input_keys: dict[str, tuple[str, ...]] = {}
        # Names given exactly by register(), to the step they name (ITL-1387).
        self._exact_names: dict[str, str] = {}
        # The state when the open ``with`` block was entered (ITL-1387).
        self._state_at_entry: dict[str, Any] | None = None

        # --- Fields populated by compile(); used by is_runnable(), run(), save() ----
        # Populated from _persistent_node_map / _hash_graph at end of compile().
        self._node_lut: dict[str, Any] = {}  # hash → non-source compiled node
        self._upstreams: dict[str, Any] = {}  # hash → source compiled node
        self._graph_edges: list[tuple[str, str]] = []  # edge list from _hash_graph

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> tuple[str, ...]:
        """Pipeline name tuple."""
        return self._name

    @property
    def graph(self) -> "nx.DiGraph":
        """Directed hash graph of accumulated pipeline structure."""
        return self._hash_graph

    @property
    def nodes(self) -> dict[str, Any]:
        """Copy of the compiled nodes dict (label → node)."""
        return self._nodes.copy()

    @property
    def source_pods(self) -> dict[str, Any]:
        """Copy of compiled nodes that are source nodes (label → node)."""
        return {k: v for k, v in self._nodes.items() if v.node_type == "source"}

    @property
    def function_pods(self) -> dict[str, Any]:
        """Copy of compiled nodes that are function-pod nodes (label → node)."""
        return {k: v for k, v in self._nodes.items() if v.node_type == "function"}

    @property
    def operator_pods(self) -> dict[str, Any]:
        """Copy of compiled nodes that are operator-pod nodes (label → node)."""
        return {k: v for k, v in self._nodes.items() if v.node_type == "operator"}

    def set_ephemeral_database(self, database: "ArrowDatabaseProtocol | None") -> None:
        """Assign or remove the ephemeral result database for all nodes.

        Propagates ``database`` to every function-pod and operator-pod node in the
        pipeline by calling ``node.set_ephemeral_database(database)`` on each. Source
        nodes are skipped — they produce, not consume, data. Nodes that do not
        support ephemeral storage (e.g. operator nodes in v1) treat this as a
        no-op.

        Pass ``None`` to detach the ephemeral database from all nodes, reverting
        them to persistent-only writes for subsequent runs.

        Args:
            database: An ``ArrowDatabaseProtocol`` instance to attach, or ``None``
                to detach.

        Raises:
            UnstorableColumnError: If ``database`` cannot store a function node's
                result table (ITL-1162); no node is changed.
        """
        if database is not None:
            # Every function node is checked before any changes, so a database that
            # cannot store a result table leaves every node as it was (ITL-1162).
            for node in self._nodes.values():
                if isinstance(node, FunctionNodeBase):
                    node.require_storable_results(database)
        for node in self._nodes.values():
            if node.node_type not in ("source", "side_effect"):
                node.set_ephemeral_database(database)

    @property
    def dag(self) -> OrcaDAG[NodeT]:
        """Node-object DAG for this pipeline.

        Returns an ``OrcaDAG`` whose vertices are the compiled node objects
        (``GraphNode`` for ``Pipeline``, ``JobNode`` for ``PipelineJob``) and
        whose edges follow the data-flow topology.

        Raises:
            RuntimeError: If the pipeline has not been compiled yet.
        """
        if self._node_graph is None:
            raise RuntimeError(
                "Pipeline has not been compiled. "
                "Use 'with pipeline:' or call compile() first."
            )
        return self._node_graph

    # ------------------------------------------------------------------
    # Abstract — subclass node-factory declarations
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def source_node_class(self) -> type:
        """Node class to use for source (leaf) nodes — e.g. ``SourceNode``."""
        ...

    @property
    @abstractmethod
    def function_node_class(self) -> type:
        """Node class to use for function-pod invocations — e.g. ``FunctionNode``."""
        ...

    @property
    @abstractmethod
    def operator_node_class(self) -> type:
        """Node class to use for operator-pod invocations — e.g. ``OperatorNode``."""
        ...

    @property
    @abstractmethod
    def side_effect_node_class(self) -> type:
        """Node class to use for side-effect pod invocations — e.g. ``SideEffectJobNode``."""
        ...

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record_function_pod_invocation(
        self,
        pod: "cp.FunctionPodProtocol",
        input_stream: "cp.StreamProtocol",
        label: str | None = None,
    ) -> None:
        """Record a function pod invocation into the graph.

        Args:
            pod: The function pod being invoked.
            input_stream: The upstream stream.
            label: Optional display label for the resulting compiled node.
        """
        self._record_invocation(
            FunctionInvocation(pod=pod, input_streams=(input_stream,), label=label)
        )

    def record_operator_pod_invocation(
        self,
        pod: "cp.OperatorPodProtocol",
        upstreams: "tuple[cp.StreamProtocol, ...]" = (),
        label: str | None = None,
    ) -> None:
        """Record an operator pod invocation into the graph.

        Args:
            pod: The operator pod being invoked.
            upstreams: Upstream streams for this operator.
            label: Optional display label for the resulting compiled node.
        """
        self._record_invocation(
            OperatorInvocation(pod=pod, input_streams=tuple(upstreams), label=label)
        )

    def record_side_effect_pod_invocation(
        self,
        pod: "cp.SideEffectPodProtocol",
        input_stream: "cp.StreamProtocol",
        label: str | None = None,
    ) -> None:
        """Record a side-effect pod invocation into the graph.

        Args:
            pod: The side-effect pod being invoked.
            input_stream: The upstream stream.
            label: Optional display label for the resulting compiled node.
        """
        self._record_invocation(
            SideEffectInvocation(pod=pod, input_streams=(input_stream,), label=label)
        )

    def _record_invocation(self, invocation: PodInvocation) -> None:
        """Store *invocation* and update the topology graph.

        Non-source nodes in ``_persistent_node_map`` are keyed by the
        corresponding ``PodInvocation.content_hash()`` string.  For
        ``PipelineJob`` (where ``source_node_class`` is ``SourceJobNode``),
        a bound ``SourceJobNode`` delegates its ``content_hash()`` to the
        underlying concrete source, so invocation and compiled-node hashes
        agree throughout the graph.

        For ``Pipeline`` (where ``source_node_class`` is ``SourceNode``), a
        concrete ``RootSource`` upstream is promoted to a ``SourceNode`` whose
        identity is ``("source_node", name, tag_schema, data_schema)`` — a
        different structure from the raw source.  As a result the compiled
        ``FunctionNode`` / ``OperatorNode``'s own ``content_hash()`` may
        diverge from the invocation hash used as its key.  This is intentional:
        the hash-graph topology is always built from raw stream hashes, and
        ``_persistent_node_map`` uses those same keys for consistent lookup.

        Args:
            invocation: The pod invocation to record.

        Raises:
            ValueError: If the invocation reads a source under another restriction
                than the pipeline's streams read it under now, or two of its inputs
                read one source under two restrictions (ITL-1218), whatever streams
                lead there. Nothing is recorded then.
        """
        # An input made by a pod comes in with its pod and every upstream step down
        # to its sources (ITL-1387): a stream is a source only when the user made it
        # one. A stream built inside the block is already held, so nothing is walked.
        planned = self._plan_steps(invocation.input_streams)
        self._record_planned([*planned, invocation])

    def _record_planned(
        self,
        invocations: list[PodInvocation],
        also_read: "Sequence[cp.StreamProtocol]" = (),
    ) -> None:
        """Record *invocations*, in order, after the checks that cover all of them.

        Args:
            invocations: Invocations in an order where each one's inputs that are
                steps come before it.
            also_read: Further streams whose sources the restriction check covers:
                the streams given to ``register``, whose steps may already be held
                under another restriction of one source (ITL-1387).

        Raises:
            ValueError: If the invocations read a source under another restriction
                than the pipeline's streams read it under now, or under two
                restrictions among themselves (ITL-1218); or a step is labelled with
                a name given exactly to another step (``register``'s keyword names).
                Nothing is recorded then.
        """
        from orcapod.pipeline.restrictions import require_one_restriction

        # Checked before anything is recorded, so a refused invocation leaves the
        # graph as it was. A restriction does not change a source's identity, so the
        # pipeline keeps one restriction (or none) per source identity, whatever
        # streams lead to it: every source these invocations read is checked with
        # every source the pipeline's streams read now (nothing is remembered: a
        # slot rebound since the last invocation changes what they read).
        require_one_restriction(
            [
                *self._held_streams(),
                *(stream for inv in invocations for stream in inv.input_streams),
                *also_read,
            ]
        )
        for invocation in invocations:
            name = invocation.label
            if name is not None and name in self._exact_names:
                if self._exact_names[name] != invocation.content_hash().to_string():
                    raise ValueError(
                        f"The name {name!r} was given to another step by register(); "
                        "a step labelled so would rename it. Give this step another "
                        "label."
                    )
        for invocation in invocations:
            self._store_invocation(invocation)

    def _store_invocation(self, invocation: PodInvocation) -> None:
        """Add *invocation* to the recorded state, with no check.

        Its inputs' keys are kept as they are now, so a stream whose identity can
        change later (a slot of another job that is rebound) still finds this step's
        inputs; a slot of another pipeline is kept as a copy holding its binding now,
        so this pipeline's bindings stay its own (ITL-1387).
        """
        key = invocation.content_hash().to_string()
        if not self._remember_invocation(key, invocation):
            return
        upstreams = [
            (upstream, upstream.content_hash().to_string())
            for upstream in invocation.input_streams
        ]
        self._input_keys[key] = tuple(upstream_hash for _, upstream_hash in upstreams)
        self._hash_graph.add_node(key)
        for upstream, upstream_hash in upstreams:
            self._hash_graph.add_edge(upstream_hash, key)
            if upstream_hash not in self._source_streams:
                self._source_streams[upstream_hash] = self._own_leaf(
                    upstream, upstream_hash
                )

    def _own_leaf(self, stream: Any, key: str) -> Any:
        """The leaf to keep for *stream*: a copy when it is another pipeline's slot."""
        from orcapod.core.nodes.source_node import SourceJobNode

        # A ``SourceNode`` holds no binding (a schema-only slot), so only a job's
        # slot is copied.
        if (
            isinstance(stream, SourceJobNode)
            and self._persistent_node_map.get(key) is not stream
        ):
            return SourceJobNode.from_stream(stream)
        return stream

    def _plan_steps(
        self,
        streams: "Sequence[cp.StreamProtocol]",
        names: "dict[str, str] | None" = None,
        *,
        refuse_label_conflicts: bool = False,
    ) -> list[PodInvocation]:
        """The steps that bring *streams* in, with every upstream step (ITL-1387).

        Walks each stream's producer and upstreams back to its sources and returns
        an invocation for each step the pipeline does not hold yet, upstream steps
        first. A source (any ``RootSource``) and a slot (any ``SourceNodeBase``) end
        the walk: a stream is a source only when the user made it one. A step the
        pipeline holds is left as it is (its label too). A stream with no producer
        that is neither a source nor a node (a raw ``ArrowTableStream``) is left as a
        leaf, and the bind check refuses it when the pipeline compiles, as before.
        Computes nothing: only producers, upstreams, labels and content hashes are
        read.

        One step reached twice in the walk takes an assigned label over none. Two
        different assigned labels are refused when *refuse_label_conflicts* is set
        (``register``); otherwise the smallest explicit label is kept (ITL-1436).

        Args:
            streams: The streams to bring in.
            names: Label to give the step of a stream, by the stream's content hash
                (``register``'s keyword names); every other step keeps the label
                it was given.
            refuse_label_conflicts: Refuse one step under two different labels.

        Returns:
            The invocations to record, each after the steps it reads.

        Raises:
            ValueError: If a step's pod is not attached (a node of a loaded job
                whose function or pod was not loadable or not loaded), or, with
                *refuse_label_conflicts*, one step is reached under two different
                assigned labels.
            TypeError: If a stream's producer is neither a function pod, a
                side-effect pod nor an operator.
        """
        names = names or {}
        planned: dict[str, PodInvocation] = {}
        expanded_keys: set[str] = set()
        # Iterative post-order walk: (stream, upstreams visited).
        stack: list[tuple[Any, bool]] = [
            (stream, False) for stream in reversed(streams)
        ]
        while stack:
            stream, expanded = stack.pop()
            if _is_source(stream):
                continue
            key = stream.content_hash().to_string()
            if key in self._invocation_lut:
                continue
            label = names.get(key, _recorded_label(stream))
            if key in planned:
                earlier = planned[key].label
                if label is not None and label != earlier:
                    if earlier is not None and refuse_label_conflicts:
                        raise ValueError(
                            f"One step is given two labels, {earlier!r} and "
                            f"{label!r}: a step has one node, so give it one label."
                        )
                    if earlier is None or label < earlier:
                        planned[key] = _with_label(planned[key], label)
                continue
            producer = stream.producer
            if producer is None:
                if _is_node(stream):
                    raise ValueError(_unattached_message(stream))
                continue  # a leaf: the bind check decides when the pipeline compiles
            if not expanded:
                if key in expanded_keys:
                    continue
                expanded_keys.add(key)
                stack.append((stream, True))
                stack.extend((up, False) for up in reversed(tuple(stream.upstreams)))
                continue
            planned[key] = _invocation_of(stream, producer, label)
        return list(planned.values())

    def _remember_invocation(self, key: str, invocation: PodInvocation) -> bool:
        """Store the chosen invocation, retaining exact names and otherwise the smallest label."""
        previous = self._invocation_lut.get(key)
        if previous is not None and previous.label is not None:
            if (
                self._exact_names.get(previous.label) == key
                or invocation.label is None
                or previous.label < invocation.label
            ):
                return False
        self._invocation_lut[key] = invocation
        return True

    def _held_streams(self) -> list[Any]:
        """Every stream the pipeline holds: its leaves and every recorded input."""
        return [
            *self._source_streams.values(),
            *(
                stream
                for invocation in self._invocation_lut.values()
                for stream in invocation.input_streams
            ),
        ]

    # ------------------------------------------------------------------
    # register() (ITL-1387)
    # ------------------------------------------------------------------

    def register(
        self,
        *streams: "cp.StreamProtocol | SideEffectPodStream",
        **named_streams: "cp.StreamProtocol | SideEffectPodStream",
    ) -> None:
        """Bring streams into the pipeline, with every step above them.

        Each stream comes in with its pod and every upstream step down to its
        sources, exactly as if those steps had been written inside a ``with``
        block: the nodes, hashes, stored paths and results are the same. A stream
        becomes a source only when you made it one (a source object, or a node's
        ``as_source()``). Steps the pipeline already holds are not added again, and
        registering only such steps changes nothing; a stream built on a node of
        this pipeline adds only its new steps; a node of another pipeline or job
        brings its steps too.

        Outside a ``with`` block the pipeline is compiled at once (a job built with
        ``auto_compile=False`` is not); inside one, when the block ends. A keyword
        name is exact: the node is named so, never with a suffix, in every form.
        Registering computes nothing, calls no user code and stores nothing. A
        refused call leaves the pipeline as it was.

        Args:
            *streams: Streams to bring in; each step keeps the label it was given.
            **named_streams: Streams to bring in, each node named by its keyword.

        Raises:
            ValueError: If no stream is given; a stream is a source (it has no step)
                or neither a source nor made by a pod; a keyword name starts with
                ``_``, names a step that already has another label, or is taken by
                another step; one stream is given two names; one step is reached
                under two different labels; a step's pod is not attached (a node of a
                loaded job); one source is read under two restrictions; or the
                pipeline is a loaded, read-only job.
            TypeError: If a stream's producer is neither a function pod, a
                side-effect pod nor an operator.
        """
        self._require_recordable()
        # A side-effect stream is not typed as a StreamProtocol; every stream is
        # read here only through producer, upstreams, labels and content hash.
        given: list[Any] = [*streams, *named_streams.values()]
        if not given:
            raise ValueError("register() needs at least one stream.")
        for stream in given:
            if _is_source(stream):
                raise ValueError(
                    f"{stream.label!r} is a source: it has no step to register. "
                    "Register a stream built from it."
                )
            if stream.producer is None:
                if _is_node(stream):
                    raise ValueError(_unattached_message(stream))
                raise ValueError(
                    f"{stream.label!r} is neither a source nor made by a pod, so it "
                    "cannot be registered. Wrap its table as a source (for example "
                    "ArrowTableSource) and build on that."
                )
        names: dict[str, str] = {}
        for name, stream in named_streams.items():
            if name.startswith("_"):
                raise ValueError(f"A node name cannot start with '_': {name!r}.")
            key = stream.content_hash().to_string()
            if key in names and names[key] != name:
                raise ValueError(
                    f"One stream is given two names, {names[key]!r} and {name!r}."
                )
            held = self._invocation_lut.get(key)
            if held is not None and held.label != name:
                already = (
                    "an unlabelled step" if held.label is None else repr(held.label)
                )
                raise ValueError(
                    f"The step named {name!r} is already in the pipeline as {already}."
                )
            names[key] = name
        planned = self._plan_steps(given, names, refuse_label_conflicts=True)
        self._require_exact_names(names, planned)
        if not planned:
            # Every step is held: nothing to record, nothing to compile. Only the
            # restriction rule is checked, since a held step's identity does not
            # show under which restriction this stream reads its source.
            from orcapod.pipeline.restrictions import require_one_restriction

            require_one_restriction([*self._held_streams(), *given])
            return
        saved = self._state()
        try:
            self._record_planned(planned, also_read=given)
            for key, name in names.items():
                self._exact_names[name] = key
            if not self.is_active() and self._compiles_on_exit():
                self.compile()
        except BaseException:
            self._restore_state(saved)
            raise

    def _require_exact_names(
        self, names: dict[str, str], planned: list[PodInvocation]
    ) -> None:
        """Refuse a keyword name that another step holds or would hold.

        A keyword name is exact: no other node may be named so, or compilation would
        number both (``<name>_1``, ``<name>_2``). Checked against every node, every
        recorded step's label, every name given before, and every step this call
        brings in, before anything is recorded.

        Raises:
            ValueError: If a keyword name is taken by another step.
        """
        if not names:
            return
        labels: dict[str, set[str]] = {}

        def note(label: str | None, key: str) -> None:
            if label is not None:
                labels.setdefault(label, set()).add(key)

        for key, inv in self._invocation_lut.items():
            note(inv.label, key)
        for inv in planned:
            note(inv.label, inv.content_hash().to_string())
            if inv.label is None:
                note(_computed_label(inv), inv.content_hash().to_string())
        for name, key in self._exact_names.items():
            note(name, key)
        for key, name in names.items():
            others = labels.get(name, set()) - {key}
            taken = self._nodes.get(name)
            if others or (
                taken is not None and taken is not self._persistent_node_map.get(key)
            ):
                raise ValueError(f"The name {name!r} is taken by another node.")

    def _require_recordable(self) -> None:
        """Refuse to record into a pipeline that cannot take new steps.

        Raises:
            ValueError: In a subclass that cannot record (a loaded job).
        """

    def _compiles_on_exit(self) -> bool:
        """Whether the end of a ``with`` block (and ``register``) compiles."""
        return True

    #: Attributes a refused ``with`` block or ``register`` restores (ITL-1387): the
    #: recorded steps and everything ``compile()`` publishes.
    _STATE_ATTRIBUTES: tuple[str, ...] = (
        "_invocation_lut",
        "_source_streams",
        "_input_keys",
        "_exact_names",
        "_hash_graph",
        "_persistent_node_map",
        "_nodes",
        "_node_graph",
        "_compiled",
        "_node_lut",
        "_upstreams",
        "_graph_edges",
    )

    def _state(self) -> dict[str, Any]:
        """A copy of the recorded and compiled state, to restore after a refusal."""
        saved: dict[str, Any] = {}
        for name in self._STATE_ATTRIBUTES:
            value = getattr(self, name)
            saved[name] = value.copy() if hasattr(value, "copy") else value
        return saved

    def _restore_state(self, saved: dict[str, Any]) -> None:
        """Put back the state *saved* by ``_state``: the same node objects."""
        for name, value in saved.items():
            setattr(self, name, value.copy() if hasattr(value, "copy") else value)

    # ------------------------------------------------------------------
    # Diagram (ITL-1197)
    # ------------------------------------------------------------------

    def diagram(
        self,
        *,
        direction: Literal["LR", "TB"] = "LR",
        collapse: bool = False,
        max_nodes: int = 400,
        highlight: Highlight | None = None,
    ) -> PipelineDiagram:
        """A diagram of the DAG, drawn in a notebook without Graphviz.

        In development: this interface may change. The first diagram made in a
        process issues ``InDevelopmentWarning``, once.

        Shown at the end of a notebook cell (as the pipeline or job itself is) it is a
        picture of the nodes and the streams between them; where the notebook runs
        scripts it can be dragged and zoomed, and selecting a node shows its schema and,
        for a job, its run status. ``print`` gives a plain-text listing. Making it
        computes nothing.

        Args:
            direction: ``"LR"`` lays the graph out left to right, ``"TB"`` top to
                bottom.
            collapse: Fold the joins and the column selections, drops and maps that
                feed exactly one node into the edges they lie on.
            max_nodes: Above this many nodes, show the text listing instead of the
                picture.
            highlight: Nodes and edges to emphasize, with a note: a trace's path
                (``node.trace(...).highlight()``) or a missing row's
                (``node.why_missing(...).highlight()``).

        Returns:
            The diagram.

        Raises:
            RuntimeError: If the pipeline has not been compiled yet.
            ValueError: If ``highlight`` names a node the pipeline does not have.
        """
        from orcapod.pipeline.diagram import PipelineDiagram

        return PipelineDiagram(
            self,
            direction=direction,
            collapse=collapse,
            max_nodes=max_nodes,
            highlight=highlight,
        )

    def _repr_mimebundle_(
        self,
        include: Any = None,
        exclude: Any = None,
        **kwargs: Any,
    ) -> dict[str, str]:
        """The notebook display: the diagram, or the plain repr before compiling."""
        if self._node_graph is None:
            return {"text/plain": repr(self)}
        return self.diagram()._repr_mimebundle_()

    # ------------------------------------------------------------------
    # reset() — no-op after refactor
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """No-op — all recording state is additive and persists across with-blocks.

        Note:
            ``_invocation_lut``, ``_source_streams``, and ``_hash_graph``
            intentionally accumulate across ``with`` blocks.
            ``_persistent_node_map``, ``_node_lut``, ``_upstreams``, and
            ``_graph_edges`` are compiled artifacts overwritten by each
            ``compile()`` call.  Nothing needs to be cleared on re-entry.
        """

    def __enter__(self):
        """Enter the recording context, keeping the state to restore."""
        self._state_at_entry = self._state()
        return super().__enter__()

    def __exit__(self, exc_type=None, exc_value=None, traceback=None) -> None:
        """Exit the recording context, compiling if no exception occurred.

        A block whose body or compilation raises leaves the pipeline as it was when
        the block was entered (ITL-1387): its recorded steps, its nodes (the same
        objects) and its bindings, so the next block starts clean.
        """
        super().__exit__(exc_type, exc_value, traceback)
        saved, self._state_at_entry = self._state_at_entry, None
        if exc_type is not None:
            if saved is not None:
                self._restore_state(saved)
            return
        if not self._compiles_on_exit():
            return
        try:
            self.compile()
        except BaseException:
            if saved is not None:
                self._restore_state(saved)
            raise

    def __getattr__(self, item: str) -> Any:
        """Look up compiled nodes by label as attribute access."""
        if item.startswith("_"):
            raise AttributeError(item)
        # Use object.__getattribute__ to avoid recursion
        nodes = object.__getattribute__(self, "_nodes")
        if item in nodes:
            return nodes[item]
        raise AttributeError(
            f"{type(self).__name__!r} has no attribute {item!r}. "
            f"Available node labels: {sorted(nodes.keys())}"
        )

    # ------------------------------------------------------------------
    # compile() — single-pass, class-property driven
    # ------------------------------------------------------------------

    def compile(self) -> None:
        """Compile recorded invocations into a frozen DAG.

        Walks ``_hash_graph`` topologically.  Streams that appear as inputs
        but have no registered invocation are promoted to source nodes via
        ``self.source_node_class.from_stream()``.  Each ``PodInvocation`` in
        ``_invocation_lut`` becomes a ``function_node_class`` or
        ``operator_node_class`` instance.

        After compilation:

        - ``_persistent_node_map`` — hash → compiled node (all node types)
        - ``_nodes`` — label → compiled node (labelled nodes only)
        - ``_node_graph`` — OrcaDAG with node objects as vertices
        - ``_node_lut``, ``_upstreams``, and ``_graph_edges`` are repopulated
          and used by ``is_runnable()``, ``run()``, and ``save()``.
        - ``_compiled`` is set to ``True``.

        This method always rebuilds from scratch; it does NOT perform
        incremental compilation.

        Raises:
            ValueError: If one source is read under two different restrictions
                (ITL-1218), judged from what the recorded streams read now.
        """
        import networkx as _nx

        from orcapod.pipeline.restrictions import require_one_restriction

        # Checked again here, from what the streams read now: a slot rebound after
        # the last invocation would otherwise connect a branch recorded over one
        # restriction to another (ITL-1218).
        require_one_restriction(self._held_streams())

        source_node_cls = self.source_node_class

        # 1. Resolve source nodes in hash order, then insert them by their slot
        # labels. The original stream hash breaks ties between identical labels.
        sources = [
            (h, source_node_cls.from_stream(self._source_streams[h]))
            for h in sorted(self._source_streams.keys() - self._invocation_lut.keys())
        ]
        node_map: dict[str, Any] = dict(
            sorted(sources, key=lambda item: (item[1].label, item[0]))
        )

        # 2. Validate source-node name uniqueness.
        # Source node names are identity-forming (used as bind() keys and
        # included in content_hash()).  Two distinct source nodes with the
        # same name are refused: with different schemas by InconsistentSourceError,
        # and when two or more are sources (RootSource), whatever their schemas, by
        # SourceSlotNameCollisionError (ITL-1425: a slot is named by the source's
        # name without its kind part, so another kind, version, key or id would
        # otherwise share the slot). Only streams that are not sources, with equal
        # schemas and colliding label:hash names, are left as they are.
        name_to_hashes: dict[str, list[str]] = {}
        for h, node in node_map.items():
            name_to_hashes.setdefault(node._name, []).append(h)
        for node_name, hashes in name_to_hashes.items():
            if len(hashes) <= 1:
                continue
            nodes = [node_map[h] for h in hashes]
            first = nodes[0]
            schemas_differ = any(
                n._tag_schema != first._tag_schema
                or n._data_schema != first._data_schema
                for n in nodes[1:]
            )
            if schemas_differ:
                from orcapod.errors import InconsistentSourceError

                from orcapod.core.sources.base import RootSource

                ids = sorted(
                    {
                        s.source_id
                        for s in (self._source_streams[h] for h in hashes)
                        if isinstance(s, RootSource)
                    }
                )
                named = f" ({', '.join(repr(i) for i in ids)})" if ids else ""
                raise InconsistentSourceError(
                    f"Pipeline '{'.'.join(self._name)}' has {len(hashes)} source "
                    f"nodes all named {node_name!r}{named} but with different "
                    f"schemas. Assign distinct source_id values to the conflicting "
                    f"sources so each slot has a unique, stable identity."
                )
            _refuse_sources_sharing_a_slot_name(
                node_name, [self._source_streams[h] for h in hashes]
            )

        # 3. Topological pass with independent invocations ordered by hash.
        for key in _nx.lexicographical_topological_sort(self._hash_graph):
            if key in node_map:
                continue  # already added as a source node in step 2
            if key not in self._invocation_lut:
                continue  # vertex with no invocation (e.g. pure source hash)
            inv = self._invocation_lut[key]
            input_keys = self._input_keys.get(key) or tuple(
                up.content_hash().to_string() for up in inv.input_streams
            )
            upstream_nodes = [node_map[input_key] for input_key in input_keys]
            if isinstance(inv, FunctionInvocation):
                node_map[key] = self.function_node_class(
                    function_pod=inv.pod,
                    input_stream=upstream_nodes[0],
                    label=inv.label,
                )
            elif isinstance(inv, SideEffectInvocation):
                node_map[key] = self.side_effect_node_class(
                    side_effect_pod=inv.pod,
                    input_stream=upstream_nodes[0],
                    label=inv.label,
                )
            else:
                node_map[key] = self.operator_node_class(
                    operator=inv.pod,
                    input_streams=tuple(upstream_nodes),
                    label=inv.label,
                )
            # The label as recorded, kept past the disambiguation below, so another
            # pipeline that brings this node in records its step as it was written.
            setattr(node_map[key], _RECORDED_LABEL_ATTRIBUTE, inv.label)

        # 4. Reserve every original label before generating any suffixes.
        name_candidates: dict[str, list] = {}
        for node in node_map.values():
            name_candidates.setdefault(node.label, []).append(node)

        reserved_names = set(name_candidates)
        for label, nodes in name_candidates.items():
            if len(nodes) > 1:
                explicitly_named = [node for node in nodes if node._label is not None]
                if len(explicitly_named) == 1:
                    # A computed label can coincide with an otherwise unique explicit one.
                    nodes = [node for node in nodes if node is not explicitly_named[0]]
                sorted_nodes = sorted(nodes, key=lambda n: n.content_hash().to_string())
                i = 1
                for node in sorted_nodes:
                    key = f"{label}_{i}"
                    while key in reserved_names:
                        i += 1
                        key = f"{label}_{i}"
                    reserved_names.add(key)
                    node._label = key
                    i += 1

        node_map = self._ordered_node_map(node_map, self._hash_graph)
        self._persistent_node_map = node_map
        self._nodes.clear()
        self._nodes.update((node.label, node) for node in node_map.values())

        # 5. Build node_graph (OrcaDAG with node objects as vertices).
        graph_edges = sorted(self._hash_graph.edges())
        node_dag: OrcaDAG[NodeT] = OrcaDAG()
        for node in node_map.values():
            node_dag.add_node(node)
        for up_hash, down_hash in graph_edges:
            up_node = node_map.get(up_hash)
            down_node = node_map.get(down_hash)
            if up_node is not None and down_node is not None:
                node_dag.add_edge(up_node, down_node)
        self._node_graph = node_dag

        # 6. Enrich hash_graph node attributes (used by serialization).
        for node_hash, node in node_map.items():
            if node_hash not in self._hash_graph:
                continue
            attrs = self._hash_graph.nodes[node_hash]
            if not attrs.get("node_type"):
                attrs["node_type"] = node.node_type
            if not attrs.get("label"):
                computed = node._label or (
                    node.computed_label() if hasattr(node, "computed_label") else None
                )
                if computed:
                    attrs["label"] = computed
            if not attrs.get("pipeline_hash"):
                attrs["pipeline_hash"] = node.pipeline_hash().to_string()

        # 7. Populate fields used by is_runnable(), run(), and save().
        self._node_lut = {
            h: n for h, n in node_map.items() if not isinstance(n, source_node_cls)
        }
        self._upstreams = {
            h: n for h, n in node_map.items() if isinstance(n, source_node_cls)
        }
        self._graph_edges = graph_edges

        self._compiled = True

    @staticmethod
    def _ordered_node_map(
        nodes: dict[str, Any], graph: nx.DiGraph[str]
    ) -> dict[str, Any]:
        """Order compiled or restored nodes with slots first and dependencies intact."""
        from orcapod.core.nodes.source_node import SourceNodeBase

        def order_key(key: str) -> tuple[int, str, str, str]:
            node = nodes.get(key)
            if isinstance(node, SourceNodeBase):
                return (0, node.name, node.label, key)
            if node is None:  # An unrestored node may still be in the saved graph.
                return (1, key, "", key)
            # Live and saved nodes share the schema-only pipeline hash.
            return (1, node.pipeline_hash().to_string(), node.label, key)

        return {
            key: nodes[key]
            for key in nx.lexicographical_topological_sort(graph, key=order_key)
            if key in nodes
        }

    # ------------------------------------------------------------------
    # InvocationGraph transitions
    # ------------------------------------------------------------------

    def to_invocations(self) -> InvocationGraph:
        """Extract an ``InvocationGraph`` from the compiled ``_persistent_node_map``.

        Reconstructs from ``_persistent_node_map`` (not raw ``_invocation_lut``) so
        that the in-memory path and the save/load path produce consistent results.
        Source nodes (leaves) go into ``source_streams``; function and operator
        nodes become ``FunctionInvocation`` / ``OperatorInvocation`` objects.

        Returns:
            An ``InvocationGraph`` with topologically ordered invocations and
            a mapping of hash → source node (which acts as its own stream).

        Raises:
            RuntimeError: If the pipeline has not been compiled, or if a node was
                loaded from a saved file without its function pod, operator or
                side-effect pod.
        """
        import networkx as _nx

        if not self._compiled:
            raise RuntimeError(
                "Cannot call to_invocations() before compile(). "
                "Use 'with pipeline:' or call compile() first."
            )

        source_node_cls = self.source_node_class
        fn_node_cls = self.function_node_class
        source_streams: dict[str, Any] = {}
        # Map node_hash (from _persistent_node_map / _hash_graph) → invocation.
        # Using node_hash (not inv.content_hash()) so the topological sort keys
        # from _hash_graph align with inv_by_node_hash.
        inv_by_node_hash: dict[str, PodInvocation] = {}

        for node_hash, node in self._persistent_node_map.items():
            if isinstance(node, source_node_cls):
                source_streams[node.content_hash().to_string()] = node
            elif isinstance(node, fn_node_cls):
                if node._function_pod is None:
                    raise RuntimeError(
                        f"to_invocations() cannot serialise FunctionNode {node_hash!r}: "
                        "node._function_pod is None. This node was loaded without a live "
                        "function pod (read-only / stub mode). "
                        "Use PipelineJob.load() to work with loaded pipelines, or "
                        "ensure the function pod is available before calling "
                        "to_invocations() / from_pipeline()."
                    )
                inv_by_node_hash[node_hash] = FunctionInvocation(
                    pod=node._function_pod,
                    input_streams=(node.upstreams[0],),
                    label=node._label,
                )
            elif isinstance(node, SideEffectNode):
                side_effect_pod = node.producer
                if side_effect_pod is None:
                    raise RuntimeError(
                        f"to_invocations() cannot serialise SideEffectNode "
                        f"{node_hash!r}: it was loaded with no live side-effect pod "
                        "(read-only / stub mode). Use PipelineJob.load() to work with "
                        "loaded pipelines, or build the pipeline from its code before "
                        "calling to_invocations() / from_pipeline()."
                    )
                inv_by_node_hash[node_hash] = SideEffectInvocation(
                    pod=side_effect_pod,
                    input_streams=(node.upstreams[0],),
                    label=node._label,
                )
            else:
                if node._operator is None:
                    raise RuntimeError(
                        f"to_invocations() cannot serialise OperatorNode {node_hash!r}: "
                        "node._operator is None. This node was loaded without a live "
                        "operator (read-only / stub mode). "
                        "Use PipelineJob.load() to work with loaded pipelines, or "
                        "ensure the operator is available before calling "
                        "to_invocations() / from_pipeline()."
                    )
                inv_by_node_hash[node_hash] = OperatorInvocation(
                    pod=node._operator,
                    input_streams=tuple(node.upstreams),
                    label=node._label,
                )

        # Sort topologically using _hash_graph (node_hash keys match _hash_graph).
        topo_keys = _nx.lexicographical_topological_sort(self._hash_graph)
        ordered = [inv_by_node_hash[k] for k in topo_keys if k in inv_by_node_hash]

        return InvocationGraph(
            invocations=tuple(ordered),
            source_streams=source_streams,
        )

    @classmethod
    def from_invocations(
        cls,
        graph: InvocationGraph,
        name: str | tuple[str, ...] = "pipeline",
    ) -> "AbstractPipelineBase":
        """Reconstruct a Pipeline or PipelineJob from an ``InvocationGraph``.

        Calls ``cls(name=name)`` to properly initialise all fields, then
        populates ``_invocation_lut``, ``_source_streams``, and ``_hash_graph``
        from *graph* before calling ``compile()``.

        Args:
            graph: The ``InvocationGraph`` to reconstruct from.
            name: Pipeline name for the new instance.

        Returns:
            A compiled instance of ``cls``.
        """
        instance = cls(name=name)
        instance._source_streams = dict(graph.source_streams)
        for inv in graph.invocations:
            key = inv.content_hash().to_string()
            instance._remember_invocation(key, inv)
            instance._hash_graph.add_node(key)
            for upstream in inv.input_streams:
                instance._hash_graph.add_edge(upstream.content_hash().to_string(), key)
        # Also add source hashes as isolated nodes (no incoming edges)
        for h in graph.source_streams:
            if h not in instance._hash_graph:
                instance._hash_graph.add_node(h)
        instance.compile()
        return instance


# ----------------------------------------------------------------------
# The walk of ITL-1387: helpers
# ----------------------------------------------------------------------

#: Attribute ``compile()`` sets on each node it builds: the label its step was
#: recorded with, before any disambiguating suffix. A node of another pipeline is
#: brought in under that label, as its step was written.
_RECORDED_LABEL_ATTRIBUTE = "_orcapod_recorded_label"
_NOT_RECORDED = object()


def _is_source(stream: Any) -> bool:
    """True for a stream the user made a source: a ``RootSource`` or a slot."""
    from orcapod.core.nodes.source_node import SourceNodeBase
    from orcapod.core.sources.base import RootSource

    return isinstance(stream, (RootSource, SourceNodeBase))


def _recorded_label(stream: Any) -> str | None:
    """The label *stream*'s step was given, or ``None`` for an unlabelled step."""
    recorded = getattr(stream, _RECORDED_LABEL_ATTRIBUTE, _NOT_RECORDED)
    if recorded is not _NOT_RECORDED:
        return cast("str | None", recorded)
    return stream.label if stream.has_assigned_label else None


def _invocation_of(stream: Any, producer: Any, label: str | None) -> PodInvocation:
    """The invocation that made *stream*, classified by its producer."""
    from orcapod.core.function_pod import FunctionPodStream
    from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
    from orcapod.side_effects import SideEffectPod

    inputs = tuple(stream.upstreams)
    if isinstance(producer, SideEffectPod):
        return SideEffectInvocation(pod=producer, input_streams=inputs, label=label)
    if isinstance(producer, StaticOutputOperatorPod):
        return OperatorInvocation(pod=producer, input_streams=inputs, label=label)
    if isinstance(stream, (FunctionPodStream, FunctionNodeBase)):
        return FunctionInvocation(pod=producer, input_streams=inputs, label=label)
    raise TypeError(
        f"Cannot bring {stream.label!r} into a pipeline: its producer "
        f"{type(producer).__name__} is neither a function pod, a side-effect pod nor "
        "an operator."
    )


def _is_node(stream: Any) -> bool:
    """True for a function, operator or side-effect node of a pipeline or job."""
    from orcapod.core.nodes.operator_node import OperatorNode
    from orcapod.side_effects import SideEffectNode

    return isinstance(stream, (FunctionNodeBase, OperatorNode, SideEffectNode))


def _unattached_message(stream: Any) -> str:
    """The refusal of a node whose pod is not attached (a node of a loaded job)."""
    return (
        f"The function of step {stream.label!r} is not attached (it was not "
        "loadable, or not loaded), so the step cannot be brought into a pipeline "
        "yet. For its stored results use node.as_source()."
    )


def _with_label(invocation: PodInvocation, label: str | None) -> PodInvocation:
    """*invocation* with another label."""
    return type(invocation)(
        pod=invocation.pod, input_streams=invocation.input_streams, label=label
    )


def _computed_label(invocation: PodInvocation) -> str | None:
    """The label an unlabelled step's node takes: its pod's label."""
    return getattr(invocation.pod, "label", None)
