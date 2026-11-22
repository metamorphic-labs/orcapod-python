"""Synchronous pipeline orchestrator.

Walks a compiled pipeline's node graph topologically, delegating to each
node's ``execute()`` method with observer injection.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any, Literal

from orcapod.errors import OrcapodDatabaseError, TypeIdentityError
from orcapod.pipeline.result import OrchestratorResult
from orcapod.utils.database_errors import keep_database_error
from orcapod.protocols.node_protocols import (
    is_function_node,
    is_operator_node,
    is_side_effect_function_node,
    is_side_effect_node,
    is_source_node,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from orcapod.pipeline.dag import GraphProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
    from orcapod.protocols.core_protocols import StreamProtocol

logger = logging.getLogger(__name__)


class SyncPipelineOrchestrator:
    """Execute a compiled pipeline synchronously via node ``execute()`` methods.

    Walks the node graph in topological order. For each node, delegates
    to ``node.execute(observer=...)`` which owns all per-data logic,
    cache lookups, and observer hooks internally.

    The orchestrator is responsible only for topological ordering and for handing
    each node's output stream to its successors: a node's ``execute`` returns the
    stream every successor reads (ITL-1347, stage 2).
    """

    def __init__(
        self,
        error_policy: Literal["continue", "fail_fast"] = "continue",
    ) -> None:
        self._error_policy = error_policy

    def run(
        self,
        graph: GraphProtocol[Any],
        *,
        observer: ExecutionObserverProtocol | None = None,
        materialize_results: bool = True,
        run_id: str | None = None,
        pipeline_uri: str = "",
        load_only: bool = False,
        held: Mapping[Any, StreamProtocol] | None = None,
    ) -> OrchestratorResult:
        """Execute the node graph synchronously.

        Args:
            graph: A ``GraphProtocol`` DAG with GraphNode objects as vertices.
            observer: Optional execution observer forwarded to nodes.
            materialize_results: If True, keep every node's output stream and return
                them. If False, drop each one once its successors have run (only
                DB-persisted results survive).
            run_id: Optional run identifier.  If not provided, a UUID is
                generated automatically.
            pipeline_uri: Opaque URI string identifying this pipeline run
                (e.g. ``"my_pipeline@a1b2c3d4e5f6a1b2"``).  Forwarded
                verbatim to ``observer.on_run_start``.
            load_only: Fill the nodes from what is stored and compute nothing
                (ITL-1095): function nodes load their inputs' stored results and skip
                the others, operators run without storing, and a side-effect node
                passes its rows on and delivers nothing.
            held: Nodes of ``graph`` that are not run: each one's output stream (of
                its current run, or of a stream outside the job) feeds its successors
                as if it had just run. Used by ``node.run()``, which runs only the
                nodes it depends on that hold no current run (ITL-1095).

        Returns:
            OrchestratorResult with each node's output stream.
        """
        from orcapod.pipeline.observer import NoOpObserver

        run_id = run_id or str(uuid.uuid4())
        effective_observer = observer if observer is not None else NoOpObserver()
        effective_observer.on_run_start(run_id, pipeline_uri=pipeline_uri)

        database_error: OrcapodDatabaseError | TypeIdentityError | None = None
        current: Any = None
        try:
            topo_order = list(graph.topological_sort())
            outputs: dict[Any, StreamProtocol] = {}
            processed: set[Any] = set()

            held_streams = held if held is not None else {}
            for node in topo_order:
                current = node
                if node in held_streams:
                    outputs[node] = held_streams[node]
                elif is_source_node(node):
                    outputs[node] = node.execute(observer=effective_observer)
                elif is_function_node(node) or is_side_effect_function_node(node):
                    input_stream = self._single_input(node, graph, outputs)
                    outputs[node] = node.execute(
                        input_stream,
                        observer=effective_observer,
                        error_policy=self._error_policy,
                        run_id=run_id,
                        load_only=load_only,
                    )
                elif is_operator_node(node):
                    input_streams = self._ordered_inputs(node, graph, outputs)
                    outputs[node] = node.execute(
                        *input_streams, observer=effective_observer, load_only=load_only
                    )
                elif is_side_effect_node(node):
                    input_stream = self._single_input(node, graph, outputs)
                    # In a load-only run the node passes its rows on and delivers nothing.
                    outputs[node] = node.execute(
                        input_stream,
                        observer=effective_observer,
                        run_id=run_id,
                        load_only=load_only,
                    )
                else:
                    raise TypeError(
                        f"Unknown node type: {getattr(node, 'node_type', None)!r}"
                    )

                processed.add(node)

                if not materialize_results:
                    self._gc_outputs(node, graph, outputs, processed)

            if not materialize_results:
                outputs.clear()

            return OrchestratorResult(node_outputs=outputs)
        except OrcapodDatabaseError as exc:
            # Name the node the failure belongs to, if the raising site did not.
            if current is not None:
                exc.add_context(node_label=current.label)
            database_error = exc
            raise
        except TypeIdentityError as exc:
            # A stored row that cannot be decoded as its class stops the run as a
            # database error does; on_run_end must not replace it (ITL-881).
            database_error = exc
            raise
        finally:
            # A failing on_run_end must not replace a failed database write.
            with keep_database_error(database_error, "Observer on_run_end"):
                effective_observer.on_run_end(run_id)

    @staticmethod
    def _single_input(
        node: Any,
        graph: "GraphProtocol[Any]",
        outputs: dict[Any, StreamProtocol],
    ) -> StreamProtocol:
        """The output stream of a node's one predecessor (function and side-effect nodes)."""
        predecessors = list(graph.predecessors(node))
        if len(predecessors) != 1:
            raise ValueError(
                f"FunctionNode expects exactly 1 upstream, got {len(predecessors)}"
            )
        from orcapod.core.operators.static_output_pod import at_edge

        return at_edge(outputs[predecessors[0]])

    @staticmethod
    def _ordered_inputs(
        node: Any,
        graph: "GraphProtocol[Any]",
        outputs: dict[Any, StreamProtocol],
    ) -> list[StreamProtocol]:
        """The output streams of an operator node's predecessors, in ``node.upstreams`` order."""
        predecessors = list(graph.predecessors(node))
        upstream_order = {id(upstream): i for i, upstream in enumerate(node.upstreams)}
        sorted_preds = sorted(
            predecessors,
            key=lambda p: upstream_order.get(id(p), 0),
        )
        from orcapod.core.operators.static_output_pod import at_edge

        return [at_edge(outputs[p]) for p in sorted_preds]

    @staticmethod
    def _gc_outputs(
        current_node: Any,
        graph: "GraphProtocol[Any]",
        outputs: dict[Any, StreamProtocol],
        processed: set[Any],
    ) -> None:
        """Drop output streams no unprocessed successor still needs."""
        for pred in graph.predecessors(current_node):
            if pred not in outputs:
                continue
            all_successors_done = all(
                succ in processed for succ in graph.successors(pred)
            )
            if all_successors_done:
                del outputs[pred]
