"""Synchronous pipeline orchestrator.

Walks a compiled pipeline's node graph topologically, delegating to each
node's ``execute()`` method with observer injection.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any, Literal

from orcapod.core.nodes._execution_cache import execute_node, verified_graph_execution
from orcapod.errors import StoreError, TypeIdentityError
from orcapod.pipeline.result import OrchestratorResult
from orcapod.utils import arrow_utils
from orcapod.utils.store_errors import keep_store_error
from orcapod.protocols.node_protocols import (
    is_function_node,
    is_operator_node,
    is_side_effect_function_node,
    is_side_effect_node,
    is_source_node,
)

if TYPE_CHECKING:
    from orcapod.pipeline.dag import GraphProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol

logger = logging.getLogger(__name__)


class SyncPipelineOrchestrator:
    """Execute a compiled pipeline synchronously via node ``execute()`` methods.

    Walks the node graph in topological order. For each node, delegates
    to ``node.execute(observer=...)`` which owns all per-data logic,
    cache lookups, and observer hooks internally.

    The orchestrator is responsible only for topological ordering,
    buffer management, and stream materialization between nodes.
    """

    @staticmethod
    def _execution_snapshot_types() -> dict[str, tuple[type, ...]]:
        """Known immutable source implementations and completion-aware node kinds."""
        from orcapod.core.nodes import FunctionJobNode, OperatorJobNode, SourceJobNode
        from orcapod.core.sources import ArrowTableSource, DictSource, ListSource
        from orcapod.core.sources.spiraldb_table_source import SpiralDBTableSource
        from orcapod.core.streams.arrow_table_stream import ArrowTableStream

        return {
            "source_node": (SourceJobNode,),
            "computed": (FunctionJobNode, OperatorJobNode),
            "arrow": (ArrowTableStream,),
            "memory_source": (ArrowTableSource, DictSource, ListSource),
            "spiral": (SpiralDBTableSource,),
        }

    def __init__(
        self,
        error_policy: Literal["continue", "fail_fast"] = "continue",
    ) -> None:
        self._error_policy = error_policy

    @verified_graph_execution
    def run(
        self,
        graph: GraphProtocol[Any],
        *,
        observer: ExecutionObserverProtocol | None = None,
        materialize_results: bool = True,
        run_id: str | None = None,
        pipeline_uri: str = "",
    ) -> OrchestratorResult:
        """Execute the node graph synchronously.

        Args:
            graph: A ``GraphProtocol`` DAG with GraphNode objects as vertices.
            observer: Optional execution observer forwarded to nodes.
            materialize_results: If True, keep all node outputs in memory
                and return them. If False, discard buffers after downstream
                consumption (only DB-persisted results survive).
            run_id: Optional run identifier.  If not provided, a UUID is
                generated automatically.
            pipeline_uri: Opaque URI string identifying this pipeline run
                (e.g. ``"my_pipeline@a1b2c3d4e5f6a1b2"``).  Forwarded
                verbatim to ``observer.on_run_start``.

        Returns:
            OrchestratorResult with node outputs.
        """
        from orcapod.pipeline.observer import NoOpObserver

        run_id = run_id or str(uuid.uuid4())
        effective_observer = observer if observer is not None else NoOpObserver()
        effective_observer.on_run_start(run_id, pipeline_uri=pipeline_uri)

        store_error: StoreError | TypeIdentityError | None = None
        current: Any = None
        try:
            topo_order = list(graph.topological_sort())
            buffers: dict[Any, list[tuple[TagProtocol, DataProtocol]]] = {}
            processed: set[Any] = set()

            for node in topo_order:
                current = node
                if is_source_node(node):
                    buffers[node] = execute_node(
                        node, node.execute, observer=effective_observer
                    )
                elif is_function_node(node) or is_side_effect_function_node(node):
                    upstream_buf = self._gather_upstream(node, graph, buffers)
                    upstream_node = list(graph.predecessors(node))[0]
                    input_stream = self._materialize_as_stream(
                        upstream_buf, upstream_node
                    )
                    buffers[node] = execute_node(
                        node,
                        node.execute,
                        input_stream,
                        observer=effective_observer,
                        error_policy=self._error_policy,
                        run_id=run_id,
                    )
                elif is_operator_node(node):
                    upstream_buffers = self._gather_upstream_multi(node, graph, buffers)
                    input_streams = [
                        self._materialize_as_stream(buf, upstream_node)
                        for buf, upstream_node in upstream_buffers
                    ]
                    buffers[node] = execute_node(
                        node, node.execute, *input_streams, observer=effective_observer
                    )
                elif is_side_effect_node(node):
                    upstream_buf = self._gather_upstream(node, graph, buffers)
                    upstream_node = list(graph.predecessors(node))[0]
                    input_stream = self._materialize_as_stream(
                        upstream_buf, upstream_node
                    )
                    buffers[node] = execute_node(
                        node,
                        node.execute,
                        input_stream,
                        observer=effective_observer,
                        run_id=run_id,
                    )
                else:
                    raise TypeError(
                        f"Unknown node type: {getattr(node, 'node_type', None)!r}"
                    )

                processed.add(node)

                if not materialize_results:
                    self._gc_buffers(node, graph, buffers, processed)

            if not materialize_results:
                buffers.clear()

            return OrchestratorResult(node_outputs=buffers)
        except StoreError as exc:
            # Name the node the failure belongs to, if the raising site did not.
            if current is not None:
                exc.add_context(node_label=current.label)
            store_error = exc
            raise
        except TypeIdentityError as exc:
            # A stored row that cannot be decoded as its class stops the run as a
            # store error does; on_run_end must not replace it (ITL-881).
            store_error = exc
            raise
        finally:
            # A failing on_run_end must not replace a failed store write.
            with keep_store_error(store_error, "Observer on_run_end"):
                effective_observer.on_run_end(run_id)

    @staticmethod
    def _gather_upstream(
        node: Any,
        graph: "GraphProtocol[Any]",
        buffers: dict[Any, list[tuple[Any, Any]]],
    ) -> list[tuple[Any, Any]]:
        """Gather a single upstream buffer (for function nodes)."""
        predecessors = list(graph.predecessors(node))
        if len(predecessors) != 1:
            raise ValueError(
                f"FunctionNode expects exactly 1 upstream, got {len(predecessors)}"
            )
        return buffers[predecessors[0]]

    @staticmethod
    def _gather_upstream_multi(
        node: Any,
        graph: "GraphProtocol[Any]",
        buffers: dict[Any, list[tuple[Any, Any]]],
    ) -> list[tuple[list[tuple[Any, Any]], Any]]:
        """Gather multiple upstream buffers with their nodes (for operators).

        Returns list of (buffer, upstream_node) tuples preserving the
        order that matches the operator's input_streams order.
        """
        predecessors = list(graph.predecessors(node))
        upstream_order = {id(upstream): i for i, upstream in enumerate(node.upstreams)}
        sorted_preds = sorted(
            predecessors,
            key=lambda p: upstream_order.get(id(p), 0),
        )
        return [(buffers[p], p) for p in sorted_preds]

    @staticmethod
    def _materialize_as_stream(buf: list[tuple[Any, Any]], upstream_node: Any) -> Any:
        """Wrap a (tag, data) buffer as an ArrowTableStream.

        Builds the table with ``StaticOutputOperatorPod._rows_to_table``, as
        ``StaticOutputOperatorPod._materialize_to_stream`` does, so every row
        keeps its own system tags and source info.

        Args:
            buf: List of (tag, data) tuples.
            upstream_node: The node that produced this buffer (used to
                determine tag column names).

        Returns:
            An ArrowTableStream.
        """
        from orcapod.core.streams.arrow_table_stream import (
            ArrowTableStream,
            stored_data,
            stored_tag,
        )

        if not buf:
            clear_rows = getattr(upstream_node, "clear_retained_rows", None)
            if clear_rows is not None:
                clear_rows()
            # Build an empty stream preserving declared field nullability.
            tag_schema, data_schema = upstream_node.output_schema(
                columns={"system_tags": True, "source": True}
            )
            type_converter = upstream_node.data_context.type_converter
            empty_table = arrow_utils.make_empty_table(
                {**tag_schema, **data_schema}, type_converter
            )
            tag_keys = upstream_node.keys()[0]
            return ArrowTableStream(
                empty_table,
                tag_columns=tag_keys,
                producer=upstream_node.producer,
                upstreams=upstream_node.upstreams,
                data_context=upstream_node.data_context,
            )

        retain_rows = getattr(upstream_node, "retain_storage_rows", None)
        return ArrowTableStream.from_rows(
            retain_rows(buf, tag_view=stored_tag, data_view=stored_data)
            if retain_rows is not None
            else buf,
            producer=upstream_node.producer,
            upstreams=upstream_node.upstreams,
            data_context=upstream_node.data_context,
        )

    @staticmethod
    def _gc_buffers(
        current_node: Any,
        graph: "GraphProtocol[Any]",
        buffers: dict[Any, list[tuple[Any, Any]]],
        processed: set[Any],
    ) -> None:
        """Discard buffers no longer needed by any unprocessed downstream."""
        for pred in graph.predecessors(current_node):
            if pred not in buffers:
                continue
            all_successors_done = all(
                succ in processed for succ in graph.successors(pred)
            )
            if all_successors_done:
                del buffers[pred]
