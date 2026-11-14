"""``node.run()`` of a job node: run it and the job nodes it depends on (ITL-1095).

Developer, November 13, 2026: "given that the initially loaded job is empty, I think it's
fair that node.run will cascade node.run on all nodes it depends on recursively.
Critically, node.run(load_only=True) would trigger the upstream node.run also with
load_only=True." And: "if it's not stale, it shouldn't even trigger a run and simply
return what's in the stream cached already."

The cascade is ``job.run()`` restricted to the node's dependencies, on the synchronous
orchestrator: a dependency that holds a current run is not run again (its rows feed the
nodes after it), every other one runs once, in dependency order, and the run ends with
the commit ``job.run()`` makes.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from orcapod.errors import StoreError, StoreWriteError
from orcapod.pipeline.run_summary import (
    RunCountingObserver,
    RunSummary,
    run_node_summaries,
)
from orcapod.utils.store_errors import flush_best_effort

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol


def _is_job_node(stream: Any) -> bool:
    """Whether ``stream`` is a node of a job (as opposed to a stream outside any job)."""
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.nodes.staleness import RunHoldingNode
    from orcapod.side_effects import SideEffectJobNode

    return isinstance(stream, (RunHoldingNode, SideEffectJobNode, SourceJobNode))


def depends_on_job_nodes(node: Any) -> bool:
    """Whether any direct input of ``node`` is a job node.

    A node built directly over streams outside a job (a source, a pod's stream) has
    nothing to run first: its ``run()`` reads its input as it always has.
    """
    return any(_is_job_node(upstream) for upstream in node.upstreams)


def _stores(nodes: list[Any]) -> list[ArrowDatabaseProtocol]:
    """The distinct stores the given nodes write to: pipeline and result databases."""
    found: list[ArrowDatabaseProtocol] = []
    for node in nodes:
        pipeline_database = getattr(node, "_pipeline_database", None)
        cached_pod = getattr(node, "_cached_function_pod", None)
        result_database = getattr(cached_pod, "result_database", None)
        for database in (pipeline_database, result_database):
            if database is not None and not any(database is d for d in found):
                found.append(database)
    return found


def _job_name(node: Any) -> str:
    """The name of the job ``node`` belongs to: its pipeline database's location."""
    database = getattr(node, "_pipeline_database", None)
    base_path: tuple[str, ...] = getattr(database, "base_path", ())
    return "/".join(base_path)


def clear_run_rows(nodes: Iterable[Any]) -> None:
    """Drop the rows of a run whose end-of-run commit failed from every node it filled.

    A run that raised defines no rows (ITL-1093): each function and operator job node of
    the run is cleared (not run, stale) before the commit's error propagates. What was
    committed stays stored. A load-only run writes nothing, so its commit has nothing
    to fail on.

    Args:
        nodes: The nodes of the run.
    """
    from orcapod.core.nodes.staleness import RunHoldingNode

    for node in nodes:
        if isinstance(node, RunHoldingNode):
            node.clear_cache()  # pyright: ignore[reportAttributeAccessIssue]  # every RunHoldingNode is a job node with clear_cache


def run_with_dependencies(target: Any, *, load_only: bool) -> RunSummary:
    """Run ``target`` and every job node it depends on that holds no current run.

    The dependencies are found through ``upstreams``. A dependency that holds a current
    run (``holds_current_run``) is not run: its rows feed the nodes after it. A stream
    outside the job is read. Every other node runs once, in dependency order, on the
    ``SyncPipelineOrchestrator`` (error policy ``"continue"``), as ``job.run()`` would
    run that sub-graph; with ``load_only`` as ``job.run(load_only=True)`` would. The run
    ends with a commit of every store it wrote to, also, best-effort, when it fails.

    Args:
        target: The job node whose ``run()`` this is.
        load_only: Whether to load stored results only.

    Returns:
        The run's summary, one entry per function node that ran.

    Raises:
        StoreWriteError: If committing the run's writes failed.
    """
    from orcapod.core.nodes.source_node import SourceJobNode
    from orcapod.core.nodes.staleness import RunHoldingNode
    from orcapod.pipeline.dag import OrcaDAG
    from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

    dag: OrcaDAG[Any] = OrcaDAG()
    held: dict[Any, list[Any]] = {}
    seen: list[Any] = []

    def visit(node: Any) -> None:
        """Add ``node`` and, unless it is held or a source, what it depends on."""
        if any(node is other for other in seen):
            return
        seen.append(node)
        dag.add_node(node)
        if (
            node is not target
            and isinstance(node, RunHoldingNode)
            and node.holds_current_run(load_only=load_only)
        ):
            held[node] = list(node.iter_data())
            return
        if isinstance(node, SourceJobNode):
            return
        if not _is_job_node(node):
            # A stream outside the job: its rows, read as the node's run reads them.
            held[node] = list(node.iter_data())
            return
        for upstream in node.upstreams:
            visit(upstream)
            dag.add_edge(upstream, node)

    visit(target)
    run_nodes = [node for node in seen if not any(node is h for h in held)]
    stores = _stores(run_nodes)
    counter = RunCountingObserver()
    try:
        _ = SyncPipelineOrchestrator().run(
            dag, observer=counter, load_only=load_only, held_rows=held
        )
    except Exception as run_error:
        # Commit the writes that succeeded before the failure, as job.run() does.
        flush_best_effort(run_error, *stores)
        raise
    for store in stores:
        try:
            store.flush()
        except StoreError:
            # A run whose commit failed did not complete: no node holds its rows.
            clear_run_rows(run_nodes)
            raise
        except Exception as exc:
            clear_run_rows(run_nodes)
            raise StoreWriteError(store.base_path, flush=True) from exc
    return RunSummary(
        _job_name(target),
        run_node_summaries(dag, counter, skip=list(held)),
        load_only=load_only,
        target=target.label,
    )
