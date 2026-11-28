"""PipelineJob — pipeline + source bindings + execution context."""

from __future__ import annotations

import contextlib
import enum
import logging
import sys
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import orcapod
from orcapod.core.nodes import JobNode
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.nodes.source_node import SourceJobNode
from orcapod.core.sources.derived_source import DerivedSource
from orcapod.core.streams.base import StreamBase
from orcapod.side_effects import SideEffectJobNode, SideEffectNode
from orcapod.core.tracker import AutoRegisteringContextBasedTracker
from orcapod.errors import (
    InconsistentSourceError,
    OrcapodDatabaseError,
    OrcapodDatabaseReadError,
    OrcapodDatabaseWriteError,
)
from orcapod.pipeline.base import AbstractPipelineBase
from orcapod.pipeline.dag import OrcaDAG
from orcapod.pipeline.node_run import (
    RunPlan,
    check_sources,
    clear_run_rows,
    plan_run,
)
from orcapod.pipeline.restrictions import (
    refuse_restricted_operator_caches,
    refuse_restriction_conflicts,
    restrictions_in_force,
    warn_restricted_aggregation,
)
from orcapod.pipeline.run_summary import (
    RunCountingObserver,
    RunSummary,
    run_node_summaries,
    warn_row_failures,
)
from orcapod.pipeline.source_consistency import (
    invocation_source_uses,
    require_consistent_sources,
)
from orcapod.protocols import core_protocols as cp
from orcapod.databases.utils import as_table_committing
from orcapod.types import NodeConfig
from orcapod.utils.database_errors import database_write_path, flush_best_effort

if TYPE_CHECKING:
    from pathlib import Path

    from orcapod.core.nodes.source_node import _DeclaredForm  # pyright: ignore[reportPrivateUsage]  # shared binding transaction

    from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
    from orcapod.pipeline.execution_context import ExecutionContext
    from orcapod.pipeline.graph import Pipeline
    from orcapod.pipeline.pod_invocation import PodInvocation
    from orcapod.pipeline.restrictions import Rebinding
    from orcapod.pipeline.source_consistency import SourceUse
    from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol

logger = logging.getLogger(__name__)


class _Unchanged(enum.Enum):
    """The default of ``PipelineJob.bind(result_database=...)``: keep the binding.

    ``None`` there means something else: store results in the job's own database
    again (ITL-1408).
    """

    UNCHANGED = "unchanged"


_PENDING_BEFORE_RUN_NOTE = (
    "These writes were left pending in the database before this run started (written "
    "without a flush, or by a run that failed). The run stopped before computing "
    "anything."
)


@dataclass(frozen=True)
class _PreparedRun:
    """What one run of a job needs, built by ``PipelineJob._prepare_run``.

    Attributes:
        exec_dag: The nodes to run (unbound sources and their dependents left out).
        run_id: The run's identifier.
        pipeline_uri: ``<job name>@<snapshot hash>``, passed to the observer.
        pipeline_db: The job's view of the database.
        result_db: The job's function-result view of the database.
        plan: What the run runs and holds (``plan_run``, ITL-1095).
        unchecked_sources: The sources the start-of-run check could not check
            (``check_sources``, ITL-1102).
    """

    exec_dag: OrcaDAG[Any]
    run_id: str
    pipeline_uri: str
    pipeline_db: "ArrowDatabaseProtocol"
    result_db: "ArrowDatabaseProtocol"
    plan: "RunPlan"
    unchecked_sources: tuple[str, ...]


def _with_counter(
    observer: "ExecutionObserverProtocol | None", counter: RunCountingObserver
) -> "ExecutionObserverProtocol":
    """The observer a run passes to its orchestrator: the user's, plus the row counter.

    Args:
        observer: The observer the user passed to ``run``, if any.
        counter: The run's row counter.

    Returns:
        ``counter`` alone, or a ``CompositeObserver`` calling ``observer`` then ``counter``.
    """
    if observer is None:
        return counter
    from orcapod.pipeline.composite_observer import CompositeObserver

    return CompositeObserver(observer, counter)  # pyright: ignore[reportReturnType]  # forwards every hook; its **kwargs signatures do not match the protocol structurally


class PipelineJob(AbstractPipelineBase[JobNode]):
    """Pipeline + source bindings + execution context.

    ``PipelineJob`` is the everyday working object. It is built incrementally:
    its ``with``-block records both the DAG structure and any concrete source
    bindings simultaneously. Concrete sources are automatically promoted to
    ``SourceJobNode`` declarations in the underlying graph, with their
    concrete instances stored in ``job.sources``.

    After the ``with`` block, ``job.pipeline`` returns a lightweight
    ``Pipeline`` blueprint (SourceNode-only leaves). ``job.run()`` executes
    the resolvable subgraph — nodes whose upstream SourceNodes are all bound.

    ``PipelineJob`` can also be created from a ``Pipeline`` via
    ``PipelineJob.from_pipeline(pipeline, sources=..., database=...)`` for the
    "explicit blueprint" workflow.

    Args:
        name: Pipeline name (string or tuple). Used as the path prefix for
            all cache/pipeline paths when the pipeline is run.
        database: Database for the job's records: pipeline records, operator
            caches, side-effect logs and, unless ``result_database`` is given,
            function results (under ``<name>/_result``).
        execution_context: Optional execution configuration.
        tracker_manager: Optional tracker manager override.
        auto_compile: If ``True`` (default), ``compile()`` is called
            automatically when the context manager exits.
        result_database: Optional database for function results, used as given (no
            job name is added to its paths), so jobs that share it share function
            results (ITL-1408). ``None`` (the default) stores them under the job's
            own database, at ``<name>/_result``.
    """

    # ------------------------------------------------------------------
    # Node-factory class attributes (used by AbstractPipelineBase.compile())
    # ------------------------------------------------------------------

    source_node_class = SourceJobNode
    function_node_class = FunctionJobNode
    operator_node_class = OperatorJobNode
    side_effect_node_class = SideEffectJobNode

    def __init__(
        self,
        name: str | tuple[str, ...] = "pipeline",
        database: "ArrowDatabaseProtocol | None" = None,
        execution_context: "ExecutionContext | None" = None,
        tracker_manager: cp.TrackerManagerProtocol | None = None,
        auto_compile: bool = True,
        *,
        result_database: "ArrowDatabaseProtocol | None" = None,
    ) -> None:
        super().__init__(name=name, tracker_manager=tracker_manager)
        self._database = database
        self._result_database: "ArrowDatabaseProtocol | None" = result_database
        # For a loaded job whose file names a separate result database that is not
        # available (it could not be rebuilt, or it was in memory) and none was given:
        # that saved configuration. Its function nodes get no database rather than the
        # job's own ``_result`` location, which holds none of their results, and a save
        # writes the configuration back as it was (ITL-1408).
        self._unresolved_result_database: dict[str, Any] | None = None
        self._execution_context = execution_context
        self._auto_compile = auto_compile
        # name → bound concrete source (user-facing API: job.sources).
        self._sources: dict[str, cp.StreamProtocol] = {}
        self._rebound_source_keys: set[str] = set()
        self._sources_as_captured: dict[str, cp.StreamProtocol | None] = {}
        self._has_run: bool = False
        # True only while the last run started has finished and committed its writes:
        # what the database then holds for each node is what that run left (ITL-1043).
        self._last_run_finished: bool = False
        self._run_id: str | None = None
        # The summary of the last run this object completed (ITL-997).
        self._last_run: RunSummary | None = None
        # True for a job built by load(): it reads stored results and never runs.
        self._loaded: bool = False

        # Lazy cache for the corresponding Pipeline blueprint.
        # Set to None after compile() to force recomputation on next access.
        # Pre-set by from_pipeline() via _set_compiled_pipeline().
        self._compiled_pipeline: Pipeline | None = None

    # ------------------------------------------------------------------
    # Context manager — respects auto_compile flag
    # ------------------------------------------------------------------

    def __exit__(self, exc_type=None, exc_value=None, traceback=None) -> None:
        """Exit the recording context, compiling if no exception occurred."""
        AutoRegisteringContextBasedTracker.__exit__(
            self, exc_type, exc_value, traceback
        )
        if exc_type is None and self._auto_compile:
            self.compile()

    def prepare_invocation(self, invocation: PodInvocation) -> Callable[[], None]:
        """Prepare a step only while every compiled slot has its original binding."""
        # ITL-1559: refuse additive capture until the relationship between captured
        # objects and slots is defined. Check at recording so a bind inside an
        # open capture context cannot bypass the rule.
        changed = sorted(
            {
                node.name
                for key, node in self._persistent_node_map.items()
                if isinstance(node, SourceJobNode)
                and key in self._sources_as_captured
                and node.bound_source is not self._sources_as_captured[key]
            }
        )
        if changed:
            names = ", ".join(repr(name) for name in changed)
            raise InconsistentSourceError(
                f"Cannot record a step because these job slots are not as captured: "
                f"{names}. Bind them back to their original objects or build a new "
                "job (ITL-1559)."
            )
        if any(
            isinstance(stream, SourceJobNode) for stream in invocation.input_streams
        ):
            # Each job owns its slots. Keep the binding at capture, even if the
            # supplying slot changes before this job first compiles.
            invocation = type(invocation)(
                pod=invocation.pod,
                input_streams=tuple(
                    SourceJobNode.from_stream(stream)
                    if isinstance(stream, SourceJobNode)
                    else stream
                    for stream in invocation.input_streams
                ),
                label=invocation.label,
            )
        return super().prepare_invocation(invocation)

    def _source_uses(self, extra: PodInvocation | None = None) -> Iterator[SourceUse]:
        """Validate actual graph leaves after a binding replaces a captured input."""
        if not self._rebound_source_keys:
            yield from super()._source_uses(extra)
            return
        # A recorded operator's Python stream still holds its old upstreams.
        # Compilation connects its recorded keys to the job's actual leaves.
        for key, stream in self._source_streams.items():
            if key in self._invocation_lut:
                continue
            slot = self._persistent_node_map.get(key)
            if key in self._rebound_source_keys and isinstance(slot, SourceJobNode):
                yield f"node {slot.label!r}", slot
            else:
                yield f"source {stream.label!r}", stream
        if extra is not None:
            yield from invocation_source_uses(extra)

    def _source_node_from_recording(self, key: str) -> SourceJobNode:
        """Preserve an explicit binding when recompiling its slot."""
        if key in self._rebound_source_keys:
            slot = self._persistent_node_map.get(key)
            if isinstance(slot, SourceJobNode):
                return self.source_node_class.from_stream(slot)
        return self.source_node_class.from_stream(self._source_streams[key])

    def compile(self) -> None:
        """Compile recorded invocations into a frozen DAG of job nodes.

        Delegates to ``AbstractPipelineBase.compile()`` which uses
        ``source_node_class``, ``function_node_class``, and
        ``operator_node_class`` to create job node instances.

        Concrete source bindings already in ``_sources`` are preserved.
        Any ``SourceJobNode`` in ``_persistent_node_map`` that has a bound
        source also contributes to ``_sources`` (covers both the
        direct-recording path and the ``bind()``-before-compile path).

        The lazy ``compiled_pipeline`` cache is invalidated.

        If ``_database`` is set, distributes databases to all function and
        operator job nodes.
        """
        previous_slots = [
            node
            for node in self._persistent_node_map.values()
            if isinstance(node, SourceJobNode)
        ]
        super().compile()

        for slot in previous_slots:
            slot._set_binding_owner(None)  # pyright: ignore[reportPrivateUsage]  # the replaced slot no longer belongs to this job
        previous_sources = self._sources
        self._sources = {}
        for key, node in self._persistent_node_map.items():
            if isinstance(node, SourceJobNode):
                if not self._loaded:
                    self._sources_as_captured.setdefault(key, node.bound_source)
                node._set_binding_owner(self)  # pyright: ignore[reportPrivateUsage]  # setters participate in this job's binding transaction
                if node.bound_source is not None:
                    self._sources[node.name] = node.bound_source
        if self._loaded:
            # Loaded jobs have no recorded invocations (ITL-1560). Preserve the
            # bindings restored from the saved job when recompiling.
            self._sources.update(previous_sources)

        # Invalidate the lazy compiled_pipeline cache.
        self._compiled_pipeline = None

        # Wire databases if a database is already set.
        if self._database is not None:
            self._distribute_databases()

    # ------------------------------------------------------------------
    # compiled_pipeline — lazy cache
    # ------------------------------------------------------------------

    @property
    def last_run(self) -> RunSummary | None:
        """The summary of the last run this job completed, or ``None`` before one.

        The same object ``run`` and ``run_async`` returned: per function node, the rows
        computed, read from the database and failed, and each node's first error. Use
        ``job.last_run.print_traceback("<node label>")`` to see a failed row's full
        traceback. A run that raised leaves it unchanged. A loaded job has none.
        """
        return self._last_run

    @property
    def compiled_pipeline(self) -> "Pipeline":
        """The lightweight ``Pipeline`` blueprint for this job (lazy cache).

        Computed on first access by calling ``as_pipeline()``.  Invalidated
        by ``compile()``, which sets ``_compiled_pipeline`` to ``None`` so the
        next access recomputes a fresh schema-normalised blueprint.

        ``bind()`` does **not** invalidate this cache: the blueprint contains
        only ``SourceNode`` schema-slot declarations with no concrete data, so
        it is unaffected by changes to concrete source bindings.

        Raises:
            RuntimeError: If the job has not been compiled yet.
        """
        if self._compiled_pipeline is None:
            if not self._compiled:
                raise RuntimeError(
                    "No compiled pipeline — use 'with job:' to record a DAG first."
                )
            self._compiled_pipeline = self.as_pipeline()
        return self._compiled_pipeline

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def pipeline(self) -> "Pipeline":
        """The compiled pure Pipeline (SourceNode-only leaves).

        Raises:
            RuntimeError: If the with-block has not been completed yet.
        """
        return self.compiled_pipeline

    @property
    def sources(self) -> dict[str, cp.StreamProtocol]:
        """Mapping of SourceNode name to bound concrete source."""
        return dict(self._sources)

    @property
    def database(self) -> "ArrowDatabaseProtocol | None":
        """The job's database: pipeline records, operator caches, side-effect logs.

        It also holds the function results unless ``result_database`` is set.
        """
        return self._database

    @property
    def result_database(self) -> "ArrowDatabaseProtocol | None":
        """The separate database for function results, or ``None`` when not given.

        ``None`` means the results are stored in the job's own database, under
        ``<name>/_result`` (ITL-1408).
        """
        return self._result_database

    @property
    def execution_context(self) -> "ExecutionContext | None":
        """The execution configuration, or ``None`` if unset."""
        return self._execution_context

    # ------------------------------------------------------------------
    # from_pipeline() — classmethod constructor
    # ------------------------------------------------------------------

    @classmethod
    def from_pipeline(
        cls,
        pipeline: "Pipeline",
        database: "ArrowDatabaseProtocol | None" = None,
        sources: "dict[str, cp.StreamProtocol] | None" = None,
        execution_context: "ExecutionContext | None" = None,
        *,
        result_database: "ArrowDatabaseProtocol | None" = None,
    ) -> "PipelineJob":
        """Create a runnable ``PipelineJob`` from a compiled ``Pipeline``.

        Converts the Pipeline blueprint to an ``InvocationGraph`` via
        ``pipeline.to_invocations()``, then reconstructs a PipelineJob
        using ``from_invocations()`` (which calls ``SourceJobNode.from_stream``
        for each source, creating unbound job nodes).  If *sources* are
        provided, validates and binds them.

        Args:
            pipeline: A compiled ``Pipeline`` (``pipeline._compiled`` must
                be ``True``).
            database: Database for result caching and operator records.
            sources: Mapping of ``SourceNode.name`` → concrete source.
            execution_context: Optional execution configuration.
            result_database: Optional separate database for function results, as
                for the constructor.

        Returns:
            A new ``PipelineJob`` ready to run (or ``bind()`` further).

        Raises:
            ValueError: If *pipeline* has not been compiled.
        """
        from orcapod.core.nodes.source_node import SourceNodeBase

        if not pipeline._compiled:
            raise ValueError(
                "Pipeline must be compiled before creating a PipelineJob from it. "
                "Call pipeline.compile() or use auto_compile=True."
            )

        bound_sources: dict[str, cp.StreamProtocol] = dict(sources or {})

        # Report unknown names against the blueprint. Bind checks the sources.
        if bound_sources:
            source_names = {
                node.name
                for node in pipeline._persistent_node_map.values()
                if isinstance(node, SourceNodeBase)
            }
            unknown = set(bound_sources.keys()) - source_names
            if unknown:
                raise ValueError(
                    f"from_pipeline() received source keys with no matching SourceNode: "
                    f"{sorted(unknown)}. Known names: {sorted(source_names)}"
                )

        # Reconstruct via InvocationGraph (creates unbound SourceJobNodes).
        # Leave the compiled blueprint lazy so as_pipeline() builds it with
        # the schema-based keys of the new job's slots.
        job = cls.from_invocations(pipeline.to_invocations(), name=pipeline._name)
        job._database = database  # pyright: ignore[reportAttributeAccessIssue]  # from_invocations is typed as the base class; the instance is a PipelineJob
        job._execution_context = execution_context
        job._result_database = result_database  # pyright: ignore[reportAttributeAccessIssue]  # as for _database above

        # Bind concrete sources and distribute databases.
        if bound_sources:
            job.bind(sources=bound_sources)
        if database is not None:
            job._distribute_databases()

        return job

    # ------------------------------------------------------------------
    # bind() — mutating
    # ------------------------------------------------------------------

    def bind(
        self,
        sources: "dict[str, cp.StreamProtocol] | None" = None,
        database: "ArrowDatabaseProtocol | None" = None,
        execution_context: "ExecutionContext | None" = None,
        *,
        result_database: "ArrowDatabaseProtocol | None | _Unchanged" = _Unchanged.UNCHANGED,
    ) -> None:
        """Update bindings in place. Returns ``None``.

        Mutating — modifies ``self`` directly. Existing bindings not mentioned
        in this call are preserved.

        When *sources* is provided, each concrete source is validated against
        its matching ``SourceJobNode`` slot schema, then the corresponding
        ``SourceJobNode.bound_source`` is updated in-place. A source fits its slot
        when its tag and data columns have the declared names, roles and Python
        types, so a bind never changes a pipeline hash (ITL-1112). Every source is
        validated before anything is changed: a refused source leaves the job's
        bindings and database as they were.

        When *database* or *result_database* is provided and differs from the
        current one, ``_distribute_databases()`` is called (once a database is set)
        so that all job nodes receive live DB references immediately.

        Args:
            sources: Mapping of ``SourceNode.name`` → concrete source.
            database: Replaces the current database and triggers DB redistribution.
            execution_context: Replaces the current execution context.
            result_database: Replaces the separate database for function results
                (see the constructor) and triggers DB redistribution. Leave it out to
                keep the current binding; pass ``None`` to store function results in
                the job's own database again, at ``<name>/_result`` (ITL-1408).

        Raises:
            InconsistentSourceError: If distinct source objects share a content
                hash, or sources with one ID differ in schema or key columns
                (ITL-1506). The job is left as it was.
            SourceSpecMismatchError: If any source's columns differ from its slot's
                declared names, roles or types.
            ValueError: If a source key has no matching ``SourceJobNode`` slot, or an
                operator node that caches (``LOG`` or ``REPLAY``) would read a
                restricted source, one this bind binds or one already bound
                (ITL-1218); the job is left as it was.
            UnstorableColumnError: If *database* or *result_database* cannot store
                a table of a node (ITL-1162); the job is left as it was.
        """
        from orcapod.core.nodes.source_node import SourceNodeBase

        result_database_given = not isinstance(result_database, _Unchanged)
        database_changed = (
            database is not None and database is not self._database
        ) or (
            result_database_given
            and (
                result_database is not self._result_database
                or self._unresolved_result_database is not None
            )
        )

        if sources is not None:
            source_names = {
                node.name
                for node in (self._persistent_node_map or {}).values()
                if isinstance(node, SourceNodeBase)
            }
            unknown = set(sources.keys()) - source_names
            if unknown:
                raise ValueError(
                    f"bind() received source keys with no matching source slot "
                    f"(SourceJobNode): {sorted(unknown)}. "
                    f"Known slot names: {sorted(source_names)}"
                )

        # A database that cannot store a node's tables is refused before anything
        # changes too (ITL-1162), and so is a caching operator over a restricted
        # source, one this bind binds or one already bound (ITL-1218): a bind of a
        # database alone would otherwise set it and fail while distributing it.
        # The slots this bind changes are the job's own, matched by object: a source
        # being bound may itself be a slot of another job with the same name.
        to_bind = sources or {}
        rebinding = [
            (node, to_bind[node.name])
            for node in (self._persistent_node_map or {}).values()
            if isinstance(node, SourceJobNode) and node.name in to_bind
        ]
        prepared = self._prepare_source_bindings(rebinding)
        new_database = database if database is not None else self._database
        new_result_database = (
            self._result_database
            if isinstance(result_database, _Unchanged)
            else result_database
        )
        if database_changed and new_database is not None:
            self._require_storable_tables(new_database, new_result_database)

        # Every source is validated before anything changes: a refused source leaves
        # the job as it was, its database included (ITL-1112).
        self._database = new_database
        self._result_database = new_result_database
        if result_database_given:
            self._unresolved_result_database = None

        self._apply_source_bindings(prepared)

        if execution_context is not None:
            self._execution_context = execution_context

        if database_changed and self._database is not None:
            self._distribute_databases()

    def _replace_source_binding(
        self, slot: SourceJobNode, source: cp.StreamProtocol | None
    ) -> None:
        """Apply a setter change through the same transaction as bind()."""
        self._apply_source_bindings(self._prepare_source_bindings([(slot, source)]))

    def _prepare_source_bindings(
        self, rebinding: Rebinding
    ) -> list[tuple[SourceJobNode, cp.StreamProtocol | None, _DeclaredForm | None]]:
        """Validate the prospective job and prepare each changed slot's row form."""
        nodes = self._persistent_node_map.values()
        require_consistent_sources(
            ((f"node {node.label!r}", node) for node in nodes), rebinding
        )
        prepared = [
            (
                slot,
                source,
                slot._declared_form_for(source) if source is not None else None,  # pyright: ignore[reportPrivateUsage]  # validate the exact binding before committing it
            )
            for slot, source in rebinding
        ]
        refuse_restriction_conflicts(nodes, rebinding=rebinding)
        return prepared

    def _apply_source_bindings(
        self,
        prepared: Sequence[
            tuple[SourceJobNode, cp.StreamProtocol | None, _DeclaredForm | None]
        ],
    ) -> None:
        """Commit validated slot replacements and invalidate dependent predictions."""
        if not prepared:
            return
        keys = {id(node): key for key, node in self._persistent_node_map.items()}
        for slot, source, form in prepared:
            slot._apply_binding(source, form)  # pyright: ignore[reportPrivateUsage]  # install the validated row form
            slot._update_modified_time()  # pyright: ignore[reportPrivateUsage]  # the job owns the slot's binding
            self._rebound_source_keys.add(keys[id(slot)])
            if source is None:
                self._sources.pop(slot.name, None)
            else:
                self._sources[slot.name] = source

        # A binding preserves the declared schema. Clear predictions anyway so
        # that each node reads the schema from its current inputs (ITL-1111).
        for node in self._persistent_node_map.values():
            if isinstance(node, StreamBase):
                node._forget_output_schema()  # pyright: ignore[reportPrivateUsage]  # the job owns its nodes' bindings

    # ------------------------------------------------------------------
    # _distribute_databases()
    # ------------------------------------------------------------------

    def _database_views(
        self,
        database: "ArrowDatabaseProtocol",
        result_database: "ArrowDatabaseProtocol | None",
    ) -> "tuple[ArrowDatabaseProtocol, ArrowDatabaseProtocol]":
        """The job's two views: its pipeline database and its function-result database.

        The one place that decides where a job stores (ITL-1408): the pipeline view is
        ``database`` under the job's name; function results go to ``result_database``
        as given, or, when none is given, under the pipeline view at ``_result``.

        Args:
            database: The job's database.
            result_database: The separate database for function results, if any.

        Returns:
            ``(pipeline view, result view)``.
        """
        pipeline_db = database.at(*self._name)
        if result_database is not None:
            return pipeline_db, result_database
        return pipeline_db, pipeline_db.at("_result")

    def _require_storable_tables(
        self,
        database: "ArrowDatabaseProtocol",
        result_database: "ArrowDatabaseProtocol | None",
    ) -> None:
        """Refuse databases that cannot store a table of one of the job's nodes.

        The same views as ``_distribute_databases`` gives the nodes. Nothing
        changes (ITL-1162).

        Args:
            database: The job database to check.
            result_database: The separate function-result database to check, if any.

        Raises:
            UnstorableColumnError: For the first node whose table a database
                cannot store, naming the node.
        """
        pipeline_db, result_db = self._database_views(database, result_database)
        for node in (self._persistent_node_map or {}).values():
            if isinstance(node, FunctionJobNode):
                # Checked against the candidate databases only, never the job's
                # current binding state: a loaded job (the only one whose result
                # database can be unavailable) has function nodes without a function,
                # which this check skips by itself.
                node.require_storable_tables(pipeline_db, result_db)
            elif isinstance(node, OperatorJobNode):
                node.require_storable_tables(pipeline_db, node.cache_mode)
            elif isinstance(node, SideEffectJobNode):
                node.require_storable_tables(pipeline_db)

    def _distribute_databases(self) -> None:
        """Wire live DB references to all FunctionJobNode and OperatorJobNode objects.

        Called by ``compile()`` when a database is already set, and by ``bind()``
        when *database* is changed.

        Raises:
            RuntimeError: If ``_database`` is not set.
        """
        if self._database is None:
            raise RuntimeError(
                "Cannot distribute databases: no database is set. "
                "Call bind(database=...) or from_pipeline(..., database=...) first."
            )

        # Every node is checked before any is attached, so a refused database
        # leaves every node as it was (ITL-1162), and so is a caching operator over
        # a restricted source (ITL-1218).
        refuse_restricted_operator_caches((self._persistent_node_map or {}).values())
        self._require_storable_tables(self._database, self._result_database)
        pipeline_db, result_db = self._database_views(
            self._database, self._result_database
        )

        for node in (self._persistent_node_map or {}).values():
            if isinstance(node, FunctionJobNode):
                if self._unresolved_result_database is not None:
                    # Left without a database (see ``load``), never given ``_result``.
                    continue
                node.attach_databases(
                    pipeline_database=pipeline_db,
                    result_database=result_db,
                )
            elif isinstance(node, OperatorJobNode):
                node.attach_databases(
                    pipeline_database=pipeline_db,
                    cache_mode=node.cache_mode,
                )
            elif isinstance(node, SideEffectJobNode):
                node.attach_databases(pipeline_database=pipeline_db)

    # ------------------------------------------------------------------
    # _iter_function_job_nodes() / apply_node_config()
    # ------------------------------------------------------------------

    def _iter_function_job_nodes(self) -> Iterator[FunctionJobNode]:
        """Iterate over all ``FunctionJobNode`` instances in the compiled pipeline.

        Returns:
            An iterator over every ``FunctionJobNode`` in the persistent node map.
        """
        return (
            node
            for node in (self._persistent_node_map or {}).values()
            if isinstance(node, FunctionJobNode)
        )

    def apply_node_config(
        self,
        node_config: NodeConfig,
        replace_existing: bool = False,
    ) -> None:
        """Apply a ``NodeConfig`` to all ``FunctionJobNode``s in this pipeline.

        Args:
            node_config: The config to apply.
            replace_existing: If ``False``, directly sets ``node_config`` on
                every node, replacing whatever config each node currently holds.
                If ``True``, merges ``node_config`` into each node's existing
                config — non-``None`` fields in ``node_config`` override the
                node's current values, while ``None`` fields leave existing
                values unchanged.

        Raises:
            RuntimeError: If the job has not been compiled yet (use ``with job:``
                to record a DAG before applying node config).
        """
        if not self._compiled:
            raise RuntimeError(
                "No compiled pipeline — use 'with job:' to record a DAG before "
                "calling apply_node_config()."
            )
        for node in self._iter_function_job_nodes():
            if replace_existing:
                node.node_config = node.node_config.merge(node_config)
            else:
                node.node_config = node_config

    # ------------------------------------------------------------------
    # as_pipeline()
    # ------------------------------------------------------------------

    def as_pipeline(self) -> "Pipeline":
        """Return the lightweight ``Pipeline`` blueprint for this job.

        Walks ``_persistent_node_map`` topologically and rewires each job
        node to a fresh blueprint node whose upstreams point at the
        already-built blueprint nodes in ``node_map`` (not at job nodes).
        This ensures that the content_hash of every node in the returned
        ``Pipeline`` is consistent with the hash keys in
        ``_persistent_node_map``.

        Returns:
            A compiled ``Pipeline`` whose ``_persistent_node_map`` contains
            only lightweight ``SourceNode`` / ``FunctionNode`` / ``OperatorNode``
            objects with blueprint (non-job) upstream references.  Keys in
            ``_persistent_node_map`` match those in ``self._persistent_node_map``.

        Raises:
            RuntimeError: If this job has not been compiled.
        """
        import networkx as _nx
        from orcapod.core.nodes.function_node import FunctionNode
        from orcapod.core.nodes.operator_node import OperatorNode
        from orcapod.pipeline.graph import Pipeline
        from orcapod.pipeline.loaded_side_effects import (
            LoadedSideEffectJobNode,
            LoadedSideEffectNode,
        )
        from orcapod.side_effects import SideEffectNode

        if not self._compiled:
            raise RuntimeError(
                "PipelineJob has no compiled pipeline. "
                "Either use 'with job:' to record a DAG, "
                "or create the job via PipelineJob.from_pipeline()."
            )

        persistent = self._persistent_node_map

        # Reverse lookup: job node Python id → its key in _persistent_node_map.
        # Required to wire blueprint upstreams to the correct hash key even when
        # job node identity (content_hash) differs from the map key.
        job_id_to_bp_hash: dict[int, str] = {
            id(job_node): bp_hash for bp_hash, job_node in persistent.items()
        }

        node_map: dict[str, object] = {}

        for node_hash, job_node in persistent.items():
            if isinstance(job_node, SourceJobNode):
                node_map[node_hash] = job_node.as_node()

            elif isinstance(job_node, FunctionJobNode):
                upstream_bp_hash = job_id_to_bp_hash[id(job_node._input_stream)]
                node_map[node_hash] = FunctionNode(
                    function_pod=job_node._function_pod,
                    input_stream=node_map[upstream_bp_hash],
                    label=job_node._label,
                    table_scope=job_node._table_scope,
                    tracker_manager=job_node.tracker_manager,
                )

            elif isinstance(job_node, OperatorJobNode):
                blueprint_upstreams = tuple(
                    node_map[job_id_to_bp_hash[id(s)]] for s in job_node._input_streams
                )
                node_map[node_hash] = OperatorNode(
                    operator=job_node._operator,
                    input_streams=blueprint_upstreams,
                    label=job_node._label,
                    table_scope=job_node._table_scope,
                    tracker_manager=job_node.tracker_manager,
                )

            elif isinstance(job_node, LoadedSideEffectJobNode):
                # No pod to wrap: the blueprint node is restored from the saved
                # entry, with the saved blueprint identity (ITL-1094).
                upstream_bp_hash = job_id_to_bp_hash[id(job_node._input_stream)]
                node_map[node_hash] = LoadedSideEffectNode(
                    job_node._descriptor,
                    cast("cp.StreamProtocol", node_map[upstream_bp_hash]),
                )

            elif isinstance(job_node, SideEffectJobNode):
                upstream_bp_hash = job_id_to_bp_hash[id(job_node._input_stream)]
                node_map[node_hash] = SideEffectNode(
                    side_effect_pod=job_node._pod,
                    input_stream=node_map[upstream_bp_hash],
                    label=job_node._label,
                )

            else:
                node_map[node_hash] = job_node.as_node()

        # Translate old (concrete) hash keys → new (schema-based) node content hashes.
        # Blueprint nodes use schema-based content_hash() (SourceNode is schema-only,
        # so FunctionNode/OperatorNode upstreaming SourceNodes are also schema-based).
        old_to_new: dict[str, str] = {
            h: bp_node.content_hash().to_string() for h, bp_node in node_map.items()
        }
        # Re-key node_map with schema-based hashes.
        new_node_map: dict[str, object] = {
            new_h: node_map[h] for h, new_h in old_to_new.items()
        }

        # Rebuild hash graph with schema-based keys.
        # Carry over node attributes (node_type, label, pipeline_hash) from the
        # job's hash graph — these are structural/type attributes that remain
        # valid for the blueprint nodes.
        bp_hash_graph = _nx.DiGraph()
        for old_h, new_h in old_to_new.items():
            old_attrs = (
                dict(self._hash_graph.nodes[old_h]) if old_h in self._hash_graph else {}
            )
            bp_hash_graph.add_node(new_h, **old_attrs)
        new_graph_edges = sorted(
            (old_to_new[u], old_to_new[v])
            for u, v in self._graph_edges
            if u in old_to_new and v in old_to_new
        )
        bp_hash_graph.add_edges_from(new_graph_edges)

        pipeline = Pipeline(name=self._name, auto_compile=False)
        pipeline._hash_graph = bp_hash_graph
        pipeline._graph_edges = new_graph_edges
        pipeline._persistent_node_map = new_node_map
        pipeline._upstreams = {
            old_to_new[h]: node_map[h] for h in self._upstreams if h in old_to_new
        }
        pipeline._node_lut = {
            old_to_new[h]: node_map[h] for h in self._node_lut if h in old_to_new
        }
        pipeline._nodes = {
            label: node_map[job_id_to_bp_hash[id(job_node)]]
            for label, job_node in self._nodes.items()
            if id(job_node) in job_id_to_bp_hash
            and job_id_to_bp_hash[id(job_node)] in node_map
        }

        # Build _node_graph (OrcaDAG with node objects as vertices).
        # node_map here contains GraphNode objects (blueprint nodes).
        bp_dag: OrcaDAG = OrcaDAG()
        for node in pipeline._persistent_node_map.values():
            bp_dag.add_node(node)
        for up_h, down_h in new_graph_edges:
            bp_dag.add_edge(new_node_map[up_h], new_node_map[down_h])
        pipeline._node_graph = bp_dag

        pipeline._compiled = True

        return pipeline

    def to_invocations(self):  # type: ignore[override]
        """Extract an ``InvocationGraph`` from the schema-based blueprint.

        Delegates to ``as_pipeline().to_invocations()`` so that the
        produced invocations always use schema-based (not data-based)
        hashes, regardless of whether sources are currently bound.

        This ensures stable serialization across bind/rebind cycles.

        Raises:
            RuntimeError: If this job has not been compiled.
        """
        return self.as_pipeline().to_invocations()

    # ------------------------------------------------------------------
    # Completeness introspection
    # ------------------------------------------------------------------

    @property
    def unbound_sources(self) -> list[str]:
        """Names of source slots not yet bound in this job.

        Computed live from ``_persistent_node_map`` — reflects the current
        binding state at all times (before and after run).

        Returns:
            List of unbound source slot names, in order of appearance in
            the pipeline graph. Empty list if the job is not yet compiled.
        """
        if not self._compiled:
            return []
        seen: set[str] = set()
        result: list[str] = []
        for node in (self._persistent_node_map or {}).values():
            if (
                isinstance(node, SourceJobNode)
                and node.bound_source is None
                and node.name not in seen
            ):
                result.append(node.name)
                seen.add(node.name)
        return result

    def is_complete(self) -> bool:
        """Return ``True`` when all source nodes are bound and a database is set.

        Returns:
            ``True`` if all SourceNode slots are bound and a database is set.
        """
        return self._database is not None and not self.unbound_sources

    def is_runnable(self, node_label: str) -> bool:
        """Return whether a live job has all upstream inputs of *node_label* resolved.

        A loaded job is read-only and always returns ``False``.

        Uses hash-based traversal of ``_hash_graph`` combined with
        ``_persistent_node_map`` to check whether every upstream source slot
        is bound.  This avoids relying on the object-identity graph
        (``_node_graph``), which breaks after ``bind()`` because
        ``SourceJobNode.__hash__()`` changes when ``bound_source`` is mutated.

        Args:
            node_label: Label of the node to check.

        Returns:
            ``True`` if the node can be executed with current bindings.
        """
        if self._loaded or not self._compiled:
            return False

        target = self._nodes.get(node_label)
        if target is None:
            return False

        # Build a reverse lookup: node object id → hash key.
        # _persistent_node_map covers SourceJobNode slots (all compiled paths)
        # and all node types for directly-compiled jobs.
        # _node_lut covers non-source nodes for loaded jobs, where
        # _persistent_node_map only holds SourceJobNodes.
        node_id_to_hash: dict[int, str] = {}
        for h, node in self._node_lut.items():
            node_id_to_hash[id(node)] = h
        for h, node in self._persistent_node_map.items():
            # _persistent_node_map wins on collision (it contains live job nodes)
            node_id_to_hash[id(node)] = h

        target_hash = node_id_to_hash.get(id(target))
        if target_hash is None:
            return False

        import networkx as nx

        for h in nx.ancestors(self._hash_graph, target_hash) | {target_hash}:
            node = self._persistent_node_map.get(h)
            if isinstance(node, SourceJobNode) and node.bound_source is None:
                return False
        return True

    # ------------------------------------------------------------------
    # run()
    # ------------------------------------------------------------------

    def run(
        self,
        orchestrator: "SyncPipelineOrchestrator | AsyncPipelineOrchestrator | None" = None,
        observer: "ExecutionObserverProtocol | None" = None,
        materialize_results: bool = True,
        commit_every_rows: int = 100,
        *,
        load_only: bool = False,
    ) -> RunSummary:
        """Execute the resolvable subgraph of this job in place.

        Nodes whose upstream includes an unbound source (and all their
        dependents) are excluded from execution. Partial execution is a
        first-class outcome — ``unbound_sources`` reports which source
        slots were excluded.

        After a successful run, ``job.nodes`` returns the execution-ready
        nodes with populated database caches.

        State-driven, as every job node's ``run()`` (developer, November 14, 2026;
        ITL-1095): a node that holds a current run (completed in this process, not
        stale, and for a full run not filled by a load-only run) is held, not run; its
        rows feed the nodes after it, and nothing is computed, read, written or
        delivered for it. A source is read only for the nodes that run. So a second run
        of an unchanged job does nothing. To run a node again, call ``clear_cache()`` on
        it (every node below it runs too), or bind a new source object.

        ``job.run(orchestrator=AsyncPipelineOrchestrator())`` works the same in a
        script and inside a running event loop, such as a Jupyter kernel: there the
        run goes on a separate thread while the call waits, and blocks that loop
        until it ends. From async code, ``await job.run_async()`` instead.

        Args:
            orchestrator: Orchestrator to use for execution. Defaults to
                ``SyncPipelineOrchestrator`` when ``None``.
            observer: Optional execution observer.
            materialize_results: Controls whether the orchestrator retains
                node outputs in its transient in-process buffer during
                execution. When ``True`` (the default) the buffer is kept for
                the duration of the run. When ``False`` each node's buffer is
                released as soon as the node completes, reducing peak memory.
                This setting does not affect database persistence or post-run
                result access: DB records are written regardless, and
                ``job.nodes["label"]`` returns the same rows from
                ``as_table()`` and ``iter_data()`` in either case, on either
                orchestrator. An operator node with the default
                ``cache_mode=OFF`` (the only mode a job sets) keeps its output
                in memory and stores nothing. Its ``get_all_records()`` returns
                ``None`` when no matching records exist; records from an earlier
                LOG run remain readable after switching to OFF.
            commit_every_rows: How many rows a function node processes between
                commits of its results and pipeline records to the database
                (default 100). Each node also commits when it finishes, before a
                failed run raises, and the job commits at its end. On a durable
                database (``DeltaTableDatabase``) a hard crash (power loss, killed
                process) loses at most the rows of the running node since its
                last commit; re-running the job recomputes only those. A larger
                value commits less often (faster, fewer files); ``1`` commits
                after every row. A node run on its own, outside ``run()``,
                commits after every row. It must be smaller than the database's
                ``batch_size`` when the database has one (``DeltaTableDatabase``,
                default 1000): the database commits every pending table by itself
                once one table's pending rows reach that size, which would
                break the node's results-before-records commit order.
            load_only: Fill the nodes from what is already stored and compute
                nothing (ITL-1095). Sources are read and operators run on the rows
                that reach them; each function node takes the results stored for
                this run's inputs (its own records under its content hash) and skips
                the inputs without one, which the summary counts (``not_stored``);
                a function node forwards only the rows it loaded. No user function
                is called, no side effect is delivered (a side-effect node passes its
                rows on) and nothing is written to the database, a ``LOG`` operator's
                cache included. Afterwards each node shows the rows it loaded.

        Rows whose function raised (with the default ``error_policy="continue"``) are
        skipped, and the run goes on. At the end, if any row failed, a single
        ``RowFailuresWarning`` names each node with failed rows, how many failed, the
        first error, and how to print its traceback. A run without failures is quiet.

        Returns:
            The run's ``RunSummary``: per function node, the rows computed, read from
            the database and failed, with each node's first error. Shown at the end of a
            notebook cell; also kept as ``job.last_run``. Results are read from
            ``job.nodes``.

        Raises:
            ValueError: If no database is set, or ``commit_every_rows`` is not a
                positive integer smaller than the database's ``batch_size``.
            RuntimeError: If no pipeline has been compiled, or the job was built
                by ``PipelineJob.load`` (a loaded job is read-only).
            OrcapodDatabaseWriteError: If a node's results or records could not be written
                to the database, whatever the orchestrator's ``error_policy``. The run
                stops at the first failed write. Writes that succeeded before it
                are flushed first, so rows stored before the failure are kept, and
                re-running the job does not recompute stored function results.
                A failed commit of a node's batch names the node but no row
                (``flush=True``). When a result batch fails, its pending pipeline
                records are withdrawn. The flush after the failure may retry the
                result write if the database kept the batch.
            OrcapodDatabaseReadError: If a stored table exists but could not be read,
                whatever the orchestrator's ``error_policy``, also when the read
                was needed to write a node's results. Its rows are not recomputed
                and the table is not overwritten.
        """
        from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

        prepared = self._prepare_run(commit_every_rows, load_only=load_only)
        # The state rule (ITL-1095): a node that holds a current run is not run again.
        plan = prepared.plan
        counter = RunCountingObserver()
        effective_orchestrator = orchestrator or SyncPipelineOrchestrator()
        is_sync = isinstance(effective_orchestrator, SyncPipelineOrchestrator)
        with self._executing(prepared, plan, commit_every_rows, load_only=load_only):
            if is_sync:
                # Held nodes give their output streams on the sync path (ITL-1347).
                _ = effective_orchestrator.run(
                    plan.dag,
                    observer=_with_counter(observer, counter),
                    run_id=prepared.run_id,
                    pipeline_uri=prepared.pipeline_uri,
                    materialize_results=materialize_results,
                    load_only=load_only,
                    held=plan.held,
                )
            else:
                # The async path sends the rows the plan took (until stage 5).
                _ = effective_orchestrator.run(
                    plan.dag,
                    observer=_with_counter(observer, counter),
                    run_id=prepared.run_id,
                    pipeline_uri=prepared.pipeline_uri,
                    materialize_results=materialize_results,
                    load_only=load_only,
                    held_rows=plan.held_rows,
                )
        summary = self._finish_run(
            prepared,
            counter,
            plan,
            load_only=load_only,
            unchecked_sources=prepared.unchecked_sources,
            asynchronous=not is_sync,
        )
        if not summary.ok:
            warn_row_failures(summary, sys._getframe(1))  # pyright: ignore[reportPrivateUsage]  # the caller's line is where the warning is shown
        return summary

    async def run_async(
        self,
        orchestrator: "AsyncPipelineOrchestrator | None" = None,
        observer: "ExecutionObserverProtocol | None" = None,
        materialize_results: bool = True,
        commit_every_rows: int = 100,
        *,
        load_only: bool = False,
    ) -> RunSummary:
        """Execute the job with the async orchestrator, from async code.

        The awaitable form of ``run``: ``await job.run_async()`` runs on the caller's
        event loop instead of blocking it. ``job.run(orchestrator=AsyncPipelineOrchestrator())``
        also works inside a running loop (a Jupyter kernel), but blocks that loop until
        the run ends. Both store the same rows and record ids. Arguments, errors and
        result are those of ``run``.

        Args:
            orchestrator: The async orchestrator to run with. Defaults to
                ``AsyncPipelineOrchestrator()``.
            observer: Optional execution observer.
            materialize_results: As for ``run``.
            commit_every_rows: As for ``run``.
            load_only: As for ``run``.

        Returns:
            The run's ``RunSummary``, as ``run`` returns.

        Raises:
            TypeError: If ``orchestrator`` is not an ``AsyncPipelineOrchestrator``;
                run a ``SyncPipelineOrchestrator`` with ``job.run``.
            ValueError: As for ``run``.
            RuntimeError: As for ``run``.
            OrcapodDatabaseWriteError: As for ``run``.
            OrcapodDatabaseReadError: As for ``run``.
        """
        from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator

        if orchestrator is None:
            orchestrator = AsyncPipelineOrchestrator()
        elif not isinstance(orchestrator, AsyncPipelineOrchestrator):
            raise TypeError(
                "PipelineJob.run_async() runs an AsyncPipelineOrchestrator, got "
                f"{type(orchestrator).__name__}. Use job.run(orchestrator=...) to run "
                "it."
            )
        prepared = self._prepare_run(commit_every_rows, load_only=load_only)
        # The state rule (ITL-1095): a node that holds a current run is not run again.
        plan = prepared.plan
        counter = RunCountingObserver()
        with self._executing(prepared, plan, commit_every_rows, load_only=load_only):
            _ = await orchestrator.run_async(
                plan.dag,
                observer=_with_counter(observer, counter),
                run_id=prepared.run_id,
                pipeline_uri=prepared.pipeline_uri,
                materialize_results=materialize_results,
                load_only=load_only,
                held_rows=plan.held_rows,
            )
        summary = self._finish_run(
            prepared,
            counter,
            plan,
            load_only=load_only,
            unchecked_sources=prepared.unchecked_sources,
            asynchronous=True,
        )
        if not summary.ok:
            warn_row_failures(summary, sys._getframe(1))  # pyright: ignore[reportPrivateUsage]  # the caller's line is where the warning is shown
        return summary

    def _prepare_run(
        self, commit_every_rows: int, *, load_only: bool = False
    ) -> "_PreparedRun":
        """Check the job can run and build what a run needs; shared by both run forms.

        Args:
            commit_every_rows: As for ``run``.
            load_only: As for ``run``: a loaded job can run only this way.

        Returns:
            The execution DAG, run id, pipeline URI and the database views of the run.

        Raises:
            ValueError: As for ``run``.
            RuntimeError: As for ``run``.
            OrcapodDatabaseError: If writes still pending from before the run cannot be committed.
        """
        import hashlib
        import uuid

        if (
            isinstance(commit_every_rows, bool)
            or not isinstance(commit_every_rows, int)
            or commit_every_rows < 1
        ):
            raise ValueError(
                "commit_every_rows must be a positive integer (rows per commit), "
                f"got {commit_every_rows!r}"
            )
        if not self._compiled:
            raise RuntimeError(
                "PipelineJob has no compiled pipeline. "
                "Record invocations inside a 'with job:' block first."
            )
        # A loaded job never computes: function pods are not saved with it, and even
        # an operator-only loaded job, whose operators are rebuilt, stays read-only.
        # A load-only run fills it from the database (ITL-1095).
        if self._loaded and not load_only:
            raise RuntimeError(
                "PipelineJob.run() cannot execute a loaded job: its function pods are "
                "not saved with it, so it cannot compute. Fill its nodes from the "
                "database with run(load_only=True) (bind in-memory sources first), or "
                "read a node's stored results with node.as_source(). To run a new "
                "computation, create a fresh PipelineJob from the original pipeline "
                "definition."
            )
        # Until this run finishes, what the database holds for a node is not certain.
        self._last_run_finished = False

        # The job's database, then a separate result database (ITL-1408): each one
        # that commits table by table.
        committing = [
            committing_db
            for committing_db in (
                as_table_committing(self._database),
                as_table_committing(self._result_database),
            )
            if committing_db is not None
        ]
        table_committing = committing[0] if committing else None
        for committing_db in committing:
            if commit_every_rows >= committing_db.batch_size:
                raise ValueError(
                    f"commit_every_rows={commit_every_rows} must be smaller than the "
                    + f"database's batch_size={committing_db.batch_size}: the database "
                    + "commits a table by itself once it holds batch_size pending rows, "
                    + "which could commit pipeline records before their results. Use a "
                    + "smaller commit_every_rows, or create the database with a larger "
                    + "batch_size."
                )
        if self._database is None:
            raise ValueError(
                "PipelineJob.run() requires a database. "
                "Call job.bind(database=db) before run(). "
                "For transient (in-memory) storage use "
                "job.bind(database=InMemoryArrowDatabase())."
            )

        # Reject loaded read-only stubs: nodes created by load() have no live
        # function pod / operator and cannot compute new outputs.  Replaying
        # cached results would silently produce no output for cache misses; a
        # clear error is better.  Use get_all_records() on the loaded job to
        # inspect results.
        from orcapod.pipeline.serialization import LoadStatus

        # A loaded function node has no function pod, whatever its load status
        # (READ_ONLY once a database is attached, so it can read its stored results).
        unavailable_labels = [
            node.label
            for node in self._persistent_node_map.values()
            if (
                isinstance(node, (FunctionJobNode, OperatorJobNode))
                and node.load_status == LoadStatus.UNAVAILABLE
            )
            or (
                not load_only
                and isinstance(node, FunctionJobNode)
                and node.producer is None
            )
        ]
        if unavailable_labels:
            raise RuntimeError(
                "PipelineJob.run() cannot execute a loaded job: the following "
                f"nodes are read-only stubs: {unavailable_labels}. "
                "PipelineJob.load() is for result inspection only. "
                "To run a new computation, create a fresh PipelineJob from the "
                "original pipeline definition."
            )

        # --- Build hash-keyed OrcaDAG for topological ordering + exclusion ---
        # A caller can change a slot or proxy directly after binding the job.
        require_consistent_sources(
            (f"node {node.label!r}", node)
            for node in self._persistent_node_map.values()
        )
        hash_dag: OrcaDAG[str] = OrcaDAG()
        for u, v in self._graph_edges:
            hash_dag.add_edge(u, v)
        for node_hash in self._persistent_node_map:
            if node_hash not in hash_dag:
                hash_dag.add_node(node_hash)

        # Walk topologically; exclude unbound sources and all their dependents.
        excluded_hashes: set[str] = set()
        for node_hash in hash_dag.topological_sort():
            if node_hash not in self._persistent_node_map:
                # A node a loaded job did not restore (a kind PipelineJob.load does
                # not know): below an unbound source it is left out too, so the nodes
                # below it are as well.
                if any(p in excluded_hashes for p in hash_dag.predecessors(node_hash)):
                    excluded_hashes.add(node_hash)
                continue
            node = self._persistent_node_map[node_hash]
            if isinstance(node, SourceJobNode) and node.bound_source is None:
                excluded_hashes.add(node_hash)
            elif any(p in excluded_hashes for p in hash_dag.predecessors(node_hash)):
                excluded_hashes.add(node_hash)

        # A loaded job whose graph lost a node (the file records a node of a kind
        # PipelineJob.load does not restore) cannot feed the nodes below it: every
        # saved input of a node must be a restored node.
        if self._loaded:
            for node_hash, node in self._persistent_node_map.items():
                if node_hash in excluded_hashes:
                    continue  # below an unbound source: not part of this run
                if not isinstance(node, (FunctionJobNode, OperatorJobNode)):
                    continue
                if node_hash not in self._hash_graph:
                    continue
                missing = [
                    upstream
                    for upstream in self._hash_graph.predecessors(node_hash)
                    if upstream not in self._persistent_node_map
                ]
                if missing:
                    raise RuntimeError(
                        f"Node {node.label!r} of the loaded job has lost "
                        f"{len(missing)} of its input nodes: the saved file records a "
                        "node of a kind PipelineJob.load does not restore, so the nodes "
                        "below it cannot run, not even load-only. Read their stored "
                        "results with node.as_source()."
                    )

        # --- Build execution OrcaDAG from existing node objects (no cloning) ---
        exec_dag: OrcaDAG = OrcaDAG()
        for node_hash, node in self._persistent_node_map.items():
            if node_hash not in excluded_hashes:
                exec_dag.add_node(node)
        for u_hash, v_hash in self._graph_edges:
            if (
                u_hash not in excluded_hashes
                and v_hash not in excluded_hashes
                and u_hash in self._persistent_node_map
                and v_hash in self._persistent_node_map
            ):
                exec_dag.add_edge(
                    self._persistent_node_map[u_hash],
                    self._persistent_node_map[v_hash],
                )

        run_id = uuid.uuid4().hex[:16]

        # Snapshot hash: SHA-256 of the sink nodes' content hashes, for the run URI.
        # Each enters with its label (ITL-1154); the result is written into a name,
        # the run URI, and so is bare hex.
        leaf_hashes = sorted(
            node.content_hash().to_string()
            for node in exec_dag.nodes()
            if not exec_dag.successors(node) and hasattr(node, "content_hash")
        )
        snapshot_hash = hashlib.sha256("\n".join(leaf_hashes).encode()).hexdigest()[:16]
        pipeline_uri = "/".join(self._name) + "@" + snapshot_hash

        pipeline_db, result_db = self._database_views(
            self._database, self._result_database
        )

        # A source read under two restrictions, or a caching operator over a restricted
        # source, is refused before any source is checked and before anything is
        # computed or written (ITL-1218).
        refuse_restriction_conflicts(exec_dag.nodes())
        # Every source first takes in any new data, moving its modified time, before
        # the state rule judges any node (ITL-1102).
        unchecked = check_sources(exec_dag.nodes())
        # A restricted source under an aggregation is allowed, and said (ITL-1218).
        warn_restricted_aggregation(exec_dag)
        # The state rule (ITL-1095): a node that holds a current run is not run again.
        plan = plan_run(exec_dag, load_only=load_only)
        # A database that commits a table by itself at batch_size: commit what it already
        # holds pending (writes made without a flush, or a failed run's batches) before
        # this run writes. Those rows would otherwise count towards a table's
        # batch_size, which could then be reached mid-run and commit this run's pipeline
        # records ahead of their results (ITL-919).
        # A load-only run writes nothing, and a run that runs no node touches no database,
        # so neither commits anything, not even pending writes of earlier operations.
        if table_committing is not None and not load_only and plan.to_run:
            # A separate result database (ITL-1408) is committed too, results first.
            pending_dbs = (
                (result_db, pipeline_db)
                if self._result_database is not None
                else (pipeline_db,)
            )
            for pending_db in pending_dbs:
                try:
                    pending_db.flush()
                except OrcapodDatabaseError as exc:
                    exc.add_note(_PENDING_BEFORE_RUN_NOTE)
                    raise
                except Exception as exc:
                    flush_error = OrcapodDatabaseWriteError(
                        pending_db.base_path, flush=True
                    )
                    flush_error.add_note(_PENDING_BEFORE_RUN_NOTE)
                    raise flush_error from exc

        return _PreparedRun(
            exec_dag=exec_dag,
            run_id=run_id,
            pipeline_uri=pipeline_uri,
            pipeline_db=pipeline_db,
            result_db=result_db,
            plan=plan,
            unchecked_sources=unchecked,
        )

    @contextlib.contextmanager
    def _executing(
        self,
        prepared: "_PreparedRun",
        plan: RunPlan,
        commit_every_rows: int,
        *,
        load_only: bool = False,
    ) -> Iterator[None]:
        """Hold every function node to the run's commit cadence while the orchestrator runs.

        A failure inside commits the writes that succeeded before it (not in a load-only
        run, which writes nothing), clears every node the run ran (whether their writes
        were committed is unknown), then propagates.

        Args:
            prepared: The run, from ``_prepare_run``.
            plan: The nodes the run runs (``plan_run``).
            commit_every_rows: As for ``run``.
            load_only: As for ``run``.
        """
        try:
            with contextlib.ExitStack() as batched:
                # Function nodes commit every commit_every_rows rows (ITL-919).
                for node in plan.to_run:
                    if isinstance(node, FunctionJobNode):
                        batched.enter_context(
                            node._batched_commits(commit_every_rows)  # pyright: ignore[reportPrivateUsage]  # the job owns its nodes' commit cadence
                        )
                yield
        except BaseException as run_error:
            # Retry remaining writes. Function records with failed results have
            # been withdrawn, and pending results still gate their dependent records.
            # A cancelled or interrupted run clears its nodes without flushing here.
            if isinstance(run_error, Exception) and not load_only:
                flush_best_effort(
                    run_error,
                    prepared.pipeline_db,
                    prepared.result_db,
                    # Records a separate result database's flush released (ITL-1408).
                    *(
                        (prepared.pipeline_db,)
                        if self._result_database is not None
                        else ()
                    ),
                )
            clear_run_rows(plan.to_run)
            raise

    def _finish_run(
        self,
        prepared: "_PreparedRun",
        counter: RunCountingObserver,
        plan: RunPlan,
        *,
        load_only: bool = False,
        unchecked_sources: tuple[str, ...] = (),
        asynchronous: bool,
    ) -> RunSummary:
        """Commit the run's last writes, mark the job as run and keep its summary.

        A failed commit clears the nodes the run ran (they hold no completed run);
        the nodes it held keep the rows of their earlier run.

        Args:
            prepared: The run, from ``_prepare_run``.
            counter: The observer that counted the run's rows.
            plan: The nodes the run ran and held (``plan_run``).
            load_only: Whether the run loaded stored results only.
            unchecked_sources: The sources the start-of-run check could not check.
            asynchronous: Whether the async orchestrator executed the run.

        Returns:
            The run's summary, also kept as ``last_run``.

        Raises:
            OrcapodDatabaseReadError: As for ``run``.
            OrcapodDatabaseWriteError: As for ``run``.
        """
        pipeline_db = prepared.pipeline_db
        result_db = prepared.result_db
        # Flush databases. Every view of the database shares one set of pending
        # batches, so a failed batch can leave the ones after it unwritten.
        pending_note = (
            "Writes that were still pending when the flush failed, such as a "
            + "cache_mode=LOG operator's output or a node's last records, may not "
            + "have been stored."
        )
        database = pipeline_db
        try:
            if not load_only and plan.to_run:
                # A load-only run writes nothing and commits nothing, and a run that
                # ran no node touches no database (ITL-1095).
                # Pending results gate the pipeline records that name them (ITL-1412),
                # across databases too, so no order of these flushes publishes a record
                # before its result.
                database.flush()
                database = result_db
                database.flush()
                if self._result_database is not None:
                    # A separate result database (ITL-1408): records the result flush
                    # just released are still queued in the pipeline database.
                    database = pipeline_db
                    database.flush()
        except OrcapodDatabaseReadError as exc:
            # A stored table could not be read: report the read failure itself.
            exc.add_note(pending_note)
            # A run whose commit failed did not complete: no node holds its rows.
            clear_run_rows(plan.to_run)
            raise
        except Exception as exc:
            flush_error = OrcapodDatabaseWriteError(
                database_write_path(database), flush=True
            )
            flush_error.add_note(pending_note)
            clear_run_rows(plan.to_run)
            raise flush_error from exc
        except BaseException:
            # An interruption (KeyboardInterrupt, cancellation) leaves the commit's
            # outcome unknown: no node holds the run's rows. Re-raised unchanged.
            clear_run_rows(plan.to_run)
            raise

        # Mutate self in place.
        self._has_run = True
        self._last_run_finished = True
        self._run_id = prepared.run_id
        self._last_run = RunSummary(
            "/".join(self._name),
            run_node_summaries(prepared.exec_dag, counter, held=plan.held_counts),
            load_only=load_only,
            unchecked_sources=unchecked_sources,
            ran_nothing=not plan.to_run,
            restricted_sources=restrictions_in_force(prepared.exec_dag.nodes()),
            asynchronous=asynchronous,
        )
        return self._last_run

    def __repr__(self) -> str:
        n_sources = len(self._sources)
        has_database = self._database is not None
        is_compiled = self._compiled
        return (
            f"PipelineJob(sources={n_sources}, database={has_database}, "
            f"compiled={is_compiled})"
        )

    # ------------------------------------------------------------------
    # save() / load()
    # ------------------------------------------------------------------

    def save(self, path: "str | Path") -> None:
        """Serialize this job to a JSON file.

        Saves topology (via the embedded Pipeline) plus bindings metadata and
        run state. The format covers both "template" (pre-run) and
        "completed" (post-run) states — distinguished by ``run.status``.

        Args:
            path: File path to write JSON output to.

        Raises:
            ValueError: If no compiled pipeline exists, or a source cannot save its
                configuration exactly (a ``CSVSource`` whose explicit schema holds
                Arrow metadata); nothing is written then.
        """
        import json as _json
        import os
        import tempfile
        from pathlib import Path as _Path

        from orcapod.pipeline.serialization import PIPELINE_JOB_FORMAT_VERSION

        # Allow save when either compile() was called or _compiled_pipeline is
        # pre-set (e.g. result of run() which has no invocation_lut but does
        # have a compiled_pipeline from exec graph construction).
        if not self._compiled and self._compiled_pipeline is None:
            raise ValueError("No compiled pipeline to save.")

        pipeline = self.compiled_pipeline

        path = _Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Save the pipeline blueprint inline via a temporary file
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tmp:
            tmp_path = tmp.name
        try:
            pipeline.save(tmp_path)
            with open(tmp_path) as f:
                pipeline_data = _json.load(f)
        finally:
            os.unlink(tmp_path)

        # Serialize source configs (best-effort — skip non-reconstructable sources)
        sources_block: dict[str, Any] = {}
        for spec_name, source in sorted(self._sources.items()):
            if hasattr(source, "to_config"):
                try:
                    # ``hasattr`` checked it: every source declares it.
                    sources_block[spec_name] = cast(
                        "cp.SourceProtocol", source
                    ).to_config()
                except ValueError as exc:
                    # A source that cannot save its configuration exactly refuses
                    # (a CSVSource's explicit schema with Arrow metadata, ITL-1315):
                    # nothing is written.
                    exc.add_note(f"Saving the job's source {spec_name!r}")
                    raise
            else:
                sources_block[spec_name] = {"source_type": "unknown"}

        # Database config (best-effort)
        database_block = None
        if self._database is not None and hasattr(self._database, "to_config"):
            database_block = self._database.to_config()
        # A separate result database (ITL-1408), saved by the same rule; ``None`` when
        # the results are in the job's own database (``<name>/_result``).
        # A loaded job's binding that was not available is written back as it was, so
        # the next load refuses it again rather than reading ``_result``.
        result_database_block = self._unresolved_result_database
        if self._result_database is not None:
            result_database_block = self._result_database.to_config()

        if not self._has_run:
            status = "pending"
        elif self.unbound_sources:
            status = "partial"
        else:
            status = "complete"

        # Collect the live (data-inclusive) content hashes of function, operator and
        # side-effect job nodes.
        # The pipeline blueprint stores schema-only blueprint hashes; but the DB
        # records are written with the live hash (which includes the upstream data
        # content).  Saving these ensures that PipelineJob.load() can reconstruct
        # stubs whose content_hash() matches the _node_content_hash column in the
        # DB — required for _filter_by_content_hash() in get_all_records().
        node_job_content_hashes: dict[str, str] = {}
        # Advisory only (ITL-1043): how many rows the database holds under each node's
        # run key, written only when certain (the last run finished and committed, or
        # a loaded node's own record). A loaded node compares it with its database and
        # warns on a difference; the database stays the source of truth.
        expected_rows: dict[str, int] = {}
        for node in (self._persistent_node_map or {}).values():
            if isinstance(node, (FunctionJobNode, OperatorJobNode)):
                count = node._expected_row_count_to_save(self._last_run_finished)
                if count is not None:
                    expected_rows[node.label] = count
            if isinstance(node, FunctionJobNode):
                node_job_content_hashes[node.label] = node.content_hash().to_string()
            elif isinstance(node, OperatorJobNode):
                # An operator node's stored rows carry its run's content hash too
                # (ITL-1043); a loaded node has the one its file records.
                node_job_content_hashes[node.label] = (
                    node._run_content_hash().to_string()
                )
            elif isinstance(node, SideEffectJobNode):
                # Its descriptor holds the blueprint's hash; a loaded side-effect node
                # takes the job's from here, as a function node does (ITL-1094).
                node_job_content_hashes[node.label] = node.content_hash().to_string()

        output: dict[str, Any] = {
            "orcapod_pipeline_job_version": PIPELINE_JOB_FORMAT_VERSION,
            "orcapod_version": orcapod.__version__,
            "run": {
                "run_id": self._run_id,
                "status": status,
                "unbound_sources": list(self.unbound_sources),
                "node_job_content_hashes": node_job_content_hashes,
                # A separate, optional section: deleting it changes nothing but the
                # check (ITL-1043).
                **({"expected_rows": expected_rows} if expected_rows else {}),
            },
            "pipeline": pipeline_data,
            "bindings": {
                "sources": sources_block,
                "database": database_block,
                "result_database": result_database_block,
            },
        }

        with open(path, "w") as f:
            _json.dump(output, f, indent=2)

    @classmethod
    def load(
        cls,
        path: "str | Path",
        database: "ArrowDatabaseProtocol | None" = None,
        *,
        result_database: "ArrowDatabaseProtocol | None" = None,
    ) -> "PipelineJob":
        """Deserialize a ``PipelineJob`` from a JSON file to read its stored results.

        A loaded job is for reading the results its saved run stored; it cannot be
        run again, because function pods are not saved with the job. Its nodes have
        not run in this process, so they are empty (ITL-1095): read a node's stored
        results through its stored view, ``job.nodes[label].as_source()``, or with
        ``get_all_records()``. Operators store outputs only when caching is enabled;
        the stored view of a loaded operator whose run's rows cannot be named raises
        ``RuntimeError``.

        The embedded ``Pipeline`` blueprint is always restored. Sources whose data
        lives in files (CSV, Delta) are rebuilt from the saved config. In-memory
        sources (``DictSource``, ``ListSource``, ``ArrowTableSource``,
        ``DataFrameSource``) are not saved; loading logs that at INFO, and their
        source nodes can be read after ``job.bind(sources=...)``. A source that
        cannot be rebuilt for another reason (a moved file) is logged as a
        warning. Neither affects reading the other nodes.

        Args:
            path: Path to the JSON file produced by ``save()``.
            database: Optional database override. When provided, takes precedence
                over any database configuration in the file.
            result_database: Optional override of the separate function-result
                database (ITL-1408). When provided, it is used whatever the file
                says. Otherwise the one the file names is rebuilt from its saved
                configuration, as the job's database is; a file that names none
                keeps the results in the job's database (``<name>/_result``). When
                the file names one that cannot be rebuilt, the function nodes get no
                database (a warning says what to pass) rather than the job's own
                ``_result`` location, which holds none of their results.

        Returns:
            A read-only ``PipelineJob`` whose nodes' stored views serve their stored
            results.

        Raises:
            InconsistentSourceError: If distinct reconstructed source objects
                share a content hash, or sources with one ID differ in schema
                or key columns (ITL-1506).
            ValueError: If the file's format version is not the one this Orcapod
                writes (an earlier format is refused, never migrated), or if it was
                saved before the database key was renamed (``bindings.store``
                instead of ``bindings.database``) and no ``database`` is given.
                An explicit ``database`` makes the saved block irrelevant, so
                such a file then loads.
        """
        import json as _json
        import os
        import tempfile
        from pathlib import Path as _Path

        from orcapod.core.nodes.source_node import SourceNodeBase
        from orcapod.pipeline.graph import Pipeline
        from orcapod.pipeline.serialization import (
            PIPELINE_JOB_FORMAT_VERSION,
            check_saved_source_config,
            check_source_config_binding,
            require_current_format,
            require_tag_only_columns,
            resolve_source_from_config,
        )

        path = _Path(path)
        with open(path) as f:
            data = _json.load(f)

        try:
            require_current_format(
                data.get("orcapod_pipeline_job_version", ""),
                PIPELINE_JOB_FORMAT_VERSION,
            )
        except ValueError as exc:
            exc.add_note(f"Loading the job file {path}")
            raise

        # Reconstruct pipeline from embedded blueprint data
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tmp:
            tmp_path = tmp.name
            _json.dump(data["pipeline"], tmp)
        try:
            pipeline = Pipeline.load(tmp_path)
        except ValueError as exc:
            # The embedded pipeline was read from a temporary file: name the job file.
            message = str(exc)
            if tmp_path in message:
                exc.args = (message.replace(tmp_path, str(path)),)
            raise
        finally:
            os.unlink(tmp_path)

        # Reconstruct sources (best-effort)
        slots = {
            node.name: node
            for node in pipeline._persistent_node_map.values()
            if isinstance(node, SourceNodeBase)
        }
        sources: dict[str, cp.StreamProtocol] = {}
        sources_by_config: dict[str, cp.StreamProtocol | None] = {}
        for spec_name, src_config in (
            data.get("bindings", {}).get("sources", {}).items()
        ):
            source_type = src_config.get("source_type")
            # Every source states its plain tags (ITL-1019): a refusal is the file's
            # error, not a source that could not be rebuilt.
            try:
                require_tag_only_columns(src_config)
                # A tags_as_data source saved without its required source id is an
                # invalid file, refused here; the best-effort rebuild below would
                # leave the slot unbound instead (ITL-1313).
                DerivedSource.require_saved_source_id(src_config)
                if spec_name in slots:
                    slot = slots[spec_name]
                    check_source_config_binding(
                        src_config, slot.tag_schema.keys(), slot.data_schema.keys()
                    )
                # A malformed entry of a source's own (a CSVSource's saved schema,
                # ITL-1315) is the file's error: refused here, where the best-effort
                # rebuild below would leave the slot unbound.
                check_saved_source_config(src_config)
            except ValueError as exc:
                raise ValueError(f"{path}: source {spec_name!r}: {exc}") from exc
            try:
                # Slots written with the same complete config share one rebuild.
                config_key = _json.dumps(src_config, sort_keys=True)
                if config_key not in sources_by_config:
                    sources_by_config[config_key] = resolve_source_from_config(
                        src_config
                    )
                source = sources_by_config[config_key]
                if source is not None:
                    # A rebuilt source its slot refuses (a file whose column types
                    # changed) is one that could not be rebuilt (ITL-1112).
                    if spec_name in slots:
                        slots[spec_name].validate(source)
                    sources[spec_name] = source
            except NotImplementedError:
                # In-memory sources (DictSource, ListSource, ArrowTableSource,
                # DataFrameSource) are not saved with the job, by design.
                logger.info(
                    "Source %r (%s) keeps its data in memory and is not saved with the "
                    "job; the loaded job has no data for it. Stored function-node "
                    "results are still readable from the database. To read source node "
                    "%r itself, bind a source: job.bind(sources={%r: <source>}).",
                    spec_name,
                    source_type,
                    spec_name,
                    spec_name,
                )
            except Warning:
                # A warning turned into an error (a ProvisionalHashWarning under an
                # error filter) is the caller's request: it propagates, never read as a
                # source that could not be rebuilt (ITL-1331).
                raise
            except Exception as exc:
                logger.warning(
                    "Could not reconstruct source %r (%s) from its saved config: %s. "
                    "Stored function-node results are still readable from the database. "
                    "To read source node %r itself, bind a source: "
                    "job.bind(sources={%r: <source>}).",
                    spec_name,
                    source_type,
                    exc,
                    spec_name,
                    spec_name,
                )

        require_consistent_sources(
            (f"slot {name!r}", source) for name, source in sources.items()
        )

        # Reconstruct database
        effective_database = database
        if effective_database is None:
            bindings = data.get("bindings", {})
            if bindings.get("store") and "database" not in bindings:
                # A file saved before ITL-1082 renamed the key. It is not read under
                # the old name: the job would load without its database, silently.
                raise ValueError(
                    f"{path} names its database under the key 'bindings.store', "
                    "which was renamed 'bindings.database' (ITL-1082); the file was "
                    "saved before the rename. Load it with an explicit database: "
                    "PipelineJob.load(path, database=...), or rewrite the key as "
                    "'bindings.database' and save it again."
                )
            store_config = bindings.get("database")
            if store_config:
                try:
                    from orcapod.pipeline.serialization import (
                        resolve_database_from_config,
                    )

                    effective_database = resolve_database_from_config(store_config)
                except Exception as exc:
                    logger.warning(
                        "Could not reconstruct the database from its saved config: %s. "
                        "Node results cannot be read until a database is given: "
                        "PipelineJob.load(path, database=...) or job.bind(database=...).",
                        exc,
                    )

        # Reconstruct the job directly without going through to_invocations().
        # The loaded pipeline may have stub nodes (function_pod=None, operator=None)
        # that cannot compute content hashes, so the to_invocations() → from_invocations()
        # path used by from_pipeline() would fail.  Instead, attach the loaded
        # pipeline directly as the compiled blueprint and set up source bindings.
        # Reconstruct the separate result database (ITL-1408), by the same rule.
        effective_result_database = result_database
        unresolved_result_database: dict[str, Any] | None = None
        if effective_result_database is None:
            result_config = data.get("bindings", {}).get("result_database")
            if result_config and result_config.get("type") == "in_memory":
                # An in-memory database holds nothing once its process ended: a new
                # one would be empty, so it is not available, as a connector is not.
                unresolved_result_database = result_config
                logger.warning(
                    "The job's separate result database (bindings.result_database) "
                    "was an in-memory database, which keeps no data after its process "
                    "ends. The function nodes' results cannot be read until it is "
                    "given: PipelineJob.load(path, result_database=...) or "
                    "job.bind(result_database=...)."
                )
            elif result_config:
                try:
                    from orcapod.pipeline.serialization import (
                        resolve_database_from_config,
                    )

                    effective_result_database = resolve_database_from_config(
                        result_config
                    )
                except Exception as exc:
                    unresolved_result_database = result_config
                    logger.warning(
                        "Could not reconstruct the job's separate result database "
                        "(bindings.result_database) from its saved config: %s. The "
                        "function nodes' results cannot be read until it is given: "
                        "PipelineJob.load(path, result_database=...) or "
                        "job.bind(result_database=...).",
                        exc,
                    )

        job = cls(
            name=pipeline._name,
            database=effective_database,
            result_database=effective_result_database,
        )
        job._unresolved_result_database = unresolved_result_database
        job._loaded = True
        job._compiled_pipeline = pipeline
        # Mark compiled so save() / compiled_pipeline can be accessed.
        job._compiled = True

        # Populate _upstreams / _node_lut / _graph_edges / _hash_graph from pipeline.
        job._upstreams = dict(pipeline._upstreams)
        job._node_lut = dict(pipeline._node_lut)
        job._graph_edges = list(pipeline._graph_edges)
        job._hash_graph = pipeline._hash_graph.copy()

        # Populate _persistent_node_map with job-node instances for ALL node types,
        # keyed by the same hash keys as pipeline._persistent_node_map.
        #
        # SourceJobNode stubs must be present so that bind() can locate source slots
        # and update them in-place.  FunctionJobNode and OperatorJobNode stubs must
        # also be present so that:
        #   • _distribute_databases() wires their databases correctly
        #   • job.nodes["label"].get_all_records() works after loading a completed job
        #   • run() includes them in the execution DAG
        from orcapod.core.nodes.function_node import FunctionNode as _FunctionNode
        from orcapod.core.nodes.operator_node import OperatorNode as _OperatorNode
        from orcapod.core.nodes.source_node import SourceNode as _SourceNode
        from orcapod.pipeline.loaded_side_effects import LoadedSideEffectJobNode

        job._sources = dict(sources)

        # Extract the live (data-inclusive) content hashes saved at run time.
        # These are keyed by node label and used below to override the blueprint
        # hash stored in each function and operator node descriptor so that loaded stubs'
        # content_hash() matches the _node_content_hash column in the DB.
        run_block = data.get("run", {})
        node_job_content_hashes: dict[str, str] = run_block.get(
            "node_job_content_hashes", {}
        )
        # Every function, operator and side-effect node records its run's content hash
        # (ITL-1043, ITL-1094): its stored rows carry it, and inputs rebuilt from the
        # file cannot give it. A file without one is malformed and refused, never
        # loaded with a guessed hash (developer, 2026-11-21: no backward compatibility
        # for saved files).
        # An entry that is not a hash text (null, a number) is no record either
        # (CodeRabbit on PR 454).
        unrecorded = sorted(
            bp_node.label
            for bp_node in pipeline._persistent_node_map.values()
            if isinstance(bp_node, (_FunctionNode, _OperatorNode, SideEffectNode))
            and not isinstance(node_job_content_hashes.get(bp_node.label), str)
        )
        if unrecorded:
            raise ValueError(
                f"{path}: saved nodes {unrecorded} have no run content hash text "
                "(run.node_job_content_hashes); a saved job records it for every "
                "function, operator and side-effect node. Save the job again with this "
                "version of Orcapod."
            )
        # The optional, advisory section (ITL-1043): a node with an entry warns when
        # its database holds another number of rows. Without it nothing is checked. The
        # ITL-904 flag (node_has_stored_records) of older files is not used: the database
        # is the source of truth.
        expected_rows: dict[str, int] = run_block.get("expected_rows", {})

        for node_hash, bp_node in pipeline._persistent_node_map.items():
            if isinstance(bp_node, _SourceNode):
                # Keeps the loaded slot's saved hashes and saved type texts (ITL-1138).
                sjn = SourceJobNode._from_loaded_slot(  # pyright: ignore[reportPrivateUsage]  # the loader is the one caller
                    bp_node, bound_source=sources.get(bp_node.name)
                )
                sjn._set_binding_owner(job)  # pyright: ignore[reportPrivateUsage]  # loading assigns the job's active slots
                job._persistent_node_map[node_hash] = sjn

            elif isinstance(bp_node, _FunctionNode):
                descriptor = getattr(bp_node, "_descriptor", None) or {}
                preds = list(job._hash_graph.predecessors(node_hash))
                input_stream = job._persistent_node_map.get(preds[0]) if preds else None
                # Pass the live content hash (from the run) so the stub's
                # content_hash() matches the _node_content_hash column in
                # the DB — required for _filter_by_content_hash() to work
                # in get_all_records() on a loaded job.
                live_content_hash = node_job_content_hashes.get(bp_node.label)
                fjn = FunctionJobNode.from_descriptor(
                    descriptor,
                    input_stream=input_stream,
                    job_content_hash=live_content_hash,
                    expected_row_count=expected_rows.get(bp_node.label),
                )
                job._persistent_node_map[node_hash] = fjn

            elif isinstance(bp_node, _OperatorNode):
                descriptor = getattr(bp_node, "_descriptor", None) or {}
                # The inputs in argument order, as the file gives them (ITL-1417); a
                # graph's predecessors do not keep that order. A read-only node keeps
                # its inputs too, as before.
                preds = pipeline._loaded_input_keys.get(  # pyright: ignore[reportPrivateUsage]  # set by Pipeline.load for this loader
                    node_hash
                ) or list(job._hash_graph.predecessors(node_hash))
                input_streams = tuple(
                    job._persistent_node_map[p]
                    for p in preds
                    if p in job._persistent_node_map
                )
                # Use the reconstructed operator from the blueprint if available.
                operator = getattr(bp_node, "_operator", None)
                # The run's content hash, when the file records it (ITL-1043): the
                # rows the run stored carry it, and the rebuilt inputs cannot give it.
                ojn = OperatorJobNode.from_descriptor(
                    descriptor,
                    operator=operator,
                    input_streams=input_streams,
                    databases={},
                    job_content_hash=node_job_content_hashes.get(bp_node.label),
                    expected_row_count=expected_rows.get(bp_node.label),
                )
                job._persistent_node_map[node_hash] = ojn

            elif isinstance(bp_node, SideEffectNode):
                # Read-only: it never delivers (ITL-1094). Its upstream is the restored
                # job node, so the nodes below it keep their position in the graph.
                preds = list(job._hash_graph.predecessors(node_hash))
                # JobNode predates side-effect nodes; compile() stores them in this map
                # too, through the untyped side_effect_node_class.
                job._persistent_node_map[node_hash] = LoadedSideEffectJobNode(  # pyright: ignore[reportArgumentType]
                    getattr(bp_node, "_descriptor", None) or {},
                    input_stream=job._persistent_node_map[preds[0]],
                    job_content_hash=node_job_content_hashes.get(bp_node.label),
                )

        # Update _nodes to point to job nodes (FunctionJobNode/OperatorJobNode stubs)
        # rather than blueprint nodes, so that job.nodes["label"].get_all_records()
        # works on a loaded job.
        bp_id_to_hash: dict[int, str] = {
            id(bp): h for h, bp in pipeline._persistent_node_map.items()
        }
        job._nodes = {
            label: job._persistent_node_map.get(
                bp_id_to_hash.get(id(bp_node), ""), bp_node
            )
            for label, bp_node in pipeline._nodes.items()
        }

        # Build job._node_graph as OrcaDAG[JobNode] from the job's own persistent
        # node map.  This makes job.dag consistent with the compiled-job path
        # (where compile() builds _node_graph from job nodes).
        # Callers needing blueprint nodes for rendering use job.pipeline.dag.
        job_dag: OrcaDAG = OrcaDAG()
        for node in job._persistent_node_map.values():
            job_dag.add_node(node)
        for u_hash, v_hash in job._graph_edges:
            u_node = job._persistent_node_map.get(u_hash)
            v_node = job._persistent_node_map.get(v_hash)
            if u_node is not None and v_node is not None:
                job_dag.add_edge(u_node, v_node)
        job._node_graph = job_dag

        if effective_database is not None:
            job._distribute_databases()

        # Restore run metadata (run_block was already parsed earlier in this method)
        status = run_block.get("status", "pending")
        if status in ("complete", "partial"):
            job._has_run = True
        if run_id := run_block.get("run_id"):
            job._run_id = run_id

        return job
