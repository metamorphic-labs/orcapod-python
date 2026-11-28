from __future__ import annotations

import copy
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import orcapod
from orcapod.contexts import resolve_saved_context
from orcapod.core.nodes import (
    FunctionNode,
    GraphNode,
    OperatorNode,
    SourceNode,
)
from orcapod.side_effects import SideEffectNode
from orcapod.core.tracker import AutoRegisteringContextBasedTracker
from orcapod.pipeline.base import AbstractPipelineBase
from orcapod.pipeline.dag import OrcaDAG
from orcapod.protocols import core_protocols as cp
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import networkx as nx
else:
    nx = LazyModule("networkx")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class Pipeline(AbstractPipelineBase[GraphNode]):
    """A pure computational blueprint recording operator and function pod invocations.

    During the ``with`` block, operator and function pod invocations are
    recorded into an internal graph via the unified ``_record_invocation()``
    path inherited from ``AbstractPipelineBase``. On context exit,
    ``compile()`` (also inherited) rewires the graph into a frozen DAG:

    - Leaf streams not registered as invocations → ``SourceNode`` declarations
    - Function pod invocations → ``FunctionNode``
    - Operator invocations → ``OperatorNode``

    To run a ``Pipeline``, use
    ``PipelineJob.from_pipeline(pipeline, sources=..., database=...)`` to create
    a ``PipelineJob``.

    Args:
        name: Pipeline name (string or tuple). Used as the path prefix for
            all cache/pipeline paths when the pipeline is run via a
            ``PipelineJob``.
        auto_compile: If ``True`` (default), ``compile()`` is called
            automatically when the context manager exits.
    """

    # ------------------------------------------------------------------
    # Node-factory class attributes (used by AbstractPipelineBase.compile())
    # ------------------------------------------------------------------

    source_node_class = SourceNode
    function_node_class = FunctionNode
    operator_node_class = OperatorNode
    side_effect_node_class = SideEffectNode

    # The saved output schema of every node of a pipeline loaded from a file (by hash
    # key): a node whose Arrow types cannot be derived yet (below a slot whose class is
    # not registered) is saved again from it, never from its Python types (ITL-640).
    _loaded_output_schemas: "dict[str, dict[str, Any]]" = {}

    def __init__(
        self,
        name: str | tuple[str, ...],
        tracker_manager: cp.TrackerManagerProtocol | None = None,
        auto_compile: bool = True,
    ) -> None:
        """Initialize a pure computational blueprint pipeline.

        Args:
            name: Pipeline name (string or tuple). Used to scope database paths
                when the pipeline is run via a ``PipelineJob``.
            tracker_manager: Optional tracker manager override. Defaults to
                ``DEFAULT_TRACKER_MANAGER``.
            auto_compile: If ``True`` (default), ``compile()`` is called
                automatically when the context manager exits.
        """
        super().__init__(name=name, tracker_manager=tracker_manager)
        self._auto_compile = auto_compile
        # A loaded blueprint's operator input keys in argument order (ITL-1417).
        self._loaded_input_keys: dict[str, list[str]] = {}

    # ------------------------------------------------------------------
    # Context manager — respects auto_compile flag
    # ------------------------------------------------------------------

    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        # Call AutoRegisteringContextBasedTracker.__exit__ directly (deactivates the tracker)
        # but NOT AbstractPipelineBase.__exit__ (which calls compile() unconditionally).
        AutoRegisteringContextBasedTracker.__exit__(
            self, exc_type, exc_value, traceback
        )
        if exc_type is None and self._auto_compile:
            self.compile()

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def save(self, path: str | Path) -> None:
        """Serialize the pure pipeline blueprint to a JSON file.

        Saves the full pipeline topology: SourceNode declarations, function
        and operator pod configurations, and all edge connections.  Runtime
        state — databases, execution context, and run metadata — is not
        persisted.

        Args:
            path: File path to write JSON output to.

        Raises:
            ValueError: If the pipeline has not been compiled.
        """
        if not self._compiled:
            raise ValueError(
                "Pipeline is not compiled. Call compile() or use "
                "auto_compile=True before saving."
            )

        import json as _json
        from orcapod.pipeline.serialization import (
            FALL_THROUGH_TAGS_KEY,
            OPERATOR_INPUTS_KEY,
            PIPELINE_FORMAT_VERSION,
            serialize_arrow_schemas,
            serialize_output_schema,
        )
        from orcapod.core.nodes import OperatorNode, FunctionNode
        from orcapod.core.nodes.source_node import SourceNode as SourceNodeClass
        from orcapod.core.streams.base import shared_zero_row_memo, typed_arrow_schemas

        # Every node's own Arrow types, derived in one walk: an upstream that several
        # nodes share is typed once (ITL-640).
        # A node whose Arrow types cannot be derived yet (a loaded slot whose class is
        # not registered, and loaded nodes below it) is saved from the type texts it
        # was loaded with, never from its Python types (ITL-640).
        arrow_types: dict[str, tuple[Any, Any]] = {}
        retained: dict[str, dict[str, Any]] = {}
        # A slot's column types, from the one writer (``saved_type_texts``): its
        # output_schema and its source_config are both this entry (ITL-640).
        slot_types: dict[str, dict[str, Any]] = {}
        with shared_zero_row_memo():
            for key, node in self._persistent_node_map.items():
                if isinstance(node, SourceNodeClass):
                    try:
                        slot_types[key] = node.saved_type_texts()
                    except ValueError as exc:
                        # A type text that would not read back (CodeRabbit on PR 454).
                        exc.add_note(f"Saving the pipeline's node {node.label!r}")
                        raise
                    continue
                try:
                    arrow_types[key] = typed_arrow_schemas(node)
                except (ValueError, TypeError):
                    stored = getattr(node, "_stored_schema", None) or (
                        self._loaded_output_schemas.get(key)
                    )
                    if not stored:
                        raise
                    retained[key] = stored
        nodes: dict[str, Any] = {}
        key_of = {id(node): key for key, node in self._persistent_node_map.items()}
        for content_hash_str, node in self._persistent_node_map.items():
            try:
                type_converter = node.data_context.type_converter
            except (AttributeError, TypeError):
                from orcapod.contexts import resolve_context

                type_converter = resolve_context(None).type_converter

            try:
                data_context_key = node.data_context_key
            except (AttributeError, TypeError):
                # Stub nodes (loaded with no live operator/function_pod) store
                # the data context directly on _data_context; fall back to it.
                _dc = getattr(node, "_data_context", None)
                data_context_key = _dc.context_key if _dc is not None else None

            import dataclasses

            from orcapod.config import DEFAULT_CONFIG as _DEFAULT_CONFIG

            # Save None when the config matches DEFAULT_CONFIG so that future
            # changes to the default are picked up on load (forward-compatible).
            # See ENG-544 for the tradeoff discussion (reproducibility vs.
            # forward-compatibility).
            _cfg = node.orcapod_config
            config_val = None if _cfg == _DEFAULT_CONFIG else dataclasses.asdict(_cfg)

            try:
                output_schema: dict[str, Any] = (
                    copy.deepcopy(slot_types[content_hash_str])
                    if content_hash_str in slot_types
                    else serialize_arrow_schemas(
                        *arrow_types[content_hash_str], type_converter
                    )
                    if content_hash_str in arrow_types
                    else dict(retained[content_hash_str])
                    if content_hash_str in retained
                    # Only this branch reads the node's Python schema: a loaded node
                    # whose saved schema names an unregistered logical type keeps
                    # its retained texts above, and saving again never asks for a
                    # Python type it cannot restore (CodeRabbit on PR 454).
                    else serialize_output_schema(*node.output_schema(), type_converter)
                )
            except ValueError as exc:
                # A type text that would not read back (CodeRabbit on PR 454).
                exc.add_note(f"Saving the pipeline's node {node.label!r}")
                raise

            descriptor: dict[str, Any] = {
                "node_type": node.node_type,
                "label": node.label,
                "content_hash": node.content_hash().to_string(),
                "pipeline_hash": node.pipeline_hash().to_string(),
                # A node's own Arrow types are the saved authority (ITL-640): the
                # types its rows have, from its typed zero-row table, never types
                # re-derived from Python ones (which lose time64[ns] or a decimal's
                # precision). A slot whose class is not registered keeps its saved
                # text below (ITL-1138).
                "output_schema": output_schema,
                "node_uri": list(node.node_uri),
                "data_context_key": data_context_key,
                "config": config_val,
            }

            match node:
                case SourceNodeClass():
                    # The slot's column types from the one writer (ITL-640): its own
                    # Arrow pair, or, while unresolved, the Arrow type texts it was
                    # loaded with, every column (an int32 stays int32).
                    saved = slot_types[content_hash_str]
                    descriptor["source_config"] = {
                        "source_type": "node",
                        "name": node.name,
                        "tag_schema": dict(saved["tag"]),
                        "data_schema": dict(saved["data"]),
                        "nullable": copy.deepcopy(saved["nullable"]),
                    }
                    descriptor["reconstructable"] = True

                case FunctionNode():
                    if node._function_pod is not None:
                        descriptor["function_config"] = node._function_pod.to_config()
                    elif "function_config" in node._descriptor:
                        # A loaded read-only node (its pod not rebuilt, e.g. an output
                        # of an unregistered class) writes its saved config back as it
                        # is, so saving again keeps the file's entry (PR 454).
                        descriptor["function_config"] = copy.deepcopy(
                            node._descriptor["function_config"]
                        )
                    descriptor["table_scope"] = node._table_scope
                    # Written for every function node, also a plain one (ITL-1019).
                    descriptor[FALL_THROUGH_TAGS_KEY] = list(node.declared_data_tags())

                case OperatorNode():
                    # The inputs in argument order (ITL-1417): an ordered operator
                    # (MergeJoin, SemiJoin) is rebuilt from this list, never from the
                    # edge list, which is sorted by hash before serialization.
                    inputs = [
                        key_of[id(upstream)]
                        for upstream in node.upstreams
                        if id(upstream) in key_of
                    ]
                    if not inputs and OPERATOR_INPUTS_KEY in node._descriptor:
                        # A loaded read-only node holds no inputs: its saved list is
                        # written back as it is, so saving again keeps the entry.
                        inputs = list(node._descriptor[OPERATOR_INPUTS_KEY])
                    if inputs:
                        descriptor[OPERATOR_INPUTS_KEY] = inputs
                    if node._operator is not None:
                        descriptor["operator_config"] = node._operator.to_config()
                    elif "operator_config" in node._descriptor:
                        # A loaded read-only node writes its saved config back as it
                        # is (PR 454).
                        descriptor["operator_config"] = copy.deepcopy(
                            node._descriptor["operator_config"]
                        )
                    descriptor["table_scope"] = node._table_scope

            nodes[content_hash_str] = descriptor

        output: dict[str, Any] = {
            "orcapod_pipeline_version": PIPELINE_FORMAT_VERSION,
            "orcapod_version": orcapod.__version__,
            "pipeline": {"name": list(self._name)},
            "nodes": nodes,
            "edges": [list(edge) for edge in self._graph_edges],
        }

        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            _json.dump(output, f, indent=2)

    @classmethod
    def load(cls, path: str | Path) -> "Pipeline":
        """Deserialize a pure pipeline blueprint from a JSON file.

        Reconstructs topology and SourceNode declarations. The loaded
        pipeline is topology-only — to run it, use
        ``PipelineJob.from_pipeline(pipeline, sources=..., database=...)``.

        Args:
            path: Path to the JSON file produced by ``save()``.

        Returns:
            A compiled ``Pipeline`` instance with SourceNode leaf nodes.

        Raises:
            ValueError: If the file's format version is not the one this Orcapod
                writes (an earlier format is refused, never migrated).
        """
        import json as _json
        from orcapod.pipeline.serialization import (
            FALL_THROUGH_TAGS_KEY,
            OPERATOR_INPUTS_KEY,
            PIPELINE_FORMAT_VERSION,
            require_current_format,
            require_positively_commutative,
            require_nullability_records,
            arrow_schema_from_texts,
            deserialize_schema,
            saved_fall_through_tags,
        )
        from orcapod.core.nodes import FunctionNode, OperatorNode
        from orcapod.core.nodes.source_node import SourceNode as SourceNodeClass
        from orcapod.types import Schema

        path = Path(path)
        with open(path) as f:
            data = _json.load(f)

        try:
            require_current_format(
                data.get("orcapod_pipeline_version", ""), PIPELINE_FORMAT_VERSION
            )
        except ValueError as exc:
            exc.add_note(f"Loading the pipeline file {path}")
            raise

        pipeline_meta = data["pipeline"]
        name = tuple(pipeline_meta["name"])
        nodes_data = data["nodes"]
        edges = data["edges"]

        # Build topological order
        edge_graph: "nx.DiGraph" = nx.DiGraph()
        for up_hash, down_hash in edges:
            edge_graph.add_edge(up_hash, down_hash)
        for node_hash in nodes_data:
            if node_hash not in edge_graph:
                edge_graph.add_node(node_hash)
        topo_order = nx.lexicographical_topological_sort(edge_graph)

        upstream_map: dict[str, list[str]] = {}
        for up_hash, down_hash in edges:
            upstream_map.setdefault(down_hash, []).append(up_hash)

        reconstructed: dict[str, GraphNode] = {}
        # Each operator's input keys in argument order, for the job loader (ITL-1417).
        loaded_input_keys: dict[str, list[str]] = {}

        for node_hash in topo_order:
            descriptor = nodes_data.get(node_hash)
            if descriptor is None:
                continue

            from orcapod.logical_types.identifier import LogicalTypeIdentifierError
            from orcapod.pipeline.serialization import (
                SavedSchemaVersionError,
                deserialize_output_schema,
            )

            # Every node records its nullability (ITL-1043), and every function node
            # names its data-tags (ITL-1019).
            try:
                require_nullability_records(descriptor)
                if descriptor.get("node_type") == "function":
                    descriptor[FALL_THROUGH_TAGS_KEY] = list(
                        saved_fall_through_tags(descriptor)
                    )
            except ValueError as exc:
                raise ValueError(
                    f"{path}: saved node {descriptor.get('label')!r}: {exc}"
                ) from exc

            # Validate even a leaf node's saved identifiers before exposing the job.
            context = resolve_saved_context(descriptor.get("data_context_key"))
            try:
                deserialize_output_schema(
                    descriptor.get("output_schema", {}), context.type_converter
                )
            except (LogicalTypeIdentifierError, SavedSchemaVersionError) as exc:
                exc.args = (
                    f"Cannot restore the output schema of loaded node "
                    f"{descriptor.get('label')!r}: {exc}",
                )
                raise
            except ValueError as exc:
                logger.warning(
                    "Cannot restore the output schema of loaded node %r: %s",
                    descriptor.get("label"),
                    exc,
                )

            node_type = descriptor.get("node_type")
            source_config = descriptor.get("source_config") or {}

            if node_type == "source":
                source_type = source_config.get("source_type")
                if source_type == "node":
                    node_name = source_config.get("name") or source_config.get(
                        "node_name"
                    )
                    if not node_name:
                        node_name = descriptor.get("label") or "unknown"
                    # Which columns may be null: recorded (``require_nullability_records``).
                    saved_nullable: dict[str, list[str]]
                    if "tag_schema" in source_config and "data_schema" in source_config:
                        saved_tag = source_config["tag_schema"]
                        saved_data = source_config["data_schema"]
                        saved_nullable = source_config["nullable"]
                    else:
                        saved_tag = descriptor["output_schema"]["tag"]
                        saved_data = descriptor["output_schema"]["data"]
                        saved_nullable = descriptor["output_schema"]["nullable"]
                    tag_schema = Schema(
                        deserialize_schema(
                            saved_tag,
                            context.type_converter,
                            nullable=saved_nullable["tag"],
                        )
                    )
                    data_schema = Schema(
                        deserialize_schema(
                            saved_data,
                            context.type_converter,
                            nullable=saved_nullable["data"],
                        )
                    )
                    saved_context = resolve_saved_context(
                        descriptor.get("data_context_key")
                    )
                    # The slot's Arrow types, rebuilt from the saved text (ITL-640);
                    # none while a column's class is not registered in this process.
                    try:
                        arrow_schemas = (
                            arrow_schema_from_texts(
                                saved_tag, context.type_converter, saved_nullable["tag"]
                            ),
                            arrow_schema_from_texts(
                                saved_data,
                                context.type_converter,
                                saved_nullable["data"],
                            ),
                        )
                    except (ValueError, TypeError, KeyError):
                        arrow_schemas = None
                    node = SourceNodeClass(
                        name=node_name,
                        tag_schema=tag_schema,
                        data_schema=data_schema,
                        data_context=saved_context,
                        arrow_schemas=arrow_schemas,
                    )
                    # The saved hashes, and the saved type text of a column whose
                    # class this process has not registered yet (ITL-1138).
                    node._keep_saved(  # pyright: ignore[reportPrivateUsage]  # the loader is the one caller
                        pipeline_hash=descriptor.get("pipeline_hash"),
                        content_hash=descriptor.get("content_hash"),
                        saved_types={"tag": saved_tag, "data": saved_data},
                        nullable=saved_nullable,
                    )
                    # Restore label from descriptor if set explicitly
                    stored_label = descriptor.get("label")
                    if stored_label and stored_label != node_name:
                        node._label = stored_label
                else:
                    raise ValueError(
                        f"Unknown source_type {source_type!r} in pipeline descriptor."
                    )
                reconstructed[node_hash] = node

            elif node_type == "function":
                up_hashes = upstream_map.get(node_hash, [])
                upstream_node = reconstructed.get(up_hashes[0]) if up_hashes else None
                node = FunctionNode.from_descriptor(
                    descriptor,
                    function_pod=None,
                    input_stream=upstream_node,
                    databases={},
                )
                reconstructed[node_hash] = node

            elif node_type == "operator":
                # The inputs in argument order (ITL-1417); a file saved before the
                # list was written gives only the edges, in recording order.
                saved_inputs = descriptor.get(OPERATOR_INPUTS_KEY)
                up_hashes = (
                    list(saved_inputs)
                    if saved_inputs is not None
                    else upstream_map.get(node_hash, [])
                )
                upstream_nodes = tuple(
                    reconstructed[h] for h in up_hashes if h in reconstructed
                )
                # Attempt to reconstruct the operator from its saved config via
                # the centralised resolver so registry logic and error handling
                # are consistent with all other deserialization paths.
                operator = None
                op_config = descriptor.get("operator_config")
                if op_config:
                    try:
                        from orcapod.pipeline.serialization import (
                            resolve_operator_from_config,
                        )

                        operator = resolve_operator_from_config(op_config)
                    except Warning:
                        # A warning turned into an error (a ProvisionalHashWarning
                        # under an error filter) propagates (ITL-1331).
                        raise
                    except Exception as exc:
                        logger.warning(
                            "Could not reconstruct operator %r from config — "
                            "node will be in read-only mode: %s",
                            op_config.get("class_name"),
                            exc,
                        )
                if operator is None and op_config:
                    # A provisional operator kept read-only (Pick and Index have no
                    # config to rebuild from, ITL-1326) never reaches its constructor:
                    # the loaded job warns for it here, as a rebuilt one does (ITL-1279).
                    from orcapod.core.operators.provisional import (
                        PROVISIONAL_OPERATORS,
                        warn_provisional_operator,
                    )

                    saved_name = op_config.get("class_name")
                    if saved_name in PROVISIONAL_OPERATORS:
                        warn_provisional_operator(saved_name)
                if operator is not None:
                    from orcapod.logical_types.identifier import (
                        LogicalTypeIdentifierError,
                    )
                    from orcapod.pipeline.serialization import SavedSchemaVersionError

                    try:
                        for upstream in upstream_nodes:
                            upstream.output_schema()
                    except (LogicalTypeIdentifierError, SavedSchemaVersionError):
                        raise
                    except ValueError as exc:
                        logger.warning(
                            "Cannot reconstruct operator node %r because an input "
                            "schema cannot be restored; keeping a read-only stub: %s",
                            descriptor.get("label"),
                            exc,
                        )
                        operator = None
                if saved_inputs is None:
                    require_positively_commutative(
                        operator, upstream_nodes, up_hashes, descriptor.get("label")
                    )
                loaded_input_keys[node_hash] = list(up_hashes)
                node = OperatorNode.from_descriptor(
                    descriptor,
                    operator=operator,
                    input_streams=upstream_nodes,
                    databases={},
                )
                reconstructed[node_hash] = node

            elif node_type == "side_effect":
                from orcapod.pipeline.loaded_side_effects import LoadedSideEffectNode

                # Restored read-only: the file holds no side-effect function (ITL-1094).
                up_hashes = upstream_map.get(node_hash, [])
                upstream_node = reconstructed.get(up_hashes[0]) if up_hashes else None
                if upstream_node is None:
                    # Its input is a node this version does not restore: leave it out,
                    # as that node; the rest of the file still loads.
                    logger.warning(
                        "Skipping the saved side-effect node %r: its upstream node was "
                        "not restored from the file.",
                        descriptor.get("label"),
                    )
                    continue
                # GraphNode predates side-effect nodes; compile() stores them in the
                # same maps through the untyped side_effect_node_class.
                reconstructed[node_hash] = LoadedSideEffectNode(  # pyright: ignore[reportArgumentType]
                    descriptor, input_stream=upstream_node
                )

        # Keep old files' edge order only for the legacy input-order fallback above.
        edges = sorted((up, down) for up, down in edges)
        reconstructed = cls._ordered_node_map(reconstructed, edge_graph)

        # Build Pipeline instance
        pipeline = cls(name=name, auto_compile=False)
        pipeline._loaded_output_schemas = {
            node_hash: descriptor["output_schema"]
            for node_hash, descriptor in nodes_data.items()
            if isinstance(descriptor.get("output_schema"), dict)
        }
        pipeline._persistent_node_map = dict(reconstructed)
        pipeline._loaded_input_keys = loaded_input_keys

        nodes_by_label: dict[str, GraphNode] = {}
        for node in reconstructed.values():
            if node.label:
                if node.label in nodes_by_label:
                    logger.warning(
                        "Label collision in loaded pipeline: %r. "
                        "The first node with this label wins.",
                        node.label,
                    )
                else:
                    nodes_by_label[node.label] = node
        pipeline._nodes = nodes_by_label

        node_dag: OrcaDAG[GraphNode] = OrcaDAG()
        for node in reconstructed.values():
            node_dag.add_node(node)
        for up_hash, down_hash in edges:
            up_node = reconstructed.get(up_hash)
            down_node = reconstructed.get(down_hash)
            if up_node is not None and down_node is not None:
                node_dag.add_edge(up_node, down_node)
        pipeline._node_graph = node_dag

        pipeline._graph_edges = edges
        pipeline._hash_graph = nx.DiGraph()
        for up_hash, down_hash in edges:
            pipeline._hash_graph.add_edge(up_hash, down_hash)
        for node_hash, node in reconstructed.items():
            if node_hash not in pipeline._hash_graph:
                pipeline._hash_graph.add_node(node_hash)
            attrs = pipeline._hash_graph.nodes[node_hash]
            attrs["node_type"] = node.node_type
            if node.label:
                attrs["label"] = node.label

        # Restore _node_lut and _upstreams so PipelineJob can substitute bound
        # sources and build a correct execution graph at run time.
        pipeline._node_lut = {
            h: n for h, n in reconstructed.items() if n.node_type != "source"
        }
        # SourceNode IS the upstream — store it directly so run() can find it
        # by hash and substitute a concrete source at run time.
        pipeline._upstreams = {
            h: n for h, n in reconstructed.items() if n.node_type == "source"
        }

        pipeline._compiled = True
        return pipeline

    def _clone_for_execution(self) -> "Pipeline":
        """Create a lightweight copy of this compiled pipeline for isolated execution.

        All structural state (``_node_lut``, ``_upstreams``, ``_graph_edges``,
        ``_hash_graph``, ``_node_graph``, ``_persistent_node_map``) is shared
        read-only with the original — these are immutable after ``compile()``.
        Only ``_nodes`` (the label → exec-node mapping) gets its own copy so
        that execution setup can update it without affecting other
        ``PipelineJob`` instances that reference this blueprint.

        The clone is never registered as a tracker context manager.

        Returns:
            A new ``Pipeline`` instance sharing read-only state with ``self``.
        """
        clone = Pipeline.__new__(Pipeline)
        # Base class state — clone is inactive and never registered
        clone._tracker_manager = self._tracker_manager
        clone._active = False
        # Shared read-only structural state (recording + compiled)
        clone._name = self._name
        clone._invocation_lut = self._invocation_lut
        clone._invocation_input_keys = self._invocation_input_keys
        clone._source_streams = self._source_streams
        clone._node_lut = self._node_lut
        clone._upstreams = self._upstreams
        clone._graph_edges = self._graph_edges
        clone._hash_graph = self._hash_graph
        clone._persistent_node_map = self._persistent_node_map
        clone._node_graph = self._node_graph
        clone._auto_compile = self._auto_compile
        clone._compiled = self._compiled
        clone._loaded_job_graph = self._loaded_job_graph
        clone._loaded_output_schemas = self._loaded_output_schemas
        # Mutable per-execution state — own copy so runs don't interfere
        clone._nodes = dict(self._nodes)
        return clone

    def __dir__(self) -> list[str]:
        return list(super().__dir__()) + list(self._nodes.keys())
