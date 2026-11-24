"""Pipeline serialization registries, helpers, and constants."""

from __future__ import annotations

import hashlib
import json as _json
import logging
import re
from collections.abc import Collection, Iterable
from enum import Enum
from typing import TYPE_CHECKING, Any, cast

from orcapod.types import Schema
from orcapod.logical_types.canonical_form import (
    declaration_form_field,
    declaration_form_schema,
    storage_form_schema,
)
from orcapod.logical_types.identifier import (
    LogicalTypeIdentifier,
    LogicalTypeIdentifierError,
    arrow_type_identifier,
    require_readable,
)

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.database_protocols import DatabaseRegistryProtocol

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Format version
# ---------------------------------------------------------------------------

# The version a pipeline file is written at, and the only one a load reads.
#   0.4.0: a nested field name in saved type text that is not plain is JSON-quoted
#          (ITL-640, ``_nested_field_name_text``); a map's key and item are written as
#          whole fields. Every source config that takes the option writes
#          ``tag_only_columns`` and every function node writes ``fall_through_tags``;
#          a load refuses a file without them (ITL-1159, ITL-1019).
#          Each operator node lists its inputs in argument order under ``inputs``
#          (ITL-1417, added within 0.4.0: a file without it still loads, except an
#          operator over two or more inputs not known to be commutative, refused).
# A file of an earlier format (0.1.0 to 0.3.0) is refused at load, never read in its
# old grammar: load and save had not been used in production (developer, 2026-11-21).
# Taking such files to the current format is the migration tool's work (ITL-1048).
PIPELINE_FORMAT_VERSION = "0.4.0"

#: The key of an operator node's inputs in a saved file, their keys in argument order
#: (ITL-1417). Written for every operator node within format 0.4.0: a file saved
#: before it has none, and its edges list the inputs in the order the steps were
#: recorded, not in argument order.
OPERATOR_INPUTS_KEY = "inputs"


def require_positively_commutative(
    operator: Any,
    input_streams: tuple[Any, ...],
    input_keys: list[str],
    label: str | None,
) -> None:
    """Accept an operator of a legacy file only when it is known to be commutative.

    A file saved before ``OPERATOR_INPUTS_KEY`` was written lists an operator's
    inputs only through its edges, in the order the steps were recorded (ITL-1417).
    Its argument order cannot be known, so an operator node with two or more
    different inputs is accepted only when it is positively known to be fully
    commutative: the operator is available, and ``argument_symmetry``, called as its
    contract requires (on the rebuilt input streams), returns a flat ``frozenset`` of
    exactly those inputs. A binary operator with other than two edges (one stream
    given twice) cannot have its inputs restored and is refused too. Anything else (the call raises, a tuple, a nested group, an
    operator that is not available, an input not restored) is refused: when in doubt,
    refuse.

    Args:
        operator: The rebuilt operator, or ``None`` when it is not available.
        input_streams: The rebuilt input nodes.
        input_keys: The keys of its inputs, in edge order.
        label: The node's label, for the message.

    Raises:
        ValueError: If the operator is not positively known to be commutative.
    """
    from orcapod.core.operators.base import BinaryOperator

    # A binary operator given one stream twice has one edge in a legacy file (an edge
    # list cannot repeat an edge), so its two inputs cannot be restored (CodeRabbit on
    # PR 513): refused like an unknown order, never rebuilt with one input.
    one_input_of_two = isinstance(operator, BinaryOperator) and len(input_keys) != 2
    if len(set(input_keys)) < 2 and not one_input_of_two:
        return
    refusal = ValueError(
        f"The saved job's node {label!r} has several inputs, and the file does not say "
        "in which order: it was saved before Orcapod wrote each operator's inputs in "
        "argument order (ITL-1417), and the operator is not known to be commutative, "
        "so loading it could swap them. Build the job from its code and save it again."
    )
    if one_input_of_two:
        raise ValueError(
            f"The saved job's node {label!r} takes two inputs, and the file does not "
            "say in which order nor restore both: it was saved before Orcapod wrote "
            "each operator's inputs in argument order (ITL-1417), and it holds one edge "
            "for one stream given twice. Build the job from its code and save it again."
        )
    if operator is None or len(input_streams) != len(input_keys):
        raise refusal
    try:
        symmetry = operator.argument_symmetry(input_streams)
    except Exception as exc:
        raise refusal from exc
    if not isinstance(symmetry, frozenset):
        raise refusal
    members = cast("frozenset[Any]", symmetry)
    if {id(member) for member in members} != {id(stream) for stream in input_streams}:
        raise refusal


def require_current_format(version: str, current: str) -> None:
    """Refuse a saved file whose format version is not the one this Orcapod writes.

    Args:
        version: The format version the file records.
        current: The format version this Orcapod writes and reads.

    Raises:
        ValueError: If ``version`` is not ``current``.
    """
    if version != current:
        raise ValueError(
            f"Unsupported format version {version!r}: the file was saved with format "
            f"{version!r}; this Orcapod reads only format {current}. Save the "
            "pipeline again with this version of Orcapod."
        )


# ---------------------------------------------------------------------------
# LoadStatus
# ---------------------------------------------------------------------------


class LoadStatus(Enum):
    """Status of a node after loading from a serialized pipeline."""

    FULL = "full"
    READ_ONLY = "read_only"
    CACHE_ONLY = "cache_only"
    UNAVAILABLE = "unavailable"


# ---------------------------------------------------------------------------
# DatabaseRegistry
# ---------------------------------------------------------------------------


class DatabaseRegistry:
    """Deduplicates database configs into a keyed registry at save time.

    Keys are deterministic: same config → same key, cross-session stable.
    Format: ``db_{sha256[:8]}`` with ``_2``, ``_3``... suffixes on collision.
    """

    def __init__(self) -> None:
        self._entries: dict[str, dict] = {}
        # Reverse map: canonical JSON string → assigned key, for O(1) re-registration
        self._reverse: dict[str, str] = {}

    def register(self, config: dict) -> str:
        """Register a database config and return its registry key.

        If the config is already registered (under any key), returns the existing key.
        On hash collision (different configs with same 8-char prefix), appends
        ``_2``, ``_3``... to the key.
        """
        canonical = _json.dumps(config, sort_keys=True, separators=(",", ":"))

        # O(1) lookup via reverse map
        if canonical in self._reverse:
            return self._reverse[canonical]

        # Not registered yet — find the right key
        base_key = self._make_key(config)
        if base_key not in self._entries:
            key = base_key
        else:
            i = 2
            while f"{base_key}_{i}" in self._entries:
                i += 1
            key = f"{base_key}_{i}"

        self._entries[key] = config
        self._reverse[canonical] = key
        return key

    def resolve(self, key: str) -> dict:
        """Return the config dict for a registry key."""
        return self._entries[key]

    def to_dict(self) -> dict[str, dict]:
        """Return the full registry for JSON serialization."""
        return dict(self._entries)

    @classmethod
    def from_dict(cls, data: dict[str, dict]) -> DatabaseRegistry:
        """Reconstruct a registry from the saved ``databases`` block."""
        registry = cls()
        registry._entries = dict(data)
        registry._reverse = {
            _json.dumps(cfg, sort_keys=True, separators=(",", ":")): key
            for key, cfg in data.items()
        }
        return registry

    @staticmethod
    def _make_key(config: dict) -> str:
        canonical = _json.dumps(config, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(canonical.encode()).hexdigest()[:8]
        return f"db_{digest}"


# ---------------------------------------------------------------------------
# Registries
# ---------------------------------------------------------------------------


def _build_database_registry() -> dict[str, type]:
    """Build the database type registry mapping type keys to classes.

    Returns:
        Dict mapping type key strings to database classes.
    """
    from orcapod.databases.delta_lake_databases import DeltaTableDatabase
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
    from orcapod.databases.noop_database import NoOpArrowDatabase

    return {
        "delta_table": DeltaTableDatabase,
        "in_memory": InMemoryArrowDatabase,
        "noop": NoOpArrowDatabase,
    }


def _build_source_registry() -> dict[str, type]:
    """Build the source type registry mapping type keys to classes.

    Returns:
        Dict mapping type key strings to source classes.
    """
    from orcapod.core.sources.arrow_table_source import ArrowTableSource
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.csv_source import CSVSource
    from orcapod.core.sources.data_frame_source import DataFrameSource
    from orcapod.core.sources.delta_table_source import DeltaTableSource
    from orcapod.core.sources.derived_source import DerivedSource
    from orcapod.core.sources.dict_source import DictSource
    from orcapod.core.sources.list_source import ListSource
    from orcapod.core.sources.spiraldb_table_source import SpiralDBTableSource
    from orcapod.core.sources.sqlite_table_source import SQLiteTableSource
    from orcapod.core.sources.postgresql_table_source import PostgreSQLTableSource

    return {
        CSVSource.SOURCE_KIND: CSVSource,
        DeltaTableSource.SOURCE_KIND: DeltaTableSource,
        DictSource.SOURCE_KIND: DictSource,
        ListSource.SOURCE_KIND: ListSource,
        DataFrameSource.SOURCE_KIND: DataFrameSource,
        ArrowTableSource.SOURCE_KIND: ArrowTableSource,
        CachedSource.SOURCE_KIND: CachedSource,
        DerivedSource.SOURCE_KIND: DerivedSource,
        SpiralDBTableSource.SOURCE_KIND: SpiralDBTableSource,
        SQLiteTableSource.SOURCE_KIND: SQLiteTableSource,
        PostgreSQLTableSource.SOURCE_KIND: PostgreSQLTableSource,
    }


def _build_operator_registry() -> dict[str, type]:
    """Build the operator type registry mapping class names to classes.

    Returns:
        Dict mapping class name strings to operator classes.
    """
    from orcapod.core.operators import (
        AddTags,
        Batch,
        DropColumns,
        DropDataColumns,
        DropNulls,
        DropTagColumns,
        Filter,
        GroupBy,
        Join,
        MapColumns,
        MapData,
        MapTags,
        MergeJoin,
        PolarsFilter,
        Rejoin,
        SelectColumns,
        SelectDataColumns,
        SelectTagColumns,
        SemiJoin,
        TagBy,
    )

    return {
        "Join": Join,
        "Rejoin": Rejoin,
        "MergeJoin": MergeJoin,
        "SemiJoin": SemiJoin,
        "Batch": Batch,
        "GroupBy": GroupBy,
        "SelectTagColumns": SelectTagColumns,
        "DropTagColumns": DropTagColumns,
        "SelectDataColumns": SelectDataColumns,
        "DropDataColumns": DropDataColumns,
        "SelectColumns": SelectColumns,
        "DropColumns": DropColumns,
        "MapTags": MapTags,
        "MapData": MapData,
        "MapColumns": MapColumns,
        "PolarsFilter": PolarsFilter,
        "Filter": Filter,
        "DropNulls": DropNulls,
        "TagBy": TagBy,
        "AddTags": AddTags,
    }


def _build_data_function_registry() -> dict[str, type]:
    """Build the data function type registry mapping type IDs to classes.

    Returns:
        Dict mapping type ID strings to data function classes.
    """
    from orcapod.core.data_function import PythonDataFunction
    from orcapod.core.data_function import PYTHON_FUNCTION

    return {
        PYTHON_FUNCTION.type_id: PythonDataFunction,
    }


# Registries populated at module load time.
# Each registry maps a type key (or class name) to the corresponding class.
DATABASE_REGISTRY: dict[str, type] = _build_database_registry()
SOURCE_REGISTRY: dict[str, type] = _build_source_registry()
OPERATOR_REGISTRY: dict[str, type] = _build_operator_registry()
DATA_FUNCTION_REGISTRY: dict[str, type] = _build_data_function_registry()


_observer_registry: dict[str, Any] | None = None


def _get_observer_registry() -> dict[str, Any]:
    global _observer_registry
    if _observer_registry is None:
        from orcapod.pipeline.status_observer import StatusObserver
        from orcapod.pipeline.logging_observer import LoggingObserver
        from orcapod.pipeline.composite_observer import CompositeObserver

        _observer_registry = {
            "status": lambda cfg, reg: StatusObserver.from_config(cfg, reg),
            "logging": lambda cfg, reg: LoggingObserver.from_config(cfg, reg),
            "composite": lambda cfg, reg: CompositeObserver.from_config(cfg, reg),
        }
    return _observer_registry


def resolve_observer_from_config(config: dict, db_registry: dict | None = None) -> Any:
    """Reconstruct an observer from its config dict.

    Args:
        config: Observer config dict with ``"type"`` key.
        db_registry: Optional plain dict mapping database registry keys to
            configs (the output of ``DatabaseRegistry.to_dict()``).

    Returns:
        Reconstructed observer instance.

    Raises:
        ValueError: If observer type is unknown.
    """
    registry = _get_observer_registry()
    obs_type = config.get("type")
    if obs_type not in registry:
        raise ValueError(
            f"Unknown observer type: {obs_type!r}. "
            f"Known types: {sorted(registry.keys())}"
        )
    return registry[obs_type](config, db_registry)


def _ensure_registries() -> None:
    """Ensure registries are populated.

    The registries are built at module import time, so this is a no-op in
    normal use.  It exists as a hook for tests or code that calls
    ``register_*`` helpers before the first resolver call.
    """
    # Registries are already populated at module load; nothing to do.


# ---------------------------------------------------------------------------
# Resolver helpers
# ---------------------------------------------------------------------------


def resolve_database_from_config(
    config: dict[str, Any], db_registry: dict | None = None
) -> Any:
    """Reconstruct a database instance from a config dict.

    Args:
        config: Dict with at least a ``"type"`` key matching a registered
            database type, or ``"type": "scoped"`` for a scoped database
            reference.
        db_registry: Optional plain dict mapping ``"db_<key>"`` → config dict
            (the output of ``DatabaseRegistry.to_dict()``).  Required when
            ``config["type"] == "scoped"``; ignored otherwise.

    Returns:
        A new database instance constructed from the config.

    Raises:
        ValueError: If the ``"type"`` key is missing or unknown, or if
            ``config["type"] == "scoped"`` and no db_registry is provided.
    """
    _ensure_registries()
    db_type = config.get("type")
    if db_type == "scoped":
        if db_registry is None:
            raise ValueError("db_registry required to resolve scoped database config")
        ref = config["ref"]
        if ref not in db_registry:
            raise ValueError(
                f"Registry key {ref!r} not found; available keys: {sorted(db_registry.keys())}"
            )
        root_config = db_registry[ref]
        root_db = resolve_database_from_config(root_config)
        return root_db.at(*config["path"])
    if db_type not in DATABASE_REGISTRY:
        raise ValueError(
            f"Unknown database type. Known types: {sorted(DATABASE_REGISTRY.keys())}"
        )
    if db_type == "in_memory":
        logger.warning(
            "Loading pipeline with in-memory database. Cached data from the "
            "original run is not available — nodes will have UNAVAILABLE status."
        )
    cls = DATABASE_REGISTRY[db_type]
    return cls.from_config(config)


#: The kinds of connector a message may name: a saved kind outside this list is never
#: shown, since an odd file can hold a credential there.
_CONNECTOR_KINDS = ("sqlite", "postgresql", "spiraldb")


def saved_connector_kind(config: Any) -> str | None:
    """The kind of connector a saved database configuration is over, or ``None``.

    A connector is not rebuilt from a saved file: the user supplies it at load
    (developer, 2026-11-22, ITL-1391). Nothing of the saved text is returned: it may
    hold a credential.

    Args:
        config: A saved database configuration, or ``None``.

    Returns:
        ``"sqlite"``, ``"postgresql"`` or ``"spiraldb"``, ``"unknown connector kind"``
        for anything else the file says, or ``None`` for a database that is not over
        a connector.
    """
    from orcapod.databases.connector_arrow_database import CONNECTOR_DATABASE_TYPE

    if not isinstance(config, dict):
        return None
    saved = cast("dict[str, Any]", config)
    if saved.get("type") != CONNECTOR_DATABASE_TYPE:
        return None
    connector = saved.get("connector")
    if isinstance(connector, dict):
        kind = cast("dict[str, Any]", connector).get("connector_type")
        for known in _CONNECTOR_KINDS:
            if kind == known:
                return known
    return "unknown connector kind"


def resolve_operator_from_config(config: dict[str, Any]) -> Any:
    """Reconstruct an operator instance from a config dict.

    Args:
        config: Dict with at least a ``"class_name"`` key matching a registered
            operator class.

    Returns:
        A new operator instance constructed from the config.

    Raises:
        ValueError: If the ``"class_name"`` key is missing or unknown.
    """
    _ensure_registries()
    class_name = config.get("class_name")
    if class_name not in OPERATOR_REGISTRY:
        raise ValueError(
            f"Unknown operator: {class_name!r}. "
            f"Known operators: {sorted(OPERATOR_REGISTRY.keys())}"
        )
    cls = OPERATOR_REGISTRY[class_name]
    return cls.from_config(config)


def resolve_data_function_from_config(
    config: dict[str, Any],
    *,
    fallback_to_proxy: bool = False,
) -> Any:
    """Reconstruct a data function from a config dict.

    Args:
        config: Dict with at least a ``"data_function_type_id"`` key matching
            a registered data function type.
        fallback_to_proxy: If ``True`` and reconstruction fails (unknown type,
            ``ImportError``, ``AttributeError``, etc.), return a
            ``DataFunctionProxy`` instead of raising.

    Returns:
        A new data function instance constructed from the config, or a
        ``DataFunctionProxy`` if reconstruction fails and
        *fallback_to_proxy* is ``True``.

    Raises:
        ValueError: If the type ID is missing or unknown and
            *fallback_to_proxy* is ``False``.
    """
    _ensure_registries()
    type_id = config.get("data_function_type_id")
    if type_id not in DATA_FUNCTION_REGISTRY:
        if fallback_to_proxy:
            from orcapod.core.data_function_proxy import DataFunctionProxy

            return DataFunctionProxy.from_config(config)
        raise ValueError(
            f"Unknown data function type: {type_id!r}. "
            f"Known types: {sorted(DATA_FUNCTION_REGISTRY.keys())}"
        )
    cls = DATA_FUNCTION_REGISTRY[type_id]
    try:
        return cls.from_config(config)
    except (ImportError, ModuleNotFoundError, AttributeError) as exc:
        if fallback_to_proxy:
            logger.warning(
                "Could not reconstruct data function from config (%s); "
                "returning DataFunctionProxy.",
                exc,
            )
            from orcapod.core.data_function_proxy import DataFunctionProxy

            return DataFunctionProxy.from_config(config)
        raise


def resolve_source_from_config(
    config: dict[str, Any],
    *,
    fallback_to_proxy: bool = False,
    db_registry: DatabaseRegistryProtocol | None = None,
    node_descriptor: dict[str, Any] | None = None,
) -> Any:
    """Reconstruct a source instance from a config dict.

    Args:
        config: Dict with at least a ``"source_type"`` key matching a registered
            source type.
        fallback_to_proxy: If ``True`` and reconstruction fails, return a
            ``SourceProxy`` preserving identity hashes and schemas.
            Identity fields (``content_hash``, ``pipeline_hash``, and
            ``output_schema``) are taken from *node_descriptor* when provided,
            or from *config* when available (e.g. inner sources embedded inside
            a ``CachedSource`` always include identity via ``_identity_config()``).
        db_registry: Optional registry forwarded to ``from_config`` for all
            source types.  Sources that do not embed database references ignore
            it; ``CachedSource`` uses it to resolve its cache database key.
        node_descriptor: Optional node descriptor dict (new save format) containing
            identity fields used when creating a ``SourceProxy`` fallback.
            If omitted, identity is inferred from *config* where possible.

    Returns:
        A new source instance constructed from the config, or a ``SourceProxy``
        if reconstruction fails and *fallback_to_proxy* is ``True``.

    Raises:
        ValueError: If the source type is missing or unknown.
        Exception: Re-raised from ``from_config`` when *fallback_to_proxy* is
            ``False`` and reconstruction fails.
    """
    _ensure_registries()
    source_type = config.get("source_type")
    if source_type not in SOURCE_REGISTRY:
        if fallback_to_proxy:
            return _source_proxy_from_config(config, node_descriptor=node_descriptor)
        raise ValueError(
            f"Unknown source type: {source_type!r}. "
            f"Known types: {sorted(SOURCE_REGISTRY.keys())}"
        )
    cls = SOURCE_REGISTRY[source_type]
    try:
        # All sources accept db_registry — always forward it.
        # Sources that don't embed database references simply ignore it.
        return cls.from_config(config, db_registry=db_registry)
    except Warning:
        # A warning turned into an error (a ProvisionalHashWarning under an error
        # filter) propagates; it is not a source that could not be rebuilt (ITL-1331).
        raise
    except Exception:
        if fallback_to_proxy:
            logger.warning(
                "Could not reconstruct %s source; returning SourceProxy.",
                source_type,
            )
            return _source_proxy_from_config(config, node_descriptor=node_descriptor)
        raise


def saved_source_id(config: dict[str, Any]) -> str:
    """Rebuild the source id a saved source config names (ITL-1425).

    A config holds the kind (``source_type``), the implementation version
    (``source_version``) and the name (``source_name``) apart; the id is rebuilt from
    them by ``source_ids.build_source_id``, never parsed out of one string. A config
    that does not name its source (a plain derived source, whose default name is
    recomputed from its origin) gives the full id saved among its identity fields
    (``full_source_id``, written by ``RootSource._identity_config``), checked by the
    one parser; one without that either gives ``"unknown"``.

    Args:
        config: A saved source config.

    Returns:
        The source id, or ``"unknown"``.
    """
    from orcapod.core.sources.source_ids import build_source_id, parse_source_id

    kind = config.get("source_type")
    version = config.get("source_version")
    name = config.get("source_name")
    if kind is not None and version is not None and name is not None:
        return build_source_id(kind, version, name)
    full_id = config.get("full_source_id")
    if isinstance(full_id, str):
        _ = parse_source_id(full_id)
        return full_id
    return "unknown"


def _source_proxy_from_config(
    config: dict[str, Any],
    node_descriptor: dict[str, Any] | None = None,
) -> Any:
    """Create a SourceProxy from identity fields.

    Identity fields are read from ``node_descriptor`` when provided (new
    format for top-level pipeline nodes).  When ``node_descriptor`` is
    ``None``, identity is read directly from ``config`` — this supports
    inner sources (e.g. the wrapped source inside a ``CachedSource``) which
    always embed identity via ``_identity_config()``.

    Args:
        config: Source config dict.  Always consulted for ``source_type``
            and ``source_id``; also used for identity fields when
            ``node_descriptor`` is ``None``.
        node_descriptor: Node descriptor dict (new format) containing
            ``content_hash``, ``pipeline_hash``, and ``output_schema``.
            When provided, identity fields are taken from here rather than
            from ``config``.

    Returns:
        A ``SourceProxy`` preserving the original source's identity.

    Raises:
        ValueError: If required identity fields are not available from
            either ``node_descriptor`` or ``config``.
    """
    from orcapod.core.sources.source_proxy import SourceProxy
    from orcapod.types import Schema

    inner_config = config.get("inner_source")
    if (
        node_descriptor is None
        and not config.get("content_hash")
        and config.get("source_type") == "cached"
        and isinstance(inner_config, dict)
    ):
        # A cache has its wrapped source's identity (id, hashes, schemas), and its own
        # config holds none: the proxy stands in with the wrapped source's, so a cache
        # nested in another cache that cannot be rebuilt keeps its source id and the
        # outer cache still reads its rows (ITL-1425).
        return _source_proxy_from_config(cast("dict[str, Any]", inner_config))

    if node_descriptor is not None:
        # New format: top-level node descriptor carries identity
        content_hash = node_descriptor.get("content_hash")
        pipeline_hash_val = node_descriptor.get("pipeline_hash")
        output_schema = node_descriptor.get("output_schema", {})
        tag_schema_dict = output_schema.get("tag", {})
        data_schema_dict = output_schema.get("data", {})
        saved_nullable = output_schema.get("nullable")
    else:
        # Inner sources (e.g. inside CachedSource) embed identity via _identity_config()
        content_hash = config.get("content_hash")
        pipeline_hash_val = config.get("pipeline_hash")
        tag_schema_dict = config.get("tag_schema", {})
        data_schema_dict = config.get("data_schema", {})
        saved_nullable = config.get("nullable")

    if not content_hash or not pipeline_hash_val:
        raise ValueError(
            "Cannot create SourceProxy: missing identity fields. "
            "For top-level pipeline nodes, pass node_descriptor with "
            "content_hash, pipeline_hash, and output_schema. "
            "For inner sources, ensure the source config includes "
            "_identity_config() fields."
        )

    source_type = config.get("source_type")
    expected_class_name: str | None = None
    if source_type and source_type in SOURCE_REGISTRY:
        expected_class_name = SOURCE_REGISTRY[source_type].__name__

    # A config without the nullability record (written before ITL-1043) is lossy.
    tag_schema = Schema(
        deserialize_schema(
            tag_schema_dict,
            nullable=None if saved_nullable is None else saved_nullable["tag"],
        )
    )
    data_schema = Schema(
        deserialize_schema(
            data_schema_dict,
            nullable=None if saved_nullable is None else saved_nullable["data"],
        )
    )

    return SourceProxy(
        source_id=saved_source_id(config),
        content_hash_str=content_hash,
        pipeline_hash_str=pipeline_hash_val,
        tag_schema=tag_schema,
        data_schema=data_schema,
        expected_class_name=expected_class_name,
        source_config=config,
    )


# ---------------------------------------------------------------------------
# Registration helpers (extensibility)
# ---------------------------------------------------------------------------


def register_database(type_key: str, cls: type) -> None:
    """Register a custom database implementation for deserialization.

    Args:
        type_key: The string key to use in serialized configs.
        cls: The database class to register.
    """
    _ensure_registries()
    DATABASE_REGISTRY[type_key] = cls


def register_source(type_key: str, cls: type) -> None:
    """Register a custom source implementation for deserialization.

    Args:
        type_key: The string key to use in serialized configs.
        cls: The source class to register.
    """
    _ensure_registries()
    SOURCE_REGISTRY[type_key] = cls


def register_operator(class_name: str, cls: type) -> None:
    """Register a custom operator implementation for deserialization.

    Args:
        class_name: The class name string to use in serialized configs.
        cls: The operator class to register.
    """
    _ensure_registries()
    OPERATOR_REGISTRY[class_name] = cls


def register_data_function(type_id: str, cls: type) -> None:
    """Register a custom data function implementation for deserialization.

    Args:
        type_id: The type ID string to use in serialized configs.
        cls: The data function class to register.
    """
    _ensure_registries()
    DATA_FUNCTION_REGISTRY[type_id] = cls


# ---------------------------------------------------------------------------
# Schema serialization helpers
# ---------------------------------------------------------------------------


class SavedSchemaVersionError(ValueError):
    """A saved schema predates complete logical type identifiers."""


def _serialize_field_type(field: Any) -> str:
    """Render a field's type, with its logical identifier in either representation.

    A logical type is carried as a live extension type, or as its storage type with
    the ``ARROW:extension:name`` and ``ARROW:extension:metadata`` field metadata (the
    IPC form of a table read from a store). Both are written as the identifier,
    ``extension<name;hex-metadata>``, so the saved text never drops it (ITL-640).
    """
    import pyarrow as pa

    metadata = field.metadata or {}
    name = metadata.get(b"ARROW:extension:name")
    if name is not None and not isinstance(field.type, pa.ExtensionType):
        identifier = LogicalTypeIdentifier(
            name.decode("utf-8"), metadata.get(b"ARROW:extension:metadata", b"")
        )
        return f"extension<{identifier.text}>"
    return _serialize_arrow_type(field.type)


_PLAIN_FIELD_NAME = re.compile(r"[A-Za-z0-9_.\-]+")
"""A nested field name written as it is: ASCII letters, digits, ``_``, ``.`` and ``-``."""


def _nested_field_name_text(name: str) -> str:
    """The text of a nested field's name: as it is when plain, otherwise JSON-quoted.

    One rule (CodeRabbit on PR 454): a name made only of characters that cannot
    confuse the reader (``_PLAIN_FIELD_NAME``: ``item``, ``key``, ``value``,
    ``element``, ``x_1``, ``a.b``) keeps the text it always had; every other name (a
    character of the type grammar, whitespace anywhere, a newline, a quote, a
    backslash, a non-ASCII character, the empty name) is written as a JSON string,
    ``"a:b"``, ``"a\\nb"``, ``""``, which the reader takes back exactly. This is
    the grammar of format 0.4.0, the only one a load reads
    (``require_current_format``).
    """
    return name if _PLAIN_FIELD_NAME.fullmatch(name) else _json.dumps(name)


def _serialize_nested_field(field: Any) -> str:
    """Render a nested field exactly: ``<name>: <type>``, then `` not null`` if it is.

    The one text of a list, large list or fixed-size list item, a struct child and a
    map's key and item field, so each keeps its name, nullability and logical
    identifier through a save and load. The name is quoted when it must be
    (``_nested_field_name_text``).
    """
    nullable = "" if field.nullable else " not null"
    name = _nested_field_name_text(field.name)
    return f"{name}: {_serialize_field_type(field)}{nullable}"


def _serialize_arrow_type(arrow_type: Any) -> str:
    """Render a schema type, preserving complete logical identifiers at every depth."""
    import pyarrow as pa

    if isinstance(arrow_type, pa.ExtensionType):
        return f"extension<{arrow_type_identifier(arrow_type).text}>"
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        outer = "large_list" if pa.types.is_large_list(arrow_type) else "list"
        return f"{outer}<{_serialize_nested_field(arrow_type.value_field)}>"
    if pa.types.is_fixed_size_list(arrow_type):
        return (
            f"fixed_size_list<{_serialize_nested_field(arrow_type.value_field)}>"
            f"[{arrow_type.list_size}]"
        )
    if pa.types.is_struct(arrow_type):
        fields = [_serialize_nested_field(field) for field in arrow_type]
        return "struct<" + ", ".join(fields) + ">"
    if pa.types.is_dictionary(arrow_type):
        # Dictionary encoding is a layout, not part of a schema's identity (ITL-640):
        # the saved type is the value type.
        return _serialize_arrow_type(arrow_type.value_type)
    if pa.types.is_map(arrow_type):
        # The key and the item as whole fields (ITL-640, Codex review 3 of PR 454): a
        # bare ``map<K, V>`` would lose the item's name and its nullability, which are
        # part of the schema's identity.
        ordered = ", keys_sorted" if arrow_type.keys_sorted else ""
        return (
            f"map<{_serialize_nested_field(arrow_type.key_field)}, "
            f"{_serialize_nested_field(arrow_type.item_field)}{ordered}>"
        )
    return str(arrow_type)


def serialize_schema(schema: Any, type_converter: Any | None = None) -> dict[str, str]:
    """Convert a Schema mapping to JSON-serializable Arrow type strings.

    The result contains human-readable Arrow type strings for each field
    (e.g. ``"int64"``, ``"large_string"``, ``"list<item: int64>"``).
    Primitive/container strings follow Arrow's format. Logical types use
    ``extension<name;hex-metadata>`` with the canonical full identifier, including
    nested positions; the reader validates it against the registered type.

    Args:
        schema: A Schema-like mapping from field name to data type.
        type_converter: Optional type converter for Python→Arrow conversion.
            When provided, Python types (e.g. ``int``, ``str``) are converted
            to Arrow type strings (e.g. ``"int64"``, ``"large_string"``).
            When ``None``, values are stringified directly.

    Returns:
        A dict mapping field names to Arrow type string representations.
    """
    if type_converter is not None:
        result = {}
        for k, v in schema.items():
            try:
                arrow_type = type_converter.python_type_to_arrow_type(v)
                result[k] = _serialize_arrow_type(arrow_type)
            except Exception:
                result[k] = str(v)
        return result
    import pyarrow as pa

    return {
        k: _serialize_arrow_type(v) if isinstance(v, pa.DataType) else str(v)
        for k, v in schema.items()
    }


def nullable_column_names(schema: Any, type_converter: Any) -> list[str]:
    """Return the names of the columns of ``schema`` whose values may be null.

    A column may be null when its type is ``T | None``, by the rule
    ``type_converter.python_schema_to_arrow_schema`` uses to set a field's
    nullability. ``serialize_schema`` writes an Arrow type, which carries no field
    nullability, so a saved schema records these names beside its type strings
    (ITL-1043). A column whose type has no Arrow type is left out: its saved type text
    cannot be restored either.

    Args:
        schema: A Schema-like mapping from column name to Python type.
        type_converter: The type converter that writes the schema's type strings.

    Returns:
        The names, in the schema's column order.
    """
    names: list[str] = []
    for name, python_type in schema.items():
        try:
            arrow_schema = type_converter.python_schema_to_arrow_schema(
                {name: python_type}
            )
        except Exception:
            continue
        if arrow_schema.field(name).nullable:
            names.append(name)
    return names


def serialize_nullability(
    tag_schema: Any, data_schema: Any, type_converter: Any
) -> dict[str, list[str]]:
    """Return the saved record of which tag and data columns may be null.

    Written next to the type strings of a saved ``(tag, data)`` schema pair, as
    ``{"tag": [...], "data": [...]}``. An empty list records that no column of that
    group may be null; a schema saved without the record has unknown nullability.

    Args:
        tag_schema: The tag schema, a mapping from column name to Python type.
        data_schema: The data schema, a mapping from column name to Python type.
        type_converter: The type converter that writes the schemas' type strings.

    Returns:
        The nullable tag and data column names.
    """
    return {
        "tag": nullable_column_names(tag_schema, type_converter),
        "data": nullable_column_names(data_schema, type_converter),
    }


def _python_type_of_saved_column(
    name: str, arrow_type: Any, nullable: bool, type_converter: Any
) -> Any:
    """Return the Python type of a saved column: its Arrow type, ``T | None`` if nullable.

    The field is first brought to the declaration form (``declaration_form_field``:
    a nested logical type in the storage form, never a nested live extension type),
    then goes through ``arrow_schema_to_python_schema``, the converter's own rule for
    a field's nullability, so a restored type hashes as the running node's does.
    """
    import pyarrow as pa

    field = declaration_form_field(
        pa.field(name, arrow_type, nullable=nullable), type_converter
    )
    return type_converter.arrow_schema_to_python_schema(pa.schema([field]))[name]


def _check_nullable_names(columns: Any, nullable: Any) -> None:
    """Raise ``ValueError`` if a saved nullable column name is not a saved column."""
    unknown = sorted(set(nullable) - set(columns))
    if unknown:
        raise ValueError(
            f"The saved nullable columns {unknown} are not among the saved columns "
            f"{list(columns)}."
        )


def deserialize_schema(
    schema_dict: dict[str, str],
    type_converter: Any | None = None,
    nullable: Collection[str] | None = None,
) -> dict[str, Any]:
    """Reconstruct a Python-type schema from Arrow type string values.

    Parses Arrow type strings (e.g. ``"int64"``, ``"list<item: int64>"``)
    back into ``pa.DataType`` objects, then converts them to Python types
    via the type converter.  Falls back to raw strings for fields that
    cannot be parsed.

    Args:
        schema_dict: Dict mapping field names to Arrow type strings, as
            produced by `serialize_schema`.
        type_converter: Optional type converter for Arrow→Python conversion.
            When ``None``, the default data context's converter is used.
        nullable: The columns saved as nullable (one group of
            ``serialize_nullability``); each becomes ``T | None``. ``None`` when the
            file records no nullability: every column is then restored as ``T``.

    Returns:
        A dict mapping field names to Python types (or raw strings if
        parsing fails).

    Raises:
        ValueError: If ``nullable`` names a column that is not in ``schema_dict``.
    """
    if type_converter is None:
        from orcapod.contexts import resolve_context

        type_converter = resolve_context(None).type_converter
    if nullable is not None:
        _check_nullable_names(schema_dict, nullable)

    result: dict[str, Any] = {}
    for name, type_str in schema_dict.items():
        try:
            arrow_type = parse_arrow_type_string(type_str, type_converter)
            result[name] = _python_type_of_saved_column(
                name,
                arrow_type,
                nullable is not None and name in nullable,
                type_converter,
            )
        except LogicalTypeIdentifierError as exc:
            raise exc.for_column(name) from exc
        except (SavedSchemaVersionError, Warning):
            # A warning turned into an error (a provisional library value type under an
            # error filter) propagates (ITL-1331).
            raise
        except Exception:
            result[name] = type_str
    return result


def arrow_type_texts(
    schema: "pa.Schema", type_converter: Any | None = None
) -> dict[str, str]:
    """Return the saved type strings of an Arrow schema, its own types (ITL-640).

    The text of each field's own Arrow type (``_serialize_field_type``: nested
    nullability, units, zones, widths and logical type identifiers), never a type
    re-derived from a Python type. The schema is first brought to the storage form
    (``storage_form_schema``, the walker ``hash_schema`` applies), so a logical type
    is written as its identifier, ``extension<name;hex-metadata>``, whether it came
    live, in metadata form or nested in a list, struct or map. Top-level nullability
    is saved apart (``arrow_nullable_names``).

    Args:
        schema: The Arrow schema.
        type_converter: The converter of the data context the text is read back
            with (it resolves logical type identifiers); ``None`` for the default
            data context's.

    Returns:
        A dict mapping field names to Arrow type strings, in the schema's order.

    Raises:
        ValueError: If a field's text would not read back as its type
            (``_require_text_reads_back``).
    """
    texts: dict[str, str] = {}
    for field in storage_form_schema(schema):
        text = _serialize_field_type(field)
        _require_text_reads_back(field.name, text, type_converter)
        texts[field.name] = text
    return texts


def _require_text_reads_back(name: str, text: str, type_converter: Any | None) -> None:
    """Refuse a saved type text that does not read back as the type it was written for.

    The one check of the writer of saved Arrow types (``arrow_type_texts``; CodeRabbit
    on PR 454): the text is read (``parse_arrow_type_string``) and written again, and
    must come back the same, so no type can be saved into a file that cannot be loaded
    or loads as another type. Checked before anything is written: ``Pipeline.save``
    and ``PipelineJob.save`` raise, and the caller names the node.

    Args:
        name: The column the text is for.
        text: Its saved type text.
        type_converter: The converter the text is read back with (``None``: the
            default data context's).

    Raises:
        ValueError: If the text cannot be read, or reads back as another type.
    """
    try:
        back = _serialize_arrow_type(parse_arrow_type_string(text, type_converter))
    except Warning:
        # A warning turned into an error (a provisional library value type under an
        # error filter) is not the text's fault (ITL-1331).
        raise
    except Exception as exc:
        raise ValueError(
            f"column {name!r}: its saved type text {text!r} cannot be read back "
            f"({exc}), so a file holding it could not be loaded; it is not saved"
        ) from exc
    if back != text:
        raise ValueError(
            f"column {name!r}: its saved type text {text!r} reads back as {back!r}, "
            "another type; it is not saved"
        )


def arrow_nullable_names(schema: "pa.Schema") -> list[str]:
    """Return the names of the nullable top-level fields of ``schema``, in order."""
    return [field.name for field in schema if field.nullable]


def serialize_arrow_schemas(
    tag_schema: "pa.Schema", data_schema: "pa.Schema", type_converter: Any | None = None
) -> dict[str, Any]:
    """Return the saved entry of a tag and data Arrow schema pair (ITL-640).

    The one writer of a saved schema pair, for a slot's ``source_config`` and for
    every node's ``output_schema``: ``{"tag": {...}, "data": {...}, "nullable":
    {"tag": [...], "data": [...]}}``, the type text of each field's own Arrow type
    (``arrow_type_texts``: exact widths, units, zones, precision, nested
    nullability, logical type identifiers) and the nullable top-level fields. The
    text is read back by the readers of the Python form (``deserialize_output_schema``)
    and of the Arrow form (``arrow_schema_from_texts``) alike.

    Args:
        tag_schema: The Arrow schema of the tag columns.
        data_schema: The Arrow schema of the data columns (data-tags included).
        type_converter: The converter of the node's data context, which the texts
            are read back with (``arrow_type_texts``); ``None`` for the default's.

    Returns:
        The entry, ready for JSON.

    Raises:
        ValueError: If a field's type text would not read back as its type; nothing
            is written (the caller names the node).
    """
    return {
        "tag": arrow_type_texts(tag_schema, type_converter),
        "data": arrow_type_texts(data_schema, type_converter),
        "nullable": {
            "tag": arrow_nullable_names(tag_schema),
            "data": arrow_nullable_names(data_schema),
        },
    }


def arrow_schema_from_texts(
    schema_dict: dict[str, str],
    type_converter: Any,
    nullable: Collection[str] | None,
) -> "pa.Schema":
    """Rebuild the Arrow schema saved by ``arrow_type_texts`` (ITL-640).

    The rebuilt schema is in the declaration form (``declaration_form_schema``): a
    logical type at the top of a column is the live type, one inside a list, struct
    or map is its storage type with its identifier as field metadata, as a schema
    declared in Arrow is.

    Args:
        schema_dict: The saved type strings.
        type_converter: The saved node's converter, for logical type identifiers.
        nullable: The saved nullable column names; ``None`` when the file records no
            nullability, and every column is then non-nullable.

    Returns:
        The Arrow schema.

    Raises:
        ValueError: If a type string cannot be parsed (a class this process has not
            registered yet), or ``nullable`` names a column not in ``schema_dict``.
    """
    import pyarrow as pa

    if nullable is not None:
        _check_nullable_names(schema_dict, nullable)
    fields = []
    for name, type_str in schema_dict.items():
        try:
            arrow_type = parse_arrow_type_string(type_str, type_converter)
        except LogicalTypeIdentifierError as exc:
            raise exc.for_column(name) from exc
        fields.append(
            pa.field(
                name, arrow_type, nullable=nullable is not None and name in nullable
            )
        )
    return declaration_form_schema(pa.schema(fields), type_converter)


def serialize_output_schema(
    tag_schema: Any, data_schema: Any, type_converter: Any
) -> dict[str, Any]:
    """Return a node's saved ``output_schema`` entry.

    ``{"tag": {...}, "data": {...}, "nullable": {"tag": [...], "data": [...]}}``: the
    type strings of `serialize_schema` and the record of `serialize_nullability`.

    Args:
        tag_schema: The node's tag schema.
        data_schema: The node's data schema.
        type_converter: The node's type converter.

    Returns:
        The entry, ready for JSON.
    """
    return {
        "tag": serialize_schema(tag_schema, type_converter),
        "data": serialize_schema(data_schema, type_converter),
        "nullable": serialize_nullability(tag_schema, data_schema, type_converter),
    }


def deserialize_output_schema(
    output_schema: dict[str, Any],
    type_converter: Any,
) -> tuple[Schema, Schema]:
    """Reconstruct a node's ``(tag, data)`` output schema from its saved descriptor.

    Args:
        output_schema: The descriptor's ``output_schema`` entry, as written by
            `serialize_output_schema`. An entry without ``"nullable"`` (a file written
            before it was recorded) restores every column as ``T``: lossy.
        type_converter: The node's type converter, used to turn each Arrow type back
            into the Python type that ``output_schema()`` reports.

    Returns:
        The tag schema and the data schema, with Python types.

    Raises:
        ValueError: If a column's saved type cannot be restored to a Python type,
            such as an unregistered logical type, or a saved nullable column name is
            not a saved column. The message names the column and its saved type string.
    """
    saved_nullable: dict[str, list[str]] | None = output_schema.get("nullable")

    def restore(columns: dict[str, str], nullable: list[str] | None) -> Schema:
        if nullable is not None:
            _check_nullable_names(columns, nullable)
        restored: dict[str, Any] = {}
        for name, type_str in columns.items():
            try:
                arrow_type = parse_arrow_type_string(type_str, type_converter)
                restored[name] = _python_type_of_saved_column(
                    name,
                    arrow_type,
                    nullable is not None and name in nullable,
                    type_converter,
                )
            except LogicalTypeIdentifierError as exc:
                raise exc.for_column(name) from exc
            except SavedSchemaVersionError as exc:
                raise SavedSchemaVersionError(f"Column {name!r}: {exc}") from exc
            except Warning:
                # A warning turned into an error propagates as itself (ITL-1331).
                raise
            except Exception as exc:
                raise ValueError(
                    f"Column {name!r} has the saved type {type_str!r}, which cannot "
                    "be restored to a Python type."
                ) from exc
        return Schema(restored)

    return (
        restore(
            output_schema.get("tag", {}),
            None if saved_nullable is None else saved_nullable["tag"],
        ),
        restore(
            output_schema.get("data", {}),
            None if saved_nullable is None else saved_nullable["data"],
        ),
    )


def serialize_arrow_schema(
    schema: pa.Schema, type_converter: Any | None = None
) -> dict[str, Any]:
    """Return the saved form of an Arrow schema, exactly as given.

    ``{"columns": {...}, "nullable": [...]}``: each field's Arrow type text, in field
    order (the text of `serialize_schema`, which carries nested field names and their
    nullability), and the names of the nullable top-level fields, which the type text
    cannot say (the record of `serialize_nullability`). The types are kept as given,
    not normalized to their large variants. `deserialize_arrow_schema` reads it back.

    Args:
        schema: The Arrow schema to save.
        type_converter: The converter that restores a logical type, used to check the
            saved form. If omitted, the default data context's converter.

    Returns:
        The entry, ready for JSON.

    Raises:
        ValueError: If the saved form would not restore ``schema`` exactly (equal
            with its metadata compared): a repeated field name, field or schema
            metadata, or a type the type text cannot restore. The message names the
            field.
    """
    names = [field.name for field in schema]
    repeated = sorted({name for name in names if names.count(name) > 1})
    if repeated:
        raise ValueError(
            f"The schema repeats the field names {repeated}; it cannot be saved."
        )
    entry: dict[str, Any] = {
        "columns": {field.name: _serialize_arrow_type(field.type) for field in schema},
        "nullable": [field.name for field in schema if field.nullable],
    }
    try:
        restored = deserialize_arrow_schema(entry, type_converter)
    except ValueError as exc:
        raise ValueError(f"The schema cannot be saved: {exc}") from exc
    for field, restored_field in zip(schema, restored):
        if not restored_field.equals(field, check_metadata=True):
            raise ValueError(
                f"Field {field.name!r} ({field.type}, nullable={field.nullable}"
                f"{', with metadata' if field.metadata else ''}) cannot be saved "
                f"exactly: it reads back as {restored_field.type}, "
                f"nullable={restored_field.nullable}. Field metadata and types the "
                "saved Arrow type text cannot restore are not saved."
            )
    if not restored.equals(schema, check_metadata=True):
        raise ValueError(
            "The schema's own metadata cannot be saved: build the schema without it."
        )
    return entry


_SAVED_ARROW_SCHEMA_KEYS = ("columns", "nullable")


def _saved_arrow_schema_parts(entry: object) -> tuple[dict[str, str], list[str]]:
    """Return the columns and nullable names of a saved schema entry, checked.

    Only the shape `serialize_arrow_schema` writes is read; anything else is refused,
    never interpreted (a ``null`` entry, a string where a list belongs).

    Raises:
        ValueError: Naming what is wrong with the entry.
    """
    if not isinstance(entry, dict):
        raise ValueError(
            "the saved schema must be an object with 'columns' and 'nullable', "
            f"not {type(entry).__name__} {entry!r}"
        )
    fields = cast("dict[str, object]", entry)
    missing = [key for key in _SAVED_ARROW_SCHEMA_KEYS if key not in fields]
    unknown = sorted(str(key) for key in fields if key not in _SAVED_ARROW_SCHEMA_KEYS)
    if missing or unknown:
        raise ValueError(
            "the saved schema holds exactly 'columns' and 'nullable'"
            + (f"; missing {missing}" if missing else "")
            + (f"; unknown keys {unknown}" if unknown else "")
        )
    columns = fields["columns"]
    if not isinstance(columns, dict) or not columns:
        raise ValueError(
            "the saved schema's 'columns' must be a non-empty object of column names "
            f"to type text, not {columns!r}"
        )
    for name, type_text in cast("dict[object, object]", columns).items():
        if not isinstance(name, str) or not isinstance(type_text, str):
            raise ValueError(
                f"the saved schema's column {name!r} must name its type as text, not "
                f"{type_text!r}"
            )
    nullable = fields["nullable"]
    if not isinstance(nullable, list) or not all(
        isinstance(name, str) for name in cast("list[object]", nullable)
    ):
        raise ValueError(
            "the saved schema's 'nullable' must be a list of column names, not "
            f"{nullable!r}"
        )
    names = cast("list[str]", nullable)
    repeated = sorted({name for name in names if names.count(name) > 1})
    if repeated:
        raise ValueError(f"the saved schema's 'nullable' repeats {repeated}")
    _check_nullable_names(columns, names)
    return cast("dict[str, str]", columns), names


def deserialize_arrow_schema(
    entry: object, type_converter: Any | None = None
) -> pa.Schema:
    """Restore an Arrow schema saved by `serialize_arrow_schema`.

    Args:
        entry: The saved ``{"columns": {...}, "nullable": [...]}`` entry.
        type_converter: The converter that restores a logical type. If omitted, the
            default data context's converter.

    Returns:
        The schema, with its fields in the saved order.

    Raises:
        ValueError: If the entry is not of the saved shape (an object with exactly
            ``columns``, a non-empty object of column names to type text, and
            ``nullable``, a list of distinct column names), or a saved type cannot
            be parsed. The message says what is wrong, naming the column.
    """
    import pyarrow as pa

    columns, nullable = _saved_arrow_schema_parts(entry)
    fields = []
    for name, type_str in columns.items():
        try:
            arrow_type = parse_arrow_type_string(type_str, type_converter)
        except LogicalTypeIdentifierError as exc:
            raise exc.for_column(name) from exc
        except SavedSchemaVersionError as exc:
            raise SavedSchemaVersionError(f"Column {name!r}: {exc}") from exc
        except ValueError as exc:
            raise ValueError(
                f"Column {name!r} has the saved type {type_str!r}, which cannot be "
                "restored."
            ) from exc
        fields.append(pa.field(name, arrow_type, nullable=name in nullable))
    return pa.schema(fields)


def parse_arrow_type_string(type_str: str, type_converter: Any | None = None) -> Any:
    """Parse an Arrow type string into a ``pa.DataType``.

    Handles both primitive types (``"int64"``, ``"large_string"``) and
    nested types (``"list<item: int64>"``, ``"struct<a: int64, b: string>"``,
    ``"map<string, int64>"``).

    The grammar follows Arrow's canonical ``str(pa.DataType)`` output.

    Args:
        type_str: Arrow type string to parse.
        type_converter: The saved node's converter for resolving logical identifiers.
            If omitted, logical types use the default context's converter.

    Returns:
        The corresponding ``pa.DataType``.

    Raises:
        ValueError: If the type string cannot be parsed.
    """
    import pyarrow as pa

    type_str = type_str.strip()

    if type_str.startswith("fixed_size_list<"):
        import re

        match = re.fullmatch(r"fixed_size_list<(.*)>\[(\d+)\]", type_str)
        if match is None:
            raise ValueError(f"Malformed fixed-size list: {type_str!r}")
        return pa.list_(
            _parse_arrow_field(match.group(1), type_converter), int(match.group(2))
        )

    if type_str.startswith("extension<"):
        if not type_str.endswith(">"):
            raise LogicalTypeIdentifierError(LogicalTypeIdentifier(type_str, b""), ())
        identifier = type_str[len("extension<") : -1]
        name, separator, metadata_text = identifier.partition(";")
        if not name:
            raise LogicalTypeIdentifierError(
                LogicalTypeIdentifier(name, metadata_text.encode()), ()
            )
        if not separator:
            raise SavedSchemaVersionError(
                "This file was saved by an earlier version without a full logical "
                "type identifier; read it with that version and save it again with "
                "the original pipeline under the current version."
            )
        try:
            metadata = bytes.fromhex(metadata_text)
        except ValueError as exc:
            error = LogicalTypeIdentifierError(
                LogicalTypeIdentifier(name, metadata_text.encode()), ()
            )
            error.add_note("Saved logical type metadata is not hexadecimal.")
            raise error from exc
        if type_converter is None:
            from orcapod.contexts import resolve_context

            type_converter = resolve_context(None).type_converter
        logical_type = type_converter.get_logical_type_by_arrow_extension_name(name)
        if logical_type is None:
            # A native container (list[T], set[T]) is fully described by its
            # identifier: rebuild it, as a read of stored data does (ITL-611).
            _register_native_container(name, metadata, type_converter)
            logical_type = type_converter.get_logical_type_by_arrow_extension_name(name)
        if logical_type is None:
            raise ValueError(f"Unregistered extension type: {name!r}")
        require_readable(logical_type, metadata)
        return logical_type.get_arrow_extension_type()

    # Primitive types — try direct lookup via pa.<name>()
    if "<" not in type_str:
        factory = _get_arrow_primitive_types().get(type_str)
        if factory is not None:
            return factory()
        parameterized = _parse_parameterized_type(type_str)
        if parameterized is not None:
            return parameterized
        raise ValueError(f"Unknown Arrow type: {type_str!r}")

    # Nested types — parse the outer type and recurse
    _require_balanced_delimiters(type_str)
    bracket_pos = type_str.index("<")
    outer = type_str[:bracket_pos].strip()
    inner = type_str[bracket_pos + 1 : -1].strip()  # strip < and >

    if outer in ("list", "large_list"):
        # "list<item: int64>" or "list<item: struct<...>>"
        child_type = _parse_arrow_field(inner, type_converter)
        return (
            pa.large_list(child_type) if outer == "large_list" else pa.list_(child_type)
        )

    if outer == "struct":
        # "struct<a: int64, b: string>" — split on top-level commas. "struct<>" is the
        # struct with no fields (CodeRabbit on PR 454), never one field of no text.
        if not inner.strip():
            return pa.struct([])
        fields = _split_struct_fields(inner)
        pa_fields = [
            _parse_arrow_field(field_str, type_converter) for field_str in fields
        ]
        return pa.struct(pa_fields)

    if outer == "map":
        # "map<key: large_string not null, value: int64 not null, keys_sorted>": the
        # key and item fields whole, as Orcapod writes a map. Arrow's own text of a
        # map, the bare "map<large_string, int64>" (``str(type)``), reads as Arrow's
        # default fields (a non-null "key", a nullable "value").
        parts = _split_top_level(inner, ",")
        keys_sorted = len(parts) == 3 and parts[-1].strip() == "keys_sorted"
        if keys_sorted:
            parts = parts[:-1]
        if len(parts) != 2:
            raise ValueError(f"Cannot parse map type: {type_str!r}")
        key_text, item_text = (part.strip() for part in parts)
        named = [_names_a_field(text) for text in (key_text, item_text)]
        if named == [True, True]:
            key = _parse_arrow_field(key_text, type_converter)
            item = _parse_arrow_field(item_text, type_converter)
            return pa.map_(key, item, keys_sorted)  # pyright: ignore[reportCallIssue, reportArgumentType]  # pyarrow takes fields for both; the stubs only types
        if named != [False, False]:
            raise ValueError(f"Cannot parse map type: {type_str!r}")
        key_type = parse_arrow_type_string(key_text, type_converter)
        value_type = parse_arrow_type_string(item_text, type_converter)
        if keys_sorted:
            return pa.map_(key_type, value_type, True)
        return pa.map_(key_type, value_type, False)

    raise ValueError(f"Unknown nested Arrow type: {type_str!r}")


def _require_balanced_delimiters(type_str: str) -> None:
    """Raise unless a nested type's ``<`` and ``>`` balance and its first ``<`` closes last.

    ``list<item: int64X`` or ``list<item: int64>x`` is not a type: the parser must not
    drop or ignore a character to read one.

    Raises:
        ValueError: If the delimiters do not balance, or text follows the closing ``>``.
    """
    depth = 0
    for position, char in _unquoted(type_str):
        if char == "<":
            depth += 1
        elif char == ">":
            depth -= 1
            if depth < 0 or (depth == 0 and position != len(type_str) - 1):
                break
    else:
        if depth == 0 and type_str.endswith(">"):
            return
    raise ValueError(f"Unbalanced '<' and '>' in the Arrow type text {type_str!r}")


def _native_container_storage(
    name: str, metadata: bytes, type_converter: Any
) -> Any | None:
    """Return the storage type a native container's identifier describes.

    A native container's element is a plain Arrow type (a set of a primitive,
    ``"element_kind": "arrow"``: the element type is the text inside the extension
    name, ``set[<element>]``) or a logical type its metadata names
    (``element_ext_name`` and ``element_ext_metadata``). The storage is a
    ``large_list`` of the element's storage. A logical element must be registered
    in this process, or be a native container itself.

    Args:
        name: The container's extension name.
        metadata: The container's extension metadata.
        type_converter: The converter that resolves a logical element.

    Returns:
        The storage type, or ``None`` if ``metadata`` is not a native container's or
        names a logical element that is not registered.
    """
    import pyarrow as pa

    from orcapod.logical_types.list_logical_type_factory import (
        ARROW_ELEMENT_KIND,
        ELEMENT_KIND_KEY,
        LIST_CATEGORY,
        SET_CATEGORY,
    )

    try:
        description: object = _json.loads(metadata.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None
    if not isinstance(description, dict):
        return None
    fields = cast("dict[str, object]", description)
    if fields.get("category") not in (LIST_CATEGORY, SET_CATEGORY):
        return None
    if fields.get(ELEMENT_KIND_KEY) == ARROW_ELEMENT_KIND:
        prefix = f"{SET_CATEGORY}["
        if not (name.startswith(prefix) and name.endswith("]")):
            return None
        element_text = name[len(prefix) : -1]
        return pa.large_list(parse_arrow_type_string(element_text, type_converter))
    element_name = fields.get("element_ext_name")
    if not isinstance(element_name, str):
        return None
    element_metadata_text = fields.get("element_ext_metadata")
    element_metadata = (
        element_metadata_text.encode("utf-8")
        if isinstance(element_metadata_text, str)
        else b""
    )
    element = type_converter.get_logical_type_by_arrow_extension_name(element_name)
    if element is not None:
        return pa.large_list(element.get_arrow_extension_type().storage_type)
    element_storage = _native_container_storage(
        element_name, element_metadata, type_converter
    )
    if element_storage is None:
        return None
    return pa.large_list(element_storage)


def _register_native_container(name: str, metadata: bytes, type_converter: Any) -> None:
    """Register the native container a saved identifier names, if it is one.

    Saved schemas hold an extension type as its identifier only. A native container
    (``list[T]`` or ``set[T]``) is fully described by it, so it is rebuilt through
    the registered factory, as reading stored data rebuilds it; any other logical
    type must already be registered in this process.

    Args:
        name: The extension name.
        metadata: The extension metadata.
        type_converter: The converter to register the container with.
    """
    storage = _native_container_storage(name, metadata, type_converter)
    if storage is not None:
        type_converter.register_logical_type_from_arrow_metadata(
            name, metadata, storage
        )


def _parse_arrow_field(text: str, type_converter: Any | None) -> Any:
    """Restore a nested field's type and nullability from schema text."""
    import pyarrow as pa

    text = text.strip()
    if text.startswith('"'):
        # A quoted name (``_nested_field_name_text``): exactly the JSON string.
        name, end = _json.JSONDecoder().raw_decode(text)
        rest = text[end:].lstrip()
        if not isinstance(name, str) or not rest.startswith(":"):
            raise ValueError(f"Malformed nested field: {text!r}")
        type_text = rest[1:]
    else:
        name, separator, type_text = text.partition(":")
        if not separator:
            name, type_text = "item", text
        name = name.strip()
    type_text = type_text.strip()
    nullable = not type_text.endswith(" not null")
    if not nullable:
        type_text = type_text[: -len(" not null")]
    return pa.field(
        name,
        parse_arrow_type_string(type_text, type_converter),
        nullable=nullable,
    )


def _names_a_field(text: str) -> bool:
    """Return True if ``text`` is ``<name>: <type>``, not a bare type.

    A bare type holds a ``:`` only inside its brackets (``list<item: int64>``), so
    the text names a field exactly when a ``:`` comes before any bracket.
    """
    if text.lstrip().startswith('"'):
        return True
    colon = text.find(":")
    if colon == -1:
        return False
    brackets = [position for position in map(text.find, "<[(") if position != -1]
    return not brackets or colon < min(brackets)


def _parse_parameterized_type(type_str: str) -> Any | None:
    """Parse a parameterized non-nested Arrow type string, or return ``None``.

    Handles the canonical ``str()`` forms of ``timestamp[us]``,
    ``timestamp[us, tz=UTC]``, ``duration[us]``, ``time32[ms]``,
    ``time64[us]``, ``date64[ms]``, ``decimal32(5, 2)``, ``decimal64(12, 3)``,
    ``decimal128(10, 2)``, ``decimal256(40, 2)`` and ``fixed_size_binary[3]``.

    Args:
        type_str: Arrow type string, stripped of surrounding whitespace.

    Returns:
        The corresponding ``pa.DataType``, or ``None`` if *type_str* is not one
        of these forms.
    """
    import re
    from typing import Literal, cast

    import pyarrow as pa

    # Each regex admits only the units Arrow accepts, so the casts below hold.
    if match := re.fullmatch(r"timestamp\[(s|ms|us|ns)(?:, tz=(.+))?\]", type_str):
        unit = cast(Literal["s", "ms", "us", "ns"], match.group(1))
        return pa.timestamp(unit, tz=match.group(2))
    if match := re.fullmatch(r"duration\[(s|ms|us|ns)\]", type_str):
        return pa.duration(cast(Literal["s", "ms", "us", "ns"], match.group(1)))
    if match := re.fullmatch(r"time32\[(s|ms)\]", type_str):
        return pa.time32(cast(Literal["s", "ms"], match.group(1)))
    if match := re.fullmatch(r"time64\[(us|ns)\]", type_str):
        return pa.time64(cast(Literal["us", "ns"], match.group(1)))
    if type_str == "date64[ms]":
        return pa.date64()
    if match := re.fullmatch(r"decimal(32|64|128|256)\((\d+), (-?\d+)\)", type_str):
        # Every width Arrow has (``decimal32`` and ``decimal64`` since pyarrow 18;
        # Orcapod requires 22): a saved decimal reads back at its own width.
        factory = {
            "32": pa.decimal32,
            "64": pa.decimal64,
            "128": pa.decimal128,
            "256": pa.decimal256,
        }[match.group(1)]
        return factory(int(match.group(2)), int(match.group(3)))
    if match := re.fullmatch(r"fixed_size_binary\[(\d+)\]", type_str):
        return pa.binary(int(match.group(1)))
    return None


def _strip_field_name(s: str) -> str:
    """Strip the ``"item: "`` or ``"field_name: "`` prefix from a child type string."""
    if ":" in s:
        # Only strip if there's no nested '<' before the ':'
        colon_pos = s.index(":")
        bracket_pos = s.find("<")
        if bracket_pos == -1 or colon_pos < bracket_pos:
            return s[colon_pos + 1 :].strip()
    return s


def _quoted_positions(text: str) -> "list[tuple[int, str]]":
    """The positions and characters of ``text`` inside a JSON-quoted field name."""
    inside: list[tuple[int, str]] = []
    position = 0
    while position < len(text):
        if text[position] == '"':
            try:
                _, end = _json.JSONDecoder().raw_decode(text, position)
            except ValueError:
                end = len(text)
            inside.extend((p, text[p]) for p in range(position, end))
            position = end
        else:
            position += 1
    return inside


def _unquoted(text: str) -> "list[tuple[int, str]]":
    """The positions and characters of ``text`` outside JSON-quoted field names."""
    quoted = {position for position, _ in _quoted_positions(text)}
    return [(p, c) for p, c in enumerate(text) if p not in quoted]


def _split_top_level(s: str, sep: str, max_splits: int = -1) -> list[str]:
    """Split *s* on *sep*, but only at the top level (not inside ``<>``, ``[]`` or ``()``).

    Brackets and parentheses count because parameterized types carry commas,
    e.g. ``timestamp[us, tz=UTC]`` and ``decimal128(10, 2)``.

    Args:
        s: String to split.
        sep: Separator character.
        max_splits: Maximum number of splits (-1 for unlimited).

    Returns:
        List of substrings.
    """
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    quoted = {position for position, _ in _quoted_positions(s)}
    for position, ch in enumerate(s):
        if position in quoted:
            current.append(ch)
            continue
        if ch in "<[(":
            depth += 1
        elif ch in ">])":
            depth -= 1
        if ch == sep and depth == 0 and (max_splits == -1 or len(parts) < max_splits):
            parts.append("".join(current))
            current = []
            continue
        current.append(ch)
    parts.append("".join(current))
    return parts


def _split_struct_fields(inner: str) -> list[str]:
    """Split struct fields on top-level commas."""
    return _split_top_level(inner, ",")


def _build_arrow_primitive_types() -> dict[str, Any]:
    """Build a mapping of Arrow type string names to factory callables."""
    import pyarrow as pa

    types = {}
    for name in [
        "null",
        "bool_",
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float16",
        "float32",
        "float64",
        "string",
        "utf8",
        "large_string",
        "large_utf8",
        "binary",
        "large_binary",
        "date32",
        "date64",
        "time32",
        "time64",
        "duration",
        "timestamp",
    ]:
        factory = getattr(pa, name, None)
        if factory is not None:
            types[name] = factory
            # Also map the str() output if it differs from the factory name
            try:
                canonical = str(factory())
                if canonical != name:
                    types[canonical] = factory
            except Exception:
                pass
    # Common aliases
    types["double"] = pa.float64
    types["float"] = pa.float32
    types["bool"] = pa.bool_
    return types


# Initialized to None and built on first use by _get_arrow_primitive_types() so
# that importing this module does not trigger an eager pyarrow import.  pyarrow
# is only needed when actually deserializing an Arrow type string, not at module
# load time.
_ARROW_PRIMITIVE_TYPES: dict[str, Any] | None = None


def _get_arrow_primitive_types() -> dict[str, Any]:
    """Return the Arrow primitive types map, building it on first call."""
    global _ARROW_PRIMITIVE_TYPES
    if _ARROW_PRIMITIVE_TYPES is None:
        _ARROW_PRIMITIVE_TYPES = _build_arrow_primitive_types()
    return _ARROW_PRIMITIVE_TYPES


# ---------------------------------------------------------------------------
# Builtin Python type map for schema deserialization from to_config() format
# ---------------------------------------------------------------------------

_BUILTIN_TYPE_MAP: dict[str, type] = {
    "<class 'int'>": int,
    "<class 'float'>": float,
    "<class 'str'>": str,
    "<class 'bool'>": bool,
    "<class 'bytes'>": bytes,
}

# ---------------------------------------------------------------------------
# PipelineJob format version
# ---------------------------------------------------------------------------

# Same scheme as ``PIPELINE_FORMAT_VERSION``: the written version, the only one a
# load reads (``PipelineJob.load``, ``require_current_format``).
PIPELINE_JOB_FORMAT_VERSION = "0.4.0"


# ---------------------------------------------------------------------------
# Data-tags in saved files (ITL-1019)
# ---------------------------------------------------------------------------

FALL_THROUGH_TAGS_KEY = "fall_through_tags"
"""The function node descriptor key naming the data-tags its pod lets fall
through (a list; empty for a plain pod)."""


def require_nullability_records(descriptor: dict[str, Any]) -> None:
    """Refuse a saved node descriptor that does not record which columns may be null.

    A saved file records it for every node, in its ``output_schema`` and, for a source
    slot, in its ``source_config`` (ITL-1043): the Arrow type text cannot say it. A
    descriptor without the record is malformed and is refused, never loaded with every
    column read as non-null (developer, 2026-11-21: no backward compatibility for
    saved files).

    Args:
        descriptor: A node descriptor of a saved pipeline.

    Raises:
        ValueError: If a record is missing, naming it.
    """
    missing = []
    if "nullable" not in (descriptor.get("output_schema") or {}):
        missing.append("output_schema.nullable")
    if "source_config" in descriptor and "nullable" not in descriptor["source_config"]:
        missing.append("source_config.nullable")
    if missing:
        raise ValueError(
            f"the file has no {' or '.join(missing)} record; a saved file records "
            "which columns may be null for every node. Save the pipeline again with "
            "this version of Orcapod."
        )


def saved_fall_through_tags(descriptor: dict[str, Any]) -> tuple[str, ...]:
    """Return the data-tags a saved function node lets fall through.

    Every function node of a saved file names them (ITL-1019). The saved output schema
    is checked against it: its tag and data halves share exactly these names. When the
    descriptor holds the pod's config, the config's list must agree too.

    Args:
        descriptor: A function node descriptor of a saved pipeline.

    Returns:
        The data-tags, in the saved order.

    Raises:
        ValueError: If the key is missing or not a list of names, or disagrees with
            the saved output schema or the saved pod config.
    """
    from orcapod.core.data_tags import data_tag_names

    output_schema = descriptor.get("output_schema") or {}
    shared = data_tag_names(output_schema.get("tag", {}), output_schema.get("data", {}))
    if FALL_THROUGH_TAGS_KEY not in descriptor:
        raise ValueError(
            "a saved file names the fall-through tags of every function node, but "
            f"this one has no {FALL_THROUGH_TAGS_KEY}"
        )
    saved = descriptor[FALL_THROUGH_TAGS_KEY]
    if not isinstance(saved, list) or not all(isinstance(n, str) for n in saved):
        raise ValueError(
            f"{FALL_THROUGH_TAGS_KEY} must be a list of names, got {saved!r}"
        )
    data_tags = tuple(saved)
    if set(shared) != set(data_tags):
        raise ValueError(
            f"the saved output schema has the data-tags {sorted(shared)}, but "
            f"{FALL_THROUGH_TAGS_KEY} is {sorted(data_tags)}"
        )
    function_config = descriptor.get("function_config")
    if function_config is not None:
        configured = function_config.get(FALL_THROUGH_TAGS_KEY, [])
        if set(configured) != set(data_tags):
            raise ValueError(
                f"the saved pod lets {sorted(configured)} fall through, but "
                f"{FALL_THROUGH_TAGS_KEY} is {sorted(data_tags)}"
            )
    return data_tags


# The fields of a source config that hold a nested source config: a cached source's
# inner source and a derived source's origin.
NESTED_SOURCE_CONFIG_KEYS = ("inner_source", "origin")


def check_saved_source_config(config: dict[str, Any]) -> None:
    """Refuse a saved source config whose own entries are malformed.

    Each source class that reads entries of its own from a saved config may declare
    ``check_saved_config(config)``, which raises ``ValueError`` on an entry it would
    have to interpret (a ``CSVSource``'s ``schema``, ITL-1315), and every source
    config saved before source ids named their kind (a ``source_id`` without a
    ``source_name``, ITL-1425) is refused. It is called here for
    the config and for every source config nested in it, so ``PipelineJob.load``
    refuses such a file instead of loading it with the source unbound.

    Args:
        config: A saved source config.

    Raises:
        ValueError: If the config or a config nested in it is malformed.
    """
    _ensure_registries()
    for key in NESTED_SOURCE_CONFIG_KEYS:
        nested = config.get(key)
        if isinstance(nested, dict):
            check_saved_source_config(cast("dict[str, Any]", nested))
    cls = SOURCE_REGISTRY.get(config.get("source_type"))  # pyright: ignore[reportArgumentType]  # any saved value
    from orcapod.core.sources.base import RootSource

    if isinstance(cls, type) and issubclass(cls, RootSource):
        cls.require_saved_name_fields(config)
    check = getattr(cls, "check_saved_config", None)
    if check is not None:
        check(config)


def require_tag_only_columns(config: dict[str, Any]) -> None:
    """Refuse a saved source config that does not state its plain tags.

    Every source of a saved file that takes ``tag_only_columns`` writes it (ITL-1019,
    ITL-1159), nested sources (the inner source of a cached source, a derived source's
    origin) included; a missing key is never read as a default.

    Args:
        config: A saved source config.

    Raises:
        ValueError: If the config, or a source config nested in it, takes the option
            and lacks it.
    """
    _ensure_registries()
    # Only the nested source fields hold source configs; any other mapping (a filter
    # such as ``where``) is the source's own data, whatever its keys.
    for key in NESTED_SOURCE_CONFIG_KEYS:
        nested = config.get(key)
        if isinstance(nested, dict):
            require_tag_only_columns(cast("dict[str, Any]", nested))
    cls = SOURCE_REGISTRY.get(config.get("source_type"))  # pyright: ignore[reportArgumentType]  # any saved value
    if cls is None or not getattr(cls, "TAKES_TAG_ONLY_COLUMNS", False):
        return
    if "tag_only_columns" not in config:
        raise ValueError(
            "a saved file states the plain tags of every source, but source config "
            f"{config.get('source_type')!r} has no tag_only_columns"
        )


def check_source_config_binding(
    config: dict[str, Any], tag_names: Iterable[str], data_names: Iterable[str]
) -> None:
    """Refuse a saved source config whose data-tags contradict its slot.

    The source config (``tag_columns`` and ``tag_only_columns``) and the slot's saved
    schema (a name in both halves is a data-tag) each state the source's
    bindings, independently; a file in which they disagree is refused before the
    source is rebuilt (ITL-1019). A config of a source that does not take the option,
    or one whose ``tag_columns`` is absent or ``None`` (a database source that takes
    its tags from the table's keys), is not checked here.

    Args:
        config: A saved source config (``require_tag_only_columns`` checked).
        tag_names: The slot's saved tag column names.
        data_names: The slot's saved data column names.

    Raises:
        ValueError: If the config and the slot name different data-tags.
    """
    from orcapod.core.data_tags import data_tag_names, source_data_tags

    _ensure_registries()
    cls = SOURCE_REGISTRY.get(config.get("source_type"))  # pyright: ignore[reportArgumentType]  # any saved value
    if cls is None or not getattr(cls, "TAKES_TAG_ONLY_COLUMNS", False):
        return
    tag_columns = config.get("tag_columns")
    # ``None``: a database source takes its tags from the table's keys when read.
    if tag_columns is None or "tag_only_columns" not in config:
        return
    # A source takes one tag name as a bare string: that name, never its characters.
    if isinstance(tag_columns, str):
        tag_columns = [tag_columns]
    configured = source_data_tags(tag_columns, config["tag_only_columns"])
    declared = data_tag_names(tag_names, data_names)
    if set(configured) != set(declared):
        raise ValueError(
            f"its config makes {sorted(configured)} data-tags, but its slot's saved "
            f"schema declares {sorted(declared)}"
        )
