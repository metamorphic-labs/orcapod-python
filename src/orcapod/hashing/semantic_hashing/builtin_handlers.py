"""
Built-in PythonTypeHandlerProtocol implementations.

  UUIDHandler           -- uuid.UUID: 16-byte binary representation
  BytesHandler          -- bytes/bytearray: hex string representation
  FunctionHandler       -- callable with __code__: via FunctionInfoExtractorProtocol
  TypeObjectHandler     -- type objects: stable "type:<module>.<qualname>" string
  SpecialFormHandler    -- typing._SpecialForm
  GenericAliasHandler   -- generic alias type annotations
  UnionTypeHandler      -- types.UnionType (Python 3.10+ X | Y syntax)
  ArrowTableHandler     -- pa.Table / pa.RecordBatch values: arrow_table_v1 (ordered Arrow digest)
  SchemaHandler         -- Schema objects
  FileHandler           -- orcapod.File: file content hash
  DirectoryHandler      -- orcapod.Directory: recursive Merkle tree hash
  NumpyArrayHandler     -- numpy.ndarray: SHA-256 ContentHash of .npy bytes
  PandasDataFrameHandler -- pd.DataFrame: pandas_dataframe_v1 (descriptor + ordered Arrow digest)
  PandasSeriesHandler   -- pd.Series: pandas_series_v1 (descriptor + ordered Arrow digest)

``register_builtin_python_type_handlers(registry)`` populates a registry
with all of the above handlers.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from uuid import UUID

from orcapod.types import ContentHash, Schema
from orcapod.utils.canonical_type_names import canonical_type_name

if TYPE_CHECKING:
    from orcapod.protocols.hashing_protocols import (
        ArrowHasherProtocol,
        DirectoryHasherProtocol,
        FileContentHasherProtocol,
        HandlerRegistryProtocol,
        SemanticHasherProtocol,
    )
    from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol

logger = logging.getLogger(__name__)


class UUIDHandler:
    """Hasher for ``uuid.UUID`` objects — returns the raw 16-byte binary representation."""

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        return obj.bytes


class BytesHandler:
    """Hasher for bytes and bytearray objects — returns the lowercase hex string."""

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        if isinstance(obj, (bytes, bytearray)):
            return obj.hex()
        raise TypeError(f"BytesHandler: expected bytes or bytearray, got {type(obj)!r}")


class FunctionHandler:
    """Hasher for Python functions/callables with a ``__code__`` attribute.

    Args:
        function_info_extractor: Any object with an
            ``extract_function_info(func) -> dict`` method.
    """

    def __init__(self, function_info_extractor: Any) -> None:
        self.function_info_extractor = function_info_extractor

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        if not (callable(obj) and hasattr(obj, "__code__")):
            raise TypeError(
                f"FunctionHandler: expected a callable with __code__, got {type(obj)!r}"
            )
        func_name = getattr(obj, "__name__", repr(obj))
        logger.debug("FunctionHandler: extracting info for function %r", func_name)
        info: dict[str, Any] = self.function_info_extractor.extract_function_info(obj)
        return info


class TypeObjectHandler:
    """Hasher for type objects (classes passed as values).

    Resolves types registered in the ``LogicalTypeRegistry`` exposed by
    *type_converter* to their stable ``logical_type_name``
    (e.g. ``"type:orcapod.file"`` for ``op.File``).
    Falls back to ``"type:<module>.<qualname>"`` for unregistered types or
    when no ``type_converter`` is provided. The module is the canonical one
    from ``canonical_type_name``, so standard-library types that moved into a
    private submodule (``pathlib._local`` on Python 3.13) hash identically on
    every Python version.

    Args:
        type_converter: Optional ``TypeConverterProtocol``.  When provided,
            ``type_converter.get_logical_type(obj)`` is called to resolve
            registered logical types to their stable canonical name.  When
            ``None`` (the default), the fallback ``"type:<module>.<qualname>"``
            serialisation is always used.
    """

    def __init__(self, type_converter: "TypeConverterProtocol | None" = None) -> None:
        self._type_converter = type_converter

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        if not isinstance(obj, type):
            raise TypeError(
                f"TypeObjectHandler: expected a type/class, got {type(obj)!r}"
            )
        if self._type_converter is not None:
            lt = self._type_converter.get_logical_type(obj)
            if lt is not None:
                return f"type:{lt.logical_type_name}"
        return f"type:{canonical_type_name(obj)}"


class SpecialFormHandler:
    """Hasher for ``typing._SpecialForm`` objects such as ``typing.Union``."""

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        name = getattr(obj, "_name", None) or repr(obj)
        return f"special_form:typing.{name}"


class GenericAliasHandler:
    """Hasher for generic alias type annotations (``dict[int, str]``, ``Optional[X]``, etc.)."""

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        import typing

        origin = getattr(obj, "__origin__", None)
        args = getattr(obj, "__args__", None) or ()
        if origin is None:
            return f"generic_alias:{obj!r}"
        if origin is typing.Union:
            hashed_args = sorted(hasher.hash_object(arg).to_string() for arg in args)
            return {"__type__": "union", "args": hashed_args}
        return {
            "__type__": "generic_alias",
            "origin": hasher.hash_object(origin).to_string(),
            "args": [hasher.hash_object(arg).to_string() for arg in args],
        }


class UnionTypeHandler:
    """Hasher for ``types.UnionType`` objects (Python 3.10+ ``X | Y`` syntax)."""

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        args = getattr(obj, "__args__", None) or ()
        hashed_args = sorted(hasher.hash_object(arg).to_string() for arg in args)
        return {"__type__": "union", "args": hashed_args}


ARROW_TABLE_HASH_METHOD = "arrow_table_v1"
"""``ContentHash`` method label of a ``pa.Table`` / ``pa.RecordBatch`` value hash."""


class ArrowTableHandler:
    """Hasher for ``pa.Table`` and ``pa.RecordBatch`` values: ``arrow_table_v1``.

    A table used as a value has an order-dependent identity, as ``pa.Table.equals`` does
    (developer's decision, November 11, 2026): the hash is the Arrow hasher's
    ``hash_table_ordered`` digest (the same semantic preprocessing as every Orcapod Arrow hash,
    with column order and struct field order at every depth included) under the label
    ``arrow_table_v1``. The label's definition includes that Arrow hasher (``arrow_v0.1``): a
    label is bumped whenever the hash of any value can change (developer's rule, November 11,
    2026), so a new Arrow hasher means ``arrow_table_v2``; a test pins the pair. A ``RecordBatch`` hashes like the one-batch ``Table`` with the same
    content; chunking is not part of the hash.

    Orcapod's own tables (stream and datagram identities) do not come here: they ignore order
    on purpose and call ``arrow_hasher.hash_table`` directly.

    Args:
        arrow_hasher: Any object satisfying ``ArrowHasherProtocol``.  When
            ``None``, the default data context's ``arrow_hasher`` is resolved
            lazily at call time (breaking the circular dependency that would
            arise if the registry were constructed before the arrow hasher).
    """

    def __init__(self, arrow_hasher: "ArrowHasherProtocol | None" = None) -> None:
        self._arrow_hasher = arrow_hasher

    def _get_arrow_hasher(self) -> "ArrowHasherProtocol":
        if self._arrow_hasher is not None:
            return self._arrow_hasher
        from orcapod.contexts import get_default_context

        return get_default_context().arrow_hasher  # type: ignore[return-value]

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
        """Return the ``arrow_table_v1`` content hash of ``obj``.

        Args:
            obj: A ``pa.Table`` or ``pa.RecordBatch``.
            hasher: Ignored. Present for protocol conformance.

        Returns:
            A ``ContentHash`` with method ``"arrow_table_v1"``.

        Raises:
            TypeError: If ``obj`` is neither a ``pa.Table`` nor a ``pa.RecordBatch``.
        """
        import pyarrow as _pa

        if not isinstance(obj, (_pa.Table, _pa.RecordBatch)):
            raise TypeError(
                f"ArrowTableHandler: expected pa.Table or pa.RecordBatch, got {type(obj)!r}"
            )
        digest = self._get_arrow_hasher().hash_table_ordered(obj).digest
        return ContentHash(method=ARROW_TABLE_HASH_METHOD, digest=digest)


class SchemaHandler:
    """Hasher for ``Schema`` objects.

    Canonical, explicit path for schema hashing. For each field, hashes the Python
    type via ``hasher.hash_object`` (which dispatches to ``TypeObjectHandler``, using
    the stable ``logical_type_name`` / Arrow extension name for registered types and the
    stable ``builtins.*`` / stdlib path for native types). Field names are sorted so the
    hash is deterministic regardless of insertion order.

    This produces the same hash as the previous accidental path through ``_expand_mapping``
    (Schema is a Mapping), preserving all existing hash values.

    ``Schema.optional_fields`` is intentionally excluded from the hash. Two schemas with
    the same field names and types but different optionality are hash-equivalent.
    Optionality is a Python-level execution contract (which parameters have defaults),
    not part of the structural identity used for caching or pipeline routing.

    When ``type_converter`` is provided, every field type is verified to be
    Arrow-translatable before hashing. A type that cannot be converted raises
    ``TypeError`` with a diagnostic message, catching unregistered types at hash time
    rather than silently falling back to a potentially unstable Python module path.

    Args:
        type_converter: Optional ``TypeConverterProtocol``. When provided, validates
            Arrow-translatability per field. When ``None`` (e.g. in tests without a
            full ``DataContext``), validation is skipped.
    """

    def __init__(self, type_converter: "TypeConverterProtocol | None" = None) -> None:
        self._type_converter = type_converter

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
        """Hash ``obj`` by hashing each field type individually and sorting by name.

        Args:
            obj: A ``Schema`` instance.
            hasher: The calling ``SemanticHasherProtocol`` — used to hash each field's
                Python type.

        Returns:
            A ``dict[str, str]`` mapping field name to the ``to_string()`` of each
            field type's hash, sorted by field name for determinism.

        Raises:
            TypeError: If ``obj`` is not a ``Schema``, or if ``type_converter`` is
                provided and a field type is not Arrow-translatable.
        """
        if not isinstance(obj, Schema):
            raise TypeError(f"SchemaHandler: expected a Schema, got {type(obj)!r}")
        result: dict[str, str] = {}
        for field_name, python_type in obj.items():
            if self._type_converter is not None:
                try:
                    self._type_converter.python_type_to_arrow_type(python_type)
                except (TypeError, ValueError) as exc:
                    raise TypeError(
                        f"SchemaHandler: field {field_name!r} has type "
                        f"{python_type!r} that is not Arrow-translatable. "
                        f"Every type in a schema must be Arrow-convertible — "
                        f"register it as an orcapod logical type or use a "
                        f"supported native type (int, str, float, bool, bytes, "
                        f"datetime, date)."
                    ) from exc
            result[field_name] = hasher.hash_object(python_type).to_string()
        # Sort by field name for determinism — matches _expand_mapping's sort_keys.
        return dict(sorted(result.items()))


class FileHandler:
    """Hasher for ``orcapod.File`` objects — hashes file *content*.

    By the time ``handle`` is called, ``File``'s constructor has already validated
    that the path exists and is a non-directory file (and is not a symlink when
    ``follow_symlinks=False``). The hash is produced by reading file bytes through
    the wrapped ``UPath``, which follows symlinks by default.

    Args:
        file_hasher: Any object with a ``hash_file(path) -> ContentHash`` method.
    """

    def __init__(self, file_hasher: "FileContentHasherProtocol") -> None:
        self.file_hasher = file_hasher

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
        # Deferred import breaks the circular dependency between this module and
        # file_type.py — the same pattern used by ArrowTableHandler.
        from orcapod.logical_types.file_type import File

        if not isinstance(obj, File):
            raise TypeError(f"FileHandler: expected an orcapod.File, got {type(obj)!r}")
        wrapped = getattr(obj, "__wrapped__")
        logger.debug("FileHandler: hashing file content at %s", wrapped)
        return self.file_hasher.hash_file(wrapped)


class DirectoryHandler:
    """Hasher for ``orcapod.Directory`` objects — hashes directory *content* via Merkle tree.

    When a ``Directory`` is created via its normal constructor, existence and
    traversability are validated at construction time. Derived ``Directory`` instances
    created by path-navigation operations (e.g. ``.parent``, ``/`` operator) bypass that
    validation via ``_from_upath`` — so existence is not guaranteed at ``handle`` time.
    The hash is produced by ``BasicDirectoryHasher`` using a recursive Merkle scheme.

    Args:
        directory_hasher: Any object with a
            ``hash_directory(path, ignore) -> ContentHash`` method.
    """

    def __init__(self, directory_hasher: "DirectoryHasherProtocol") -> None:
        self.directory_hasher = directory_hasher

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
        # Deferred import breaks the circular dependency between this module and
        # directory_type.py — the same pattern used by FileHandler.
        from orcapod.logical_types.directory_type import Directory

        if not isinstance(obj, Directory):
            raise TypeError(
                f"DirectoryHandler: expected an orcapod.Directory, got {type(obj)!r}"
            )
        wrapped = getattr(obj, "__wrapped__")
        ignore = getattr(obj, "_ignore", None)
        logger.debug("DirectoryHandler: hashing directory content at %s", wrapped)
        return self.directory_hasher.hash_directory(wrapped, ignore=ignore)


class NumpyArrayHandler:
    """Hasher for ``numpy.ndarray`` — content hash via SHA-256 of numpy's ``.npy`` bytes.

    Serialises the array to numpy's ``.npy`` binary format (which encodes dtype,
    including structured/record field names, shape, byte order, and data), then
    returns a ``ContentHash`` produced by SHA-256 of those bytes. This is identical
    to what ``LogicalNumpyArray`` stores in Arrow, so the hash input and storage
    representation are always consistent.

    Returning ``ContentHash`` directly (rather than the raw ``.npy`` bytes) avoids
    the hex-expansion and JSON-serialisation overhead that the semantic hasher would
    apply to ``bytes`` returns — important for large arrays.

    Object-dtype arrays are rejected with ``ValueError`` immediately (before
    ``np.save`` is called). Structured dtypes containing object-typed fields
    are caught by ``allow_pickle=False`` in ``np.save``.
    """

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
        """Return a SHA-256 ``ContentHash`` of the ``.npy`` bytes for ``obj``.

        Args:
            obj: A ``numpy.ndarray`` instance.
            hasher: Ignored. Present for protocol conformance.

        Returns:
            A ``ContentHash`` with ``method="sha256"`` and digest equal to the
            SHA-256 of the array's ``.npy`` binary representation.

        Raises:
            TypeError: If ``obj`` is not a ``numpy.ndarray``.
            ValueError: If ``obj`` has ``dtype.kind == "O"`` (object dtype).
        """
        import hashlib
        import io
        import numpy as np

        if not isinstance(obj, np.ndarray):
            raise TypeError(
                f"NumpyArrayHandler: expected numpy.ndarray, got {type(obj)!r}"
            )
        if obj.dtype.kind == "O":
            raise ValueError(
                f"NumpyArrayHandler does not support object-dtype arrays "
                f"(dtype={obj.dtype!r}). Object arrays require pickling, "
                "which is disabled for security."
            )
        buf = io.BytesIO()
        np.save(buf, obj, allow_pickle=False)
        return ContentHash(
            method="sha256", digest=hashlib.sha256(buf.getvalue()).digest()
        )


class PandasDataFrameHandler:
    """Hasher for ``pd.DataFrame``: Orcapod's pandas descriptor plus an ordered Arrow digest.

    Delegates to ``pandas_frame_content_hash`` in ``orcapod.hashing.pandas_value_hash``, whose
    module docstring defines the ``pandas_dataframe_v1`` scheme. The hash distinguishes what
    ``assert_frame_equal`` distinguishes with its defaults: column order, index levels and their
    order, label types, ``columns.names``, pandas dtypes and categorical categories, and the
    values (except ``flags``, and ``-0.0`` vs ``0.0``, which hash apart).

    Args:
        arrow_hasher: An object satisfying ``ArrowHasherProtocol``; its ``hash_table_ordered``
            digests the values. When ``None``, the default data context's ``arrow_hasher`` is
            resolved lazily at call time (breaking the circular dependency that would arise if
            the registry were constructed before the arrow hasher).
    """

    def __init__(self, arrow_hasher: "ArrowHasherProtocol | None" = None) -> None:
        self._arrow_hasher = arrow_hasher

    def _get_arrow_hasher(self) -> "ArrowHasherProtocol":
        if self._arrow_hasher is not None:
            return self._arrow_hasher
        from orcapod.contexts import get_default_context

        return get_default_context().arrow_hasher  # type: ignore[return-value]

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
        """Return the content hash of ``obj``.

        Args:
            obj: A ``pd.DataFrame`` instance.
            hasher: Ignored. Present for protocol conformance.

        Returns:
            A ``ContentHash`` with method ``"pandas_dataframe_v1"``.

        Raises:
            TypeError: If ``obj`` is not a ``pd.DataFrame``.
            ValueError: If a label or dtype is outside the descriptor grammar, or a column
                cannot be converted to Arrow.
        """
        import pandas as _pd

        from orcapod.hashing.pandas_value_hash import pandas_frame_content_hash

        if not isinstance(obj, _pd.DataFrame):
            raise TypeError(
                f"PandasDataFrameHandler: expected pd.DataFrame, got {type(obj)!r}"
            )
        return pandas_frame_content_hash(obj, self._get_arrow_hasher())


class PandasSeriesHandler:
    """Hasher for ``pd.Series``: Orcapod's pandas descriptor plus an ordered Arrow digest.

    Delegates to ``pandas_series_content_hash`` in ``orcapod.hashing.pandas_value_hash``
    (``pandas_series_v1``). The name is part of the hash, typed (``0`` and ``"0"`` differ), as in
    ``assert_series_equal``.

    Args:
        arrow_hasher: An object satisfying ``ArrowHasherProtocol``; its ``hash_table_ordered``
            digests the values. When ``None``, the default data context's ``arrow_hasher`` is
            resolved lazily at call time (breaking the circular dependency that would arise if
            the registry were constructed before the arrow hasher).
    """

    def __init__(self, arrow_hasher: "ArrowHasherProtocol | None" = None) -> None:
        self._arrow_hasher = arrow_hasher

    def _get_arrow_hasher(self) -> "ArrowHasherProtocol":
        if self._arrow_hasher is not None:
            return self._arrow_hasher
        from orcapod.contexts import get_default_context

        return get_default_context().arrow_hasher  # type: ignore[return-value]

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
        """Return the content hash of ``obj``.

        Args:
            obj: A ``pd.Series`` instance.
            hasher: Ignored. Present for protocol conformance.

        Returns:
            A ``ContentHash`` with method ``"pandas_series_v1"``.

        Raises:
            TypeError: If ``obj`` is not a ``pd.Series``.
            ValueError: If a label or the dtype is outside the descriptor grammar, or the values
                cannot be converted to Arrow.
        """
        import pandas as _pd

        from orcapod.hashing.pandas_value_hash import pandas_series_content_hash

        if not isinstance(obj, _pd.Series):
            raise TypeError(
                f"PandasSeriesHandler: expected pd.Series, got {type(obj)!r}"
            )
        return pandas_series_content_hash(obj, self._get_arrow_hasher())


def register_builtin_python_type_handlers(
    registry: "HandlerRegistryProtocol",
    file_hasher: Any = None,
    function_info_extractor: Any = None,
    arrow_hasher: "ArrowHasherProtocol | None" = None,
    directory_hasher: Any = None,
    type_converter: "TypeConverterProtocol | None" = None,
) -> None:
    """Register all built-in semantic hashers into *registry*.

    ``pa.Table`` and ``pa.RecordBatch`` are registered via ``ArrowTableHandler``.
    ``pd.DataFrame`` and ``pd.Series`` are registered via ``PandasDataFrameHandler``
    and ``PandasSeriesHandler`` (``pandas_dataframe_v1`` / ``pandas_series_v1``, values
    digested by the Arrow hasher's ``hash_table_ordered``). When ``arrow_hasher`` is
    provided it is passed through for immediate use; when ``None``, these handlers
    resolve the active arrow hasher lazily via ``get_default_context()`` at hash time,
    breaking the construction-time circular dependency.

    ``orcapod.File`` is registered via ``FileHandler`` for content-based file
    hashing. ``orcapod.Directory`` is registered via ``DirectoryHandler`` for
    recursive Merkle tree directory hashing. ``pathlib.Path`` and ``upath.UPath``
    are NOT registered here. When these types appear in pipeline columns they are
    handled at the Arrow level through their ``LogicalPath`` / ``LogicalUPath``
    extension types, which store the path string in ``large_string()`` storage.
    The Arrow hasher then operates directly on that string storage — no Python-level
    roundtrip and no file I/O occurs. Passing a raw ``Path`` or ``UPath`` directly to
    the Python semantic hasher raises ``TypeError`` in strict mode (the default).

    Args:
        registry: The ``HandlerRegistryProtocol`` instance to populate.
        file_hasher: Optional ``FileContentHasherProtocol`` for file content hashing.
            Defaults to ``FileHasher(sha256)``.
        function_info_extractor: Optional ``FunctionInfoExtractorProtocol``.
            Defaults to ``FunctionSignatureExtractor``.
        arrow_hasher: Optional ``ArrowHasherProtocol`` for nested table hashing.
            When ``None``, lazy resolution via the default context is used.
        directory_hasher: Optional ``DirectoryHasherProtocol`` for directory tree hashing.
            Defaults to ``BasicDirectoryHasher(sha256)``.
        type_converter: Optional ``TypeConverterProtocol`` forwarded to
            ``TypeObjectHandler``, ``FunctionSignatureExtractor``, and
            ``SchemaHandler`` for stable canonical type-name resolution via
            ``get_logical_type()`` and Arrow-translatability validation.
            When ``None`` (the default), all three handlers fall back to the raw
            ``"type:<module>.<qualname>"`` serialisation and skip Arrow validation.
    """
    if file_hasher is None:
        from orcapod.hashing.file_hashers import FileHasher

        file_hasher = FileHasher(algorithm="sha256")

    if directory_hasher is None:
        from orcapod.hashing.directory_hashers import BasicDirectoryHasher

        directory_hasher = BasicDirectoryHasher(
            file_hasher=file_hasher,
            algorithm="sha256",
        )

    if function_info_extractor is None:
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionSignatureExtractor,
        )

        function_info_extractor = FunctionSignatureExtractor(
            include_module=True,
            include_defaults=True,
            type_converter=type_converter,
        )

    bytes_hasher = BytesHandler()
    registry.register(bytes, bytes_hasher)
    registry.register(bytearray, bytes_hasher)

    registry.register(UUID, UUIDHandler())

    from orcapod.logical_types.file_type import File

    registry.register(File, FileHandler(file_hasher))

    from orcapod.logical_types.directory_type import Directory

    registry.register(Directory, DirectoryHandler(directory_hasher))

    import types as _types

    function_hasher = FunctionHandler(function_info_extractor)
    registry.register(_types.FunctionType, function_hasher)
    registry.register(_types.BuiltinFunctionType, function_hasher)
    registry.register(_types.MethodType, function_hasher)

    registry.register(type, TypeObjectHandler(type_converter=type_converter))
    registry.register(_types.UnionType, UnionTypeHandler())

    generic_alias_hasher = GenericAliasHandler()
    registry.register(_types.GenericAlias, generic_alias_hasher)
    try:
        import typing as _typing

        registry.register(_typing._GenericAlias, generic_alias_hasher)  # type: ignore[attr-defined]
        registry.register(_typing._SpecialForm, SpecialFormHandler())  # type: ignore[attr-defined]
    except AttributeError:
        pass

    registry.register(Schema, SchemaHandler(type_converter=type_converter))

    import pyarrow as _pa

    arrow_table_hasher = ArrowTableHandler(arrow_hasher)
    registry.register(_pa.Table, arrow_table_hasher)
    registry.register(_pa.RecordBatch, arrow_table_hasher)

    import numpy as _np

    registry.register(_np.ndarray, NumpyArrayHandler())

    import pandas as _pd

    registry.register(_pd.DataFrame, PandasDataFrameHandler(arrow_hasher))
    registry.register(_pd.Series, PandasSeriesHandler(arrow_hasher))

    logger.debug(
        "register_builtin_python_type_handlers: registered %d hashers",
        len(registry),
    )
