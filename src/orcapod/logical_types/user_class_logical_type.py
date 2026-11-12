"""Shared logical type and factory for user classes (dataclasses and pydantic models).

A user class's logical type name and Arrow extension name are its *identity string*
(``user_class_identity.identity_string``): the name, kind and the fields' names and
types in declared order, so two classes with the same name and different fields, or
the same fields in another order, are different types with different tables
(ITL-881). The Arrow extension metadata carries the full identity record, which
the read path uses to find the class and to describe a mismatch.

Write path (``create_for_python_type``):
    Resolves the name (declared name or naming policy), resolves each stored field's
    Arrow type and type token through the converter, and builds the identity.

Read path (``reconstruct_from_arrow``):
    Reads the identity record from the metadata, finds candidate classes by kind and
    name, and accepts the one whose identity string equals the stored extension name.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from orcapod.errors import RecursiveTypeError, TypeIdentityError
import orcapod.logical_types.user_class_identity as uci
from orcapod.logical_types.base_logical_type import BaseLogicalType
from orcapod.logical_types.registry import (
    make_arrow_extension_type,
    make_polars_extension_type,
)
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa

    from orcapod.logical_types.protocols import TypeConverterProtocol
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

logger = logging.getLogger(__name__)


class UserClassLogicalType(BaseLogicalType):
    """Logical type binding a user class to its Arrow extension type.

    The logical type name and Arrow extension name are the class's identity string;
    the storage type is a ``pa.struct`` of the stored fields in declared order.
    Subclasses set ``category``.

    Args:
        logical_name: The identity string (e.g. ``"Point@76d961e4..."``).
        python_type: The class.
        storage_type: The Arrow ``pa.StructType`` of the stored fields.
        field_annotations: ``(field_name, python_annotation)`` pairs matching the
            fields in ``storage_type``.
        identity_record: The identity record, embedded in the Arrow extension metadata.
            ``None`` embeds only the category (for low-level tests).
    """

    category: str = ""

    #: A subclass of the class is a different class with its own identity, so the
    #: registry must not match it to this logical type by ``issubclass``.
    exact_python_type_only: bool = True

    def __init__(
        self,
        logical_name: str,
        python_type: type,
        storage_type: pa.StructType,
        field_annotations: list[tuple[str, Any]],
        identity_record: dict[str, Any] | None = None,
    ) -> None:
        self._logical_name = logical_name
        self._python_type = python_type
        self._storage_type = storage_type
        self._field_annotations = field_annotations
        self._identity_record = identity_record

        metadata: dict[str, Any] = {"category": self.category}
        if identity_record is not None:
            metadata["identity"] = identity_record
        metadata_json = uci.canonical_json(metadata)
        # ``storage_type`` never contains nested extension types (ET1 in DESIGN_ISSUES.md):
        # the factory strips each field's top-level extension type before building it.
        self._arrow_ext_class = make_arrow_extension_type(
            logical_name, storage_type, metadata=metadata_json.encode("utf-8")
        )
        self._arrow_ext: pa.ExtensionType | None = None
        self._polars_ext_class = make_polars_extension_type(
            logical_name, storage_type, metadata=metadata_json
        )
        self._polars_ext: pl.BaseExtension | None = None

    @property
    def logical_type_name(self) -> str:
        """The class's identity string."""
        return self._logical_name

    @property
    def python_type(self) -> type:
        """The class this logical type represents."""
        return self._python_type

    @property
    def identity_record(self) -> dict[str, Any] | None:
        """The identity record, or ``None`` for a logical type built without one."""
        return self._identity_record

    @property
    def storage_type(self) -> pa.StructType:
        """The Arrow struct type of the stored fields."""
        return self._storage_type

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the cached Arrow extension type (name = identity string)."""
        if self._arrow_ext is None:
            self._arrow_ext = self._arrow_ext_class()  # pyright: ignore[reportCallIssue]  # synthesized class takes no arguments
        return self._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type."""
        if self._polars_ext is None:
            self._polars_ext = self._polars_ext_class()  # pyright: ignore[reportCallIssue]  # synthesized class takes no arguments
        return self._polars_ext

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None
    ) -> dict[str, Any]:
        """Convert an instance to an Arrow-compatible struct dict.

        Args:
            value: An instance of ``python_type``.
            converter: The active converter for per-field conversion.

        Returns:
            A dict mapping field names to their Arrow storage values.

        Raises:
            ValueError: If ``converter`` is ``None``.
        """
        if converter is None:
            raise ValueError(
                f"{type(self).__name__}.python_to_storage requires a converter — "
                "pass a TypeConverterProtocol instance for field-level conversion."
            )
        return {
            name: converter.python_to_storage(getattr(value, name), annotation)
            for name, annotation in self._field_annotations
        }

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None
    ) -> Any:
        """Build an instance from an Arrow struct dict.

        Args:
            storage_value: A dict mapping field names to Arrow storage values, or
                ``None`` for a null value (an optional field of this class that
                holds ``None``).
            converter: The active converter for per-field conversion.

        Returns:
            An instance of ``python_type``, or ``None`` for a null value.

        Raises:
            ValueError: If ``converter`` is ``None``.
        """
        if converter is None:
            raise ValueError(
                f"{type(self).__name__}.storage_to_python requires a converter — "
                "pass a TypeConverterProtocol instance for field-level conversion."
            )
        if storage_value is None:
            return None
        kwargs = {
            name: converter.storage_to_python(storage_value[name], annotation)
            for name, annotation in self._field_annotations
        }
        return self._python_type(**kwargs)


class UserClassLogicalTypeFactory:
    """Base factory for user-class logical types.

    Subclasses set ``kind``, ``category`` and ``logical_type_class``.
    """

    kind: str = ""
    category: str = ""
    logical_type_class: type[UserClassLogicalType] = UserClassLogicalType

    def supports_class(self, python_type: type) -> bool:
        """Return True if this factory handles ``python_type``."""
        return uci.class_kind(python_type) == self.kind

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def create_for_python_type(
        self,
        python_type: type,
        converter: TypeConverterProtocol,
    ) -> UserClassLogicalType:
        """Build the logical type of a user class (write path).

        Args:
            python_type: The class.
            converter: The active converter for field-type resolution.

        Returns:
            A logical type whose name is the class's identity string.

        Raises:
            ValueError: If the class is local or its type hints cannot be resolved.
            TypeError: If the class refers back to itself (directly or through other
                classes), naming the field path.
        """
        uci.reject_local_class(python_type)
        name = uci.built_name(python_type) or uci.resolve_name(python_type)
        record, storage_type, field_annotations = self._describe(
            python_type, name, converter
        )
        identity = uci.identity_string(record)
        uci.mark_built(python_type, name, identity, record)
        uci.remember(self.kind, name, python_type)
        uci.notice_main_class(python_type, name)
        logger.debug("%s: built %r for %r", type(self).__name__, identity, python_type)
        return self.logical_type_class(
            identity, python_type, storage_type, field_annotations, record
        )

    def _describe(
        self, python_type: type, name: str, converter: TypeConverterProtocol
    ) -> tuple[dict[str, Any], pa.StructType, list[tuple[str, Any]]]:
        """Return the identity record, storage struct and field annotations of a class.

        Registers the field types (their Arrow types are needed), but not
        ``python_type`` itself.

        Args:
            python_type: The class.
            name: The name component to use.
            converter: The active converter for field-type resolution.

        Returns:
            ``(record, storage_type, field_annotations)``.

        Raises:
            ValueError: If the type hints cannot be resolved.
            RecursiveTypeError: If the class refers back to itself, naming the path.
        """
        hints = uci.type_hints(python_type)

        arrow_fields = []
        field_annotations: list[tuple[str, Any]] = []
        tokens: list[tuple[str, str]] = []
        # Declared order: it is part of the identity, and the storage struct follows
        # it, so one identity always has one struct.
        for field_name in uci.stored_field_names(python_type):
            annotation = hints.get(field_name, Any)
            try:
                arrow_type = converter.register_python_class(annotation)
            except TypeError as exc:
                if isinstance(exc, RecursiveTypeError):
                    raise RecursiveTypeError(
                        f"{python_type.__qualname__}.{field_name} -> {exc}"
                    ) from exc
                raise
            # Struct fields cannot hold extension types (ET1; see DESIGN_ISSUES.md).
            if isinstance(arrow_type, pa.ExtensionType):
                arrow_type = arrow_type.storage_type
            arrow_fields.append(pa.field(field_name, arrow_type))
            field_annotations.append((field_name, annotation))
            tokens.append((field_name, uci.type_token(annotation, converter)))

        record = uci.identity_record(self.kind, name, tokens)
        return record, pa.struct(arrow_fields), field_annotations

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: pa.DataType,
        metadata: dict[str, Any],
        converter: TypeConverterProtocol,
    ) -> UserClassLogicalType:
        """Find the class of a stored column and return its logical type (read path).

        Args:
            arrow_extension_name: The stored identity string.
            storage_type: The stored struct type (already resolved).
            metadata: The parsed extension metadata, with ``category`` and ``identity``.
            converter: The active converter.

        Returns:
            The logical type of the class whose identity matches the stored one.

        Raises:
            ImportError: If no class with the stored kind and name can be found.
            TypeIdentityError: If the column has no identity record (written before
                Orcapod 0.3), or the classes found have a different identity or a
                different storage type.
        """
        record = metadata.get("identity")
        if not isinstance(record, dict):
            raise TypeIdentityError(
                f"The stored {self.kind} column type {arrow_extension_name!r} was written "
                f"by an Orcapod version before 0.3, which identified classes by module "
                f"and qualified name only. Orcapod 0.3 identifies a class by its name, "
                f"kind and fields and does not read such columns; re-running the "
                f"pipeline computes the results into new tables."
            )
        name = record.get("name")
        kind = record.get("kind")
        if (
            kind != self.kind
            or not isinstance(name, str)
            or not isinstance(record.get("fields"), list)
        ):
            raise TypeIdentityError(
                f"Stored column type {arrow_extension_name!r} has an invalid identity "
                f"record for category {self.category!r}: {record!r}."
            )

        if uci.identity_string(record) != arrow_extension_name:
            raise TypeIdentityError(
                f"Stored column type {arrow_extension_name!r} does not match its "
                f"identity record {uci.describe_record(record)}."
            )

        kind = self.kind
        found_any = False
        mismatches: list[str] = []
        renamable = False  # a candidate has the stored fields under another name
        # Lazy: the dotted name is imported only if no registered class matches.
        for cls in uci.candidate_classes(kind, name):
            found_any = True
            built = uci.identity_of(cls)
            if built is not None:
                if built != arrow_extension_name:
                    built_record = uci.built_record(cls)
                    mismatches.append(_describe_candidate(cls, built_record))
                    continue
                lt = converter.get_logical_type_for_python_type(cls)
                if isinstance(lt, UserClassLogicalType):
                    _check_storage(lt, storage_type, record)
                    return lt
                continue
            # Not used yet in this process: test it without registering it or the
            # classes it refers to, so a rejected candidate and its nested classes
            # keep their freedom to be declared under a name.
            try:
                candidate_record = uci.probe_record(cls, converter)
            except (TypeError, ValueError) as exc:
                mismatches.append(
                    f"{cls.__module__}.{cls.__qualname__} (cannot be stored: {exc})"
                )
                continue
            if uci.identity_string(candidate_record) != arrow_extension_name:
                mismatches.append(_describe_candidate(cls, candidate_record))
                renamable = renamable or candidate_record["fields"] == record["fields"]
                continue
            _ = converter.register_python_class(cls)
            lt = converter.get_logical_type_for_python_type(cls)
            if isinstance(lt, UserClassLogicalType):
                _check_storage(lt, storage_type, record)
                return lt

        if not found_any:
            raise ImportError(
                f"Cannot find the {kind} {name!r} to read stored values of type "
                f"{uci.describe_record(record)}. Define the class and register it "
                f"under that name before reading, for example "
                f"op.register_type({name.rsplit('.', 1)[-1]}, name={name!r})."
            )

        hint = (
            f" A class with the stored fields exists under another name: register it "
            f"as {name!r} before it is first used, e.g. "
            f"op.register_type(<class>, name={name!r})."
            if renamable
            else " A class with the same name but different fields, or the same fields "
            "in another order, is a different type; restore the stored fields and "
            "their order, or use the class as it is now to compute new results."
        )
        raise TypeIdentityError(
            f"Stored values of {uci.describe_record(record)} do not match the classes "
            f"found: {'; '.join(mismatches)}.{hint}"
        )


def _describe_candidate(cls: type, record: dict[str, Any] | None) -> str:
    """Render a candidate class for a read error: its path and, if known, its identity."""
    where = f"{cls.__module__}.{cls.__qualname__}"
    if record is None:
        return where
    return f"{where} as {uci.describe_record(record)}"


def _check_storage(
    lt: UserClassLogicalType, storage_type: pa.DataType, record: dict[str, Any]
) -> None:
    """Raise ``TypeIdentityError`` if stored and registered storage differ beyond layout."""
    from orcapod.utils.arrow_utils import normalize_to_large_types

    if normalize_to_large_types(
        lt.storage_type, keep_null=True
    ) != normalize_to_large_types(storage_type, keep_null=True):
        raise TypeIdentityError(
            f"Stored values of {uci.describe_record(record)} have Arrow storage type "
            f"{storage_type}, but the registered class stores {lt.storage_type}."
        )
