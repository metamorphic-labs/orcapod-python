"""Core data structures and exceptions for the OrcaPod context system."""

from dataclasses import dataclass

from orcapod.protocols.hashing_protocols import (
    ArrowHasherProtocol,
    SemanticHasherProtocol,
)
from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol


@dataclass
class DataContext:
    """Data context containing all versioned components needed for data interpretation.

    Attributes:
        context_key: Unique identifier (e.g., "std:v1:default")
        version: Version string (e.g., "v1")
        description: Human-readable description
        type_converter: Type converter for Python ↔ Arrow conversion and
            registration. This is the single public API for all type operations.
        arrow_hasher: Arrow table hasher for this context
        semantic_hasher: General semantic hasher for this context. The
            ``PythonTypeHandlerRegistry`` used for hashing is accessible via
            ``semantic_hasher.type_handler_registry``.
    """

    context_key: str
    version: str
    description: str
    type_converter: TypeConverterProtocol
    arrow_hasher: ArrowHasherProtocol
    semantic_hasher: SemanticHasherProtocol

    def __post_init__(self) -> None:
        """Refuse an Arrow hasher without ``hash_schema`` (ITL-640).

        Every schema that enters an identity is hashed by ``arrow_hasher.hash_schema``
        (a member of ``ArrowHasherProtocol``), so a hasher without it would fail only
        when an identity is first computed, far from the context that holds it.

        Raises:
            ContextValidationError: Naming the hasher's class.
        """
        if not callable(getattr(self.arrow_hasher, "hash_schema", None)):
            raise ContextValidationError(
                f"Data context {self.context_key!r}: its Arrow hasher "
                f"{type(self.arrow_hasher).__qualname__} has no hash_schema method; "
                "every schema in an identity is hashed by it (ArrowHasherProtocol)."
            )


class ContextValidationError(Exception):
    """Raised when context validation fails."""

    pass


class ContextResolutionError(Exception):
    """Raised when context cannot be resolved."""

    pass
