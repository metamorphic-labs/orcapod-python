from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from orcapod.protocols.core_protocols.datagrams import DataProtocol
from orcapod.protocols.core_protocols.executor import DataFunctionExecutorProtocol
from orcapod.protocols.core_protocols.labelable import LabelableProtocol
from orcapod.protocols.hashing_protocols import (
    ContentIdentifiableProtocol,
    PipelineElementProtocol,
)
from orcapod.types import Schema

if TYPE_CHECKING:
    from orcapod.core.data_function_types import DataFunctionTypeDeclaration
    from orcapod.protocols.observability_protocols import DataExecutionLoggerProtocol


@runtime_checkable
class DataFunctionProtocol(
    ContentIdentifiableProtocol, PipelineElementProtocol, LabelableProtocol, Protocol
):
    """Protocol for a data-processing function.

    Processes individual data with declared input/output schemas.
    """

    # ==================== Identity & Metadata ====================
    @property
    def data_function_type_id(self) -> str:
        """The data function type and its major (e.g. ``python.function.v1``)."""
        ...

    @property
    def declaration(self) -> "DataFunctionTypeDeclaration":
        """The declaration of the data function type (ITL-1259).

        It gives the revision of the type's record, a component of the result table
        path, and the columns the type stores with every result row.
        """
        ...

    @property
    def canonical_function_name(self) -> str:
        """Human-readable function identifier"""
        ...

    @property
    def major_version(self) -> int:
        """Breaking changes increment this"""
        ...

    @property
    def minor_version_string(self) -> str:
        """Flexible minor version (e.g., "1", "4.3rc", "apple")"""
        ...

    @property
    def uri(self) -> tuple[str, ...]:
        """Stable, content-addressable identifier for this data function.

        Returns a tuple of strings uniquely identifying the function by name,
        output schema hash, major version, and function type.
        """
        ...

    @property
    def input_data_schema(self) -> Schema:
        """Schema describing the input data this function accepts."""
        ...

    @property
    def accepts_arbitrary_keywords(self) -> bool:
        """Whether the function takes input columns beyond ``input_data_schema``.

        ``True`` means every data column of the input is passed to the function
        (a ``**kwargs`` function), so no column is unused. A function pod never
        drops a column for such a function.
        """
        ...

    @property
    def output_data_schema(self) -> Schema:
        """Schema describing the output data this function produces."""
        ...

    # ==================== Content-Addressable Identity ====================
    def get_function_variation_data(self) -> dict[str, Any]:
        """Raw data defining function variation - system computes hash"""
        ...

    def get_function_variation_data_schema(self) -> Schema:
        """Schema for the data returned by ``get_function_variation_data``."""
        ...

    def get_execution_data(self) -> dict[str, Any]:
        """Raw data defining execution context - system computes hash"""
        ...

    def get_execution_data_schema(self) -> Schema:
        """Schema for the data returned by ``get_execution_data``."""
        ...

    # ==================== Executor ====================

    @property
    def executor(self) -> DataFunctionExecutorProtocol | None:
        """The executor used to run this function, or ``None`` for direct execution."""
        ...

    @executor.setter
    def executor(self, executor: DataFunctionExecutorProtocol | None) -> None:
        """Set or clear the executor."""
        ...

    # ==================== Execution ====================

    def call(
        self,
        data: DataProtocol,
        *,
        logger: "DataExecutionLoggerProtocol | None" = None,
        **kwargs: Any,
    ) -> DataProtocol | None:
        """Process a single data, routing through the executor if one is set.

        Args:
            data: The data payload to process.
            logger: Optional logger for recording captured I/O.
            **kwargs: Extra keyword arguments merged with ``data.as_dict()``
                before the underlying callable is invoked. Pods use this to
                supply auto-provided arguments (e.g. ``InvocationContext``)
                that are not part of the data schema.

        Returns:
            The output data, or ``None`` when the function filters the
            data out or when execution failed (exception is re-raised).
        """
        ...

    async def async_call(
        self,
        data: DataProtocol,
        *,
        logger: "DataExecutionLoggerProtocol | None" = None,
        **kwargs: Any,
    ) -> DataProtocol | None:
        """Asynchronously process a single data, routing through the executor if set.

        Args:
            data: The data payload to process.
            logger: Optional logger for recording captured I/O.
            **kwargs: Extra keyword arguments merged with ``data.as_dict()``
                before the underlying callable is invoked.

        Returns:
            The output data, or ``None``.
        """
        ...

    def direct_call(
        self,
        data: DataProtocol,
        **kwargs: Any,
    ) -> DataProtocol | None:
        """Execute the function's native computation on *data*.

        This is the method executors invoke, bypassing executor routing.
        On user-function failure the exception is re-raised.

        Args:
            data: The data payload to process.
            **kwargs: Extra keyword arguments merged with ``data.as_dict()``
                before the underlying callable is invoked.

        Returns:
            The output data, or ``None`` if filtered.
        """
        ...

    async def direct_async_call(
        self,
        data: DataProtocol,
        **kwargs: Any,
    ) -> DataProtocol | None:
        """Asynchronous counterpart of ``direct_call``."""
        ...

    # ==================== Preview ====================

    def require_preview(self) -> None:
        """Refuse a preview of this function before it is called (ITL-1464).

        Raises:
            PreviewRefusedError: If this function, or one it wraps, cannot be called
                read-only (``preview_call``).
        """
        ...

    def preview_call(self, data: DataProtocol) -> DataProtocol | None:
        """Process a single data for a preview: read-only at every layer (ITL-1464).

        A layer that keeps results reads them and stores none, and creates no table;
        it hands a miss to the function it wraps, read-only too.

        Args:
            data: The data payload to process.

        Returns:
            The output data, or ``None`` when the function filters the data out.

        Raises:
            PreviewRefusedError: As ``require_preview``.
        """
        ...

    # ==================== Serialization ====================

    def to_config(self) -> dict[str, Any]:
        """Serialize this data function to a JSON-compatible config dict."""
        ...

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "DataFunctionProtocol":
        """Reconstruct a data function from a config dict."""
        ...
