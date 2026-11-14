"""Side-effect nodes restored from a saved pipeline or job file, read-only (ITL-1094).

A saved file records a side-effect node's label, content hash, pipeline hash, output
schema, node uri, data context and Orcapod configuration. It records neither the
side-effect function nor the pod configuration (``on_error``, ``track_completion``,
``drop_on_failure``, ``max_concurrency``). ``Pipeline.load`` and ``PipelineJob.load``
restore the node from that record, in its saved place in the graph, with no pod: it
never delivers and never writes its invocation log.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from orcapod.side_effects import SideEffectJobNode, SideEffectNode

if TYPE_CHECKING:
    from orcapod.channels import ReadableChannel, WritableChannel
    from orcapod.config import OrcapodConfig
    from orcapod.contexts import DataContext
    from orcapod.core.streams.view import StreamView
    from orcapod.protocols.core_protocols import (
        DataProtocol,
        StreamProtocol,
        TagProtocol,
    )
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
    from orcapod.side_effects import SideEffectPod
    from orcapod.types import ColumnConfig, ContentHash, Schema


class SavedSideEffectNode:
    """What a saved pipeline or job file records for one side-effect node.

    Args:
        descriptor: The node's entry in the saved file (``node_type``
            ``"side_effect"``).

    Raises:
        ValueError: If the descriptor lacks ``content_hash``, ``pipeline_hash`` or
            ``node_uri``.
    """

    def __init__(self, descriptor: dict[str, Any]) -> None:
        from orcapod.config import DEFAULT_CONFIG, OrcapodConfig
        from orcapod.contexts import resolve_saved_context
        from orcapod.types import ContentHash

        missing = [
            name
            for name in ("content_hash", "pipeline_hash", "node_uri")
            if not descriptor.get(name)
        ]
        if missing:
            raise ValueError(
                f"The saved side-effect node {descriptor.get('label')!r} has no "
                f"{', '.join(missing)}."
            )
        self.descriptor: dict[str, Any] = descriptor
        self.label: str | None = descriptor.get("label")
        self.content_hash: ContentHash = ContentHash.from_string(
            descriptor["content_hash"]
        )
        self.pipeline_hash: ContentHash = ContentHash.from_string(
            descriptor["pipeline_hash"]
        )
        self.node_uri: tuple[str, ...] = tuple(descriptor["node_uri"])
        self.data_context: DataContext = resolve_saved_context(
            descriptor.get("data_context_key")
        )
        config_dict = descriptor.get("config")
        self.orcapod_config: OrcapodConfig = (
            OrcapodConfig.from_dict(config_dict)
            if config_dict is not None
            else DEFAULT_CONFIG
        )
        self._output_schema: dict[str, Any] = descriptor.get("output_schema") or {}

    def output_schema(self) -> tuple[Schema, Schema]:
        """The saved output schema; every column ``T`` when nullability is unrecorded.

        Raises:
            ValueError: If a column's saved type cannot be restored to a Python type.
        """
        from orcapod.pipeline.serialization import deserialize_output_schema

        return deserialize_output_schema(
            self._output_schema, self.data_context.type_converter
        )

    def keys(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """The saved tag and data column names."""
        return (
            tuple(self._output_schema.get("tag", {})),
            tuple(self._output_schema.get("data", {})),
        )


def cannot_deliver_error(label: str | None) -> RuntimeError:
    """Return the error for asking a loaded side-effect node to deliver."""
    return RuntimeError(
        f"Side-effect node {label!r} was loaded from a saved file, which holds no "
        "side-effect function, so it cannot deliver. Build the job from its code to "
        "run its side effects."
    )


class LoadedSideEffectNode(SideEffectNode):
    """A side-effect node of a loaded pipeline blueprint: no pod, never delivers.

    Built by ``Pipeline.load``. Its identity, schema and node uri are the saved ones.
    A blueprint side-effect node delivers when it is read, so every read raises.

    Args:
        descriptor: The node's entry in the saved file.
        input_stream: The restored node upstream of it.

    Raises:
        ValueError: As for ``SavedSideEffectNode``.
    """

    def __init__(
        self, descriptor: dict[str, Any], input_stream: StreamProtocol
    ) -> None:
        saved = SavedSideEffectNode(descriptor)
        super().__init__(
            # A loaded node has no pod: every method that would use it is overridden.
            side_effect_pod=None,  # pyright: ignore[reportArgumentType]
            input_stream=input_stream,
            label=saved.label,
        )
        self._saved: SavedSideEffectNode = saved
        self._descriptor: dict[str, Any] = descriptor
        self._data_context = saved.data_context
        self._orcapod_config = saved.orcapod_config

    @property
    @override
    def producer(self) -> SideEffectPod | None:
        """``None``: a loaded node has no pod."""
        return None

    @override
    def identity_structure(self) -> Any:
        """The saved content hash."""
        return self._saved.content_hash

    @override
    def pipeline_identity_structure(self) -> Any:
        """The saved pipeline hash."""
        return self._saved.pipeline_hash

    @override
    def content_hash(self, hasher: Any = None) -> ContentHash:
        """The saved content hash."""
        return self._saved.content_hash

    @override
    def pipeline_hash(self, hasher: Any = None) -> ContentHash:
        """The saved pipeline hash."""
        return self._saved.pipeline_hash

    @override
    def computed_label(self) -> str | None:
        return None

    @override
    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """The saved output schema."""
        return self._saved.output_schema()

    @override
    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """The saved column names."""
        return self._saved.keys()

    @override
    def _rows_data_context(self) -> DataContext:
        return self._saved.data_context

    @property
    @override
    def node_uri(self) -> tuple[str, ...]:
        """The saved node uri."""
        return self._saved.node_uri

    @override
    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Raise: a blueprint node delivers when read, and this one cannot deliver."""
        raise cannot_deliver_error(self.label)


class LoadedSideEffectJobNode(SideEffectJobNode):
    """A side-effect node of a loaded job: no pod, never delivers.

    Built by ``PipelineJob.load``. Its identity, schema and node uri are the saved
    ones; the pipeline hash names the saved run's invocation log, which the node never
    reads or writes. Its reads are those of every ``SideEffectJobNode``: the rows its
    last run in this process passed on, so none. ``execute``, ``async_execute`` and
    ``run()`` raise before anything is read, delivered or logged.

    Args:
        descriptor: The node's entry in the saved file.
        input_stream: The restored job node upstream of it.

    Raises:
        ValueError: As for ``SavedSideEffectNode``.
    """

    def __init__(
        self, descriptor: dict[str, Any], input_stream: StreamProtocol
    ) -> None:
        saved = SavedSideEffectNode(descriptor)
        super().__init__(
            # A loaded node has no pod: every method that would use it is overridden.
            side_effect_pod=None,  # pyright: ignore[reportArgumentType]
            input_stream=input_stream,
            label=saved.label,
        )
        self._saved: SavedSideEffectNode = saved
        self._descriptor: dict[str, Any] = descriptor
        self._data_context = saved.data_context
        self._orcapod_config = saved.orcapod_config

    @property
    @override
    def producer(self) -> SideEffectPod | None:
        """``None``: a loaded node has no pod."""
        return None

    @override
    def identity_structure(self) -> Any:
        """The saved content hash."""
        return self._saved.content_hash

    @override
    def pipeline_identity_structure(self) -> Any:
        """The saved pipeline hash."""
        return self._saved.pipeline_hash

    @override
    def content_hash(self, hasher: Any = None) -> ContentHash:
        """The saved content hash."""
        return self._saved.content_hash

    @override
    def pipeline_hash(self, hasher: Any = None) -> ContentHash:
        """The saved pipeline hash: it names the saved run's invocation log."""
        return self._saved.pipeline_hash

    @override
    def computed_label(self) -> str | None:
        return None

    @override
    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """The saved output schema."""
        return self._saved.output_schema()

    @override
    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """The saved column names."""
        return self._saved.keys()

    @override
    def _rows_data_context(self) -> DataContext:
        return self._saved.data_context

    @property
    @override
    def node_uri(self) -> tuple[str, ...]:
        """The saved node uri."""
        return self._saved.node_uri

    @override
    def execute(
        self,
        input_stream: StreamProtocol,
        *,
        observer: ExecutionObserverProtocol | None = None,
        run_id: str | None = None,
    ) -> list[tuple[TagProtocol, DataProtocol]]:
        """Raise: the node has no side-effect function."""
        raise cannot_deliver_error(self.label)

    @override
    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        observer: ExecutionObserverProtocol | None = None,
        run_id: str | None = None,
    ) -> None:
        """Close ``output`` and raise: the node has no side-effect function."""
        try:
            raise cannot_deliver_error(self.label)
        finally:
            await output.close()

    @override
    def run(self) -> StreamView:
        """Raise: the node has no side-effect function."""
        raise cannot_deliver_error(self.label)
