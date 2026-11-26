"""``stream.preview(n)``: the first rows of a stream, computing only what they need (ITL-1464).

A preview reads a stream's rows as ``_preview_batches`` yields them, in the order of the
sync path, and stops at ``n``. Each kind of stream says how it gives its rows in batches
(a source reads a limit, an operator applied batch by batch takes its input's batches, a
function pod takes one row at a time); a stream that needs a whole input is refused unless
``compute_all`` was given. Nothing is kept on a stream, written to a database or
delivered.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Generator
from contextlib import closing
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from orcapod.errors import PreviewRefusedError

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import StreamProtocol

FIRST_BATCH_ROWS = 1_024
"""The rows of the first batch read when a step may drop rows; each next one doubles."""

PREVIEW_COLUMNS = {"source": True, "system_tags": True, "context": True}
"""The column groups a batch carries from step to step: every one but the hashes."""


@dataclasses.dataclass(frozen=True)
class PreviewRequest:
    """What a preview asks of a stream.

    Attributes:
        limit: The most rows that can be used, or ``None`` when that is not known (a
            step above may drop rows). A source reads no more.
        compute_all: Whether a step that needs its whole input may be computed.
        whole: Whether every row is asked for, by such a step.
        kept: Each stream read in full by such steps with its batches, by identity: a
            stream two branches share is computed once. The stream is held with its
            batches: many are temporary (a step's result), and the identity of one that
            was freed is given to another. Discarded with the preview.
    """

    limit: int | None
    compute_all: bool = False
    whole: bool = False
    kept: dict[int, tuple[Any, list[StreamProtocol]]] = dataclasses.field(
        default_factory=dict
    )

    def without_limit(self) -> PreviewRequest:
        """The request to the input of a step that may drop rows."""
        return dataclasses.replace(self, limit=None)

    def of_every_row(self) -> PreviewRequest:
        """The request to the inputs of a step that needs them whole."""
        return dataclasses.replace(self, limit=None, whole=True)


def preview_batches(
    stream: Any, request: PreviewRequest
) -> Generator[StreamProtocol, None, None]:
    """The rows of ``stream``, in order, as one stream per batch; at least one batch.

    Args:
        stream: The stream, or anything else a step read as its input.
        request: What is asked of it.

    Raises:
        PreviewRefusedError: If the stream, or one beneath it, cannot be previewed.
    """
    give = getattr(stream, "_preview_batches", None)
    if give is None:
        raise refused(stream, "has no preview")
    if not request.whole:
        with closing(give(request)) as batches:
            yield from batches
        return
    if id(stream) not in request.kept:
        with closing(give(request)) as batches:
            request.kept[id(stream)] = (stream, list(batches))
    yield from request.kept[id(stream)][1]


def slices_of(
    stream: Any, request: PreviewRequest
) -> Generator[StreamProtocol, None, None]:
    """The rows of a stream that holds them (or reads them without effect), in slices.

    Args:
        stream: A stream whose ``as_table`` computes nothing.
        request: What is asked of it.
    """
    from orcapod.core.data_tags import stream_data_tags
    from orcapod.core.streams.arrow_table_stream import ArrowTableStream

    table: pa.Table = stream.as_table(columns=PREVIEW_COLUMNS)
    size = table.num_rows if request.whole else request.limit or FIRST_BATCH_ROWS
    end = table.num_rows if request.limit is None else request.limit
    start = 0
    while True:
        yield ArrowTableStream(
            table.slice(start, min(size, end - start)),
            tag_columns=stream.keys()[0],
            data_tags=stream_data_tags(stream),
            data_context=stream.data_context_key,
        )
        start += size
        size *= 2
        if start >= min(end, table.num_rows):
            return


def exactly_its_class(obj: object, shortcut: str) -> bool:
    """Whether ``obj`` is exactly the library class its ``shortcut`` was written for.

    A preview shortcut or read-only path (an operator's ``input_given_back``, a pod's
    ``preview_data``, a data function's ``preview_call``) is written for one class of
    the library. It holds only for an object of exactly that class, with no member
    replaced on the instance (``reads_as_its_class``): a subclass, however small, can
    compute or act in a way the shortcut does not know (developer decision N55,
    review of PR 571).

    Args:
        obj: The operator, pod or data function.
        shortcut: The name of the shortcut method.

    Returns:
        Whether the shortcut may be taken for ``obj``.
    """
    from orcapod.core.streams.base import reads_as_its_class

    definer = next((c for c in type(obj).__mro__ if shortcut in vars(c)), None)
    return definer is not None and reads_as_its_class(obj, definer)


def refused(stream: Any, why: str) -> PreviewRefusedError:
    """Return the error for a stream that cannot be previewed, naming it and the remedy."""
    return PreviewRefusedError(
        f"preview() stopped at {type(stream).__name__}[{getattr(stream, 'label', '')}], "
        f"which {why}. run() computes every row."
    )


def refuse_without_compute_all(stream: Any, request: PreviewRequest, why: str) -> None:
    """Refuse a step that needs its whole input, unless ``compute_all`` was given.

    Args:
        stream: The step's stream.
        request: What is asked of it.
        why: What the step needs, after the stream's name.

    Raises:
        PreviewRefusedError: Unless ``request.compute_all``.
    """
    if not request.compute_all:
        raise PreviewRefusedError(
            f"preview() stopped at {type(stream).__name__}[{stream.label}], which {why}. "
            "preview(n, compute_all=True) computes that step in full, and keeps nothing; "
            "run() computes every row and keeps what it computes."
        )
