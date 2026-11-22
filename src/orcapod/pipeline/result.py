"""Result type returned by pipeline orchestrators."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import StreamProtocol


class StreamsOnRead(Mapping[Any, "StreamProtocol"]):
    """Each node's output stream, built from its rows when it is read.

    The async orchestrator collects each node's rows as they are sent; a node's stream
    is built from them (``build``) when it is read. A run whose rows cannot make one
    table (rows of a store written before ITL-928) therefore finishes, as it does on
    ``main``, and only reading that node's output raises, as reading its rows as a
    table does (ITL-1360). Membership, length and iteration never build a stream. A run
    plan's held streams are built the same way, when the sync orchestrator reaches the
    node, and are not kept (``keep=False``): the orchestrator drops each one once its
    successors have run.
    """

    def __init__(
        self,
        rows: Mapping[Any, Any],
        build: Callable[[Any, Any], "StreamProtocol"],
        *,
        keep: bool = True,
    ) -> None:
        """Hold each node's rows and how a stream is built from them.

        Args:
            rows: Each node's rows.
            build: Builds a node's stream from its rows, given ``(rows, node)``.
            keep: Keep a stream once built, so a second read returns it again.
        """
        self._rows = dict(rows)
        self._build = build
        self._keep = keep
        self._built: dict[Any, StreamProtocol] = {}

    def __getitem__(self, node: Any) -> "StreamProtocol":
        if node in self._built:
            return self._built[node]
        stream = self._build(self._rows[node], node)
        if self._keep:
            self._built[node] = stream
        return stream

    def __contains__(self, node: object) -> bool:
        return node in self._rows

    def __iter__(self) -> Iterator[Any]:
        return iter(self._rows)

    def __len__(self) -> int:
        return len(self._rows)


@dataclass
class OrchestratorResult:
    """Result of an orchestrator run.

    Attributes:
        node_outputs: Mapping from graph node to its output stream (backed by an
            Arrow table; ``as_table()`` gives it, ``iter_data()`` its rows). Empty when
            ``materialize_results=False``. The async orchestrator builds each stream
            when it is first read (``StreamsOnRead``).
    """

    node_outputs: Mapping[Any, "StreamProtocol"] = field(default_factory=dict)
