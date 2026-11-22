"""Result type returned by pipeline orchestrators."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import StreamProtocol


class StreamsOnRead(Mapping[Any, "StreamProtocol"]):
    """Each node's output stream, built when it is first read.

    The async orchestrator collects each node's rows as they are sent; a node's stream
    is built from them (``build``) the first time it is read, and kept. A run whose
    rows cannot make one table (rows of a store written before ITL-928) therefore
    finishes, as it does on ``main``, and only reading that node's output raises, as
    reading its rows as a table does (ITL-1360).
    """

    def __init__(
        self,
        rows: Mapping[Any, Any],
        build: Callable[[Any, Any], "StreamProtocol"],
    ) -> None:
        """Hold each node's rows and how a stream is built from them.

        Args:
            rows: Each node's collected rows.
            build: Builds a node's stream from its rows, given ``(rows, node)``.
        """
        self._rows = dict(rows)
        self._build = build
        self._built: dict[Any, StreamProtocol] = {}

    def __getitem__(self, node: Any) -> "StreamProtocol":
        if node not in self._built:
            self._built[node] = self._build(self._rows[node], node)
        return self._built[node]

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
