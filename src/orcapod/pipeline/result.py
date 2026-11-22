"""Result type returned by pipeline orchestrators."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import StreamProtocol


@dataclass
class OrchestratorResult:
    """Result of an orchestrator run.

    Attributes:
        node_outputs: Mapping from graph node to its output stream (backed by an
            Arrow table; ``as_table()`` gives it, ``iter_data()`` its rows). Empty when
            ``materialize_results=False``.
    """

    node_outputs: dict[Any, "StreamProtocol"] = field(default_factory=dict)
