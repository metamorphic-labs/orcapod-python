"""A commutative pod is never given one stream twice (ITL-1419).

A pod whose identity is formed from its operands as a set (``Join``, ``Rejoin``, a
commutative binary operator, a function pod, which joins its inputs) would give
``Join()(d)`` and ``Join()(d, d2)`` one identity, one stored path and one cache entry,
when ``d2`` is ``d`` or a separate stream of ``d``'s identity, yet different rows (a stream
joined with itself repeats every row whose key repeats). So the set is formed in one
place, ``operand_set``, which refuses two operands of one identity, the hash the set is
built from (decided on the developer's behalf, November 26, 2026). Whatever forms the
identity meets the refusal: hashing a stream standalone, recording a step in a pipeline,
compiling it. An ordered operator (``MergeJoin``, ``SemiJoin``) takes one stream twice:
its positions are part of its identity.

The rule adds no error of its own for an operand whose identity cannot be computed (a raw
stream holding a value that cannot be hashed): it raises what hashing that stream raises.
"""

from __future__ import annotations

from collections.abc import Collection

from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol


class RepeatedOperandError(InputValidationError):
    """Two operands of a commutative pod have one identity.

    Args:
        reason: What is repeated and what to do instead.
        pod: The operator or function the operands are given to, as the message names
            it, or ``None`` before a caller adds it.
    """

    def __init__(self, reason: str, pod: str | None = None) -> None:
        self.reason: str = reason
        super().__init__(f"{pod}: {reason}" if pod else reason)


def operand_set(streams: Collection[StreamProtocol]) -> frozenset[StreamProtocol]:
    """The operands of a commutative pod as the set its identity is formed from.

    Args:
        streams: The operands.

    Returns:
        The operands as a ``frozenset``.

    Raises:
        RepeatedOperandError: If two operands have one identity (they are equal, as a
            set compares them), one object or two. The caller names its pod
            (``RepeatedOperandError(err.reason, pod=...)``).
    """
    seen: dict[StreamProtocol, StreamProtocol] = {}
    for stream in streams:
        first = seen.get(stream)
        if first is not None:
            names = (
                repr(stream.label)
                if first is stream or first.label == stream.label
                else f"{first.label!r} and {stream.label!r}"
            )
            raise RepeatedOperandError(
                f"the same stream ({names}, one identity) is given more than once. Its "
                "inputs form a set, so a repeated input would change the rows but not "
                "the identity. Give each stream once; to join a stream with itself, use "
                "MergeJoin, whose inputs are ordered: MergeJoin()(stream, stream)."
            )
        seen[stream] = stream
    return frozenset(seen)
