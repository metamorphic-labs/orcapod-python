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

    The message names the pod that got the repeated operand, set once by the pod whose
    own operands repeat (``named``) and never replaced. A pod further down, or a function
    pod whose join got the operands, can only add context (``add_context``): what the
    repeated operand is part of.

    Args:
        reason: What is repeated and what to do instead.
        pod: The operator or function that got the repeated operand, or ``None`` until
            that pod names itself.
    """

    def __init__(self, reason: str, pod: str | None = None) -> None:
        self.reason: str = reason
        self.pod: str | None = pod
        self.context: list[str] = []
        super().__init__(self._message())

    def _message(self) -> str:
        message = f"{self.pod}: {self.reason}" if self.pod else self.reason
        if self.context:
            message += f" (in {'; '.join(self.context)})"
        return message

    def named(self, pod: str) -> RepeatedOperandError:
        """This error, naming ``pod`` as the one that got the repeated operand, unless
        a pod is already named (the error then comes from upstream and is returned as
        it is)."""
        if self.pod is not None:
            return self
        named = RepeatedOperandError(self.reason, pod=pod)
        named.context = list(self.context)
        named.args = (named._message(),)
        return named

    def add_context(self, context: str) -> None:
        """Say what the repeated operand is part of (``"the inputs of ..."``); the named
        pod is kept."""
        self.context.append(context)
        self.args = (self._message(),)


def operand_set(streams: Collection[StreamProtocol]) -> frozenset[StreamProtocol]:
    """The operands of a commutative pod as the set its identity is formed from.

    Args:
        streams: The operands.

    Returns:
        The operands as a ``frozenset``.

    Raises:
        RepeatedOperandError: If two operands have one identity (they are equal, as a
            set compares them), one object or two, unnamed: the caller names its pod
            (``err.named(...)``). An error met while hashing an operand (raised by a
            pod upstream) is already named and passes through.
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
