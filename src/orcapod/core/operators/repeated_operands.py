"""Validate an operand group when a pod's argument symmetry is read (ITL-1419).

The developer approved refusing collapsed operands as a stopgap until ITL-1511 adds
multiset identity and the unique/repeat toggle. Hashing and job recording read the
group. A standalone row read can skip identity formation. The group is built first
so hashing errors propagate before the repeated-operand check.
"""

from __future__ import annotations

from collections.abc import Collection
from typing import Protocol

from typing_extensions import override

from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import ArgumentGroup, StreamProtocol


class SymmetryDeclaring(Protocol):
    """A pod that can describe the grouping of its inputs."""

    def argument_symmetry(
        self, streams: Collection[StreamProtocol]
    ) -> ArgumentGroup: ...


class RepeatedOperandError(InputValidationError):
    """The operand group for a pod's identity lost a repeated operand.

    The developer approved this stopgap on 2026-11-27 until ITL-1511 adds a unique/repeat
    toggle. A job forms the identity and raises this error, while a standalone row read
    can skip identity formation. The message names the pod whose own operands repeat.
    Downstream pods can attach context with ``add_context``.

    Args:
        reason: What is repeated and what to do instead.
        pod: The operator or function that got the repeated operand.
    """

    def __init__(self, reason: str, pod: str) -> None:
        self.reason: str = reason
        self.pod: str = pod
        self.context: list[str] = []
        super().__init__(self._message())

    def _message(self) -> str:
        message = f"{self.pod}: {self.reason}"
        if self.context:
            message += f" (in {'; '.join(self.context)})"
        return message

    def add_context(self, context: str) -> None:
        """Append context while keeping the name of the pod whose input repeated."""
        self.context.append(context)
        self.args = (self._message(),)

    @override
    def __reduce__(self) -> tuple[object, ...]:
        """Rebuild from the constructor fields and keep the contextual message."""
        return (type(self), (self.reason, self.pod), {**vars(self), "args": self.args})


def read_argument_symmetry(
    pod: SymmetryDeclaring, streams: Collection[StreamProtocol]
) -> ArgumentGroup:
    """Read a pod's operand group and check for collapse in a flat unordered group.

    Args:
        pod: The pod declaring the operand group.
        streams: The original operands.

    Returns:
        The operand group as declared by the pod.

    Raises:
        RepeatedOperandError: If the flat set of inputs loses an operand (ITL-1511).
    """
    group = pod.argument_symmetry(streams)
    if not isinstance(group, frozenset) or len(group) >= len(streams):
        return group
    # A flat set takes all inputs. A nested set does not tell us which inputs went
    # into each group before set formation (ITL-1548).
    if any(isinstance(member, (tuple, frozenset)) for member in group):
        return group

    seen: dict[StreamProtocol, StreamProtocol] = {}
    repeated_labels: set[str] = set()
    for stream in streams:
        first = seen.get(stream)
        if first is None:
            seen[stream] = stream
        else:
            repeated_labels.update((first.label, stream.label))
    names = " and ".join(repr(label) for label in sorted(repeated_labels))
    detail = f" ({names}, one identity)" if names else ""
    raise RepeatedOperandError(
        f"the same stream{detail} is given more than once, which is not supported yet "
        "(ITL-1511 is to allow it). Today its inputs form a set, so a repeated input "
        "would change the rows but not the identity. For now give each stream once; "
        "to join a stream with itself in argument order, use MergeJoin, whose inputs "
        "are ordered: MergeJoin()(stream, stream).",
        pod=type(pod).__name__,
    )


def is_flat_commutative(
    group: ArgumentGroup, streams: Collection[StreamProtocol]
) -> bool:
    """Return whether the group is a set of the input streams themselves."""
    return isinstance(group, frozenset) and {id(member) for member in group} == {
        id(stream) for stream in streams
    }
