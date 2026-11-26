"""A commutative pod is never given the same stream twice (ITL-1419).

A pod whose ``argument_symmetry`` is a ``frozenset`` (``Join``, ``Rejoin``, a function pod,
which joins its inputs) hashes its operands as a set: ``Join()(d)`` and ``Join()(d, d)``
would have one identity, one stored path and one cache entry, yet give different rows
(a stream joined with itself repeats every row whose key repeats). So the same stream
twice is refused before anything is recorded, standalone (the same object) and when a
pipeline records the step (also two streams of one content hash) (decided on the
developer's behalf, November 26, 2026). An ordered operator (``MergeJoin``, ``SemiJoin``) takes one
stream twice: its positions are part of its identity.
"""

from __future__ import annotations

from collections.abc import Hashable, Sequence
from typing import TYPE_CHECKING, Protocol

from orcapod.errors import InputValidationError

if TYPE_CHECKING:
    from collections.abc import Collection

    from orcapod.protocols.core_protocols import StreamProtocol


class _SymmetryDeclaring(Protocol):
    """A pod, as far as this rule reads it: its argument symmetry."""

    def argument_symmetry(self, streams: Collection[StreamProtocol]) -> object: ...


def refuse_repeated_operands(
    pod: _SymmetryDeclaring,
    streams: Sequence[StreamProtocol],
    keys: Sequence[Hashable] | None = None,
) -> None:
    """Refuse the same stream given twice to a commutative pod.

    Two operands are the same stream when they have one key. Without ``keys`` the key
    is the object itself: a pod called on its own compares its operands by identity,
    since it computes no content hash for them (hashing a raw stream reads its rows, and
    not every value type can be hashed). A pipeline recording the step passes the
    operands' content hashes, which it computes anyway: there two streams of one
    identity would make one step of two, so they are the same stream too.

    Args:
        pod: The pod the streams are given to.
        streams: Its operands, in argument order.
        keys: One key per operand, in argument order; the objects' identity when
            ``None``.

    Raises:
        InputValidationError: If ``pod`` is commutative (its ``argument_symmetry`` is a
            ``frozenset``) and two of ``streams`` have one key. The message names the
            pod and the stream, and ``MergeJoin`` for a self-join.
    """
    seen: set[Hashable] = set()
    repeated: StreamProtocol | None = None
    for stream, key in zip(
        streams, keys if keys is not None else [id(s) for s in streams], strict=True
    ):
        if key in seen:
            repeated = stream
            break
        seen.add(key)
    # The symmetry is asked only for a repeat: a commutative operator's
    # ``argument_symmetry`` builds a frozenset of its streams, which hashes them.
    if repeated is None or not isinstance(pod.argument_symmetry(streams), frozenset):
        return
    raise InputValidationError(
        f"{type(pod).__name__} is given the same stream {repeated.label!r} more than "
        "once. It treats its inputs as a set, so a repeated input would change its rows "
        "but not its identity. Give each stream once; to join a stream with itself, use "
        "MergeJoin, whose inputs are ordered: MergeJoin()(stream, stream)."
    )
