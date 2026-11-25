"""Uniqueness certificates: the tag sets no two rows of a stream agree on (ITL-1464).

A *unique key* of a stream is a set of its tags such that no two of its rows agree on all
of them, under the joins' equality of values (NaN equals NaN, ``-0.0`` equals ``0.0``). A
stream's certificate is a set of unique keys (``StreamProtocol.unique_keys``); the empty
set certifies nothing. A join uses it to let a row go once it has matched, so a wrong
certificate would lose rows: every rule here certifies only what is certain.

A source certifies its key columns when it checks them on every read, and they are all
tags. Every other stream derives its certificate from its inputs' by the rule its pod's
class declares, at the time it is asked: nothing is stored on a stream, and nothing here
enters a hash, an identity, a config, a stored row or equality. A rule is read from the
pod's exact class: a subclass, which may change the rows, certifies nothing unless it
declares a rule itself.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from contextvars import ContextVar
from typing import Protocol

UniqueKeys = frozenset[frozenset[str]]

NO_UNIQUE_KEYS: UniqueKeys = frozenset()

MAX_UNIQUE_KEYS = 8
"""How many unique keys a certificate keeps (a join's keys combine pairwise)."""

_MEMO: ContextVar[dict[int, tuple[object, UniqueKeys]] | None] = ContextVar(
    "_UNIQUE_KEYS_MEMO", default=None
)
"""The certificates derived during one outermost call: a graph of diamonds reaches one
stream by many ways."""


def certificate(keys: Iterable[Iterable[str]]) -> UniqueKeys:
    """``keys`` as a certificate: no key that holds another one, at most a few.

    Args:
        keys: Unique keys.
    """
    sets = sorted({frozenset(key) for key in keys}, key=lambda k: (len(k), sorted(k)))
    kept: list[frozenset[str]] = []
    for key in sets:
        if not any(other <= key for other in kept):
            kept.append(key)
    return frozenset(kept[:MAX_UNIQUE_KEYS])


def unique_on(keys: UniqueKeys, columns: Iterable[str]) -> bool:
    """Whether a stream certified with ``keys`` has no two rows agreeing on ``columns``."""
    names = frozenset(columns)
    return any(key <= names for key in keys)


def renamed_keys(
    keys: UniqueKeys, renames: Mapping[str, str], dropped: Iterable[str] = ()
) -> UniqueKeys:
    """``keys`` after tags are renamed, all at once, and some dropped.

    Args:
        keys: Unique keys.
        renames: The new name of each renamed tag.
        dropped: The tags that are gone; a name in it may also be another tag's new one.
    """
    gone = frozenset(dropped)
    return certificate(
        {renames.get(name, name) for name in key} for key in keys if not key & gone
    )


def joined_keys(
    left: UniqueKeys, right: UniqueKeys, shared: Iterable[str]
) -> UniqueKeys:
    """The certificate of an inner join on ``shared`` tags.

    A joined row is named by a key of each side. When one side is unique on the shared
    tags, a row of the other side is in at most one joined row, so that side's keys stay.

    Args:
        left: The left side's unique keys.
        right: The right side's unique keys.
        shared: The tags matched on.
    """
    names = frozenset(shared)
    keys: set[frozenset[str]] = {a | b for a in left for b in right}
    if unique_on(right, names):
        keys |= left
    if unique_on(left, names):
        keys |= right
    return certificate(keys)


class _HasKeys(Protocol):
    """What the certificate needs of the stream it is of."""

    def keys(self) -> tuple[tuple[str, ...], tuple[str, ...]]: ...


def derived_keys(
    pod: object, upstreams: Sequence[object], stream: _HasKeys
) -> UniqueKeys:
    """The certificate of ``stream``, the result of ``pod`` over ``upstreams``.

    The pod's class declares its rule as a method ``derive_unique_keys(*upstreams)``;
    without one on the exact class, nothing is certified.

    Args:
        pod: The producing pod.
        upstreams: Its inputs.
        stream: Its result, whose tags the keys must be among.
    """
    rule = vars(type(pod)).get("derive_unique_keys")
    if rule is None:
        return NO_UNIQUE_KEYS
    memo = _MEMO.get()
    token = None
    if memo is None:
        memo = {}
        token = _MEMO.set(memo)
    try:
        found = memo.get(id(stream))
        if found is None:
            tags = frozenset(stream.keys()[0])
            keys = certificate(k for k in rule(pod, *upstreams) if k <= tags)
            # The stream is kept beside its keys: an id is its own only while it lives.
            found = memo[id(stream)] = (stream, keys)
        return found[1]
    finally:
        if token is not None:
            _MEMO.reset(token)
