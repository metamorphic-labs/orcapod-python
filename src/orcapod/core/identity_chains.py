"""Compute structure, source and name chains for a stream or invocation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from itertools import groupby
from typing import TYPE_CHECKING, cast, final

from orcapod.errors import UnboundSourceError
from orcapod.protocols.hashing_protocols import IdentityChains
from orcapod.types import ContentHash

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import ArgumentGroup, StreamProtocol
    from orcapod.protocols.hashing_protocols import (
        IdentityChainProtocol,
        IdentityWalkProtocol,
        SemanticHasherProtocol,
    )

SLOT_NAME_HASH_LABEL = "slot_name_v1"
SOURCE_CHAIN_HASH_LABEL = "source_chain_v1"
NAME_CHAIN_HASH_LABEL = "name_chain_v1"
CONTENT_HASH_LABEL = "content_hash_v1"
NODE_HASH_LABEL = "node_hash_v1"


def _hash(label: str, preimage: tuple[object, ...]) -> ContentHash:
    """Hash canonical semantic tuples under the label of the chain algorithm."""
    from orcapod.hashing.semantic_hashing.semantic_hasher import (
        SemanticAwarePythonHasher,
    )

    return SemanticAwarePythonHasher(hasher_id=label).hash_object(preimage)


@final
class IdentityWalk:
    """Memoize by object for one walk, so a diamond visits its shared inputs once."""

    def __init__(self, hasher: SemanticHasherProtocol) -> None:
        self.hasher = hasher
        self.unbound_slots: set[str] = set()
        self._memo: dict[int, IdentityChains] = {}

    def visit(self, element: IdentityChainProtocol) -> IdentityChains:
        """Compute the element's chains or reuse them from this walk."""
        key = id(element)
        if key not in self._memo:
            self._memo[key] = element._identity_chains(self)  # pyright: ignore[reportPrivateUsage]  # Internal back edge of the identity walk.
        return self._memo[key]


@final
class _InputOccurrence:
    """One position in a pod's input list, with access to the original stream.

    Identity-based equality keeps repetitions even inside a frozenset.
    """

    def __init__(self, stream: StreamProtocol, chains: IdentityChains) -> None:
        self._stream = stream
        self.chains = chains

    def __getattr__(self, name: str) -> object:
        return getattr(self._stream, name)


@dataclass(frozen=True, slots=True)
class _GroupChains:
    """Chains and a structural sort key for an input or a nested symmetry group."""

    structure_key: tuple[str, ...]
    sources: ContentHash | None
    names: ContentHash


def _pool(
    label: str, inputs: tuple[ContentHash, ...], own_term: object | None = None
) -> ContentHash:
    """Pass a lone input through, or hash the pool and any node contribution."""
    pooled = inputs[0] if len(inputs) == 1 else _hash(label, inputs)
    if own_term is None:
        return pooled
    return _hash(label, ("term", pooled, own_term))


def _pool_group(
    group: object,
    source_term: object | None = None,
    name_term: object | None = None,
) -> _GroupChains:
    """Pool in structural order, breaking structural ties separately per chain."""
    if isinstance(group, _InputOccurrence):
        chains = group.chains
        return _GroupChains(
            (chains.structure.to_hex(),),
            chains.sources,
            chains.names,
        )
    if not isinstance(group, (tuple, frozenset)):
        raise TypeError(
            "argument_symmetry must group the input occurrences it receives"
        )

    children = [
        _pool_group(child)
        for child in cast("tuple[object, ...] | frozenset[object]", group)
    ]
    symmetric = isinstance(group, frozenset)
    if symmetric:
        children.sort(key=lambda child: child.structure_key)
    structure_key = (
        "{" if symmetric else "(",
        *(part for child in children for part in child.structure_key),
        "}" if symmetric else ")",
    )

    names: list[ContentHash] = []
    sources: list[ContentHash] = []
    for _, group_members in groupby(children, key=lambda child: child.structure_key):
        members = list(group_members)
        group_names = [child.names for child in members]
        group_sources = [
            child.sources for child in members if child.sources is not None
        ]
        if symmetric and len(members) > 1:
            group_names.sort(key=ContentHash.to_string)
            group_sources.sort(key=ContentHash.to_string)
        names.extend(group_names)
        sources.extend(group_sources)
    return _GroupChains(
        structure_key,
        _pool(SOURCE_CHAIN_HASH_LABEL, tuple(sources), source_term)
        if len(sources) == len(children)
        else None,
        _pool(NAME_CHAIN_HASH_LABEL, tuple(names), name_term),
    )


class IdentityChainsMixin(ABC):
    """Compute the new accessors without caching a mutable binding or slot name."""

    @abstractmethod
    def pipeline_hash(
        self, hasher: SemanticHasherProtocol | None = None
    ) -> ContentHash:
        """Return the existing structure hash."""

    @property
    @abstractmethod
    def _identity_hasher(self) -> SemanticHasherProtocol:
        """Hasher used for the existing pipeline hash."""

    def _identity_inputs(self) -> tuple[StreamProtocol, ...]:
        """Inputs in argument order, including repeated streams."""
        return ()

    def _identity_symmetry(self, inputs: tuple[StreamProtocol, ...]) -> ArgumentGroup:
        """Group inputs according to the producer's argument symmetry."""
        return inputs

    def _identity_leaf(self) -> tuple[str, ContentHash | None] | None:
        """Return a leaf's slot name and binding identity, if this is a leaf."""
        return None

    def _identity_terms(self) -> tuple[object | None, object | None]:
        """Return any terms this element contributes to the source and name chains."""
        return None, None

    def _identity_chains(self, walk: IdentityWalkProtocol) -> IdentityChains:
        leaf = self._identity_leaf()
        if leaf is not None:
            name, source = leaf
            if source is None:
                walk.unbound_slots.add(name)
            return IdentityChains(
                self.pipeline_hash(walk.hasher),
                source,
                _hash(SLOT_NAME_HASH_LABEL, ("slot", name)),
            )

        inputs = self._identity_inputs()
        if len(inputs) == 1:
            upstream = walk.visit(inputs[0])
            source_term, name_term = self._identity_terms()
            return IdentityChains(
                self.pipeline_hash(walk.hasher),
                _pool(SOURCE_CHAIN_HASH_LABEL, (upstream.sources,), source_term)
                if upstream.sources is not None
                else None,
                _pool(NAME_CHAIN_HASH_LABEL, (upstream.names,), name_term),
            )

        # Feed occurrences to the same symmetry rule as the legacy pipeline hash.
        # The latter still operates on stream objects in step 1.
        occurrences = tuple(
            cast(
                "StreamProtocol",
                cast(object, _InputOccurrence(stream, walk.visit(stream))),
            )
            for stream in inputs
        )
        group = self._identity_symmetry(occurrences) if occurrences else ()
        pooled = _pool_group(group, *self._identity_terms())
        return IdentityChains(
            self.pipeline_hash(walk.hasher), pooled.sources, pooled.names
        )

    def source_chain(self) -> ContentHash | None:
        """Return the source chain, absent if any contributing slot is unbound."""
        return IdentityWalk(self._identity_hasher).visit(self).sources

    def name_chain(self) -> ContentHash:
        """Return the chain of source-slot names."""
        return IdentityWalk(self._identity_hasher).visit(self).names

    def _chain_content_hash(self) -> ContentHash:
        """Combine structure and sources. This becomes ``content_hash()`` at the switch.

        Raises:
            UnboundSourceError: If any contributing source slot is unbound.
        """
        walk = IdentityWalk(self._identity_hasher)
        chains = walk.visit(self)
        if chains.sources is None:
            names = ", ".join(repr(name) for name in sorted(walk.unbound_slots))
            raise UnboundSourceError(
                f"Content identity is undefined for unbound source slots {names}."
            )
        return _hash(CONTENT_HASH_LABEL, (chains.structure, chains.sources))

    def node_hash(self) -> ContentHash:
        """Combine structure and slot names, independently of binding."""
        chains = IdentityWalk(self._identity_hasher).visit(self)
        return _hash(NODE_HASH_LABEL, (chains.structure, chains.names))
