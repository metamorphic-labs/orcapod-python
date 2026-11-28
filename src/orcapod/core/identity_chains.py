"""Compute structure, source and name chains for a stream or invocation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from functools import cached_property
from itertools import groupby
from typing import TYPE_CHECKING, cast, final
from weakref import WeakValueDictionary

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
    """Walk uncached inputs and connect their caches to their dependents."""

    def __init__(self, hasher: SemanticHasherProtocol) -> None:
        self.hasher = hasher
        self._consumer: IdentityChainsMixin | None = None

    def visit(self, element: IdentityChainProtocol) -> IdentityChains:
        """Reuse the node's cached chains, recording dependencies through wrappers."""
        if not isinstance(element, IdentityChainsMixin):
            return element._identity_chains(self)  # pyright: ignore[reportPrivateUsage]  # Transparent stream wrapper.

        consumer = self._consumer
        if consumer is not None:
            element._identity_dependents[id(consumer)] = consumer
            consumer._identity_dependencies[id(element)] = element
        self._consumer = element
        try:
            return element._identity_chains(self)  # pyright: ignore[reportPrivateUsage]  # Internal back edge of the identity walk.
        finally:
            self._consumer = consumer


@final
class _InputOccurrence:
    """One position in a pod's input list, with access to the original stream.

    Identity-based equality keeps repetitions even inside a frozenset.
    """

    def __init__(self, stream: StreamProtocol, index: int) -> None:
        self._stream = stream
        self.index = index

    def __getattr__(self, name: str) -> object:
        return getattr(self._stream, name)


# JSON needs one explicit marker to distinguish symmetric groups from lists.
INPUT_SYMMETRY_KEY = "input_symmetry"
InputSymmetry = int | list["InputSymmetry"] | dict[str, list["InputSymmetry"]]


def encode_input_symmetry(
    inputs: tuple[StreamProtocol, ...],
    symmetry: Callable[[tuple[StreamProtocol, ...]], ArgumentGroup],
) -> InputSymmetry | None:
    """Encode a nondefault grouping by argument position, preserving repetitions."""

    def encode(group: object) -> InputSymmetry:
        if isinstance(group, _InputOccurrence):
            return group.index
        if isinstance(group, tuple):
            return [encode(child) for child in cast("tuple[object, ...]", group)]
        if isinstance(group, frozenset):
            children = [encode(child) for child in cast("frozenset[object]", group)]
            return {"symmetric": sorted(children, key=repr)}
        raise TypeError(
            "argument_symmetry must group the input occurrences it receives"
        )

    if len(inputs) < 2:
        return None
    grouped = encode(symmetry(_occurrences(inputs)))
    return None if grouped == list(range(len(inputs))) else grouped


def decode_input_symmetry(
    group: InputSymmetry, inputs: tuple[StreamProtocol, ...]
) -> ArgumentGroup:
    """Apply the saved grouping to the current input occurrences."""
    if isinstance(group, int):
        return inputs[group]
    if isinstance(group, list):
        return tuple(decode_input_symmetry(child, inputs) for child in group)
    return frozenset(
        decode_input_symmetry(child, inputs) for child in group["symmetric"]
    )


def _occurrences(inputs: tuple[StreamProtocol, ...]) -> tuple[StreamProtocol, ...]:
    return tuple(
        cast("StreamProtocol", cast(object, _InputOccurrence(stream, index)))
        for index, stream in enumerate(inputs)
    )


@dataclass(frozen=True, slots=True)
class _GroupChains:
    """Chains and a structural sort key for an input or a nested symmetry group."""

    structure_key: tuple[str, ...]
    sources: ContentHash | None
    names: ContentHash


def _pool(
    hash_term: Callable[[str, tuple[object, ...]], ContentHash],
    label: str,
    inputs: tuple[ContentHash, ...],
    own_term: object | None = None,
) -> ContentHash:
    """Pass a lone input through, or hash the pool and any node contribution."""
    pooled = inputs[0] if len(inputs) == 1 else hash_term(label, inputs)
    if own_term is None:
        return pooled
    return hash_term(label, ("term", pooled, own_term))


def _pool_group(
    group: object,
    inputs: tuple[_GroupChains, ...],
    hash_term: Callable[[str, tuple[object, ...]], ContentHash],
    source_term: object | None = None,
    name_term: object | None = None,
) -> _GroupChains:
    """Pool in structural order, breaking structural ties separately per chain."""
    if isinstance(group, _InputOccurrence):
        return inputs[group.index]
    if not isinstance(group, (tuple, frozenset)):
        raise TypeError(
            "argument_symmetry must group the input occurrences it receives"
        )

    children = [
        _pool_group(child, inputs, hash_term)
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
        _pool(hash_term, SOURCE_CHAIN_HASH_LABEL, tuple(sources), source_term)
        if len(sources) == len(children)
        else None,
        _pool(hash_term, NAME_CHAIN_HASH_LABEL, tuple(names), name_term),
    )


class IdentityChainsMixin(ABC):
    """Memoize chains per node and invalidate descendants of a changed slot."""

    # Defaults cover loaded stubs constructed without __init__.
    _identity_cache: dict[str, IdentityChains] | None = None
    _identity_name_hashes: dict[tuple[str, tuple[object, ...]], ContentHash] | None = (
        None
    )
    _identity_source_hashes: (
        dict[tuple[str, tuple[object, ...]], ContentHash] | None
    ) = None

    @cached_property
    def _identity_dependents(self) -> WeakValueDictionary[int, IdentityChainsMixin]:
        """Weak inverse edges of the computed identity graph."""
        return WeakValueDictionary()

    @cached_property
    def _identity_dependencies(self) -> WeakValueDictionary[int, IdentityChainsMixin]:
        """Inputs used by the cached chains, including a slot's current binding."""
        return WeakValueDictionary()

    def _invalidate_identity_chains(self, *, names: bool = False) -> None:
        """Invalidate cached chains below this node, keeping name hashes on bind."""
        pending: list[IdentityChainsMixin] = [self]
        seen: set[int] = set()
        while pending:
            node = pending.pop()
            key = id(node)
            if key in seen:
                continue
            seen.add(key)
            node._identity_cache = None
            node._identity_source_hashes = None
            if names:
                node._identity_name_hashes = None
            pending.extend(node._identity_dependents.values())
            for dependency in list(node._identity_dependencies.values()):
                dependency._identity_dependents.pop(key, None)
            node._identity_dependencies.clear()

    def _identity_hash(self, label: str, preimage: tuple[object, ...]) -> ContentHash:
        """Reuse canonical hash terms across chain reads and unchanged name inputs."""
        if label in (SOURCE_CHAIN_HASH_LABEL, CONTENT_HASH_LABEL):
            if self._identity_source_hashes is None:
                self._identity_source_hashes = {}
            hashes = self._identity_source_hashes
        else:
            if self._identity_name_hashes is None:
                self._identity_name_hashes = {}
            hashes = self._identity_name_hashes
        key = (label, preimage)
        if key not in hashes:
            hashes[key] = _hash(label, preimage)
        return hashes[key]

    def _identity_pipeline_hash(self, hasher: SemanticHasherProtocol) -> ContentHash:
        """Use the legacy structure, evaluated only when a consumer needs it."""
        return self.pipeline_hash(hasher)

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

    def _identity_leaf(
        self, walk: IdentityWalkProtocol
    ) -> tuple[str | None, ContentHash | None] | None:
        """Return a leaf's slot name and binding identity, if this is a leaf."""
        return None

    def _identity_terms(self) -> tuple[object | None, object | None]:
        """Return any terms this element contributes to the source and name chains."""
        return None, None

    def _identity_chains(self, walk: IdentityWalkProtocol) -> IdentityChains:
        if self._identity_cache is None:
            self._identity_cache = {}
        key = walk.hasher.hasher_id
        if key not in self._identity_cache:
            self._identity_cache[key] = self._compute_identity_chains(walk)
        return self._identity_cache[key]

    def _compute_identity_chains(self, walk: IdentityWalkProtocol) -> IdentityChains:
        leaf = self._identity_leaf(walk)
        if leaf is not None:
            name, source = leaf
            return IdentityChains(
                self._identity_pipeline_hash,
                source,
                self._identity_hash(SLOT_NAME_HASH_LABEL, ("slot", name)),
                frozenset((name,))
                if name is not None and source is None
                else frozenset(),
            )

        inputs = self._identity_inputs()
        source_term, name_term = self._identity_terms()
        upstreams = tuple(walk.visit(stream) for stream in inputs)
        if len(upstreams) == 1:
            upstream = upstreams[0]
            return IdentityChains(
                self._identity_pipeline_hash,
                _pool(
                    self._identity_hash,
                    SOURCE_CHAIN_HASH_LABEL,
                    (upstream.sources,),
                    source_term,
                )
                if upstream.sources is not None
                else None,
                _pool(
                    self._identity_hash,
                    NAME_CHAIN_HASH_LABEL,
                    (upstream.names,),
                    name_term,
                ),
                upstream.unbound_slots,
            )

        # Symmetric groups use structural ordering. Occurrences keep repeats.
        group = self._identity_symmetry(_occurrences(inputs)) if inputs else ()
        children = tuple(
            _GroupChains(
                (upstream.pipeline_hash(walk.hasher).to_hex(),),
                upstream.sources,
                upstream.names,
            )
            for upstream in upstreams
        )
        pooled = _pool_group(
            group, children, self._identity_hash, source_term, name_term
        )
        return IdentityChains(
            self._identity_pipeline_hash,
            pooled.sources,
            pooled.names,
            frozenset(
                name for upstream in upstreams for name in upstream.unbound_slots
            ),
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
            names = ", ".join(repr(name) for name in sorted(chains.unbound_slots))
            raise UnboundSourceError(
                f"Content identity is undefined for unbound source slots {names}."
            )
        return self._identity_hash(
            CONTENT_HASH_LABEL, (chains.pipeline_hash(walk.hasher), chains.sources)
        )

    def node_hash(self) -> ContentHash:
        """Combine structure and slot names, independently of binding."""
        walk = IdentityWalk(self._identity_hasher)
        chains = walk.visit(self)
        return self._identity_hash(
            NODE_HASH_LABEL, (chains.pipeline_hash(walk.hasher), chains.names)
        )
