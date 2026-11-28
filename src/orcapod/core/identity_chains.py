"""Compute structure, source and name chains for a stream or invocation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from functools import cached_property
from itertools import groupby
from threading import RLock
from typing import TYPE_CHECKING, ClassVar, final
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


# JSON needs one explicit marker to distinguish symmetric groups from lists.
INPUT_SYMMETRY_KEY = "input_symmetry"
InputSymmetry = int | list["InputSymmetry"] | dict[str, list["InputSymmetry"]]


def encode_input_symmetry(
    inputs: tuple[StreamProtocol, ...],
    symmetry: Callable[[tuple[StreamProtocol, ...]], ArgumentGroup],
) -> InputSymmetry | None:
    """Translate a pod's grouping of real streams into argument positions."""
    if len(inputs) < 2:
        return None

    remaining = dict(enumerate(inputs))
    symmetric: list[tuple[frozenset[ArgumentGroup], list[InputSymmetry]]] = []

    def encode(group: ArgumentGroup) -> InputSymmetry:
        if isinstance(group, tuple):
            return [encode(child) for child in group]
        if isinstance(group, frozenset):
            children = [encode(child) for child in group]
            symmetric.append((group, children))
            return {"symmetric": children}
        for index, stream in remaining.items():
            if stream is group:
                del remaining[index]
                return index
        raise ValueError("argument_symmetry did not group arguments by occurrence")

    group = symmetry(inputs)
    try:
        grouped = encode(group)
    except ValueError:
        # Positions are optional metadata. A legacy descriptor can use input order
        # when a custom grouping cannot be expressed with argument positions.
        return None

    # A frozenset can collapse repeated or content-equal streams. Restore their
    # positions after claiming the explicit leaves, including leaves of ordered groups.
    symmetric.sort(
        key=lambda entry: min(_input_positions(entry[1]), default=len(inputs))
    )
    for index, stream in remaining.items():
        for members, children in symmetric:
            if stream in members:
                children.append(index)
                break

    for _, children in symmetric:
        children.sort(key=repr)
    return None if grouped == list(range(len(inputs))) else grouped


def _input_positions(group: InputSymmetry) -> tuple[int, ...]:
    """Return every explicit position in a grouping."""
    if isinstance(group, int):
        return (group,)
    children = group if isinstance(group, list) else group["symmetric"]
    return tuple(index for child in children for index in _input_positions(child))


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
    group: InputSymmetry,
    inputs: tuple[_GroupChains, ...],
    hash_term: Callable[[str, tuple[object, ...]], ContentHash],
    source_term: object | None = None,
    name_term: object | None = None,
) -> _GroupChains:
    """Pool in structural order, breaking structural ties separately per chain."""
    if isinstance(group, int):
        return inputs[group]

    symmetric = isinstance(group, dict)
    group_children = group["symmetric"] if isinstance(group, dict) else group
    children = [_pool_group(child, inputs, hash_term) for child in group_children]
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

    # A walk spans arbitrary slots and wrappers, including bindings between jobs.
    # Guard its reverse edges and memo publication with the same lock as binding.
    _identity_lock: ClassVar[AbstractContextManager[object]] = RLock()

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
        with self._identity_lock:
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
                    _ = dependency._identity_dependents.pop(key, None)
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
        try:
            cached = hashes.get(key)
        except TypeError:
            # Semantic terms can contain values that Python cannot use as dict keys.
            return _hash(label, preimage)
        if cached is None:
            cached = hashes[key] = _hash(label, preimage)
        return cached

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

    def _identity_input_symmetry(
        self, inputs: tuple[StreamProtocol, ...]
    ) -> InputSymmetry | None:
        """Return positional grouping when it differs from argument order."""
        return encode_input_symmetry(inputs, self._identity_symmetry)

    def _identity_node_pipeline_hash(
        self, hasher: SemanticHasherProtocol
    ) -> ContentHash:
        """Return the structure term for a leaf's public node hash."""
        return self._identity_pipeline_hash(hasher)

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
                self._identity_node_pipeline_hash,
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
        group = self._identity_input_symmetry(inputs) if inputs else None
        if group is None:
            group = list[InputSymmetry](range(len(inputs)))
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
        with self._identity_lock:
            return IdentityWalk(self._identity_hasher).visit(self).sources

    def name_chain(self) -> ContentHash:
        """Return the chain of source-slot names."""
        with self._identity_lock:
            return IdentityWalk(self._identity_hasher).visit(self).names

    def _chain_content_hash(self) -> ContentHash:
        """Combine structure and sources. This becomes ``content_hash()`` at the switch.

        Raises:
            UnboundSourceError: If any contributing source slot is unbound.
        """
        with self._identity_lock:
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
        with self._identity_lock:
            walk = IdentityWalk(self._identity_hasher)
            chains = walk.visit(self)
            structure = chains.node_pipeline_hash or chains.pipeline_hash
            return self._identity_hash(
                NODE_HASH_LABEL, (structure(walk.hasher), chains.names)
            )
