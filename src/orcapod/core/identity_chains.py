"""Compute structure, source and name chains for a stream or invocation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
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
        ContentIdentifiableProtocol,
        IdentityChainProtocol,
        IdentityWalkProtocol,
        SemanticHasherProtocol,
    )

SLOT_NAME_HASH_LABEL = "slot_name_v1"
SOURCE_CHAIN_HASH_LABEL = "source_chain_v1"
NAME_CHAIN_HASH_LABEL = "name_chain_v1"
CONTENT_HASH_LABEL = "content_hash_v1"
NODE_HASH_LABEL = "node_hash_v1"

_reading_chains: ContextVar[bool] = ContextVar("reading_identity_chains", default=False)


@contextmanager
def _identity_read() -> Iterator[None]:
    """Keep recursive schema reads from inferring identity from fetched rows."""
    token = _reading_chains.set(True)
    try:
        yield
    finally:
        _reading_chains.reset(token)


def refuse_identity_io() -> None:
    """Report an unavailable schema when identity would have to read a source."""
    if _reading_chains.get():
        raise ValueError(
            "Pipeline identity is undefined: this source requires a schema."
        )


_binding_hash_walk: ContextVar[IdentityWalk | None] = ContextVar(
    "binding_hash_walk", default=None
)


def track_binding_dependency(slot: IdentityChainsMixin) -> None:
    """Record a slot reached through a source wrapper's legacy identity."""
    walk = _binding_hash_walk.get()
    if walk is not None:
        walk.depend_on(slot)


def _hash(label: str, preimage: tuple[object, ...]) -> ContentHash:
    """Hash canonical semantic tuples under the label of the chain algorithm."""
    from orcapod.hashing.semantic_hashing.semantic_hasher import (
        SemanticAwarePythonHasher,
    )

    return SemanticAwarePythonHasher(hasher_id=label).hash_object(preimage)


@final
class IdentityWalk:
    """Walk uncached inputs and connect their caches to their dependents."""

    def __init__(
        self, hasher: SemanticHasherProtocol, *, include_sources: bool
    ) -> None:
        self.hasher = hasher
        self.include_sources = include_sources
        self._consumer: IdentityChainsMixin | None = None

    def depend_on(self, element: IdentityChainsMixin) -> None:
        """Record a dependency for the node whose chains are being evaluated."""
        consumer = self._consumer
        if consumer is not None and consumer is not element:
            element._identity_dependents[id(consumer)] = consumer
            consumer._identity_dependencies[id(element)] = element

    def binding_hash(self, stream: ContentIdentifiableProtocol) -> ContentHash:
        """Resolve the legacy preimage with live slot bindings and a transient cache."""
        from orcapod.core.base import live_content_hashes

        token = _binding_hash_walk.set(self)
        try:
            with live_content_hashes():
                return stream.content_hash()
        finally:
            _binding_hash_walk.reset(token)

    def visit(self, element: IdentityChainProtocol) -> IdentityChains:
        """Reuse the node's cached chains, recording dependencies through wrappers."""
        if not isinstance(element, IdentityChainsMixin):
            return element._identity_chains(self)  # pyright: ignore[reportPrivateUsage]  # Transparent stream wrapper.

        self.depend_on(element)
        consumer = self._consumer
        self._consumer = element
        try:
            return element._identity_chains(self)  # pyright: ignore[reportPrivateUsage]  # Internal back edge of the identity walk.
        finally:
            self._consumer = consumer


@dataclass(slots=True)
class _SymmetricInputs:
    """An unordered group of argument positions and nested groups."""

    children: list[InputSymmetry]


InputSymmetry = int | list["InputSymmetry"] | _SymmetricInputs


def encode_input_symmetry(
    inputs: tuple[StreamProtocol, ...],
    symmetry: Callable[[tuple[StreamProtocol, ...]], ArgumentGroup],
) -> tuple[InputSymmetry | None, bool]:
    """Encode argument positions and report whether a symmetric group lost inputs."""
    if len(inputs) < 2:
        return None, False

    remaining = dict(enumerate(inputs))
    symmetric: list[tuple[ArgumentGroup, InputSymmetry, list[InputSymmetry]]] = []

    def encode(
        group: ArgumentGroup, available: dict[int, StreamProtocol], *, repeat: bool
    ) -> InputSymmetry:
        if isinstance(group, tuple):
            return [encode(child, available, repeat=repeat) for child in group]
        if isinstance(group, frozenset):
            children: list[InputSymmetry] = []
            for child in group:
                position = encode(child, available, repeat=repeat)
                children.append(position)
                if not repeat:
                    symmetric.append((child, position, children))
            return _SymmetricInputs(children)
        index = next(
            (index for index, stream in available.items() if stream is group), None
        )
        if index is None and repeat:
            index = next(
                (index for index, stream in available.items() if stream == group),
                None,
            )
        if index is not None:
            del available[index]
            return index
        raise ValueError("argument_symmetry did not group arguments by occurrence")

    group = symmetry(inputs)
    try:
        grouped = encode(group, remaining, repeat=False)
    except ValueError:
        # Use argument order when the callback does not return the input streams.
        return None, False

    collapsed = bool(remaining and symmetric)
    # A frozenset can collapse an equal leaf or an entire equal subgroup.
    # Keep the standalone chains' existing fallback. This can recover repeats,
    # but cannot recover the order of equal inputs within a missing subgroup.
    if remaining:
        members = [
            (child, siblings, _input_positions(encoded))
            for child, encoded, siblings in symmetric
        ]
        members.sort(
            key=lambda member: (-len(member[2]), min(member[2], default=len(inputs)))
        )
        for child, siblings, positions in members:
            while positions and len(remaining) >= len(positions):
                available = remaining.copy()
                try:
                    siblings.append(encode(child, available, repeat=True))
                except ValueError:
                    break
                remaining = available
    if remaining:
        return None, collapsed
    return (None if grouped == list(range(len(inputs))) else grouped), collapsed


def _input_positions(group: InputSymmetry) -> tuple[int, ...]:
    """Return every explicit position in a grouping."""
    if isinstance(group, int):
        return (group,)
    children = group.children if isinstance(group, _SymmetricInputs) else group
    return tuple(index for child in children for index in _input_positions(child))


def _has_symmetric_group(group: InputSymmetry) -> bool:
    """Whether pooling needs structural keys anywhere in this grouping."""
    return isinstance(group, _SymmetricInputs) or (
        isinstance(group, list) and any(_has_symmetric_group(child) for child in group)
    )


@dataclass(frozen=True, slots=True)
class _GroupChains:
    """Chains and a structural sort key for an input or a nested symmetry group."""

    structure: Mapping[str, object] | None
    structure_key: str
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
    # Extension terms may compare equal in Python while encoding differently.
    # The enclosing chain memo already caches their result until invalidation.
    return _hash(label, ("term", pooled, own_term))


def _pool_group(
    group: InputSymmetry,
    inputs: tuple[_GroupChains, ...],
    hash_term: Callable[[str, tuple[object, ...]], ContentHash],
    source_term: object | None = None,
    name_term: object | None = None,
) -> _GroupChains:
    """Pool in structural order, breaking structural ties separately per chain."""
    from orcapod.hashing.semantic_hashing.semantic_hasher import canonical_json

    if isinstance(group, int):
        return inputs[group]

    symmetric = isinstance(group, _SymmetricInputs)
    group_children = group.children if isinstance(group, _SymmetricInputs) else group
    children = [_pool_group(child, inputs, hash_term) for child in group_children]
    if symmetric:
        children.sort(key=lambda child: child.structure_key)
    structure = None
    if (
        symmetric
        or not children
        or any(child.structure is not None for child in children)
    ):
        structure = {
            "__type__": "frozenset" if symmetric else "tuple",
            "items": [child.structure for child in children],
        }

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
        structure,
        canonical_json(structure) if structure is not None else "",
        _pool(hash_term, SOURCE_CHAIN_HASH_LABEL, tuple(sources), source_term)
        if len(sources) == len(children)
        else None,
        _pool(hash_term, NAME_CHAIN_HASH_LABEL, tuple(names), name_term),
    )


def _require_complete_symmetry(chains: IdentityChains) -> None:
    """Refuse to combine chains if a commutative ancestor lost input order."""
    if chains.collapsed_input_symmetry:
        raise ValueError("A commutative group dropped content-equal inputs (ITL-1590).")


class IdentityChainsMixin(ABC):
    """Memoize chains per node and invalidate descendants of a changed slot."""

    # A walk spans arbitrary slots and wrappers, including bindings between jobs.
    # Guard its reverse edges and memo publication with the same lock as binding.
    _identity_lock: ClassVar[AbstractContextManager[object]] = RLock()

    # Defaults cover loaded stubs constructed without __init__.
    _identity_cache: dict[tuple[str, bool], IdentityChains] | None = None
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
        cached = hashes.get(key)
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
        hasher_id = walk.hasher.hasher_id
        if not walk.include_sources and (hasher_id, True) in self._identity_cache:
            return self._identity_cache[hasher_id, True]
        key = (hasher_id, walk.include_sources)
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
                if walk.include_sources and name is not None and source is None
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
                collapsed_input_symmetry=upstream.collapsed_input_symmetry,
            )

        # Symmetric groups use structural ordering. Occurrences keep repeats.
        group, collapsed = encode_input_symmetry(inputs, self._identity_symmetry)
        if group is None:
            group = list[InputSymmetry](range(len(inputs)))
        from orcapod.hashing.semantic_hashing.semantic_hasher import (
            canonical_json,
            nested_hash,
        )

        sort_inputs = _has_symmetric_group(group)
        children: list[_GroupChains] = []
        for upstream in upstreams:
            structure = (
                nested_hash(upstream.pipeline_hash(walk.hasher))
                if sort_inputs
                else None
            )
            children.append(
                _GroupChains(
                    structure,
                    canonical_json(structure) if structure is not None else "",
                    upstream.sources,
                    upstream.names,
                )
            )
        pooled = _pool_group(
            group, tuple(children), self._identity_hash, source_term, name_term
        )
        return IdentityChains(
            self._identity_pipeline_hash,
            pooled.sources,
            pooled.names,
            frozenset(
                name for upstream in upstreams for name in upstream.unbound_slots
            ),
            collapsed_input_symmetry=collapsed
            or any(upstream.collapsed_input_symmetry for upstream in upstreams),
        )

    def source_chain(self) -> ContentHash | None:
        """Return the source chain, absent if any contributing slot is unbound."""
        with self._identity_lock, _identity_read():
            return (
                IdentityWalk(self._identity_hasher, include_sources=True)
                .visit(self)
                .sources
            )

    def name_chain(self) -> ContentHash:
        """Return the chain of source-slot names."""
        with self._identity_lock, _identity_read():
            return (
                IdentityWalk(self._identity_hasher, include_sources=False)
                .visit(self)
                .names
            )

    def _chain_content_hash(self) -> ContentHash:
        """Combine structure and sources. This becomes ``content_hash()`` at the switch.

        Raises:
            UnboundSourceError: If any contributing source slot is unbound.
            ValueError: If a commutative group dropped content-equal inputs.
        """
        with self._identity_lock, _identity_read():
            walk = IdentityWalk(self._identity_hasher, include_sources=True)
            chains = walk.visit(self)
            _require_complete_symmetry(chains)
            if chains.sources is None:
                names = ", ".join(repr(name) for name in sorted(chains.unbound_slots))
                raise UnboundSourceError(
                    f"Content identity is undefined for unbound source slots {names}."
                )
            return self._identity_hash(
                CONTENT_HASH_LABEL, (chains.pipeline_hash(walk.hasher), chains.sources)
            )

    def node_hash(self) -> ContentHash:
        """Combine structure and slot names.

        Raises:
            ValueError: If a commutative group dropped content-equal inputs.
        """
        with self._identity_lock, _identity_read():
            walk = IdentityWalk(self._identity_hasher, include_sources=False)
            chains = walk.visit(self)
            _require_complete_symmetry(chains)
            structure = chains.node_pipeline_hash or chains.pipeline_hash
            return self._identity_hash(
                NODE_HASH_LABEL, (structure(walk.hasher), chains.names)
            )
