"""Semantic hash handler for ``torch.Tensor`` (ITL-871).

Loaded on first use through the ``deferred_value_handlers`` entry of
``contexts/logical_types.json``, so
importing Orcapod never imports torch.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orcapod.logical_types.array_format import array_digest_v1
from orcapod.logical_types.torch_type import TORCH_TENSOR_HASH_LABEL, canonical_tensor
from orcapod.types import ContentHash

if TYPE_CHECKING:
    from orcapod.protocols.hashing_protocols import SemanticHasherProtocol


class TorchTensorHandler:
    """Hasher for ``torch.Tensor``: the version-1 array digest under ``torch_tensor_v1``.

    The digest is ``array_format.array_digest_v1(tag, shape, data)`` of exactly the
    row ``LogicalTorchTensor`` stores, so device, gradient state, memory layout,
    lazy conjugate/negative bits and subclass do not change the hash, while
    dtype, shape and every element bit do. The digest body equals the one of an
    ``np.ndarray`` with the same dtype, shape and values. The two types are
    told apart by their logical type identifiers, which enter every type,
    schema and table hash; the label is informational and enters no hash, so
    inside an untyped Python structure a tensor and an equal ndarray hash the
    same.
    """

    HASH_LABEL = TORCH_TENSOR_HASH_LABEL
    """``ContentHash.method`` of a tensor hash."""

    def handle(self, obj: Any, hasher: SemanticHasherProtocol) -> ContentHash:
        """Return the ``torch_tensor_v1`` value hash of ``obj``.

        Args:
            obj: A ``torch.Tensor`` instance.
            hasher: Ignored. Present for protocol conformance.

        Returns:
            ``ContentHash("torch_tensor_v1", array_digest_v1(tag, shape, data))``.

        Raises:
            TypeError: If ``obj`` is not a ``torch.Tensor``.
            UnsupportedValueError: If the tensor cannot be stored.
        """
        tag, shape, data = canonical_tensor(obj)
        return ContentHash(
            method=self.HASH_LABEL, digest=array_digest_v1(tag, shape, data)
        )
