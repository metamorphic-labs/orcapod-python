"""``torch.Tensor`` logical type: a tensor stored in the v0.3 array format (ITL-871).

A tensor is stored as one row of the shared array struct
(``array_format.ARRAY_STORAGE_TYPE``): an Orcapod dtype tag, the shape, and the
element bytes in C order, little-endian and packed. The extension name is
``orcapod.torch_tensor`` and the hash label ``torch_tensor_v1``, so a tensor and
an ndarray with equal values are different values with different hashes, while
their digest bodies (``array_format.array_digest_v1``) are equal. The format is
specified in ``superpowers/specs/2026-11-11-torch-tensor-v0.3.md`` and
documented in ``docs/concepts/arrays.md``.

Identity is dtype, shape and element bits. What is not identity is not stored:
device, gradient state (``requires_grad``, ``grad``, ``grad_fn``), memory layout
(strides, views, channels-last), lazy conjugate and negative bits, and subclass
(``nn.Parameter``). Writing takes a detached, CPU, resolved, contiguous copy or
view; the caller's tensor is never modified. A tensor reads back as a CPU,
contiguous, writeable plain ``torch.Tensor`` without gradients, with exactly the
stored dtype and shape.

Supported dtypes: ``bool``, ``int8`` to ``int64``, ``uint8`` to ``uint64``,
``float16``, ``float32``, ``float64``, ``complex64`` and ``complex128`` (the same
tags and bytes as NumPy), and the torch-only dtypes ``bfloat16``, ``complex32``
and the float8 family (``float8_e4m3fn``, ``float8_e5m2``, ``float8_e4m3fnuz``,
``float8_e5m2fnuz``, ``float8_e8m0fnu``), stored as their raw bits under the
torch dtype name. A torch dtype is looked up when it is used, never at import:
reading a stored dtype that the installed torch does not have raises
``ValueError`` naming the dtype and the torch version. Everything else is
refused at write time with ``UnsupportedValueError`` naming the reason (the type
converter adds the column): sparse and mkldnn layouts, quantized, nested and
meta tensors, named tensors, and other dtypes (sub-byte and packed types,
``bcomplex32``).

This module imports torch; it is loaded on first use through the deferred
registration in ``contexts/logical_types.json`` (``orcapod.utils.deferred_types``), so
``import orcapod`` never imports torch.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np
import polars as pl
import pyarrow as pa
import torch

from orcapod.errors import UnsupportedValueError
from orcapod.logical_types.array_format import (
    ARRAY_FORMAT_METADATA,
    ARRAY_STORAGE_TYPE,
    array_storage_sort_key,
)
from orcapod.logical_types.base_logical_type import BaseLogicalType
from orcapod.logical_types.numpy_type import array_bytes, canonical_array, tag_to_dtype
from orcapod.logical_types.registry import (
    make_arrow_extension_type,
    make_polars_extension_type,
)

if TYPE_CHECKING:
    from orcapod.logical_types.protocols import TypeConverterProtocol

TORCH_TENSOR_EXTENSION_NAME = "orcapod.torch_tensor"
"""Arrow extension name (and logical type name) of a stored tensor."""

TORCH_TENSOR_HASH_LABEL = "torch_tensor_v1"
"""``ContentHash.method`` of a tensor hash."""

_SHARED_TAGS = (
    "bool",
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float16",
    "float32",
    "float64",
    "complex64",
    "complex128",
)
"""Tags whose element bytes are the same as NumPy's (and whose name is the torch dtype name)."""

_RAW_BIT_TAGS: dict[str, tuple[str, int]] = {
    "bfloat16": ("int16", 1),
    "complex32": ("int16", 2),
    "float8_e4m3fn": ("uint8", 1),
    "float8_e5m2": ("uint8", 1),
    "float8_e4m3fnuz": ("uint8", 1),
    "float8_e5m2fnuz": ("uint8", 1),
    "float8_e8m0fnu": ("uint8", 1),
}
"""Tags without a NumPy dtype: the NumPy tag their bits are stored as, and how many
of those make one element (``complex32`` is a real then an imaginary binary16)."""

_SUPPORTED_TAGS = frozenset(_SHARED_TAGS) | frozenset(_RAW_BIT_TAGS)


def _tag_of(dtype: Any) -> str | None:
    """Return the tag of a torch dtype (its name), or ``None`` if it is not supported."""
    tag = str(dtype).removeprefix("torch.")
    return tag if tag in _SUPPORTED_TAGS else None


def _dtype_of(tag: str) -> Any:
    """Return the torch dtype for a supported tag, looked up in the installed torch.

    Raises:
        ValueError: If the tag is not a tensor tag, or the installed torch does not
            have the dtype (for example ``float8_e8m0fnu`` before torch 2.7).
    """
    if tag not in _SUPPORTED_TAGS:
        raise ValueError(f"unknown torch tensor dtype tag {tag!r}")
    dtype = getattr(torch, tag, None)
    if not isinstance(dtype, torch.dtype):
        raise ValueError(
            f"stored torch tensor has dtype {tag!r}, which the installed PyTorch "
            f"{torch.__version__} does not have; install a PyTorch version that has "
            "it to read this value"
        )
    return dtype


def _refuse(what: str, reason: str) -> UnsupportedValueError:
    """Build the refusal for a tensor that cannot be stored.

    Args:
        what: The offending property, e.g. ``"layout torch.sparse_coo"``.
        reason: Why it cannot be stored and, where possible, what to do.

    Returns:
        The error to raise; the type converter prefixes the column name.
    """
    return UnsupportedValueError(
        f"torch tensors with {what} cannot be stored: {reason}"
    )


def _check_storable(value: Any) -> str:
    """Refuse what cannot be stored, and return the dtype tag of ``value``.

    Raises:
        TypeError: If ``value`` is not a ``torch.Tensor``.
        UnsupportedValueError: For extended or unsupported tensors.
    """
    if not isinstance(value, torch.Tensor):
        raise TypeError(f"expected a torch.Tensor, got {type(value).__name__}")
    if value.is_nested:
        raise _refuse(
            "nested (jagged) layout",
            "nested tensors are extended data and are not supported",
        )
    if value.layout != torch.strided:
        raise _refuse(
            f"layout {value.layout}",
            "sparse and other non-dense layouts are extended data and are not "
            "supported (convert with .to_dense())",
        )
    if value.is_quantized:
        raise _refuse(
            f"quantized dtype {value.dtype}",
            "quantized tensors are extended data and are not supported "
            "(convert with .dequantize())",
        )
    if value.device.type == "meta":
        raise _refuse("device 'meta'", "a meta tensor holds no data")
    names = getattr(value, "names", None)
    if names and any(name is not None for name in names):
        raise _refuse(
            f"dimension names {tuple(names)}",
            "named tensors are not supported (drop the names with .rename(None))",
        )
    tag = _tag_of(value.dtype)
    if tag is None:
        raise _refuse(
            f"dtype {value.dtype}",
            "this dtype is not supported in Orcapod v0.3 (supported: bool, integer, "
            "float16/32/64, bfloat16, the float8 types, complex32/64/128)",
        )
    return tag


def canonical_tensor(value: Any) -> tuple[str, tuple[int, ...], memoryview]:
    """Return the dtype tag, shape and canonical element bytes of a tensor.

    The bytes are the elements in C order, little-endian, packed, read from a
    detached CPU copy or view with lazy conjugate and negative bits resolved.
    ``value`` itself is never modified.

    Args:
        value: A ``torch.Tensor`` (subclasses are treated as plain tensors).

    Returns:
        ``(tag, shape, data)``, exactly the three stored fields.

    Raises:
        TypeError: If ``value`` is not a ``torch.Tensor``.
        UnsupportedValueError: For tensors that cannot be stored.
    """
    tag = _check_storable(value)
    t = value.detach()
    if t.device.type != "cpu":
        t = t.to("cpu")
    t = t.resolve_conj().resolve_neg().contiguous()
    if tag == "complex32":
        # NumPy has no complex32: store each element's real then imaginary binary16.
        t = torch.view_as_real(t)
    raw = _RAW_BIT_TAGS.get(tag)
    if raw is not None:
        t = t.view(_dtype_of(raw[0]))
    _, array = canonical_array(t.numpy())
    return tag, tuple(int(d) for d in value.shape), array_bytes(array)


def _tensor_from_row(tag: str, dims: tuple[int, ...], data: bytes) -> Any:
    """Build a writeable CPU tensor from a stored row's fields."""
    dtype = _dtype_of(tag)
    raw_tag, parts = _RAW_BIT_TAGS.get(tag, (tag, 1))
    np_dtype = tag_to_dtype(raw_tag)
    count = (int(np.prod(dims, dtype=np.int64)) if dims else 1) * parts
    if len(data) != count * np_dtype.itemsize:
        raise ValueError(
            f"stored torch tensor value has {len(data)} data bytes; dtype {tag!r} "
            f"and shape {list(dims)} need {count * np_dtype.itemsize}"
        )
    array = np.frombuffer(data, dtype=np_dtype, count=count)
    array = array.reshape((*dims, parts) if parts > 1 else dims)
    # A native-order, owned copy: torch.from_numpy needs native byte order, and the
    # tensor must not share memory with Arrow.
    array = array.astype(np_dtype.newbyteorder("="), copy=True)
    tensor = torch.from_numpy(array)
    if tag == "complex32":
        return torch.view_as_complex(tensor.view(torch.float16))
    if tag in _RAW_BIT_TAGS:
        tensor = tensor.view(dtype)
    return tensor


class LogicalTorchTensor(BaseLogicalType):
    """Logical type for ``torch.Tensor`` in the v0.3 array format.

    Stores each tensor as ``{"dtype": tag, "shape": [...], "data": bytes}`` in
    ``ARRAY_STORAGE_TYPE`` under the extension name ``orcapod.torch_tensor``. A
    value reads back as a CPU, contiguous, writeable plain ``torch.Tensor``.

    Example:
        >>> lt = LogicalTorchTensor()
        >>> t = torch.arange(6.0).reshape(2, 3)
        >>> torch.equal(lt.storage_to_python(lt.python_to_storage(t)), t)
        True
    """

    _arrow_ext_class = make_arrow_extension_type(
        TORCH_TENSOR_EXTENSION_NAME, ARRAY_STORAGE_TYPE, ARRAY_FORMAT_METADATA
    )
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type(
        TORCH_TENSOR_EXTENSION_NAME,
        ARRAY_STORAGE_TYPE,
        ARRAY_FORMAT_METADATA.decode(),
    )
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = TORCH_TENSOR_EXTENSION_NAME
    python_type: type = torch.Tensor

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the cached Arrow extension type ``orcapod.torch_tensor``.

        Returns:
            The ``pa.ExtensionType`` over ``ARRAY_STORAGE_TYPE``.
        """
        if LogicalTorchTensor._arrow_ext is None:
            LogicalTorchTensor._arrow_ext = LogicalTorchTensor._arrow_ext_class()  # pyright: ignore[reportCallIssue]  # the generated class takes no arguments
        return LogicalTorchTensor._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type ``orcapod.torch_tensor``.

        Returns:
            The ``pl.BaseExtension`` registered under ``orcapod.torch_tensor``.
        """
        if LogicalTorchTensor._polars_ext is None:
            LogicalTorchTensor._polars_ext = LogicalTorchTensor._polars_ext_class()  # pyright: ignore[reportCallIssue]  # the generated class takes no arguments
        return LogicalTorchTensor._polars_ext

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> dict[str, Any]:
        """Encode a tensor as its canonical stored row.

        Args:
            value: A ``torch.Tensor``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            ``{"dtype": tag, "shape": list_of_ints, "data": bytes}``.

        Raises:
            UnsupportedValueError: For tensors that cannot be stored.
            TypeError: If ``value`` is not a ``torch.Tensor``.
        """
        tag, shape, data = canonical_tensor(value)
        return {"dtype": tag, "shape": list(shape), "data": data.tobytes()}

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> Any:
        """Decode a stored row into a CPU ``torch.Tensor``.

        Args:
            storage_value: The stored ``{"dtype", "shape", "data"}`` mapping.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A CPU, contiguous, writeable ``torch.Tensor`` with the stored dtype
            and shape, without gradients.

        Raises:
            ValueError: If a field is missing or null, the tag is not a tensor
                tag, or the data length does not match the dtype and shape.
        """
        tag = storage_value.get("dtype")
        shape = storage_value.get("shape")
        data = storage_value.get("data")
        if tag is None or shape is None or data is None:
            raise ValueError(
                "stored torch tensor value has a null field "
                f"(dtype={tag!r}, shape={shape!r}, data is None: {data is None})"
            )
        return _tensor_from_row(tag, tuple(int(d) for d in shape), bytes(data))

    def storage_sort_key(self, storage_value: Any) -> tuple[Any, ...]:
        """Ordering key for stored values, used where operators sort values.

        Orders by the stored ``(dtype tag, shape, data)``; ``None`` sorts first
        (``array_format.array_storage_sort_key``).

        Args:
            storage_value: A stored row, or ``None``.

        Returns:
            A tuple that orders stored rows deterministically.
        """
        return array_storage_sort_key(storage_value)
