"""Shared candidate encoders and decoders for the Part B experiments (ITL-874).

The other ``b*_*.py`` scripts import this module. Run it directly to print the
candidates and the Arrow type each one gives a 2x3 ``float32`` array:

    uv run python superpowers/specs/array-tensor-storage-experiments/b0_common.py

Candidates:

* ``npy``: today's ``LogicalNumpyArray`` (``numpy.ndarray`` over ``large_binary``,
  one ``.npy`` file per row).
* ``fst``: Arrow canonical ``arrow.fixed_shape_tensor`` (built in to pyarrow).
* ``vst``: Arrow canonical ``arrow.variable_shape_tensor``. The locked pyarrow
  has no built-in type for it, so this module defines a Python
  ``pa.ExtensionType`` with that name over the spec's storage
  ``struct<data: list<T>, shape: fixed_size_list<int32>[ndim]>``.
* ``flat``: a throwaway Orcapod-style logical type over ``large_list<T>`` of the
  C-order flattened values, with ``dtype`` and ``shape`` in the type metadata
  (shape fixed per column).
* ``row``: a throwaway Orcapod-style logical type over
  ``struct<shape: large_list<int64>, data: large_list<T>>`` (shape per row),
  ``dtype`` in the type metadata.
* ``raw``: a throwaway Orcapod-style logical type over
  ``struct<shape: large_list<int64>, data: large_binary>`` holding the
  little-endian C-order bytes (shape per row), ``dtype`` in the type metadata.

None of these types live in ``src/``. They exist only to measure behaviour.
"""

from __future__ import annotations

from typing import Any, cast

import contextlib
import io
import json
from collections.abc import Callable, Iterator

import numpy as np
import numpy.typing as npt
import pyarrow as pa

# ---------------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------------


def report(status: str, check: str, observed: object) -> None:
    """Print one result line in the shared ``[STATUS] check: observed`` format."""
    print(f"[{status}] {check}: {observed}")


@contextlib.contextmanager
def checking(check: str) -> Iterator[None]:
    """Run a check body, printing ``[FAIL]`` with the exception if it raises."""
    try:
        yield
    except Exception as exc:  # noqa: BLE001 - experiments report every failure
        msg = str(exc).splitlines()[0][:220] if str(exc) else ""
        report("FAIL", check, f"{type(exc).__name__}: {msg}")


# ---------------------------------------------------------------------------
# Throwaway extension types (candidates 3 and 4)
# ---------------------------------------------------------------------------


class _JsonExt(pa.ExtensionType):
    """A Python extension type whose parameters are a JSON object."""

    NAME = ""

    def __init__(self, storage_type: pa.DataType, params: dict[str, Any]) -> None:
        self.params = params
        super().__init__(storage_type, self.NAME)

    def __arrow_ext_serialize__(self) -> bytes:
        return json.dumps(self.params, sort_keys=True).encode()

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> _JsonExt:
        return cls(storage_type, json.loads(serialized.decode()) if serialized else {})


class VariableShapeTensorType(_JsonExt):
    """``arrow.variable_shape_tensor`` defined in Python (pyarrow 23 has none)."""

    NAME = "arrow.variable_shape_tensor"


class FlatNDArrayType(_JsonExt):
    """Candidate 4a: ``large_list<T>`` of flattened values; dtype and shape in metadata."""

    NAME = "experiment.ndarray_flat"


class RowNDArrayType(_JsonExt):
    """Candidate 4b: ``struct<shape, data: large_list<T>>``; dtype in metadata."""

    NAME = "experiment.ndarray_row"


class RawNDArrayType(_JsonExt):
    """Candidate 4c: ``struct<shape, data: large_binary>`` little-endian; dtype in metadata."""

    NAME = "experiment.ndarray_raw"


def _register(ext: pa.ExtensionType) -> None:
    with contextlib.suppress(pa.ArrowKeyError, KeyError):
        pa.register_extension_type(ext)  # pyright: ignore[reportArgumentType]  # pyarrow-stubs annotate PyExtensionType; ExtensionType is the documented argument


_register(VariableShapeTensorType(pa.struct([("data", pa.list_(pa.int8()))]), {}))
_register(FlatNDArrayType(pa.large_list(pa.int8()), {}))
_register(RowNDArrayType(pa.struct([("shape", pa.large_list(pa.int64()))]), {}))
_register(RawNDArrayType(pa.struct([("shape", pa.large_list(pa.int64()))]), {}))


def le_dtype(dtype: np.dtype[Any]) -> np.dtype[Any]:
    """Return ``dtype`` with little-endian byte order (no-op for 1-byte kinds)."""
    return dtype.newbyteorder("<") if dtype.byteorder == ">" else dtype


def dtype_param(dtype: np.dtype[Any]) -> object:
    """JSON-able full dtype description (keeps structured field names and order)."""
    return np.lib.format.dtype_to_descr(le_dtype(dtype))


def dtype_from_param(param: object) -> np.dtype[Any]:
    """Inverse of ``dtype_param`` (JSON turns the descr tuples into lists)."""
    if isinstance(param, list):
        param = [tuple(x) for x in param]
    return np.lib.format.descr_to_dtype(param)


def arrow_value_type(dtype: np.dtype[Any]) -> pa.DataType:
    """Map a NumPy dtype onto the Arrow value type, raising if Arrow has none."""
    return pa.from_numpy_dtype(le_dtype(dtype))


def fst_type(t: pa.DataType) -> pa.FixedShapeTensorType[Any]:
    """Type ``t`` as the ``arrow.fixed_shape_tensor`` type it is (pyarrow-stubs say
    ``ExtensionType``, which has no ``shape`` / ``permutation`` / ``dim_names``)."""
    return cast("pa.FixedShapeTensorType[Any]", t)


# ---------------------------------------------------------------------------
# Encoders: list[np.ndarray] -> pa.Array
# ---------------------------------------------------------------------------


def encode_npy(arrays: list[npt.NDArray[Any]]) -> pa.ExtensionArray[Any]:
    """Encode with today's ``LogicalNumpyArray`` (``.npy`` bytes per row)."""
    from orcapod.logical_types import LogicalNumpyArray

    lt = LogicalNumpyArray()
    storage = pa.array([lt.python_to_storage(a) for a in arrays], pa.large_binary())
    return pa.ExtensionArray.from_storage(lt.get_arrow_extension_type(), storage)


def encode_fst(arrays: list[npt.NDArray[Any]]) -> pa.ExtensionArray[Any]:
    """Encode as ``arrow.fixed_shape_tensor`` (one shape and dtype per column)."""
    return pa.FixedShapeTensorArray.from_numpy_ndarray(np.stack(arrays))


def encode_vst(arrays: list[npt.NDArray[Any]]) -> pa.ExtensionArray[Any]:
    """Encode as ``arrow.variable_shape_tensor`` per the canonical spec."""
    dtype = arrays[0].dtype
    ndim = arrays[0].ndim
    value_type = arrow_value_type(dtype)
    storage_type = pa.struct(
        [
            ("data", pa.list_(value_type)),
            ("shape", pa.list_(pa.int32(), ndim)),
        ]
    )
    data = pa.array(
        [np.ascontiguousarray(a, dtype=le_dtype(dtype)).ravel() for a in arrays],
        pa.list_(value_type),
    )
    shape = pa.array([list(a.shape) for a in arrays], pa.list_(pa.int32(), ndim))
    storage = pa.StructArray.from_arrays([data, shape], fields=list(storage_type))
    return pa.ExtensionArray.from_storage(
        VariableShapeTensorType(storage_type, {}), storage
    )


def encode_flat(arrays: list[npt.NDArray[Any]]) -> pa.ExtensionArray[Any]:
    """Encode as candidate 4a: flattened ``large_list<T>``, shape in metadata."""
    dtype = arrays[0].dtype
    shape = list(arrays[0].shape)
    value_type = arrow_value_type(dtype)
    storage_type = pa.large_list(pa.field("item", value_type, nullable=False))
    storage = pa.array(
        [np.ascontiguousarray(a, dtype=le_dtype(dtype)).ravel() for a in arrays],
        storage_type,
    )
    ext = FlatNDArrayType(storage_type, {"dtype": dtype_param(dtype), "shape": shape})
    return pa.ExtensionArray.from_storage(ext, storage)


def encode_row(arrays: list[npt.NDArray[Any]]) -> pa.ExtensionArray[Any]:
    """Encode as candidate 4b: per-row ``struct<shape, data: large_list<T>>``."""
    dtype = arrays[0].dtype
    value_type = arrow_value_type(dtype)
    storage_type = pa.struct(
        [
            pa.field(
                "shape", pa.large_list(pa.field("item", pa.int64(), False)), False
            ),
            pa.field("data", pa.large_list(pa.field("item", value_type, False)), False),
        ]
    )
    shape = pa.array([list(a.shape) for a in arrays], storage_type.field(0).type)
    data = pa.array(
        [np.ascontiguousarray(a, dtype=le_dtype(dtype)).ravel() for a in arrays],
        storage_type.field(1).type,
    )
    storage = pa.StructArray.from_arrays([shape, data], fields=list(storage_type))
    ext = RowNDArrayType(storage_type, {"dtype": dtype_param(dtype)})
    return pa.ExtensionArray.from_storage(ext, storage)


def encode_raw(arrays: list[npt.NDArray[Any]]) -> pa.ExtensionArray[Any]:
    """Encode as candidate 4c: per-row ``struct<shape, data: large_binary>``."""
    dtype = arrays[0].dtype
    storage_type = pa.struct(
        [
            pa.field(
                "shape", pa.large_list(pa.field("item", pa.int64(), False)), False
            ),
            pa.field("data", pa.large_binary(), False),
        ]
    )
    shape = pa.array([list(a.shape) for a in arrays], storage_type.field(0).type)
    data = pa.array(
        [np.ascontiguousarray(a, dtype=le_dtype(dtype)).tobytes() for a in arrays],
        pa.large_binary(),
    )
    storage = pa.StructArray.from_arrays([shape, data], fields=list(storage_type))
    ext = RawNDArrayType(storage_type, {"dtype": dtype_param(dtype)})
    return pa.ExtensionArray.from_storage(ext, storage)


# ---------------------------------------------------------------------------
# Decoders: pa.Array (extension or storage) -> list[np.ndarray]
# ---------------------------------------------------------------------------


def _storage(arr: pa.Array[Any] | pa.ChunkedArray[Any]) -> Any:
    """Return the storage array; its layout (and so its Arrow class) varies by candidate."""
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    return arr.storage if isinstance(arr, pa.ExtensionArray) else arr


def decode_npy(arr: pa.Array[Any] | pa.ChunkedArray[Any]) -> list[npt.NDArray[Any]]:
    return [
        np.load(io.BytesIO(v), allow_pickle=False) for v in _storage(arr).to_pylist()
    ]


def decode_fst(arr: pa.Array[Any] | pa.ChunkedArray[Any]) -> list[npt.NDArray[Any]]:
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    return list(cast("pa.FixedShapeTensorArray[Any]", arr).to_numpy_ndarray())


def decode_vst(
    arr: pa.Array[Any] | pa.ChunkedArray[Any], dtype: np.dtype[Any]
) -> list[npt.NDArray[Any]]:
    s = _storage(arr)
    data, shape = s.field("data"), s.field("shape")
    return [
        np.asarray(data[i].values.to_numpy(zero_copy_only=False), dtype=dtype).reshape(
            shape[i].as_py()
        )
        for i in range(len(s))
    ]


def decode_flat(
    arr: pa.Array[Any] | pa.ChunkedArray[Any], dtype: np.dtype[Any], shape
) -> list[npt.NDArray[Any]]:
    s = _storage(arr)
    return [
        np.asarray(s[i].values.to_numpy(zero_copy_only=False), dtype=dtype).reshape(
            shape
        )
        for i in range(len(s))
    ]


def decode_row(
    arr: pa.Array[Any] | pa.ChunkedArray[Any], dtype: np.dtype[Any]
) -> list[npt.NDArray[Any]]:
    s = _storage(arr)
    data, shape = s.field("data"), s.field("shape")
    return [
        np.asarray(data[i].values.to_numpy(zero_copy_only=False), dtype=dtype).reshape(
            shape[i].as_py()
        )
        for i in range(len(s))
    ]


def decode_raw(
    arr: pa.Array[Any] | pa.ChunkedArray[Any], dtype: np.dtype[Any]
) -> list[npt.NDArray[Any]]:
    s = _storage(arr)
    data, shape = s.field("data"), s.field("shape")
    return [
        np.frombuffer(data[i].as_py(), dtype=le_dtype(dtype)).reshape(shape[i].as_py())
        for i in range(len(s))
    ]


def stored_dtype(arr: pa.Array[Any]) -> np.dtype[Any]:
    t = arr.type
    params = getattr(t, "params", None)
    if params and "dtype" in params:
        return dtype_from_param(params["dtype"])
    # vst: value type of the ``data`` list
    return np.dtype(t.storage_type.field("data").type.value_type.to_pandas_dtype())


def decode_stored(name: str, arr: pa.ExtensionArray[Any]) -> list[npt.NDArray[Any]]:
    """Decode using only what the column stores."""
    if name == "npy":
        return [
            np.load(io.BytesIO(v), allow_pickle=False) for v in arr.storage.to_pylist()
        ]
    if name == "fst":
        return list(cast("pa.FixedShapeTensorArray[Any]", arr).to_numpy_ndarray())
    dtype = stored_dtype(arr)
    s = arr.storage
    if name == "flat":
        shape = cast(_JsonExt, arr.type).params["shape"]
        return [
            np.asarray(s[i].values.to_numpy(zero_copy_only=False))
            .astype(dtype, copy=False)
            .reshape(shape)
            for i in range(len(s))
        ]
    data, shape = s.field("data"), s.field("shape")
    if name == "raw":
        return [
            np.frombuffer(data[i].as_py(), dtype=dtype).reshape(shape[i].as_py())
            for i in range(len(s))
        ]
    return [
        np.asarray(data[i].values.to_numpy(zero_copy_only=False))
        .astype(dtype, copy=False)
        .reshape(shape[i].as_py())
        for i in range(len(s))
    ]


ENCODERS: dict[str, Callable[[list[npt.NDArray[Any]]], pa.ExtensionArray[Any]]] = {
    "npy": encode_npy,
    "fst": encode_fst,
    "vst": encode_vst,
    "flat": encode_flat,
    "row": encode_row,
    "raw": encode_raw,
}


def decode(
    name: str, arr: pa.Array[Any] | pa.ChunkedArray[Any], like: npt.NDArray[Any]
) -> list[npt.NDArray[Any]]:
    """Decode a candidate column, using ``like`` for dtype/shape where needed."""
    if name == "npy":
        return decode_npy(arr)
    if name == "fst":
        return decode_fst(arr)
    if name == "vst":
        return decode_vst(arr, like.dtype)
    if name == "flat":
        return decode_flat(arr, like.dtype, like.shape)
    if name == "row":
        return decode_row(arr, like.dtype)
    if name == "raw":
        return decode_raw(arr, like.dtype)
    raise KeyError(name)


def arrays_equal(a: list[npt.NDArray[Any]], b: list[npt.NDArray[Any]]) -> bool:
    """True when both lists hold arrays of equal dtype, shape and values."""
    return len(a) == len(b) and all(
        x.dtype == y.dtype and x.shape == y.shape and np.array_equal(x, y)
        for x, y in zip(a, b, strict=True)
    )


def sample_arrays() -> list[npt.NDArray[Any]]:
    """Two 2x3 ``float32`` arrays used by most checks."""
    return [
        np.arange(6, dtype=np.float32).reshape(2, 3),
        np.arange(6, 12, dtype=np.float32).reshape(2, 3),
    ]


def sample_table(name: str) -> pa.Table:
    """A table with an ``id`` column and the candidate column ``x``."""
    return pa.table(
        {"id": pa.array([0, 1], pa.int64()), "x": ENCODERS[name](sample_arrays())}
    )


if __name__ == "__main__":
    for cand in ENCODERS:
        with checking(f"{cand} type"):
            report("INFO", f"{cand} type", sample_table(cand).schema.field("x").type)
