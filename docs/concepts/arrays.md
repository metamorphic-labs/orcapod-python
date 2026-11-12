# Arrays and Tensors

A `numpy.ndarray` can be a data value like any other: a source column, a function pod
parameter or a return value. Orcapod stores each array by its **value**, its dtype, its shape
and its elements. How the array happens to sit in memory is not stored. The hash is defined on
that value, so two arrays that hold the same values hash the same, however each was laid out.

```python
import numpy as np

from orcapod import function_pod
from orcapod.sources import DictSource

@function_pod(output_keys="scaled")
def scale(signal: np.ndarray) -> np.ndarray:
    return signal * 2

source = DictSource(
    data=[{"trial": "t1", "signal": np.arange(6, dtype=np.float32).reshape(2, 3)}],
    tag_columns=["trial"],
    source_id="trials",
)
for tag, data in scale.pod(source).iter_data():
    print(tag["trial"], data["scaled"].dtype, data["scaled"].shape)
# t1 float32 (2, 3)
```

!!! warning "v0.3 changed the array format"
    Orcapod v0.2 stored arrays as `.npy` files under the extension name `numpy.ndarray` and
    hashed the file. v0.3 replaces both, as described below, and does not migrate older
    stores. Every node that handles arrays, and every node downstream of an array column, gets
    new tables in v0.3. Because v0.3 also changed every hash of Arrow data, v0.3 reuses no
    result of a store written by v0.2: it ignores the v0.2 rows and computes every input
    again (see [A store written by an older version](function-pods.md#a-store-written-by-an-older-version)).
    Nodes that perform side effects are the exception: a side-effect pod, or a function pod
    that takes the invocation context, does not run again on such a store but raises
    `SideEffectLogDataContextError` before it delivers anything.
    The v0.2 tables and rows stay on disk unchanged, readable with v0.2. A migration is
    planned after v0.3 (ITL-889).

## What counts as the same array

Two arrays are the same value when they have the same dtype, the same shape and the same
element bits.

- **Memory layout does not count.** C order, Fortran order, strided views and transposed views
  of equal values are the same array. An array always reads back in C order.
- **Byte order does not count.** Arrays are stored little-endian; a big-endian `>f4` array
  reads back as `<f4` with equal values.
- **Structured field order counts**, as do field names and field types, at every nesting
  depth: `[("a", "i4"), ("b", "f8")]` and `[("b", "f8"), ("a", "i4")]` are different arrays,
  as they are to NumPy's own dtype equality. Offsets, padding and alignment do not count:
  fields are stored packed in declared order, so an aligned `[("b", "f8"), ("a", "i2")]`
  reads back as the packed `[("b", "<f8"), ("a", "<i2")]`.
- **Dtype counts**, including width: `int32` and `int64` arrays with equal values differ.
- **Shape counts**, including empty shapes: `(0, 3)` and `(3, 0)` differ, and so do a 0-d
  array and a one-element 1-d array.
- **Every bit counts**: `0.0` and `-0.0` differ, and so do NaNs with different payloads.
- **Subclasses are plain arrays by default.** `np.matrix`, `np.recarray`, `np.memmap` and
  other subclasses use the array format: they are stored as their values and read back as
  plain `np.ndarray`. The exception is a subclass with its own registered logical type, which
  is used for that subclass and its own subclasses instead.

```python
from orcapod.contexts import get_default_context

hasher = get_default_context().semantic_hasher
a = np.arange(6, dtype=np.float32).reshape(2, 3)

print(hasher.hash_object(a) == hasher.hash_object(np.asfortranarray(a)))
# True
print(hasher.hash_object(a) == hasher.hash_object(a.astype(">f4")))
# True
print(hasher.hash_object(a) == hasher.hash_object(a.astype(np.float64)))
# False
ab = np.zeros(1, dtype=[("a", "i4"), ("b", "f8")])
ba = np.zeros(1, dtype=[("b", "f8"), ("a", "i4")])
print(hasher.hash_object(ab) == hasher.hash_object(ba))
# False
```

## What cannot be stored

Some arrays are refused when they are written, with a `ValueError` (an
`orcapod.errors.UnsupportedValueError`) that names the column and the dtype:

| Array | Why | What to do |
|---|---|---|
| Masked arrays (`np.ma.MaskedArray`) | extended data: the mask would be lost | store the data and the mask as two arrays |
| `dtype=object`, or a structured dtype with an object field | extended data: would need pickling | convert to a fixed dtype |
| NumPy 2 `StringDType` | extended data: variable width | use a fixed-width string dtype such as `U10` |
| `longdouble`, `clongdouble` | width and format differ by platform | `.astype(np.float64)` or `.astype(np.complex128)` |
| `datetime64` or `timedelta64` without a unit | the integer has no defined meaning | give a unit, e.g. `datetime64[ns]` |
| Structured dtypes with field titles | titles are aliases of fields | drop the titles |

Extended data may be supported later, stored as several parts rather than as one array.
Whether `longdouble` and `clongdouble`, unit-less datetimes and timedeltas, and structured
dtypes with titles should be accepted in a later release is tracked in ITL-911; v0.3 refuses
them.

```python
try:
    DictSource(
        data=[{"trial": "t1", "signal": np.ma.array([1.0, 2.0], mask=[False, True])}],
        tag_columns=["trial"],
    )
except ValueError as exc:
    print(type(exc).__name__)
# UnsupportedValueError
```

A function pod that declares `-> np.ndarray` must return an array. A NumPy scalar, for
example `x * 2` on a 0-d array, raises `TypeError` that names the output. Return
`np.asarray(...)` instead.

## How an array is stored

An array column has the Arrow extension type `orcapod.ndarray`, with extension metadata
`{"version":1}`. Its storage is one struct per row:

| Field | Arrow type | Holds |
|---|---|---|
| `dtype` | `large_string` | the Orcapod dtype tag (below) |
| `shape` | `large_list<int64>` | the dimensions, outermost first; empty for a 0-d array |
| `data` | `large_binary` | the elements in C order, little-endian, packed |

The struct's children are nullable in the Arrow schema only because Polars drops nested
non-null flags. A stored array never has a null child. A null array is a null struct, allowed
when the column is annotated `np.ndarray | None`.

```python
table = source.as_table()
print(table.schema.field("signal").type.extension_name)
# orcapod.ndarray
print(table.column("signal").chunk(0).storage[0].as_py()["dtype"])
# float32
```

### Dtype tags

The tag names the element type without a byte order. For a structured dtype, the tag is a JSON
list with one `[name, tag]` entry per field, or `[name, tag, [dims]]` for a subarray field,
in declared field order. It is written with no whitespace and with non-ASCII characters escaped
(`json.dumps(..., separators=(",", ":"), ensure_ascii=True)`).

| NumPy | Tag | Element bytes |
|---|---|---|
| `bool` | `bool` | one byte, 0 or 1 |
| `int8` ... `int64`, `uint8` ... `uint64` | `int8` ... `uint64` | two's complement or unsigned, little-endian |
| `float16`, `float32`, `float64` | same names | IEEE 754, little-endian |
| `complex64`, `complex128` | same names | real part, then imaginary part |
| `datetime64[ns]`, `datetime64[10ms]`, ... | `datetime64[ns]`, `datetime64[10ms]`, ... | `int64`, little-endian (NaT is the minimum) |
| `timedelta64[s]`, ... | `timedelta64[s]`, ... | `int64`, little-endian |
| `U5` | `ucs4[5]` | five UCS-4 code units, each `uint32` little-endian, NUL-padded |
| `S3` | `bytes[3]` | three raw bytes, NUL-padded |
| `V4` | `void[4]` | four raw bytes |
| `[("b", "f8"), ("a", "i4", (2,))]` | `[["b","float64"],["a","int32",[2]]]` | fields packed in tag order |

## How an array is hashed

The hash of an array is the version-1 array digest of its stored row, labelled `ndarray_v1`.
The byte-level definition, and a reference implementation that does not use Orcapod, are in
the [Hashing Reference](../reference/hashing.md#8-array-value-digest). PyTorch tensors share
the digest under their own label (below), so an array and a tensor with equal values have
different hashes.

## PyTorch tensors

A `torch.Tensor` can be a data value too. PyTorch is an optional dependency. Tensor support
works whenever PyTorch can be imported, however it was installed: `pip install "orcapod[torch]"`
installs it from PyPI, or install the build that suits your hardware yourself (see
pytorch.org; on Linux the PyPI build includes CUDA, and pytorch.org also offers smaller
CPU-only builds). Orcapod needs PyTorch 2.3 or later and is tested with PyTorch 2.15. Orcapod
converts tensors through NumPy (`Tensor.numpy()` and `torch.from_numpy`), so your PyTorch build
must support the NumPy version you have installed: some older builds cannot use NumPy 2 and
fail with "Numpy is not available". If you see that error, upgrade PyTorch or install a NumPy
version your PyTorch build supports.

Importing Orcapod never imports PyTorch. The tensor type is set up the first time Orcapod
meets a tensor or a `torch.Tensor` annotation, or reads a stored tensor column. Reading a
stored tensor column without PyTorch installed raises
`orcapod.errors.MissingOptionalDependencyError`, which names the missing package.

```python
import torch

from orcapod import function_pod
from orcapod.sources import DictSource

@function_pod(output_keys="scaled")
def scale_tensor(signal: torch.Tensor) -> torch.Tensor:
    return signal * 2

tensors = DictSource(
    data=[{"trial": "t1", "signal": torch.arange(6, dtype=torch.float32).reshape(2, 3)}],
    tag_columns=["trial"],
    source_id="tensor_trials",
)
for tag, data in scale_tensor.pod(tensors).iter_data():
    print(tag["trial"], data["scaled"].dtype, tuple(data["scaled"].shape))
# t1 torch.float32 (2, 3)
```

**A tensor is a different type from an array.** A tensor and an ndarray with equal dtype,
shape and values are stored in the same struct, with the same bytes, but under the extension
name `orcapod.torch_tensor` and the hash label `torch_tensor_v1`. The extension name is part
of the type's [identifier](logical-type-identifiers.md), which is hashed with every column,
schema and type: a tensor column and an array column never hash the same, with or without
rows. The value digest itself is shared, so inside an untyped Python structure (a `dict` or
`list` hashed directly) a tensor and an equal array hash the same. A pod that declares
`x: torch.Tensor` cannot take an `np.ndarray` column (or the reverse).
Convert at the pod boundary when you need to: `torch.from_numpy(a)` or `t.numpy()`.

**What counts as the same tensor.** Dtype, shape and element bits, exactly as for arrays. These
do not count, and are not stored:

| Property | On write |
|---|---|
| Device (CPU, CUDA, MPS) | the values are copied to the CPU |
| `requires_grad`, `grad`, `grad_fn` | the tensor is detached; the autograd graph is not stored |
| Memory layout: strides, views, `channels_last` | stored in C order |
| Lazy conjugate and negative views (`t.conj()`) | the conjugated values are stored |
| Subclass, such as `torch.nn.Parameter` | stored as a plain tensor |

Orcapod never modifies your tensor: it reads it through a detached copy or view. A pod may
return a GPU tensor or one that still tracks gradients, and a source may hold one. Its value is
stored from a detached CPU copy, and Orcapod issues `orcapod.errors.ValueConversionWarning`,
which names the function and output (or the source and column), says what was converted, and
how to avoid it: return or pass `value.detach().cpu()`. A function warns once per output and
conversion, however many rows it processes; a source warns once per column, with the number
of values converted. Plain CPU tensors without gradients never warn. To treat the conversion
as an error, run with `-W error::orcapod.errors.ValueConversionWarning`; every conversion then
raises, and in a pipeline that row fails.

```python
import warnings

from orcapod.errors import ValueConversionWarning

@function_pod(output_keys="weights")
def fit(signal: torch.Tensor) -> torch.Tensor:
    return (signal * 0.5).requires_grad_(True)

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    list(fit.pod(tensors).iter_data())
print(caught[0].category is ValueConversionWarning)
# True
print(caught[0].message)
# Function 'fit' output 'weights' returned a torch tensor that tracks gradients (requires_grad=True); Orcapod stored a detached CPU copy, and the returned tensor is unchanged. Return value.detach().cpu() to avoid this warning.
```

**What comes back.** A tensor read from a store or a source is a CPU, contiguous, writeable,
plain `torch.Tensor` without gradients, with exactly the stored dtype and shape (a `bfloat16`
tensor reads back as `bfloat16`). Within a single in-memory run, though, the next pod receives
the very object the previous pod returned, on its device and with its gradient state. The same
holds for NumPy arrays (a Fortran-ordered array passes through in Fortran order). Write pods
that do not depend on device, gradient state or layout of their inputs.

**Dtypes.** `bool`, `int8` to `int64`, `uint8` to `uint64`, `float16`, `float32`, `float64`,
`complex64` and `complex128` use the NumPy tags and bytes from the table above. The dtypes
NumPy does not have are stored as their raw bits, each element little-endian, under the
PyTorch dtype name as the tag:

| Dtype | Element bytes |
|---|---|
| `bfloat16` | 2: the bfloat16 bit pattern |
| `complex32` | 4: the real part, then the imaginary part, each a `float16` |
| `float8_e4m3fn`, `float8_e5m2`, `float8_e4m3fnuz`, `float8_e5m2fnuz`, `float8_e8m0fnu` | 1: the float8 bit pattern |

A stored dtype that your PyTorch does not have (`float8_e8m0fnu` needs PyTorch 2.7) raises a
`ValueError` naming the dtype and your PyTorch version when it is read; everything else still
works. Everything else is refused when it is written, with an `UnsupportedValueError` that
names the column and the reason:

| Tensor | What to do |
|---|---|
| sparse or `mkldnn` layout | `.to_dense()` |
| quantized | `.dequantize()` |
| nested (jagged) | store the parts as separate tensors |
| on the `meta` device | it holds no data |
| named dimensions (PyTorch before 2.15) | `.rename(None)` |
| sub-byte and packed dtypes (`int1` to `int7`, `uint1` to `uint7`, `float4_e2m1fn_x2`, `bits*`), `bcomplex32` | convert to a supported dtype |

A function pod that declares `-> torch.Tensor` must return a tensor. An `np.ndarray` or a
Python number raises `TypeError` that names the output. Return `torch.as_tensor(...)`
instead.

```python
from orcapod.contexts import get_default_context

hasher = get_default_context().semantic_hasher
t = torch.arange(6, dtype=torch.float32).reshape(2, 3)
print(hasher.hash_object(t) == hasher.hash_object(t.T.contiguous().T))
# True
print(hasher.hash_object(t).digest == hasher.hash_object(t.numpy()).digest)
# True
print(hasher.hash_object(t) == hasher.hash_object(t.numpy()))
# False
print(hasher.hash_object(t).to_string())
# torch_tensor_v1:318f6e503e8064e89ce6039453fce709f8e01f60c501af2b63bbbe9cf8fbf90e
```
