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
the [Hashing Reference](../reference/hashing.md#8-array-value-digest). A PyTorch tensor uses
the same digest for its elements but is hashed as a tensor (below), so an array and a tensor
with equal values never have the same hash.

## PyTorch tensors

A `torch.Tensor` can be a data value too. PyTorch is an optional dependency. Tensor support
works whenever PyTorch can be imported, however it was installed: `pip install "orcapod[torch]"`
installs it from PyPI, or install the build that suits your hardware yourself (see
pytorch.org; on Linux the PyPI build includes CUDA, and pytorch.org also offers smaller
CPU-only builds). Orcapod needs PyTorch 2.6 or later, the first version whose restricted
loader (below) reads `uint16`, `uint32` and `uint64` tensors, and is tested with PyTorch 2.15. Orcapod
hashes a tensor's elements through NumPy (`Tensor.numpy()`), so your PyTorch build must
support the NumPy version you have installed: some older builds cannot use NumPy 2 and fail
with "Numpy is not available". If you see that error, upgrade PyTorch or install a NumPy
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

**A tensor is a different type from an array.** The column's extension name is
`orcapod.torch.tensor` and the hash label is `torch_tensor_v1`. A tensor and an ndarray with
equal dtype, shape and values never hash the same: not as values, not inside a `dict` or
`list`, not as columns, schemas or types. A pod that declares `x: torch.Tensor` cannot take an
`np.ndarray` column (or the reverse). Convert at the pod boundary when you need to:
`torch.from_numpy(a)` or `t.numpy()`.

### How a tensor is stored

Storage and hash are decided separately. A tensor is stored with PyTorch's own serialization:
one cell holds the bytes `torch.save` writes for that tensor, in a `large_binary` column. It
is read with `torch.load(..., map_location="cpu", weights_only=True)`. That is PyTorch's
restricted loader: it rebuilds tensors and runs no other pickled code. Orcapod always passes
both arguments, never falls back to an unrestricted load, and never adds a class to the
loader's allow list; the environment variable `TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD` does not
change that. A cell that is not a tensor archive raises an error naming the column.

What a stored tensor keeps:

| Property | Stored and read back? |
|---|---|
| dtype, shape, element bits | yes |
| `requires_grad` | yes |
| Memory layout: strides (transposed views, `channels_last`) | yes, for a tensor that covers its buffer (below) |
| Lazy conjugate and negative bits (`t.conj()`) | yes, for a tensor that covers its buffer |
| Class: `torch.Tensor` or `torch.nn.Parameter` | yes |
| Device (CUDA, MPS) | no: a stored tensor is read on the CPU |
| Autograd history: `grad_fn`, `.grad`, hooks | no: a stored tensor is read as a leaf |

**Only the elements you pass are stored.** `torch.save` writes the whole buffer under a
tensor, so a 3-element slice of a 1,000,000-element tensor would store all million elements.
When a tensor does not cover its underlying buffer exactly (a slice, a step slice such as
`t[::2]`, an expanded view), Orcapod stores `value.clone(memory_format=torch.preserve_format)`
instead. The clone holds just the elements of the view. It has PyTorch's strides for that
clone, and a lazy conjugate or negative bit is resolved into the elements: `x.conj().imag`,
which is a negative view of every second number of `x`, is stored as plain negated values.
A tensor that covers its buffer (`x.conj()`, `x.transpose(0, 1)`) is stored as it is.

Orcapod never modifies your tensor. One stored tensor costs about 1.5 KB more than its
elements, so a column of many very small tensors is larger than the same data in one tensor.

### What counts as the same tensor

The hash is defined by Orcapod and never reads the stored bytes. It is strict: by default two
tensors hash the same only when all of these are equal.

| Property | In the hash | Can be ignored (option name) |
|---|---|---|
| dtype, shape, element bits (`-0.0` and NaN payloads count) | always | no |
| `requires_grad` | by default | `requires_grad` |
| Memory layout (strides) | by default | `memory_layout` |
| Lazy conjugate bit | by default | `conj_bit` |
| Lazy negative bit | by default | `neg_bit` |
| Class (`torch.Tensor` or `torch.nn.Parameter`) | by default | `subclass` |
| Device, autograd history, `.grad`, position inside a larger buffer | never | |

A property that storage does not keep is never in the hash: otherwise a value read back from
a store would hash differently from the value that was written, and every cached result would
be computed again. For the same reason the optional properties are those of the *stored*
tensor: a step slice hashes like its contiguous clone.

The properties the hash ignores are the type's **ignore list**. It is empty by default. It is
configured per data context (`hash_ignore` in `orcapod/contexts/logical_types.json`) and is
written in the column's extension metadata, `{"ignore":[],"version":1}`, so it is part of the
type's [identifier](logical-type-identifiers.md): columns with different lists are different
types, and a data context refuses to read a tensor column stored under another list. An
ignored property is still stored and read back; it only stops counting in the hash. Orcapod
v0.3 has no user API for the list.

```python
from orcapod.contexts import get_default_context

hasher = get_default_context().semantic_hasher
t = torch.arange(6, dtype=torch.float32).reshape(2, 3)
print(hasher.hash_object(t) == hasher.hash_object(t.clone()))
# True
print(hasher.hash_object(t.T) == hasher.hash_object(t.T.contiguous()))
# False
print(hasher.hash_object(t) == hasher.hash_object(t.clone().requires_grad_(True)))
# False
print(hasher.hash_object(t) == hasher.hash_object(torch.nn.Parameter(t, requires_grad=False)))
# False
print(hasher.hash_object(t).digest == hasher.hash_object(t.numpy()).digest)
# False
print(hasher.hash_object(t).to_string())
# torch_tensor_v1:...
```

The byte-level definition is in the
[Hashing Reference](../reference/hashing.md#torch-tensor-value-hash).

### Device and autograd history

A pod may return a GPU tensor, or one computed from tensors that track gradients (it has a
`grad_fn`), and a source may hold one. Storage keeps neither the device nor the history, so
Orcapod stores a CPU leaf tensor and issues `orcapod.errors.ValueConversionWarning`, which
names the function and output (or the source and column), says what was converted, and how to
avoid it: return or pass `value.detach().cpu()`. A function warns once per output and
conversion, however many rows it processes; a source warns once per column, with the number
of values converted. A CPU tensor without a `grad_fn` never warns, also when it has
`requires_grad=True` or is an `nn.Parameter`: those are stored. To treat the conversion as an
error, run with `-W error::orcapod.errors.ValueConversionWarning`; every conversion then
raises, and in a pipeline that row fails.

```python
import warnings

from orcapod.errors import ValueConversionWarning

@function_pod(output_keys="weights")
def fit(signal: torch.Tensor) -> torch.Tensor:
    return signal.clone().requires_grad_(True) * 0.5

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    list(fit.pod(tensors).iter_data())
print(caught[0].category is ValueConversionWarning)
# True
print(caught[0].message)
# Function 'fit' output 'weights' returned a torch tensor with autograd history (a grad_fn); Orcapod stored a CPU tensor without autograd history, and the returned tensor is unchanged. Return value.detach().cpu() to avoid this warning.
```

**What comes back.** A tensor read from a store or a source is a CPU leaf tensor with the
stored dtype, shape, elements, strides, `requires_grad`, lazy bits and class (a `bfloat16`
tensor reads back as `bfloat16`, an `nn.Parameter` as an `nn.Parameter`). Within a single
in-memory run, though, the next pod receives the very object the previous pod returned, on its
device and with its autograd history. The same holds for NumPy arrays (a Fortran-ordered array
passes through in Fortran order). Write pods that do not depend on the device or the autograd
history of their inputs.

### Dtypes and refusals

`bool`, `int8` to `int64`, `uint8` to `uint64`, `float16`, `float32`, `float64`, `complex64`
and `complex128` are hashed with the NumPy tags and bytes from the table above. The dtypes
NumPy does not have are hashed as their raw bits, each element little-endian, under the
PyTorch dtype name as the tag:

| Dtype | Element bytes |
|---|---|
| `bfloat16` | 2: the bfloat16 bit pattern |
| `complex32` | 4: the real part, then the imaginary part, each a `float16` |
| `float8_e4m3fn`, `float8_e5m2`, `float8_e4m3fnuz`, `float8_e5m2fnuz`, `float8_e8m0fnu` | 1: the float8 bit pattern |

A stored dtype that your PyTorch does not have (`float8_e8m0fnu` needs PyTorch 2.7) raises an
error naming the column when it is read; everything else still works. These are refused when
they are written, with an `UnsupportedValueError` that names the column and the reason:

| Tensor | What to do |
|---|---|
| sparse or `mkldnn` layout | `.to_dense()` |
| quantized | `.dequantize()` |
| nested (jagged) | store the parts as separate tensors |
| on the `meta` device | it holds no data |
| named dimensions (PyTorch versions that have them) | `.rename(None)` |
| sub-byte and packed dtypes (`int1` to `int7`, `uint1` to `uint7`, `float4_e2m1fn_x2`, `bits*`), `bcomplex32` | convert to a supported dtype |
| a subclass other than `torch.nn.Parameter` | `.as_subclass(torch.Tensor)`, or register a logical type for the class (PyTorch's restricted loader cannot read it back) |

A function pod that declares `-> torch.Tensor` must return a tensor. An `np.ndarray` or a
Python number raises `OutputTypeError` that names the function and the output. Return
`torch.as_tensor(...)` instead. An `nn.Parameter` is a tensor, so it is accepted; it is
stored, read back and hashed as an `nn.Parameter`. A pod that declares `-> torch.nn.Parameter`
must return one.
