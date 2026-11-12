## `src/orcapod/logical_types/torch_type.py` — PyTorch tensors

### TT1 — `torch.Tensor` could not be a data value
**Status:** resolved
**Severity:** medium
**Issue:** ITL-871 (design: `superpowers/specs/2026-11-11-torch-tensor-v0.3.md`)

A pod annotated with `torch.Tensor` could not be defined, and the semantic hasher refused
tensors. Users converted at every pod boundary by hand.

**Fix:** `LogicalTorchTensor` (`orcapod.torch_tensor`) stores a tensor in the shared array
struct and `TorchTensorHandler` hashes it under `torch_tensor_v1` (the array digest body). Both
register on first use through deferred entries in `contexts/logical_types.json`, so `import orcapod`
never imports PyTorch, and support depends only on `torch` being importable. Developer
decisions (November 11, 2026): name `orcapod.torch_tensor` and label `torch_tensor_v1`; dtypes
are the NumPy-shared ones plus `bfloat16`, `complex32` and the float8 family as raw bits, with
a dtype the installed torch lacks failing when read, never at import; GPU and grad tensors are
stored from a detached CPU copy with `ValueConversionWarning`, issued once per function output
or source column and conversion; packaging is the convenience extra `orcapod[torch]`. The dev
dependency group pulls the extra, with CPU-only wheels on Linux, so the required CI always runs
the torch tests. A guard test fails if torch is missing under CI.

Known limits: within one in-memory run a downstream pod receives the very tensor the upstream
pod returned (device, gradients and layout included); only stored or read-back values are
canonical. NumPy arrays behave the same.
