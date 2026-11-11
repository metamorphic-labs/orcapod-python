"""B6: how a ``torch.Tensor`` maps onto each candidate.

Run (torch is not a project dependency; every check prints [SKIP] without it):
    uv run --with torch python superpowers/specs/array-tensor-storage-experiments/b6_torch_mapping.py

Claims tested:

* A CPU tensor and a NumPy array with equal dtype, shape and values give
  identical stored values under every candidate (``tensor.numpy()`` path), and
  identical Orcapod hashes where the candidate can be hashed today.
* What the conversion drops or refuses: ``requires_grad`` (``.numpy()`` raises
  until ``.detach()``), device (``mps`` available on this Mac; CUDA not
  verifiable here), sparse layout, non-contiguous views.
* Dtypes without a NumPy equivalent (``bfloat16``, ``float8_*``, ``complex32``):
  ``.numpy()`` fails; a bit-exact path stores the bits as a same-width integer
  plus a dtype tag.
* DLPack: Arrow arrays export DLPack, so ``torch.from_dlpack`` reads a flat
  Arrow value buffer without a copy; ``np.from_dlpack`` reads a CPU tensor.
* What Orcapod's semantic hasher does with a tensor today.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pyarrow as pa
from b0_common import ENCODERS, checking, fst_type, report  # pyright: ignore[reportImplicitRelativeImport]  # run as a script: sys.path[0] is this directory

from orcapod.contexts import get_default_context

try:
    import torch  # pyright: ignore[reportMissingImports]  # optional: run with uv run --with torch
except ImportError:
    torch = None


def _storage_equal(a: pa.Array[Any], b: pa.Array[Any]) -> bool:
    sa = a.storage if isinstance(a, pa.ExtensionArray) else a
    sb = b.storage if isinstance(b, pa.ExtensionArray) else b
    return sa.equals(sb) and a.type == b.type


def equal_values_equal_storage() -> None:
    assert torch is not None  # only called when torch imported
    t = torch.arange(6, dtype=torch.float32).reshape(2, 3)
    n = np.arange(6, dtype=np.float32).reshape(2, 3)
    hasher = get_default_context().arrow_hasher
    for name, enc in ENCODERS.items():
        with checking(f"{name}: tensor.numpy() vs ndarray storage"):
            ct, cn = enc([t.numpy()]), enc([n])
            report(
                "INFO",
                f"{name}: tensor vs ndarray storage identical",
                _storage_equal(ct, cn),
            )
        with checking(f"{name}: orcapod arrow_hasher tensor vs ndarray"):
            ht = hasher.hash_table(pa.table({"x": ct}))
            hn = hasher.hash_table(pa.table({"x": cn}))
            report("INFO", f"{name}: orcapod hash equal", ht == hn)


def semantic_hasher_on_tensor() -> None:
    assert torch is not None  # only called when torch imported
    sh = get_default_context().semantic_hasher
    t = torch.arange(6, dtype=torch.float32).reshape(2, 3)
    with checking("semantic_hasher.hash_object(tensor)"):
        h = sh.hash_object(t)
        report(
            "INFO",
            "semantic_hasher.hash_object(tensor)",
            f"returned {h.to_string()[:40]} (no torch handler registered)",
        )
    with checking("semantic_hasher tensor vs ndarray"):
        report(
            "INFO",
            "semantic hash tensor == ndarray",
            sh.hash_object(t) == sh.hash_object(t.numpy()),
        )


def what_numpy_conversion_drops() -> None:
    assert torch is not None  # only called when torch imported
    with checking("requires_grad tensor .numpy()"):
        g = torch.ones(2, 3, requires_grad=True)
        g.numpy()
        report("INFO", "requires_grad .numpy()", "accepted")
    with checking("requires_grad .detach().numpy()"):
        g = torch.ones(2, 3, requires_grad=True)
        report(
            "INFO",
            "requires_grad .detach().numpy()",
            f"ok; grad flag lost: {g.detach().requires_grad}",
        )
    if torch.backends.mps.is_available():
        with checking("mps tensor .numpy()"):
            torch.ones(2, device="mps").numpy()
            report("INFO", "mps tensor .numpy()", "accepted")
        with checking("mps tensor .cpu().numpy()"):
            torch.ones(2, device="mps").cpu().numpy()
            report(
                "INFO", "mps .cpu().numpy()", "ok; device not recorded in any candidate"
            )
    else:
        report("SKIP", "mps tensor", "mps not available")
    report(
        "SKIP",
        "cuda tensor",
        f"cuda available={torch.cuda.is_available()}; not verified",
    )
    with checking("sparse_coo tensor .numpy()"):
        sp = torch.eye(3).to_sparse()
        sp.numpy()
        report("INFO", "sparse .numpy()", "accepted")
    with checking("non-contiguous tensor (t.T) .numpy()"):
        t = torch.arange(6, dtype=torch.float32).reshape(2, 3).T
        v = t.numpy()
        npy_view = ENCODERS["npy"]([v]).storage[0].as_py()
        npy_copy = ENCODERS["npy"]([np.ascontiguousarray(v)]).storage[0].as_py()
        report(
            "INFO",
            "t.T.numpy()",
            f"F-contiguous={v.flags.f_contiguous}; npy bytes equal to C copy={npy_view == npy_copy}",
        )


def dtypes_without_numpy() -> None:
    assert torch is not None  # only called when torch imported
    names = [
        "bfloat16",
        "complex32",
        "float8_e4m3fn",
        "float8_e5m2",
        "uint16",
        "uint32",
        "uint64",
    ]
    for dname in names:
        dt = getattr(torch, dname, None)
        if dt is None:
            report("SKIP", f"torch.{dname}", "not in this torch")
            continue
        with checking(f"torch.{dname} .numpy()"):
            x = torch.zeros(2, 3, dtype=torch.float32).to(dt)
            arr = x.numpy()
            report("INFO", f"torch.{dname} .numpy()", f"ok -> numpy {arr.dtype}")
    with checking("bfloat16 bit-exact via int16 view"):
        x = torch.tensor([[1.5, -2.25, 3.0]], dtype=torch.bfloat16)
        bits = x.view(torch.int16).numpy()
        col = ENCODERS["raw"]([bits])
        back = torch.from_numpy(
            np.frombuffer(col.storage.field("data")[0].as_py(), dtype="<i2")
            .reshape(1, 3)
            .copy()
        ).view(torch.bfloat16)
        report(
            "INFO",
            "bfloat16 via int16 bits in raw/npy",
            f"bit-exact={torch.equal(back, x)}; stored dtype tag would read int16 unless a "
            f"'bfloat16' tag is added",
        )
    with checking("bfloat16 in Arrow value types"):
        report("INFO", "pyarrow has bfloat16 type", hasattr(pa, "bfloat16"))


def dlpack() -> None:
    assert torch is not None  # only called when torch imported
    with checking("torch.from_dlpack(arrow float32 array)"):
        col = ENCODERS["flat"]([np.arange(6, dtype=np.float32).reshape(2, 3)])
        values = col.storage.values
        t = torch.from_dlpack(values)
        shares = t.data_ptr() == values.buffers()[1].address
        report("INFO", "torch.from_dlpack(arrow values)", f"ok; zero-copy={shares}")
    with checking("np.from_dlpack(cpu tensor)"):
        t = torch.arange(6, dtype=torch.float32)
        n = np.from_dlpack(t)
        report(
            "INFO",
            "np.from_dlpack(cpu tensor)",
            f"zero-copy={n.ctypes.data == t.data_ptr()}",
        )
    with checking("torch.from_dlpack(fst storage values)"):
        col = ENCODERS["fst"]([np.arange(6, dtype=np.float32).reshape(2, 3)])
        t = torch.from_dlpack(col.storage.flatten()).reshape(
            -1, *fst_type(col.type).shape
        )
        report(
            "INFO",
            "torch.from_dlpack(fst flattened storage)",
            f"ok shape={tuple(t.shape)}",
        )


if __name__ == "__main__":
    if torch is None:
        report(
            "SKIP",
            "all torch checks",
            "torch not installed; run with uv run --with torch",
        )
    else:
        report("INFO", "torch version", torch.__version__)
        equal_values_equal_storage()
        semantic_hasher_on_tensor()
        what_numpy_conversion_drops()
        dtypes_without_numpy()
        dlpack()
