"""``torch.Tensor`` values: storage, identity, hash, refusals, no mutation (ITL-871).

Specification: ``superpowers/specs/2026-11-11-torch-tensor-v0.3.md``. Tests go
through sources, streams, function pods and the default data context's
semantic hasher.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("torch")

import numpy as np  # noqa: E402
import polars as pl  # noqa: E402
import pyarrow as pa  # noqa: E402
import torch  # noqa: E402

from orcapod import function_pod  # noqa: E402
from orcapod.contexts import create_registry, get_default_context  # noqa: E402
from orcapod.errors import UnsupportedValueError  # noqa: E402
from orcapod.logical_types.base_logical_type import BaseLogicalType  # noqa: E402
from orcapod.logical_types.registry import (  # noqa: E402
    make_arrow_extension_type,
    make_polars_extension_type,
)
from orcapod.semantic_types.universal_converter import (  # noqa: E402
    UniversalTypeConverter,
)
from orcapod.sources import DictSource  # noqa: E402
from tests.test_torch.tensor_cases import (  # noqa: E402
    EXTENSION_NAME,
    LABEL,
    TENSORS,
    assert_tensors_identical,
    expected_digest,
    shares_numpy_dtype,
    tag_of,
    value_bytes,
)


def stored(value: Any) -> dict[str, Any]:
    """The stored row of a one-row ``x`` column."""
    table = DictSource(data=[{"k": "r", "x": value}], tag_columns=["k"]).as_table()
    chunk = table.column("x").chunk(0)
    assert isinstance(chunk, pa.ExtensionArray)
    return chunk.storage[0].as_py()


def read_back(value: Any) -> Any:
    """The value of a one-row ``x`` column, read back from its source."""
    source = DictSource(data=[{"k": "r", "x": value}], tag_columns=["k"])
    return next(iter(source.iter_data()))[1].as_dict()["x"]


def semantic_hash(value: Any) -> Any:
    """The default context's semantic hash of ``value``."""
    return get_default_context().semantic_hasher.hash_object(value)


def data_hash(value: Any) -> Any:
    """The content hash of a one-row data holding ``value``."""
    source = DictSource(data=[{"k": "r", "x": value}], tag_columns=["k"])
    return next(iter(source.iter_data()))[1].content_hash()


# ---------------------------------------------------------------------------
# Column type and stored row
# ---------------------------------------------------------------------------


def test_column_type_is_orcapod_torch_tensor_over_the_array_struct() -> None:
    """A tensor column is ``orcapod.torch_tensor`` over the array struct, version 1."""
    table = DictSource(
        data=[{"k": "r", "x": torch.arange(3.0)}], tag_columns=["k"]
    ).as_table()
    ext = table.schema.field("x").type
    assert isinstance(ext, pa.ExtensionType)
    assert ext.extension_name == EXTENSION_NAME
    assert ext.storage_type == pa.struct(
        [
            pa.field("dtype", pa.large_string()),
            pa.field("shape", pa.large_list(pa.int64())),
            pa.field("data", pa.large_binary()),
        ]
    )
    assert ext.__arrow_ext_serialize__() == b'{"version":1}'


@pytest.mark.parametrize("name", sorted(TENSORS))
def test_stored_row_holds_tag_shape_and_c_order_little_endian_data(name: str) -> None:
    """The stored row is the dtype tag, the shape and the C-order little-endian bytes."""
    t = TENSORS[name]
    assert stored(t) == {
        "dtype": tag_of(t),
        "shape": list(t.shape),
        "data": value_bytes(t),
    }


@pytest.mark.parametrize("name", sorted(TENSORS))
def test_round_trip_is_exact(name: str) -> None:
    """Each tensor reads back with its dtype, shape and bits."""
    assert_tensors_identical(read_back(TENSORS[name]), TENSORS[name])


def test_shared_dtypes_store_the_same_row_as_the_equal_ndarray() -> None:
    """Dtypes shared with NumPy store the same row as the equal ndarray."""
    for name, t in TENSORS.items():
        if not shares_numpy_dtype(t):
            continue
        assert stored(t) == stored(t.numpy()), name


# ---------------------------------------------------------------------------
# Hash
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(TENSORS))
def test_semantic_hash_is_the_v1_digest_under_the_tensor_label(name: str) -> None:
    """The hash is the reference v1 digest under ``torch_tensor_v1``."""
    h = semantic_hash(TENSORS[name])
    assert h.method == LABEL
    assert h.digest == expected_digest(TENSORS[name])


def test_tensor_and_equal_ndarray_differ_in_hash_but_share_the_digest_body() -> None:
    """A tensor and its equal ndarray differ in hash but share the digest body."""
    for name, t in TENSORS.items():
        if not shares_numpy_dtype(t):
            continue
        ht, hn = semantic_hash(t), semantic_hash(t.numpy())
        assert ht != hn, name
        assert ht.digest == hn.digest, name
        assert data_hash(t) != data_hash(t.numpy()), name


def test_table_token_carries_identifier_and_digest() -> None:
    """A stored cell hashes as the logical type identifier and the digest, no label."""
    from orcapod.hashing.arrow_hashers import StarfixArrowHasher

    hasher = get_default_context().arrow_hasher
    assert isinstance(hasher, StarfixArrowHasher)
    t = torch.arange(3.0)
    table = DictSource(data=[{"k": "r", "x": t}], tag_columns=["k"]).as_table()
    # Non-public helper: this test is about the token the hasher builds for a cell.
    processed = hasher._process_table_columns(table.select(["x"]))  # pyright: ignore[reportPrivateUsage]
    token = processed.column("x")[0].as_py()
    assert (
        token
        == b"orcapod.torch_tensor;7b2276657273696f6e223a317d::"
        + semantic_hash(t).digest
    )


def _base() -> Any:
    """A 2x3x4 float32 tensor."""
    return torch.arange(24, dtype=torch.float32).reshape(2, 3, 4)


def _same_value_variants() -> list[tuple[str, Any, Any]]:
    """Pairs of tensors that differ only in what is not identity."""
    base = _base()
    four_d = torch.arange(2 * 3 * 4 * 5, dtype=torch.float32).reshape(2, 3, 4, 5)
    complex_t = torch.tensor([1 + 2j, 3 - 4j], dtype=torch.complex64)
    variants = [
        ("transposed_view", base.transpose(0, 2), base.transpose(0, 2).contiguous()),
        ("strided_slice", base[:, ::2, 1:], base[:, ::2, 1:].contiguous()),
        (
            "channels_last",
            four_d.to(memory_format=torch.channels_last),
            four_d.contiguous(),
        ),
        ("conj_view", complex_t.conj(), complex_t.conj().resolve_conj()),
        ("neg_view", complex_t.conj().imag, complex_t.conj().imag.resolve_neg()),
        ("requires_grad", base.clone().requires_grad_(True), base),
        ("grad_fn", base.clone().requires_grad_(True) * 1, base),
        ("parameter", torch.nn.Parameter(base.clone()), base),
    ]
    if torch.backends.mps.is_available():
        variants.append(("mps", base.to("mps"), base))
    if torch.cuda.is_available():
        variants.append(("cuda", base.to("cuda"), base))
    return variants


@pytest.mark.parametrize(
    ("a", "b"),
    [v[1:] for v in _same_value_variants()],
    ids=[v[0] for v in _same_value_variants()],
)
def test_layout_device_grad_and_subclass_do_not_change_the_value(
    a: Any, b: Any
) -> None:
    """Layout, device, gradients and subclass change neither hash nor stored row."""
    assert semantic_hash(a) == semantic_hash(b)
    assert data_hash(a) == data_hash(b)
    assert stored(a) == stored(b)
    assert_tensors_identical(read_back(a), b.detach())


DIFFERENT: list[tuple[str, Any, Any]] = [
    ("width", torch.arange(3, dtype=torch.int32), torch.arange(3, dtype=torch.int64)),
    ("empty_shapes", torch.zeros((0, 3)), torch.zeros((3, 0))),
    ("zero_d_vs_one", torch.tensor(1.0), torch.tensor([1.0])),
    ("signed_zero", torch.tensor([0.0]), torch.tensor([-0.0])),
    (
        "bfloat16_vs_float16",
        torch.ones(2, dtype=torch.bfloat16),
        torch.ones(2, dtype=torch.float16),
    ),
    ("shape", torch.arange(6.0).reshape(2, 3), torch.arange(6.0).reshape(3, 2)),
    (
        "complex32_vs_float32",
        torch.zeros(2, dtype=torch.complex32),
        torch.zeros(2, dtype=torch.float32),
    ),
    (
        "float8_e4m3fn_vs_e5m2",
        torch.zeros(2, dtype=torch.float8_e4m3fn),
        torch.zeros(2, dtype=torch.float8_e5m2),
    ),
    (
        "float8_vs_uint8",
        torch.zeros(2, dtype=torch.float8_e4m3fn),
        torch.zeros(2, dtype=torch.uint8),
    ),
]


@pytest.mark.parametrize(
    ("a", "b"), [d[1:] for d in DIFFERENT], ids=[d[0] for d in DIFFERENT]
)
def test_dtype_shape_and_bits_change_the_hash(a: Any, b: Any) -> None:
    """Dtype, shape and element bits each change the hash."""
    assert semantic_hash(a) != semantic_hash(b)
    assert data_hash(a) != data_hash(b)


# ---------------------------------------------------------------------------
# No mutation of the user's tensor
# ---------------------------------------------------------------------------


def _snapshot(t: Any) -> tuple[Any, ...]:
    """Everything storing or hashing could change about ``t``."""
    return (
        t.requires_grad,
        t.grad_fn is None,
        t.device.type,
        t.dtype,
        tuple(t.shape),
        t.stride(),
        t.storage_offset(),
        t.is_conj(),
        t.is_neg(),
        t.data_ptr(),
        type(t),
        value_bytes(t),
    )


@pytest.mark.parametrize(
    "a",
    [v[1] for v in _same_value_variants()],
    ids=[v[0] for v in _same_value_variants()],
)
def test_storing_and_hashing_never_modify_the_tensor(a: Any) -> None:
    """Storing and hashing leave the caller's tensor exactly as it was."""
    before = _snapshot(a)
    stored(a)
    semantic_hash(a)
    data_hash(a)
    assert _snapshot(a) == before


# ---------------------------------------------------------------------------
# Refusals: named column and reason
# ---------------------------------------------------------------------------


def _refused() -> list[tuple[str, Any, str]]:
    """Tensors that cannot be stored, with a word the refusal must contain."""
    cases: list[tuple[str, Any, str]] = [
        ("sparse_coo", torch.eye(3).to_sparse(), "sparse"),
        ("sparse_csr", torch.eye(3).to_sparse_csr(), "sparse"),
        (
            "quantized",
            torch.quantize_per_tensor(torch.zeros(3), 1.0, 0, torch.qint8),
            "quantized",
        ),
        (
            "nested",
            torch.nested.nested_tensor([torch.zeros(2), torch.zeros(3)]),
            "nested",
        ),
        ("meta", torch.empty(3, device="meta"), "meta"),
    ]
    # Sub-byte, packed and not yet defined dtypes (where this torch has them).
    for name in ("int4", "uint3", "bits8", "float4_e2m1fn_x2", "bcomplex32"):
        dtype = getattr(torch, name, None)
        if dtype is not None:
            cases.append((name, torch.empty(2, dtype=dtype), name))
    return cases


@pytest.mark.parametrize(
    ("value", "reason"), [c[1:] for c in _refused()], ids=[c[0] for c in _refused()]
)
def test_unsupported_tensors_are_refused_naming_column_and_reason(
    value: Any, reason: str
) -> None:
    """Storing an unsupported tensor raises naming the column and the reason."""
    with pytest.raises(UnsupportedValueError, match="'payload'") as info:
        DictSource(data=[{"k": "r", "payload": value}], tag_columns=["k"]).as_table()
    assert reason.lower() in str(info.value).lower()


@pytest.mark.parametrize(
    ("value", "reason"), [c[1:] for c in _refused()], ids=[c[0] for c in _refused()]
)
def test_unsupported_tensors_are_refused_by_the_hasher(value: Any, reason: str) -> None:
    """Hashing an unsupported tensor raises naming the reason."""
    with pytest.raises(UnsupportedValueError) as info:
        semantic_hash(value)
    assert reason.lower() in str(info.value).lower()


# ---------------------------------------------------------------------------
# Function pods: a declared torch.Tensor return must be a tensor
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("returned", "type_name"),
    [(np.zeros(2, np.float32), "ndarray"), (1.5, "float")],
    ids=["ndarray", "float"],
)
def test_non_tensor_returned_for_declared_tensor_is_refused_at_the_pod(
    returned: Any, type_name: str
) -> None:
    """A non-tensor returned for ``-> torch.Tensor`` raises naming the output."""

    @function_pod(output_keys="y")
    def bad(x: torch.Tensor) -> torch.Tensor:
        """Return the value under test instead of a tensor."""
        return returned

    source = DictSource(data=[{"k": "a", "x": torch.zeros(2)}], tag_columns=["k"])
    with pytest.raises(TypeError, match=f"'y'.*Tensor.*{type_name}"):
        list(bad.pod(source).iter_data())


def test_none_returned_for_optional_tensor_is_accepted() -> None:
    """``None`` is accepted for ``-> torch.Tensor | None``."""

    @function_pod(output_keys="y")
    def maybe(x: torch.Tensor) -> torch.Tensor | None:
        """Return ``None`` for an empty tensor, else the input."""
        return None if x.numel() == 0 else x

    source = DictSource(
        data=[{"k": "a", "x": torch.zeros(0)}, {"k": "b", "x": torch.ones(2)}],
        tag_columns=["k"],
    )
    got = {t.as_dict()["k"]: d.as_dict()["y"] for t, d in maybe.pod(source).iter_data()}
    assert got["a"] is None
    assert_tensors_identical(got["b"], torch.ones(2))


def test_declared_tensor_subclass_refuses_a_plain_tensor() -> None:
    """A pod declared ``-> nn.Parameter`` must return an ``nn.Parameter``."""

    @function_pod(output_keys="y")
    def as_parameter(x: torch.Tensor) -> torch.nn.Parameter:
        """Return a plain tensor where ``nn.Parameter`` is declared."""
        return x * 2  # pyright: ignore[reportReturnType]  # deliberately the base class

    source = DictSource(data=[{"k": "a", "x": torch.ones(2)}], tag_columns=["k"])
    with pytest.raises(TypeError, match="'y'.*Parameter.*Tensor"):
        list(as_parameter.pod(source).iter_data())


def test_subclass_instance_is_accepted_for_declared_tensor() -> None:
    """A pod declared ``-> torch.Tensor`` may return a subclass instance."""

    @function_pod(output_keys="y")
    def as_parameter(x: torch.Tensor) -> torch.Tensor:
        """Return an ``nn.Parameter`` where ``torch.Tensor`` is declared."""
        return torch.nn.Parameter(x * 2, requires_grad=False)

    source = DictSource(data=[{"k": "a", "x": torch.ones(2)}], tag_columns=["k"])
    stream = as_parameter.pod(source)
    y_type = stream.as_table().schema.field("y").type
    assert isinstance(y_type, pa.ExtensionType)
    assert y_type.extension_name == "orcapod.torch_tensor"
    got = [d.as_dict()["y"] for _, d in stream.iter_data()]
    assert torch.equal(got[0], torch.ones(2) * 2)


# ---------------------------------------------------------------------------
# A stored dtype the installed torch does not have
# ---------------------------------------------------------------------------

OLDER_TORCH = textwrap.dedent(
    """
    import pyarrow as pa
    import torch

    # Simulate a PyTorch that predates float8_e8m0fnu (added in torch 2.7); a supported
    # torch older than 2.7 lacks it already.
    if hasattr(torch, "float8_e8m0fnu"):
        del torch.float8_e8m0fnu

    from orcapod.sources import ArrowTableSource, DictSource

    # Orcapod's torch support loads and stores the dtypes this torch has.
    source = DictSource(data=[{"k": "a", "x": torch.arange(3.0)}], tag_columns=["k"])
    table = source.as_table()
    print("stored:", next(iter(source.iter_data()))[1].as_dict()["x"].tolist())

    # A row written by a newer torch.
    column_type = table.schema.field("x").type
    storage = pa.array(
        [{"dtype": "float8_e8m0fnu", "shape": [2], "data": bytes([127, 128])}],
        type=column_type.storage_type,
    )
    newer = pa.table(
        {
            "k": table.column("k"),
            "x": pa.ExtensionArray.from_storage(column_type, storage),
        }
    )
    try:
        for _, data in ArrowTableSource(newer, tag_columns=["k"]).iter_data():
            data.as_dict()
    except Exception as exc:  # the type converter wraps it, naming the column
        print("raised:", exc)
    else:
        print("no error")
    """
)


def test_a_dtype_the_installed_torch_lacks_fails_clearly_when_read() -> None:
    """Reading a dtype this torch lacks raises naming it; importing and other dtypes work."""
    repo_root = Path(__file__).resolve().parents[2]
    proc = subprocess.run(
        [sys.executable, "-c", OLDER_TORCH],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(repo_root)},
        cwd=repo_root,
        timeout=300,
    )
    assert proc.returncode == 0, proc.stderr[-3000:]
    lines = proc.stdout.strip().splitlines()
    assert lines[0] == "stored: [0.0, 1.0, 2.0]"
    assert lines[-1].startswith("raised:"), proc.stdout
    assert "column 'x'" in lines[-1]
    assert "'float8_e8m0fnu'" in lines[-1]
    assert torch.__version__ in lines[-1]


class LabeledTensor(torch.Tensor):
    """Toy tensor subclass with its own logical type."""


class LogicalLabeledTensor(BaseLogicalType):
    """Toy logical type for ``LabeledTensor``: stores the raw float32 bytes."""

    _arrow_ext_class = make_arrow_extension_type(
        "test.labeled_tensor", pa.large_binary()
    )
    _polars_ext_class = make_polars_extension_type(
        "test.labeled_tensor", pa.large_binary()
    )
    logical_type_name: str = "test.labeled_tensor"
    python_type: type = LabeledTensor

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the toy extension type."""
        return self._arrow_ext_class()  # pyright: ignore[reportCallIssue]  # synthesized class takes no arguments

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the toy Polars extension type."""
        return self._polars_ext_class()  # pyright: ignore[reportCallIssue]  # synthesized class takes no arguments

    def python_to_storage(self, value: Any, converter: Any = None) -> bytes:
        """Store the float32 bytes."""
        return value.detach().to(torch.float32).numpy().tobytes()

    def storage_to_python(self, storage_value: Any, converter: Any = None) -> Any:
        """Read the bytes back as a ``LabeledTensor``."""
        array = np.frombuffer(storage_value, dtype=np.float32).copy()
        return torch.from_numpy(array).as_subclass(LabeledTensor)


def test_declared_subclass_with_its_own_logical_type_requires_that_subclass() -> None:
    """A pod declared ``-> LabeledTensor`` (registered) must return a ``LabeledTensor``."""
    ctx = create_registry().get_context()
    converter = ctx.type_converter
    assert isinstance(converter, UniversalTypeConverter)
    converter.register_logical_type(LogicalLabeledTensor())  # pyright: ignore[reportArgumentType]  # toy logical type implements the protocol structurally

    @function_pod(output_keys="y", data_context=ctx)
    def plain(x: torch.Tensor) -> LabeledTensor:
        """Return a plain tensor where the registered subclass is declared."""
        return x * 2  # pyright: ignore[reportReturnType]  # deliberately the base class

    @function_pod(output_keys="y", data_context=ctx)
    def labeled(x: torch.Tensor) -> LabeledTensor:
        """Return the registered subclass."""
        return (x * 2).as_subclass(LabeledTensor)

    source = DictSource(
        data=[{"k": "a", "x": torch.ones(2)}], tag_columns=["k"], data_context=ctx
    )
    with pytest.raises(TypeError, match="'y'.*LabeledTensor.*Tensor"):
        list(plain.pod(source).iter_data())
    got = [d.as_dict()["y"] for _, d in labeled.pod(source).iter_data()]
    assert type(got[0]) is LabeledTensor
