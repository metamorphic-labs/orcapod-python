"""Tensor values round trip exactly through sources and function pods (ITL-871)."""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("torch")

import numpy as np  # noqa: E402
import torch  # noqa: E402  # pyright: ignore[reportMissingImports]  # optional peer dependency, importorskip above

from orcapod import function_pod  # noqa: E402
from orcapod.sources import DictSource  # noqa: E402
from tests.test_torch.tensor_cases import (  # noqa: E402
    NUMERIC,
    TENSORS,
    assert_tensors_identical,
)


def _source(tensors: dict[str, Any], source_id: str = "tensors") -> DictSource:
    return DictSource(
        data=[{"k": k, "x": v} for k, v in tensors.items()],
        tag_columns=["k"],
        source_id=source_id,
    )


def _read(stream: Any, column: str) -> dict[str, Any]:
    return {t.as_dict()["k"]: d.as_dict()[column] for t, d in stream.iter_data()}


def test_one_column_holds_every_dtype_and_shape() -> None:
    got = _read(_source(TENSORS), "x")
    for name, expected in TENSORS.items():
        assert_tensors_identical(got[name], expected)


def test_function_pod_input_and_output() -> None:
    seen: list[Any] = []

    @function_pod(output_keys="y")
    def identity(x: torch.Tensor) -> torch.Tensor:
        seen.append(x)
        return x.clone()

    got = _read(identity.pod(_source(TENSORS)), "y")
    for name, expected in TENSORS.items():
        assert_tensors_identical(got[name], expected)
    for x in seen:
        assert type(x) is torch.Tensor and not x.requires_grad


def test_chained_pods() -> None:
    @function_pod(output_keys="d")
    def double(x: torch.Tensor) -> torch.Tensor:
        return x * 2

    @function_pod(output_keys="total")
    def total(d: torch.Tensor) -> float:
        return float(d.to(torch.complex128).real.sum())

    numeric = {k: TENSORS[k] for k in NUMERIC}
    doubled = double.pod(_source(numeric))
    totals = _read(total.pod(doubled), "total")
    got = _read(doubled, "d")
    for name in NUMERIC:
        expected = TENSORS[name] * 2
        assert_tensors_identical(got[name], expected)
        assert totals[name] == pytest.approx(
            float(expected.to(torch.complex128).real.sum()), nan_ok=True
        )


def test_pod_returning_a_grad_tensor_stores_its_value() -> None:
    weight = torch.ones(3, requires_grad=True)

    @function_pod(output_keys="y")
    def scale(x: torch.Tensor) -> torch.Tensor:
        return x * weight  # tracks gradients

    got = _read(scale.pod(_source({"a": torch.arange(3.0)})), "y")
    assert_tensors_identical(got["a"], torch.arange(3.0))
    assert weight.requires_grad and weight.grad is None


@pytest.mark.skipif(not torch.backends.mps.is_available(), reason="needs MPS")
def test_pod_returning_an_mps_tensor_reads_back_on_cpu() -> None:
    @function_pod(output_keys="y")
    def to_gpu(x: torch.Tensor) -> torch.Tensor:
        return x.to("mps") * 2

    got = _read(to_gpu.pod(_source({"a": torch.arange(3.0)})), "y")
    assert_tensors_identical(got["a"], torch.arange(3.0) * 2)


@pytest.mark.skipif(not torch.cuda.is_available(), reason="needs CUDA")
def test_pod_returning_a_cuda_tensor_reads_back_on_cpu() -> None:
    @function_pod(output_keys="y")
    def to_gpu(x: torch.Tensor) -> torch.Tensor:
        return x.to("cuda") * 2

    got = _read(to_gpu.pod(_source({"a": torch.arange(3.0)})), "y")
    assert_tensors_identical(got["a"], torch.arange(3.0) * 2)


def test_read_back_is_writeable_and_owns_its_memory() -> None:
    source = _source({"a": torch.arange(3.0)})
    first = _read(source, "x")["a"]
    first[0] = 99.0
    assert_tensors_identical(_read(source, "x")["a"], torch.arange(3.0))


def test_optional_tensor_column_keeps_none() -> None:
    @function_pod(output_keys="y")
    def maybe(x: torch.Tensor) -> torch.Tensor | None:
        return None if x.numel() == 0 else x

    got = _read(maybe.pod(_source(TENSORS)), "y")
    for name, expected in TENSORS.items():
        if expected.numel() == 0:
            assert got[name] is None
        else:
            assert_tensors_identical(got[name], expected)


def test_list_of_tensors_round_trips() -> None:
    @function_pod(output_keys="parts")
    def split(x: torch.Tensor) -> list[torch.Tensor]:
        return [x, x * 2]

    got = _read(split.pod(_source({"a": torch.arange(3.0)})), "parts")
    assert len(got["a"]) == 2
    assert_tensors_identical(got["a"][0], torch.arange(3.0))
    assert_tensors_identical(got["a"][1], torch.arange(3.0) * 2)


def test_ndarray_column_into_tensor_parameter_is_refused() -> None:
    @function_pod(output_keys="y")
    def identity(x: torch.Tensor) -> torch.Tensor:
        return x

    arrays = DictSource(data=[{"k": "a", "x": np.arange(3.0)}], tag_columns=["k"])
    with pytest.raises(Exception, match="x") as info:
        list(identity.pod(arrays).iter_data())
    assert "ndarray" in str(info.value) or "Tensor" in str(info.value)
