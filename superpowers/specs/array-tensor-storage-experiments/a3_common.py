"""Shared helpers for the a3_* and a4_* experiment scripts (ITL-874).

Not an experiment on its own. It holds the reporting helpers and the sample arrays that the
pipeline experiments reuse.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import numpy as np
import numpy.typing as npt


def report(status: str, check: str, observed: object) -> None:
    """Print one result line in the shared ``[STATUS] check: observed`` format."""
    print(f"[{status}] {check}: {observed}")


def run_check(check: str, fn: Callable[[], Any]) -> Any:
    """Run ``fn``. Print ``[OK]`` with its result, or ``[DEFECT]`` with the exception.

    ``fn`` may return ``(status, observed)`` to choose the status itself.
    """
    try:
        result = fn()
    except Exception as exc:  # noqa: BLE001 - experiments report every failure
        message = " ".join(str(exc).split())
        report("DEFECT", check, f"{type(exc).__name__}: {message[:400]}")
        return None
    if isinstance(result, tuple) and len(result) == 2 and result[0] in STATUSES:
        report(result[0], check, result[1])
    else:
        report("OK", check, result)
    return result


STATUSES = ("OK", "DEFECT", "INFO")


def sample_arrays() -> dict[str, npt.NDArray[Any]]:
    """Return one array per tag value, with a different dtype, shape and layout each."""
    return {
        "a": np.arange(6, dtype=np.float32).reshape(2, 3),  # float32, C order
        "b": np.asfortranarray(np.arange(6, dtype=np.float64).reshape(3, 2)),  # F order
        "c": np.array([1, -2, 3], dtype=np.int16),  # int16, 1-D
        "d": np.array(7.5, dtype=np.float32),  # zero-dimensional
    }


def same_array(got: object, want: npt.NDArray[Any]) -> bool:
    """Return True when ``got`` is an ndarray with ``want``'s dtype, shape and values."""
    return (
        isinstance(got, np.ndarray)
        and got.dtype == want.dtype
        and got.shape == want.shape
        and bool(np.array_equal(got, want))
    )


def describe(value: object) -> str:
    """Describe an array value compactly: type, dtype, shape, memory order."""
    if isinstance(value, np.ndarray):
        order = "F" if value.flags["F_CONTIGUOUS"] and value.ndim > 1 else "C"
        return f"ndarray(dtype={value.dtype}, shape={value.shape}, order={order})"
    if isinstance(value, list):
        return "[" + ", ".join(describe(v) for v in value) + "]"
    return f"{type(value).__name__}({value!r})"
