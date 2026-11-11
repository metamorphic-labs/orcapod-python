"""A4: array annotations on function pods (ITL-874).

Claims tested, one function pod per annotation, each applied to a source of float64 arrays.
Each function is decorated inside its check, so an annotation the decorator rejects is
reported as a result instead of stopping the script:

* ``-> np.ndarray``, ``-> numpy.typing.NDArray[np.float32]``,
  ``-> np.ndarray[Any, np.dtype[np.float32]]``, ``-> Optional[np.ndarray]``,
  ``-> np.ndarray | None``, ``-> list[np.ndarray]``, ``-> dict[str, np.ndarray]``, and a
  two-array tuple output. For each: does the pod build, what output schema and Arrow type
  result, and do the values round trip?
* Does ``NDArray[np.float32]`` enforce its dtype? The pod returns float64 under a float32
  annotation (output side), and a float32-annotated parameter receives float64 (input side).
  Is the value rejected, converted, or stored as it is?

Run: ``uv run python superpowers/specs/array-tensor-storage-experiments/a4_annotations.py``
"""

from __future__ import annotations

from typing import Any, Optional

import numpy as np
import numpy.typing as npt

from a3_common import describe, run_check  # pyright: ignore[reportImplicitRelativeImport]  # run as a script: sys.path[0] is this directory
from orcapod import function_pod
from orcapod.sources import DictSource

X = {
    "a": np.arange(4, dtype=np.float64).reshape(2, 2),
    "b": np.array([1.5, 2.5], dtype=np.float64),
}


def source() -> DictSource:
    return DictSource(
        data=[{"k": k, "x": v} for k, v in X.items()],
        tag_columns=["k"],
        source_id="a4_float64",
    )


def plain(x: np.ndarray) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    return x


def ndarray_f32_ok(x: np.ndarray) -> npt.NDArray[np.float32]:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    return x.astype(np.float32)


def ndarray_f32_wrong(x: np.ndarray) -> npt.NDArray[np.float32]:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    return x  # float64 under a float32 annotation


def ndarray_generic_wrong(x: np.ndarray) -> np.ndarray[Any, np.dtype[np.float32]]:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    return x  # float64 under a float32 annotation


def input_f32(x: npt.NDArray[np.float32]) -> str:
    return str(x.dtype)  # reports the dtype the function actually received


def optional_typing(x: np.ndarray) -> Optional[np.ndarray]:  # noqa: UP045 - testing this spelling  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test
    return None if x.ndim == 1 else x


def optional_pipe(x: np.ndarray) -> np.ndarray | None:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    return None if x.ndim == 1 else x


def list_of_arrays(x: np.ndarray) -> list[np.ndarray]:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    return [x, x.astype(np.int32)]


def dict_of_arrays(x: np.ndarray) -> dict[str, np.ndarray]:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    return {"orig": x, "neg": -x}


def two_arrays(x: np.ndarray) -> tuple[np.ndarray, np.ndarray]:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    return x, x.T.copy()


def summarize(fn, column: str = "y") -> tuple[str, str]:
    """Decorate ``fn`` as a pod here, so a rejected annotation is reported, not fatal."""
    keys = ["p", "q"] if column in ("p", "q") else "y"
    try:
        pod = function_pod(output_keys=keys)(fn).pod
    except Exception as exc:  # noqa: BLE001 - the rejection is the observation
        return "DEFECT", (
            f"pod construction raised {type(exc).__name__}: {' '.join(str(exc).split())[:200]}"
        )
    stream = pod(source())
    schema = stream.output_schema()[1]
    values = {t.as_dict()["k"]: d.as_dict()[column] for t, d in stream.iter_data()}
    try:
        arrow = str(stream.as_table().schema.field(column).type)
    except Exception as exc:  # noqa: BLE001 - reported inline
        arrow = f"as_table() raised {type(exc).__name__}: {' '.join(str(exc).split())[:150]}"
    shown = {k: describe(v) for k, v in values.items()}
    return "INFO", f"schema[{column}]={schema[column]}; arrow={arrow}; values={shown}"


def main() -> None:
    run_check("-> np.ndarray", lambda: summarize(plain))
    run_check(
        "-> NDArray[np.float32], returns float32", lambda: summarize(ndarray_f32_ok)
    )
    run_check(
        "-> NDArray[np.float32], returns float64 (dtype enforced?)",
        lambda: summarize(ndarray_f32_wrong),
    )
    run_check(
        "-> np.ndarray[Any, np.dtype[np.float32]], returns float64",
        lambda: summarize(ndarray_generic_wrong),
    )
    run_check(
        "param NDArray[np.float32] given float64 (what the function receives)",
        lambda: summarize(input_f32),
    )
    run_check(
        "-> Optional[np.ndarray] (None for 1-D)", lambda: summarize(optional_typing)
    )
    run_check("-> np.ndarray | None (None for 1-D)", lambda: summarize(optional_pipe))
    run_check("-> list[np.ndarray]", lambda: summarize(list_of_arrays))
    run_check("-> dict[str, np.ndarray]", lambda: summarize(dict_of_arrays))
    run_check(
        "-> tuple[np.ndarray, np.ndarray] (p)", lambda: summarize(two_arrays, "p")
    )
    run_check(
        "-> tuple[np.ndarray, np.ndarray] (q)", lambda: summarize(two_arrays, "q")
    )


if __name__ == "__main__":
    main()
