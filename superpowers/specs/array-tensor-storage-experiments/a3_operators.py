"""A3: NumPy arrays through function pods and every operator, via the public interface (ITL-874).

Claims tested:

* A function pod can return an ``np.ndarray``, take one as input, and two such pods chain.
* Every operator in ``orcapod.operators`` runs over a stream whose data column holds arrays,
  and each array read back through ``iter_data`` keeps its dtype, shape and values.
  The sample column mixes dtypes, shapes and memory orders on purpose (float32 C order,
  float64 Fortran order, int16 1-D, a zero-dimensional float32).
* The output schema and the Arrow type of the array column after each operator.
* Whether Fortran order survives (the ``.npy`` envelope records ``fortran_order``).

Run: ``uv run python superpowers/specs/array-tensor-storage-experiments/a3_operators.py``
"""

from __future__ import annotations

from typing import Any, cast

import numpy as np
import numpy.typing as npt
import polars as pl

from a3_common import describe, report, run_check, same_array, sample_arrays  # pyright: ignore[reportImplicitRelativeImport]  # run as a script: sys.path[0] is this directory
from orcapod import function_pod
from orcapod.operators import (
    Batch,
    DropDataColumns,
    DropTagColumns,
    GroupBy,
    Index,
    Join,
    MapData,
    MergeJoin,
    Pick,
    PolarsFilter,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.sources import DictSource

ARRAYS = sample_arrays()
# np.asarray: arithmetic on a 0-d array returns an np.generic scalar, not an ndarray.
OTHER = {k: np.asarray(v + 100, dtype=v.dtype) for k, v in ARRAYS.items()}
GROUP = {"a": "g1", "b": "g1", "c": "g2", "d": "g2"}


def base_source() -> DictSource:
    """Tags ``k`` and ``g``; data column ``x`` holds the sample arrays."""
    return DictSource(
        data=[{"k": k, "g": GROUP[k], "x": v} for k, v in ARRAYS.items()],
        tag_columns=["k", "g"],
        source_id="arrays_x",
    )


def other_source(column: str, keys: list[str] | None = None) -> DictSource:
    """Tag ``k``; data column ``column`` holds ``ARRAYS + 100`` for ``keys``."""
    keys = keys or list(ARRAYS)
    return DictSource(
        data=[{"k": k, column: OTHER[k]} for k in keys],
        tag_columns=["k"],
        source_id=f"arrays_{column}_{'_'.join(keys)}",
    )


def rows(stream) -> dict[str, dict[str, Any]]:
    """Map each row's ``k`` tag (or the full tag tuple) to its data dict."""
    out: dict[str, dict[str, Any]] = {}
    for tag, data in stream.iter_data():
        t = tag.as_dict()
        key = (
            t["k"] if "k" in t and isinstance(t["k"], str) else repr(sorted(t.items()))
        )
        out[key] = data.as_dict()
    return out


def arrow_type(stream, column: str) -> str:
    return str(stream.as_table().schema.field(column).type)


def check_arrays(
    stream, column: str, want: dict[str, npt.NDArray[Any]]
) -> tuple[str, str]:
    """Compare ``column`` per ``k`` with ``want``. Return a status and a summary."""
    got = rows(stream)
    bad = [k for k, v in want.items() if not same_array(got[k][column], v)]
    schema = stream.output_schema()[1]
    summary = (
        f"rows={len(got)} schema[{column}]={schema[column]} "
        f"arrow={arrow_type(stream, column)}"
    )
    if bad:
        detail = {k: describe(got[k][column]) for k in bad}
        return "DEFECT", f"{summary}; mismatched rows {detail}"
    return "OK", summary + "; dtype, shape and values preserved"


# --- function pods -------------------------------------------------------


@function_pod(output_keys="y")
def double(x: np.ndarray) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    return np.asarray(x * 2)


@function_pod(output_keys="y")
def double_raw(x: np.ndarray) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    return x * 2  # 0-d input gives an np.float32 scalar, not an ndarray


@function_pod(output_keys="z")
def add_one(y: np.ndarray) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    return np.asarray(y + 1)


@function_pod(output_keys="total")
def total(x: np.ndarray) -> float:  # pyright: ignore[reportMissingTypeArgument]  # pod annotation under test: keep the bare np.ndarray spelling
    return float(np.sum(x))


def pods() -> None:
    src = base_source()
    run_check(
        "source holds arrays",
        lambda: check_arrays(src, "x", ARRAYS),
    )
    run_check(
        "pod returns ndarray (x*2)",
        lambda: check_arrays(
            double.pod(src), "y", {k: v * 2 for k, v in ARRAYS.items()}
        ),
    )
    run_check(
        "pod takes ndarray, returns float",
        lambda: {k: d["total"] for k, d in rows(total.pod(src)).items()},
    )
    run_check(
        "two pods chained (x*2+1)",
        lambda: check_arrays(
            add_one.pod(double.pod(src)), "z", {k: v * 2 + 1 for k, v in ARRAYS.items()}
        ),
    )

    def generic_return() -> tuple[str, str]:
        """A pod annotated ``-> np.ndarray`` that returns ``x * 2`` for a 0-d array.

        NumPy returns an ``np.float32`` scalar there. Does Orcapod reject it, convert it, or
        store it?
        """
        out = double_raw.pod(src)
        kinds = {t.as_dict()["k"]: type(d["y"]).__name__ for t, d in out.iter_data()}
        try:
            out.as_table()
        except Exception as exc:  # noqa: BLE001 - the failure is the observation
            return "DEFECT", (
                f"iter_data yields {kinds} without a type check; as_table() then raises "
                f"{type(exc).__name__}: {' '.join(str(exc).split())[:200]}"
            )
        return "INFO", f"iter_data yields {kinds}; as_table() succeeded"

    run_check("pod -> np.ndarray returning np.float32 (0-d arithmetic)", generic_return)

    def fortran_kept() -> tuple[str, str]:
        got = rows(src)["b"]["x"]
        status = "OK" if got.flags["F_CONTIGUOUS"] else "INFO"
        return status, f"row b read back as {describe(got)}"

    run_check("Fortran order survives the source", fortran_kept)


# --- operators -----------------------------------------------------------


def operators() -> None:
    src = base_source()

    run_check(
        "Join (x with y on k)",
        lambda: check_arrays(Join()(src, other_source("y")), "x", ARRAYS),
    )
    run_check(
        "Join keeps the other side's arrays",
        lambda: check_arrays(Join()(src, other_source("y")), "y", OTHER),
    )

    def merge_join() -> tuple[str, str]:
        left = other_source("x")
        right = DictSource(
            data=[{"k": k, "x": v} for k, v in ARRAYS.items()],
            tag_columns=["k"],
            source_id="arrays_x_right",
        )
        out = MergeJoin()(left, right)
        got = rows(out)
        lines = []
        status = "OK"
        for k in ARRAYS:
            members = got[k]["x"]
            ok = (
                isinstance(members, list)
                and len(members) == 2
                and any(same_array(m, ARRAYS[k]) for m in members)
                and any(same_array(m, OTHER[k]) for m in members)
            )
            if not ok:
                status = "DEFECT"
            lines.append(f"{k}:{describe(members)}")
        schema = out.output_schema()[1]["x"]
        return status, (
            f"schema[x]={schema} arrow={arrow_type(out, 'x')}; " + "; ".join(lines[:2])
        )

    run_check("MergeJoin with a colliding array column", merge_join)

    def merge_join_order() -> str:
        """Which order MergeJoin puts the two arrays in (it sorts the merged list)."""
        left = other_source("x")
        right = DictSource(
            data=[{"k": k, "x": v} for k, v in ARRAYS.items()],
            tag_columns=["k"],
            source_id="arrays_x_right",
        )
        got = rows(MergeJoin()(left, right))
        firsts = {
            k: ("orig" if same_array(got[k]["x"][0], ARRAYS[k]) else "plus100")
            for k in ARRAYS
        }
        return f"first member per row: {firsts}"

    run_check("MergeJoin member order", merge_join_order)

    def merge_join_dtype_mismatch() -> tuple[str, str]:
        """Same column name, float32 on one side, float64 on the other.

        For a plain Arrow column a type disagreement raises ``InputValidationError``. Here
        the dtype lives inside each ``.npy`` value, not in the column type.
        """
        left = DictSource(
            data=[{"k": "a", "x": np.ones(3, dtype=np.float32)}],
            tag_columns=["k"],
            source_id="mm_left",
        )
        right = DictSource(
            data=[{"k": "a", "x": np.ones(3, dtype=np.float64)}],
            tag_columns=["k"],
            source_id="mm_right",
        )
        try:
            out = MergeJoin()(left, right)
            members = rows(out)["a"]["x"]
        except Exception as exc:  # noqa: BLE001 - the rejection is the observation
            return "INFO", f"rejected: {type(exc).__name__}: {exc}"
        return "INFO", (
            f"accepted; schema[x]={out.output_schema()[1]['x']}; members={describe(members)}"
        )

    run_check(
        "MergeJoin float32 vs float64 under one column", merge_join_dtype_mismatch
    )

    run_check(
        "SemiJoin (keep k in {a, c})",
        lambda: check_arrays(
            SemiJoin()(src, other_source("w", ["a", "c"])),
            "x",
            {k: ARRAYS[k] for k in ("a", "c")},
        ),
    )

    def batch() -> tuple[str, str]:
        out = Batch(batch_size=2)(src)
        table = out.as_table()
        got = [cast(list[Any], d.as_dict()["x"]) for _, d in out.iter_data()]
        flat = [a for members in got for a in members]
        want = list(ARRAYS.values())
        ok = len(flat) == len(want) and all(
            any(same_array(a, w) for a in flat) for w in want
        )
        return ("OK" if ok else "DEFECT"), (
            f"schema[x]={out.output_schema()[1]['x']} "
            f"arrow={table.schema.field('x').type}; batches={[describe(b) for b in got]}"
        )

    run_check("Batch(batch_size=2)", batch)

    def group_by() -> tuple[str, str]:
        out = GroupBy(by=["g"])(src)
        got = {
            tag.as_dict()["g"]: cast(list[Any], data.as_dict()["x"])
            for tag, data in out.iter_data()
        }
        ok = all(
            sum(
                any(same_array(m, ARRAYS[k]) for m in got[g])
                for k in ARRAYS
                if GROUP[k] == g
            )
            == 2
            for g in ("g1", "g2")
        )
        return ("OK" if ok else "DEFECT"), (
            f"schema[x]={out.output_schema()[1]['x']} "
            f"arrow={arrow_type(out, 'x')}; groups={ {g: describe(v) for g, v in got.items()} }"
        )

    run_check("GroupBy(by=['g'])", group_by)

    def index_after_batch() -> tuple[str, str]:
        batched = Batch(batch_size=2)(src)
        out = Index(column="x", i=0)(batched)
        firsts = [d.as_dict()["x"] for _, d in out.iter_data()]
        ok = all(any(same_array(f, w) for w in ARRAYS.values()) for f in firsts)
        return ("OK" if ok else "DEFECT"), (
            f"schema[x]={out.output_schema()[1]['x']} arrow={arrow_type(out, 'x')}; "
            f"{[describe(f) for f in firsts]}"
        )

    run_check("Index(x, 0) over Batch output", index_after_batch)

    def index_after_group_by() -> tuple[str, str]:
        out = Index(column="x", i=-1, out="last")(GroupBy(by=["g"])(src))
        got = [d.as_dict()["last"] for _, d in out.iter_data()]
        ok = all(any(same_array(f, w) for w in ARRAYS.values()) for f in got)
        return ("OK" if ok else "DEFECT"), (
            f"schema[last]={out.output_schema()[1]['last']} "
            f"arrow={arrow_type(out, 'last')}; {[describe(f) for f in got]}"
        )

    run_check("Index(x, -1, out='last') over GroupBy output", index_after_group_by)

    def pick_on_array() -> tuple[str, str]:
        try:
            out = Pick(column="x", key="f0")(src)
        except Exception as exc:  # noqa: BLE001 - the rejection is the observation
            return "INFO", f"rejected with {type(exc).__name__}: {exc}"
        return "INFO", f"built: {out.output_schema()}"

    run_check(
        "Pick(x, 'f0') on an ndarray column (expected to be rejected)", pick_on_array
    )

    run_check(
        "MapData({'x': 'renamed'})",
        lambda: check_arrays(MapData({"x": "renamed"})(src), "renamed", ARRAYS),
    )
    run_check(
        "SelectDataColumns(['x']) after Join",
        lambda: check_arrays(
            SelectDataColumns(["x"])(Join()(src, other_source("y"))), "x", ARRAYS
        ),
    )
    run_check(
        "DropDataColumns(['y']) after Join",
        lambda: check_arrays(
            DropDataColumns(["y"])(Join()(src, other_source("y"))), "x", ARRAYS
        ),
    )
    run_check(
        "SelectTagColumns(['k'])",
        lambda: check_arrays(SelectTagColumns(["k"])(src), "x", ARRAYS),
    )
    run_check(
        "DropTagColumns(['g'])",
        lambda: check_arrays(DropTagColumns(["g"])(src), "x", ARRAYS),
    )
    run_check(
        "PolarsFilter(pl.col('g') == 'g1')",
        lambda: check_arrays(
            PolarsFilter(predicates=[pl.col("g") == "g1"])(src),
            "x",
            {k: v for k, v in ARRAYS.items() if GROUP[k] == "g1"},
        ),
    )

    def filter_fortran() -> tuple[str, str]:
        out = PolarsFilter(predicates=[pl.col("k") == "b"])(src)
        got = rows(out)["b"]["x"]
        status = "OK" if got.flags["F_CONTIGUOUS"] else "INFO"
        return status, f"row b after the Polars round trip: {describe(got)}"

    run_check("Fortran order survives PolarsFilter", filter_fortran)


def main() -> None:
    report("INFO", "numpy", np.__version__)
    pods()
    operators()


if __name__ == "__main__":
    main()
