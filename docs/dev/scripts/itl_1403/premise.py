"""ITL-1403: can a job reach the refusal of a recorded type change on its own tables?

The developer's premise: a table Orcapod's nodes write (function result, pipeline
records, operator cache, cached source, side-effect log) has its column definition fixed
by its path (a schema hash in the path), so no job can append rows whose recorded Arrow
type differs from the stored one. Each case below runs a job twice into one
``DeltaTableDatabase``, varying one thing between the runs, and reports whether the
second run was refused, and which stored tables it wrote: the same path (its rows had to
carry the same record) or a new one.

Cases (each where the Python schema may be equal but the Arrow record could differ):

- ``optional_fields``: a dataclass output whose instances leave an optional field
  ``None`` in one run and set it in the other.
- ``list_item_nulls``: a ``list[int]`` output holding ``None`` items in one run only.
- ``class_gains_field``: a user class output gaining a field between the runs (the same
  module and name, redefined).
- ``nullable_vs_not``: a source tag column ``T | None`` (nulls present) in one run, ``T``
  in the other.
- ``widths``: a source data column ``int32`` in one run, ``int64`` in the other.
- ``struct_gains_field``: a source data column, a struct with a recorded type
  (``uint8``), gaining a field between the runs, through a cached source, a function
  pod, an operator that logs and a side-effect pod.
- ``operator_nested_nullability`` (the stated EXCEPTION, found by review): a custom
  operator in a node that logs (``CacheMode.LOG``) whose result type depends on its
  data, not on its input schemas: ``large_list<uint8>`` with non-nullable items in one
  run and nullable items in the next. Its declared and observed Python schemas are
  equal and its cache path is the same, but the recorded type differs (nested
  nullability is part of the record), so the second run fails at the store. On
  deltalake 1.0.2 both runs were stored and the table's recorded type changed
  silently. Such an operator is a bug of the operator (an operator's output schema is
  fully determined by its inputs' schemas); ITL-1410 is to refuse it at the operator.

usage: uv run python docs/dev/scripts/itl_1403/premise.py WORKDIR
"""

import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.sources import ArrowTableSource
from orcapod.core.sources.cached_source import CachedSource
from orcapod.databases import DeltaTableDatabase
from orcapod.side_effects import SideEffectPod
from orcapod.types import CacheMode


@dataclass
class Point:
    x: float
    label: str | None


# The same module path and name in both runs; the second run (another process) has a
# field more.
_SHAPE_EXTRA = os.environ.get("ITL1403_SHAPE_EXTRA") == "1"


@dataclass
class Shape:
    x: float
    if _SHAPE_EXTRA:
        y: float = 0.0


def _shape(v: float) -> Shape:
    return Shape(v, v) if _SHAPE_EXTRA else Shape(v)


@function_pod(output_keys=["s"])
def to_shape(v: float) -> Shape:
    return _shape(v)


def _source(
    name: str, columns: dict[str, "pa.Array[Any]"], tags: list[str]
) -> ArrowTableSource:
    schema = pa.schema(
        [pa.field(n, a.type, nullable=a.null_count > 0) for n, a in columns.items()]
    )
    return ArrowTableSource(
        pa.Table.from_arrays(list(columns.values()), schema=schema),
        tag_columns=tags,
        tag_only_columns=True,
        source_id=name,
        record_id_columns=["k"],
    )


def _keys(n: int) -> "pa.Array[Any]":
    return pa.array([f"k{i}" for i in range(n)], pa.large_string())


def notify(ctx: Any, v: Any) -> None:
    return None


def _job(store: Path, source: ArrowTableSource, pod: Any, data: str) -> None:
    """Every stored table kind over ``source``: cached source, function pod, a logging
    operator node and a side-effect pod."""
    database = DeltaTableDatabase(store)
    job = PipelineJob(name="premise", database=database)
    cached = CachedSource(source, cache_database=database)
    other = _source(
        "other", {"k": _keys(2), "w": pa.array([1.0, 2.0], pa.float64())}, ["k"]
    )
    with job:
        pod(cached, label="fn")
        Join()(cached, other, label="joined")
        SideEffectPod(notify)(cached.select_data_columns([data]), label="notify")
    job.nodes["joined"].attach_databases(
        pipeline_database=database, cache_mode=CacheMode.LOG
    )
    job.run()
    database.flush()


def _tables(store: Path) -> set[str]:
    return {"/".join(p) for p in DeltaTableDatabase(store).list_table_paths()}


@function_pod(output_keys=["out"])
def identity_v(v: Any) -> Any:  # replaced per case with a typed pod
    return v


def _case(store: Path, first: Callable[[], None], second: Callable[[], None]) -> str:
    shutil.rmtree(store, ignore_errors=True)
    first()
    before = _tables(store)
    try:
        second()
    except Exception as e:  # noqa: BLE001
        return f"REFUSED on the second run: {type(e).__name__}: {str(e)[:300]}"
    after = _tables(store)
    new = sorted(after - before)
    return (
        f"no refusal; {len(before)} tables after run 1, {len(new)} new after run 2"
        + "".join(f"\n      new: {t[:110]}" for t in new)
    )


class ShapeValues(UnaryOperator):
    """Lists of one Python type whose items' Arrow nullability follows the values."""

    def validate_unary_input(self, stream: Any) -> None:
        pass

    def identity_structure(self) -> Any:
        return ("itl1403-shape-values",)

    def unary_output_schema(self, stream: Any, **kwargs: Any) -> Any:
        tags, _ = stream.output_schema(**kwargs)
        values = pa.array([], type=pa.large_list(pa.field("item", pa.uint8())))
        _, data = ArrowTableStream(pa.table({"v": values})).output_schema(**kwargs)
        return tags, data

    def unary_static_process(self, stream: Any) -> Any:
        table = stream.as_table(columns={"source": True, "system_tags": True})
        values = table["v"].to_pylist()
        nullable = any(v > 10 for v in values)
        shape = pa.large_list(pa.field("item", pa.uint8(), nullable=nullable))
        wrapped = pa.array([[v] for v in values], shape)
        table = table.set_column(table.schema.get_field_index("v"), "v", wrapped)
        return ArrowTableStream(table, tag_columns=stream.keys()[0])


def _operator_job(store: Path, rerun: bool) -> None:
    database = DeltaTableDatabase(store)
    source = ArrowTableSource(
        pa.table(
            {"k": [3, 4] if rerun else [1, 2], "v": [11, 12] if rerun else [1, 2]}
        ),
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="input",
        record_id_columns=["k"],
    )
    job = PipelineJob(name="premise-operator", database=database)
    with job:
        ShapeValues()(source, label="shape")
    job.nodes["shape"].attach_databases(database, cache_mode=CacheMode.LOG)
    job.run()
    database.flush()


def main(work: Path) -> None:
    results: dict[str, str] = {}

    # optional_fields
    @function_pod(output_keys=["p"])
    def to_point(v: float) -> Point:
        return Point(v, None if v < 10 else f"p{v}")

    def run_points(values: list[float]) -> Callable[[], None]:
        src = _source("s", {"k": _keys(2), "v": pa.array(values, pa.float64())}, ["k"])
        return lambda: _job(work / "optional_fields", src, to_point.pod, "v")

    results["optional_fields"] = _case(
        work / "optional_fields", run_points([1.0, 2.0]), run_points([11.0, 12.0])
    )

    # list_item_nulls
    @function_pod(output_keys=["items"])
    def to_items(v: float) -> list[int | None]:
        return [int(v), None] if v > 10 else [int(v)]

    def run_items(values: list[float]) -> Callable[[], None]:
        src = _source("s", {"k": _keys(2), "v": pa.array(values, pa.float64())}, ["k"])
        return lambda: _job(work / "list_item_nulls", src, to_items.pod, "v")

    results["list_item_nulls"] = _case(
        work / "list_item_nulls", run_items([1.0, 2.0]), run_items([11.0, 12.0])
    )

    # class_gains_field: each run in its own process (see ``Shape``).
    def run_shape(extra: bool) -> Callable[[], None]:
        def go() -> None:
            subprocess.run(
                [sys.executable, __file__, "shape", str(work / "class_gains_field")],
                env={**os.environ, "ITL1403_SHAPE_EXTRA": "1" if extra else "0"},
                check=True,
            )

        return go

    results["class_gains_field"] = _case(
        work / "class_gains_field", run_shape(False), run_shape(True)
    )

    # nullable_vs_not: a tag T | None (nulls present), then T.
    @function_pod(output_keys=["d"])
    def double(v: float) -> float:
        return 2 * v

    def run_tag(nullable: bool) -> Callable[[], None]:
        t = pa.array([1, None] if nullable else [1, 2], pa.int64())
        src = ArrowTableSource(
            pa.Table.from_arrays(
                [_keys(2), t, pa.array([1.0, 2.0], pa.float64())],
                schema=pa.schema(
                    [
                        pa.field("k", pa.large_string(), nullable=False),
                        pa.field("t", pa.int64(), nullable=nullable),
                        pa.field("v", pa.float64(), nullable=False),
                    ]
                ),
            ),
            tag_columns=["k", "t"],
            tag_only_columns=True,
            source_id="s",
            record_id_columns=["k"],
        )
        return lambda: _job(work / "nullable_vs_not", src, double.pod, "v")

    results["nullable_vs_not"] = _case(
        work / "nullable_vs_not", run_tag(True), run_tag(False)
    )

    # widths: an int32 data column, then int64.
    @function_pod(output_keys=["d"])
    def widen(v: int) -> int:
        return v + 1

    def run_width(width: pa.DataType) -> Callable[[], None]:
        src = _source("s", {"k": _keys(2), "v": pa.array([1, 2], width)}, ["k"])
        return lambda: _job(work / "widths", src, widen.pod, "v")

    results["widths"] = _case(
        work / "widths", run_width(pa.int32()), run_width(pa.int64())
    )

    # struct_gains_field: a recorded struct data column (uint8 inside) gaining a field.
    @function_pod(output_keys=["n"])
    def count(v: Any) -> int:
        return 1

    def run_struct(extra: bool) -> Callable[[], None]:
        fields = [pa.field("a", pa.uint8())] + (
            [pa.field("b", pa.uint8())] if extra else []
        )
        values = [{"a": 1, "b": 2}, {"a": 3, "b": 4}] if extra else [{"a": 1}, {"a": 3}]
        src = _source(
            "s", {"k": _keys(2), "v": pa.array(values, pa.struct(fields))}, ["k"]
        )
        return lambda: _job(work / "struct_gains_field", src, count.pod, "v")

    results["struct_gains_field"] = _case(
        work / "struct_gains_field", run_struct(False), run_struct(True)
    )

    # The known exception: the second run is refused.
    results["operator_nested_nullability (known exception)"] = _case(
        work / "operator_nested_nullability",
        lambda: _operator_job(work / "operator_nested_nullability", False),
        lambda: _operator_job(work / "operator_nested_nullability", True),
    )

    for name, outcome in results.items():
        print(f"{name}: {outcome}")


if __name__ == "__main__":
    if sys.argv[1] == "shape":
        store = Path(sys.argv[2])
        src = _source(
            "s", {"k": _keys(2), "v": pa.array([1.0, 2.0], pa.float64())}, ["k"]
        )
        _job(store, src, to_shape.pod, "v")
    else:
        main(Path(sys.argv[1]))
