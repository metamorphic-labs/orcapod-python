"""ITL-1295: does a function node present (and store) the values that went in?

usage: uv run python docs/dev/scripts/itl_1295/measure_read_path.py [memory|delta] [name]

For each Arrow type T, a source with tags ``k`` (T) and ``n`` (int64) and data ``x``
(int64); ``inc(x) -> y`` (node ``one``), ``dbl(y) -> z`` over it (node ``two``). Then:

* ``run``: a job run (sync orchestrator); ``k`` as node ``one`` and ``two`` present it
  (``as_table``) and as ``iter_data`` tags hold it;
* ``stored``: ``k`` in every stored table of the database that has a ``k`` column, read
  directly;
* ``as_source``: ``node.as_source().as_table()``;
* ``reuse``: a NEW job object over the same database, same graph, run again: the run
  summary's computed / reused counts, and ``k`` as presented;
* ``load_only``: another new job, ``run(load_only=True)``;
* ``in_memory``: the same graph without a job (``FunctionPodStream``), no database;
* ``head_sorted``: ``as_table(sort_by_tags=True)`` of the source (``sort_by_tag_columns``
  falls back to Polars for types Arrow cannot sort).

Each value is compared with the input exactly (temporal values as integers, floats as
bits, decimals as text, logical types through storage, lists and structs recursively).
A Polars panic is a ``BaseException`` and is reported.
"""

from __future__ import annotations

import decimal
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any, cast

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.contexts import get_default_context
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import SyncPipelineOrchestrator

CTX = get_default_context()
D = decimal.Decimal


@function_pod(output_keys="y")
def inc(x: int) -> int:
    return x + 1


@function_pod(output_keys="z")
def dbl(y: int) -> int:
    return 2 * y


def exact(array: Any) -> list[Any]:
    if isinstance(array, pa.ChunkedArray):
        array = array.combine_chunks()
    if isinstance(array, pa.ExtensionArray):
        return exact(array.storage)
    if isinstance(array, pa.DictionaryArray):
        return exact(array.dictionary_decode())
    t = array.type
    if pa.types.is_large_list(t) or pa.types.is_list(t):
        values = exact(array.values)
        offsets = array.offsets.to_pylist()
        return [
            values[offsets[i] : offsets[i + 1]] if array[i].is_valid else None
            for i in range(len(array))
        ]
    if pa.types.is_struct(t):
        fields = [exact(array.field(i)) for i in range(t.num_fields)]
        return [
            tuple(f[j] for f in fields) if array[j].is_valid else None
            for j in range(len(array))
        ]
    if pa.types.is_floating(t):
        unsigned = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}[t.bit_width]
        return [None if b is None else hex(b) for b in array.view(unsigned).to_pylist()]
    if pa.types.is_temporal(t) and not pa.types.is_interval(t):
        return array.view(pa.int32() if t.bit_width == 32 else pa.int64()).to_pylist()
    if pa.types.is_decimal(t):
        return [None if x is None else str(x) for x in array.to_pylist()]
    return array.to_pylist()


def ints(values: list[int], arrow_type: pa.DataType) -> Any:
    return pa.array(values, pa.int64()).cast(arrow_type)


def floats(bits: list[int], width: int) -> Any:
    unsigned = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}[width]
    floating = {16: pa.float16(), 32: pa.float32(), 64: pa.float64()}[width]
    return pa.array(bits, unsigned).view(floating)


def logical(python_type: type, storage: list[Any]) -> Any:
    t = cast(
        pa.BaseExtensionType, CTX.type_converter.python_type_to_arrow_type(python_type)
    )
    return pa.ExtensionArray.from_storage(t, pa.array(storage, t.storage_type))


def one_list_each(values: Any) -> Any:
    return pa.LargeListArray.from_arrays(
        pa.array(range(len(values) + 1), pa.int64()), values
    )


DEC = pa.decimal256(76, 40)
DATES = ints([1, 86_400_001], pa.date64())
DECS = pa.array([D("1." + "0" * 39 + "1"), D("-3")], DEC)

CASES: dict[str, Any] = {
    "int64 (control)": pa.array([1, 2], pa.int64()),
    "date64 off midnight": DATES,
    "decimal256(76,40)": DECS,
    "large_list<date64>": one_list_each(DATES),
    "large_list<decimal256>": one_list_each(DECS),
    "struct<a: date64>": pa.StructArray.from_arrays([DATES], names=["a"]),
    "struct<a: decimal256>": pa.StructArray.from_arrays([DECS], names=["a"]),
    "time64[ns]": ints([1, 1001], pa.time64("ns")),
    "timestamp[ns, UTC]": ints([1, 1001], pa.timestamp("ns", "UTC")),
    "float32 sNaN": floats([0x7F800001, 0x3F800000], 32),
    "float16 -0.0": floats([0x8000, 0x3C00], 16),
    "float64 payload NaN": floats([0x7FF8000000000001, 0x8000000000000000], 64),
    "logical Path": logical(Path, ["a", "b"]),
    "logical UUID": logical(
        uuid.UUID, [uuid.UUID(int=1).bytes, uuid.UUID(int=2).bytes]
    ),
}


def source(values: Any) -> ArrowTableSource:
    arrays = [values, pa.array([1, 2], pa.int64()), pa.array([10, 20], pa.int64())]
    schema = pa.schema(
        pa.field(name, a.type, nullable=False) for name, a in zip("knx", arrays)
    )
    table = pa.Table.from_arrays(arrays, schema=schema)
    return ArrowTableSource(
        table, tag_columns=["k", "n"], tag_only_columns=True, source_id="s"
    )


def build(values: Any, db: Any) -> PipelineJob:
    job = PipelineJob(name="itl1295", database=db)
    with job:
        one = inc.pod(source(values), label="one")
        dbl.pod(one, label="two")
    return job


def verdict(got: Any, want: list[Any]) -> str:
    return (
        "exact"
        if sorted(map(repr, got)) == sorted(map(repr, want))
        else f"CHANGED {got}"
    )


def attempt(label: str, action: Any, want: list[Any]) -> str:
    try:
        return verdict(action(), want)
    except BaseException as error:  # noqa: BLE001  # a Polars panic is a BaseException
        kind = "PANIC" if "Panic" in type(error).__name__ else "RAISES"
        return f"{kind} {type(error).__name__}: {str(error)[:80]}"


def node_k(job: PipelineJob, label: str) -> list[Any]:
    return exact(job.nodes[label].as_table().column("k"))


def iter_k(job: PipelineJob, label: str) -> list[Any]:
    out: list[Any] = []
    for tag, _ in job.nodes[label].iter_data():
        out.extend(exact(tag.as_table().column("k")))
    return out


def stored_k(db: Any) -> list[str]:
    lines = []
    for path in db.list_table_paths():
        table = db.get_all_records(path)
        if table is not None and "k" in table.column_names:
            lines.append(f"{'/'.join(path)[-40:]}: {exact(table.column('k'))}")
    return lines


def measure(name: str, values: Any, kind: str) -> None:
    want = exact(values)
    results: dict[str, str] = {}
    with tempfile.TemporaryDirectory() as tmp:
        db = (
            InMemoryArrowDatabase()
            if kind == "memory"
            else DeltaTableDatabase(Path(tmp) / "db")
        )
        job = build(values, db)
        try:
            job.run(orchestrator=SyncPipelineOrchestrator())
            results["run"] = "ok"
        except BaseException as error:  # noqa: BLE001
            results["run"] = f"RAISES {type(error).__name__}: {str(error)[:100]}"
        for label in ("one", "two"):
            results[f"run {label} as_table"] = attempt(
                label, lambda: node_k(job, label), want
            )
            results[f"run {label} iter_data"] = attempt(
                label, lambda: iter_k(job, label), want
            )
            results[f"run {label} as_source"] = attempt(
                label,
                lambda: exact(job.nodes[label].as_source().as_table().column("k")),
                want,
            )
        try:
            for line in stored_k(db):
                stored_values = line.split(": ", 1)[1]
                results[f"stored {line.split(':')[0]}"] = (
                    "exact"
                    if stored_values == repr(want)
                    or sorted(eval(stored_values), key=repr) == sorted(want, key=repr)
                    else f"CHANGED {stored_values}"  # noqa: S307
                )
        except BaseException as error:  # noqa: BLE001
            results["stored"] = f"RAISES {type(error).__name__}: {str(error)[:80]}"
        reuse = build(values, db)
        try:
            summary = reuse.run(orchestrator=SyncPipelineOrchestrator())
            results["reuse counts"] = ", ".join(
                f"{label}: computed {summary.nodes[label].computed} reused {summary.nodes[label].reused}"
                for label in ("one", "two")
            )
        except BaseException as error:  # noqa: BLE001
            results["reuse counts"] = (
                f"RAISES {type(error).__name__}: {str(error)[:100]}"
            )
        for label in ("one", "two"):
            results[f"reuse {label} as_table"] = attempt(
                label, lambda: node_k(reuse, label), want
            )
        loaded = build(values, db)
        try:
            loaded.run(orchestrator=SyncPipelineOrchestrator(), load_only=True)
            results["load_only one as_table"] = attempt(
                "one", lambda: node_k(loaded, "one"), want
            )
        except BaseException as error:  # noqa: BLE001
            results["load_only"] = f"RAISES {type(error).__name__}: {str(error)[:100]}"
    stream = dbl.pod(inc.pod(source(values)))
    results["in_memory as_table"] = attempt(
        "mem", lambda: exact(stream.as_table().column("k")), want
    )
    results["head_sorted source"] = attempt(
        "sorted",
        lambda: exact(
            source(values).as_table(columns={"sort_by_tags": True}).column("k")
        ),
        want,
    )
    print(f"== {name} [{kind}]")
    for key, value in results.items():
        if value not in ("exact", "ok"):
            print(f"   {key:28} {value}")
    print(
        f"   ({sum(v in ('exact', 'ok') for v in results.values())} of {len(results)} exact/ok)"
    )


def main() -> None:
    args = sys.argv[1:]
    kind = args[0] if args else "memory"
    only = args[1] if len(args) > 1 else None
    for name, values in CASES.items():
        if only is None or only in name:
            measure(name, values, kind)


if __name__ == "__main__":
    main()
