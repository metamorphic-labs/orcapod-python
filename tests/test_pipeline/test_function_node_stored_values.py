"""A function node's stored results read back exactly as they went in (ITL-1295).

Every read of stored results joined the pipeline table and the result table in Polars,
carrying every column through it and back: a ``date64`` tag came back at midnight (also
in a list or a struct), and a ``decimal256`` tag made Polars panic, so a second run over
the same database crashed. The reads are ``node.as_source()``, a run of a new job over
the same database (results reused), ``run(load_only=True)`` and a saved and loaded job.
Downstream, a join of a reused node found nothing, a node added over a reused node
stored the changed tag, and every reuse added pipeline entries under the changed tag.

Now Polars sees only the bookkeeping columns and row numbers, and every column is taken
from the stored tables. The graph here is a source with two tags, ``k`` (the type under
test) and ``n``, -> ``split`` (two outputs) -> ``double``; each read is compared with
the input bit for bit (temporal values as integers, floats as bits, decimals as text,
logical types through their storage, lists and structs element by element), on the
sync and the async orchestrator, over an in-memory database and over Delta Lake for the
types Delta can store.
"""

from __future__ import annotations

import decimal
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeAlias, cast

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.contexts import get_default_context
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator

Column: TypeAlias = "pa.Array[Any]"
D = decimal.Decimal
CONTEXT = get_default_context()


@function_pod(output_keys=("y", "w"))
def split(x: int) -> tuple[int, str]:
    """Return ``x + 1`` and a text naming ``x``."""
    return x + 1, f"w{x}"


@function_pod(output_keys="z")
def double(y: int) -> int:
    """Return ``2 * y``."""
    return 2 * y


# ── exact values ─────────────────────────────────────────────────────────────


def _exact(array: "Column | pa.ChunkedArray[Any]") -> list[Any]:
    """Return a column's values exactly, as plain Python values."""
    if isinstance(array, pa.ChunkedArray):
        array = array.combine_chunks()
    if isinstance(array, pa.ExtensionArray):
        return _exact(array.storage)
    arrow_type = array.type
    if pa.types.is_large_list(arrow_type) or pa.types.is_list(arrow_type):
        lists = cast("pa.LargeListArray[Any]", array)
        values = _exact(lists.values)
        offsets = cast(list[int], lists.offsets.to_pylist())
        return [values[offsets[i] : offsets[i + 1]] for i in range(len(lists))]
    if pa.types.is_struct(arrow_type):
        struct = cast("pa.StructArray", array)
        fields = [_exact(struct.field(i)) for i in range(arrow_type.num_fields)]
        return [tuple(f[j] for f in fields) for j in range(len(struct))]
    if pa.types.is_floating(arrow_type):
        unsigned = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}
        return array.view(unsigned[arrow_type.bit_width]).to_pylist()
    if pa.types.is_temporal(arrow_type):
        width = cast(int, arrow_type.bit_width)
        return array.view(pa.int32() if width == 32 else pa.int64()).to_pylist()
    if pa.types.is_decimal(arrow_type):
        return [str(value) for value in array.to_pylist()]
    return array.to_pylist()


def _ints(values: list[int], arrow_type: pa.DataType) -> Column:
    return pa.array(values, pa.int64()).cast(arrow_type)


def _floats(bits: list[int], width: int) -> Column:
    unsigned = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}[width]
    floating = {16: pa.float16(), 32: pa.float32(), 64: pa.float64()}[width]
    return pa.array(bits, unsigned).view(floating)


def _logical(python_type: type, storage: list[Any]) -> Column:
    arrow_type = cast(
        pa.BaseExtensionType,
        CONTEXT.type_converter.python_type_to_arrow_type(python_type),
    )
    return pa.ExtensionArray.from_storage(
        arrow_type, pa.array(storage, arrow_type.storage_type)
    )


def _lists(values: Column) -> Column:
    return pa.LargeListArray.from_arrays(
        pa.array(range(len(values) + 1), pa.int64()), values
    )


DATES = _ints([1, 86_400_001], pa.date64())
DECIMALS = pa.array([D("1." + "0" * 39 + "1"), D("-3")], pa.decimal256(76, 40))

AFFECTED: dict[str, Column] = {
    "date64 off midnight": DATES,
    "decimal256(76,40)": DECIMALS,
    "large_list<date64>": _lists(DATES),
    "large_list<decimal256>": _lists(DECIMALS),
    "struct<a: date64, b: decimal256>": pa.StructArray.from_arrays(
        [DATES, DECIMALS], names=["a", "b"]
    ),
    "struct<a: date64>": pa.StructArray.from_arrays([DATES], names=["a"]),
}
EXACT_ON_MAIN: dict[str, Column] = {
    "time64[ns]": _ints([1, 1001], pa.time64("ns")),
    "float32 signaling NaN": _floats([0x7F800001, 0x3F800000], 32),
    "float16 -0.0": _floats([0x8000, 0x3C00], 16),
    "uint64 max": pa.array([2**64 - 1, 0], pa.uint64()),
    "fixed_size_binary(3)": pa.array([b"abc", b"\x00\x00\x00"], pa.binary(3)),
    "timestamp[ns, America/New_York]": _ints(
        [1, 1001], pa.timestamp("ns", "America/New_York")
    ),
    "duration[ns]": _ints([1, 1001], pa.duration("ns")),
    "decimal128(38,10)": pa.array([D("1.0000000001"), D("-2")], pa.decimal128(38, 10)),
    "logical Path": _logical(Path, ["a", "é"]),
    "logical UUID": _logical(uuid.UUID, [uuid.UUID(int=n).bytes for n in (1, 2)]),
}
TYPES = {**AFFECTED, **EXACT_ON_MAIN}
# Delta Lake has no decimal256 and no float16 ("Invalid data type for Delta Lake").
NOT_IN_DELTA = {
    "decimal256(76,40)",
    "large_list<decimal256>",
    "struct<a: date64, b: decimal256>",
}

ORCHESTRATORS: dict[str, Callable[[], Any]] = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}


def _cases() -> list[Any]:
    params = []
    for name in TYPES:
        for database in ("memory", "delta"):
            marks = []
            if database == "delta" and name in NOT_IN_DELTA:
                marks = [pytest.mark.skip(reason="Delta Lake cannot store this type")]
            params.append(
                pytest.param(name, database, id=f"{name}-{database}", marks=marks)
            )
    return params


def _database(kind: str, tmp_path: Path) -> Any:
    return (
        InMemoryArrowDatabase()
        if kind == "memory"
        else DeltaTableDatabase(tmp_path / "db")
    )


# ── the graph ────────────────────────────────────────────────────────────────


def _source(values: Column, name: str = "s", data: str = "x") -> ArrowTableSource:
    """Tags ``k`` (``values``) and ``n``; data ``data``; record ids by position."""
    arrays = [values, pa.array([1, 2], pa.int64()), pa.array([10, 20], pa.int64())]
    schema = pa.schema(
        pa.field(column, array.type, nullable=False)
        for column, array in zip(["k", "n", data], arrays)
    )
    return ArrowTableSource(
        pa.Table.from_arrays(arrays, schema=schema),
        tag_columns=["k", "n"],
        tag_only_columns=True,
        label=name,
        source_id=name,
    )


def _job(database: Any, values: Column, *, with_two: bool = True) -> PipelineJob:
    job = PipelineJob(name="itl1295", database=database)
    with job:
        one = split.pod(_source(values), label="one")
        if with_two:
            double.pod(one, label="two")
    return job


def _k(stream: Any) -> list[Any]:
    """``k`` of a stream's table, and of its rows' tags: both must be one list."""
    table_k = sorted(map(repr, _exact(stream.as_table().column("k"))))
    rows_k = sorted(
        repr(value)
        for tag, _ in stream.iter_data()
        for value in _exact(tag.as_table().column("k"))
    )
    assert table_k == rows_k
    return table_k


def _want(values: Column) -> list[Any]:
    return sorted(map(repr, _exact(values)))


def _stored_rows(database: Any, function: str) -> pa.Table:
    """The function's stored pipeline table, read from the database directly."""
    [path] = [
        p
        for p in database.list_table_paths()
        if p[1] == function and p[-1].startswith("pdb")
    ]
    return database.get_all_records(path)


# ── every read ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("type_name", "database_kind"), _cases())
@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_every_read_of_stored_results_gives_the_tag_exactly(
    type_name: str, database_kind: str, orchestrator: str, tmp_path: Path
) -> None:
    values = TYPES[type_name]
    want = _want(values)
    database = _database(database_kind, tmp_path)
    make = ORCHESTRATORS[orchestrator]

    first = _job(database, values)
    first.run(orchestrator=make())
    for label in ("one", "two"):
        assert _k(first.nodes[label]) == want, f"after a run: {label}"
        assert _k(first.nodes[label].as_source()) == want, f"as_source: {label}"

    reuse = _job(database, values)
    summary = reuse.run(orchestrator=make())
    for label in ("one", "two"):
        assert (summary.nodes[label].computed, summary.nodes[label].reused) == (0, 2)
        assert _k(reuse.nodes[label]) == want, f"reuse run: {label}"

    loading = _job(database, values)
    loading.run(orchestrator=make(), load_only=True)
    for label in ("one", "two"):
        assert _k(loading.nodes[label]) == want, f"load_only: {label}"

    saved = tmp_path / "job.json"
    reuse.save(saved)
    loaded = PipelineJob.load(saved, database=database)
    for label in ("one", "two"):
        assert _k(loaded.nodes[label].as_source()) == want, f"loaded job: {label}"
        records = loaded.nodes[label].get_all_records()
        assert sorted(map(repr, _exact(records.column("k")))) == want

    # The outputs are the function's results, on every read.
    outputs = reuse.nodes["one"].as_table()
    assert sorted(outputs.column("y").to_pylist()) == [11, 21]
    assert sorted(outputs.column("w").to_pylist()) == ["w10", "w20"]
    assert sorted(reuse.nodes["two"].as_table().column("z").to_pylist()) == [22, 42]


# ── downstream consequences ──────────────────────────────────────────────────

DOWNSTREAM = pytest.mark.parametrize("type_name", list(AFFECTED))


@DOWNSTREAM
@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_node_added_over_a_reused_node_stores_the_exact_tag(
    type_name: str, orchestrator: str
) -> None:
    values = TYPES[type_name]
    database = InMemoryArrowDatabase()
    _job(database, values, with_two=False).run(
        orchestrator=ORCHESTRATORS[orchestrator]()
    )
    second = _job(database, values)
    summary = second.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert (summary.nodes["one"].computed, summary.nodes["one"].reused) == (0, 2)
    assert (summary.nodes["two"].computed, summary.nodes["two"].reused) == (2, 0)
    stored = _stored_rows(database, "double")
    assert sorted(map(repr, _exact(stored.column("k")))) == _want(values)
    assert _k(second.nodes["two"]) == _want(values)


@DOWNSTREAM
@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_reuse_run_adds_no_pipeline_entry(type_name: str, orchestrator: str) -> None:
    values = TYPES[type_name]
    database = InMemoryArrowDatabase()
    _job(database, values).run(orchestrator=ORCHESTRATORS[orchestrator]())
    before = {f: _stored_rows(database, f).num_rows for f in ("split", "double")}
    for _ in range(2):
        _job(database, values).run(orchestrator=ORCHESTRATORS[orchestrator]())
    after = {f: _stored_rows(database, f).num_rows for f in ("split", "double")}
    assert before == after == {"split": 2, "double": 2}


@pytest.mark.parametrize(
    "type_name", ["date64 off midnight", "struct<a: date64>", "large_list<date64>"]
)
@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_join_of_a_reused_node_finds_the_rows_of_the_first_run(
    type_name: str, orchestrator: str
) -> None:
    """A join of a node's results with a fresh source on the same tag values: the
    same rows on the first run and on a run that reuses the node."""
    values = TYPES[type_name]
    database = InMemoryArrowDatabase()
    rows = []
    for _ in range(2):
        job = PipelineJob(name="itl1295join", database=database)
        with job:
            one = split.pod(_source(values), label="one")
            Join()(one, _source(values, name="other", data="v"), label="joined")
        job.run(orchestrator=ORCHESTRATORS[orchestrator]())
        joined = job.nodes["joined"].as_table()
        rows.append(
            sorted(
                zip(
                    joined.column("n").to_pylist(),
                    joined.column("y").to_pylist(),
                    joined.column("v").to_pylist(),
                )
            )
        )
    assert rows[0] == rows[1] == [(1, 11, 10), (2, 21, 20)]


# ── a sorted read of stored list tags (the sort fallback, ITL-1295 part 2) ─────


def _no_panic(read: Callable[[], Any]) -> Any:
    """Run ``read``; a Polars panic (a ``BaseException``) becomes an assertion error."""
    try:
        return read()
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:
        raise AssertionError(f"{type(error).__name__}: {error}") from None


@pytest.mark.parametrize("type_name", ["large_list<date64>", "large_list<decimal256>"])
@pytest.mark.parametrize("read", ["as_table", "view"])
def test_a_sorted_read_of_a_list_tag_is_exact(type_name: str, read: str) -> None:
    """``as_table(columns={"sort_by_tags": True})`` and ``head().sort_by_tags()``."""
    values = TYPES[type_name]
    source = _source(values)
    if read == "view":
        table = _no_panic(lambda: source.head(10).sort_by_tags().as_table())
    else:
        table = _no_panic(lambda: source.as_table(columns={"sort_by_tags": True}))
    assert sorted(map(repr, _exact(table.column("k")))) == _want(values)
