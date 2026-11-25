"""Rows held in chunks read exactly as the pairs that were held (ITL-1464)."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime as dt
import errno
import math
import os
import uuid
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.channels import Channel
from orcapod.core import held_rows
from orcapod.core.datagrams import Data, EmptyData, Tag
from orcapod.core.held_rows import HeldPairs, HeldRows, RowTypes, _same
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.side_effects import side_effect_pod
from orcapod.sources import ArrowTableSource
from orcapod.system_constants import constants

_TYPES = RowTypes(Tag, Data)
_N = 11


@pytest.fixture()
def spilled(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Chunks of three rows, every chunk on disk."""
    monkeypatch.setattr(held_rows, "HELD_CHUNK_ROWS", 3)
    monkeypatch.setattr(held_rows, "HELD_MEMORY_BYTES", 0)
    root = tmp_path / "spool"
    root.mkdir()
    monkeypatch.setattr("tempfile.tempdir", str(root))
    return root


def _files(root: Path) -> list[Path]:
    return [p for p in root.rglob("*") if p.is_file() and not p.name.endswith("lock")]


@dataclasses.dataclass
class Point:
    x: int


@dataclasses.dataclass
class FarPoint(Point):
    """A subclass of the declared class: the row keeps the returned instance."""

    y: int


class Odd(Data):
    """A subclass of ``Data``: never built again as a plain one."""


def _arrow_rows() -> list[tuple[Any, Any]]:
    table = pa.table(
        {
            "subject": pa.array([f"s{n % 3}" for n in range(_N)], pa.large_string()),
            "trial": pa.array(range(_N), pa.int8()),
            "when": pa.array(
                [dt.datetime(2026, 1, 1, n) for n in range(_N)],
                pa.timestamp("us", "UTC"),
            ),
            "x": pa.array([n / 3 for n in range(_N)], pa.float32()),
            "note": pa.array([None if n % 4 == 0 else f"n{n}" for n in range(_N)]),
            "samples": pa.array(
                [[n, None] for n in range(_N)], pa.large_list(pa.int8())
            ),
        }
    )
    source = ArrowTableSource(
        table,
        tag_columns=["subject", "trial"],
        tag_only_columns=True,
        record_id_columns=["trial"],
        source_id="rows",
    )
    return list(source.iter_data())


def _tag(n: int) -> Tag:
    return Tag(
        {"k": n, "name": f"k{n}"},
        system_tags={f"{constants.SYSTEM_TAG_PREFIX}a": f"r{n}"},
    )


def _dict_data(n: int, **values: Any) -> Data:
    return Data(
        values,
        source_info={name: f"src::{n}::{name}" for name in values},
        record_uuid=uuid.uuid4(),
        python_schema={
            name: Path if isinstance(value, Path) else type(value)
            for name, value in values.items()
            if not isinstance(value, (list, dict))
        },
        meta_info={"__computed": True},
    )


_VALUES: dict[str, Any] = {
    "numbers": lambda n: {"a": n, "b": n + 0.5, "c": bool(n % 2)},
    "floats": lambda n: {"a": [-0.0, math.nan, math.inf, 0.0][n % 4]},
    "text": lambda n: {"a": f"t{n}", "b": bytes([n])},
    "lists": lambda n: {"a": [n, n + 1], "b": [[0.5], []]},
    "dicts": lambda n: {"a": {"p": n, "q": n}},
    "paths": lambda n: {"a": Path(f"/x/{n}"), "b": uuid.UUID(int=n)},
    "dates": lambda n: {"a": dt.date(2026, 1, n + 1)},
    "utc": lambda n: {"a": dt.datetime(2026, 1, n + 1, tzinfo=dt.timezone.utc)},
    "zoned": lambda n: {
        "a": dt.datetime(2026, 1, 1, n, tzinfo=dt.timezone(dt.timedelta(hours=5)))
    },
    "arrays": lambda n: {"a": np.arange(n + 1)},
    "numpy_scalars": lambda n: {"a": float(n), "b": np.float64(n)},
}


def _rows(kind: str) -> list[tuple[Any, Any]]:
    if kind == "arrow":
        return _arrow_rows()
    if kind == "arrow_read":
        rows = _arrow_rows()
        for tag, data in rows:
            _ = tag.as_dict(), data.as_dict()
        return rows
    rows = _arrow_rows() if kind == "mixed" else []
    if kind == "mixed":
        rows[1] = (rows[1][0], None)
        rows[2] = (rows[2][0], rows[2][1].with_source_info(x=None))
        rows[4] = (rows[4][0], EmptyData())
        rows[5] = (rows[5][0], Odd({"x": 1}))
        rows[7] = (rows[7][0], rows[7][1].with_meta_columns(__extra=[1, 2]))
        rows[8] = (rows[8][0], rows[8][1].drop("x"))
        rows[9] = (_tag(9), _dict_data(9, a=1))
        return rows
    return [(_tag(n), _dict_data(n, **_VALUES[kind](n))) for n in range(_N)]


def _is_exactly(read: Any, given: Any) -> None:
    """``read`` and ``given`` agree in type and in every view."""
    assert type(read) is type(given)
    if given is None or isinstance(given, EmptyData):
        assert read is given
        return
    assert read.datagram_uuid == given.datagram_uuid
    assert read.data_context_key == given.data_context_key
    for config in ({}, {"all_info": True}):
        assert read.keys(**config) == given.keys(**config)
        assert list(read.schema(**config).items()) == list(
            given.schema(**config).items()
        )
        assert read.arrow_schema(**config).equals(
            given.arrow_schema(**config), check_metadata=True
        )
        table, expected = read.as_table(**config), given.as_table(**config)
        assert table.schema.equals(expected.schema, check_metadata=True)
        # ``equals`` takes NaN for unequal: the values, bit for bit, by their text.
        assert repr(table.to_pylist()) == repr(expected.to_pylist())
        values, wanted = read.as_dict(**config), given.as_dict(**config)
        assert list(values) == list(wanted)
        for name, value in wanted.items():
            assert type(values[name]) is type(value)
            assert repr(values[name]) == repr(value)
    assert read.content_hash() == given.content_hash()
    assert read.get_meta_info() == given.get_meta_info()
    if isinstance(given, Data):
        assert read.source_info() == given.source_info()
    else:
        assert read.system_tags() == given.system_tags()


_KINDS = ["arrow", "arrow_read", "mixed", *_VALUES]


@pytest.mark.parametrize("kind", _KINDS)
def test_held_pairs_read_as_the_pairs_held(spilled: Path, kind: str) -> None:
    rows = _rows(kind)
    pairs = HeldPairs(_TYPES)
    for row in rows:
        pairs.append(row)
        pairs.spill()

    assert len(pairs) == _N
    assert [pairs.has_data(i) for i in range(_N)] == [d is not None for _, d in rows]
    for read in (list(pairs), [pairs[i] for i in range(_N)], pairs[:]):
        for (tag, data), (given_tag, given_data) in zip(read, rows, strict=True):
            _is_exactly(tag, given_tag)
            _is_exactly(data, given_data)
    for i in (7, 0, 10, 3, -1):
        _is_exactly(pairs[i][1], rows[i][1])
    with pytest.raises(IndexError):
        _ = pairs[_N]


@pytest.mark.parametrize(
    ("kind", "chunked"),
    [
        ("arrow", 9),
        ("arrow_read", 9),
        ("numbers", 9),
        ("floats", 9),
        ("text", 9),
        ("lists", 9),
        ("paths", 9),
        # A value Arrow does not give back as it is stays the object it is.
        ("zoned", 0),
        ("arrays", 0),
        ("numpy_scalars", 0),
    ],
)
def test_only_rows_that_read_the_same_become_chunks(
    spilled: Path, kind: str, chunked: int
) -> None:
    rows = _rows(kind)
    pairs = HeldPairs(_TYPES)
    for row in rows:
        pairs.append(row)
    pairs.spill()
    assert sum(not pairs.is_kept(i) for i in range(_N)) == chunked
    for i in range(_N):
        if pairs.is_kept(i):
            assert pairs[i] is rows[i]
    assert bool(_files(spilled)) == bool(chunked)


@pytest.mark.parametrize(
    ("a", "b"),
    [
        (0.0, -0.0),
        (1, True),
        (1, 1.0),
        ([1], [True]),
        ((1,), (1,)),
        ({"a": 1, "b": 2}, {"b": 2, "a": 1}),
        (Decimal("1.0"), Decimal("1.00")),
        (Point(1), Point(1)),
        (dt.datetime(2026, 1, 1), dt.datetime(2026, 1, 1, fold=1)),
    ],
)
def test_values_that_differ_in_anything_are_not_the_same(a: Any, b: Any) -> None:
    assert not _same(a, b)


def test_rows_being_read_outlive_the_holder(spilled: Path) -> None:
    rows = _rows("mixed")
    pairs = HeldPairs(_TYPES)
    for row in rows:
        pairs.append(row)
    pairs.spill()
    reading = iter(pairs)
    first = next(reading)
    assert _files(spilled)

    pairs.close()

    assert not _files(spilled)
    assert len(pairs) == 0
    for (_, data), (_, given) in zip([first, *reading], rows, strict=True):
        _is_exactly(data, given)
    pairs.append(rows[0])
    assert pairs[0] is rows[0]


@pytest.mark.skipif(not hasattr(os, "fork"), reason="no fork on this system")
def test_a_forked_child_reads_the_rows(spilled: Path) -> None:
    pairs = HeldPairs(_TYPES)
    for row in _rows("arrow"):
        pairs.append(row)
    pairs.spill()
    child = os.fork()
    if child == 0:
        try:
            ok = [t.as_dict()["trial"] for t, _ in pairs] == list(range(_N))
            ok = ok and pairs[4][0].as_dict()["trial"] == 4
            os._exit(0 if ok else 1)
        except BaseException:
            os._exit(2)
    assert os.waitstatus_to_exitcode(os.waitpid(child, 0)[1]) == 0
    assert _files(spilled)


def test_rows_by_entry_id(spilled: Path) -> None:
    rows = _rows("mixed")
    held = HeldRows(_TYPES)
    for n, row in enumerate(rows):
        held[b"%d" % n] = row
        held.spill()
    held[b"0"] = rows[3]
    assert list(held) == [b"%d" % n for n in range(_N)]
    assert len(held) == _N and b"4" in held and b"x" not in held
    assert held.get(b"x") is None
    _is_exactly(held[b"0"][1], rows[3][1])
    assert not held.has_data(b"1") and held.has_data(b"0") and not held.has_data(b"x")
    assert not held.is_kept(b"x")
    del held[b"2"]
    assert b"2" not in held
    held.clear()
    assert not held and not _files(spilled)


class _FullDisk:
    def __init__(self, room: int) -> None:
        self.room, self.calls, self.real = room, 0, pa.ipc.new_file

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1
        if self.calls > self.room:
            raise OSError(errno.ENOSPC, "synthetic full disk")
        return self.real(*args, **kwargs)


def test_a_chunk_that_cannot_be_written_leaves_every_pair_once(
    spilled: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = _rows("arrow")
    pairs = HeldPairs(_TYPES)
    for row in rows:
        pairs.append(row)
    with monkeypatch.context() as patched:
        patched.setattr(pa.ipc, "new_file", _FullDisk(1))
        with pytest.raises(OSError, match="synthetic full disk"):
            pairs.spill()
    for retry in (False, True):
        if retry:
            pairs.spill()
        assert [t.as_dict()["trial"] for t, _ in pairs] == list(range(_N))


@pytest.mark.parametrize("step", ["_candidate", "_PythonSide"])
def test_a_failed_allocation_leaves_every_pair_once(
    spilled: Path, monkeypatch: pytest.MonkeyPatch, step: str
) -> None:
    pairs = HeldPairs(_TYPES)
    for row in _rows("arrow"):
        pairs.append(row)

    def fail(*_: Any) -> Any:
        raise MemoryError("synthetic")

    with monkeypatch.context() as patched:
        patched.setattr(held_rows, step, fail)
        with pytest.raises(MemoryError):
            pairs.spill()
    pairs.spill()
    assert [t.as_dict()["trial"] for t, _ in pairs] == list(range(_N))


# ---------------------------------------------------------------------------
# Job nodes
# ---------------------------------------------------------------------------


@function_pod(output_keys=["total", "label"], version="v0.0")
def _numbers(a: int, b: int) -> tuple[int, str]:
    return a + b, f"{a}"


@function_pod(output_keys="seen", version="v0.0")
def _array(a: int, b: int) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # the declared type under test
    return np.arange(a + b)


@function_pod(output_keys="seen", version="v0.0")
def _some(a: int, b: int) -> int | None:
    return None if a % 4 == 0 else a


_DELIVERED: list[int] = []


@function_pod(output_keys="point", version="v0.0")
def _instance(a: int, b: int) -> Point:
    return FarPoint(a, b) if a % 2 else Point(a)


@side_effect_pod
def _deliver(a: int, b: int, ctx: Any) -> None:
    _DELIVERED.append(a)


def _source(rows: int = _N) -> ArrowTableSource:
    numbers = list(range(rows))
    return ArrowTableSource(
        pa.table(
            {
                "subject": [f"s{n % 3}" for n in numbers],
                "trial": numbers,
                "a": numbers,
                "b": [n * 10 for n in numbers],
            },
            schema=pa.schema(
                [
                    pa.field("subject", pa.large_string(), nullable=False),
                    *(
                        pa.field(name, pa.int64(), nullable=False)
                        for name in ("trial", "a", "b")
                    ),
                ]
            ),
        ),
        tag_columns=["subject", "trial"],
        tag_only_columns=True,
        record_id_columns=["trial"],
        source_id="numbers",
    )


_PODS: dict[str, Any] = {
    "numbers": _numbers,
    "arrays": _array,
    "instances": _instance,
    "some": _some,
    "effect": _deliver,
}


def _job(pod: str, database: Any = None) -> PipelineJob:
    with PipelineJob(
        name=f"held_{pod}", database=database or InMemoryArrowDatabase()
    ) as job:
        source = _source()
        step = _PODS[pod]
        out = getattr(step, "pod", step).process(source, label="out")
        _ = out.select_tag_columns(["trial"], label="after")
    return job


def _run(job: PipelineJob, orchestrator: str) -> Any:
    if orchestrator == "sync":
        return job.run()
    return job.run(orchestrator=AsyncPipelineOrchestrator())


def _by_trial(table: pa.Table) -> pa.Table:
    names = [n for n in table.column_names if "record" not in n]
    return table.select(names).sort_by("trial")


def _view(node: Any) -> dict[str, Any]:
    # Record ids of computed results are random; everything else is compared.
    table = node.as_table(columns={"system_tags": True})
    return {
        "table": table.to_pylist(),
        "schema": table.schema,
        "rows": [
            (t.as_dict(), {k: repr(v) for k, v in d.as_dict(all_info=False).items()})
            for t, d in node.iter_data()
        ],
        "types": [[type(v) for v in d.as_dict().values()] for _, d in node.iter_data()],
        "count": node.held_row_count(),
        "head": node.head(4).table.to_pylist(),
        "head_rows": node.head(4).total_rows,
        "sorted_head": node.head(4, columns={"sort_by_tags": True}).table.to_pylist(),
        "hash": node.content_hash(),
    }


@pytest.mark.parametrize("pod", list(_PODS))
def test_a_sync_node_over_chunks_reads_exactly_as_one_over_pairs(
    spilled: Path, pod: str
) -> None:
    job = _job(pod)
    _ = _run(job, "sync")
    found = {label: _view(job.nodes[label]) for label in ("out",)}
    after = job.nodes["after"].as_table(columns={"system_tags": True})
    assert bool(_files(spilled)) == (pod not in ("arrays", "instances"))

    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(held_rows, "HELD_CHUNK_ROWS", 8192)
        plain = _job(pod)
        _ = _run(plain, "sync")
        expected = {label: _view(plain.nodes[label]) for label in ("out",)}
        expected_after = plain.nodes["after"].as_table(columns={"system_tags": True})

    assert repr(found) == repr(expected)
    assert after.equals(expected_after)
    # Tables, source info included, are the rows': the same on every read.
    node = job.nodes["out"]
    first = node.as_table(all_info=True)
    ids = [d.datagram_uuid for _, d in node.iter_data()]
    assert ids == [d.datagram_uuid for _, d in node.iter_data()]
    node._cached_output_table = None
    assert node.as_table(all_info=True).to_pylist() == first.to_pylist()


@pytest.mark.parametrize("pod", list(_PODS))
def test_an_async_node_over_chunks_holds_the_sync_rows(spilled: Path, pod: str) -> None:
    sync_job, async_job = _job(pod), _job(pod)
    _ = _run(sync_job, "sync")
    _ = _run(async_job, "async")
    for label in ("out", "after"):
        found = _by_trial(async_job.nodes[label].as_table())
        expected = _by_trial(sync_job.nodes[label].as_table())
        assert found.schema.equals(expected.schema, check_metadata=True)
        assert repr(found.to_pylist()) == repr(expected.to_pylist())


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
@pytest.mark.parametrize("pod", list(_PODS))
def test_clearing_a_node_removes_its_files(
    spilled: Path, pod: str, orchestrator: str
) -> None:
    job = _job(pod)
    _ = _run(job, orchestrator)
    table = job.nodes["out"].as_table()
    values = repr(table.to_pylist())
    for node in job.nodes.values():
        if hasattr(node, "clear_cache"):
            node.clear_cache()
    assert not _files(spilled)
    assert job.nodes["out"].as_table().num_rows == 0
    assert repr(table.to_pylist()) == values


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_stored_results_are_loaded_and_held_in_chunks(
    spilled: Path, orchestrator: str
) -> None:
    database = InMemoryArrowDatabase()
    first = _job("numbers", database)
    _ = _run(first, "sync")
    expected = _by_trial(first.nodes["out"].as_table(columns={"source": True}))

    again = _job("numbers", database)
    _ = _run(again, orchestrator)

    found = _by_trial(again.nodes["out"].as_table(columns={"source": True}))
    assert found.to_pylist() == expected.to_pylist()
    assert _files(spilled)
    loaded = _job("numbers", database)
    _ = loaded.run(load_only=True)
    assert (
        _by_trial(loaded.nodes["out"].as_table(columns={"source": True})).to_pylist()
        == expected.to_pylist()
    )


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
@pytest.mark.parametrize("pod", ["numbers", "effect"])
@pytest.mark.parametrize("room", [0, 1])
def test_a_full_disk_fails_the_run_and_leaves_no_file(
    spilled: Path,
    monkeypatch: pytest.MonkeyPatch,
    pod: str,
    orchestrator: str,
    room: int,
) -> None:
    """It is no row failure: the run fails, whatever the error policy."""
    job = _job(pod)
    with monkeypatch.context() as patched:
        patched.setattr(pa.ipc, "new_file", _FullDisk(room))
        with pytest.raises(BaseException) as kept:
            _ = _run(job, orchestrator)
    assert not _files(spilled), f"left behind with {kept.value!r} kept"
    assert job.nodes["out"].as_table().num_rows == 0
    _ = _run(job, orchestrator)
    assert job.nodes["out"].as_table().num_rows == _N


@pytest.mark.parametrize("pod", ["numbers", "effect"])
def test_a_cancelled_run_leaves_no_file(spilled: Path, pod: str) -> None:
    job = _job(pod)
    node = job.nodes["out"]

    async def run() -> BaseException:
        given: Channel[tuple[Any, Any]] = Channel(buffer_size=_N)
        output: Channel[tuple[Any, Any]] = Channel(buffer_size=_N)
        if pod == "effect":
            started = node.async_execute([given.reader], output.writer)
        else:
            started = node.async_execute(given.reader, output.writer)
        task = asyncio.create_task(started)
        for row in _source().iter_data():
            await given.writer.send(row)
        for _ in range(_N):
            _ = await output.reader.receive()
        assert _files(spilled)
        _ = task.cancel()
        with pytest.raises(asyncio.CancelledError) as kept:
            await task
        return kept.value

    error = asyncio.run(run())
    assert not _files(spilled), f"left behind with {error!r} kept"
    assert node.as_table().num_rows == 0


def test_a_side_effect_nodes_read_goes_on_after_it_is_cleared(spilled: Path) -> None:
    job = _job("effect")
    _ = _run(job, "sync")
    reading: Iterator[Any] = job.nodes["out"].iter_data()
    first = next(reading)
    job.nodes["out"].clear_cache()
    assert [t.as_dict()["trial"] for t, _ in [first, *reading]] == list(range(_N))
