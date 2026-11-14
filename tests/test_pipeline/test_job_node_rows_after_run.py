"""After a run, a function job node returns exactly the rows of that run's inputs (ITL-1093).

One store, a source of one ``source_id`` holding 3 rows, then 6, then the same 3 again, then
3 different rows. After each run, on either orchestrator, every function job node returns
the rows of that run's inputs through ``as_table``, ``iter_data``, ``head`` and ``run()``,
and so does a consumer built over the node afterwards; the two orchestrators agree row for
row. Stored results of other inputs stay stored and reusable. A run that raised defines no
rows: the node then reads as a node with no run in this process (every row stored under
its content hash).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.data_function import PythonDataFunction
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import StoreWriteError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import DictSource


@function_pod(output_keys="y")
def inc(x: int) -> int:
    """Return ``x + 1``."""
    return x + 1


@function_pod(output_keys="z")
def dbl(y: int) -> int:
    """Return ``2 * y``."""
    return 2 * y


@function_pod(output_keys="total")
def total(x: list[int]) -> int:
    """Return the sum of a group's values."""
    return sum(x)


@function_pod(output_keys="y")
def null_on_10(x: int) -> int | None:
    """Return ``x + 1``, or a null value for ``x == 10``."""
    return None if x == 10 else x + 1


@function_pod(output_keys="y")
def fails_on_20(x: int) -> int:
    """Return ``x + 1``; raise for ``x == 20``."""
    if x == 20:
        raise ValueError("x is 20")
    return x + 1


@function_pod(output_keys="y")
def store_fails_on_20(x: int) -> int:
    """Return ``x + 1``; for ``x == 20`` raise the error a failed store write raises.

    A ``StoreError`` fails the run whatever the error policy, on both orchestrators, as a
    failed write of the node's results does.
    """
    if x == 20:
        raise StoreWriteError(("simulated", "results"))
    return x + 1


ROWS = [{"g": i % 2, "k": i, "x": 10 * i} for i in range(6)]
# 3 rows, 6 rows (3 more), the same 3 again (fewer), 3 different rows. The source's record
# ids are positional, so the last run's rows are new inputs whose results are stored.
SEQUENCE = [ROWS[:3], ROWS, ROWS[:3], ROWS[3:]]
ORCHESTRATORS = ["sync", "async"]


def _source(data: list[dict[str, int]]) -> DictSource:
    return DictSource(data, tag_columns=["g", "k"], source_id="s")


def _build(data: list[dict[str, int]], db: Any) -> PipelineJob:
    job = PipelineJob(name="itl1093", database=db)
    with job:
        rowwise = inc.pod(_source(data), label="rowwise")
        dbl.pod(rowwise, label="downstream")
        total.pod(_source(data).group_by("g"), label="grouped")
    return job


def _orchestrator(kind: str, error_policy: str = "continue") -> Any:
    if kind == "sync":
        return SyncPipelineOrchestrator(error_policy=error_policy)  # pyright: ignore[reportArgumentType]
    return AsyncPipelineOrchestrator(error_policy=error_policy)  # pyright: ignore[reportArgumentType]


def _canonical(rows: list[dict[str, Any]]) -> list[tuple[tuple[str, str], ...]]:
    """Rows as a sorted list, each a sorted tuple of (column, repr(value))."""
    return sorted(tuple(sorted((k, repr(v)) for k, v in row.items())) for row in rows)


ALL = {"system_tags": True}


def _reads(node: Any) -> dict[str, list[tuple[tuple[str, str], ...]]]:
    """Every public read of ``node``, rows with system tags, canonical."""
    iterated = [
        {**tag.as_dict(all_info=True), **data.as_dict()}
        for tag, data in node.iter_data()
    ]
    return {
        "as_table": _canonical(node.as_table(columns=ALL).to_pylist()),
        "head": _canonical(node.head(100, columns=ALL).as_table().to_pylist()),
        "iter_data": _canonical(
            [
                {k: v for k, v in row.items() if not k.startswith("__")}
                for row in iterated
            ]
        ),
    }


def _values(node: Any, key: Callable[[dict[str, Any]], Any]) -> list[Any]:
    return sorted(key(row) for row in node.as_table().to_pylist())


def _expected_rowwise(data: list[dict[str, int]]) -> list[tuple[int, int]]:
    return sorted((row["k"], row["x"] + 1) for row in data)


def _expected_downstream(data: list[dict[str, int]]) -> list[tuple[int, int]]:
    return sorted((row["k"], 2 * (row["x"] + 1)) for row in data)


def _expected_grouped(data: list[dict[str, int]]) -> list[tuple[int, int]]:
    sums: dict[int, int] = {}
    for row in data:
        sums[row["g"]] = sums.get(row["g"], 0) + row["x"]
    return sorted(sums.items())


def _check_run(job: PipelineJob, data: list[dict[str, int]]) -> None:
    """Assert every node of ``job`` returns the rows of ``data``, through every read."""
    nodes = job.nodes
    assert _values(nodes["rowwise"], lambda r: (r["k"], r["y"])) == _expected_rowwise(
        data
    )
    assert _values(nodes["downstream"], lambda r: (r["k"], r["z"])) == (
        _expected_downstream(data)
    )
    assert _values(nodes["grouped"], lambda r: (r["g"], r["total"])) == (
        _expected_grouped(data)
    )
    for label in ("rowwise", "downstream", "grouped"):
        reads = _reads(nodes[label])
        assert reads["head"] == reads["as_table"], label
        assert len(reads["iter_data"]) == len(reads["as_table"]), label
    # A consumer built over the node after the run receives the run's rows.
    consumer = dbl.pod(nodes["rowwise"])
    assert sorted((r["k"], r["z"]) for r in consumer.as_table().to_pylist()) == (
        _expected_downstream(data)
    )


def _snapshot(job: PipelineJob) -> dict[str, Any]:
    return {
        label: _reads(job.nodes[label])
        for label in ("rowwise", "downstream", "grouped")
    }


class TestFreshJobPerRun:
    """A new job over the same store for every run, as the issue's reproduction."""

    @pytest.mark.parametrize("kind", ORCHESTRATORS)
    def test_each_run_returns_its_inputs_rows(self, kind: str) -> None:
        db = InMemoryArrowDatabase()
        for index, data in enumerate(SEQUENCE):
            job = _build(data, db)
            summary = job.run(orchestrator=_orchestrator(kind))
            _check_run(job, data)
            if index == 1:
                # The 6-row run reuses the 3 stored results of the first run.
                assert summary.nodes["rowwise"].reused == 3
                assert summary.nodes["rowwise"].computed == 3

    def test_sync_and_async_agree_row_for_row(self) -> None:
        snapshots: dict[str, list[dict[str, Any]]] = {}
        for kind in ORCHESTRATORS:
            db = InMemoryArrowDatabase()
            snapshots[kind] = []
            for data in SEQUENCE:
                job = _build(data, db)
                job.run(orchestrator=_orchestrator(kind))
                snapshots[kind].append(_snapshot(job))
        assert snapshots["sync"] == snapshots["async"]

    @pytest.mark.parametrize("kind", ORCHESTRATORS)
    def test_node_run_returns_its_inputs_rows(self, kind: str) -> None:
        db = InMemoryArrowDatabase()
        for data in SEQUENCE:
            job = _build(data, db)
            job.run(orchestrator=_orchestrator(kind))
            view = job.nodes["rowwise"].run()
            assert sorted((r["k"], r["y"]) for r in view.as_table().to_pylist()) == (
                _expected_rowwise(data)
            )


class TestSameJobRebound:
    """One job object, its source rebound before every run: the same node objects run again."""

    @pytest.mark.parametrize("kind", ORCHESTRATORS)
    def test_each_run_returns_its_inputs_rows(self, kind: str) -> None:
        db = InMemoryArrowDatabase()
        job = _build(SEQUENCE[0], db)
        for data in SEQUENCE:
            job.bind(sources={"s": _source(data)})
            job.run(orchestrator=_orchestrator(kind))
            _check_run(job, data)

    def test_sync_and_async_agree_row_for_row(self) -> None:
        snapshots: dict[str, list[dict[str, Any]]] = {}
        for kind in ORCHESTRATORS:
            db = InMemoryArrowDatabase()
            job = _build(SEQUENCE[0], db)
            snapshots[kind] = []
            for data in SEQUENCE:
                job.bind(sources={"s": _source(data)})
                job.run(orchestrator=_orchestrator(kind))
                snapshots[kind].append(_snapshot(job))
        assert snapshots["sync"] == snapshots["async"]


def _single(pod: Any, data: list[dict[str, int]], db: Any) -> PipelineJob:
    job = PipelineJob(name="itl1093_single", database=db)
    with job:
        pod(_source(data), label="node")
    return job


class TestNullAndInactive:
    """A null value is a row; an inactive function's inputs (no output) are rows of no read."""

    @pytest.mark.parametrize("kind", ORCHESTRATORS)
    def test_null_value_is_a_row(self, kind: str) -> None:
        db = InMemoryArrowDatabase()
        _single(null_on_10.pod, ROWS, db).run(orchestrator=_orchestrator(kind))
        job = _single(null_on_10.pod, ROWS[:3], db)
        job.run(orchestrator=_orchestrator(kind))
        node = job.nodes["node"]
        assert _values(node, lambda r: (r["k"], r["y"])) == [(0, 1), (1, None), (2, 21)]
        reads = _reads(node)
        assert reads["head"] == reads["as_table"]
        assert len(reads["iter_data"]) == 3

    @pytest.mark.parametrize("kind", ORCHESTRATORS)
    def test_inactive_function_gives_no_rows(self, kind: str) -> None:
        @function_pod(output_keys="y")
        def add_one(x: int) -> int:
            return x + 1

        db = InMemoryArrowDatabase()
        _single(add_one.pod, ROWS[3:], db).run(orchestrator=_orchestrator(kind))
        # A function switched off produces no output for the inputs it is given.
        data_function: object = add_one.pod.data_function
        assert isinstance(data_function, PythonDataFunction)
        data_function.set_active(False)
        job = _single(add_one.pod, ROWS[:3], db)
        job.run(orchestrator=_orchestrator(kind))
        node = job.nodes["node"]
        assert node.as_table().num_rows == 0
        assert list(node.iter_data()) == []
        assert node.head(100).as_table().num_rows == 0


class TestRaisedRun:
    """A run that raised defines no rows: the node reads as a node with no run."""

    @staticmethod
    def _no_run_read(fn: Any, data: list[dict[str, int]], db: Any) -> list[Any]:
        fresh = _single(fn.pod, data, db)
        fresh.compile()
        return _values(fresh.nodes["node"], lambda r: (r["k"], r["y"]))

    @pytest.mark.parametrize(
        ("kind", "fn", "policy"),
        [
            ("sync", fails_on_20, "fail_fast"),
            ("sync", store_fails_on_20, "continue"),
            ("async", store_fails_on_20, "continue"),
        ],
    )
    def test_raised_run_reads_as_no_run(self, kind: str, fn: Any, policy: str) -> None:
        db = InMemoryArrowDatabase()
        # Store the results of rows 3..5 first, then run rows 0..3, which fails at x=20.
        _single(fn.pod, ROWS[3:], db).run(orchestrator=_orchestrator(kind))
        job = _single(fn.pod, ROWS[:4], db)
        with pytest.raises((ValueError, StoreWriteError)):
            job.run(orchestrator=_orchestrator(kind, policy))
        node = job.nodes["node"]
        expected = self._no_run_read(fn, ROWS[:4], db)
        assert _values(node, lambda r: (r["k"], r["y"])) == expected
        reads = _reads(node)
        assert reads["head"] == reads["as_table"]
        # Nothing stored was dropped: the stored rows of the earlier run are among them.
        assert {(3, 31), (4, 41), (5, 51)} <= set(expected)


class TestDuplicateInputs:
    """Two inputs with one base entry id give two held rows, as downstream receives them.

    Only a stream without system tags can carry two such inputs (a source gives every
    row its own record id), so the node is built directly over an ``ArrowTableStream``.
    """

    @staticmethod
    def _node() -> Any:
        import pyarrow as pa

        from orcapod.core.nodes import FunctionJobNode
        from orcapod.core.streams import ArrowTableStream

        schema = pa.schema(
            [
                pa.field("k", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        )
        table = pa.Table.from_arrays(
            [pa.array([0, 0, 2], pa.int64()), pa.array([5, 5, 7], pa.int64())],
            schema=schema,
        )
        return FunctionJobNode(
            inc.pod,  # pyright: ignore[reportArgumentType]  # the decorator's pod type is not declared as FunctionPodProtocol
            ArrowTableStream(table, tag_columns=["k"]),
            pipeline_database=InMemoryArrowDatabase(),
            result_database=InMemoryArrowDatabase(),
        )

    def test_sync(self) -> None:
        node = self._node()
        view = node.run()
        held = sorted((r["k"], r["y"]) for r in node.as_table().to_pylist())
        assert held == [(0, 6), (0, 6), (2, 8)]
        assert sorted((r["k"], r["y"]) for r in view.as_table().to_pylist()) == held

    def test_async(self) -> None:
        import asyncio

        from orcapod.channels import Channel

        node = self._node()

        async def run() -> list[tuple[int, int]]:
            inputs = Channel(buffer_size=16)
            outputs = Channel(buffer_size=16)
            # The input stream is the node's own upstream (a test of async_execute).
            for item in node.upstreams[0].iter_data():
                await inputs.writer.send(item)
            await inputs.writer.close()
            await node.async_execute(inputs.reader, outputs.writer)
            got = [
                (tag.as_dict()["k"], data.as_dict()["y"])
                async for tag, data in outputs.reader
            ]
            return sorted(got)

        received = asyncio.run(run())
        assert received == [(0, 6), (0, 6), (2, 8)]
        assert sorted((r["k"], r["y"]) for r in node.as_table().to_pylist()) == received
