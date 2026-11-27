"""Result reuse of a function job node (ITL-1503).

Three rules:

- **Ephemeral mode asks the persistent store first.** With both result stores set,
  a row's result comes from the persistent store, else from the ephemeral store,
  else from the function; a fresh result is saved to the ephemeral store only, and
  nothing is ever saved to the persistent store. (A stopgap in the node, until the
  cache wrappers are redesigned: ITL-1516.)
- **A conditional write.** A node's cache wrapper stores a result only when its
  table holds none for the same input data hash and data context: a computation
  that finds one when it is about to store its own discards its own and uses the
  stored one, reported as a cache hit. It holds on one event loop or one thread,
  over one database object and its views, when no conditional write for the
  table is made from inside a callback that runs during a commit of it. Several
  threads, such a write, another database handle or process and an
  unconditional store are not covered; two tests here record the first two as
  known limits.
- **One instance, one result in one run.** In ephemeral mode an async run asks
  the persistent store once per instance.

The cases run on the synchronous and the asynchronous path unless they name one.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.system_constants import constants
from orcapod.types import NodeConfig, PodConfig

# The name the first version of this fix put on a tag as an internal marker.
OLD_MARKER = "_orcapod_result_from_fallback"

PATHS = pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])


def _non_nullable(columns: dict[str, Any]) -> pa.Table:
    schema = pa.schema(
        [pa.field(name, array.type, nullable=False) for name, array in columns.items()]
    )
    return pa.table(columns, schema=schema)


def _input(rows: dict[str, int], *, marker: bool = False) -> ArrowTableStream:
    """A stream with tags ``key`` and ``group`` and data ``value`` and ``weight``.

    Args:
        rows: Tag ``key`` to data ``value``; a key may not repeat (use
            ``_repeated_input`` for rows of one instance).
        marker: Whether to add a user tag named like the old internal marker.
    """
    columns: dict[str, Any] = {
        "key": pa.array(list(rows), pa.large_string()),
        "group": pa.array(["g"] * len(rows), pa.large_string()),
        "value": pa.array(list(rows.values()), pa.int64()),
        "weight": pa.array([1.5] * len(rows), pa.float64()),
    }
    tags = ["key", "group"]
    if marker:
        columns[OLD_MARKER] = pa.array([True] * len(rows), pa.bool_())
        tags.append(OLD_MARKER)
    return ArrowTableStream(_non_nullable(columns), tag_columns=tags)


def _repeated_input(keys: list[str], value: int) -> ArrowTableStream:
    """Rows that all hold ``value``; rows with one key are one instance."""
    return ArrowTableStream(
        _non_nullable(
            {
                "key": pa.array(keys, pa.large_string()),
                "group": pa.array(["g"] * len(keys), pa.large_string()),
                "value": pa.array([value] * len(keys), pa.int64()),
                "weight": pa.array([1.5] * len(keys), pa.float64()),
            }
        ),
        tag_columns=["key", "group"],
    )


class _Counted:
    """A pod over a function that records the values it was called with."""

    def __init__(self) -> None:
        self.calls: list[int] = []

        def scale(value: int, weight: float) -> tuple[int, float]:
            self.calls.append(value)
            return value * 2, weight * 2

        self.pod = FunctionPod.from_fn(scale, output_keys=["doubled", "heavier"])


async def _run_async(node: FunctionJobNode, source: ArrowTableStream) -> list[Any]:
    input_ch: Channel[Any] = Channel(buffer_size=32)
    output_ch: Channel[Any] = Channel(buffer_size=32)

    async def feed() -> None:
        for pair in source.iter_data():
            await input_ch.writer.send(pair)
        await input_ch.writer.close()

    await asyncio.gather(feed(), node.async_execute(input_ch.reader, output_ch.writer))
    return await output_ch.reader.collect()


def _run(node: FunctionJobNode, source: ArrowTableStream, use_async: bool) -> list[Any]:
    """Run ``node`` over ``source`` and return the rows it passed downstream."""
    if use_async:
        return asyncio.run(_run_async(node, source))
    return list(node.execute(source, error_policy="fail_fast").iter_data())


def _result_ids(database: Any) -> list[bytes]:
    """The ids of every result stored in ``database``, in any of its tables."""
    ids: list[bytes] = []
    for path in database.list_table_paths():
        table = database.get_all_records(path, record_id_column="rid")
        if table is not None:
            ids.extend(table["rid"].to_pylist())
    return ids


def _seed(
    database: Any, counted: _Counted, source: ArrowTableStream, keys: set[str]
) -> dict[str, bytes]:
    """Store the results of the rows named ``keys`` in ``database``.

    Returns:
        Key to the id of the result stored for it.
    """
    writer = FunctionJobNode(
        counted.pod,
        source,
        pipeline_database=InMemoryArrowDatabase(),
        result_database=database,
    )
    stored: dict[str, bytes] = {}
    for tag, data in source.iter_data():
        key = str(tag.as_dict()["key"])
        if key in keys:
            _, output = writer.execute_data(tag, data)
            assert output is not None
            stored[key] = output.datagram_uuid.bytes
    database.flush()
    counted.calls.clear()
    return stored


def _ephemeral_node(
    counted: _Counted,
    source: ArrowTableStream,
    *,
    pipeline: Any,
    persistent: Any,
    ephemeral: Any,
) -> FunctionJobNode:
    node = FunctionJobNode(
        counted.pod, source, pipeline_database=pipeline, result_database=persistent
    )
    node.node_config = NodeConfig(is_result_ephemeral=True)
    node.set_ephemeral_database(ephemeral)
    return node


def _records(node: FunctionJobNode) -> dict[str, dict[str, Any]]:
    """The node's stored pipeline records by key: result id, origin, computed."""
    table = node.get_all_records(all_info=True)
    assert table is not None
    return {
        row["key"]: {
            "result_id": row[constants.DATA_RECORD_ID],
            "is_ephemeral": row[constants.IS_EPHEMERAL_COL],
            "computed": row["__computed"],
        }
        for row in table.to_pylist()
    }


def _delivered(rows: list[Any]) -> dict[str, tuple[bytes, int, float]]:
    return {
        tag.as_dict()["key"]: (
            data.datagram_uuid.bytes,
            data.as_dict()["doubled"],
            data.as_dict()["heavier"],
        )
        for tag, data in rows
    }


# ---------------------------------------------------------------------------
# Ephemeral mode: persistent store, then ephemeral store, then the function
# ---------------------------------------------------------------------------


@PATHS
@pytest.mark.parametrize("marker", [False, True], ids=["plain", "old-marker-tag"])
def test_persistent_hit_in_ephemeral_mode(use_async: bool, marker: bool) -> None:
    """The persistent result is used: no call, no write, recorded as persistent."""
    counted = _Counted()
    source = _input({"a": 1, "b": 2, "c": 3}, marker=marker)
    persistent, ephemeral = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    stored = _seed(persistent, counted, source, {"a", "b", "c"})

    node = _ephemeral_node(
        counted,
        source,
        pipeline=InMemoryArrowDatabase(),
        persistent=persistent,
        ephemeral=ephemeral,
    )
    rows = _run(node, source, use_async)

    assert counted.calls == []
    assert _delivered(rows) == {
        "a": (stored["a"], 2, 3.0),
        "b": (stored["b"], 4, 3.0),
        "c": (stored["c"], 6, 3.0),
    }
    assert _result_ids(ephemeral) == []
    assert sorted(_result_ids(persistent)) == sorted(stored.values())
    assert _records(node) == {
        key: {"result_id": stored[key], "is_ephemeral": False, "computed": False}
        for key in "abc"
    }
    if marker:
        # A user tag with the old marker's name is an ordinary tag.
        assert all(tag.as_dict()[OLD_MARKER] is True for tag, _ in rows)
        assert set(node.keys()[0]) == {"key", "group", OLD_MARKER}


@PATHS
@pytest.mark.parametrize(
    "has_pipeline_record", [True, False], ids=["with-record", "result-only"]
)
def test_persistent_hit_in_ephemeral_mode_on_reopened_delta_stores(
    use_async: bool, has_pipeline_record: bool, tmp_path: Path
) -> None:
    """The same over Delta stores opened again, mixed with a row to compute."""
    counted = _Counted()
    source = _input({"stored": 1, "new": 2})
    pipeline_path = tmp_path / "pipeline"
    current_path = pipeline_path if has_pipeline_record else tmp_path / "other"
    first_pipeline = DeltaTableDatabase(base_path=pipeline_path)
    first = FunctionJobNode(
        counted.pod,
        source,
        pipeline_database=first_pipeline,
        result_database=DeltaTableDatabase(base_path=tmp_path / "results"),
    )
    for tag, data in source.iter_data():
        if tag.as_dict()["key"] == "stored":
            _ = first.execute_data(tag, data)
    first_pipeline.flush()
    assert counted.calls == [1]
    counted.calls.clear()

    opened: list[DeltaTableDatabase] = []

    def open_node() -> FunctionJobNode:
        opened[:] = [
            DeltaTableDatabase(base_path=current_path),
            DeltaTableDatabase(base_path=tmp_path / "results"),
            DeltaTableDatabase(base_path=tmp_path / "ephemeral"),
        ]
        return _ephemeral_node(
            counted,
            source,
            pipeline=opened[0],
            persistent=opened[1],
            ephemeral=opened[2],
        )

    rows = _run(open_node(), source, use_async)
    for database in reversed(opened):
        database.flush()
    assert counted.calls == [2]
    assert {k: v[1:] for k, v in _delivered(rows).items()} == {
        "stored": (2, 3.0),
        "new": (4, 3.0),
    }

    records = _records(open_node())
    assert {k: r["is_ephemeral"] for k, r in records.items()} == {
        "stored": False,
        "new": True,
    }
    assert records["new"]["computed"] is True
    assert len(_result_ids(DeltaTableDatabase(base_path=tmp_path / "results"))) == 1
    assert _result_ids(DeltaTableDatabase(base_path=tmp_path / "ephemeral")) == [
        records["new"]["result_id"]
    ]


@PATHS
def test_persistent_miss_then_ephemeral_hit(use_async: bool) -> None:
    """The ephemeral result is used: no call, no write, recorded as ephemeral."""
    counted = _Counted()
    source = _input({"a": 1, "b": 2})
    persistent, ephemeral = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    stored = _seed(ephemeral, counted, source, {"a", "b"})

    node = _ephemeral_node(
        counted,
        source,
        pipeline=InMemoryArrowDatabase(),
        persistent=persistent,
        ephemeral=ephemeral,
    )
    rows = _run(node, source, use_async)

    assert counted.calls == []
    assert _delivered(rows) == {"a": (stored["a"], 2, 3.0), "b": (stored["b"], 4, 3.0)}
    assert _result_ids(persistent) == []
    assert sorted(_result_ids(ephemeral)) == sorted(stored.values())
    assert _records(node) == {
        key: {"result_id": stored[key], "is_ephemeral": True, "computed": False}
        for key in "ab"
    }


@PATHS
def test_double_miss_computes_once_into_the_ephemeral_store(use_async: bool) -> None:
    """Neither store holds it: one call, one ephemeral row, no persistent row."""
    counted = _Counted()
    source = _input({"a": 1, "b": 2})
    persistent, ephemeral = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    node = _ephemeral_node(
        counted,
        source,
        pipeline=InMemoryArrowDatabase(),
        persistent=persistent,
        ephemeral=ephemeral,
    )
    rows = _run(node, source, use_async)

    assert sorted(counted.calls) == [1, 2]
    delivered = _delivered(rows)
    assert {k: v[1:] for k, v in delivered.items()} == {"a": (2, 3.0), "b": (4, 3.0)}
    assert _result_ids(persistent) == []
    assert sorted(_result_ids(ephemeral)) == sorted(v[0] for v in delivered.values())
    assert _records(node) == {
        key: {"result_id": delivered[key][0], "is_ephemeral": True, "computed": True}
        for key in "ab"
    }


@PATHS
def test_both_stores_hold_a_result_the_persistent_one_is_used(use_async: bool) -> None:
    """With a result in each store, the persistent one is returned and recorded."""
    counted = _Counted()
    source = _input({"a": 1, "b": 2})
    persistent, ephemeral = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    in_ephemeral = _seed(ephemeral, counted, source, {"a", "b"})
    in_persistent = _seed(persistent, counted, source, {"a", "b"})
    assert set(in_ephemeral.values()).isdisjoint(in_persistent.values())

    node = _ephemeral_node(
        counted,
        source,
        pipeline=InMemoryArrowDatabase(),
        persistent=persistent,
        ephemeral=ephemeral,
    )
    rows = _run(node, source, use_async)

    assert counted.calls == []
    assert {k: v[0] for k, v in _delivered(rows).items()} == in_persistent
    assert _records(node) == {
        key: {"result_id": in_persistent[key], "is_ephemeral": False, "computed": False}
        for key in "ab"
    }
    assert sorted(_result_ids(ephemeral)) == sorted(in_ephemeral.values())
    assert sorted(_result_ids(persistent)) == sorted(in_persistent.values())


HOOK_PATHS = ("sync row", "async row", "async_execute")


def _hooks_fired(path: str, case: str) -> list[tuple[Any, ...]]:
    """The post-run hook payloads one row gives on ``path``, as comparable tuples.

    Args:
        path: ``"sync row"`` (``execute``), ``"async row"``
            (``_async_process_data_internal``) or ``"async_execute"``.
        case: ``"persistent hit"``, ``"ephemeral hit"`` or ``"computed"``.
    """
    counted = _Counted()
    fired: list[tuple[Any, ...]] = []

    def record(payload: Any) -> None:
        fired.append(
            (
                payload.stats.status.name,
                payload.stats.error,
                payload.result_id is not None,
                None if payload.output is None else payload.output.as_dict(),
                payload.tag.as_dict(),
                payload.input.as_dict(),
                payload.pod.label,
                payload.pod.pod_hash,
                payload.invocation_context is not None,
            )
        )

    counted.pod.add_post_run_hook(record)
    source = _input({"a": 1})
    persistent, ephemeral = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    if case == "persistent hit":
        _ = _seed(persistent, counted, source, {"a"})
    elif case == "ephemeral hit":
        _ = _seed(ephemeral, counted, source, {"a"})
    fired.clear()
    node = _ephemeral_node(
        counted,
        source,
        pipeline=InMemoryArrowDatabase(),
        persistent=persistent,
        ephemeral=ephemeral,
    )
    if path == "async row":
        # The entry point this case is about: the async form of the per-row path.
        tag, data = next(iter(source.iter_data()))
        _, output = asyncio.run(node._async_process_data_internal(tag, data))  # pyright: ignore[reportPrivateUsage]  # the path under test
        assert output is not None
    else:
        assert len(_run(node, source, path == "async_execute")) == 1
    assert counted.calls == ([1] if case == "computed" else [])
    return fired


@pytest.mark.parametrize("case", ["persistent hit", "ephemeral hit", "computed"])
def test_every_path_fires_the_same_post_run_hooks(case: str) -> None:
    """A row is observed alike on the sync row path, the async row path and
    ``async_execute``: the same post-run hooks, with the same status and payload,
    for a persistent hit served by the stopgap lookup as for an ephemeral hit and
    a computed row. (Inside a job node no post-run hook of the pod fires today, on
    any path, for any of the three: ITL-891. This test holds the paths equal,
    whatever that issue decides they fire.)"""
    by_path = {path: _hooks_fired(path, case) for path in HOOK_PATHS}
    assert by_path["async row"] == by_path["sync row"]
    assert by_path["async_execute"] == by_path["sync row"]


def test_ephemeral_store_only_is_unchanged() -> None:
    """Without a persistent store the ephemeral store is asked, then the function."""
    counted = _Counted()
    source = _input({"a": 1, "b": 2})
    ephemeral = InMemoryArrowDatabase()

    def run_once() -> dict[str, tuple[bytes, int, float]]:
        node = FunctionJobNode(counted.pod, source, ephemeral_database=ephemeral)
        node.node_config = NodeConfig(is_result_ephemeral=True)
        return _delivered([node.execute_data(*pair) for pair in source.iter_data()])

    first = run_once()
    assert counted.calls == [1, 2]
    assert sorted(_result_ids(ephemeral)) == sorted(v[0] for v in first.values())
    assert run_once() == first
    assert counted.calls == [1, 2]
    assert len(_result_ids(ephemeral)) == 2


# ---------------------------------------------------------------------------
# Write if missing: equal inputs computed at the same moment
# ---------------------------------------------------------------------------


class _Gated:
    """An async function whose calls all start before any of them returns."""

    def __init__(self, expected: int, release_order: list[int]) -> None:
        self.calls = 0
        self._expected = expected
        self._order = release_order
        self._all_started = asyncio.Event()
        self._returned = [asyncio.Event() for _ in range(expected)]

        async def scale(value: int, weight: float) -> tuple[int, float]:
            self.calls += 1
            call = self.calls
            if call == self._expected:
                self._all_started.set()
            _ = await asyncio.wait_for(self._all_started.wait(), timeout=5)
            position = self._order.index(call)
            if position:
                previous = self._returned[self._order[position - 1] - 1]
                _ = await asyncio.wait_for(previous.wait(), timeout=5)
            self._returned[call - 1].set()
            # Each call gives its own value, so a result tells which call made it.
            return value + call, weight

        self.pod = FunctionPod.from_fn(
            scale,
            output_keys=["doubled", "heavier"],
            pod_config=PodConfig(max_concurrency=expected),
        )


@pytest.mark.parametrize("release_order", [[1, 2, 3], [3, 1, 2], [2, 3, 1]])
@pytest.mark.parametrize("ephemeral_mode", [False, True], ids=["persistent", "eph"])
def test_equal_inputs_computed_together_store_one_result(
    release_order: list[int], ephemeral_mode: bool
) -> None:
    """Three rows of one input (two of one instance, one of another) in flight
    together: every call runs, ONE result is stored, every row delivers it, the
    pipeline records name it, and a read gives what was delivered."""
    gated = _Gated(3, release_order)
    source = _repeated_input(["a", "a", "b"], 10)
    pipeline, persistent, ephemeral = (InMemoryArrowDatabase() for _ in range(3))
    node = FunctionJobNode(
        gated.pod, source, pipeline_database=pipeline, result_database=persistent
    )
    if ephemeral_mode:
        node.node_config = NodeConfig(is_result_ephemeral=True)
        node.set_ephemeral_database(ephemeral)
    assert len({node.compute_instance_id(*pair) for pair in source.iter_data()}) == 2

    rows = asyncio.run(_run_async(node, source))

    assert gated.calls == 3
    written, untouched = (
        (ephemeral, persistent) if ephemeral_mode else (persistent, ephemeral)
    )
    stored_ids = _result_ids(written)
    assert len(stored_ids) == 1
    assert _result_ids(untouched) == []
    # The first call to return made the stored result.
    expected = (stored_ids[0], 10 + release_order[0], 1.5)

    def as_delivered(pairs: list[Any]) -> list[tuple[Any, ...]]:
        return [
            (
                tag.as_dict()["key"],
                data.datagram_uuid.bytes,
                data.as_dict()["doubled"],
                data.as_dict()["heavier"],
            )
            for tag, data in pairs
        ]

    delivered = as_delivered(rows)
    assert sorted(delivered) == sorted(
        [("a", *expected), ("a", *expected), ("b", *expected)]
    )
    # Readback is what was delivered, row for row, in the emitted order.
    assert as_delivered(list(node.iter_data())) == delivered
    assert node.held_row_count() == 3
    records = _records(node)
    assert set(records) == {"a", "b"}
    assert {r["result_id"] for r in records.values()} == {stored_ids[0]}
    assert {r["is_ephemeral"] for r in records.values()} == {ephemeral_mode}
    stored = node.get_all_records()
    assert stored is not None
    assert sorted(map(str, stored["key"].to_pylist())) == ["a", "b"]
    assert set(stored["doubled"].to_pylist()) == {expected[1]}


@pytest.mark.parametrize("release_order", [[1, 2], [2, 1]])
def test_two_nodes_sharing_a_result_table_store_one_result(
    release_order: list[int],
) -> None:
    """Two nodes of one function over equal inputs, run together on one result
    database: one stored result, and both nodes' rows and records name it."""
    gated = _Gated(2, release_order)
    sources = [_input({"left": 7}), _input({"right": 7})]
    pipeline, results = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    nodes = [
        FunctionJobNode(
            gated.pod, source, pipeline_database=pipeline, result_database=results
        )
        for source in sources
    ]

    async def run_both() -> list[list[Any]]:
        return list(await asyncio.gather(*map(_run_async, nodes, sources)))

    delivered = asyncio.run(run_both())

    assert gated.calls == 2
    stored_ids = _result_ids(results)
    assert len(stored_ids) == 1
    for node, rows, key in zip(nodes, delivered, ["left", "right"], strict=True):
        expected = {key: (stored_ids[0], 7 + release_order[0], 1.5)}
        assert _delivered(rows) == expected
        assert _delivered(list(node.iter_data())) == expected
        assert _records(node)[key]["result_id"] == stored_ids[0]


def _stores(tmp_path: Path) -> dict[str, Any]:
    return {
        "memory": InMemoryArrowDatabase(),
        "delta": DeltaTableDatabase(base_path=tmp_path / "delta"),
    }


@PATHS
@pytest.mark.parametrize("store", ["memory", "delta"])
@pytest.mark.parametrize("auto_flush", [True, False], ids=["committed", "queued"])
def test_a_second_computation_uses_the_stored_result_in_every_store(
    use_async: bool, store: str, auto_flush: bool, tmp_path: Path
) -> None:
    """A result stored for the input after the lookup, committed or still queued,
    is found when the computation stores: nothing is added, and the stored result
    is returned as a cache hit."""
    database = _stores(tmp_path)[store]
    source = _input({"a": 5})
    tag, data = next(iter(source.iter_data()))

    def scale(value: int, weight: float) -> tuple[int, float]:
        return value * 2, weight * 2

    plain = FunctionPod.from_fn(scale, output_keys=["doubled", "heavier"])

    class _StoresMeanwhile(FunctionPod):
        """Computes, and has an equal input's result stored before it returns."""

        def process_data(self, tag: Any, data: Any, **kwargs: Any) -> Any:
            result = super().process_data(tag, data, **kwargs)
            first.append(other.process_data(tag, data)[1])
            return result

        async def async_process_data(self, tag: Any, data: Any, **kwargs: Any) -> Any:
            result = await super().async_process_data(tag, data, **kwargs)
            first.append((await other.async_process_data(tag, data))[1])
            return result

    first: list[Any] = []
    other = CachedFunctionPod(plain, result_database=database)
    other.set_auto_flush(auto_flush)
    cached_pod = CachedFunctionPod(
        _StoresMeanwhile(plain.data_function), result_database=database
    )
    cached_pod.set_auto_flush(auto_flush)

    if use_async:
        _, output = asyncio.run(cached_pod.async_process_data(tag, data))
    else:
        _, output = cached_pod.process_data(tag, data)

    assert output is not None and len(first) == 1
    assert first[0].get_meta_value(CachedFunctionPod.RESULT_COMPUTED_FLAG) is True
    assert output.datagram_uuid == first[0].datagram_uuid
    assert output.get_meta_value(CachedFunctionPod.RESULT_COMPUTED_FLAG) is False
    assert output.as_dict() == {"doubled": 10, "heavier": 3.0}
    assert output.source_info() == first[0].source_info()
    database.flush()
    assert _result_ids(database) == [first[0].datagram_uuid.bytes]


# ---------------------------------------------------------------------------
# Reviews of c1d2b6fd and later heads: one result per instance in a run, threads
# and commit callbacks (known limits), a failing lookup, repeated instances
# ---------------------------------------------------------------------------


def _ids(pairs: list[Any]) -> list[bytes]:
    return [data.datagram_uuid.bytes for _, data in pairs]


@pytest.mark.parametrize("first_in_flight", [False, True], ids=["after", "during"])
def test_async_readback_is_what_each_occurrence_delivered(
    first_in_flight: bool,
) -> None:
    """One instance gets one result in one async run. Two occurrences of an
    instance, with a persistent result for their input published between them:
    the persistent store is asked once per instance, so the second occurrence
    takes the route of the first (the ephemeral wrapper), both are delivered one
    result id, and a read gives that id for both.

    ``after``: the first occurrence was delivered before the result is published.
    ``during``: it is still computing, so both compute and one result is kept.
    """
    release = asyncio.Event()
    started = [asyncio.Event(), asyncio.Event()]
    calls: list[int] = []

    async def scale(value: int, weight: float) -> tuple[int, float]:
        calls.append(value)
        started[len(calls) - 1].set()
        if first_in_flight:
            _ = await asyncio.wait_for(release.wait(), timeout=5)
        return value * 2, weight * 2

    pod = FunctionPod.from_fn(
        scale,
        output_keys=["doubled", "heavier"],
        pod_config=PodConfig(max_concurrency=2),
    )
    seeding = _Counted()
    source = _input({"a": 2})
    (row,) = source.iter_data()
    persistent, ephemeral = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    node = FunctionJobNode(
        pod,
        source,
        pipeline_database=InMemoryArrowDatabase(),
        result_database=persistent,
    )
    node.node_config = NodeConfig(is_result_ephemeral=True)
    node.set_ephemeral_database(ephemeral)
    published: list[bytes] = []

    async def go() -> list[Any]:
        input_ch: Channel[Any] = Channel(buffer_size=3)
        output_ch: Channel[Any] = Channel(buffer_size=3)
        outputs = output_ch.reader.__aiter__()
        run = asyncio.create_task(node.async_execute(input_ch.reader, output_ch.writer))
        await input_ch.writer.send(row)
        delivered = []
        if first_in_flight:
            _ = await asyncio.wait_for(started[0].wait(), timeout=5)
        else:
            delivered.append(await anext(outputs))
        published.append(_seed(persistent, seeding, source, {"a"})["a"])
        await input_ch.writer.send(row)
        await input_ch.writer.close()
        if first_in_flight:
            _ = await asyncio.wait_for(started[1].wait(), timeout=5)
            release.set()
            delivered.append(await anext(outputs))
        delivered.append(await anext(outputs))
        await run
        return delivered

    delivered = asyncio.run(go())
    assert calls == ([2, 2] if first_in_flight else [2])
    (result_id,) = set(_ids(delivered))
    assert result_id != published[0]
    assert _result_ids(ephemeral) == [result_id]
    assert _ids(list(node.iter_data())) == [result_id, result_id]
    assert node.held_row_count() == 2
    assert _records(node)["a"] == {
        "result_id": result_id,
        "is_ephemeral": True,
        "computed": True,
    }


def test_sync_readback_is_what_each_occurrence_delivered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The sync path with the same shape. It reads its whole input before the
    first row, and serves a repeated instance from the rows it holds: a persistent
    result published while the input is read is served to the first occurrence,
    the second gets the same row, and a read gives both."""
    counted, seeding = _Counted(), _Counted()
    source = _repeated_input(["a", "a"], 2)
    persistent, ephemeral = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    node = _ephemeral_node(
        counted,
        source,
        pipeline=InMemoryArrowDatabase(),
        persistent=persistent,
        ephemeral=ephemeral,
    )
    rows = list(source.iter_data())
    published: list[bytes] = []

    def publishing_between_rows() -> Any:
        yield rows[0]
        monkeypatch.undo()
        published.append(_seed(persistent, seeding, source, {"a"})["a"])
        yield rows[1]

    monkeypatch.setattr(source, "iter_data", publishing_between_rows)
    delivered = list(node.execute(source, error_policy="fail_fast").iter_data())

    assert counted.calls == []
    assert _ids(delivered) == [published[0], published[0]]
    assert _ids(list(node.iter_data())) == _ids(delivered)


@pytest.mark.parametrize("store", ["memory", "delta"])
@pytest.mark.parametrize("auto_flush", [True, False], ids=["committed", "queued"])
def test_two_threads_on_one_database_are_not_covered(
    store: str, auto_flush: bool, tmp_path: Path
) -> None:
    """A KNOWN LIMIT, not a guarantee (ITL-1517). The conditional write holds on
    one event loop or one thread; it has no lock. Here two threads compute one
    input over one database object, and the second reaches its write while the
    first is between its own lookup and its write: each finds no result and each
    stores its own. The test asserts no uniqueness. It holds only what stays true
    either way: each thread returns a result that is stored, and nothing else is.
    """
    import threading
    from concurrent.futures import ThreadPoolExecutor

    database = _stores(tmp_path)[store]
    lookup = database.get_records_with_column_value
    local = threading.local()
    both_missed = threading.Barrier(2)
    first_in_write = threading.Event()
    second_looked = threading.Event()
    first_done = threading.Event()

    def staged_lookup(*args: Any, **kwargs: Any) -> Any:
        result = lookup(*args, **kwargs)
        local.lookups += 1
        if local.lookups == 1:
            _ = both_missed.wait(10)
        elif local.role == 1:
            # Between this thread's lookup and its write: give the other thread
            # the time to look the table up too, if nothing holds it back.
            first_in_write.set()
            _ = second_looked.wait(0.5)
        else:
            second_looked.set()
            assert first_done.wait(10)
        return result

    database.get_records_with_column_value = staged_lookup

    def scale(value: int, weight: float) -> tuple[int, float]:
        if local.role == 2:
            assert first_in_write.wait(10)
        return value * 2, weight * 2

    plain = FunctionPod.from_fn(scale, output_keys=["doubled", "heavier"])
    pods = [CachedFunctionPod(plain, result_database=database) for _ in range(2)]
    for pod in pods:
        pod.set_auto_flush(auto_flush)
    tag, data = next(iter(_input({"a": 2}).iter_data()))

    def run(role: int) -> tuple[bytes, Any]:
        local.role, local.lookups = role, 0
        try:
            _, output = pods[role - 1].process_data(tag, data)
        finally:
            if role == 1:
                first_done.set()
        assert output is not None
        return (
            output.datagram_uuid.bytes,
            output.get_meta_value(CachedFunctionPod.RESULT_COMPUTED_FLAG),
        )

    with ThreadPoolExecutor(2) as pool:
        (first_id, first_computed), (second_id, second_computed) = pool.map(run, (1, 2))
    database.flush()

    stored = _result_ids(database)
    assert set(stored) == {first_id, second_id}
    assert len(stored) == len(set(stored))
    assert first_computed is True
    # A thread that returns the other's result reports a hit.
    assert second_computed is (second_id != first_id)


class _RowEvents:
    """An observer that records each row event with the row's key."""

    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    def contextualize(self, *path: str) -> "_RowEvents":
        return self

    def on_run_start(self, *args: Any, **kwargs: Any) -> None: ...

    def on_run_end(self, *args: Any, **kwargs: Any) -> None: ...

    def on_node_start(self, *args: Any, **kwargs: Any) -> None: ...

    def on_node_end(self, *args: Any, **kwargs: Any) -> None: ...

    def create_data_logger(self, tag: Any, data: Any) -> Any:
        return None

    def on_data_start(self, label: str, tag: Any, data: Any) -> None:
        self.events.append(("start", tag.as_dict()["key"]))

    def on_data_end(self, label: str, tag: Any, *args: Any, **kwargs: Any) -> None:
        self.events.append(("end", tag.as_dict()["key"]))

    def on_data_crash(self, label: str, tag: Any, data: Any, error: Any) -> None:
        self.events.append(("crash", tag.as_dict()["key"]))


def _leaf(error: BaseException) -> BaseException:
    while isinstance(error, BaseExceptionGroup):
        assert len(error.exceptions) == 1
        error = error.exceptions[0]
    return error


def _failing_lookup_outcome(
    use_async: bool, database_error: bool, error_policy: Any
) -> dict[str, Any]:
    """What a run reports when the persistent store's lookup by input hash raises."""
    from orcapod.errors import OrcapodDatabaseReadError

    counted = _Counted()
    source = _input({"a": 2, "b": 3})
    persistent, ephemeral = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    node = _ephemeral_node(
        counted,
        source,
        pipeline=InMemoryArrowDatabase(),
        persistent=persistent,
        ephemeral=ephemeral,
    )
    node.label = "scaled"

    def broken(*args: Any, **kwargs: Any) -> Any:
        if database_error:
            raise OrcapodDatabaseReadError("fault", ("fault",))
        raise RuntimeError("fault")

    persistent.get_records_with_column_value = broken  # pyright: ignore[reportAttributeAccessIssue]  # the injected fault
    observer = _RowEvents()

    async def go() -> None:
        input_ch: Channel[Any] = Channel(buffer_size=8)
        output_ch: Channel[Any] = Channel(buffer_size=8)
        for pair in source.iter_data():
            await input_ch.writer.send(pair)
        await input_ch.writer.close()
        await node.async_execute(
            input_ch.reader,
            output_ch.writer,
            observer=observer,  # pyright: ignore[reportArgumentType]  # a recording observer
            error_policy=error_policy,
        )

    raised: BaseException | None = None
    try:
        if use_async:
            asyncio.run(go())
        else:
            _ = node.execute(
                source,
                observer=observer,  # pyright: ignore[reportArgumentType]  # a recording observer
                error_policy=error_policy,
            )
    except BaseException as error:  # noqa: BLE001  # the outcome under test
        raised = _leaf(error)
    return {
        "events": observer.events,
        "error": None if raised is None else type(raised).__name__,
        "node_label": getattr(raised, "node_label", None),
        "tag": getattr(raised, "tag", None),
        "calls": counted.calls,
        "rows": len(list(node.iter_data())),
        "ephemeral results": len(_result_ids(ephemeral)),
    }


@pytest.mark.parametrize("error_policy", ["continue", "fail_fast"])
@pytest.mark.parametrize("database_error", [True, False], ids=["database", "other"])
def test_a_failing_persistent_lookup_is_reported_alike_on_both_paths(
    database_error: bool, error_policy: str
) -> None:
    """A persistent lookup that raises, in ephemeral mode, gives the async job path
    what it gives the sync path: the row's start and crash events, the node and
    tag on a database error, and the same end under each error policy (a database
    error stops the run whatever the policy; another error is a failed row)."""
    sync = _failing_lookup_outcome(False, database_error, error_policy)
    assert sync["events"][:2] == [("start", "a"), ("crash", "a")]
    assert sync["calls"] == []
    if database_error:
        assert (sync["error"], sync["node_label"]) == (
            "OrcapodDatabaseReadError",
            "scaled",
        )
        assert sync["tag"] == {"key": "a", "group": "g"}
    assert _failing_lookup_outcome(True, database_error, error_policy) == sync


def test_a_result_this_pipeline_recorded_is_served_before_the_persistent_store() -> (
    None
):
    """The persistent store comes first only in the lookup by input data hash,
    after the row's instance was not found: a result this node's own pipeline
    record points to (here an ephemeral one) is still served first, on both paths,
    when the persistent store holds another result for the same input."""
    counted = _Counted()
    source = _input({"a": 2})
    pipeline, persistent, ephemeral = (InMemoryArrowDatabase() for _ in range(3))

    def node() -> FunctionJobNode:
        return _ephemeral_node(
            counted,
            source,
            pipeline=pipeline,
            persistent=persistent,
            ephemeral=ephemeral,
        )

    recorded = _ids(_run(node(), source, False))
    assert counted.calls == [2]
    published = _seed(persistent, counted, source, {"a"})["a"]
    assert recorded != [published]
    for use_async in (False, True):
        assert _ids(_run(node(), source, use_async)) == recorded
    assert counted.calls == []


def test_async_execute_without_a_persistent_store_runs_the_plain_pod() -> None:
    """What ``async_execute`` does today for a node with only an ephemeral database
    (behaviour of the base branch, recorded here, not a rule): it hands the rows to
    the plain pod, so the ephemeral store is neither read nor written and the
    pod's post-run hook fires, while the row paths go through the ephemeral
    wrapper and fire none (ITL-891)."""
    fired: dict[str, list[str]] = {}
    stored: dict[str, int] = {}
    for path in ("sync row", "async_execute"):
        counted = _Counted()
        fired[path] = []
        counted.pod.add_post_run_hook(
            lambda payload, path=path: fired[path].append(payload.stats.status.name)
        )
        source = _input({"a": 2})
        ephemeral = InMemoryArrowDatabase()
        node = FunctionJobNode(counted.pod, source, ephemeral_database=ephemeral)
        node.node_config = NodeConfig(is_result_ephemeral=True)
        if path == "sync row":
            _ = node.execute_data(*next(iter(source.iter_data())))
        else:
            assert len(_run(node, source, True)) == 1
        stored[path] = len(_result_ids(ephemeral))
    assert fired == {"sync row": [], "async_execute": ["COMPUTED"]}
    assert stored == {"sync row": 1, "async_execute": 0}


def _occurrence_node(
    ephemeral_mode: bool,
) -> tuple[FunctionJobNode, ArrowTableStream, list[Any]]:
    """A node over one stored result, and three occurrences of its one row whose
    tags differ only in a meta column: one instance, one result, one computed flag."""
    counted = _Counted()
    source = _input({"a": 2})
    persistent = InMemoryArrowDatabase()
    _ = _seed(persistent, counted, source, {"a"})
    if ephemeral_mode:
        node = _ephemeral_node(
            counted,
            source,
            pipeline=InMemoryArrowDatabase(),
            persistent=persistent,
            ephemeral=InMemoryArrowDatabase(),
        )
    else:
        node = FunctionJobNode(
            counted.pod,
            source,
            pipeline_database=InMemoryArrowDatabase(),
            result_database=persistent,
        )
    tag, data = next(iter(source.iter_data()))
    rows = [
        (tag.with_meta_columns(occurrence=name), data)
        for name in ("first", "second", "third", "second")
    ]
    assert len({node.compute_instance_id(*row) for row in rows}) == 1
    return node, source, rows


def _occurrences(pairs: list[Any]) -> list[Any]:
    return [tag.get_meta_value("occurrence") for tag, _ in pairs]


async def _through_async_execute(node: FunctionJobNode, rows: list[Any]) -> list[Any]:
    input_ch: Channel[Any] = Channel(buffer_size=8)
    output_ch: Channel[Any] = Channel(buffer_size=8)
    for row in rows:
        await input_ch.writer.send(row)
    await input_ch.writer.close()
    await node.async_execute(input_ch.reader, output_ch.writer)
    return await output_ch.reader.collect()


@pytest.mark.parametrize("ephemeral_mode", [False, True], ids=["persistent", "eph"])
def test_repeated_instance_delivery_differs_between_sync_and_async_itl_1521(
    ephemeral_mode: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A KNOWN DIFFERENCE that predates this work (the same on the base branch),
    recorded as it is today; ITL-1521 decides what a repeated instance delivers.

    Occurrences of one instance whose tags differ only in a meta column. The sync
    path serves a repeated instance from the row it holds, so every occurrence is
    delivered the first one's row; the async path sends each occurrence on with
    its own tag. A read of the node gives one row per instance for every
    occurrence: on sync the first one's, on async the last one's. On both paths
    every occurrence has one result id.
    """
    node, source, rows = _occurrence_node(ephemeral_mode)
    monkeypatch.setattr(source, "iter_data", lambda: iter(rows))
    sync_delivered = list(node.execute(source, error_policy="fail_fast").iter_data())
    assert _occurrences(sync_delivered) == ["first"] * 4
    assert _occurrences(list(node.iter_data())) == ["first"] * 4
    assert len(set(_ids(sync_delivered))) == 1

    node, _, rows = _occurrence_node(ephemeral_mode)
    async_delivered = asyncio.run(_through_async_execute(node, rows))
    assert _occurrences(async_delivered) == ["first", "second", "third", "second"]
    assert _occurrences(list(node.iter_data())) == ["second"] * 4
    assert len(set(_ids(async_delivered))) == 1
    assert _ids(list(node.iter_data())) == _ids(async_delivered)


def test_a_persistent_hit_is_reused_for_later_occurrences_of_its_instance() -> None:
    """In ephemeral mode the persistent store is asked once per instance in an
    async run, and a hit serves every later occurrence of that instance too:
    nothing is computed and the ephemeral store stays empty."""
    counted = _Counted()
    source = _repeated_input(["a", "a", "b", "a"], 2)
    persistent, ephemeral = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    stored = _seed(persistent, counted, source, {"a"})["a"]
    lookups: list[int] = []
    lookup = persistent.get_records_with_column_value

    def counting(*args: Any, **kwargs: Any) -> Any:
        lookups.append(1)
        return lookup(*args, **kwargs)

    node = _ephemeral_node(
        counted,
        source,
        pipeline=InMemoryArrowDatabase(),
        persistent=persistent,
        ephemeral=ephemeral,
    )
    persistent.get_records_with_column_value = counting  # pyright: ignore[reportAttributeAccessIssue]  # counts the lookups
    delivered = asyncio.run(_through_async_execute(node, list(source.iter_data())))

    assert counted.calls == []
    assert _ids(delivered) == [stored] * 4
    assert _result_ids(ephemeral) == []
    # One lookup by input data hash per instance ("a" and "b"), not per occurrence.
    assert len(lookups) == 2
    assert _ids(list(node.iter_data())) == [stored] * 4


@pytest.mark.parametrize("commit", ["the write's own flush", "a separate flush"])
@PATHS
def test_a_write_made_inside_a_commit_of_its_table_is_not_covered(
    use_async: bool, commit: str
) -> None:
    """A KNOWN LIMIT, not a guarantee. The conditional write does not cover a
    write made from inside a callback that runs during a commit of the same
    result table. Here a schema guard on the result table runs during a commit,
    after the first write's row left the queue and before it is stored, and makes
    a second conditional write for the same input: it finds no result and stores
    its own. The commit is the first write's own flush, or a flush of the
    database called separately after that write returned. The test asserts no
    uniqueness. It holds only what stays true either way: each write returns a
    result that is stored, and nothing else is."""
    database = InMemoryArrowDatabase()
    tag, data = next(iter(_input({"a": 2}).iter_data()))
    pod = _Counted().pod
    outer_pod, inner_pod = (
        CachedFunctionPod(pod, result_database=database) for _ in range(2)
    )
    inner_pod.set_auto_flush(False)
    outer_pod.set_auto_flush(commit == "the write's own flush")
    inner: list[Any] = []

    def during_commit(schema: pa.Schema) -> None:
        if not inner:
            inner.append(inner_pod.process_data(tag, data)[1])

    with database.guard_table_schema(outer_pod.record_path, during_commit):
        if use_async:
            _, outer = asyncio.run(outer_pod.async_process_data(tag, data))
        else:
            _, outer = outer_pod.process_data(tag, data)
    if commit == "a separate flush":
        # The first write has returned; the callback runs in this commit.
        assert not inner
        database.flush()
    database.flush()

    assert outer is not None and len(inner) == 1
    returned = {outer.datagram_uuid.bytes, inner[0].datagram_uuid.bytes}
    stored = _result_ids(database)
    assert set(stored) == returned
    assert len(stored) == len(set(stored))
