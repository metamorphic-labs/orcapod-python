"""``error_policy`` means the same on both orchestrators (ITL-879).

Before the fix the async orchestrator stored ``error_policy`` and never used it: under
``"fail_fast"`` a row whose function raised was skipped as under ``"continue"`` and the run
finished. A failing node's exception also reached the caller inside an ``ExceptionGroup``.
Now ``"fail_fast"`` raises the row's own exception, as the sync orchestrator does, and an
async run raises one exception, never a group.

The jobs are driven through ``PipelineJob.run`` / ``run_async``. Every async run sits inside
an ``asyncio.timeout`` guard, so a hang fails the test instead of the suite.
"""

from __future__ import annotations

import asyncio
import logging
import warnings
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod, side_effect_function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import RowFailuresWarning
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline import async_orchestrator as async_orchestrator_module
from orcapod.pipeline.observer import NoOpObserver
from orcapod.side_effects import InvocationContext, SideEffectPodConfig, side_effect_pod
from orcapod.sources import DictSource
from orcapod.types import PodConfig

RUN_TIMEOUT_SECONDS = 60
N_ROWS = 6

ORCHESTRATORS: dict[str, type[Any]] = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}

# Where the failing row sits among the source rows.
FAILING_POSITIONS = {"first": 0, "middle": N_ROWS // 2, "last": N_ROWS - 1}


# ---------------------------------------------------------------------------
# Functions under test: a row fails when its x is negative
# ---------------------------------------------------------------------------


def check(x: int) -> int:
    """Return ``x``; raise for a negative ``x``."""
    if x < 0:
        raise ValueError(f"bad x={x}")
    return x


checked = function_pod(output_keys="y")(check)


@function_pod(output_keys="z")
def double(y: int) -> int:
    """Return twice ``y``."""
    return 2 * y


@side_effect_function_pod(output_keys="y")
def check_with_ctx(x: int, ctx: InvocationContext) -> int:
    """Return ``x``; raise for a negative ``x`` (a pod that takes the invocation context)."""
    if x < 0:
        raise ValueError(f"bad x={x}")
    return x


def deliver(x: int, ctx: InvocationContext) -> None:
    """A side effect that fails for a negative ``x``."""
    if x < 0:
        raise ValueError(f"bad x={x}")


def xs_failing_at(position: int) -> list[int]:
    """Source values ``0..N-1`` with the value at ``position`` made negative."""
    return [-(i + 1) if i == position else i for i in range(N_ROWS)]


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


def source_of(xs: list[int]) -> DictSource:
    """A source of rows ``k = r<i>``, ``x = xs[i]``."""
    rows = [{"k": f"r{i}", "x": x} for i, x in enumerate(xs)]
    return DictSource(data=rows, tag_columns=["k"], source_id="rows")


def function_job(
    store: Any, xs: list[int], *, max_concurrency: int | None = None
) -> PipelineJob:
    """Source -> ``f`` (fails on a negative x) -> ``down`` (doubles)."""
    job = PipelineJob(name="itl879", database=store)
    with job:
        if max_concurrency is None:
            out = checked.pod(source_of(xs), label="f")
        else:
            pod = FunctionPod(
                PythonDataFunction(check, output_keys="y"),
                pod_config=PodConfig(max_concurrency=max_concurrency),
            )
            out = pod(source_of(xs), label="f")
        double.pod(out, label="down")
    return job


def ctx_function_job(store: Any, xs: list[int]) -> PipelineJob:
    """Source -> ``f`` (a side-effect function pod failing on a negative x) -> ``down``."""
    job = PipelineJob(name="itl879", database=store)
    with job:
        out = check_with_ctx(source_of(xs), label="f")
        double.pod(out, label="down")
    return job


def side_effect_job(store: Any, xs: list[int], on_error: str) -> PipelineJob:
    """Source -> side-effect node ``se`` whose delivery fails on a negative x."""
    pod = side_effect_pod(
        deliver, config=SideEffectPodConfig(on_error=on_error), name="deliver"
    )
    job = PipelineJob(name="itl879", database=store)
    with job:
        pod(source_of(xs), label="se")
    return job


FUNCTION_JOBS: dict[str, Callable[[Any, list[int]], PipelineJob]] = {
    "function": function_job,
    "ctx_function": ctx_function_job,
}


class Recorder(NoOpObserver):
    """Records ``(event, node label, row key)`` for every observer call."""

    def __init__(self) -> None:
        """Start with no events."""
        self.events: list[tuple[str, str, str | None]] = []

    def on_node_start(self, node_label: str, node_hash: str, tag_schema: Any = None):
        """Record a node start."""
        self.events.append(("node_start", node_label, None))

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        """Record a node end."""
        self.events.append(("node_end", node_label, None))

    def on_data_end(self, node_label, tag, input_data, output_data, cached) -> None:
        """Record a row end."""
        self.events.append(("data_end", node_label, tag.as_dict()["k"]))

    def on_data_crash(self, node_label, tag, data, error) -> None:
        """Record a row crash."""
        self.events.append(("data_crash", node_label, tag.as_dict()["k"]))

    def of(self, event: str, label: str) -> list[str | None]:
        """The row keys (or ``None``) of ``event`` calls for ``label``."""
        return [k for e, lbl, k in self.events if e == event and lbl == label]


def run_job(
    job: PipelineJob, orchestrator: str, policy: str, observer: Any = None
) -> Any:
    """Run ``job`` on ``orchestrator`` with ``policy``; async runs have a timeout guard."""
    orch = ORCHESTRATORS[orchestrator](error_policy=policy)
    if orchestrator == "sync":
        return job.run(orchestrator=orch, observer=observer)

    async def guarded() -> Any:
        async with asyncio.timeout(RUN_TIMEOUT_SECONDS):
            return await job.run_async(orchestrator=orch, observer=observer)

    return asyncio.run(guarded())


def ks(job: PipelineJob, label: str) -> list[str]:
    """The sorted row keys node ``label`` of ``job`` reads."""
    return sorted(job.nodes[label].as_table().column("k").to_pylist())


def stored(store: Any, build: Callable[[Any, list[int]], PipelineJob], label: str):
    """The rows a fresh job over ``store`` reads for ``label`` without running."""
    return build(store, xs_failing_at(0)).nodes[label].as_table()


@pytest.fixture(params=list(ORCHESTRATORS))
def orchestrator(request: pytest.FixtureRequest) -> str:
    """Each orchestrator's name."""
    return request.param


@pytest.fixture(params=list(FUNCTION_JOBS))
def build(request: pytest.FixtureRequest) -> Callable[[Any, list[int]], PipelineJob]:
    """Each kind of function job."""
    return FUNCTION_JOBS[request.param]


@pytest.fixture
def caught_warnings() -> Iterator[list[warnings.WarningMessage]]:
    """Every warning issued in the test."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield caught


def row_failure_warnings(caught: list[warnings.WarningMessage]) -> list[str]:
    """The messages of the ``RowFailuresWarning``s in ``caught``."""
    return [str(w.message) for w in caught if w.category is RowFailuresWarning]


# ---------------------------------------------------------------------------
# continue: already the same on both orchestrators; locked here
# ---------------------------------------------------------------------------


def test_continue_skips_and_reports_the_row(
    orchestrator: str, build: Callable[..., PipelineJob], caught_warnings: list[Any]
) -> None:
    """The failing row is skipped, counted, warned about once and seen by the observer."""
    position = FAILING_POSITIONS["middle"]
    job = build(InMemoryArrowDatabase(), xs_failing_at(position))
    recorder = Recorder()

    summary = run_job(job, orchestrator, "continue", recorder)

    node = summary.nodes["f"]
    assert (node.computed, node.reused, node.failed) == (N_ROWS - 1, 0, 1)
    assert isinstance(node.first_error, ValueError)
    assert str(node.first_error) == f"bad x={-(position + 1)}"
    assert summary.nodes["down"].computed == N_ROWS - 1
    messages = row_failure_warnings(caught_warnings)
    assert len(messages) == 1
    assert f"1 of {N_ROWS} rows failed in node 'f'" in messages[0]
    assert recorder.of("data_crash", "f") == [f"r{position}"]
    assert recorder.of("node_end", "f") == [None]
    assert recorder.of("node_end", "down") == [None]
    expected = [f"r{i}" for i in range(N_ROWS) if i != position]
    assert ks(job, "f") == expected
    assert ks(job, "down") == expected


# ---------------------------------------------------------------------------
# fail_fast
# ---------------------------------------------------------------------------


def test_fail_fast_raises_the_row_error(
    orchestrator: str, build: Callable[..., PipelineJob], caught_warnings: list[Any]
) -> None:
    """The run raises the row's own exception: same type and message on both orchestrators."""
    position = FAILING_POSITIONS["middle"]
    job = build(InMemoryArrowDatabase(), xs_failing_at(position))
    recorder = Recorder()

    with pytest.raises(ValueError) as info:
        run_job(job, orchestrator, "fail_fast", recorder)

    assert type(info.value) is ValueError
    assert str(info.value) == f"bad x={-(position + 1)}"
    assert getattr(info.value, "__notes__", []) == []
    assert job.last_run is None
    assert row_failure_warnings(caught_warnings) == []
    assert recorder.of("data_crash", "f") == [f"r{position}"]
    assert recorder.of("node_end", "f") == [None]


@pytest.mark.parametrize("position", list(FAILING_POSITIONS))
def test_fail_fast_stores_only_correct_rows(
    orchestrator: str, build: Callable[..., PipelineJob], position: str
) -> None:
    """Every stored row is its input's correct result; the failing row is never stored."""
    store = InMemoryArrowDatabase()
    xs = xs_failing_at(FAILING_POSITIONS[position])
    with pytest.raises(ValueError):
        run_job(build(store, xs), orchestrator, "fail_fast")

    by_key = {f"r{i}": x for i, x in enumerate(xs)}
    f_rows = stored(store, build, "f").to_pylist()
    for row in f_rows:
        assert row["y"] == by_key[row["k"]] >= 0
    for row in stored(store, build, "down").to_pylist():
        assert row["z"] == 2 * by_key[row["k"]]
        assert by_key[row["k"]] >= 0
    if orchestrator == "sync":
        # The sync run stores exactly the rows before the failing one.
        assert sorted(r["k"] for r in f_rows) == [
            f"r{i}" for i in range(FAILING_POSITIONS[position])
        ]


@pytest.mark.parametrize("max_concurrency", [1, None])
@pytest.mark.parametrize("position", list(FAILING_POSITIONS))
def test_downstream_never_completes_on_partial_input(
    max_concurrency: int | None, position: str
) -> None:
    """The failing node's output ends early; its downstream node must not finish on it.

    The failing node closes its output as it stops. Were the downstream node to read that
    as a normal end of input before the run's cancellation reached it, it would complete
    on part of its input. It must be cancelled instead: no ``on_node_end``.
    """
    job = function_job(
        InMemoryArrowDatabase(),
        xs_failing_at(FAILING_POSITIONS[position]),
        max_concurrency=max_concurrency,
    )
    recorder = Recorder()

    with pytest.raises(ValueError):
        run_job(job, "async", "fail_fast", recorder)

    assert recorder.of("node_end", "down") == []


def test_max_concurrency_one_raises_the_first_failing_row(orchestrator: str) -> None:
    """With one row at a time, the first failing row in input order is the one raised."""
    xs = [0, -1, 2, -3, 4, 5]
    job = function_job(InMemoryArrowDatabase(), xs, max_concurrency=1)

    with pytest.raises(ValueError, match=r"^bad x=-1$"):
        run_job(job, orchestrator, "fail_fast")


def test_nothing_left_running_after_fail_fast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After the raise, no task of the run is pending and every channel is closed."""
    created: list[Any] = []

    # The orchestrator's channels are internal; record them through its module's names.
    class RecordingChannel(async_orchestrator_module.Channel):
        def __post_init__(self) -> None:
            super().__post_init__()
            created.append(self)

    class RecordingBroadcastChannel(async_orchestrator_module.BroadcastChannel):
        def __init__(self, buffer_size: int = 64) -> None:
            super().__init__(buffer_size)
            created.append(self)

    monkeypatch.setattr(async_orchestrator_module, "Channel", RecordingChannel)
    monkeypatch.setattr(
        async_orchestrator_module, "BroadcastChannel", RecordingBroadcastChannel
    )
    job = PipelineJob(name="itl879", database=InMemoryArrowDatabase())
    with job:
        out = checked.pod(source_of(xs_failing_at(2)), label="f")
        double.pod(out, label="down")
        double.pod(out, label="down2")  # fan-out: a broadcast channel

    async def main() -> set[asyncio.Task[Any]]:
        async with asyncio.timeout(RUN_TIMEOUT_SECONDS):
            with pytest.raises(ValueError):
                await job.run_async(
                    orchestrator=AsyncPipelineOrchestrator(error_policy="fail_fast")
                )
        return asyncio.all_tasks() - {asyncio.current_task()}

    assert asyncio.run(main()) == set()
    assert any(isinstance(c, RecordingBroadcastChannel) for c in created)
    for channel in created:
        # Closed state is private to the channel classes.
        closed = channel._closed
        assert closed.is_set() if isinstance(closed, asyncio.Event) else closed


# ---------------------------------------------------------------------------
# Side-effect pods: their own on_error, not error_policy
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("policy", ["continue", "fail_fast"])
def test_side_effect_raise_is_the_delivery_error(orchestrator: str, policy: str) -> None:
    """``on_error="raise"``: the run raises the delivery's own error, never a group."""
    job = side_effect_job(InMemoryArrowDatabase(), xs_failing_at(2), "raise")

    with pytest.raises(ValueError) as info:
        run_job(job, orchestrator, policy)

    assert type(info.value) is ValueError
    assert str(info.value) == "bad x=-3"


def test_side_effect_log_skips_and_warns(
    orchestrator: str, caplog: pytest.LogCaptureFixture
) -> None:
    """``on_error="log"``: logged at WARNING, the run goes on."""
    job = side_effect_job(InMemoryArrowDatabase(), xs_failing_at(2), "log")

    with caplog.at_level(logging.WARNING, logger="orcapod"):
        run_job(job, orchestrator, "fail_fast")

    assert any(
        r.levelno == logging.WARNING and "delivery failed: bad x=-3" in r.getMessage()
        for r in caplog.records
    )


# ---------------------------------------------------------------------------
# Choosing the one exception of an async run
# ---------------------------------------------------------------------------


class _Node:
    """A stand-in node: only its label is read."""

    def __init__(self, label: str) -> None:
        self.label = label


def test_single_failure_takes_the_earliest_node_and_notes_the_others() -> None:
    """The failure of the node first in topological order wins, whatever the group order."""
    # Calls the private helper directly: the test is about its ordering rule, which an
    # end-to-end run cannot force into both group orders.
    from orcapod.pipeline.async_orchestrator import _single_failure

    topo_order = [_Node("up"), _Node("down")]
    for order in ("up_first", "down_first"):
        up_error = ValueError("up failed")
        down_error = KeyError("down failed")
        failed_nodes = {id(up_error): "up", id(down_error): "down"}
        leaves = [up_error, down_error] if order == "up_first" else [down_error, up_error]
        group = ExceptionGroup("run", [ExceptionGroup("node", leaves)])

        chosen = _single_failure(group, topo_order, failed_nodes)

        assert chosen is up_error
        assert up_error.__notes__ == [
            "Node 'down' also failed in the same run: KeyError: 'down failed'"
        ]


def test_single_failure_alone_is_raised_unchanged() -> None:
    """One failure is returned as it is, with no note."""
    # Calls the private helper directly, as above.
    from orcapod.pipeline.async_orchestrator import _single_failure

    error = ValueError("only")
    chosen = _single_failure(
        ExceptionGroup("run", [error]), [_Node("f")], {id(error): "f"}
    )
    assert chosen is error
    assert getattr(error, "__notes__", None) is None
