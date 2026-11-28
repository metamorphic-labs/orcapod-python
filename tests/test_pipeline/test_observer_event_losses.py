"""A run says which observer events it could not record (ITL-1556).

The status and logging observers record run events as rows in a database. A write that
fails does not stop the run (its results do not depend on those rows), but the loss must
not be silent: the run's ``RunSummary`` counts the lost events per observer kind with the
first error's text, and one ``ObserverEventsLostWarning`` is issued at the end of the run.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import warnings
from collections.abc import Callable
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import ObserverEventsLostWarning
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    CompositeObserver,
    LoggingObserver,
    LostObserverEvents,
    RunSummary,
    StatusObserver,
    SyncPipelineOrchestrator,
)
from tests.test_core.operators.several_items import (
    DATA_FIRST_REVERSED,
    MAIN,
    MIXED,
    PLAIN,
    TAGS_FIRST,
    Arrangement,
    make_source,
)

ORCHESTRATORS: dict[str, Callable[[], Any]] = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}

# Three tag and three data columns, in two arrangements (tag order, layout, roles).
ARRANGEMENTS = [
    Arrangement(MAIN.tags, TAGS_FIRST, PLAIN),
    Arrangement(tuple(reversed(MAIN.tags)), DATA_FIRST_REVERSED, MIXED),
]
N_ROWS = 6
LABEL = "described"


@function_pod(output_keys=("n", "s"))
def describe(d1: int, d3: str) -> tuple[int, str]:
    """Return ``d1 + 1`` and ``d3`` doubled."""
    return d1 + 1, d3 + d3


class _Refusals:
    """Which writes a ``_RefusingDatabase`` and its scoped views refuse.

    Attributes:
        accept_first: The number of writes accepted before every later one is refused;
            ``None`` accepts every write.
        writes: The writes attempted so far.
    """

    def __init__(self, accept_first: int | None) -> None:
        self.accept_first: int | None = accept_first
        self.writes: int = 0
        # Observer hooks may be called from several threads.
        self.lock: threading.Lock = threading.Lock()


class _RefusingDatabase:
    """A database that refuses ``add_record`` as its ``_Refusals`` says.

    Everything else is the wrapped in-memory database's; a scoped view (``at``) shares
    the refusals, so a write counts whichever node made it.
    """

    def __init__(self, database: InMemoryArrowDatabase, refusals: _Refusals) -> None:
        self._database = database
        self.refusals = refusals

    def at(self, *path: str) -> _RefusingDatabase:
        """Return the scoped view, refusing by the same rule."""
        return _RefusingDatabase(self._database.at(*path), self.refusals)

    def add_record(self, *args: Any, **kwargs: Any) -> None:
        """Write, or refuse with an ``OSError``."""
        refusals = self.refusals
        with refusals.lock:
            refusals.writes += 1
            refused = (
                refusals.accept_first is not None
                and refusals.writes > refusals.accept_first
            )
        if refused:
            raise OSError("store refuses writes")
        self._database.add_record(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._database, name)


def _database(accept_first: int | None = 0) -> Any:
    """A database refusing every write after the first ``accept_first``."""
    return _RefusingDatabase(InMemoryArrowDatabase(), _Refusals(accept_first))


def _job(arrangement: Arrangement, store: Any = None) -> PipelineJob:
    """``MAIN`` in ``arrangement`` feeding ``describe`` under ``LABEL``."""
    job = PipelineJob(name="losses", database=store or InMemoryArrowDatabase())
    with job:
        describe.pod(make_source(MAIN, arrangement), label=LABEL)
    return job


def _rows(job: PipelineJob) -> list[tuple[Any, ...]]:
    """The node's rows, as sorted ``(t1, t2, t3, n, s)``."""
    return sorted(
        (row["t1"], row["t2"], row["t3"], row["n"], row["s"])
        for row in job.nodes[LABEL].as_table().to_pylist()
    )


EXPECTED_ROWS = sorted(
    (t1, t2, t3, d1 + 1, d3 + d3)
    for t1, t2, t3, d1, d3 in zip(
        *(MAIN.columns[name].to_pylist() for name in ("t1", "t2", "t3", "d1", "d3"))
    )
)


def _run_losing(job: PipelineJob, **kwargs: Any) -> tuple[RunSummary, list[str]]:
    """Run ``job``; return its summary and the lost-events warnings' messages."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        summary = job.run(**kwargs)
    return summary, [
        str(w.message)
        for w in caught
        if issubclass(w.category, ObserverEventsLostWarning)
    ]


@pytest.fixture(params=list(ORCHESTRATORS))
def orchestrator(request: pytest.FixtureRequest) -> Any:
    """Each orchestrator, freshly built."""
    return ORCHESTRATORS[request.param]()


@pytest.fixture(params=ARRANGEMENTS, ids=lambda arrangement: arrangement.id)
def arrangement(request: pytest.FixtureRequest) -> Arrangement:
    """Each arrangement of the source's columns."""
    return request.param


def test_a_store_that_refuses_every_write_is_reported(
    orchestrator: Any, arrangement: Arrangement
) -> None:
    """Every lost status and log event is counted; the run and its rows are unchanged."""
    observer = CompositeObserver(
        StatusObserver(_database()), LoggingObserver(_database())
    )
    job = _job(arrangement)

    summary, messages = _run_losing(job, orchestrator=orchestrator, observer=observer)

    # RUNNING and SUCCESS per row; one log row per row.
    assert summary.lost_observer_events == {
        "status": LostObserverEvents(
            "status", 2 * N_ROWS, "OSError: store refuses writes"
        ),
        "logging": LostObserverEvents(
            "logging", N_ROWS, "OSError: store refuses writes"
        ),
    }
    assert summary.ok
    node = summary.nodes[LABEL]
    assert (node.computed, node.reused, node.failed) == (N_ROWS, 0, 0)
    assert _rows(job) == EXPECTED_ROWS
    assert job.last_run is summary

    assert len(messages) == 1
    assert f"{2 * N_ROWS} status events" in messages[0]
    assert f"{N_ROWS} logging events" in messages[0]
    assert "OSError: store refuses writes" in messages[0]
    assert "lost_observer_events" in messages[0]


@pytest.mark.parametrize("kind", ["status", "logging"])
def test_one_observer_alone_reports_its_kind_only(
    kind: str, orchestrator: Any, arrangement: Arrangement
) -> None:
    """A status or a logging observer given on its own reports its own events."""
    observer: Any = (
        StatusObserver(_database())
        if kind == "status"
        else LoggingObserver(_database())
    )
    job = _job(arrangement)

    summary, messages = _run_losing(job, orchestrator=orchestrator, observer=observer)

    count = 2 * N_ROWS if kind == "status" else N_ROWS
    assert summary.lost_observer_events == {
        kind: LostObserverEvents(kind, count, "OSError: store refuses writes")
    }
    assert len(messages) == 1
    assert _rows(job) == EXPECTED_ROWS


def test_observers_nested_in_composites_are_reported(
    orchestrator: Any, arrangement: Arrangement
) -> None:
    """A composite inside a composite, and two observers of one kind, all count."""
    observer = CompositeObserver(
        CompositeObserver(StatusObserver(_database())),
        StatusObserver(_database()),
        LoggingObserver(_database()),
    )
    job = _job(arrangement)

    summary, messages = _run_losing(job, orchestrator=orchestrator, observer=observer)

    assert {
        kind: lost.count for kind, lost in summary.lost_observer_events.items()
    } == {"status": 4 * N_ROWS, "logging": N_ROWS}
    assert list(summary.lost_observer_events) == ["logging", "status"]
    assert len(messages) == 1


def test_a_store_that_fails_partway_reports_only_the_events_after(
    orchestrator: Any, arrangement: Arrangement
) -> None:
    """Events written before the store failed are kept and not counted as lost."""
    status_database = _database(accept_first=5)
    logging_database = _database(accept_first=2)
    status = StatusObserver(status_database)
    logs = LoggingObserver(logging_database)
    job = _job(arrangement)

    summary, messages = _run_losing(
        job, orchestrator=orchestrator, observer=CompositeObserver(status, logs)
    )

    assert summary.lost_observer_events == {
        "status": LostObserverEvents(
            "status", 2 * N_ROWS - 5, "OSError: store refuses writes"
        ),
        "logging": LostObserverEvents(
            "logging", N_ROWS - 2, "OSError: store refuses writes"
        ),
    }
    assert len(messages) == 1
    stored_status = status.get_status()
    assert stored_status is not None and stored_status.num_rows == 5
    stored_logs = logs.get_logs()
    assert stored_logs is not None and stored_logs.num_rows == 2
    assert _rows(job) == EXPECTED_ROWS


def test_a_run_that_records_every_event_reports_nothing(
    orchestrator: Any, arrangement: Arrangement
) -> None:
    """No loss: an empty mapping and no warning."""
    observer = CompositeObserver(
        StatusObserver(_database(accept_first=None)),
        LoggingObserver(_database(accept_first=None)),
    )
    job = _job(arrangement)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        summary = job.run(orchestrator=orchestrator, observer=observer)

    assert summary.lost_observer_events == {}
    assert "observer events" not in repr(summary)
    assert "observer events" not in summary._repr_html_()


def test_run_async_reports_lost_events(arrangement: Arrangement) -> None:
    """``await job.run_async()`` reports and warns as ``job.run()`` does."""
    observer = CompositeObserver(
        StatusObserver(_database()), LoggingObserver(_database())
    )
    job = _job(arrangement)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        summary = asyncio.run(job.run_async(observer=observer))

    assert {
        kind: lost.count for kind, lost in summary.lost_observer_events.items()
    } == {"status": 2 * N_ROWS, "logging": N_ROWS}
    lost_warnings = [
        w for w in caught if issubclass(w.category, ObserverEventsLostWarning)
    ]
    assert len(lost_warnings) == 1
    assert _rows(job) == EXPECTED_ROWS


def test_a_load_only_run_reports_lost_events(
    orchestrator: Any, arrangement: Arrangement
) -> None:
    """A load-only run's status events (RUNNING and CACHED per loaded row) count too."""
    store = InMemoryArrowDatabase()
    _ = _job(arrangement, store).run()
    job = _job(arrangement, store)
    observer = CompositeObserver(
        StatusObserver(_database()), LoggingObserver(_database())
    )

    summary, messages = _run_losing(
        job, orchestrator=orchestrator, observer=observer, load_only=True
    )

    # No function is called in a load-only run, so there is no log row to lose.
    assert summary.load_only
    assert summary.lost_observer_events == {
        "status": LostObserverEvents(
            "status", 2 * N_ROWS, "OSError: store refuses writes"
        )
    }
    assert summary.nodes[LABEL].reused == N_ROWS
    assert len(messages) == 1
    assert _rows(job) == EXPECTED_ROWS


def test_each_run_reports_its_own_losses(
    orchestrator: Any, arrangement: Arrangement
) -> None:
    """A later run with the same observer does not repeat an earlier run's losses."""
    status_database = _database()
    observer = StatusObserver(status_database)
    store = InMemoryArrowDatabase()

    first, first_messages = _run_losing(
        _job(arrangement, store), orchestrator=orchestrator, observer=observer
    )
    status_database.refusals.accept_first = None
    second_job = _job(arrangement, store)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        second = second_job.run(orchestrator=orchestrator, observer=observer)
        # Every node now holds a current run: nothing runs and nothing can be lost.
        held = second_job.run(orchestrator=orchestrator, observer=observer)

    assert first.lost_observer_events["status"].count == 2 * N_ROWS
    assert len(first_messages) == 1
    assert second.lost_observer_events == {}
    assert held.ran_nothing and held.lost_observer_events == {}


def test_a_job_nodes_own_run_takes_no_observer_and_reports_no_loss(
    arrangement: Arrangement,
) -> None:
    """``node.run()`` has no observer argument, so its summary lists no lost event."""
    job = _job(arrangement)
    node: Any = job.nodes[LABEL]

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _ = node.run()
        full = node.last_run
        node.clear_cache()
        _ = node.run(load_only=True)
        loaded = node.last_run

    assert full.lost_observer_events == {}
    assert loaded.load_only and loaded.lost_observer_events == {}


def test_the_summary_shows_the_lost_events(arrangement: Arrangement) -> None:
    """``repr`` and the notebook display name each kind, its count and first error."""
    observer = CompositeObserver(
        StatusObserver(_database()), LoggingObserver(_database())
    )
    summary, _ = _run_losing(_job(arrangement), observer=observer)

    line = (
        f"observer events not recorded: {N_ROWS} logging events (first error: "
        f"OSError: store refuses writes); {2 * N_ROWS} status events (first error: "
        "OSError: store refuses writes)"
    )
    assert line in repr(summary)
    assert line in summary._repr_html_()
    # The headline still speaks of rows only: no existing field changed meaning.
    assert "all rows succeeded" in repr(summary)


def test_the_log_line_of_each_failed_write_is_kept(
    arrangement: Arrangement, caplog: pytest.LogCaptureFixture
) -> None:
    """Each failed write is still logged with its traceback, when it happens."""
    observer = CompositeObserver(
        StatusObserver(_database()), LoggingObserver(_database())
    )
    with caplog.at_level(logging.ERROR, logger="orcapod.pipeline"):
        _ = _run_losing(_job(arrangement), observer=observer)

    status_lines = [r for r in caplog.records if "status event" in r.getMessage()]
    log_lines = [r for r in caplog.records if "log row" in r.getMessage()]
    assert len(status_lines) == 2 * N_ROWS
    assert len(log_lines) == N_ROWS
    assert all(r.exc_info is not None for r in status_lines + log_lines)


def test_the_warning_can_be_turned_into_an_error_after_the_run_is_kept(
    arrangement: Arrangement,
) -> None:
    """With the warning as an error, the run is complete and kept before it raises."""
    job = _job(arrangement)
    with warnings.catch_warnings():
        warnings.simplefilter("error", ObserverEventsLostWarning)
        with pytest.raises(ObserverEventsLostWarning):
            _ = job.run(observer=StatusObserver(_database()))

    assert job.last_run is not None
    assert job.last_run.lost_observer_events["status"].count == 2 * N_ROWS
    assert _rows(job) == EXPECTED_ROWS
