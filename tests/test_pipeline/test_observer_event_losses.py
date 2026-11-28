"""A run says which observer events it could not record (ITL-1556).

The status and logging observers record run events as rows in a database. A write that
fails does not stop the run (its results do not depend on those rows), but the loss must
not be silent: the run's ``RunSummary`` counts the lost events per observer kind with the
first error's text, and one ``ObserverEventsLostWarning`` is issued at the end of the run.
"""

from __future__ import annotations

import asyncio
import inspect
import io
import logging
import threading
import warnings
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from functools import wraps
from typing import TYPE_CHECKING, Any, cast

import pytest
from hypothesis import HealthCheck, example, given, settings, strategies as st
from typing_extensions import override

from orcapod import PipelineJob, function_pod
from orcapod.core.datagrams import Data, Tag
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import ObserverEventsLostWarning, RowFailuresWarning
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    CompositeObserver,
    LoggingObserver,
    LostObserverEvents,
    RunSummary,
    StatusObserver,
    SyncPipelineOrchestrator,
)
from orcapod.pipeline.observer import NoOpObserver
from orcapod.pipeline.observer_losses import RunLosses, report_lost_event
from tests.test_core.operators.several_items import (
    DATA_FIRST_REVERSED,
    MAIN,
    MIXED,
    PLAIN,
    TAGS_FIRST,
    Arrangement,
    make_source,
)

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

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


@function_pod(output_keys=("n", "s"))
def describe_and_log(d1: int, d3: str) -> tuple[int, str]:
    """Log a filename before returning the row's result."""
    logging.getLogger(__name__).warning("filename: bad\udcff")
    return d1 + 1, d3 + d3


@function_pod(output_keys=("n", "s"))
def describe_except_first(d1: int, d3: str) -> tuple[int, str]:
    """Raise for one of the six input rows."""
    if d1 == 10:
        raise ValueError("bad row")
    return d1 + 1, d3 + d3


@function_pod(output_keys=("n", "s"))
def describe_except_unencodable_error(d1: int, d3: str) -> tuple[int, str]:
    """Raise with a filesystem-decoded filename for the first row."""
    if d1 == 10:
        raise ValueError("filename: bad\udcff")
    return d1 + 1, d3 + d3


class _UnprintableRowError(ValueError):
    @override
    def __str__(self) -> str:
        raise RuntimeError("cannot format row error")


@function_pod(output_keys=("n", "s"))
def describe_except_unprintable_error(d1: int, d3: str) -> tuple[int, str]:
    """Raise an error whose message cannot be formatted for the first row."""
    if d1 == 10:
        raise _UnprintableRowError()
    return d1 + 1, d3 + d3


class _Refusals:
    """Which writes a ``_RefusingDatabase`` and its scoped views refuse.

    Attributes:
        accept_first: The number of writes accepted before every later one is refused;
            ``None`` accepts every write.
        writes: The writes attempted so far.
    """

    def __init__(
        self, accept_first: int | None, error: type[OSError] = OSError
    ) -> None:
        self.accept_first: int | None = accept_first
        self.error = error
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
            raise refusals.error("store refuses writes")
        self._database.add_record(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._database, name)


def _database(accept_first: int | None = 0, *, error: type[OSError] = OSError) -> Any:
    """A database refusing every write after the first ``accept_first``."""
    return _RefusingDatabase(InMemoryArrowDatabase(), _Refusals(accept_first, error))


def _job(
    arrangement: Arrangement, store: Any = None, *, pod: Any = describe
) -> PipelineJob:
    """``MAIN`` in ``arrangement`` feeding ``describe`` under ``LABEL``."""
    job = PipelineJob(name="losses", database=store or InMemoryArrowDatabase())
    with job:
        pod.pod(make_source(MAIN, arrangement), label=LABEL)
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


class _CompletionCounter:
    """An observer subclass must receive its own lifecycle events."""

    finished: int = 0

    def __copy__(self) -> Any:
        """Require lifecycle hooks to use the original instance."""
        raise AssertionError("custom observers cannot be copied")


class _CustomComposite(_CompletionCounter, CompositeObserver):
    """A composite with a stateful lifecycle hook."""

    def _contextualize(self) -> str:
        """Return an application-specific context label."""
        return "custom"

    @override
    def on_run_end(self, run_id: str) -> None:
        self.finished += 1
        super().on_run_end(run_id)


class _CustomStatus(_CompletionCounter, StatusObserver):
    """A status observer with a stateful lifecycle hook."""

    @override
    def on_run_end(self, run_id: str) -> None:
        self.finished += 1
        super().on_run_end(run_id)


class _CustomLogging(_CompletionCounter, LoggingObserver):
    """A logging observer with a stateful lifecycle hook."""

    @override
    def on_run_end(self, run_id: str) -> None:
        self.finished += 1
        super().on_run_end(run_id)


@pytest.fixture(params=["sync", "async", "run_async"])
def run_job(request: pytest.FixtureRequest) -> Callable[[PipelineJob, Any], RunSummary]:
    """Run a job through each public run path."""
    if request.param == "run_async":
        return lambda job, observer: asyncio.run(job.run_async(observer=observer))
    return lambda job, observer: job.run(
        orchestrator=ORCHESTRATORS[request.param](), observer=observer
    )


@pytest.mark.parametrize("kind", ["composite", "status", "logging"])
def test_custom_observers_keep_their_lifecycle(
    kind: str,
    arrangement: Arrangement,
    run_job: Callable[[PipelineJob, Any], RunSummary],
) -> None:
    """Job accounting leaves the caller's custom observer in place."""
    observers = {
        "composite": _CustomComposite(StatusObserver(InMemoryArrowDatabase())),
        "status": _CustomStatus(InMemoryArrowDatabase()),
        "logging": _CustomLogging(InMemoryArrowDatabase()),
    }
    observer = observers[kind]
    job = _job(arrangement)

    assert run_job(job, observer).ok
    assert observer.finished == 1
    assert _rows(job) == EXPECTED_ROWS


@pytest.mark.parametrize("comparison_raises", [True, False], ids=["raises", "matches"])
def test_custom_observer_class_is_never_compared(
    arrangement: Arrangement,
    run_job: Callable[[PipelineJob, StatusObserver], RunSummary],
    comparison_raises: bool,
) -> None:
    """Only concrete built-in observers have their losses counted by the job."""

    class ObserverMeta(type):
        @override
        def __eq__(cls, other: object) -> bool:
            if comparison_raises:
                raise TypeError("this application does not compare observer classes")
            return True

    class CustomObserver(StatusObserver, metaclass=ObserverMeta):
        """Scope to a built-in status writer through the inherited hook."""

    database = _RefusingDatabase(InMemoryArrowDatabase(), _Refusals(0))
    # The test double delegates the rest of the database protocol via __getattr__.
    observer = CustomObserver(cast("ArrowDatabaseProtocol", cast("object", database)))
    job = _job(arrangement)
    with warnings.catch_warnings():
        warnings.simplefilter("error", ObserverEventsLostWarning)
        summary = run_job(job, observer)

    assert summary.ok
    assert database.refusals.writes == 2 * N_ROWS
    assert summary.lost_observer_events == {}
    assert _rows(job) == EXPECTED_ROWS


def test_custom_completion_observer_sees_the_current_logging_run(
    arrangement: Arrangement, run_job: Callable[[PipelineJob, Any], RunSummary]
) -> None:
    """A custom hook sees the caller's built-in logger updated by the run."""
    db = InMemoryArrowDatabase()
    logging_observer = LoggingObserver(db)

    class LogCompletion(NoOpObserver):
        run_id: str = ""

        @override
        def on_run_end(self, run_id: str) -> None:
            self.run_id = run_id
            logger = logging_observer.create_data_logger(
                Tag({"t1": "completion", "t2": 0, "t3": True}), Data({})
            )
            logger.record(success=True)

    completion = LogCompletion()
    job = _job(arrangement)
    observer = CompositeObserver(CompositeObserver(logging_observer), completion)

    assert run_job(job, observer).ok
    root_logs = db.get_all_records(("execution_logs",))
    assert root_logs is not None
    assert root_logs["_log_run_id"].to_pylist() == [completion.run_id]
    all_logs = logging_observer.get_logs()
    assert all_logs is not None
    assert all_logs["_log_run_id"].to_pylist() == [completion.run_id] * (N_ROWS + 1)


class _Utf8CaptureFormatter(logging.Formatter):
    """Escape lone surrogates after pytest formats the message and traceback."""

    def __init__(self, formatter: logging.Formatter | None) -> None:
        super().__init__()
        self._formatter: logging.Formatter = formatter or logging.Formatter()

    @override
    def format(self, record: logging.LogRecord) -> str:
        return (
            self._formatter.format(record)
            .encode("utf-8", "backslashreplace")
            .decode("utf-8")
        )


@pytest.fixture
def utf8_log_capture(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> pytest.LogCaptureFixture:
    """Keep the original records and make both pytest log captures safe for xdist."""
    # Pytest captures logs for caplog and for its test report with separate handlers.
    for handler in logging.getLogger().handlers:
        if isinstance(handler, type(caplog.handler)):
            monkeypatch.setattr(
                handler, "formatter", _Utf8CaptureFormatter(handler.formatter)
            )
    return caplog


class _WriteErrorWithSurrogate(OSError):
    @override
    def __str__(self) -> str:
        return "unreadable filename: café\udcff"


@pytest.mark.usefixtures("utf8_log_capture")
@pytest.mark.parametrize("encoding", ["utf-8", "ascii"])
def test_observer_warning_displays_on_strict_stderr(
    arrangement: Arrangement,
    run_job: Callable[[PipelineJob, Any], RunSummary],
    monkeypatch: pytest.MonkeyPatch,
    encoding: str,
) -> None:
    """The ordinary warning path can display any failed write's error text."""
    observer = StatusObserver(_database(error=_WriteErrorWithSurrogate))
    job = _job(arrangement)
    with (
        io.TextIOWrapper(io.BytesIO(), encoding=encoding) as stderr,
        warnings.catch_warnings(),
    ):

        def show_on_stderr(
            message: Warning | str,
            category: type[Warning],
            filename: str,
            lineno: int,
            file: Any = None,
            line: str | None = None,
        ) -> None:
            _ = stderr.write(
                warnings.formatwarning(message, category, filename, lineno, line)
            )

        # Format and write the warning even inside pytest's recorder.
        monkeypatch.setattr(warnings, "showwarning", show_on_stderr)
        warnings.simplefilter("default", ObserverEventsLostWarning)
        summary = run_job(job, observer)
        _ = stderr.seek(0)
        message = stderr.read()

    assert message.count("ObserverEventsLostWarning:") == 1
    assert "unreadable filename:" in message
    assert r"\udcff" in message
    assert summary.lost_observer_events["status"].first_error.endswith(r"caf\xe9\udcff")
    assert summary is job.last_run and summary.ok
    assert _rows(job) == EXPECTED_ROWS


@pytest.mark.parametrize("nested", [False, True], ids=["direct", "nested"])
@pytest.mark.parametrize("kind", ["composite", "logging", "status"])
def test_concrete_observer_uses_its_custom_contextualize(
    arrangement: Arrangement,
    run_job: Callable[[PipelineJob, Any], RunSummary],
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    nested: bool,
) -> None:
    """An instance's hook controls node scoping and owns its delegated observer."""
    originals = {
        "composite": CompositeObserver(),
        "logging": LoggingObserver(InMemoryArrowDatabase()),
        "status": StatusObserver(InMemoryArrowDatabase()),
    }
    original = originals[kind]
    database = _database()
    delegate = StatusObserver(database)
    paths: list[tuple[str, ...]] = []

    def scope(*path: str) -> Any:
        paths.append(path)
        return delegate.contextualize(*path)

    monkeypatch.setattr(original, "contextualize", scope)
    observer = CompositeObserver(original) if nested else original
    job = _job(arrangement)
    with warnings.catch_warnings():
        warnings.simplefilter("error", ObserverEventsLostWarning)
        summary = run_job(job, observer)

    assert paths.count(job.nodes[LABEL].node_identity_path) == 1
    assert database.refusals.writes == 2 * N_ROWS
    assert summary.lost_observer_events == {}
    assert summary.ok and _rows(job) == EXPECTED_ROWS


def test_unencodable_log_event_preserves_rows(
    arrangement: Arrangement,
    run_job: Callable[[PipelineJob, Any], RunSummary],
    utf8_log_capture: pytest.LogCaptureFixture,
) -> None:
    """A filename containing a surrogate can lose a log row but not a result."""
    observer = LoggingObserver(InMemoryArrowDatabase())
    job = _job(arrangement, pod=describe_and_log)

    with pytest.warns(ObserverEventsLostWarning, match="UnicodeEncodeError") as caught:
        summary = run_job(job, observer)

    assert len(caught) == 1
    assert summary.nodes[LABEL].computed == N_ROWS
    assert summary.ok
    assert summary.lost_observer_events["logging"].count == N_ROWS
    assert _rows(job) == EXPECTED_ROWS
    assert rb"filename: bad\udcff" in utf8_log_capture.text.encode("utf-8")


def test_promoted_row_warning_still_reports_observer_losses(
    arrangement: Arrangement, run_job: Callable[[PipelineJob, Any], RunSummary]
) -> None:
    """A completed run reports losses even when the row warning raises."""
    job = _job(arrangement, pod=describe_except_first)
    observer = StatusObserver(_database())

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ObserverEventsLostWarning)
        warnings.simplefilter("error", RowFailuresWarning)
        with pytest.raises(RowFailuresWarning):
            _ = run_job(job, observer)

    summary = job.last_run
    assert summary is not None
    assert summary.nodes[LABEL].computed == N_ROWS - 1
    assert summary.nodes[LABEL].failed == 1
    assert summary.lost_observer_events["status"].count == 2 * N_ROWS
    assert [warning.category for warning in caught] == [ObserverEventsLostWarning]


@pytest.mark.parametrize("category", [RowFailuresWarning, ObserverEventsLostWarning])
def test_completion_warning_interrupt_propagates_after_report(
    arrangement: Arrangement,
    run_job: Callable[[PipelineJob, Any], RunSummary],
    category: type[Warning],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An interrupt propagates after the completed run reports its observer losses."""
    interrupts_row_warning = category is RowFailuresWarning
    pod = describe_except_first if interrupts_row_warning else describe
    job = _job(arrangement, pod=pod)
    observer = StatusObserver(_database())
    interrupt = KeyboardInterrupt("warning interrupted")
    shown: list[type[Warning]] = []

    def show_warning(
        _message: Warning, emitted_category: type[Warning], *args: object
    ) -> None:
        shown.append(emitted_category)
        if emitted_category is category:
            raise interrupt

    with warnings.catch_warnings():
        warnings.simplefilter("always")
        monkeypatch.setattr(warnings, "showwarning", show_warning)
        with pytest.raises(KeyboardInterrupt) as raised:
            _ = run_job(job, observer)

    assert raised.value is interrupt
    summary = job.last_run
    assert summary is not None
    assert summary.lost_observer_events["status"].count == 2 * N_ROWS
    if interrupts_row_warning:
        assert shown == [RowFailuresWarning, ObserverEventsLostWarning]
    else:
        assert shown == [ObserverEventsLostWarning]
        assert caplog.text.count("Observer events were not recorded in this run:") == 1


@pytest.mark.parametrize("promote_observer_warning", [False, True])
def test_unprintable_row_error_still_reports_observer_losses(
    arrangement: Arrangement,
    run_job: Callable[[PipelineJob, Any], RunSummary],
    promote_observer_warning: bool,
) -> None:
    """A row's broken formatter remains the primary error after completion."""
    job = _job(arrangement, pod=describe_except_unprintable_error)
    observer = StatusObserver(InMemoryArrowDatabase())

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter(
            "error" if promote_observer_warning else "always", ObserverEventsLostWarning
        )
        with pytest.raises(RuntimeError, match="cannot format row error") as error:
            _ = run_job(job, observer)

    summary = job.last_run
    assert summary is not None
    assert summary.nodes[LABEL].computed == N_ROWS - 1
    assert summary.nodes[LABEL].failed == 1
    assert summary.lost_observer_events["status"].count == 1
    if promote_observer_warning:
        assert isinstance(error.value.__cause__, ObserverEventsLostWarning)
    else:
        assert [warning.category for warning in caught] == [ObserverEventsLostWarning]


def test_unencodable_status_event_preserves_other_rows(
    arrangement: Arrangement,
    run_job: Callable[[PipelineJob, Any], RunSummary],
    utf8_log_capture: pytest.LogCaptureFixture,
) -> None:
    """A status row with an unencodable error is lost and the job still finishes."""
    job = _job(arrangement, pod=describe_except_unencodable_error)
    observer = StatusObserver(InMemoryArrowDatabase())

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RowFailuresWarning)
        warnings.simplefilter("always", ObserverEventsLostWarning)
        summary = run_job(job, observer)

    assert summary.nodes[LABEL].computed == N_ROWS - 1
    assert summary.nodes[LABEL].failed == 1
    assert summary.lost_observer_events["status"].count == 1
    assert summary.lost_observer_events["status"].first_error.startswith(
        "UnicodeEncodeError:"
    )
    assert [warning.category for warning in caught] == [
        RowFailuresWarning,
        ObserverEventsLostWarning,
    ]
    assert _rows(job) == [row for row in EXPECTED_ROWS if row[3] != 11]
    assert rb"filename: bad\udcff" in utf8_log_capture.text.encode("utf-8")


def test_run_async_is_a_coroutine_function() -> None:
    """Both standard coroutine-function checks recognize the public method."""
    run_async = _job(ARRANGEMENTS[0]).run_async
    assert inspect.iscoroutinefunction(run_async)
    assert asyncio.iscoroutinefunction(run_async)  # pyright: ignore[reportDeprecated]  # verifies existing asyncio callers


def test_run_async_warning_points_to_the_await(
    arrangement: Arrangement,
) -> None:
    """The completion warning points to the line awaiting the coroutine."""
    job = _job(arrangement)
    observer = StatusObserver(_database())

    async def run() -> int:
        pending = job.run_async(observer=observer)
        frame = inspect.currentframe()
        assert frame is not None
        await_line = frame.f_lineno + 1
        _ = await pending
        return await_line

    with pytest.warns(ObserverEventsLostWarning) as caught:
        await_line = asyncio.run(run())

    assert len(caught) == 1
    assert (caught[0].filename, caught[0].lineno) == (__file__, await_line)


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


def test_precontextualized_status_observer_preserves_empty_metadata(
    arrangement: Arrangement, run_job: Callable[[PipelineJob, Any], RunSummary]
) -> None:
    """Re-contextualized status rows retain main's empty metadata and report losses."""
    observer = StatusObserver(_database(accept_first=1))
    scoped = observer.contextualize("tenant")
    with pytest.warns(ObserverEventsLostWarning):
        summary = run_job(_job(arrangement), scoped)

    assert summary.lost_observer_events["status"].count == 2 * N_ROWS - 1
    table = observer.get_status()
    assert table is not None
    assert table["_status_run_id"].to_pylist() == [""]
    assert table["_status_pipeline_uri"].to_pylist() == [""]


def test_overlapping_async_jobs_can_share_observers() -> None:
    """Both jobs count the events lost by their own execution."""
    observer = CompositeObserver(
        StatusObserver(_database()), LoggingObserver(_database())
    )
    first, second = (_job(arrangement) for arrangement in ARRANGEMENTS)

    async def run_both() -> tuple[RunSummary, RunSummary]:
        return await asyncio.gather(
            first.run_async(observer=observer), second.run_async(observer=observer)
        )

    with pytest.warns(ObserverEventsLostWarning) as caught:
        summaries = asyncio.run(run_both())

    assert len(caught) == 2
    for summary in summaries:
        assert {
            kind: lost.count for kind, lost in summary.lost_observer_events.items()
        } == {
            "status": 2 * N_ROWS,
            "logging": N_ROWS,
        }
    assert _rows(first) == _rows(second) == EXPECTED_ROWS


class _RunStart(NoOpObserver):
    """Remember the ID delivered to the observer at the start of the run."""

    run_id: str = ""

    @override
    def on_run_start(self, run_id: str, pipeline_uri: str = "") -> None:
        self.run_id = run_id


class _GateRunStart(_RunStart):
    """Hold a run in its start hook while a second run finishes."""

    def __init__(self) -> None:
        self.started: threading.Event = threading.Event()
        self.release: threading.Event = threading.Event()

    @override
    def on_run_start(self, run_id: str, pipeline_uri: str = "") -> None:
        super().on_run_start(run_id, pipeline_uri)
        self.started.set()
        assert self.release.wait(timeout=10)


@pytest.mark.parametrize("same_id", [False, True], ids=["shared-observer", "same-id"])
def test_overlapping_sync_jobs_keep_separate_losses(
    same_id: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sharing the observer or reusing the public ID cannot connect two runs."""
    if same_id:
        original = PipelineJob._prepare_run  # pyright: ignore[reportPrivateUsage]

        def prepare_run(job: PipelineJob, *args: Any, **kwargs: Any) -> Any:
            return replace(original(job, *args, **kwargs), run_id="12345678abcd4eee")

        monkeypatch.setattr(PipelineJob, "_prepare_run", prepare_run)

    gate = _GateRunStart()
    second_start = _RunStart()
    first, second = (_job(arrangement) for arrangement in ARRANGEMENTS)
    status = StatusObserver(_database())
    second_status = StatusObserver(_database()) if same_id else status
    first_observer: Any = CompositeObserver(status, gate)
    second_observer: Any = CompositeObserver(second_status, second_start)
    with (
        pytest.warns(ObserverEventsLostWarning) as caught,
        ThreadPoolExecutor(2) as pool,
    ):
        pending = pool.submit(first.run, observer=first_observer)
        try:
            assert gate.started.wait(timeout=10)
            second_summary = pool.submit(second.run, observer=second_observer).result(
                timeout=10
            )
        finally:
            gate.release.set()
        first_summary = pending.result(timeout=10)

    assert len(caught) == 2
    assert (gate.run_id == second_start.run_id) is same_id
    for summary in (first_summary, second_summary):
        assert summary.lost_observer_events["status"].count == 2 * N_ROWS


@pytest.mark.parametrize(
    "shared_observer", [False, True], ids=["direct-only", "shared"]
)
def test_direct_orchestrator_with_the_jobs_id_cannot_report_losses_for_it(
    shared_observer: bool,
) -> None:
    """A nested direct execution shares neither the job's report nor its warning."""
    direct_store = _database()
    direct_job = _job(ARRANGEMENTS[1])
    status = StatusObserver(direct_store)

    class RunDirectly(NoOpObserver):
        @override
        def on_run_start(self, run_id: str, pipeline_uri: str = "") -> None:
            _ = SyncPipelineOrchestrator().run(
                direct_job.dag, observer=status, run_id=run_id
            )

    nested_run = RunDirectly()
    observer = CompositeObserver(status, nested_run) if shared_observer else nested_run
    summary, messages = _run_losing(_job(ARRANGEMENTS[0]), observer=observer)

    assert direct_store.refusals.writes == (4 if shared_observer else 2) * N_ROWS
    if shared_observer:
        assert summary.lost_observer_events["status"].count == 2 * N_ROWS
        assert len(messages) == 1
    else:
        assert summary.lost_observer_events == {}
        assert messages == []


class _UnprintableWriteError(OSError):
    @override
    def __str__(self) -> str:
        raise RuntimeError("could not format the failed write")


class _UnformattableMessage(str):
    @override
    def __format__(self, format_spec: str, /) -> str:
        raise RuntimeError("could not format the write error's message")


class _WriteErrorWithStringSubclass(OSError):
    @override
    def __str__(self) -> str:
        return _UnformattableMessage("failed to write")


@pytest.mark.parametrize("forward_warning", [False, True], ids=["direct", "forwarded"])
def test_observer_warning_filter_raises_through_forwarding_call(
    forward_warning: bool,
    arrangement: Arrangement,
    run_job: Callable[[PipelineJob, StatusObserver], RunSummary],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A forwarding warning call preserves promotion of the loss warning."""
    warn_explicit = warnings.warn_explicit

    @wraps(warn_explicit)
    def forward(*args: Any, **kwargs: Any) -> None:
        warn_explicit(*args, **kwargs)

    if forward_warning:
        monkeypatch.setattr(warnings, "warn_explicit", forward)
    job = _job(arrangement)
    with warnings.catch_warnings():
        warnings.simplefilter("error", ObserverEventsLostWarning)
        with pytest.raises(ObserverEventsLostWarning):
            _ = run_job(job, StatusObserver(_database()))

    assert job.last_run is not None and job.last_run.ok
    assert _rows(job) == EXPECTED_ROWS


def test_observer_loss_displays_escape_control_bytes(
    arrangement: Arrangement,
    run_job: Callable[[PipelineJob, StatusObserver], RunSummary],
) -> None:
    """NUL, terminal escape sequences and other controls are shown as text."""

    class WriteError(OSError):
        @override
        def __str__(self) -> str:
            return "bad bytes: \x00\x1b[2K\t\x7f\nsecond line"

    job = _job(arrangement)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ObserverEventsLostWarning)
        summary = run_job(job, StatusObserver(_database(error=WriteError)))

    (warning,) = [item for item in caught if item.category is ObserverEventsLostWarning]
    notebook = summary._repr_html_()  # pyright: ignore[reportPrivateUsage]  # notebook display protocol
    assert notebook is not None
    for display in (repr(summary), notebook, str(warning.message)):
        assert r"bad bytes: \x00\x1b[2K\x09\x7f ..." in display
        assert all(control not in display for control in "\x00\x1b\t\x7f")


def _run_with_non_ascii_caller(
    job: PipelineJob, observer: StatusObserver, path: str
) -> RunSummary:
    async def run() -> RunSummary:
        return await job.run_async(observer=observer)  # café

    if path == "run_async":
        return asyncio.run(run())
    orchestrator = ORCHESTRATORS[path]()
    return job.run(orchestrator=orchestrator, observer=observer)  # café


@pytest.mark.parametrize("path", ["sync", "async", "run_async"])
@pytest.mark.parametrize("mode", ["default", "error", "strict-ascii"])
@settings(
    max_examples=12,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(message=st.none() | st.text(alphabet=st.characters(), max_size=256))
@example(message="caf\xe9 😀\ud800\udfff <script>\x00\x1b\nnext line")
@example(message="long é\udcff " * 4096)
@example(message="")
@example(message=None)
def test_loss_reporting_handles_arbitrary_error_text(
    path: str,
    mode: str,
    arrangement: Arrangement,
    utf8_log_capture: pytest.LogCaptureFixture,
    message: str | None,
) -> None:
    """Store encoding-safe error text and keep the result under each warning policy."""

    class WriteError(OSError):
        @override
        def __str__(self) -> str:
            if message is None:
                return str(_UnprintableWriteError())
            return _UnformattableMessage(message)

    observer = StatusObserver(_database(error=WriteError))
    job = _job(arrangement)
    utf8_log_capture.clear()
    with (
        io.TextIOWrapper(io.BytesIO(), encoding="ascii", errors="strict") as stderr,
        warnings.catch_warnings(record=mode == "default") as caught,
        pytest.MonkeyPatch.context() as patch,
    ):
        if mode == "error":
            warnings.simplefilter("error")
        else:
            warnings.simplefilter("default", ObserverEventsLostWarning)

        if mode == "strict-ascii":

            def render_warning(
                warning: Warning, category: type[Warning], filename: str, lineno: int
            ) -> None:
                # Pytest installs its own recorder. Exercise standard rendering, including
                # the source line from linecache, and write it to the strict stream.
                _ = stderr.write(
                    warnings.formatwarning(warning, category, filename, lineno)
                )

            patch.setattr(warnings, "showwarning", render_warning)

        if mode == "error":
            with pytest.raises(ObserverEventsLostWarning) as raised:
                _ = _run_with_non_ascii_caller(job, observer, path)
            warning_text = str(raised.value)
            summary = job.last_run
        else:
            summary = _run_with_non_ascii_caller(job, observer, path)
            if mode == "strict-ascii":
                reports = [
                    record.getMessage()
                    for record in utf8_log_capture.records
                    if record.name == "orcapod.pipeline.run_summary"
                ]
                assert len(reports) == 1
                (warning_text,) = reports
            else:
                assert caught is not None
                observer_warnings = [
                    item
                    for item in caught
                    if item.category is ObserverEventsLostWarning
                ]
                assert len(observer_warnings) == 1
                warning_text = str(observer_warnings[0].message)

    assert summary is job.last_run and summary is not None and summary.ok
    notebook = summary._repr_html_()  # pyright: ignore[reportPrivateUsage]  # notebook display protocol
    assert notebook is not None
    for display in (repr(summary), notebook, warning_text):
        assert display.isascii()
        assert "first error: WriteError" in display
    first_error = "WriteError"
    if message is not None:
        first_error += ": " + message.encode("ascii", "backslashreplace").decode(
            "ascii"
        )
    assert summary.lost_observer_events == {
        "status": LostObserverEvents("status", 2 * N_ROWS, first_error)
    }
    assert _rows(job) == EXPECTED_ROWS
    utf8_log_capture.clear()


@pytest.mark.parametrize("fallback_raises", [False, True])
def test_failing_warning_renderer_cannot_interrupt_completed_run(
    fallback_raises: bool,
    arrangement: Arrangement,
    run_job: Callable[[PipelineJob, Any], RunSummary],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A renderer raising even the warning itself is different from filter promotion."""

    def fail_to_show(message: Warning, *args: object, **kwargs: object) -> None:
        raise message

    def fail_to_log(*args: object, **kwargs: object) -> None:
        raise RuntimeError("log handler is unavailable")

    observer = StatusObserver(_database())
    job = _job(arrangement)
    with warnings.catch_warnings():
        monkeypatch.setattr(warnings, "showwarning", fail_to_show)
        if fallback_raises:
            monkeypatch.setattr(
                logging.getLogger("orcapod.pipeline.run_summary"),
                "warning",
                fail_to_log,
            )
        warnings.simplefilter("default", ObserverEventsLostWarning)
        summary = run_job(job, observer)

    assert summary is job.last_run and summary.ok
    assert summary.lost_observer_events["status"].count == 2 * N_ROWS
    assert _rows(job) == EXPECTED_ROWS
    if not fallback_raises:
        assert caplog.text.count("Observer events were not recorded in this run:") == 1


@pytest.mark.parametrize("kind", ["status", "logging"])
@pytest.mark.parametrize(
    ("error", "first_error"),
    [
        (_UnprintableWriteError, "_UnprintableWriteError"),
        (
            _WriteErrorWithStringSubclass,
            "_WriteErrorWithStringSubclass: failed to write",
        ),
    ],
    ids=["broken-str", "string-subclass"],
)
def test_unprintable_write_error_preserves_rows(
    kind: str,
    orchestrator: Any,
    arrangement: Arrangement,
    error: type[OSError],
    first_error: str,
) -> None:
    """Summarizing either observer's error cannot damage the run."""
    database = _database(error=error)
    observer = (
        StatusObserver(database) if kind == "status" else LoggingObserver(database)
    )
    count = 2 * N_ROWS if kind == "status" else N_ROWS
    job = _job(arrangement)
    summary, messages = _run_losing(job, orchestrator=orchestrator, observer=observer)

    assert summary.lost_observer_events == {
        kind: LostObserverEvents(kind, count, first_error)
    }
    assert len(messages) == 1
    assert summary.ok and _rows(job) == EXPECTED_ROWS


@pytest.mark.parametrize("kind", ["status", "logging"])
def test_summary_formatting_preserves_observer_tracebacks(
    kind: str,
    arrangement: Arrangement,
    run_job: Callable[[PipelineJob, Any], RunSummary],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The existing traceback formats each error before the summary can format it."""

    class WriteError(OSError):
        def __init__(self, message: str) -> None:
            super().__init__(message)
            self.formats = 0

        @override
        def __str__(self) -> str:
            self.formats += 1
            return f"format call {self.formats}"

    tracebacks: list[str] = []

    class TracebackHandler(logging.Handler):
        @override
        def emit(self, record: logging.LogRecord) -> None:
            tracebacks.append(self.format(record).splitlines()[-1].rsplit(": ", 1)[-1])

    logger = logging.getLogger(f"orcapod.pipeline.{kind}_observer")
    monkeypatch.setattr(logger, "handlers", [TracebackHandler()])
    monkeypatch.setattr(logger, "propagate", False)
    database = _database(error=WriteError)
    observer = (
        StatusObserver(database) if kind == "status" else LoggingObserver(database)
    )
    job = _job(arrangement)

    with pytest.warns(ObserverEventsLostWarning):
        summary = run_job(job, observer)

    count = 2 * N_ROWS if kind == "status" else N_ROWS
    assert tracebacks == ["format call 1"] * count
    assert summary.lost_observer_events[kind].count == count


def test_concurrent_losses_keep_the_first_reported_error() -> None:
    """A later report cannot claim first place while the first formats its error."""
    formatting = threading.Event()
    release = threading.Event()
    losses = RunLosses()
    logger = logging.Logger(__name__, level=logging.CRITICAL)

    class FirstWriteError(OSError):
        @override
        def __str__(self) -> str:
            formatting.set()
            if not release.wait(timeout=10):
                raise RuntimeError("timed out waiting for the second report")
            return "failed first"

    def report(error: OSError) -> None:
        try:
            raise error
        except OSError as caught:
            report_lost_event(losses, "status", caught, logger, "store refuses writes")

    with ThreadPoolExecutor(2) as pool:
        first = pool.submit(report, FirstWriteError())
        try:
            assert formatting.wait(timeout=10)
            second = pool.submit(report, OSError("failed second"))
            second.result(timeout=10)
        finally:
            release.set()
        first.result(timeout=10)

    assert losses.by_kind() == {
        "status": LostObserverEvents("status", 2, "FirstWriteError: failed first")
    }


def test_a_failing_log_handler_cannot_interrupt_loss_reporting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A log sink failure cannot prevent either built-in from recording a loss."""

    class RefusingHandler(logging.Handler):
        @override
        def emit(self, record: logging.LogRecord) -> None:
            raise OSError("log sink unavailable")

    log = logging.getLogger("orcapod.pipeline")
    monkeypatch.setattr(log, "handlers", [RefusingHandler()])
    observer = CompositeObserver(
        StatusObserver(_database()), LoggingObserver(_database())
    )
    job = _job(ARRANGEMENTS[0])
    summary, messages = _run_losing(job, observer=observer)

    assert {
        kind: loss.count for kind, loss in summary.lost_observer_events.items()
    } == {
        "status": 2 * N_ROWS,
        "logging": N_ROWS,
    }
    assert len(messages) == 1
    assert summary.ok and _rows(job) == EXPECTED_ROWS


def test_precontextualized_logging_observer_keeps_its_existing_run_id(
    arrangement: Arrangement, run_job: Callable[[PipelineJob, Any], RunSummary]
) -> None:
    """A twice scoped observer keeps its stored run ID and counts failed writes."""
    observer = LoggingObserver(_database(accept_first=1))
    observer.on_run_start("earlier-run")
    scoped = observer.contextualize("tenant").contextualize("project")
    job = _job(arrangement)
    with pytest.warns(ObserverEventsLostWarning) as caught:
        summary = run_job(job, scoped)

    assert summary.lost_observer_events["logging"].count == N_ROWS - 1
    assert len(caught) == 1
    logs = observer.get_logs()
    assert logs is not None
    assert logs["_log_run_id"].to_pylist() == ["earlier-run"]
    assert _rows(job) == EXPECTED_ROWS


def test_direct_precontextualized_logging_observer_keeps_its_existing_run_id(
    orchestrator: Any,
) -> None:
    """A direct orchestrator leaves the contextualized logger's run ID alone."""
    observer = LoggingObserver(InMemoryArrowDatabase())
    observer.on_run_start("earlier-run")
    scoped = observer.contextualize("tenant")
    _ = orchestrator.run(
        _job(ARRANGEMENTS[0]).dag, observer=scoped, run_id="direct-run"
    )

    logs = observer.get_logs()
    assert logs is not None
    assert logs["_log_run_id"].to_pylist() == ["earlier-run"] * N_ROWS


def test_async_orchestrator_on_its_own_thread_keeps_the_jobs_losses() -> None:
    """A blocking async run in a running event loop reports from its new thread."""

    async def run() -> tuple[RunSummary, list[str]]:
        return _run_losing(
            _job(ARRANGEMENTS[0]),
            orchestrator=AsyncPipelineOrchestrator(),
            observer=LoggingObserver(_database()).contextualize("tenant"),
        )

    summary, messages = asyncio.run(run())

    assert summary.lost_observer_events["logging"].count == N_ROWS
    assert len(messages) == 1
