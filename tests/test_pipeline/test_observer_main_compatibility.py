"""Compare existing run behavior with the main source used to review ITL-1556."""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import subprocess
import sys
import warnings
from zipfile import ZipFile
from dataclasses import asdict, replace
from pathlib import Path
from types import FrameType
from typing import TYPE_CHECKING, TypedDict, cast
from unittest.mock import patch
from uuid import UUID

import pytest
from typing_extensions import override

import orcapod
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    CompositeObserver,
    LoggingObserver,
    RunSummary,
    StatusObserver,
    SyncPipelineOrchestrator,
)
from orcapod.pipeline.observer import NoOpLogger
from orcapod.protocols.core_protocols import DatagramProtocol
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
    from collections.abc import Callable, Mapping

    import pyarrow as pa

    from orcapod.pipeline import LostObserverEvents
    from orcapod.protocols.core_protocols import StreamProtocol
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = Path(orcapod.__file__).resolve().parent
# origin/main at the independent review. Keep the same comparison after main advances.
BASE = "37f784318931e5e276203c44277571b042057145"
PATHS = ("sync", "async", "run_async")
SCENARIOS = (
    "success",
    "quiet-frame-audit",
    "continue",
    "fail_fast",
    "loss",
    "row-and-loss",
    "custom",
    "composite",
)
ARRANGEMENTS = (
    Arrangement(MAIN.tags, TAGS_FIRST, PLAIN),
    Arrangement(tuple(reversed(MAIN.tags)), DATA_FIRST_REVERSED, MIXED),
)


class _RunRecord(TypedDict):
    """The observable state of a run, serialized for the comparison."""

    outcome: list[object]
    summary: dict[str, object] | None
    losses: dict[str, dict[str, object]]
    rows: list[dict[str, object]]
    calls: list[list[object]]
    warnings: list[list[str | int | None]]


class _StyledLine(str):
    @override
    def __format__(self, format_spec: str, /) -> str:
        raise RuntimeError("custom line formatter")


class _StyledMessage(str):
    @override
    def splitlines(self, keepends: bool = False) -> list[str]:
        return [_StyledLine(line) for line in super().splitlines(keepends)]


class _RowError(ValueError):
    @override
    def __str__(self) -> str:
        return _StyledMessage("first line\nsecond line")


@function_pod(output_keys=("n", "s"))
def _succeed(d1: int, d3: str) -> tuple[int, str]:
    """Produce a row and log a message that a built-in logger can record."""
    logging.getLogger(__name__).warning("computed %s", d1)
    return d1 + 1, d3 + d3


@function_pod(output_keys=("n", "s"))
def _fail(d1: int) -> tuple[int, str]:
    """Fail with a multiline exception whose line formatter would raise."""
    logging.getLogger(__name__).warning("failing %s", d1)
    raise _RowError()


def _record_value(value: object) -> object:
    """Turn a hook argument into a JSON value for comparison."""
    if isinstance(value, BaseException):
        return [type(value).__name__, str(value)]
    if isinstance(value, DatagramProtocol):
        return value.as_dict()
    if isinstance(value, dict):
        return {
            key: _record_value(item)
            for key, item in cast("dict[str, object]", value).items()
        }
    if isinstance(value, type):
        return [value.__module__, value.__qualname__]
    return value


class _Recorder:
    """Record custom observer lifecycle, scope, node, data and logger calls."""

    def __init__(
        self, calls: list[list[object]], name: str, path: tuple[str, ...] = ()
    ) -> None:
        self.calls: list[list[object]] = calls
        self.name: str = name
        self.path: tuple[str, ...] = path

    def contextualize(self, *path: str) -> _Recorder:
        """Record the scope request and return a distinct observer for that scope."""
        self.calls.append([self.name, self.path, "contextualize", path])
        return _Recorder(self.calls, self.name, self.path + path)

    def __getattr__(self, method: str) -> Callable[..., _RecordedLogger | None]:
        if not (method.startswith("on_") or method == "create_data_logger"):
            raise AttributeError(method)

        def call(*args: object, **kwargs: object) -> _RecordedLogger | None:
            self.calls.append(
                [
                    self.name,
                    self.path,
                    method,
                    [_record_value(arg) for arg in args],
                    _record_value(kwargs),
                ]
            )
            if method == "create_data_logger":
                return _RecordedLogger(self.calls, self.name, self.path)
            return None

        return call


class _RecordedLogger(NoOpLogger):
    """Record the captured output passed to a custom data logger."""

    def __init__(
        self, calls: list[list[object]], name: str, path: tuple[str, ...]
    ) -> None:
        self.calls: list[list[object]] = calls
        self.name: str = name
        self.path: tuple[str, ...] = path

    @override
    def record(self, **kwargs: object) -> None:
        self.calls.append([self.name, self.path, "record", _record_value(kwargs)])


class _ObserverStore:
    """Record built-in observer store calls, optionally refusing the event writes."""

    def __init__(
        self,
        calls: list[list[object]],
        refuse: bool,
        path: tuple[str, ...] = (),
    ) -> None:
        self.calls: list[list[object]] = calls
        self.refuse: bool = refuse
        self.path: tuple[str, ...] = path

    def at(self, *path: str) -> _ObserverStore:
        """Return a scoped database and record the requested path."""
        self.calls.append(["store.at", self.path, path])
        return _ObserverStore(self.calls, self.refuse, self.path + path)

    def add_record(
        self, path: tuple[str, ...], record_id: bytes, row: pa.Table, *, flush: bool
    ) -> None:
        """Record the full write, normalizing fresh IDs and wall-clock timestamps."""
        (values,) = cast("list[dict[str, object]]", row.to_pylist())
        for prefix in ("_status", "_log"):
            if f"{prefix}_id" in values:
                assert values[f"{prefix}_id"] == record_id
                values[f"{prefix}_id"] = "<new ID>"
                values[f"{prefix}_timestamp"] = "<timestamp>"
        traceback_text = values.get("_log_traceback")
        if isinstance(traceback_text, str):
            values["_log_traceback"] = traceback_text.replace(str(PACKAGE), "orcapod")
        self.calls.append(
            ["store.add_record", self.path, path, str(row.schema), values, flush]
        )
        if self.refuse:
            raise OSError("observer store unavailable")


def _run_case(path: str, scenario: str, arrangement: Arrangement) -> _RunRecord:
    """Collect warnings, observer calls, rows, summary and the return or exception."""
    calls: list[list[object]] = []
    custom = cast(
        "ExecutionObserverProtocol", cast("object", _Recorder(calls, "first"))
    )
    loses_events = scenario in ("loss", "row-and-loss")
    store = cast(
        "ArrowDatabaseProtocol",
        cast("object", _ObserverStore(calls, loses_events)),
    )
    observer = None
    if loses_events:
        observer = CompositeObserver(StatusObserver(store), LoggingObserver(store))
    elif scenario in ("continue", "fail_fast", "custom"):
        observer = custom
    elif scenario == "composite":
        last = cast(
            "ExecutionObserverProtocol", cast("object", _Recorder(calls, "last"))
        )
        observer = CompositeObserver(
            custom,
            CompositeObserver(StatusObserver(store), LoggingObserver(store)),
            last,
        )

    # A single row removes ordering differences among concurrent row callbacks.
    spec = replace(
        MAIN, columns={key: col.slice(0, 1) for key, col in MAIN.columns.items()}
    )
    job = PipelineJob(name="observer-comparison", database=InMemoryArrowDatabase())
    fails = scenario in ("continue", "fail_fast", "row-and-loss")
    with job:
        _ = (_fail if fails else _succeed).pod(
            make_source(spec, arrangement), label="result"
        )
    policy = "fail_fast" if scenario == "fail_fast" else "continue"

    async def run_async() -> RunSummary:
        return await job.run_async(
            orchestrator=AsyncPipelineOrchestrator(error_policy=policy),
            observer=observer,
        )

    outcome: list[object]
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            if path == "run_async":
                result = asyncio.run(run_async())
            else:
                orchestrator = (
                    SyncPipelineOrchestrator(error_policy=policy)
                    if path == "sync"
                    else AsyncPipelineOrchestrator(error_policy=policy)
                )
                result = job.run(orchestrator=orchestrator, observer=observer)
        except Exception as error:
            outcome = ["raised", _record_value(error)]
        else:
            assert result is job.last_run
            outcome = ["returned", type(result).__name__]

    summary = job.last_run
    values: dict[str, object] | None = None
    losses: Mapping[str, LostObserverEvents] = {}
    rows: list[dict[str, object]] = []
    if summary is not None:
        losses = cast(
            "Mapping[str, LostObserverEvents]",
            getattr(summary, "lost_observer_events", {}),
        )
        values = dict(cast("dict[str, object]", vars(summary)))
        _ = values.pop("lost_observer_events", None)
        values["nodes"] = {
            label: _record_value(asdict(node)) for label, node in summary.nodes.items()
        }
        node = cast("StreamProtocol", job.nodes["result"])
        rows = cast("list[dict[str, object]]", node.as_table().to_pylist())
    return {
        "outcome": outcome,
        "summary": values,
        "losses": {kind: asdict(loss) for kind, loss in losses.items()},
        "rows": rows,
        "calls": calls,
        "warnings": [
            [
                item.category.__module__,
                item.category.__qualname__,
                str(item.message),
                item.filename.replace(str(PACKAGE), "orcapod"),
                item.lineno,
                item.line,
            ]
            for item in caught
        ],
    }


def _probe() -> None:
    """Run identical jobs in the interpreter selected by the test."""
    assert PACKAGE.parent == Path(sys.argv[1]).resolve()
    path = sys.argv[2]
    fixed_uuid = UUID("01234567-89ab-4def-8abc-0123456789ab")
    audit_caller = False

    def audit(event: str, args: tuple[object, ...]) -> None:
        if event == "sys._getframe" and audit_caller:
            (frame,) = args
            assert isinstance(frame, FrameType)
            if frame.f_code.co_qualname in (
                "_run_case",
                "_run_case.<locals>.run_async",
            ):
                raise PermissionError("caller frame access denied")

    # An audit hook is permanent, so install it only in the probe process.
    sys.addaudithook(audit)
    records: dict[str, _RunRecord] = {}
    with patch("uuid.uuid4", return_value=fixed_uuid):
        for scenario in SCENARIOS:
            audit_caller = scenario == "quiet-frame-audit"
            for arrangement in ARRANGEMENTS:
                records[f"{path}/{scenario}/{arrangement.id}"] = _run_case(
                    path, scenario, arrangement
                )
    print(json.dumps(records, default=str))


def _run_probe(source: Path, path: str) -> dict[str, _RunRecord]:
    """Import Orcapod from one source tree in a clean process."""
    completed = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), str(source), path],
        env={**os.environ, "PYTHONPATH": os.pathsep.join((str(source), str(ROOT)))},
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stderr
    return cast("dict[str, _RunRecord]", json.loads(completed.stdout))


def _operator_completion(calls: list[list[object]]) -> list[list[object]]:
    """Partition out the column selector's async completion after checking its order."""
    completion: list[list[object]] = []
    started: list[list[object]] = []
    for call in calls:
        if len(call) < 4 or call[2] not in ("on_node_start", "on_node_end"):
            continue
        payload = call[3]
        if not isinstance(payload, list) or payload[0] != "SelectDataColumns":
            continue
        scope = call[:2]
        if call[2] == "on_node_start":
            started.append(scope)
        else:
            assert scope in started
            started.remove(scope)
            completion.append(call)
    for call in completion:
        calls.remove(call)
    return completion


@pytest.mark.parametrize("path", PATHS)
def test_run_behavior_matches_reviewed_main(tmp_path: Path, path: str) -> None:
    """Compare the whole record after removing only the two loss-report additions."""
    baseline = subprocess.run(
        ["git", "archive", "--format=zip", BASE, "src"],
        cwd=ROOT,
        capture_output=True,
        check=False,
    )
    if baseline.returncode != 0:
        pytest.skip(
            f"Reviewed origin/main commit {BASE} is unavailable in this checkout"
        )
    with ZipFile(io.BytesIO(baseline.stdout)) as archive:
        archive.extractall(tmp_path)

    main = _run_probe(tmp_path / "src", path)
    branch = _run_probe(ROOT / "src", path)
    assert main.keys() == branch.keys()
    for case, actual in branch.items():
        expected = main[case]
        assert expected["losses"] == {}
        warnings_before = expected["warnings"]
        recorded = actual["warnings"]
        losses = actual["losses"]
        if "/loss/" in case or "/row-and-loss/" in case:
            assert {kind: entry["count"] for kind, entry in losses.items()} == {
                "status": 2,
                "logging": 1,
            }
            assert recorded[-1][1] == "ObserverEventsLostWarning"
            assert recorded[:-1] == warnings_before
            _ = recorded.pop()
        else:
            assert losses == {}
        actual["losses"] = {}
        if path != "sync":
            # The operator forwards rows before it completes. Its end hook can run
            # before or after downstream row hooks, even with one input row.
            assert _operator_completion(actual["calls"]) == _operator_completion(
                expected["calls"]
            ), case
        assert actual == expected, case


if __name__ == "__main__":
    _probe()
