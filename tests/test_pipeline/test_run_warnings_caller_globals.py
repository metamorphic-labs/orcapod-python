"""A run warning is issued whatever the caller's module globals look like (ITL-1344).

``RestrictedAggregationWarning``, ``SourceCheckFailedWarning`` and ``RowFailuresWarning``
are shown at the user's line that started the run. Orcapod used to hand that caller's
globals to ``warnings.warn_explicit(module_globals=...)``, and Python then looks for the
module's source through them: on Python 3.12 a notebook cell (``__spec__`` and
``__loader__`` both ``None``) made ``job.run()`` raise ``ValueError: Module globals is
missing a __spec__.loader`` instead of warning. Other callers failed the same way on
other versions: ``python -m package.module`` (``ImportError`` on 3.11 and 3.12), a module
whose file got shorter after it was imported (``IndexError`` on 3.11 and 3.12), a module
made with ``types.ModuleType`` (``ValueError`` on 3.12 and later), a script when
``DeprecationWarning`` is an error (3.12), and a module whose loader raises when asked
for the source (every version).

Each test starts a run from code executed with such globals, as a notebook kernel executes
a cell, and reads the warnings the run issued.
"""

from __future__ import annotations

import asyncio
import importlib.abc
import importlib.machinery
import importlib.util
import types
import warnings
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import orcapod
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import (
    RestrictedAggregationWarning,
    RowFailuresWarning,
    SourceCheckFailedWarning,
)
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.sources import DictSource, SpiralDBTableSource
from tests.test_databases import fake_spiral_sdk

RUN_WARNINGS = (
    RestrictedAggregationWarning,
    SourceCheckFailedWarning,
    RowFailuresWarning,
)
_PROJECT = "workshop-sample"
_KEYS = pa.schema(
    [pa.field("subject_id", pa.string()), pa.field("timestamp", pa.uint64())]
)

# --- The code that starts a run, as a user writes it; the run is started on line 2 ------

RUN_LINE = 2
RUN_FORMS: dict[str, str] = {
    "run": "# a cell\nsummary = job.run()\n",
    "run_with_async_orchestrator": (
        "# a cell\nsummary = job.run(orchestrator=AsyncPipelineOrchestrator())\n"
    ),
    "run_async": (
        "async def cell():\n"
        "    return await job.run_async()\n"
        "summary = asyncio.run(cell())\n"
    ),
}

# --- The caller's globals --------------------------------------------------------------
# Each returns the globals and the file name the caller's code is compiled under.


def _notebook(folder: Path, code: str) -> tuple[dict[str, Any], str]:
    """An IPython or Jupyter user namespace: no spec and no loader."""
    caller = {"__name__": "__main__", "__spec__": None, "__loader__": None}
    return caller, "<ipython-input-7-1344>"


def _run_as_module(folder: Path, code: str) -> tuple[dict[str, Any], str]:
    """``python -m lab.analysis``: named ``__main__``, loaded as ``lab.analysis``."""
    path = folder / "analysis.py"
    _ = path.write_text(code)
    spec = importlib.util.spec_from_file_location("lab.analysis", path)
    assert spec is not None
    caller = {
        "__name__": "__main__",
        "__spec__": spec,
        "__loader__": spec.loader,
        "__file__": str(path),
    }
    return caller, str(path)


def _script(folder: Path, code: str) -> tuple[dict[str, Any], str]:
    """``python analysis.py``: a loader and no spec."""
    path = folder / "analysis.py"
    _ = path.write_text(code)
    loader = importlib.machinery.SourceFileLoader("__main__", str(path))
    caller = {
        "__name__": "__main__",
        "__spec__": None,
        "__loader__": loader,
        "__file__": str(path),
    }
    return caller, str(path)


def _made_module(folder: Path, code: str) -> tuple[dict[str, Any], str]:
    """A module made with ``types.ModuleType``: a name, no spec and no loader."""
    return dict(vars(types.ModuleType("scratch"))), "<scratch>"


def _module_with_a_shorter_file(folder: Path, code: str) -> tuple[dict[str, Any], str]:
    """An imported module whose file was edited: the run's line is past its end."""
    path = folder / "helper.py"
    _ = path.write_text("x = 1\n")
    spec = importlib.util.spec_from_file_location("lab_helper", path)
    assert spec is not None
    caller = {
        "__name__": "lab_helper",
        "__spec__": spec,
        "__loader__": spec.loader,
        "__file__": str(path),
    }
    return caller, str(path)


class _NoSource(importlib.abc.Loader):
    """A loader that raises when asked for its module's source."""

    def get_source(self, fullname: str) -> str:
        raise RuntimeError(f"no source for {fullname}")


def _module_whose_loader_raises(folder: Path, code: str) -> tuple[dict[str, Any], str]:
    """A module with no file on disk and a loader that cannot give its source."""
    loader = _NoSource()
    spec = importlib.machinery.ModuleSpec("lab.sealed", loader)
    caller = {"__name__": "lab.sealed", "__spec__": spec, "__loader__": loader}
    return caller, str(folder / "sealed.py")


CALLERS: dict[str, Callable[[Path, str], tuple[dict[str, Any], str]]] = {
    "notebook": _notebook,
    "run_as_module": _run_as_module,
    "script": _script,
    "made_module": _made_module,
    "module_with_a_shorter_file": _module_with_a_shorter_file,
    "module_whose_loader_raises": _module_whose_loader_raises,
}


def _execute(
    code: str, caller: dict[str, Any], filename: str, job: PipelineJob
) -> tuple[dict[str, Any], list[warnings.WarningMessage]]:
    """Execute ``code`` with ``caller`` as its globals; return them and the run warnings.

    Every warning is recorded, and the ``DeprecationWarning`` Python issues about a
    module's loader is an error, as under ``python -W error``.
    """
    namespace = {
        **caller,
        "job": job,
        "asyncio": asyncio,
        "AsyncPipelineOrchestrator": AsyncPipelineOrchestrator,
    }
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warnings.filterwarnings(
            "error", message="Module globals", category=DeprecationWarning
        )
        exec(compile(code, filename, "exec"), namespace)
    return namespace, [w for w in caught if issubclass(w.category, RUN_WARNINGS)]


# --- Three jobs, each issuing one of the warnings, about two things -------------------


@function_pod(output_keys="y")
def even_only(x: int) -> int:
    """Return ``x``; raise for an odd ``x``."""
    if x % 2:
        raise ValueError(f"odd x={x}")
    return x


@function_pod(output_keys="y")
def never(x: int) -> int:
    """Raise for every row."""
    raise RuntimeError(f"no result for x={x}")


@function_pod(output_keys="y")
def double(x: int) -> int:
    """Return twice ``x``."""
    return 2 * x


class _FailingCheck(DictSource):
    """A ``DictSource`` whose ``check_for_new_data`` raises ``error``."""

    def __init__(self, error: Exception, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.error = error

    def check_for_new_data(self) -> bool:
        raise self.error


def _rows(n: int) -> list[dict[str, Any]]:
    return [{"k": f"r{i}", "x": i} for i in range(n)]


@pytest.fixture()
def spiral(tmp_path: Path) -> Iterator[None]:
    """Provide a fake SpiralDB project with two tables, ``event`` and ``trial``."""
    with fake_spiral_sdk.install(tmp_path / "spiral"):
        project = fake_spiral_sdk.ExternalProject(_PROJECT)
        for name in ("event", "trial"):
            project.create_table(f"default.{name}", key_schema=_KEYS).write(
                pa.table(
                    {
                        "subject_id": ["m1", "m1", "m2", "m2"],
                        "timestamp": pa.array([1, 2, 3, 4], pa.uint64()),
                        "channel": pa.array([0, 1, 0, 1], pa.int16()),
                        "amplitude": pa.array([0.5, 1.5, 2.5, 3.5], pa.float32()),
                    }
                )
            )
        yield


def _restricted_job() -> PipelineJob:
    """Two restricted sources, one feeding a ``GroupBy`` and one a ``Batch``."""
    job = PipelineJob(name="restricted", database=InMemoryArrowDatabase())
    with job:
        events = SpiralDBTableSource(
            _PROJECT, "event", label="events", source_id="events", tag_only_columns=True
        ).where(subject_id="m1")
        trials = SpiralDBTableSource(
            _PROJECT, "trial", label="trials", source_id="trials", tag_only_columns=True
        ).where(subject_id="m2")
        _ = events.group_by(["subject_id"], label="by_subject")
        _ = trials.batch(2, label="pairs")
    return job


def _unchecked_job() -> PipelineJob:
    """Two sources whose check for new data fails, each feeding a node."""
    job = PipelineJob(name="unchecked", database=InMemoryArrowDatabase())
    with job:
        left = _FailingCheck(
            ConnectionError("server unreachable"),
            data=_rows(3),
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="left",
            label="left",
        )
        right = _FailingCheck(
            OSError("disk gone"),
            data=_rows(2),
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="right",
            label="right",
        )
        _ = double.pod(left, label="doubled")
        _ = right.drop_nulls(label="present")
    return job


def _failing_job() -> PipelineJob:
    """Two function nodes with failed rows; an operator node reads one of them."""
    job = PipelineJob(name="failing", database=InMemoryArrowDatabase())
    with job:
        source = DictSource(
            data=_rows(4),
            tag_columns=["k"],
            tag_only_columns=True,
            label="rows",
            source_id="rows",
        )
        evens: Any = even_only.pod(source, label="evens")
        _ = never.pod(source, label="broken")
        _ = evens.drop_nulls(label="present")
    return job


def _restricted_messages(namespace: dict[str, Any]) -> list[tuple[str, ...]]:
    assert sorted(namespace["summary"].restricted_source_ids) == ["events", "trials"]
    return [
        (
            "A restricted source feeds an aggregating step",
            "'events' feeds GroupBy 'by_subject'",
            "'trials' feeds Batch 'pairs'",
        )
    ]


def _unchecked_messages(namespace: dict[str, Any]) -> list[tuple[str, ...]]:
    assert sorted(namespace["summary"].unchecked_sources) == [
        "dict_v1:left",
        "dict_v1:right",
    ]
    return [
        (
            "Source 'dict_v1:left' could not be checked",
            "ConnectionError: server unreachable",
        ),
        ("Source 'dict_v1:right' could not be checked", "OSError: disk gone"),
    ]


def _failing_messages(namespace: dict[str, Any]) -> list[tuple[str, ...]]:
    assert namespace["summary"].failed_rows == 6
    return [
        (
            "2 of 4 rows failed in node 'evens' (first error: ValueError: odd x=",
            "4 of 4 rows failed in node 'broken' (first error: RuntimeError: no result",
            'job.last_run.print_traceback("evens")',
        )
    ]


# Per warning: the job, the category, and (given the caller's namespace after the run)
# the parts of each expected message, one tuple per warning.
WARNINGS: dict[str, tuple[Callable[[], PipelineJob], type[Warning], Any]] = {
    "restricted_aggregation": (
        _restricted_job,
        RestrictedAggregationWarning,
        _restricted_messages,
    ),
    "source_check_failed": (
        _unchecked_job,
        SourceCheckFailedWarning,
        _unchecked_messages,
    ),
    "row_failures": (_failing_job, RowFailuresWarning, _failing_messages),
}


def _assert_messages(
    issued: list[warnings.WarningMessage], expected: list[tuple[str, ...]]
) -> None:
    """Each expected warning was issued once: every part is in exactly one message."""
    messages = sorted(str(w.message) for w in issued)
    assert len(messages) == len(expected), messages
    for parts, message in zip(sorted(expected), messages):
        for part in parts:
            assert part in message


def _shown(w: warnings.WarningMessage) -> str:
    """The text Python prints for ``w``; building it reads the caller's source line."""
    return warnings.formatwarning(w.message, w.category, w.filename, w.lineno)


@pytest.mark.parametrize("run_form", list(RUN_FORMS))
@pytest.mark.parametrize("caller_kind", list(CALLERS))
@pytest.mark.parametrize("warning", list(WARNINGS))
def test_a_run_warns_whatever_the_callers_globals(
    warning: str, caller_kind: str, run_form: str, spiral: None, tmp_path: Path
) -> None:
    """The run completes and its warning is issued at the caller's line."""
    make_job, category, expected = WARNINGS[warning]
    code = RUN_FORMS[run_form]
    caller, filename = CALLERS[caller_kind](tmp_path, code)

    namespace, issued = _execute(code, caller, filename, make_job())

    assert [w.category for w in issued] == [category] * len(issued)
    _assert_messages(issued, expected(namespace))
    assert {(w.filename, w.lineno) for w in issued} == {(filename, RUN_LINE)}
    for w in issued:
        assert f"{filename}:{RUN_LINE}: {category.__name__}: " in _shown(w)


# --- A job node's run() ------------------------------------------------------------------

NODE_RUNS: dict[str, tuple[Callable[[], PipelineJob], str, type[Warning], str]] = {
    "function_node_with_failed_rows": (
        _failing_job,
        "evens",
        RowFailuresWarning,
        'job.nodes["evens"].last_run.print_traceback("evens")',
    ),
    "operator_node_over_failed_rows": (
        _failing_job,
        "present",
        RowFailuresWarning,
        "2 of 4 rows failed in node 'evens'",
    ),
    "function_node_over_an_unchecked_source": (
        _unchecked_job,
        "doubled",
        SourceCheckFailedWarning,
        "Source 'dict_v1:left' could not be checked",
    ),
    "operator_node_over_an_unchecked_source": (
        _unchecked_job,
        "present",
        SourceCheckFailedWarning,
        "Source 'dict_v1:right' could not be checked",
    ),
    "source_node_of_an_unchecked_source": (
        _unchecked_job,
        "right",
        SourceCheckFailedWarning,
        "Source 'dict_v1:right' could not be checked",
    ),
}


@pytest.mark.parametrize("caller_kind", list(CALLERS))
@pytest.mark.parametrize("node_run", list(NODE_RUNS))
def test_a_node_run_warns_whatever_the_callers_globals(
    node_run: str, caller_kind: str, tmp_path: Path
) -> None:
    """``job.nodes[label].run()`` issues its warning at the caller's line."""
    make_job, label, category, part = NODE_RUNS[node_run]
    code = f"# a cell\nview = job.nodes[{label!r}].run()\n"
    caller, filename = CALLERS[caller_kind](tmp_path, code)

    namespace, issued = _execute(code, caller, filename, make_job())

    assert namespace["view"] is not None
    assert [w.category for w in issued] == [category]
    assert part in str(issued[0].message)
    assert (issued[0].filename, issued[0].lineno) == (filename, RUN_LINE)
    assert f"{filename}:{RUN_LINE}: {category.__name__}: " in _shown(issued[0])


# --- What the user's filters do, from a notebook cell -------------------------------------


@pytest.mark.parametrize("warning", list(WARNINGS))
def test_a_cell_run_twice_warns_twice(
    warning: str, spiral: None, tmp_path: Path
) -> None:
    """Python's once-per-line rule does not hide the second run of one cell."""
    make_job, category, _ = WARNINGS[warning]
    code = RUN_FORMS["run"]
    caller, filename = _notebook(tmp_path, code)
    cell = compile(code, filename, "exec")
    per_run = len(_execute(code, caller, filename, make_job())[1])

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default")
        for _ in range(2):
            exec(cell, {**caller, "job": make_job()})

    issued = [w for w in caught if issubclass(w.category, category)]
    assert len(issued) == 2 * per_run


@pytest.mark.parametrize("warning", list(WARNINGS))
def test_the_users_filters_apply_in_a_cell(
    warning: str, spiral: None, tmp_path: Path
) -> None:
    """``error`` raises the warning itself; ``ignore`` shows nothing."""
    make_job, category, _ = WARNINGS[warning]
    code = RUN_FORMS["run"]
    caller, filename = _notebook(tmp_path, code)
    cell = compile(code, filename, "exec")

    with warnings.catch_warnings():
        warnings.simplefilter("error", category)
        with pytest.raises(category):
            exec(cell, {**caller, "job": make_job()})

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warnings.simplefilter("ignore", category)
        exec(cell, {**caller, "job": make_job()})
    assert not [w for w in caught if issubclass(w.category, category)]


# --- The line shown with the warning ---------------------------------------------------


class _SourceOnly(importlib.abc.Loader):
    """A loader that only knows its module's source, as a zip import's does."""

    def __init__(self, source: str) -> None:
        self.source = source

    def get_source(self, fullname: str) -> str:
        return self.source


@pytest.mark.parametrize("warning", list(WARNINGS))
def test_the_callers_line_is_shown_for_a_module_with_no_file(
    warning: str, spiral: None, tmp_path: Path
) -> None:
    """A module imported from an archive has no file to read; its loader has the source."""
    make_job, category, _ = WARNINGS[warning]
    code = RUN_FORMS["run"]
    loader = _SourceOnly(code)
    spec = importlib.machinery.ModuleSpec("lab.archived", loader)
    caller = {"__name__": "lab.archived", "__spec__": spec, "__loader__": loader}
    filename = str(tmp_path / "lab.zip" / "lab" / f"archived_{warning}.py")

    _, issued = _execute(code, caller, filename, make_job())

    assert issued
    for w in issued:
        assert _shown(w).rstrip().endswith("summary = job.run()")


# --- One place ---------------------------------------------------------------------------


def test_one_place_issues_a_warning_at_a_frame() -> None:
    """Only ``utils/warning_utils.py`` calls ``warn_explicit``; no other file may."""
    package = Path(orcapod.__file__).parent
    callers = sorted(
        path.relative_to(package).as_posix()
        for path in package.rglob("*.py")
        if "warn_explicit" in path.read_text(encoding="utf-8")
    )
    assert callers == ["utils/warning_utils.py"]
