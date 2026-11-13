"""Every row of every job node's table carries its own derived columns (ITL-1034, ITL-1029).

The product: input row order (shuffled, tag-sorted, reverse tag-sorted) x tie scenario
(unique, duplicate, null tags) x store (in memory, Delta) x entry point and completion
order x node view (function node ``as_table`` / ``head`` / ``as_source``, operator nodes
``joined`` and ``grouped``) x every ``ColumnConfig`` that changes columns or row order.
Entry points and orders: sync ``run`` (input order); async ``run`` in every forced order
(``ORDERS``); ``run_async`` and ``run`` inside a running loop in the orders
``ENTRY_POINT_ORDERS`` (they share the async execution path with async ``run`` and differ
only in how the run is entered). Each run is made once and shared by the tests that read
it; each test checks every column configuration of one view and names every configuration
that fails. ``head`` refuses ``sort_by_tags``, so it is checked without it.

Readback: the stored rows read by a second run (rows from the store, not computed) and by
a job loaded with ``PipelineJob.load``, after writes in input order and in forced orders.

Known defects, each filed, are marked ``xfail(strict=True)`` on exactly the cases they
break, so a case passes the day its defect is fixed only by dropping its mark:

* ITL-1040: an async run stores an extra ``_source_<dropped column>`` in the function
  node's records (the stored-records check, async runs only).
* ITL-1042: after an async run an operator node's ``head()`` says "not computed" and
  shows no rows (``joined.head`` and ``grouped.head`` after an async run or rerun).
* ITL-1043: a loaded job cannot read a cached operator node (``joined`` in a loaded job).
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.types import CacheMode

from .checker import (
    Layout,
    assert_rows_aligned,
    assert_same_rows_by_identity,
)
from .fixtures import COLUMN_CONFIGS, INPUT_ORDERS, SCENARIOS
from .gate import ORDERS
from .jobs import (
    JOB_NAME,
    MODES,
    PER_RUN_COLUMNS,
    STORES,
    VIEWS,
    Run,
    View,
    build_job,
    function_tokens,
    layout,
    make_run,
    natural_keys,
    produce,
    run_job,
)

RunKey = tuple[str, str, str, str, str]

ITL_1040 = pytest.mark.xfail(
    strict=True,
    reason="ITL-1040: an async run stores an extra _source_<dropped column> column",
)
ITL_1042 = pytest.mark.xfail(
    strict=True,
    reason="ITL-1042: after an async run an operator node's head() shows no rows",
)
ITL_1043 = pytest.mark.xfail(
    strict=True,
    reason="ITL-1043: a loaded job cannot read a cached operator node",
)

ENTRY_POINT_ORDERS = ("reversed", "one_slow", "random_0")


@pytest.fixture(scope="module")
def runs(tmp_path_factory: pytest.TempPathFactory) -> Callable[[RunKey], Run]:
    """Make each run once per module: ``(scenario, input order, mode, order, store)``."""
    made: dict[RunKey, Run] = {}

    def get(key: RunKey) -> Run:
        """The run ``key`` names, made on first use."""
        if key not in made:
            scenario, input_order, mode, order_name, store_kind = key
            keys = natural_keys(scenario, input_order)
            order = None if mode == "sync" else ORDERS[order_name](keys)
            made[key] = make_run(
                scenario,
                input_order,
                mode,
                order,
                store_kind,
                tmp_path_factory.mktemp("run"),
            )
        return made[key]

    return get


def all_run_keys() -> list[RunKey]:
    """Every run of the product (see the module docstring)."""
    orders_of = {
        "sync": ["natural"],
        "async_run": list(ORDERS),
        "async_run_async": list(ENTRY_POINT_ORDERS),
        "async_in_loop": list(ENTRY_POINT_ORDERS),
    }
    return [
        (scenario, input_order, mode, order, store_kind)
        for store_kind in STORES
        for scenario in SCENARIOS
        for input_order in INPUT_ORDERS
        for mode in MODES
        for order in orders_of[mode]
    ]


def is_async(key: RunKey) -> bool:
    """Whether the run ``key`` names ran on the async orchestrator."""
    return key[2] != "sync"


def operator_head(view: View) -> bool:
    """Whether ``view`` is an operator node's ``head()`` (ITL-1042)."""
    return view.call == "head" and view.node != "scaled"


def run_keys(marks: Callable[[RunKey], list[Any]] | None = None) -> list[Any]:
    """Every run key as a parameter, with the marks ``marks`` gives it."""
    return [
        pytest.param(key, id="-".join(key), marks=marks(key) if marks else [])
        for key in all_run_keys()
    ]


def run_view_params(keys: list[RunKey], views: tuple[View, ...] = VIEWS) -> list[Any]:
    """``(run key, view)`` for each key and view; ITL-1042 on async operator heads."""
    return [
        pytest.param(
            key,
            view,
            id=f"{'-'.join(key)}-{view.name}",
            marks=[ITL_1042] if is_async(key) and operator_head(view) else [],
        )
        for key in keys
        for view in views
    ]


# A derived source's table has no system tags, so its rows have no record ids to be
# matched by; its rows are checked by recomputation in the test above.
KEYED_VIEWS = tuple(v for v in VIEWS if v.call != "as_source")


def check_every_config(
    job: PipelineJob,
    view: View,
    oracle: Any,
    tokens: dict[tuple[str, bytes], str] | None,
) -> None:
    """Check ``view`` under every column configuration; name every one that fails."""
    failures: list[str] = []
    for name, config in configs_for(view):
        table = produce(job, view, config)
        view_layout: Layout = layout(job, view, config)
        context = job.nodes[view.node].data_context
        try:
            assert_rows_aligned(
                table, view_layout, oracle, context, function_tokens=tokens
            )
        except AssertionError as exc:
            failures.append(f"[{name}] {exc}")
    assert not failures, f"{view.name}: " + "\n".join(failures)


def configs_for(view: View) -> list[tuple[str, dict[str, Any]]]:
    """The column configurations ``view`` accepts: ``head`` refuses ``sort_by_tags``."""
    return [
        (name, config)
        for name, config in COLUMN_CONFIGS.items()
        if not (view.call == "head" and config.get("columns", {}).get("sort_by_tags"))
    ]


@pytest.mark.parametrize(("key", "view"), run_view_params(all_run_keys()))
def test_every_row_of_a_node_view_is_aligned(
    key: RunKey, view: View, runs: Callable[[RunKey], Run]
) -> None:
    run = runs(key)
    check_every_config(run.job, view, run.sources.oracle, function_tokens(run.job))


@pytest.mark.parametrize("key", run_keys())
def test_the_gate_realised_the_order(
    key: RunKey, runs: Callable[[RunKey], Run]
) -> None:
    """The run completed its rows in the order the test meant to force."""
    scenario, input_order, mode, order_name, _ = key
    keys = natural_keys(scenario, input_order)
    expected = keys if mode == "sync" else ORDERS[order_name](keys)
    assert runs(key).completed == expected


@pytest.mark.parametrize(
    "key", run_keys(lambda key: [ITL_1040] if is_async(key) else [])
)
def test_stored_records_are_aligned(key: RunKey, runs: Callable[[RunKey], Run]) -> None:
    """The function node's stored records: hashes, tokens and ids belong to their rows."""
    from .records import assert_records_aligned

    run = runs(key)
    assert_records_aligned(run.job, run.sources)


@pytest.mark.parametrize(
    ("key", "view"),
    run_view_params([k for k in all_run_keys() if is_async(k)], KEYED_VIEWS),
)
def test_async_equals_sync_row_by_row(
    key: RunKey, view: View, runs: Callable[[RunKey], Run]
) -> None:
    """Keyed on record ids, every async table equals the sync one, cell for cell."""
    scenario, input_order, _, _, store_kind = key
    sync = runs((scenario, input_order, "sync", "natural", store_kind))
    run = runs(key)
    failures = []
    compared = 0
    for name, config in configs_for(view):
        expected = produce(sync.job, view, config)
        if not _has_record_ids(expected):
            continue
        compared += 1
        try:
            assert_same_rows_by_identity(
                expected, produce(run.job, view, config), ignore=PER_RUN_COLUMNS
            )
        except AssertionError as exc:
            failures.append(f"[{name}]: {exc}")
    assert compared, "no configuration has record ids to key rows by"
    assert not failures, f"{view.name}: " + "\n".join(failures)


def _has_record_ids(table: pa.Table) -> bool:
    """Whether ``table`` has the system tags rows are keyed by."""
    return any(c.startswith("_tag_record_id") for c in table.column_names)


# ---------------------------------------------------------------------------
# Readback
# ---------------------------------------------------------------------------

WRITES = ("sync-natural", "async_run-reversed", "async_run-random_0")
READS = ("rerun_sync", "rerun_async", "load")
ReadKey = tuple[str, str, str, str, str]


def read_back(
    run: Run, scenario: str, input_order: str, how: str, directory: Path
) -> PipelineJob:
    """A job that reads ``run``'s stored rows: a second run, or ``PipelineJob.load``."""
    if how == "load":
        saved = directory / "job.json"
        run.job.save(saved)
        loaded = PipelineJob.load(str(saved), store=run.store)
        # A loaded operator node reads its stored rows in REPLAY mode, and the saved job
        # does not keep the cache mode: attach it again, the public way (ITL-1043).
        loaded.nodes["joined"].attach_databases(
            pipeline_database=run.store.at(JOB_NAME), cache_mode=CacheMode.REPLAY
        )
        return loaded
    job, _ = build_job(scenario, input_order, run.store)
    summary = run_job(job, "sync" if how == "rerun_sync" else "async_run")
    assert summary.nodes["scaled"].computed == 0, "the rows were not read back"
    return job


@pytest.fixture(scope="module")
def readbacks(
    runs: Callable[[RunKey], Run], tmp_path_factory: pytest.TempPathFactory
) -> Callable[[ReadKey], tuple[Run, PipelineJob]]:
    """Make each readback once per module: ``(scenario, input order, store, write, how)``."""
    made: dict[ReadKey, tuple[Run, PipelineJob]] = {}

    def get(key: ReadKey) -> tuple[Run, PipelineJob]:
        """The written run and the job that reads it back, made on first use."""
        if key not in made:
            scenario, input_order, store_kind, write, how = key
            mode, order = write.split("-")
            run = runs((scenario, input_order, mode, order, store_kind))
            job = read_back(
                run, scenario, input_order, how, tmp_path_factory.mktemp("read")
            )
            made[key] = (run, job)
        return made[key]

    return get


def readback_params() -> list[Any]:
    """Every readback and view, marked where a filed defect breaks it.

    A loaded job leaves out ``grouped``: its operator stores nothing, so a loaded job has
    nothing to read for it, by design.
    """
    params = []
    for scenario in SCENARIOS:
        for input_order in INPUT_ORDERS:
            for store_kind in STORES:
                for write in WRITES:
                    for how in READS:
                        key = (scenario, input_order, store_kind, write, how)
                        for view in VIEWS:
                            if how == "load" and view.node == "grouped":
                                continue
                            marks = []
                            if how == "load" and view.node == "joined":
                                marks.append(ITL_1043)
                            elif how == "rerun_async" and operator_head(view):
                                marks.append(ITL_1042)
                            params.append(
                                pytest.param(
                                    key,
                                    view,
                                    id=f"{'-'.join(key)}-{view.name}",
                                    marks=marks,
                                )
                            )
    return params


@pytest.mark.parametrize(("key", "view"), readback_params())
def test_rows_read_back_from_the_store_are_aligned(
    key: ReadKey,
    view: View,
    readbacks: Callable[[ReadKey], tuple[Run, PipelineJob]],
) -> None:
    run, job = readbacks(key)
    check_every_config(job, view, run.sources.oracle, function_tokens(run.job))
