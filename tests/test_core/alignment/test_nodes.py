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
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob

from .checker import (
    Layout,
    assert_rows_aligned,
    assert_same_rows_by_identity,
)
from .fixtures import COLUMN_CONFIGS, INPUT_ORDERS, SCENARIOS
from .gate import ORDERS
from .jobs import (
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


def run_keys() -> list[pytest.ParameterSet]:
    """Every run of the product (see the module docstring)."""
    orders_of = {
        "sync": ["natural"],
        "async_run": list(ORDERS),
        "async_run_async": list(ENTRY_POINT_ORDERS),
        "async_in_loop": list(ENTRY_POINT_ORDERS),
    }
    keys = []
    for store_kind in STORES:
        for scenario in SCENARIOS:
            for input_order in INPUT_ORDERS:
                for mode in MODES:
                    for order in orders_of[mode]:
                        key = (scenario, input_order, mode, order, store_kind)
                        keys.append(pytest.param(key, id="-".join(key)))
    return keys


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


@pytest.mark.parametrize("view", VIEWS, ids=[v.name for v in VIEWS])
@pytest.mark.parametrize("key", run_keys())
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


@pytest.mark.parametrize("key", run_keys())
def test_stored_records_are_aligned(
    key: RunKey, runs: Callable[[RunKey], Run]
) -> None:
    """The function node's stored records: hashes, tokens and ids belong to their rows."""
    from .records import assert_records_aligned

    run = runs(key)
    assert_records_aligned(run.job, run.sources)


@pytest.mark.parametrize("key", [k for k in run_keys() if k.values[0][2] != "sync"])
def test_async_equals_sync_row_by_row(
    key: RunKey, runs: Callable[[RunKey], Run]
) -> None:
    """Keyed on record ids, every async table equals the sync one, cell for cell."""
    scenario, input_order, _, _, store_kind = key
    sync = runs((scenario, input_order, "sync", "natural", store_kind))
    run = runs(key)
    failures = []
    for view in VIEWS:
        for name, config in configs_for(view):
            expected = produce(sync.job, view, config)
            if not _has_record_ids(expected):
                continue
            try:
                assert_same_rows_by_identity(
                    expected, produce(run.job, view, config), ignore=PER_RUN_COLUMNS
                )
            except AssertionError as exc:
                failures.append(f"{view.name} [{name}]: {exc}")
    assert not failures, "\n".join(failures)


def _has_record_ids(table: pa.Table) -> bool:
    """Whether ``table`` has the system tags rows are keyed by."""
    return any(c.startswith("_tag_record_id") for c in table.column_names)


# ---------------------------------------------------------------------------
# Readback
# ---------------------------------------------------------------------------

WRITES = ("sync-natural", "async_run-reversed", "async_run-random_0")
READS = ("rerun_sync", "rerun_async", "load")


def read_back(
    run: Run, scenario: str, input_order: str, how: str, directory: Path
) -> PipelineJob:
    """A job that reads ``run``'s stored rows: a second run, or ``PipelineJob.load``."""
    if how == "load":
        saved = directory / "job.json"
        run.job.save(saved)
        return PipelineJob.load(str(saved), store=run.store)
    job, _ = build_job(scenario, input_order, run.store)
    summary = run_job(job, "sync" if how == "rerun_sync" else "async_run")
    assert summary.nodes["scaled"].computed == 0, "the rows were not read back"
    return job


@pytest.mark.parametrize("how", READS)
@pytest.mark.parametrize("write", WRITES)
@pytest.mark.parametrize("store_kind", list(STORES))
@pytest.mark.parametrize("input_order", INPUT_ORDERS)
@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_rows_read_back_from_the_store_are_aligned(
    scenario: str,
    input_order: str,
    store_kind: str,
    write: str,
    how: str,
    runs: Callable[[RunKey], Run],
    tmp_path: Path,
) -> None:
    mode, order = write.split("-")
    run = runs((scenario, input_order, mode, order, store_kind))
    job = read_back(run, scenario, input_order, how, tmp_path)
    tokens = function_tokens(run.job)
    # A loaded job reads its function node from the store. Its operator nodes are left
    # out: ``grouped`` stores nothing, and a loaded ``joined`` cannot read its stored
    # cache (reported with ITL-1034: re-attached in REPLAY mode it reads no rows, or
    # looks for another path when a tag column is nullable).
    views = [v for v in VIEWS if not (how == "load" and v.node != "scaled")]
    failures = []
    for view in views:
        try:
            check_every_config(job, view, run.sources.oracle, tokens)
        except AssertionError as exc:
            failures.append(str(exc))
    assert not failures, "\n".join(failures)
