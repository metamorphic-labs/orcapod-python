"""A job's run summary and tables belong to the rows that produced them (ITL-1034).

``gated_fragile`` raises for every ``x`` with ``fails(x)`` and the run goes on (the default
error policy). The run summary must count exactly the failed and computed rows, and its
first error must be the error of the first row to fail in the order the rows completed.
The node's table must hold exactly the rows that succeeded, each aligned. Every entry
point (sync ``run``, async ``run``, ``run_async``, ``run`` inside a running loop), every
forced completion order on the async ones, both stores and every tie scenario.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from orcapod.errors import RowFailuresWarning

from .checker import Layout, assert_rows_aligned
from .fixtures import SCENARIOS, keys_a
from .gate import ORDERS, fails
from .jobs import MODES, STORES, function_tokens, make_run


def job_params() -> list[pytest.ParameterSet]:
    """Every entry point, its orders, store and scenario."""
    params = []
    for scenario in SCENARIOS:
        for store_kind in STORES:
            for mode in MODES:
                for order in ["natural"] if mode == "sync" else list(ORDERS):
                    params.append(
                        pytest.param(
                            scenario,
                            store_kind,
                            mode,
                            order,
                            id=f"{scenario}-{store_kind}-{mode}-{order}",
                        )
                    )
    return params


@pytest.mark.parametrize(("scenario", "store_kind", "mode", "order"), job_params())
def test_run_summary_and_table_belong_to_the_rows_that_ran(
    scenario: str, store_kind: str, mode: str, order: str, tmp_path: Path
) -> None:
    keys = keys_a(scenario, "shuffled")
    completion = keys if mode == "sync" else ORDERS[order](keys)
    with pytest.warns(RowFailuresWarning, match="rows failed in node 'scaled'"):
        run = make_run(
            scenario,
            "shuffled",
            mode,
            None if mode == "sync" else completion,
            store_kind,
            tmp_path,
            fragile=True,
        )
    assert run.completed == completion, "the gate did not realise the order"

    failing = [x for x in completion if fails(x)]
    succeeding = [x for x in completion if not fails(x)]
    assert failing and succeeding, "the fixture must have both kinds of row"
    summary = run.summary.nodes["scaled"]
    assert summary.failed == len(failing)
    assert summary.computed == len(succeeding)
    assert summary.reused == 0
    assert str(summary.first_error) == f"gated_fragile fails for x={failing[0]}"

    node = run.job.nodes["scaled"]
    table = node.as_table(all_info=True)
    tags, data = node.keys()
    assert_rows_aligned(
        table,
        Layout(tag_keys=tuple(tags), data_keys=tuple(data)),
        run.sources.oracle,
        node.data_context,
        function_tokens=function_tokens(run.job),
    )
    computed_ys = sorted(table.column("y").to_pylist())
    assert computed_ys == sorted(10 * x for x in succeeding)
