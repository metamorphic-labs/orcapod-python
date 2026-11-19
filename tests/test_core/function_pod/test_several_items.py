"""Function pods over several inputs, reading several parameters, with several outputs (ITL-1236).

The rules tested:

* ``pod(a, b, c)`` joins its inputs as ``Join`` does, so the order of the inputs changes
  nothing: rows, hashes, system-tag names and column order, in memory and in a job (a
  second job over the inputs in another order finds every result stored).
* Data columns that the function does not read are dropped by the automatic select
  (ITL-807), whatever their number and position.
* The function's parameters are matched by name, never by position.
* A data-tag is a data column a function may read (ITL-1023).
"""

from __future__ import annotations

import itertools

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

from ..operators.several_items import (
    DATA_TAGS,
    LAYOUTS,
    MAIN,
    MIXED,
    ROLES,
    TAGS_FIRST,
    Arrangement,
    P,
    Q,
    Spec,
    fingerprint,
    make_source,
    presentation,
    rows_fingerprint,
)

INPUTS: tuple[Spec, ...] = (MAIN, P, Q)


# Parameters in another order than the columns: q2 is in Q, d1 in MAIN, p1 in P.
@function_pod(output_keys=["total", "product"])
def combine(q2: int, d1: int, p1: int) -> tuple[int, int]:
    # Weighted: no two parameters can be swapped without changing ``total``.
    return q2 + 2 * d1 + 3 * p1, d1 * p1


def _inputs(role: str) -> list[ArrowTableSource]:
    return [
        make_source(spec, Arrangement(spec.tags, TAGS_FIRST, role)) for spec in INPUTS
    ]


@pytest.mark.parametrize("role", ROLES)
def test_the_order_of_the_inputs_changes_nothing(role: str) -> None:
    """All six orders of three inputs: same rows, hashes and columns.

    Each in-memory computation names a fresh record in its outputs' source info, so the
    rows are compared without it.
    """
    results = {}
    for permutation in itertools.permutations(range(len(INPUTS))):
        inputs = _inputs(role)
        stream = combine.pod(*(inputs[i] for i in permutation))
        results[permutation] = {
            **fingerprint(stream, source_info=False),
            "presentation": presentation(stream),
        }
    first = results[(0, 1, 2)]
    for permutation, result in results.items():
        assert result == first, permutation
    # The extra data columns (d2, d3, p2, q1) were dropped; every tag stays.
    tag_keys, data_keys = first["presentation"]
    assert sorted(tag_keys) == ["t1", "t2", "t3"]
    assert {"d2", "d3", "p2", "q1"}.isdisjoint(data_keys)
    assert {"total", "product"} <= set(data_keys)


@pytest.mark.parametrize("role", ROLES)
def test_values_are_matched_by_parameter_name(role: str) -> None:
    stream = combine.pod(*_inputs(role))
    rows = sorted(
        (tag["t1"], tag["t2"], tag["t3"], data["total"], data["product"])
        for tag, data in stream.iter_data()
    )
    # MAIN x P on (t1, t2) x Q on (t2, t3); total = q2 + 2 d1 + 3 p1, product = d1 p1.
    assert rows == [
        ("a", 1, 1, 400 + 2 * 50 + 3 * 9, 50 * 9),
        ("a", 1, 2, 100 + 2 * 20 + 3 * 9, 20 * 9),
        ("a", 2, 1, 200 + 2 * 30 + 3 * 7, 30 * 7),
        ("b", 1, 2, 100 + 2 * 40 + 3 * 8, 40 * 8),
        ("b", 2, 1, 200 + 2 * 10 + 3 * 6, 10 * 6),
        ("b", 2, 2, 300 + 2 * 60 + 3 * 6, 60 * 6),
    ]


@pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
@pytest.mark.parametrize("role", ROLES)
def test_a_job_over_the_inputs_in_another_order_finds_every_result_stored(
    role: str, orchestrator: type
) -> None:
    database = InMemoryArrowDatabase()
    stored = {}
    for permutation in ((0, 1, 2), (2, 0, 1), (1, 2, 0)):
        inputs = _inputs(role)
        job = PipelineJob(name="several", database=database)
        with job:
            combine.pod(*(inputs[i] for i in permutation), label="combine")
        summary = job.run(orchestrator=orchestrator())
        node = job.nodes["combine"]
        stored[permutation] = (
            rows_fingerprint(list(node.iter_data()), source_info=False),
            node.pipeline_hash(),
            node.content_hash(),
            presentation(node),
        )
        if permutation != (0, 1, 2):
            assert summary.nodes["combine"].computed == 0, permutation
            assert summary.nodes["combine"].reused == 6, permutation
    first = stored[(0, 1, 2)]
    assert all(result == first for result in stored.values())


@function_pod(output_keys="labelled")
def label_rows(t1: str, d3: str, t3: int) -> str:
    return f"{t1}{t3}{d3}"


@pytest.mark.parametrize("role", [DATA_TAGS, MIXED])
def test_a_function_reads_several_data_tags_and_data_in_any_arrangement(
    role: str,
) -> None:
    """t1 and t3 are data-tags; the tag order and layout change no row."""
    results = {}
    for order in MAIN.tag_orders():
        for layout in LAYOUTS:
            stream = label_rows.pod(make_source(MAIN, Arrangement(order, layout, role)))
            results[(order, layout)] = rows_fingerprint(
                list(stream.iter_data()), source_info=False
            )
            values = sorted(str(data["labelled"]) for _, data in stream.iter_data())
            assert values == ["a1v", "a1y", "a2x", "b1w", "b2u", "b2z"]
    first = next(iter(results.values()))
    assert all(result == first for result in results.values())


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("order", MAIN.tag_orders(), ids="-".join)
@pytest.mark.parametrize("role", [DATA_TAGS, MIXED])
def test_a_function_over_several_data_tags_gives_the_same_rows_on_both_orchestrators(
    role: str, order: tuple[str, ...], layout: str
) -> None:
    """In a job, sync and async: same rows, column order and stored results."""
    tables = []
    for orchestrator in (SyncPipelineOrchestrator, AsyncPipelineOrchestrator):
        job = PipelineJob(name="labels", database=InMemoryArrowDatabase())
        with job:
            label_rows.pod(
                make_source(MAIN, Arrangement(order, layout, role)), label="labels"
            )
        job.run(orchestrator=orchestrator())
        node = job.nodes["labels"]
        tables.append(
            (
                presentation(node),
                [(tuple(t.keys()), tuple(d.keys())) for t, d in node.iter_data()],
                rows_fingerprint(list(node.iter_data()), source_info=False),
                sorted(str(data["labelled"]) for _, data in node.iter_data()),
            )
        )
    assert tables[0] == tables[1]
    assert tables[0][3] == ["a1v", "a1y", "a2x", "b1w", "b2u", "b2z"]


@pytest.mark.parametrize("role", [DATA_TAGS, MIXED])
def test_a_layout_of_the_input_does_not_change_a_fall_through_pods_hashes(
    role: str,
) -> None:
    hashes = {
        layout: fingerprint(
            label_rows.pod(make_source(MAIN, Arrangement(MAIN.tags, layout, role))),
            source_info=False,
        )
        for layout in LAYOUTS
    }
    first = next(iter(hashes.values()))
    assert all(result == first for result in hashes.values())


FALL_THROUGH_ORDER_ISSUE = "ITL-1246"


@pytest.mark.xfail(
    strict=True,
    reason=f"{FALL_THROUGH_ORDER_ISSUE}: the fall-through tags enter the pod's "
    + "identity in the input's tag order",
)
@pytest.mark.parametrize("role", [DATA_TAGS, MIXED])
def test_the_tag_order_of_the_input_does_not_change_a_fall_through_pods_hashes(
    role: str,
) -> None:
    results = {
        order: fingerprint(
            label_rows.pod(make_source(MAIN, Arrangement(order, TAGS_FIRST, role))),
            source_info=False,
        )
        for order in MAIN.tag_orders()
    }
    first = next(iter(results.values()))
    assert all(result == first for result in results.values())


OUTPUT_KEYS_ISSUE = "ITL-1244 (rule undecided)"


@pytest.mark.xfail(
    strict=True,
    reason=f"{OUTPUT_KEYS_ISSUE}: the order of output_keys is not in the identity",
)
def test_swapped_output_keys_are_another_function() -> None:
    """Swapped output keys map the results to other names: not the same function.

    Today both have one uri, so a job serves one's stored values under the other's
    names.
    """

    def make(keys: list[str]):
        @function_pod(output_keys=keys)
        def stats(d1: int, t2: int) -> tuple[int, int]:
            return d1 + t2, d1 * t2

        return stats

    forward, backward = make(["sum", "product"]), make(["product", "sum"])
    assert forward.pod.uri != backward.pod.uri
