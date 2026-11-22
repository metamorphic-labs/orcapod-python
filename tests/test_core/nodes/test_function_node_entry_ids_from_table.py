"""A function node's entry ids taken from its input table equal the rows' (ITL-1361, L3).

``FunctionJobNode._input_entries`` computes each input row's base entry id from slices of
the input stream's Arrow table (the tag side with its system tags, and the hash of the
data columns) instead of from tables built from each row's datagrams
(``compute_base_entry_id``). The ids decide which stored results a run finds, so they must
be equal for every input a job can hand a node: every operator, column role and
arrangement of the async-sync matrix, behind an operator and behind a function pod, in
the form a job hands it on (``_materialize_upstream``).
"""

from __future__ import annotations

from typing import Any

import pytest

from orcapod.core.data_tags import stream_data_tags
from orcapod import function_pod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from tests.test_core.operators.test_async_sync_equivalence import (
    AFTER_OPERATOR_CASES,
    CASES,
    FUNCTION_POD_CASES,
    Case,
    source_a,
)


@function_pod(output_keys="c")
def _constant(i8: int) -> int:
    return i8


def _node() -> FunctionJobNode:
    """A function node; only its data context's hasher is used by the ids."""
    return FunctionJobNode(
        _constant.pod,  # pyright: ignore[reportArgumentType]  # a function pod; the checker does not match it to FunctionPodProtocol
        source_a().select_data_columns(["i8"]),
        pipeline_database=InMemoryArrowDatabase(),
    )


def _all_cases() -> list[Any]:
    cases: list[Any] = [pytest.param(c, id=c.label) for c in CASES]
    cases += list(AFTER_OPERATOR_CASES)
    cases += [pytest.param(c, id=f"fp-{c.label}") for c in FUNCTION_POD_CASES]
    return cases


@pytest.mark.parametrize("handed_on", [True, False], ids=["handed_on", "raw"])
@pytest.mark.parametrize("case", _all_cases())
def test_entry_ids_from_the_table_equal_the_rows(case: Case, handed_on: bool) -> None:
    """Every row's id from table slices equals ``compute_base_entry_id`` of its row.

    For the stream a job hands a node (``handed_on``), and for a raw stream over the
    operator result's own table, whose system-tag fields may be declared nullable (a
    join over inputs tied on pipeline hash).
    """
    result = case.make_op()(*case.make_sources())
    rows = list(result.iter_data())
    if not rows:
        pytest.skip("no rows")
    stream = (
        StaticOutputOperatorPod._materialize_upstream(rows, result)  # pyright: ignore[reportPrivateUsage]  # the stream a job hands a node
        if handed_on
        else ArrowTableStream(
            result.as_table(columns={"system_tags": True, "source": True}),
            tag_columns=result.keys()[0],
            data_tags=stream_data_tags(result),
        )
    )
    node = _node()
    entries = node._input_entries(stream)  # pyright: ignore[reportPrivateUsage]  # under test
    expected = [
        node.compute_base_entry_id(tag, data) for tag, data in stream.iter_data()
    ]
    assert [entry_id for _, _, entry_id in entries] == expected


def test_entry_ids_of_a_stream_that_is_not_a_table_stream_take_the_row_path() -> None:
    """Any other stream kind (here a source) is read row by row, as before."""
    stream = source_a()
    assert not isinstance(stream, ArrowTableStream)
    node = _node()
    entries = node._input_entries(stream)  # pyright: ignore[reportPrivateUsage]  # under test
    expected = [
        node.compute_base_entry_id(tag, data) for tag, data in stream.iter_data()
    ]
    assert [entry_id for _, _, entry_id in entries] == expected
