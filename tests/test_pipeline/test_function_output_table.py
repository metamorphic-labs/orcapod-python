"""A function node's output takes stored results from their table (ITL-1361, L3).

``FunctionJobNode._output_stream`` builds its output from pieces: the rows of results
read from the store are taken from the stored rows' table, the others are built from
their datagrams, and F's whole-table steps finish the table. It must equal the table
rebuilt from every output row (``StaticOutputOperatorPod._materialize_upstream``), in
identity and in every column group, whatever mix of stored and computed rows a run has:
all stored, none stored, interleaved, duplicate inputs, a function returning ``None``,
over the several-items arrangements (tag orders, layouts, data-tags).
"""

from __future__ import annotations

import warnings
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import ProvisionalHashWarning
from orcapod.pipeline.observer import NoOpObserver
from tests.test_core.operators.several_items import MAIN, arrangements, make_source
from tests.test_pipeline.test_stream_transport_equivalence import (
    _rebuilt,  # pyright: ignore[reportPrivateUsage]  # the row rebuild these tests share
    assert_same_stream,
)


@function_pod(output_keys="half")
def halved(d1: int) -> int | None:
    """``d1 / 10`` for an even tenth, ``None`` (no output row) otherwise."""
    return d1 // 10 if (d1 // 10) % 2 == 0 else None


@function_pod(output_keys="tripled")
def tripled(d1: int) -> int:
    return 3 * d1


@pytest.fixture
def compared(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Check every function node output against the rebuild of its rows."""
    checked: list[int] = []
    original = FunctionJobNode._output_stream  # pyright: ignore[reportPrivateUsage]  # under test

    def output_stream(self: Any, rows: list[Any]) -> Any:
        out = original(self, rows)
        assert_same_stream(out, _rebuilt(rows, self))
        checked.append(len(rows))
        return out

    monkeypatch.setattr(FunctionJobNode, "_output_stream", output_stream)
    return checked


def _job(source: Any, pod: Any, database: Any) -> PipelineJob:
    job = PipelineJob(name="function_output", database=database)
    with job:
        pod.pod(source.select_data_columns(["d1"]), label="fn")
    return job


@pytest.mark.parametrize("pod", [tripled, halved], ids=["every_row", "some_none"])
@pytest.mark.parametrize("arrangement", arrangements(MAIN), ids=lambda a: a.id)
def test_the_output_is_the_rebuild_of_its_rows(
    arrangement: Any, pod: Any, compared: list[int]
) -> None:
    """Computed, then stored and computed, then all stored."""
    database = InMemoryArrowDatabase()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ProvisionalHashWarning)
        # First the first three rows (same positional record ids as in the full
        # source): the full run then takes them from the store and computes the rest.
        _job(make_source(MAIN, arrangement, rows=[0, 1, 2]), pod, database).run()
        _job(make_source(MAIN, arrangement), pod, database).run()
        _job(make_source(MAIN, arrangement), pod, database).run()
    assert len(compared) == 3


def _keyed(keys: list[int]) -> Any:
    """Rows keyed by ``j`` (``record_id_columns``), so a subset keeps its record ids."""
    from orcapod.sources import DictSource

    return DictSource(
        [{"k": j % 3, "j": j, "d1": 10 * j} for j in keys],
        tag_columns=["k", "j"],
        record_id_columns=["j"],
        tag_only_columns=True,
        source_id="keyed",
    )


@pytest.mark.parametrize("pod", [tripled, halved], ids=["every_row", "some_none"])
def test_stored_and_computed_rows_interleave(pod: Any, compared: list[int]) -> None:
    """Every other row stored: the output alternates stored and computed rows."""
    database = InMemoryArrowDatabase()
    _job(_keyed([0, 2, 4, 6]), pod, database).run()
    _job(_keyed(list(range(8))), pod, database).run()
    _job(_keyed([7, 6, 5, 4, 3, 2, 1, 0]), pod, database).run()
    assert len(compared) == 3


def test_duplicate_inputs_give_one_row_each(compared: list[int]) -> None:
    """Two equal inputs are two output rows, both from the store on a warm run."""
    from orcapod.sources import DictSource

    database = InMemoryArrowDatabase()
    source = DictSource(
        [{"k": i % 2, "j": i, "d1": 10 * (i % 2)} for i in range(4)],
        tag_columns=["k", "j"],
        tag_only_columns=True,
        source_id="duplicates",
    )
    _job(source, tripled, database).run()
    _job(source, tripled, database).run()
    assert compared == [4, 4]


class _ReadsRows(NoOpObserver):
    """Records every input row an observer is shown, as its values."""

    def __init__(self) -> None:
        self.rows: list[tuple[Any, Any]] = []

    def contextualize(self, *identity_path: str) -> _ReadsRows:
        return self

    def on_data_start(self, node_label: str, tag: Any, data: Any) -> None:
        self.rows.append(
            (tag.as_dict(all_info=True), data.as_dict(columns={"source": True}))
        )


def test_an_observer_reading_the_input_rows_sees_them(compared: list[int]) -> None:
    """On a run over stored results, the rows an observer reads are the input's rows.

    The node shows its observers views of its input rows (``_RowView``), made when
    read; their values are the rows' own.
    """
    database = InMemoryArrowDatabase()
    source = _keyed(list(range(6)))
    _job(source, tripled, database).run()
    job = _job(source, tripled, database)
    observer = _ReadsRows()
    job.run(observer=observer)
    expected = [
        (tag.as_dict(all_info=True), data.as_dict(columns={"source": True}))
        for tag, data in source.select_data_columns(["d1"]).iter_data()
    ]
    assert observer.rows == expected
