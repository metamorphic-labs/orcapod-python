"""A function node's output takes stored results from their table (ITL-1361, L3).

``FunctionJobNode._output_stream`` builds its output from pieces: the rows of results
read from the store are taken from the stored rows' table, the others are built from
their datagrams, and F's whole-table steps finish the table. It must equal the table
rebuilt from every output row (``StaticOutputOperatorPod._materialize_upstream``), in
identity and in every column group, whatever mix of stored and computed rows a run has:
all stored, none stored, interleaved, duplicate inputs, a null output value,
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
    """``d1 / 10`` for an even tenth, ``None`` (a row with a null ``half``) otherwise."""
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


@pytest.mark.parametrize("load_only", [False, True], ids=["run", "load_only"])
@pytest.mark.parametrize("pod", [tripled, halved], ids=["every_row", "some_none"])
@pytest.mark.parametrize(
    "stored", [[], [6, 1], [5, 3, 1], list(range(8))], ids=lambda s: f"{len(s)}_stored"
)
def test_the_held_rows_are_the_outputs_rows_in_order(
    stored: list[int], pod: Any, load_only: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """What a read of the node gives is the table downstream received, in input order
    (ITL-1441), also when that table mixes rows of the stored table and computed rows."""
    handed_on: list[Any] = []
    original = FunctionJobNode._output_stream  # pyright: ignore[reportPrivateUsage]  # what a successor reads

    def output_stream(self: Any, rows: list[Any]) -> Any:
        out = original(self, rows)
        handed_on.append(out)
        return out

    database = InMemoryArrowDatabase()
    if stored:
        _job(_keyed(stored), pod, database).run()
    monkeypatch.setattr(FunctionJobNode, "_output_stream", output_stream)
    keys = [3, 0, 7, 1, 6, 2, 5, 4]
    job = _job(_keyed(keys), pod, database)
    job.run(load_only=load_only)
    node = job.nodes["fn"]
    expected = [j for j in keys if j in stored or not load_only]
    config = {"system_tags": True, "source": True}
    (output,) = handed_on
    # The rows, before the output's table is built, and the table built from them.
    assert [tag["j"] for tag, _ in node.iter_data()] == expected
    if expected:
        table = output.as_table(columns=config)
        assert table["j"].to_pylist() == expected
        assert node.as_table(columns=config).to_pylist() == table.to_pylist()


def test_repeated_values_give_one_row_each(compared: list[int]) -> None:
    """Rows with equal data values (distinct inputs: each its own tag ``j``) are one
    output row each, all from the store on a warm run."""
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


def test_duplicate_inputs_give_one_row_each(compared: list[int]) -> None:
    """Inputs that are equal in every column, system tags included (one base entry
    id, the same record id), are one output row each, computed once on a cold run
    and every occurrence taken from the store on a warm run (repeated positions in
    the stored table)."""
    import pyarrow as pa

    from orcapod.core.streams import ArrowTableStream

    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("d1", pa.int64(), nullable=False),
            pa.field("sid", pa.large_string(), nullable=False),
            pa.field("rid", pa.large_string(), nullable=False),
        ]
    )
    table = pa.table(
        {
            "k": [0, 1, 0, 1],
            "d1": [10, 20, 10, 20],
            "sid": ["s", "s", "s", "s"],
            "rid": ["row_0", "row_1", "row_0", "row_1"],
        },
        schema=schema,
    )
    stream = ArrowTableStream(
        table, tag_columns=["k"], system_tag_columns=["sid", "rid"]
    )
    database = InMemoryArrowDatabase()
    nodes = [
        FunctionJobNode(
            tripled.pod,  # pyright: ignore[reportArgumentType]  # a function pod
            stream,
            pipeline_database=database,
        )
        for _ in range(2)
    ]
    for node in nodes:  # cold, then warm over the same store
        output = node.execute(stream)
        assert output.as_table()["tripled"].to_pylist() == [30, 60, 30, 60]
    entries = nodes[-1]._input_entries(stream)  # pyright: ignore[reportPrivateUsage]  # the ids repeat
    assert len({entry_id for _, _, entry_id in entries}) == 2
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

    An observer that may read the rows (any but the library's row-blind ones, so this
    one) receives the rows themselves, as on ``main``; their values are the rows' own.
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
