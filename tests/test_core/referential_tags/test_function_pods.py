"""Referential tags at function pods (ITL-1023).

Specification "Orcapod v0.4 — Referential Tags" (ITL-1004), sections 5, 6, 12.4 and
tests 15.2 and 15.6. A function reads a referenced column through the ordinary data
binding. A pod's output data is the function's outputs, so every referential tag of the
input is a plain tag of the output, with the same values (developer, November 12, 2026).
An output named like a tag is an error when the pipeline is built.
"""

from __future__ import annotations

import warnings

import pytest

import orcapod as op
from orcapod.core.operators import SelectDataColumns
from orcapod.core.referential_tags import (
    ReferenceMaterializationWarning,
    stream_references,
)
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline import PipelineJob

ROWS = [{"t": 10, "amp": 0.5}, {"t": 20, "amp": 0.25}]


def _source(**kwargs: object) -> op.sources.DictSource:
    return op.sources.DictSource(ROWS, tag_columns=["t"], source_id="spikes", **kwargs)


def test_function_reads_a_referenced_column_by_name() -> None:
    @op.function_pod(output_keys=["scaled"])
    def scale(t: int, amp: float) -> float:
        return t * amp

    out = scale.pod(_source())
    assert sorted(r["scaled"] for r in out.as_table().to_pylist()) == [5.0, 5.0]


def test_tags_pass_through_and_become_plain_at_the_pod() -> None:
    @op.function_pod(output_keys=["double"])
    def double(t: int) -> int:
        return 2 * t

    out = double.pod(_source())
    assert out.keys() == (("t",), ("double",))
    assert stream_references(out) == ()
    rows = sorted(out.as_table().to_pylist(), key=lambda r: r["t"])
    assert rows == [{"t": 10, "double": 20}, {"t": 20, "double": 40}]


@pytest.mark.parametrize("tag_only", [(), True], ids=["referential", "plain"])
def test_output_named_like_a_tag_raises_when_built(tag_only: object) -> None:
    @op.function_pod(output_keys=["t"])
    def clobber(amp: float) -> int:
        return 0

    with pytest.raises(InputValidationError, match="conflicts with existing tag"):
        clobber.pod(_source(tag_only_columns=tag_only))


def test_ordinary_data_replacement_is_unchanged() -> None:
    @op.function_pod(output_keys=["amp"])
    def halve(amp: float) -> float:
        return amp / 2

    out = halve.pod(_source())
    assert out.keys() == (("t",), ("amp",))


def test_automatic_select_materializes_without_a_diagnostic() -> None:
    @op.function_pod(output_keys=["half"])
    def half(amp: float) -> float:
        return amp / 2

    with warnings.catch_warnings():
        warnings.simplefilter("error", ReferenceMaterializationWarning)
        out = half.pod(_source())
    # The same pipeline as with the select written by hand (ITL-807).
    with pytest.warns(ReferenceMaterializationWarning):
        by_hand = half.pod(SelectDataColumns(["amp"])(_source()))
    assert out.content_hash() == by_hand.content_hash()
    assert out.pipeline_hash() == by_hand.pipeline_hash()


CALLS: list[float] = []


@op.function_pod(output_keys=["half"])
def half_amp(amp: float) -> float:
    """Reads only ``amp``: never the referenced ``t``."""
    CALLS.append(amp)
    return amp / 2


def test_function_not_reading_the_reference_keeps_its_result_identity() -> None:
    """Section 12.4: retagging the source does not force the function to run again."""
    store = InMemoryArrowDatabase()
    CALLS.clear()
    plain_job = PipelineJob(store=store)
    with plain_job:
        half_amp.pod(_source(tag_only_columns=True), label="half")
    plain_job.run()
    assert sorted(CALLS) == [0.25, 0.5]

    # The same source with a referential tag: a new pipeline (new records), but the
    # function's inputs are the same data, so every result is found in the store.
    CALLS.clear()
    referential_job = PipelineJob(store=store)
    with referential_job:
        half_amp.pod(_source(), label="half")
    referential_job.run()
    assert CALLS == []
    plain_rows = plain_job.nodes["half"].as_table().to_pylist()
    referential_rows = referential_job.nodes["half"].as_table().to_pylist()
    assert sorted(plain_rows, key=lambda r: r["t"]) == sorted(
        referential_rows, key=lambda r: r["t"]
    )
    # The pipelines differ: the binding is part of pipeline structure (section 12.3).
    assert (
        plain_job.nodes["half"].pipeline_hash()
        != referential_job.nodes["half"].pipeline_hash()
    )
