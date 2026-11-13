"""Referential tags at function pods (ITL-1023).

Specification "Orcapod v0.4 — Referential Tags" (ITL-1004), sections 5, 6, 12.4 and
tests 15.2 and 15.6. A function reads a referenced column through the ordinary data
binding. Developer decisions, November 12, 2026: with ``pod.process(stream)`` a pod's
output data is the function's outputs, so every referential tag of the input is a plain
tag of the output; with the call form ``pod(stream)`` the input's referential tags fall
through the pod and stay referential (``ReferenceFallThroughPod``), while the function,
its result identity and its result store are the plain pod's. An output named like a
tag is an error when the pipeline is built.
"""

from __future__ import annotations

import warnings
from typing import Any, cast

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


def _source(**kwargs: Any) -> op.sources.DictSource:
    return op.sources.DictSource(ROWS, tag_columns=["t"], source_id="spikes", **kwargs)


def test_function_reads_a_referenced_column_by_name() -> None:
    @op.function_pod(output_keys=["scaled"])
    def scale(t: int, amp: float) -> float:
        return t * amp

    out = scale.pod(_source())
    assert sorted(r["scaled"] for r in out.as_table().to_pylist()) == [5.0, 5.0]


def test_process_makes_references_plain_at_the_pod() -> None:
    @op.function_pod(output_keys=["double"])
    def double(t: int) -> int:
        return 2 * t

    out = double.pod.process(_source().select_data_columns(["t"]))
    assert out.keys() == (("t",), ("double",))
    assert stream_references(out) == ()
    rows = sorted(out.as_table().to_pylist(), key=lambda r: r["t"])
    assert rows == [{"t": 10, "double": 20}, {"t": 20, "double": 40}]


def test_the_call_lets_references_fall_through() -> None:
    @op.function_pod(output_keys=["half"])
    def half(amp: float) -> float:
        return amp / 2

    out = half.pod(_source())
    assert out.keys() == (("t",), ("t", "half"))
    assert stream_references(out) == ("t",)
    table = out.as_table(columns={"source": True})
    assert table.column_names.count("t") == 1
    # The column keeps its source info: it is the input row's own.
    assert sorted(str(t) for t in table.column("_source_t").to_pylist()) == [
        "spikes::row_0::t",
        "spikes::row_1::t",
    ]
    for tag, data in out.iter_data():
        assert tag["t"] == data["t"]


def test_fall_through_equals_the_composition_it_abbreviates() -> None:
    """A unique temporary tag per row, the plain pod, a join back, the temporary tag
    dropped: the same rows, duplicate user tags included."""
    rows = [{"t": 10, "amp": 0.5}, {"t": 10, "amp": 0.25}, {"t": 20, "amp": 1.0}]
    source = op.sources.DictSource(rows, tag_columns=["t"], source_id="dup")

    @op.function_pod(output_keys=["half"])
    def half(amp: float) -> float:
        return amp / 2

    fallen = sorted(
        (tag["t"], data["t"], data["half"])
        for tag, data in half.pod(source).iter_data()
    )
    # The composition, written out with a per-row tag ``row``.
    with_row = op.sources.DictSource(
        [{**r, "row": i} for i, r in enumerate(rows)],
        tag_columns=["t", "row"],
        tag_only_columns=["row"],
        source_id="dup",
    )
    # The plain pod (``t`` becomes a plain tag), joined back on ``t`` and ``row`` with
    # the input's referenced column: plain with referential gives the reference back.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ReferenceMaterializationWarning)
        computed = half.pod.process(with_row.select_data_columns(["amp"]))
    joined = op.operators.Join()(computed, with_row.select_data_columns(["t"]))
    composed = sorted(
        (tag["t"], data["t"], data["half"]) for tag, data in joined.iter_data()
    )
    assert fallen == composed == [(10, 10, 0.125), (10, 10, 0.25), (20, 20, 0.5)]


def test_without_references_the_call_is_the_plain_pod() -> None:
    @op.function_pod(output_keys=["half"])
    def half(amp: float) -> float:
        return amp / 2

    plain = _source(tag_only_columns=True)
    assert half.pod(plain).content_hash() == half.pod.process(plain).content_hash()
    assert half.pod(plain).pipeline_hash() == half.pod.process(plain).pipeline_hash()


def test_strict_call_is_process() -> None:
    @op.function_pod(output_keys=["double"])
    def double(t: int, amp: float) -> float:
        return t * amp

    strict = double.pod(_source(), strict=True)
    assert stream_references(strict) == ()


@pytest.mark.parametrize("tag_only", [(), True], ids=["referential", "plain"])
def test_output_named_like_a_tag_raises_when_built(tag_only: object) -> None:
    @op.function_pod(output_keys=["t"])
    def clobber(amp: float) -> int:
        return 0

    with pytest.raises(InputValidationError, match="is also a tag column of the input"):
        clobber.pod(_source(tag_only_columns=tag_only))


def test_ordinary_data_replacement_is_unchanged() -> None:
    @op.function_pod(output_keys=["amp"])
    def halve(amp: float) -> float:
        return amp / 2

    out = halve.pod(_source())
    assert out.keys() == (("t",), ("t", "amp"))


def test_the_call_reports_no_materialization() -> None:
    @op.function_pod(output_keys=["half"])
    def half(amp: float) -> float:
        return amp / 2

    with warnings.catch_warnings():
        warnings.simplefilter("error", ReferenceMaterializationWarning)
        half.pod(_source())
    # A select written by hand that drops the referenced column does report.
    with pytest.warns(ReferenceMaterializationWarning):
        SelectDataColumns(["amp"])(_source())


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
        half_amp.pod.process(
            _source(tag_only_columns=True).select_data_columns(["amp"]), label="half"
        )
    plain_job.run()
    assert sorted(CALLS) == [0.25, 0.5]

    # The same source with a referential tag: a new pipeline (new records), but the
    # function's inputs are the same data, so every result is found in the store.
    CALLS.clear()
    referential_job = PipelineJob(store=store)
    with referential_job:
        # The call form: ``t`` falls through the pod.
        half_amp.pod(_source(), label="half")
    referential_job.run()
    assert CALLS == []
    plain_rows = plain_job.nodes["half"].as_table().to_pylist()
    referential_rows = referential_job.nodes["half"].as_table().to_pylist()
    assert sorted(plain_rows, key=lambda r: r["t"]) == sorted(
        referential_rows, key=lambda r: r["t"]
    )
    assert referential_job.nodes["half"].keys() == (("t",), ("t", "half"))
    # The pipelines differ: the binding is part of pipeline structure (section 12.3).
    assert (
        plain_job.nodes["half"].pipeline_hash()
        != referential_job.nodes["half"].pipeline_hash()
    )


def test_a_side_effect_function_gets_a_referenced_column_only_when_it_names_it() -> (
    None
):
    """After a pod call the keys fall through; a sink written for the data still works."""
    from orcapod.side_effects import SideEffectPod, side_effect_pod

    seen: list[dict[str, object]] = []

    @op.function_pod(output_keys=["half"])
    def half(amp: float) -> float:
        return amp / 2

    @side_effect_pod
    def record(half: float, ctx: object) -> None:
        seen.append({"half": half})

    @side_effect_pod
    def record_with_key(half: float, t: int, ctx: object) -> None:
        seen.append({"half": half, "t": t})

    out = half.pod(_source())
    assert stream_references(out) == ("t",)
    for sink in (record, record_with_key):
        list(cast(SideEffectPod, sink)(out).iter_data())
    assert sorted(seen, key=lambda r: (len(r), r["half"])) == [
        {"half": 0.125},
        {"half": 0.25},
        {"half": 0.125, "t": 20},
        {"half": 0.25, "t": 10},
    ]
