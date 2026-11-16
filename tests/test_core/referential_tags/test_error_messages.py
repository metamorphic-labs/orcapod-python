"""Errors that a referential tag causes say so and name the way out (ITL-1022, ITL-1142).

No behaviour changes here: the bind stays strict (a role difference is refused,
ITL-1142 audit Q3), ``pod.process`` stays exact, and the operations not supported yet
still raise. Only the messages explain the referential tag and the remedy.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

import orcapod as op
from orcapod import PipelineJob
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.source_node import SourceNode
from orcapod.core.operators import Batch, DropNulls, MergeJoin
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import SourceSpecMismatchError
from orcapod.types import Schema

ROWS = [{"k": 1, "x": 10, "y": 2}, {"k": 2, "x": 20, "y": 3}]


def _source(tags: list[str], **kwargs: Any) -> op.sources.DictSource:
    return op.sources.DictSource(ROWS, tag_columns=tags, source_id="s", **kwargs)


def _job(tag_schema: dict[str, type], data_schema: dict[str, type]) -> PipelineJob:
    """A job with slot ``src`` of the given schemas, followed by one operator."""
    slot = SourceNode(
        name="src", tag_schema=Schema(tag_schema), data_schema=Schema(data_schema)
    )
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = DropNulls()(slot, label="op")
    return job


# Slots: ``x`` a plain tag, a referential tag (both roles), or data only.
PLAIN_SLOT = ({"k": int, "x": int}, {"y": int})
REFERENTIAL_SLOT = ({"k": int, "x": int}, {"x": int, "y": int})
DATA_SLOT = ({"k": int}, {"x": int, "y": int})


def test_a_referential_source_fits_a_referential_slot() -> None:
    job = _job(*REFERENTIAL_SLOT)
    job.bind(sources={"src": _source(["k", "x"], tag_only_columns=["k"])})


@pytest.mark.parametrize(
    ("slot", "source", "listed", "explained"),
    [
        (
            PLAIN_SLOT,
            lambda: _source(["k", "x"], tag_only_columns=["k"]),
            "unexpected data columns: ['x']",
            [
                "'x' is a referential tag of the source",
                "a plain tag of the slot",
                # ``tag_only_columns`` lists every plain tag: ``k`` stays plain.
                "tag_only_columns=['k', 'x']",
                "declare the slot with 'x' as both a tag and a data column",
            ],
        ),
        (
            DATA_SLOT,
            lambda: _source(["k", "x"], tag_only_columns=["k"]),
            "unexpected tag columns: ['x']",
            [
                "'x' is a referential tag of the source",
                "a data column only of the slot",
                "without 'x' in tag_columns",
            ],
        ),
        (
            REFERENTIAL_SLOT,
            lambda: _source(["k", "x"], tag_only_columns=True),
            "missing data columns: ['x']",
            [
                "'x' is a plain tag of the source but a referential tag of the slot",
                "without 'x' in tag_only_columns",
                "declare the slot with 'x' as a plain tag",
            ],
        ),
        (
            REFERENTIAL_SLOT,
            lambda: _source(["k"], tag_only_columns=True),
            "missing tag columns: ['x']",
            [
                "'x' is a data column only of the source but a referential tag",
                "with 'x' in tag_columns",
            ],
        ),
    ],
    ids=["ref_into_plain", "ref_into_data", "plain_into_ref", "data_into_ref"],
)
def test_a_role_difference_is_refused_and_explained(
    slot: tuple[dict[str, type], dict[str, type]],
    source: Callable[[], op.sources.DictSource],
    listed: str,
    explained: list[str],
) -> None:
    job = _job(*slot)
    with pytest.raises(SourceSpecMismatchError) as excinfo:
        job.bind(sources={"src": source()})
    message = str(excinfo.value)
    assert listed in message
    for text in explained:
        assert text in message


def test_the_named_remedy_binds() -> None:
    """Following the first remedy of the plain-slot refusal binds the source."""
    job = _job(*PLAIN_SLOT)
    job.bind(sources={"src": _source(["k", "x"], tag_only_columns=["k", "x"])})


def test_a_plain_mismatch_says_nothing_about_referential_tags() -> None:
    job = _job({"k": int}, {"y": int})
    with pytest.raises(SourceSpecMismatchError) as excinfo:
        job.bind(sources={"src": _source(["k", "x"], tag_only_columns=True)})
    assert "referential" not in str(excinfo.value)


# ── pod.process and strict=True: the exact form (ITL-1022) ──────────────────


@op.function_pod(output_keys=["half"])
def half(y: int) -> float:
    return y / 2


@pytest.mark.parametrize(
    "call",
    [lambda s: half.pod.process(s), lambda s: half.pod(s, strict=True)],
    ids=["process", "strict"],
)
def test_the_exact_form_names_unread_references_and_the_ways_out(
    call: Callable[[op.sources.DictSource], object],
) -> None:
    source = _source(["k", "x"], tag_only_columns=["k"])
    with pytest.raises(ValueError, match="column 'x' is not a parameter") as excinfo:
        _ = call(source)
    message = str(excinfo.value)
    assert "['x'] are referential tags of the input" in message
    assert "pod(stream) to let them pass through as referential tags" in message
    assert "add ['x'] to the tag_only_columns of the source" in message


def test_a_pod_without_fall_through_does_not_promise_it() -> None:
    """A ``CachedFunctionPod`` called directly keeps its plain call (no fall-through)."""
    pod = half.pod
    assert isinstance(pod, FunctionPod)
    cached = CachedFunctionPod(pod, result_database=InMemoryArrowDatabase())
    source = _source(["k", "x"], tag_only_columns=["k"])
    with pytest.raises(ValueError, match="column 'x' is not a parameter") as excinfo:
        _ = cached.process(source)
    message = str(excinfo.value)
    assert "pass through as referential tags" not in message
    assert "pod(stream), which keeps them as plain tags" in message
    # And that is what the call does.
    assert cached(source).keys() == (("k", "x"), ("half",))


def test_the_exact_form_on_plain_tags_says_nothing_about_referential_tags() -> None:
    plain = _source(["k", "x"], tag_only_columns=True)
    assert half.pod.process(plain).keys() == (("k", "x"), ("half",))
    extra = op.sources.DictSource(
        [{"k": 1, "y": 2, "z": 3}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="e",
    )
    with pytest.raises(ValueError, match="column 'z' is not a parameter") as excinfo:
        _ = half.pod.process(extra)
    assert "referential" not in str(excinfo.value)


# ── not supported yet: the plain-tag way out and the issue ──────────────────


@pytest.mark.parametrize(
    ("call", "issue"),
    [
        (lambda s: Batch(batch_size=2)(s), "ITL-1017"),
        (lambda s: MergeJoin()(s, s), "ITL-1018"),
    ],
    ids=["batch", "merge_join"],
)
def test_not_supported_yet_names_tag_only_columns_true_and_the_issue(
    call: Callable[[op.sources.DictSource], object], issue: str
) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        _ = call(_source(["k", "x"]))
    message = str(excinfo.value)
    assert issue in message
    assert "tag_only_columns=True" in message
