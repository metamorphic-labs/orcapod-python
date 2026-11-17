"""A fall-through pod is wired as its plain pod (ITL-1023, CodeRabbit review of PR #342).

``ReferenceFallThroughPod`` calls the function exactly as the plain pod does, so what
sits around the call is the plain pod's too: the tracker manager that records the
pipeline step, and the invocation context the post-run hooks see. A fall-through pod
called directly (one rebuilt from a saved job) keeps its references.
"""

from __future__ import annotations

from typing import Any

import pytest

import orcapod as op
from orcapod.core.function_pod import FunctionPod, ReferenceFallThroughPod
from orcapod.core.referential_tags import stream_references
from orcapod.core.tracker import BasicTrackerManager
from orcapod.invocation import InvocationContext
from orcapod.hooks import PostRunPayload

ROWS = [{"t": 10, "amp": 0.5}, {"t": 20, "amp": 0.25}]


def _plain(function: Any) -> FunctionPod:
    pod = function.pod
    assert isinstance(pod, FunctionPod)
    return pod


def _source(**kwargs: Any) -> op.sources.DictSource:
    return op.sources.DictSource(ROWS, tag_columns=["t"], source_id="spikes", **kwargs)


def test_the_fall_through_pod_keeps_the_plain_pods_tracker_manager() -> None:
    @op.function_pod(output_keys=["half"])
    def half(amp: float) -> float:
        return amp / 2

    manager = BasicTrackerManager()
    half.pod.tracker_manager = manager
    fall_through = ReferenceFallThroughPod(_plain(half), ["t"])
    assert fall_through.tracker_manager is manager


def test_hooks_see_the_invocation_the_function_sees() -> None:
    seen_by_function: list[str] = []

    @op.function_pod(output_keys=["half"], ctx_arg="ctx")
    def half(amp: float, ctx: InvocationContext) -> float:
        seen_by_function.append(ctx.invocation_hash)
        return amp / 2

    payloads: list[PostRunPayload] = []
    half.pod.add_post_run_hook(payloads.append)
    out = half.pod(_source())
    assert stream_references(out) == ("t",)
    rows = out.as_table().to_pylist()
    assert sorted(r["half"] for r in rows) == [0.125, 0.25]

    assert len(seen_by_function) == 2
    seen_by_hooks = [
        p.invocation_context.invocation_hash
        for p in payloads
        if p.invocation_context is not None
    ]
    assert sorted(seen_by_hooks) == sorted(seen_by_function)
    # The hooks see the function's own input: the referenced column it does not
    # read falls through after the call.
    assert all("t" not in p.input.keys() for p in payloads)


def test_a_fall_through_pod_called_directly_keeps_its_references() -> None:
    @op.function_pod(output_keys=["half"])
    def half(amp: float) -> float:
        return amp / 2

    fall_through = ReferenceFallThroughPod(_plain(half), ["t"])
    out = fall_through(_source())
    assert out.keys() == (("t",), ("t", "half"))
    assert stream_references(out) == ("t",)
    assert out.content_hash() == half.pod(_source()).content_hash()


def test_a_fall_through_pod_refuses_an_input_without_its_references() -> None:
    @op.function_pod(output_keys=["half"])
    def half(amp: float) -> float:
        return amp / 2

    fall_through = ReferenceFallThroughPod(_plain(half), ["t"])
    with pytest.raises(ValueError, match="fall through"):
        fall_through(_source(tag_only_columns=True))
