"""A side-effect node's output is its input table at the rows it passed on (ITL-1361, L4).

A side-effect job node passes its input rows on unchanged (or drops the ones whose
delivery failed). Its output stream is F of the input's Arrow table taken at the rows'
positions (``passed_rows_output_stream``), instead of the table rebuilt from the rows.
It must equal the rebuild (``StaticOutputOperatorPod._materialize_upstream`` of the
rows passed on) in identity and in every column group, for every arrangement of the
several-items family, with rows dropped and with none, and on a load-only run.
"""

from __future__ import annotations

import warnings
from typing import Any

import pytest

from orcapod import PipelineJob
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import ProvisionalHashWarning
from orcapod.side_effects import SideEffectJobNode, SideEffectPodConfig, side_effect_pod
from tests.test_core.operators.several_items import MAIN, arrangements, make_source
from tests.test_pipeline.test_stream_transport_equivalence import (
    _rebuilt,  # pyright: ignore[reportPrivateUsage]  # the row rebuild these tests share
    assert_same_stream,
)


def _drop_odd(d1: int, ctx: Any) -> None:
    """Fail the delivery of every other row (``d1`` is 10, 20, ...): the row is dropped."""
    if (d1 // 10) % 2:
        raise ValueError("odd")


_DROPPING = side_effect_pod(
    _drop_odd,
    config=SideEffectPodConfig(
        track_completion=False, drop_on_failure=True, on_error="log"
    ),
)


def _keep_all(d1: int, ctx: Any) -> None:
    """Deliver every row."""
    return None


_KEEPING = side_effect_pod(
    _keep_all, config=SideEffectPodConfig(track_completion=False, on_error="log")
)


@pytest.fixture
def side_effect_outputs(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, Any]]:
    """Each side-effect node's output, with the rows it held, as ``execute`` gave it."""
    seen: list[tuple[Any, Any]] = []
    original = SideEffectJobNode.execute

    def execute(self: Any, *args: Any, **kwargs: Any) -> Any:
        out = original(self, *args, **kwargs)
        seen.append((out, list(self._held_rows or [])))  # pyright: ignore[reportPrivateUsage]  # the rows the run passed on
        return out

    monkeypatch.setattr(SideEffectJobNode, "execute", execute)
    return seen


@pytest.mark.parametrize("load_only", [False, True], ids=["run", "load_only"])
@pytest.mark.parametrize("pod", [_DROPPING, _KEEPING], ids=["dropping", "keeping"])
@pytest.mark.parametrize("arrangement", arrangements(MAIN), ids=lambda a: a.id)
def test_the_output_is_the_rebuild_of_the_rows_passed_on(
    arrangement: Any,
    pod: Any,
    load_only: bool,
    side_effect_outputs: list[tuple[Any, Any]],
) -> None:
    """L4: the output equals the rows passed on, rebuilt with the node's identity."""
    database = InMemoryArrowDatabase()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ProvisionalHashWarning)
        source = make_source(MAIN, arrangement)
        job = PipelineJob(name="side_effect_output", database=database)
        with job:
            delivered: Any = pod
            delivered(source.select_data_columns(["d1"]), label="delivered")
        if load_only:
            job.run()
            # A node holding a current run is not run again: clear it.
            job.nodes["delivered"].clear_cache()
            side_effect_outputs.clear()
        job.run(load_only=load_only)
    assert side_effect_outputs
    node = job.nodes["delivered"]
    for out, rows in side_effect_outputs:
        assert_same_stream(out, _rebuilt(rows, node))
    if pod is _DROPPING and not load_only:
        _, rows = side_effect_outputs[-1]
        assert 0 < len(rows) < len(list(source.iter_data()))
