"""A pandas frame's column order is kept in storage and is part of its cache key (ITL-897).

Through the public interface, for the in-memory and Delta stores and the sync and async
orchestrators. On v0.2.0 a cached pod returning ``df.columns[0]`` was handed the result
computed for the same frame with its columns in another order.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.sources import DictSource


def _frame(reverse: bool) -> pd.DataFrame:
    """Columns ``a, b`` over a two-level index ``i, j``, or both orders reversed."""
    if reverse:
        index = pd.MultiIndex.from_tuples([("p", 0), ("q", 1)], names=["j", "i"])
        return pd.DataFrame({"b": ["x", "y"], "a": [1, 2]}, index=index)
    index = pd.MultiIndex.from_tuples([(0, "p"), (1, "q")], names=["i", "j"])
    return pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}, index=index)


@function_pod(output_keys="df")
def make_frame(reverse: bool) -> pd.DataFrame:
    return _frame(reverse)


@function_pod(output_keys="leading")
def leading_names(df: pd.DataFrame) -> str:
    """The first column's name and the first index level's name."""
    return f"{df.columns[0]}/{df.index.names[0]}"


_COUNTED_CALLS: list[str] = []


@function_pod(output_keys="leading")
def counted_leading_names(df: pd.DataFrame) -> str:
    """As ``leading_names``, recording every call that actually computes."""
    leading = f"{df.columns[0]}/{df.index.names[0]}"
    _COUNTED_CALLS.append(leading)
    return leading


_CONSUMER_CALLS: list[int] = []


@function_pod(output_keys="df")
def make_frame_storage_changes(n: int) -> pd.DataFrame:
    """A frame storage reads back in another form: object ints, pyarrow strings, attrs."""
    frame = pd.DataFrame(
        {
            "ints": pd.Series(list(range(n)), dtype=object),
            "text": pd.array(["x"] * n, dtype="string[pyarrow]"),
        },
        index=pd.RangeIndex(n),
    )
    frame.attrs = {1: "one"}
    return frame


@function_pod(output_keys="total")
def counted_total(df: pd.DataFrame) -> int:
    """The sum of ``ints``, recording every call that actually computes."""
    _CONSUMER_CALLS.append(len(df))
    return int(df["ints"].sum())


_STORES: dict[str, Callable[[Path], ArrowDatabaseProtocol]] = {
    "in_memory": lambda _: InMemoryArrowDatabase(),
    "delta": lambda tmp: DeltaTableDatabase(tmp / "delta"),
}
_ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}
_EXPECTED = {"fwd": "a/i", "rev": "b/j"}


def _run(
    store: ArrowDatabaseProtocol, orchestrator_name: str, rows: list[tuple[str, bool]]
) -> PipelineJob:
    source = DictSource(
        [{"k": key, "reverse": reverse} for key, reverse in rows], tag_columns=["k"]
    )
    job = PipelineJob(name="pandas_order", store=store)
    with job:
        frames = make_frame.pod(source, label="make")
        leading_names.pod(frames, label="leading")
    job.run(orchestrator=_ORCHESTRATORS[orchestrator_name]())
    return job


@pytest.mark.parametrize("store_name", list(_STORES))
@pytest.mark.parametrize("orchestrator_name", list(_ORCHESTRATORS))
class TestPandasColumnOrder:
    def test_frames_stored_with_their_order(
        self, store_name: str, orchestrator_name: str, tmp_path: Path
    ) -> None:
        job = _run(
            _STORES[store_name](tmp_path),
            orchestrator_name,
            [("fwd", False), ("rev", True)],
        )
        got = {tag["k"]: data["df"] for tag, data in job.nodes["make"].iter_data()}
        assert_frame_equal(got["fwd"], _frame(reverse=False))
        assert_frame_equal(got["rev"], _frame(reverse=True))

    def test_cached_pod_computes_for_each_order(
        self, store_name: str, orchestrator_name: str, tmp_path: Path
    ) -> None:
        """Both orders in one run: each row gets the result for its own frame."""
        job = _run(
            _STORES[store_name](tmp_path),
            orchestrator_name,
            [("fwd", False), ("rev", True)],
        )
        got = {
            tag["k"]: data["leading"] for tag, data in job.nodes["leading"].iter_data()
        }
        assert got == _EXPECTED

    def test_warm_cache_does_not_serve_another_order(
        self, store_name: str, orchestrator_name: str, tmp_path: Path
    ) -> None:
        """One order per run on one store, so the second run meets a warm cache."""
        store = _STORES[store_name](tmp_path)
        got: dict[str, str] = {}
        for key, reverse in (("fwd", False), ("rev", True)):
            job = _run(store, orchestrator_name, [(key, reverse)])
            got.update(
                {
                    tag["k"]: data["leading"]
                    for tag, data in job.nodes["leading"].iter_data()
                }
            )
        assert got == _EXPECTED

    def test_equal_frame_is_served_from_the_cache(
        self, store_name: str, orchestrator_name: str, tmp_path: Path
    ) -> None:
        """An equal frame in a later run is a cache hit; a reordered one computes."""
        store = _STORES[store_name](tmp_path)
        _COUNTED_CALLS.clear()
        got: dict[str, str] = {}
        for key, reverse in (("first", False), ("equal", False), ("reordered", True)):
            source = DictSource([{"k": key, "reverse": reverse}], tag_columns=["k"])
            job = PipelineJob(name="pandas_order_counted", store=store)
            with job:
                frames = make_frame.pod(source, label="make")
                counted_leading_names.pod(frames, label="leading")
            job.run(orchestrator=_ORCHESTRATORS[orchestrator_name]())
            got.update(
                {
                    tag["k"]: data["leading"]
                    for tag, data in job.nodes["leading"].iter_data()
                }
            )
        assert got == {"first": "a/i", "equal": "a/i", "reordered": "b/j"}
        assert _COUNTED_CALLS == ["a/i", "b/j"]

    def test_frame_read_back_from_the_store_keeps_its_cache_key(
        self, store_name: str, orchestrator_name: str, tmp_path: Path
    ) -> None:
        """A frame that storage reads back in another form is still a cache hit.

        Run 1 hashes the frame the pod returned; run 2 hashes the frame read back from
        the store (``object`` ints as ``int64``, ``string[pyarrow]`` as ``string[python]``,
        ``attrs`` as JSON). The consumer must not compute again.
        """
        store = _STORES[store_name](tmp_path)
        _CONSUMER_CALLS.clear()
        totals: list[int] = []
        for _ in range(2):
            source = DictSource([{"k": "a", "n": 3}], tag_columns=["k"])
            job = PipelineJob(name="pandas_storage_form", store=store)
            with job:
                frames = make_frame_storage_changes.pod(source, label="make")
                counted_total.pod(frames, label="total")
            job.run(orchestrator=_ORCHESTRATORS[orchestrator_name]())
            totals += [data["total"] for _, data in job.nodes["total"].iter_data()]
        assert totals == [3, 3]
        assert _CONSUMER_CALLS == [3]
