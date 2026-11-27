"""Findings of the fourth independent review of PR #601 (ITL-1491), each as a test.

The replay of an ordinary pipeline over logical lists (a held operator node whose
rows hold ``list[Path]``), and the sync call sites of the guards against a null
inner list.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Batch, GroupBy, Index, MergeJoin
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import DictSource
from tests.test_pipeline.test_token_types_review_2 import (
    _keyed,  # pyright: ignore[reportPrivateUsage]  # the shared stream fixtures
)

STR = pa.large_string()
TOKENS = pa.large_list(STR)
NESTED = pa.large_list(TOKENS)
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}

pytestmark = pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")


# ---------------------------------------------------------------------------
# 1. A held node over logical lists: the replay of an ordinary pipeline
# ---------------------------------------------------------------------------


def _paths(source_id: str) -> DictSource:
    return DictSource(
        [
            {"g": "m", "k": 1, "p": Path("/a"), "n": 1},
            {"g": "m", "k": 2, "p": Path("/b"), "n": 2},
            {"g": "n", "k": 1, "p": Path("/c"), "n": 3},
        ],
        tag_columns=["g", "k"],
        tag_only_columns=True,
        source_id=source_id,
    )


@function_pod(output_keys="total")
def count_paths(p: list[Path], n: list[int]) -> int:
    return len(p) + sum(n)


@function_pod(output_keys="total")
def count_nested_paths(p: list[list[Path]], n: list[list[int]]) -> int:
    return sum(len(paths) for paths in p) + sum(sum(numbers) for numbers in n)


# name: (the stream of lists the function reads, the function, its rows)
LIST_BUILDERS: dict[str, Any] = {
    "batch": (lambda: Batch(batch_size=2)(_paths("a"), label="lists"), count_paths, 2),
    "group_by": (lambda: GroupBy(by=["g"])(_paths("a"), label="lists"), count_paths, 2),
    "merge_join": (
        lambda: MergeJoin()(_paths("a"), _paths("b"), label="lists"),
        count_paths,
        3,
    ),
    "group_by_then_batch": (
        lambda: Batch(batch_size=2)(GroupBy(by=["g"])(_paths("a")), label="lists"),
        count_nested_paths,
        1,
    ),
}


@pytest.mark.parametrize("second", list(ORCHESTRATORS))
@pytest.mark.parametrize("first", list(ORCHESTRATORS))
@pytest.mark.parametrize("builder", list(LIST_BUILDERS))
def test_a_replay_over_logical_lists_reuses_its_rows(
    builder: str, first: str, second: str
) -> None:
    """A source with a ``Path`` column, an operator that builds lists, a function
    over the lists: this must never fail again.

    After a run, the function's cache alone is cleared and the job runs again: the
    operator node is held, and its rows (a ``list[Path]`` column, an Arrow
    extension type, with list tokens) feed the function, which finds every row
    stored. A check of held rows the async orchestrator had for a while hashed the
    rows' Arrow types, and Orcapod's own extension type for a list of a logical
    element cannot be hashed: ``TypeError: unhashable type`` on the second run
    (fourth review of PR #601, finding 1; the check was taken out, ITL-1534).
    Every order of the two routes over one store reuses every row, for ``Batch``,
    ``GroupBy``, ``MergeJoin`` and ``GroupBy`` then ``Batch``.
    """
    make, function, rows = LIST_BUILDERS[builder]
    job = PipelineJob(name="logical_held", database=InMemoryArrowDatabase())
    with job:
        function.pod(make(), label="f")
    ran = job.run(orchestrator=ORCHESTRATORS[first]())
    assert (ran.nodes["f"].computed, ran.nodes["f"].failed) == (rows, 0)
    cast("Any", job.nodes["f"]).clear_cache()
    again = job.run(orchestrator=ORCHESTRATORS[second]())
    assert (again.nodes["f"].reused, again.nodes["f"].computed) == (rows, 0)
    assert again.nodes["f"].failed == 0


# ---------------------------------------------------------------------------
# 2. The sync call sites of the guards against a null inner list
# ---------------------------------------------------------------------------

KEY_OF_LISTS = pa.large_list(pa.large_list(pa.int64()))
KEYS = [[[1], [2]], [[3], [4]]]
WHOLE = [[["p"], ["a"]], [["q"], ["b"]]]
HOLED = [None, ["a"]]


class _Tampered(ArrowTableStream):
    """A stream that hands out a token no stream can hold: its table's
    ``_source_k`` is replaced, after construction, by a null inner list."""

    def as_table(self, *args: Any, **kwargs: Any) -> pa.Table:
        table = super().as_table(*args, **kwargs)
        if "_source_k" not in table.column_names or not table.num_rows:
            return table
        index = table.schema.get_field_index("_source_k")
        return table.set_column(
            index,
            table.schema.field(index),
            pa.array([HOLED] * table.num_rows, NESTED),
        )


def _tampered(stream: ArrowTableStream) -> ArrowTableStream:
    cast("Any", stream).__class__ = _Tampered
    return stream


@pytest.mark.parametrize("side", [0, 1], ids=["first", "second"])
@pytest.mark.parametrize("tags", [("k", "g"), ("g", "k")], ids=["k_g", "g_k"])
def test_merge_join_s_sync_call_guards_against_a_null_inner_list(
    side: int, tags: tuple[str, ...]
) -> None:
    """``MergeJoin`` called on streams: no stream can hold a null inner list, so
    one input is made to hand out such a token. The sync merge refuses, naming
    the operator and the token column (fourth review of PR #601, finding 2)."""
    streams = [
        _keyed(("x", "w"), KEYS, WHOLE, NESTED, tags, key_type=KEY_OF_LISTS),
        _keyed(("y", "v"), KEYS, WHOLE, NESTED, tags, key_type=KEY_OF_LISTS),
    ]
    streams[side] = _tampered(streams[side])
    with pytest.raises(InputValidationError) as refused:
        _ = MergeJoin()(*streams).as_table()
    message = str(refused.value)
    assert message.startswith("MergeJoin") and "'_source_k'" in message


@pytest.mark.parametrize("layout", ["tags_first", "reversed"])
def test_index_s_sync_call_guards_against_a_null_element(layout: str) -> None:
    """``Index`` called on a stream whose row hands out a null inner list (no
    stream holds one): taking that element would give a null token whose type is
    a list, and the sync path refuses, naming the operator and the column. (With
    ``out`` the row keeps ``x`` and its token, and the result stream is refused
    where it is built.)"""
    data_type = pa.large_list(pa.large_list(pa.int64()))
    columns: dict[str, Any] = {
        "g": pa.array(["m", "n"], STR),
        "k": pa.array([1, 2], pa.int64()),
        "x": pa.array([[[1], [2]], [[3], [4]]], data_type),
        "w": pa.array([10, 20], pa.int64()),
        "_source_x": pa.array(WHOLE, NESTED),
    }
    names = list(columns) if layout == "tags_first" else list(reversed(columns))
    table = pa.table({name: columns[name] for name in names})
    table = table.cast(pa.schema([f.with_nullable(False) for f in table.schema]))

    class Handing(ArrowTableStream):
        def iter_data(self) -> Any:
            for tag, data in super().iter_data():
                yield tag, data.with_source_info(x=pa.scalar(HOLED, type=NESTED))

    stream = Handing(table, tag_columns=["g", "k"])
    with pytest.raises(InputValidationError) as refused:
        _ = Index("x", 0)(stream).as_table()
    message = str(refused.value)
    assert message.startswith("Index") and "'_source_x'" in message
    taken = Index("x", 1)(stream).as_table(columns={"source": True})
    assert taken.column("_source_x").to_pylist() == [["a"], ["a"]]
