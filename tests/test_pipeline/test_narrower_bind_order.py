"""A narrower bind hands on its rows in their order (ITL-1360, L2; Codex on PR #485).

A source slot declared from a source whose ``x`` is ``int | None``, bound to the same
columns with ``x: int`` (a narrower source, given as the declared schema), feeding a
``Batch`` node in ``LOG`` mode. The source node's output is F of the bound table with
the keys its rows have (``SourceJobNode._row_keys``: ``_DeclaredForm.apply`` of a
row's tag and data tables, as each row is given). Every pairing of declared layout, tag
order and data-tag roles must keep and store what the row path keeps and stores: the
same run with the source's output built from its rows
(``StaticOutputOperatorPod._materialize_upstream``), as on ``main``. The arrangements
where the row path raises (one data-tag of two, ITL-1366) are spec row e10, tested in
``test_declared_slot_one_data_tag.py``.
"""

from __future__ import annotations

import itertools
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.core.nodes.source_node import SourceNodeBase
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.operators import Batch
from orcapod.sources import ArrowTableSource
from orcapod.types import CacheMode

ROLES: dict[str, bool | tuple[str, ...]] = {
    "no_data_tag": True,
    "a_data_tag": ("b",),
    "b_data_tag": ("a",),
    "both_data_tags": (),
}


def _source(
    nullable_x: bool, layout: tuple[str, ...], tag_order: tuple[str, str], roles: Any
) -> ArrowTableSource:
    columns = {
        "x": pa.array([1, 2], pa.int64()),
        "a": pa.array([5, 6], pa.int64()),
        "b": pa.array([3, 4], pa.int64()),
    }
    table = pa.table({name: columns[name] for name in layout})
    table = table.cast(
        pa.schema([f.with_nullable(f.name == "x" and nullable_x) for f in table.schema])
    )
    return ArrowTableSource(
        table, tag_columns=list(tag_order), tag_only_columns=roles, source_id="s"
    )


def _run(
    layout: tuple[str, ...], tag_order: tuple[str, str], roles: Any
) -> tuple[list[str], pa.Schema]:
    """Run the job; return the batch node's kept columns and its raw stored schema."""
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="narrower_bind_order", database=database)
    with job:
        Batch(batch_size=2)(_source(True, layout, tag_order, roles), label="batched")
    job.nodes["batched"].attach_databases(
        pipeline_database=database.at("log"), cache_mode=CacheMode.LOG
    )
    slot = next(label for label in job.nodes if label != "batched")
    job.bind(sources={slot: _source(False, ("x", "a", "b"), tag_order, roles)})
    job.run()
    node = job.nodes["batched"]
    kept = node.as_table(columns={"source": True}).column_names
    stored = node.get_all_records(columns={"source": True})
    assert stored is not None
    return kept, stored.schema


def _row_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every source node hand on its rows rebuilt, as on ``main``."""

    def execute(self: Any, *, observer: Any = None) -> Any:
        if observer is not None:
            observer.on_node_start(self.label, self.content_hash().to_string())
        out = StaticOutputOperatorPod._materialize_upstream(
            list(self.iter_data()), self
        )  # pyright: ignore[reportPrivateUsage]  # the row path
        if observer is not None:
            observer.on_node_end(self.label, self.content_hash().to_string())
        return out

    monkeypatch.setattr(SourceNodeBase, "execute", execute)


def _raises_on_the_row_path(roles: str) -> bool:
    """The row path raises only with one data-tag of two (ITL-1366, e10)."""
    return roles in ("a_data_tag", "b_data_tag")


@pytest.mark.parametrize("roles", list(ROLES), ids=list(ROLES))
@pytest.mark.parametrize("tag_order", [("a", "b"), ("b", "a")], ids=["a-b", "b-a"])
@pytest.mark.parametrize(
    "layout", list(itertools.permutations(("a", "b", "x"))), ids="".join
)
def test_a_narrower_bind_keeps_and_stores_as_the_row_path(
    layout: tuple[str, ...],
    tag_order: tuple[str, str],
    roles: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Kept columns and the raw stored schema equal the row path's, in order."""
    got = _run(layout, tag_order, ROLES[roles])
    with monkeypatch.context() as patch:
        _row_path(patch)
        try:
            expected = _run(layout, tag_order, ROLES[roles])
        except ValueError as error:
            assert "Duplicate column name" in str(error)
            assert _raises_on_the_row_path(roles)
            pytest.skip("the row path raises here: spec row e10")
    assert got[0] == expected[0]
    assert got[1].equals(expected[1], check_metadata=True)
