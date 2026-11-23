"""A contradictory condition is kept and keeps zero rows (developer, 2026-11-22; ITL-1374).

"If there is no straightforward way to achieve, then the contradictory case should just
be taken as is and result in 0 rows." On a tag, a data column and a data-tag, with
several terms in several orders, on the sync and the async path, and through a saved job.
"""

from __future__ import annotations

import itertools
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, col
from orcapod.core.operators import Filter
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase
from orcapod.types import CacheMode, Condition

from .several_items import async_rows

pytestmark = pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")


def _units(tag_only_columns: Any = True) -> ArrowTableSource:
    """Tags ``session`` and ``unit``; data ``t`` and ``probe``. With
    ``tag_only_columns=["session"]``, ``unit`` is a data-tag."""
    table = pa.table(
        {
            "session": pa.array(["s1", "s1", "s2", "s2"], pa.large_string()),
            "unit": pa.array([3, 7, 3, 7], pa.int64()),
            "t": pa.array([0.5, 1.5, 2.5, 3.5], pa.float64()),
            "probe": pa.array(["imec0", "imec1", "imec0", "imec1"], pa.large_string()),
        }
    )
    return ArrowTableSource(
        table,
        tag_columns=["session", "unit"],
        tag_only_columns=tag_only_columns,
        source_id="units",
    )


# (role, source options, the column, its two values)
_ROLES = [
    ("tag", True, "session", ("s1", "s2")),
    ("data", True, "probe", ("imec0", "imec1")),
    ("data-tag", ["session"], "unit", (3, 7)),
]


def _terms(column: str, values: tuple[object, object]) -> list[Condition]:
    """Two equalities that contradict each other, and a term every row satisfies."""
    return [col(column) == values[0], col(column) == values[1], col("t") >= 0.0]


@pytest.mark.parametrize(
    "role, options, column, values", _ROLES, ids=[r[0] for r in _ROLES]
)
class TestZeroRows:
    def test_every_order_builds_one_operator_and_keeps_no_row(
        self, role, options, column, values
    ):
        source = _units(options)
        hashes = set()
        for order in itertools.permutations(_terms(column, values)):
            filtered = Filter(Condition.of(*order))(source)
            table = filtered.as_table()
            assert table.num_rows == 0
            assert table.schema == source.as_table().schema
            hashes.add(filtered.pipeline_hash().to_string())
        assert len(hashes) == 1

    def test_the_and_spelling_and_the_method_agree(self, role, options, column, values):
        source = _units(options)
        first, second, every = _terms(column, values)
        by_and = Filter(first & every & second)(source)
        by_method = source.filter(second, every, first)
        assert by_and.as_table().num_rows == by_method.as_table().num_rows == 0
        assert by_and.pipeline_hash() == by_method.pipeline_hash()

    def test_async_keeps_no_row(self, role, options, column, values):
        source = _units(options)
        for order in (_terms(column, values), list(reversed(_terms(column, values)))):
            assert async_rows(Filter(Condition.of(*order)), source) == []

    def test_each_half_alone_keeps_rows(self, role, options, column, values):
        """The zero rows come from the contradiction, not from either term."""
        source = _units(options)
        first, second, every = _terms(column, values)
        assert source.filter(first, every).as_table().num_rows == 2
        assert source.filter(second, every).as_table().num_rows == 2


@pytest.mark.parametrize(
    "condition",
    [
        Condition.of(col("unit").is_in([3]), col("unit").is_in([7])),
        Condition.of(col("t") > 3.0, col("t") < 1.0),
        Condition.of(col("probe") == "imec0", col("probe").is_null()),
        Condition.of(col("unit") == 3, col("unit") > 5),
    ],
    ids=["disjoint is_in", "empty range", "value and null", "value and comparison"],
)
def test_other_contradictions_keep_no_row(condition):
    source = _units()
    assert Filter(condition)(source).as_table().num_rows == 0
    assert async_rows(Filter(condition), source) == []


def test_a_saved_job_keeps_the_contradiction(tmp_path: Path):
    """Saved and loaded, the contradiction is the same condition and its node stored
    zero rows."""
    contradiction = Condition.of(col("session") == "s2", col("t") >= 0.0, session="s1")
    database = DeltaTableDatabase(tmp_path / "store")
    job = PipelineJob(name="contradiction", database=database)
    with job:
        _units().filter(contradiction, label="none")
    job.nodes["none"].attach_databases(
        pipeline_database=database.at("contradiction"), cache_mode=CacheMode.LOG
    )
    job.run()
    assert job.nodes["none"].as_table().num_rows == 0

    path = tmp_path / "job.json"
    job.save(path)
    loaded = PipelineJob.load(path, database=DeltaTableDatabase(tmp_path / "store"))
    node, reloaded = job.nodes["none"], loaded.nodes["none"]
    assert isinstance(reloaded.producer, Filter)
    assert reloaded.producer.condition == contradiction
    assert (
        reloaded.producer.condition.canonical_form() == contradiction.canonical_form()
    )
    assert reloaded.pipeline_hash() == node.pipeline_hash()
    assert reloaded.as_source().as_table().num_rows == 0
