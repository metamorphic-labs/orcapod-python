"""Every operator's output takes the large Arrow types, on every path (ITL-1122).

The rule (developer, November 14, 2026): every Arrow table Orcapod creates has its string,
binary and list types in their large variants; only a user's own Arrow table, and a store
that keeps the distinction recoverably, keep a non-large type. An operator's output is a
table Orcapod creates, also when the operator passes its rows through unchanged (a select,
a rename, a filter with nothing to filter): its columns take the large-normalized form of
their input types, on a plain stream as inside a job, sync and async, with rows and without.
Large and non-large variants hash identically, so no hash moves.
"""

from __future__ import annotations

from collections.abc import Callable

import polars as pl
import pyarrow as pa
import pytest

from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.operators import (
    Batch,
    DropColumns,
    DropDataColumns,
    DropNulls,
    DropTagColumns,
    GroupBy,
    Index,
    Join,
    MapData,
    MapTags,
    MergeJoin,
    PolarsFilter,
    SelectColumns,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.utils import arrow_utils

ALL_COLUMNS = {"source": True, "system_tags": True, "context": True}
ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}

NESTED = pa.struct([pa.field("x", pa.list_(pa.string())), pa.field("y", pa.string())])
SMALL_SCHEMA = pa.schema(
    [
        pa.field("t", pa.string(), nullable=False),
        pa.field("k", pa.int64(), nullable=False),
        pa.field("s", pa.string(), nullable=False),
        pa.field("b", pa.binary(), nullable=False),
        pa.field("l", pa.list_(pa.string()), nullable=False),
        pa.field("n", NESTED, nullable=False),
    ]
)
ROWS = [
    {"t": "a", "k": 0, "s": "x", "b": b"1", "l": ["p"], "n": {"x": ["q"], "y": "r"}},
    {"t": "b", "k": 1, "s": "y", "b": b"2", "l": ["p", "q"], "n": {"x": [], "y": "s"}},
]


def _non_large(table: pa.Table) -> dict[str, pa.DataType]:
    """The columns of ``table`` whose type is not its large-normalized form."""
    return {
        field.name: field.type
        for field in table.schema
        if arrow_utils.normalize_to_large_types(field.type, keep_null=True)
        != field.type
    }


def _source(n_rows: int, *, large: bool = False) -> ArrowTableSource:
    """A user's Arrow table: non-large string, binary, list and nested columns.

    With ``large``, the same rows with every type in its large variant.
    """
    schema = (
        arrow_utils.normalize_schema_to_large_types(SMALL_SCHEMA, keep_null=True)
        if large
        else SMALL_SCHEMA
    )
    return ArrowTableSource(
        pa.Table.from_pylist(ROWS[:n_rows], schema=schema),
        tag_columns=["t", "k"],
        source_id="main",
    )


def _other(n_rows: int, data_column: str) -> ArrowTableSource:
    """A second user's table: tag ``t`` and one data column, both ``string``."""
    table = pa.table(
        {
            "t": pa.array(["a", "b"][:n_rows], pa.string()),
            data_column: pa.array(["u", "v"][:n_rows], pa.string()),
        }
    )
    table = table.cast(
        pa.schema([pa.field(field.name, field.type, False) for field in table.schema])
    )
    return ArrowTableSource(table, tag_columns=["t"], source_id=f"other_{data_column}")


Apply = Callable[[StreamProtocol, int, str | None], StreamProtocol]

OPERATORS: dict[str, Apply] = {
    "SelectTagColumns": lambda s, n, label: SelectTagColumns(["t"])(s, label=label),
    "DropTagColumns": lambda s, n, label: DropTagColumns(["k"])(s, label=label),
    "SelectDataColumns": lambda s, n, label: SelectDataColumns(["s", "l"])(
        s, label=label
    ),
    "DropDataColumns": lambda s, n, label: DropDataColumns(["b"])(s, label=label),
    "SelectColumns": lambda s, n, label: SelectColumns(["t", "k", "s", "n"])(
        s, label=label
    ),
    "DropColumns": lambda s, n, label: DropColumns(["b"])(s, label=label),
    "MapTags": lambda s, n, label: MapTags({"t": "t2"})(s, label=label),
    "MapData": lambda s, n, label: MapData({"s": "s2"})(s, label=label),
    "PolarsFilter(predicate)": lambda s, n, label: PolarsFilter(
        predicates=[pl.col("k") >= 0]
    )(s, label=label),
    "PolarsFilter()": lambda s, n, label: PolarsFilter()(s, label=label),
    "DropNulls()": lambda s, n, label: DropNulls()(s, label=label),
    "DropNulls([s])": lambda s, n, label: DropNulls(["s"])(s, label=label),
    "Join": lambda s, n, label: Join()(s, _other(n, "z"), label=label),
    "MergeJoin": lambda s, n, label: MergeJoin()(s, _other(n, "s"), label=label),
    "SemiJoin": lambda s, n, label: SemiJoin()(s, _other(n, "z"), label=label),
    "Batch": lambda s, n, label: Batch(batch_size=2)(s, label=label),
    "GroupBy(t)": lambda s, n, label: GroupBy(by=["t"])(s, label=label),
    "GroupBy(k)": lambda s, n, label: GroupBy(by=["k"])(s, label=label),
    "Index": lambda s, n, label: Index("l", 0)(s, label=label),
}


@pytest.mark.parametrize("name", list(OPERATORS))
class TestEveryOperator:
    @pytest.mark.parametrize("n_rows", [2, 0])
    def test_plain_stream_output_is_large(self, name: str, n_rows: int) -> None:
        out = OPERATORS[name](_source(n_rows), n_rows, None)
        table = out.as_table(columns=ALL_COLUMNS)
        assert _non_large(table) == {}

    def test_zero_row_schema_equals_populated(self, name: str) -> None:
        populated = OPERATORS[name](_source(2), 2, None).as_table(columns=ALL_COLUMNS)
        empty = OPERATORS[name](_source(0), 0, None).as_table(columns=ALL_COLUMNS)
        # The user tag and data columns: which system columns a zero-row result
        # carries is ITL-912's subject, not this rule's.
        user_columns = [name for name in populated.column_names if name[0] != "_"]
        assert (
            empty.select(user_columns).schema == populated.select(user_columns).schema
        )

    @pytest.mark.parametrize("orchestrator", ["sync", "async"])
    def test_job_output_is_large_and_equals_plain(
        self, name: str, orchestrator: str
    ) -> None:
        job = PipelineJob(name="itl1122", database=InMemoryArrowDatabase())
        with job:
            OPERATORS[name](_source(2), 2, "op")
        job.run(orchestrator=ORCHESTRATORS[orchestrator]())
        job_table = job.nodes["op"].as_table()
        plain_table = OPERATORS[name](_source(2), 2, None).as_table()
        assert _non_large(job_table) == {}
        assert job_table.schema == plain_table.schema

    def test_row_hashes_equal_the_large_input(self, name: str) -> None:
        """Large and non-large variants hash identically: no row hash moves."""
        small = OPERATORS[name](_source(2), 2, None)
        large = OPERATORS[name](_source(2, large=True), 2, None)
        assert small.content_hash() == large.content_hash()
        assert small.pipeline_hash() == large.pipeline_hash()

        def row_hashes(stream: StreamProtocol) -> list[tuple[str, str]]:
            return sorted(
                (tag.content_hash().to_hex(), data.content_hash().to_hex())
                for tag, data in stream.iter_data()
            )

        assert row_hashes(small) == row_hashes(large)


class TestJoinNeverPicksASide:
    """A shared tag that is large in one input and ``string`` in the other is large."""

    @pytest.mark.parametrize("n_rows", [2, 0])
    def test_either_argument_order_gives_large(self, n_rows: int) -> None:
        large = _source(n_rows, large=True)
        small = _other(n_rows, "z")
        forward = Join()(large, small).as_table(columns=ALL_COLUMNS)
        backward = Join()(small, large).as_table(columns=ALL_COLUMNS)
        assert forward.schema.field("t").type == pa.large_string()
        assert backward.schema.field("t").type == pa.large_string()
        assert _non_large(forward) == {}
        assert _non_large(backward) == {}
