"""Sources present large Arrow types unless the user chose the types (ITL-1122).

The rule (developer, November 14, 2026): every Arrow table Orcapod creates has its string,
binary and list types in their large variants. Two exceptions keep the types as given: an
Arrow table (or Arrow schema) the user made, and a store that keeps the large / non-large
distinction in a recoverable way.

* ``CSVSource`` reads a file that has no such distinction, so its table is Orcapod's: large.
  An Arrow ``schema=`` given by the user is kept.
* ``CachedSource`` presents the rows it read from its cache store, a table Orcapod creates:
  large whatever the store (in memory or Delta) and whether it has rows or not.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pa_csv
import pytest

from orcapod.core.sources import ArrowTableSource, CachedSource, CSVSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.operators import SelectTagColumns
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import SourceProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
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
        pa.field("s", pa.string(), nullable=False),
        pa.field("b", pa.binary(), nullable=False),
        pa.field("l", pa.list_(pa.string()), nullable=False),
        pa.field("n", NESTED, nullable=False),
    ]
)
ROWS = [
    {"t": "a", "s": "x", "b": b"1", "l": ["p"], "n": {"x": ["q"], "y": "r"}},
    {"t": "b", "s": "y", "b": b"2", "l": [], "n": {"x": [], "y": "s"}},
]


def _non_large(table: pa.Table) -> dict[str, pa.DataType]:
    """The columns of ``table`` whose type is not its large-normalized form."""
    return {
        field.name: field.type
        for field in table.schema
        if arrow_utils.normalize_to_large_types(field.type, keep_null=True)
        != field.type
    }


def _small_source(n_rows: int, source_id: str = "small") -> ArrowTableSource:
    """A user's Arrow table with string, binary, list and nested columns."""
    return ArrowTableSource(
        pa.Table.from_pylist(ROWS[:n_rows], schema=SMALL_SCHEMA),
        tag_columns=["t"],
        source_id=source_id,
    )


def _source_node_table(source: SourceProtocol, orchestrator: str) -> pa.Table:
    """The table of ``source``'s node in a job run on ``orchestrator``."""
    job = PipelineJob(name="itl1122", database=InMemoryArrowDatabase())
    with job:
        SelectTagColumns(["t"])(source, label="selected")
    job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    # The job's two nodes: the source's node and ``selected``.
    (source_label,) = [label for label in job.nodes if label != "selected"]
    return job.nodes[source_label].as_table(columns=ALL_COLUMNS)


# ---------------------------------------------------------------------------
# CSVSource
# ---------------------------------------------------------------------------


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "rows.csv"
    pa_csv.write_csv(
        pa.table({"t": ["a", "b"], "k": [0, 1], "s": ["x", "y"]}),
        path,
    )
    return path


class TestCSVSource:
    def test_table_is_large(self, csv_path: Path) -> None:
        source = CSVSource(str(csv_path), tag_columns=["t"], source_id="csv")
        table = source.as_table(columns=ALL_COLUMNS)
        assert _non_large(table) == {}
        assert table.schema.field("s").type == pa.large_string()

    @pytest.mark.parametrize("orchestrator", ["sync", "async"])
    def test_source_node_in_a_job_is_large(
        self, csv_path: Path, orchestrator: str
    ) -> None:
        source = CSVSource(str(csv_path), tag_columns=["t"], source_id="csv")
        assert _non_large(_source_node_table(source, orchestrator)) == {}

    def test_explicit_arrow_schema_is_kept(self, csv_path: Path) -> None:
        """The user chose the Arrow types: ``string`` stays ``string``."""
        schema = pa.schema(
            [
                pa.field("t", pa.string(), nullable=False),
                pa.field("k", pa.int64(), nullable=False),
                pa.field("s", pa.string(), nullable=False),
            ]
        )
        source = CSVSource(
            str(csv_path), tag_columns=["t"], source_id="csv", schema=schema
        )
        table = source.as_table()
        assert table.schema.field("t").type == pa.string()
        assert table.schema.field("s").type == pa.string()

    def test_row_hashes_equal_the_explicit_string_schema(self, csv_path: Path) -> None:
        """Large and non-large variants hash identically: no row hash moves."""
        schema = pa.schema(
            [
                pa.field("t", pa.string(), nullable=False),
                pa.field("k", pa.int64(), nullable=False),
                pa.field("s", pa.string(), nullable=False),
            ]
        )
        large = CSVSource(str(csv_path), tag_columns=["t"], source_id="csv")
        small = CSVSource(
            str(csv_path), tag_columns=["t"], source_id="csv", schema=schema
        )
        assert large.content_hash() == small.content_hash()
        assert large.pipeline_hash() == small.pipeline_hash()
        large_rows = [
            (tag.content_hash(), data.content_hash()) for tag, data in large.iter_data()
        ]
        small_rows = [
            (tag.content_hash(), data.content_hash()) for tag, data in small.iter_data()
        ]
        assert large_rows == small_rows


# ---------------------------------------------------------------------------
# CachedSource
# ---------------------------------------------------------------------------


def _store(kind: str, tmp_path: Path) -> ArrowDatabaseProtocol:
    if kind == "in_memory":
        return InMemoryArrowDatabase()
    return DeltaTableDatabase(base_path=tmp_path / "cache")


@pytest.mark.parametrize("store_kind", ["in_memory", "delta"])
class TestCachedSource:
    @pytest.mark.parametrize("n_rows", [2, 0])
    def test_table_is_large(self, store_kind: str, n_rows: int, tmp_path: Path) -> None:
        cached = CachedSource(
            _small_source(n_rows), cache_database=_store(store_kind, tmp_path)
        )
        table = cached.as_table(columns=ALL_COLUMNS)
        assert table.num_rows == n_rows
        assert _non_large(table) == {}

    def test_zero_row_schema_equals_populated(
        self, store_kind: str, tmp_path: Path
    ) -> None:
        populated = CachedSource(
            _small_source(2), cache_database=_store(store_kind, tmp_path / "a")
        ).as_table(columns=ALL_COLUMNS)
        empty = CachedSource(
            _small_source(0), cache_database=_store(store_kind, tmp_path / "b")
        ).as_table(columns=ALL_COLUMNS)
        user_columns = [field.name for field in SMALL_SCHEMA]
        assert (
            empty.select(user_columns).schema == populated.select(user_columns).schema
        )

    @pytest.mark.parametrize("orchestrator", ["sync", "async"])
    def test_source_node_in_a_job_is_large(
        self, store_kind: str, orchestrator: str, tmp_path: Path
    ) -> None:
        cached = CachedSource(
            _small_source(2),
            cache_database=_store(store_kind, tmp_path),
        )
        assert _non_large(_source_node_table(cached, orchestrator)) == {}

    def test_row_hashes_equal_the_wrapped_source(
        self, store_kind: str, tmp_path: Path
    ) -> None:
        """Large and non-large variants hash identically: no row hash moves."""
        source = _small_source(2)
        cached = CachedSource(source, cache_database=_store(store_kind, tmp_path))
        source_rows = sorted(
            (tag.content_hash().to_hex(), data.content_hash().to_hex())
            for tag, data in source.iter_data()
        )
        cached_rows = sorted(
            (tag.content_hash().to_hex(), data.content_hash().to_hex())
            for tag, data in cached.iter_data()
        )
        assert cached_rows == source_rows

    def test_a_cache_written_before_is_read_and_appended(
        self, store_kind: str, tmp_path: Path
    ) -> None:
        """A second cached source on the same store reads the stored rows and adds its own.

        The cache's write path is unchanged: it stores what the wrapped source gives.
        """
        store = _store(store_kind, tmp_path)
        first = CachedSource(_small_source(1, source_id="small"), cache_database=store)
        assert first.as_table().num_rows == 1
        second = CachedSource(_small_source(2, source_id="small"), cache_database=store)
        table = second.as_table(columns=ALL_COLUMNS)
        assert table.num_rows == 2
        assert _non_large(table) == {}
