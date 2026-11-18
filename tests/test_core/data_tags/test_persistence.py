"""A stored table states which of its tags are data-tags (ITL-1019).

Developer decision, November 16, 2026: the stored table fully expresses what it stores. A
stored tag column that is a data-tag carries Arrow field metadata
(``data_tags.TAG_KIND_METADATA_KEY``); a plain tag carries none. A function
node marks the data-tags of its pipeline (tag) table, reads the binding back from
that table, and refuses a table that disagrees with its pod. The result table never records
data-tag status. The marker moves no hash.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.contexts import resolve_context
from orcapod.core.data_tags import (
    TAG_KIND_METADATA_KEY,
    DATA_TAG_KIND,
    mark_data_tags,
    stored_data_tags,
    without_tag_kind_marks,
)
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

CALLS: list[float] = []

ROWS = [
    {"unit": 1, "time": 10, "amp": 0.5},
    {"unit": 2, "time": 20, "amp": 1.5},
]
MORE_ROWS = [*ROWS, {"unit": 3, "time": 30, "amp": 2.5}]


@op.function_pod(output_keys=["y"])
def double(amp: float) -> float:
    """Double the amplitude; reads no tag."""
    CALLS.append(amp)
    return 2 * amp


def _source(
    rows: list[dict[str, float]], tag_only: object = ("time",)
) -> op.sources.DictSource:
    """``unit`` a data-tag and ``time`` plain unless ``tag_only`` says otherwise."""
    return op.sources.DictSource(
        rows,
        tag_columns=["unit", "time"],
        tag_only_columns=tag_only,  # pyright: ignore[reportArgumentType]  # the test varies it
        source_id="spikes",
    )


def _in_memory(_: Path) -> ArrowDatabaseProtocol:
    return InMemoryArrowDatabase()


def _delta(tmp_path: Path) -> ArrowDatabaseProtocol:
    return DeltaTableDatabase(tmp_path / "store")


STORES = pytest.mark.parametrize(
    "make_store", [_in_memory, _delta], ids=["memory", "delta"]
)
ORCHESTRATORS = pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)


def _run(
    store: ArrowDatabaseProtocol,
    rows: list[dict[str, float]],
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
    tag_only: object = ("time",),
    load_only: bool = False,
) -> PipelineJob:
    job = PipelineJob(database=store)
    with job:
        double.pod(_source(rows, tag_only), label="double")
    job.run(orchestrator=orchestrator(), load_only=load_only)
    return job


def _tables(store: ArrowDatabaseProtocol) -> dict[str, pa.Table]:
    """Return the stored tables by kind: ``pipeline`` (tag) and ``result``."""
    tables: dict[str, pa.Table] = {}
    for path in store.list_table_paths():
        table = store.get_all_records(path)
        assert table is not None
        kind = "result" if "_result" in path else "pipeline"
        assert kind not in tables, f"more than one {kind} table: {path}"
        tables[kind] = table
    return tables


def _marked(schema: pa.Schema) -> list[str]:
    """The columns carrying the marker key, whatever its value."""
    return [f.name for f in schema if TAG_KIND_METADATA_KEY in (f.metadata or {})]


# ---------------------------------------------------------------------------
# The marker itself
# ---------------------------------------------------------------------------


def test_mark_data_tags_marks_exactly_the_named_columns() -> None:
    stray = pa.field("b", pa.int64(), metadata={TAG_KIND_METADATA_KEY: b"x"})
    table = pa.Table.from_arrays(
        [pa.array([1]), pa.array([2]), pa.array(["c"])],
        schema=pa.schema(
            [
                pa.field("a", pa.int64(), nullable=False),
                stray,
                pa.field("c", pa.string()),
            ]
        ),
    )
    marked = mark_data_tags(table, ["a"])
    # A marker that came in on another column is not this writer's binding.
    assert _marked(marked.schema) == ["a"]
    assert marked.schema.field("a").metadata == {TAG_KIND_METADATA_KEY: DATA_TAG_KIND}
    assert marked.schema.field("b").metadata is None
    # Values, types and nullability are untouched.
    assert marked.schema.field("a").nullable is False
    assert marked.equals(table)
    assert stored_data_tags(marked.schema) == ("a",)
    # The read side drops every marker and gives back the unmarked schema.
    assert without_tag_kind_marks(marked).schema.equals(
        mark_data_tags(table, []).schema, check_metadata=True
    )


def test_mark_data_tags_keeps_other_field_metadata() -> None:
    field = pa.field("a", pa.int64(), metadata={b"other": b"kept"})
    table = pa.Table.from_arrays([pa.array([1])], schema=pa.schema([field]))
    marked = mark_data_tags(table, ["a"])
    assert marked.schema.field("a").metadata == {
        b"other": b"kept",
        TAG_KIND_METADATA_KEY: DATA_TAG_KIND,
    }
    assert without_tag_kind_marks(marked).schema.field("a").metadata == {
        b"other": b"kept"
    }


def test_an_unmarked_table_is_returned_as_it_is() -> None:
    table = pa.table({"a": [1]})
    assert mark_data_tags(table, []) is table


def test_marking_a_column_the_table_lacks_raises() -> None:
    with pytest.raises(ValueError, match="not columns of the stored table"):
        mark_data_tags(pa.table({"a": [1]}), ["b"])


def test_a_marker_with_another_value_is_refused_on_read() -> None:
    field = pa.field("a", pa.int64(), metadata={TAG_KIND_METADATA_KEY: b"plain"})
    with pytest.raises(ValueError, match="only valid value"):
        stored_data_tags(pa.schema([field]))


def test_the_marker_moves_no_table_hash() -> None:
    """Audit probe 6: the table hash ignores the marker (no golden value can move)."""
    hasher = resolve_context(None).arrow_hasher
    table = pa.table({"unit": [1, 2], "y": [0.5, 1.5]})
    marked = mark_data_tags(table, ["unit"])
    assert marked.schema.field("unit").metadata
    assert hasher.hash_table(marked) == hasher.hash_table(table)


# ---------------------------------------------------------------------------
# A function node's pipeline table
# ---------------------------------------------------------------------------


@STORES
@ORCHESTRATORS
def test_the_pipeline_table_marks_only_the_data_tags(
    tmp_path: Path,
    make_store: Callable[[Path], ArrowDatabaseProtocol],
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
) -> None:
    store = make_store(tmp_path)
    _run(store, ROWS, orchestrator)
    tables = _tables(store)
    pipeline = tables["pipeline"]
    # ``unit`` is a data-tag and is marked; ``time`` is plain and carries nothing.
    assert _marked(pipeline.schema) == ["unit"]
    assert TAG_KIND_METADATA_KEY not in (pipeline.schema.field("time").metadata or {})
    assert stored_data_tags(pipeline.schema) == ("unit",)
    # The result table never records data-tag status: no marker, no tag column.
    assert _marked(tables["result"].schema) == []
    assert "unit" not in tables["result"].column_names


@STORES
@ORCHESTRATORS
def test_a_plain_pipeline_table_carries_no_marker(
    tmp_path: Path,
    make_store: Callable[[Path], ArrowDatabaseProtocol],
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
) -> None:
    store = make_store(tmp_path)
    _run(store, ROWS, orchestrator, tag_only=True)
    for table in _tables(store).values():
        assert _marked(table.schema) == []


@STORES
@ORCHESTRATORS
def test_the_marker_survives_an_append(
    tmp_path: Path,
    make_store: Callable[[Path], ArrowDatabaseProtocol],
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
) -> None:
    store = make_store(tmp_path)
    _run(store, ROWS, orchestrator)
    # More rows of the same pipeline: the same table, appended to.
    CALLS.clear()
    _run(store, MORE_ROWS, orchestrator)
    assert CALLS == [2.5]
    pipeline = _tables(store)["pipeline"]
    assert pipeline.num_rows == 3
    assert stored_data_tags(pipeline.schema) == ("unit",)
    # A fresh handle on the same store reads the marker back from storage.
    if isinstance(store, DeltaTableDatabase):
        assert stored_data_tags(
            _tables(DeltaTableDatabase(tmp_path / "store"))["pipeline"].schema
        ) == ("unit",)


@STORES
@ORCHESTRATORS
def test_the_binding_is_read_back_from_the_table(
    tmp_path: Path,
    make_store: Callable[[Path], ArrowDatabaseProtocol],
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
) -> None:
    store = make_store(tmp_path)
    first = _run(store, ROWS, orchestrator).nodes["double"]
    CALLS.clear()
    # A load-only run reads every row from the store.
    loaded = _run(store, ROWS, orchestrator, load_only=True).nodes["double"]
    assert CALLS == []
    assert loaded.keys() == first.keys() == (("unit", "time"), ("unit", "y"))
    table = loaded.as_table()
    assert table.column_names.count("unit") == 1
    # The marker belongs to the stored table, not to the stream read from it.
    assert _marked(table.schema) == []
    assert sorted(table.to_pylist(), key=lambda r: r["unit"]) == sorted(
        first.as_table().to_pylist(), key=lambda r: r["unit"]
    )
    for tag, data in loaded.iter_data():
        assert tag["unit"] == data["unit"]


@ORCHESTRATORS
def test_a_table_that_disagrees_with_the_pod_is_refused(
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
) -> None:
    written = InMemoryArrowDatabase()
    _run(written, ROWS, orchestrator)
    # The same tables with the marker removed: as if another pipeline wrote them.
    tampered = InMemoryArrowDatabase()
    for path in written.list_table_paths():
        id_column = "__tampered_id"
        table = written.get_all_records(path, record_id_column=id_column)
        assert table is not None
        tampered.add_records(
            path, without_tag_kind_marks(table), record_id_column=id_column, flush=True
        )
    with pytest.raises(InputValidationError, match=r"marks the data-tags \[\]"):
        _run(tampered, ROWS, orchestrator, load_only=True)


# ---------------------------------------------------------------------------
# Every write path of a store keeps the marker
# ---------------------------------------------------------------------------


def _rows(ids: list[int], extra: bool = False) -> pa.Table:
    """Marked rows keyed by a binary record id; ``extra`` adds a column."""
    columns: dict[str, pa.Array[Any]] = {
        "rid": pa.array([str(i).encode() for i in ids], pa.large_binary()),
        "unit": pa.array(ids, pa.int64()),
        "y": pa.array([float(i) for i in ids], pa.float64()),
    }
    if extra:
        columns["z"] = pa.array(ids, pa.int64())
    return mark_data_tags(pa.Table.from_pydict(columns), ["unit"])


@pytest.mark.parametrize(
    "make_store",
    [
        _in_memory,
        lambda tmp_path: DeltaTableDatabase(
            tmp_path / "store", allow_schema_evolution=True
        ),
    ],
    ids=["memory", "delta"],
)
def test_every_write_path_keeps_the_marker(
    tmp_path: Path, make_store: Callable[[Path], ArrowDatabaseProtocol]
) -> None:
    store = make_store(tmp_path)
    path = ("node", "table")

    def data_tags() -> tuple[str, ...]:
        table = store.get_all_records(path)
        assert table is not None
        return stored_data_tags(table.schema)

    store.add_records(path, _rows([1, 2]), record_id_column="rid", flush=True)
    assert data_tags() == ("unit",)  # first write
    store.add_records(path, _rows([3]), record_id_column="rid")
    assert data_tags() == ("unit",)  # stored rows read together with pending ones
    store.flush()
    assert data_tags() == ("unit",)  # append
    store.add_records(
        path, _rows([3, 4]), record_id_column="rid", skip_duplicates=True, flush=True
    )
    assert data_tags() == ("unit",)  # some ids stored already: a merge
    unit = store.get_records_with_column_value(path, {"unit": 1})
    assert unit is not None and stored_data_tags(unit.schema) == ("unit",)
    if isinstance(store, DeltaTableDatabase):
        # A new column: the stored schema evolves (in-memory tables do not evolve).
        store.add_records(
            path, _rows([5], extra=True), record_id_column="rid", flush=True
        )
        assert data_tags() == ("unit",)
        reopened = DeltaTableDatabase(tmp_path / "store")
        table = reopened.get_all_records(path)
        assert table is not None and stored_data_tags(table.schema) == ("unit",)


# ---------------------------------------------------------------------------
# A database that cannot keep the marker is refused before anything is written
# ---------------------------------------------------------------------------


def test_a_database_without_field_metadata_is_refused_before_any_write() -> None:
    from orcapod.databases.connector_arrow_database import ConnectorArrowDatabase
    from orcapod.databases.sqlite_connector import SQLiteConnector

    connector = SQLiteConnector(":memory:")
    database = ConnectorArrowDatabase(connector)
    CALLS.clear()
    with pytest.raises(InputValidationError) as raised:
        _run(database, ROWS, SyncPipelineOrchestrator)
    message = str(raised.value)
    # The database kind and both ways out are named.
    assert "SQLiteConnector does not preserve Arrow metadata" in message
    assert "keeps field metadata" in message and "tag_only_columns" in message
    assert CALLS == []
    assert database.list_table_paths() == []


def test_plain_tags_ask_nothing_of_the_database() -> None:
    from orcapod.core.data_tags import require_database_keeps_marker
    from orcapod.databases.connector_arrow_database import ConnectorArrowDatabase
    from orcapod.databases.sqlite_connector import SQLiteConnector

    database = ConnectorArrowDatabase(SQLiteConnector(":memory:"))
    require_database_keeps_marker(database, ())
    with pytest.raises(InputValidationError, match="cannot store the data-tags"):
        require_database_keeps_marker(database, ["unit"])
    # A database that keeps field metadata is accepted.
    require_database_keeps_marker(InMemoryArrowDatabase(), ["unit"])
