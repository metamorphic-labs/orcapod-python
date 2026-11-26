"""A SpiralDB read builds source-info tokens only for a result that holds them (ITL-1464).

The tokens are one string per row and data column, many times the size of the rows. A
read whose table shows no source-info column and no content hash must not build them,
and must return exactly the table the read that builds them returns.

Runs against the disk-backed fake of the SpiralDB client.
"""

from __future__ import annotations

import collections
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.core.sources import spiraldb_table_source as source_module
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.sources import SpiralDBTableSource
from orcapod.system_constants import constants
from orcapod.utils import arrow_utils
from tests.test_databases import fake_spiral_sdk

_PROJECT = "sample"
_KEYS = pa.schema(
    [pa.field("subject_id", pa.string()), pa.field("timestamp", pa.uint64())]
)
_ROWS = 5

# Column configurations whose table holds no token and no row hash.
_WITHOUT_TOKENS: list[dict[str, Any]] = [
    {},
    {"system_tags": True},
    {"context": True},
    {"meta": True},
    {"sort_by_tags": True},
    {"system_tags": True, "context": True, "sort_by_tags": True},
]
# Column configurations whose table holds the tokens or a hash computed from the rows.
_WITH_TOKENS: list[dict[str, Any]] = [
    {"source": True},
    {"content_hash": True},
    {"source": True, "system_tags": True, "sort_by_tags": True},
]


def _rows() -> pa.Table:
    """Five rows with two key columns and two value columns, not in key order."""
    timestamps = [4, 1, 5, 2, 3]
    table = pa.table(
        {
            "subject_id": ["m1", "m1", "m2", "m2", "m1"],
            "timestamp": pa.array(timestamps, pa.uint64()),
            "amplitude": pa.array([t + 0.5 for t in timestamps], pa.float32()),
            "quality": pa.array(["a", None, "c", "d", "e"], pa.string()),
        }
    )
    return table.cast(pa.schema([pa.field(f.name, f.type, True) for f in table.schema]))


@pytest.fixture()
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Provide a fake project holding ``event``, scanned 2 rows per batch."""
    monkeypatch.setattr(fake_spiral_sdk, "scan_batch_rows", 2)
    with fake_spiral_sdk.install(tmp_path / "spiral"):
        made = fake_spiral_sdk.ExternalProject(_PROJECT)
        made.create_table("default.event", key_schema=_KEYS).write(_rows())
        # The same rows with the value columns laid out in the other order.
        made.create_table("default.event_other_layout", key_schema=_KEYS).write(
            _rows().select(["subject_id", "timestamp", "quality", "amplitude"])
        )
        yield made


@pytest.fixture()
def token_builds(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Record the row count of every table the source builds tokens for."""
    calls: list[int] = []
    original = arrow_utils.add_source_info

    def recording(table: pa.Table, *given: Any, **options: Any) -> pa.Table:
        calls.append(table.num_rows)
        return original(table, *given, **options)

    monkeypatch.setattr(arrow_utils, "add_source_info", recording)
    return calls


def _source(tag_only: bool) -> SpiralDBTableSource:
    return SpiralDBTableSource(_PROJECT, "event", tag_only_columns=tag_only)


def _with_tokens_always(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every read build the tokens, as every read did before ITL-1464."""
    monkeypatch.setattr(source_module, "_reads_source_info", lambda config: True)


@pytest.mark.parametrize("tag_only", [False, True], ids=["data_tags", "tag_only"])
@pytest.mark.parametrize("columns", _WITHOUT_TOKENS)
def test_a_table_without_tokens_builds_none_and_equals_the_one_that_did(
    project: Any,
    token_builds: list[int],
    monkeypatch: pytest.MonkeyPatch,
    tag_only: bool,
    columns: dict[str, Any],
) -> None:
    source = _source(tag_only)
    token_builds.clear()

    table = source.as_table(columns=columns)

    assert token_builds == []
    assert table.num_rows == _ROWS
    with monkeypatch.context() as patched:
        _with_tokens_always(patched)
        expected = _source(tag_only).as_table(columns=columns)
    assert sum(token_builds) == _ROWS
    assert table.schema.equals(expected.schema, check_metadata=True)
    assert table.equals(expected)


@pytest.mark.parametrize("tag_only", [False, True], ids=["data_tags", "tag_only"])
@pytest.mark.parametrize("columns", _WITH_TOKENS)
def test_a_table_with_tokens_or_row_hashes_still_builds_them(
    project: Any,
    token_builds: list[int],
    monkeypatch: pytest.MonkeyPatch,
    tag_only: bool,
    columns: dict[str, Any],
) -> None:
    source = _source(tag_only)
    token_builds.clear()

    table = source.as_table(columns=columns)

    assert sum(token_builds) == _ROWS
    with monkeypatch.context() as patched:
        _with_tokens_always(patched)
        expected = _source(tag_only).as_table(columns=columns)
    assert table.schema.equals(expected.schema, check_metadata=True)
    assert table.equals(expected)


@pytest.mark.parametrize("tag_only", [False, True], ids=["data_tags", "tag_only"])
def test_all_info_holds_every_token(
    project: Any, token_builds: list[int], tag_only: bool
) -> None:
    source = _source(tag_only)

    table = source.as_table(all_info=True)

    assert sum(token_builds) == _ROWS
    tokens = [name for name in table.column_names if name.startswith("_source_")]
    # A data-tag has a token; a tag kept tag-only has none.
    expected = {"_source_amplitude", "_source_quality"} | (
        set() if tag_only else {"_source_subject_id", "_source_timestamp"}
    )
    assert set(tokens) == expected
    by_time = dict(
        zip(
            table.column("timestamp").to_pylist(),
            table.column("_source_amplitude").to_pylist(),
            strict=True,
        )
    )
    assert by_time[4] == (
        f'{source.source_id}::{{"subject_id":"m1","timestamp":4}}::amplitude'
    )
    for name in tokens:
        assert table.column(name).null_count == 0


def test_run_and_the_data_frame_build_no_tokens(
    project: Any, token_builds: list[int]
) -> None:
    source = _source(False)
    token_builds.clear()

    view = source.run()
    frame = source.as_polars_df()

    assert token_builds == []
    assert view.total_rows == _ROWS
    assert frame.height == _ROWS
    assert frame.columns == source.as_table().column_names


@pytest.mark.parametrize("tag_only", [False, True], ids=["data_tags", "tag_only"])
def test_an_operator_over_the_source_still_reads_its_tokens(
    project: Any, monkeypatch: pytest.MonkeyPatch, tag_only: bool
) -> None:
    kept = _source(tag_only).drop_nulls(["quality", "amplitude"])

    table = kept.as_table(all_info=True)

    assert table.num_rows == _ROWS - 1
    assert table.column("_source_quality").null_count == 0
    assert table.column("_source_amplitude").null_count == 0
    with monkeypatch.context() as patched:
        _with_tokens_always(patched)
        expected = (
            _source(tag_only)
            .drop_nulls(["amplitude", "quality"])
            .as_table(all_info=True)
        )
    assert table.equals(expected)


def _job_rows(
    table: str,
    tag_columns: list[str],
    tag_only: bool,
    dropped: list[str],
    orchestrator: Any,
) -> pa.Table:
    """Run ``drop_nulls`` over the source as a job's only node; return its rows."""
    source = SpiralDBTableSource(
        _PROJECT, table, tag_columns=tag_columns, tag_only_columns=tag_only
    )
    with PipelineJob(name="tokens", database=InMemoryArrowDatabase()) as job:
        source.drop_nulls(dropped, label="kept")
    job.run(orchestrator=orchestrator)
    return job.nodes["kept"].as_table(all_info=True)


def _row_counts(table: pa.Table) -> collections.Counter[tuple[tuple[str, Any], ...]]:
    """Count each row of a table: a comparison by rows that ignores their order."""
    return collections.Counter(tuple(sorted(row.items())) for row in table.to_pylist())


# The named columns in two orders, as data only and with a tag among them (a data-tag
# or a tag-only tag, by ``tag_only``).
_DROPPED = [
    ["quality", "amplitude"],
    ["amplitude", "quality"],
    ["quality", "subject_id", "amplitude"],
    ["timestamp", "amplitude", "subject_id", "quality"],
]


@pytest.mark.parametrize("table", ["event", "event_other_layout"])
@pytest.mark.parametrize("tag_only", [False, True], ids=["data_tags", "tag_only"])
@pytest.mark.parametrize(
    "tag_columns", [["subject_id", "timestamp"], ["timestamp", "subject_id"]]
)
@pytest.mark.parametrize("dropped", _DROPPED, ids="-".join)
def test_an_operator_in_a_job_reads_the_tokens_on_both_paths(
    project: Any,
    monkeypatch: pytest.MonkeyPatch,
    table: str,
    tag_columns: list[str],
    tag_only: bool,
    dropped: list[str],
) -> None:
    sync = _job_rows(table, tag_columns, tag_only, dropped, None)
    on_async = _job_rows(
        table, tag_columns, tag_only, dropped, AsyncPipelineOrchestrator()
    )

    assert sync.num_rows == _ROWS - 1
    assert on_async.num_rows == _ROWS - 1
    for name in sync.column_names:
        if name.startswith("_source_"):
            assert sync.column(name).null_count == 0
    with monkeypatch.context() as patched:
        _with_tokens_always(patched)
        expected = _job_rows(table, tag_columns, tag_only, dropped, None)
    # The whole table is compared: rows, Arrow schema, system tags, tokens and row
    # hashes. Sync is exact; async rows are compared by count, never by order.
    assert "_content_hash" in expected.column_names
    assert any(
        name.startswith(constants.SYSTEM_TAG_PREFIX) for name in expected.column_names
    )
    assert sync.schema.equals(expected.schema, check_metadata=True)
    assert sync.equals(expected)
    assert on_async.schema.equals(expected.schema, check_metadata=True)
    assert _row_counts(on_async) == _row_counts(expected)
    # The order of the named columns changes nothing.
    again = _job_rows(table, tag_columns, tag_only, dropped[::-1], None)
    assert again.equals(sync)


def test_iteration_still_gives_each_row_its_tokens(project: Any) -> None:
    source = _source(False)

    tokens = {
        tag.as_dict()["timestamp"]: data.source_info()["quality"]
        for tag, data in source.iter_data()
    }

    assert tokens[2] == (
        f'{source.source_id}::{{"subject_id":"m2","timestamp":2}}::quality'
    )
    assert len(tokens) == _ROWS
