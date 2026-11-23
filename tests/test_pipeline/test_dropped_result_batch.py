"""A result batch the store refuses at a flush leaves no pipeline record behind.

Review of PR 504 (second round, N1): when ``DeltaTableDatabase`` drops a result batch
at the flush (names Delta folds alike only by its own case folding, U+1C89 and U+1C8A),
the job's failure-time flush still committed the node's pipeline records, which then
name results that were never stored. The invariant (no pipeline record without its
result) is ITL-1412's (on main, PR 509): the flush's refusal goes through the same
pending-commit path, so the records waiting on the dropped batch are withdrawn.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase
from orcapod.system_constants import constants

UPPER, LOWER = "Ᲊ", "ᲊ"


@function_pod(output_keys=[UPPER, LOWER])
def _pair(x: int | None) -> tuple[int, int]:
    return x or 0, (x or 0) + 10


@pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
def test_no_pipeline_record_names_a_dropped_result(
    tmp_path: Path, use_async: bool
) -> None:
    source = ArrowTableSource(
        pa.table({"k": [1, 2], "x": [3, 4]}),
        tag_columns=["k"],
        tag_only_columns=True,
        record_id_columns=["k"],
        source_id="s",
    )
    database = DeltaTableDatabase(tmp_path)
    job = PipelineJob(name="itl1403", database=database)
    with job:
        _pair.pod(source, label="pair")
    with pytest.raises(Exception):  # noqa: B017  # the run fails: the result batch is dropped
        if use_async:
            asyncio.run(job.run_async())
        else:
            job.run()
    fresh = DeltaTableDatabase(tmp_path)
    referenced: list[object] = []
    for path in fresh.list_table_paths():
        table = fresh.get_all_records(path, record_id_column="__record_id")
        if table is not None and constants.DATA_RECORD_ID in table.column_names:
            referenced += table[constants.DATA_RECORD_ID].to_pylist()
    assert [r for r in referenced if r is not None] == []


@function_pod(output_keys=["doubled"])
def _double(x: int | None) -> int:
    return 2 * (x or 0)


@pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
def test_a_result_batch_delta_refuses_withdraws_its_pipeline_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, use_async: bool
) -> None:
    """A result batch the commit refuses permanently (deltalake's own
    ``SchemaMismatchError`` on the result table): the flush raises it once
    (``InputValidationError``, the job's ``OrcapodDatabaseWriteError`` cause), the batch
    leaves the queue, and the pipeline records waiting on it are never published."""
    import deltalake
    from deltalake.exceptions import SchemaMismatchError

    from orcapod.errors import InputValidationError, OrcapodDatabaseWriteError

    real_write = deltalake.write_deltalake

    def refuse_results(table_or_uri: Any, *args: Any, **kwargs: Any) -> None:
        if "_result" in str(table_or_uri):
            raise SchemaMismatchError("Schema error: refused for the test")
        real_write(table_or_uri, *args, **kwargs)

    monkeypatch.setattr(deltalake, "write_deltalake", refuse_results)
    source = ArrowTableSource(
        pa.table({"k": [1, 2], "x": [3, 4]}),
        tag_columns=["k"],
        tag_only_columns=True,
        record_id_columns=["k"],
        source_id="s",
    )
    database = DeltaTableDatabase(tmp_path)
    job = PipelineJob(name="itl1403", database=database)
    with job:
        _double.pod(source, label="double")
    with pytest.raises(OrcapodDatabaseWriteError) as info:
        if use_async:
            asyncio.run(job.run_async())
        else:
            job.run()
    assert isinstance(info.value.__cause__, InputValidationError)
    assert "Delta refused the commit" in str(info.value.__cause__)
    fresh = DeltaTableDatabase(tmp_path)
    referenced: list[object] = []
    for path in fresh.list_table_paths():
        table = fresh.get_all_records(path, record_id_column="__record_id")
        if table is not None and constants.DATA_RECORD_ID in table.column_names:
            referenced += table[constants.DATA_RECORD_ID].to_pylist()
    assert [r for r in referenced if r is not None] == []
