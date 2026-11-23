"""A result batch the store refuses at a flush leaves no pipeline record behind.

Review of PR 504 (second round, N1): when ``DeltaTableDatabase`` drops a result batch
at the flush (names Delta folds alike only by its own case folding, U+1C89 and U+1C8A),
the job's failure-time flush still committed the node's pipeline records, which then
name results that were never stored. The invariant (no pipeline record without its
result) is fixed for every cause under ITL-1412, which PR 504 waits for.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

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


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="ITL-1412: the failure-time flush commits pipeline records of a dropped result batch",
)
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
