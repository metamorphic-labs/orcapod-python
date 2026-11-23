"""Jobs that reach a ``DeltaTableDatabase`` refusal of deltalake 1.5+ (ITL-1403).

Pins the behaviour disclosed in the review of PR 504, so that a change to it is noticed:

- finding 6: an operator whose result TYPE depends on its data (list items nullable only
  when a value exceeds 10) in an operator node that logs (``CacheMode.LOG``): the run
  whose result type differs from the stored one fails at the store, where Delta
  refuses the commit (the flush raises it once), instead of silently changing the
  table's recorded type. Such an operator is a bug of the
  operator; ITL-1410 is to refuse it at the operator.
- a function with output keys ``a`` and ``A`` (legal before) stops at its first write:
  Delta column names are case-insensitive.

Both on the sync and the async orchestrator.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import (
    InputValidationError,
    OrcapodDatabaseWriteError,
    UnstorableColumnError,
)
from orcapod.types import CacheMode


class _ItemNullabilityFromValues(UnaryOperator):
    """Lists whose items' Arrow nullability follows the values (a buggy operator)."""

    def validate_unary_input(self, stream: Any) -> None:
        pass

    def identity_structure(self) -> Any:
        return ("itl1403-item-nullability-from-values",)

    def unary_output_schema(self, stream: Any, **kwargs: Any) -> Any:
        tags, _ = stream.output_schema(**kwargs)
        values = pa.array([], type=pa.large_list(pa.field("item", pa.uint8())))
        _, data = ArrowTableStream(pa.table({"v": values})).output_schema(**kwargs)
        return tags, data

    def unary_static_process(self, stream: Any) -> Any:
        table = stream.as_table(columns={"source": True, "system_tags": True})
        values = table["v"].to_pylist()
        nullable = any(v > 10 for v in values)
        shape = pa.large_list(pa.field("item", pa.uint8(), nullable=nullable))
        wrapped = pa.array([[v] for v in values], shape)
        table = table.set_column(table.schema.get_field_index("v"), "v", wrapped)
        return ArrowTableStream(table, tag_columns=stream.keys()[0])


def _source(keys: list[int], values: list[int]) -> ArrowTableSource:
    return ArrowTableSource(
        pa.table({"k": keys, "v": values}),
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="input",
        record_id_columns=["k"],
    )


def _run(job: PipelineJob, use_async: bool) -> None:
    if use_async:
        asyncio.run(job.run_async())
    else:
        job.run()


@pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
def test_an_operator_whose_result_type_follows_its_data_fails_at_the_store(
    tmp_path: Path, use_async: bool
) -> None:
    def run(keys: list[int], values: list[int]) -> None:
        database = DeltaTableDatabase(tmp_path)
        job = PipelineJob(name="itl1403", database=database)
        with job:
            _ItemNullabilityFromValues()(_source(keys, values), label="shape")
        job.nodes["shape"].attach_databases(database, cache_mode=CacheMode.LOG)
        _run(job, use_async)
        database.flush()

    run([1, 2], [1, 2])  # items non-nullable: stored
    with pytest.raises(OrcapodDatabaseWriteError) as info:
        run([3, 4], [11, 12])  # items nullable: the recorded type would change
    # The flush is the point of truth (ITL-1403): Delta refuses the commit, and the
    # flush raises it once, as the write error's cause; nothing stays queued.
    cause = info.value.__cause__
    assert isinstance(cause, InputValidationError), repr(cause)
    assert "Delta refused the commit" in str(cause)
    with pytest.raises(OrcapodDatabaseWriteError):
        run([3, 4], [11, 12])  # the same rows fail the same way again, never queued


@function_pod(output_keys=["a", "A"])
def _pair(v: int | None) -> tuple[int, int]:
    return v or 0, (v or 0) + 10


@pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
def test_a_function_with_output_keys_differing_in_case_stops(
    tmp_path: Path, use_async: bool
) -> None:
    # Refused when the job attaches the database to the node, before anything runs.
    with pytest.raises(UnstorableColumnError, match="differ only in case"):
        database = DeltaTableDatabase(tmp_path)
        job = PipelineJob(name="itl1403", database=database)
        with job:
            _pair.pod(_source([1, 2], [1, 2]), label="fn")
        _run(job, use_async)
    assert not (tmp_path / "itl1403").exists()
