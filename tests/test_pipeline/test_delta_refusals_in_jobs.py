"""Job-level checks for an unstable operator schema and Delta column names.

The cache refuses a value-derived output type before the write. A function with
output keys ``a`` and ``A`` is refused because Delta names are case-insensitive.
Both cases exercise the sync and async orchestrators (ITL-1403).
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
def test_an_operator_whose_result_type_follows_its_data_fails_before_the_write(
    tmp_path: Path, use_async: bool
) -> None:
    database = DeltaTableDatabase(tmp_path)

    def run(keys: list[int], values: list[int]) -> None:
        job = PipelineJob(name="itl1403", database=database)
        with job:
            _ItemNullabilityFromValues()(_source(keys, values), label="shape")
        job.nodes["shape"].attach_databases(database, cache_mode=CacheMode.LOG)
        _run(job, use_async)
        database.flush()

    run([1, 2], [1, 2])
    [path] = database.list_table_paths()
    with pytest.raises(InputValidationError, match=r"output field 'v'.*nullability"):
        run([3, 4], [11, 12])
    database.flush()
    stored = database.get_all_records(path, record_id_column="_record_hash")
    assert stored is not None
    assert stored.num_rows == 2


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
