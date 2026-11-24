"""Commit guards across every pipeline store, using storable column names."""

from collections.abc import Callable

import pyarrow as pa
import pytest

from orcapod.errors import InputValidationError
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from tests.pipeline_stores import STORE_KINDS

RESULTS = ("results", "rows")
RECORDS = ("pipeline", "rows")


def _row(value: int) -> pa.Table:
    """Two tags and two data columns, including the referenced result ID."""
    return pa.table(
        {
            "group": pa.array(["a"], type=pa.large_string()),
            "tag": [value],
            "value": [value * 2],
            "result": pa.array([bytes([value])], type=pa.large_binary()),
        }
    )


def _enqueue(
    results: ArrowDatabaseProtocol, pipeline: ArrowDatabaseProtocol, value: int
) -> None:
    """Stage a result and a record that may publish only after it commits."""
    result_id = bytes([value])
    row = _row(value)
    results.add_record(RESULTS, result_id, row)
    pending = results.pending_result(RESULTS, result_id)
    assert pending is not None
    with pipeline.guard_record(RECORDS, result_id, pending):
        pipeline.add_record(RECORDS, result_id, row)


@pytest.mark.parametrize("pipeline_store_factory", STORE_KINDS, indirect=True)
@pytest.mark.parametrize("separate", [False, True], ids=["one-store", "two-stores"])
def test_each_batch_publishes_only_references_to_committed_results(
    pipeline_store_factory: Callable[[str], ArrowDatabaseProtocol], separate: bool
) -> None:
    pipeline = pipeline_store_factory("pipeline")
    name = "results" if separate else "pipeline"
    results = pipeline_store_factory(name) if separate else pipeline
    committed: set[bytes] = set()
    for batch in ((1, 2), (3, 4), (5,)):
        for value in batch:
            _enqueue(results, pipeline, value)

        pipeline.flush_batch(RECORDS)
        reader = pipeline_store_factory("pipeline")
        if reader is not pipeline:
            # A fresh handle sees exactly the records from the earlier commits.
            visible = reader.get_all_records(RECORDS)
            assert (
                set(visible["result"].to_pylist() if visible is not None else ())
                == committed
            )

        results.flush_result_batch(RESULTS, publish_records=True)
        pipeline.flush_batch(RECORDS)

        committed.update(bytes([value]) for value in batch)
        durable_results = pipeline_store_factory(name).get_all_records(RESULTS)
        durable_records = pipeline_store_factory("pipeline").get_all_records(RECORDS)
        assert durable_records is not None and durable_results is not None
        assert durable_records.num_rows == durable_results.num_rows == len(committed)
        assert set(durable_records["result"].to_pylist()) == committed
        assert set(durable_results["result"].to_pylist()) == committed


@pytest.mark.parametrize("pipeline_store_factory", STORE_KINDS, indirect=True)
@pytest.mark.parametrize("separate", [False, True], ids=["one-store", "two-stores"])
def test_failed_result_commit_withdraws_guarded_records(
    pipeline_store_factory: Callable[[str], ArrowDatabaseProtocol], separate: bool
) -> None:
    pipeline = pipeline_store_factory("pipeline")
    name = "results" if separate else "pipeline"
    results = pipeline_store_factory(name) if separate else pipeline

    def refuse(_: pa.Schema) -> None:
        """Inject a permanent validation failure at the result commit boundary."""
        raise InputValidationError("injected result schema refusal")

    with results.guard_table_schema(RESULTS, refuse):
        _enqueue(results, pipeline, 1)
    with pytest.raises(InputValidationError, match="injected result schema refusal"):
        results.flush_result_batch(RESULTS, publish_records=True)

    pipeline.flush()
    assert pipeline_store_factory("pipeline").get_all_records(RECORDS) is None
    assert pipeline_store_factory(name).get_all_records(RESULTS) is None

    _enqueue(results, pipeline, 2)
    results.flush_result_batch(RESULTS, publish_records=True)
    pipeline.flush_batch(RECORDS)
    durable_records = pipeline_store_factory("pipeline").get_all_records(RECORDS)
    assert durable_records is not None
    assert durable_records["result"].to_pylist() == [b"\x02"]
