"""Store requirements that can run even when a pipeline's schema is refused."""

from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.core.operators import SelectDataColumns
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.sources import ArrowTableSource
from tests.pipeline_stores import STORE_KINDS

COLUMNS = ("tag", "group", "measure", "note")
ORDERS = (
    COLUMNS,
    ("group", "note", "tag", "measure"),
    ("note", "measure", "group", "tag"),
)
_SHADOWED_PATHS = tuple(
    pytest.param(
        kind,
        marks=pytest.mark.xfail(
            strict=True,
            raises=AssertionError,
            reason="TODO-ITL: Distinct record paths sanitize to the same connector table name.",
        ),
    )
    if kind in {"sqlite", "spiral"}
    else kind
    for kind in STORE_KINDS
)
_NO_CONNECTOR_LOADER = tuple(
    pytest.param(
        kind,
        marks=pytest.mark.xfail(
            strict=True,
            raises=AssertionError,
            reason="TODO-ITL: PipelineJob.load cannot reconstruct a saved connector database (ITL-1391).",
        ),
    )
    if kind in {"sqlite", "spiral"}
    else kind
    for kind in STORE_KINDS
)


def _row(tag: int) -> pa.Table:
    """Build a row with two tags and two values."""
    return pa.table(
        {
            "tag": [tag],
            "group": pa.array(["a"], type=pa.large_string()),
            "measure": [10 * tag],
            "note": pa.array([f"n{tag}"], type=pa.large_string()),
        }
    )


@pytest.mark.parametrize("pipeline_store_factory", STORE_KINDS, indirect=True)
def test_a_scan_includes_committed_and_pending_rows_in_different_column_orders(
    pipeline_store: ArrowDatabaseProtocol,
) -> None:
    path = ("ordered", "rows")
    for tag, order in enumerate(ORDERS, start=1):
        pipeline_store.add_record(
            path, bytes([tag]), _row(tag).select(list(order)), flush=tag == 1
        )

    table = pipeline_store.get_all_records(path)
    assert table is not None
    assert (
        table.sort_by("tag")
        .select(list(COLUMNS))
        .equals(pa.concat_tables([_row(tag) for tag in (1, 2, 3)]))
    )


@pytest.mark.parametrize("pipeline_store_factory", _SHADOWED_PATHS, indirect=True)
def test_distinct_record_paths_keep_distinct_rows(
    pipeline_store: ArrowDatabaseProtocol,
) -> None:
    first, second = ("a__b", "c"), ("a", "b__c")
    pipeline_store.add_record(first, b"a", _row(1), flush=True)
    pipeline_store.add_record(second, b"b", _row(2), flush=True)

    one = pipeline_store.get_all_records(first)
    two = pipeline_store.get_all_records(second)
    assert one is not None and one["tag"].to_pylist() == [1]
    assert two is not None and two["tag"].to_pylist() == [2]


@pytest.mark.parametrize("pipeline_store_factory", _NO_CONNECTOR_LOADER, indirect=True)
def test_job_load_restores_the_saved_database(
    pipeline_store_factory: Callable[[str], ArrowDatabaseProtocol], tmp_path: Path
) -> None:
    database = pipeline_store_factory("pipeline")
    source = ArrowTableSource(
        pa.concat_tables([_row(1), _row(2)]),
        tag_columns=["tag", "group"],
        tag_only_columns=True,
        source_id="rows",
    )
    job = PipelineJob(name="conformance", database=database)
    with job:
        SelectDataColumns(["measure", "note"])(source, label="selected")
    job.run()
    path = tmp_path / "job.json"
    job.save(path)

    # A caller that supplies a store can load and bind the job for every backend.
    rebound = PipelineJob.load(path, database=pipeline_store_factory("pipeline"))
    rebound.bind(sources={"rows": source})
    rebound.run(load_only=True)
    assert rebound.nodes["selected"].as_table().equals(job.nodes["selected"].as_table())

    # The saved configuration itself must also be sufficient to rebuild the binding.
    assert PipelineJob.load(path).database is not None
