"""SpiralDBTableSource reads no rows until asked and then reads them in batches (ITL-186).

Runs against the disk-backed pyspiral fake, whose ``scan_log`` records every scan a source
opens and how many batches were taken from it.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.sources import SpiralDBTableSource
from tests.test_databases import fake_pyspiral

_PROJECT = "edi-sample"
_KEYS = pa.schema(
    [pa.field("subject_id", pa.string()), pa.field("timestamp", pa.uint64())]
)
_ROWS = 5


@pytest.fixture()
def event_table(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Provide a fake root holding a 5-row ``event`` table read 2 rows per batch."""
    monkeypatch.setattr(fake_pyspiral, "scan_batch_rows", 2)
    with fake_pyspiral.install(tmp_path / "spiral"):
        project = fake_pyspiral.FakeSpiral().project(_PROJECT)
        table = project.create_table("default.event", key_schema=_KEYS)
        rows = pa.table(
            {
                "subject_id": ["m1"] * _ROWS,
                "timestamp": pa.array(range(_ROWS), pa.uint64()),
                "amplitude": pa.array([0.5, 1.5, None, 3.5, 4.5], pa.float32()),
            }
        )
        table.write(
            rows.cast(pa.schema([pa.field(f.name, f.type, True) for f in rows.schema]))
        )
        fake_pyspiral.scan_log.clear()
        yield
        fake_pyspiral.scan_log.clear()


@pytest.mark.xfail(strict=True, reason="ITL-186: the source reads the table when built")
def test_construction_reads_no_rows(event_table):
    source = SpiralDBTableSource(_PROJECT, "event")
    source.output_schema(all_info=True)
    source.keys(all_info=True)
    source.content_hash()
    source.pipeline_hash()
    source.to_config()

    assert fake_pyspiral.scan_log == []


@pytest.mark.xfail(strict=True, reason="ITL-186: the source reads the table when built")
def test_iter_data_reads_batches_only_as_rows_are_consumed(event_table):
    source = SpiralDBTableSource(_PROJECT, "event")
    rows = iter(source.iter_data())

    next(rows)
    assert [scan.batches_read for scan in fake_pyspiral.scan_log] == [1]

    assert sum(1 for _ in rows) == _ROWS - 1
    assert [scan.batches_read for scan in fake_pyspiral.scan_log] == [3]


def test_streamed_rows_equal_as_table_rows(event_table):
    source = SpiralDBTableSource(_PROJECT, "event")

    streamed = [
        {**tag.as_dict(all_info=True), **data.as_dict(all_info=True)}
        for tag, data in source.iter_data()
    ]
    table_rows = source.as_table(all_info=True).to_pylist()

    assert len(streamed) == _ROWS
    assert len(table_rows) == _ROWS
    for streamed_row, table_row in zip(streamed, table_rows, strict=True):
        shared = streamed_row.keys() & table_row.keys()
        assert {"subject_id", "timestamp", "amplitude"} <= shared
        assert any(name.startswith("_tag_record_id::") for name in shared)
        assert any(name.startswith("_source_") for name in shared)
        assert {k: streamed_row[k] for k in shared} == {k: table_row[k] for k in shared}
