"""Stored and registered shapes of a user class must agree on read (ITL-881, S2).

A stored column whose extension name matches a registered class but whose Arrow storage
differs (possible only if two data contexts map a field differently, or a table was
written by other means) must raise ``TypeIdentityError`` from ``DeltaTableDatabase``
instead of being logged and read as "no records".
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

import deltalake
import pyarrow as pa
import pytest

import orcapod as op
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import TypeIdentityError
from orcapod.sources import DictSource


@op.register_type(name="itl881.StoredPoint")
@dataclasses.dataclass
class StoredPoint:
    x: int
    y: int


def _extension_field_metadata() -> dict[bytes | str, bytes | str]:
    value: Any = StoredPoint(1, 2)
    source = DictSource(data=[{"k": "a", "p": value}], tag_columns=["k"])
    ext = source.as_table().schema.field("p").type
    return {
        b"ARROW:extension:name": ext.extension_name.encode(),
        b"ARROW:extension:metadata": ext.__arrow_ext_serialize__(),
    }


def test_storage_mismatch_raises_instead_of_reading_nothing(tmp_path: Path) -> None:
    wrong = pa.struct([pa.field("x", pa.float64()), pa.field("y", pa.int64())])
    field = pa.field("q", wrong).with_metadata(_extension_field_metadata())
    table = pa.table(
        {"q": pa.array([{"x": 1.5, "y": 2}], type=wrong)},
        schema=pa.schema([field]),
    )
    deltalake.write_deltalake(str(tmp_path / "points"), table)

    with pytest.raises(TypeIdentityError, match="expected storage_type"):
        DeltaTableDatabase(tmp_path).get_all_records(("points",))


@op.register_type(name="itl881.Labelled")
@dataclasses.dataclass
class Labelled:
    label: str
    n: int


def _run_labelled(db_root: Path) -> tuple[int, list[Labelled]]:
    from orcapod import PipelineJob, function_pod

    calls: list[Labelled] = []

    @function_pod(output_keys="q")
    def echo(p: Labelled) -> Labelled:
        calls.append(p)
        return p

    rows: list[Any] = [{"k": f"r{i}", "p": Labelled(f"x{i}", i)} for i in (1, 2)]
    job = PipelineJob(name="compact", store=DeltaTableDatabase(db_root))
    with job:
        echo.pod(DictSource(data=rows, tag_columns=["k"], source_id="s"), label="echo")
    job.run()
    values = [data.as_dict()["q"] for _, data in job.nodes["echo"].iter_data()]
    return len(calls), sorted(values, key=lambda v: v.n)


def test_stored_values_read_after_delta_compaction(tmp_path: Path) -> None:
    """Delta compaction rewrites large_string struct fields as string; still readable."""
    expected = [Labelled("x1", 1), Labelled("x2", 2)]
    assert _run_labelled(tmp_path) == (2, expected)
    for log in tmp_path.rglob("_delta_log"):
        deltalake.DeltaTable(str(log.parent)).optimize.compact()
    assert _run_labelled(tmp_path) == (0, expected)
