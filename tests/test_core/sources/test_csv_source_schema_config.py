"""A ``CSVSource`` built with ``schema=`` saves and restores it (ITL-1315).

Before the fix ``to_config`` left the schema out, so ``from_config`` inferred the types
again: a nullable column came back non-nullable, a narrow width wide, a string of digits
an integer, and the reloaded source had another content hash and pipeline hash under the
same source id.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.core.sources import CSVSource
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)
from orcapod.pipeline.serialization import (
    deserialize_arrow_schema,
    serialize_arrow_schema,
)

# Two tag columns and four data columns of several types: a narrow integer, a string
# holding digits (inference reads it as an integer), a timestamp with a zone, a float32,
# a decimal and a large string; nullable and non-nullable fields.
_FIELDS = {
    "unit": pa.field("unit", pa.int16(), nullable=False),
    "code": pa.field("code", pa.string(), nullable=False),
    "at": pa.field("at", pa.timestamp("us", tz="UTC"), nullable=False),
    "amp": pa.field("amp", pa.float32()),
    "price": pa.field("price", pa.decimal128(10, 2)),
    "note": pa.field("note", pa.large_string()),
}
_ROWS = [
    {
        "unit": "1",
        "code": "007",
        "at": "2026-01-01T00:00:00Z",
        "amp": "0.5",
        "price": "1.25",
        "note": "a",
    },
    {
        "unit": "2",
        "code": "010",
        "at": "2026-01-02T00:00:00Z",
        "amp": "1.5",
        "price": "2.50",
        "note": "b",
    },
]
# Two layouts of the same columns: the file's column order and the schema's agree.
ORDERS = pytest.mark.parametrize(
    "order",
    [
        ("unit", "code", "at", "amp", "price", "note"),
        ("note", "price", "code", "amp", "unit", "at"),
    ],
    ids=["tags-first", "mixed"],
)
# Tag roles: both tags data-tags (the default), one kept plain, both plain.
TAG_ONLY = pytest.mark.parametrize(
    "tag_only", [(), ("code",), True], ids=["data-tags", "one-plain", "plain"]
)


def _write(tmp_path: Path, order: tuple[str, ...]) -> str:
    path = tmp_path / "readings.csv"
    lines = [",".join(order)] + [",".join(row[c] for c in order) for row in _ROWS]
    path.write_text("\n".join(lines) + "\n")
    return str(path)


def _schema(order: tuple[str, ...]) -> pa.Schema:
    return pa.schema([_FIELDS[name] for name in order])


def _source(
    tmp_path: Path, order: tuple[str, ...], tag_only: Any = (), **kwargs: Any
) -> CSVSource:
    return CSVSource(
        _write(tmp_path, order),
        tag_columns=["unit", "code"],
        schema=_schema(order),
        tag_only_columns=tag_only,
        **kwargs,
    )


def _reloaded(source: CSVSource) -> CSVSource:
    # Through JSON, as a saved file holds it.
    return CSVSource.from_config(json.loads(json.dumps(source.to_config())))


def _identity(source: Any) -> tuple[Any, ...]:
    return (
        source.output_schema(),
        source.as_table(columns={"all_info": True}).schema,
        source.content_hash(),
        source.pipeline_hash(),
        source.source_id,
    )


@ORDERS
@TAG_ONLY
def test_a_reloaded_source_has_the_schema_and_hashes_of_the_original(
    tmp_path: Path, order: tuple[str, ...], tag_only: Any
) -> None:
    source = _source(tmp_path, order, tag_only)
    reloaded = _reloaded(source)
    assert _identity(reloaded) == _identity(source)
    assert reloaded.as_table().to_pylist() == source.as_table().to_pylist()
    tag_schema, data_schema = reloaded.output_schema()
    assert tag_schema["code"] is str  # inference would read "007" as 7
    assert data_schema["amp"] == float | None
    assert data_schema["at"] is not None and "None" not in str(data_schema["at"])


@ORDERS
def test_the_saved_schema_keeps_field_order_types_and_nullability(
    tmp_path: Path, order: tuple[str, ...]
) -> None:
    saved = _source(tmp_path, order).to_config()["schema"]
    assert list(saved["columns"]) == list(order)
    assert saved["nullable"] == [n for n in order if n in ("amp", "price", "note")]
    assert deserialize_arrow_schema(saved).equals(_schema(order), check_metadata=True)


def test_the_reported_case_keeps_a_nullable_column_and_a_width(tmp_path: Path) -> None:
    path = tmp_path / "k.csv"
    path.write_text("k,x,y\n1,10,3\n2,20,4\n")
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("x", pa.int64()),
            pa.field("y", pa.int32(), nullable=False),
        ]
    )
    source = CSVSource(str(path), schema=schema, tag_columns=["k"])
    reloaded = _reloaded(source)
    assert reloaded.output_schema()[1]["x"] == int | None
    assert reloaded.as_table().schema.field("y").type == pa.int32()
    assert _identity(reloaded) == _identity(source)


def test_a_source_without_a_schema_saves_no_schema_entry(tmp_path: Path) -> None:
    path = _write(tmp_path, tuple(_FIELDS))
    source = CSVSource(path, tag_columns=["unit", "code"])
    config = source.to_config()
    assert "schema" not in config
    assert _identity(_reloaded(source)) == _identity(source)


def test_a_config_without_a_schema_entry_infers_the_types_as_before(
    tmp_path: Path,
) -> None:
    source = _source(tmp_path, tuple(_FIELDS))
    config = source.to_config()
    del config["schema"]  # a config written before ITL-1315
    reloaded = CSVSource.from_config(config)
    inferred = CSVSource(
        source._file_path,  # pyright: ignore[reportPrivateUsage]  # the same file
        tag_columns=["unit", "code"],
    )
    assert _identity(reloaded) == _identity(inferred)
    assert reloaded.output_schema()[0]["code"] is int


_AB = [pa.field("a", pa.int64()), pa.field("b", pa.int64())]
_METADATA_CASES = {
    "one-field": (
        pa.schema([_AB[0].with_metadata({b"unit": b"mV"}), _AB[1]]),
        "'a'",
    ),
    "two-fields": (
        pa.schema(
            [
                _AB[0].with_metadata({b"unit": b"mV"}),
                _AB[1].with_metadata({b"unit": b"s"}),
            ]
        ),
        "'a'",
    ),
    "second-field": (
        pa.schema([_AB[0], _AB[1].with_metadata({b"unit": b"s"})]),
        "'b'",
    ),
    "schema": (pa.schema(_AB, metadata={b"origin": b"lab"}), "schema's own metadata"),
}


def _ab_source(tmp_path: Path, schema: pa.Schema, **kwargs: Any) -> CSVSource:
    path = tmp_path / "m.csv"
    path.write_text("a,b\n1,2\n3,4\n")
    return CSVSource(str(path), schema=schema, tag_columns=["a"], **kwargs)


@pytest.mark.parametrize(
    ("schema", "named"), list(_METADATA_CASES.values()), ids=list(_METADATA_CASES)
)
def test_metadata_the_saved_form_cannot_hold_is_refused_at_save(
    tmp_path: Path, schema: pa.Schema, named: str
) -> None:
    source = _ab_source(tmp_path, schema)
    # The constructor keeps the metadata; only saving refuses.
    assert source.as_table().schema.equals(schema, check_metadata=True)
    with pytest.raises(ValueError, match="metadata") as info:
        source.to_config()
    assert named in str(info.value)
    assert any(str(tmp_path / "m.csv") in n for n in info.value.__notes__)


def test_the_same_schema_without_metadata_saves(tmp_path: Path) -> None:
    source = _ab_source(tmp_path, pa.schema(_AB))
    assert source.to_config()["schema"] == {
        "columns": {"a": "int64", "b": "int64"},
        "nullable": ["a", "b"],
    }
    assert _identity(_reloaded(source)) == _identity(source)


def _ab_job(tmp_path: Path, schema: pa.Schema) -> PipelineJob:
    source = _ab_source(tmp_path, schema, source_id="ab")
    job = PipelineJob(database=DeltaTableDatabase(tmp_path / "store"))
    with job:
        double_b.pod(source, label="double")
    return job


def test_a_job_over_a_schema_it_cannot_save_fails_to_save(tmp_path: Path) -> None:
    schema, _ = _METADATA_CASES["one-field"]
    job = _ab_job(tmp_path, schema)
    job.run()
    with pytest.raises(ValueError, match="metadata") as info:
        job.save(tmp_path / "job.json")
    notes = info.value.__notes__
    assert any("source 'ab'" in n or "'ab'" in n for n in notes), notes
    assert any(str(tmp_path / "m.csv") in n for n in notes), notes
    assert not (tmp_path / "job.json").exists()


def test_a_job_over_the_schema_without_metadata_saves(tmp_path: Path) -> None:
    job = _ab_job(tmp_path, pa.schema(_AB))
    job.run()
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(tmp_path / "job.json")
    (source,) = job.sources.values()
    (reloaded,) = loaded.sources.values()
    assert _identity(reloaded) == _identity(source)


def test_nested_names_and_nullability_round_trip() -> None:
    schema = pa.schema(
        [
            pa.field(
                "point",
                pa.struct(
                    [
                        pa.field("x", pa.int8(), nullable=False),
                        pa.field("y", pa.float32()),
                    ]
                ),
                nullable=False,
            ),
            pa.field("ids", pa.large_list(pa.field("id", pa.int32(), nullable=False))),
            pa.field("at", pa.timestamp("ns", tz="America/New_York")),
        ]
    )
    saved = json.loads(json.dumps(serialize_arrow_schema(schema)))
    assert deserialize_arrow_schema(saved).equals(schema, check_metadata=True)


@pytest.mark.parametrize(
    "column",
    [
        pa.struct([pa.field("x", pa.int8(), metadata={b"unit": b"mV"})]),
        pa.large_list(pa.field("item", pa.int8(), metadata={b"unit": b"mV"})),
    ],
    ids=["struct-child", "list-item"],
)
def test_nested_field_metadata_is_refused(column: pa.DataType) -> None:
    with pytest.raises(ValueError, match="'point'.*metadata"):
        serialize_arrow_schema(pa.schema([pa.field("point", column)]))


def test_a_saved_nullable_name_that_is_not_a_column_is_refused() -> None:
    with pytest.raises(ValueError, match="nullable"):
        deserialize_arrow_schema({"columns": {"a": "int64"}, "nullable": ["b"]})


@op.function_pod(output_keys=["doubled"])
def double_b(b: int | None) -> int | None:
    """Double ``b``."""
    return None if b is None else 2 * b


@op.function_pod(output_keys=["scaled"])
def scale(amp: float | None) -> float | None:
    """Scale the amplitude."""
    return None if amp is None else 2 * amp


@ORDERS
@pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
def test_a_saved_job_reloads_the_source_with_its_schema(
    tmp_path: Path,
    order: tuple[str, ...],
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
) -> None:
    # Without the decimal: a job's saved slot schema holds Python types, and a
    # decimal column has none yet (its saved type is "null"), whatever the source.
    order = tuple(name for name in order if name != "price")
    source = _source(tmp_path, order, source_id="readings")
    job = PipelineJob(database=DeltaTableDatabase(tmp_path / "store"))
    with job:
        scale.pod(source, label="scale")
    job.run(orchestrator=orchestrator())
    job.save(tmp_path / "job.json")
    saved = json.loads((tmp_path / "job.json").read_text())
    (config,) = saved["bindings"]["sources"].values()
    assert list(config["schema"]["columns"]) == list(order)

    loaded = PipelineJob.load(tmp_path / "job.json")
    (reloaded,) = loaded.sources.values()
    assert isinstance(reloaded, CSVSource)
    assert _identity(reloaded) == _identity(source)
    assert loaded.nodes["scale"].content_hash() == job.nodes["scale"].content_hash()
    assert loaded.nodes["scale"].pipeline_hash() == job.nodes["scale"].pipeline_hash()
    loaded.run(load_only=True, orchestrator=orchestrator())
    rows = sorted(loaded.nodes["scale"].as_table().to_pylist(), key=lambda r: r["unit"])
    assert rows == sorted(
        job.nodes["scale"].as_table().to_pylist(), key=lambda r: r["unit"]
    )
