"""A ``CSVSource`` built with ``schema=`` saves and restores it (ITL-1315).

Before the fix ``to_config`` left the schema out, so ``from_config`` inferred the types
again: a nullable column came back non-nullable, a narrow width wide, a string of digits
an integer, and the reloaded source had another content hash and pipeline hash under the
same source id.
"""

from __future__ import annotations

import json
import re
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
    NESTED_SOURCE_CONFIG_KEYS,
    check_saved_source_config,
    deserialize_arrow_schema,
    parse_arrow_type_string,
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
    assert any("source 'CSVSource'" in n for n in notes), notes
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
@pytest.mark.parametrize(
    "with_decimal",
    # A job's saved slot schema holds the slot's own Arrow types (ITL-640), so a
    # decimal column, which has no Python type, is saved as decimal128(10, 2) and the
    # loaded job rebuilds the source (ITL-1323).
    [False, True],
    ids=["no-decimal", "decimal"],
)
def test_a_saved_job_reloads_the_source_with_its_schema(
    tmp_path: Path,
    order: tuple[str, ...],
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
    with_decimal: bool,
) -> None:
    if not with_decimal:
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
    assert len(loaded.sources) == 1, "the loaded job did not rebuild the source"
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


# A saved "schema" entry other than the one to_config writes is refused, never
# interpreted: only an ABSENT key means "no explicit schema".
_MALFORMED = {
    "null": (None, "must be an object"),
    "empty-object": ({}, "missing ['columns', 'nullable']"),
    "unknown-key": (
        {
            "columns": {"a": "int64", "b": "int64", "ab": "int64"},
            "nullable": [],
            "metadata": {},
        },
        "unknown keys ['metadata']",
    ),
    "nullable-as-text": (
        # Overlapping names: iterated as characters, "ab" would have matched them all.
        {"columns": {"a": "int64", "b": "int64", "ab": "int64"}, "nullable": "ab"},
        "must be a list of column names",
    ),
    "nullable-unknown-column": (
        {"columns": {"a": "int64", "b": "int64", "ab": "int64"}, "nullable": ["c"]},
        "not among the saved columns",
    ),
    "nullable-repeated": (
        {
            "columns": {"a": "int64", "b": "int64", "ab": "int64"},
            "nullable": ["b", "b"],
        },
        "repeats ['b']",
    ),
    "columns-empty": ({"columns": {}, "nullable": []}, "non-empty object"),
    "type-unbalanced": (
        # The parser once dropped the last character unchecked and read list<item: int64>.
        {
            "columns": {"a": "int64", "b": "list<item: int64X", "ab": "int64"},
            "nullable": [],
        },
        "Column 'b' has the saved type 'list<item: int64X'",
    ),
    "type-not-text": (
        {"columns": {"a": "int64", "b": 64, "ab": "int64"}, "nullable": []},
        "column 'b' must name its type as text",
    ),
}
MALFORMED = pytest.mark.parametrize(
    ("entry", "message"), list(_MALFORMED.values()), ids=list(_MALFORMED)
)


def _overlap_source(tmp_path: Path) -> CSVSource:
    path = tmp_path / "overlap.csv"
    path.write_text("a,b,ab\n1,2,3\n4,5,6\n")
    schema = pa.schema(
        [
            pa.field("a", pa.int64(), nullable=False),
            pa.field("b", pa.int64(), nullable=False),
            pa.field("ab", pa.int64(), nullable=False),
        ]
    )
    return CSVSource(
        str(path),
        schema=schema,
        tag_columns=["a"],
        label="overlap",
        source_id="overlap",
    )


@MALFORMED
def test_a_malformed_schema_entry_is_refused_by_from_config(
    tmp_path: Path, entry: object, message: str
) -> None:
    config = _overlap_source(tmp_path).to_config()
    config["schema"] = entry
    with pytest.raises(ValueError, match=re.escape(message)) as info:
        CSVSource.from_config(config)
    assert any(str(tmp_path / "overlap.csv") in n for n in info.value.__notes__)


@MALFORMED
def test_a_job_file_with_a_malformed_schema_entry_is_refused(
    tmp_path: Path, entry: object, message: str
) -> None:
    job = PipelineJob(database=DeltaTableDatabase(tmp_path / "store"))
    with job:
        double_b.pod(_overlap_source(tmp_path), label="double")
    job.run()
    job.save(tmp_path / "job.json")
    saved = json.loads((tmp_path / "job.json").read_text())
    saved["bindings"]["sources"]["overlap"]["schema"] = entry
    (tmp_path / "bad.json").write_text(json.dumps(saved))
    # Refused as the file's error, not loaded with the source unbound.
    with pytest.raises(ValueError, match=re.escape(message)) as info:
        PipelineJob.load(tmp_path / "bad.json")
    assert "source 'overlap'" in str(info.value)
    assert str(tmp_path / "bad.json") in str(info.value)


def test_a_job_file_without_the_schema_entry_loads_bound(tmp_path: Path) -> None:
    job = PipelineJob(database=DeltaTableDatabase(tmp_path / "store"))
    with job:
        double_b.pod(_overlap_source(tmp_path), label="double")
    job.run()
    job.save(tmp_path / "job.json")
    saved = json.loads((tmp_path / "job.json").read_text())
    del saved["bindings"]["sources"]["overlap"]["schema"]
    (tmp_path / "old.json").write_text(json.dumps(saved))
    loaded = PipelineJob.load(tmp_path / "old.json")
    (reloaded,) = loaded.sources.values()
    # The file's columns infer as the declared int64 non-null: the slot accepts it.
    assert reloaded.output_schema() == job.sources["overlap"].output_schema()


@pytest.mark.parametrize("key", NESTED_SOURCE_CONFIG_KEYS)
def test_a_nested_malformed_schema_entry_is_refused(tmp_path: Path, key: str) -> None:
    inner = _overlap_source(tmp_path).to_config()
    check_saved_source_config(inner)  # the entry to_config writes passes
    inner["schema"] = None
    with pytest.raises(ValueError, match="must be an object"):
        check_saved_source_config({"source_type": "cached", key: inner})


@pytest.mark.parametrize(
    "text",
    [
        "list<item: int64X",
        "list<item: int64>x",
        "list<item: int64>>",
        "large_list<item: struct<a: int8>",
        "struct<a: int64",
        "map<large_string, int64",
    ],
)
def test_a_type_text_with_unbalanced_delimiters_is_refused(text: str) -> None:
    with pytest.raises(ValueError, match="Unbalanced"):
        parse_arrow_type_string(text)


@pytest.mark.parametrize(
    "arrow_type",
    [
        pa.large_list(pa.field("item", pa.struct([pa.field("a", pa.int8(), False)]))),
        pa.struct([pa.field("p", pa.list_(pa.field("x", pa.float32(), False)))]),
        pa.map_(pa.large_string(), pa.large_list(pa.int64()), True),
        pa.list_(pa.field("item", pa.int64()), 2),
    ],
    ids=["list-of-struct", "struct-of-list", "map-of-list", "fixed-size-list"],
)
def test_nested_type_text_still_round_trips(arrow_type: pa.DataType) -> None:
    schema = pa.schema([pa.field("v", arrow_type)])
    assert deserialize_arrow_schema(serialize_arrow_schema(schema)).equals(schema)
