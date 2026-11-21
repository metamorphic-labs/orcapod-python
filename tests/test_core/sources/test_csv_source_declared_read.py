"""A ``CSVSource`` with ``schema=`` reads every column as declared (ITL-1322).

The text of each cell is parsed as its column's declared type: nothing is inferred and
then cast, so ``007`` under a string type stays ``"007"``. Nulls follow the developer's
option C (November 21, 2026): in a string column only an unquoted empty field is null
(a quoted ``""`` is the empty string, ``NA`` is the text ``NA``); in any other column
pyarrow's null markers are null.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.core.sources import CSVSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    PipelineJob,
    SyncPipelineOrchestrator,
)


def _csv(tmp_path: Path, text: str) -> str:
    path = tmp_path / "declared.csv"
    path.write_text(text)
    return str(path)


def _rows(source: CSVSource) -> list[dict[str, Any]]:
    return source.as_table().to_pylist()


# ---------------------------------------------------------------------------
# Text is parsed as the declared type
# ---------------------------------------------------------------------------


def test_numeric_looking_text_under_a_string_type_is_kept(tmp_path: Path) -> None:
    path = _csv(
        tmp_path,
        "k,code,ratio,power,big,when\n"
        "1,007,1.0,1e3,12345678901234567890,2026-01-02T03:04:05\n"
        '2,"010",2.50,-1E-2,00000000000000000001,2026-01-02T03:04:05+02:00\n',
    )
    text = pa.string()
    schema = pa.schema(
        [pa.field("k", pa.int8(), nullable=False)]
        + [pa.field(n, text) for n in ("code", "ratio", "power", "big", "when")]
    )
    source = CSVSource(path, schema=schema, tag_columns=["k"], tag_only_columns=True)
    assert [{k: v for k, v in r.items() if k != "k"} for r in _rows(source)] == [
        {
            "code": "007",
            "ratio": "1.0",
            "power": "1e3",
            "big": "12345678901234567890",
            "when": "2026-01-02T03:04:05",
        },
        {
            "code": "010",
            "ratio": "2.50",
            "power": "-1E-2",
            "big": "00000000000000000001",
            "when": "2026-01-02T03:04:05+02:00",
        },
    ]


def test_a_decimal_is_parsed_from_its_text_not_through_a_double(
    tmp_path: Path,
) -> None:
    path = _csv(tmp_path, "k,price,rate\n1,1.005,0.29\n2,007,1e2\n")
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("price", pa.decimal128(10, 3)),
            pa.field("rate", pa.decimal256(40, 2)),  # read as text, checked cast
        ]
    )
    source = CSVSource(path, schema=schema, tag_columns=["k"], tag_only_columns=True)
    assert _rows(source) == [
        {"k": 1, "price": Decimal("1.005"), "rate": Decimal("0.29")},
        {"k": 2, "price": Decimal("7.000"), "rate": Decimal("100.00")},
    ]


@pytest.mark.parametrize(
    ("declared", "cell"),
    [
        (pa.int8(), "1.0"),  # main gave 1
        (pa.int64(), "1e3"),  # main gave 1000
        (pa.bool_(), "1.0"),  # main gave True
        (pa.float32(), "true"),  # main gave 1.0
        (pa.date32(), "2026-01-02T03:04:05"),  # main truncated to the date
        (pa.timestamp("us"), "007"),  # main gave 7 microseconds after 1970
        (pa.timestamp("us", tz="UTC"), "2026-01-02T03:04:05"),  # main assumed UTC
        (pa.decimal128(10, 2), "1.005"),  # main truncated to 1.00
        (pa.float16(), "abc"),  # read as text, checked cast
    ],
    ids=lambda v: str(v),
)
def test_a_value_that_is_not_of_its_declared_type_raises_naming_the_column(
    tmp_path: Path, declared: pa.DataType, cell: str
) -> None:
    path = _csv(tmp_path, f"k,good,bad\n1,x,{cell}\n")
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("good", pa.string()),
            pa.field("bad", declared),
        ]
    )
    with pytest.raises(ValueError) as info:
        CSVSource(path, schema=schema, tag_columns=["k"], tag_only_columns=True)
    notes = info.value.__notes__
    assert "column 'bad'" in notes, notes
    assert any(path in note for note in notes), notes


@pytest.mark.parametrize(
    "declared",
    [pa.list_(pa.int64()), pa.struct([("a", pa.int64())])],
    ids=["list", "struct"],
)
def test_a_type_no_csv_cell_can_hold_is_refused_naming_the_column(
    tmp_path: Path, declared: pa.DataType
) -> None:
    path = _csv(tmp_path, "k,v\n1,1\n")
    schema = pa.schema([pa.field("k", pa.int64()), pa.field("v", declared)])
    with pytest.raises(ValueError, match="cannot be read from it") as info:
        CSVSource(path, schema=schema, tag_columns=["k"], tag_only_columns=True)
    assert "column 'v'" in info.value.__notes__


@pytest.mark.parametrize(
    "names",
    [["k", "v"], ["k", "v", "w", "x"], ["v", "k", "w"]],
    ids=["fewer", "more", "order"],
)
def test_a_schema_names_every_column_of_the_file_in_order(
    tmp_path: Path, names: list[str]
) -> None:
    path = _csv(tmp_path, "k,v,w\n1,a,b\n")
    schema = pa.schema([pa.field(n, pa.string()) for n in names])
    with pytest.raises(ValueError, match="not the schema's fields"):
        CSVSource(path, schema=schema, tag_columns=["k"], tag_only_columns=True)


# ---------------------------------------------------------------------------
# Nulls: option C
# ---------------------------------------------------------------------------

# (cell as written in the file, value under a string type)
_STRING_CELLS = [
    ("", None),  # unquoted empty field: null
    ('""', ""),  # quoted empty field: the empty string
    ("NA", "NA"),
    ("null", "null"),
    ("N/A", "N/A"),
    ("NaN", "NaN"),
    ('"NULL"', "NULL"),
    (" x ", " x "),
    ('" "', " "),
]


@pytest.mark.parametrize("declared", [pa.string(), pa.large_string()])
def test_in_a_string_column_only_an_unquoted_empty_field_is_null(
    tmp_path: Path, declared: pa.DataType
) -> None:
    body = "".join(f"{i},{cell}\n" for i, (cell, _) in enumerate(_STRING_CELLS))
    path = _csv(tmp_path, "k,s\n" + body)
    schema = pa.schema(
        [pa.field("k", pa.int64(), nullable=False), pa.field("s", declared)]
    )
    source = CSVSource(path, schema=schema, tag_columns=["k"], tag_only_columns=True)
    assert [r["s"] for r in _rows(source)] == [value for _, value in _STRING_CELLS]


def test_a_non_nullable_string_column_accepts_every_text_but_no_empty_field(
    tmp_path: Path,
) -> None:
    texts = [cell for cell, value in _STRING_CELLS if value is not None]
    body = "".join(f"{i},{cell}\n" for i, cell in enumerate(texts))
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("s", pa.string(), nullable=False),
        ]
    )
    source = CSVSource(
        _csv(tmp_path, "k,s\n" + body),
        schema=schema,
        tag_columns=["k"],
        tag_only_columns=True,
    )
    assert None not in [r["s"] for r in _rows(source)]
    with pytest.raises(ValueError, match="'s'"):
        CSVSource(
            _csv(tmp_path, "k,s\n" + body + "99,\n"),
            schema=schema,
            tag_columns=["k"],
            tag_only_columns=True,
        )


# Two string and two non-string columns, interleaved, in two layouts; a quoted
# field holding the delimiter and one holding a newline keep the two reads aligned.
_COLUMNS = {
    "name": pa.field("name", pa.string()),
    "count": pa.field("count", pa.int16()),
    "note": pa.field("note", pa.large_string()),
    "score": pa.field("score", pa.float32()),
}
_CELLS = [
    {"name": "007", "count": "", "note": '"a,b"', "score": "NA"},
    {"name": "", "count": "NA", "note": '"line one\nline two"', "score": ""},
    {"name": '""', "count": "3", "note": "NA", "score": "0.5"},
]
_EXPECTED = [
    {"name": "007", "count": None, "note": "a,b", "score": None},
    {"name": None, "count": None, "note": "line one\nline two", "score": None},
    {"name": "", "count": 3, "note": "NA", "score": 0.5},
]
LAYOUTS = pytest.mark.parametrize(
    "layout",
    [("name", "count", "note", "score"), ("score", "note", "count", "name")],
    ids=["string-first", "number-first"],
)
ROLES = pytest.mark.parametrize(
    ("tags", "tag_only"),
    [
        (["row"], True),  # every column data
        (["row", "name", "count"], True),  # a string tag and a number tag, plain
        (["row", "name", "count"], ()),  # the same as data-tags
    ],
    ids=["data", "plain-tags", "data-tags"],
)


def _interleaved(tmp_path: Path, layout: tuple[str, ...], **kwargs: Any) -> CSVSource:
    lines = [",".join(("row", *layout))]
    for i, cells in enumerate(_CELLS):
        lines.append(",".join((str(i), *(cells[c] for c in layout))))
    path = _csv(tmp_path, "\n".join(lines) + "\n")
    schema = pa.schema(
        [pa.field("row", pa.int64(), nullable=False)] + [_COLUMNS[c] for c in layout]
    )
    return CSVSource(path, schema=schema, **kwargs)


@LAYOUTS
@ROLES
def test_string_and_other_columns_interleaved_read_their_own_nulls(
    tmp_path: Path, layout: tuple[str, ...], tags: list[str], tag_only: Any
) -> None:
    source = _interleaved(tmp_path, layout, tag_columns=tags, tag_only_columns=tag_only)
    rows = sorted(_rows(source), key=lambda r: r["row"])
    assert [{c: r[c] for c in _COLUMNS} for r in rows] == _EXPECTED
    tag_schema, data_schema = source.output_schema()
    assert set(tag_schema) == set(tags)
    assert source.as_table().schema.field("count").type == pa.int16()


@LAYOUTS
def test_a_reloaded_source_reads_the_same_values(
    tmp_path: Path, layout: tuple[str, ...]
) -> None:
    source = _interleaved(
        tmp_path, layout, tag_columns=["row", "name"], tag_only_columns=True
    )
    reloaded = CSVSource.from_config(json.loads(json.dumps(source.to_config())))
    assert _rows(reloaded) == _rows(source)
    assert reloaded.content_hash() == source.content_hash()
    assert reloaded.pipeline_hash() == source.pipeline_hash()


@LAYOUTS
def test_a_check_reads_the_changed_file_as_declared(
    tmp_path: Path, layout: tuple[str, ...]
) -> None:
    source = _interleaved(tmp_path, layout, tag_columns=["row"], tag_only_columns=True)
    path = Path(source.to_config()["file_path"])
    text = path.read_text().replace("007", "0070")
    path.write_text(text + f"9,{','.join('' for _ in layout)}\n")
    assert source.check_for_new_data()
    rows = sorted(_rows(source), key=lambda r: r["row"])
    assert rows[0]["name"] == "0070"
    assert {c: rows[-1][c] for c in _COLUMNS} == dict.fromkeys(_COLUMNS)


def test_without_a_schema_the_types_are_inferred_as_before(tmp_path: Path) -> None:
    path = _csv(tmp_path, "k,code,note\n1,007,NA\n2,010,x\n")
    source = CSVSource(path, tag_columns=["k"], tag_only_columns=True)
    assert [r["code"] for r in _rows(source)] == [7, 10]
    assert [r["note"] for r in _rows(source)] == ["NA", "x"]


@op.function_pod(output_keys=["tagged"])
def tag_name(name: str | None) -> str | None:
    """Echo the name with a marker, keeping nulls."""
    return None if name is None else f"<{name}>"


@LAYOUTS
def test_sync_and_async_runs_see_the_declared_values(
    tmp_path: Path, layout: tuple[str, ...]
) -> None:
    results = []
    for orchestrator in (SyncPipelineOrchestrator, AsyncPipelineOrchestrator):
        source = _interleaved(
            tmp_path, layout, tag_columns=["row"], tag_only_columns=True
        )
        job = PipelineJob(database=InMemoryArrowDatabase())
        with job:
            tag_name.pod(source.select_data_columns(["name"]), label="tag")
        job.run(orchestrator=orchestrator())
        rows = job.nodes["tag"].as_table().to_pylist()
        results.append(sorted(rows, key=lambda r: r["row"]))
    assert results[0] == results[1]
    assert [r["tagged"] for r in results[0]] == ["<007>", None, "<>"]
