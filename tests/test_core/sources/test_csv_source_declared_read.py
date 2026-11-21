"""A ``CSVSource`` with ``schema=`` reads every column as declared (ITL-1322).

The text of each cell is parsed as its column's declared type: nothing is inferred and
then cast, so ``007`` under a string type stays ``"007"``. Nulls follow the developer's
option C (November 21, 2026): in a string column only an unquoted empty field is null
(a quoted ``""`` is the empty string, ``NA`` is the text ``NA``); in any other column
pyarrow's null markers are null.
"""

from __future__ import annotations

import gzip
import json
import os
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest import mock

import pyarrow as pa
import pyarrow.csv as pa_csv
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


# ---------------------------------------------------------------------------
# Independent review of PR 467 (second round)
# ---------------------------------------------------------------------------


def _replace_after_first_parse(path: str, replacement: str) -> Any:
    """Wrap the real CSV reader: after its first call, replace the file atomically."""
    real = pa_csv.read_csv
    calls: list[int] = []

    def wrapper(*args: Any, **kwargs: Any) -> pa.Table:
        result = real(*args, **kwargs)
        calls.append(1)
        if len(calls) == 1:
            new = Path(path).with_suffix(".new")
            new.write_text(replacement)
            os.replace(new, path)
        return result

    return wrapper, calls


def test_the_parses_read_one_snapshot_of_the_file(tmp_path: Path) -> None:
    # A text column and a number column: two parses. Replacing the file between
    # them (same row count, rows reordered) must not combine the two versions.
    path = _csv(tmp_path, "id,label\n1,A\n2,B\n")
    schema = pa.schema(
        [pa.field("id", pa.int64(), nullable=False), pa.field("label", pa.string())]
    )
    wrapper, calls = _replace_after_first_parse(path, "id,label\n2,B\n1,A\n")
    with mock.patch("pyarrow.csv.read_csv", wrapper):
        source = CSVSource(
            path,
            schema=schema,
            tag_columns=["id"],
            record_id_columns=["id"],
            tag_only_columns=True,
        )
    assert len(calls) == 2
    assert sorted((r["id"], r["label"]) for r in _rows(source)) == [(1, "A"), (2, "B")]


def test_parses_that_disagree_on_the_rows_raise(tmp_path: Path) -> None:
    # A defence kept beside the snapshot: a second parse with other rows is refused.
    path = _csv(tmp_path, "id,label\n1,A\n2,B\n")
    schema = pa.schema(
        [pa.field("id", pa.int64(), nullable=False), pa.field("label", pa.string())]
    )
    real = pa_csv.read_csv
    calls: list[int] = []

    def short_second_parse(*args: Any, **kwargs: Any) -> pa.Table:
        calls.append(1)
        table = real(*args, **kwargs)
        return table.slice(0, 1) if len(calls) == 2 else table

    with mock.patch("pyarrow.csv.read_csv", short_second_parse):
        with pytest.raises(ValueError, match="the two parses disagree"):
            CSVSource(path, schema=schema, tag_columns=["id"], tag_only_columns=True)


def test_a_compressed_file_is_read_as_declared(tmp_path: Path) -> None:
    path = tmp_path / "declared.csv.gz"
    with gzip.open(path, "wt") as f:
        f.write("k,code\n1,007\n")
    schema = pa.schema(
        [pa.field("k", pa.int64(), nullable=False), pa.field("code", pa.string())]
    )
    source = CSVSource(
        str(path), schema=schema, tag_columns=["k"], tag_only_columns=True
    )
    assert _rows(source) == [{"k": 1, "code": "007"}]


# (declared float type, a finite text too large for it, a finite text that rounds)
_FLOATS = [
    (pa.float16(), "70000", "65504"),
    (pa.float32(), "1e39", "16777217"),
    (pa.float64(), "1e400", "1e308"),
]
FLOATS = pytest.mark.parametrize(
    ("declared", "too_large", "fits"), _FLOATS, ids=["float16", "float32", "float64"]
)
NULLABLE = pytest.mark.parametrize("nullable", [True, False], ids=["null", "not-null"])
_SPELLED = ["inf", "-inf", "+INF", "Infinity", "-Infinity", " inf "]


@FLOATS
@NULLABLE
def test_a_spelled_infinity_is_kept_and_a_finite_value_rounds(
    tmp_path: Path, declared: pa.DataType, too_large: str, fits: str, nullable: bool
) -> None:
    body = "".join(f"{i},{s},{fits}\n" for i, s in enumerate(_SPELLED))
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("a", declared, nullable=nullable),
            pa.field("b", declared, nullable=nullable),
        ]
    )
    source = CSVSource(
        _csv(tmp_path, "k,a,b\n" + body),
        schema=schema,
        tag_columns=["k"],
        tag_only_columns=True,
    )
    rows = _rows(source)
    assert [r["a"] for r in rows] == [
        float("-inf") if s.strip().startswith("-") else float("inf") for s in _SPELLED
    ]
    expected = pa.array([float(fits)], declared)[0].as_py()  # ordinary rounding
    assert {r["b"] for r in rows} == {expected}


@FLOATS
@NULLABLE
@pytest.mark.parametrize("sign", ["", "-"], ids=["positive", "negative"])
def test_a_finite_value_too_large_for_its_float_type_raises(
    tmp_path: Path,
    declared: pa.DataType,
    too_large: str,
    fits: str,
    nullable: bool,
    sign: str,
) -> None:
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("a", declared, nullable=nullable),
            pa.field("b", declared, nullable=nullable),
        ]
    )
    path = _csv(tmp_path, f"k,a,b\n1,inf,{fits}\n2,{fits},{sign}{too_large}\n")
    with pytest.raises(ValueError, match="does not fit") as info:
        CSVSource(path, schema=schema, tag_columns=["k"], tag_only_columns=True)
    notes = info.value.__notes__
    assert "column 'b'" in notes, notes
    assert any(path in note for note in notes), notes


@pytest.mark.parametrize("declared", [pa.binary(), pa.large_binary()])
def test_in_a_binary_column_only_an_unquoted_empty_field_is_null(
    tmp_path: Path, declared: pa.DataType
) -> None:
    body = "".join(f"{i},{cell}\n" for i, (cell, _) in enumerate(_STRING_CELLS))
    schema = pa.schema(
        [pa.field("k", pa.int64(), nullable=False), pa.field("s", declared)]
    )
    source = CSVSource(
        _csv(tmp_path, "k,s\n" + body),
        schema=schema,
        tag_columns=["k"],
        tag_only_columns=True,
    )
    assert [r["s"] for r in _rows(source)] == [
        None if value is None else value.encode() for _, value in _STRING_CELLS
    ]


@pytest.mark.parametrize("declared", [pa.binary(), pa.large_binary()])
def test_a_non_nullable_binary_column_accepts_text_but_no_empty_field(
    tmp_path: Path, declared: pa.DataType
) -> None:
    texts = [cell for cell, value in _STRING_CELLS if value is not None]
    body = "".join(f"{i},{cell}\n" for i, cell in enumerate(texts))
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("s", declared, nullable=False),
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


def test_a_fixed_size_binary_column_follows_the_text_rule(tmp_path: Path) -> None:
    schema = pa.schema(
        [pa.field("k", pa.int64(), nullable=False), pa.field("s", pa.binary(3))]
    )
    source = CSVSource(
        _csv(tmp_path, "k,s\n1,\n2,NaN\n3,abc\n"),
        schema=schema,
        tag_columns=["k"],
        tag_only_columns=True,
    )
    assert [r["s"] for r in _rows(source)] == [None, b"NaN", b"abc"]
    # A quoted empty field is the empty value, which is not three bytes long.
    with pytest.raises(ValueError):
        CSVSource(
            _csv(tmp_path, 'k,s\n1,""\n'),
            schema=schema,
            tag_columns=["k"],
            tag_only_columns=True,
        )


@pytest.mark.parametrize(
    "declared",
    [pa.decimal32(6, 2), pa.decimal64(10, 2), pa.string_view(), pa.binary_view()],
    ids=str,
)
def test_a_scalar_type_the_reader_cannot_parse_is_refused_naming_the_column(
    tmp_path: Path, declared: pa.DataType
) -> None:
    path = _csv(tmp_path, "k,v\n1,1\n")
    schema = pa.schema([pa.field("k", pa.int64()), pa.field("v", declared)])
    with pytest.raises(ValueError, match="cannot read") as info:
        CSVSource(path, schema=schema, tag_columns=["k"], tag_only_columns=True)
    notes = info.value.__notes__
    assert "column 'v'" in notes, notes
    assert any(path in note for note in notes), notes
