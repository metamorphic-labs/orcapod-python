"""Source boundaries validate Arrow Null and explicitly nominated system tags."""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
from re import escape
from collections.abc import Collection, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypedDict

import deltalake
import polars as pl
import pyarrow as pa
import pyarrow.csv as pa_csv
import pytest
from pydantic import (
    BaseModel,
    ConfigDict,
    SerializerFunctionWrapHandler,
    computed_field,
    model_serializer,
)

from orcapod import PipelineJob
from orcapod.contexts.stored import with_data_context
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.sources import (
    ArrowTableSource,
    CachedSource,
    CSVSource,
    DBTableSource,
    DataFrameSource,
    DeltaTableSource,
    DictSource,
    ListSource,
    PollingSource,
    PostgreSQLTableSource,
    SpiralDBTableSource,
    SQLiteTableSource,
)
from orcapod.core.sources.source_proxy import SourceProxy
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.operators import DropNulls
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.types import CacheMode, ColumnInfo, Cursor
from tests.test_core.sources.test_db_table_source import MockDBConnector
from tests.test_databases import fake_spiral_sdk

TAGS = ("g", "k")
SOURCE_KINDS = (
    "arrow",
    "dict",
    "dataframe",
    "csv",
    "delta",
    "db",
    "postgres",
    "spiral",
    "sqlite",
)


class _SourceOptions(TypedDict):
    """Keyword arguments supported by the sources in the validation matrix."""

    tag_columns: Collection[str]
    tag_only_columns: bool | Collection[str]
    source_id: str
    system_tag_columns: Collection[str]


class _Connector(MockDBConnector):
    """Mock connector exposing the declared flags of an Arrow table."""

    def __init__(self, table: pa.Table) -> None:
        super().__init__({"rows": table}, {"rows": ["k"]})
        self.table = table

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        """Return the database's field declarations."""
        return [
            ColumnInfo(info.name, info.arrow_type, field.nullable)
            for info, field in zip(
                super().get_column_info(table_name),
                self.table.schema,
                strict=True,
            )
        ]

    def current_location(self) -> tuple[str, int, str, str]:
        """Return the connected server's canonical location."""
        return "localhost", 5432, "test", "public"

    def given_server(self) -> tuple[str, str]:
        """Return the server specified in the connection string."""
        return "localhost", ""


def _source(
    kind: str,
    table: pa.Table,
    path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    system_tag_columns: Collection[str] = (),
    tag_columns: Collection[str] = TAGS,
    tag_only_columns: bool | Collection[str] = True,
) -> StreamProtocol:
    """Build the named source over the same columns using its regular reader."""
    options: _SourceOptions = {
        "tag_columns": tag_columns,
        "tag_only_columns": tag_only_columns,
        "source_id": "rows",
        "system_tag_columns": system_tag_columns,
    }
    if kind == "arrow":
        return ArrowTableSource(table, **options)
    if kind == "dict":
        return DictSource(table.to_pylist(), data_schema=table.schema, **options)
    if kind == "dataframe":
        return DataFrameSource(pl.from_arrow(table), schema=table.schema, **options)
    if kind == "csv":
        file = path / "rows.csv"
        pa_csv.write_csv(table, file)
        return CSVSource(str(file), schema=table.schema, **options)
    if kind == "delta":
        deltalake.write_deltalake(path / "delta", table)
        return DeltaTableSource(path / "delta", **options)
    if kind == "sqlite":
        file = path / "rows.db"
        types = {pa.string(): "TEXT", pa.int64(): "INTEGER"}
        cols = ", ".join(
            f'"{f.name}" {types[f.type]}' + ("" if f.nullable else " NOT NULL")
            for f in table.schema
        )
        with sqlite3.connect(file) as conn:
            conn.execute(f"CREATE TABLE rows ({cols}, PRIMARY KEY (k))")
            placeholders = ", ".join("?" for _ in table.schema)
            conn.executemany(
                f"INSERT INTO rows VALUES ({placeholders})",
                [tuple(row.values()) for row in table.to_pylist()],
            )
        return SQLiteTableSource(file, "rows", **options)
    if kind == "spiral":
        with fake_spiral_sdk.install(path / "spiral"):
            project = fake_spiral_sdk.ExternalProject("project")
            project.create_table(
                "default.rows", key_schema=pa.schema([table.schema.field("k")])
            ).write(table)
            project.table("default.rows").record(table.schema)
            return SpiralDBTableSource("project", "rows", **options)
    connector = _Connector(table)
    if kind == "postgres":
        monkeypatch.setattr(
            "orcapod.core.sources.postgresql_table_source.PostgreSQLConnector",
            lambda *args, **kwargs: connector,
        )
        return PostgreSQLTableSource("postgresql://localhost/test", "rows", **options)
    assert kind == "db"
    return DBTableSource(connector, "rows", **options)


@pytest.mark.parametrize("kind", SOURCE_KINDS)
@pytest.mark.parametrize("role", ["data", "tag", "data-tag"])
@pytest.mark.parametrize("first", [False, True])
def test_raw_sources_refuse_all_reserved_columns(
    kind: str, role: str, first: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = pa.table(
        {
            "g": ["a", "b"],
            "k": [1, 2],
            "x": [3, 4],
            "y": [5, 6],
            "_foo": ["a", "b"],
            "_content_hash": ["c", "d"],
        }
    )
    if first:
        table = table.select(["_content_hash", "g", "_foo", "k", "y", "x"])
    tags = TAGS if role == "data" else (*TAGS, "_foo")
    with pytest.raises(InputValidationError, match="drop them first") as info:
        _source(
            kind,
            table,
            tmp_path,
            monkeypatch,
            tag_columns=tags,
            tag_only_columns=TAGS if role == "data-tag" else True,
        )
    assert "'_foo'" in str(info.value)
    assert "'_content_hash'" in str(info.value)


@pytest.mark.parametrize("arrow_schema", [False, True], ids=["python", "arrow"])
def test_dict_source_refuses_reserved_fields_omitted_from_schema(
    arrow_schema: bool,
) -> None:
    schema = (
        pa.schema(
            [
                ("g", pa.string()),
                ("k", pa.int64()),
                ("x", pa.int64()),
                ("y", pa.int64()),
            ]
        )
        if arrow_schema
        else {"g": str, "k": int, "x": int, "y": int}
    )
    rows = [
        {"g": "a", "k": 1, "x": 2, "y": 3, "_foo": 4},
        {"g": "b", "k": 2, "x": 3, "y": 4, "_source_x": 5},
    ]
    with pytest.raises(
        InputValidationError, match="'_foo'.*'_source_x'.*drop them first"
    ):
        DictSource(rows, tag_columns=TAGS, tag_only_columns=True, data_schema=schema)


def test_csv_refuses_reserved_declared_columns_before_parsing_values(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rows.csv"
    path.write_text("g,k,x,y,_foo,_source_x\na,1,2,3,invalid,token\n")
    schema = pa.schema(
        [
            ("g", pa.string()),
            ("k", pa.int64()),
            ("x", pa.int64()),
            ("y", pa.int64()),
            ("_foo", pa.int64()),
            ("_source_x", pa.string()),
        ]
    )
    with pytest.raises(
        InputValidationError, match="'_foo'.*'_source_x'.*drop them first"
    ):
        CSVSource(str(path), tag_columns=TAGS, tag_only_columns=True, schema=schema)


@pytest.mark.parametrize("declared", [True, False], ids=["declared", "inferred"])
def test_csv_refuses_reserved_header_before_parsing_bad_rows(
    tmp_path: Path, declared: bool
) -> None:
    path = tmp_path / "rows.csv"
    path.write_text(
        '\ufeff\r\nk,x,"_foo","_source_x"\r\n1,invalid,token,source,extra\r\n'
    )
    schema = pa.schema([("k", pa.int64()), ("x", pa.int64())]) if declared else None
    with pytest.raises(
        InputValidationError, match="'_foo'.*'_source_x'.*drop them first"
    ):
        CSVSource(str(path), tag_columns=["k"], schema=schema)


def _late_reserved_rows(reverse: bool) -> list[dict[str, int]]:
    rows = [{"g": 0, "k": i, "x": i + 1, "y": i + 2} for i in range(1002)]
    names = ("_foo", "_source_x")
    if reverse:
        names = names[::-1]
    rows[-2][names[0]] = 3
    rows[-1][names[1]] = 4
    return rows


@pytest.mark.parametrize("iterable", [list, iter], ids=["sequence", "iterator"])
def test_dataframe_source_checks_fields_beyond_polars_inference(iterable) -> None:
    message = "'_foo'.*drop them first"
    if iterable is list:
        message = "'_foo'.*'_source_x'.*drop them first"
    with pytest.raises(InputValidationError, match=message):
        DataFrameSource(
            iterable(_late_reserved_rows(False)),
            tag_columns=TAGS,
            tag_only_columns=True,
        )


def test_dataframe_source_preserves_inference_for_single_pass_rows() -> None:
    def rows() -> Iterator[dict[str, int]]:
        row = {"g": 0, "k": 0, "x": 1, "y": 2}
        for i in range(1002):
            row["k"] = i
            yield row

    expected = DataFrameSource(
        pl.DataFrame(rows()), tag_columns=TAGS, tag_only_columns=True
    )
    source = DataFrameSource(rows(), tag_columns=TAGS, tag_only_columns=True)
    assert source.as_table().equals(expected.as_table())
    assert source.source_id == expected.source_id


def test_dataframe_source_stops_after_the_first_invalid_iterator_row() -> None:
    def rows() -> Iterator[dict[str, int]]:
        yield {"k": 1, "x": 2, "_foo": 3, "_source_x": 4}
        pytest.fail("reserved fields must stop iteration")

    with pytest.raises(
        InputValidationError, match="'_foo'.*'_source_x'.*drop them first"
    ):
        DataFrameSource(rows(), tag_columns=["k"])


@dataclass
class _FrameRow:
    k: int
    x: int


@dataclass
class _ReservedFrameRow:
    k: int
    _foo: int


@dataclass
class _ExtendedFrameRow(_FrameRow):
    _foo: int


@dataclass
class _RenamedFrameRow:
    k: int
    y: int


@dataclass
class _ReorderedFrameRow:
    x: int
    k: int


@dataclass
class _TypedFrameRow:
    k: int
    x: float


class _TypedFrameModel(BaseModel):
    k: int
    x: list[int]


class _FrameModel(BaseModel):
    model_config = ConfigDict(extra="allow")

    k: int
    x: int


class _PlainFrameModel(BaseModel):
    k: int
    x: int


class _ReservedComputedFrameModel(_PlainFrameModel):
    @computed_field  # type: ignore[prop-decorator]
    @property
    def _foo(self) -> int:
        return 12345


class _ComputedFrameModel(_PlainFrameModel):
    @computed_field  # type: ignore[prop-decorator]
    @property
    def total(self) -> int:
        raise AssertionError(
            "checking the row layout must not evaluate computed fields"
        )


class _SerializedFrameModel(BaseModel):
    k: int
    x: _PlainFrameModel

    @model_serializer(mode="wrap")
    def _serialize(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        fields = handler(self)
        if self.k > 100:
            fields["_foo"] = 12345
        return fields


def _model_with_explicit_attribute() -> _PlainFrameModel:
    row = _PlainFrameModel(k=2, x=4)
    object.__setattr__(row, "_raw", 12345)
    return row


FRAME_ROW_CASES = [
    pytest.param(
        [None]
        + [{"k": i, "x": i + 2} for i in range(101)]
        + [{"k": 101, "x": 102, "_foo": 12345}],
        "'_foo'.*drop them first",
        id="leading-null",
    ),
    pytest.param(
        [_FrameRow(i, i + 2) for i in range(101)] + [_ReservedFrameRow(101, 12345)],
        "'_foo'.*drop them first",
        id="dataclass-replacement",
    ),
    pytest.param(
        [_FrameRow(i, i + 2) for i in range(101)]
        + [_ExtendedFrameRow(101, 102, 12345)],
        "'_foo'.*drop them first",
        id="dataclass-extension",
    ),
    pytest.param(
        [_FrameModel(k=i, x=i + 2) for i in range(101)]
        + [_FrameModel.model_validate({"k": 101, "x": 102, "_foo": 12345})],
        "'_foo'.*drop them first",
        id="pydantic-extra",
    ),
    pytest.param(
        [_ReservedComputedFrameModel(k=1, x=2)],
        r"row 0.*'_foo'.*drop them first",
        id="pydantic-reserved-computed",
    ),
    pytest.param(
        [
            _PlainFrameModel(k=1, x=2),
            _PlainFrameModel(k=2, x=4).model_copy(update={"_foo": 3}),
        ],
        r"row 1.*'_foo'.*drop them first",
        id="pydantic-reserved-undeclared",
    ),
    pytest.param(
        [_PlainFrameModel(k=1, x=2), _model_with_explicit_attribute()],
        r"row 1.*'_raw'.*drop them first",
        id="pydantic-explicit-instance-attribute",
    ),
    pytest.param(
        [_ComputedFrameModel(k=1, x=2)],
        r"row 0.*'total'.*mapping",
        id="pydantic-ordinary-computed",
    ),
    pytest.param(
        [_PlainFrameModel(k=1, x=2).model_copy(update={"total": 3})],
        r"row 0.*'total'.*mapping",
        id="pydantic-ordinary-undeclared",
    ),
    pytest.param(
        [
            _SerializedFrameModel(k=i, x=_PlainFrameModel(k=i, x=i + 2))
            for i in range(102)
        ],
        r"row 0.*serialized field names.*mapping",
        id="pydantic-dynamic-serialization",
    ),
    pytest.param(
        [_FrameRow(i, i + 2) for i in range(101)] + [_RenamedFrameRow(101, 12345)],
        r"row 101.*field names.*'x'.*'y'",
        id="different-legal-fields",
    ),
    pytest.param(
        [_FrameRow(1, 2), _ReorderedFrameRow(4, 3)],
        r"row 1.*layout.*'k'.*'x'",
        id="reordered-typed-fields",
    ),
    pytest.param(
        [_FrameModel.model_validate({"k": 1, "x": 2, "extra": 3})],
        r"row 0.*extra.*'extra'",
        id="legal-model-extra",
    ),
    pytest.param(
        [(1, 2), (3, 4)],
        r"row 0.*named fields",
        id="unnamed-rows",
    ),
]


@pytest.mark.parametrize("rows, message", FRAME_ROW_CASES)
@pytest.mark.parametrize("use_iterator", [False, True], ids=["sequence", "iterator"])
def test_dataframe_source_checks_named_row_shapes(
    rows: list[Any], message: str, use_iterator: bool
) -> None:
    with pytest.raises(InputValidationError, match=message):
        DataFrameSource(iter(rows) if use_iterator else rows)


@pytest.mark.parametrize(
    ("rows", "arrow_type"),
    [
        ([_TypedFrameRow(1, 2)], pa.float64()),
        ([_TypedFrameModel(k=1, x=[])], pa.large_list(pa.int64())),
    ],
    ids=["dataclass-float", "pydantic-empty-list"],
)
@pytest.mark.parametrize("use_iterator", [False, True], ids=["sequence", "iterator"])
def test_homogeneous_typed_rows_keep_schema_and_identity(
    rows: list[Any], arrow_type: pa.DataType, use_iterator: bool
) -> None:
    expected = DataFrameSource(pl.DataFrame(rows), tag_columns=["k"])
    source = DataFrameSource(iter(rows) if use_iterator else rows, tag_columns=["k"])
    assert source.as_table().schema.field("x").type == arrow_type
    assert source.as_table().equals(expected.as_table())
    assert source.source_id == expected.source_id
    assert source.content_hash() == expected.content_hash()
    assert source.pipeline_hash() == expected.pipeline_hash()


@pytest.mark.parametrize(
    "rows",
    [
        [None, {"k": 1, "x": 2}, None, {"x": 4, "k": 3}],
        [None, _FrameRow(1, 2), None, _FrameRow(3, 4)],
    ],
    ids=["mapping", "dataclass"],
)
@pytest.mark.parametrize("use_iterator", [False, True], ids=["sequence", "iterator"])
def test_frame_rows_keep_null_positions_and_match_mapping_fields_by_name(
    rows: list[Any], use_iterator: bool
) -> None:
    source = DataFrameSource(iter(rows) if use_iterator else rows, tag_columns=["k"])
    assert source.as_table().to_pylist() == [
        {"k": None, "x": None},
        {"k": 1, "x": 2},
        {"k": None, "x": None},
        {"k": 3, "x": 4},
    ]


@pytest.mark.parametrize(
    "name",
    [
        "_",
        "_foo",
        "_tag_y",
        "_tag::y",
        "_source_y",
        "_content_hash",
        "_context_key",
        "__meta",
    ],
)
def test_list_source_refuses_reserved_value_column(name: str) -> None:
    with pytest.raises(InputValidationError, match=escape(repr(name))):
        ListSource(name, [10, 20])


@pytest.mark.parametrize("data", [[], [10, 20]], ids=["empty", "projected"])
def test_list_source_refuses_reserved_value_name_even_without_arrow_field(
    data: list[int],
) -> None:
    schema = {"element_index": int} if data else None
    with pytest.raises(InputValidationError, match="'_foo'.*drop them first"):
        ListSource("_foo", data, data_schema=schema)


def test_list_source_refuses_reserved_tags_omitted_from_schema() -> None:
    def tags(value: int, index: int) -> dict[str, int]:
        return (
            {"k": index, "g": value, "_foo": value}
            if index
            else {"k": index, "g": value, "_source_x": value}
        )

    with pytest.raises(
        InputValidationError, match="'_foo'.*'_source_x'.*drop them first"
    ):
        ListSource(
            "x",
            [10, 20],
            tag_function=tags,
            expected_tag_keys=TAGS,
            data_schema={"k": int, "g": int, "x": int},
        )


def test_list_source_refuses_reserved_tags() -> None:
    def tags(value: int, index: int) -> dict[str, int]:
        return {"k": index, "g": value, "_foo": value, "_content_hash": index}

    with pytest.raises(
        InputValidationError, match="'_content_hash'.*'_foo'.*drop them first"
    ):
        ListSource("value", [10, 20], tag_function=tags)


def test_rereading_an_orcapod_table_requires_dropping_system_columns() -> None:
    source = DictSource(
        [
            {"g": "a", "k": 1, "x": 3, "y": 4},
            {"g": "b", "k": 2, "x": 5, "y": 6},
        ],
        tag_columns=TAGS,
        tag_only_columns=True,
    )
    raw = source.as_table(all_info=True)
    system_columns = [name for name in raw.column_names if name.startswith("_")]
    with pytest.raises(InputValidationError, match="drop them first") as info:
        ArrowTableSource(raw, tag_columns=TAGS, tag_only_columns=True)
    assert system_columns
    assert all(repr(name) in str(info.value) for name in system_columns)
    rebuilt = ArrowTableSource(
        raw.drop(system_columns), tag_columns=TAGS, tag_only_columns=True
    )
    assert rebuilt.as_table().equals(source.as_table())


def test_database_refuses_reserved_columns_before_reading_rows() -> None:
    class SchemaOnlyConnector(_Connector):
        def iter_batches(self, query, params=None, batch_size=1000):
            pytest.fail("a reserved database column must prevent reading rows")

    table = _table(pa.field("_foo", pa.string()), ["a", "b"])
    with pytest.raises(InputValidationError, match="'_foo'.*drop them first"):
        DBTableSource(
            SchemaOnlyConnector(table), "rows", tag_columns=TAGS, source_id="rows"
        )


def test_source_accepts_underscores_after_the_first_character() -> None:
    source = DictSource(
        [{"foo_": "a", "a_b": 1, "tag_x": 2, "other": 3}],
        tag_columns=["foo_", "a_b"],
        tag_only_columns=True,
    )
    assert source.keys() == (("foo_", "a_b"), ("tag_x", "other"))


def _table(field: pa.Field[Any], values: list[Any]) -> pa.Table:
    """Two tags, two ordinary data columns and two additional columns."""
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("g", pa.string(), nullable=False),
            pa.field("x", pa.int64(), nullable=False),
            pa.field("y", pa.int64(), nullable=False),
            pa.field("rev", pa.string(), nullable=False),
            field,
        ]
    )
    return pa.Table.from_pylist(
        [
            dict(k=i, g="G", x=2, y=3, rev="r", **{field.name: value})
            for i, value in enumerate(values)
        ],
        schema=schema,
    )


@pytest.mark.parametrize("kind", SOURCE_KINDS)
@pytest.mark.parametrize(
    ("arrow_type", "nullable", "values"),
    [
        (pa.int64(), False, [7, 8]),
        (pa.string(), True, ["a", "b"]),
        (pa.string(), True, ["a", None]),
    ],
)
def test_rejects_invalid_custom_system_tag_columns(
    kind: str,
    arrow_type: pa.DataType,
    nullable: bool,
    values: list[Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = _table(pa.field("extra", arrow_type, nullable=nullable), values)
    with pytest.raises(InputValidationError, match=r"'extra'.*non-null.*text"):
        _source(kind, table, tmp_path, monkeypatch, system_tag_columns=["rev", "extra"])


@pytest.mark.parametrize("kind", SOURCE_KINDS)
@pytest.mark.parametrize(
    "names",
    [("missing",), ("extra", "missing", "rev"), ("rev", "missing", "extra")],
)
def test_refuses_missing_custom_system_tag_columns(
    kind: str,
    names: tuple[str, ...],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = _table(pa.field("extra", pa.string(), nullable=False), ["a", "b"])
    with pytest.raises(ValueError, match=r"system_tag_columns not found.*'missing'"):
        _source(kind, table, tmp_path, monkeypatch, system_tag_columns=names)


@pytest.mark.parametrize("kind", SOURCE_KINDS)
def test_accepts_two_non_nullable_string_system_tags(
    kind: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = _table(pa.field("extra", pa.string(), nullable=False), ["a", "b"])
    source = _source(
        kind, table, tmp_path, monkeypatch, system_tag_columns=["extra", "rev"]
    )
    tags, _ = source.output_schema(columns={"system_tags": True})
    assert {name: tags[name] for name in ("_tag_extra", "_tag_rev")} == {
        "_tag_extra": str,
        "_tag_rev": str,
    }


@pytest.mark.parametrize("kind", [kind for kind in SOURCE_KINDS if kind != "sqlite"])
def test_refuses_arrow_null_from_readers(
    kind: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    column = "unused"
    table = _table(pa.field(column, pa.null()), [None, None])
    with pytest.raises(
        InputValidationError, match=rf"{column!r}.*declare a supported nullable type"
    ):
        _source(kind, table, tmp_path, monkeypatch)


def test_dataframe_schema_can_type_null_values() -> None:
    table = _table(pa.field("extra", pa.null()), [None, None])
    schema = pa.schema(
        [f.with_type(pa.string()) if f.name == "extra" else f for f in table.schema]
    )
    source = DataFrameSource(
        pl.from_arrow(table), schema=schema, tag_columns=TAGS, tag_only_columns=True
    )
    assert source.as_table().schema.field("extra") == pa.field(
        "extra", pa.large_string()
    )


@pytest.mark.parametrize(
    "kind", ["arrow", "dict", "dataframe", "csv", "db", "postgres"]
)
def test_rejects_null_value_even_under_a_non_nullable_system_tag_field(
    kind: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = _table(pa.field("extra", pa.string(), nullable=False), ["a", None])
    with pytest.raises(InputValidationError, match=r"'extra'.*contains null values"):
        _source(kind, table, tmp_path, monkeypatch, system_tag_columns=["extra", "rev"])


def test_sqlite_sql_null_is_a_value_in_a_typed_column(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = _table(pa.field("extra", pa.string()), [None, None])
    source = _source("sqlite", table, tmp_path, monkeypatch)
    assert source.as_table().schema.field("extra") == pa.field(
        "extra", pa.large_string()
    )
    assert source.as_table().column("extra").to_pylist() == [None, None]


def test_list_source_declared_any_does_not_turn_into_a_string() -> None:
    with pytest.raises(
        InputValidationError, match=r"'x'.*declare a supported nullable type"
    ):
        ListSource(
            "x", [None, None], data_schema={"element_index": int, "x": Any | None}
        )


def test_no_declared_type_for_python_none_still_infers_a_string() -> None:
    sources = [
        ListSource("x", [None, None]),
        DictSource([{"x": None, "y": 1}, {"x": None, "y": 2}]),
    ]
    for source in sources:
        assert source.as_table().schema.field("x") == pa.field("x", pa.large_string())


class _Poll:
    """Minimal impl for testing schema validation before the first fetch."""

    def __init__(
        self, schema: pa.Schema | None, batch: pl.DataFrame | list[Any]
    ) -> None:
        self._schema = schema
        self._batch = batch

    def schema(self) -> pa.Schema | None:
        """Return the optional Arrow schema."""
        return self._schema

    def identity(self) -> str:
        """Return a stable identity for this poll."""
        return "test poll"

    def to_config(self) -> None:
        """This implementation holds a test batch in memory."""

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> _Poll:
        """Reject loading this in-memory implementation."""
        raise NotImplementedError

    async def fetch(
        self, cursor: Cursor[int] | None = None
    ) -> tuple[Cursor[int], pl.DataFrame | list[Any]]:
        """Return the prepared batch on the initial fetch."""
        return Cursor(value=0), self._batch

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        """There are no rows after the initial fetch."""
        return cursor is None

    async def close(self) -> None:
        """There is no connection to close."""


@pytest.mark.parametrize("rows, message", FRAME_ROW_CASES)
@pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
def test_polling_checks_named_row_shapes(
    rows: list[Any], message: str, use_async: bool
) -> None:
    source = PollingSource(
        _Poll(None, rows), ["k"], source_id="poll", record_id_columns="k"
    )

    async def read_first() -> None:
        async with contextlib.aclosing(source.async_iter_data()) as records:
            await anext(records, None)

    with pytest.raises(InputValidationError, match=message):
        if use_async:
            asyncio.run(read_first())
        else:
            source.as_table()


@pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("reverse", [False, True], ids=["foo-first", "source-first"])
@pytest.mark.parametrize("as_tag", [False, True], ids=["data", "tag"])
def test_polling_checks_raw_fields_beyond_polars_inference(
    use_async: bool, reverse: bool, as_tag: bool
) -> None:
    tags = (*TAGS, "_foo") if as_tag else TAGS
    source = PollingSource(
        _Poll(None, _late_reserved_rows(reverse)),
        tags,
        source_id="poll",
        record_id_columns="k",
    )

    async def read_first() -> None:
        async with contextlib.aclosing(source.async_iter_data()) as rows:
            await anext(rows)

    with pytest.raises(
        InputValidationError, match="'_foo'.*'_source_x'.*drop them first"
    ):
        if use_async:
            asyncio.run(read_first())
        else:
            source.check_for_new_data()


@pytest.mark.parametrize("declare", [True, False], ids=["schema", "batch"])
@pytest.mark.parametrize("as_tag", [True, False], ids=["tag", "data"])
def test_polling_refuses_reserved_schema_and_batch_columns(
    declare: bool, as_tag: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = _table(pa.field("_foo", pa.string()), ["a", "b"])
    table = table.append_column("_source_x", pa.array(["p", "q"]))
    batch = pl.from_arrow(table)
    assert isinstance(batch, pl.DataFrame)
    tags = (*TAGS, "_foo") if as_tag else TAGS
    if declare:

        async def unexpected_fetch(*args: Any, **kwargs: Any) -> None:
            pytest.fail("a reserved declared column must prevent polling")

        monkeypatch.setattr(_Poll, "fetch", unexpected_fetch)
    with pytest.raises(InputValidationError, match="drop them first") as info:
        source = PollingSource(
            _Poll(table.schema if declare else None, batch),
            tags,
            source_id="poll",
            record_id_columns="k",
        )
        source.check_for_new_data()
    assert "'_foo'" in str(info.value)
    assert "'_source_x'" in str(info.value)


@pytest.mark.parametrize("values", [[], [[None], []]])
def test_polling_refuses_null_in_declaration_and_batch(values: list[Any]) -> None:
    table = _table(pa.field("__unused", pa.large_list(pa.null())), values)
    batch = pl.from_arrow(table)
    assert isinstance(batch, pl.DataFrame)
    with pytest.raises(
        InputValidationError, match=r"'__unused'.*declare a supported nullable type"
    ):
        PollingSource(
            _Poll(table.schema, batch), TAGS, source_id="poll", record_id_columns="k"
        )
    poll = PollingSource(
        _Poll(None, batch), TAGS, source_id="poll", record_id_columns="k"
    )
    with pytest.raises(
        InputValidationError, match=r"'__unused'.*declare a supported nullable type"
    ):
        poll.check_for_new_data()


def test_a_raw_arrow_table_stream_preserves_nested_null() -> None:
    field = pa.field("extra", pa.large_list(pa.null()))
    table = _table(field, [[None], []])
    assert (
        ArrowTableStream(table, tag_columns=TAGS).as_table().schema.field("extra")
        == field
    )


def test_cached_source_refuses_an_existing_stored_null_column() -> None:
    table = _table(pa.field("extra", pa.string(), nullable=False), ["a", "b"])
    source = ArrowTableSource(
        table, tag_columns=TAGS, tag_only_columns=True, source_id="rows"
    )
    tag_schema, data_schema = source.output_schema()
    proxy = SourceProxy(
        source_id=source.source_id,
        content_hash_str=source.content_hash().to_string(),
        pipeline_hash_str=source.pipeline_hash().to_string(),
        tag_schema=tag_schema,
        data_schema=data_schema,
    )
    database = InMemoryArrowDatabase()
    cached = CachedSource(proxy, cache_database=database)
    raw = source.as_table(columns={"source": True, "system_tags": True})
    raw = raw.append_column("__unused", pa.nulls(2))
    raw = raw.append_column(CachedSource.HASH_COLUMN_NAME, pa.array([b"a", b"b"]))
    database.add_records(
        cached.cache_path,
        with_data_context(raw, source.data_context_key),
        record_id_column=CachedSource.HASH_COLUMN_NAME,
        flush=True,
    )
    with pytest.raises(
        InputValidationError, match=r"'__unused'.*declare a supported nullable type"
    ):
        cached.as_table()


@pytest.mark.parametrize("row_count", [0, 2])
def test_derived_source_refuses_stored_null_in_its_direct_table_view(
    row_count: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = _table(pa.field("extra", pa.string(), nullable=False), ["a", "b"])
    source = ArrowTableSource(
        table, tag_columns=TAGS, tag_only_columns=True, source_id="rows"
    )
    job = PipelineJob(name="derived", database=InMemoryArrowDatabase())
    with job:
        DropNulls(["x", "y"])(source, label="origin")
    node = job.nodes["origin"]
    stored = (
        source.as_table()
        .slice(0, row_count)
        .append_column("__unused", pa.nulls(row_count))
    )
    monkeypatch.setattr(node, "get_all_records", lambda *args, **kwargs: stored)
    derived = node.as_source()
    with pytest.raises(
        InputValidationError, match=r"'__unused'.*declare a supported nullable type"
    ):
        derived.as_table()


@pytest.mark.parametrize("copy_tags", [False, True])
def test_derived_source_checks_reserved_columns_before_filtering_storage(
    copy_tags: bool,
) -> None:
    class NullMetadataStore(InMemoryArrowDatabase):
        """Inject a reserved Arrow Null column at the database read boundary."""

        def get_all_records(
            self, record_path: tuple[str, ...], *args: Any, **kwargs: Any
        ) -> pa.Table | None:
            """Add a reserved column to the stored operator table."""
            table = super().get_all_records(record_path, *args, **kwargs)
            if table is not None and record_path[-1] == "odb_v2":
                return table.append_column("__raw_missing", pa.nulls(table.num_rows))
            return table

    source = ArrowTableSource(
        _table(pa.field("extra", pa.string(), nullable=False), ["a", "b"]),
        tag_columns=TAGS,
        tag_only_columns=True,
        source_id="rows",
    )
    store = NullMetadataStore()
    job = PipelineJob(name="derived", database=store)
    with job:
        DropNulls(["x", "y"])(source, label="origin")
    node = job.nodes["origin"]
    assert isinstance(node, OperatorJobNode)
    node.attach_databases(pipeline_database=store, cache_mode=CacheMode.LOG)
    job.run()
    derived = (
        node.as_source(tags_as_data={"k": "copied"}, source_id="new")
        if copy_tags
        else node.as_source()
    )
    with pytest.raises(
        InputValidationError,
        match=r"'__raw_missing'.*declare a supported nullable type",
    ):
        derived.as_table()


def test_spiral_can_nominate_a_tag_copy_as_a_custom_system_tag(
    tmp_path: Path,
) -> None:
    schema = pa.schema(
        [pa.field("k", pa.string(), False), pa.field("x", pa.int8(), False)]
    )
    table = pa.Table.from_pylist([{"k": "a", "x": 1}, {"k": "b", "x": 2}], schema)
    with fake_spiral_sdk.install(tmp_path):
        external = fake_spiral_sdk.ExternalProject("project").create_table(
            "default.rows", key_schema=pa.schema([schema.field("k")])
        )
        external.write(table)
        external.record(schema)
        source = SpiralDBTableSource(
            "project",
            "rows",
            tag_columns=["k"],
            tag_only_columns=True,
            tags_as_data={"k": "copy"},
            system_tag_columns=["copy"],
        )
        columns = {"system_tags": True}
        assert source.as_table(columns=columns).column("_tag_copy").to_pylist() == [
            "a",
            "b",
        ]
        assert [tag.system_tags()["_tag_copy"] for tag, _ in source.iter_data()] == [
            "a",
            "b",
        ]


def test_spiral_validates_reserved_null_in_an_empty_scan_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = pa.schema(
        [pa.field("k", pa.string(), False), pa.field("x", pa.int8(), False)]
    )
    table = pa.Table.from_pylist([{"k": "a", "x": 1}], schema)
    with fake_spiral_sdk.install(tmp_path):
        external = fake_spiral_sdk.ExternalProject("project").create_table(
            "default.rows", key_schema=pa.schema([schema.field("k")])
        )
        external.write(table)
        external.record(schema)
        source = SpiralDBTableSource("project", "rows", tag_only_columns=True)
        handle = source._lookup_table()  # pyright: ignore[reportPrivateUsage]  # intercept the scan boundary
        invalid = schema.append(pa.field("__unused", pa.null()))
        batch = pa.RecordBatch.from_arrays(
            [pa.array([], f.type) for f in invalid], schema=invalid
        )
        monkeypatch.setattr(source, "_lookup_table", lambda: handle)
        monkeypatch.setattr(handle, "scan_batches", lambda **kwargs: iter([batch]))
        with pytest.raises(
            InputValidationError, match=r"'__unused'.*declare a supported nullable type"
        ):
            source.as_table()
