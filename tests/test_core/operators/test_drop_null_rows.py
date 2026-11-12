"""``DropNullRows``: drop rows with a null in a named column, narrow ``T | None`` to ``T`` (ITL-943).

The operator keeps the rows where every named column is non-null and reports each
named column as non-nullable afterwards, in ``output_schema()`` and in the Arrow
table. Everything else passes through: other columns' types and nullability,
system tags, source info and row order.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from pathlib import Path
from typing import Any, TypeAlias

import numpy as np
import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.sources import ArrowTableSource, DictSource
from orcapod.errors import InputValidationError
from orcapod.operators import DropNullRows, Join
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants

Column: TypeAlias = "pa.Array[Any] | pa.ChunkedArray[Any]"

ALL = {"source": True, "system_tags": True}


def _source(
    columns: dict[str, Column],
    tags: Sequence[str] = ("id",),
    nullable: Sequence[str] = (),
    source_id: str = "src",
) -> ArrowTableSource:
    """Build a source; every column not listed in ``nullable`` is non-nullable."""
    table = pa.table(list(columns.values()), names=list(columns))
    schema = pa.schema([f.with_nullable(f.name in nullable) for f in table.schema])
    return ArrowTableSource(
        table.cast(schema), tag_columns=list(tags), source_id=source_id
    )


def readings(nullable: Sequence[str] = ("x", "y")) -> ArrowTableSource:
    """Four rows; ``x`` is null in row 2, ``y`` in row 3; ``note`` is nullable too."""
    return _source(
        {
            "id": pa.array([1, 2, 3, 4], pa.int16()),
            "x": pa.array([10, None, 30, 40], pa.int8()),
            "y": pa.array([1.5, 2.5, None, 4.5], pa.float32()),
            "note": pa.array(["a", None, "c", None], pa.string()),
        },
        nullable=[*nullable, "note"],
    )


def _ids(stream: StreamProtocol) -> list[Any]:
    return stream.as_table().column("id").to_pylist()


# ── rows ────────────────────────────────────────────────────────────────────


def test_drops_rows_where_the_column_is_null() -> None:
    result = DropNullRows("x")(readings())
    assert result.as_table().to_pylist() == [
        {"id": 1, "x": 10, "y": 1.5, "note": "a"},
        {"id": 3, "x": 30, "y": None, "note": "c"},
        {"id": 4, "x": 40, "y": 4.5, "note": None},
    ]


def test_every_named_column_must_be_non_null() -> None:
    assert _ids(DropNullRows(["x", "y"])(readings())) == [1, 4]


def test_row_order_is_preserved() -> None:
    source = _source(
        {
            "id": pa.array([4, 1, 3, 2], pa.int16()),
            "x": pa.array([40, 10, None, 20], pa.int8()),
        },
        nullable=["x"],
    )
    assert [tag["id"] for tag, _ in DropNullRows("x")(source).iter_data()] == [4, 1, 2]


def test_nan_is_not_null() -> None:
    source = _source(
        {
            "id": pa.array([1, 2, 3], pa.int16()),
            "v": pa.array([1.0, float("nan"), None], pa.float64()),
        },
        nullable=["v"],
    )
    result = DropNullRows("v")(source)
    first, second = result.as_table().column("v").to_pylist()
    assert first == 1.0
    assert isinstance(second, float) and math.isnan(second)
    assert result.as_table().schema.field("v").nullable is False
    assert result.output_schema()[1]["v"] is float


# ── types ───────────────────────────────────────────────────────────────────


def test_output_schema_narrows_only_the_named_columns() -> None:
    source = readings()
    assert source.output_schema()[1] == {
        "x": int | None,
        "y": float | None,
        "note": str | None,
    }
    tag_schema, data_schema = DropNullRows("x")(source).output_schema()
    assert tag_schema == {"id": int}
    assert data_schema == {"x": int, "y": float | None, "note": str | None}


def test_table_narrows_only_the_named_columns() -> None:
    source = readings()
    before = source.as_table(columns=ALL).schema
    after = DropNullRows("x")(source).as_table(columns=ALL).schema
    assert after.names == before.names
    assert after.field("x") == pa.field("x", pa.int8(), nullable=False)
    # Width and nullability are kept; ``string`` takes its large variant.
    assert after.field("y") == pa.field("y", pa.float32(), nullable=True)
    assert after.field("note") == pa.field("note", pa.large_string(), nullable=True)
    for name in before.names:
        if name not in ("x", "note"):
            assert after.field(name) == before.field(name)


def test_schema_prediction_matches_the_computed_stream() -> None:
    source = readings()
    op = DropNullRows(["x", "id"])
    for config in ({}, {"system_tags": True}, {"source": True, "system_tags": True}):
        # Non-public on purpose: a prediction is verified against the static result.
        computed = op.unary_static_process(source).output_schema(columns=config)
        assert op(source).output_schema(columns=config) == computed
        assert op.output_schema(source, columns=config) == computed


def test_zero_surviving_rows_keep_the_narrowed_types() -> None:
    source = _source(
        {
            "id": pa.array([1, 2], pa.int16()),
            "x": pa.array([None, None], pa.int8()),
            "s": pa.array(["a", "b"], pa.string()),
        },
        nullable=["x", "s"],
    )
    non_empty = _source(
        {
            "id": pa.array([1, 2], pa.int16()),
            "x": pa.array([5, None], pa.int8()),
            "s": pa.array(["a", "b"], pa.string()),
        },
        nullable=["x", "s"],
    )
    result = DropNullRows("x")(source)
    table = result.as_table(columns=ALL)
    assert table.num_rows == 0
    assert list(result.iter_data()) == []
    assert table.schema.field("x") == pa.field("x", pa.int8(), nullable=False)
    assert result.output_schema() == ({"id": int}, {"x": int, "s": str | None})
    expected = DropNullRows("x")(non_empty).as_table(columns=ALL).schema
    assert table.schema == expected


def test_already_non_nullable_column_is_a_no_op() -> None:
    source = _source(
        {
            "id": pa.array([1, 2], pa.int16()),
            "x": pa.array([10, 20], pa.int8()),
        }
    )
    result = DropNullRows("x")(source)
    assert result.output_schema() == source.output_schema()
    assert result.as_table(columns=ALL) == source.as_table(columns=ALL)
    for (tag, data), (in_tag, in_data) in zip(result.iter_data(), source.iter_data()):
        assert tag.content_hash() == in_tag.content_hash()
        assert data.content_hash() == in_data.content_hash()


def test_applying_twice_equals_applying_once() -> None:
    once = DropNullRows("x")(readings())
    twice = DropNullRows("x")(once)
    assert twice.as_table(columns=ALL) == once.as_table(columns=ALL)
    assert twice.output_schema() == once.output_schema()


# ── tag columns ─────────────────────────────────────────────────────────────


def keyed(nullable: Sequence[str] = ("site", "x")) -> ArrowTableSource:
    """Tags ``site`` (null in row 2) and ``id``; ``x`` is null in row 3."""
    return _source(
        {
            "site": pa.array(["a", None, "c"], pa.large_string()),
            "id": pa.array([1, 2, 3], pa.int16()),
            "x": pa.array([10, 20, None], pa.int8()),
        },
        tags=("site", "id"),
        nullable=nullable,
    )


def test_tag_column_is_narrowed() -> None:
    source = keyed()
    assert source.output_schema()[0] == {"site": str | None, "id": int}
    result = DropNullRows("site")(source)
    assert _ids(result) == [1, 3]
    assert result.keys() == source.keys()
    assert result.output_schema() == (
        {"site": str, "id": int},
        {"x": int | None},
    )
    assert result.as_table().schema.field("site").nullable is False
    assert result.as_table().schema.field("x").nullable is True


def test_tag_and_data_columns_in_one_call() -> None:
    result = DropNullRows(["x", "site"])(keyed())
    assert _ids(result) == [1]
    assert result.output_schema() == ({"site": str, "id": int}, {"x": int})


def test_narrowed_nullable_key_joins_with_a_non_nullable_key() -> None:
    """A source whose key columns are nullable by specification (as a Spiral table's
    are) cannot join a source whose keys are ``T``; after ``DropNullRows`` it can."""
    events = _source(
        {
            "session": pa.array(["s1", None, "s3"], pa.large_string()),
            "start": pa.array([100, 200, 300], pa.int64()),
        },
        tags=("session",),
        nullable=["session", "start"],
        source_id="events",
    )
    sessions = _source(
        {
            "session": pa.array(["s1", "s3", "s4"], pa.large_string()),
            "animal": pa.array(["m1", "m3", "m4"], pa.large_string()),
        },
        tags=("session",),
        source_id="sessions",
    )
    with pytest.raises(InputValidationError, match="not compatible"):
        Join()(events, sessions)

    joined = Join()(DropNullRows("session")(events), sessions)
    assert joined.output_schema() == (
        {"session": str},
        {"start": int | None, "animal": str},
    )
    rows = sorted(joined.as_table().to_pylist(), key=lambda r: r["session"])
    assert rows == [
        {"session": "s1", "start": 100, "animal": "m1"},
        {"session": "s3", "start": 300, "animal": "m3"},
    ]


def test_stream_without_tag_columns() -> None:
    source = _source(
        {
            "x": pa.array([1, None, 3], pa.int8()),
            "s": pa.array(["a", "b", "c"], pa.string()),
        },
        tags=(),
        nullable=["x", "s"],
    )
    result = DropNullRows("x")(source)
    assert result.as_table().to_pylist() == [{"x": 1, "s": "a"}, {"x": 3, "s": "c"}]
    assert result.output_schema() == ({}, {"x": int, "s": str | None})


# ── nested and logical columns ──────────────────────────────────────────────

ITEMS = pa.large_list(pa.field("item", pa.int8(), nullable=True))
POINT = pa.struct([pa.field("a", pa.int8()), pa.field("b", pa.large_string())])


def nested() -> ArrowTableSource:
    return _source(
        {
            "id": pa.array([1, 2, 3, 4], pa.int16()),
            "items": pa.array([[1, None], None, [], [4]], ITEMS),
            "point": pa.array(
                [{"a": 1, "b": None}, {"a": 2, "b": "x"}, None, {"a": None, "b": None}],
                POINT,
            ),
        },
        nullable=["items", "point"],
    )


def test_only_the_top_level_value_is_tested_for_a_list() -> None:
    result = DropNullRows("items")(nested())
    # A list holding a null and an empty list are not null lists.
    assert result.as_table().column("items").to_pylist() == [[1, None], [], [4]]
    field = result.as_table().schema.field("items")
    assert field.nullable is False
    assert field.type == ITEMS  # the item field keeps its name and nullability
    # The Python schema does not express item nullability; the Arrow type above does.
    assert nested().output_schema()[1]["items"] == list[int] | None
    assert result.output_schema()[1]["items"] == list[int]


def test_only_the_top_level_value_is_tested_for_a_struct() -> None:
    result = DropNullRows("point")(nested())
    # A struct whose children are all null is not a null struct.
    assert _ids(result) == [1, 2, 4]
    field = result.as_table().schema.field("point")
    assert field.nullable is False
    assert field.type == POINT  # children stay nullable


def logical() -> DictSource:
    return DictSource(
        data=[
            {"k": "a", "p": Path("/a"), "arr": np.arange(3)},
            {"k": "b", "p": None, "arr": np.arange(2)},
            {"k": "c", "p": Path("/c"), "arr": None},
        ],
        tag_columns=["k"],
        source_id="logical",
    )


def test_logical_columns_keep_their_logical_type() -> None:
    source = logical()
    before = source.as_table().schema
    assert source.output_schema()[1] == {
        "p": Path | None,
        "arr": np.ndarray | None,  # pyright: ignore[reportMissingTypeArgument]  # the schema holds the bare class
    }

    result = DropNullRows(["p", "arr"])(source)
    assert result.output_schema()[1] == {"p": Path, "arr": np.ndarray}
    after = result.as_table().schema
    for name in ("p", "arr"):
        assert isinstance(after.field(name).type, pa.BaseExtensionType)
        assert after.field(name).type == before.field(name).type
        assert after.field(name).nullable is False
    ((tag, data),) = list(result.iter_data())
    assert tag["k"] == "a"
    assert data["p"] == Path("/a")
    array = data["arr"]
    assert isinstance(array, np.ndarray)
    assert array.tolist() == [0, 1, 2]


# ── arguments ───────────────────────────────────────────────────────────────


def test_missing_column_is_an_input_validation_error() -> None:
    with pytest.raises(InputValidationError, match="nope"):
        DropNullRows(["x", "nope"])(readings())


def test_system_and_source_columns_cannot_be_named() -> None:
    source = readings()
    hidden = [
        n
        for n in source.as_table(columns=ALL).column_names
        if n.startswith((constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX))
    ]
    assert len(hidden) >= 2
    for name in hidden:
        with pytest.raises(InputValidationError):
            DropNullRows(name)(source)


def test_no_column_is_an_error_at_construction() -> None:
    with pytest.raises(ValueError, match="at least one"):
        DropNullRows([])


# ── identity ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "columns",
    [["x", "y"], ("x", "y"), {"x", "y"}, ["y", "x"], ["x", "y", "x"], frozenset("xy")],
    ids=["list", "tuple", "set", "reversed", "duplicate", "frozenset"],
)
def test_identity_is_the_set_of_names(columns: Any) -> None:
    reference = DropNullRows(["x", "y"])
    op = DropNullRows(columns)
    assert op.columns == ("x", "y")
    assert op.content_hash() == reference.content_hash()
    assert op.pipeline_hash() == reference.pipeline_hash()
    source = readings()
    assert op(source).content_hash() == reference(source).content_hash()
    assert op(source).pipeline_hash() == reference(source).pipeline_hash()


def test_a_bare_name_is_one_column() -> None:
    assert DropNullRows("note").columns == ("note",)
    assert DropNullRows("x").content_hash() == DropNullRows(["x"]).content_hash()


def test_different_names_are_different_identities() -> None:
    source = readings()
    assert DropNullRows("x").content_hash() != DropNullRows("y").content_hash()
    assert DropNullRows("x").content_hash() != DropNullRows(["x", "y"]).content_hash()
    assert (
        DropNullRows("x")(source).pipeline_hash()
        != DropNullRows("y")(source).pipeline_hash()
    )
    assert DropNullRows("x")(source).pipeline_hash() != source.pipeline_hash()


def test_the_caller_s_collection_is_not_kept_or_mutated() -> None:
    names = ["y", "x"]
    op = DropNullRows(names)
    names.append("z")
    assert names == ["y", "x", "z"]
    assert op.columns == ("x", "y")


# ── hashes and provenance of surviving rows ─────────────────────────────────


def test_narrowed_rows_hash_as_rows_declared_non_nullable() -> None:
    """Nullability is part of the Arrow schema hash, so narrowing a data column
    changes the row's ``Data`` hash, to the hash the same row has in a stream
    that declared the column non-nullable from the start. Tags are untouched."""
    columns: dict[str, Column] = {
        "id": pa.array([1, 3], pa.int16()),
        "x": pa.array([10, 30], pa.int8()),
    }
    declared = _source(columns)
    nullable = _source(
        {
            "id": pa.array([1, 2, 3], pa.int16()),
            "x": pa.array([10, None, 30], pa.int8()),
        },
        nullable=["x"],
    )
    input_rows = {tag["id"]: (tag, data) for tag, data in nullable.iter_data()}
    narrowed = list(DropNullRows("x")(nullable).iter_data())
    assert len(narrowed) == 2
    for (tag, data), (ref_tag, ref_data) in zip(narrowed, declared.iter_data()):
        in_tag, in_data = input_rows[tag["id"]]
        assert data.content_hash() == ref_data.content_hash()
        assert data.content_hash() != in_data.content_hash()
        assert tag.content_hash() == in_tag.content_hash() == ref_tag.content_hash()


def test_narrowing_a_tag_changes_the_tag_hash_only() -> None:
    source = keyed()
    input_rows = {tag["id"]: (tag, data) for tag, data in source.iter_data()}
    for tag, data in DropNullRows("site")(source).iter_data():
        in_tag, in_data = input_rows[tag["id"]]
        assert tag.content_hash() != in_tag.content_hash()
        assert data.content_hash() == in_data.content_hash()


def test_system_tags_and_source_info_pass_through() -> None:
    source = readings()
    before = source.as_table(columns=ALL)
    after = DropNullRows(["x", "y"])(source).as_table(columns=ALL)
    hidden = [
        n
        for n in before.column_names
        if n.startswith((constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX))
    ]
    assert hidden and after.column_names == before.column_names
    kept = before.take(pa.array([0, 3]))
    for name in hidden:
        assert after.schema.field(name) == before.schema.field(name)
        assert after.column(name).to_pylist() == kept.column(name).to_pylist()

    input_rows = {tag["id"]: (tag, data) for tag, data in source.iter_data()}
    for tag, data in DropNullRows(["x", "y"])(source).iter_data():
        in_tag, in_data = input_rows[tag["id"]]
        assert tag.system_tags() == in_tag.system_tags()
        assert data.source_info() == in_data.source_info()


def test_the_input_stream_is_not_changed() -> None:
    source = readings()
    schema = source.as_table(columns=ALL).schema
    rows = source.as_table(columns=ALL).to_pylist()
    DropNullRows(["x", "y"])(source).as_table()
    assert source.as_table(columns=ALL).schema == schema
    assert source.as_table(columns=ALL).to_pylist() == rows


# ── stream method ───────────────────────────────────────────────────────────


def test_stream_method_equals_the_operator() -> None:
    source = readings()
    by_method = source.drop_null_rows(["x", "y"])
    by_operator = DropNullRows(["x", "y"])(source)
    assert by_method.as_table(columns=ALL) == by_operator.as_table(columns=ALL)
    assert by_method.content_hash() == by_operator.content_hash()
    assert by_method.pipeline_hash() == by_operator.pipeline_hash()
    assert _ids(source.drop_null_rows("x")) == [1, 3, 4]


# ── downstream ──────────────────────────────────────────────────────────────


@function_pod(output_keys="doubled")
def double_x(x: int) -> int:
    """A pod with a non-optional parameter."""
    return x * 2


def test_pod_with_a_non_optional_parameter_runs_only_after_the_operator() -> None:
    source = _source(
        {
            "id": pa.array([1, 2, 3], pa.int16()),
            "x": pa.array([10, None, 30], pa.int64()),
        },
        nullable=["x"],
    )
    with pytest.raises(ValueError, match="not compatible"):
        double_x.pod(source)

    result = double_x.pod(source.drop_null_rows("x"))
    assert result.as_table().to_pylist() == [
        {"id": 1, "doubled": 20},
        {"id": 3, "doubled": 60},
    ]


def test_join_after_the_operator() -> None:
    weights = _source(
        {
            "id": pa.array([1, 2, 3, 4], pa.int16()),
            "w": pa.array([1.0, 2.0, 3.0, 4.0], pa.float64()),
        },
        source_id="weights",
    )
    joined = Join()(DropNullRows("x")(readings()), weights)
    assert joined.output_schema()[1]["x"] is int
    assert sorted(_ids(joined)) == [1, 3, 4]
    assert joined.as_table().schema.field("x").nullable is False
