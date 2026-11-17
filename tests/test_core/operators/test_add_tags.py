"""``AddTags`` / ``stream.add_tags``: tags with constant values on every row (ITL-1196)."""

from __future__ import annotations

import datetime
import decimal
import math
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.operators import AddTags, PolarsFilter
from orcapod.core.sources import ArrowTableSource
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants


def _readings() -> ArrowTableSource:
    """Tag ``subject``; data ``x`` (int8, nullable) and ``name``."""
    table = pa.table(
        {
            "subject": pa.array(["a", "b", "c"], pa.large_string()),
            "x": pa.array([1, None, 3], pa.int8()),
            "name": pa.array(["p", "q", "r"], pa.large_string()),
        }
    )
    return ArrowTableSource(
        table, tag_columns=["subject"], tag_only_columns=True, source_id="readings"
    )


def _system_and_source(table: pa.Table) -> pa.Table:
    """The system-tag, source-info and data columns of ``table``."""
    return table.select(
        [
            n
            for n in table.column_names
            if n.startswith((constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX))
            or n in ("x", "name")
        ]
    )


ALL_INFO: dict[str, Any] = {"source": True, "system_tags": True, "sort_by_tags": False}


class TestValues:
    def test_every_row_gets_the_values(self) -> None:
        out = _readings().add_tags(cohort="A", run=3)
        table = out.as_table()
        assert table.column("cohort").to_pylist() == ["A", "A", "A"]
        assert table.column("run").to_pylist() == [3, 3, 3]

    def test_new_tags_follow_the_input_tags_in_the_order_given(self) -> None:
        out = _readings().add_tags(run=3, cohort="A")
        assert out.keys() == (("subject", "run", "cohort"), ("x", "name"))
        assert out.as_table().column_names[:3] == ["subject", "run", "cohort"]

    def test_arrow_type_of_each_accepted_value_type(self) -> None:
        out = _readings().add_tags(b=True, i=3, f=1.5, s="A", raw=b"\x01")
        schema = out.as_table().schema
        assert schema.field("b").type == pa.bool_()
        assert schema.field("i").type == pa.int64()
        assert schema.field("f").type == pa.float64()
        assert schema.field("s").type == pa.large_string()
        assert schema.field("raw").type == pa.large_binary()
        assert not any(schema.field(n).nullable for n in ("b", "i", "f", "s", "raw"))

    def test_predicted_schema(self) -> None:
        out = _readings().add_tags(b=True, i=3, f=1.5, s="A", raw=b"\x01")
        tag_schema, data_schema = out.output_schema()
        assert dict(tag_schema) == {
            **_readings().output_schema()[0],
            "b": bool,
            "i": int,
            "f": float,
            "s": str,
            "raw": bytes,
        }
        assert dict(data_schema) == dict(_readings().output_schema()[1])

    def test_predicted_order_with_system_tags_matches_the_table(self) -> None:
        out = _readings().add_tags(cohort="A", run=3)
        predicted = list(out.output_schema(columns={"system_tags": True})[0])
        table = out.as_table(columns={"system_tags": True})
        assert predicted[:3] == ["subject", "cohort", "run"]
        assert predicted == [n for n in table.column_names if n in predicted]

    def test_data_system_tags_and_source_info_are_unchanged(self) -> None:
        source = _readings()
        out = source.add_tags(cohort="A")
        assert _system_and_source(out.as_table(columns=ALL_INFO)).equals(
            _system_and_source(source.as_table(columns=ALL_INFO))
        )

    def test_dict_form_equals_keyword_form(self) -> None:
        by_dict = _readings().add_tags({"cohort": "A", "run": 3})
        by_keyword = _readings().add_tags(cohort="A", run=3)
        assert by_dict.as_table().equals(by_keyword.as_table())
        assert by_dict.content_hash() == by_keyword.content_hash()

    def test_a_tag_named_label_through_the_dict(self) -> None:
        out = _readings().add_tags({"label": "x"}, label="node")
        assert "label" in out.keys()[0]

    def test_without_input_tags(self) -> None:
        source = ArrowTableSource(
            pa.table({"x": pa.array([1, 2], pa.int8())}), source_id="tagless"
        )
        out = source.add_tags(cohort="A")
        assert out.keys() == (("cohort",), ("x",))
        assert out.as_table().column("cohort").to_pylist() == ["A", "A"]


def _no_rows(stream: StreamProtocol) -> StreamProtocol:
    """``stream`` behind a filter that matches no row."""
    return PolarsFilter(constraints={"subject": "none"})(stream)


def test_zero_row_result_has_the_types_of_a_populated_one() -> None:
    op = AddTags({"cohort": "A", "run": 3})
    populated = op(_readings()).as_table(columns=ALL_INFO)
    empty = op(_no_rows(_readings())).as_table(columns=ALL_INFO)
    assert empty.num_rows == 0
    assert empty.schema == populated.schema


class TestIdentity:
    def test_different_values_are_different_operators(self) -> None:
        hashes = {
            AddTags({"run": v}).content_hash()  # pyright: ignore[reportArgumentType]  # every value is accepted
            for v in (1, True, 1.0, "1", b"1", 2)
        }
        assert len(hashes) == 6

    def test_order_is_part_of_the_identity(self) -> None:
        assert (
            AddTags({"a": 1, "b": 2}).content_hash()
            != AddTags({"b": 2, "a": 1}).content_hash()
        )

    def test_names_are_part_of_the_identity(self) -> None:
        assert AddTags({"a": 1}).content_hash() != AddTags({"b": 1}).content_hash()

    def test_pipeline_hash_differs_by_value(self) -> None:
        a = _readings().add_tags(cohort="A")
        b = _readings().add_tags(cohort="B")
        assert a.pipeline_hash() != b.pipeline_hash()


def test_tags_are_read_only() -> None:
    op = AddTags({"cohort": "A"})
    with pytest.raises(TypeError):
        op.tags["cohort"] = "B"  # pyright: ignore[reportIndexIssue]  # the refusal under test
    with pytest.raises(AttributeError):
        op.tags = {"cohort": "B"}  # pyright: ignore[reportAttributeAccessIssue]  # the refusal under test
    assert dict(op.tags) == {"cohort": "A"}


class TestRefusals:
    def test_no_tag_given(self) -> None:
        with pytest.raises(ValueError, match="no tag given"):
            _readings().add_tags()

    def test_both_dict_and_keywords(self) -> None:
        with pytest.raises(TypeError, match="not both"):
            _readings().add_tags({"a": 1}, b=2)

    @pytest.mark.parametrize("name", ["subject", "x", "name"])
    def test_existing_column_name(self, name: str) -> None:
        with pytest.raises(InputValidationError, match=repr(name)):
            _readings().add_tags({name: "A"})

    @pytest.mark.parametrize(
        "name", ["__meta", "_tag_x", "_source_x", "_context_key", "a::b", ":a"]
    )
    def test_reserved_name(self, name: str) -> None:
        with pytest.raises(InputValidationError):
            AddTags({name: "A"})

    def test_empty_name(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            AddTags({"": "A"})

    def test_name_not_a_string(self) -> None:
        with pytest.raises(TypeError, match="strings"):
            AddTags({1: "A"})  # pyright: ignore[reportArgumentType]  # the refusal under test

    @pytest.mark.parametrize(
        "value",
        [
            None,
            decimal.Decimal("1.5"),
            datetime.date(2026, 1, 1),
            datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            [1],
            {"a": 1},
            bytearray(b"x"),
        ],
    )
    def test_unsupported_type_names_the_tag(self, value: Any) -> None:
        with pytest.raises(TypeError, match="'cohort'"):
            AddTags({"cohort": value})

    def test_subclass_of_an_accepted_type(self) -> None:
        class Label(str):
            pass

        with pytest.raises(TypeError, match="'cohort'"):
            AddTags({"cohort": Label("A")})

    @pytest.mark.parametrize("value", [2**63, -(2**63) - 1])
    def test_int_outside_int64(self, value: int) -> None:
        with pytest.raises(ValueError, match="int64"):
            AddTags({"run": value})

    @pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
    def test_non_finite_float(self, value: float) -> None:
        with pytest.raises(ValueError, match="finite"):
            AddTags({"w": value})


class TestConfig:
    def test_round_trip(self) -> None:
        op = AddTags({"b": True, "i": 3, "f": 1.5, "s": "A", "raw": b"\x01"})
        config = op.to_config()
        assert config["class_name"] == "AddTags"
        assert config["config"]["tags"][4] == {
            "name": "raw",
            "type": "bytes",
            "value": "01",
        }
        restored = AddTags.from_config(config)
        assert restored.tags == op.tags
        assert [type(v) for v in restored.tags.values()] == [
            bool,
            int,
            float,
            str,
            bytes,
        ]
        assert restored.content_hash() == op.content_hash()

    def test_unknown_saved_type(self) -> None:
        config = AddTags({"cohort": "A"}).to_config()
        config["config"]["tags"][0]["type"] = "date"
        with pytest.raises(ValueError, match="'date'"):
            AddTags.from_config(config)
