"""System-tag validation on raw Arrow streams (ITL-1341)."""

from __future__ import annotations

import re

import pyarrow as pa
import pytest

from orcapod.core.operators import Join
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.system_constants import constants
from orcapod.utils.arrow_utils import add_system_tag_columns, system_tag_column_names

SOURCE_ID, RECORD_ID = system_tag_column_names("origin")


@pytest.mark.parametrize(
    ("dtype", "values", "nullable", "message"),
    [
        (pa.float64(), [1.5], False, "string"),
        (pa.string(), [None], False, "null values"),
        (pa.string(), [None], True, "null values"),
    ],
)
@pytest.mark.parametrize("nominated", [False, True])
def test_invalid_system_tags_are_refused_at_construction(
    dtype: pa.DataType,
    values: list[object],
    nullable: bool,
    message: str,
    nominated: bool,
) -> None:
    """Both ways of defining a system tag validate its field and values."""
    name = "st" if nominated else f"{constants.SYSTEM_TAG_PREFIX}st"
    table = add_system_tag_columns(pa.table({"x": [1]}), "origin", "source", ["row"])
    table = table.append_column(
        pa.field(name, dtype, nullable=nullable), pa.array(values, type=dtype)
    )
    with pytest.raises(InputValidationError, match=rf"_tag_st.*{message}"):
        ArrowTableStream(table, system_tag_columns=[name] if nominated else ())


@pytest.mark.parametrize("name", [SOURCE_ID, RECORD_ID])
def test_a_system_tag_id_requires_its_partner(name: str) -> None:
    """Report the present column when either partner is missing."""
    table = pa.table({"x": [1]}).append_column(
        pa.field(name, pa.string(), nullable=False), pa.array(["id"])
    )
    with pytest.raises(InputValidationError, match=re.escape(name)):
        ArrowTableStream(table)


def test_pairing_uses_the_full_provenance_path() -> None:
    """A second source ID cannot be paired with a record ID of another path."""
    table = add_system_tag_columns(pa.table({"x": [1]}), "origin", "source", ["row"])
    unpaired, _ = system_tag_column_names("origin::branch:1")
    table = table.append_column(
        pa.field(unpaired, pa.string(), nullable=False), pa.array(["id"])
    )
    with pytest.raises(InputValidationError, match=re.escape(unpaired)):
        ArrowTableStream(table)


def test_nullable_string_field_is_preserved_when_values_are_present() -> None:
    """Wrapping a raw stream preserves the declaration of a valid string tag."""
    name = f"{constants.SYSTEM_TAG_PREFIX}label"
    table = pa.table({"x": [1], name: ["custom"]})
    stream = ArrowTableStream(table)
    result = stream.as_table(columns={"system_tags": True})
    assert result.schema.field(name).equals(pa.field(name, pa.large_string()))


def test_join_extends_complete_pairs_and_custom_system_tags() -> None:
    """Joined output can carry multiple valid pairs and preserve custom tags."""
    left_table = add_system_tag_columns(pa.table({"x": [1]}), "left", "l", ["row"])
    left_table = left_table.append_column(
        pa.field("label", pa.string_view(), nullable=False),
        pa.array(["custom"], type=pa.string_view()),
    )
    right_table = add_system_tag_columns(pa.table({"y": [2]}), "right", "r", ["row"])
    left = ArrowTableStream(left_table, system_tag_columns=["label"])
    right = ArrowTableStream(right_table)
    joined = Join()(left, right).as_table(columns={"system_tags": True})
    system_names = [
        name
        for name in joined.column_names
        if name.startswith(constants.SYSTEM_TAG_PREFIX)
    ]
    assert len(system_names) == 5
    custom = next(name for name in system_names if name.startswith("_tag_label::"))
    assert joined.column(custom).to_pylist() == ["custom"]
