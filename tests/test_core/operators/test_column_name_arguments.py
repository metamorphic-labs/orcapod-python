"""A bare string is one column name wherever an operator takes column names (ITL-1221).

``GroupBy("subject")`` used to iterate the string and group on the columns ``'s'``,
``'u'``, ... ; a one-character name worked by accident. Every operator that takes a
collection of column names now reads a bare string as one name, through one rule
(``schema_utils.column_names``).
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.operators import (
    DropColumns,
    DropDataColumns,
    DropNulls,
    DropTagColumns,
    GroupBy,
    SelectColumns,
    SelectDataColumns,
    SelectTagColumns,
    TagBy,
)
from orcapod.core.sources import ArrowTableSource
from orcapod.pipeline.serialization import resolve_operator_from_config
from orcapod.utils.schema_utils import column_names


def _source() -> ArrowTableSource:
    """Return a source with two-letter tag names (a one-letter name hides the bug)."""
    return ArrowTableSource(
        pa.table(
            {
                "subject": ["a", "a", "b"],
                "session": [1, 2, 1],
                "value": [1.0, 2.0, 3.0],
                "other": [10, 20, 30],
            }
        ),
        tag_columns=["subject", "session"],
        infer_nullable=True,
        tag_only_columns=True,
        source_id="s",
    )


def _rows(table: pa.Table) -> list[dict[str, Any]]:
    """Return the rows of ``table`` as dicts."""
    return table.to_pylist()


class TestGroupByBareString:
    """``GroupBy("subject")`` is ``GroupBy(["subject"])``."""

    def test_rows_and_schema_equal_the_list_form(self):
        """A bare string groups on that one column."""
        by_string = GroupBy("subject")(_source())
        by_list = GroupBy(["subject"])(_source())

        assert by_string.output_schema() == by_list.output_schema()
        assert _rows(by_string.as_table(all_info=True)) == _rows(
            by_list.as_table(all_info=True)
        )
        assert by_string.as_table().column("subject").to_pylist() == ["a", "b"]

    def test_identity_equals_the_list_form(self):
        """Identity structure, hashes and the stream's hashes are those of the list."""
        by_string, by_list = GroupBy("subject"), GroupBy(["subject"])

        assert by_string.by == ("subject",)
        assert by_string.identity_structure() == by_list.identity_structure()
        assert by_string.content_hash() == by_list.content_hash()
        assert by_string(_source()).content_hash() == by_list(_source()).content_hash()
        assert (
            by_string(_source()).pipeline_hash() == by_list(_source()).pipeline_hash()
        )

    def test_config_round_trip(self):
        """The saved config holds the one name and loads as the same operator."""
        config = json.loads(json.dumps(GroupBy("subject").to_config()))

        assert config["config"] == {"by": ["subject"]}
        loaded = resolve_operator_from_config(config)
        assert loaded.content_hash() == GroupBy(["subject"]).content_hash()

    def test_stream_method_takes_a_bare_string(self):
        """``stream.group_by("subject")`` groups on ``subject``."""
        grouped = _source().group_by("subject")

        assert grouped.keys()[0] == ("subject",)

    def test_a_one_character_name_still_works(self):
        """A one-character name was already one name and stays one."""
        source = ArrowTableSource(
            pa.table({"g": [1, 1, 2], "x": [1, 2, 3]}),
            tag_columns=["g"],
            tag_only_columns=True,
        )

        assert GroupBy("g")(source).as_table().column("g").to_pylist() == [1, 2]
        assert GroupBy("g").identity_structure() == GroupBy(["g"]).identity_structure()

    @pytest.mark.parametrize(
        "by", [["session", "subject"], ("session", "subject"), ["subject", "session"]]
    )
    def test_collections_keep_their_identity(self, by):
        """A list or tuple keeps its order in the identity, as before."""
        assert GroupBy(by).identity_structure() == ("GroupBy", tuple(by))

    def test_bytes_are_not_a_name(self):
        """``bytes`` is neither a name nor a collection of names, empty or not."""
        with pytest.raises(TypeError, match="str"):
            GroupBy(b"subject")  # pyright: ignore[reportArgumentType]
        with pytest.raises(TypeError, match="bytes"):
            DropColumns(b"")  # pyright: ignore[reportArgumentType]

    def test_a_name_that_is_not_a_string_is_refused(self):
        """A collection holding a non-string is refused."""
        with pytest.raises(TypeError, match="int"):
            GroupBy(["subject", 1])  # pyright: ignore[reportArgumentType]


class TestEveryOperatorTakingNames:
    """Every operator taking column names reads a bare string as one name."""

    @pytest.mark.parametrize(
        "string_form, list_form",
        [
            (lambda: GroupBy("subject"), lambda: GroupBy(["subject"])),
            (lambda: SelectDataColumns("value"), lambda: SelectDataColumns(["value"])),
            (lambda: DropDataColumns("value"), lambda: DropDataColumns(["value"])),
            (
                lambda: SelectTagColumns("subject"),
                lambda: SelectTagColumns(["subject"]),
            ),
            (lambda: DropTagColumns("subject"), lambda: DropTagColumns(["subject"])),
            (lambda: SelectColumns("value"), lambda: SelectColumns(["value"])),
            (lambda: DropColumns("value"), lambda: DropColumns(["value"])),
            (lambda: DropNulls("value"), lambda: DropNulls(["value"])),
            (lambda: TagBy("other"), lambda: TagBy(["other"])),
        ],
    )
    def test_string_equals_one_element_list(self, string_form, list_form):
        """Same identity and same rows for ``"name"`` and ``["name"]``."""
        by_string, by_list = string_form(), list_form()

        assert by_string.identity_structure() == by_list.identity_structure()
        assert _rows(by_string(_source()).as_table(all_info=True)) == _rows(
            by_list(_source()).as_table(all_info=True)
        )


class TestTheRule:
    """``column_names`` is the one rule."""

    def test_a_string_is_one_name(self):
        """A bare string is one name; a collection keeps its order."""
        assert column_names("subject") == ("subject",)
        assert column_names(["b", "a"]) == ("b", "a")

    @pytest.mark.parametrize("value", [b"ab", b"", bytearray(b"ab"), 3, None])
    def test_other_values_are_refused(self, value):
        """Bytes, numbers and ``None`` are not column names."""
        with pytest.raises(TypeError):
            column_names(value)  # pyright: ignore[reportArgumentType]
