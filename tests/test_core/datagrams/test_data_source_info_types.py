"""Source-info values may be lists, not just scalar strings.

Many->one operators (GroupBy, MergeJoin) produce one provenance token per
member.  `Data` must represent those without collapsing or crashing.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.datagrams import Data
from orcapod.types import SourceInfoValue


def _data_with_mixed_source_info() -> Data:
    """Data with one list-valued and one scalar-null source token."""
    return Data(
        {"probe": [0, 1], "path": ["a", "b"]},
        source_info={"probe": None, "path": ["s0", "s1"]},
    )


class TestListValuedSourceInfo:
    def test_schema_reports_list_type_for_list_valued_token(self):
        data = _data_with_mixed_source_info()
        schema = data.schema(columns={"source": True})
        assert schema["_source_path"] == list[str]

    def test_schema_reports_str_for_none_token(self):
        data = _data_with_mixed_source_info()
        schema = data.schema(columns={"source": True})
        assert schema["_source_probe"] is str

    def test_as_table_round_trips_list_valued_token(self):
        data = _data_with_mixed_source_info()
        table = data.as_table(columns={"source": True})
        assert table.schema.field("_source_path").type == pa.large_list(
            pa.large_string()
        )
        assert table.column("_source_path").to_pylist() == [["s0", "s1"]]

    def test_as_table_keeps_none_token_as_large_string(self):
        data = _data_with_mixed_source_info()
        table = data.as_table(columns={"source": True})
        assert table.schema.field("_source_probe").type == pa.large_string()
        assert table.column("_source_probe").to_pylist() == [None]

    def test_empty_list_token_defaults_to_list_of_string(self):
        data = Data({"path": ["a"]}, source_info={"path": []})
        table = data.as_table(columns={"source": True})
        assert table.schema.field("_source_path").type == pa.large_list(
            pa.large_string()
        )

    def test_nested_list_token_recurses(self):
        """Type derivation descends into nested lists rather than flattening.

        A single level of list-wrapping would be satisfied by returning
        ``large_list(large_string)`` unconditionally; this pins the recursion.
        """
        data = Data({"path": ["a", "b"]}, source_info={"path": [["s0"], ["s1"]]})
        assert data.schema(columns={"source": True})["_source_path"] == list[list[str]]
        table = data.as_table(columns={"source": True})
        assert table.schema.field("_source_path").type == pa.large_list(
            pa.large_list(pa.large_string())
        )
        assert table.column("_source_path").to_pylist() == [[["s0"], ["s1"]]]

    def test_scalar_token_unchanged(self):
        """Existing scalar behavior must not regress."""
        data = Data({"path": "a"}, source_info={"path": "src::row_0::path"})
        table = data.as_table(columns={"source": True})
        assert table.schema.field("_source_path").type == pa.large_string()
        assert data.schema(columns={"source": True})["_source_path"] is str

    def test_arrow_table_construction_recovers_list_token(self):
        """Data built from an Arrow table keeps list-valued source info."""
        table = pa.table(
            {
                "path": pa.array([["a", "b"]], pa.list_(pa.large_string())),
                "_source_path": pa.array([["s0", "s1"]], pa.list_(pa.large_string())),
            }
        )
        data = Data(table)
        assert data.source_info()["path"] == ["s0", "s1"]
        assert data.as_table(columns={"source": True}).column(
            "_source_path"
        ).to_pylist() == [["s0", "s1"]]


@pytest.mark.parametrize("via_constructor", [True, False])
@pytest.mark.parametrize(
    "replacement",
    [["new"], "wrong depth", [None]],
    ids=["preserves-item-field", "refuses-scalar", "refuses-null-item"],
)
def test_python_token_replacement_uses_the_held_type(
    via_constructor: bool, replacement: SourceInfoValue
) -> None:
    """Replacements use the declared list item name, metadata and nullability."""
    kind = pa.large_list(
        pa.field(
            "leaf", pa.large_string(), nullable=False, metadata={b"unit": b"token"}
        )
    )
    table = pa.table({"x": [[1]], "_source_x": pa.array([["old"]], type=kind)})

    def replace() -> Data:
        if via_constructor:
            return Data(table, source_info={"x": replacement})
        return Data(table).with_source_info(x=replacement)

    if replacement != ["new"]:
        with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError)):
            _ = replace()
        return
    updated = replace().as_table(columns={"source": True})
    assert updated.schema.field("_source_x").equals(
        pa.field("_source_x", kind, nullable=True), check_metadata=True
    )
    assert updated["_source_x"].to_pylist() == [replacement]


def test_a_null_replacement_agrees_with_the_held_string_type() -> None:
    """Fifth review of PR #607, finding 2: ``with_source_info(x=None)`` on an
    Arrow-backed row whose token column is a string gives one declaration, a
    ``large_string`` null, in ``as_table`` and in ``source_info_table``."""
    table = pa.table({"x": [1], "_source_x": pa.array(["old"], type=pa.large_string())})
    cleared = Data(table).with_source_info(x=None)
    expected = pa.field("_source_x", pa.large_string(), nullable=True)
    shown = cleared.as_table(columns={"source": True})
    for held in (shown, cleared.source_info_table()):
        assert held.schema.field("_source_x").equals(expected, check_metadata=True)
        assert held["_source_x"].to_pylist() == [None]


def test_a_null_replacement_of_a_held_list_token_is_refused() -> None:
    """Fifth review of PR #607, finding 2: a list token is never null, so
    ``with_source_info(x=None)`` on an Arrow-backed row whose token column is a
    list is refused. It used to give the row two declarations: a null list in
    ``as_table`` and a ``large_string`` null in ``source_info_table``."""
    kind = pa.large_list(
        pa.field("leaf", pa.large_string(), nullable=True, metadata={b"u": b"t"})
    )
    table = pa.table({"x": [[1]], "_source_x": pa.array([["old"]], type=kind)})
    row = Data(table)
    with pytest.raises(pa.ArrowTypeError, match="list token is never null"):
        _ = row.with_source_info(x=None)
    # The row is unchanged, and its two forms agree with the held type.
    for held in (row.as_table(columns={"source": True}), row.source_info_table()):
        assert held.schema.field("_source_x").type.equals(kind, check_metadata=True)
        assert held["_source_x"].to_pylist() == [["old"]]


@pytest.mark.parametrize("via_constructor", [False, True])
def test_a_replacement_cannot_put_a_null_inside_nested_token_lists(
    via_constructor: bool,
) -> None:
    kind = pa.large_list(pa.large_list(pa.large_string()))
    table = pa.table({"x": [[[1]]], "_source_x": pa.array([[["old"]]], type=kind)})
    row = Data(table)
    with pytest.raises(pa.ArrowTypeError, match="list token is never null"):
        if via_constructor:
            _ = Data(table, source_info={"x": [None]})
        else:
            _ = row.with_source_info(x=[None])
    assert row.with_source_info(x=[[None]]).source_info_table()[
        "_source_x"
    ].to_pylist() == [[[None]]]
