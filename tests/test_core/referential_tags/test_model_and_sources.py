"""Referential tags: the stream model and sources (ITL-1023).

Specification "Orcapod v0.4 — Referential Tags" (ITL-1004), sections 4 (namespaces),
11 (sources) and 15.1 (schema tests). A referential tag ``x`` is one column whose name is
both a tag key and a data key. A source's declared tags are referential by default
(developer, November 12, 2026); ``tag_only_columns`` keeps chosen tags plain, as in v0.3.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.core.referential_tags import check_namespaces, stream_references
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError

ROWS = [{"x": 1, "y": 0.5}, {"x": 2, "y": 0.75}]


def _source(**kwargs: Any) -> op.sources.DictSource:
    return op.sources.DictSource(ROWS, tag_columns=["x"], source_id="s", **kwargs)


def test_declared_tag_is_referential_by_default() -> None:
    source = _source()
    assert source.keys() == (("x",), ("x", "y"))
    tag_schema, data_schema = source.output_schema()
    assert tag_schema == {"x": int} and data_schema == {"x": int, "y": float}
    assert stream_references(source) == ("x",)


def test_referential_column_is_stored_once_and_seen_through_both_roles() -> None:
    source = _source()
    table = source.as_table(columns={"source": True})
    assert table.column_names.count("x") == 1
    # As data, the column has source info like any data column.
    assert "_source_x" in table.column_names
    ((tag, data), _) = list(source.iter_data())
    assert tag.as_dict() == {"x": 1}
    assert data.as_dict() == {"x": 1, "y": 0.5}
    # Section 4.4: the same Arrow type in both roles.
    assert tag.arrow_schema().field("x") == data.arrow_schema().field("x")


@pytest.mark.parametrize("tag_only", [["x"], "x", True])
def test_tag_only_columns_keep_the_v03_shape(tag_only: object) -> None:
    source = _source(tag_only_columns=tag_only)
    assert source.keys() == (("x",), ("y",))
    assert stream_references(source) == ()
    assert "_source_x" not in source.as_table(columns={"source": True}).column_names


def test_tag_only_columns_must_be_declared_tags() -> None:
    with pytest.raises(ValueError, match="tag_only_columns"):
        _source(tag_only_columns=["y"])


def test_referential_and_plain_sources_have_different_identities() -> None:
    referential, plain = _source(), _source(tag_only_columns=True)
    # The source id is the source's name: the same.
    assert referential.source_id == plain.source_id
    assert referential.content_hash() != plain.content_hash()
    assert referential.pipeline_hash() != plain.pipeline_hash()


def test_arrow_table_stream_identity_has_the_references_only_when_present() -> None:
    table = pa.table({"x": [1, 2], "y": [0.5, 0.75]})
    plain = ArrowTableStream(table, tag_columns=["x"])
    referential = ArrowTableStream(table, tag_columns=["x"], reference_tags=["x"])
    assert plain.keys() == (("x",), ("y",))
    assert referential.keys() == (("x",), ("x", "y"))
    assert plain.content_hash() != referential.content_hash()
    # Without references the identity structure is the v0.3 one.
    assert plain.identity_structure()[-1] == ("x",)


def test_reference_must_be_a_tag_column() -> None:
    table = pa.table({"x": [1], "y": [0.5]})
    with pytest.raises(ValueError, match="Reference tags"):
        ArrowTableStream(table, tag_columns=["x"], reference_tags=["y"])


def test_namespace_validator() -> None:
    check_namespaces({"x": int}, {"x": int, "y": float}, references=["x"])
    with pytest.raises(InputValidationError, match="plain tag and a data column"):
        check_namespaces({"x": int}, {"x": int}, references=[])
    with pytest.raises(InputValidationError, match="as a tag"):
        check_namespaces({"x": int}, {"x": str}, references=["x"])


def test_arrow_table_source_default_and_tag_only() -> None:
    table = pa.table({"x": pa.array([1, 2], pa.int8()), "y": [0.5, 0.75]})
    referential = ArrowTableSource(table, tag_columns=["x"])
    plain = ArrowTableSource(table, tag_columns=["x"], tag_only_columns=True)
    assert referential.keys() == (("x",), ("x", "y"))
    assert plain.keys() == (("x",), ("y",))
    # The referenced column keeps its exact Arrow type (section 4.4).
    ((_, data), *_) = list(referential.iter_data())
    assert data.arrow_schema().field("x").type == pa.int8()


def test_to_config_always_writes_tag_only_columns(tmp_path: object) -> None:
    """Format 0.3.0 states every source's plain tags; the version, not a missing key,
    decides an older file's meaning (ITL-1019)."""
    from pathlib import Path

    path = Path(str(tmp_path)) / "rows.csv"
    path.write_text("x,y\n1,0.5\n2,0.75\n")
    plain = op.sources.CSVSource(str(path), tag_columns=["x"], tag_only_columns=True)
    referential = op.sources.CSVSource(str(path), tag_columns=["x"])
    assert plain.to_config()["tag_only_columns"] is True
    assert referential.to_config()["tag_only_columns"] == []
    assert op.sources.CSVSource.from_config(plain.to_config()).keys() == plain.keys()
    assert (
        op.sources.CSVSource.from_config(referential.to_config()).keys()
        == referential.keys()
    )
    # A config without the key is refused: its meaning is not guessed.
    config = plain.to_config()
    del config["tag_only_columns"]
    with pytest.raises(ValueError, match="has no tag_only_columns"):
        op.sources.CSVSource.from_config(config)


@pytest.mark.parametrize("tag_only", [(), ("k",)], ids=["referential", "plain"])
def test_a_reload_keeps_the_tag_only_choice(tmp_path: Any, tag_only: Any) -> None:
    """A source that takes in new rows (ITL-1102) keeps its referential tags."""
    path = tmp_path / "rows.csv"
    path.write_text("k,v\n1,10\n2,20\n")
    source = op.sources.CSVSource(
        str(path), tag_columns=["k"], record_id_columns="k", tag_only_columns=tag_only
    )
    expected = () if tag_only else ("k",)
    assert stream_references(source) == expected
    path.write_text("k,v\n1,10\n2,20\n3,30\n")
    assert source.check_for_new_data() is True
    assert stream_references(source) == expected
    assert sorted(r["k"] for r in source.as_table().to_pylist()) == [1, 2, 3]
