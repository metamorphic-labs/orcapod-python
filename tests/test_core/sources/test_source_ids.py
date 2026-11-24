"""Source ids are ``<kind>_v<N>:<name>``: the one builder and parser (ITL-957, ITL-1425)."""

import warnings

import pyarrow as pa
import pytest

from orcapod.core.sources.source_ids import (
    ParsedSourceId,
    build_source_id,
    hash_source_name,
    kind_part,
    location_source_name,
    parse_source_id,
    require_source_name,
    warn_if_name_looks_like_an_id,
)
from orcapod.errors import SourceNameWarning
from orcapod.sources import ArrowTableSource, CSVSource, DictSource
from orcapod.types import ContentHash

HASH = ContentHash("algo_v1", b"\x00\xab" * 16)


# --- hash and location names --------------------------------------------------


def test_hash_name_is_label_and_hex():
    assert hash_source_name(HASH) == HASH.to_string()
    assert hash_source_name(HASH).split(":") == ["algo_v1", "00ab" * 16]


def test_hexdigits_truncates_the_hex_only():
    assert hash_source_name(HASH, hexdigits=6) == "algo_v1:00ab00"


@pytest.mark.parametrize("label", ["", "a:b"])
def test_label_must_be_non_empty_without_a_colon(label):
    with pytest.raises(ValueError, match="label"):
        hash_source_name(ContentHash(label, b"\x00" * 32))


@pytest.mark.parametrize("location", ["", "a::b", ":a", "a:"])
def test_a_location_that_blurs_a_token_gives_no_default(location):
    with pytest.raises(ValueError, match="give the source a source_id"):
        location_source_name(location)


def test_a_location_may_hold_a_single_colon():
    assert location_source_name("/db.sqlite:main.units") == "/db.sqlite:main.units"


# --- the kind part ------------------------------------------------------------


@pytest.mark.parametrize(
    ("kind", "version", "expected"),
    [("csv", 1, "csv_v1"), ("delta_table", 2, "delta_table_v2"), ("x9", 10, "x9_v10")],
)
def test_kind_part(kind, version, expected):
    assert kind_part(kind, version) == expected


@pytest.mark.parametrize("kind", ["", "Csv", "a:b", "9x", "a-b"])
def test_kind_part_refuses_a_bad_kind(kind):
    with pytest.raises(ValueError, match="kind"):
        kind_part(kind, 1)


@pytest.mark.parametrize("version", [0, -1, 1.0, True, "1"])
def test_kind_part_refuses_a_bad_version(version):
    with pytest.raises(ValueError, match="version"):
        kind_part("csv", version)


# --- build and parse ----------------------------------------------------------


@pytest.mark.parametrize(
    ("kind", "version", "name"),
    [
        ("csv", 1, "units"),
        ("sqlite_table", 1, "/data/db.sqlite:main.units"),
        ("arrow_table", 3, "arrow_v1:" + "ab" * 32),
        ("delta_table", 2, "delta_table_v1:units"),  # a nested name
        ("dict", 1, "a b c"),
    ],
)
def test_build_and_parse_round_trip(kind, version, name):
    source_id = build_source_id(kind, version, name)
    assert source_id == f"{kind}_v{version}:{name}"
    assert parse_source_id(source_id) == ParsedSourceId(kind, version, name)


def test_parse_splits_at_the_first_colon():
    parsed = parse_source_id("csv_v1:delta_table_v1:units")
    assert parsed.kind == "csv"
    assert parsed.version == 1
    assert parsed.name == "delta_table_v1:units"


@pytest.mark.parametrize(
    "text", ["units", "csv:units", "csv_v0:units", "csv_v1", "csv_v1:", "Csv_v1:x"]
)
def test_parse_refuses_what_is_not_a_source_id(text):
    with pytest.raises(ValueError):
        parse_source_id(text)


@pytest.mark.parametrize("name", ["", "a::b", ":a", "a:"])
def test_a_name_that_blurs_a_token_is_refused(name):
    with pytest.raises(ValueError):
        require_source_name(name)
    with pytest.raises(ValueError):
        build_source_id("csv", 1, name)


# --- the nested-name warning --------------------------------------------------


def test_a_name_starting_like_an_id_of_a_registered_kind_warns():
    with pytest.warns(SourceNameWarning, match="delta_table"):
        warn_if_name_looks_like_an_id("delta_table_v1:units")


@pytest.mark.parametrize(
    "name", ["units", "run_v2:x", "csv:units", "csv_v1", "a:b:c", "/p/x.csv"]
)
def test_other_names_do_not_warn(name):
    with warnings.catch_warnings():
        warnings.simplefilter("error", SourceNameWarning)
        warn_if_name_looks_like_an_id(name)


def _table() -> pa.Table:
    return pa.table(
        {
            "a": pa.array(["x", "y"], pa.large_string()),
            "b": pa.array([1, 2], pa.int64()),
            "c": pa.array([1.0, 2.0], pa.float64()),
            "d": pa.array([True, False], pa.bool_()),
        }
    )


def test_a_source_given_a_nested_name_keeps_it_and_warns():
    with pytest.warns(SourceNameWarning):
        source = ArrowTableSource(
            _table(),
            tag_columns=["a", "b"],
            tag_only_columns=True,
            source_id="csv_v1:units",
        )
    assert source.source_name == "csv_v1:units"
    assert source.source_id == "arrow_table_v1:csv_v1:units"


# --- sources: the kind part is always added -------------------------------------


@pytest.mark.parametrize(
    "make",
    [
        lambda sid: ArrowTableSource(
            _table(), tag_columns=["a", "b"], tag_only_columns=True, source_id=sid
        ),
        lambda sid: DictSource(
            _table().to_pylist(),
            tag_columns=["b", "a"],
            tag_only_columns=True,
            source_id=sid,
        ),
    ],
)
def test_a_given_name_gets_the_kind_part(make):
    source = make("units")
    assert source.source_name == "units"
    assert source.source_id == f"{type(source).kind_part()}:units"
    assert parse_source_id(source.source_id).name == "units"


def test_a_csv_source_id_names_its_kind_and_version(tmp_path):
    path = tmp_path / "units.csv"
    path.write_text("a,b,c,d\nx,1,1.0,true\ny,2,2.0,false\n")
    given = CSVSource(str(path), tag_columns=["a", "b"], source_id="units")
    default = CSVSource(str(path), tag_columns=["b", "a"])
    assert given.source_id == "csv_v1:units"
    assert default.source_id == f"csv_v1:{path}"
    assert default.source_name == str(path)
