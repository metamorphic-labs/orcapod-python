"""The schema table's stored form, frozen by test vectors, and its refusals (ITL-1186).

The form of a row and of the completion marker is a data format (version 1): rows written
by the migration tool are read by every later Orcapod. A vector that fails is a bug in the
change; a new form is a new ``FORMAT_VERSION``.
"""

from __future__ import annotations

import asyncio
import base64
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod.databases import SpiralDBConnector
from orcapod.databases import spiraldb_schema_table as st
from orcapod.errors import TableSchemaError
from orcapod.sources import SpiralDBTableSource
from tests.test_databases import fake_spiral_sdk as fake

_SCHEMA = pa.schema(
    [
        pa.field("k", pa.large_string(), False),
        pa.field("a", pa.int64(), True, {"unit": "mV", "note": "x", "b": "1"}),
        pa.field("g", pa.struct([pa.field("y", pa.int8(), False)]), False),
        pa.field("l", pa.large_list(pa.field("item", pa.float32(), False))),
    ],
    metadata={"z": "1", "a": "2"},
)
# The bytes pyarrow 23.0.1 encoded ``_SCHEMA`` to: given, so that the vectors do not
# depend on how another pyarrow encodes it.
_BYTES = base64.b64decode(
    (Path(__file__).parent / "spiraldb_schema_v1.b64").read_text()
)
_ROW = st.SchemaRow(
    "table_1", "units", ("k",), _BYTES, st.FROM_RECORD, 1795550964587941
)
_SCOPE = st.Scope("p", "d", "f" * 64)


def test_the_stored_columns() -> None:
    assert [(f.name, str(f.type)) for f in st.ROW_SCHEMA] == [
        ("table_id", "large_string"),
        ("table_name", "large_string"),
        ("format", "large_string"),
        ("format_version", "int64"),
        ("nullability", "large_string"),
        ("schema", "large_binary"),
        ("key_columns", "large_string"),
        ("nullable_columns", "large_string"),
        ("captured_version", "int64"),
        ("project", "large_string"),
        ("dataset", "large_string"),
        ("plan_digest", "large_string"),
        ("table_count", "int64"),
        ("rows_digest", "large_string"),
        ("written_at", "large_string"),
        ("written_by", "large_string"),
    ]
    assert (st.SCHEMA_TABLE, st.MARKER_KEY) == ("__orcapod_schemas__", "__complete__")


def test_a_stored_row() -> None:
    assert _ROW.schema.equals(_SCHEMA, check_metadata=True)
    assert _ROW.stored() == {
        "table_id": "table_1",
        "table_name": "units",
        "format": "orcapod.table_schema",
        "format_version": 1,
        "nullability": "record",
        "captured_version": 1795550964587941,
        "schema": _BYTES,
        "key_columns": '["k"]',
        "nullable_columns": '[["a"],["l"]]',
    }
    assert st.SchemaRow.from_stored(_ROW.stored()) == _ROW


def test_the_marker() -> None:
    other = st.SchemaRow("table_0", "a", ("k", "a"), _BYTES, st.FROM_SERVICE, 0)
    marker = st.marker_values(_SCOPE, [_ROW, other])
    assert {k: v for k, v in marker.items() if v is not None} == {
        "table_id": "__complete__",
        "format": "orcapod.table_schema",
        "format_version": 1,
        "project": "p",
        "dataset": "d",
        "plan_digest": "f" * 64,
        "table_count": 2,
        "rows_digest": "93540c92016a267d6c7c61f00214c863ffe4452d5b095b41fd9209524a9d99bc",
    }
    # The order the rows are given in is not part of it.
    assert marker == st.marker_values(_SCOPE, [other, _ROW])
    rows = {r.table_id: r for r in (_ROW, other)}
    st.require_complete("p", "d", rows, marker)


@pytest.mark.parametrize(
    "change",
    [
        {"table_id": "table_2"},
        {"table_name": "other"},
        {"key_columns": ("a",)},
        {"nullability": st.FROM_SERVICE},
        {"captured_version": 2},
    ],
)
def test_the_digest_covers_every_part_of_a_row(change: dict[str, Any]) -> None:
    values: dict[str, Any] = {
        "table_id": _ROW.table_id,
        "table_name": _ROW.table_name,
        "key_columns": _ROW.key_columns,
        "schema_bytes": _ROW.schema_bytes,
        "nullability": _ROW.nullability,
        "captured_version": _ROW.captured_version,
    }
    changed = st.SchemaRow(**{**values, **change})
    assert st.rows_digest(_SCOPE, [changed]) != st.rows_digest(_SCOPE, [_ROW])


def test_the_digest_does_not_depend_on_registered_extension_types() -> None:
    """Review finding B5: the stored bytes are digested, never a schema encoded again."""

    class Measured(pa.ExtensionType):
        """A synthetic extension type."""

        def __init__(self) -> None:
            super().__init__(pa.large_binary(), "orcapod.test.measured")

        def __arrow_ext_serialize__(self) -> bytes:
            return b"meta"

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type: Any, serialized: Any) -> Any:
            return cls()

    schema = pa.schema(
        [
            pa.field("k", pa.large_string(), False),
            pa.field(
                "v",
                pa.large_binary(),
                metadata={
                    "a": "1",
                    "ARROW:extension:name": "orcapod.test.measured",
                    "z": "2",
                    "ARROW:extension:metadata": "meta",
                },
            ),
        ]
    )
    row = st.SchemaRow(
        "t", "n", ("k",), schema.serialize().to_pybytes(), st.FROM_RECORD, 1
    )
    stored = row.stored()
    marker = st.marker_values(_SCOPE, [row])
    before = st.SchemaRow.from_stored(stored)
    pa.register_extension_type(cast(Any, Measured()))
    try:
        after = st.SchemaRow.from_stored(stored)
        assert before == after == row
        st.require_complete("p", "d", {"t": after}, marker)
        assert after.schema.equals(before.schema, check_metadata=True)
        assert after.schema.field("v").type == pa.large_binary()
    finally:
        pa.unregister_extension_type("orcapod.test.measured")


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"format": "other"}, "unknown format"),
        ({"format_version": 2}, "format version 2"),
        ({"nullability": "maybe"}, "unknown nullability"),
        ({"schema": b"broken"}, "cannot be decoded"),
        ({"schema": None}, "cannot be decoded"),
        ({"key_columns": "[]"}, "key_columns"),
        ({"key_columns": '["gone"]'}, "key_columns"),
        ({"key_columns": "no json"}, "cannot be decoded"),
        ({"nullable_columns": "[]"}, "disagree"),
        ({"captured_version": None}, "captured_version"),
    ],
)
def test_a_row_that_cannot_be_read_in_full_is_refused(
    change: dict[str, Any], message: str
) -> None:
    with pytest.raises(TableSchemaError, match=message):
        st.SchemaRow.from_stored({**_ROW.stored(), **change})


# ------------------------------------------------------------- a dataset's schema table


_WRITTEN = pa.schema(
    [
        pa.field("k", pa.string(), False),
        pa.field("j", pa.int32(), False),
        pa.field("v", pa.int64(), False),
        pa.field("w", pa.string()),
    ]
)


@pytest.fixture()
def project(tmp_path: Path):
    with fake.install(tmp_path / "spiral"):
        made = fake.ExternalProject("p")
        for name, keys in (("units", ["k", "j"]), ("trials", ["j", "k"])):
            made.create_table(
                f"default.{name}", key_schema=[_WRITTEN.field(key) for key in keys]
            ).write(
                pa.table(
                    {
                        "w": ["x", None],
                        "k": ["a", "b"],
                        "v": [1, 2],
                        "j": pa.array([2, 1], pa.int32()),
                    }
                )
            )
        yield made


def _edit_schema_table(**change: Any) -> None:
    """Overwrite the marker's values in the stored schema table."""
    client = fake.FakeClient()
    table = client.keyed_table("p", "default", st.SCHEMA_TABLE)
    (marker,) = [
        r for r in table.scan().to_table().to_pylist() if r["table_id"] == st.MARKER_KEY
    ]
    table.append(pa.Table.from_pylist([{**marker, **change}], schema=st.ROW_SCHEMA))


# Every way a person reaches a table's rows or what is known of it.
_PATHS = {
    "source": lambda: SpiralDBTableSource("p", "units", tag_only_columns=True),
    "schema": lambda: SpiralDBConnector("p").get_arrow_schema("units"),
    "columns": lambda: SpiralDBConnector("p").get_column_info("units"),
    "batches": lambda: list(SpiralDBConnector("p").scan_batches("units")),
    "query": lambda: list(SpiralDBConnector("p").iter_batches('SELECT * FROM "units"')),
    "count": lambda: SpiralDBConnector("p").get_row_count_estimate("units"),
    "bounded count": lambda: (
        SpiralDBConnector("p").lookup_table("units").count_rows(at_most=1)
    ),
    "listing": lambda: SpiralDBConnector("p").get_table_names(),
}


@pytest.mark.parametrize("path", sorted(_PATHS))
@pytest.mark.parametrize(
    "damage",
    [
        "no marker",
        "another count",
        "another digest",
        "another dataset",
        "another project",
        "another plan",
        "withdrawn",
    ],
)
def test_a_schema_table_that_is_not_complete_is_refused_on_every_path(
    project: Any, path: str, damage: str
) -> None:
    """Review finding B1."""
    fake.migrate("p", {"units": _WRITTEN}, complete=damage != "no marker")
    if damage != "no marker":
        _edit_schema_table(
            **{
                "another count": {"table_count": 3},
                "another digest": {"rows_digest": "0" * 64},
                "another dataset": {"dataset": "other"},
                "another project": {"project": "other"},
                "another plan": {"plan_digest": "1" * 64},
                "withdrawn": {"rows_digest": "withdrawn: the dataset changed"},
            }[damage]
        )
    with pytest.raises(TableSchemaError):
        _PATHS[path]()


async def _collect(source: SpiralDBTableSource) -> list[Any]:
    return [pair async for pair in source.async_iter_data()]


@pytest.mark.parametrize("name", ["units", "trials"])
@pytest.mark.parametrize("tag_only", [True, False], ids=["plain tags", "data-tags"])
def test_open_readers_refuse_too(project: Any, name: str, tag_only: bool) -> None:
    """A source and its previews, built while the table was complete, sync and async."""
    fake.migrate("p", {"units": _WRITTEN, "trials": _WRITTEN})
    source = SpiralDBTableSource("p", name, tag_only_columns=tag_only)
    assert len(asyncio.run(_collect(source))) == len(list(source.iter_data())) == 2
    _edit_schema_table(table_count=9)
    for read in (
        source.as_table,
        lambda: list(source.iter_data()),
        lambda: asyncio.run(_collect(source)),
        source.head,
        lambda: source.row_count_estimate,
        source.check_for_new_data,
    ):
        with pytest.raises(TableSchemaError):
            read()


def test_a_display_names_the_refusal_and_shows_no_row(project: Any) -> None:
    fake.migrate("p", {"units": _WRITTEN})
    source = SpiralDBTableSource("p", "units", tag_only_columns=True)
    _edit_schema_table(table_count=9)
    html = source._repr_html_()
    assert "could not read" in html and "completion marker" in html
    assert "<td>" not in html


def test_a_foreign_table_at_the_reserved_name_is_refused(tmp_path: Path) -> None:
    with fake.install(tmp_path / "spiral"):
        made = fake.ExternalProject("p")
        made.create_table("default.units", key_schema=[("k", pa.string())]).write(
            pa.table({"k": ["a"], "v": [1]})
        )
        made.create_table(
            f"default.{st.SCHEMA_TABLE}", key_schema=[("x", pa.string())]
        ).write(pa.table({"x": ["a"], "y": [1]}))
        with pytest.raises(TableSchemaError, match="key columns"):
            SpiralDBTableSource("p", "units", tag_only_columns=True)


def test_the_reserved_name_is_refused_and_unlisted(project: Any) -> None:
    fake.migrate("p", {"units": _WRITTEN})
    connector = SpiralDBConnector("p")
    assert connector.get_table_names() == ["trials", "units"]
    for use in (connector.lookup_table, connector.get_column_info):
        with pytest.raises(ValueError, match="reserved"):
            use(st.SCHEMA_TABLE)
    with pytest.raises(ValueError, match="reserved"):
        SpiralDBTableSource("p", st.SCHEMA_TABLE)
