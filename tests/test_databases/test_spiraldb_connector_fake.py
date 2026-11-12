"""SpiralDBConnector read path against the disk-backed pyspiral fake (no live SpiralDB).

These tests push real Arrow tables through the connector, unlike the ``MagicMock`` tests in
``test_spiraldb_connector.py``. See ``fake_pyspiral`` for the storage behaviour modelled.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.databases import SpiralDBConnector
from orcapod.sources import ArrowTableSource, SpiralDBTableSource
from tests.test_databases import fake_pyspiral

_NESTED = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("s", pa.string()),
        pa.field("list_str", pa.list_(pa.string())),
        pa.field("list_nonnull_item", pa.list_(pa.field("item", pa.string(), False))),
        pa.field(
            "st",
            pa.struct([pa.field("a", pa.string(), False), pa.field("b", pa.binary())]),
        ),
        pa.field("large_list_bin", pa.large_list(pa.binary())),
        pa.field(
            "tags",
            # key field "key" (non-null) and item field "value" (nullable)
            pa.map_(pa.string(), pa.list_(pa.string())),
        ),
    ]
)

# What the connector must hand out: the large variant at every depth, with every nested
# field's name and nullability unchanged.
_EXPECTED = {
    "id": pa.int64(),
    "s": pa.large_string(),
    "list_str": pa.large_list(pa.large_string()),
    "list_nonnull_item": pa.large_list(pa.field("item", pa.large_string(), False)),
    "st": pa.struct(
        [pa.field("a", pa.large_string(), False), pa.field("b", pa.large_binary())]
    ),
    "large_list_bin": pa.large_list(pa.large_binary()),
    "tags": pa.map_(pa.large_string(), pa.large_list(pa.large_string())),
}


def _nested_table() -> pa.Table:
    """Return two rows covering every nested type in ``_NESTED``."""
    return pa.table(
        {
            "id": [1, 2],
            "s": ["x", None],
            "list_str": [["a"], []],
            "list_nonnull_item": [["p", "q"], ["r"]],
            "st": [{"a": "u", "b": b"1"}, {"a": "v", "b": None}],
            "large_list_bin": [[b"z"], []],
            "tags": [[("k", ["a", "b"])], []],
        },
        schema=_NESTED,
    )


@pytest.fixture()
def spiral_root(tmp_path: Path):
    """Provide a fake SpiralDB root holding table ``default.nested`` in project ``p``."""
    with fake_pyspiral.install(tmp_path / "spiral"):
        table = (
            fake_pyspiral.FakeSpiral()
            .project("p")
            .create_table("default.nested", key_schema=[("id", pa.int64())])
        )
        table.write(_nested_table())
        # Sources hash their rows, and starfix 0.4.0 cannot hash map columns yet, so the
        # source-level tests read a copy without the map column.
        hashable = (
            fake_pyspiral.FakeSpiral()
            .project("p")
            .create_table("default.hashable", key_schema=[("id", pa.int64())])
        )
        hashable.write(_nested_table().drop_columns(["tags"]))
        yield tmp_path


class TestLargeTypeNormalization:
    """ITL-40: both read paths give the same, fully large-normalized types."""

    def test_iter_batches_normalizes_nested_types(self, spiral_root):
        connector = SpiralDBConnector("p")
        batches = list(connector.iter_batches('SELECT * FROM "nested"'))
        assert batches
        for batch in batches:
            assert {f.name: f.type for f in batch.schema} == _EXPECTED

    def test_get_column_info_matches_iter_batches(self, spiral_root):
        connector = SpiralDBConnector("p")
        info = {c.name: c.arrow_type for c in connector.get_column_info("nested")}
        assert info == _EXPECTED

    def test_source_exposes_normalized_types(self, spiral_root):
        source = SpiralDBTableSource("p", "hashable")
        schema = source.as_table().schema
        expected = {k: v for k, v in _EXPECTED.items() if k != "tags"}
        assert {name: schema.field(name).type for name in expected} == expected

    def test_normalization_leaves_hashes_unchanged(self, spiral_root):
        # Large and non-large layouts hash identically, so the Spiral source's per-row
        # content hashes equal those of the same rows given in their original layout.
        spiral = SpiralDBTableSource("p", "hashable", source_id="nested")
        stored = (
            fake_pyspiral.FakeSpiral()
            .project("p")
            .table("default.hashable")
            .schema()
            .to_arrow()
        )
        direct = ArrowTableSource(
            _nested_table().drop_columns(["tags"]).cast(stored),
            tag_columns=["id"],
            source_id="nested",
        )

        def hashes(source) -> list[str]:
            return (
                source.as_table(columns={"content_hash": True})
                .column("_content_hash")
                .to_pylist()
            )

        assert hashes(spiral) == hashes(direct)


def test_nested_normalization_preserves_restored_metadata(tmp_path):
    """Normalizing nested layouts must retain restored field and schema metadata."""
    item = pa.field("element", pa.string(), False, metadata={b"role": b"label"})
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), False),
            pa.field("labels", pa.list_(item), False, metadata={b"unit": b"names"}),
        ],
        metadata={b"origin": b"normalization-regression"},
    )
    data = pa.table({"id": [1, 2, 3], "labels": [["x"], [], ["y", "z"]]}, schema=schema)
    with fake_pyspiral.install(tmp_path / "spiral"):
        fake_pyspiral.FakeSpiral().project("p").create_table(
            "default.labels", key_schema=[("id", pa.int64())]
        )
        connector = SpiralDBConnector("p")
        connector.upsert_records("labels", data, id_column="id")
        batches = list(connector.iter_batches('SELECT * FROM "labels"'))
        expected_schema = pa.schema(
            [
                schema.field("id"),
                schema.field("labels").with_type(
                    pa.large_list(item.with_type(pa.large_string()))
                ),
            ],
            metadata=schema.metadata,
        )
        assert batches
        for batch in batches:
            assert batch.schema.equals(expected_schema, check_metadata=True)
        assert pa.Table.from_batches(batches).to_pylist() == data.to_pylist()
        info = connector.get_column_info("labels")
        assert info[1].arrow_type.equals(expected_schema.field("labels").type)
        assert info[1].nullable is False
        connector.close()
