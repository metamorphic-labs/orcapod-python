"""Arrow hashing on starfix hash format 2 under the label ``arrow_v1`` (ITL-910).

starfix 0.5 replaced hash format 1 (prefix ``00 00 01``) with the lossless format 2 (prefix
``00 00 02``). These tests pin what that fixes in Orcapod (the collisions of ITL-848), what it
newly accepts (``fixed_size_list`` data, ITL-927; canonical extension types) and what it newly
rejects.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.core.operators import Join

# Non-public on purpose: the label constant itself is under test.
from orcapod.hashing.arrow_hashers import ARROW_HASH_LABEL
from orcapod.sources import ArrowTableSource
from orcapod.types import ContentHash

FORMAT_2_PREFIX = b"\x00\x00\x02"


def _hash_table(table: pa.Table) -> ContentHash:
    """Hash ``table`` with the default context's Arrow hasher."""
    return get_default_context().arrow_hasher.hash_table(table)


def _row_hashes(column: pa.Array[Any]) -> list[str]:
    """Content hashes of the data of a one-column source keyed by a row tag."""
    table = pa.table(
        {
            "k": pa.array([f"r{i}" for i in range(len(column))], pa.large_string()),
            "x": column,
        }
    )
    source = ArrowTableSource(table, tag_columns=["k"])
    return [data.content_hash().to_string() for _, data in source.iter_data()]


def _non_nullable(name: str, column: pa.Array[Any]) -> pa.Table:
    """A one-column table whose field is declared non-nullable."""
    return pa.Table.from_arrays(
        [column], schema=pa.schema([pa.field(name, column.type, nullable=False)])
    )


# ---------------------------------------------------------------------------
# Label and format
# ---------------------------------------------------------------------------


class TestLabel:
    def test_current_arrow_label_is_arrow_v0_2(self):
        assert ARROW_HASH_LABEL == "arrow_v1"

    def test_default_context_uses_arrow_v0_2(self):
        assert get_default_context().arrow_hasher.hasher_id == "arrow_v1"

    def test_table_hash_is_format_2(self):
        result = _hash_table(pa.table({"x": pa.array([1, 2], pa.int64())}))
        assert result.method == "arrow_v1"
        assert result.digest[:3] == FORMAT_2_PREFIX

    def test_row_data_hash_carries_new_label(self):
        hashes = _row_hashes(pa.array([1], pa.int64()))
        assert hashes[0].startswith("arrow_v1:000002")


# ---------------------------------------------------------------------------
# ITL-848 collisions are gone
# ---------------------------------------------------------------------------


class TestFormat1CollisionsFixed:
    def test_nested_list_lengths_are_part_of_the_row_hash(self):
        list3 = pa.large_list(pa.large_list(pa.large_list(pa.int64())))
        first = _row_hashes(pa.array([[[[1]], [[2], [3]]]], list3))
        second = _row_hashes(pa.array([[[[1], [2]], [[3]]]], list3))
        assert first != second

    def test_null_struct_differs_from_struct_of_nulls(self):
        struct = pa.struct([("a", pa.int64())])
        null_struct = _row_hashes(pa.array([None], struct))
        struct_of_nulls = _row_hashes(pa.array([{"a": None}], struct))
        assert null_struct != struct_of_nulls

    def test_boolean_hash_does_not_depend_on_chunking(self):
        table = pa.table({"x": pa.array([True, False, True])})
        rechunked = pa.Table.from_batches(table.to_batches(max_chunksize=1))
        assert rechunked.column("x").num_chunks == 3
        assert _hash_table(table) == _hash_table(rechunked)

    def test_boolean_row_count_is_part_of_the_hash(self):
        one = _non_nullable("x", pa.array([True]))
        three = _non_nullable("x", pa.array([True, False, False]))
        assert _hash_table(one) != _hash_table(three)

    def test_slice_offset_does_not_change_the_hash(self):
        sliced = pa.table({"x": pa.array([1, 2, 3, 4], pa.int64())}).slice(1, 2)
        fresh = pa.table({"x": pa.array([2, 3], pa.int64())})
        assert _hash_table(sliced) == _hash_table(fresh)

    def test_chunk_layout_does_not_change_the_hash(self):
        table = pa.table({"x": pa.array(["a", None, "ccc", "dd"], pa.large_string())})
        chunked = pa.Table.from_batches(table.to_batches(max_chunksize=3))
        assert chunked.column("x").num_chunks == 2
        assert _hash_table(chunked) == _hash_table(chunked.combine_chunks())
        assert _hash_table(chunked) == _hash_table(table)


# ---------------------------------------------------------------------------
# fixed_size_list (ITL-927)
# ---------------------------------------------------------------------------


def _fsl_table() -> pa.Table:
    return pa.table(
        {
            "k": pa.array(["a", "b"], pa.large_string()),
            "v": pa.array([[1.0, 2.0], [3.0, 4.0]], pa.list_(pa.float32(), 2)),
        }
    )


class TestFixedSizeList:
    def test_fixed_size_list_column_hashes(self):
        assert _hash_table(_fsl_table()).digest[:3] == FORMAT_2_PREFIX

    def test_fixed_size_list_differs_from_large_list_of_same_values(self):
        fsl = pa.table({"v": pa.array([[1.0, 2.0]], pa.list_(pa.float32(), 2))})
        large = pa.table({"v": pa.array([[1.0, 2.0]], pa.large_list(pa.float32()))})
        assert _hash_table(fsl) != _hash_table(large)

    def test_nested_fixed_size_list_hashes(self):
        nested = pa.table(
            {
                "s": pa.array(
                    [{"v": [1, 2]}], pa.struct([("v", pa.list_(pa.int64(), 2))])
                ),
                "l": pa.array([[[1, 2]]], pa.large_list(pa.list_(pa.int64(), 2))),
            }
        )
        assert _hash_table(nested).digest[:3] == FORMAT_2_PREFIX

    def test_source_with_fixed_size_list_column_keeps_its_type(self):
        source = ArrowTableSource(_fsl_table(), tag_columns=["k"])
        assert source.as_table().schema.field("v").type == pa.list_(pa.float32(), 2)
        hashes = [data.content_hash() for _, data in source.iter_data()]
        assert len(set(hashes)) == 2

    def test_fixed_size_list_column_runs_through_a_join(self):
        source = ArrowTableSource(_fsl_table(), tag_columns=["k"], infer_nullable=True)
        other = ArrowTableSource(
            pa.table(
                {
                    "k": pa.array(["a", "b"], pa.large_string()),
                    "w": pa.array([1, 2], pa.int64()),
                }
            ),
            tag_columns=["k"],
            infer_nullable=True,
        )
        joined = Join()(source, other)
        table = joined.as_table()
        assert table.schema.field("v").type == pa.list_(pa.float32(), 2)
        assert table.column("v").to_pylist() == [[1.0, 2.0], [3.0, 4.0]]
        assert len({data.content_hash() for _, data in joined.iter_data()}) == 2


# ---------------------------------------------------------------------------
# Canonical Arrow extension types keep their identity
# ---------------------------------------------------------------------------


class TestCanonicalExtensionTypes:
    def test_uuid_column_differs_from_its_storage(self):
        storage = pa.array([b"0" * 16, b"1" * 16], pa.binary(16))
        uuid_table = pa.table({"u": pa.ExtensionArray.from_storage(pa.uuid(), storage)})
        storage_table = pa.table({"u": storage})
        assert _hash_table(uuid_table) != _hash_table(storage_table)

    def test_fixed_shape_tensor_differs_from_its_storage(self):
        tensor_type = pa.fixed_shape_tensor(pa.float32(), [2])
        storage = pa.array([[1.0, 2.0]], pa.list_(pa.float32(), 2))
        tensor_table = pa.table(
            {"t": pa.ExtensionArray.from_storage(tensor_type, storage)}
        )
        assert _hash_table(tensor_table) != _hash_table(pa.table({"t": storage}))

    def test_uuid_schema_differs_from_its_storage(self):
        hasher = get_default_context().arrow_hasher
        uuid_schema = pa.schema([pa.field("u", pa.uuid())])
        storage_schema = pa.schema([pa.field("u", pa.binary(16))])
        assert hasher.hash_schema(uuid_schema) != hasher.hash_schema(storage_schema)  # pyright: ignore[reportAttributeAccessIssue]  # concrete hasher method, not on the protocol


# ---------------------------------------------------------------------------
# Inputs starfix 0.5 rejects
# ---------------------------------------------------------------------------


class TestRejections:
    def test_zero_row_map_column_is_rejected(self):
        table = pa.table({"m": pa.array([], pa.map_(pa.large_string(), pa.int64()))})
        with pytest.raises(ValueError, match="map"):
            _hash_table(table)

    def test_non_nullable_column_holding_a_null_is_rejected(self):
        table = pa.Table.from_arrays(
            [pa.array(["a", "b"], pa.large_string()), pa.array([1, None], pa.int64())],
            schema=pa.schema(
                [
                    pa.field("k", pa.large_string()),
                    pa.field("x", pa.int64(), nullable=False),
                ]
            ),
        )
        with pytest.raises(ValueError, match="Non-nullable field 'x'"):
            ArrowTableSource(table, tag_columns=["k"])

    def test_duplicate_column_names_are_rejected(self):
        table = pa.Table.from_arrays([pa.array([1]), pa.array([2])], names=["x", "x"])
        with pytest.raises(ValueError, match="Ambiguous node path 'x'"):
            get_default_context().semantic_hasher.hash_object(table)

    def test_slash_name_aliasing_a_struct_child_is_rejected(self):
        table = pa.table(
            {
                "k": pa.array(["a"], pa.large_string()),
                "a/b": pa.array([1], pa.int64()),
                "a": pa.array([{"b": 2}], pa.struct([("b", pa.int64())])),
            }
        )
        with pytest.raises(ValueError, match="Ambiguous node path 'a/b'"):
            ArrowTableSource(table, tag_columns=["k"])

    def test_slash_name_aliasing_nothing_is_accepted(self):
        table = pa.table(
            {
                "k": pa.array(["a"], pa.large_string()),
                "km/h": pa.array([1], pa.int64()),
            }
        )
        ArrowTableSource(table, tag_columns=["k"])
