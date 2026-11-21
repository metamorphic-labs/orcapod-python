"""Tests for schema_utils utility functions."""

from __future__ import annotations

import dataclasses

import pyarrow as pa
import pytest

from orcapod.contexts import resolve_context
from orcapod.hashing.arrow_hashers import ARROW_SCHEMA_HASH_LABEL


@pytest.fixture
def ctx():
    return resolve_context(None)


TAG = pa.schema(
    [pa.field("id", pa.int64(), nullable=False), pa.field("site", pa.large_string())]
)
DATA = pa.schema([pa.field("value", pa.float64()), pa.field("n", pa.int32())])


class TestArrowSchemaIdentity:
    def test_is_a_pair_of_labelled_schema_hashes(self, ctx):
        from orcapod.utils.schema_utils import arrow_schema_identity

        tag_hash, data_hash = arrow_schema_identity(TAG, DATA, ctx)
        assert tag_hash == ctx.arrow_hasher.hash_schema(TAG)
        assert data_hash == ctx.arrow_hasher.hash_schema(DATA)
        assert tag_hash.method == data_hash.method == ARROW_SCHEMA_HASH_LABEL

    def test_tag_and_data_are_hashed_apart(self, ctx):
        """A column moved from the tags to the data is another identity."""
        from orcapod.utils.schema_utils import arrow_schema_identity

        moved_tag = pa.schema([TAG.field("id")])
        moved_data = pa.schema([TAG.field("site"), *DATA])
        assert arrow_schema_identity(TAG, DATA, ctx) != arrow_schema_identity(
            moved_tag, moved_data, ctx
        )


class TestComputeSchemaHash:
    def test_returns_full_hex_digest(self, ctx):
        from orcapod.utils.schema_utils import compute_schema_hash

        result = compute_schema_hash(TAG, DATA, ctx, None)
        assert isinstance(result, str)
        # SHA-256 digest is 32 bytes = 64 hex characters, no label.
        assert len(result) == 64
        assert ":" not in result

    def test_deterministic(self, ctx):
        from orcapod.utils.schema_utils import compute_schema_hash

        assert compute_schema_hash(TAG, DATA, ctx, None) == compute_schema_hash(
            TAG, DATA, ctx, None
        )

    def test_length_equals_char_count_when_explicit(self, ctx):
        from orcapod.utils.schema_utils import compute_schema_hash

        assert len(compute_schema_hash(TAG, DATA, ctx, 12)) == 12

    @pytest.mark.parametrize(
        "other",
        [
            pa.field("n", pa.int64()),
            pa.field("n", pa.uint32()),
            pa.field("n", pa.int32(), nullable=False),
            pa.field("n", pa.float32()),
        ],
        ids=["width", "signedness", "nullability", "kind"],
    )
    def test_an_arrow_type_difference_is_another_hash(self, ctx, other):
        from orcapod.utils.schema_utils import compute_schema_hash

        changed = pa.schema([DATA.field("value"), other])
        assert compute_schema_hash(TAG, DATA, ctx, None) != compute_schema_hash(
            TAG, changed, ctx, None
        )

    def test_column_order_and_large_variants_are_not_part_of_it(self, ctx):
        """Two orders of two tags and two data columns, in large and plain layouts."""
        from orcapod.utils.schema_utils import compute_schema_hash

        reordered_tag = pa.schema([TAG.field("site"), TAG.field("id")])
        reordered_data = pa.schema([DATA.field("n"), DATA.field("value")])
        plain_tag = pa.schema(
            [TAG.field("id"), pa.field("site", pa.string())]
        )  # string, not large_string
        expected = compute_schema_hash(TAG, DATA, ctx, None)
        assert compute_schema_hash(reordered_tag, reordered_data, ctx, None) == expected
        assert compute_schema_hash(plain_tag, DATA, ctx, None) == expected


@dataclasses.dataclass
class _PointForDeclaration:
    """A dataclass nothing registers before the test below."""

    x: int
    y: int


def test_declared_arrow_schema_registers_only_the_field_that_needs_it() -> None:
    """A tuple field converts directly; an unregistered dataclass beside it is
    registered alone (CodeRabbit on PR 454: registration refuses tuples)."""
    import pyarrow as pa

    from orcapod.contexts import get_default_context
    from orcapod.utils.schema_utils import declared_arrow_schema

    converter = get_default_context().type_converter
    for order in (("pair", "point"), ("point", "pair")):
        declared = {"pair": tuple[int, int], "point": _PointForDeclaration}
        schema = declared_arrow_schema(
            {name: declared[name] for name in order}, converter
        )
        assert schema.names == list(order)
        assert schema.field("pair").type == pa.list_(pa.field("item", pa.int64()), 2)
        assert isinstance(schema.field("point").type, pa.ExtensionType)
