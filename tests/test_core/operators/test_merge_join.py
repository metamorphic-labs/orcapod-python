"""Tests for MergeJoin operator."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.operators import MergeJoin
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import PodProtocol


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
#
# Left and right fixtures are deliberately asymmetric:
# - Different tag column sets (left: ["id"], right: ["id", "group"])
#   to prove tag union and that inner join on shared tags works.
# - Colliding "value" column has left > right for id=2 (500 > 200)
#   and left < right for id=3 (30 < 300): argument order, never a value sort.
# - Non-overlapping ids (left has 1, right has 4) prove inner join filters.


@pytest.fixture
def left_stream() -> ArrowTableStream:
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "value": pa.array([10, 500, 30], type=pa.int64()),
            "extra_left": pa.array(["a", "b", "c"], type=pa.large_string()),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("value", pa.int64(), nullable=False),
                pa.field("extra_left", pa.large_string(), nullable=False),
            ]
        ),
    )
    return ArrowTableStream(table, tag_columns=["id"])


@pytest.fixture
def right_stream() -> ArrowTableStream:
    table = pa.table(
        {
            "id": pa.array([2, 3, 4], type=pa.int64()),
            "group": pa.array(["X", "Y", "Z"], type=pa.large_string()),
            "value": pa.array([200, 300, 400], type=pa.int64()),
            "extra_right": pa.array(["x", "y", "z"], type=pa.large_string()),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("group", pa.large_string(), nullable=False),
                pa.field("value", pa.int64(), nullable=False),
                pa.field("extra_right", pa.large_string(), nullable=False),
            ]
        ),
    )
    return ArrowTableStream(table, tag_columns=["id", "group"])


@pytest.fixture
def left_source() -> ArrowTableSource:
    return ArrowTableSource(
        pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "value": pa.array([10, 500, 30], type=pa.int64()),
                "extra_left": pa.array(["a", "b", "c"], type=pa.large_string()),
            }
        ),
        tag_columns=["id"],
        tag_only_columns=True,
        infer_nullable=True,
    )


@pytest.fixture
def right_source() -> ArrowTableSource:
    return ArrowTableSource(
        pa.table(
            {
                "id": pa.array([2, 3, 4], type=pa.int64()),
                "group": pa.array(["X", "Y", "Z"], type=pa.large_string()),
                "value": pa.array([200, 300, 400], type=pa.int64()),
                "extra_right": pa.array(["x", "y", "z"], type=pa.large_string()),
            }
        ),
        tag_columns=["id", "group"],
        tag_only_columns=True,
        infer_nullable=True,
    )


# ===================================================================
# PodProtocol conformance
# ===================================================================


class TestMergeJoinConformance:
    def test_is_pod_protocol(self):
        op = MergeJoin()
        assert isinstance(op, PodProtocol)

    def test_is_ordered(self):
        """MergeJoin is ordered, not commutative (ITL-1281)."""
        op = MergeJoin()
        assert op.is_commutative() is False


# ===================================================================
# Functional correctness
# ===================================================================


class TestMergeJoinBasic:
    def test_inner_join_on_shared_tags(self, left_stream, right_stream):
        """Only matching tag values survive the inner join."""
        op = MergeJoin()
        result = op.static_process(left_stream, right_stream)
        result_table = result.as_table()

        ids = result_table.column("id").to_pylist()
        # id=1 only in left, id=4 only in right => only 2,3 survive
        assert sorted(ids) == [2, 3]

    def test_tag_columns_are_union(self, left_stream, right_stream):
        """Output should have the union of both tag column sets."""
        op = MergeJoin()
        result = op.static_process(left_stream, right_stream)
        tag_keys, _ = result.keys()
        # left has ["id"], right has ["id", "group"] => union is {"id", "group"}
        assert set(tag_keys) == {"id", "group"}

    def test_colliding_columns_become_lists_in_argument_order(
        self, left_stream, right_stream
    ):
        """A merged list is [left value, right value], never sorted (ITL-1281).
        id=2: left=500, right=200 => [500, 200]; id=3: left=30, right=300 => [30, 300]."""
        op = MergeJoin()
        result = op.static_process(left_stream, right_stream)
        rows = {r["id"]: r for r in result.as_table().to_pylist()}
        assert rows[2]["value"] == [500, 200]
        assert rows[3]["value"] == [30, 300]

        swapped = op.static_process(right_stream, left_stream)
        rows = {r["id"]: r for r in swapped.as_table().to_pylist()}
        assert rows[2]["value"] == [200, 500]
        assert rows[3]["value"] == [300, 30]

    def test_non_colliding_columns_stay_scalar(self, left_stream, right_stream):
        """Non-colliding data columns should remain as scalars."""
        op = MergeJoin()
        result = op.static_process(left_stream, right_stream)
        rows = result.as_table().to_pylist()
        for row in rows:
            assert isinstance(row["extra_left"], str)
            assert isinstance(row["extra_right"], str)

    def test_lists_of_a_row_are_parallel(self):
        """Every colliding column of a row follows the same input order (ITL-1281).
        col_a: left > right for id=1, left < right for id=2; col_b the opposite.
        A per-column value sort would put different inputs first in the two lists."""
        left = ArrowTableStream(
            pa.table(
                {
                    "id": [1, 2],
                    "col_a": [100, 5],
                    "col_b": [1, 50],
                }
            ),
            tag_columns=["id"],
        )
        right = ArrowTableStream(
            pa.table(
                {
                    "id": [1, 2],
                    "col_a": [3, 200],
                    "col_b": [99, 2],
                }
            ),
            tag_columns=["id"],
        )

        op = MergeJoin()
        rows = {
            r["id"]: r for r in op.static_process(left, right).as_table().to_pylist()
        }
        assert rows[1]["col_a"] == [100, 3]
        assert rows[1]["col_b"] == [1, 99]
        assert rows[2]["col_a"] == [5, 200]
        assert rows[2]["col_b"] == [50, 2]

        rows = {
            r["id"]: r for r in op.static_process(right, left).as_table().to_pylist()
        }
        assert rows[1]["col_a"] == [3, 100]
        assert rows[1]["col_b"] == [99, 1]
        assert rows[2]["col_a"] == [200, 5]
        assert rows[2]["col_b"] == [2, 50]


class TestMergeJoinSourceColumns:
    def test_source_columns_follow_argument_order(self, left_source, right_source):
        """A merged column's source-info list is in argument order, as its values."""
        from orcapod.system_constants import constants

        op = MergeJoin()
        result = op.static_process(left_source, right_source)
        result_table = result.as_table(columns={"source": True})

        source_col_name = f"{constants.SOURCE_PREFIX}value"
        rows = {r["id"]: r for r in result_table.to_pylist()}
        left_id = left_source.source_id
        right_id = right_source.source_id
        # id=2 is left row 1 and right row 0; id=3 is left row 2 and right row 1.
        assert rows[2]["value"] == [500, 200]
        assert rows[2][source_col_name] == [
            f"{left_id}::row_1::value",
            f"{right_id}::row_0::value",
        ]
        assert rows[3]["value"] == [30, 300]
        assert rows[3][source_col_name] == [
            f"{left_id}::row_2::value",
            f"{right_id}::row_1::value",
        ]

    def test_non_colliding_source_columns_preserved(self, left_source, right_source):
        """Source columns for non-colliding data columns should remain as scalars."""
        from orcapod.system_constants import constants

        op = MergeJoin()
        result = op.static_process(left_source, right_source)
        result_table = result.as_table(columns={"source": True})

        left_source_col = f"{constants.SOURCE_PREFIX}extra_left"
        right_source_col = f"{constants.SOURCE_PREFIX}extra_right"

        assert left_source_col in result_table.column_names
        assert right_source_col in result_table.column_names

        rows = result_table.to_pylist()
        for row in rows:
            assert isinstance(row[left_source_col], str)
            assert isinstance(row[right_source_col], str)

    def test_source_columns_of_two_colliding_columns_are_parallel(self):
        """With two colliding columns (math, reading) whose values run in opposite
        orders, both lists and both source-info lists follow the argument order:
        element 0 is always east's, element 1 west's (ITL-1281)."""
        from orcapod.system_constants import constants

        east = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "math": pa.array([95, 40], type=pa.int64()),
                    "reading": pa.array([30, 88], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            tag_only_columns=True,
            source_id="east",
            infer_nullable=True,
        )
        west = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "math": pa.array([70, 85], type=pa.int64()),
                    "reading": pa.array([92, 10], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            tag_only_columns=True,
            source_id="west",
            infer_nullable=True,
        )

        src_math = f"{constants.SOURCE_PREFIX}math"
        src_reading = f"{constants.SOURCE_PREFIX}reading"
        for first, second in (("east", "west"), ("west", "east")):
            inputs = {"east": east, "west": west}
            values = {
                "east": {"math": [95, 40], "reading": [30, 88]},
                "west": {"math": [70, 85], "reading": [92, 10]},
            }
            result = MergeJoin().static_process(inputs[first], inputs[second])
            rows = {
                r["id"]: r
                for r in result.as_table(columns={"source": True}).to_pylist()
            }
            for i, key in enumerate([1, 2]):
                for column, source_column in (
                    ("math", src_math),
                    ("reading", src_reading),
                ):
                    assert rows[key][column] == [
                        values[first][column][i],
                        values[second][column][i],
                    ]
                    assert rows[key][source_column] == [
                        f"{first}::row_{i}::{column}",
                        f"{second}::row_{i}::{column}",
                    ]


class TestMergeJoinArgumentOrder:
    def test_argument_orders_give_reversed_lists(self, left_stream, right_stream):
        """MergeJoin(A, B) and MergeJoin(B, A) hold the same pairs, reversed."""
        op = MergeJoin()
        result_lr = op.static_process(left_stream, right_stream)
        result_rl = op.static_process(right_stream, left_stream)

        rows_lr = sorted(result_lr.as_table().to_pylist(), key=lambda r: r["id"])
        rows_rl = sorted(result_rl.as_table().to_pylist(), key=lambda r: r["id"])

        assert [r["value"] for r in rows_lr] == [r["value"][::-1] for r in rows_rl]
        # Two operations, two identities (the operator's own streams, not the
        # ``static_process`` result, carry the identity).
        assert (
            op(left_stream, right_stream).content_hash()
            != op(right_stream, left_stream).content_hash()
        )
        assert (
            op(left_stream, right_stream).pipeline_hash()
            != op(right_stream, left_stream).pipeline_hash()
        )

    def test_system_tag_positions_are_argument_indices(self, left_source, right_source):
        """Each input's system-tag columns end in its argument index."""
        from orcapod.system_constants import constants

        op = MergeJoin()
        n_char = op.orcapod_config.hashing.system_tag_n_char
        for order in ((left_source, right_source), (right_source, left_source)):
            table = op.static_process(*order).as_table(columns={"system_tags": True})
            sys_cols = [
                c
                for c in table.column_names
                if c.startswith(constants.SYSTEM_TAG_PREFIX)
            ]
            for index, stream in enumerate(order):
                suffix = f"{stream.pipeline_hash().to_hex(n_char)}:{index}"
                assert sum(c.endswith(suffix) for c in sys_cols) == 2, suffix

    def test_system_tag_values_same_pipeline_hash_follow_argument_order(self):
        """Two inputs with one pipeline hash: position 0 holds the first argument's
        source and record, position 1 the second's; nothing is sorted (ITL-1281)."""
        from orcapod.system_constants import constants

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([300, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            tag_only_columns=True,
            infer_nullable=True,
            source_id="a",
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            tag_only_columns=True,
            infer_nullable=True,
            source_id="b",
        )
        assert src_a.pipeline_hash().to_hex() == src_b.pipeline_hash().to_hex()

        op = MergeJoin()
        for first, second in ((src_a, src_b), (src_b, src_a)):
            table = op.static_process(first, second).as_table(
                columns={"system_tags": True}
            )
            for row in table.to_pylist():
                for name, value in row.items():
                    if name.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX):
                        expected = first if name.endswith(":0") else second
                        assert value == expected.source_id
                first_values = first.as_table().to_pylist()
                second_values = second.as_table().to_pylist()
                index = row["id"] - 1
                assert row["value"] == [
                    first_values[index]["value"],
                    second_values[index]["value"],
                ]


class TestMergeJoinOutputSchema:
    def test_colliding_columns_become_list_type(self, left_stream, right_stream):
        """Output schema should have list[T] for colliding columns."""
        op = MergeJoin()
        _, data_schema = op.output_schema(left_stream, right_stream)

        assert data_schema["value"] == list[int]

    def test_non_colliding_columns_stay_original_type(self, left_stream, right_stream):
        """Output schema should keep original type for non-colliding columns."""
        op = MergeJoin()
        _, data_schema = op.output_schema(left_stream, right_stream)

        assert data_schema["extra_left"] is str
        assert data_schema["extra_right"] is str

    def test_tag_schema_is_union(self, left_stream, right_stream):
        """Tag schema should be the union of both input tag schemas."""
        op = MergeJoin()
        tag_schema, _ = op.output_schema(left_stream, right_stream)

        # left tags: {"id"}, right tags: {"id", "group"}
        assert "id" in tag_schema
        assert "group" in tag_schema

    def test_output_schema_excludes_system_tags_by_default(
        self, left_source, right_source
    ):
        """Without system_tags=True, no system tag columns in tag schema."""
        from orcapod.system_constants import constants

        op = MergeJoin()
        tag_schema, _ = op.output_schema(left_source, right_source)

        for key in tag_schema:
            assert not key.startswith(constants.SYSTEM_TAG_PREFIX)

    def test_output_schema_includes_system_tags_when_requested(
        self, left_source, right_source
    ):
        """With system_tags=True, tag schema should include system tag columns."""
        from orcapod.system_constants import constants

        op = MergeJoin()
        tag_schema, _ = op.output_schema(
            left_source, right_source, columns={"system_tags": True}
        )

        sys_tag_keys = [
            k for k in tag_schema if k.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        assert len(sys_tag_keys) == 4  # 2 sources × 2 fields (source_id + record_id)

    def test_output_schema_system_tags_match_actual_output(
        self, left_source, right_source
    ):
        """Predicted system tag column names must match the actual result."""
        from orcapod.system_constants import constants

        op = MergeJoin()

        # Predicted schema
        tag_schema, _ = op.output_schema(
            left_source, right_source, columns={"system_tags": True}
        )
        predicted_sys_tags = sorted(
            k for k in tag_schema if k.startswith(constants.SYSTEM_TAG_PREFIX)
        )

        # Actual result
        result = op.static_process(left_source, right_source)
        result_table = result.as_table(columns={"system_tags": True})
        actual_sys_tags = sorted(
            c
            for c in result_table.column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )

        assert predicted_sys_tags == actual_sys_tags

    def test_output_schema_system_tags_match_with_same_pipeline_hash(self):
        """System tag prediction should work when both inputs have the
        same pipeline_hash — columns distinguished by canonical position."""
        from orcapod.system_constants import constants

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            tag_only_columns=True,
            infer_nullable=True,
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            tag_only_columns=True,
            infer_nullable=True,
        )

        assert src_a.pipeline_hash().to_hex() == src_b.pipeline_hash().to_hex()

        op = MergeJoin()
        tag_schema, _ = op.output_schema(src_a, src_b, columns={"system_tags": True})
        predicted = sorted(
            k for k in tag_schema if k.startswith(constants.SYSTEM_TAG_PREFIX)
        )

        result = op.static_process(src_a, src_b)
        actual = sorted(
            c
            for c in result.as_table(columns={"system_tags": True}).column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )

        assert predicted == actual
        # Must have 4 system tag columns (2 per source: source_id + record_id)
        assert len(predicted) == 4

    def test_output_schema_all_info_includes_system_tags(
        self, left_source, right_source
    ):
        """all_info=True should include system tag columns in the schema."""
        from orcapod.system_constants import constants

        op = MergeJoin()
        tag_schema, _ = op.output_schema(left_source, right_source, all_info=True)

        sys_tag_keys = [
            k for k in tag_schema if k.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        assert len(sys_tag_keys) == 4  # 2 sources × 2 fields

    def test_predicted_schema_matches_result_stream_schema(
        self, left_source, right_source
    ):
        """Operator's predicted output_schema must equal the result stream's
        output_schema — both tag and data schemas, without system tags."""
        op = MergeJoin()

        predicted_tag, predicted_pkt = op.output_schema(left_source, right_source)
        result = op.static_process(left_source, right_source)
        actual_tag, actual_pkt = result.output_schema()

        assert dict(predicted_tag) == dict(actual_tag)
        assert dict(predicted_pkt) == dict(actual_pkt)

    def test_predicted_schema_matches_result_stream_schema_with_system_tags(
        self, left_source, right_source
    ):
        """Operator's predicted output_schema(system_tags=True) must equal
        the result stream's output_schema(system_tags=True)."""
        op = MergeJoin()

        predicted_tag, predicted_pkt = op.output_schema(
            left_source, right_source, columns={"system_tags": True}
        )
        result = op.static_process(left_source, right_source)
        actual_tag, actual_pkt = result.output_schema(columns={"system_tags": True})

        assert dict(predicted_tag) == dict(actual_tag)
        assert dict(predicted_pkt) == dict(actual_pkt)


class TestMergeJoinValidation:
    def test_colliding_columns_with_incompatible_types_raises(self):
        """MergeJoin should reject colliding columns with different types."""
        left = ArrowTableStream(
            pa.table({"id": [1, 2], "value": [10, 20]}),
            tag_columns=["id"],
        )
        right = ArrowTableStream(
            pa.table({"id": [1, 2], "value": ["a", "b"]}),
            tag_columns=["id"],
        )

        op = MergeJoin()
        with pytest.raises(InputValidationError, match="incompatible types"):
            op.validate_inputs(left, right)

    def test_colliding_columns_with_same_types_passes(self, left_stream, right_stream):
        """MergeJoin should accept colliding columns with the same type."""
        op = MergeJoin()
        # Should not raise
        op.validate_inputs(left_stream, right_stream)


class TestMergeJoinSystemTags:
    """System tag tests demonstrating why both pipeline_hash and canonical
    position are needed."""

    @staticmethod
    def _get_system_tag_columns(table, constants):
        return sorted(
            c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )

    @staticmethod
    def _parse_system_tag_column(col, constants):
        """Parse system tag column name into its component blocks.

        Format: _tag_{field_type}::{schema_hash}::{stream_hash}:{canonical_position}
        Returns: (field_type, schema_hash, stream_hash, index)
        """
        after_prefix = col[len(constants.SYSTEM_TAG_PREFIX) :]
        blocks = after_prefix.split(constants.BLOCK_SEPARATOR)
        field_type = blocks[0]
        schema_hash = blocks[1]
        join_block_fields = blocks[2].split(constants.FIELD_SEPARATOR)
        stream_hash = join_block_fields[0]
        index = join_block_fields[1]
        return field_type, schema_hash, stream_hash, index

    def test_two_system_tag_columns_produced(self, left_source, right_source):
        """MergeJoin of two sources should produce 4 system tag columns (2 per source: source_id + record_id)."""
        from orcapod.system_constants import constants

        op = MergeJoin()
        result = op.static_process(left_source, right_source)
        result_table = result.as_table(columns={"system_tags": True})
        sys_cols = self._get_system_tag_columns(result_table, constants)
        assert len(sys_cols) == 4

    def test_system_tag_positions_are_argument_indices(self, left_source, right_source):
        """System tag columns carry each input's argument index (ITL-1281)."""
        from orcapod.config import OrcapodConfig
        from orcapod.system_constants import constants

        n_char = OrcapodConfig().hashing.system_tag_n_char

        op = MergeJoin()
        result = op.static_process(left_source, right_source)
        result_table = result.as_table(columns={"system_tags": True})
        sys_cols = self._get_system_tag_columns(result_table, constants)

        # Filter to just source_id columns for position checking
        sid_cols = [
            c for c in sys_cols if c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
        ]

        positions = {}
        for column in sid_cols:
            _, _, stream_hash, index_str = self._parse_system_tag_column(
                column, constants
            )
            positions[index_str] = stream_hash
        assert positions == {
            "0": left_source.pipeline_hash().to_hex(n_char),
            "1": right_source.pipeline_hash().to_hex(n_char),
        }

    def test_same_schema_inputs_distinguished_by_argument_position(self):
        """Two streams with identical schemas (same pipeline_hash) still produce
        distinct system tag columns, by argument index.

        Values have mixed ordering (a>b for id=1, a<b for id=2): the lists stay in
        argument order, never sorted (ITL-1281)."""
        from orcapod.system_constants import constants

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([300, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            tag_only_columns=True,
            infer_nullable=True,
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            tag_only_columns=True,
            infer_nullable=True,
        )

        # Same schema => same pipeline_hash
        assert src_a.pipeline_hash().to_hex() == src_b.pipeline_hash().to_hex()

        op = MergeJoin()
        result = op.static_process(src_a, src_b)
        result_table = result.as_table(columns={"system_tags": True})
        sys_cols = self._get_system_tag_columns(result_table, constants)

        # Must have 4 system tag columns (2 per source: source_id + record_id)
        assert len(sys_cols) == 4

        # Filter to source_id columns only for position checking
        sid_cols = [
            c for c in sys_cols if c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
        ]
        assert len(sid_cols) == 2

        # Both should have the same pipeline_hash but different positions
        _, _, hash_0, pos_0 = self._parse_system_tag_column(sid_cols[0], constants)
        _, _, hash_1, pos_1 = self._parse_system_tag_column(sid_cols[1], constants)

        assert hash_0 == hash_1  # Same pipeline hash
        assert pos_0 != pos_1  # Different argument positions
        assert {pos_0, pos_1} == {"0", "1"}

        # The merged values are in argument order: a's first.
        rows = {r["id"]: r for r in result_table.to_pylist()}
        assert rows[1]["value"] == [300, 100]
        assert rows[2]["value"] == [20, 200]

    def test_different_schema_inputs_have_different_pipeline_hashes(
        self, left_source, right_source
    ):
        """Two sources with different schemas should have different pipeline_hashes
        in their system tag columns."""
        from orcapod.system_constants import constants

        op = MergeJoin()
        result = op.static_process(left_source, right_source)
        result_table = result.as_table(columns={"system_tags": True})
        sys_cols = self._get_system_tag_columns(result_table, constants)

        # Filter to source_id columns for pipeline hash comparison
        sid_cols = [
            c for c in sys_cols if c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
        ]
        _, _, hash_0, _ = self._parse_system_tag_column(sid_cols[0], constants)
        _, _, hash_1, _ = self._parse_system_tag_column(sid_cols[1], constants)

        assert hash_0 != hash_1

    def test_system_tag_values_follow_argument_order_for_same_pipeline_hash(self):
        """When two streams share the same pipeline_hash, position :0 holds the
        first argument's (source_id, record_id) and :1 the second's, whatever their
        lexicographic order: nothing is sorted (ITL-1281). Swapping the inputs
        swaps the values; the column names are the same."""
        from orcapod.system_constants import constants

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([300, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            tag_only_columns=True,
            source_id="zzz_source",
            infer_nullable=True,
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            tag_only_columns=True,
            source_id="aaa_source",
            infer_nullable=True,
        )

        assert src_a.pipeline_hash().to_hex() == src_b.pipeline_hash().to_hex()

        op = MergeJoin()
        table_ab = op.static_process(src_a, src_b).as_table(
            columns={"system_tags": True}
        )
        table_ba = op.static_process(src_b, src_a).as_table(
            columns={"system_tags": True}
        )

        sys_cols = self._get_system_tag_columns(table_ab, constants)
        assert len(sys_cols) == 4  # 2 sources × 2 fields
        assert sys_cols == self._get_system_tag_columns(table_ba, constants)

        sid_cols = sorted(
            c for c in sys_cols if c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
        )
        assert [c[-2:] for c in sid_cols] == [":0", ":1"]
        for row in table_ab.to_pylist():
            assert row[sid_cols[0]] == "zzz_source"
            assert row[sid_cols[1]] == "aaa_source"
        for row in table_ba.to_pylist():
            assert row[sid_cols[0]] == "aaa_source"
            assert row[sid_cols[1]] == "zzz_source"


class TestMergeJoinWithListExtensionColumn:
    """Regression for ITL-627 Defect 1: MergeJoin Polars round-trip with list extension columns."""

    def test_merge_join_preserves_non_colliding_list_extension_column(self):
        """MergeJoin must not raise and must preserve extension<list[orcapod.path]>
        for a non-colliding list[Path] data column.

        MergeJoin also does a Polars round-trip; without Fix 1 it raises ValueError.
        """
        import pyarrow as pa
        from pathlib import Path
        from orcapod.contexts import get_default_context

        # Use the shared type-converter cache so both Arrow and Polars always see
        # the same extension class object, avoiding ArrowTypeError on table.cast().
        ctx = get_default_context()
        ctx.type_converter.register_python_class(list[Path])
        ext_type = ctx.type_converter.python_type_to_arrow_type(list[Path])

        storage = pa.array(
            [["/a.txt", "/b.txt"], ["/c.txt"]],
            type=pa.large_list(pa.large_string()),
        )
        ext_array = pa.ExtensionArray.from_storage(ext_type, storage)

        # Left stream: list[Path] data column (non-colliding) + shared tag
        left_table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "paths": ext_array,
            }
        )
        left_stream = ArrowTableStream(left_table, tag_columns=["id"])

        # Right stream: different non-colliding data column + same tag
        right_table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "score": pa.array([10.0, 20.0], type=pa.float64()),
            }
        )
        right_stream = ArrowTableStream(right_table, tag_columns=["id"])

        result = MergeJoin().static_process(left_stream, right_stream)  # must not raise
        out_table = result.as_table()

        paths_type = out_table.schema.field("paths").type
        assert isinstance(paths_type, pa.ExtensionType), (
            f"'paths' column must remain an extension type, got {paths_type}"
        )
        assert paths_type.extension_name == "list[orcapod.path]"

        # Data integrity: 2 rows (inner join on id=1 and id=2)
        assert len(out_table) == 2
        # Values are preserved
        paths_values = out_table.column("paths").to_pylist()
        assert len(paths_values) == 2
        assert all(
            len(row) >= 1 for row in paths_values
        )  # each row has at least one path


class TestMergeJoinLogicalTypeColumns:
    """Regression tests for ITL-627 Defect 3.

    Before Fix 3, pa.array(merged_vals) inferred the array type from raw storage
    values, producing plain large_list(storage_type) — the extension wrapper was lost.
    """

    def test_merge_join_scalar_logical_type_column_yields_list_extension(
        self, tmp_path
    ):
        """Merging a File column must produce extension<list[orcapod.file]>, not large_list.

        Before Fix 3: pa.array([[json1, json2]]) inferred large_list(large_string).
        After Fix 3: pa.ExtensionArray.from_storage(list_file_ext, ...) gives the correct type.
        """
        import pyarrow as pa
        from orcapod.logical_types.file_type import File, LogicalFile
        from orcapod.contexts import get_default_context

        ctx = get_default_context()
        # Registration required so that list[File] resolves to the extension type
        # and Polars round-trips preserve the extension wrapper.
        ctx.type_converter.register_python_class(File)
        ctx.type_converter.register_python_class(list[File])

        f1 = tmp_path / "mj1.txt"
        f1.write_text("merge left")
        f2 = tmp_path / "mj2.txt"
        f2.write_text("merge right")

        scalar_lt = LogicalFile()
        ext_type = scalar_lt.get_arrow_extension_type()

        s1 = ctx.type_converter.python_to_storage(File(f1), File)
        s2 = ctx.type_converter.python_to_storage(File(f2), File)

        left_table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "file": pa.ExtensionArray.from_storage(
                    ext_type, pa.array([s1], type=pa.large_string())
                ),
            }
        )
        right_table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "file": pa.ExtensionArray.from_storage(
                    ext_type, pa.array([s2], type=pa.large_string())
                ),
            }
        )
        left_stream = ArrowTableStream(left_table, tag_columns=["id"])
        right_stream = ArrowTableStream(right_table, tag_columns=["id"])

        result = MergeJoin().static_process(left_stream, right_stream)
        out_table = result.as_table()

        file_type = out_table.schema.field("file").type
        assert isinstance(file_type, pa.ExtensionType), (
            f"'file' column must be extension<list[orcapod.file]>, got {file_type}. "
            "Buggy code produces plain large_list(large_string)."
        )
        assert file_type.extension_name == "list[orcapod.file]"

        # Values must be a list of two storage values
        file_values = out_table.column("file").to_pylist()
        assert len(file_values) == 1, f"Expected 1 row, got {len(file_values)}"
        assert len(file_values[0]) == 2, (
            f"Expected 2 merged elements, got {len(file_values[0])}"
        )

    def test_merge_join_list_backed_column_yields_nested_list_extension(self, tmp_path):
        """Merging a list[File] column must produce extension<list[list[orcapod.file]]>.

        Fix 3 handles this naturally: elem_python_type = list[File],
        get_logical_type_for_python_type(list[list[File]]) = ListLogicalType(ListLogicalType(LogicalFile())).
        """
        import pyarrow as pa
        from orcapod.logical_types.file_type import File, LogicalFile
        from orcapod.logical_types.list_logical_type_factory import ListLogicalType
        from orcapod.contexts import get_default_context

        ctx = get_default_context()
        # Registration required so that list[File] and list[list[File]] resolve to
        # extension types and Polars round-trips preserve the extension wrapper.
        ctx.type_converter.register_python_class(File)
        ctx.type_converter.register_python_class(list[File])
        ctx.type_converter.register_python_class(list[list[File]])

        f1 = tmp_path / "nl1.txt"
        f1.write_text("nested left")
        f2 = tmp_path / "nl2.txt"
        f2.write_text("nested right")

        inner_lt = ListLogicalType(LogicalFile(), is_set=False)
        inner_ext_type = inner_lt.get_arrow_extension_type()

        s1 = ctx.type_converter.python_to_storage(File(f1), File)
        s2 = ctx.type_converter.python_to_storage(File(f2), File)

        # Each row's "files" value is a list of one file-storage-value
        left_storage = pa.array([[s1]], type=pa.large_list(pa.large_string()))
        right_storage = pa.array([[s2]], type=pa.large_list(pa.large_string()))

        left_table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "files": pa.ExtensionArray.from_storage(inner_ext_type, left_storage),
            }
        )
        right_table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "files": pa.ExtensionArray.from_storage(inner_ext_type, right_storage),
            }
        )
        left_stream = ArrowTableStream(left_table, tag_columns=["id"])
        right_stream = ArrowTableStream(right_table, tag_columns=["id"])

        result = MergeJoin().static_process(left_stream, right_stream)
        out_table = result.as_table()

        files_type = out_table.schema.field("files").type
        assert isinstance(files_type, pa.ExtensionType), (
            f"'files' column must be extension<list[list[orcapod.file]]>, got {files_type}"
        )
        assert files_type.extension_name == "list[list[orcapod.file]]"

        # One row with two inner lists
        files_values = out_table.column("files").to_pylist()
        assert len(files_values) == 1, f"Expected 1 row, got {len(files_values)}"
        assert len(files_values[0]) == 2, (
            f"Expected 2 merged inner lists, got {len(files_values[0])}"
        )
