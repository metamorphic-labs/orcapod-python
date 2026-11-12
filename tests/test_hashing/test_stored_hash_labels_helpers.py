"""Helpers that tell stored rows of one Arrow hash label from another (ITL-910)."""

from __future__ import annotations

import logging

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.databases import InMemoryArrowDatabase
from orcapod.hashing.stored_hash_labels import (
    current_hash_format,
    hash_labels_of,
    keep_rows_hashed_under,
    other_arrow_label,
    report_skipped_rows,
    stored_hash_labels,
)

OLD_KEY = b"arrow_v0.1:\x00\x00\x01y"
NEW_KEY = b"arrow_v0.2:\x00\x00\x02x"
OLD_DIGEST = "000001" + "ab" * 32
NEW_DIGEST = "000002" + "cd" * 32


class TestHashLabelsOf:
    def test_prefixed_digests_give_their_arrow_label(self):
        values = pa.array([NEW_KEY, OLD_KEY, None], pa.large_binary())
        assert hash_labels_of(values).to_pylist() == ["arrow_v0.2", "arrow_v0.1", None]

    def test_hex_digests_give_their_starfix_format(self):
        values = pa.array([NEW_DIGEST.encode(), OLD_DIGEST.encode()], pa.large_binary())
        assert hash_labels_of(values).to_pylist() == ["000002", "000001"]

    def test_string_columns_are_read_like_binary_ones(self):
        values = pa.array([NEW_DIGEST, "arrow_v0.1:abc"], pa.large_string())
        assert hash_labels_of(values).to_pylist() == ["000002", "arrow_v0.1"]

    def test_other_values_carry_no_label(self):
        values = pa.array(
            [
                b"semantic_v0.1:abc",  # another hasher's label
                b"hand-made-id",
                b"arrow_v0.1",  # no digest after the label
                b"000001abcd",  # not a full starfix digest
                OLD_DIGEST.upper().encode(),
                b"\xff\xfe:\x00",  # not UTF-8
            ],
            pa.large_binary(),
        )
        assert hash_labels_of(values).to_pylist() == [None] * 6

    def test_chunked_and_empty_columns(self):
        chunked = pa.chunked_array(
            [pa.array([NEW_KEY], pa.binary()), pa.array([OLD_KEY], pa.binary())]
        )
        assert hash_labels_of(chunked).to_pylist() == ["arrow_v0.2", "arrow_v0.1"]
        assert hash_labels_of(pa.array([], pa.large_binary())).to_pylist() == []


class TestKeepRowsHashedUnder:
    def test_rows_of_another_label_are_dropped(self):
        table = pa.table(
            {"key": pa.array([OLD_KEY, NEW_KEY], pa.large_binary()), "v": [1, 2]}
        )
        kept = keep_rows_hashed_under(table, "key", "arrow_v0.2")
        assert kept.to_pylist() == [{"key": NEW_KEY, "v": 2}]
        assert kept.schema == table.schema

    def test_rows_of_another_starfix_format_are_dropped(self):
        table = pa.table(
            {
                "h": pa.array([OLD_DIGEST.encode(), NEW_DIGEST.encode()]),
                "v": [1, 2],
            }
        )
        assert keep_rows_hashed_under(table, "h", "000002")["v"].to_pylist() == [2]

    def test_rows_without_a_label_are_kept(self):
        table = pa.table(
            {"key": pa.array([b"hand-made", None, OLD_KEY]), "v": [1, 2, 3]}
        )
        assert keep_rows_hashed_under(table, "key", "arrow_v0.2")["v"].to_pylist() == [
            1,
            2,
        ]

    def test_table_of_the_current_label_is_returned_unchanged(self):
        table = pa.table({"key": pa.array([NEW_KEY, NEW_KEY]), "v": [1, 2]})
        assert keep_rows_hashed_under(table, "key", "arrow_v0.2") is table

    def test_empty_table(self):
        table = pa.table({"key": pa.array([], pa.large_binary())})
        assert keep_rows_hashed_under(table, "key", "arrow_v0.2").num_rows == 0


class TestOtherArrowLabel:
    def test_none_when_every_label_is_current(self):
        values = pa.array([NEW_KEY, None, b"hand-made"], pa.large_binary())
        assert other_arrow_label(values, "arrow_v0.2") is None

    def test_another_label_is_found(self):
        values = pa.array([NEW_KEY, OLD_KEY], pa.large_binary())
        assert other_arrow_label(values, "arrow_v0.2") == "arrow_v0.1"

    def test_empty_column(self):
        assert other_arrow_label(pa.array([], pa.large_binary()), "arrow_v0.2") is None


class TestReportSkippedRows:
    def test_warns_with_the_total_and_the_table(self, caplog: pytest.LogCaptureFixture):
        with caplog.at_level(logging.WARNING):
            report_skipped_rows(("job", "fn", "pdb_v1"), 3)
            report_skipped_rows(("job", "fn", "rdb_v1"), 1)
        first, second = (record.getMessage() for record in caplog.records)
        assert first == (
            "Skipped 3 rows of stored table 'job/fn/pdb_v1': written under another "
            "hash label than the current one. The rows are left in place unchanged "
            "and can be migrated later."
        )
        assert second.startswith("Skipped 1 row of stored table 'job/fn/rdb_v1':")
        assert {record.levelno for record in caplog.records} == {logging.WARNING}


class TestCurrentHashFormat:
    def test_current_hasher_writes_format_2(self):
        assert current_hash_format(get_default_context().arrow_hasher) == "000002"


class TestStoredHashLabels:
    def _store(self, column: str, values: list[bytes]) -> InMemoryArrowDatabase:
        store = InMemoryArrowDatabase()
        table = pa.table(
            {
                "__rid": pa.array(
                    [f"r{i}".encode() for i in range(len(values))], pa.large_binary()
                ),
                column: pa.array(values, pa.large_binary()),
            }
        )
        store.add_records(("t",), table, record_id_column="__rid", flush=True)
        return store

    def test_counts_rows_per_label(self):
        store = self._store("__input_data_hash", [OLD_KEY, NEW_KEY, NEW_KEY])
        assert stored_hash_labels(store, ("t",)) == {"arrow_v0.1": 1, "arrow_v0.2": 2}

    def test_rows_without_a_label_are_counted_under_none(self):
        store = self._store("__input_data_hash", [b"hand-made", NEW_KEY])
        assert stored_hash_labels(store, ("t",)) == {None: 1, "arrow_v0.2": 1}

    def test_record_ids_are_read_when_no_key_column_is_stored(self):
        """Operator-node and cached-source tables keep their hash as the record id."""
        store = InMemoryArrowDatabase()
        table = pa.table(
            {
                "_record_hash": pa.array(
                    [OLD_DIGEST.encode(), NEW_DIGEST.encode()], pa.large_binary()
                ),
                "v": [1, 2],
            }
        )
        store.add_records(("t",), table, record_id_column="_record_hash", flush=True)
        assert stored_hash_labels(store, ("t",)) == {"arrow_v0.1": 1, "arrow_v0.2": 1}

    def test_unknown_starfix_format_is_named(self):
        store = InMemoryArrowDatabase()
        table = pa.table(
            {"_record_hash": pa.array([("000009" + "ab" * 32).encode()]), "v": [1]}
        )
        store.add_records(("t",), table, record_id_column="_record_hash", flush=True)
        assert stored_hash_labels(store, ("t",)) == {"starfix hash format 000009": 1}

    def test_missing_table(self):
        assert stored_hash_labels(InMemoryArrowDatabase(), ("t",)) == {}
