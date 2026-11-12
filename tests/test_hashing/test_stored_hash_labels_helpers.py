"""Helpers that find stored rows hashed under another Arrow hash label (ITL-910)."""

from __future__ import annotations

import pyarrow as pa

from orcapod.contexts import get_default_context
from orcapod.hashing.stored_hash_labels import (
    current_hash_format,
    foreign_arrow_label,
    foreign_hash_format,
)

OLD_DIGEST = "000001" + "ab" * 32
NEW_DIGEST = "000002" + "cd" * 32


class TestForeignArrowLabel:
    def test_current_label_only(self):
        values = pa.array([b"arrow_v0.2:\x00\x00\x02x", None], pa.large_binary())
        assert foreign_arrow_label(values, "arrow_v0.2") is None

    def test_other_arrow_label_is_found(self):
        values = pa.array(
            [b"arrow_v0.2:\x00\x00\x02x", b"arrow_v0.1:\x00\x00\x01y"],
            pa.large_binary(),
        )
        assert foreign_arrow_label(values, "arrow_v0.2") == "arrow_v0.1"

    def test_labels_of_other_hashers_are_not_considered(self):
        values = pa.array(
            [b"semantic_v0.1:abc", b"no-label", b"arrow_v0.2:x"], pa.large_binary()
        )
        assert foreign_arrow_label(values, "arrow_v0.2") is None

    def test_chunked_and_empty_columns(self):
        chunked = pa.chunked_array(
            [pa.array([b"arrow_v0.2:x"]), pa.array([b"arrow_v0.1:y"])]
        )
        assert foreign_arrow_label(chunked, "arrow_v0.2") == "arrow_v0.1"
        assert foreign_arrow_label(pa.array([], pa.binary()), "arrow_v0.2") is None


class TestForeignHashFormat:
    def test_current_hasher_writes_format_2(self):
        assert current_hash_format(get_default_context().arrow_hasher) == "000002"

    def test_format_1_digest_is_reported_by_its_label(self):
        values = pa.array([NEW_DIGEST.encode(), OLD_DIGEST.encode()], pa.large_binary())
        assert foreign_hash_format(values, "000002") == "arrow_v0.1"

    def test_unknown_format_is_described(self):
        values = pa.array([("000009" + "00" * 32).encode()], pa.large_binary())
        assert foreign_hash_format(values, "000002") == "starfix hash format 000009"

    def test_values_that_are_not_full_digests_are_not_considered(self):
        values = pa.array(
            [b"v014-0", b"000001", ("000001" + "ab" * 31).encode(), None],
            pa.large_binary(),
        )
        assert foreign_hash_format(values, "000002") is None
