"""The shared labelled-hash helper (ITL-897).

A per-type value hash is ``sha256(canonical_json(descriptor) + b"\\x00" + values_digest)`` under a
versioned method label. The order-dependent values digest is ``StarfixArrowHasher.hash_table_ordered``
(tested in ``test_starfix_arrow_hasher.py``).
"""

from __future__ import annotations

import hashlib

from orcapod.hashing.labelled_hash import canonical_json, labelled_content_hash


class TestCanonicalJson:
    def test_sorted_keys_compact_separators(self) -> None:
        assert canonical_json({"b": [1, 2], "a": None}) == b'{"a":null,"b":[1,2]}'

    def test_non_ascii_kept_as_utf8(self) -> None:
        assert canonical_json({"k": "é"}) == '{"k":"é"}'.encode()

    def test_list_order_kept(self) -> None:
        assert canonical_json(["b", "a"]) == b'["b","a"]'


class TestLabelledContentHash:
    def test_method_and_digest(self) -> None:
        result = labelled_content_hash("thing_v1", {"a": 1}, b"digest")
        assert result.method == "thing_v1"
        assert result.digest == hashlib.sha256(b'{"a":1}\x00digest').digest()

    def test_descriptor_and_values_are_separated(self) -> None:
        """The 0x00 separator keeps descriptor and values digest from running together."""
        first = labelled_content_hash("x_v1", "a", b"b")
        second = labelled_content_hash("x_v1", "a\x00", b"")
        assert first != second
