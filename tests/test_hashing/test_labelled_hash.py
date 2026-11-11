"""The shared labelled-hash helper (ITL-897).

A per-type value hash is ``sha256(canonical_json(descriptor) + b"\\x00" + values_digest)`` under a
versioned method label. ``ordered_arrow_digest`` is the one place Orcapod asks starfix for an
order-dependent digest (starfix 0.5, hash format 2, ITL-898).
"""

from __future__ import annotations

import hashlib

import pyarrow as pa

from orcapod.hashing.labelled_hash import (
    canonical_json,
    labelled_content_hash,
    ordered_arrow_digest,
)


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


UV = pa.struct([("u", pa.int64()), ("v", pa.large_string())])
VU = pa.struct([("v", pa.large_string()), ("u", pa.int64())])


class TestOrderedArrowDigest:
    def test_column_order_is_part_of_the_digest(self) -> None:
        table = pa.table({"a": [1, 2], "b": ["x", "y"]})
        assert ordered_arrow_digest(table) != ordered_arrow_digest(
            table.select(["b", "a"])
        )

    def test_struct_field_order_is_part_of_the_digest(self) -> None:
        first = pa.table({"s": pa.array([{"u": 1, "v": "x"}], UV)})
        second = pa.table({"s": pa.array([{"v": "x", "u": 1}], VU)})
        assert ordered_arrow_digest(first) != ordered_arrow_digest(second)

    def test_nested_struct_field_order_is_part_of_the_digest(self) -> None:
        first = pa.table({"s": pa.array([[{"u": 1, "v": "x"}]], pa.large_list(UV))})
        second = pa.table({"s": pa.array([[{"v": "x", "u": 1}]], pa.large_list(VU))})
        assert ordered_arrow_digest(first) != ordered_arrow_digest(second)

    def test_chunking_is_not_part_of_the_digest(self) -> None:
        table = pa.table({"a": [1, 2], "b": ["x", "y"]})
        rechunked = pa.concat_tables([table.slice(0, 1), table.slice(1, 1)])
        assert ordered_arrow_digest(table) == ordered_arrow_digest(rechunked)

    def test_values_are_part_of_the_digest(self) -> None:
        assert ordered_arrow_digest(pa.table({"a": [1]})) != ordered_arrow_digest(
            pa.table({"a": [2]})
        )

    def test_extension_column_hashes_by_storage(self) -> None:
        """pyarrow's own extension types (pandas Period) reduce to their storage."""
        import pandas as pd

        table = pa.Table.from_pandas(
            pd.DataFrame({"p": pd.period_range("2020-01", periods=2, freq="M")})
        )
        column = table.column("p")
        assert isinstance(column.type, pa.ExtensionType)
        storage = pa.chunked_array(
            [chunk.storage for chunk in column.chunks],  # pyright: ignore[reportAttributeAccessIssue]
        )
        plain = pa.Table.from_arrays([storage], names=["p"])
        assert ordered_arrow_digest(table) == ordered_arrow_digest(plain)
