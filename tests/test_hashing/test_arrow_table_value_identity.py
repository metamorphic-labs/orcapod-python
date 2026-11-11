"""A ``pa.Table`` hashed as a value is order dependent; Orcapod streams stay order independent.

Developer decision, November 11, 2026 (ITL-897): a ``pa.Table`` or ``pa.RecordBatch`` used as a
value has an order-dependent identity, as ``pa.Table.equals`` does. Orcapod streams and their
rows (datagrams) deliberately ignore column order. Today both reach the same handler, so the
stream guards below pin the behaviour that must survive the change.

Developer decisions, November 11, 2026: the value hash is labelled ``arrow_table_v1``, shared by
``pa.Table`` and ``pa.RecordBatch``, and gets the same semantic preprocessing as every Orcapod Arrow
hash (a ``File`` column by its content); only starfix's order options differ (ITL-898).
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.sources import ArrowTableSource
from orcapod.streams import ArrowTableStream


def _hash(value: Any) -> str:
    return get_default_context().semantic_hasher.hash_object(value).to_string()


def _table() -> pa.Table:
    return pa.table(
        {
            "k": pa.array(["r1", "r2"], pa.large_string()),
            "a": pa.array([1, 2], pa.int64()),
            "b": pa.array(["x", "y"], pa.large_string()),
        }
    )


UV = pa.struct([("u", pa.int64()), ("v", pa.large_string())])
VU = pa.struct([("v", pa.large_string()), ("u", pa.int64())])

ORDER_PAIRS: dict[str, tuple[pa.Table, pa.Table]] = {
    "column order": (_table(), _table().select(["b", "a", "k"])),
    "struct field order": (
        pa.table({"s": pa.array([{"u": 1, "v": "x"}], UV)}),
        pa.table({"s": pa.array([{"v": "x", "u": 1}], VU)}),
    ),
    "struct field order in a list": (
        pa.table({"s": pa.array([[{"u": 1, "v": "x"}]], pa.large_list(UV))}),
        pa.table({"s": pa.array([[{"v": "x", "u": 1}]], pa.large_list(VU))}),
    ),
}


class TestTableValueOrderIsIdentity:
    @pytest.mark.parametrize("name", list(ORDER_PAIRS))
    def test_arrow_itself_distinguishes_the_pair(self, name: str) -> None:
        first, second = ORDER_PAIRS[name]
        assert not first.equals(second)

    @pytest.mark.parametrize("name", list(ORDER_PAIRS))
    def test_tables_differing_only_in_order_hash_differently(self, name: str) -> None:
        first, second = ORDER_PAIRS[name]
        assert _hash(first) != _hash(second)

    def test_record_batches_differing_only_in_column_order_hash_differently(
        self,
    ) -> None:
        first, second = ORDER_PAIRS["column order"]
        assert _hash(first.to_batches()[0]) != _hash(second.to_batches()[0])

    def test_equal_tables_hash_equal(self) -> None:
        assert _hash(_table()) == _hash(_table())

    def test_chunking_is_not_identity(self) -> None:
        table = _table()
        rechunked = pa.concat_tables([table.slice(0, 1), table.slice(1, 1)])
        assert table.equals(rechunked)
        assert _hash(table) == _hash(rechunked)


class TestTableValueLabel:
    def test_table_label(self) -> None:
        assert get_default_context().semantic_hasher.hash_object(_table()).method == (
            "arrow_table_v1"
        )

    def test_label_is_pinned_to_the_arrow_hasher(self) -> None:
        """A label is bumped whenever the hash of any value can change (developer, Nov 11, 2026).

        ``arrow_table_v1`` embeds the Arrow hasher's digest, so a new Arrow hasher (a new
        ``hasher_id``) needs a new label. When this fails, bump the label and then the pin;
        update the pin alone only while the label is still unreleased.
        """
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            ARROW_TABLE_HASH_METHOD,
        )

        pinned = {"arrow_v0.1": "arrow_table_v1"}
        hasher_id = get_default_context().arrow_hasher.hasher_id
        assert pinned.get(hasher_id) == ARROW_TABLE_HASH_METHOD, (
            f"Arrow hasher is {hasher_id!r}: bump arrow_table_v1, then this pin"
        )

    def test_record_batch_shares_the_table_label_and_hash(self) -> None:
        hasher = get_default_context().semantic_hasher
        batch = _table().to_batches()[0]
        assert hasher.hash_object(batch).method == "arrow_table_v1"
        assert hasher.hash_object(batch) == hasher.hash_object(_table())

    def test_file_column_hashed_by_content(self, tmp_path: Any) -> None:
        """Semantic preprocessing applies to table values: a ``File`` column by its content."""
        from orcapod.logical_types.file_type import File

        converter: Any = get_default_context().type_converter
        arrow_type = converter.register_python_class(File)

        def file_table(path: Any) -> pa.Table:
            storage = converter.python_to_storage(File(path), File)
            column = pa.ExtensionArray.from_storage(
                arrow_type, pa.array([storage], arrow_type.storage_type)
            )
            return pa.Table.from_arrays([column], names=["f"])

        same_a, same_b, other = (tmp_path / n for n in ("a.txt", "b.txt", "c.txt"))
        same_a.write_text("same")
        same_b.write_text("same")
        other.write_text("other")
        assert _hash(file_table(same_a)) == _hash(file_table(same_b))
        assert _hash(file_table(same_a)) != _hash(file_table(other))


class TestStreamsStayOrderIndependent:
    """Guards: these pass before the change and must pass after it."""

    def test_stream_content_hash_ignores_column_order(self) -> None:
        first, second = ORDER_PAIRS["column order"]
        assert (
            ArrowTableStream(first, tag_columns=["k"]).content_hash()
            == ArrowTableStream(second, tag_columns=["k"]).content_hash()
        )

    def test_row_data_hash_ignores_column_order(self) -> None:
        first, second = ORDER_PAIRS["column order"]
        rows = [
            {
                tag["k"]: data.content_hash()
                for tag, data in ArrowTableSource(
                    table, tag_columns=["k"], source_id="s"
                ).iter_data()
            }
            for table in (first, second)
        ]
        assert rows[0] == rows[1]

    def test_record_ids_ignore_column_order(self) -> None:
        first, second = ORDER_PAIRS["column order"]
        assert _record_ids(first) == _record_ids(second)


def _record_ids(table: pa.Table) -> list[Any]:
    full = ArrowTableSource(table, tag_columns=["k"], source_id="s").as_table(
        all_info=True
    )
    names = [n for n in full.column_names if n.startswith("_tag_record_id")]
    assert names
    return full.select(names).to_pylist()
