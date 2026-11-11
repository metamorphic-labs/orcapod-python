"""A ``pa.Table`` hashed as a value is order dependent; Orcapod streams stay order independent.

Developer decision, November 11, 2026 (ITL-897): a ``pa.Table`` or ``pa.RecordBatch`` used as a
value has an order-dependent identity, as ``pa.Table.equals`` does. Orcapod streams and their
rows (datagrams) deliberately ignore column order. Today both reach the same handler, so the
stream guards below pin the behaviour that must survive the change.

The tests assert only distinctness and equality, not a hash label or digest: the mechanism is not
decided yet (ITL-898).
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
