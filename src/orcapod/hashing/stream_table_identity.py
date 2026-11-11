"""An Orcapod table (a stream's or a row's) inside an identity structure.

Orcapod streams and their rows ignore column order and struct field order on purpose, so their
tables hash with the data context's order-independent ``arrow_hasher.hash_table``. A ``pa.Table``
handed to the semantic hasher as a value goes to ``ArrowTableHandler`` instead, which keeps the
order (``arrow_table_v1``, ITL-897). ``StreamTableIdentity`` is how a stream or a datagram puts its
table in its identity structure without taking the value route.

It is lazy: nothing is hashed until the identity is hashed. Two identities are equal when their
tables are equal (``pa.Table.equals``), which is what comparing identity structures that held the
table itself used to do, so ``==`` on streams and datagrams still never hashes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.hashing_protocols import ArrowHasherProtocol
    from orcapod.types import ContentHash


class StreamTableIdentity:
    """A stream's or a row's table, hashed without its column or struct field order.

    Args:
        table: The table.
        arrow_hasher: The data context's Arrow hasher; its ``hash_table`` is the identity.
    """

    __slots__ = ("_arrow_hasher", "_table")

    def __init__(self, table: pa.Table, arrow_hasher: ArrowHasherProtocol) -> None:
        self._table = table
        self._arrow_hasher = arrow_hasher

    def identity_structure(self) -> Any:
        """Return the order-independent Arrow hash of the table."""
        return self.content_hash()

    def content_hash(self, hasher: Any = None) -> ContentHash:
        """Return ``arrow_hasher.hash_table(table)``.

        Args:
            hasher: Ignored; the identity is the Arrow hasher's. Present for protocol
                conformance.

        Returns:
            The order-independent Arrow hash of the table.
        """
        return self._arrow_hasher.hash_table(self._table)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, StreamTableIdentity):
            return NotImplemented
        return bool(self._table.equals(other._table))

    def __hash__(self) -> int:
        """Unhashable, like ``pa.Table``: equality is by table content."""
        raise TypeError("StreamTableIdentity is unhashable")
