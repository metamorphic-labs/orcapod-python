"""``OrcapodTable``: an Arrow table Orcapod built, as it enters an identity structure.

The semantic hasher hashes a bare ``pa.Table`` or ``pa.RecordBatch`` as a VALUE: strictly,
every bit counts (``ArrowTableHandler``, label ``arrow_strict_v1``). A table Orcapod built
itself (a datagram's row, a raw stream's table) is hashed instead by the Arrow hasher's
``hash_table``, under Orcapod's float rule (``-0.0`` as ``0.0``, every NaN as the ordinary
NaN; label ``arrow_v1``; ITL-1179). An identity structure marks such a table by wrapping it
in ``OrcapodTable``, which the semantic hasher sends to ``OrcapodTableHandler``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa


class OrcapodTable:
    """A table Orcapod built, in an identity structure.

    Two are equal when their tables are equal (``pa.Table.equals``), as two bare tables
    in an identity structure compared before; nothing is hashed to compare them.

    Args:
        table: The table.
    """

    __slots__ = ("table",)

    def __init__(self, table: "pa.Table") -> None:
        self.table = table

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OrcapodTable):
            return NotImplemented
        equal: bool = self.table.equals(other.table)  # pyright: ignore[reportAssignmentType]  # the stub returns Table; pyarrow returns bool
        return equal

    __hash__ = None  # pyright: ignore[reportAssignmentType]  # mutable-equality wrapper, as pa.Table

    def __repr__(self) -> str:
        return f"OrcapodTable({self.table.num_rows} rows, {self.table.column_names})"
