"""Comparing the rows a source reads again with the rows it holds (ITL-1102).

A table-backed source that is asked at the start of a run whether its data changed
(``check_for_new_data``) reads its table again and compares. The comparison ignores row
order: a table read again in another order (an unordered ``SELECT``, a Delta table whose
files are scanned in another order) holds the same data, and must not make every node
below the source run again.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.ipc as pa_ipc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")
    pa_ipc = LazyModule("pyarrow.ipc")
    pl = LazyModule("polars")


def same_rows(held: pa.Table, read: pa.Table) -> bool:
    """Return whether two tables hold the same rows, in any order.

    The tables are equal as multisets of rows: the same schema (field metadata
    included), the same number of rows, and each row as many times in one as in the
    other. A difference is never missed: tables in the same order are compared
    directly; otherwise both are put in the order of a per-row hash and compared
    exactly, so a hash collision can only report a change that is not there.

    Args:
        held: The rows the source holds.
        read: The rows just read again.

    Returns:
        ``True`` if the rows are the same.
    """
    if held.num_rows != read.num_rows or not held.schema.equals(
        read.schema, check_metadata=True
    ):
        return False
    if held.equals(read):
        return True
    held_hashes = _row_hashes(held)
    read_hashes = _row_hashes(read)
    held_order: pa.Array[Any] = pc.sort_indices(held_hashes)  # pyright: ignore[reportUnknownMemberType]  # pyarrow.compute is untyped
    read_order: pa.Array[Any] = pc.sort_indices(read_hashes)  # pyright: ignore[reportUnknownMemberType]  # pyarrow.compute is untyped
    held_sorted: pa.Table = held.take(held_order)
    read_sorted: pa.Table = read.take(read_order)
    return bool(held_sorted.equals(read_sorted))


def _row_hashes(table: pa.Table) -> "pa.Array[Any]":
    """Return one hash per row, used only to put two tables in a common order.

    Polars hashes the rows of tables it can hold. A table it cannot hold (a column of
    a type Polars does not know) is hashed row by row from its Arrow IPC bytes.
    """
    try:
        return pl.from_arrow(table).hash_rows(seed=0).to_arrow()  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]  # a table always gives a DataFrame
    except Exception:
        return pa.array(
            [_ipc_bytes(table.slice(i, 1)) for i in range(table.num_rows)],
            type=pa.large_binary(),
        )


def _ipc_bytes(row: pa.Table) -> bytes:
    """Return the Arrow IPC stream bytes of a one-row table."""
    sink = pa.BufferOutputStream()
    with pa_ipc.new_stream(sink, row.schema) as writer:
        writer.write_table(row)
    return sink.getvalue().to_pybytes()
