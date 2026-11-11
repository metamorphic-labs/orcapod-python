"""Arrow IPC stream helpers shared by the frame-valued logical types.

``LogicalPandasDataFrame``, ``LogicalPandasSeries`` and ``LogicalPolarsDataFrame`` all store
their values as Arrow IPC stream bytes in a ``large_binary`` column. These two functions are
that envelope.
"""

from __future__ import annotations

import io
from typing import Any

import pyarrow as pa
import pyarrow.ipc


def table_to_ipc_bytes(table: pa.Table) -> bytes:
    """Serialise an Arrow Table to IPC stream bytes.

    Args:
        table: The Arrow Table to serialise.

    Returns:
        Raw bytes in Arrow IPC stream format.
    """
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, table.schema) as writer:
        writer.write_table(table)
    return buf.getvalue()


def ipc_bytes_to_table(data: Any) -> pa.Table:
    """Deserialise IPC stream bytes to an Arrow Table.

    Args:
        data: Raw bytes (or buffer) containing an Arrow IPC stream.

    Returns:
        The deserialised Arrow Table.
    """
    buf = io.BytesIO(bytes(data))
    return pa.ipc.open_stream(buf).read_all()
