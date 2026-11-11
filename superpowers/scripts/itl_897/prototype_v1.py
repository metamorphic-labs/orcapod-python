"""Prototype of the proposed pandas hash, for the ITL-897 investigation scripts only.

``install()`` patches ``PandasDataFrameHandler.handle`` / ``PandasSeriesHandler.handle`` in
the running process (never in the library) so that::

    pandas_dataframe_v1 / pandas_series_v1 =
        sha256(descriptor + b"\\x00" + values_digest)

where ``descriptor`` is canonical JSON ``{"fields": [<Arrow field name>, ...], "kind": ...}``
listing the fields of ``pa.Table.from_pandas(value, preserve_index=True)`` in order (data
columns, then index levels), and ``values_digest`` is today's Arrow hasher digest of that same
table. This is the minimal variant: it adds order only. It is not the final grammar.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import pyarrow as pa


def _labelled(label: str, table: pa.Table, values: bytes) -> Any:
    from orcapod.types import ContentHash

    descriptor = json.dumps(
        {"kind": label, "fields": table.schema.names},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    return ContentHash(label, hashlib.sha256(descriptor + b"\x00" + values).digest())


def install() -> None:
    """Patch the pandas handlers in this process with the prototype hash."""
    from orcapod.hashing.semantic_hashing import builtin_handlers as bh

    def df_handle(self: Any, obj: Any, hasher: Any) -> Any:
        table = pa.Table.from_pandas(obj, preserve_index=True)
        values = self._get_arrow_hasher().hash_table(table).digest
        return _labelled("pandas_dataframe_v1", table, values)

    def series_handle(self: Any, obj: Any, hasher: Any) -> Any:
        name = obj.name if obj.name is not None else self._UNNAMED_SENTINEL
        table = pa.Table.from_pandas(obj.to_frame(name=name), preserve_index=True)
        values = self._get_arrow_hasher().hash_table(table).digest
        return _labelled("pandas_series_v1", table, values)

    bh.PandasDataFrameHandler.handle = df_handle  # type: ignore[method-assign]
    bh.PandasSeriesHandler.handle = series_handle  # type: ignore[method-assign]


def install_arrow_table(*, separated: bool) -> None:
    """Patch ``ArrowTableHandler`` with an order-dependent ``arrow_table_v1`` prototype.

    The descriptor lists the table's top-level field names in order; the values digest is
    today's Arrow hasher digest. With ``separated=True``, ``Datagram`` and producer-less
    ``ArrowTableStream`` identities no longer route their table through the type-dispatched
    handler: they return ``arrow_hasher.hash_table(table)`` (a terminal ``ContentHash``),
    which is what the handler returns today, so stream and datagram hashes stay put.
    """
    from orcapod.core.datagrams.datagram import Datagram
    from orcapod.core.streams.arrow_table_stream import ArrowTableStream
    from orcapod.hashing.semantic_hashing import builtin_handlers as bh

    def table_handle(self: Any, obj: Any, hasher: Any) -> Any:
        if isinstance(obj, pa.RecordBatch):
            obj = pa.Table.from_batches([obj])
        values = self._get_arrow_hasher().hash_table(obj).digest
        return _labelled("arrow_table_v1", obj, values)

    bh.ArrowTableHandler.handle = table_handle  # type: ignore[method-assign]
    if not separated:
        return

    def datagram_identity(self: Any) -> Any:
        return self.data_context.arrow_hasher.hash_table(self._ensure_data_table())

    original_stream_identity = ArrowTableStream.identity_structure

    def stream_identity(self: Any) -> Any:
        structure = original_stream_identity(self)
        if self._producer is not None:
            return structure
        name, table, tag_columns = structure
        return (name, self.data_context.arrow_hasher.hash_table(table), tag_columns)

    Datagram.identity_structure = datagram_identity  # type: ignore[method-assign]
    ArrowTableStream.identity_structure = stream_identity  # type: ignore[method-assign]
