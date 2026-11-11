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
