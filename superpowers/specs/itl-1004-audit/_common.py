"""Shared helpers for the ITL-1004 audit scripts (measurements only, no library change)."""

from __future__ import annotations

import asyncio
import traceback
from collections.abc import Callable
from typing import Any

import pyarrow as pa

from orcapod.sources import ArrowTableSource


def src(columns: dict[str, Any], tags: list[str], sid: str) -> ArrowTableSource:
    """Build an ArrowTableSource with explicit Arrow types and a fixed source id."""
    return ArrowTableSource(
        pa.Table.from_pydict(columns), tag_columns=tags, source_id=sid
    )


def i64(*v: Any) -> "pa.Array[Any]":
    return pa.array(list(v), type=pa.int64())


def f64(*v: Any) -> "pa.Array[Any]":
    return pa.array(list(v), type=pa.float64())


def s(*v: Any) -> "pa.Array[Any]":
    return pa.array(list(v), type=pa.large_string())


def show(title: str, fn: Callable[[], Any]) -> Any:
    """Run ``fn`` and print its result or the exception type and message."""
    print(f"--- {title}")
    try:
        out = fn()
    except Exception as exc:  # noqa: BLE001 - a measurement records any failure
        last = traceback.extract_tb(exc.__traceback__)[-1]
        print(f"    RAISES {type(exc).__name__}: {str(exc)[:300]}")
        print(f"    (raised at {last.filename.split('src/')[-1]}:{last.lineno})")
        return None
    if out is not None:
        for line in str(out).splitlines():
            print(f"    {line}")
    return out


def describe(stream: Any) -> str:
    """Keys, schema and rows of a stream, read through its public methods."""
    tk, dk = stream.keys()
    ts, ds = stream.output_schema()
    rows = [(t.as_dict(), d.as_dict()) for t, d in stream.iter_data()]
    table = stream.as_table()
    return (
        f"keys: tags={tk} data={dk}\n"
        f"schema: tags={dict(ts)} data={dict(ds)}\n"
        f"as_table columns: {table.column_names}\n"
        f"rows: {rows}"
    )


def async_rows(op: Any, *streams: Any) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Run an operator's ``async_execute`` over channels and collect the rows.

    Same harness as ``tests/test_core/operators/test_async_sync_equivalence.py``.
    """
    from orcapod.channels import Channel

    async def run() -> list[tuple[dict[str, Any], dict[str, Any]]]:
        chans = [Channel(buffer_size=64) for _ in streams]
        out = Channel(buffer_size=64)
        for st, ch in zip(streams, chans):
            for row in st.iter_data():
                await ch.writer.send(row)
            await ch.writer.close()
        await op.async_execute(
            [c.reader for c in chans],
            out.writer,
            input_pipeline_hashes=[st.pipeline_hash() for st in streams],
        )
        return [(t.as_dict(), d.as_dict()) for t, d in await out.reader.collect()]

    return asyncio.run(run())
