"""ITL-640 Q4: where does a source's Arrow schema come from before it holds rows, and
does it equal the Arrow schema of the rows that arrive?

Run: ``uv run python docs/dev/scripts/itl_640/q4_sources_without_rows.py [tmpdir]``

For each kind of source, three Arrow schemas of its (tag, data) columns are compared
with ``StarfixArrowHasher.hash_schema``:

* ``zero-row``: ``typed_zero_row_table`` (ITL-912), before any row is read;
* ``converter``: the data context's converter applied to the declared Python schema;
* ``rows``: the schema of ``as_table()`` once rows exist.

The source table has an ``int32`` tag ``k``, an ``int8`` data column ``x`` and a
``timestamp[ns]`` data column ``t``: three types the converter does not produce from
``int`` and ``datetime``.
"""

from __future__ import annotations

import datetime as dt
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Any

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes.source_node import SourceNode
from orcapod.core.streams.base import typed_zero_row_table
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.sources import ArrowTableSource
from orcapod.core.sources.cached_source import CachedSource
from orcapod.core.sources.polling_source import PollingSource
from orcapod.types import Cursor, Schema

warnings.simplefilter("ignore")

TS = [dt.datetime(2026, 1, 1), dt.datetime(2026, 1, 2)]


def table() -> pa.Table:
    return pa.Table.from_arrays(
        [
            pa.array([1, 2], pa.int32()),
            pa.array([1, 2], pa.int8()),
            pa.array(TS, pa.timestamp("ns")),
        ],
        schema=pa.schema(
            [
                pa.field("k", pa.int32(), nullable=False),
                pa.field("x", pa.int8(), nullable=False),
                pa.field("t", pa.timestamp("ns"), nullable=False),
            ]
        ),
    )


@function_pod(output_keys="y")
def double(x: int) -> int:
    return x * 2


def user_schema(stream: Any, arrow_schema: pa.Schema) -> pa.Schema:
    tag_keys, data_keys = stream.keys()
    names = [*tag_keys, *[n for n in data_keys if n not in tag_keys]]
    return pa.schema([arrow_schema.field(n) for n in names])


def describe(schema: pa.Schema | None) -> str:
    if schema is None:
        return "None"
    return "; ".join(
        f"{f.name}: {f.type}{'' if f.nullable else ' not null'}" for f in schema
    )


def schemas(stream: Any, with_rows: bool = True) -> dict[str, pa.Schema | None]:
    ctx = stream.data_context
    out: dict[str, pa.Schema | None] = {}
    z = typed_zero_row_table(stream)
    out["zero-row"] = None if z is None else user_schema(stream, z.schema)
    tag, data = stream.output_schema()
    out["converter"] = ctx.type_converter.python_schema_to_arrow_schema(
        Schema({**dict(tag), **dict(data)})
    )
    if with_rows:
        try:
            t = stream.as_table()
            out["rows"] = user_schema(stream, t.schema)
        except Exception as exc:  # noqa: BLE001
            print(f"    as_table FAILED: {type(exc).__name__}: {exc}")
            out["rows"] = None
    return out


def report(label: str, stream: Any, with_rows: bool = True) -> None:
    ctx = stream.data_context
    found = schemas(stream, with_rows)
    print(f"== {label}")
    hashes = {}
    for name, schema in found.items():
        print(f"    {name:10} {describe(schema)}")
        hashes[name] = None if schema is None else ctx.arrow_hasher.hash_schema(schema)
    keys = [k for k, v in hashes.items() if v is not None]
    for i, a in enumerate(keys):
        for b in keys[i + 1 :]:
            print(f"    {a} == {b}: {hashes[a] == hashes[b]}")


class Impl:
    """A polling implementation declaring Python types, fetching a fixed table."""

    def __init__(self, fetched: Any) -> None:
        self._fetched = fetched

    def identity(self) -> Any:
        return ("q4", 1)

    def to_config(self) -> dict[str, Any] | None:
        return None

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "Impl":
        raise NotImplementedError

    def schema(self) -> Any:
        return Schema({"k": int, "x": int, "t": dt.datetime})

    async def poll(self, cursor: Any = None) -> bool:
        return cursor is None

    async def fetch(self, cursor: Any = None) -> Any:
        return Cursor.now(1), self._fetched

    async def close(self) -> None:
        return None


def main(root: Path) -> None:
    source = ArrowTableSource(
        table(), tag_columns=["k"], tag_only_columns=True, source_id="s"
    )
    report("ArrowTableSource (holds its table)", source)

    for kind in ("mem", "delta"):
        db = (
            InMemoryArrowDatabase()
            if kind == "mem"
            else DeltaTableDatabase(root / kind)
        )
        job = PipelineJob(name="j", database=db)
        with job:
            double.pod(source, label="f")
            source.map_data({"x": "z"}, label="op")
        f, opn = job.nodes["f"], job.nodes["op"]
        report(f"[{kind}] function job node, before run", f, with_rows=False)
        report(f"[{kind}] operator job node, before run", opn, with_rows=False)
        report(f"[{kind}] f.as_source(), before run", f.as_source())
        report(f"[{kind}] op.as_source(), before run", opn.as_source())
        job.run()
        report(f"[{kind}] function job node, after run", f)
        report(f"[{kind}] operator job node, after run", opn)
        report(f"[{kind}] f.as_source(), after run", f.as_source())
        report(f"[{kind}] op.as_source(), after run", opn.as_source())
        slot = job.nodes.get("s")
        if slot is not None:
            report(f"[{kind}] input slot of the job (bound)", slot)
        cache_db = (
            InMemoryArrowDatabase()
            if kind == "mem"
            else DeltaTableDatabase(root / "cache")
        )
        cached = CachedSource(source, cache_database=cache_db)
        report(f"[{kind}] CachedSource over the ArrowTableSource", cached)
        cached_derived = CachedSource(f.as_source(), cache_database=cache_db)
        report(f"[{kind}] CachedSource over f.as_source()", cached_derived)

    slot = SourceNode(
        name="declared", tag_schema=Schema({"k": int}), data_schema=Schema({"x": int})
    )
    report("SourceNode declared by hand (unbound)", slot, with_rows=False)

    print()
    print("PollingSource: declared {k: int, x: int, t: datetime}")
    for label, fetched in (
        ("pa.Table int32/int8/ts[ns]", table()),
        ("Python dicts", {"k": [1, 2], "x": [1, 2], "t": TS}),
    ):
        p = PollingSource(Impl(fetched), tag_columns="k", record_id_columns="k")
        report(f"PollingSource before poll, fetch returns {label}", p, with_rows=False)
        before = p.pipeline_hash()
        report(f"PollingSource after poll, fetch returned {label}", p)
        print("    pipeline_hash unchanged by the poll:", before == p.pipeline_hash())


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(Path(sys.argv[1]))
    else:
        with tempfile.TemporaryDirectory() as tmp:
            main(Path(tmp))
