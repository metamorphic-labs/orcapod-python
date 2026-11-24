"""ITL-1446: a throwaway prototype of option (a), to measure it on pyspiral's disk fake.

Not the implementation: it maps names and types in the connector, keeps the logical schema
in the table's key-value metadata, and runs a whole job on the fake. It answers three
questions with numbers:

1. ``codec``: is the column-name encoding injective (random names, adversarial alphabet)?
2. ``job``: does a job (function, join, filter, group by, second function, side effect) give
   the in-memory store's rows, record ids and hashes, and does a rerun call nothing?
3. ``cost``: how many SpiralDB calls does each run make today, by number of rows?

Run: ``PYTHONPATH=. uv run python docs/dev/specs/itl-1446/prototype.py``
"""

from __future__ import annotations

import base64
import hashlib
import random
import tempfile
from collections import Counter
from contextlib import contextmanager
from typing import Any, cast
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Batch, GroupBy, Join, PolarsFilter
from orcapod.core.sources import ArrowTableSource, DerivedSource
from orcapod.core.sources.cached_source import CachedSource
from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    InMemoryArrowDatabase,
    SpiralDBConnector,
)
from orcapod.side_effects import SideEffectPod, SideEffectPodConfig
from orcapod.types import CacheMode, ColumnInfo
from orcapod.utils.arrow_utils import normalize_table_to_large_types
from tests.test_databases import fake_pyspiral

# ---------------------------------------------------------------- column-name codec

MARK = "ox_"
ESCAPED = frozenset(".$*;") | {chr(i) for i in range(32)} | {chr(127)}


def encode(name: str) -> str:
    """The stored name of a value column (section 3.1 of the design)."""
    plain = (
        name != ""
        and not name.startswith(("__", MARK))
        and not any(c in ESCAPED and c != ";" for c in name)
    )
    if plain:
        return name
    return MARK + "".join(f";{ord(c):02X}" if c in ESCAPED else c for c in name)


def decode(stored: str) -> str:
    """The Orcapod name of a stored value column."""
    if not stored.startswith(MARK):
        return stored
    out, rest = [], stored[len(MARK) :]
    i = 0
    while i < len(rest):
        if rest[i] == ";":
            out.append(chr(int(rest[i + 1 : i + 3], 16)))
            i += 3
        else:
            out.append(rest[i])
            i += 1
    return "".join(out)


def check_codec(rounds: int = 200_000) -> None:
    """Round trip, injectivity and SpiralDB's rules over adversarial names."""
    rng = random.Random(1446)
    pieces = [
        "_",
        "__",
        MARK,
        ".",
        ";",
        ";2E",
        ";3B",
        "$",
        "*",
        "a",
        "B",
        "é",
        "é",
        "::",
        " ",
        "\n",
        "\x00",
        "{",
        '"',
        "x_",
        "o",
    ]
    names = {"", "__record_id", MARK, "a.b", "a;2Eb", "ox_a;2Eb"}
    while len(names) < rounds:
        names.add("".join(rng.choices(pieces, k=rng.randint(0, 7))))
    stored = {encode(n) for n in names}
    assert len(stored) == len(names), "not injective"
    for n in names:
        s = encode(n)
        assert decode(s) == n, (n, s, decode(s))
        assert s and "." not in s and not s.startswith(("__", "$")) and s != "*"
    print(
        f"codec: {len(names)} distinct names -> {len(stored)} stored names, all decoded"
    )


# ------------------------------------------------------------------- the connector

SCHEMA_KEY = "__orcapod_prototype__"


class MappingConnector(SpiralDBConnector):
    """Maps names and (top-level) types on write, and back on read."""

    def _logical(self, table_name: str) -> pa.Schema | None:
        kv = self._project.table(self._table_id(table_name)).get_metadata()
        parts = [kv[k] for k in sorted(kv) if k.startswith(SCHEMA_KEY)]
        if not parts:
            return None
        raw = base64.b64decode(b"".join(parts))
        return pa.ipc.read_schema(pa.py_buffer(raw))

    def unstorable_column_reason(
        self, name: str, arrow_type: pa.DataType, *, primary_key: bool
    ) -> str | None:
        return None

    def upsert_records(
        self,
        table_name: str,
        records: pa.Table,
        id_column: str,
        skip_existing: bool = False,
    ) -> None:
        tbl = self._project.table(self._table_id(table_name))
        if self._logical(table_name) is None:
            raw = base64.b64encode(records.schema.serialize().to_pybytes())
            tbl.set_metadata(
                {
                    f"{SCHEMA_KEY}/{i:04d}": raw[i * 8000 : (i + 1) * 8000]
                    for i in range(-(-len(raw) // 8000))
                }
            )
        names, arrays = [], []
        for field, column in zip(records.schema, records.columns):
            if isinstance(field.type, pa.BaseExtensionType):
                storages: list[Any] = [cast(Any, c).storage for c in column.chunks]
                column = pa.chunked_array(
                    storages, type=cast(Any, field.type.storage_type)
                )
            if pa.types.is_timestamp(column.type) and column.type.tz is not None:
                column = pa.chunked_array(
                    [c.view(pa.timestamp(column.type.unit)) for c in column.chunks]
                )
            names.append(field.name if field.name == id_column else encode(field.name))
            arrays.append(column)
        rows = pa.table(arrays, names=names)
        if skip_existing:
            held = self._spiral.scan(tbl.select()).to_table()
            ids = set(held.column(id_column).to_pylist())
            rows = rows.filter(
                pa.array([i not in ids for i in rows.column(id_column).to_pylist()])
            )
        if rows.num_rows:
            tbl.write(rows)

    def get_arrow_schema(self, table_name: str) -> pa.Schema:
        schema = self._logical(table_name)
        assert schema is not None
        return schema

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        return [
            ColumnInfo(name=f.name, arrow_type=f.type, nullable=f.nullable)
            for f in self.get_arrow_schema(table_name)
        ]

    def iter_batches(
        self, query: str, params: Any = None, batch_size: int = 1000
    ) -> Any:
        table_name = query.split('"')[1]
        tbl = self._project.table(self._table_id(table_name))
        schema = self._logical(table_name)
        if schema is None:
            return
        for batch in self._spiral.scan(tbl.select()).to_record_batches():
            rows = normalize_table_to_large_types(
                pa.Table.from_batches([batch]), keep_null=True
            )
            arrays = []
            for field in schema:
                name = (
                    field.name
                    if field.name in rows.column_names
                    else encode(field.name)
                )
                array = rows.column(name).combine_chunks()
                target = field.type
                if pa.types.is_timestamp(target) and target.tz is not None:
                    array = array.view(target)
                elif isinstance(target, pa.BaseExtensionType):
                    array = pa.ExtensionArray.from_storage(
                        target, array.cast(target.storage_type)
                    )
                elif array.type != target:
                    array = array.cast(target)
                arrays.append(array)
            yield pa.RecordBatch.from_arrays(arrays, schema=schema)


# ------------------------------------------------------------------------- the job

CALLS: Counter[str] = Counter()


@function_pod(output_keys=["total"])
def add(y: float, b: float) -> float:
    """First function."""
    CALLS["add"] += 1
    return y + b


@function_pod(output_keys=["mean"])
def mean(total: list[float]) -> float:
    """Second function, over a group."""
    CALLS["mean"] += 1
    return sum(total) / len(total)


def notify(ctx: Any, mean: float) -> None:
    """A side effect."""
    CALLS["notify"] += 1


def _source(rows: int, name: str, data: list[str]) -> ArrowTableSource:
    fields = [
        pa.field("g", pa.large_string(), nullable=False),
        pa.field("k", pa.int64(), nullable=False),
        *(pa.field(n, pa.float64(), nullable=False) for n in data),
    ]
    arrays = [
        pa.array([f"g{i % 2}" for i in range(rows)], pa.large_string()),
        pa.array(range(rows), pa.int64()),
        *(pa.array([float(i + j) for i in range(rows)]) for j in range(len(data))),
    ]
    return ArrowTableSource(
        pa.Table.from_arrays(arrays, schema=pa.schema(fields)),
        tag_columns=["g", "k"],
        tag_only_columns=True,
        source_id=name,
    )


def build(database: Any, rows: int) -> PipelineJob:
    """The job of ITL-1446's goals, without observers."""
    job = PipelineJob(name="proto", database=database)
    with job:
        joined = Join()(_source(rows, "left", ["y"]), _source(rows, "right", ["b"]))
        kept = PolarsFilter([pl.col("y") >= 0.0])(joined)
        summed = add.pod(kept, label="add")
        grouped = GroupBy(["g"])(summed)
        means = mean.pod(grouped, label="mean")
        SideEffectPod(notify, SideEffectPodConfig(track_completion=True))(
            means, label="notify"
        )
    return job


def result(job: PipelineJob) -> list[tuple[Any, ...]]:
    """Every row of both functions with its system tags and content hash."""
    out: list[tuple[Any, ...]] = []
    for label in ("add", "mean"):
        node = job.nodes[label]
        table = node.as_table(columns={"system_tags": True, "content_hash": True})
        out.append(
            (label, node.content_hash().to_string(), table.schema, table.to_pylist())
        )
    return out


@contextmanager
def counted() -> Any:
    """Count the SpiralDB calls that are round trips on the real service."""
    counts: Counter[str] = Counter()
    patches = []

    def wrap(cls: Any, method: str, label: str) -> None:
        original = getattr(cls, method)

        def call(self: Any, *a: Any, **k: Any) -> Any:
            counts[label] += 1
            return original(self, *a, **k)

        patches.append(patch.object(cls, method, call))

    wrap(fake_pyspiral.FakeProject, "list_tables", "list tables")
    wrap(fake_pyspiral.FakeProject, "table", "look up table")
    wrap(fake_pyspiral.FakeProject, "create_table", "create table")
    wrap(fake_pyspiral.FakeSpiral, "scan", "whole-table scan")
    wrap(fake_pyspiral.FakeTable, "get_metadata", "get metadata")
    wrap(fake_pyspiral.FakeTable, "set_metadata", "set metadata")
    wrap(fake_pyspiral.FakeTable, "write", "write transaction")
    for p in patches:
        p.start()
    try:
        yield counts
    finally:
        for p in patches:
            p.stop()


def check_job() -> None:
    """Same rows as in memory; a rerun from a fresh connector calls nothing."""
    with tempfile.TemporaryDirectory() as root, fake_pyspiral.install(root):
        plain = build(InMemoryArrowDatabase(), 6)
        plain.run()
        expected = result(plain)
        CALLS.clear()
        job = build(ConnectorArrowDatabase(MappingConnector("p")), 6)
        job.run()
        assert result(job) == expected, "rows differ from the in-memory store"
        assert CALLS == {"add": 6, "mean": 2, "notify": 2}, CALLS
        first = dict(CALLS)
        CALLS.clear()
        again = build(ConnectorArrowDatabase(MappingConnector("p")), 6)
        again.run()
        assert result(again) == expected
        stored = again.nodes["add"].as_source().as_table().to_pylist()
        memory = plain.nodes["add"].as_source().as_table().to_pylist()
        key = str
        assert sorted(stored, key=key) == sorted(memory, key=key)
        print(
            f"job: first run calls {first}, rerun calls {dict(CALLS)}; "
            "rows, schemas, system tags and content hashes equal the in-memory store's"
        )


def check_order() -> None:
    """Do reads of what is stored follow the in-memory store's row order?"""
    for rows in (6, 60):
        with tempfile.TemporaryDirectory() as root, fake_pyspiral.install(root):
            spiral = ConnectorArrowDatabase(MappingConnector("p"))
            memory = InMemoryArrowDatabase()
            order = {}
            for name, database in (("spiral", spiral), ("memory", memory)):
                build(database, rows // 2).run()
                job = build(database, rows)
                job.run()
                node = job.nodes["add"]
                path = node._versioned_pipeline_path
                base = node._pipeline_database
                order[name] = (
                    base.get_all_records(path).column("k").to_pylist(),
                    node.as_source().as_table().column("k").to_pylist(),
                )
            print(
                f"order ({rows} rows): pipeline table equal "
                f"{order['spiral'][0] == order['memory'][0]}, "
                f"as_source() equal {order['spiral'][1] == order['memory'][1]}"
            )


def check_cached_source_order() -> None:
    """A cached source reads its rows in key (hash) order from SpiralDB."""
    with tempfile.TemporaryDirectory() as root, fake_pyspiral.install(root):
        for name, database in (
            ("in memory", InMemoryArrowDatabase()),
            ("Delta", DeltaTableDatabase(Path(root) / "delta")),
            ("SpiralDB", ConnectorArrowDatabase(MappingConnector("p"))),
        ):
            cached = CachedSource(_source(6, "left", ["y"]), cache_database=database)
            batched = Batch(2)(cached).as_table()
            print(
                f"cached source, {name}: k {cached.as_table().column('k').to_pylist()}, "
                + f"Batch(2) gives y {batched.column('y').to_pylist()}"
            )


def check_positional_ids() -> None:
    """Positional record ids of a ``tags_as_data`` source over a logged operator."""
    for name, build_database in (
        ("in memory", lambda root: InMemoryArrowDatabase()),
        ("Delta", lambda root: DeltaTableDatabase(Path(root) / "delta")),
        ("SpiralDB", lambda root: ConnectorArrowDatabase(MappingConnector("p"))),
    ):
        with tempfile.TemporaryDirectory() as root, fake_pyspiral.install(root):
            database = build_database(root)
            for rows in (4, 8):
                job = PipelineJob(name="positional", database=database)
                with job:
                    Join()(
                        _source(rows, "left", ["y"]),
                        _source(rows, "right", ["b"]),
                        label="joined",
                    )
                node = job.nodes["joined"]
                node.attach_databases(
                    pipeline_database=database, cache_mode=CacheMode.LOG
                )
                job.run()
                for keys in (None, ["k"]):
                    origin = node.as_source(tags_as_data={"k": "kk"}, source_id="d")
                    if keys is not None:
                        origin = DerivedSource(
                            node,
                            source_id="d",
                            tags_as_data={"k": "kk"},
                            record_id_columns=keys,
                        )
                    table = origin.as_table(columns={"system_tags": True})
                    column = next(
                        c for c in table.column_names if c.startswith("_tag_record_id")
                    )
                    ids = dict(zip(table["k"].to_pylist(), table[column].to_pylist()))
                    print(
                        f"record ids, {name}, {rows} rows, key {keys}: "
                        + f"{[ids[k] for k in range(4)]}"
                    )


def check_cost() -> None:
    """SpiralDB calls per run, today's lookups."""
    print(
        "\n| Rows | Run | "
        + " | ".join(
            [
                "list tables",
                "look up table",
                "get metadata",
                "set metadata",
                "create table",
                "write transaction",
                "whole-table scan",
            ]
        )
        + " |"
    )
    print("|---|---|---|---|---|---|---|---|---|")
    for rows in (2, 10, 50):
        with tempfile.TemporaryDirectory() as root, fake_pyspiral.install(root):
            for run, count in (
                ("first", rows),
                ("unchanged rerun", rows),
                ("10 rows added", rows + 10),
            ):
                job = build(ConnectorArrowDatabase(MappingConnector("p")), count)
                with counted() as counts:
                    job.run()
                print(
                    f"| {rows} | {run} | "
                    + " | ".join(
                        str(counts[c])
                        for c in [
                            "list tables",
                            "look up table",
                            "get metadata",
                            "set metadata",
                            "create table",
                            "write transaction",
                            "whole-table scan",
                        ]
                    )
                    + " |"
                )


# --------------------------------------------------------------------- table names


def today(path: tuple[str, ...]) -> str:
    """The name ``ConnectorArrowDatabase`` gives a path today."""
    return ConnectorArrowDatabase(MappingConnector("p"))._path_to_table_name(path)


def proposed(path: tuple[str, ...]) -> str:
    """The proposed name (section 5.3 of the design)."""
    import json
    import re

    digest = hashlib.sha256(
        json.dumps(list(path), ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    words = [p for p in path if not re.search(r"[0-9a-f]{64}", p)]
    hint = re.sub(r"[^A-Za-z0-9_]+", "_", "_".join(words))[:56]
    return f"op-{hint}-{digest}"


def check_names() -> None:
    """Collisions and lengths of today's and the proposed names."""
    h = "0" * 64
    tail = (h, "v0", "python.function.v1", "rev1", "rdb_v1")
    pairs = [
        (("job", "_result", "my-fn", *tail), ("job", "_result", "my_fn", *tail)),
        (("job", "_result", "café", *tail), ("job", "_result", "cafè", *tail)),
        (("run-1", "add", "pdb_v1"), ("run_1", "add", "pdb_v1")),
        (("a", "b__c"), ("a__b", "c")),
        (("a_", "b"), ("a", "_b")),
        (("1x",), ("t_1x",)),
    ]
    print("\n| Path A | Path B | Today's one name | Proposed names differ |")
    print("|---|---|---|---|")
    with tempfile.TemporaryDirectory() as root, fake_pyspiral.install(root):
        for a, b in pairs:
            assert a != b and today(a) == today(b)
            show = lambda p: "/".join(p).replace(h, "<64 hex>")  # noqa: E731
            print(
                f"| `{show(a)}` | `{show(b)}` | `{today(a).replace(h, '<64 hex>')}` | "
                f"{proposed(a) != proposed(b)} |"
            )
        database = ConnectorArrowDatabase(MappingConnector("p"))
        build(database, 4).run()
        print("\n| Stored table | Today's length | Proposed length |")
        print("|---|---|---|")
        import re

        for name in database._connector.get_table_names():
            # the path itself is not recoverable from today's name: the length is
            shown = re.sub(r"[0-9a-f]{64}", "<64 hex>", name)
            print(f"| `{shown}` | {len(name)} | at most 124 |")


def check_wrong_result() -> None:
    """Two functions whose names collide today: one is served the other's results."""

    def double(x: float) -> float:
        return 2 * x

    def triple(x: float) -> float:
        return 3 * x

    def run(fn: Any, name: str, db: Any) -> list[float]:
        pod = function_pod(output_keys=["out"], function_name=name)(fn).pod
        job = PipelineJob(name="j", database=db)
        with job:
            pod(_source(2, "s", ["x"]), label="n")
        job.run()
        return job.nodes["n"].as_table().column("out").to_pylist()

    with tempfile.TemporaryDirectory() as root, fake_pyspiral.install(root):
        db = ConnectorArrowDatabase(MappingConnector("p"))
        first = run(double, "my-fn", db)
        second = run(triple, "my_fn", db)
        print(
            f"\nwrong result: my-fn doubles {first}; my_fn triples [0.0, 1.0] and is "
            f"given {second}"
        )


if __name__ == "__main__":
    check_codec()
    check_job()
    check_order()
    check_cached_source_order()
    check_positional_ids()
    check_cost()
    check_names()
    check_wrong_result()
