"""ITL-1295: dump every read of stored function results, to compare two checkouts.

usage: uv run python docs/dev/scripts/itl_1295/dump_reads.py <output.pickle>
       uv run python docs/dev/scripts/itl_1295/dump_reads.py --compare <a.pickle> <b.pickle>

Run it on ``main`` and on the fix (one process at a time), then compare: every table
must be equal with its schema metadata (types, nullability, field metadata, column order,
row order, values), and every ephemeral-miss row and loaded tag the same. Only types
Polars held exactly are used, so ``main`` is the reference.

Scenarios, each over an in-memory database (pipeline and results) and a separate
in-memory ephemeral database:

* ``persistent``: one job run, read back by a new job;
* ``mixed``: a first run stores two inputs persistently; a second run, with the
  ephemeral database attached, adds two inputs whose results go to it; read back by a
  new job with the same ephemeral database (both parts of the fetch);
* ``ephemeral miss``: the same, read back by a new job with a NEW, empty ephemeral
  database (cross-session miss: ``EmptyData`` rows).

Reads: ``_fetch_joined_records()`` (table, ephemeral-miss rows), ``get_all_records()``
with every column, ``as_source().as_table()`` with every column, and the tags and data
of ``_load_cached_entries()`` as Arrow tables.
"""

from __future__ import annotations

import pickle
import sys
from pathlib import Path
from typing import Any, cast

import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.contexts import get_default_context
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import SyncPipelineOrchestrator
from orcapod.types import NodeConfig

CTX = get_default_context()


@function_pod(output_keys=("y", "w"))
def two_outputs(x: int) -> tuple[int, float]:
    return x + 1, -0.0 if x % 2 else float(x)


def source(rows: list[int]) -> ArrowTableSource:
    path_type = cast(
        pa.BaseExtensionType, CTX.type_converter.python_type_to_arrow_type(Path)
    )
    arrays = {
        "k": pa.array([r % 3 for r in rows], pa.int8()),
        "s": pa.array([f"é{r}" for r in rows], pa.large_string()),
        "l": pa.array([[r, None] for r in rows], pa.large_list(pa.int64())),
        "st": pa.array(
            [{"a": r, "b": str(r)} for r in rows],
            pa.struct([("a", pa.int64()), ("b", pa.large_string())]),
        ),
        "p": pa.ExtensionArray.from_storage(
            path_type, pa.array([f"/d/{r}" for r in rows], pa.large_string())
        ),
        "x": pa.array(rows, pa.int64()),
        "rid": pa.array([f"r{r}" for r in rows], pa.large_string()),
    }
    schema = pa.schema(
        pa.field(name, a.type, nullable=False) for name, a in arrays.items()
    )
    return ArrowTableSource(
        pa.Table.from_arrays(list(arrays.values()), schema=schema),
        tag_columns=["k", "s", "l", "st", "p"],
        tag_only_columns=True,
        source_id="s",
        record_id_columns=["rid"],
    )


def build(db: Any, rows: list[int], ephemeral: Any | None) -> PipelineJob:
    job = PipelineJob(name="itl1295dump", database=db)
    with job:
        two_outputs.pod(source(rows), label="node")
    if ephemeral is not None:
        job.set_ephemeral_database(ephemeral)
        job.apply_node_config(NodeConfig(is_result_ephemeral=True))
    return job


# Values that differ between any two runs (a random record id, the source-info tokens
# naming it, a time stamp, the checkout's git state): compared by type, not value.
PER_RUN = ("__data_id", "__pod_ts", "__pf_var_git_hash", "_source_y", "_source_w")


def plain(table: pa.Table) -> tuple[Any, ...]:
    """A table as comparable plain values: each field (name, type, nullability,
    metadata) and its values (a logical column as its storage), and the schema metadata."""
    fields = []
    for field, column in zip(table.schema, table.columns):
        chunks = [
            c.storage if isinstance(c, pa.ExtensionArray) else c for c in column.chunks
        ]
        values = [repr(v) for chunk in chunks for v in chunk.to_pylist()]
        if field.name in PER_RUN:
            values = [f"<{len(values)} per-run values>"]
        fields.append(
            (field.name, str(field.type), field.nullable, field.metadata, values)
        )
    return (tuple(fields), table.schema.metadata)


def reads(job: PipelineJob) -> dict[str, Any]:
    node: Any = job.nodes["node"]
    fetched = node._fetch_joined_records()  # noqa: SLF001
    loaded = node._load_cached_entries()  # noqa: SLF001

    def attempt(read: Any) -> Any:
        try:
            return read()
        except Exception as error:  # noqa: BLE001  # the same error on both checkouts is the same read
            return f"RAISES {type(error).__name__}: {error}"

    tags = [
        (
            base,
            attempt(lambda: plain(tag.as_table(columns={"system_tags": True}))),
            attempt(lambda: plain(data.as_table(columns={"source": True})))
            if hasattr(data, "as_table")
            else repr(data),
        )
        for base, (tag, data) in sorted(loaded.items())
    ]
    return {
        "fetched": plain(fetched.table),
        "fetched taginfo columns": fetched.taginfo_columns,
        "ephemeral miss rows": {
            k: repr(
                sorted((c, "<per-run>" if c in PER_RUN else x) for c, x in v.items())
            )
            for k, v in fetched.empty_taginfo_rows.items()
        },
        "get_all_records": plain(node.get_all_records(all_info=True)),
        "as_source": plain(node.as_source().as_table(all_info=True)),
        "loaded": tags,
    }


def dump(path: str) -> None:
    out: dict[str, Any] = {}
    sync = SyncPipelineOrchestrator
    db, eph = InMemoryArrowDatabase(), InMemoryArrowDatabase()
    build(db, [1, 2, 3], None).run(orchestrator=sync())
    out["persistent"] = reads(build(db, [1, 2, 3], None))
    build(db, [1, 2, 3, 4, 5], eph).run(orchestrator=sync())
    out["mixed"] = reads(build(db, [1, 2, 3, 4, 5], eph))
    out["ephemeral miss"] = reads(build(db, [1, 2, 3, 4, 5], InMemoryArrowDatabase()))
    with open(path, "wb") as file:
        pickle.dump(out, file)
    for scenario, values in out.items():
        flags = next(f[4] for f in values["fetched"][0] if f[0] == "__is_ephemeral")
        print(
            f"{scenario}: fetched rows={len(flags)} ephemeral={flags.count('True')} "
            + f"ephemeral misses={len(values['ephemeral miss rows'])} loaded={len(values['loaded'])} "
            + f"loaded reads raising={sum('RAISES' in repr(t) for t in values['loaded'])}"
        )


def same(a: Any, b: Any) -> bool:
    if isinstance(a, pa.Table):
        return isinstance(b, pa.Table) and bool(a.equals(b, check_metadata=True))
    if isinstance(a, (list, tuple)):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(same(a[k], b[k]) for k in a)
    return a == b


def compare(first: str, second: str) -> None:
    with open(first, "rb") as file:
        a = pickle.load(file)  # noqa: S301
    with open(second, "rb") as file:
        b = pickle.load(file)  # noqa: S301
    for scenario in a:
        for key in a[scenario]:
            verdict = "same" if same(a[scenario][key], b[scenario][key]) else "DIFFER"
            print(f"{scenario:15} {key:25} {verdict}")


if __name__ == "__main__":
    if sys.argv[1] == "--compare":
        compare(sys.argv[2], sys.argv[3])
    else:
        dump(sys.argv[1])
