"""Candidate record ids: unique, deterministic, order-free? (ITL-1466).

Run: uv run python docs/dev/specs/itl-1466/probe_row_ids.py
Reads the stored keys through the node's internals: this is about the stored keys.
"""

import subprocess
import sys
import warnings
from pathlib import Path

import pyarrow as pa

warnings.simplefilter("ignore")
sys.path.insert(0, str(Path(__file__).parent))

from orcapod import PipelineJob, function_pod  # noqa: E402
from orcapod.core.streams.arrow_table_stream import ArrowTableStream  # noqa: E402
from orcapod.databases import InMemoryArrowDatabase  # noqa: E402
from orcapod.operators import Batch, GroupBy, Join  # noqa: E402
from orcapod.types import CacheMode  # noqa: E402
from orcapod.sources import DictSource  # noqa: E402


@function_pod(output_keys=["dbl"])
def double(n: int) -> int:
    return n * 2


@function_pod(output_keys=["total"])
def total(n: list[int]) -> int:
    return sum(n)


@function_pod(output_keys=["rate"])
def rate(n: int, dur: float) -> float:
    return n / dur


def data(reverse=False, key=True):
    rows = [
        {"subject": s, "unit": u, "n": i, "m": -i}
        for i, (s, u) in enumerate(
            [("m1", "u1"), ("m1", "u2"), ("m2", "u1"), ("m2", "u2")]
        )
    ]
    if reverse:
        rows.reverse()
    k = {"record_id_columns": ["subject", "unit"]} if key else {}
    u = DictSource(
        rows,
        tag_columns=["unit", "subject"] if reverse else ["subject", "unit"],
        tag_only_columns=True,
        source_id="units",
        **k,
    )
    k = {"record_id_columns": ["subject"]} if key else {}
    s = DictSource(
        [{"subject": "m1", "dur": 2.0}, {"subject": "m2", "dur": 5.0}],
        tag_columns=["subject"],
        tag_only_columns=True,
        source_id="sessions",
        **k,
    )
    return u, s


def run(reverse=False, key=True, mode="sync", db=None, swap=False):
    u, s = data(reverse, key)
    db = db or InMemoryArrowDatabase()
    job = PipelineJob(name="p", database=db)
    with job:
        double.pod(u, label="fn")
        j = Join(label="join")(*((s, u) if swap else (u, s)))
        rate.pod(j, label="rated")
        g = GroupBy(by=["subject"], label="group")(u)
        total.pod(g, label="total")
        Batch(batch_size=2, label="batch")(u)
    for name in ("join", "group", "batch"):
        job.nodes[name].attach_databases(pipeline_database=db, cache_mode=CacheMode.LOG)
    if mode == "sync":
        job.run()
    else:
        import asyncio

        asyncio.run(job.run_async())
    return job


def ids(job):
    out = {}
    for name in ("fn", "rated", "total"):
        t = job.nodes[name]._fetch_joined_records().table
        out[name] = sorted(
            x.hex()[-16:] for x in t.column("__pipeline_base_entry_id").to_pylist()
        )
    for name in ("join", "group", "batch"):
        t = job.nodes[name]._read_stored_records()
        out[name] = sorted(
            x.hex()[-16:]
            for x in t.column(job.nodes[name].HASH_COLUMN_NAME).to_pylist()
        )
    return out


if len(sys.argv) > 1:
    print(ids(run()))
    raise SystemExit

base = ids(run())
print(
    "unique",
    {k: len(set(v)) == len(v) for k, v in base.items()},
    {k: len(v) for k, v in base.items()},
)


def same(label, job):
    got = ids(job)
    print(f"{label:34}", {k: got[k] == base[k] for k in base})


same("sync again", run())
same("async", run(mode="async"))
same("reordered, keyed", run(reverse=True))
same("join inputs swapped", run(swap=True))
other = subprocess.run(
    [sys.executable, __file__, "child"],
    capture_output=True,
    text=True,
    env={"PYTHONHASHSEED": "7"},
)
print(
    "other process                     ",
    other.stdout.strip() == str(base),
    other.stderr[-200:],
)
pos, pos_r = ids(run(key=False)), ids(run(key=False, reverse=True))
print("positional source, reordered      ", {k: pos[k] == pos_r[k] for k in pos})

print("\nplain operator stream: the stored recipe recomputed on the rows")


def recipe(stream, with_source=True):
    t = stream.as_table(columns={"source": with_source, "system_tags": True})
    t = t.append_column(
        "__node_content_hash",
        pa.array(
            [stream.content_hash().to_prefixed_digest()] * t.num_rows, pa.large_binary()
        ),
    )
    h = stream.data_context.arrow_hasher
    return sorted(
        h.hash_table(t.slice(i, 1)).to_prefixed_digest().hex()[-16:]
        for i in range(t.num_rows)
    )


u, s = data()
for name, st in (
    ("join", Join()(u, s)),
    ("group", GroupBy(by=["subject"])(u)),
    ("batch", Batch(batch_size=2)(u)),
):
    print(" ", name, "plain == stored:", recipe(st) == base[name])
fs = double.pod(u)
from orcapod.core.nodes.function_node import _build_record_id_preimage  # noqa: E402

got = sorted(
    u.data_context.arrow_hasher.hash_table(_build_record_id_preimage(t, d))
    .to_prefixed_digest()
    .hex()[-16:]
    for t, d in u.select_data_columns(["n"]).iter_data()
)  # the pod's automatic select
print(
    "  fn    plain == stored:",
    got == base["fn"],
    "| content equal",
    fs.content_hash() == run().nodes["fn"].content_hash(),
)

print("\nduplicates")
raw = ArrowTableStream(
    pa.table({"t": ["a", "a", "b"], "n": [1, 1, 1]}), tag_columns=["t"]
)
r = recipe(raw)
print("  raw stream with a repeated row: ids", len(r), "distinct", len(set(r)))
got = [
    u.data_context.arrow_hasher.hash_table(_build_record_id_preimage(t, d)).to_hex()
    for t, d in raw.iter_data()
]
print(
    "  function over it (tags a,a,b, one input value): ids",
    len(got),
    "distinct",
    len(set(got)),
)

print("\nsystem tags only: do they name every row?")
for name in ("join", "group", "batch"):
    t = run().nodes[name]._read_stored_records()
    cols = [c for c in t.column_names if c.startswith("_tag")]
    keys = [tuple(r.values()) for r in t.select(cols).to_pylist()]
    print(" ", name, len(cols), "columns, unique", len(set(keys)) == len(keys))

print("\nsource id of the origin inside the key: rename an upstream source")
u2 = DictSource(
    u.as_table().to_pylist(),
    tag_columns=["subject", "unit"],
    tag_only_columns=True,
    source_id="renamed",
    record_id_columns=["subject", "unit"],
)
j = PipelineJob(name="p", database=InMemoryArrowDatabase())
with j:
    double.pod(u2, label="fn")
j.run()
t = j.nodes["fn"]._fetch_joined_records().table
print(
    "  entry ids equal after renaming the source:",
    sorted(x.hex()[-16:] for x in t.column("__pipeline_base_entry_id").to_pylist())
    == base["fn"],
)
