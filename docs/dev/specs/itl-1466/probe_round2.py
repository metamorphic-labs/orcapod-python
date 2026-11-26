"""Round 2 (ITL-1466): the developer's decisions, on the prototype.

Needs the prototype (scratch branch, not in the PR): on the library as it is this
fails. Run: uv run python docs/dev/specs/itl-1466/probe_round2.py
"""

import asyncio
import os
import subprocess
import sys
import tempfile
import warnings

import pyarrow as pa

warnings.simplefilter("ignore")

from orcapod import PipelineJob, function_pod, side_effect_pod  # noqa: E402
from orcapod.core.streams.arrow_table_stream import ArrowTableStream  # noqa: E402
from orcapod.databases import InMemoryArrowDatabase  # noqa: E402
from orcapod.operators import (  # noqa: E402
    Batch,
    GroupBy,
    Join,
    MergeJoin,
    SelectDataColumns,
)
from orcapod.side_effects import SideEffectPodConfig  # noqa: E402
from orcapod.sources import DictSource  # noqa: E402
from orcapod.types import CacheMode  # noqa: E402

CALLS = {"n": 0, "delivered": 0}
ALL = {"source": True, "system_tags": True}


@function_pod(output_keys=["r"])
def rnd(x: float) -> int:
    CALLS["n"] += 1
    return round(x)


@function_pod(output_keys=["total"])
def total(x: list[float]) -> float:
    return sum(x)


@function_pod(output_keys=["z"])
def plus(r: int) -> int:
    return r + 1


@side_effect_pod
def tell(x: float, y: float, ctx) -> None:
    CALLS["delivered"] += 1


def units(x0=1.1, reverse=False, key=True, name="units"):
    rows = [
        {"s": s, "u": u, "x": v, "y": -v}
        for s, u, v in [("m1", "a", x0), ("m1", "b", 2.6), ("m2", "a", 3.2)]
    ]
    if reverse:
        rows.reverse()
    k = {"record_id_columns": ["s", "u"]} if key else {}
    tags = ["u", "s"] if reverse else ["s", "u"]
    return DictSource(
        rows, tag_columns=tags, tag_only_columns=True, source_id=name, **k
    )


def sess(reverse=False, key=True):
    rows = [{"s": "m1", "dur": 2.0}, {"s": "m2", "dur": 5.0}]
    if reverse:
        rows.reverse()
    k = {"record_id_columns": ["s"]} if key else {}
    return DictSource(
        rows, tag_columns=["s"], tag_only_columns=True, source_id="sess", **k
    )


OPS = ("join", "group", "batch", "merge", "sel")


def build(u, s, db, mode=CacheMode.LOG, swap=False):
    job = PipelineJob(name="p", database=db)
    with job:
        rnd.pod(u, label="fn")
        Join(label="join")(*((s, u) if swap else (u, s)))
        g = GroupBy(by=["s"], label="group")(u)
        total.pod(g, label="tot")
        Batch(batch_size=2, label="batch")(u)
        MergeJoin(label="merge")(u, u.select_data_columns(["x"]))
        SelectDataColumns(["x"], label="sel")(u)
    for n in OPS:
        job.nodes[n].attach_databases(pipeline_database=db, cache_mode=mode)
    return job


def run(u=None, s=None, db=None, mode=CacheMode.LOG, swap=False, how="sync"):
    job = build(u or units(), s or sess(), db or InMemoryArrowDatabase(), mode, swap)
    if how == "sync":
        job.run()
    else:
        asyncio.run(job.run_async())
    return job


def pair(src):
    return [c for c in src.as_table(columns=ALL).column_names if c.startswith("_tag_")]


def ids(src):
    t = src.as_table(columns=ALL)
    return sorted(t.column(next(c for c in pair(src) if "record_id" in c)).to_pylist())


NAMES = ("fn", "tot", *OPS)


def all_ids(job):
    return {n: ids(job.nodes[n].as_source()) for n in NAMES}


def attempt(label, f):
    try:
        print(f"  {label}:", f())
    except Exception as e:  # noqa: BLE001
        print(f"  {label}: {type(e).__name__}: {str(e)[:170]}")


if len(sys.argv) > 1:
    print(all_ids(run()))
    raise SystemExit

print("1. One pair, unique ids")
job = run()
base = all_ids(job)
for n in NAMES:
    src = job.nodes[n].as_source()
    print(f"  {n:6}", "pair columns", len(pair(src)), "rows", len(base[n]), "unique",
          len(set(base[n])) == len(base[n]), base[n][0][:17])  # fmt: skip

print("2. Stable?")


def same(label, got):
    diff = [n for n in base if got[n] != base[n]]
    print(f"  {label:40}", "equal" if not diff else f"DIFFERS: {diff}")


same("a second fresh store", all_ids(run()))
same("async", all_ids(run(how="async")))
same("rows and tags reordered, keyed", all_ids(run(units(reverse=True), sess(True))))
same("join inputs swapped", all_ids(run(swap=True)))
for mode in (CacheMode.OFF, CacheMode.REPLAY):
    if mode is CacheMode.REPLAY:
        db = InMemoryArrowDatabase()
        run(db=db)
        same("REPLAY over a LOG store", all_ids(run(db=db, mode=mode)))
    else:
        same("cache OFF (current run)", all_ids(run(mode=mode)))
child = subprocess.run([sys.executable, __file__, "c"], capture_output=True, text=True,
                       env={**os.environ, "PYTHONHASHSEED": "7"})  # fmt: skip
print("  another process:", child.stdout.strip() == str(base), child.stderr[-200:])
p = all_ids(run(units(key=False), sess(key=False)))
q = all_ids(run(units(key=False, reverse=True), sess(True, key=False)))
print("  positional, reordered, differ:", [n for n in p if p[n] != q[n]])

print("3. Plain streams against nodes: same default id? same record ids?")
u, s = units(), sess()
plain = {
    "fn": rnd.pod(u),
    "join": Join()(u, s),
    "group": GroupBy(by=["s"])(u),
    "batch": Batch(batch_size=2)(u),
    "merge": MergeJoin()(u, u.select_data_columns(["x"])),
    "sel": SelectDataColumns(["x"])(u),
}
for n, st in plain.items():
    a, b = st.as_source(), job.nodes[n].as_source()
    print(f"  {n:6} source_id equal {a.source_id == b.source_id}, content hash equal "
          f"{a.content_hash() == b.content_hash()}, record ids equal {ids(a) == base[n]}")  # fmt: skip
raw = ArrowTableStream(
    pa.table({"t": ["a", "a"], "n": [1, 1]}), tag_columns=["t"]
).as_source()
attempt("raw stream with two identical rows", lambda: ids(raw))

print("4. Input corrected 1.1 -> 1.2 in one store (keyed)")
db = InMemoryArrowDatabase()
run(units(1.1), db=db)
job2 = run(units(1.2), db=db)
fn = job2.nodes["fn"].as_source()
print("  fn rows", fn.as_table().num_rows, "ids unique",
      len(set(ids(fn))) == len(ids(fn)), "run's own rows", job2.nodes["fn"].as_table().num_rows)  # fmt: skip
for n in OPS:
    attempt(f"{n} (LOG)", lambda n=n: f"{len(ids(job2.nodes[n].as_source()))} rows")
print("  a select OVER fn, LOG, after the same correction:")
db = InMemoryArrowDatabase()
for x in (1.1, 1.2):
    j = PipelineJob(name="q", database=db)
    with j:
        SelectDataColumns(["r"], label="sel")(rnd.pod(units(x), label="fn"))
    j.nodes["sel"].attach_databases(pipeline_database=db, cache_mode=CacheMode.LOG)
    j.run()
node = j.nodes["sel"]
rows = node.get_all_records(columns=ALL)
print("   stored rows", rows.num_rows, "distinct (tags, system tags, data)",
      len({tuple(r.values()) for r in rows.drop(
          [c for c in rows.column_names if c.startswith("_source_")]).to_pylist()}))  # fmt: skip
attempt("LOG as_source", lambda: len(ids(node.as_source())))
j = PipelineJob(name="q", database=db)
with j:
    SelectDataColumns(["r"], label="sel")(rnd.pod(units(1.2), label="fn"))
j.run()
attempt("OFF as_source", lambda: f"{len(ids(j.nodes['sel'].as_source()))} rows")

print(
    "4b. Nothing corrected: pipeline store kept, results recomputed (new result store)"
)
pdb = InMemoryArrowDatabase()
VALUE = {"v": 0}


@function_pod(output_keys=["r"])
def drift(x: float) -> int:
    return round(x) + VALUE["v"]


for change in (0, 100):
    VALUE["v"] = change
    j = PipelineJob(name="q", database=pdb, result_database=InMemoryArrowDatabase())
    with j:
        SelectDataColumns(["r"], label="sel")(drift.pod(units(), label="fn"))
    j.nodes["sel"].attach_databases(pipeline_database=pdb, cache_mode=CacheMode.LOG)
    j.run()
    src = j.nodes["fn"].as_source()
    print("   fn", ids(src)[0][-12:], src.as_table().column("r").to_pylist())
    attempt("sel LOG, outputs changed", lambda: len(ids(j.nodes["sel"].as_source())))
pdb = InMemoryArrowDatabase()
VALUE["v"] = 0
for _ in range(2):
    j = PipelineJob(name="q", database=pdb, result_database=InMemoryArrowDatabase())
    with j:
        SelectDataColumns(["r"], label="sel")(drift.pod(units(), label="fn"))
    j.nodes["sel"].attach_databases(pipeline_database=pdb, cache_mode=CacheMode.LOG)
    j.run()
    entries = j.nodes["fn"]._pipeline_database.get_all_records(
        j.nodes["fn"]._versioned_pipeline_path
    )
    print("   entries", entries.num_rows, "fn rows",
          j.nodes["fn"].as_source().as_table().num_rows)  # fmt: skip
    attempt("sel LOG, outputs equal", lambda: len(ids(j.nodes["sel"].as_source())))

print("5. Never computes")
CALLS["n"] = 0
src = plain_fn = rnd.pod(units()).as_source()
src.output_schema(), src.keys(), src.content_hash(), src.pipeline_hash(), src.source_id
v = src.head()
src.check_for_new_data()
src._repr_html_() if hasattr(src, "_repr_html_") else None
print("  schema, hashes, head, preview, check: calls", CALLS["n"], "| head", v.status)
src.as_table()
src.as_table()
print("  two reads: calls", CALLS["n"])
st = rnd.pod(units())
one = next(iter(st.iter_data()))
CALLS["n"] = 0
v = st.as_source().head()
print("  head over a stream holding 1 of 3 rows:", v.status, v.table.num_rows,
      "calls", CALLS["n"])  # fmt: skip
st.as_table()
h = st.as_source().head(2, columns=ALL)
rec = next(c for c in h.table.column_names if "record_id" in c)
print("  head(2) ids are the read's first 2:", h.table.column(rec).to_pylist()
      == st.as_source().as_table(columns=ALL).column(rec).to_pylist()[:2])  # fmt: skip
attempt("tags_as_data={}", lambda: units().as_source({}, source_id="e"))

print("6. Side effects")
CALLS["delivered"] = 0
u = units()
st = tell(u)
print("  over a source: is the source", st.as_source() is u)
fed = u.select_data_columns(["x"])
se = tell(fed).as_source()
print("  over a plain select: id equals the select's",
      se.source_id == fed.as_source().source_id, "ids", ids(se) == ids(fed.as_source()))  # fmt: skip
print("  delivered", CALLS["delivered"])
over = rnd.pod(tell(u))
try:
    over.as_source().head()
    d = CALLS["delivered"]
    over.as_source().as_table()
    print("  function OVER a standalone side effect: delivered by head", d, "by a read",
          CALLS["delivered"] - d)  # fmt: skip
except Exception as e:  # noqa: BLE001
    print("  function over side effect:", type(e).__name__, e)
print("  its identity equals the one over the feeding stream:",
      rnd.pod(tell(u)).content_hash() == rnd.pod(u).content_hash())  # fmt: skip


@side_effect_pod(config=SideEffectPodConfig(on_error="log"))
def fails(x: float, y: float, ctx) -> None:
    if x > 3:
        raise ValueError("no")


if True:
    db = InMemoryArrowDatabase()
    j = PipelineJob(name="se", database=db)
    with j:
        tell(units(), label="tell")
        fails(units(), label="fails")
    CALLS["delivered"] = 0
    try:
        j.run()
    except Exception as e:  # noqa: BLE001
        print("  run raised", type(e).__name__)
    d = CALLS["delivered"]
    a = j.nodes["tell"].as_source()
    a.as_table()
    print("  job node: as_source is", type(a).__name__, a.source_id,
          "delivered by as_source", CALLS["delivered"] - d)  # fmt: skip
    print("  rows the node passed on", j.nodes["fails"].as_table().num_rows,
          "rows of its as_source", j.nodes["fails"].as_source().as_table().num_rows)  # fmt: skip

print("7. Bound as an input: multi-pair join, stored columns, loaded job")
db = InMemoryArrowDatabase()


@function_pod(output_keys=["rate"])
def rate(x: float, dur: float) -> float:
    return x / dur


ncols = {}
for label, src in (
    ("join node", run().nodes["join"].as_source(source_id="joined")),
    ("plain join", Join()(units(), sess()).as_source(source_id="joined")),
    (
        "ordinary",
        DictSource(
            Join()(units(), sess()).as_table().to_pylist(),
            tag_columns=["s", "u"],
            tag_only_columns=True,
            source_id="joined",
        ),
    ),
):
    for how in ("sync", "async"):
        j = PipelineJob(name="b", database=InMemoryArrowDatabase())
        with j:
            rate.pod(src, label="rate")
        j.run() if how == "sync" else asyncio.run(j.run_async())
        d = j._pipeline_database if hasattr(j, "_pipeline_database") else None
        n = j.nodes["rate"]
        t = n._pipeline_database.get_all_records(n._versioned_pipeline_path)
        ncols[label, how] = (
            n._versioned_pipeline_path,
            {f.name: f.type for f in t.schema},
        )
        print(f"  {label:10} {how:5} rows {t.num_rows}")
print("  one path:", len({p for p, _ in ncols.values()}) == 1, "| columns and types equal:",
      all(c == ncols["ordinary", "sync"][1] for _, c in ncols.values()))  # fmt: skip
with tempfile.TemporaryDirectory() as d:
    from orcapod.databases import DeltaTableDatabase

    db = DeltaTableDatabase(d + "/db")
    job = run(db=db)
    job.save(d + "/job.json")
    loaded = PipelineJob.load(d + "/job.json")
    print("  loaded cache mode", loaded.nodes["join"].cache_mode)
    for n in NAMES:
        attempt(f"loaded {n}: ids equal", lambda n=n: ids(loaded.nodes[n].as_source()) == base[n])  # fmt: skip
    for n in OPS:
        loaded.nodes[n].attach_databases(pipeline_database=DeltaTableDatabase(d + "/db"), cache_mode=CacheMode.LOG)  # fmt: skip
        attempt(f"loaded {n}, LOG attached again", lambda n=n: ids(loaded.nodes[n].as_source()) == base[n])  # fmt: skip

print("8. Join of two as_sources; as_source of a step over as_source")
a = job_join = run().nodes["join"].as_source(source_id="j")
g = run().nodes["group"].as_source(source_id="g")
g = g.map_data({"x": "xs", "y": "ys"})
both = Join()(a, g)
print("  join of two: system-tag columns", sum(c.startswith("_tag_") for c in both.as_table(columns=ALL).column_names),
      "-> as_source", len(pair(both.as_source())))  # fmt: skip
print("  same object:", a.as_source() is a)

print("9. Two authors, one default id; held rows")
j1, j2 = run(units(1.1)), run(units(9.9))
a, b = j1.nodes["join"].as_source(), j2.nodes["join"].as_source()
print("  one id", a.source_id == b.source_id, "| record ids equal", ids(a) == ids(b))
