"""A function entry is one upstream entry under this function (ITL-1466, round 3).

Needs the prototype, as probe_round2. Run: uv run python docs/dev/specs/itl-1466/probe_entries.py
"""

import asyncio
import os
import subprocess
import sys
import warnings

import pyarrow as pa

warnings.simplefilter("ignore")

from orcapod import PipelineJob, function_pod, side_effect_pod  # noqa: E402
from orcapod.core.executors import LocalPythonFunctionExecutor  # noqa: E402
from orcapod.core.nodes.function_node import _build_record_id_preimage  # noqa: E402
from orcapod.databases import InMemoryArrowDatabase  # noqa: E402
from orcapod.operators import GroupBy, Join  # noqa: E402
from orcapod.side_effects import SideEffectPodConfig  # noqa: E402
from orcapod.sources import DictSource  # noqa: E402

CALLS = {"n": 0, "v": 0, "told": 0}


@function_pod(output_keys=["r"])
def rnd(x: float, y: float) -> int:
    CALLS["n"] += 1
    return round(x + y) + CALLS["v"]


def src(rows, name="units", key=(), tags=("s", "u")):
    return DictSource(
        rows,
        tag_columns=list(tags),
        tag_only_columns=True,
        source_id=name,
        record_id_columns=list(key),
    )


def units(reverse=False, key=("s", "u")):
    rows = [
        {"s": s, "u": u, "x": x, "y": 1.0}
        for s, u, x in [("m1", "a", 1.1), ("m1", "b", 2.6), ("m2", "a", 3.2)]
    ]
    return src(rows[::-1] if reverse else rows, key=key, tags=("u", "s") if reverse else ("s", "u"))  # fmt: skip


def ids(s):
    t = s.as_table(columns={"system_tags": True})
    return sorted(
        t.column(next(c for c in t.column_names if "record_id" in c)).to_pylist()
    )


def node(stream, how="sync", db=None):
    job = PipelineJob(
        name="p",
        database=db or InMemoryArrowDatabase(),
        result_database=InMemoryArrowDatabase(),
    )
    with job:
        rnd.pod(stream, label="fn")
    job.run() if how == "sync" else asyncio.run(job.run_async())
    return job.nodes["fn"]


base = ids(node(units()).as_source())
if len(sys.argv) > 1:
    print(base, ids(rnd.pod(units()).as_source()) == base)
    raise SystemExit

print("1. Node and plain stream: one recipe")


def eq(label, got):
    print(f"  {label:44}", "equal" if got == base else "DIFFERS")


st = rnd.pod(units())
eq("plain", ids(st.as_source()))
eq("plain, read before as_source", (st.as_table(), ids(st.as_source()))[1])
part = rnd.pod(units())
next(iter(part.iter_data()))
eq("plain, one row read before", ids(part.as_source()))
eq("plain, rows and tags reordered (keyed)", ids(rnd.pod(units(True)).as_source()))
eq("node async", ids(node(units(), "async").as_source()))
CALLS["v"] = 100
a = node(units()).as_source()
b = rnd.pod(units()).as_source()
eq("node, another answer", ids(a))
eq("plain, another answer", ids(b))
print("   answers now", sorted(a.as_table().column("r").to_pylist()))
CALLS["v"] = 0
rnd.pod.executor = LocalPythonFunctionExecutor()
eq("node, LocalPythonFunctionExecutor", ids(node(units()).as_source()))
eq("plain, LocalPythonFunctionExecutor", ids(rnd.pod(units()).as_source()))
rnd.pod.executor = None
child = subprocess.run([sys.executable, __file__, "c"], capture_output=True, text=True,
                       env={**os.environ, "PYTHONHASHSEED": "7"})  # fmt: skip
print("  another process:", child.stdout.strip() == f"{base} True", child.stderr[-200:])


@function_pod(output_keys=["t"])
def total(x: list[float], y: list[float]) -> float:
    return sum(x) + sum(y)


job = PipelineJob(name="p", database=InMemoryArrowDatabase())
with job:
    total.pod(GroupBy(by=["s"])(units()), label="t")
job.run()
print("  over GroupBy: node equals plain",
      ids(job.nodes["t"].as_source()) == ids(total.pod(GroupBy(by=["s"])(units())).as_source()))  # fmt: skip
job = PipelineJob(name="p", database=InMemoryArrowDatabase())
pre = GroupBy(by=["s"])(units())
with job:
    total.pod(pre, label="t")
job.run()
print("  over a GroupBy built BEFORE the job (it becomes a slot): node equals plain",
      ids(job.nodes["t"].as_source()) == ids(total.pod(pre).as_source()),
      "| content hash equal", job.nodes["t"].content_hash() == total.pod(pre).content_hash(),
      "| system-tag names equal",
      [c for c in job.nodes["t"].as_table(columns={"system_tags": True}).column_names if c.startswith("_tag")]
      == [c for c in total.pod(pre).as_table(columns={"system_tags": True}).column_names if c.startswith("_tag")])  # fmt: skip

print("2. Why system tags are in the id: entries with and without them")


def case(label, make):
    CALLS["n"] = 0
    db = InMemoryArrowDatabase()
    job = PipelineJob(name="p", database=db)
    with job:
        rnd.pod(make(), label="fn")
    stream = make()
    job.run()
    calls = CALLS["n"]
    n = job.nodes["fn"]
    stored = n._pipeline_database.get_all_records(n._versioned_pipeline_path)
    h = n.data_context.arrow_hasher
    without = set()
    for tag, data in stream.select_data_columns(["x", "y"]).iter_data():
        t = _build_record_id_preimage(tag, data)
        keep = [c for c in t.column_names if not c.startswith("_tag_")]
        without.add(h.hash_table(t.select(keep)).to_hex())
    plain = ids(rnd.pod(stream).as_source())
    print(f"  {label:38} rows {stream.as_table().num_rows} | entries with",
          len(set(stored.column("__pipeline_base_entry_id").to_pylist())),
          "without", len(without), "| calls", calls,
          "result records", len(set(stored.column("__data_id").to_pylist())),
          "| as_source rows", n.as_source().as_table().num_rows,
          "equal plain", ids(n.as_source()) == plain)  # fmt: skip


two = [{"s": "m1", "u": "a", "x": 1.0, "y": 2.0}, {"s": "m1", "u": "a", "x": 1.0, "y": 2.0}]  # fmt: skip
case("repeated tag (one tag pair, two rows)", lambda: src(two))
drop = [{"s": "m1", "u": "a", "x": 1.0, "y": 2.0}, {"s": "m1", "u": "b", "x": 1.0, "y": 2.0}]  # fmt: skip
case("dropped tag, before (s, u)", lambda: src(drop, key=("s", "u")))
case("dropped tag, after (u dropped)",
     lambda: src(drop, key=("s", "u")).select_tag_columns(["s"]))  # fmt: skip


def left():
    return src([{"s": "m1", "u": "a", "x": 1.0, "y": 2.0}], key=("s", "u"))


def right():
    return src(
        [{"s": "m1", "k": 1, "dur": 1.0}, {"s": "m1", "k": 2, "dur": 2.0}],
        name="sess",
        tags=("s",),
    )


case("join, two matches, dur not an input", lambda: Join()(left(), right()))
case("join, inputs swapped", lambda: Join()(right(), left()))

print("3. Side effect: which rows passed? (facts for the open decision)")


def teller(**kw):
    @side_effect_pod(config=SideEffectPodConfig(on_error="log", **kw))
    def tell(x: float, y: float, ctx) -> None:
        CALLS["told"] += 1
        if x > 3:
            raise ValueError("no")

    return tell


for kw in ({}, {"drop_on_failure": False}):
    db = InMemoryArrowDatabase()
    for i in range(2):
        CALLS["told"] = 0
        job = PipelineJob(name="s", database=db)
        with job:
            teller(**kw)(units(), label="tell")
        n = job.nodes["tell"]
        before = n.as_table().num_rows
        job.run()
        log = n._pipeline_database.get_all_records(n._table_path)
        print(f"  {kw or 'default'} run {i + 1}: before the run {before}, called {CALLS['told']},",
              f"passed {n.as_table().num_rows}, log rows {log.num_rows}")  # fmt: skip
    job = PipelineJob(name="s", database=InMemoryArrowDatabase())
    with job:
        teller(**kw)(units(), label="tell")
    CALLS["told"] = 0
    job.run(load_only=True)
    print("     load only: called", CALLS["told"], "passed", job.nodes["tell"].as_table().num_rows)  # fmt: skip
u = units()
CALLS["told"] = 0
s = teller()(u)
s.head()
h = CALLS["told"]
print("  standalone: head delivered", h, "| a read passes", s.as_table().num_rows,
      "called", CALLS["told"], "| a second read: called", (s.as_table(), CALLS["told"])[1])  # fmt: skip
f1, f2 = rnd.pod(s), rnd.pod(u)
print("  content hash: stream equals feeding", s.content_hash() == u.content_hash(),
      "| pipeline hash", s.pipeline_hash() == u.pipeline_hash())  # fmt: skip
got = [
    h.to_string() for t, d in s.iter_data()
    for h in [u.data_context.arrow_hasher.hash_table(_build_record_id_preimage(t, d))]
]  # fmt: skip
fed = [
    h.to_string() for t, d in u.iter_data()
    for h in [u.data_context.arrow_hasher.hash_table(_build_record_id_preimage(t, d))]
]  # fmt: skip
print("  passed rows keep their rows' identity:", set(got) < set(fed), len(got), "of", len(fed))  # fmt: skip
assert isinstance(pa.table({}), pa.Table)
