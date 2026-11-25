"""What as_source does today (ITL-1466). Run: uv run python docs/dev/specs/itl-1466/probe_today.py"""

import tempfile
import warnings
from pathlib import Path

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.operators import Join
from orcapod.sources import DerivedSource, DictSource
from orcapod.types import CacheMode

warnings.simplefilter("ignore")


@function_pod(output_keys="dbl")
def double(n: int) -> int:
    return n * 2


@function_pod(output_keys="rate")
def rate(n: int, dur: float) -> float:
    return n / dur


CALLS: list[int] = []


@function_pod(output_keys="out")
def after(dbl: int) -> int:
    CALLS.append(dbl)
    return dbl + 1


@function_pod(output_keys="out")
def after_rate(rate: float) -> float:
    return rate + 1


def sources(first=10, name="units", reverse=False):
    rows = [
        {"subject": "m1", "unit": "u1", "n": first},
        {"subject": "m1", "unit": "u2", "n": 30},
        {"subject": "m2", "unit": "u1", "n": 50},
    ]
    if reverse:
        rows.reverse()
    units = DictSource(
        rows,
        tag_columns=["subject", "unit"],
        tag_only_columns=True,
        source_id=name,
        record_id_columns=["subject", "unit"],
    )
    sessions = DictSource(
        [{"subject": "m1", "dur": 2.0}, {"subject": "m2", "dur": 5.0}],
        tag_columns=["subject"],
        tag_only_columns=True,
        source_id="sessions",
        record_id_columns=["subject"],
    )
    return units, sessions


def build(db, first=10, reverse=False, cache=CacheMode.OFF, name="shared"):
    u, s = sources(first, reverse=reverse)
    job = PipelineJob(name=name, database=db)
    with job:
        double.pod(u, label="single")
        j = Join(label="joined")(u, s)
        rate.pod(j, label="rated")
    if cache is not CacheMode.OFF:
        job.nodes["joined"].attach_databases(pipeline_database=db, cache_mode=cache)
    return job


def attempt(label, fn):
    try:
        print(f"  {label}: {fn()}")
    except Exception as e:  # noqa: BLE001
        print(f"  {label}: {type(e).__name__}: {str(e)[:230]}")


def run_over(src, pod, name="next", db=None):
    j = PipelineJob(name=name, database=db or InMemoryArrowDatabase())
    with j:
        pod.pod(src, label="next")
    j.run()
    return j


print("1 which objects have as_source")
u, s = sources()
from orcapod.core.streams.arrow_table_stream import ArrowTableStream  # noqa: E402

plain = Join()(u, s)
objs = {
    "root source": u,
    "operator stream": plain,
    "function stream": double.pod(u),
    "raw stream": ArrowTableStream(u.as_table(), tag_columns=["subject", "unit"]),
}
job = build(InMemoryArrowDatabase())
job.run()
for n, node in job.nodes.items():
    objs[f"{type(node).__name__} {n}"] = node
for n, o in objs.items():
    print(f"  {n:40} {hasattr(o, 'as_source')}")

print("2 plain as_source bound into a new job")
for label, pod in (("single", after), ("rated", after_rate)):
    attempt(
        label,
        lambda: len(list(run_over(job.nodes[label].as_source(), pod).nodes["next"])),
    )

print("3 operator node, cache OFF / LOG")
off = job.nodes["joined"].as_source()
print(
    "  OFF rows",
    off.as_table().num_rows,
    "| node shows",
    job.nodes["joined"].as_table().num_rows,
)

print("4 explicit source_id and the content hash")
n = job.nodes["single"]
a, b, c = n.as_source(), n.as_source(source_id="a"), n.as_source(source_id="b")
print(
    "  content equal:",
    a.content_hash() == b.content_hash() == c.content_hash(),
    "ids",
    b.source_id,
    c.source_id,
)
t1, t2 = (n.as_source(source_id=x, tags_as_data={}) for x in "ab")
print(
    "  tags_as_data={} content equal:",
    t1.content_hash() == t2.content_hash(),
    "| pipeline equal to plain:",
    t1.pipeline_hash() == a.pipeline_hash(),
)

print("5 what rows of a plain source carry (single-pair origin)")
t = a.as_table(all_info=True)
print(
    " ",
    {k[:34]: v for k, v in t.to_pydict().items() if k.startswith(("_tag", "_source"))},
)

print(
    "6 same origin, plain against a fresh root, one downstream store (what breaking costs)"
)
db = InMemoryArrowDatabase()
j1 = run_over(a, after, db=db)
c1 = len(CALLS)
fresh = n.as_source(source_id="x", tags_as_data={})


def tail_of(fn):
    try:
        return fn()
    except Exception as e:  # noqa: BLE001
        return f"{type(e).__name__}: {str(e)[:150]}"


print(
    "  plain, then fresh root, same store:",
    tail_of(lambda: run_over(fresh, after, db=db)),
)
print("  function calls the second time:", len(CALLS) - c1)
print(
    "  next: stored system-tag columns",
    [
        c
        for c in j1.nodes["next"].get_all_records(all_info=True).column_names
        if c.startswith("_tag")
    ],
)
ordinary = DictSource(
    a.as_table().to_pylist(),
    tag_columns=["subject", "unit"],
    tag_only_columns=True,
    source_id="ordinary",
)
print(
    "  plain, then an ordinary source of the same schema, same store:",
    tail_of(lambda: run_over(ordinary, after, db=db)),
)
db2 = InMemoryArrowDatabase()
run_over(fresh, after, db=db2)
c2 = len(CALLS)
jo = run_over(ordinary, after, db=db2)
print(
    "  fresh root, then ordinary: calls",
    len(CALLS) - c2,
    "rows",
    jo.nodes["next"].as_table().num_rows,
)
same_value = DictSource(
    [{"k": k, "n": 7} for k in "abc"],
    tag_columns=["k"],
    tag_only_columns=True,
    source_id="sv",
)
jsv = PipelineJob(name="sv", database=InMemoryArrowDatabase())
with jsv:
    double.pod(same_value, label="single")
jsv.run()
for form, src in (
    ("plain", jsv.nodes["single"].as_source()),
    ("fresh", jsv.nodes["single"].as_source(tags_as_data={}, source_id="f")),
):
    jn = run_over(src, after)
    print(
        f"  three rows of one value, {form}: run",
        jn.nodes["next"].as_table().num_rows,
        "stored",
        jn.nodes["next"].get_all_records().num_rows,
    )

print("7 loaded job, moved store, warnings on")
with tempfile.TemporaryDirectory() as d:
    store = Path(d) / "store"
    job3 = build(DeltaTableDatabase(str(store)))
    job3.run()
    job3.save(Path(d) / "job.json")
    L = PipelineJob.load(Path(d) / "job.json")
    attempt(
        "loaded single plain",
        lambda: len(list(run_over(L.nodes["single"].as_source(), after).nodes["next"])),
    )
    attempt(
        "loaded rated plain",
        lambda: len(
            list(run_over(L.nodes["rated"].as_source(), after_rate).nodes["next"])
        ),
    )
    attempt(
        "loaded joined (OFF) rows",
        lambda: L.nodes["joined"].as_source().as_table().num_rows,
    )
    print(
        "  default id same after load:",
        job3.nodes["single"].as_source().source_id
        == L.nodes["single"].as_source().source_id,
    )
    J = run_over(
        L.nodes["single"].as_source(source_id="x", tags_as_data={}),
        after,
        db=DeltaTableDatabase(str(Path(d) / "s2")),
    )
    J.save(Path(d) / "j2.json")
    L2 = PipelineJob.load(Path(d) / "j2.json")
    print("  reloaded, unbound", L2.unbound_sources)
    attempt(
        "reloaded stored rows", lambda: L2.nodes["next"].as_source().as_table().num_rows
    )
    attempt(
        "reloaded load_only run",
        lambda: L2.run(load_only=True) and L2.nodes["next"].as_table().num_rows,
    )
    store.rename(Path(d) / "moved")
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        L3 = PipelineJob.load(Path(d) / "job.json")
        attempt(
            "moved, no database",
            lambda: L3.nodes["single"].as_source().as_table().num_rows,
        )
        print(
            "  warnings",
            sorted({x.category.__name__ for x in w}),
            [str(x.message)[:160] for x in w if "Content" in x.category.__name__],
        )

print("8 two authors, same names, other data; both in one job")
A, B = build(InMemoryArrowDatabase(), 10), build(InMemoryArrowDatabase(), 999)
A.run()
B.run()  # noqa: E702
sa, sb = A.nodes["single"].as_source(), B.nodes["single"].as_source()
print(
    "  ids equal",
    sa.source_id == sb.source_id,
    "content equal",
    sa.content_hash() == sb.content_hash(),
)


def both():
    j = PipelineJob(name="both", database=InMemoryArrowDatabase())
    with j:
        after.pod(sa, label="a")
        after.pod(sb, label="b")
    j.run()
    return {
        k: sorted(v.as_table().column("out").to_pylist())
        for k, v in j.nodes.items()
        if k in "ab"
    }


attempt("one job over both", both)


def after_each_other():
    db = InMemoryArrowDatabase()
    x = run_over(sa, after, db=db)
    y = run_over(sb, after, db=db)
    return sorted(x.nodes["next"].get_all_records().column("out").to_pylist()), sorted(
        y.nodes["next"].as_table().column("out").to_pylist()
    )


attempt(
    "one store, one after the other: (stored for the node, second run's rows)",
    after_each_other,
)

print("9 stored history: a corrected value and shifted rows")
db = InMemoryArrowDatabase()
build(db, 10).run()
j = build(db, 999)
j.run()
print(
    "  corrected: run",
    j.nodes["single"].as_table().num_rows,
    "as_source",
    j.nodes["single"].as_source().as_table().to_pylist(),
)
src = DerivedSource(u, tags_as_data={}, source_id="over_source")
print(
    "10 derived over a keyed source: record ids",
    [
        v
        for k, v in src.as_table(columns={"system_tags": True}).to_pydict().items()
        if "record" in k
    ],
)
