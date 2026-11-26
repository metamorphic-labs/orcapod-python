"""Further checks (ITL-1466). Run: uv run python docs/dev/specs/itl-1466/probe_more.py"""

import sqlite3
import sys
import tempfile
import warnings
from pathlib import Path

import polars as pl
import pyarrow as pa

warnings.simplefilter("ignore")
sys.path.insert(0, str(Path(__file__).parent))

from deltalake import write_deltalake  # noqa: E402

from orcapod import PipelineJob, function_pod, side_effect_pod  # noqa: E402
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase  # noqa: E402
from orcapod.sources import (  # noqa: E402
    ArrowTableSource,
    CSVSource,
    DataFrameSource,
    DeltaTableSource,
    DerivedSource,
    DictSource,
    ListSource,
    SQLiteTableSource,
)


@function_pod(output_keys=["dbl"])
def double(n: int) -> int:
    return n * 2


@function_pod(output_keys="out")
def after(dbl: int) -> int:
    return dbl + 1


def src(rows=3, **k):
    return DictSource(
        [{"k": f"k{i}", "n": i} for i in range(rows)],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="s",
        **k,
    )


def job_of(stream, pod, label, db):
    j = PipelineJob(name=label, database=db)
    with j:
        pod.pod(stream, label=label)
    j.run()
    return j


d = Path(tempfile.mkdtemp())
t = pa.table({"k": ["a", "b"], "n": [1, 2]})
pl.DataFrame(t.to_pydict()).write_csv(d / "x.csv")
write_deltalake(str(d / "delta"), t)
c = sqlite3.connect(d / "x.db")
c.execute("create table t (k text primary key, n integer)")
c.executemany("insert into t values (?,?)", [("a", 1), ("b", 2)])
c.commit()
kw = {"tag_columns": ["k"], "tag_only_columns": True}
kinds = {
    "ArrowTableSource": lambda i: ArrowTableSource(t, source_id=i, **kw),
    "DictSource": lambda i: DictSource(t.to_pylist(), source_id=i, **kw),
    "DataFrameSource": lambda i: DataFrameSource(
        pl.DataFrame(t.to_pydict()), source_id=i, **kw
    ),
    "ListSource": lambda i: ListSource("n", [1, 2], source_id=i),
    "CSVSource": lambda i: CSVSource(str(d / "x.csv"), source_id=i, **kw),
    "DeltaTableSource": lambda i: DeltaTableSource(str(d / "delta"), source_id=i, **kw),
    "SQLiteTableSource": lambda i: SQLiteTableSource(str(d / "x.db"), "t", source_id=i),
    "DerivedSource tags_as_data": lambda i: DerivedSource(
        src(), tags_as_data={}, source_id=i
    ),
    "CachedSource(Arrow)": lambda i: ArrowTableSource(t, source_id=i, **kw).cached(
        InMemoryArrowDatabase()
    ),
}
origin = job_of(src(), double, "one", InMemoryArrowDatabase()).nodes["one"]
kinds["DerivedSource plain"] = lambda i: origin.as_source(source_id=i)
print("1 source id in the content hash")
for name, make in kinds.items():
    try:
        print(f"  {name:28} {make('a').content_hash() != make('b').content_hash()}")
    except Exception as e:  # noqa: BLE001
        print(f"  {name:28} {type(e).__name__} {str(e)[:120]}")

print("2 Delta store: a step over today's plain form, then over a fresh root, one path")
for order in ("plain then fresh", "fresh then plain"):
    db = DeltaTableDatabase(str(d / order.replace(" ", "_")))
    forms = [origin.as_source(), origin.as_source(tags_as_data={}, source_id="f")]
    if order.startswith("fresh"):
        forms.reverse()
    _ = job_of(forms[0], after, "next", db)
    try:
        j = job_of(forms[1], after, "next", db)
        print(" ", order, "accepted; rows", j.nodes["next"].as_table().num_rows)
    except Exception as e:  # noqa: BLE001
        print(" ", order, type(e).__name__, str(e)[:260])

print("3 save and load of a job over a derived source")
for form, s in (
    ("plain", origin.as_source()),
    ("fresh", origin.as_source(tags_as_data={}, source_id="f")),
):
    db = DeltaTableDatabase(str(d / f"save_{form}"))
    j = job_of(s, after, "next", db)
    j.save(d / f"{form}.json")
    L = PipelineJob.load(d / f"{form}.json")
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        L.run(load_only=True)
    print(
        " ",
        form,
        "unbound:",
        L.unbound_sources,
        "| stored",
        L.nodes["next"].get_all_records().num_rows,
        "| load_only rows",
        L.nodes["next"].as_table().num_rows,
        sorted({x.category.__name__ for x in w}),
    )

print("4 two authors in one job, different default ids but one given name")
other = job_of(
    DictSource(
        [{"k": "k0", "n": 100}], tag_columns=["k"], tag_only_columns=True, source_id="s"
    ),
    double,
    "one",
    InMemoryArrowDatabase(),
).nodes["one"]
for label, x, y in (
    ("default ids", origin.as_source(), other.as_source()),
    (
        "one given name, plain",
        origin.as_source(source_id="x"),
        other.as_source(source_id="x"),
    ),
    (
        "two given names, plain",
        origin.as_source(source_id="x"),
        other.as_source(source_id="y"),
    ),
    (
        "two given names, fresh",
        origin.as_source(source_id="x", tags_as_data={}),
        other.as_source(source_id="y", tags_as_data={}),
    ),
    (
        "one given name, fresh",
        origin.as_source(source_id="x", tags_as_data={}),
        other.as_source(source_id="x", tags_as_data={}),
    ),
):
    j = PipelineJob(name="both", database=InMemoryArrowDatabase())
    try:
        with j:
            after.pod(x, label="a")
            after.pod(y, label="b")
        j.run()
        print(
            f"  {label:26}",
            {
                k: sorted(v.as_table().column("out").to_pylist())
                for k, v in j.nodes.items()
                if k in "ab"
            },
        )
    except Exception as e:  # noqa: BLE001
        print(f"  {label:26}", type(e).__name__, str(e)[:120])
a1, a2 = (
    DictSource([{"k": "k0", "n": 1}], source_id="q", **kw),
    DictSource([{"k": "k0", "n": 2}], source_id="q", **kw),
)
j = PipelineJob(name="both", database=InMemoryArrowDatabase())
with j:
    double.pod(a1, label="a")
    double.pod(a2, label="b")
j.run()
print(
    "  two DictSources, one name  ",
    {
        k: v.as_table().column("dbl").to_pylist()
        for k, v in j.nodes.items()
        if k in "ab"
    },
)

print("5 shifted rows: a positional source gains a first row")
db = InMemoryArrowDatabase()
job_of(src(2), double, "one", db)
more = DictSource(
    [{"k": "new", "n": 9}, *({"k": f"k{i}", "n": i} for i in range(2))],
    tag_columns=["k"],
    tag_only_columns=True,
    source_id="s",
)
j = job_of(more, double, "one", db)
print(
    "  run",
    j.nodes["one"].as_table().num_rows,
    "as_source",
    j.nodes["one"].as_source().as_table().num_rows,
)
db = InMemoryArrowDatabase()
job_of(src(2, record_id_columns=["k"]), double, "one", db)
more = DictSource(
    [{"k": "new", "n": 9}, *({"k": f"k{i}", "n": i} for i in range(2))],
    tag_columns=["k"],
    tag_only_columns=True,
    source_id="s",
    record_id_columns=["k"],
)
j = job_of(more, double, "one", db)
print(
    "  keyed: run",
    j.nodes["one"].as_table().num_rows,
    "as_source",
    j.nodes["one"].as_source().as_table().num_rows,
)

print("6 side-effect job node")


@side_effect_pod
def look(dbl: int, ctx: object) -> None:
    pass


j = PipelineJob(name="se", database=InMemoryArrowDatabase())
with j:
    look(double.pod(src(), label="one"), label="look")
j.run()
print(
    "  has as_source",
    hasattr(j.nodes["look"], "as_source"),
    "rows",
    j.nodes["look"].as_table().num_rows,
)
