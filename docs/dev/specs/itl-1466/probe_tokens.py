"""Source-info tokens across as_source (ITL-1466, D11), on the prototype.

Needs the prototype, as probe_round2. Run: uv run python docs/dev/specs/itl-1466/probe_tokens.py
Variant FRESH drops the kept tokens, to compare.
"""

import re
import warnings

import pyarrow as pa

warnings.simplefilter("ignore")

import orcapod.core.sources.derived_source as ds  # noqa: E402
from orcapod import PipelineJob, function_pod  # noqa: E402
from orcapod.core.streams.arrow_table_stream import ArrowTableStream  # noqa: E402
from orcapod.databases import InMemoryArrowDatabase  # noqa: E402
from orcapod.operators import GroupBy, Join, SelectDataColumns  # noqa: E402
from orcapod.sources import DictSource  # noqa: E402
from orcapod.types import CacheMode  # noqa: E402

CALLS = {"n": 0}


@function_pod(output_keys=["r"])
def rnd(x: float) -> int:
    return round(x)


@function_pod(output_keys=["z"])
def plus(r: int) -> int:
    CALLS["n"] += 1
    return r + 1


@function_pod(output_keys=["n"])
def count(xs: list[float]) -> int:
    return len(xs)


def units():
    return DictSource(
        [{"s": "m1", "u": "a", "x": 1.1}, {"s": "m1", "u": "b", "x": 2.6}],
        tag_columns=["s", "u"],
        tag_only_columns=True,
        source_id="units",
        record_id_columns=["s", "u"],
    )


def short(v):
    if isinstance(v, list):
        return [short(x) for x in v]
    v = re.sub(r"([0-9a-f]{8})[0-9a-f]{20,}", r"\1..", str(v))
    return re.sub(r"[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f-]{22}", "<uuid7>", v)


def show(title, stream, n=1):
    t = stream.as_table(columns={"source": True})
    for c in t.column_names:
        if c.startswith("_source_"):
            print(f"  {title:7} {c} {t.schema.field(c).type}")
            for v in t.column(c).to_pylist()[:n]:
                print("          ", short(v))


print("Case 1: a single token per cell (a select of the units): kept")
sel = units().select_data_columns(["x"])
show("before", sel)
show("after", sel.as_source(source_id="picked"))
print("Case 1 over a function node: its own result's token (random id)")
job = PipelineJob(name="p", database=InMemoryArrowDatabase())
with job:
    rnd.pod(units(), label="fn")
job.run()
show("before", job.nodes["fn"])
show("after", job.nodes["fn"].as_source(source_id="rounded"))

print("Case 2: a list of tokens per cell (GroupBy): fresh")
g = GroupBy(by=["s"])(units())
show("before", g)
show("after", g.as_source(source_id="grouped"))

print("Case 3: tags_as_data copy, and a null token: fresh; x beside it kept")
show("before", sel)
show("after", sel.as_source({"u": "unit"}, source_id="copied"))
raw = ArrowTableStream(pa.table({"t": ["a"], "n": [1]}), tag_columns=["t"])
show("before", raw)
show("after", raw.as_source(source_id="raw"))

print("A list DATA column with one token (the list came from a source): kept")
lst = DictSource(
    [{"t": "a", "xs": [1.0, 2.0]}],
    tag_columns=["t"],
    tag_only_columns=True,
    source_id="lists",
)
show("after", lst.select_data_columns(["xs"]).as_source(source_id="l2"))
print("Two origins in one row (Join): each column keeps its own source's token")
d = DictSource([{"s": "m1", "dur": 2.0}], tag_columns=["s"], tag_only_columns=True,
               source_id="sess", record_id_columns=["s"])  # fmt: skip
show("after", Join()(units(), d).as_source(source_id="j"))

print("\nEffects downstream, kept against fresh")


def downstream():
    """Two fresh stores: base entry ids, results, operator row hashes, columns."""
    out = []
    for _ in range(2):
        job = PipelineJob(name="p", database=InMemoryArrowDatabase())
        with job:
            rnd.pod(units(), label="fn")
        job.run()
        src = job.nodes["fn"].as_source(source_id="rounded")
        db = InMemoryArrowDatabase()
        j = PipelineJob(name="d", database=db)
        with j:
            plus.pod(src, label="plus")
            SelectDataColumns(["r"], label="sel")(src)
        j.nodes["sel"].attach_databases(pipeline_database=db, cache_mode=CacheMode.LOG)
        j.run()
        n, s = j.nodes["plus"], j.nodes["sel"]
        t = n._pipeline_database.get_all_records(n._versioned_pipeline_path)
        r = s._read_stored_records()
        out.append(
            {
                "base entry ids": sorted(
                    t.column("__pipeline_base_entry_id").to_pylist()
                ),
                "operator row hash": sorted(r.column(s.HASH_COLUMN_NAME).to_pylist()),
                "stored input tokens": sorted(t.column("_source_r").to_pylist()),
                "columns": {f.name: f.type for f in t.schema},
                "op columns": {f.name: f.type for f in r.schema},
                "content hash": n.content_hash().to_string(),
                "record ids": sorted(
                    v
                    for c in t.column_names
                    if "record_id" in c and c.startswith("_tag")
                    for v in t.column(c).to_pylist()
                ),
                "as_source of sel": sorted(
                    v
                    for c in s.as_source()
                    .as_table(columns={"system_tags": True})
                    .column_names
                    if "record_id" in c
                    for v in s.as_source()
                    .as_table(columns={"system_tags": True})
                    .column(c)
                    .to_pylist()
                ),
            }
        )
    return out


kept = downstream()
original = ds.split


def fresh_split(table):
    t, ids, _ = original(table)
    return t, ids, {}


ds.split = fresh_split
fresh = downstream()
ds.split = original
for k in kept[0]:
    print(
        f"  {k:20} two stores equal: kept {kept[0][k] == kept[1][k]!s:5} "
        f"fresh {fresh[0][k] == fresh[1][k]!s:5} | kept equals fresh {kept[0][k] == fresh[0][k]}"
    )
print("  input token type", kept[0]["columns"]["_source_r"])

print("\nMemoization: a second upstream store, one downstream store")
for label, f in (("kept", original), ("fresh", fresh_split)):
    ds.split = f
    db = InMemoryArrowDatabase()
    for i in range(2):
        job = PipelineJob(name="p", database=InMemoryArrowDatabase())
        with job:
            rnd.pod(units(), label="fn")
        job.run()
        src = job.nodes["fn"].as_source(source_id="rounded")
        j = PipelineJob(name="d", database=db)
        with j:
            plus.pod(src, label="plus")
            SelectDataColumns(["r"], label="sel")(src)
        j.nodes["sel"].attach_databases(pipeline_database=db, cache_mode=CacheMode.LOG)
        CALLS["n"] = 0
        j.run()
        print(f"  {label:5} run {i + 1}: calls {CALLS['n']}, rows in the select's cache",
              j.nodes["sel"]._read_stored_records().num_rows)  # fmt: skip
        if i:
            try:
                print("        as_source of that cache:",
                      j.nodes["sel"].as_source().as_table().num_rows, "rows")  # fmt: skip
            except Exception as e:  # noqa: BLE001
                print("        as_source of that cache:", type(e).__name__)
ds.split = original
print("\nTracing a row computed from the as_source")


@function_pod(output_keys=["w"])
def half(x: float) -> float:
    return x / 2


for label, f in (("kept", original), ("fresh", fresh_split)):
    ds.split = f
    for over, pod, src, col in (
        ("function node", plus, job.nodes["fn"], "z"),
        ("plain select", half, units().select_data_columns(["x"]), "w"),
    ):
        j = PipelineJob(name="d", database=InMemoryArrowDatabase())
        with j:
            pod.pod(src.as_source(source_id="traced"), label="step")
        j.run()
        try:
            got = str(j.nodes["step"].trace({"s": "m1", "u": "a"}, col))
        except Exception as e:  # noqa: BLE001
            got = type(e).__name__ + ": " + str(e)
        got = re.sub(r"[0-9a-f]{20,}", "..", got)
        print(f"  {label:5} {over}:", got[:330].replace("\n", " / "))
ds.split = original
