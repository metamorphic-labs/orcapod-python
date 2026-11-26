"""Round 6: the store as a complete record (ITL-1466).

Needs the round 6 prototype (local branch scratch/itl-1466-prototype). Run from
its checkout: uv run python <path to this file>
"""

import warnings

warnings.simplefilter("ignore")

from orcapod import PipelineJob, function_pod, side_effect_pod  # noqa: E402
from orcapod.databases import InMemoryArrowDatabase  # noqa: E402
from orcapod.errors import RowIdentifierError  # noqa: E402
from orcapod.operators import SelectDataColumns  # noqa: E402
from orcapod.side_effects import SideEffectPodConfig  # noqa: E402
from orcapod.sources import DictSource  # noqa: E402
from orcapod.types import CacheMode  # noqa: E402

CALLS = {"f": 0, "g": 0, "fired": 0}


@function_pod(output_keys=["r"])
def f(x: float) -> float:
    CALLS["f"] += 1
    return x * 2


@function_pod(output_keys=["q"])
def g(r: float) -> float:
    CALLS["g"] += 1
    return r + 1


@side_effect_pod(config=SideEffectPodConfig(track_completion=True))
def tell(q: float, ctx) -> None:
    CALLS["fired"] += 1


ROWS = [{"s": "a", "x": 1.0, "y": 0.0}, {"s": "b", "x": 2.0, "y": 0.0}]


def src():
    return DictSource(ROWS, tag_columns=["s"], tag_only_columns=True, source_id="units")


def build(db):
    job = PipelineJob(name="p", database=db)
    with job:
        sel = SelectDataColumns(["x"], label="sel")(src())
        r = f.pod(sel, label="f")
        q = g.pod(r, label="g")
        tell(q, label="tell")
    job.nodes["sel"].attach_databases(pipeline_database=db, cache_mode=CacheMode.LOG)
    return job


def ids(source):
    t = source.as_table(columns={"system_tags": True})
    col = next(c for c in t.column_names if c.startswith("_tag_record_id"))
    return [i[-12:] for i in t.column(col).to_pylist()]


def lose_results(db, function):
    for key in [k for k in db._tables if f"/{function}/" in f"/{k}" and "rdb_v1" in k]:
        db._tables.pop(key)
        db._indexes.pop(key, None)
        print(f"   lost the result table of {function}")


def section(title):
    print(f"\n== {title}")


db = InMemoryArrowDatabase()
section("1. run, then the same run again in a new job")
build(db).run()
print("   calls", CALLS)
job = build(db)
job.run()
print("   calls", CALLS, "(nothing computed, nothing fired again)")
print("   g passages:", ids(job.nodes["g"].as_source()))

section("2. f's results are lost; a new job runs: f recomputes (new result id)")
lose_results(db, "f")
job = build(db)
job.run()
print("   calls", CALLS)
tok = job.nodes["f"].as_table(columns={"source": True}).column("_source_r").to_pylist()
print("   f's output tokens name the result:", [t.split("::")[1][-12:] for t in tok])
print("   f passages (served rows):", ids(job.nodes["f"].as_source()))
gs = job.nodes["g"].as_source()
print("   g passages, default (everything):", ids(gs))
print("   g, one row per content, latest:", ids(gs.one_row_per_content("latest")))
print("   g, one row per content, first: ", ids(gs.one_row_per_content("first")))
try:
    gs.one_row_per_content("raise").as_table()
except RowIdentifierError as e:
    print("   g, one row per content, raise:  RowIdentifierError", str(e)[:40], "...")
print("   g computed again?", CALLS["g"] > 2, "| tell fired again?", CALLS["fired"] > 2)

section("3. plain function pod: result ids with and without a result store")


def plain(result_database=None):
    @function_pod(output_keys=["r"], result_database=result_database)
    def h(x: float) -> float:
        return x * 2

    return h.pod(SelectDataColumns(["x"])(src()))


store = InMemoryArrowDatabase()
a, b = plain(store), plain(store)
print("   with a store, two builds equal:", ids(a.as_source()) == ids(b.as_source()))
c, d = plain(), plain()
print("   without, two builds equal:     ", ids(c.as_source()) == ids(d.as_source()))

section("4. node and plain stream, one recipe")
job = build(InMemoryArrowDatabase())
job.run()
plain_sel = SelectDataColumns(["x"])(src())
print("   operator: node ids == plain ids:", sorted(ids(job.nodes["sel"].as_source())) == sorted(ids(plain_sel.as_source())))  # fmt: skip
rows = job.nodes["sel"]._read_stored_records()
print("   operator cache columns:", [n for n in rows.column_names if n.startswith("_") and not n.startswith("_tag")])  # fmt: skip
s1 = job.nodes["sel"].as_source()
print("   operator node, latest:", ids(s1.one_row_per_content("latest")))
