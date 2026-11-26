"""Round 7: three nested ids (ITL-1466).

entry id    = hash(tags, system tags, labelled input data hash)
instance id = hash(entry id, labelled; the input's source-info tokens in name order)
record id   = hash(instance id, labelled; the result id)

Needs the round 7 prototype (local branch scratch/itl-1466-prototype). Run from
its checkout: uv run python <path to this file>
"""

import warnings

warnings.simplefilter("ignore")

import pyarrow as pa  # noqa: E402

from orcapod import PipelineJob, function_pod, side_effect_pod  # noqa: E402
from orcapod.databases import InMemoryArrowDatabase  # noqa: E402
from orcapod.errors import SideEffectLogDataContextError  # noqa: E402
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


def src(rows=ROWS):
    return DictSource(rows, tag_columns=["s"], tag_only_columns=True, source_id="units")


def build(db, rows=ROWS):
    job = PipelineJob(name="p", database=db)
    with job:
        sel = SelectDataColumns(["x"], label="sel")(src(rows))
        r = f.pod(sel, label="f")
        q = g.pod(r, label="g")
        tell(q, label="tell")
    job.nodes["sel"].attach_databases(pipeline_database=db, cache_mode=CacheMode.LOG)
    return job


def tables(db, marker):
    return {k: v for k, v in db._tables.items() if k.endswith(marker)}


def rows_of(db, key, key_column="__pipeline_record_id"):
    return db.get_all_records(tuple(key.split("/")), record_id_column=key_column)


def section(title):
    print(f"\n== {title}")


db = InMemoryArrowDatabase()
job = build(db)
job.run()
hasher = job.nodes["g"].data_context.arrow_hasher

section("1. the pipeline table of g: columns and the nesting, recomputed from the row")
(g_key,) = [k for k in tables(db, "pdb_v2") if "/g/" in f"/{k}/"]
t = rows_of(db, g_key)
print("   layout:", g_key.rsplit("/", 1)[-1])
print(
    "   id columns:",
    sorted(c for c in t.column_names if "id" in c and c.startswith("__")),
)
print("   entry id stored here?", "__pipeline_entry_id" in t.column_names)  # fmt: skip
user_tags = ["s"]
sys_tags = [c for c in t.column_names if c.startswith("_tag")]
tokens = sorted(c for c in t.column_names if c.startswith("_source_"))
ok_input = ok_record = 0
for i in range(t.num_rows):
    row = t.slice(i, 1)
    entry_pre = row.select(user_tags + sys_tags + ["__input_data_hash"])
    entry = hasher.hash_table(entry_pre)
    input_pre = pa.table({"__pipeline_entry_id": pa.array([entry.to_prefixed_digest()], pa.large_binary())})  # fmt: skip
    for c in tokens:
        input_pre = input_pre.append_column(c, row.column(c))
    inp = hasher.hash_table(input_pre)
    ok_input += (
        inp.to_prefixed_digest() == row.column("__pipeline_instance_id")[0].as_py()
    )
    rec_pre = pa.table(
        {
            "__pipeline_instance_id": pa.array(
                [inp.to_prefixed_digest()], pa.large_binary()
            ),  # fmt: skip
            "__data_id": row.column("__data_id").combine_chunks(),
        }
    )
    rec = hasher.hash_table(rec_pre)
    ok_record += (
        rec.to_prefixed_digest() == row.column("__pipeline_record_id")[0].as_py()
    )
print(f"   instance id = hash(entry id, tokens): {ok_input}/{t.num_rows} rows")
print(
    f"   record id (row key, __pipeline_record_id) = hash(instance id, result id): {ok_record}/{t.num_rows} rows"
)

section("2. the side-effect log: keyed by the entry id only, layout tdb_v2")
(tell_key,) = tables(db, "tdb_v2")
log = rows_of(db, tell_key, "__log_key")
# tell's input rows are g's output rows: the entry of each, by the prototype's helper
from orcapod.core.nodes.function_node import entry_id  # noqa: E402

g_entries = {entry_id(hasher, tag, data).to_prefixed_digest() for tag, data in job.nodes["g"].iter_data()}  # fmt: skip
print("   layout:", tell_key.rsplit("/", 1)[-1], "| log columns:", log.column_names)
print(
    "   log keys == entry ids of tell's input rows:",
    set(log.column("__log_key").to_pylist()) == g_entries,
)

section("3. f's results are lost; a new job runs (only the tokens downstream change)")
for key in [k for k in db._tables if "/f/" in f"/{k}/" and "rdb_v1" in k]:
    db._tables.pop(key)
    db._indexes.pop(key, None)
before = dict(CALLS)
job = build(db)
job.run()
t2 = rows_of(db, g_key)
print("   calls before", before, "after", CALLS)
print("   g rows:", t.num_rows, "->", t2.num_rows, "| distinct instance ids:", len(set(t2.column("__pipeline_instance_id").to_pylist())))  # fmt: skip
print("   g computed again?", CALLS["g"] > before["g"], "| tell fired again?", CALLS["fired"] > before["fired"])  # fmt: skip

section(
    "4. a side-effect log of the old layout (tdb_v1) beside it: refused before delivery"
)
db._tables[tell_key[: -len("tdb_v2")] + "tdb_v1"] = db._tables[tell_key]
fired = CALLS["fired"]
try:
    build(db, ROWS + [{"s": "c", "x": 3.0, "y": 0.0}]).run()
    print("   ran; fired", CALLS["fired"] - fired)
except SideEffectLogDataContextError as e:
    print("   SideEffectLogDataContextError:", str(e)[:70], "...")
print("   delivered:", CALLS["fired"] - fired)
