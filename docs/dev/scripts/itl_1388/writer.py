"""Build, run and save the audit's jobs (ITL-1388). Synthetic data only.

Usage: ``python writer.py <workdir> <delta|sqlite>``

Writes into ``<workdir>``:

- ``data/*.csv``: the sources (CSV, so a loaded job can rebuild them).
- ``mods_v1/itl1388_pods.py`` (the definitions used here) and
  ``mods_changed/itl1388_pods.py`` (situation c).
- the database (``store/`` for Delta, ``store.sqlite`` for SQLite).
- ``main_job.json``: the job ``main``, run three times over three generations of
  ``data/readings.csv`` (case vi), each run a fresh job object built from code, as a
  user re-running a script does; saved after the third run.
- a second job of the SAME name over ``data/other_readings.csv`` (case i), run after it,
  so the pipeline and result tables hold rows of both jobs.
- ``downstream_job.json``: a job over ``main``'s ``score`` node through
  ``node.as_source()`` (a ``DerivedSource``).
- ``main_pipeline.json``: the job's blueprint, ``job.compiled_pipeline.save``.
- ``expected.json``: what the live jobs showed (rows per node, hashes, paths).
"""

from __future__ import annotations

from typing import Any

import json
import sys
import warnings
from pathlib import Path

from common import (  # pyright: ignore[reportImplicitRelativeImport]  # a script folder, run as scripts
    META,
    OTHER_READINGS,
    READINGS,
    SELECTED,
    rows_of,
    user_columns,
    write_csv,
    write_pods_module,
)

warnings.simplefilter("ignore")

workdir = Path(sys.argv[1]).resolve()
kind = sys.argv[2]
workdir.mkdir(parents=True, exist_ok=True)

write_pods_module(workdir / "mods_v1", "v1")
write_pods_module(workdir / "mods_changed", "changed")
sys.path.insert(0, str(workdir / "mods_v1"))

import polars as pl  # noqa: E402

import itl1388_pods as P  # noqa: E402  # pyright: ignore[reportMissingImports]  # written by this script, put on sys.path above
from orcapod import PipelineJob  # noqa: E402
from orcapod.sources import CSVSource  # noqa: E402
from orcapod.core.sources.cached_source import CachedSource  # noqa: E402
from orcapod.sources import DerivedSource  # noqa: E402
from orcapod.types import CacheMode  # noqa: E402

data = workdir / "data"
write_csv(data / "meta.csv", ["subject", "group"], META)
write_csv(data / "selected.csv", ["subject", "flag"], SELECTED)
write_csv(data / "other_readings.csv", ["subject", "session", "value"], OTHER_READINGS)


def make_db():
    if kind == "delta":
        from orcapod.databases import DeltaTableDatabase

        return DeltaTableDatabase(workdir / "store")
    if kind == "sqlite":
        from orcapod.databases import ConnectorArrowDatabase, SQLiteConnector

        return ConnectorArrowDatabase(SQLiteConnector(workdir / "store.sqlite"))
    raise SystemExit(f"unknown kind {kind}")


db = make_db()


def csv_source(name: str, tags: list[str], source_id: str, keys=None):
    return CSVSource(
        str(data / name),
        tag_columns=tags,
        tag_only_columns=True,
        source_id=source_id,
        record_id_columns=keys,
    )


def build_ops_job(
    readings_file: str, readings_id: str, name: str = "main"
) -> PipelineJob:
    """The SQLite form of ``main``: operators only (a connector database cannot hold a
    function node's tables, docs/concepts/pipeline-job.md), two of them caching (LOG)."""
    job = PipelineJob(name=name, database=db)
    readings = csv_source(
        readings_file, ["subject", "session"], readings_id, ["subject", "session"]
    )
    meta = csv_source("meta.csv", ["subject"], "meta", ["subject"])
    selected = csv_source("selected.csv", ["subject"], "selected", ["subject"])
    with job:
        filt = readings.polars_filter(pl.col("value") > 1.5, label="filt")
        joined = filt.join(meta, label="join")
        joined.semi_join(selected, label="semijoin")
        readings.drop_nulls(label="dropnulls")
    pipeline_db = db.at(name)
    job.nodes["filt"].attach_databases(
        pipeline_database=pipeline_db, cache_mode=CacheMode.LOG
    )
    job.nodes["join"].attach_databases(
        pipeline_database=pipeline_db, cache_mode=CacheMode.LOG
    )
    return job


def build_job(readings_file: str, readings_id: str, name: str = "main") -> PipelineJob:
    """The audit job ``main``: one node of every kind above one CSV source."""
    if kind == "sqlite":
        return build_ops_job(readings_file, readings_id, name)
    job = PipelineJob(name=name, database=db)
    readings = csv_source(
        readings_file, ["subject", "session"], readings_id, ["subject", "session"]
    )
    meta = csv_source("meta.csv", ["subject"], "meta", ["subject"])
    selected = csv_source("selected.csv", ["subject"], "selected", ["subject"])
    cached = CachedSource(
        csv_source(
            readings_file,
            ["subject", "session"],
            readings_id + "_c",
            ["subject", "session"],
        ),
        cache_database=db.at("source_cache"),
    )
    with job:
        score = P.score.pod(readings, label="score")
        # (iii) a filter step above a function node; the filter caches (LOG), case viii
        filt = score.polars_filter(pl.col("score") > 4, label="filt")
        P.label.pod(filt, label="label_after_filter")
        maybe = P.maybe.pod(readings, label="maybe")
        dn = maybe.drop_nulls(label="dropnulls")
        P.kept.pod(dn, label="kept_after_dropnulls")
        # (iv) a join (LOG) and a semijoin (OFF)
        joined = score.join(meta, label="join")
        P.joined_label.pod(joined, label="after_join")
        semi = score.semi_join(selected, label="semijoin")
        P.label.pod(semi, label="after_semijoin")
        # (v) group_by and batch
        grp = score.group_by("subject", label="groupby")
        P.mean_score.pod(grp, label="after_groupby")
        bat = score.batch(2, label="batch")
        P.batch_sum.pod(bat, label="after_batch")
        # a side-effect node, and a function node below it
        tapped = P.tap(score, label="tap")
        P.label.pod(tapped, label="after_tap")
        # an exact user class as an output type
        P.summarize.pod(readings, label="summary")
        # a structural class (orcapod.dataclass) as an output type
        P.pair.pod(readings, label="pair")
        # a CachedSource feeding a function node
        P.score.pod(cached, label="score_cached")
    pipeline_db = db.at(name)
    job.nodes["filt"].attach_databases(
        pipeline_database=pipeline_db, cache_mode=CacheMode.LOG
    )
    job.nodes["join"].attach_databases(
        pipeline_database=pipeline_db, cache_mode=CacheMode.LOG
    )
    return job


def node_record(job, label):
    node = job.nodes[label]
    rec: dict[str, Any] = {"type": type(node).__name__}
    for attr in ("content_hash", "pipeline_hash"):
        try:
            rec[attr] = getattr(node, attr)().to_string()
        except Exception as exc:  # noqa: BLE001 - recorded
            rec[attr] = f"ERROR {type(exc).__name__}: {exc}"
    for attr in ("node_identity_path", "node_name_path"):
        try:
            rec[attr] = list(getattr(node, attr))
        except Exception as exc:  # noqa: BLE001
            rec[attr] = f"ERROR {type(exc).__name__}"
    try:
        rec["rows"] = rows_of(user_columns(node.as_table()))
    except Exception as exc:  # noqa: BLE001
        rec["rows"] = f"ERROR {type(exc).__name__}: {exc}"
    return rec


expected: dict[str, Any] = {"generations": {}}
main = None
for gen in (1, 2, 3):
    write_csv(data / "readings.csv", ["subject", "session", "value"], READINGS[gen])
    main = build_job("readings.csv", "readings")
    summary = main.run()
    expected["generations"][gen] = {
        label: rows_of(user_columns(main.nodes[label].as_table()))
        for label in sorted(main.nodes)
    }

assert main is not None
expected["main"] = {label: node_record(main, label) for label in sorted(main.nodes)}
main.save(workdir / "main_job.json")
main.compiled_pipeline.save(workdir / "main_pipeline.json")

# (i) another job of the same name, same schemas, other source: shares every table.
other = build_job("other_readings.csv", "readings_other")
other.run()
expected["other"] = {label: node_record(other, label) for label in sorted(other.nodes)}
other.save(workdir / "other_job.json")

# The contrast to (i): the same pipeline under another job name on the same database:
# every table path is relative to the job's database and its name, so nothing is shared.
separate = build_job("other_readings.csv", "readings_other", name="separate")
separate.run()
expected["separate"] = {
    label: node_record(separate, label) for label in sorted(separate.nodes)
}
separate.save(workdir / "separate_job.json")
expected["table_paths"] = sorted("/".join(p) for p in db.list_table_paths(()))

if kind == "sqlite":
    (workdir / "expected.json").write_text(
        json.dumps(expected, indent=1, sort_keys=True)
    )
    print("wrote", workdir)
    raise SystemExit(0)

# A job over a DerivedSource (main's stored score rows).
derived = DerivedSource(origin=main.nodes["score"])
down = PipelineJob(name="downstream", database=db)
with down:
    P.label.pod(derived, label="from_derived")
down.run()
expected["downstream"] = {
    label: node_record(down, label) for label in sorted(down.nodes)
}
down.save(workdir / "downstream_job.json")

# A job over a PollingSource (its impl class lives in the pods module).
from orcapod.core.sources.polling_source import PollingSource  # noqa: E402
from orcapod.types import PollingConfig  # noqa: E402

try:
    polled = PollingSource(
        P.TwoBatch(),
        tag_columns="id",
        record_id_columns="id",
        polling_config=PollingConfig(
            interval=0.01, duration=0.1, max_missed_intervals=50
        ),
        source_id="polled",
    )
    pjob = PipelineJob(name="polling", database=db)
    with pjob:
        P.double_val.pod(polled, label="doubled")
    pjob.run()
    expected["polling"] = {
        label: node_record(pjob, label) for label in sorted(pjob.nodes)
    }
    pjob.save(workdir / "polling_job.json")
except Exception as exc:  # noqa: BLE001 - recorded
    expected["polling"] = f"ERROR {type(exc).__name__}: {exc}"

(workdir / "expected.json").write_text(json.dumps(expected, indent=1, sort_keys=True))
print("wrote", workdir)
