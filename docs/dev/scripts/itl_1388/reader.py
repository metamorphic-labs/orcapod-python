"""Load a saved job in a fresh process and record what every read shows (ITL-1388).

Usage: ``python reader.py <job.json> <out.json> [--database-root <dir>]``

Run by ``run_audit.py`` with the pods module importable (unchanged or changed) or not.
Database configs are not printed, only the reads' results.

Records, per node, before and after ``job.run(load_only=True)``: ``as_table()``,
``head()``, the notebook preview, ``output_schema()``, ``keys()``, ``as_source()``,
``get_all_records()``, hashes and paths; the load-only run's summary; ``node.run(load_only
=True)`` on a fresh load; an operator node attached in ``CacheMode.REPLAY``; a plain
``job.run()``; whether the pods module was imported; and the file saved again.
"""

from __future__ import annotations

from typing import Any

import importlib.util
import json
import logging
import sys
import traceback
import warnings
from pathlib import Path

from common import POD_MODULE, rows_of, user_columns  # pyright: ignore[reportImplicitRelativeImport]  # a script folder, run as scripts

job_path = Path(sys.argv[1])
out_path = Path(sys.argv[2])
db_root = None
sqlite_path = None
if "--sqlite" in sys.argv:
    sqlite_path = Path(sys.argv[sys.argv.index("--sqlite") + 1])
if "--database-root" in sys.argv:
    db_root = Path(sys.argv[sys.argv.index("--database-root") + 1])

log_records: list[str] = []


class _Keep(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        if record.name.startswith("orcapod") and record.levelno >= logging.INFO:
            log_records.append(
                f"{record.levelname} {record.name}: {record.getMessage()}"
            )


logging.getLogger().addHandler(_Keep())
logging.getLogger().setLevel(logging.INFO)
warnings.simplefilter("always")

from orcapod import PipelineJob  # noqa: E402
from orcapod.types import CacheMode  # noqa: E402


def err(exc: BaseException) -> str:
    return f"ERROR {type(exc).__name__}: {exc}"


def attempt(fn):
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 - every outcome is recorded
        return err(exc)


def database_override():
    if sqlite_path is not None:
        from orcapod.databases import ConnectorArrowDatabase, SQLiteConnector

        return ConnectorArrowDatabase(SQLiteConnector(sqlite_path))
    if db_root is None:
        return None
    from orcapod.databases import DeltaTableDatabase

    return DeltaTableDatabase(db_root)


def database_of(job):
    if job.database is None:
        raise RuntimeError("the loaded job has no database")
    return job.database


def load():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        job = PipelineJob.load(job_path, database=database_override())
    return job, [f"{w.category.__name__}: {w.message}" for w in caught]


def probe(node) -> dict[str, Any]:
    rec: dict[str, Any] = {"type": type(node).__name__}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        rec["content_hash"] = attempt(lambda: node.content_hash().to_string())
        rec["pipeline_hash"] = attempt(lambda: node.pipeline_hash().to_string())
        rec["node_identity_path"] = attempt(lambda: list(node.node_identity_path))
        rec["output_schema"] = attempt(
            lambda: [str(dict(s)) for s in node.output_schema()]
        )
        rec["keys"] = attempt(lambda: [list(k) for k in node.keys()])
        rec["as_table"] = attempt(lambda: rows_of(user_columns(node.as_table())))

        def head():
            view = node.head()
            return {
                "status": str(view.status),
                "rows": view.table.num_rows,
                "header": view.header()
                if callable(getattr(view, "header", None))
                else None,
            }

        rec["head"] = attempt(head)

        def preview():
            html = node._repr_html_()
            return {"length": len(html), "not_computed": "not" in html.lower()}

        rec["preview"] = attempt(preview)
        rec["as_source"] = attempt(
            lambda: rows_of(user_columns(node.as_source().as_table()))
        )
        rec["get_all_records"] = attempt(
            lambda: (
                (lambda t: 0 if t is None else t.num_rows)(node.get_all_records())
                if hasattr(node, "get_all_records")
                else "n/a"
            )
        )
    rec["warnings"] = sorted({f"{w.category.__name__}: {w.message}" for w in caught})
    return rec


def summary_record(summary) -> dict[str, Any]:
    return {
        "load_only": summary.load_only,
        "unchecked_sources": list(summary.unchecked_sources),
        "nodes": {
            label: {
                "computed": n.computed,
                "reused": n.reused,
                "failed": n.failed,
                "not_stored": n.not_stored,
                "held": n.held,
                "inputs_incomplete": list(n.inputs_incomplete),
                "first_error": None if n.first_error is None else err(n.first_error),
            }
            for label, n in summary.nodes.items()
        },
    }


result: dict[str, Any] = {"job": str(job_path), "pods_importable": None}
if "--import-pods" in sys.argv:
    # Situation a': the reader imports the definitions before loading, as a user
    # who has them would.
    __import__(POD_MODULE)
# find_spec does not import it, so the record below says whether loading did.
result["pods_importable"] = importlib.util.find_spec(POD_MODULE) is not None
imported_before = POD_MODULE in sys.modules

try:
    job, load_warnings = load()
except Exception as exc:  # noqa: BLE001
    result["load"] = err(exc)
    result["load_traceback"] = traceback.format_exc()
    out_path.write_text(json.dumps(result, indent=1, sort_keys=True))
    raise SystemExit(0)

result["load"] = "ok"
result["load_warnings"] = load_warnings
labels = sorted(job.nodes)
result["before"] = {label: probe(job.nodes[label]) for label in labels}

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    try:
        result["load_only_summary"] = summary_record(job.run(load_only=True))
    except Exception as exc:  # noqa: BLE001
        result["load_only_summary"] = err(exc)
result["load_only_warnings"] = sorted(
    {f"{w.category.__name__}: {w.message}" for w in caught}
)
result["after"] = {label: probe(job.nodes[label]) for label in labels}

# A plain run of a loaded job.
result["plain_run"] = attempt(lambda: str(job.run()))

# Save again (round trip): the reader writes it beside its own output.
resaved = out_path.with_suffix(".resaved.json")
result["resave"] = attempt(lambda: (job.save(resaved), str(resaved))[1])

# node.run(load_only=True) on a fresh load, one node at a time.
node_runs = {}
for label in labels:
    fresh, _ = load()

    def run_one(fresh=fresh, label=label):
        view = fresh.nodes[label].run(load_only=True)
        return {"status": str(view.status), "rows": rows_of(user_columns(view.table))}

    node_runs[label] = attempt(run_one)
result["node_run_load_only"] = node_runs

# Operator nodes attached in REPLAY, as the docs say to read a loaded operator's log.
replay = {}
fresh, _ = load()
for label in labels:
    node = fresh.nodes[label]
    if type(node).__name__ != "OperatorJobNode":
        continue

    def replayed(node=node):
        node.attach_databases(
            pipeline_database=database_of(fresh).at(*fresh._name),
            cache_mode=CacheMode.REPLAY,
        )
        return rows_of(user_columns(node.as_source().as_table()))

    replay[label] = attempt(replayed)
result["operator_replay_as_source"] = replay

result["pods_imported_by_loaded_job"] = (
    not imported_before
) and POD_MODULE in sys.modules
result["log"] = log_records
out_path.write_text(json.dumps(result, indent=1, sort_keys=True, default=str))
print("wrote", out_path)
