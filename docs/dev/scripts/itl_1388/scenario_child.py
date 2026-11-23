"""One situation of ``scenarios.py``, in a fresh process; prints one JSON line (ITL-1388).

Modes:

- ``load_only <job.json> <label> [--database-root D] [--rebind-csv DATA]``: load the
  job, read ``<label>``'s stored view, run ``job.run(load_only=True)``, read the node.
- ``in_memory <dir>``: a job over a ``DictSource`` and an ``InMemoryArrowDatabase``,
  saved and loaded in this process.
- ``blueprint <workdir>``: ``Pipeline.load`` of ``main_pipeline.json``, saved again,
  then ``PipelineJob.from_pipeline`` with the CSV sources and the Delta database.
"""

from __future__ import annotations

from typing import Any

import json
import logging
import sys
import warnings
from pathlib import Path

from common import rows_of, user_columns  # pyright: ignore[reportImplicitRelativeImport]  # a script folder, run as scripts

logs: list[str] = []


class _Keep(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        if (
            record.name.startswith("orcapod.pipeline")
            and record.levelno >= logging.INFO
        ):
            logs.append(f"{record.levelname}: {record.getMessage()}")


logging.getLogger().addHandler(_Keep())
logging.getLogger().setLevel(logging.INFO)
warnings.simplefilter("ignore")

from orcapod import PipelineJob  # noqa: E402


def err(exc: BaseException) -> str:
    return f"ERROR {type(exc).__name__}: {exc}"


def attempt(fn):
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 - recorded
        return err(exc)


def summary_of(s) -> dict[str, Any]:
    return {
        label: {
            "reused": n.reused,
            "not_stored": n.not_stored,
            "computed": n.computed,
            "inputs_incomplete": list(n.inputs_incomplete),
        }
        for label, n in s.nodes.items()
    }


def csv_sources(data: Path) -> dict[str, Any]:
    from orcapod.sources import CSVSource

    def src(name, tags, sid):
        return CSVSource(
            str(data / name),
            tag_columns=tags,
            tag_only_columns=True,
            source_id=sid,
            record_id_columns=tags,
        )

    return {
        "readings": src("readings.csv", ["subject", "session"], "readings"),
        "meta": src("meta.csv", ["subject"], "meta"),
        "selected": src("selected.csv", ["subject"], "selected"),
    }


def mode_load_only(argv: list[str]) -> dict[str, Any]:
    path, label = Path(argv[0]), argv[1]
    database = None
    if "--database-root" in argv:
        from orcapod.databases import DeltaTableDatabase

        database = DeltaTableDatabase(Path(argv[argv.index("--database-root") + 1]))
    out: dict[str, Any] = {}
    try:
        job = PipelineJob.load(path, database=database)
    except Exception as exc:  # noqa: BLE001
        return {"load": err(exc), "log": logs}
    out["load"] = "ok"
    if "--rebind-csv" in argv:
        data = Path(argv[argv.index("--rebind-csv") + 1])
        out["bind"] = attempt(lambda: job.bind(sources=csv_sources(data)) or "ok")
    out["unbound_sources"] = attempt(lambda: list(job.unbound_sources))
    out["as_source"] = attempt(
        lambda: rows_of(user_columns(job.nodes[label].as_source().as_table()))
    )
    out["load_only"] = attempt(lambda: summary_of(job.run(load_only=True)))
    out["after_as_table"] = attempt(
        lambda: rows_of(user_columns(job.nodes[label].as_table()))
    )
    out["node_run_load_only"] = attempt(
        lambda: rows_of(user_columns(job.nodes[label].run(load_only=True).table))
    )
    out["log"] = sorted(set(logs))
    return out


def mode_in_memory(argv: list[str]) -> dict[str, Any]:
    from orcapod import function_pod
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.sources import DictSource

    folder = Path(argv[0])
    folder.mkdir(parents=True, exist_ok=True)

    @function_pod(output_keys="double")
    def double(x: float) -> float:
        return 2 * x

    def src(rows):
        return DictSource(
            data=rows,
            tag_columns=["k"],
            tag_only_columns=True,
            source_id="mem",
            record_id_columns=["k"],
        )

    rows = [{"k": "a", "x": 1.0}, {"k": "b", "x": 2.0}]
    db = InMemoryArrowDatabase()
    job = PipelineJob(name="mem", database=db)
    with job:
        double.pod(src(rows), label="double")
    job.run()
    path = folder / "mem_job.json"
    job.save(path)
    saved = json.loads(path.read_text())
    out: dict[str, Any] = {
        "saved_sources": saved["bindings"]["sources"],
        "saved_database": saved["bindings"]["database"],
    }
    # Loaded without the database: the saved in-memory config is rebuilt (empty).
    plain = PipelineJob.load(path)
    out["no_db.as_source"] = attempt(
        lambda: rows_of(user_columns(plain.nodes["double"].as_source().as_table()))
    )
    out["no_db.load_only"] = attempt(lambda: summary_of(plain.run(load_only=True)))
    # Loaded with the database given.
    given = PipelineJob.load(path, database=db)
    out["db.as_source"] = attempt(
        lambda: rows_of(user_columns(given.nodes["double"].as_source().as_table()))
    )
    out["db.load_only_before_bind"] = attempt(
        lambda: summary_of(given.run(load_only=True))
    )
    out["db.node_run_before_bind"] = attempt(
        lambda: given.nodes["double"].run(load_only=True).table.num_rows
    )
    out["db.bind"] = attempt(lambda: given.bind(sources={"mem": src(rows)}) or "ok")
    out["db.load_only_after_bind"] = attempt(
        lambda: summary_of(given.run(load_only=True))
    )
    out["db.as_table_after"] = attempt(
        lambda: rows_of(user_columns(given.nodes["double"].as_table()))
    )
    out["log"] = sorted(set(logs))
    return out


def mode_blueprint(argv: list[str]) -> dict[str, Any]:
    from orcapod import Pipeline
    from orcapod.databases import DeltaTableDatabase

    w = Path(argv[0])
    expected = json.loads((w / "expected.json").read_text())["main"]
    out: dict[str, Any] = {}
    pipeline = Pipeline.load(w / "main_pipeline.json")
    again = w / "main_pipeline.resaved.json"
    out["resave"] = attempt(lambda: (pipeline.save(again), "ok")[1])
    if again.exists():
        out["resave_equal"] = json.loads(again.read_text()) == json.loads(
            (w / "main_pipeline.json").read_text()
        )
        again.unlink()
    sources = csv_sources(w / "data")
    out["unbound_without_readings_c"] = "readings_c is a CachedSource: not rebound here"

    def make():
        return PipelineJob.from_pipeline(
            pipeline, database=DeltaTableDatabase(w / "store"), sources=sources
        )

    try:
        job = make()
    except Exception as exc:  # noqa: BLE001 - recorded
        out["from_pipeline"] = err(exc)
        out["log"] = sorted(set(logs))
        return out
    out["from_pipeline"] = "ok"
    out["unbound_sources"] = list(job.unbound_sources)
    out["hashes_equal_live"] = {
        label: attempt(
            lambda label=label: (
                job.nodes[label].content_hash().to_string()
                == expected[label]["content_hash"]
            )
        )
        for label in ("score", "after_join", "join")
    }
    out["load_only"] = attempt(lambda: summary_of(job.run(load_only=True)))
    out["score_rows_equal_live"] = attempt(
        lambda: (
            rows_of(user_columns(job.nodes["score"].as_table()))
            == expected["score"]["rows"]
        )
    )
    out["as_source_rows"] = attempt(
        lambda: len(rows_of(user_columns(job.nodes["score"].as_source().as_table())))
    )
    out["plain_run"] = attempt(lambda: str(make().run()))
    out["log"] = sorted(set(logs))
    return out


def mode_steps(argv: list[str]) -> dict[str, Any]:
    """Which step creates files at the saved database location ``watch``."""
    import os

    path, label, watch = Path(argv[0]), argv[1], Path(argv[2])

    def listing():
        if not watch.exists():
            return []
        return sorted(str(Path(d).relative_to(watch)) for d, _, _ in os.walk(watch))[:3]

    out: dict[str, Any] = {"before_load": listing()}
    job = PipelineJob.load(path)
    out["after_load"] = listing()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out["as_source"] = attempt(
            lambda: job.nodes[label].as_source().as_table().num_rows
        )
    out["as_source_warnings"] = sorted(
        {f"{w.category.__name__}: {w.message}" for w in caught}
    )
    out["after_as_source"] = listing()
    out["get_all_records"] = attempt(lambda: job.nodes[label].get_all_records())
    out["after_get_all_records"] = listing()
    out["load_only"] = attempt(lambda: summary_of(job.run(load_only=True)))
    out["after_load_only"] = listing()
    out["log"] = sorted(set(logs))
    return out


MODES = {
    "steps": mode_steps,
    "load_only": mode_load_only,
    "in_memory": mode_in_memory,
    "blueprint": mode_blueprint,
}
print(json.dumps(MODES[sys.argv[1]](sys.argv[2:]), default=str))
