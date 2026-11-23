"""ITL-1347 differential, one side: run a fixture job and dump everything it shows.

Run it with the interpreter of the checkout under test (pinned ``main`` or a stage branch);
the script uses only the public API:

    cd <checkout> && uv run python <this file> <fixture> <mode> <orchestrator> <workdir> <out>

- ``fixture``: a name in ``fixtures.FIXTURES``.
- ``mode``: ``cold`` (fresh store, one run), ``warm`` (a fresh process over the store a
  ``cold`` run left in ``workdir``, one run), ``held`` (two runs in one process over a fresh
  store; the second is dumped), ``load_only`` (a fresh process over the store a ``cold``
  run left, ``run(load_only=True)``).
- ``orchestrator``: ``sync`` or ``async``.
- ``workdir``: holds the dataset (``data/``) and the Delta store (``store/``).
- ``out``: an empty directory for the dump.

The dump: ``manifest.json`` (the run summary, the warnings, the recording observer's events,
each node's table status, the stored tables with their Delta version counts, peak RSS and
wall time) and one Arrow IPC file per node table (``as_table(all_info=True)``) and per stored
table. ``compare.py`` compares two dumps.
"""

from __future__ import annotations

import dataclasses
import json
import os
import resource
import sys
import time
import warnings
from pathlib import Path
from typing import Any

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "tutorial"))


def _jsonable(given: object) -> Any:
    """A JSON-ready form of ``given``: plain data kept, anything else as its repr."""
    value: Any = given
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    items = getattr(value, "items", None)
    if callable(items):
        pairs: Any = items()
        return {str(k): _jsonable(v) for k, v in pairs}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        fields = dataclasses.fields(value)
        return {f.name: _jsonable(getattr(given, f.name)) for f in fields}
    if hasattr(value, "__dict__") and not isinstance(value, type):
        return {k: _jsonable(v) for k, v in sorted(vars(value).items()) if k[:1] != "_"}
    return repr(value)


class RecordingObserver:
    """An observer that records every event, with the tag values it was given."""

    def __init__(self) -> None:
        self.events: list[list[Any]] = []

    def contextualize(self, *identity_path: str) -> RecordingObserver:
        return self

    def on_run_start(self, run_id: str, pipeline_uri: str = "") -> None:
        self.events.append(["run_start"])

    def on_run_end(self, run_id: str) -> None:
        self.events.append(["run_end"])

    def on_node_start(
        self, node_label: str, node_hash: str, tag_schema: Any = None
    ) -> None:
        self.events.append(["node_start", node_label, node_hash])

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        self.events.append(["node_end", node_label, node_hash])

    def on_data_start(self, node_label: str, tag: Any, data: Any) -> None:
        self.events.append(["data_start", node_label, _jsonable(tag.as_dict())])

    def on_data_end(
        self,
        node_label: str,
        tag: Any,
        input_data: Any,
        output_data: Any,
        cached: bool,
    ) -> None:
        out = None if output_data is None else _jsonable(output_data.as_dict())
        self.events.append(
            ["data_end", node_label, _jsonable(tag.as_dict()), out, cached]
        )

    def on_data_crash(self, node_label: str, tag: Any, data: Any, error: Any) -> None:
        self.events.append(
            ["data_crash", node_label, _jsonable(tag.as_dict()), type(error).__name__]
        )

    def create_data_logger(self, tag: Any, data: Any) -> Any:
        from orcapod.pipeline.observer import NoOpObserver

        return NoOpObserver().create_data_logger(tag, data)


def _write_ipc(table: pa.Table, path: Path) -> None:
    with (
        pa.OSFile(str(path), "wb") as sink,
        pa.ipc.new_file(sink, table.schema) as writer,
    ):
        writer.write_table(table)


def _stored_tables(store: Path) -> dict[str, tuple[pa.Table, int]]:
    """Every Delta table under ``store``: its rows and its number of versions."""
    from deltalake import DeltaTable

    found: dict[str, tuple[pa.Table, int]] = {}
    for dirpath, dirnames, _ in os.walk(store):
        if "_delta_log" in dirnames:
            log = Path(dirpath) / "_delta_log"
            versions = len([f for f in os.listdir(log) if f.endswith(".json")])
            table = DeltaTable(dirpath).to_pyarrow_table()
            found[os.path.relpath(dirpath, store)] = (table, versions)
            dirnames.clear()
    return found


def main(argv: list[str]) -> int:
    fixture, mode, orchestrator, workdir_arg, out_arg = argv
    import importlib

    fixtures_module: Any = importlib.import_module("fixtures")
    workshop_data: Any = importlib.import_module("workshop_data")
    FIXTURES = fixtures_module.FIXTURES
    make_dataset = workshop_data.make_dataset

    from orcapod.databases import DeltaTableDatabase
    from orcapod.pipeline import AsyncPipelineOrchestrator

    workdir = Path(workdir_arg)
    out = Path(out_arg)
    out.mkdir(parents=True, exist_ok=False)
    data_dir = make_dataset(workdir / "data")
    store = workdir / "store"
    database = DeltaTableDatabase(store)
    job = FIXTURES[fixture](data_dir, database)
    run_kwargs: dict[str, Any] = {}
    if orchestrator == "async":
        run_kwargs["orchestrator"] = AsyncPipelineOrchestrator()

    observer = RecordingObserver()
    error: str | None = None
    summary: Any = None
    started = time.perf_counter()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            if mode == "held":
                job.run(**run_kwargs)
                observer.events.clear()
            summary = job.run(
                observer=observer, load_only=(mode == "load_only"), **run_kwargs
            )
        except Exception as exc:  # recorded: an error is a result too
            error = f"{type(exc).__name__}: {exc}"
    seconds = time.perf_counter() - started

    nodes: dict[str, Any] = {}
    for label in sorted(job.nodes):
        node = job.nodes[label]
        try:
            table = node.as_table(all_info=True)
        except Exception as exc:
            nodes[label] = {"error": f"{type(exc).__name__}: {exc}"}
            continue
        name = f"node_{len(nodes):03d}.arrow"
        _write_ipc(table, out / name)
        nodes[label] = {"file": name, "rows": table.num_rows}

    stored: dict[str, Any] = {}
    for index, (path, (table, versions)) in enumerate(
        sorted(_stored_tables(store).items())
    ):
        name = f"stored_{index:03d}.arrow"
        _write_ipc(table, out / name)
        stored[path] = {"file": name, "rows": table.num_rows, "versions": versions}

    manifest = {
        "fixture": fixture,
        "mode": mode,
        "orchestrator": orchestrator,
        "polars_max_threads": os.environ.get("POLARS_MAX_THREADS", "default"),
        "error": error,
        "summary": _jsonable(summary),
        "warnings": sorted({f"{w.category.__name__}: {w.message}" for w in caught}),
        "events": observer.events,
        "nodes": nodes,
        "stored": stored,
        "seconds": round(seconds, 3),
        "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
