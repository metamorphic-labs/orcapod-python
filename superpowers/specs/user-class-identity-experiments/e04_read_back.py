"""E04: reading stored values back in a new process after the class changed.

A writer process runs ``shift(p: Point) -> Point`` over two rows with ``Point`` imported
from ``pkg.models`` and stores results in a Delta database. Then, for each variant, the
project is copied, the class is changed as described, and a fresh reader process
tries four things:

* ``raw``    - ``DeltaTableDatabase.get_all_records(<result table>)``: type of column ``q``
* ``aware``  - the same through ``ExtensionAwareDatabase`` with the default type converter
  (what Orcapod does when it loads a table): type of ``q``
* ``python`` - ``type_converter.arrow_table_to_python_dicts`` on the aware table: first ``q``
* ``rerun``  - re-run the same pipeline with the reader's ``Point`` (if it has one):
  function calls (cache misses), node rows. Normally after the reads; a variant marked
  ``(rerun first)`` re-runs before reading, to show that the order matters.
* ``process`` - shown only when the reader did not exit cleanly

Variant ``W:__main__`` stores from a script whose ``Point`` lives in ``__main__`` and reads
from an analysis script that never defines ``Point``.

Run: ``uv run python superpowers/specs/user-class-identity-experiments/e04_read_back.py``
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from typing import Any

from _harness import run_python, write_files  # pyright: ignore[reportImplicitRelativeImport]  # run as a script beside _harness.py

POINT = """
from dataclasses import dataclass

@dataclass
class {name}:
    x: int
    {fields}
"""

WSJOB = """
import os
from pathlib import Path
from typing import Any

from orcapod import PipelineJob, function_pod
from orcapod.contexts import get_default_context
from orcapod.databases import DeltaTableDatabase, ExtensionAwareDatabase
from orcapod.sources import DictSource
from _harness import capture, delta_table_paths, report


def run_job(point_cls: Any) -> dict[str, Any]:
    calls: list[Any] = []

    def shift(p):
        calls.append(p)
        return p

    shift.__annotations__ = {"p": point_cls, "return": point_cls}
    pod = function_pod(output_keys="q")(shift)
    names = [f for f in point_cls.__dataclass_fields__]
    rows = [{"k": f"r{i}", "p": point_cls(**{n: i for n in names})} for i in (1, 2)]
    job = PipelineJob(name="ws", store=DeltaTableDatabase(Path(os.environ["WS_DB"])))
    with job:
        pod.pod(DictSource(data=rows, tag_columns=["k"], source_id="s"), label="shift")
    job.run()
    node = job.nodes["shift"]
    return {"function calls": len(calls), "node rows": node.as_table().num_rows}


def read_back(point_cls: Any) -> None:
    db_dir = Path(os.environ["WS_DB"])
    out: dict[str, Any] = {}
    rerun_first = bool(os.environ.get("RERUN_FIRST"))
    rerun_result = capture(lambda: run_job(point_cls)) if rerun_first else None
    paths = [p for p in delta_table_paths(db_dir) if "/_result/" in p]
    path = tuple(paths[0].split("/"))
    raw = DeltaTableDatabase(db_dir).get_all_records(path)
    out["raw"] = capture(lambda: str(raw.schema.field("q").type))
    conv = get_default_context().type_converter

    def aware_read():
        return ExtensionAwareDatabase(DeltaTableDatabase(db_dir), converter=conv).get_all_records(path)

    aware = capture(aware_read)
    out["aware"] = capture(lambda: str(aware.schema.field("q").type)) if not isinstance(aware, str) else aware
    out["python"] = (
        capture(lambda: repr(conv.arrow_table_to_python_dicts(aware.select(["q"]))[0]["q"]))
        if not isinstance(aware, str) else "(no table)"
    )
    out["rerun"] = rerun_result if rerun_first else (
        capture(lambda: run_job(point_cls)) if point_cls is not None else "(no Point in reader)"
    )
    report(out)
"""

READER = """
import importlib, os, wsjob
spec = os.environ.get("READER_POINT", "")
cls = None
if spec:
    mod, _, name = spec.partition(":")
    cls = getattr(importlib.import_module(mod), name)
wsjob.read_back(cls)
"""

WRITER_MODULE = (
    "from pkg.models import Point\nimport wsjob\nprint(wsjob.run_job(Point))\n"
)
WRITER_MAIN = (
    POINT.format(name="Point", fields="y: int")
    + "\nimport wsjob\nprint(wsjob.run_job(Point))\n"
)

BASE_FILES = {
    "wsjob.py": WSJOB,
    "reader.py": READER,
    "writer_module.py": WRITER_MODULE,
    "writer_main.py": WRITER_MAIN,
    "pkg/__init__.py": "",
    "pkg/models.py": POINT.format(name="Point", fields="y: int"),
}

# variant -> (writer, files to overwrite/delete in the reader's copy, READER_POINT).
# A variant name ending in "(rerun first)" re-runs the pipeline before reading.
VARIANTS: dict[str, tuple[str, dict[str, str | None], str]] = {
    "unchanged": ("writer_module.py", {}, "pkg.models:Point"),
    "module deleted": ("writer_module.py", {"pkg/models.py": None}, ""),
    "class renamed Point->Point2": (
        "writer_module.py",
        {"pkg/models.py": POINT.format(name="Point2", fields="y: int")},
        "pkg.models:Point2",
    ),
    "moved to pkg.shapes": (
        "writer_module.py",
        {
            "pkg/models.py": None,
            "pkg/shapes.py": POINT.format(name="Point", fields="y: int"),
        },
        "pkg.shapes:Point",
    ),
    "y retyped to float": (
        "writer_module.py",
        {"pkg/models.py": POINT.format(name="Point", fields="y: float")},
        "pkg.models:Point",
    ),
    "y retyped to float (rerun first)": (
        "writer_module.py",
        {"pkg/models.py": POINT.format(name="Point", fields="y: float")},
        "pkg.models:Point",
    ),
    "field z: int = 0 added": (
        "writer_module.py",
        {"pkg/models.py": POINT.format(name="Point", fields="y: int\n    z: int = 0")},
        "pkg.models:Point",
    ),
    "field y removed": (
        "writer_module.py",
        {"pkg/models.py": POINT.format(name="Point", fields="")},
        "pkg.models:Point",
    ),
    "W:__main__ -> analysis script": ("writer_main.py", {}, ""),
}


def main() -> None:
    """Write once per variant, change the class, read back in a new process."""
    with tempfile.TemporaryDirectory() as tmp:
        for name, (writer, changes, reader_point) in VARIANTS.items():
            proj = Path(tmp) / f"v{len(list(Path(tmp).iterdir()))}"
            write_files(proj, BASE_FILES)
            env = {"WS_DB": str(proj / "db")}
            written = run_python([writer], cwd=proj, env=env)
            if not written["ok"]:
                print(f"{name}: writer FAILED {written['error']}")
                continue
            for rel, content in changes.items():
                if content is None:
                    (proj / rel).unlink()
                else:
                    write_files(proj, {rel: content})
            shutil.rmtree(proj / "pkg" / "__pycache__", ignore_errors=True)
            env["READER_POINT"] = reader_point
            if name.endswith("(rerun first)"):
                env["RERUN_FIRST"] = "1"
            res = run_python(["reader.py"], cwd=proj, env=env, timeout=90)
            print(f"\n## {name}")
            result: dict[str, Any] = res["result"] or {}
            if not res["ok"]:
                result["process"] = res["error"]
            for key, value in result.items():
                print(f"  {key:<7} {value}")


if __name__ == "__main__":
    main()
