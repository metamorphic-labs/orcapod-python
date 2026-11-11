"""E03: caching across processes that share one Delta database.

A throw-away project is written to a temp dir:

* ``pkg/models.py``   - ``Point(x: int, y: int)``
* ``pkg/pipeline.py`` - defines ``Point`` itself and runs the pipeline under
  ``if __name__ == "__main__"``; run as a script, as ``python -m``, or imported
* ``nb1.py``/``nb2.py`` - stand-ins for two notebooks: ``Point`` defined in ``__main__``
* ``nb_float.py``/``nb_z.py`` - a notebook whose ``Point`` has ``y: float`` / an extra
  field ``z``
* ``uses_models.py``  - imports ``Point`` from ``pkg.models`` (class moved to a module)

Every run applies the same function pod ``shift(p: Point) -> Point`` to the same two
rows inside ``PipelineJob(name="ws")`` and reports how many times the function body ran
(cache misses), what the node returns, and which Delta tables exist afterwards. When a
run returns no rows, the log messages it printed to stderr are shown under it.
Each scenario starts from an empty database.

Run: ``uv run python superpowers/specs/user-class-identity-experiments/e03_cross_process_cache.py``
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from _harness import run_python, write_files  # pyright: ignore[reportImplicitRelativeImport]  # run as a script beside _harness.py

WSJOB = '''
"""Shared job body: run the workshop pipeline with whichever Point the caller has."""
import os
from pathlib import Path
from typing import Any

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import DictSource
from _harness import capture, delta_table_paths, report


def run(point_cls: Any, make: Any) -> None:
    calls: list[Any] = []

    def shift(p):
        calls.append(p)
        return p

    shift.__annotations__ = {"p": point_cls, "return": point_cls}
    pod = function_pod(output_keys="q")(shift)
    db_dir = Path(os.environ["WS_DB"])

    def body() -> dict[str, Any]:
        job = PipelineJob(name="ws", store=DeltaTableDatabase(db_dir))
        with job:
            src = DictSource(data=[{"k": "r1", "p": make(1, 2)}, {"k": "r2", "p": make(3, 4)}],
                             tag_columns=["k"], source_id="s")
            pod.pod(src, label="shift")
        job.run()
        node = job.nodes["shift"]
        out: dict[str, Any] = {"function calls": len(calls)}
        out["node rows"] = node.as_table().num_rows
        out["get_all_records rows"] = capture(lambda: node.get_all_records().num_rows)
        out["q via iter_data"] = capture(lambda: sorted({repr(d.as_dict()["q"]) for _, d in node.iter_data()}))
        return out

    result = capture(body)
    if not isinstance(result, dict):
        result = {"error": result}
    result["class"] = f"{point_cls.__module__}.{point_cls.__qualname__}"
    result["tables"] = len([p for p in delta_table_paths(db_dir) if "/_result/" not in p])
    report(result)
'''

POINT_INT = """
from dataclasses import dataclass

@dataclass
class Point:
    x: int
    y: int
"""

FILES = {
    "wsjob.py": WSJOB,
    "pkg/__init__.py": "",
    "pkg/models.py": POINT_INT,
    "pkg/pipeline.py": POINT_INT
    + """
def main():
    import wsjob
    wsjob.run(Point, Point)

if __name__ == "__main__":
    main()
""",
    "nb1.py": POINT_INT + "\nimport wsjob\nwsjob.run(Point, Point)\n",
    "nb2.py": POINT_INT + "\nimport wsjob\nwsjob.run(Point, Point)\n",
    "nb_float.py": POINT_INT.replace("y: int", "y: float")
    + "\nimport wsjob\nwsjob.run(Point, lambda x, y: Point(x, float(y)))\n",
    "nb_z.py": POINT_INT.replace("y: int", "y: int\n    z: int = 0")
    + "\nimport wsjob\nwsjob.run(Point, Point)\n",
    "uses_models.py": "from pkg.models import Point\nimport wsjob\nwsjob.run(Point, Point)\n",
}

RUNS: dict[str, list[str]] = {
    "script": ["pkg/pipeline.py"],
    "-m": ["-m", "pkg.pipeline"],
    "import": ["-c", "import pkg.pipeline as m; m.main()"],
    "nb1": ["nb1.py"],
    "nb2": ["nb2.py"],
    "nb_float": ["nb_float.py"],
    "nb_z": ["nb_z.py"],
    "uses_models": ["uses_models.py"],
}

SCENARIOS: dict[str, list[str]] = {
    "S1 script run twice": ["script", "script"],
    "S2 script, then python -m": ["script", "-m"],
    "S3 python -m, then imported by another script": ["-m", "import"],
    "S4 two notebooks, identical Point, shared DB": ["nb1", "nb2"],
    "S5 two notebooks, same name, y: int vs y: float": ["nb1", "nb_float"],
    "S6 two notebooks, same name, extra field z": ["nb1", "nb_z"],
    "S7 prototype in notebook, then class moved to pkg.models": ["nb1", "uses_models"],
    "S8 S7 then back to the notebook": ["nb1", "uses_models", "nb1"],
}


def main() -> None:
    """Run every scenario against a fresh database and print one line per run."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        write_files(root, FILES)
        for i, (name, steps) in enumerate(SCENARIOS.items()):
            db = root / f"db{i}"
            print(f"\n## {name}")
            for step in steps:
                res = run_python(RUNS[step], cwd=root, env={"WS_DB": str(db)})
                result: dict[str, Any] = res["result"] or {
                    "process error": res["error"]
                }
                print(f"  {step:<12} {result}")
                if result.get("node rows") == 0:
                    for message in res["log"]:
                        print(f"  {'':<12}   stderr: {message}")


if __name__ == "__main__":
    main()
