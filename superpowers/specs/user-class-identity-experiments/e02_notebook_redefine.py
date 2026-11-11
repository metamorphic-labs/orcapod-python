"""E02: re-running a notebook cell that defines a dataclass (same kernel).

Uses a real IPython ``InteractiveShell`` so cells execute exactly as in Jupyter: classes
get ``__module__ == "__main__"``, and re-running a cell creates a new class object with
the same module and qualified name.

Each scenario runs in a fresh process (a fresh kernel). Cells:

1. ``define``   - ``@dataclass class Point`` (fields vary per scenario)
2. ``pod``      - ``@function_pod`` whose signature uses ``Point``
3. ``run``      - ``PipelineJob`` over a Delta database in a temp dir, two rows
4. ``reload_mod`` - ``importlib.reload`` of a module defining ``Point`` (what IPython's
   ``%autoreload`` does when the module file changes)

Scenarios list the cells run in order; the output shows, per ``run`` cell, whether it
raised and how many times the function body actually executed (cache misses).

Run: ``uv run python superpowers/specs/user-class-identity-experiments/e02_notebook_redefine.py``
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from _harness import run_code, write_files  # pyright: ignore[reportImplicitRelativeImport]  # run as a script beside _harness.py

WSMODELS = """
from dataclasses import dataclass

@dataclass
class Point:
    x: int
    y: int
"""

KERNEL = r'''
import json, tempfile
from IPython.core.interactiveshell import InteractiveShell
from _harness import report

shell = InteractiveShell.instance()
db_dir = tempfile.mkdtemp()
CELLS = {
    "setup": f"""
from dataclasses import dataclass
from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import DictSource
db = DeltaTableDatabase({db_dir!r})
calls = []
""",
    "define": "@dataclass\nclass Point:\n    x: int\n    y: int\n",
    "define_float": "@dataclass\nclass Point:\n    x: int\n    y: float\n",
    "define_z": "@dataclass\nclass Point:\n    x: int\n    y: int\n    z: int = 0\n",
    "define_model": "import pydantic\nclass Point(pydantic.BaseModel):\n    x: int\n    y: int\n",
    "import_mod": "from wsmodels import Point\n",
    "reload_mod": "import importlib, wsmodels\nimportlib.reload(wsmodels)\nfrom wsmodels import Point\n",
    "pod": """
@function_pod(output_keys="q")
def shift(p: Point) -> Point:
    calls.append(p)
    return Point(x=p.x + 1, y=p.y)
""",
    "run": """
_before = len(calls)
job = PipelineJob(name="nb", store=db)
with job:
    src = DictSource(data=[{"k": "r1", "p": Point(x=1, y=2)}, {"k": "r2", "p": Point(x=3, y=4)}],
                     tag_columns=["k"], source_id="s")
    shift.pod(src, label="shift")
job.run()
_rows = job.nodes["shift"].as_table().num_rows
_out = [type(d.as_dict()["q"]).__qualname__ + ("(same class object)" if type(d.as_dict()["q"]) is Point else "(OLD class object)")
        for _, d in job.nodes["shift"].iter_data()]
""",
}
log = []
for step in SCENARIO:
    res = shell.run_cell(CELLS[step], store_history=False, silent=True)
    err = res.error_in_exec or res.error_before_exec
    entry = {"cell": step}
    if err is not None:
        entry["error"] = f"{type(err).__name__}: {str(err).splitlines()[0]}"
    elif step == "run":
        ns = shell.user_ns
        entry["function calls"] = len(ns["calls"]) - ns["_before"]
        entry["rows"] = ns["_rows"]
        entry["iter_data q types"] = sorted(set(ns["_out"]))
    log.append(entry)
report(log)
'''

SCENARIOS: dict[str, list[str]] = {
    "1. run, re-run define cell only, run": [
        "setup",
        "define",
        "pod",
        "run",
        "define",
        "run",
    ],
    "2. run, re-run define + pod cells, run": [
        "setup",
        "define",
        "pod",
        "run",
        "define",
        "pod",
        "run",
    ],
    "3. run, re-run ALL cells (Run All), run": [
        "setup",
        "define",
        "pod",
        "run",
        "setup",
        "define",
        "pod",
        "run",
    ],
    "4. run, change y to float (define + pod), run": [
        "setup",
        "define",
        "pod",
        "run",
        "define_float",
        "pod",
        "run",
    ],
    "5. run, add field z (define + pod), run": [
        "setup",
        "define",
        "pod",
        "run",
        "define_z",
        "pod",
        "run",
    ],
    "6. pydantic model: run, re-run define + pod cells, run": [
        "setup",
        "define_model",
        "pod",
        "run",
        "define_model",
        "pod",
        "run",
    ],
    "7. class in module; importlib.reload (as %autoreload does), pod, run": [
        "setup",
        "import_mod",
        "pod",
        "run",
        "reload_mod",
        "pod",
        "run",
    ],
}


def main() -> None:
    """Run each scenario in a fresh kernel and print the per-cell log."""
    with tempfile.TemporaryDirectory() as tmp:
        write_files(Path(tmp), {"wsmodels.py": WSMODELS})
        for name, cells in SCENARIOS.items():
            code = f"SCENARIO = {cells!r}\n" + KERNEL
            res = run_code(code, cwd=Path(tmp))
            print(f"\n## {name}")
            if not res["ok"]:
                print(f"  kernel FAILED: {res['error']}")
                continue
            log: list[dict[str, Any]] = res["result"]
            for entry in log:
                if entry["cell"] in {"setup", "pod"} and "error" not in entry:
                    continue
                print(f"  {entry}")


if __name__ == "__main__":
    main()
