"""E05: classes defined inside a function, and ``__main__`` classes across processes.

Part A - a dataclass defined inside a function (qualified name contains ``<locals>``):
what a user sees when building a source with it and when decorating a pod with it.

Part B - ``RayExecutor``: a pod whose input and output are a ``__main__`` dataclass runs
on a local Ray cluster. Reports the class name seen inside the Ray worker, the function
calls, and whether results match a local run (same pod hash, same table).

Part C - ``multiprocessing`` with the ``spawn`` start method (the macOS and Windows
default): the module name a ``__main__`` class has inside the child process.

Run: ``uv run python superpowers/specs/user-class-identity-experiments/e05_locals_and_executors.py``
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from _harness import run_code, run_python  # pyright: ignore[reportImplicitRelativeImport]  # run as a script beside _harness.py

PART_A = """
from dataclasses import dataclass
from orcapod import function_pod
from orcapod.sources import DictSource
from _harness import capture, report

def make():
    @dataclass
    class Point:
        x: int
        y: int
    return Point

Point = make()

def source():
    return DictSource(data=[{"k": "r1", "p": Point(1, 2)}], tag_columns=["k"], source_id="s")

def pod():
    def shift(p):
        return p
    shift.__annotations__ = {"p": Point, "return": Point}
    return function_pod(output_keys="q")(shift)

report({
    "qualname": Point.__qualname__,
    "DictSource(...)": capture(lambda: type(source()).__name__),
    "@function_pod": capture(lambda: type(pod()).__name__),
})
"""

PART_B = """
import os, tempfile
from dataclasses import dataclass
from pathlib import Path
import ray
from orcapod import PipelineJob, function_pod
from orcapod.core.executors.ray import RayExecutor
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import DictSource
from _harness import capture, delta_table_paths, report

@dataclass
class Point:
    x: int
    y: int

def make_pod(executor):
    @function_pod(output_keys=["q", "seen"], executor=executor)
    def shift(p: Point) -> tuple[Point, str]:
        return Point(p.x + 1, p.y), f"{type(p).__module__}.{type(p).__qualname__}"
    return shift

def run(executor, db_dir):
    shift = make_pod(executor)
    job = PipelineJob(name="ray", store=DeltaTableDatabase(db_dir))
    with job:
        src = DictSource(data=[{"k": "r1", "p": Point(1, 2)}], tag_columns=["k"], source_id="s")
        shift.pod(src, label="shift")
    job.run()
    node = job.nodes["shift"]
    rows = [d.as_dict() for _, d in node.iter_data()]
    q = [r["q"] for r in rows]
    return {
        "pod hash": shift.pod.pipeline_hash().to_string()[-12:],
        "class seen in function": [r["seen"] for r in rows],
        "driver gets same class object": [type(v) is Point for v in q],
        "tables": [p for p in delta_table_paths(db_dir) if "/_result/" not in p],
    }

ray.init(num_cpus=1, include_dashboard=False, log_to_driver=False)
local = capture(lambda: run(None, Path(tempfile.mkdtemp())))
remote = capture(lambda: run(RayExecutor(), Path(tempfile.mkdtemp())))
ray.shutdown()
report({"local": local, "ray": remote})
"""

PART_C = """
import multiprocessing as mp
from dataclasses import dataclass
from _harness import report

@dataclass
class Point:
    x: int
    y: int

def where(p):
    return f"{type(p).__module__}.{type(p).__qualname__}"

if __name__ == "__main__":
    ctx = mp.get_context("spawn")
    with ctx.Pool(1) as pool:
        child = pool.apply(where, (Point(1, 2),))
    report({"parent": where(Point(1, 2)), "spawn child": child})
"""


def main() -> None:
    """Run the three parts in fresh processes and print what each reports."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for title, code in (
            ("A. class defined inside a function", PART_A),
            ("B. __main__ class through RayExecutor", PART_B),
        ):
            res = run_code(code, cwd=root)
            print(f"\n## {title}")
            print(f"  {res['result'] if res['ok'] else 'FAILED ' + res['error']}")
        script = root / "spawn_demo.py"
        _ = script.write_text(PART_C, encoding="utf-8")
        res = run_python([str(script)], cwd=root)
        print("\n## C. __main__ class inside a multiprocessing spawn child")
        print(f"  {res['result'] if res['ok'] else 'FAILED ' + res['error']}")


if __name__ == "__main__":
    main()
