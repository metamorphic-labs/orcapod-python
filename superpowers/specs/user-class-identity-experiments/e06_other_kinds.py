"""E06: which user-defined class kinds Orcapod accepts, and how each one is named.

For each kind the class is defined once in ``__main__`` and once in module ``kinds_mod``
(identical source), each in a fresh process. Each run reports:

* ``source``  - whether ``DictSource`` accepts a column of it; the stored Arrow type
  (extension name + storage type) or the error
* ``pod``     - whether a pod ``f(v: T) -> T`` runs over it in memory and what comes back
* ``pipeline_hash`` - the source's pipeline hash (last 12 hex digits)

The last column says whether the ``__main__`` and module definitions give the same
pipeline hash.

Run: ``uv run python superpowers/specs/user-class-identity-experiments/e06_other_kinds.py``
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from _harness import run_code, write_files  # pyright: ignore[reportImplicitRelativeImport]  # run as a script beside _harness.py

DEFS = """
import enum
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple, TypedDict
import pydantic

@dataclass
class Point:
    x: int
    y: int

@dataclass
class Line:
    a: Point
    b: Point

@dataclass
class WithPath:
    p: Path

class Model(pydantic.BaseModel):
    name: str
    count: int

class Color(enum.Enum):
    RED = 1
    BLUE = 2

class Mode(str, enum.Enum):
    FAST = "fast"
    SLOW = "slow"

class Pair(NamedTuple):
    a: int
    b: str

class TD(TypedDict):
    a: int
    b: str
"""

KINDS: dict[str, tuple[str, str]] = {
    "dataclass": ("Point", "Point(1, 2)"),
    "pydantic BaseModel": ("Model", "Model(name='a', count=1)"),
    "nested dataclass": ("Line", "Line(Point(1, 2), Point(3, 4))"),
    "dataclass with Path field": ("WithPath", "WithPath(Path('/a/b'))"),
    "list[Point]": ("list[Point]", "[Point(1, 2), Point(3, 4)]"),
    "Point | None": ("Point | None", "Point(1, 2)"),
    "Enum": ("Color", "Color.RED"),
    "str Enum": ("Mode", "Mode.FAST"),
    "NamedTuple": ("Pair", "Pair(1, 'x')"),
    "TypedDict": ("TD", "TD(a=1, b='x')"),
}

PROBE = """
{prelude}
from orcapod import function_pod
from orcapod.sources import DictSource
from _harness import capture, extension_info, report

T = {annotation}
value = {value}

def source_info():
    src = DictSource(data=[{{"k": "r1", "v": value}}], tag_columns=["k"], source_id="s")
    info = extension_info(src.as_table().schema.field("v").type)
    name = info["extension_name"] or "(no extension)"
    return f"{{name}} [{{info['storage_type']}}]"

def pipeline_hash():
    src = DictSource(data=[{{"k": "r1", "v": value}}], tag_columns=["k"], source_id="s")
    return src.pipeline_hash().to_string()[-12:]

def pod_run():
    def f(v):
        return v
    f.__annotations__ = {{"v": T, "return": T}}
    pod = function_pod(output_keys="q")(f)
    src = DictSource(data=[{{"k": "r1", "v": value}}], tag_columns=["k"], source_id="s")
    out = [d.as_dict()["q"] for _, d in pod.pod(src).iter_data()]
    return repr(out[0])

report({{"source": capture(source_info), "pod": capture(pod_run),
        "pipeline_hash": capture(pipeline_hash)}})
"""


def main() -> None:
    """Probe every kind from ``__main__`` and from a module."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        write_files(root, {"kinds_mod.py": DEFS})
        for kind, (annotation, value) in KINDS.items():
            results: dict[str, Any] = {}
            for where, prelude in (
                ("__main__", DEFS),
                ("kinds_mod", "from kinds_mod import *"),
            ):
                res = run_code(
                    PROBE.format(prelude=prelude, annotation=annotation, value=value),
                    cwd=root,
                )
                results[where] = res["result"] or {"process error": res["error"]}
            hashes = [str(results[w].get("pipeline_hash")) for w in results]
            if any(h.startswith("ERROR") for h in hashes):
                same = "n/a (not supported)"
            else:
                same = str(hashes[0] == hashes[1])
            print(f"\n## {kind}  (annotation {annotation})")
            for where, values in results.items():
                for key, val in values.items():
                    print(f"  {where:<9} {key:<13} {val}")
            print(f"  same pipeline_hash in __main__ and kinds_mod: {same}")


if __name__ == "__main__":
    main()
