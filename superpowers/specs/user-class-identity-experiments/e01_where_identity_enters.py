"""E01: where a user class's identity enters Orcapod's hashes, names and stored schema.

The same two-field dataclass is defined in several places. Each variant runs in its own
process and reports every identity component (see ``_harness.identity_report``). The
table marks components equal to the ``__main__`` baseline.

Variants:

* ``__main__.Point``      - class defined in the running script (or a notebook cell)
* ``models_a.Point``      - identical source, imported from module ``models_a``
* ``models_b.Point``      - identical source, imported from module ``models_b``
* ``__main__.Coord``      - identical fields, other class name
* ``__main__.Shapes.Point`` - identical class nested in another class
* ``__main__.Point y:float`` - same name, field ``y`` retyped
* ``__main__.Point +z``  - same name, one extra field

A control at the end checks that the defining module of the pod FUNCTION does not
enter the pod hash, so every pod-hash difference in the table comes from the class.

Run: ``uv run python superpowers/specs/user-class-identity-experiments/e01_where_identity_enters.py``
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from _harness import print_table, run_code, write_files  # pyright: ignore[reportImplicitRelativeImport]  # run as a script beside _harness.py

MODEL_SOURCE = """
from dataclasses import dataclass

@dataclass
class Point:
    x: int
    y: int
"""

PROBE = """
import json, tempfile
from pathlib import Path
from dataclasses import dataclass
from _harness import identity_report, report
{define}
report(identity_report({value}, {cls}, Path(tempfile.mkdtemp())))
"""

VARIANTS: dict[str, dict[str, str]] = {
    "__main__.Point": {
        "define": "@dataclass\nclass Point:\n    x: int\n    y: int",
        "value": "Point(1, 2)",
        "cls": "Point",
    },
    "models_a.Point": {
        "define": "from models_a import Point",
        "value": "Point(1, 2)",
        "cls": "Point",
    },
    "models_b.Point": {
        "define": "from models_b import Point",
        "value": "Point(1, 2)",
        "cls": "Point",
    },
    "__main__.Coord": {
        "define": "@dataclass\nclass Coord:\n    x: int\n    y: int",
        "value": "Coord(1, 2)",
        "cls": "Coord",
    },
    "__main__.Shapes.Point": {
        "define": (
            "class Shapes:\n    @dataclass\n    class Point:\n"
            "        x: int\n        y: int"
        ),
        "value": "Shapes.Point(1, 2)",
        "cls": "Shapes.Point",
    },
    "__main__.Point y:float": {
        "define": "@dataclass\nclass Point:\n    x: int\n    y: float",
        "value": "Point(1, 2.0)",
        "cls": "Point",
    },
    "__main__.Point +z": {
        "define": "@dataclass\nclass Point:\n    x: int\n    y: int\n    z: int = 0",
        "value": "Point(1, 2)",
        "cls": "Point",
    },
}

ROWS = [
    "python schema (repr)",
    "arrow extension name",
    "arrow extension metadata",
    "arrow storage type",
    "source content_hash",
    "source pipeline_hash",
    "system tag columns",
    "row data content_hash",
    "pod content_hash",
    "pod pipeline_hash",
    "node pipeline_hash",
    "node identity path",
    "DB table paths",
    "read back q",
]


CONTROL_FN = """
from orcapod import function_pod

@function_pod(output_keys="q")
def inc(x: int) -> int:
    return x + 1
"""

CONTROL = """
from _harness import report
{define}
report(inc.pod.pipeline_hash().to_string())
"""


def control(root: Path) -> None:
    """Control: does the defining module of the FUNCTION enter the pod hash?"""
    main_def = CONTROL_FN
    mod_def = "from fn_mod import inc"
    a = run_code(CONTROL.format(define=main_def), cwd=root)["result"]
    b = run_code(CONTROL.format(define=mod_def), cwd=root)["result"]
    print("\n## control: same int -> int function defined in __main__ vs module fn_mod")
    print(f"  __main__.inc pod pipeline_hash {a}")
    print(f"  fn_mod.inc   pod pipeline_hash {b}")
    print(f"  equal: {a == b}")


def main() -> None:
    """Run every variant and print the comparison table."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        write_files(
            root,
            {
                "models_a.py": MODEL_SOURCE,
                "models_b.py": MODEL_SOURCE,
                "fn_mod.py": CONTROL_FN,
            },
        )
        columns: dict[str, dict[str, Any]] = {}
        for name, parts in VARIANTS.items():
            res = run_code(PROBE.format(**parts), cwd=root)
            columns[name] = res["result"] or {"error": res["error"]}
            if not res["ok"]:
                print(f"{name}: FAILED {res['error']}")
        print_table(ROWS, columns, baseline="__main__.Point")
        control(root)


if __name__ == "__main__":
    main()
