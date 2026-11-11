"""E07: how a user class gets registered today, and whether a declared name would work.

Part A - implicit registration. Shows that the first use (``DictSource``, ``@function_pod``)
or an explicit ``type_converter.register_python_class(cls)`` registers the class under
``f"{cls.__module__}.{cls.__qualname__}"``, and that neither offers a name parameter.

Part B - a declared name, built with INTERNAL API only (``DataclassLogicalType`` and
``type_converter.register_logical_type``; there is no public way to do this today). The
same ``Point`` is registered as ``"ws.Point"`` from ``__main__`` and from module
``models_a``. The table compares every identity component (see
``_harness.identity_report``) against ``__main__.Point`` registered as ``"ws.Point"``.
If all rows match, the existing hashing machinery already follows a declared name and
only the registration entry point is missing.

Part C - reading data stored under ``"ws.Point"`` in a fresh process, with and without
registering ``"ws.Point"`` first.

Run: ``uv run python superpowers/specs/user-class-identity-experiments/e07_registration_today.py``
"""

from __future__ import annotations

import inspect
import tempfile
from pathlib import Path
from typing import Any

from _harness import print_table, run_code, run_python, write_files  # pyright: ignore[reportImplicitRelativeImport]  # run as a script beside _harness.py

MODEL_SOURCE = """
from dataclasses import dataclass

@dataclass
class Point:
    x: int
    y: int
"""

PART_A = """
import inspect
from dataclasses import dataclass
from orcapod.contexts import get_default_context
from orcapod.sources import DictSource
from _harness import report

@dataclass
class Point:
    x: int
    y: int

conv = get_default_context().type_converter
before = conv.get_logical_type(Point)
DictSource(data=[{"k": "r1", "p": Point(1, 2)}], tag_columns=["k"], source_id="s")
after = conv.get_logical_type(Point)
report({
    "registered before first use": before is not None,
    "logical_type_name after DictSource": after.logical_type_name,
    "register_python_class signature": str(inspect.signature(conv.register_python_class)),
})
"""

DECLARE = """
import dataclasses, typing
from orcapod.contexts import get_default_context
from orcapod.logical_types.dataclass_logical_type_factory import (  # internal API
    DataclassLogicalType, DataclassLogicalTypeFactory,
)

def declare(cls, name):
    conv = get_default_context().type_converter
    storage = DataclassLogicalTypeFactory().create_for_python_type(cls, conv)
    storage = storage.get_arrow_extension_type().storage_type
    hints = typing.get_type_hints(cls)
    ann = [(f.name, hints[f.name]) for f in dataclasses.fields(cls)]
    conv.register_logical_type(DataclassLogicalType(name, cls, storage, ann))
"""

PROBE = """
import tempfile
from pathlib import Path
from dataclasses import dataclass
from _harness import identity_report, report
{declare}
{define}
{register}
report(identity_report(Point(1, 2), Point, Path(tempfile.mkdtemp())))
"""

VARIANTS: dict[str, dict[str, str]] = {
    '__main__.Point as "ws.Point"': {
        "define": "@dataclass\nclass Point:\n    x: int\n    y: int",
        "register": 'declare(Point, "ws.Point")',
    },
    'models_a.Point as "ws.Point"': {
        "define": "from models_a import Point",
        "register": 'declare(Point, "ws.Point")',
    },
    "__main__.Point (default)": {
        "define": "@dataclass\nclass Point:\n    x: int\n    y: int",
        "register": "",
    },
}

ROWS = [
    "arrow extension name",
    "source content_hash",
    "source pipeline_hash",
    "system tag columns",
    "row data content_hash",
    "pod pipeline_hash",
    "node pipeline_hash",
    "DB table paths",
]

PART_C_WRITE = """
import os
from pathlib import Path
from dataclasses import dataclass
from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import DictSource
from _harness import report
{declare}
@dataclass
class Point:
    x: int
    y: int
declare(Point, "ws.Point")
@function_pod(output_keys="q")
def shift(p: Point) -> Point:
    return p
job = PipelineJob(name="ws", store=DeltaTableDatabase(Path(os.environ["WS_DB"])))
with job:
    shift.pod(DictSource(data=[{{"k": "r1", "p": Point(1, 2)}}], tag_columns=["k"],
                         source_id="s"), label="shift")
job.run()
report("written")
"""

PART_C_READ = """
import os
from pathlib import Path
from orcapod.contexts import get_default_context
from orcapod.databases import DeltaTableDatabase, ExtensionAwareDatabase
from _harness import capture, delta_table_paths, report
{declare}
{register}
root = Path(os.environ["WS_DB"])
path = tuple(next(p for p in delta_table_paths(root) if "/_result/" in p).split("/"))
conv = get_default_context().type_converter

def read():
    t = ExtensionAwareDatabase(DeltaTableDatabase(root), converter=conv).get_all_records(path)
    return repr(conv.arrow_table_to_python_dicts(t.select(["q"]))[0]["q"])

report(capture(read))
"""


def main() -> None:
    """Run parts A, B and C and print what each shows."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        write_files(root, {"models_a.py": MODEL_SOURCE})

        res = run_code(PART_A, cwd=root)
        print("## A. implicit registration")
        for key, value in (res["result"] or {"error": res["error"]}).items():
            print(f"  {key}: {value}")

        columns: dict[str, dict[str, Any]] = {}
        for name, parts in VARIANTS.items():
            res = run_code(PROBE.format(declare=DECLARE, **parts), cwd=root)
            columns[name] = res["result"] or {"error": res["error"]}
        print("\n## B. declared name (internal API)")
        print_table(ROWS, columns, baseline='__main__.Point as "ws.Point"')

        print("\n## C. reading data stored under a declared name")
        env = {"WS_DB": str(root / "db")}
        code = inspect.cleandoc(PART_C_WRITE.format(declare=DECLARE))
        written = run_python(["-c", code], cwd=root, env=env)
        print(f"  write: {written['result'] or written['error']}")
        for label, register in (
            ("reader does not declare ws.Point", ""),
            (
                "reader declares ws.Point (class from models_a)",
                'from models_a import Point\ndeclare(Point, "ws.Point")',
            ),
        ):
            code = PART_C_READ.format(declare=DECLARE, register=register)
            res = run_python(["-c", code], cwd=root, env=env)
            print(f"  {label}: {res['result'] if res['ok'] else res['error']}")


if __name__ == "__main__":
    main()
