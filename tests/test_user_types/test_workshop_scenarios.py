"""Workshop scenarios W1–W12 for user class identity (ITL-881).

Each scenario runs notebook cells (IPython ``InteractiveShell``, as a Jupyter kernel runs
them) or scripts in fresh processes, against one ``DeltaTableDatabase``, and drives the
pipeline only through the public API (``DictSource``, ``function_pod``, ``PipelineJob``,
``ExtensionAwareDatabase``). Scenario numbers follow the findings in PR #288.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests.test_user_types.child_harness import WSJOB, run_child, write_files

pytestmark = pytest.mark.integration

KERNEL = r"""
import warnings
from IPython.core.interactiveshell import InteractiveShell
from child_harness import error_text, report

shell = InteractiveShell.instance()
CELLS = {
    "setup": "import wsjob\nfrom dataclasses import dataclass\nimport orcapod as op\n",
    "define": "@dataclass\nclass Point:\n    x: int\n    y: int\n",
    "define_reordered": "@dataclass\nclass Point:\n    y: int\n    x: int\n",
    "define_float": "@dataclass\nclass Point:\n    x: int\n    y: float\n",
    "define_model": "import pydantic\nclass Point(pydantic.BaseModel):\n    x: int\n    y: int\n",
    "import_mod": "from wsmodels import Point\n",
    "reload_mod": "import importlib, wsmodels\nimportlib.reload(wsmodels)\nfrom wsmodels import Point\n",
    "pod": "shift = wsjob.make_pod(Point)\n",
    "run": "_result = wsjob.run_job(Point)\n",
    "run_pod": "_result = wsjob.run_job(Point, shift)\n",
}
log = []
for step in SCENARIO:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        res = shell.run_cell(CELLS[step], store_history=False, silent=True)
    entry = {
        "cell": step,
        "warnings": [str(w.message) for w in caught if issubclass(w.category, UserWarning)],
    }
    err = res.error_in_exec or res.error_before_exec
    if err is not None:
        entry["error"] = error_text(err)
    elif step.startswith("run"):
        entry.update(shell.user_ns["_result"])
    log.append(entry)
report(log)
"""

WSMODELS = """
from dataclasses import dataclass


@dataclass
class Point:
    x: int
    y: int
"""


@pytest.fixture
def workdir(tmp_path: Path) -> Path:
    write_files(tmp_path, {"wsjob.py": WSJOB, "wsmodels.py": WSMODELS})
    (tmp_path / "db").mkdir()
    return tmp_path


def _kernel(workdir: Path, cells: list[str]) -> list[dict[str, Any]]:
    code = f"SCENARIO = {cells!r}\n" + KERNEL
    return run_child(["-c", code], cwd=workdir, env={"WS_DB": str(workdir / "db")})


def _runs(log: list[dict[str, Any]]) -> list[dict[str, Any]]:
    errors = [e for e in log if "error" in e]
    assert not errors, errors
    return [e for e in log if e["cell"].startswith("run")]


def _script(workdir: Path, code: str, **env: str) -> Any:
    return run_child(
        ["-c", code], cwd=workdir, env={"WS_DB": str(workdir / "db"), **env}
    )


# ---------------------------------------------------------------------------
# Notebook cell re-runs in one kernel
# ---------------------------------------------------------------------------


def test_w1_rerun_define_cell_same_fields_rebinds(workdir: Path) -> None:
    log = _kernel(workdir, ["setup", "define", "run", "define", "run"])
    first, second = _runs(log)
    assert (first["calls"], first["rows"]) == (2, 2)
    assert (second["calls"], second["rows"]) == (0, 2)
    assert second["q_class_is_point"]
    assert first["pod_pipeline_hash"] == second["pod_pipeline_hash"]


def test_w1_old_pod_cell_with_redefined_class(workdir: Path) -> None:
    """Re-running only the class cell keeps the earlier pod cell usable."""
    log = _kernel(workdir, ["setup", "define", "pod", "run_pod", "define", "run_pod"])
    first, second = _runs(log)
    assert (first["calls"], second["calls"], second["rows"]) == (2, 0, 2)


def test_w1_run_all_twice(workdir: Path) -> None:
    log = _kernel(workdir, ["setup", "define", "pod", "run_pod"] * 2)
    first, second = _runs(log)
    assert (first["calls"], second["calls"], second["rows"]) == (2, 0, 2)


def test_field_order_does_not_change_identity(workdir: Path) -> None:
    log = _kernel(workdir, ["setup", "define", "run", "define_reordered", "run"])
    first, second = _runs(log)
    assert second["calls"] == 0
    assert first["extension_name"] == second["extension_name"]


def test_w2_rerun_with_changed_fields_is_a_new_type(workdir: Path) -> None:
    log = _kernel(workdir, ["setup", "define", "run", "define_float", "run"])
    first, second = _runs(log)
    assert (second["calls"], second["rows"]) == (2, 2)
    assert second["q_class_is_point"]
    assert first["pod_pipeline_hash"] != second["pod_pipeline_hash"]
    assert first["extension_name"].split("@")[0] == "Point"
    assert second["extension_name"].split("@")[0] == "Point"
    assert first["extension_name"] != second["extension_name"]
    assert set(first["table_paths"]) < set(second["table_paths"])


def test_w3_pydantic_rerun_rebinds(workdir: Path) -> None:
    log = _kernel(workdir, ["setup", "define_model", "run", "define_model", "run"])
    first, second = _runs(log)
    assert (first["calls"], second["calls"], second["rows"]) == (2, 0, 2)


def test_dataclass_and_pydantic_with_same_name_and_fields_are_distinct(
    workdir: Path,
) -> None:
    log = _kernel(workdir, ["setup", "define", "run", "define_model", "run"])
    first, second = _runs(log)
    assert second["calls"] == 2
    assert first["pod_pipeline_hash"] != second["pod_pipeline_hash"]
    assert first["extension_name"] != second["extension_name"]


def test_w4_module_reload_rebinds(workdir: Path) -> None:
    log = _kernel(workdir, ["setup", "import_mod", "run", "reload_mod", "run"])
    first, second = _runs(log)
    assert (first["calls"], second["calls"], second["rows"]) == (2, 0, 2)
    assert first["extension_name"].startswith("wsmodels.Point@")


def test_main_class_notice_once(workdir: Path) -> None:
    log = _kernel(workdir, ["setup", "define", "run", "define", "run"])
    notices = [w for e in log for w in e["warnings"] if "notebook or script" in w]
    assert len(notices) == 1
    assert "'Point'" in notices[0]


# ---------------------------------------------------------------------------
# Across processes
# ---------------------------------------------------------------------------


def test_w5_restart_same_class_hits_cache(workdir: Path) -> None:
    first = _runs(_kernel(workdir, ["setup", "define", "run"]))[0]
    second = _runs(_kernel(workdir, ["setup", "define", "run"]))[0]
    assert (first["calls"], second["calls"], second["rows"]) == (2, 0, 2)


def test_w6_restart_with_changed_fields_recomputes(workdir: Path) -> None:
    """Changed fields after a restart compute into new tables instead of 0 rows."""
    first = _runs(_kernel(workdir, ["setup", "define", "run"]))[0]
    changed = _runs(_kernel(workdir, ["setup", "define_float", "run"]))[0]
    back = _runs(_kernel(workdir, ["setup", "define", "run"]))[0]
    assert (changed["calls"], changed["rows"]) == (2, 2)
    assert first["node_identity_path"] != changed["node_identity_path"]
    assert (back["calls"], back["rows"]) == (0, 2)


def test_w7_script_as_main_twice(workdir: Path) -> None:
    write_files(
        workdir,
        {
            "pkg/__init__.py": "",
            "pkg/pipeline.py": """
                from dataclasses import dataclass
                import wsjob
                from child_harness import report

                @dataclass
                class Point:
                    x: int
                    y: int

                def main():
                    report(wsjob.run_job(Point))

                if __name__ == "__main__":
                    main()
            """,
        },
    )
    env = {"WS_DB": str(workdir / "db")}
    first = run_child(["pkg/pipeline.py"], cwd=workdir, env=env)
    second = run_child(["-m", "pkg.pipeline"], cwd=workdir, env=env)
    assert (first["calls"], second["calls"]) == (2, 0)


def test_w8_script_then_import_hits_with_declared_name(workdir: Path) -> None:
    write_files(
        workdir,
        {
            "pkg/__init__.py": "",
            "pkg/pipeline.py": """
                from dataclasses import dataclass
                import orcapod as op
                import wsjob
                from child_harness import report

                @op.register_type(name="Point")
                @dataclass
                class Point:
                    x: int
                    y: int

                def main():
                    report(wsjob.run_job(Point))

                if __name__ == "__main__":
                    main()
            """,
        },
    )
    env = {"WS_DB": str(workdir / "db")}
    first = run_child(["-m", "pkg.pipeline"], cwd=workdir, env=env)
    second = run_child(
        ["-c", "import pkg.pipeline; pkg.pipeline.main()"], cwd=workdir, env=env
    )
    assert (first["calls"], second["calls"], second["rows"]) == (2, 0, 2)
    assert first["node_identity_path"] == second["node_identity_path"]


def test_w11_notebook_to_module_with_declared_name(workdir: Path) -> None:
    """A class moved from a notebook into a module keeps its results under its name."""
    write_files(
        workdir,
        {
            "pkg/__init__.py": "",
            "pkg/models.py": """
                from dataclasses import dataclass
                import orcapod as op

                @op.register_type(name="Point")
                @dataclass
                class Point:
                    y: int
                    x: int
            """,
        },
    )
    notebook = _runs(_kernel(workdir, ["setup", "define", "run"]))[0]
    module = _script(
        workdir,
        "import wsjob\nfrom child_harness import report\nfrom pkg.models import Point\n"
        "report(wsjob.run_job(Point))\n",
    )
    assert (module["calls"], module["rows"]) == (0, 2)
    assert module["pod_pipeline_hash"] == notebook["pod_pipeline_hash"]
    assert module["node_identity_path"] == notebook["node_identity_path"]
    assert module["table_paths"] == notebook["table_paths"]
    assert module["extension_name"] == notebook["extension_name"]


def test_w11_module_without_declared_name_is_a_different_type(workdir: Path) -> None:
    notebook = _runs(_kernel(workdir, ["setup", "define", "run"]))[0]
    module = _runs(_kernel(workdir, ["setup", "import_mod", "run"]))[0]
    assert module["calls"] == 2
    assert module["pod_pipeline_hash"] != notebook["pod_pipeline_hash"]


# ---------------------------------------------------------------------------
# Reading stored values
# ---------------------------------------------------------------------------

READER = """
import os
from pathlib import Path
from child_harness import error_text, report
from orcapod.contexts import get_default_context
from orcapod.databases import DeltaTableDatabase, ExtensionAwareDatabase
import wsjob

{define}

root = Path(os.environ["WS_DB"])
path = tuple(next(p for p in wsjob.table_paths(root) if "/_result/" in p).split("/"))
converter = get_default_context().type_converter
try:
    table = ExtensionAwareDatabase(DeltaTableDatabase(root), converter=converter).get_all_records(path)
    rows = converter.arrow_table_to_python_dicts(table.select(["q"]))
    report({{"values": sorted(repr(r["q"]) for r in rows), "classes": sorted({{type(r["q"]).__module__ + "." + type(r["q"]).__qualname__ for r in rows}})}})
except Exception as exc:
    report({{"error": error_text(exc), "message": str(exc)}})
"""


def _store_from_notebook(workdir: Path) -> None:
    _runs(_kernel(workdir, ["setup", "define", "run"]))


def test_w12_read_notebook_results_from_script_with_same_class(workdir: Path) -> None:
    _store_from_notebook(workdir)
    define = "from dataclasses import dataclass\n@dataclass\nclass Point:\n    x: int\n    y: int\n"
    out = _script(workdir, READER.format(define=define))
    assert out.get("error") is None, out
    assert out["values"] == ["Point(x=1, y=1)", "Point(x=2, y=2)"]
    assert out["classes"] == ["__main__.Point"]


def test_read_with_declared_class_from_a_module(workdir: Path) -> None:
    _store_from_notebook(workdir)
    write_files(
        workdir,
        {
            "geo.py": """
                from dataclasses import dataclass
                import orcapod as op

                @op.register_type(name="Point")
                @dataclass
                class Point:
                    x: int
                    y: int
            """
        },
    )
    out = _script(workdir, READER.format(define="import geo"))
    assert out.get("error") is None, out
    assert out["classes"] == ["geo.Point"]


def test_read_without_the_class_names_it_and_how_to_register(workdir: Path) -> None:
    _store_from_notebook(workdir)
    out = _script(workdir, READER.format(define=""))
    assert out["error"].startswith("ImportError: Column 'q'")
    assert "'Point'" in out["message"]
    assert "register_type" in out["message"]
    assert "{x: int, y: int}" in out["message"]


def test_read_with_different_fields_raises_naming_both_shapes(workdir: Path) -> None:
    _store_from_notebook(workdir)
    define = "from dataclasses import dataclass\n@dataclass\nclass Point:\n    x: int\n    y: float\n"
    out = _script(workdir, READER.format(define=define))
    assert out["error"].startswith("TypeIdentityError: Column 'q'"), out
    assert "{x: int, y: int}" in out["message"]
    assert "{x: int, y: float}" in out["message"]
