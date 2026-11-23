"""Shared helpers of the ITL-1388 save/load audit scripts (synthetic data only).

Every script takes an output directory; nothing is kept under the repository. The
reader runs in a fresh subprocess so that the module defining the function pods is
importable, absent, or changed exactly as the situation says.
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from pods_template import render  # noqa: E402  # pyright: ignore[reportImplicitRelativeImport]  # a script folder, run as scripts

POD_MODULE = "itl1388_pods"


def write_pods_module(directory: Path, variant: str) -> Path:
    """Write ``itl1388_pods.py`` of ``variant`` into ``directory`` and return the dir."""
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{POD_MODULE}.py").write_text(render(variant))
    return directory


def write_csv(path: Path, header: list[str], rows: list[list[Any]]) -> None:
    """Write a small CSV file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


# Three generations of the readings file (case vi): generation 2 changes a value and
# adds a subject, generation 3 drops a row.
READINGS = {
    1: [["s1", 1, 1.0], ["s1", 2, 2.0], ["s2", 1, 3.0], ["s2", 2, 4.0]],
    2: [["s1", 1, 1.0], ["s1", 2, 2.5], ["s2", 1, 3.0], ["s2", 2, 4.0], ["s3", 1, 5.0]],
    3: [["s1", 1, 1.0], ["s2", 1, 3.0], ["s2", 2, 4.0], ["s3", 1, 5.0], ["s3", 2, 6.0]],
}
# The second job's readings (case i): same schema, other subjects and values.
OTHER_READINGS = [["t1", 1, 10.0], ["t1", 2, 11.0], ["t2", 1, 12.0]]
META = [["s1", "g1"], ["s2", "g2"], ["s3", "g1"], ["t1", "g9"], ["t2", "g9"]]
SELECTED = [["s1", 1], ["s3", 1], ["t2", 1]]


def rows_of(table: Any) -> list[dict[str, Any]]:
    """A table's rows as sorted, JSON-friendly dicts (a set with counts, as a list)."""
    if table is None:
        return []
    rows = table.to_pylist()
    out = [json.loads(json.dumps(r, default=str, sort_keys=True)) for r in rows]
    return sorted(out, key=lambda r: json.dumps(r, sort_keys=True))


def user_columns(table: Any) -> Any:
    """Drop Orcapod's own columns (system tags, source info, meta, context)."""
    if table is None:
        return None
    keep = [
        c
        for c in table.column_names
        if not c.startswith(("_tag::", "_source_", "__", "_context_key"))
    ]
    return table.select(keep)


def run_fresh(
    script: str, args: list[str], pythonpath: list[Path] | None = None
) -> subprocess.CompletedProcess[str]:
    """Run ``script`` from this folder in a new interpreter.

    ``pythonpath`` is the only extra import path: the pods module is importable only
    when its directory is listed.
    """
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    if pythonpath:
        env["PYTHONPATH"] = os.pathsep.join(str(p) for p in pythonpath)
    return subprocess.run(
        [sys.executable, str(HERE / script), *args],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(HERE),
    )
