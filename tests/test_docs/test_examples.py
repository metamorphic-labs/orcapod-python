"""The scripts in ``examples/`` that newcomers are pointed at must run.

Each listed example is executed in its own interpreter with an empty temporary
working directory, so files it writes do not land in the repository.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# (script, a line its output must contain)
EXAMPLES = [
    ("examples/save_and_load_pipelines.py", "loaded job: 3 stored results"),
]


@pytest.mark.parametrize(("script", "expected"), EXAMPLES)
def test_example_runs(script: str, expected: str, tmp_path: Path):
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO_ROOT / "src")
    env["HOME"] = str(tmp_path)
    proc = subprocess.run(
        [sys.executable, str(REPO_ROOT / script)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert proc.returncode == 0, f"{script} failed:\n{proc.stderr[-3000:]}"
    assert expected in proc.stdout, proc.stdout
