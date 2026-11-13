"""Guard: no Spiral project id of the EDI projects may be tracked (ITL-1007).

The repository is contributed to a public repository. An id of the shape
``edi-<word>-v<digit>-<6 digits>`` names a private Spiral project, so any tracked text file that
contains one fails this test, with the file and line. The guard matches by shape, so it holds
for every project of that family.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROJECT_ID = re.compile(r"edi-[A-Za-z0-9]+-v[0-9]-[0-9]{6}")


def _tracked_files() -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=ROOT, check=True, capture_output=True
    ).stdout
    return [p for p in out.decode().split("\0") if p]


def test_no_tracked_text_file_holds_a_private_project_id() -> None:
    hits: list[str] = []
    for name in _tracked_files():
        path = ROOT / name
        if not path.is_file():
            continue
        try:
            text = path.read_bytes().decode("utf-8")
        except UnicodeDecodeError:
            continue  # binary file
        for number, line in enumerate(text.splitlines(), start=1):
            if PROJECT_ID.search(line):
                hits.append(f"{name}:{number}")
    assert not hits, "private Spiral project id in: " + ", ".join(hits)


def test_the_shape_matches_what_it_should() -> None:
    assert PROJECT_ID.search("x edi-demo-v2-123456/default")
    assert not PROJECT_ID.search("test-orcapod-362211")
