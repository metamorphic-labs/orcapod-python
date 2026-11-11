"""ITL-897 claim (d): which golden entries the proposed pandas hash moves.

Runs the golden generator twice into scratch directories, once as released and once with the
prototype (``prototype_v1.install()``) patched in, each in its own process, and lists every key
whose value differs. ``tests/test_hashing/golden/data`` is never written.

Run: ``uv run python superpowers/scripts/itl_897/claim_d_golden_diff.py``
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
GENERATOR = ROOT / "tests/test_hashing/golden/generate_golden_hashes.py"

RUNNER = """
import runpy, sys
sys.path.insert(0, {here!r})
if {patch!r}:
    import prototype_v1
    prototype_v1.install()
sys.argv = [{gen!r}, "--output-dir", {out!r}]
runpy.run_path({gen!r}, run_name="__main__")
"""


def _generate(out: Path, patch: bool) -> dict[str, dict[str, object]]:
    code = RUNNER.format(here=str(HERE), patch=patch, gen=str(GENERATOR), out=str(out))
    subprocess.run(
        [sys.executable, "-c", code], check=True, cwd=ROOT, capture_output=True
    )
    return {p.stem: json.loads(p.read_text()) for p in sorted(out.glob("*.json"))}


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        released = _generate(Path(tmp) / "released", patch=False)
        proposed = _generate(Path(tmp) / "proposed", patch=True)
    pinned = {
        p.stem: json.loads(p.read_text())
        for p in sorted((ROOT / "tests/test_hashing/golden/data").glob("*.json"))
    }
    assert released == pinned, "released run does not reproduce the pinned golden files"
    total = sum(len(v) for v in pinned.values())
    moved = [
        (group, key)
        for group, entries in pinned.items()
        for key, value in entries.items()
        if proposed[group].get(key) != value
    ]
    print(f"{len(moved)} of {total} pinned golden entries move:")
    for group, key in moved:
        print(f"  {group}.json  {key}")


if __name__ == "__main__":
    main()
