"""The diagram outputs pictured for ITL-1231, written as HTML fragments.

Writes ``docs/dev/specs/itl-1231-diagram-scale/fragments/<label>.json``: the HTML a
notebook shows for the Part 5 tutorial job after a run (9 nodes, left to right) and for a
two-node job after a run, made by the code checked out. Run it once on ``main`` with
``--label before`` and once on the branch with ``--label after``; then
``diagram_jupyterlab.py`` shows both in JupyterLab. From the repository root:

    uv run python docs/dev/scripts/itl_1231/diagram_fragments.py --label after
"""

# The jobs are built from a script loaded by path, so their objects are Any here: a
# warning per use says nothing.
# pyright: reportAny=false, reportExplicitAny=false

from __future__ import annotations

import argparse
import importlib.util
import json
import tempfile
import warnings
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[4]
OUT = ROOT / "docs" / "dev" / "specs" / "itl-1231-diagram-scale" / "fragments"


def _tutorial_job(workdir: Path) -> Any:
    """The Part 5 tutorial job, as the ITL-1197 screenshot script builds it."""
    path = ROOT / "docs" / "dev" / "scripts" / "itl_1197" / "diagram_screenshots.py"
    spec = importlib.util.spec_from_file_location("itl_1197_screenshots", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module._tutorial_job(workdir)


def _two_node_job() -> Any:
    """A source and one function pod."""
    from orcapod import PipelineJob, function_pod
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.sources import DictSource

    @function_pod(output_keys="y")
    def double(x: int) -> int:
        return 2 * x

    units = DictSource(
        [{"k": i, "x": i} for i in range(3)],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="numbers",
    )
    job = PipelineJob(name="small", database=InMemoryArrowDatabase())
    with job:
        _ = double.pod(units, label="doubled")
    return job


def main() -> None:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--label", required=True, choices=["before", "after"])
    label = parser.parse_args().label
    warnings.simplefilter("ignore")
    with tempfile.TemporaryDirectory() as tmp:
        tutorial = _tutorial_job(Path(tmp))
        _ = tutorial.run()
        small = _two_node_job()
        _ = small.run()
        fragments = {
            "tutorial": tutorial.diagram().to_html(),
            "tutorial-tb": tutorial.diagram(direction="TB").to_html(),
            "two-node": small.diagram().to_html(),
        }
    OUT.mkdir(parents=True, exist_ok=True)
    target = OUT / f"{label}.json"
    _ = target.write_text(
        json.dumps(fragments, indent=1, sort_keys=True), encoding="utf-8"
    )
    print(f"wrote {target.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
