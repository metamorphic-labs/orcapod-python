"""Pages and screenshots of the tutorial job's diagram, for review (ITL-1197).

Writes HTML pages of the Part 5 tutorial job's diagram (before and after a run, light and
dark page colours, a node selected, the collapse checkbox ticked, scripts off, and a run where ``firing_rate`` failed for
one unit, so the nodes below it are ``partial``) and, when Google Chrome is found, a PNG
screenshot of each, into ``docs/dev/specs/itl-1197-pipeline-diagram/``.

Run from the repository root:

    uv run python docs/dev/scripts/itl_1197/diagram_screenshots.py

The dark pages only set the page's background and text colour, as a dark notebook theme
does: the diagram draws its text and edges in the text colour (``currentColor``).
"""

from __future__ import annotations

import csv
import re
import shutil
import subprocess
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[4]
OUT = ROOT / "docs" / "dev" / "specs" / "itl-1197-pipeline-diagram"
CHROME_CANDIDATES = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "google-chrome",
    "chromium",
]
THEMES = {
    "light": "background:#ffffff;color:#1f1f1f",
    "dark": "background:#1e1e1e;color:#d4d4d4",
}
# Ticks the collapse checkbox, as a reader would, after ``SELECT_RATES``.
TICK_COLLAPSE = """<script>
var box=document.querySelector('.op-dag-collapse input');if(box)box.click();
</script>"""
# Selects the ``rates`` node once the diagram's own script has set the diagram up.
SELECT_RATES = """<script>
var g=[].find.call(document.querySelectorAll('.op-dag-node'),function(n){
return n.getAttribute('aria-label').indexOf('rates,')===0;});
if(g)g.dispatchEvent(new MouseEvent('click',{bubbles:true}));
</script>"""


def _tutorial_job(workdir: Path, fail_on: int | None = None):
    """The Part 5 tutorial job over an in-memory database, not run yet.

    Args:
        workdir: Where the dataset is written.
        fail_on: A spike count for which ``firing_rate`` raises, for a run with a
            failed row.
    """
    sys.path.insert(0, str(ROOT / "docs" / "tutorial"))
    from workshop_data import make_dataset  # pyright: ignore[reportMissingImports, reportAttributeAccessIssue]  # docs/tutorial, put on sys.path above

    from orcapod import PipelineJob, function_pod
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.sources import CSVSource

    data_dir = make_dataset(workdir)
    sessions = CSVSource(
        str(data_dir / "sessions.csv"),
        tag_columns=["subject_id", "session"],
        source_id="sessions",
    )
    units = CSVSource(
        str(data_dir / "units.csv"),
        tag_columns=["subject_id", "session", "probe", "unit"],
        source_id="units",
    )

    @function_pod(output_keys="rate_hz")
    def firing_rate(n_spikes: int, duration_s: float) -> float:
        if n_spikes == fail_on:
            raise ValueError(f"no rate for {n_spikes} spikes")
        return n_spikes / duration_s

    @function_pod(output_keys="mean_rate_hz")
    def mean_rate(rate_hz: list[float]) -> float:
        return sum(rate_hz) / len(rate_hz)

    job = PipelineJob(name="workshop", database=InMemoryArrowDatabase())
    with job:
        rates: Any = firing_rate.pod(
            units.select_data_columns(["n_spikes"]),
            sessions.select_data_columns(["duration_s"]),
            label="rates",
        )
        mean_rate.pod(
            rates.group_by(["subject_id", "session"]).select_data_columns(["rate_hz"]),
            label="session_means",
        )
    return job


def _first_spike_count(tmp: str) -> int:
    """The ``n_spikes`` of the first unit of the tutorial dataset written into ``tmp``."""
    with open(Path(tmp) / "units.csv", encoding="utf-8") as units:
        rows = list(csv.DictReader(units))
    return int(rows[0]["n_spikes"])


def _page(fragment: str, theme: str, extra: str = "") -> str:
    style = THEMES[theme]
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8"></head>'
        f'<body style="{style};font-family:system-ui,sans-serif;margin:16px">'
        f"{fragment}{extra}</body></html>"
    )


def _chrome() -> str | None:
    for candidate in CHROME_CANDIDATES:
        if Path(candidate).exists() or shutil.which(candidate):
            return candidate
    return None


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        job = _tutorial_job(Path(tmp))
        pages: dict[str, str] = {}
        before = job.diagram().to_html()
        for theme in THEMES:
            pages[f"before-run-{theme}"] = _page(before, theme)
        job.run()
        after = job.diagram().to_html()
        for theme in THEMES:
            pages[f"after-run-{theme}"] = _page(after, theme)
            pages[f"selected-{theme}"] = _page(after, theme, SELECT_RATES)
        pages["collapsed-light"] = _page(job.diagram(collapse=True).to_html(), "light")
        pages["collapse-checkbox-light"] = _page(
            after, "light", SELECT_RATES + TICK_COLLAPSE
        )
        pages["top-to-bottom-dark"] = _page(
            job.diagram(direction="TB").to_html(), "dark"
        )
        # A run where one unit's firing_rate raised: rates failed, the nodes below partial.
        failing = _tutorial_job(Path(tmp) / "failing", fail_on=_first_spike_count(tmp))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # the failed row's warning
            failing.run()
        pages["partial-light"] = _page(
            failing.diagram().to_html(), "light", SELECT_RATES
        )
        # As a viewer that drops scripts shows it: the picture (an image) and the node table.
        without_scripts = re.sub(r"<script\b.*?</script>", "", after, flags=re.S)
        pages["no-scripts-light"] = _page(without_scripts, "light")

    chrome = _chrome()
    for name, page in pages.items():
        html_path = OUT / f"{name}.html"
        html_path.write_text(page, encoding="utf-8")
        if chrome is None:
            continue
        height = 900 if "top-to-bottom" in name else 520
        command = [
            chrome,
            "--headless=new",
            "--disable-gpu",
            "--hide-scrollbars",
            f"--window-size=1200,{height}",
            f"--screenshot={OUT / f'{name}.png'}",
        ]
        command.append(html_path.as_uri())
        subprocess.run(command, check=True, capture_output=True)
        html_path.unlink()
    print(f"wrote {len(pages)} {'screenshots' if chrome else 'pages'} to {OUT}")


if __name__ == "__main__":
    main()
