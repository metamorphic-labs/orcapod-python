"""Pictures of Orcapod's notebook displays in JupyterLab, for the published docs (ITL-1230).

Writes the pictures of ``docs/concepts/notebooks.md`` into ``docs/concepts/`` (a published
folder: the site copies a picture a page links to), each in JupyterLab's light and dark
theme:

- ``notebooks-preview-{light,dark}.png``: the cell ``units`` and its table preview;
- ``notebooks-run-summary-{light,dark}.png``: the cell ``job.run()`` and its run summary;
- ``notebooks-diagram-{light,dark}.png``: the cell ``job`` and its diagram after the run,
  with the ``rates`` node selected;
- ``notebooks-diagram-vertical-{light,dark}.png``: the same, after ticking the
  ``vertical`` and ``collapse`` checkboxes, as a reader does (ITL-1235).

The job is the one of Part 5 of the tutorial, over the tutorial's synthetic dataset. The
script writes a notebook, executes it with ``nbclient`` (the ``python3`` kernelspec of this
environment, so JupyterLab asks for no kernel), signs it as trusted so that JupyterLab runs
the diagram's script, serves it with a JupyterLab whose configuration, data, settings and
runtime folders are all temporary, and takes each cell's picture in headless Google Chrome
through Playwright (``channel="chrome"``: the installed Chrome, no browser download). Each
picture is reduced to a palette of at most 256 colours, which keeps it small.

JupyterLab and Chrome are heavy: run it alone on the machine. From the repository root:

    uv run --with jupyterlab --with nbclient --with ipykernel --with playwright \\
        --with pillow python docs/dev/scripts/itl_1230/notebook_screenshots.py

None of the packages it adds is a dependency of Orcapod.
"""

# The packages this script drives (nbformat, nbclient, Playwright, Pillow) are not installed
# where the code is type-checked, so their objects are Any: a warning per use says nothing.
# pyright: reportAny=false, reportExplicitAny=false

from __future__ import annotations

import importlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Any


def _module(name: str) -> Any:
    """Import ``name``: a package this script runs with (``uv run --with``), not one of
    Orcapod's dependencies, so it is not installed where the code is type-checked."""
    return importlib.import_module(name)


ROOT = Path(__file__).resolve().parents[4]
OUT = ROOT / "docs" / "concepts"
TUTORIAL = ROOT / "docs" / "tutorial"
THEMES = {"light": "JupyterLab Light", "dark": "JupyterLab Dark"}
# The device pixel ratio of the pictures: sharp when the site shows them scaled down.
SCALE = 2
# The browser window's width for each picture, in CSS pixels: about as wide as the
# picture needs, so the site (a content column of about 750 pixels) scales it down little.
# The diagram fits its picture to the width of the cell, so it is taken in a window as
# wide as a reader's (ITL-1235).
WIDTHS = {
    "preview": 820,
    "run-summary": 820,
    "diagram": 1280,
    "diagram-vertical": 1280,
}
HEIGHT = 1400
# CSS pixels cut from the top of each cell picture.
TOP_TRIM = 4

SETUP = f"""\
import sys
import warnings

sys.path.insert(0, {str(TUTORIAL)!r})
from workshop_data import make_dataset
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InDevelopmentWarning
from orcapod.sources import CSVSource

warnings.filterwarnings("ignore", category=InDevelopmentWarning)
data_dir = make_dataset()

sessions = CSVSource(
    str(data_dir / "sessions.csv"),
    tag_columns=["subject_id", "session"], tag_only_columns=True,
    source_id="sessions",
)
units = CSVSource(
    str(data_dir / "units.csv"),
    tag_columns=["subject_id", "session", "probe", "unit"], tag_only_columns=True,
    source_id="units",
)


@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    return n_spikes / duration_s


@function_pod(output_keys="mean_rate_hz")
def mean_rate(rate_hz: list[float]) -> float:
    return sum(rate_hz) / len(rate_hz)


job = PipelineJob(name="workshop", database=InMemoryArrowDatabase())
with job:
    rates = firing_rate.pod(
        units.select_data_columns(["n_spikes"]),
        sessions.select_data_columns(["duration_s"]),
        label="rates",
    )
    mean_rate.pod(
        rates.group_by(["subject_id", "session"]).select_data_columns(["rate_hz"]),
        label="session_means",
    )"""

# The cells pictured, in notebook order after the setup cell: (picture name, source).
CELLS = [("preview", "units"), ("run-summary", "job.run()"), ("diagram", "job")]

# The pictures: (picture name, index of the cell after the setup cell, the diagram's
# checkboxes ticked in order before the picture is taken).
PICTURES = [
    ("preview", 1, ()),
    ("run-summary", 2, ()),
    ("diagram", 3, ()),
    ("diagram-vertical", 3, ("direction", "collapse")),
]

# Selects the ``rates`` node, as a reader's click does.
SELECT_RATES = """() => {
  const g = [...document.querySelectorAll('.op-dag-node')].find(
    (n) => (n.getAttribute('aria-label') || '').startsWith('rates,'));
  if (!g) return false;
  g.dispatchEvent(new MouseEvent('click', {bubbles: true}));
  return true;
}"""

# Ticks one of the diagram's checkboxes (``direction`` is the one labelled ``vertical``),
# as a reader's click does, without making the cell the active one: a click event only,
# no pointer press, so the picture shows no active-cell bar or cell toolbar.
TICK = """(control) => {
  const input = document.querySelector('.op-dag-' + control + ' input');
  if (!input) return false;
  input.click();
  return true;
}"""


def _write_settings(settings: Path, theme: str) -> None:
    """Write JupyterLab user settings: the theme, and no news or update notices."""
    apputils = settings / "@jupyterlab" / "apputils-extension"
    apputils.mkdir(parents=True, exist_ok=True)
    _ = (apputils / "themes.jupyterlab-settings").write_text(
        json.dumps({"theme": theme, "theme-scrollbars": False})
    )
    _ = (apputils / "notification.jupyterlab-settings").write_text(
        json.dumps({"fetchNews": "false", "checkForUpdates": False})
    )


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _write_notebook(path: Path, env: dict[str, str]) -> None:
    """Write, execute and trust the notebook of ``SETUP`` and ``CELLS``."""
    nbformat = _module("nbformat")
    notebook_client = _module("nbclient").NotebookClient

    cells = [nbformat.v4.new_code_cell(SETUP)]
    cells += [nbformat.v4.new_code_cell(source) for _, source in CELLS]
    nb = nbformat.v4.new_notebook(
        cells=cells,
        metadata={
            "kernelspec": {
                "name": "python3",
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
            }
        },
    )
    notebook_client(
        nb, kernel_name="python3", resources={"metadata": {"path": str(path.parent)}}
    ).execute()
    nbformat.write(nb, path)
    # Signed with the notary of the temporary data folder, which the server reads.
    _ = subprocess.run(
        [sys.executable, "-m", "jupyter", "trust", str(path)],
        env=env,
        check=True,
        capture_output=True,
    )


def _wait_for_server(url: str, timeout: float = 90.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url + "api/status", timeout=2):
                return
        except OSError:
            time.sleep(0.5)
    raise RuntimeError(f"JupyterLab did not start at {url}")


def _compress(png: Path) -> None:
    """Reduce ``png`` to a palette of at most 256 colours, without dithering."""
    image_module = _module("PIL.Image")

    with image_module.open(png) as image:
        quantized = image.convert("RGB").quantize(
            colors=256,
            method=image_module.Quantize.MEDIANCUT,
            dither=image_module.Dither.NONE,
        )
    quantized.save(png, optimize=True)


def _pictures(url: str, notebook: str, settings: Path) -> list[Path]:
    sync_playwright = _module("playwright.sync_api").sync_playwright

    written: list[Path] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True)
        for name, theme in THEMES.items():
            _write_settings(settings, theme)
            for picture, index, ticks in PICTURES:
                page = browser.new_page(
                    viewport={"width": WIDTHS[picture], "height": HEIGHT},
                    device_scale_factor=SCALE,
                )
                page.goto(f"{url}lab/tree/{notebook}?reset")
                page.wait_for_selector(".jp-Notebook .jp-CodeCell", timeout=60_000)
                page.wait_for_selector(".op-dag-node", timeout=60_000)
                page.wait_for_timeout(2_000)  # fonts, the theme, the diagram's script
                # Collapse the left side bar: the notebook takes the width.
                page.keyboard.press("ControlOrMeta+b")
                page.wait_for_timeout(500)
                cell = page.locator(".jp-Notebook .jp-CodeCell").nth(index)
                cell.scroll_into_view_if_needed()
                if picture.startswith("diagram"):
                    if not page.evaluate(SELECT_RATES):
                        raise RuntimeError("the diagram has no 'rates' node")
                    page.wait_for_timeout(500)
                    for control in ticks:
                        if not page.evaluate(TICK, control):
                            raise RuntimeError(
                                f"the diagram has no {control!r} checkbox"
                            )
                        page.wait_for_timeout(500)
                    cell.scroll_into_view_if_needed()
                target = OUT / f"notebooks-{picture}-{name}.png"
                # The top edge of the cell holds the focus line of the cell above it.
                box = cell.bounding_box()
                page.screenshot(
                    path=str(target),
                    animations="disabled",
                    clip={
                        "x": box["x"],
                        "y": box["y"] + TOP_TRIM,
                        "width": box["width"],
                        "height": box["height"] - TOP_TRIM,
                    },
                )
                _compress(target)
                written.append(target)
                page.close()
        browser.close()
    return written


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        folders = {
            "JUPYTER_CONFIG_DIR": base / "config",
            "JUPYTER_DATA_DIR": base / "data",
            "JUPYTER_RUNTIME_DIR": base / "runtime",
            "JUPYTERLAB_SETTINGS_DIR": base / "settings",
            "JUPYTERLAB_WORKSPACES_DIR": base / "workspaces",
        }
        for folder in folders.values():
            folder.mkdir()
        env = {**os.environ, **{key: str(value) for key, value in folders.items()}}
        os.environ.update({key: str(value) for key, value in folders.items()})
        notebooks = base / "notebooks"
        notebooks.mkdir()
        notebook = notebooks / "notebooks.ipynb"
        _write_notebook(notebook, env)

        port = _free_port()
        url = f"http://127.0.0.1:{port}/"
        server = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "jupyter",
                "lab",
                "--no-browser",
                f"--port={port}",
                "--ip=127.0.0.1",
                f"--ServerApp.root_dir={notebooks}",
                "--IdentityProvider.token=",
                "--ServerApp.password=",
                "--ServerApp.disable_check_xsrf=True",
                "--LabApp.news_url=",
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            _wait_for_server(url)
            written = _pictures(url, notebook.name, folders["JUPYTERLAB_SETTINGS_DIR"])
        finally:
            server.terminate()
            _ = server.wait(timeout=30)
    for path in written:
        print(f"{path.relative_to(ROOT)}: {path.stat().st_size / 1024:.0f} KB")


if __name__ == "__main__":
    main()
