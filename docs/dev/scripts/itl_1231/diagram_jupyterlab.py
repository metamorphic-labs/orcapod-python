"""Pictures and checks of the pipeline diagram in a real JupyterLab, before and after ITL-1231.

Reads the HTML fragments ``diagram_fragments.py`` wrote (``fragments/before.json`` from
``main``, ``fragments/after.json`` from the branch) and, for each, writes two notebooks
holding them as cell outputs: one signed as trusted (JupyterLab runs the diagram's script)
and one not (JupyterLab removes scripts, styles and inline SVG). No cell is executed: the
outputs are the fragments as a kernel would have shown them. It serves the notebooks with a
JupyterLab whose configuration, data, settings and runtime folders are all temporary and,
through Playwright in the installed Google Chrome (``channel="chrome"``):

- takes pictures of the diagram cells, in JupyterLab's light and dark theme, after
  closing the file browser (Ctrl+B) as a reader would, into
  ``docs/dev/specs/itl-1231-diagram-scale/<label>-<trust>-<theme>-<window width>-<cell>.png``:
  trusted, the tutorial job at first display and after ``fit`` in windows 820 and 1280
  pixels wide, the two-node job at 1280 and the tutorial job top to bottom (light,
  1280); untrusted, the tutorial and two-node jobs at 1280;
- in each trusted light notebook, clicks ``+``, ``-`` and ``fit`` with the mouse and
  presses ``+``, ``-``, arrow keys and ``0`` twice with the diagram focused (and ``+``
  once with it not focused), recording the viewBox after each, which cell is active,
  what has the focus and whether a dialog (the kernel restart ``0 0`` asks for) opened,
  into ``jupyterlab-checks.json`` beside the pictures.

JupyterLab and Chrome are heavy: run it alone on the machine. From the repository root:

    uv run --with jupyterlab --with playwright --with pillow \\
        python docs/dev/scripts/itl_1231/diagram_jupyterlab.py

None of the packages it adds is a dependency of Orcapod.
"""

# The packages this script drives are not installed where the code is type-checked, so
# their objects are Any: a warning per use says nothing.
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

ROOT = Path(__file__).resolve().parents[4]
OUT = ROOT / "docs" / "dev" / "specs" / "itl-1231-diagram-scale"
FRAGMENTS = OUT / "fragments"
THEMES = {"light": "JupyterLab Light", "dark": "JupyterLab Dark"}
# The window widths pictured (CSS pixels): the ITL-1230 documentation pictures' and a
# laptop's. The device pixel ratio keeps the text as sharp as on a laptop screen.
WIDTHS = (820, 1280)
HEIGHT = 1000
SCALE = 2
# The cells of each notebook: (picture name, fragment name, cell source).
CELLS = [
    ("tutorial", "tutorial", "job"),
    ("two-node", "two-node", "small"),
    ("tutorial-tb", "tutorial-tb", 'job.diagram(direction="TB")'),
]

VIEWBOX = """(i) => {
  const svg = document.querySelectorAll('.op-dag-view')[i].querySelector('svg');
  return svg ? svg.getAttribute('viewBox') : null;
}"""
STATE = """() => {
  const cells = [...document.querySelectorAll('.jp-Notebook .jp-Cell')];
  const a = document.activeElement;
  return {
    activeCell: cells.findIndex((c) => c.classList.contains('jp-mod-active')),
    focus: a ? (a.className && a.className.baseVal !== undefined
      ? a.className.baseVal : a.className) : null,
    dialogs: document.querySelectorAll('.jp-Dialog').length,
  };
}"""

CLIPPED = """() => [...document.querySelector('.op-dag').classList].filter(
  (c) => c.startsWith('op-dag-clip-'))"""
# What JupyterLab kept of the first diagram output, and how the image is laid out.
OUTPUT = """() => {
  const out = document.querySelector('.jp-OutputArea-output');
  const img = out && out.querySelector('img');
  const r = img && img.getBoundingClientRect();
  return {
    html: out ? out.innerHTML.slice(0, 600) : null,
    img: img ? {style: img.getAttribute('style'), width: r.width, height: r.height,
      src: (img.getAttribute('src') || '').slice(0, 40)} : null,
    outputWidth: out ? out.getBoundingClientRect().width : null,
  };
}"""


def _module(name: str) -> Any:
    """Import ``name``: a package this script runs with (``uv run --with``)."""
    return importlib.import_module(name)


def _notebook(fragments: dict[str, str], name: str) -> dict[str, Any]:
    cells = []
    for count, (_, fragment, source) in enumerate(CELLS, start=1):
        cells.append(
            {
                "cell_type": "code",
                "execution_count": count,
                "id": f"cell-{count}",
                "metadata": {},
                "source": source,
                "outputs": [
                    {
                        "output_type": "execute_result",
                        "execution_count": count,
                        "metadata": {},
                        "data": {
                            "text/html": fragments[fragment],
                            "text/plain": "diagram",
                        },
                    }
                ],
            }
        )
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "name": "python3",
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
            },
            "language_info": {"name": "python"},
            # Jupyter records a trusted notebook by the digest of its content: without
            # this, the untrusted notebook (the same outputs) would be trusted too.
            "itl_1231": name,
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def _write_settings(settings: Path, theme: str) -> None:
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


def _open(browser: Any, url: str, notebook: str, trusted: bool, width: int) -> Any:
    page = browser.new_page(
        viewport={"width": width, "height": HEIGHT}, device_scale_factor=SCALE
    )
    page.goto(f"{url}lab/tree/{notebook}?reset")
    page.wait_for_selector(".jp-Notebook .jp-CodeCell", timeout=60_000)
    page.wait_for_selector(
        ".op-dag-live" if trusted else ".jp-OutputArea-output",
        state="attached",
        timeout=60_000,
    )
    page.wait_for_timeout(2_000)  # fonts, the theme, the diagram's script
    # Close the file browser, as a reader would: the cell widens after the diagram's
    # script has set it up.
    page.keyboard.press("ControlOrMeta+b")
    page.wait_for_timeout(800)
    return page


def _checks(page: Any) -> dict[str, Any]:
    """Zoom with the mouse and the keyboard in the first diagram; what each step did."""
    steps: list[dict[str, Any]] = []
    root = page.locator(".op-dag").first

    def record(step: str) -> None:
        page.wait_for_timeout(200)
        state = page.evaluate(STATE)
        steps.append({"step": step, "viewBox": page.evaluate(VIEWBOX, 0)} | state)

    record("after fit (start of the checks)")
    for act in ("in", "in", "out", "fit"):
        root.locator(f"button[data-op-dag-act={act}]").click()
        record(f"click {act}")
    view = root.locator(".op-dag-view")
    box = view.bounding_box()
    # A click on the view's top left corner, away from the nodes, focuses it.
    page.mouse.click(box["x"] + 40, box["y"] + 6)
    record("click view")
    for key in ("+", "+", "-", "ArrowDown", "ArrowRight", "0", "0"):
        page.keyboard.press(key)
        record(f"key {key}")
    # What the ITL-1230 run may have done: a key pressed with the diagram not focused.
    page.locator(".jp-Notebook .jp-CodeCell").nth(1).locator(".jp-InputArea").click()
    page.keyboard.press("Escape")
    page.keyboard.press("+")
    record("key + (diagram not focused)")
    return {"steps": steps, "viewWidth": box["width"], "viewHeight": box["height"]}


def _shoot(page: Any, index: int, target: Path) -> None:
    cell = page.locator(".jp-Notebook .jp-CodeCell").nth(index)
    cell.scroll_into_view_if_needed()
    page.wait_for_timeout(300)
    page.screenshot(path=str(target), animations="disabled", clip=cell.bounding_box())
    _compress(target)


def _pictures(browser: Any, url: str, settings: Path) -> tuple[list[Path], Any]:
    written: list[Path] = []
    report: dict[str, Any] = {}

    def shoot(page: Any, index: int, name: str) -> None:
        target = OUT / f"{name}.png"
        _shoot(page, index, target)
        written.append(target)

    for theme_name, theme in THEMES.items():
        _write_settings(settings, theme)
        for label in ("before", "after"):
            for trust in ("trusted", "untrusted"):
                trusted = trust == "trusted"
                for width in WIDTHS if trusted else (1280,):
                    page = _open(browser, url, f"{label}-{trust}.ipynb", trusted, width)
                    stem = f"{label}-{trust}-{theme_name}-{width}"
                    if not trusted and theme_name == "light":
                        report[f"{label}-untrusted-output"] = page.evaluate(OUTPUT)
                    shoot(page, 0, f"{stem}-tutorial")
                    if width == 1280:
                        shoot(page, 1, f"{stem}-two-node")
                    if trusted:
                        if theme_name == "light":
                            report[f"{label}-{width}-first"] = {
                                "viewBox": page.evaluate(VIEWBOX, 0),
                                "clipped": page.evaluate(CLIPPED),
                            }
                        if width == 1280 and theme_name == "light":
                            shoot(page, 2, f"{stem}-tutorial-tb")
                        fit = page.locator(".op-dag").first.locator(
                            "button[data-op-dag-act=fit]"
                        )
                        fit.click()
                        shoot(page, 0, f"{stem}-tutorial-fit")
                        if theme_name == "light":
                            report[f"{label}-{width}"] = _checks(page)
                    page.close()
    return written, report


def main() -> None:
    fragments = {
        label: json.loads((FRAGMENTS / f"{label}.json").read_text(encoding="utf-8"))
        for label in ("before", "after")
    }
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
        notebooks = base / "notebooks"
        notebooks.mkdir()
        for label, by_name in fragments.items():
            for trust in ("trusted", "untrusted"):
                path = notebooks / f"{label}-{trust}.ipynb"
                _ = path.write_text(
                    json.dumps(_notebook(by_name, path.stem)), encoding="utf-8"
                )
                if trust == "trusted":
                    _ = subprocess.run(
                        [sys.executable, "-m", "jupyter", "trust", str(path)],
                        env=env,
                        check=True,
                        capture_output=True,
                    )

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
            sync_playwright = _module("playwright.sync_api").sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(channel="chrome", headless=True)
                written, report = _pictures(
                    browser, url, folders["JUPYTERLAB_SETTINGS_DIR"]
                )
                browser.close()
        finally:
            server.terminate()
            _ = server.wait(timeout=30)
    checks = OUT / "jupyterlab-checks.json"
    _ = checks.write_text(json.dumps(report, indent=1), encoding="utf-8")
    for path in [*written, checks]:
        print(f"{path.relative_to(ROOT)}: {path.stat().st_size / 1024:.0f} KB")


if __name__ == "__main__":
    main()
