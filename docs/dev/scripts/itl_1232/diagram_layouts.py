"""Pictures of the diagram's four layouts in a real JupyterLab, and of the node width (ITL-1232).

Reads the HTML fragments that ``docs/dev/scripts/itl_1231/diagram_fragments.py --out
docs/dev/specs/itl-1232-diagram-layout/fragments`` wrote (``before.json`` from ``main``,
``after.json`` from the branch), writes them as cell outputs of notebooks (``after``
signed as trusted, and once more unsigned; ``before`` trusted) and serves them with a
JupyterLab whose folders are all temporary, using the helpers of
``docs/dev/scripts/itl_1231/diagram_jupyterlab.py``. Through Playwright in the installed
Google Chrome, in a window 1280 pixels wide with the file browser closed, in JupyterLab's
light and dark theme, it takes into ``docs/dev/specs/itl-1232-diagram-layout/``:

- ``after-<theme>-<LR|TB>[-collapsed]-tutorial.png``: the tutorial job with its ``rates``
  node selected, then after ticking ``vertical``, ``collapse`` and unticking ``vertical``
  (the four layouts, switched as a reader does), recording after each click the viewBox,
  the checkboxes, the selected node and the clipped edges into ``jupyterlab-checks.json``;
- ``after-<theme>-two-node.png`` and ``before-<theme>-{tutorial,two-node}.png``: the node
  width before and after;
- ``after-untrusted-<theme>-tutorial.png``: the no-script picture (the layout of the
  arguments, left to right).

JupyterLab and Chrome are heavy: run it alone on the machine. From the repository root:

    uv run --with jupyterlab --with playwright --with pillow \\
        python docs/dev/scripts/itl_1232/diagram_layouts.py

None of the packages it adds is a dependency of Orcapod.
"""

# The packages this script drives are not installed where the code is type-checked, so
# their objects are Any: a warning per use says nothing.
# pyright: reportAny=false, reportExplicitAny=false

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[4]
OUT = ROOT / "docs" / "dev" / "specs" / "itl-1232-diagram-layout"
FRAGMENTS = OUT / "fragments"
WIDTH = 1280

STATE = """() => {
  const root = document.querySelector('.op-dag');
  const svg = root.querySelector('.op-dag-view svg');
  const box = (name) => {
    const input = root.querySelector('.op-dag-' + name + ' input');
    return input ? input.checked : null;
  };
  return {
    viewBox: svg ? svg.getAttribute('viewBox') : null,
    viewWidth: root.querySelector('.op-dag-view').getBoundingClientRect().width,
    vertical: box('direction'),
    collapse: box('collapse'),
    selected: [...root.querySelectorAll('.op-dag-node.op-dag-selected')].map(
      (g) => g.getAttribute('aria-label').split(', ')[0]),
    clipped: [...root.classList].filter((c) => c.startsWith('op-dag-clip-')),
  };
}"""
SELECT_RATES = """() => {
  const g = [...document.querySelectorAll('.op-dag-node')].find(
    (n) => (n.getAttribute('aria-label') || '').startsWith('rates,'));
  if (!g) return false;
  g.dispatchEvent(new MouseEvent('click', {bubbles: true}));
  return true;
}"""


def _helpers() -> Any:
    """The JupyterLab helpers of the ITL-1231 script."""
    path = ROOT / "docs" / "dev" / "scripts" / "itl_1231" / "diagram_jupyterlab.py"
    spec = importlib.util.spec_from_file_location("itl_1231_jupyterlab", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _pictures(h: Any, browser: Any, url: str, settings: Path) -> tuple[list[Path], Any]:
    written: list[Path] = []
    report: dict[str, Any] = {}

    def shoot(page: Any, index: int, name: str) -> None:
        target = OUT / f"{name}.png"
        h._shoot(page, index, target)
        written.append(target)

    for theme_name, theme in h.THEMES.items():
        h._write_settings(settings, theme)
        page = h._open(browser, url, "after-trusted.ipynb", True, WIDTH)
        if not page.evaluate(SELECT_RATES):
            raise RuntimeError("the diagram has no 'rates' node")
        steps = [{"step": "first display, rates selected"} | page.evaluate(STATE)]
        shoot(page, 0, f"after-{theme_name}-LR-tutorial")
        tools = page.locator(".op-dag").first
        for control, name in (
            ("direction", "TB"),
            ("collapse", "TB-collapsed"),
            ("direction", "LR-collapsed"),
        ):
            tools.locator(f".op-dag-{control} input").click()
            page.wait_for_timeout(300)
            steps.append({"step": f"click {control}"} | page.evaluate(STATE))
            shoot(page, 0, f"after-{theme_name}-{name}-tutorial")
        report[f"after-{theme_name}"] = steps
        shoot(page, 1, f"after-{theme_name}-two-node")
        page.close()
        page = h._open(browser, url, "before-trusted.ipynb", True, WIDTH)
        shoot(page, 0, f"before-{theme_name}-tutorial")
        shoot(page, 1, f"before-{theme_name}-two-node")
        page.close()
        page = h._open(browser, url, "after-untrusted.ipynb", False, WIDTH)
        shoot(page, 0, f"after-untrusted-{theme_name}-tutorial")
        page.close()
    return written, report


def main() -> None:
    h = _helpers()
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
        for name, label, trusted in (
            ("after-trusted", "after", True),
            ("after-untrusted", "after", False),
            ("before-trusted", "before", True),
        ):
            path = notebooks / f"{name}.ipynb"
            notebook = h._notebook(fragments[label], name)
            _ = path.write_text(json.dumps(notebook), encoding="utf-8")
            if trusted:
                _ = subprocess.run(
                    [sys.executable, "-m", "jupyter", "trust", str(path)],
                    env=env,
                    check=True,
                    capture_output=True,
                )
        port = h._free_port()
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
            h._wait_for_server(url)
            sync_playwright = h._module("playwright.sync_api").sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(channel="chrome", headless=True)
                written, report = _pictures(
                    h, browser, url, folders["JUPYTERLAB_SETTINGS_DIR"]
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
