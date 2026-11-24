"""The ITL-1361 cross-version check (``docs/dev/scripts/itl_1347/cross_version_entry_ids.py``).

The script is developer material, not part of the package: it is loaded from its file.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "docs/dev/scripts/itl_1347/cross_version_entry_ids.py"
)


@pytest.fixture
def script(monkeypatch: pytest.MonkeyPatch) -> Any:
    """The script, with two of its cases; its import ignores every warning."""
    import warnings

    with warnings.catch_warnings():
        spec = importlib.util.spec_from_file_location("itl_1361_cross_version", _SCRIPT)
        assert spec is not None and spec.loader is not None
        module: Any = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        every = module.cases()
    few = {label: case for label, case in sorted(every.items()) if "nullable/" in label}
    assert len(few) >= 2
    monkeypatch.setattr(module, "cases", lambda: few)
    return module


def _run(script: Any, monkeypatch: pytest.MonkeyPatch, *args: Any) -> None:
    monkeypatch.setattr(sys, "argv", ["script", *map(str, args)])
    script.main()


def test_an_unknown_mode_is_refused_before_any_store_is_opened(
    script: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    store = tmp_path / "store"
    with pytest.raises(SystemExit, match="'write' or 'load'"):
        _run(script, monkeypatch, store, "laod", tmp_path / "r.json")
    assert not store.exists()


def test_the_reports_hold_which_result_each_input_received(
    script: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    reports: list[Any] = []
    for mode in ("write", "load"):
        report = tmp_path / f"{mode}.json"
        _run(script, monkeypatch, tmp_path / "store", mode, report)
        reports.append(json.loads(report.read_text()))
    written, loaded = reports
    for label, case in written.items():
        assert len({tag for tag, _ in case["received"]}) == len(case["received"]) == 2
        assert loaded[label]["received"] == case["received"]
        assert loaded[label]["ids"] == case["ids"]
        assert loaded[label]["calls"] == 0
