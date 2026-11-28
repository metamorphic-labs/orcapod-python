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
    for label, (pod, stream) in script.cases().items():
        expected = [
            [
                repr(tag.as_dict(columns={"system_tags": True})),
                repr(data.as_dict()),
                repr({"y": pod(**data.as_dict())}),
            ]
            for tag, data in stream.iter_data()
        ]
        assert len(expected) == 2
        assert written[label]["received"] == expected
        assert loaded[label]["received"] == expected
        assert loaded[label]["ids"] == written[label]["ids"]
        assert loaded[label]["calls"] == 0


def test_a_result_of_another_input_shows_in_the_load_report(
    script: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Every input given one same stored entry: the report keeps the inputs' own tags."""
    from orcapod.core.nodes.function_node import FunctionJobNode

    report = tmp_path / "write.json"
    _run(script, monkeypatch, tmp_path / "store", "write", report)
    written = json.loads(report.read_text())
    monkeypatch.setattr(
        FunctionJobNode,
        "_stored_entry",
        lambda self, entry_id: next(iter(self._cached_output_datas.values())),
    )
    _run(script, monkeypatch, tmp_path / "store", "load", report)
    for label, case in json.loads(report.read_text()).items():
        inputs = [row[:2] for row in case["received"]]
        assert inputs == [row[:2] for row in written[label]["received"]]
        assert case["received"] != written[label]["received"]
