"""ITL-1357 measurement (ITL-1347 stage 0, a report only): Polars' inner-join row order.

A pytest plugin. Loaded with ``-p join_order_plugin`` (``docs/dev/scripts/itl_1347`` on
``PYTHONPATH``), it wraps ``polars.DataFrame.join`` for the test session. Every inner join
called without ``maintain_order`` (the key-frame joins of ``Join``, ``MergeJoin`` and
``Rejoin``) is computed a second time with ``maintain_order="left_right"``, and the two
results are compared row for row. The join's own result is returned unchanged, so the tests
run as they do without the plugin.

At the end of the session it writes ``ITL1357_REPORT`` (a JSON path) with the number of
such joins, how many gave the same row order, and, for each one that did not, the test, the
calling function and both row counts. Run it once with ``POLARS_MAX_THREADS=1`` and once at
the default.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any

import pytest

_state: dict[str, Any] = {"joins": 0, "same": 0, "different": [], "test": None}


def pytest_configure(config: Any) -> None:
    import polars as pl

    original = pl.DataFrame.join

    def join(self: Any, other: Any, *args: Any, **kwargs: Any) -> Any:
        result = original(self, other, *args, **kwargs)
        how = kwargs.get("how", args[2] if len(args) > 2 else "inner")
        if how != "inner" or "maintain_order" in kwargs:
            return result
        ordered = original(self, other, *args, maintain_order="left_right", **kwargs)
        _state["joins"] += 1
        if result.equals(ordered):
            _state["same"] += 1
        else:
            caller = sys._getframe(1).f_code  # pyright: ignore[reportPrivateUsage]  # the caller is the measurement
            _state["different"].append(
                {
                    "test": _state["test"],
                    "caller": f"{caller.co_filename}:{caller.co_name}",
                    "rows": [result.height, ordered.height],
                }
            )
        return result

    pl.DataFrame.join = join  # pyright: ignore[reportAttributeAccessIssue]  # the measurement wraps it for this session


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item: Any) -> None:
    _state["test"] = item.nodeid


def pytest_sessionfinish(session: Any, exitstatus: int) -> None:
    path = os.environ.get("ITL1357_REPORT")
    report = {
        "polars_max_threads": os.environ.get("POLARS_MAX_THREADS", "default"),
        "inner_joins_without_maintain_order": _state["joins"],
        "same_order_as_left_right": _state["same"],
        "different": _state["different"],
    }
    if path:
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=1)
